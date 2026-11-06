//! The `cdrs-tokio` session and the one place statements are executed.
//!
//! Every statement goes through [`CassandraSession::execute`], which sets the
//! consistency from the statement's [`StatementKind`] (design, "Consistency
//! levels"; `docs/Cassandra/schema/walkthrough.md` section 1):
//!
//! | Kind | Consistency | Serial consistency | Idempotent | Driver retry |
//! | --- | --- | --- | --- | --- |
//! | [`Read`](StatementKind::Read) | `LOCAL_QUORUM` | unset | yes | default |
//! | [`SerialRead`](StatementKind::SerialRead) | `LOCAL_SERIAL` | unset | yes | default |
//! | [`Write`](StatementKind::Write) | `LOCAL_QUORUM` | unset | yes | default |
//! | [`Cas`](StatementKind::Cas) | `LOCAL_QUORUM` (commit) | `LOCAL_SERIAL` (Paxos) | **no** | **none** |
//!
//! `cdrs-tokio` 10 defaults to `ONE` and leaves serial consistency unset, so
//! nothing here relies on a driver default.
//!
//! **A compare-and-swap is never re-sent by the driver** (ADR-aacruzgon-0024).
//! It is marked non-idempotent, which keeps it out of speculative execution
//! (`cdrs-tokio` 10.0.0, `cluster/session.rs:832-833`) and out of the default
//! retry session's re-sends after `Io`, `General`, `Overloaded`, `Server` and
//! `Truncate` (`retry/retry_policy.rs:92-123`). That session still re-sends
//! any statement once after `Unavailable` (`:124-138`), once more on the same
//! node after a `ReadTimeout` that retrieved no data (`:139-156`), and after
//! `IsBootstrapping` (`:175-182`), whatever its idempotence, so a CAS also
//! carries [`FallthroughRetryPolicy`], which never retries (`:52-69`). The
//! other three kinds keep the default session: each of those re-sends is safe
//! for a read, and for a write with a fixed `USING TIMESTAMP`. Whether a CAS
//! whose outcome is unknown ran is decided by the caller, from a
//! [`SerialRead`](StatementKind::SerialRead), never by the driver.
//!
//! No speculative-execution policy is set on the session or on any statement.
//!
//! Plain writes are idempotent: version and index rows carry a fixed
//! `USING TIMESTAMP`, and re-sending an intent row or its delete writes the
//! same key again.
//!
//! **Prepared statements** are prepared once per CQL text and kept for the
//! life of the driver session. The driver re-prepares one transparently when
//! a node answers `Unprepared`, which means that node did not execute it.
//!
//! **After a restart** the driver recovers the session by itself: since
//! `cdrs-tokio` 9.1.0 a control-connection reconnect keeps a known node, and
//! promotes it to up if it was down (10.0.0, `cluster/topology/node.rs:376-413`),
//! and its pool reconnects as soon as it is marked up. 8.1.9 lost a single node
//! for good there, which an earlier version of this module worked around by
//! rebuilding the session. `backend.rs`, `restart_tests`, checks the recovery
//! against a node of its own.
//!
//! **Deadlines.** `cdrs-tokio` 10 has no per-request timeout, so every
//! statement runs under the configured request deadline. When it elapses the
//! caller gets [`DriverError::Deadline`]; for a CAS that is an unknown outcome
//! (design, "When the LWT outcome is unknown").

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use cdrs_tokio::authenticators::StaticPasswordAuthenticatorProvider;
use cdrs_tokio::cluster::connection_pool::ConnectionPoolConfigBuilder;
use cdrs_tokio::cluster::session::{Session, SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::topology::NodeDistance;
use cdrs_tokio::cluster::{NodeAddress, NodeInfo, NodeTcpConfigBuilder, TcpConnectionManager};
use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::frame::Envelope;
use cdrs_tokio::frame::Version;
use cdrs_tokio::load_balancing::TopologyAwareLoadBalancingStrategy;
use cdrs_tokio::load_balancing::node_distance_evaluator::NodeDistanceEvaluator;
use cdrs_tokio::query::{PreparedQuery, QueryValues};
use cdrs_tokio::retry::{DefaultRetryPolicy, FallthroughRetryPolicy, RetryPolicy};
use cdrs_tokio::statement::{StatementParams, StatementParamsBuilder};
use cdrs_tokio::transport::TransportTcp;
use cdrs_tokio::types::CBytes;
use cdrs_tokio::types::rows::Row;
use cdrs_tokio::types::value::Value;
use parking_lot::RwLock;

use super::config::CassandraConfig;

/// The native protocol version the session speaks. Cassandra 5.0 serves v5
/// as a stable (non-beta) version, and `cdrs-tokio` 10 speaks v3 to v5.
pub(crate) const PROTOCOL_VERSION: Version = Version::V5;

/// Rows per page of a [`CassandraSession::rows_paged`] read. Without a page
/// size the node returns the whole result in one message (native protocol v5,
/// section 7, "Result paging"); 5000 is the DataStax Java driver's default.
///
/// A 5000-row page of `resources_by_type`'s partition keys spans several
/// protocol v5 frames (over 128 KiB). `cdrs-tokio` 8.1.9 lost such a
/// response (`cassandra-protocol` 3.3.0, `frame/frame_decoder.rs:271` and
/// `:277`); 9.0.2 fixed it (krojew/cdrs-tokio `31d69ea89`), and this module
/// is on 10.0.0.
pub(crate) const PAGE_SIZE: i32 = 5000;

/// One page request: its size, and the node's paging state from the page
/// before (`None` for the first page).
struct Paging {
    page_size: i32,
    state: Option<CBytes>,
}

type LoadBalancing = TopologyAwareLoadBalancingStrategy<TransportTcp, TcpConnectionManager>;
type CdrsSession = Session<TransportTcp, TcpConnectionManager, LoadBalancing>;

/// What a statement is, which decides how it is sent. See the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatementKind {
    /// A plain read at `LOCAL_QUORUM`.
    Read,
    /// A read at `LOCAL_SERIAL`, which completes or cancels an in-flight Paxos
    /// proposal before it answers.
    SerialRead,
    /// A plain write at `LOCAL_QUORUM`: intent rows, version rows, index
    /// batches.
    Write,
    /// A compare-and-swap on `resources`: Paxos at `LOCAL_SERIAL`, commit at
    /// `LOCAL_QUORUM`, never re-sent by the driver.
    Cas,
}

impl StatementKind {
    /// The statement's consistency level.
    pub(crate) fn consistency(self) -> Consistency {
        match self {
            StatementKind::SerialRead => Consistency::LocalSerial,
            StatementKind::Read | StatementKind::Write | StatementKind::Cas => {
                Consistency::LocalQuorum
            }
        }
    }

    /// The serial consistency of the Paxos phase; only a CAS has one.
    pub(crate) fn serial_consistency(self) -> Option<Consistency> {
        match self {
            StatementKind::Cas => Some(Consistency::LocalSerial),
            _ => None,
        }
    }

    /// Whether the driver may send the statement again.
    pub(crate) fn is_idempotent(self) -> bool {
        !matches!(self, StatementKind::Cas)
    }
}

/// A failed statement, before it is mapped to a [`BackendError`].
///
/// Kept raw so that the compare-and-swap path can classify a `WriteTimeout`
/// or `WriteFailure` by `write_type` first (Step 3); everything else goes
/// through [`super::errors::map_driver_error`].
///
/// [`BackendError`]: crate::error::BackendError
#[derive(Debug)]
pub(crate) enum DriverError {
    /// The driver, the transport or the node reported an error. Boxed: the
    /// driver's error is several hundred bytes.
    Driver(Box<cdrs_tokio::error::Error>),
    /// The backend's own request deadline elapsed first.
    Deadline {
        /// The deadline that elapsed.
        after: Duration,
        /// Whether the statement itself had been handed to the driver. `false`
        /// when the deadline ran out while connecting or preparing, so a CAS
        /// cut off there was never sent and its outcome is not unknown.
        sent: bool,
    },
}

/// Nodes in the local data centre are `Local`; every other node gets no
/// distance, which the driver treats as ignored: it opens no pool to it and
/// never puts it in a query plan (`cluster/topology/node.rs`, `is_ignored`).
///
/// The driver's own `TopologyAwareNodeDistanceEvaluator` marks other-DC nodes
/// `Remote`. Since `cdrs-tokio` 9.1.0, `TopologyAwareLoadBalancingStrategy`
/// leaves remote nodes out of a plan for a dc-local consistency on a
/// `NetworkTopologyStrategy` keyspace (10.0.0,
/// `load_balancing/topology_aware.rs:264-272`), but not for a
/// `SimpleStrategy` keyspace, whose plan ends with every unignored node, local
/// or remote (`:292-330`). A plan whose local nodes are down would then reach
/// a coordinator in another DC, which would run `LOCAL_QUORUM` and
/// `LOCAL_SERIAL` in its own DC and break the single-DC linearizability the design relies on (`cassandra-discussion.md`,
/// data model: "`LOCAL_SERIAL` is linearizable only within one data centre").
#[derive(Debug, Clone)]
pub(crate) struct LocalDcOnly {
    local_dc: String,
}

impl LocalDcOnly {
    pub(crate) fn new(local_dc: impl Into<String>) -> Self {
        Self {
            local_dc: local_dc.into(),
        }
    }
}

impl NodeDistanceEvaluator for LocalDcOnly {
    fn compute_distance(&self, node: &NodeInfo) -> Option<NodeDistance> {
        (node.datacenter == self.local_dc).then_some(NodeDistance::Local)
    }
}

impl From<cdrs_tokio::error::Error> for DriverError {
    fn from(error: cdrs_tokio::error::Error) -> Self {
        DriverError::Driver(Box::new(error))
    }
}

/// The message `cdrs-tokio` 10.0.0 returns when a statement's query plan
/// holds no node (`cluster/session.rs:892`, `:906`); when every node in the
/// plan failed, it returns the last node's error instead (since 9.1.0). The
/// driver has no error variant of its own for it: it is an
/// [`Error::General`] with this text.
///
/// [`Error::General`]: cdrs_tokio::error::Error::General
pub(crate) const NO_NODE_IN_PLAN: &str = "No nodes available in query plan!";

/// The backend's driver session and the statements prepared on it.
pub(crate) struct CassandraSession {
    session: CdrsSession,
    /// Prepared statements by CQL text, for the life of the session.
    prepared: RwLock<HashMap<String, Arc<PreparedQuery>>>,
    request_timeout: Duration,
    no_retry: Arc<dyn RetryPolicy + Send + Sync>,
}

impl std::fmt::Debug for CassandraSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CassandraSession")
            .field("prepared_statements", &self.prepared.read().len())
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

impl CassandraSession {
    /// Opens a session to `config.contact_points`, routing every request to
    /// the replicas in `config.local_dc` (ADR-aacruzgon-0003), with
    /// `config.keyspace` as the session keyspace, all within
    /// `config.connect_timeout_ms`.
    ///
    /// The driver fails the build by itself when no contact point connects
    /// (`cdrs-tokio` 10.0.0, `cluster/control_connection.rs:170-176`), but when
    /// one connects and the metadata refresh fails (a node still starting) it
    /// retries for good (`:166-169`); the deadline bounds that. Cutting the
    /// build off is safe: the driver aborts its control-connection task when
    /// the build future is dropped (`cluster/session.rs:996-1006`).
    pub(crate) async fn connect(config: &CassandraConfig) -> Result<Self, DriverError> {
        let deadline = Duration::from_millis(config.connect_timeout_ms);
        let session = within(deadline, false, async {
            session_builder(config)
                .await?
                .build()
                .await
                .map_err(build_error)
        })
        .await??;
        Ok(Self {
            session,
            prepared: RwLock::new(HashMap::new()),
            request_timeout: Duration::from_millis(config.request_timeout_ms),
            no_retry: Arc::new(FallthroughRetryPolicy),
        })
    }

    /// How many statements are prepared on the session.
    #[cfg(test)]
    pub(crate) fn prepared_count(&self) -> usize {
        self.prepared.read().len()
    }

    /// The prepared form of `cql`, preparing it on first use within
    /// `deadline`.
    async fn prepared(
        &self,
        cql: &str,
        deadline: Duration,
    ) -> Result<Arc<PreparedQuery>, DriverError> {
        if let Some(prepared) = self.prepared.read().get(cql) {
            return Ok(prepared.clone());
        }
        // Two callers may prepare the same text at once; both get the same
        // statement id from the node, and the last insert wins.
        let prepared = Arc::new(within(deadline, false, self.session.prepare(cql)).await??);
        self.prepared
            .write()
            .insert(cql.to_string(), prepared.clone());
        Ok(prepared)
    }

    /// Executes `cql` (prepared and cached) with positional `values`. The
    /// request deadline covers the prepare, on first use, and the execute.
    pub(crate) async fn execute(
        &self,
        cql: &str,
        values: Vec<Value>,
        kind: StatementKind,
    ) -> Result<Envelope, DriverError> {
        self.execute_with(cql, values, kind, None).await
    }

    /// [`Self::execute`], for one page when `paging` is set.
    async fn execute_with(
        &self,
        cql: &str,
        values: Vec<Value>,
        kind: StatementKind,
        paging: Option<Paging>,
    ) -> Result<Envelope, DriverError> {
        let started = Instant::now();
        let prepared = self.prepared(cql, self.request_timeout).await?;
        let mut params = statement_params(kind, values, &self.no_retry);
        if let Some(paging) = paging {
            params.query_params.page_size = Some(paging.page_size);
            params.query_params.paging_state = paging.state;
        }
        let remaining = self.request_timeout.saturating_sub(started.elapsed());
        Ok(within(
            remaining,
            true,
            self.session.exec_with_params(&prepared, &params),
        )
        .await??)
    }

    /// Executes `cql` and returns its rows (empty for a statement without a
    /// result set).
    pub(crate) async fn rows(
        &self,
        cql: &str,
        values: Vec<Value>,
        kind: StatementKind,
    ) -> Result<Vec<Row>, DriverError> {
        let envelope = self.execute(cql, values, kind).await?;
        let body = envelope.response_body()?;
        Ok(body.into_rows().unwrap_or_default())
    }

    /// Executes `cql` page by page, [`PAGE_SIZE`] rows at a time, and returns
    /// every row of every page.
    ///
    /// [`Self::rows`] sets no page size, so the node builds the whole result
    /// in one message. That is right for a single partition of known size,
    /// not for a range scan. Each page goes through [`Self::execute`]'s path,
    /// with the kind's consistency and its own deadline, rather than the
    /// driver's pager: `cdrs-tokio` 8.1.9's pager dropped the consistency
    /// (`cluster/pager.rs:199-223`); 9.1.0's carries the statement parameters
    /// across pages, but it would put the whole scan under no deadline of
    /// ours. A page whose metadata carries a paging state has more after it
    /// (`cassandra-protocol` 5.0.0, `frame/message_result.rs:404`: the state
    /// is read only when `HAS_MORE_PAGES` is set).
    pub(crate) async fn rows_paged(
        &self,
        cql: &str,
        values: Vec<Value>,
        kind: StatementKind,
    ) -> Result<Vec<Row>, DriverError> {
        self.rows_paged_by(cql, values, kind, PAGE_SIZE).await
    }

    /// [`Self::rows_paged`] with `page_size` rows per page.
    pub(crate) async fn rows_paged_by(
        &self,
        cql: &str,
        values: Vec<Value>,
        kind: StatementKind,
        page_size: i32,
    ) -> Result<Vec<Row>, DriverError> {
        let mut all = Vec::new();
        let mut state = None;
        loop {
            let paging = Paging {
                page_size,
                state: state.take(),
            };
            let envelope = self
                .execute_with(cql, values.clone(), kind, Some(paging))
                .await?;
            let body = envelope.response_body()?;
            let next = body
                .as_rows_metadata()
                .and_then(|metadata| metadata.paging_state.clone());
            all.extend(body.into_rows().unwrap_or_default());
            match next {
                Some(next) => state = Some(next),
                None => return Ok(all),
            }
        }
    }
}

/// The driver's session builder for `config`. Resolving the contact points
/// spawns nothing, so it is safe to cancel.
async fn session_builder(
    config: &CassandraConfig,
) -> Result<TcpSessionBuilder<LoadBalancing>, DriverError> {
    let mut node_config = NodeTcpConfigBuilder::new()
        .with_contact_points(
            config
                .contact_points
                .iter()
                .map(|point| NodeAddress::from(point.as_str()))
                .collect(),
        )
        .with_version(PROTOCOL_VERSION);
    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        node_config = node_config.with_authenticator_provider(Arc::new(
            StaticPasswordAuthenticatorProvider::new(username, password),
        ));
    }
    let node_config = node_config.build().await?;

    let pool = ConnectionPoolConfigBuilder::new()
        .with_local_size(config.pool_size)
        .with_connect_timeout(Some(Duration::from_millis(config.connect_timeout_ms)))
        .build();

    // Replicas first. Nodes outside the local DC are ignored by
    // `LocalDcOnly`, so no remote node is ever tried; the strategy's
    // remote-DC options are off as well.
    let load_balancing = TopologyAwareLoadBalancingStrategy::new(None, false);
    Ok(TcpSessionBuilder::new(load_balancing, node_config)
        .with_node_distance_evaluator(Box::new(LocalDcOnly::new(config.local_dc.clone())))
        .with_retry_policy(Box::new(DefaultRetryPolicy))
        .with_connection_pool_config(pool)
        .with_keyspace(config.keyspace.clone()))
}

/// A failed session build, as a driver error.
fn build_error(error: impl std::fmt::Display) -> DriverError {
    DriverError::from(cdrs_tokio::error::Error::General(error.to_string()))
}

/// The parameters `kind` requires; see the module docs.
fn statement_params(
    kind: StatementKind,
    values: Vec<Value>,
    no_retry: &Arc<dyn RetryPolicy + Send + Sync>,
) -> StatementParams {
    let mut builder = StatementParamsBuilder::new()
        .with_consistency(kind.consistency())
        .idempotent(kind.is_idempotent());
    if let Some(serial) = kind.serial_consistency() {
        builder = builder.with_serial_consistency(serial);
    }
    if !values.is_empty() {
        builder = builder.with_values(QueryValues::SimpleValues(values));
    }
    if kind == StatementKind::Cas {
        builder = builder.with_retry_policy(no_retry.clone());
    }
    builder.build()
}

/// Runs `future` under `deadline`; `sent` says whether `future` hands the
/// statement itself to the driver (see [`DriverError::Deadline`]).
async fn within<F: std::future::Future>(
    deadline: Duration,
    sent: bool,
    future: F,
) -> Result<F::Output, DriverError> {
    tokio::time::timeout(deadline, future)
        .await
        .map_err(|_| DriverError::Deadline {
            after: deadline,
            sent,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_sets_its_consistency_explicitly() {
        assert_eq!(StatementKind::Read.consistency(), Consistency::LocalQuorum);
        assert_eq!(StatementKind::Write.consistency(), Consistency::LocalQuorum);
        assert_eq!(StatementKind::Cas.consistency(), Consistency::LocalQuorum);
        assert_eq!(
            StatementKind::SerialRead.consistency(),
            Consistency::LocalSerial
        );
    }

    #[test]
    fn only_a_cas_has_a_serial_consistency_and_it_is_local_serial() {
        assert_eq!(
            StatementKind::Cas.serial_consistency(),
            Some(Consistency::LocalSerial)
        );
        for kind in [
            StatementKind::Read,
            StatementKind::SerialRead,
            StatementKind::Write,
        ] {
            assert_eq!(kind.serial_consistency(), None, "{kind:?}");
        }
    }

    #[test]
    fn statement_params_carry_the_kind_rules() {
        let no_retry: Arc<dyn RetryPolicy + Send + Sync> = Arc::new(FallthroughRetryPolicy);
        for kind in [
            StatementKind::Read,
            StatementKind::SerialRead,
            StatementKind::Write,
            StatementKind::Cas,
        ] {
            let params = statement_params(kind, vec![Value::from("t")], &no_retry);
            assert_eq!(
                params.query_params.consistency,
                kind.consistency(),
                "{kind:?}"
            );
            assert_eq!(
                params.query_params.serial_consistency,
                kind.serial_consistency(),
                "{kind:?}"
            );
            assert_eq!(params.is_idempotent, kind.is_idempotent(), "{kind:?}");
            assert!(params.speculative_execution_policy.is_none(), "{kind:?}");
            assert_eq!(
                params.retry_policy.is_some(),
                kind == StatementKind::Cas,
                "{kind:?}: only a CAS overrides the session retry policy"
            );
        }
    }

    fn node_in(datacenter: &str) -> NodeInfo {
        NodeInfo::new(
            Default::default(),
            "127.0.0.1:9042".parse().unwrap(),
            None,
            datacenter.to_string(),
            Vec::new(),
            "helios-rack1".to_string(),
        )
    }

    #[test]
    fn nodes_outside_the_local_dc_get_no_distance_so_are_ignored() {
        let evaluator = LocalDcOnly::new("helios-dc1");
        assert_eq!(
            evaluator.compute_distance(&node_in("helios-dc1")),
            Some(NodeDistance::Local)
        );
        assert_eq!(evaluator.compute_distance(&node_in("helios-dc2")), None);
        assert_eq!(evaluator.compute_distance(&node_in("")), None);
    }

    #[test]
    fn a_cas_is_the_only_non_idempotent_statement() {
        assert!(!StatementKind::Cas.is_idempotent());
        assert!(StatementKind::Read.is_idempotent());
        assert!(StatementKind::SerialRead.is_idempotent());
        assert!(StatementKind::Write.is_idempotent());
    }
}
