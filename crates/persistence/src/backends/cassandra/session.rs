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
//! `cdrs-tokio` 8 defaults to `ONE` and leaves serial consistency unset, so
//! nothing here relies on a driver default.
//!
//! **A compare-and-swap is never re-sent by the driver** (ADR-aacruzgon-0024).
//! It is marked non-idempotent, which keeps it out of speculative execution
//! (`cluster/session.rs:666-668`) and out of the default retry session's
//! re-sends after `Io`, `General`, `Overloaded`, `Server` and `Truncate`
//! (`retry/retry_policy.rs:72-104`). That session still re-sends any
//! statement once after `Unavailable` (`retry_policy.rs:104-118`), so a CAS
//! also carries [`FallthroughRetryPolicy`], which never retries. Whether a CAS
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
//! life of the session. The driver re-prepares one transparently when a node
//! answers `Unprepared`, which means that node did not execute it.
//!
//! **Deadlines.** `cdrs-tokio` 8 has no per-request timeout, so every
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
use cdrs_tokio::types::rows::Row;
use cdrs_tokio::types::value::Value;
use parking_lot::RwLock;

use super::config::CassandraConfig;

/// The native protocol version the session speaks. Cassandra 5.0 serves v5
/// as a stable (non-beta) version, and `cdrs-tokio` 8 speaks v3 to v5.
pub(crate) const PROTOCOL_VERSION: Version = Version::V5;

type LoadBalancing = TopologyAwareLoadBalancingStrategy<TransportTcp, TcpConnectionManager>;
type CdrsSession = Session<TransportTcp, TcpConnectionManager, LoadBalancing>;

/// What a statement is, which decides how it is sent. See the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StatementKind {
    /// A plain read at `LOCAL_QUORUM`.
    Read,
    /// A read at `LOCAL_SERIAL`, which completes or cancels an in-flight Paxos
    /// proposal before it answers.
    // Used from Step 3 on (unknown CAS outcomes, the sweeper).
    #[allow(dead_code)]
    SerialRead,
    /// A plain write at `LOCAL_QUORUM`: intent rows, version rows, index
    /// batches.
    #[allow(dead_code)]
    Write,
    /// A compare-and-swap on `resources`: Paxos at `LOCAL_SERIAL`, commit at
    /// `LOCAL_QUORUM`, never re-sent by the driver.
    #[allow(dead_code)]
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
/// `Remote`, and `TopologyAwareLoadBalancingStrategy` keeps remote **replicas**
/// in every plan, after the local ones, whatever its failover flag says
/// (`load_balancing/topology_aware.rs:190-235`). A retry or a plan whose local
/// replicas are down would then reach a coordinator in another DC, which
/// would run `LOCAL_QUORUM` and `LOCAL_SERIAL` in its own DC and break the
/// single-DC linearizability the design relies on (`cassandra-discussion.md`,
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

/// An open session plus the prepared-statement cache.
pub(crate) struct CassandraSession {
    session: CdrsSession,
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
    /// `config.keyspace` as the session keyspace.
    pub(crate) async fn connect(config: &CassandraConfig) -> Result<Self, DriverError> {
        let connect_timeout = Duration::from_millis(config.connect_timeout_ms);

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
        let node_config = within(connect_timeout, false, node_config.build()).await??;

        let pool = ConnectionPoolConfigBuilder::new()
            .with_local_size(config.pool_size)
            .with_connect_timeout(Some(connect_timeout))
            .build();

        // Replicas first. Nodes outside the local DC are ignored by
        // `LocalDcOnly`, so no remote node is ever tried; the strategy's
        // remote-DC options are off as well.
        let load_balancing = TopologyAwareLoadBalancingStrategy::new(None, false);
        let builder = TcpSessionBuilder::new(load_balancing, node_config)
            .with_node_distance_evaluator(Box::new(LocalDcOnly::new(config.local_dc.clone())))
            .with_retry_policy(Box::new(DefaultRetryPolicy))
            .with_connection_pool_config(pool)
            .with_keyspace(config.keyspace.clone());

        let session = within(connect_timeout, false, builder.build())
            .await?
            .map_err(|error| {
                DriverError::from(cdrs_tokio::error::Error::General(error.to_string()))
            })?;

        Ok(Self {
            session,
            prepared: RwLock::new(HashMap::new()),
            request_timeout: Duration::from_millis(config.request_timeout_ms),
            no_retry: Arc::new(FallthroughRetryPolicy),
        })
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
    /// request deadline covers the prepare, on first use, and the execute
    /// together.
    pub(crate) async fn execute(
        &self,
        cql: &str,
        values: Vec<Value>,
        kind: StatementKind,
    ) -> Result<Envelope, DriverError> {
        let started = Instant::now();
        let prepared = self.prepared(cql, self.request_timeout).await?;
        let params = statement_params(kind, values, &self.no_retry);
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
        NodeInfo {
            host_id: Default::default(),
            broadcast_rpc_address: "127.0.0.1:9042".parse().unwrap(),
            broadcast_address: None,
            datacenter: datacenter.to_string(),
            tokens: Vec::new(),
            rack: "helios-rack1".to_string(),
        }
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
