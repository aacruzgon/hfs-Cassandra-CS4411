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
//! for a read, and for a write with a fixed `USING TIMESTAMP`. The exception
//! is an index write sent by [`CassandraSession::batch`]: an index batch
//! carries [`FallthroughRetryPolicy`] too, so each of the writer's tries is
//! one send (below), and so does a lone index row. Whether a CAS
//! whose outcome is unknown ran is decided by the caller, from a
//! [`SerialRead`](StatementKind::SerialRead), never by the driver.
//!
//! No speculative-execution policy is set on the session or on any statement.
//!
//! Plain writes are idempotent: version and index rows carry a fixed
//! `USING TIMESTAMP`, and re-sending an intent row or its delete writes the
//! same key again.
//!
//! **Index batches** go through [`CassandraSession::batch`], the one place a
//! native-protocol batch is sent (roadmap Step 5; ADR-sam-michaels-0006,
//! IW10 and IW13). A batch is always logged, always `LOCAL_QUORUM`, and
//! idempotent like any other write. Its consistency is set on the batch
//! itself: `cdrs-tokio` 10 takes a batch's consistency from the batch
//! request, not from the statement parameters (10.0.0,
//! `cluster/session.rs:536`), and the driver's batch builder defaults it to
//! `ONE` (`cassandra-protocol` 5.0.0, `query/batch_query_builder.rs:41`), so
//! a batch sent without it would write at `ONE`. It takes only
//! [`IndexStatement`]s, so it can hold nothing but index-table rows (IW20,
//! `batch.rs`). A batch of one statement is sent as a plain write with the
//! same no-retry policy, so one try is one send either way. The driver
//! never re-sends a batch by itself: the writer retries it (IW16), and the
//! driver's default policy would add a hidden second send after a
//! `BATCH_LOG` timeout (`retry/retry_policy.rs:157-174`).
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
//! **Counting.** Every statement is counted by kind before it is sent
//! ([`StatementCounts`], `CassandraBackend::statement_counts`), for the
//! write-amplification report (Step 3 CI report R5). A batch counts once as
//! a batch request, and its statements count as batched (IW14).
//!
//! **Fault injection, tests only.** Under `cfg(test)`, and only there, a test
//! can make the next matching statement report `WriteTimeout`/CAS, with or
//! without executing it, or execute it late (`Fault`). A fault registered for
//! a write also matches a batch holding a statement that matches it. It has
//! no Cargo feature, so nothing outside this crate's unit tests can reach it.
//!
//! **Deadlines.** `cdrs-tokio` 10 has no per-request timeout, so every
//! statement runs under the configured request deadline. When it elapses the
//! caller gets [`DriverError::Deadline`]; for a CAS that is an unknown outcome
//! (design, "When the LWT outcome is unknown").

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cdrs_tokio::authenticators::StaticPasswordAuthenticatorProvider;
use cdrs_tokio::cluster::connection_pool::ConnectionPoolConfigBuilder;
use cdrs_tokio::cluster::session::{Session, SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::topology::NodeDistance;
use cdrs_tokio::cluster::{NodeAddress, NodeInfo, NodeTcpConfigBuilder, TcpConnectionManager};
use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::frame::Envelope;
use cdrs_tokio::frame::Version;
use cdrs_tokio::frame::message_batch::BatchType;
use cdrs_tokio::load_balancing::TopologyAwareLoadBalancingStrategy;
use cdrs_tokio::load_balancing::node_distance_evaluator::NodeDistanceEvaluator;
use cdrs_tokio::query::{BatchQueryBuilder, PreparedQuery, QueryBatch, QueryValues};
use cdrs_tokio::retry::{DefaultRetryPolicy, FallthroughRetryPolicy, RetryPolicy};
use cdrs_tokio::statement::{StatementParams, StatementParamsBuilder};
use cdrs_tokio::transport::TransportTcp;
use cdrs_tokio::types::CBytes;
use cdrs_tokio::types::rows::Row;
use cdrs_tokio::types::value::Value;
use parking_lot::RwLock;

use super::batch::IndexStatement;
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
    /// rows. An index batch is sent as this kind, but counted apart.
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
    /// Statements sent, per kind (CI report R5).
    counts: StatementCounters,
    /// The faults tests inject (`cfg(test)` only; see [`Fault`]).
    #[cfg(test)]
    faults: faults::Faults,
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
            counts: StatementCounters::default(),
            #[cfg(test)]
            faults: faults::Faults::default(),
        })
    }

    /// How many statements are prepared on the session.
    #[cfg(test)]
    pub(crate) fn prepared_count(&self) -> usize {
        self.prepared.read().len()
    }

    /// How many statements of each kind this session has been asked to send.
    pub(crate) fn statement_counts(&self) -> StatementCounts {
        self.counts.snapshot()
    }

    /// Makes the next statement of `kind` whose CQL contains `cql_contains`
    /// fail as `fault` says (tests only).
    #[cfg(test)]
    pub(crate) fn inject_fault(&self, kind: StatementKind, cql_contains: &str, fault: Fault) {
        self.faults.inject(kind, cql_contains, fault, false);
    }

    /// [`Self::inject_fault`], for the first page of a paged read
    /// ([`Self::rows_paged`]). Only a fault injected this way reaches a paged
    /// statement, so the others keep the statements they have always matched.
    #[cfg(test)]
    pub(crate) fn inject_fault_on_first_page(
        &self,
        kind: StatementKind,
        cql_contains: &str,
        fault: Fault,
    ) {
        self.faults.inject(kind, cql_contains, fault, true);
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
        self.execute_with(cql, values, kind, None, Retries::OfKind)
            .await
    }

    /// [`Self::execute`], for one page when `paging` is set.
    async fn execute_with(
        &self,
        cql: &str,
        values: Vec<Value>,
        kind: StatementKind,
        paging: Option<Paging>,
        retries: Retries,
    ) -> Result<Envelope, DriverError> {
        self.counts.count(kind);
        #[cfg(test)]
        {
            let first_page = paging.as_ref().is_some_and(|paging| paging.state.is_none());
            if (paging.is_none() || first_page)
                && let Some(result) = self
                    .faults
                    .intercept(self, cql, &values, kind, first_page)
                    .await
            {
                return result;
            }
        }
        self.send_with(cql, values, kind, paging, retries).await
    }

    /// Prepares (on first use) and sends one statement under the request
    /// deadline, with no counting and no injected fault.
    async fn send(
        &self,
        cql: &str,
        values: Vec<Value>,
        kind: StatementKind,
        paging: Option<Paging>,
    ) -> Result<Envelope, DriverError> {
        self.send_with(cql, values, kind, paging, Retries::OfKind)
            .await
    }

    /// [`Self::send`] with the retry policy chosen by `retries`.
    async fn send_with(
        &self,
        cql: &str,
        values: Vec<Value>,
        kind: StatementKind,
        paging: Option<Paging>,
        retries: Retries,
    ) -> Result<Envelope, DriverError> {
        let started = Instant::now();
        let prepared = self.prepared(cql, self.request_timeout).await?;
        let mut params = match retries {
            Retries::OfKind => statement_params(kind, values, &self.no_retry),
            Retries::Never => lone_row_params(values, &self.no_retry),
        };
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
        self.rows_paged_keeping(cql, values, kind, page_size, |_| true)
            .await
    }

    /// [`Self::rows_paged_by`], keeping only the rows `keep` accepts, page by
    /// page, so a scan of many rows holds only those it needs.
    pub(crate) async fn rows_paged_keeping(
        &self,
        cql: &str,
        values: Vec<Value>,
        kind: StatementKind,
        page_size: i32,
        keep: impl Fn(&Row) -> bool,
    ) -> Result<Vec<Row>, DriverError> {
        let mut all = Vec::new();
        let mut state = None;
        loop {
            let paging = Paging {
                page_size,
                state: state.take(),
            };
            let envelope = self
                .execute_with(cql, values.clone(), kind, Some(paging), Retries::OfKind)
                .await?;
            let body = envelope.response_body()?;
            let next = body
                .as_rows_metadata()
                .and_then(|metadata| metadata.paging_state.clone());
            all.extend(
                body.into_rows()
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|row| keep(row)),
            );
            match next {
                Some(next) => state = Some(next),
                None => return Ok(all),
            }
        }
    }

    /// Sends `statements` as one logged batch at `LOCAL_QUORUM` (IW10,
    /// IW13), with what [`Self::execute`] gives a statement: each prepared
    /// once and cached, counted before it is sent (one batch request,
    /// `statements.len()` batched), the test fault seam, and one request
    /// deadline over the prepares and the send. Nothing is sent for no
    /// statements, and one statement goes as a plain [`StatementKind::Write`]
    /// through [`Self::execute`]'s path, counted as a write, but with the
    /// no-retry policy a batch has ([`lone_row_params`]): a batch of one buys
    /// nothing, and the node would treat it as a single mutation anyway. So
    /// "one try, one send" holds for a batch and for a lone row alike.
    ///
    /// Each statement binds its own `USING TIMESTAMP`, so the batch sets no
    /// timestamp of its own. Cassandra applies the rows of one partition key
    /// in a batch as one mutation, whole on each replica; the batchlog makes
    /// the rest all or nothing across partitions, eventually (walkthrough
    /// section 2, "Index rows"). What a failure means is
    /// [`classify_failure`](super::batch::classify_failure)'s to say.
    pub(crate) async fn batch(&self, statements: Vec<IndexStatement>) -> Result<(), DriverError> {
        if statements.len() <= 1 {
            return match statements.into_iter().next() {
                Some(statement) => {
                    let (cql, values) = statement.into_parts();
                    self.execute_with(cql, values, StatementKind::Write, None, Retries::Never)
                        .await
                        .map(|_| ())
                }
                None => Ok(()),
            };
        }
        self.counts.count_batch(statements.len());
        #[cfg(test)]
        if let Some(result) = self.faults.intercept_batch(self, &statements).await {
            return result;
        }
        self.send_batch(&statements).await
    }

    /// Prepares (on first use) and sends one batch under the request
    /// deadline, with no counting and no injected fault.
    async fn send_batch(&self, statements: &[IndexStatement]) -> Result<(), DriverError> {
        let started = Instant::now();
        let mut prepared = Vec::with_capacity(statements.len());
        for statement in statements {
            let remaining = self.request_timeout.saturating_sub(started.elapsed());
            prepared.push(self.prepared(statement.cql(), remaining).await?);
        }
        let batch = index_batch(statements.iter().zip(prepared.iter().map(Arc::as_ref)))?;
        let params = batch_params(&self.no_retry);
        let remaining = self.request_timeout.saturating_sub(started.elapsed());
        within(
            remaining,
            true,
            self.session.batch_with_params(batch, &params),
        )
        .await??;
        Ok(())
    }
}

/// The driver batch of `statements`, each with its prepared form: logged,
/// at `LOCAL_QUORUM`, with no serial consistency and no batch-wide
/// timestamp (IW10, IW13). The consistency must be set here: the builder's
/// default is `ONE`, and the driver reads a batch's consistency from the
/// batch, not from its parameters (see the module docs).
fn index_batch<'a>(
    statements: impl IntoIterator<Item = (&'a IndexStatement, &'a PreparedQuery)>,
) -> Result<QueryBatch, DriverError> {
    let mut builder = BatchQueryBuilder::new()
        .with_batch_type(BatchType::Logged)
        .with_consistency(StatementKind::Write.consistency());
    for (statement, prepared) in statements {
        builder = builder.add_query_prepared(
            prepared,
            QueryValues::SimpleValues(statement.values().to_vec()),
        );
    }
    Ok(builder.build()?)
}

/// The parameters an index batch is sent with (IW13, IW15). The driver takes
/// a batch's consistency from the batch itself ([`index_batch`]); from these
/// it takes the idempotence and the retry policy. A batch carries
/// [`FallthroughRetryPolicy`], as a CAS does, so the driver never re-sends it
/// by itself: the default policy would re-send an idempotent batch once on
/// the same node after a `BATCH_LOG` timeout (`cdrs-tokio` 10.0.0,
/// `retry/retry_policy.rs:157-174`). Each of the writer's tries is then one
/// send, which the counters see (IW14), and a hung node costs one node
/// timeout per try, not two, inside the index phase's deadline.
fn batch_params(no_retry: &Arc<dyn RetryPolicy + Send + Sync>) -> StatementParams {
    let mut params = statement_params(StatementKind::Write, Vec::new(), no_retry);
    params.retry_policy = Some(no_retry.clone());
    params
}

/// The parameters a lone index row is sent with (IW13, IW15): those of a
/// plain [`StatementKind::Write`], with [`FallthroughRetryPolicy`] in place
/// of the session's, as a batch has ([`batch_params`]). The default policy
/// would re-send an idempotent write once after `Unavailable`, a send the
/// counters never see; with this one, each of the writer's tries is one send.
fn lone_row_params(
    values: Vec<Value>,
    no_retry: &Arc<dyn RetryPolicy + Send + Sync>,
) -> StatementParams {
    let mut params = statement_params(StatementKind::Write, values, no_retry);
    params.retry_policy = Some(no_retry.clone());
    params
}

/// Which retry policy a statement is sent with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Retries {
    /// The one its [`StatementKind`] requires ([`statement_params`]).
    OfKind,
    /// Never re-sent by the driver: a lone index row.
    Never,
}

/// Statements sent, one counter per [`StatementKind`], and the batches.
#[derive(Debug, Default)]
struct StatementCounters {
    read: AtomicU64,
    serial_read: AtomicU64,
    write: AtomicU64,
    batch: AtomicU64,
    batched: AtomicU64,
    cas: AtomicU64,
}

impl StatementCounters {
    fn count(&self, kind: StatementKind) {
        let counter = match kind {
            StatementKind::Read => &self.read,
            StatementKind::SerialRead => &self.serial_read,
            StatementKind::Write => &self.write,
            StatementKind::Cas => &self.cas,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// One batch request holding `statements` statements.
    fn count_batch(&self, statements: usize) {
        self.batch.fetch_add(1, Ordering::Relaxed);
        self.batched.fetch_add(statements as u64, Ordering::Relaxed);
    }

    fn snapshot(&self) -> StatementCounts {
        StatementCounts {
            read: self.read.load(Ordering::Relaxed),
            serial_read: self.serial_read.load(Ordering::Relaxed),
            write: self.write.load(Ordering::Relaxed),
            batch: self.batch.load(Ordering::Relaxed),
            batched: self.batched.load(Ordering::Relaxed),
            cas: self.cas.load(Ordering::Relaxed),
        }
    }
}

/// How many statements of each kind a backend's session has been asked to
/// send since it was built: every request, one per page of a paged read,
/// failed ones included (Step 3 CI report R5, write amplification). Take one
/// snapshot before and one after an operation and subtract with
/// [`StatementCounts::since`].
///
/// A batch is counted twice over, in fields of its own (IW14, separate
/// fields): once in `batch`, as the one request it is, and once per
/// statement it carries in `batched`. `write` counts only statements sent
/// alone, so a reader sees both the requests and the rows they carried.
///
/// These are the writer's requests, not wire round trips: when the
/// coordinator has lost a prepared ID, `cdrs-tokio` prepares it again and
/// re-sends inside the same call (10.0.0, `cluster/session.rs:567-600`), and
/// such a request counts once.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StatementCounts {
    /// Plain reads at `LOCAL_QUORUM` (R1, R3, `count`, …).
    pub read: u64,
    /// Reads at `LOCAL_SERIAL` (R2).
    pub serial_read: u64,
    /// Plain writes at `LOCAL_QUORUM` sent alone (V1 to V3, I1, I2, an index
    /// row with no other row in its batch).
    pub write: u64,
    /// Logged batch requests at `LOCAL_QUORUM` (index rows), each one round
    /// trip whatever it holds.
    pub batch: u64,
    /// Statements carried inside those batches.
    pub batched: u64,
    /// Compare-and-swaps (C1 to C4).
    pub cas: u64,
}

impl StatementCounts {
    /// What was sent between `earlier` and `self`.
    pub fn since(&self, earlier: &StatementCounts) -> StatementCounts {
        StatementCounts {
            read: self.read.saturating_sub(earlier.read),
            serial_read: self.serial_read.saturating_sub(earlier.serial_read),
            write: self.write.saturating_sub(earlier.write),
            batch: self.batch.saturating_sub(earlier.batch),
            batched: self.batched.saturating_sub(earlier.batched),
            cas: self.cas.saturating_sub(earlier.cas),
        }
    }

    /// Every request sent, a batch as one: the round trips. `batched` is
    /// left out, since its statements travel inside the `batch` requests.
    pub fn total(&self) -> u64 {
        self.read + self.serial_read + self.write + self.batch + self.cas
    }
}

impl std::fmt::Display for StatementCounts {
    /// `read=1 serial_read=0 write=4 batch=1 batched=2 cas=1`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "read={} serial_read={} write={} batch={} batched={} cas={}",
            self.read, self.serial_read, self.write, self.batch, self.batched, self.cas
        )
    }
}

/// A fault a test injects into the next matching statement, so that the
/// engine's unknown-outcome paths run on one healthy node (Step 3 plan,
/// section 6). Compiled only under `cfg(test)`: there is no Cargo feature, so
/// no build outside the crate's own unit tests can reach it.
///
/// **On a batch** ([`CassandraSession::batch`]). A fault registered for
/// [`StatementKind::Write`] matches a batch when any of its statements'
/// CQL contains the needle, so a test that fails "the `resources_by_type`
/// insert" keeps failing it once that row travels in a batch (IW14). The
/// fault then acts on the whole batch, never on one statement of it, since
/// the node applies or refuses a batch as a whole. A fault that reports a
/// `WriteTimeout` reports it with the write type a batch of **several
/// partitions** would get, and with one outcome that write type allows for
/// the rows (native protocol v5, section 9):
///
/// | Fault | On a batch |
/// | --- | --- |
/// | `ApplyThenTimeOut` | applied, then `WriteTimeout`/`BATCH`: the batchlog was written and the replicas were too slow to answer |
/// | `TimeOutWithoutApplying` | `WriteTimeout`/`BATCH_LOG`, not applied: the batchlog write timed out, and the batch never reached it |
/// | `ApplyLate` | `WriteTimeout`/`BATCH` now, applied just before the next statement of kind `before`: the batchlog's replay |
/// | `PassThrough` | sent as usual |
/// | every other fault | the same error as for one statement, not applied |
///
/// A real `BATCH_LOG` timeout says only that the batchlog write timed out:
/// the batch may still be applied later, by a batchlog replay. The writer
/// retries it either way, and `TimeOutWithoutApplying` takes the outcome
/// where nothing was applied.
///
/// **A batch of one partition never gets `BATCH` or `BATCH_LOG` from a real
/// node.** The node applies it as one mutation, without the batchlog
/// (`BatchStatement.java:444-445`: a logged batch goes through the batchlog
/// only across more than one mutation) and times it out as `SIMPLE`
/// (`StorageProxy.java:885`), which the writer retries. The seam does not know a batch's partitions, so a test
/// that fails such a batch with `ApplyThenTimeOut` or `ApplyLate` drives the
/// writer's `BATCH` path with a timeout the node would not send for it. With
/// the key set `[ResourcesByType]`, every write's batch is one partition, so
/// until pull request 5 adds the token, date and URI rows, those tests check
/// the writer's handling of `BATCH`, not a case a node can produce.
///
/// A batch of one statement is a plain write, and a fault acts on it as on
/// any statement.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Fault {
    /// Execute the statement, then report `WriteTimeout`/CAS: applied, but
    /// the reply was lost.
    ApplyThenTimeOut,
    /// Report `WriteTimeout`/CAS without executing it: not applied.
    TimeOutWithoutApplying,
    /// Report `WriteTimeout`/CAS now, and execute the statement just before
    /// the next statement of kind `before`: an attempt that applies late,
    /// after the serial read said it had not (ADR-aacruzgon-0051).
    ApplyLate {
        /// The kind of statement the late apply runs just before.
        before: StatementKind,
    },
    /// Fail with `WriteTimeout`/CAS without executing: what a serial read
    /// can raise on Paxos v2 (`Paxos.java:893-901`).
    FailWithCasTimeout,
    /// Fail with `WriteFailure`/CAS without executing: replicas failed, so
    /// an unknown outcome that is undecidable answers 503, not 504.
    FailWithWriteFailure,
    /// Fail with `Unavailable` without executing: refused before any
    /// replica acted, so not applied.
    FailWithUnavailable,
    /// Fail with a node-side `Server` error without executing: what the
    /// table for plain statements maps to 500 (ADR-aacruzgon-0067).
    FailWithServerError,
    /// Fail with a `ReadFailure` carrying `READ_TOO_MANY_TOMBSTONES` without
    /// executing: what a replica sends past `tombstone_failure_threshold`.
    FailWithTooManyTombstones,
    /// Send the statement as usual. It only uses up its match, so that a
    /// fault queued after it lands on a later statement of the same text.
    PassThrough,
    /// Fail with a node-side `Protocol` error without executing: a CAS the
    /// engine takes as not applied, which the table for plain statements
    /// maps to 500 (ADR-aacruzgon-0067).
    FailWithProtocolError,
    /// Fail with a node-side `Invalid` error without executing: what the node
    /// answers to a statement or batch it refuses, so nothing refused need be
    /// sent to it.
    FailWithInvalid,
}

#[cfg(test)]
mod faults {
    use cdrs_tokio::frame::message_error::{
        ErrorBody, ErrorType, FailureInfo, ReadFailureError, UnavailableError, WriteFailureError,
        WriteTimeoutError, WriteType,
    };
    use parking_lot::Mutex;

    use super::*;

    struct Pending {
        kind: StatementKind,
        cql_contains: String,
        fault: Fault,
        first_page: bool,
    }

    struct Deferred {
        before: StatementKind,
        late: Late,
    }

    /// What a deferred apply sends.
    enum Late {
        Statement {
            cql: String,
            values: Vec<Value>,
            kind: StatementKind,
        },
        Batch(Vec<IndexStatement>),
    }

    #[derive(Default)]
    pub(super) struct Faults {
        pending: Mutex<Vec<Pending>>,
        deferred: Mutex<Vec<Deferred>>,
    }

    impl Faults {
        pub(super) fn inject(
            &self,
            kind: StatementKind,
            cql_contains: &str,
            fault: Fault,
            first_page: bool,
        ) {
            self.pending.lock().push(Pending {
                kind,
                cql_contains: cql_contains.to_string(),
                fault,
                first_page,
            });
        }

        /// Sends every late apply due before a statement of `kind`.
        async fn run_due(&self, session: &CassandraSession, kind: StatementKind) {
            let due: Vec<Deferred> = {
                let mut deferred = self.deferred.lock();
                let (due, keep) = deferred.drain(..).partition(|d| d.before == kind);
                *deferred = keep;
                due
            };
            for deferred in due {
                match deferred.late {
                    Late::Statement { cql, values, kind } => {
                        session.send(&cql, values, kind, None).await.map(|_| ())
                    }
                    Late::Batch(statements) => session.send_batch(&statements).await,
                }
                .expect("the late apply of an injected fault");
            }
        }

        /// Runs any late apply due before a statement of `kind` (never before
        /// a paged read's first page), then the fault for this statement, if
        /// one matches. `None` sends it as usual.
        pub(super) async fn intercept(
            &self,
            session: &CassandraSession,
            cql: &str,
            values: &[Value],
            kind: StatementKind,
            first_page: bool,
        ) -> Option<Result<Envelope, DriverError>> {
            if !first_page {
                self.run_due(session, kind).await;
            }
            let fault = {
                let mut pending = self.pending.lock();
                let at = pending.iter().position(|p| {
                    p.kind == kind && p.first_page == first_page && cql.contains(&p.cql_contains)
                })?;
                pending.remove(at).fault
            };
            match fault {
                Fault::ApplyThenTimeOut => {
                    if let Err(error) = session.send(cql, values.to_vec(), kind, None).await {
                        return Some(Err(error));
                    }
                }
                Fault::ApplyLate { before } => self.deferred.lock().push(Deferred {
                    before,
                    late: Late::Statement {
                        cql: cql.to_string(),
                        values: values.to_vec(),
                        kind,
                    },
                }),
                Fault::FailWithWriteFailure => return Some(Err(server_error(write_failure()))),
                Fault::FailWithUnavailable => return Some(Err(server_error(unavailable()))),
                Fault::FailWithTooManyTombstones => {
                    return Some(Err(server_error(too_many_tombstones())));
                }
                Fault::FailWithServerError => return Some(Err(server_error(ErrorType::Server))),
                Fault::FailWithProtocolError => {
                    return Some(Err(server_error(ErrorType::Protocol)));
                }
                Fault::FailWithInvalid => return Some(Err(server_error(ErrorType::Invalid))),
                Fault::PassThrough => {
                    return Some(session.send(cql, values.to_vec(), kind, None).await);
                }
                Fault::TimeOutWithoutApplying | Fault::FailWithCasTimeout => {}
            }
            Some(Err(server_error(ErrorType::WriteTimeout(
                WriteTimeoutError::new(Consistency::LocalSerial, 0, 1, WriteType::Cas, None),
            ))))
        }

        /// [`Self::intercept`] for a batch of two statements or more: runs
        /// any late apply due before a write, then the first fault for a
        /// write whose needle any statement of the batch contains, on the
        /// whole batch (see [`Fault`]). `None` sends it as usual.
        pub(super) async fn intercept_batch(
            &self,
            session: &CassandraSession,
            statements: &[IndexStatement],
        ) -> Option<Result<(), DriverError>> {
            self.run_due(session, StatementKind::Write).await;
            let fault = {
                let mut pending = self.pending.lock();
                let at = pending.iter().position(|p| {
                    p.kind == StatementKind::Write
                        && !p.first_page
                        && statements
                            .iter()
                            .any(|statement| statement.cql().contains(&p.cql_contains))
                })?;
                pending.remove(at).fault
            };
            let timeout = |write_type| {
                server_error(ErrorType::WriteTimeout(WriteTimeoutError::new(
                    Consistency::LocalQuorum,
                    0,
                    1,
                    write_type,
                    None,
                )))
            };
            Some(match fault {
                Fault::ApplyThenTimeOut => match session.send_batch(statements).await {
                    Ok(()) => Err(timeout(WriteType::Batch)),
                    Err(error) => Err(error),
                },
                Fault::TimeOutWithoutApplying => Err(timeout(WriteType::BatchLog)),
                Fault::ApplyLate { before } => {
                    self.deferred.lock().push(Deferred {
                        before,
                        late: Late::Batch(statements.to_vec()),
                    });
                    Err(timeout(WriteType::Batch))
                }
                Fault::PassThrough => session.send_batch(statements).await,
                Fault::FailWithCasTimeout => Err(server_error(ErrorType::WriteTimeout(
                    WriteTimeoutError::new(Consistency::LocalSerial, 0, 1, WriteType::Cas, None),
                ))),
                Fault::FailWithWriteFailure => Err(server_error(write_failure())),
                Fault::FailWithUnavailable => Err(server_error(unavailable())),
                Fault::FailWithTooManyTombstones => Err(server_error(too_many_tombstones())),
                Fault::FailWithServerError => Err(server_error(ErrorType::Server)),
                Fault::FailWithProtocolError => Err(server_error(ErrorType::Protocol)),
                Fault::FailWithInvalid => Err(server_error(ErrorType::Invalid)),
            })
        }
    }

    /// A server error of type `ty`, as the node sends it.
    fn server_error(ty: ErrorType) -> DriverError {
        DriverError::from(cdrs_tokio::error::Error::Server {
            body: ErrorBody::new("injected fault", ty),
            addr: "127.0.0.1:9042".parse().expect("an address"),
        })
    }

    fn write_failure() -> ErrorType {
        ErrorType::WriteFailure(WriteFailureError::new(
            Consistency::LocalQuorum,
            0,
            1,
            FailureInfo::NumFailures(1),
            WriteType::Cas,
        ))
    }

    fn unavailable() -> ErrorType {
        ErrorType::Unavailable(UnavailableError::new(Consistency::LocalSerial, 1, 0))
    }

    fn too_many_tombstones() -> ErrorType {
        ErrorType::ReadFailure(ReadFailureError::new(
            Consistency::LocalQuorum,
            0,
            1,
            FailureInfo::ReasonMap(std::collections::HashMap::from([(
                std::net::IpAddr::from([127, 0, 0, 1]),
                1,
            )])),
            false,
        ))
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

    /// IW15: a batch is never re-sent by the driver. The default policy
    /// re-sends an idempotent batch once after a `BATCH_LOG` timeout; the
    /// batch's own policy declines, so each writer try is one send.
    #[test]
    fn an_index_batch_is_never_re_sent_by_the_driver() {
        use cdrs_tokio::frame::message_error::{
            ErrorBody, ErrorType, WriteTimeoutError, WriteType,
        };
        use cdrs_tokio::retry::{QueryInfo, RetryDecision};

        let no_retry: Arc<dyn RetryPolicy + Send + Sync> = Arc::new(FallthroughRetryPolicy);
        let params = batch_params(&no_retry);
        assert_eq!(params.query_params.consistency, Consistency::LocalQuorum);
        assert_eq!(params.query_params.serial_consistency, None);
        assert!(params.is_idempotent);
        assert!(params.speculative_execution_policy.is_none());
        let policy = params
            .retry_policy
            .expect("a batch carries its own retry policy");
        assert!(Arc::ptr_eq(&policy, &no_retry));

        let batch_log_timeout = cdrs_tokio::error::Error::Server {
            body: ErrorBody::new(
                "a batchlog write timed out",
                ErrorType::WriteTimeout(WriteTimeoutError::new(
                    Consistency::LocalQuorum,
                    0,
                    1,
                    WriteType::BatchLog,
                    None,
                )),
            ),
            addr: "127.0.0.1:9042".parse().expect("an address"),
        };
        assert_eq!(
            DefaultRetryPolicy
                .new_session()
                .decide(QueryInfo::new(&batch_log_timeout, true)),
            RetryDecision::RetrySameNode,
            "the default policy re-sends it, which is why a batch overrides it"
        );
        assert_eq!(
            policy
                .new_session()
                .decide(QueryInfo::new(&batch_log_timeout, true)),
            RetryDecision::DontRetry
        );
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

    /// IW15: a lone index row is a plain write with the no-retry policy, so
    /// each writer try is one send; the values and the other parameters are
    /// those of a [`StatementKind::Write`].
    #[test]
    fn a_lone_index_row_is_never_re_sent_by_the_driver() {
        let no_retry: Arc<dyn RetryPolicy + Send + Sync> = Arc::new(FallthroughRetryPolicy);
        let values = vec![Value::from("t")];
        let params = lone_row_params(values.clone(), &no_retry);
        let policy = params
            .retry_policy
            .as_ref()
            .expect("a lone row carries its own retry policy");
        assert!(Arc::ptr_eq(policy, &no_retry));
        let plain = statement_params(StatementKind::Write, values, &no_retry);
        assert!(
            plain.retry_policy.is_none(),
            "a plain write keeps the session's"
        );
        assert_eq!(
            params.query_params.consistency,
            plain.query_params.consistency
        );
        assert_eq!(params.is_idempotent, plain.is_idempotent);
        assert_eq!(
            params.query_params.values.is_some(),
            plain.query_params.values.is_some()
        );
    }

    /// IW13: the batch request itself carries `LOCAL_QUORUM` and the logged
    /// type, which is what the driver sends; the statement parameters'
    /// consistency is not used for a batch. A batch is sent with
    /// [`StatementKind::Write`]'s parameters, idempotent, but with the
    /// no-retry policy that `batch_params` sets in place of the session's
    /// (`statement_params_carry_the_kind_rules`,
    /// `an_index_batch_is_never_re_sent_by_the_driver`).
    #[test]
    fn an_index_batch_is_logged_at_local_quorum_with_no_batch_timestamp() {
        use super::super::batch::IndexCql;
        use cdrs_tokio::types::CBytesShort;

        const INSERT_ROW: IndexCql =
            IndexCql::new("INSERT INTO uri_index (tenant_id, uri) VALUES (?, ?) USING TIMESTAMP ?");
        let prepared = PreparedQuery::new(
            CBytesShort::new(vec![1; 16]),
            INSERT_ROW.as_str(),
            Some("helios".to_string()),
            Vec::new(),
            None,
        );
        let statements: Vec<IndexStatement> = (0..3_i64)
            .map(|i| {
                IndexStatement::new(
                    INSERT_ROW,
                    vec![
                        Value::from("t"),
                        Value::from(format!("u{i}")),
                        Value::from(i),
                    ],
                )
            })
            .collect();
        let batch = index_batch(statements.iter().map(|statement| (statement, &prepared)))
            .expect("a batch");
        assert_eq!(batch.request.batch_type, BatchType::Logged);
        assert_eq!(batch.request.consistency, Consistency::LocalQuorum);
        assert_eq!(batch.request.serial_consistency, None);
        assert_eq!(batch.request.timestamp, None);
        assert_eq!(batch.request.queries.len(), 3);
        for (query, statement) in batch.request.queries.iter().zip(&statements) {
            assert_eq!(
                query.values,
                QueryValues::SimpleValues(statement.values().to_vec())
            );
        }
        // Kept so the driver can prepare the statement again on a node that
        // answers `Unprepared` (10.0.0, `cluster/session.rs:566-600`).
        assert_eq!(batch.prepared_queries.len(), 1);
    }

    /// IW14, separate fields: a batch is one round trip in `batch` and its
    /// statements in `batched`; `write` counts only statements sent alone.
    #[test]
    fn a_batch_counts_as_one_round_trip_and_its_statements_as_batched() {
        let counters = StatementCounters::default();
        counters.count(StatementKind::Write);
        let before = counters.snapshot();
        counters.count(StatementKind::Read);
        counters.count(StatementKind::Write);
        counters.count(StatementKind::Cas);
        counters.count_batch(3);
        counters.count_batch(2);
        let spent = counters.snapshot().since(&before);
        assert_eq!(
            (spent.read, spent.serial_read, spent.write, spent.cas),
            (1, 0, 1, 1)
        );
        assert_eq!((spent.batch, spent.batched), (2, 5));
        assert_eq!(spent.total(), 5, "read, write, cas and two batches");
        assert_eq!(
            spent.to_string(),
            "read=1 serial_read=0 write=1 batch=2 batched=5 cas=1"
        );
        assert_eq!(StatementCounts::default().total(), 0);
    }
}

/// [`CassandraSession::batch`] against the node at `HFS_TEST_CASSANDRA_URL`
/// (skipped when unset), on `resources_by_type` rows under a tenant of each
/// test's own (ADR-aacruzgon-0020, rule 2: never cleaned up). That the batch
/// goes at `LOCAL_QUORUM` is the unit test's to show: one node answers every
/// consistency level alike.
#[cfg(test)]
mod node_tests {
    use chrono::{DateTime, Utc};
    use uuid::Uuid;

    use super::*;
    use crate::backends::cassandra::batch::{BatchFailure, IndexCql, classify_failure};
    use crate::backends::cassandra::test_support::test_config;
    use cdrs_tokio::types::IntoRustByName;

    /// X1 and its delete, as the writer sends them (`write.rs`).
    const INSERT_ROW: IndexCql = IndexCql::new(
        "INSERT INTO resources_by_type \
         (tenant_id, resource_type, last_updated, id, version_id, fhir_version) \
         VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
    );
    const DELETE_ROW: IndexCql = IndexCql::new(
        "DELETE FROM resources_by_type USING TIMESTAMP ? \
         WHERE tenant_id = ? AND resource_type = ? AND last_updated = ? AND id = ?",
    );

    async fn session() -> Option<CassandraSession> {
        let url = std::env::var("HFS_TEST_CASSANDRA_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())?;
        Some(
            CassandraSession::connect(&CassandraConfig {
                contact_points: vec![url],
                ..test_config()
            })
            .await
            .expect("connect"),
        )
    }

    fn tenant(case: &str) -> String {
        format!("test-batch-{case}-{}", Uuid::new_v4().simple())
    }

    fn at(millis: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(millis).expect("a timestamp")
    }

    /// The `resources_by_type` row of `id` at `last_updated` (ms), written
    /// `USING TIMESTAMP ts`.
    fn insert(
        tenant: &str,
        resource_type: &str,
        id: &str,
        last_updated: i64,
        ts: i64,
    ) -> IndexStatement {
        IndexStatement::new(
            INSERT_ROW,
            vec![
                Value::from(tenant),
                Value::from(resource_type),
                Value::from(at(last_updated)),
                Value::from(id),
                Value::from(1_i32),
                Value::from("4.0"),
                Value::from(ts),
            ],
        )
    }

    /// The delete of that row, `USING TIMESTAMP ts`.
    fn delete(
        tenant: &str,
        resource_type: &str,
        id: &str,
        last_updated: i64,
        ts: i64,
    ) -> IndexStatement {
        IndexStatement::new(
            DELETE_ROW,
            vec![
                Value::from(ts),
                Value::from(tenant),
                Value::from(resource_type),
                Value::from(at(last_updated)),
                Value::from(id),
            ],
        )
    }

    /// `(id, writetime)` of every row in the `(tenant, resource_type)`
    /// partition, newest `last_updated` first (the table's clustering
    /// order).
    async fn rows_of(
        session: &CassandraSession,
        tenant: &str,
        resource_type: &str,
    ) -> Vec<(String, i64)> {
        session
            .rows(
                "SELECT id, WRITETIME(version_id) AS written FROM resources_by_type \
                 WHERE tenant_id = ? AND resource_type = ?",
                vec![Value::from(tenant), Value::from(resource_type)],
                StatementKind::Read,
            )
            .await
            .expect("resources_by_type")
            .iter()
            .map(|row| {
                (
                    row.get_r_by_name("id").expect("id"),
                    row.get_r_by_name("written").expect("written"),
                )
            })
            .collect()
    }

    /// IW13 and IW14: one batch over two partitions writes every row at the
    /// timestamp its own statement binds, and counts as one batch request
    /// carrying its statements; a later batch moves a row within its
    /// partition, the delete and the insert together (IW9's unit).
    #[tokio::test]
    async fn a_batch_writes_every_row_at_its_own_timestamp_and_counts_once() {
        let Some(session) = session().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant("rows");
        let before = session.statement_counts();
        session
            .batch(vec![
                insert(&t, "Patient", "p1", 1_000, 1_000_001),
                insert(&t, "Patient", "p2", 2_000, 2_000_001),
                insert(&t, "Observation", "o1", 3_000, 3_000_001),
            ])
            .await
            .expect("the batch");
        let spent = session.statement_counts().since(&before);
        eprintln!("[statements op=batch {spent}]");
        assert_eq!(
            (spent.write, spent.batch, spent.batched, spent.total()),
            (0, 1, 3, 1),
            "{spent}"
        );
        assert_eq!(
            rows_of(&session, &t, "Patient").await,
            vec![("p2".to_string(), 2_000_001), ("p1".to_string(), 1_000_001)]
        );
        assert_eq!(
            rows_of(&session, &t, "Observation").await,
            vec![("o1".to_string(), 3_000_001)]
        );

        session
            .batch(vec![
                delete(&t, "Patient", "p1", 1_000, 4_000_001),
                insert(&t, "Patient", "p1", 4_000, 4_000_001),
            ])
            .await
            .expect("the move");
        assert_eq!(
            rows_of(&session, &t, "Patient").await,
            vec![("p1".to_string(), 4_000_001), ("p2".to_string(), 2_000_001)]
        );
    }

    /// IW13: no statement sends nothing, and one goes as a plain write.
    #[tokio::test]
    async fn a_batch_of_one_is_a_plain_write_and_an_empty_one_sends_nothing() {
        let Some(session) = session().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant("one");
        let before = session.statement_counts();
        session.batch(Vec::new()).await.expect("nothing to send");
        assert_eq!(session.statement_counts().since(&before).total(), 0);

        session
            .batch(vec![insert(&t, "Patient", "p1", 1_000, 1_000_001)])
            .await
            .expect("one row");
        let spent = session.statement_counts().since(&before);
        assert_eq!(
            (spent.write, spent.batch, spent.batched),
            (1, 0, 0),
            "{spent}"
        );
        assert_eq!(
            rows_of(&session, &t, "Patient").await,
            vec![("p1".to_string(), 1_000_001)]
        );
    }

    /// The fault seam on a batch: a fault for a write matches when any
    /// statement of the batch contains its needle, acts on the whole batch,
    /// and reports the write type a batch would get, which
    /// [`classify_failure`] reads as IW15 says (see [`Fault`]).
    #[tokio::test]
    async fn a_write_fault_matches_a_batch_by_any_of_its_statements() {
        let Some(session) = session().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant("faults");
        let move_p1 = |from: i64, to: i64, ts: i64| {
            vec![
                insert(&t, "Patient", "p1", to, ts),
                delete(&t, "Patient", "p1", from, ts),
            ]
        };
        session
            .batch(vec![insert(&t, "Patient", "p1", 1_000, 1_000_001)])
            .await
            .expect("the first row");

        // The needle is the delete's, the second statement: not applied,
        // and the batchlog timeout is worth another try.
        session.inject_fault(
            StatementKind::Write,
            "DELETE FROM resources_by_type",
            Fault::TimeOutWithoutApplying,
        );
        let before = session.statement_counts();
        let error = session
            .batch(move_p1(1_000, 2_000, 2_000_001))
            .await
            .expect_err("the injected timeout");
        let spent = session.statement_counts().since(&before);
        assert_eq!(
            (spent.batch, spent.batched),
            (1, 2),
            "counted before the seam"
        );
        assert_eq!(classify_failure(&error), BatchFailure::Retry, "{error:?}");
        assert_eq!(
            rows_of(&session, &t, "Patient").await,
            vec![("p1".to_string(), 1_000_001)],
            "not applied, as a whole"
        );

        // Applied, then reported as a batch timeout: left to the batchlog.
        session.inject_fault(
            StatementKind::Write,
            "INSERT INTO resources_by_type",
            Fault::ApplyThenTimeOut,
        );
        let error = session
            .batch(move_p1(1_000, 2_000, 2_000_001))
            .await
            .expect_err("the injected timeout");
        assert_eq!(
            classify_failure(&error),
            BatchFailure::InBatchlog,
            "{error:?}"
        );
        assert_eq!(
            rows_of(&session, &t, "Patient").await,
            vec![("p1".to_string(), 2_000_001)],
            "applied, as a whole"
        );

        // Applied late, as the batchlog replays it: just before the next
        // read, which then sees the moved row.
        session.inject_fault(
            StatementKind::Write,
            "INSERT INTO resources_by_type",
            Fault::ApplyLate {
                before: StatementKind::Read,
            },
        );
        let error = session
            .batch(move_p1(2_000, 3_000, 3_000_001))
            .await
            .expect_err("the injected timeout");
        assert_eq!(
            classify_failure(&error),
            BatchFailure::InBatchlog,
            "{error:?}"
        );
        assert_eq!(
            rows_of(&session, &t, "Patient").await,
            vec![("p1".to_string(), 3_000_001)],
            "applied before the read"
        );

        // A fault for another kind of statement never matches a batch.
        session.inject_fault(
            StatementKind::Cas,
            "INSERT INTO resources_by_type",
            Fault::TimeOutWithoutApplying,
        );
        session
            .batch(move_p1(3_000, 4_000, 4_000_001))
            .await
            .expect("no write fault pending");
        assert_eq!(
            rows_of(&session, &t, "Patient").await,
            vec![("p1".to_string(), 4_000_001)]
        );
    }
}
