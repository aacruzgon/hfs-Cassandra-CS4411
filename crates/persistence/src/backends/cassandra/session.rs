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
//! **Counting.** Every statement is counted by kind before it is sent
//! ([`StatementCounts`], `CassandraBackend::statement_counts`), for the
//! write-amplification report (Step 3 CI report R5).
//!
//! **Fault injection, tests only.** Under `cfg(test)`, and only there, a test
//! can make the next matching statement report `WriteTimeout`/CAS, with or
//! without executing it, or execute it late (`Fault`). It has no Cargo
//! feature, so nothing outside this crate's unit tests can reach it.
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
        self.send(cql, values, kind, paging).await
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
                .execute_with(cql, values.clone(), kind, Some(paging))
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
}

/// Statements sent, one counter per [`StatementKind`].
#[derive(Debug, Default)]
struct StatementCounters {
    read: AtomicU64,
    serial_read: AtomicU64,
    write: AtomicU64,
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

    fn snapshot(&self) -> StatementCounts {
        StatementCounts {
            read: self.read.load(Ordering::Relaxed),
            serial_read: self.serial_read.load(Ordering::Relaxed),
            write: self.write.load(Ordering::Relaxed),
            cas: self.cas.load(Ordering::Relaxed),
        }
    }
}

/// How many statements of each kind a backend's session has been asked to
/// send since it was built: every request, one per page of a paged read,
/// failed ones included (Step 3 CI report R5, write amplification). Take one
/// snapshot before and one after an operation and subtract with
/// [`StatementCounts::since`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StatementCounts {
    /// Plain reads at `LOCAL_QUORUM` (R1, R3, `count`, …).
    pub read: u64,
    /// Reads at `LOCAL_SERIAL` (R2).
    pub serial_read: u64,
    /// Plain writes at `LOCAL_QUORUM` (V1 to V3, index rows, I1, I2).
    pub write: u64,
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
            cas: self.cas.saturating_sub(earlier.cas),
        }
    }

    /// Every statement, whatever its kind.
    pub fn total(&self) -> u64 {
        self.read + self.serial_read + self.write + self.cas
    }
}

impl std::fmt::Display for StatementCounts {
    /// `read=1 serial_read=0 write=4 cas=1`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "read={} serial_read={} write={} cas={}",
            self.read, self.serial_read, self.write, self.cas
        )
    }
}

/// A fault a test injects into the next matching statement, so that the
/// engine's unknown-outcome paths run on one healthy node (Step 3 plan,
/// section 6). Compiled only under `cfg(test)`: there is no Cargo feature, so
/// no build outside the crate's own unit tests can reach it.
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
        cql: String,
        values: Vec<Value>,
        kind: StatementKind,
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
            let due: Vec<Deferred> = if first_page {
                Vec::new()
            } else {
                let mut deferred = self.deferred.lock();
                let (due, keep) = deferred.drain(..).partition(|d| d.before == kind);
                *deferred = keep;
                due
            };
            for late in due {
                session
                    .send(&late.cql, late.values, late.kind, None)
                    .await
                    .expect("the late apply of an injected fault");
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
                    cql: cql.to_string(),
                    values: values.to_vec(),
                    kind,
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
                Fault::PassThrough => {
                    return Some(session.send(cql, values.to_vec(), kind, None).await);
                }
                Fault::TimeOutWithoutApplying | Fault::FailWithCasTimeout => {}
            }
            Some(Err(server_error(ErrorType::WriteTimeout(
                WriteTimeoutError::new(Consistency::LocalSerial, 0, 1, WriteType::Cas, None),
            ))))
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
