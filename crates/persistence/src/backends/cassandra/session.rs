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
//! life of the driver session. The driver re-prepares one transparently when
//! a node answers `Unprepared`, which means that node did not execute it.
//!
//! **Rebuilt sessions.** `cdrs-tokio` 8.1.9 can lose a single-node cluster
//! for good after a control-connection reconnect, a Cassandra restart
//! included. [`CassandraSession`] then opens a new driver session, with an
//! empty prepared cache, and sends an idempotent statement once more on it;
//! a CAS is never sent again. See [`CassandraSession`] for the rules.
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
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::watch;

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

/// The message `cdrs-tokio` 8.1.9 returns when a statement's query plan holds
/// no node (`cluster/session.rs:723` and `:738`). The driver has no error
/// variant of its own for it: it is an [`Error::General`] with this text, so
/// [`is_no_node_in_plan`] matches both.
///
/// [`Error::General`]: cdrs_tokio::error::Error::General
pub(crate) const NO_NODE_IN_PLAN: &str = "No nodes available in query plan!";

/// The first wait after a failed rebuild; it doubles per failure, up to
/// [`REBUILD_BACKOFF_MAX`].
const REBUILD_BACKOFF_MIN: Duration = Duration::from_millis(250);

/// The longest wait between two rebuild attempts while the cluster stays
/// unreachable.
const REBUILD_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// Whether `error` is the driver's empty-query-plan error. Matches the
/// variant and the exact text (see [`NO_NODE_IN_PLAN`]).
pub(crate) fn is_no_node_in_plan(error: &cdrs_tokio::error::Error) -> bool {
    matches!(error, cdrs_tokio::error::Error::General(message) if message == NO_NODE_IN_PLAN)
}

/// The wait after `failures` consecutive failed rebuilds.
fn rebuild_backoff(failures: u32) -> Duration {
    let doublings = failures.saturating_sub(1).min(16);
    REBUILD_BACKOFF_MIN
        .saturating_mul(1 << doublings)
        .min(REBUILD_BACKOFF_MAX)
}

/// One open driver session and the statements prepared on it.
///
/// The prepared cache belongs to the session it was filled on, so a rebuilt
/// session starts with an empty cache and prepares every statement again on
/// first use.
struct Connected {
    session: CdrsSession,
    prepared: RwLock<HashMap<String, Arc<PreparedQuery>>>,
    /// 0 for the session [`CassandraSession::connect`] opened, then one more
    /// per rebuild.
    generation: u64,
}

impl Connected {
    /// Whether the driver's metadata still holds a node it would put in a
    /// query plan: one with a distance (`LocalDcOnly`: the local DC) whose
    /// state is `Up` (`cluster/topology/node.rs`, `is_ignored`).
    fn has_usable_node(&self) -> bool {
        self.session
            .cluster_metadata()
            .nodes()
            .values()
            .any(|node| !node.is_ignored())
    }
}

/// Rebuild bookkeeping, under [`Holder::state`].
#[derive(Debug, Default)]
struct RebuildState {
    /// Rebuilds started since the last one that succeeded.
    failures: u32,
    /// No new rebuild starts before this.
    not_before: Option<Instant>,
    /// The rebuild in progress: callers wait on it instead of starting
    /// another. Its task sends `true` when it has finished.
    pending: Option<Pending>,
}

/// A rebuild in progress.
#[derive(Debug)]
struct Pending {
    finished: watch::Receiver<bool>,
    started: Instant,
    /// Whether the warning for a rebuild that outlived the connect timeout
    /// has been logged.
    warned: bool,
}

/// What [`CassandraSession`] shares with its rebuild task.
struct Holder {
    /// Kept to open the replacement session. The password stays here and is
    /// never logged (`CassandraConfig`'s `Debug` redacts it).
    config: CassandraConfig,
    current: RwLock<Arc<Connected>>,
    /// Taken before `current` when both are needed.
    state: Mutex<RebuildState>,
    /// Rebuilds started, successful or not.
    rebuild_attempts: AtomicU64,
}

impl Holder {
    fn current(&self) -> Arc<Connected> {
        self.current.read().clone()
    }
}

/// The backend's session holder: the open driver session, its
/// prepared-statement cache, and the rebuild that works around `cdrs-tokio`
/// 8.1.9 losing a single-node cluster.
///
/// **Why it rebuilds.** After a control-connection reconnect whose query plan
/// still holds the node, `cdrs-tokio` 8.1.9 refreshes each known node with
/// `clone_with_node_info` (`cluster/topology/node.rs:279-290`, called from
/// `metadata_builder.rs:81`). That compares the node's stored
/// `broadcast_address`, the gossip address (`<ip>:7000`), with the fresh
/// `broadcast_rpc_address`, the CQL address (`<ip>:9042`). The two never
/// match, so the node is set `Unknown` with no distance. Such a node is
/// ignored, every statement fails with [`NO_NODE_IN_PLAN`], and with one node
/// no peer event marks it up again. A Cassandra restart is enough to cause it
/// (`docker/cassandra/README.md`).
///
/// **What it does.** When a statement fails with [`NO_NODE_IN_PLAN`] and the
/// session's metadata holds no usable node, the holder opens a new session
/// from the same configuration and swaps it in. The statement is then sent
/// once more on the new session **only if it is idempotent**. A
/// compare-and-swap returns its error, and the caller's unknown-outcome rules
/// decide what follows (ADR-aacruzgon-0024). For a CAS the error means it was
/// sent to no node: it carries `FallthroughRetryPolicy`, so the driver never
/// moves on after sending it (`send_envelope.rs`). For an idempotent statement
/// under `DefaultRetryPolicy` the same text can also follow a retry that ran
/// out of nodes after the statement was sent; the node is then still usable,
/// so no rebuild starts, and a re-send would be safe anyway. `health_check`
/// runs through the same path, so a probe both sees the broken session and
/// repairs it.
///
/// **One rebuild at a time, in a task of its own.** The rebuild runs in a
/// spawned task that callers wait on, never in a caller's future. The
/// driver's session builder spawns its control-connection task before it
/// waits for the cluster (`cluster/session.rs:826`), so a cancelled build
/// would leave that task running for good; the rebuild task is never
/// cancelled, and at most one is pending. Before building, it checks that a
/// contact point accepts a TCP connection, so a node that is down fails the
/// rebuild at once. A build whose contact point accepts TCP but whose session
/// does not start (the keyspace dropped, credentials rotated, TLS required, a
/// node still booting) stays pending until that clears, retried by the
/// driver with a backoff of up to 60 s; once it has outlived the connect
/// timeout, callers get their own error back at once instead of waiting, and
/// one warning is logged. A panic in the task clears the pending rebuild
/// through a drop guard. After a failed rebuild, or one whose new session has no
/// usable node, no new one starts for a backoff of 250 ms, doubling per
/// failure up to 5 s; callers in that window get their own error back at
/// once, so a node that is down is not hammered. A caller waits for a pending
/// rebuild only as long as its request deadline allows.
pub(crate) struct CassandraSession {
    holder: Arc<Holder>,
    request_timeout: Duration,
    no_retry: Arc<dyn RetryPolicy + Send + Sync>,
}

impl std::fmt::Debug for CassandraSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let current = self.holder.current();
        f.debug_struct("CassandraSession")
            .field("generation", &current.generation)
            .field(
                "rebuild_attempts",
                &self.holder.rebuild_attempts.load(Ordering::Relaxed),
            )
            .field("prepared_statements", &current.prepared.read().len())
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
    /// A contact point that refuses TCP fails here before the driver starts
    /// anything. When a contact point accepts TCP but the session does not
    /// come up in time (a missing keyspace, rejected credentials, a node
    /// still starting), the build is cut off by the deadline and the driver's
    /// control-connection task outlives the failed call, retrying in the
    /// background (see the type docs): one task per failed `connect`.
    pub(crate) async fn connect(config: &CassandraConfig) -> Result<Self, DriverError> {
        let deadline = Duration::from_millis(config.connect_timeout_ms);
        let started = Instant::now();
        probe_contact_points(config, deadline).await?;
        let builder = session_builder(config, deadline.saturating_sub(started.elapsed())).await?;
        let remaining = deadline.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err(DriverError::Deadline {
                after: deadline,
                sent: false,
            });
        }
        let session = within(remaining, false, builder.build())
            .await?
            .map_err(build_error)?;
        Ok(Self {
            holder: Arc::new(Holder {
                config: config.clone(),
                current: RwLock::new(Arc::new(Connected {
                    session,
                    prepared: RwLock::new(HashMap::new()),
                    generation: 0,
                })),
                state: Mutex::new(RebuildState::default()),
                rebuild_attempts: AtomicU64::new(0),
            }),
            request_timeout: Duration::from_millis(config.request_timeout_ms),
            no_retry: Arc::new(FallthroughRetryPolicy),
        })
    }

    /// The data centres, each once, of nodes the session could send to that
    /// are not `local_dc`. The driver gives every contact point a `Local` distance
    /// and keeps it after the first refresh (`cluster/session.rs:784-786`,
    /// `topology/node.rs:330`), without asking [`LocalDcOnly`], so a contact
    /// point in another DC would coordinate requests.
    pub(crate) fn usable_nodes_outside(&self, local_dc: &str) -> Vec<String> {
        let datacenters: std::collections::BTreeSet<String> = self
            .holder
            .current()
            .session
            .cluster_metadata()
            .nodes()
            .values()
            // A distance, whatever the node's state now: a remote contact
            // point that is down would coordinate requests once it is up.
            .filter(|node| node.distance().is_some() && node.datacenter() != local_dc)
            .map(|node| node.datacenter().to_string())
            .collect();
        datacenters.into_iter().collect()
    }

    /// The session in use now.
    fn current(&self) -> Arc<Connected> {
        self.holder.current()
    }

    /// How many sessions have replaced the first one.
    #[cfg(test)]
    pub(crate) fn generation(&self) -> u64 {
        self.current().generation
    }

    /// How many rebuilds have started, successful or not.
    #[cfg(test)]
    pub(crate) fn rebuild_attempts(&self) -> u64 {
        self.holder.rebuild_attempts.load(Ordering::Relaxed)
    }

    /// How many statements are prepared on the session in use now.
    #[cfg(test)]
    pub(crate) fn prepared_count(&self) -> usize {
        self.current().prepared.read().len()
    }

    /// The prepared form of `cql` on `connected`, preparing it on first use
    /// within `deadline`.
    async fn prepared(
        connected: &Connected,
        cql: &str,
        deadline: Duration,
    ) -> Result<Arc<PreparedQuery>, DriverError> {
        if let Some(prepared) = connected.prepared.read().get(cql) {
            return Ok(prepared.clone());
        }
        // Two callers may prepare the same text at once; both get the same
        // statement id from the node, and the last insert wins.
        let prepared = Arc::new(within(deadline, false, connected.session.prepare(cql)).await??);
        connected
            .prepared
            .write()
            .insert(cql.to_string(), prepared.clone());
        Ok(prepared)
    }

    /// Prepares (or reuses) `cql` on `connected` and executes it, all within
    /// `deadline`.
    async fn execute_on(
        connected: &Connected,
        cql: &str,
        params: &StatementParams,
        deadline: Duration,
    ) -> Result<Envelope, DriverError> {
        let started = Instant::now();
        let prepared = Self::prepared(connected, cql, deadline).await?;
        let remaining = deadline.saturating_sub(started.elapsed());
        Ok(within(
            remaining,
            true,
            connected.session.exec_with_params(&prepared, params),
        )
        .await??)
    }

    /// Executes `cql` (prepared and cached) with positional `values`. The
    /// request deadline covers the prepare, on first use, the execute, and a
    /// rebuild and its one retry (see the type docs).
    pub(crate) async fn execute(
        &self,
        cql: &str,
        values: Vec<Value>,
        kind: StatementKind,
    ) -> Result<Envelope, DriverError> {
        let started = Instant::now();
        let params = statement_params(kind, values, &self.no_retry);
        let connected = self.current();
        let result = Self::execute_on(&connected, cql, &params, self.request_timeout).await;
        let Err(DriverError::Driver(error)) = &result else {
            return result;
        };
        if !is_no_node_in_plan(error) || connected.has_usable_node() {
            return result;
        }
        let remaining = self.request_timeout.saturating_sub(started.elapsed());
        let Some(fresh) = self.recover(&connected, remaining).await else {
            return result;
        };
        let remaining = self.request_timeout.saturating_sub(started.elapsed());
        if !kind.is_idempotent() || remaining.is_zero() {
            return result;
        }
        Self::execute_on(&fresh, cql, &params, remaining).await
    }

    /// Replaces `broken` with a new session, or returns the one a rebuild
    /// already put in its place. `None` when no new session could be had
    /// within `budget`: the rebuild failed or is still running, or a recent
    /// failure's backoff has not passed.
    async fn recover(&self, broken: &Connected, budget: Duration) -> Option<Arc<Connected>> {
        let mut finished = {
            let mut state = self.holder.state.lock();
            let current = self.holder.current();
            if current.generation != broken.generation {
                return Some(current);
            }
            let stuck_after = Duration::from_millis(self.holder.config.connect_timeout_ms);
            match &mut state.pending {
                // A build that outlives the connect timeout is waiting on
                // something that may not clear soon (see the type docs):
                // answer at once instead of holding every request for its
                // whole deadline.
                Some(pending) if pending.started.elapsed() > stuck_after => {
                    if !pending.warned {
                        pending.warned = true;
                        tracing::warn!(
                            pending_ms = pending.started.elapsed().as_millis() as u64,
                            "cassandra: the new session is still not open; a contact point \
                             accepts TCP, but the session does not start (keyspace, \
                             credentials, TLS, or a node still starting)"
                        );
                    }
                    return None;
                }
                Some(pending) => pending.finished.clone(),
                None => {
                    if state.not_before.is_some_and(|at| Instant::now() < at) {
                        return None;
                    }
                    state.failures = state.failures.saturating_add(1);
                    let (done, pending) = watch::channel(false);
                    state.pending = Some(Pending {
                        finished: pending.clone(),
                        started: Instant::now(),
                        warned: false,
                    });
                    self.holder.rebuild_attempts.fetch_add(1, Ordering::Relaxed);
                    tokio::spawn(rebuild(self.holder.clone(), broken.generation + 1, done));
                    pending
                }
            }
        };
        // An error here means the task ended without sending (it panicked,
        // and its guard cleared `pending`); either way, the current session
        // decides.
        let _ = tokio::time::timeout(budget, finished.wait_for(|done| *done)).await;
        let current = self.holder.current();
        (current.generation > broken.generation).then_some(current)
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

/// A failed session open, for a log line: the kind of failure, never an
/// address or a credential.
fn describe_open_error(error: &DriverError) -> String {
    match error {
        DriverError::Deadline { after, .. } => {
            format!("no session within {} ms", after.as_millis())
        }
        DriverError::Driver(error) => match error.as_ref() {
            cdrs_tokio::error::Error::Io(io) => format!("I/O error: {:?}", io.kind()),
            cdrs_tokio::error::Error::Server { body, .. } => {
                format!("server error {:?}", body.ty)
            }
            _ => "driver error".to_string(),
        },
    }
}

/// Opens the replacement session for `holder` and swaps it in, or records the
/// failure and its backoff; then wakes the callers waiting on `done`. Runs in
/// its own task and is never cancelled (see [`CassandraSession`]).
async fn rebuild(holder: Arc<Holder>, generation: u64, done: watch::Sender<bool>) {
    let mut guard = PendingGuard {
        holder: holder.clone(),
        armed: true,
    };
    tracing::warn!(
        generation,
        "cassandra: the session has no node to send to; opening a new session"
    );
    let outcome = match open_replacement(&holder.config).await {
        Ok(session) => {
            let fresh = Connected {
                session,
                prepared: RwLock::new(HashMap::new()),
                generation,
            };
            if fresh.has_usable_node() {
                Ok(fresh)
            } else {
                Err("the new session has no usable node in the local data centre".to_string())
            }
        }
        Err(error) => Err(describe_open_error(&error)),
    };
    {
        let mut state = holder.state.lock();
        state.pending = None;
        guard.armed = false;
        match outcome {
            Ok(fresh) => {
                *holder.current.write() = Arc::new(fresh);
                state.failures = 0;
                state.not_before = None;
                tracing::info!(generation, "cassandra: new session open");
            }
            Err(reason) => {
                let backoff = rebuild_backoff(state.failures);
                state.not_before = Some(Instant::now() + backoff);
                tracing::warn!(
                    failures = state.failures,
                    retry_in_ms = backoff.as_millis() as u64,
                    error = %reason,
                    "cassandra: could not open a new session"
                );
            }
        }
    }
    let _ = done.send(true);
}

/// Clears [`RebuildState::pending`] and sets the backoff if [`rebuild`] ends
/// without doing so, which only a panic can cause; otherwise no rebuild could
/// ever start again.
struct PendingGuard {
    holder: Arc<Holder>,
    armed: bool,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        if self.armed {
            let mut state = self.holder.state.lock();
            state.pending = None;
            state.not_before = Some(Instant::now() + rebuild_backoff(state.failures));
        }
    }
}

/// The replacement session for [`rebuild`]. First checks, within
/// `config.connect_timeout_ms`, that some contact point accepts a TCP
/// connection, so a node that is down fails here at once. The session build
/// itself has no deadline: cancelling it would orphan the driver's
/// control-connection task, and it finishes once the node answers.
async fn open_replacement(config: &CassandraConfig) -> Result<CdrsSession, DriverError> {
    let deadline = Duration::from_millis(config.connect_timeout_ms);
    let started = Instant::now();
    probe_contact_points(config, deadline).await?;
    let builder = session_builder(config, deadline.saturating_sub(started.elapsed())).await?;
    builder.build().await.map_err(build_error)
}

/// Succeeds once some contact point accepts a TCP connection within
/// `deadline`. Cheap, and spawns nothing, so it is safe to cancel; it runs
/// before every session build so that a closed port fails without starting
/// the driver's control-connection task.
async fn probe_contact_points(
    config: &CassandraConfig,
    deadline: Duration,
) -> Result<(), DriverError> {
    let attempts: Vec<_> = config
        .contact_points
        .iter()
        .map(|point| Box::pin(tokio::net::TcpStream::connect(point.as_str())))
        .collect();
    within(deadline, false, futures::future::select_ok(attempts))
        .await?
        .map(drop)
        .map_err(|error| DriverError::from(cdrs_tokio::error::Error::Io(error)))
}

/// The driver's session builder for `config`. Resolving the contact points
/// runs within `deadline` and spawns nothing, so it is safe to cancel.
async fn session_builder(
    config: &CassandraConfig,
    deadline: Duration,
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
    let node_config = within(deadline, false, node_config.build()).await??;

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
    fn only_the_empty_plan_error_counts_as_no_node_in_plan() {
        use cdrs_tokio::error::Error;
        assert!(is_no_node_in_plan(&Error::General(
            NO_NODE_IN_PLAN.to_string()
        )));
        for other in [
            Error::General("No active connections to: 10.0.0.1:9042".to_string()),
            Error::General("Connection closed while waiting for response!".to_string()),
            Error::General(format!("{NO_NODE_IN_PLAN} (and more)")),
            Error::Timeout(NO_NODE_IN_PLAN.to_string()),
        ] {
            assert!(!is_no_node_in_plan(&other), "{other:?}");
        }
    }

    #[test]
    fn rebuild_backoff_doubles_from_250_ms_up_to_5_s() {
        let waits: Vec<u128> = (1..=8).map(|n| rebuild_backoff(n).as_millis()).collect();
        assert_eq!(waits, [250, 500, 1000, 2000, 4000, 5000, 5000, 5000]);
        assert_eq!(rebuild_backoff(0), REBUILD_BACKOFF_MIN);
        assert_eq!(rebuild_backoff(u32::MAX), REBUILD_BACKOFF_MAX);
    }

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
