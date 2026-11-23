//! The index sweeper (roadmap item 5.2; ADR-sam-michaels-0006, SW1 to SW28).
//!
//! # What it is for
//!
//! A write whose CAS on `resources` applied but whose version row or index
//! rows did not (G23) leaves its intent row in `pending_index_writes`. The
//! sweeper reads those intents once they are older than a grace period, and
//! finishes the write from what `resources` and `resource_versions` hold, so
//! search, `count` and `_history` agree with `resources` again.
//!
//! # One intent, four cases (SW11)
//!
//! For an intent of version `V`, the sweeper reads the `resources` row at
//! `LOCAL_QUORUM`, and only when that showed no row or a version below `V` it
//! reads it again at `LOCAL_SERIAL` (SW12: a CAS still in flight is completed
//! or cancelled by the serial read, so the answer is final). The version the
//! row shows decides the case:
//!
//! - **no row** or **never applied** (a version below `V`): the CAS did not
//!   apply, so nothing was written; the intent row is deleted (I2).
//! - **current** (version `V`): the request crashed or was slow. The sweeper
//!   checks that the stored `write_ts` is `write_ts_of(last_updated)`, takes
//!   `K(V)` from the row (empty for a delete marker, SW16) and `K(V-1)` from
//!   version row `V-1`, writes version row `V` with the stored cells and the
//!   stored `fhir_version` text (RW8), and only when that is confirmed sends
//!   the index delta (IW24), then deletes the intent.
//! - **superseded** (a version above `V`): version `V` is no longer current,
//!   so only the rows of `K(V-1)` that version `V` replaced are deleted; no
//!   version row and no insert is written (SW11).
//!
//! A missing version row, a key set that does not decode, a `write_ts` that
//! is not `write_ts_of(last_updated)`, and a write the node refuses as
//! `Invalid` are bugs, never an empty set (SW13): they are logged at `error`,
//! counted, and the intent row is kept. A `SearchParameter` that was replayed
//! is also put back in the registry overlay (SW19).
//!
//! # Marks, slices and the startup scan (SW8 to SW10)
//!
//! The outbox is read in slices of one outbox partition (`bucket`, `shard`)
//! and a half-open range of `enqueued_at`. Each of the 16 shards has a mark:
//! the instant up to which its intents are known to be finished or on the
//! retry list. A poll reads, for each shard, the slices of
//! `[mark, now - grace)` in hour slices (cut at the UTC hour), processes the
//! intents, and moves the mark only when every slice of that shard was read
//! and none of its intents was refused by a full retry list. A startup scan
//! covers `[start - 24 h 1 min, start - grace)` in one-minute slices, beside
//! the loop, at a lower rank for statement permits, so intents older than the
//! marks' start are not forgotten after a restart.
//!
//! # The retry list (SW18, SW13)
//!
//! An intent that failed goes to an in-memory list, retried after 5 s, 10 s,
//! 20 s and so on up to 5 min, and dropped only 24 hours after it was
//! enqueued. A bug (an invariant) is tried 3 times and then left on the
//! intent row until its TTL. The list holds at most [`RETRY_LIST_CAP`]
//! entries; when it is full the intent's shard keeps its mark, so the next
//! poll reads it again. The intent row is never deleted for a failure.
//!
//! # The load limit (RW4)
//!
//! Every statement holds permits from [`SweeperShared::permits`]: at most
//! [`MAX_IN_FLIGHT`] at once in all, and the startup scan at most
//! [`STARTUP_IN_FLIGHT`] of those, so the normal loop always has the rest.
//!
//! # Who starts it (SW1)
//!
//! `hfs` spawns [`CassandraBackend::run_sweeper`] in `start_cassandra` when
//! `sweeper_enabled`; `connect` never does. Every step takes its clock and
//! scope as arguments, so tests run one poll, or one pass, directly.
//!
//! # `sweep_pass` (SW28)
//!
//! [`CassandraBackend::sweep_pass`] is for tests and the post-suite step: one
//! pass over a window with a scope, no retry list. A grace under the
//! configured one is refused unless `allow_short_grace` says every writer of
//! the swept tenants has returned and none ended on HFS's own deadline or a
//! dropped connection ([`CasStats::client_side_unknowns`]).
//!
//! # Duplicate work is harmless (SW22)
//!
//! Every write the sweeper makes is at the version's own timestamp,
//! `write_ts_of(last_updated)`, with the cells the version already has, so two
//! sweepers, or a sweeper and a request, on one intent end in the same state.
//! There is no lease and no coordination.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::blob::Blob;
use cdrs_tokio::types::rows::Row;
use cdrs_tokio::types::value::Value;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::backend::CassandraBackend;
use super::index_keys::{self, IndexKey};
use super::read::ResourceRow;
use super::session::{DriverError, StatementKind};
#[cfg(test)]
use super::stats::CasStats;
use super::stats::IndexWriteStats;
use super::write::{
    INDEX_BATCHES_IN_FLIGHT, INTENT_SHARDS, IndexRow, IntentKey, RowKey, VersionKeys, VersionRow,
    WriteFailure, bucket_of, index_delta, write_ts_of,
};

/// I3, one slice of one outbox partition (walkthrough section 2; SW7). Both bounds in whole milliseconds.
const SELECT_INTENTS_OF_SLICE: &str = "SELECT enqueued_at, tenant_id, resource_type, id, version_id \
     FROM pending_index_writes WHERE bucket = ? AND shard = ? \
     AND enqueued_at >= minTimeuuid(?) AND enqueued_at < minTimeuuid(?)";
/// R3, the columns a replay needs (walkthrough section 2).
const SELECT_VERSION_ROW: &str = "SELECT last_updated, fhir_version, write_ts, index_keys \
     FROM resource_versions WHERE tenant_id = ? AND resource_type = ? AND id = ? AND version_id = ?";

/// Statements the whole sweeper has in flight at most (RW4).
pub(crate) const MAX_IN_FLIGHT: usize = 32;
/// Of those, the most the startup scan may hold, so the normal loop always has 24 (RW4: ranked below).
pub(crate) const STARTUP_IN_FLIGHT: usize = 8;
/// Slices read at once, and intents of one slice processed at once (SW17).
const SLICES_AT_ONCE: usize = 16;
const INTENTS_PER_SLICE_AT_ONCE: usize = 8;
/// Rows per page of I3 (SW7).
const OUTBOX_PAGE_SIZE: i32 = 1000;
/// Tries of each statement of a replay; the retry list retries (IW17).
const SWEEPER_TRIES: u32 = 1;
/// SW13: tries of an intent that hits an invariant before the sweeper stops trying it.
const INVARIANT_TRIES: u32 = 3;
/// SW18: entries the retry list holds at most (a chosen budget).
pub(crate) const RETRY_LIST_CAP: usize = 10_000;
/// How often the loop logs its `info` summary when there was work (SW25).
const SUMMARY_EVERY: Duration = Duration::from_secs(60);

/// SW9: how far back the startup scan reads, 24 h 1 min.
fn startup_window() -> chrono::Duration {
    chrono::Duration::minutes(24 * 60 + 1)
}
/// SW10: the startup scan's slice.
fn startup_slice() -> chrono::Duration {
    chrono::Duration::minutes(1)
}
/// SW8: a poll's slice.
fn poll_slice() -> chrono::Duration {
    chrono::Duration::hours(1)
}
/// SW18: the first retry delay.
fn retry_first() -> chrono::Duration {
    chrono::Duration::seconds(5)
}
/// SW18: the longest retry delay.
fn retry_max() -> chrono::Duration {
    chrono::Duration::minutes(5)
}
/// SW18: an entry is dropped this long after its intent was enqueued.
fn retry_horizon() -> chrono::Duration {
    chrono::Duration::hours(24)
}

/// `at` truncated to whole milliseconds.
fn whole_ms(at: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(at.timestamp_millis()).unwrap_or(at)
}

/// One time slice of one outbox partition: `[from, to)`, both whole milliseconds, inside `bucket`'s UTC hour.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Slice {
    pub(crate) bucket: String,
    pub(crate) shard: i16,
    pub(crate) from: DateTime<Utc>,
    pub(crate) to: DateTime<Utc>,
}

/// `[from, to)`, both truncated to whole milliseconds, cut at every UTC multiple of `len` (an hour or a
/// minute); each piece's bucket is `bucket_of(piece.from)` (SW5). Empty when `from >= to`.
pub(crate) fn plan_slices(
    shard: i16,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
    len: chrono::Duration,
) -> Vec<Slice> {
    let (from, to) = (whole_ms(from), whole_ms(to));
    let len_ms = len.num_milliseconds().max(1);
    let mut slices = Vec::new();
    let mut at = from;
    while at < to {
        let boundary_ms = (at.timestamp_millis().div_euclid(len_ms) + 1) * len_ms;
        let boundary = DateTime::from_timestamp_millis(boundary_ms).unwrap_or(to);
        let end = boundary.min(to);
        slices.push(Slice {
            bucket: bucket_of(at),
            shard,
            from: at,
            to: end,
        });
        at = end;
    }
    slices
}

/// One intent row I3 read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Intent {
    /// Its full key: the slice's bucket and shard, and the row's `enqueued_at`.
    pub(super) key: IntentKey,
    /// When it was enqueued, from the timeuuid (`Uuid::get_timestamp`); the slice's `from` if unreadable.
    pub(crate) enqueued: DateTime<Utc>,
    pub(crate) tenant_id: String,
    pub(crate) resource_type: String,
    pub(crate) id: String,
    pub(crate) version_id: i32,
}

/// SW11's four cases, by the version the row read showed, after SW12's serial read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IntentCase {
    /// No `resources` row: the CAS never applied, or the tenant is gone (SW21).
    NoRow,
    /// A version below V: the CAS never applied.
    NeverApplied,
    /// Version V is current: a crashed or slow request.
    Current,
    /// A version above V: V was superseded.
    Superseded,
}

impl IntentCase {
    pub(crate) fn of(intent_version: i32, row_version: Option<i32>) -> Self {
        match row_version {
            None => Self::NoRow,
            Some(row) if row < intent_version => Self::NeverApplied,
            Some(row) if row == intent_version => Self::Current,
            Some(_) => Self::Superseded,
        }
    }

    /// "no_row", "never_applied", "current", "superseded".
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NoRow => "no_row",
            Self::NeverApplied => "never_applied",
            Self::Current => "current",
            Self::Superseded => "superseded",
        }
    }
}

/// SW12: true when the quorum read showed no row or a version below V.
pub(crate) fn needs_serial_read(intent_version: i32, quorum_version: Option<i32>) -> bool {
    quorum_version.is_none_or(|version| version < intent_version)
}

/// A bug, never an empty set (SW13).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Invariant {
    /// Row V-1, or row V of a superseded V, is missing (G40 says it exists).
    MissingVersionRow,
    /// An `index_keys` that is unset or does not decode.
    UnreadableKeys,
    /// A stored `write_ts` that is not `write_ts_of(last_updated)` (SW11, RW10).
    WriteTsMismatch,
    /// The version row or the index delta failed as `WriteFailure::Permanent`: no try can succeed.
    PermanentWrite,
}

impl Invariant {
    /// "missing_version_row", "unreadable_keys", "write_ts", "permanent_write".
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::MissingVersionRow => "missing_version_row",
            Self::UnreadableKeys => "unreadable_keys",
            Self::WriteTsMismatch => "write_ts",
            Self::PermanentWrite => "permanent_write",
        }
    }
}

/// Why one try did not finish an intent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Failure {
    /// A read or write a later try may get through (SW18): any read error, `TriesExhausted`, `InBatchlog`.
    /// The text names the step: "read", "serial read", "version row read", "version row", "index".
    Transient(&'static str),
    Invariant(Invariant),
}

/// What one try of one intent did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IntentResult {
    /// `None` when the row read failed before a case was known.
    pub(crate) case: Option<IntentCase>,
    /// `Ok` once every write is confirmed and I2 was sent.
    pub(crate) result: Result<(), Failure>,
    /// The index delta as packed (`IndexOutcome::stats`); zero when none was sent.
    pub(crate) index: IndexWriteStats,
    /// A superseded replay that had no row to delete (`replays_noop`).
    pub(crate) noop: bool,
}

/// Who asks for statement permits (RW4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rank {
    Normal,
    Startup,
}

type RetryKey = (String, i16, Uuid, String, String, String);

fn retry_key(intent: &Intent) -> RetryKey {
    (
        intent.key.bucket().to_string(),
        intent.key.shard(),
        intent.key.enqueued_at(),
        intent.tenant_id.clone(),
        intent.resource_type.clone(),
        intent.id.clone(),
    )
}

/// The retry list (SW18 + RW3, SW13). Keyed by the intent's full row key; never holds two entries for one row.
pub(crate) struct RetryList {
    entries: BTreeMap<RetryKey, Entry>,
    cap: usize,
}

struct Entry {
    intent: Intent,
    failures: u32,
    invariant_failures: u32,
    next_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Queued {
    /// Kept; tried again at `next_at`.
    Retry { next_at: DateTime<Utc> },
    /// Dropped: `now >= enqueued + RETRY_HORIZON` (SW18). The intent row stays until its TTL.
    Expired,
    /// Dropped: the third invariant failure (SW13). The intent row stays until its TTL.
    GaveUp,
    /// Not added: the list is full. The caller holds its partition's mark (SW18).
    Overflow,
}

impl RetryList {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            cap,
        }
    }

    /// Records a failed try at `now`. Backoff after the k-th transient failure: `min(RETRY_FIRST x 2^(k-1), RETRY_MAX)`;
    /// an invariant failure is retried after `RETRY_FIRST` and counts toward `INVARIANT_TRIES`.
    pub(crate) fn failed(
        &mut self,
        intent: Intent,
        failure: Failure,
        now: DateTime<Utc>,
    ) -> Queued {
        let key = retry_key(&intent);
        if now >= intent.enqueued + retry_horizon() {
            self.entries.remove(&key);
            return Queued::Expired;
        }
        if !self.entries.contains_key(&key) && self.entries.len() >= self.cap {
            return Queued::Overflow;
        }
        let entry = self.entries.entry(key.clone()).or_insert(Entry {
            intent,
            failures: 0,
            invariant_failures: 0,
            next_at: now,
        });
        match failure {
            Failure::Transient(_) => {
                entry.failures += 1;
                let doublings = (entry.failures - 1).min(16);
                let delay = (retry_first() * 2i32.pow(doublings)).min(retry_max());
                entry.next_at = now + delay;
            }
            Failure::Invariant(_) => {
                entry.invariant_failures += 1;
                if entry.invariant_failures >= INVARIANT_TRIES {
                    self.entries.remove(&key);
                    return Queued::GaveUp;
                }
                entry.next_at = now + retry_first();
            }
        }
        Queued::Retry {
            next_at: entry.next_at,
        }
    }

    /// Removes the entry of `intent`'s row, if any.
    pub(crate) fn succeeded(&mut self, intent: &Intent) {
        self.entries.remove(&retry_key(intent));
    }

    /// The intents whose `next_at <= now`, oldest `next_at` first.
    pub(crate) fn due(&self, now: DateTime<Utc>) -> Vec<Intent> {
        let mut due: Vec<&Entry> = self
            .entries
            .values()
            .filter(|entry| entry.next_at <= now)
            .collect();
        due.sort_by_key(|entry| entry.next_at);
        due.into_iter().map(|entry| entry.intent.clone()).collect()
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

/// The loop's state: one mark per shard (SW8), the retry list, and the shutdown token (SW3).
pub(crate) struct SweepState {
    marks: parking_lot::Mutex<[DateTime<Utc>; 16]>,
    retry: parking_lot::Mutex<RetryList>,
    cancel: CancellationToken,
}

impl SweepState {
    /// Every mark at `mark` (truncated to whole ms); an empty list of `RETRY_LIST_CAP`.
    pub(crate) fn new(mark: DateTime<Utc>, cancel: CancellationToken) -> Self {
        Self {
            marks: parking_lot::Mutex::new([whole_ms(mark); 16]),
            retry: parking_lot::Mutex::new(RetryList::new(RETRY_LIST_CAP)),
            cancel,
        }
    }

    pub(crate) fn mark(&self, shard: i16) -> DateTime<Utc> {
        self.marks.lock()[shard as usize]
    }

    fn set_mark(&self, shard: i16, to: DateTime<Utc>) {
        let mut marks = self.marks.lock();
        let mark = &mut marks[shard as usize];
        if to > *mark {
            *mark = to;
        }
    }

    fn oldest_mark(&self) -> DateTime<Utc> {
        self.marks.lock().iter().min().copied().unwrap_or_default()
    }
}

macro_rules! sweeper_counters {
    (
        counters { $($(#[doc = $cdoc:literal])* $cname:ident,)* }
        gauges { $($(#[doc = $gdoc:literal])* $gname:ident,)* }
    ) => {
        /// The sweeper's counters and gauges, one atomic per field of [`SweeperStats`].
        #[derive(Debug, Default)]
        pub(crate) struct SweeperCounters {
            $($cname: AtomicU64,)*
            $($gname: AtomicU64,)*
        }

        impl SweeperCounters {
            /// A snapshot of every counter and gauge.
            pub(crate) fn snapshot(&self) -> SweeperStats {
                SweeperStats {
                    $($cname: self.$cname.load(Ordering::Relaxed),)*
                    $($gname: self.$gname.load(Ordering::Relaxed),)*
                }
            }
        }

        /// A snapshot of the sweeper's counters since the backend was built. Counters only grow, so two
        /// snapshots subtract ([`SweeperStats::since`]); gauges are the latest value.
        #[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
        #[non_exhaustive]
        pub struct SweeperStats {
            $($(#[doc = $cdoc])* pub $cname: u64,)*
            $($(#[doc = $gdoc])* pub $gname: u64,)*
        }

        impl SweeperStats {
            /// What happened between `earlier` and `self`; gauges keep `self`'s value.
            pub fn since(&self, earlier: &SweeperStats) -> SweeperStats {
                SweeperStats {
                    $($cname: self.$cname.saturating_sub(earlier.$cname),)*
                    $($gname: self.$gname,)*
                }
            }

            /// Every counter, then every gauge, as `(name, value)`, in declaration order.
            pub fn pairs(&self) -> Vec<(&'static str, u64)> {
                vec![
                    $((stringify!($cname), self.$cname),)*
                    $((stringify!($gname), self.$gname),)*
                ]
            }
        }
    };
}

sweeper_counters! {
    counters {
        /// Polls of the outbox, `sweep_pass` included.
        polls,
        /// Slices of I3 read, or tried.
        slices_read,
        /// Slices whose read failed.
        slice_errors,
        /// Node warnings that mentioned tombstones on I3 reads (SW10).
        tombstone_warnings,
        /// Intents in scope that I3 returned.
        intents_seen,
        /// Intents whose `resources` row was missing.
        case_no_row,
        /// Intents whose CAS never applied.
        case_never_applied,
        /// Intents of the current version.
        case_current,
        /// Intents of a superseded version.
        case_superseded,
        /// Intents finished and deleted.
        done,
        /// Superseded replays that had no row to delete.
        replays_noop,
        /// Failed tries kept on the retry list.
        retries,
        /// Retry entries dropped 24 hours after their intent was enqueued.
        retry_expired,
        /// Failed intents the full retry list could not take.
        overflow,
        /// Invariant violations (SW13), read errors of the outbox rows included.
        invariant_violations,
        /// Retry entries dropped after their third invariant failure.
        invariant_gave_up,
        /// SearchParameters put back in the overlay after a replay (SW19).
        search_parameters_followed,
        /// Slices read by the startup scan.
        startup_slices,
    }
    gauges {
        /// Milliseconds between now and the oldest shard mark, at the last poll.
        lag_ms,
        /// Entries on the retry list, after the last poll.
        retry_len,
        /// Milliseconds the startup scan took.
        startup_scan_ms,
    }
}

impl fmt::Display for SweeperStats {
    /// `polls=3 slices_read=48 …`, every counter and gauge, space-separated.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for (name, value) in self.pairs() {
            if !first {
                f.write_str(" ")?;
            }
            first = false;
            write!(f, "{name}={value}")?;
        }
        Ok(())
    }
}

impl SweeperCounters {
    fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    fn polled(&self) {
        Self::add(&self.polls, 1);
    }

    fn slice_tried(&self, startup: bool) {
        Self::add(&self.slices_read, 1);
        if startup {
            Self::add(&self.startup_slices, 1);
        }
    }

    fn slice_failed(&self) {
        Self::add(&self.slice_errors, 1);
    }

    fn tombstones(&self, warnings: u64) {
        Self::add(&self.tombstone_warnings, warnings);
    }

    fn seen(&self, intents: u64) {
        Self::add(&self.intents_seen, intents);
    }

    fn case(&self, case: IntentCase) {
        let counter = match case {
            IntentCase::NoRow => &self.case_no_row,
            IntentCase::NeverApplied => &self.case_never_applied,
            IntentCase::Current => &self.case_current,
            IntentCase::Superseded => &self.case_superseded,
        };
        Self::add(counter, 1);
    }

    fn finished(&self, noop: bool) {
        Self::add(&self.done, 1);
        if noop {
            Self::add(&self.replays_noop, 1);
        }
    }

    fn invariant(&self) {
        Self::add(&self.invariant_violations, 1);
    }

    fn followed(&self) {
        Self::add(&self.search_parameters_followed, 1);
    }

    fn queued(&self, queued: Queued) {
        match queued {
            Queued::Retry { .. } => Self::add(&self.retries, 1),
            Queued::Expired => Self::add(&self.retry_expired, 1),
            Queued::GaveUp => Self::add(&self.invariant_gave_up, 1),
            Queued::Overflow => Self::add(&self.overflow, 1),
        }
    }

    fn set_lag_ms(&self, ms: u64) {
        self.lag_ms.store(ms, Ordering::Relaxed);
    }

    fn set_retry_len(&self, len: u64) {
        self.retry_len.store(len, Ordering::Relaxed);
    }

    fn set_startup_scan_ms(&self, ms: u64) {
        self.startup_scan_ms.store(ms, Ordering::Relaxed);
    }
}

/// The sweeper's share of one backend: counters and the two semaphores (RW4).
pub(crate) struct SweeperShared {
    pub(crate) counters: SweeperCounters,
    in_flight: tokio::sync::Semaphore,
    startup_in_flight: tokio::sync::Semaphore,
}

impl Default for SweeperShared {
    fn default() -> Self {
        Self {
            counters: SweeperCounters::default(),
            in_flight: tokio::sync::Semaphore::new(MAX_IN_FLIGHT),
            startup_in_flight: tokio::sync::Semaphore::new(STARTUP_IN_FLIGHT),
        }
    }
}

/// Permits held until dropped.
pub(crate) struct Permits<'a> {
    _startup: Option<tokio::sync::SemaphorePermit<'a>>,
    _shared: tokio::sync::SemaphorePermit<'a>,
}

impl SweeperShared {
    /// `n` permits (clamped to 1..=8) of `in_flight`; a `Startup` caller first takes `n` of `startup_in_flight`.
    /// Held until the returned guard drops. The semaphores are never closed, so `expect` is allowed here.
    pub(crate) async fn permits(&self, rank: Rank, n: usize) -> Permits<'_> {
        let n = n.clamp(1, INDEX_BATCHES_IN_FLIGHT) as u32;
        let startup = match rank {
            Rank::Startup => Some(
                self.startup_in_flight
                    .acquire_many(n)
                    .await
                    .expect("the startup semaphore is never closed"),
            ),
            Rank::Normal => None,
        };
        let shared = self
            .in_flight
            .acquire_many(n)
            .await
            .expect("the sweeper semaphore is never closed");
        Permits {
            _startup: startup,
            _shared: shared,
        }
    }
}

/// Which intents a pass or a poll processes. Every slice is still read whole; the scope filters its rows.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SweepScope {
    /// Every tenant: the background sweeper (SW21).
    All,
    /// Only these tenant ids.
    Tenants(Vec<String>),
    /// Only tenant ids that start with this prefix (the post-suite step's `test-`).
    TenantPrefix(String),
}

impl SweepScope {
    pub fn covers(&self, tenant_id: &str) -> bool {
        match self {
            Self::All => true,
            Self::Tenants(tenants) => tenants.iter().any(|tenant| tenant == tenant_id),
            Self::TenantPrefix(prefix) => tenant_id.starts_with(prefix.as_str()),
        }
    }
}

/// One `sweep_pass` (SW28).
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct SweepOptions {
    pub scope: SweepScope,
    /// Only intents enqueued before `now - grace`.
    pub grace: Duration,
    /// The pass's clock: `Utc::now()` outside the sweeper's own tests.
    pub now: DateTime<Utc>,
    /// Only intents enqueued at or after `since`.
    pub since: DateTime<Utc>,
    /// Allows a `grace` below `HFS_CASSANDRA_SWEEPER_GRACE_MS`. Set it only after every writer of the
    /// swept tenants has returned and none ended on HFS's own deadline or a dropped connection
    /// (`CasStats::client_side_unknowns`, SW28, RV19, issue #89).
    pub allow_short_grace: bool,
}

/// What one pass or poll did.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct SweepReport {
    pub slices: u64,
    pub slice_errors: u64,
    pub tombstone_warnings: u64,
    /// Intents in scope.
    pub intents: u64,
    pub no_row: u64,
    pub never_applied: u64,
    pub current: u64,
    pub superseded: u64,
    /// Finished and deleted.
    pub done: u64,
    /// Transient failures (SW18); their intent rows stay.
    pub failed: u64,
    /// Invariant failures (SW13); their intent rows stay.
    pub invariant: u64,
    /// Poll only: intents the full retry list could not take.
    pub overflow: u64,
    /// Poll only: retry entries dropped (`Expired` or `GaveUp`).
    pub gave_up: u64,
}

impl SweepReport {
    /// No slice error, no failure, no invariant: every intent in scope that was read is finished.
    pub fn is_complete(&self) -> bool {
        self.slice_errors == 0 && self.failed == 0 && self.invariant == 0
    }

    fn absorb(&mut self, other: &SweepReport) {
        self.slices += other.slices;
        self.slice_errors += other.slice_errors;
        self.tombstone_warnings += other.tombstone_warnings;
        self.intents += other.intents;
        self.no_row += other.no_row;
        self.never_applied += other.never_applied;
        self.current += other.current;
        self.superseded += other.superseded;
        self.done += other.done;
        self.failed += other.failed;
        self.invariant += other.invariant;
        self.overflow += other.overflow;
        self.gave_up += other.gave_up;
    }
}

impl fmt::Display for SweepReport {
    /// `[sweep-pass slices=... slice_errors=... ... gave_up=...]`, every field, in declaration order.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[sweep-pass slices={} slice_errors={} tombstone_warnings={} intents={} no_row={} \
             never_applied={} current={} superseded={} done={} failed={} invariant={} \
             overflow={} gave_up={}]",
            self.slices,
            self.slice_errors,
            self.tombstone_warnings,
            self.intents,
            self.no_row,
            self.never_applied,
            self.current,
            self.superseded,
            self.done,
            self.failed,
            self.invariant,
            self.overflow,
            self.gave_up
        )
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SweepError {
    #[error(
        "sweep_pass grace {grace_ms} ms is below HFS_CASSANDRA_SWEEPER_GRACE_MS ({configured_ms} ms); \
         set allow_short_grace only after every writer of the swept tenants has returned"
    )]
    ShortGrace { grace_ms: u64, configured_ms: u64 },
}

/// SW28: refuses a grace below the configured one unless `allow_short_grace`. Pure.
fn check_grace(
    grace: Duration,
    allow_short_grace: bool,
    configured_ms: u64,
) -> Result<(), SweepError> {
    let grace_ms = grace.as_millis().min(u128::from(u64::MAX)) as u64;
    if grace_ms < configured_ms && !allow_short_grace {
        return Err(SweepError::ShortGrace {
            grace_ms,
            configured_ms,
        });
    }
    Ok(())
}

/// A version row as R3 reads it.
struct StoredVersion {
    last_updated: DateTime<Utc>,
    fhir_version: Option<String>,
    write_ts: Option<i64>,
    index_keys: Option<Vec<u8>>,
}

/// What a replay did, before I2.
struct Replay {
    result: Result<(), Failure>,
    index: IndexWriteStats,
    noop: bool,
}

impl Replay {
    fn failed(failure: Failure) -> Self {
        Self {
            result: Err(failure),
            index: IndexWriteStats::default(),
            noop: false,
        }
    }
}

/// What one slice did.
struct SliceOutcome {
    report: SweepReport,
    read_ok: bool,
    overflow: bool,
}

/// The decoded `K(V-1)`, with the `last_updated` of the version that wrote it.
struct PreviousKeys {
    keys: Vec<IndexKey>,
    last_updated: DateTime<Utc>,
}

impl CassandraBackend {
    /// SW1, SW2, SW3: the background sweeper, `run_sweeper_with(cancel, SweepScope::All, true)`.
    /// Returns once `cancel` fires. The server spawns it (`start_cassandra`) when `sweeper_enabled`;
    /// `connect` never does.
    pub async fn run_sweeper(self: Arc<Self>, cancel: CancellationToken) {
        self.run_sweeper_with(cancel, SweepScope::All, true).await;
    }

    /// The sweeper's counters since this backend was built (SW25). Subtract two snapshots with `SweeperStats::since`.
    pub fn sweeper_stats(&self) -> SweeperStats {
        self.sweeper.counters.snapshot()
    }

    /// SW28: one pass over `[options.since, options.now - options.grace)` in one-minute slices, every shard,
    /// processing the intents `options.scope` covers. No retry list: a failure is counted and its intent row stays.
    #[doc(hidden)]
    pub async fn sweep_pass(&self, options: SweepOptions) -> Result<SweepReport, SweepError> {
        check_grace(
            options.grace,
            options.allow_short_grace,
            self.config.sweeper_grace_ms,
        )?;
        self.sweeper.counters.polled();
        let grace = chrono::Duration::from_std(options.grace).unwrap_or(chrono::Duration::MAX);
        let to = whole_ms(options.now - grace);
        let from = whole_ms(options.since);
        if from >= to {
            return Ok(SweepReport::default());
        }
        let report = self
            .scan_window(
                from,
                to,
                &options.scope,
                Rank::Normal,
                None,
                &CancellationToken::new(),
            )
            .await;
        Ok(report)
    }

    /// The loop with a scope, and the startup scan only when `startup_scan` (tests turn it off).
    pub(crate) async fn run_sweeper_with(
        self: Arc<Self>,
        cancel: CancellationToken,
        scope: SweepScope,
        startup_scan: bool,
    ) {
        let started = Utc::now();
        let poll = self.config.sweeper_poll_interval();
        let grace_std = self.config.sweeper_grace();
        let grace = chrono::Duration::from_std(grace_std).unwrap_or(chrono::Duration::MAX);
        let state = SweepState::new(started - grace, cancel.clone());
        let lag_limit_ms = 2 * (poll + grace_std).as_millis() as u64;
        tracing::info!(
            poll_ms = poll.as_millis() as u64,
            grace_ms = grace_std.as_millis() as u64,
            startup_scan,
            "index sweeper started"
        );

        let scan = async {
            if !startup_scan {
                return;
            }
            let began = std::time::Instant::now();
            let report = self
                .startup_scan(&state, started - startup_window(), started - grace, &scope)
                .await;
            let took_ms = began.elapsed().as_millis() as u64;
            self.sweeper.counters.set_startup_scan_ms(took_ms);
            tracing::info!(
                slices = report.slices,
                intents = report.intents,
                done = report.done,
                took_ms,
                "startup scan finished"
            );
        };
        let main_loop = async {
            let mut summary_at = std::time::Instant::now();
            let mut summarized = self.sweeper_stats();
            let mut lag_warned = false;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(poll) => {}
                }
                let report = self.poll_once(&state, Utc::now(), &scope).await;
                let unfinished = report.failed
                    + report.invariant
                    + report.overflow
                    + report.gave_up
                    + report.slice_errors;
                if unfinished > 0 {
                    tracing::warn!(
                        failed = report.failed,
                        invariant = report.invariant,
                        overflow = report.overflow,
                        gave_up = report.gave_up,
                        slice_errors = report.slice_errors,
                        "index sweeper poll left intents in the outbox; they are retried"
                    );
                }
                let lag_ms = self.sweeper_stats().lag_ms;
                if lag_ms > lag_limit_ms {
                    if !lag_warned {
                        lag_warned = true;
                        tracing::warn!(
                            lag_ms,
                            limit_ms = lag_limit_ms,
                            "index sweeper is behind: the oldest shard mark lags the clock"
                        );
                    }
                } else {
                    lag_warned = false;
                }
                if summary_at.elapsed() >= SUMMARY_EVERY {
                    let now = self.sweeper_stats();
                    if now.intents_seen > summarized.intents_seen {
                        let stats = now.since(&summarized);
                        tracing::info!(%stats, "index sweeper summary");
                    }
                    summarized = now;
                    summary_at = std::time::Instant::now();
                }
            }
        };
        futures::future::join(scan, main_loop).await;
        tracing::info!("index sweeper stopped");
    }

    /// SW8: one poll at `now`. Each shard reads `plan_slices(s, mark(s), now - grace, POLL_SLICE)`; then the due retries.
    pub(crate) async fn poll_once(
        &self,
        state: &SweepState,
        now: DateTime<Utc>,
        scope: &SweepScope,
    ) -> SweepReport {
        self.sweeper.counters.polled();
        let grace = chrono::Duration::from_std(self.config.sweeper_grace())
            .unwrap_or(chrono::Duration::MAX);
        let to = whole_ms(now - grace);
        let shards: Vec<_> = (0..INTENT_SHARDS as i16)
            .map(|shard| async move {
                let mut report = SweepReport::default();
                let mut advance = true;
                for slice in plan_slices(shard, state.mark(shard), to, poll_slice()) {
                    if state.cancel.is_cancelled() {
                        advance = false;
                        break;
                    }
                    let outcome = self
                        .handle_slice(&slice, Rank::Normal, scope, Some(state), now)
                        .await;
                    report.absorb(&outcome.report);
                    if !outcome.read_ok {
                        advance = false;
                        break;
                    }
                    if outcome.overflow {
                        advance = false;
                    }
                }
                (shard, advance, report)
            })
            .collect();
        let mut report = SweepReport::default();
        let mut held = 0u64;
        let mut results = futures::stream::iter(shards).buffer_unordered(SLICES_AT_ONCE);
        while let Some((shard, advance, shard_report)) = results.next().await {
            report.absorb(&shard_report);
            if advance {
                state.set_mark(shard, to);
            } else if shard_report.overflow > 0 {
                held += 1;
            }
        }
        drop(results);
        if held > 0 {
            tracing::error!(
                shards_held = held,
                retry_cap = RETRY_LIST_CAP,
                "the index sweeper's retry list is full; these shards keep their marks"
            );
        }

        let due = { state.retry.lock().due(now) };
        if !due.is_empty() {
            let mut retried = SweepReport::default();
            self.run_intents(&due, Rank::Normal, Some(state), now, &mut retried)
                .await;
            retried.intents = 0;
            report.absorb(&retried);
        }
        let retry_len = state.retry.lock().len() as u64;
        self.sweeper.counters.set_retry_len(retry_len);
        let lag = (now - state.oldest_mark()).num_milliseconds().max(0) as u64;
        self.sweeper.counters.set_lag_ms(lag);
        report
    }

    /// SW9, SW10: `[from, to)` in one-minute slices at `Rank::Startup`; failures go to `state`'s retry list.
    pub(crate) async fn startup_scan(
        &self,
        state: &SweepState,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        scope: &SweepScope,
    ) -> SweepReport {
        self.scan_window(from, to, scope, Rank::Startup, Some(state), &state.cancel)
            .await
    }

    /// Minute by minute, oldest first, the 16 shards' slices of one minute at once. Serves the startup
    /// scan, which keeps a retry list, and `sweep_pass`, which does not.
    async fn scan_window(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        scope: &SweepScope,
        rank: Rank,
        retry: Option<&SweepState>,
        cancel: &CancellationToken,
    ) -> SweepReport {
        let mut report = SweepReport::default();
        let now = Utc::now();
        for minute in plan_slices(0, from, to, startup_slice()) {
            if cancel.is_cancelled() {
                break;
            }
            let slices: Vec<Slice> = (0..INTENT_SHARDS as i16)
                .map(|shard| Slice {
                    bucket: minute.bucket.clone(),
                    shard,
                    from: minute.from,
                    to: minute.to,
                })
                .collect();
            let reads: Vec<_> = slices
                .iter()
                .map(|slice| self.handle_slice(slice, rank, scope, retry, now))
                .collect();
            let mut outcomes = futures::stream::iter(reads).buffer_unordered(SLICES_AT_ONCE);
            while let Some(outcome) = outcomes.next().await {
                report.absorb(&outcome.report);
            }
        }
        report
    }

    /// I3 for one slice: its intents, and the node's tombstone warnings on it (SW10). Undecodable rows are logged at
    /// `error`, counted as `invariant_violations`, and left out.
    pub(crate) async fn read_slice(
        &self,
        slice: &Slice,
        rank: Rank,
    ) -> Result<(Vec<Intent>, u64), DriverError> {
        let (rows, warnings) = {
            let _permit = self.sweeper.permits(rank, 1).await;
            self.session
                .rows_paged_counting(
                    SELECT_INTENTS_OF_SLICE,
                    vec![
                        Value::from(slice.bucket.as_str()),
                        Value::from(slice.shard),
                        Value::from(slice.from),
                        Value::from(slice.to),
                    ],
                    StatementKind::Read,
                    OUTBOX_PAGE_SIZE,
                    |_| true,
                )
                .await?
        };
        let mut intents = Vec::with_capacity(rows.len());
        for row in &rows {
            match decode_intent(slice, row) {
                Some(intent) => intents.push(intent),
                None => {
                    self.sweeper.counters.invariant();
                    tracing::error!(
                        bucket = %slice.bucket,
                        shard = slice.shard,
                        invariant = "unreadable_intent",
                        "an intent row does not decode; it is left in the outbox"
                    );
                }
            }
        }
        Ok((intents, warnings))
    }

    /// One slice: read it, keep the intents in scope, try each.
    async fn handle_slice(
        &self,
        slice: &Slice,
        rank: Rank,
        scope: &SweepScope,
        state: Option<&SweepState>,
        now: DateTime<Utc>,
    ) -> SliceOutcome {
        let mut outcome = SliceOutcome {
            report: SweepReport {
                slices: 1,
                ..SweepReport::default()
            },
            read_ok: true,
            overflow: false,
        };
        self.sweeper.counters.slice_tried(rank == Rank::Startup);
        let (intents, warnings) = match self.read_slice(slice, rank).await {
            Ok(read) => read,
            Err(error) => {
                self.sweeper.counters.slice_failed();
                outcome.report.slice_errors = 1;
                outcome.read_ok = false;
                tracing::debug!(
                    bucket = %slice.bucket,
                    shard = slice.shard,
                    error = ?error,
                    "an outbox slice could not be read; its shard keeps its mark"
                );
                return outcome;
            }
        };
        self.sweeper.counters.tombstones(warnings);
        outcome.report.tombstone_warnings = warnings;
        let intents: Vec<Intent> = intents
            .into_iter()
            .filter(|intent| scope.covers(&intent.tenant_id))
            .collect();
        outcome.report.intents = intents.len() as u64;
        self.sweeper.counters.seen(intents.len() as u64);
        outcome.overflow = self
            .run_intents(&intents, rank, state, now, &mut outcome.report)
            .await;
        outcome
    }

    /// Tries each intent, `INTENTS_PER_SLICE_AT_ONCE` at once, and settles the results into `report`
    /// and, when there is a state, its retry list. True when the retry list refused one.
    async fn run_intents(
        &self,
        intents: &[Intent],
        rank: Rank,
        state: Option<&SweepState>,
        now: DateTime<Utc>,
        report: &mut SweepReport,
    ) -> bool {
        let cancel = state.map(|state| state.cancel.clone());
        let tries: Vec<_> = intents
            .iter()
            .map(|intent| {
                let cancel = cancel.clone();
                async move {
                    // SW3: an intent not yet started is skipped once shutdown is asked for.
                    if cancel.is_some_and(|cancel| cancel.is_cancelled()) {
                        return None;
                    }
                    Some((intent, self.process_intent(intent, rank).await))
                }
            })
            .collect();
        let mut overflow = false;
        let mut results = futures::stream::iter(tries).buffer_unordered(INTENTS_PER_SLICE_AT_ONCE);
        while let Some(done) = results.next().await {
            if let Some((intent, result)) = done {
                overflow |= self.settle(report, state, intent, result, now);
            }
        }
        overflow
    }

    /// Counts one try into `report`, and updates the retry list. True on `Queued::Overflow`.
    fn settle(
        &self,
        report: &mut SweepReport,
        state: Option<&SweepState>,
        intent: &Intent,
        result: IntentResult,
        now: DateTime<Utc>,
    ) -> bool {
        match result.case {
            Some(IntentCase::NoRow) => report.no_row += 1,
            Some(IntentCase::NeverApplied) => report.never_applied += 1,
            Some(IntentCase::Current) => report.current += 1,
            Some(IntentCase::Superseded) => report.superseded += 1,
            None => {}
        }
        let failure = match result.result {
            Ok(()) => {
                report.done += 1;
                if let Some(state) = state {
                    state.retry.lock().succeeded(intent);
                }
                return false;
            }
            Err(failure) => failure,
        };
        match failure {
            Failure::Transient(step) => {
                report.failed += 1;
                tracing::debug!(
                    tenant_id = %intent.tenant_id,
                    resource_type = %intent.resource_type,
                    id = %intent.id,
                    version_id = intent.version_id,
                    step,
                    "an intent could not be finished; it is retried"
                );
            }
            Failure::Invariant(_) => report.invariant += 1,
        }
        let Some(state) = state else {
            return false;
        };
        let queued = state.retry.lock().failed(intent.clone(), failure, now);
        self.sweeper.counters.queued(queued);
        match queued {
            Queued::Retry { .. } => false,
            Queued::Expired => {
                report.gave_up += 1;
                tracing::error!(
                    tenant_id = %intent.tenant_id,
                    resource_type = %intent.resource_type,
                    id = %intent.id,
                    version_id = intent.version_id,
                    "an intent was not finished within 24 hours; the sweeper stops retrying it"
                );
                false
            }
            Queued::GaveUp => {
                report.gave_up += 1;
                tracing::error!(
                    tenant_id = %intent.tenant_id,
                    resource_type = %intent.resource_type,
                    id = %intent.id,
                    version_id = intent.version_id,
                    "an intent hit an invariant three times; the sweeper stops retrying it"
                );
                false
            }
            Queued::Overflow => {
                report.overflow += 1;
                true
            }
        }
    }

    /// SW11 to SW16, SW19: one try of one intent.
    pub(crate) async fn process_intent(&self, intent: &Intent, rank: Rank) -> IntentResult {
        let key = RowKey {
            tenant_id: &intent.tenant_id,
            resource_type: &intent.resource_type,
            id: &intent.id,
        };
        let version = intent.version_id;
        let quorum = {
            let _permit = self.sweeper.permits(rank, 1).await;
            self.read_resource_row(&intent.tenant_id, &intent.resource_type, &intent.id)
                .await
        };
        let mut row = match quorum {
            Ok(row) => row,
            Err(_) => return self.stopped(intent, None, Failure::Transient("read")),
        };
        if needs_serial_read(version, row.as_ref().map(|row| row.version_id)) {
            let serial = {
                let _permit = self.sweeper.permits(rank, 1).await;
                self.serial_read_resource_row(&intent.tenant_id, &intent.resource_type, &intent.id)
                    .await
            };
            row = match serial {
                Ok(row) => row,
                Err(_) => return self.stopped(intent, None, Failure::Transient("serial read")),
            };
        }
        let case = IntentCase::of(version, row.as_ref().map(|row| row.version_id));
        self.sweeper.counters.case(case);
        let replay = match (case, row.as_ref()) {
            (IntentCase::Current, Some(row)) => self.replay_current(&key, intent, row, rank).await,
            (IntentCase::Superseded, Some(row)) => {
                self.replay_superseded(&key, intent, row, rank).await
            }
            _ => Replay {
                result: Ok(()),
                index: IndexWriteStats::default(),
                noop: false,
            },
        };
        if let Err(failure) = replay.result {
            if let Failure::Invariant(invariant) = failure {
                self.sweeper.counters.invariant();
                tracing::error!(
                    tenant_id = %intent.tenant_id,
                    resource_type = %intent.resource_type,
                    id = %intent.id,
                    version_id = version,
                    case = case.as_str(),
                    invariant = invariant.as_str(),
                    "an intent hit an invariant; its row is kept in the outbox"
                );
            }
            return IntentResult {
                case: Some(case),
                result: Err(failure),
                index: replay.index,
                noop: replay.noop,
            };
        }
        // SW19: a SearchParameter that was replayed follows the overlay.
        if matches!(case, IntentCase::Current | IntentCase::Superseded)
            && intent.resource_type == "SearchParameter"
        {
            self.follow_stored_search_parameter(&intent.tenant_id, &intent.id, row.as_ref());
            self.sweeper.counters.followed();
        }
        {
            let _permit = self.sweeper.permits(rank, 1).await;
            self.delete_intent(&intent.key, &key).await;
        }
        self.sweeper.counters.finished(replay.noop);
        IntentResult {
            case: Some(case),
            result: Ok(()),
            index: replay.index,
            noop: replay.noop,
        }
    }

    /// An intent that stopped before a case was known.
    fn stopped(&self, intent: &Intent, case: Option<IntentCase>, failure: Failure) -> IntentResult {
        tracing::debug!(
            tenant_id = %intent.tenant_id,
            resource_type = %intent.resource_type,
            id = %intent.id,
            version_id = intent.version_id,
            "an intent's row could not be read"
        );
        IntentResult {
            case,
            result: Err(failure),
            index: IndexWriteStats::default(),
            noop: false,
        }
    }

    /// R3: one version row of the resource.
    async fn read_version(
        &self,
        key: &RowKey<'_>,
        version: i32,
        rank: Rank,
    ) -> Result<Option<StoredVersion>, Failure> {
        let rows = {
            let _permit = self.sweeper.permits(rank, 1).await;
            self.session
                .rows(
                    SELECT_VERSION_ROW,
                    vec![
                        Value::from(key.tenant_id),
                        Value::from(key.resource_type),
                        Value::from(key.id),
                        Value::from(version),
                    ],
                    StatementKind::Read,
                )
                .await
                .map_err(|_| Failure::Transient("version row read"))?
        };
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let unreadable = || Failure::Invariant(Invariant::UnreadableKeys);
        let last_updated: DateTime<Utc> = row
            .get_r_by_name("last_updated")
            .map_err(|_| unreadable())?;
        let index_keys = IntoRustByName::<Blob>::get_by_name(row, "index_keys")
            .map_err(|_| unreadable())?
            .map(Blob::into_vec);
        Ok(Some(StoredVersion {
            last_updated,
            fhir_version: IntoRustByName::<String>::get_by_name(row, "fhir_version")
                .ok()
                .flatten(),
            write_ts: IntoRustByName::<i64>::get_by_name(row, "write_ts")
                .ok()
                .flatten(),
            index_keys,
        }))
    }

    /// `K(V-1)`: empty for V = 1, else decoded from version row V-1 (SW11).
    async fn previous_keys(
        &self,
        key: &RowKey<'_>,
        version: i32,
        rank: Rank,
    ) -> Result<PreviousKeys, Failure> {
        if version <= 1 {
            return Ok(PreviousKeys {
                keys: Vec::new(),
                last_updated: DateTime::<Utc>::default(),
            });
        }
        let Some(stored) = self.read_version(key, version - 1, rank).await? else {
            return Err(Failure::Invariant(Invariant::MissingVersionRow));
        };
        let keys = stored
            .index_keys
            .as_deref()
            .and_then(|blob| index_keys::decode(blob).ok())
            .ok_or(Failure::Invariant(Invariant::UnreadableKeys))?;
        Ok(PreviousKeys {
            keys,
            last_updated: stored.last_updated,
        })
    }

    /// Writes `delta` with the permits it needs, as the request does (IW17).
    async fn send_delta(
        &self,
        key: &RowKey<'_>,
        delta: &super::write::IndexDelta<'_>,
        ts: i64,
        rank: Rank,
    ) -> (Result<(), Failure>, IndexWriteStats) {
        let _permit = self
            .sweeper
            .permits(rank, delta.units.len().clamp(1, INDEX_BATCHES_IN_FLIGHT))
            .await;
        let outcome = self.write_index_delta(key, delta, ts, SWEEPER_TRIES).await;
        let result = match outcome.result {
            Ok(()) => Ok(()),
            Err(WriteFailure::Permanent) => Err(Failure::Invariant(Invariant::PermanentWrite)),
            Err(_) => Err(Failure::Transient("index")),
        };
        (result, outcome.stats)
    }

    /// SW11's current case: `R` is the row at version V.
    async fn replay_current(
        &self,
        key: &RowKey<'_>,
        intent: &Intent,
        row: &ResourceRow,
        rank: Rank,
    ) -> Replay {
        let version = intent.version_id;
        if row.write_ts != Some(write_ts_of(row.last_updated)) {
            return Replay::failed(Failure::Invariant(Invariant::WriteTsMismatch));
        }
        let marker = row.is_deleted();
        let (keys, blob): (Vec<IndexKey>, Vec<u8>) = if marker {
            (Vec::new(), index_keys::EMPTY.to_vec())
        } else {
            let Some(blob) = row.index_keys.clone() else {
                return Replay::failed(Failure::Invariant(Invariant::UnreadableKeys));
            };
            match index_keys::decode(&blob) {
                Ok(keys) => (keys, blob),
                Err(_) => return Replay::failed(Failure::Invariant(Invariant::UnreadableKeys)),
            }
        };
        let previous = match self.previous_keys(key, version, rank).await {
            Ok(previous) => previous,
            Err(failure) => return Replay::failed(failure),
        };
        let ts = write_ts_of(row.last_updated);
        let written = {
            let _permit = self.sweeper.permits(rank, 1).await;
            self.write_version_row(
                key,
                &VersionRow {
                    version_id: version,
                    last_updated: row.last_updated,
                    deleted_at: row.deleted_at,
                    data: if marker { None } else { row.data.as_deref() },
                    fhir_version: row.fhir_version_text(),
                    write_id: row.write_id,
                    keys_blob: &blob,
                },
                ts,
                SWEEPER_TRIES,
            )
            .await
        };
        // IW24: no index statement for a version whose row is not written.
        match written {
            Ok(()) => {}
            Err(WriteFailure::Permanent) => {
                return Replay::failed(Failure::Invariant(Invariant::PermanentWrite));
            }
            Err(_) => return Replay::failed(Failure::Transient("version row")),
        }
        let previous_rows: Vec<IndexRow<'_>> = previous
            .keys
            .iter()
            .map(|key| (key, previous.last_updated))
            .collect();
        let delta = index_delta(
            &previous_rows,
            VersionKeys {
                version_id: version,
                last_updated: row.last_updated,
                fhir_version: row.fhir_version_text(),
                keys: &keys,
            },
        );
        let (result, index) = self.send_delta(key, &delta, ts, rank).await;
        Replay {
            result,
            index,
            noop: false,
        }
    }

    /// SW11's superseded case: `R` is a row of a version above V. Deletes the rows of `K(V-1)` that
    /// version V replaced; never an insert, and no version row.
    async fn replay_superseded(
        &self,
        key: &RowKey<'_>,
        intent: &Intent,
        row: &ResourceRow,
        rank: Rank,
    ) -> Replay {
        let version = intent.version_id;
        if version <= 1 {
            return Replay {
                result: Ok(()),
                index: IndexWriteStats::default(),
                noop: true,
            };
        }
        let stored = match self.read_version(key, version, rank).await {
            Ok(Some(stored)) => stored,
            Ok(None) => return Replay::failed(Failure::Invariant(Invariant::MissingVersionRow)),
            Err(failure) => return Replay::failed(failure),
        };
        if stored.write_ts != Some(write_ts_of(stored.last_updated)) {
            return Replay::failed(Failure::Invariant(Invariant::WriteTsMismatch));
        }
        let ts = write_ts_of(stored.last_updated);
        let previous = match self.previous_keys(key, version, rank).await {
            Ok(previous) => previous,
            Err(failure) => return Replay::failed(failure),
        };
        let previous_rows: Vec<IndexRow<'_>> = previous
            .keys
            .iter()
            .map(|key| (key, previous.last_updated))
            .collect();
        let delta = index_delta(
            &previous_rows,
            VersionKeys {
                version_id: version,
                last_updated: stored.last_updated,
                fhir_version: stored
                    .fhir_version
                    .as_deref()
                    .unwrap_or_else(|| row.fhir_version_text()),
                keys: &[],
            },
        );
        if delta.units.is_empty() {
            return Replay {
                result: Ok(()),
                index: IndexWriteStats::default(),
                noop: true,
            };
        }
        let (result, index) = self.send_delta(key, &delta, ts, rank).await;
        Replay {
            result,
            index,
            noop: false,
        }
    }
}

/// One row of I3, or `None` when a column does not decode.
fn decode_intent(slice: &Slice, row: &Row) -> Option<Intent> {
    let enqueued_at: Uuid = row.get_r_by_name("enqueued_at").ok()?;
    let enqueued = enqueued_at
        .get_timestamp()
        .and_then(|timestamp| {
            let (seconds, nanos) = timestamp.to_unix();
            DateTime::from_timestamp(i64::try_from(seconds).ok()?, nanos)
        })
        .unwrap_or(slice.from);
    Some(Intent {
        key: IntentKey::from_parts(slice.bucket.clone(), slice.shard, enqueued_at),
        enqueued,
        tenant_id: row.get_r_by_name("tenant_id").ok()?,
        resource_type: row.get_r_by_name("resource_type").ok()?,
        id: row.get_r_by_name("id").ok()?,
        version_id: row.get_r_by_name("version_id").ok()?,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .unwrap()
            .with_timezone(&Utc)
    }

    /// P1 (gate): SW11's four cases, and when the serial read is needed (SW12).
    #[test]
    fn classify_follows_the_four_cases() {
        assert_eq!(IntentCase::of(2, None), IntentCase::NoRow);
        assert_eq!(IntentCase::of(2, Some(1)), IntentCase::NeverApplied);
        assert_eq!(IntentCase::of(2, Some(2)), IntentCase::Current);
        assert_eq!(IntentCase::of(2, Some(3)), IntentCase::Superseded);
        assert_eq!(IntentCase::of(1, None), IntentCase::NoRow);
        assert!(needs_serial_read(2, None));
        assert!(needs_serial_read(2, Some(1)));
        assert!(!needs_serial_read(2, Some(2)));
        assert!(!needs_serial_read(2, Some(3)));
        assert_eq!(IntentCase::NoRow.as_str(), "no_row");
        assert_eq!(IntentCase::NeverApplied.as_str(), "never_applied");
        assert_eq!(IntentCase::Current.as_str(), "current");
        assert_eq!(IntentCase::Superseded.as_str(), "superseded");
    }

    /// P2 (gate): slices are cut at the unit boundary, half open, whole milliseconds.
    #[test]
    fn slice_planner_crosses_the_hour() {
        let planned = plan_slices(
            3,
            at("2026-11-22T10:59:50Z"),
            at("2026-11-22T11:00:20Z"),
            poll_slice(),
        );
        let expected = vec![
            Slice {
                bucket: "2026-11-22T10".into(),
                shard: 3,
                from: at("2026-11-22T10:59:50Z"),
                to: at("2026-11-22T11:00:00Z"),
            },
            Slice {
                bucket: "2026-11-22T11".into(),
                shard: 3,
                from: at("2026-11-22T11:00:00Z"),
                to: at("2026-11-22T11:00:20Z"),
            },
        ];
        assert_eq!(planned, expected);

        let planned = plan_slices(
            3,
            at("2026-11-22T10:58:30Z"),
            at("2026-11-22T11:01:10Z"),
            startup_slice(),
        );
        let bounds: Vec<(String, DateTime<Utc>, DateTime<Utc>)> = planned
            .iter()
            .map(|slice| (slice.bucket.clone(), slice.from, slice.to))
            .collect();
        assert_eq!(
            bounds,
            vec![
                (
                    "2026-11-22T10".into(),
                    at("2026-11-22T10:58:30Z"),
                    at("2026-11-22T10:59:00Z")
                ),
                (
                    "2026-11-22T10".into(),
                    at("2026-11-22T10:59:00Z"),
                    at("2026-11-22T11:00:00Z")
                ),
                (
                    "2026-11-22T11".into(),
                    at("2026-11-22T11:00:00Z"),
                    at("2026-11-22T11:01:00Z")
                ),
                (
                    "2026-11-22T11".into(),
                    at("2026-11-22T11:01:00Z"),
                    at("2026-11-22T11:01:10Z")
                ),
            ]
        );
        for pair in planned.windows(2) {
            assert_eq!(pair[0].to, pair[1].from);
        }

        let sub_ms = plan_slices(
            1,
            at("2026-11-22T10:00:00.250999Z"),
            at("2026-11-22T10:00:00.500Z"),
            poll_slice(),
        );
        assert_eq!(sub_ms.len(), 1);
        assert_eq!(sub_ms[0].from, at("2026-11-22T10:00:00.250Z"));
        assert_eq!(sub_ms[0].to, at("2026-11-22T10:00:00.500Z"));

        let t = at("2026-11-22T10:00:00Z");
        assert!(plan_slices(1, t, t, poll_slice()).is_empty());
        assert!(plan_slices(1, t + chrono::Duration::seconds(1), t, poll_slice()).is_empty());

        let days = plan_slices(
            3,
            at("2026-11-22T23:59:59Z"),
            at("2026-11-23T00:00:01Z"),
            poll_slice(),
        );
        let buckets: Vec<&str> = days.iter().map(|slice| slice.bucket.as_str()).collect();
        assert_eq!(buckets, vec!["2026-11-22T23", "2026-11-23T00"]);
    }

    fn intent(id: &str, enqueued: DateTime<Utc>) -> Intent {
        Intent {
            key: IntentKey::from_parts(bucket_of(enqueued), 1, Uuid::nil()),
            enqueued,
            tenant_id: "t".into(),
            resource_type: "Patient".into(),
            id: id.into(),
            version_id: 1,
        }
    }

    /// P3: SW18's backoff and horizon, SW13's three tries, the cap.
    #[test]
    fn the_retry_list_backs_off_and_gives_up_only_as_sw13_and_sw18_say() {
        let t0 = at("2026-11-22T10:00:00Z");
        let i = intent("i", t0);
        let mut list = RetryList::new(RETRY_LIST_CAP);
        let transient = Failure::Transient("index");
        let mut now = t0;
        let mut delays = Vec::new();
        for round in 0..8 {
            let Queued::Retry { next_at } = list.failed(i.clone(), transient, now) else {
                panic!("expected a retry");
            };
            delays.push((next_at - now).num_seconds());
            assert_eq!(list.len(), 1);
            if round == 0 {
                assert!(list.due(t0 + chrono::Duration::seconds(4)).is_empty());
                assert_eq!(list.due(t0 + chrono::Duration::seconds(5)), vec![i.clone()]);
            }
            now = next_at;
        }
        assert_eq!(delays, vec![5, 10, 20, 40, 80, 160, 300, 300]);

        let horizon = chrono::Duration::hours(24);
        let near = t0 + horizon - chrono::Duration::milliseconds(1);
        assert!(matches!(
            list.failed(i.clone(), transient, near),
            Queued::Retry { .. }
        ));
        assert_eq!(
            list.failed(i.clone(), transient, t0 + horizon),
            Queued::Expired
        );
        assert_eq!(list.len(), 0);

        let j = intent("j", t0);
        let bug = Failure::Invariant(Invariant::UnreadableKeys);
        let five = t0 + chrono::Duration::seconds(5);
        assert_eq!(
            list.failed(j.clone(), bug, t0),
            Queued::Retry { next_at: five }
        );
        assert_eq!(
            list.failed(j.clone(), bug, t0),
            Queued::Retry { next_at: five }
        );
        assert_eq!(list.failed(j.clone(), bug, t0), Queued::GaveUp);
        assert_eq!(list.len(), 0);

        let mut small = RetryList::new(2);
        let (a, b, c) = (intent("a", t0), intent("b", t0), intent("c", t0));
        assert!(matches!(
            small.failed(a.clone(), transient, t0),
            Queued::Retry { .. }
        ));
        assert!(matches!(
            small.failed(b.clone(), transient, t0),
            Queued::Retry { .. }
        ));
        assert_eq!(small.failed(c, transient, t0), Queued::Overflow);
        assert_eq!(small.len(), 2);
        assert!(matches!(
            small.failed(a.clone(), transient, t0),
            Queued::Retry { .. }
        ));
        small.succeeded(&a);
        assert_eq!(small.len(), 1);
        small.succeeded(&intent("unknown", t0));
        assert_eq!(small.len(), 1);
    }

    /// P4: at most 32 statements in flight, the startup scan at most 8 of them.
    #[tokio::test]
    async fn the_load_limit_caps_statements_at_32_and_the_startup_scan_at_8() {
        let shared = SweeperShared::default();
        let (now, peak) = (AtomicUsize::new(0), AtomicUsize::new(0));
        let (startup_now, startup_peak) = (AtomicUsize::new(0), AtomicUsize::new(0));
        let tasks = (0..200usize).map(|k| {
            let (shared, now, peak) = (&shared, &now, &peak);
            let (startup_now, startup_peak) = (&startup_now, &startup_peak);
            async move {
                let rank = if k % 2 == 0 {
                    Rank::Startup
                } else {
                    Rank::Normal
                };
                let n = (1 + k % 8).clamp(1, 8);
                let _permits = shared.permits(rank, 1 + k % 8).await;
                let total = now.fetch_add(n, Ordering::SeqCst) + n;
                peak.fetch_max(total, Ordering::SeqCst);
                if rank == Rank::Startup {
                    let total = startup_now.fetch_add(n, Ordering::SeqCst) + n;
                    startup_peak.fetch_max(total, Ordering::SeqCst);
                }
                for _ in 0..3 {
                    tokio::task::yield_now().await;
                }
                now.fetch_sub(n, Ordering::SeqCst);
                if rank == Rank::Startup {
                    startup_now.fetch_sub(n, Ordering::SeqCst);
                }
            }
        });
        futures::future::join_all(tasks).await;
        assert!(peak.load(Ordering::SeqCst) <= MAX_IN_FLIGHT);
        assert!(startup_peak.load(Ordering::SeqCst) <= STARTUP_IN_FLIGHT);

        let held = shared.permits(Rank::Normal, 20).await;
        assert_eq!(shared.in_flight.available_permits(), MAX_IN_FLIGHT - 8);
        drop(held);
        assert_eq!(shared.in_flight.available_permits(), MAX_IN_FLIGHT);
    }

    /// P5: counters subtract, gauges keep the later value, every pair prints.
    #[test]
    fn sweeper_stats_subtract_counters_keep_gauges_and_print_every_pair() {
        let counters = SweeperCounters::default();
        counters.polled();
        counters.polled();
        counters.set_lag_ms(3);
        let a = counters.snapshot();
        counters.polled();
        counters.set_lag_ms(7);
        let b = counters.snapshot();
        let delta = b.since(&a);
        assert_eq!((delta.polls, delta.lag_ms), (1, 7));
        assert_eq!(b.pairs().len(), 21);
        let text = b.to_string();
        assert!(text.starts_with("polls="), "{text}");
        assert_eq!(text.matches('=').count(), 21, "{text}");
    }

    /// P6: SW28's grace rule.
    #[test]
    fn a_short_grace_is_refused_unless_allowed() {
        let error = check_grace(Duration::ZERO, false, 30_000).unwrap_err();
        assert_eq!(
            error,
            SweepError::ShortGrace {
                grace_ms: 0,
                configured_ms: 30_000
            }
        );
        assert!(error.to_string().contains("allow_short_grace"));
        assert!(check_grace(Duration::ZERO, true, 30_000).is_ok());
        assert!(check_grace(Duration::from_secs(30), false, 30_000).is_ok());
        assert!(check_grace(Duration::from_millis(29_999), false, 30_000).is_err());
    }

    /// P7: what each scope covers.
    #[test]
    fn scopes_cover_the_tenants_they_name() {
        assert!(SweepScope::All.covers("x"));
        let one = SweepScope::Tenants(vec!["a".into()]);
        assert!(one.covers("a"));
        assert!(!one.covers("ab"));
        let prefix = SweepScope::TenantPrefix("test-".into());
        assert!(prefix.covers("test-x"));
        assert!(!prefix.covers("tes"));
    }

    /// P8: the unknowns the node did not answer.
    #[test]
    fn client_side_unknowns_are_the_deadline_and_the_connection() {
        let stats = CasStats {
            unknown_deadline: 2,
            unknown_connection: 3,
            unknown_write_timeout_cas: 7,
            ..Default::default()
        };
        assert_eq!(stats.client_side_unknowns(), 5);
    }

    /// P9: RW8, a replay takes the stored text.
    #[test]
    fn a_replay_takes_the_stored_fhir_version_text() {
        let version = crate::backends::cassandra::test_support::test_fhir_version();
        let row = |stored: Option<&str>| ResourceRow {
            version_id: 1,
            data: None,
            last_updated: at("2026-11-22T10:00:00Z"),
            created_at: None,
            deleted_at: None,
            fhir_version: version,
            stored_fhir_version: stored.map(str::to_string),
            write_ts: None,
            write_id: None,
            index_keys: None,
        };
        assert_eq!(row(Some("R4")).fhir_version_text(), "R4");
        assert_eq!(row(None).fhir_version_text(), version.as_mime_param());
    }

    /// The loop's future goes to `tokio::spawn` in `hfs`, so it must be `Send`.
    #[allow(dead_code)]
    fn run_sweeper_is_send(
        backend: Arc<CassandraBackend>,
        cancel: CancellationToken,
    ) -> impl std::future::Future<Output = ()> + Send {
        backend.run_sweeper(cancel)
    }
}
