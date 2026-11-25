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
//! reads it again at `LOCAL_SERIAL` (SW12: the serial read completes a
//! proposal the replicas already accepted, but one still running at its
//! coordinator can apply after it, so the answer is not final: the grace is
//! what protects a request in flight, SW28). The version the row shows
//! decides the case:
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
//! marks' start are not forgotten after a restart. The marks start at
//! `start - grace`, so a startup slice whose read failed, or whose intents
//! overflowed the retry list, is never reached by a poll: it goes on a list of
//! pending slices, and each poll reads the due ones again with the retry
//! list's backoff, dropping a slice once it is wholly older than 24 hours,
//! when the outbox's TTL has removed its rows.
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
//! Every version and index write the sweeper makes is at the version's own
//! timestamp, `write_ts_of(last_updated)`, with the cells the version already
//! has, so two sweepers, or a sweeper and a request, on one intent end in the
//! same state. The one exception is I2, the delete of the intent row, which is
//! a delete at the server's time: an intent counts as finished only once I2 is
//! confirmed, and one that was not stays and is tried again. There is no lease
//! and no coordination.

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
use super::read::{ResourceRow, SELECT_RESOURCE};
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
/// SW18: the delay after the `failures`-th failure, `min(RETRY_FIRST x 2^(k-1), RETRY_MAX)`.
fn backoff(failures: u32) -> chrono::Duration {
    let doublings = failures.saturating_sub(1).min(16);
    (retry_first() * 2i32.pow(doublings)).min(retry_max())
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
    /// A `resources` or `resource_versions` row whose columns do not decode.
    UnreadableRow,
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
            Self::UnreadableRow => "unreadable_row",
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
    /// Keys of intents that hit an invariant three times, with each intent's `enqueued`: a later read
    /// of the same row does not try it again until it passes the retry horizon (SW13).
    gave_up: BTreeMap<RetryKey, DateTime<Utc>>,
    gave_up_order: std::collections::VecDeque<RetryKey>,
}

/// Most keys of intents that gave up the retry list remembers; the oldest is forgotten first.
const GAVE_UP_CAP: usize = 10_000;

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
            gave_up: BTreeMap::new(),
            gave_up_order: std::collections::VecDeque::new(),
        }
    }

    /// Whether the list already owns `intent`'s row: it is waiting for its `next_at`, or it gave up and
    /// is still inside the retry horizon. A slice read does not try such an intent again.
    /// Tries of intents that hit an invariant while the list was full, so the third still gives up
    /// (SW13): each key with the order it was first counted in, and its count. At most `GAVE_UP_CAP`.
    overflow_tries: BTreeMap<RetryKey, (u64, u32)>,
    overflow_seen: u64,
    pub(crate) fn owns(&mut self, intent: &Intent, now: DateTime<Utc>) -> bool {
        let key = retry_key(intent);
        if self.entries.contains_key(&key) {
            return true;
        }
        match self.gave_up.get(&key) {
            Some(enqueued) if now < *enqueued + retry_horizon() => true,
            Some(_) => {
                self.gave_up.remove(&key);
                false
            }
            None => false,
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
            overflow_tries: BTreeMap::new(),
            overflow_seen: 0,
        let entry = self.entries.entry(key.clone()).or_insert(Entry {
            intent,
            failures: 0,
            invariant_failures: 0,
            next_at: now,
        });
        match failure {
            Failure::Transient(_) => {
                entry.failures += 1;
                entry.next_at = now + backoff(entry.failures);
            }
            Failure::Invariant(_) => {
                entry.invariant_failures += 1;
                if entry.invariant_failures >= INVARIANT_TRIES {
                    let enqueued = entry.intent.enqueued;
                    self.entries.remove(&key);
                    self.give_up(key, enqueued);
                    return Queued::GaveUp;
                }
                entry.next_at = now + retry_first();
            }
        }
        Queued::Retry {
            next_at: entry.next_at,
            // A bug is tried three times however full the list is (SW13); a transient failure holds the mark (SW18).
            if matches!(failure, Failure::Invariant(_)) {
                let seen = self.overflow_seen;
                let tries = {
                    let (_, tries) = self.overflow_tries.entry(key.clone()).or_insert((seen, 0));
                    *tries += 1;
                    *tries
                };
                self.overflow_seen += 1;
                if tries >= INVARIANT_TRIES {
                    let enqueued = intent.enqueued;
                    self.overflow_tries.remove(&key);
                    self.give_up(key, enqueued);
                    return Queued::GaveUp;
                }
                while self.overflow_tries.len() > GAVE_UP_CAP {
                    let oldest = self
                        .overflow_tries
                        .iter()
                        .min_by_key(|(_, (order, _))| *order)
                        .map(|(key, _)| key.clone());
                    match oldest {
                        Some(oldest) => {
                            self.overflow_tries.remove(&oldest);
                        }
                        None => break,
                    }
                }
            }
        }
    }

    /// Removes the entry of `intent`'s row, if any.
    pub(crate) fn succeeded(&mut self, intent: &Intent) {
        let key = retry_key(intent);
        self.entries.remove(&key);
        self.overflow_tries.remove(&key);
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
    pending: parking_lot::Mutex<Vec<PendingSlice>>,
    cancel: CancellationToken,
}

impl SweepState {
    /// Every mark at `mark` (truncated to whole ms); an empty list of `RETRY_LIST_CAP`.
    pub(crate) fn new(mark: DateTime<Utc>, cancel: CancellationToken) -> Self {
        Self {
            marks: parking_lot::Mutex::new([whole_ms(mark); 16]),
            retry: parking_lot::Mutex::new(RetryList::new(RETRY_LIST_CAP)),
            pending: parking_lot::Mutex::new(Vec::new()),
    /// Remembers that `key`'s intent gave up, so a later read of its row does not try it again (SW13).
    fn give_up(&mut self, key: RetryKey, enqueued: DateTime<Utc>) {
        if self.gave_up.insert(key.clone(), enqueued).is_none() {
            self.gave_up_order.push_back(key);
        }
        while self.gave_up.len() > GAVE_UP_CAP {
            match self.gave_up_order.pop_front() {
                Some(oldest) => {
                    self.gave_up.remove(&oldest);
                }
                None => break,
            }
        }
    }

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

    /// Keeps a startup slice to be read again after its `failures`-th failed or overflowed read.
    fn hold_slice(&self, slice: Slice, failures: u32, now: DateTime<Utc>) {
        self.pending.lock().push(PendingSlice {
            slice,
            failures,
            next_at: now + backoff(failures),
        });
    }

    /// Takes the pending slices due at `now`; those wholly older than the retry horizon are dropped.
    fn take_due_slices(&self, now: DateTime<Utc>) -> Vec<PendingSlice> {
        let mut pending = self.pending.lock();
        let (due, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut *pending)
            .into_iter()
            .partition(|held| held.next_at <= now);
        *pending = kept;
        due.into_iter()
            .filter(|held| {
                let expired = held.slice.to <= now - retry_horizon();
                if expired {
                    tracing::error!(
                        bucket = %held.slice.bucket,
                        shard = held.slice.shard,
                        "an outbox slice was not read within 24 hours; its rows are past their TTL"
                    );
                }
                !expired
            })
            .collect()
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

/// A startup slice that must be read again (see the module doc).
struct PendingSlice {
    slice: Slice,
    failures: u32,
    next_at: DateTime<Utc>,
}

/// What one slice did.
struct SliceOutcome {
    slice: Slice,
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
                slice_errors = report.slice_errors,
                overflow = report.overflow,
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
                // Nothing older than the outbox's TTL is still there to read.
                let from = state.mark(shard).max(to - retry_horizon());
                for slice in plan_slices(shard, from, to, poll_slice()) {
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

        let pending = state.take_due_slices(now);
        if !pending.is_empty() {
            let reads: Vec<_> = pending
                .iter()
                .map(|held| self.handle_slice(&held.slice, Rank::Normal, scope, Some(state), now))
                .collect();
            let mut outcomes = futures::stream::iter(reads).buffer_unordered(SLICES_AT_ONCE);
            let mut redo = Vec::new();
            while let Some(outcome) = outcomes.next().await {
                report.absorb(&outcome.report);
                if !outcome.read_ok || outcome.overflow {
                    redo.push(outcome.slice);
                }
            }
            drop(outcomes);
            for slice in redo {
                let failures = pending
                    .iter()
                    .find(|held| held.slice == slice)
                    .map_or(1, |held| held.failures + 1);
                state.hold_slice(slice, failures, now);
            }
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
                .map(|slice| self.handle_slice(slice, rank, scope, retry, Utc::now()))
                .collect();
            let mut outcomes = futures::stream::iter(reads).buffer_unordered(SLICES_AT_ONCE);
            while let Some(outcome) = outcomes.next().await {
                report.absorb(&outcome.report);
                // A poll never reaches a startup slice (the marks start at `start - grace`).
                if let Some(state) = retry
                    && (!outcome.read_ok || outcome.overflow)
                {
                    state.hold_slice(outcome.slice, 1, Utc::now());
                }
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
            slice: slice.clone(),
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
        let mut intents: Vec<Intent> = intents
            .into_iter()
            .filter(|intent| scope.covers(&intent.tenant_id))
            .collect();
        // The retry list owns an intent it holds, or gave up on: its backoff, not this read, decides.
        if let Some(state) = state {
            let mut retry = state.retry.lock();
            intents.retain(|intent| !retry.owns(intent, now));
        }
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
        let mut row = match self.read_row(intent, StatementKind::Read, rank).await {
            Ok(row) => row,
            Err((failure, detail)) => return self.stopped(intent, failure, &detail),
        };
        if needs_serial_read(version, row.as_ref().map(|row| row.version_id)) {
            row = match self.read_row(intent, StatementKind::SerialRead, rank).await {
                Ok(row) => row,
                Err((failure, detail)) => return self.stopped(intent, failure, &detail),
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
        let deleted = {
            let _permit = self.sweeper.permits(rank, 1).await;
            self.try_delete_intent(&intent.key, &key).await
        };
        if let Err(error) = deleted {
            // I2 is a delete at server time; until it is confirmed the intent is not finished.
            tracing::debug!(
                tenant_id = %intent.tenant_id,
                resource_type = %intent.resource_type,
                id = %intent.id,
                version_id = version,
                error = ?error,
                "an intent's row could not be deleted; it is tried again"
            );
            return IntentResult {
                case: Some(case),
                result: Err(Failure::Transient("intent delete")),
                index: replay.index,
                noop: replay.noop,
            };
        }
        self.sweeper.counters.finished(replay.noop);
        IntentResult {
            case: Some(case),
            result: Ok(()),
            index: replay.index,
            noop: replay.noop,
        }
    }

    /// R1 (`Read`) or R2 (`SerialRead`) for an intent's resource. A driver error is transient; a row that
    /// does not decode is a bug (SW13). The error comes back with its text.
    async fn read_row(
        &self,
        intent: &Intent,
        kind: StatementKind,
        rank: Rank,
    ) -> Result<Option<ResourceRow>, (Failure, String)> {
        let step = if kind == StatementKind::SerialRead {
            "serial read"
        } else {
            "read"
        };
        let rows = {
            let _permit = self.sweeper.permits(rank, 1).await;
            self.session
                .rows(
                    SELECT_RESOURCE,
                    vec![
                        Value::from(intent.tenant_id.as_str()),
                        Value::from(intent.resource_type.as_str()),
                        Value::from(intent.id.as_str()),
                    ],
                    kind,
                )
                .await
                .map_err(|error| (Failure::Transient(step), format!("{error:?}")))?
        };
        rows.first()
            .map(ResourceRow::decode)
            .transpose()
            .map_err(|error| {
                (
                    Failure::Invariant(Invariant::UnreadableRow),
                    format!("{error:?}"),
                )
            })
    }

    /// An intent that stopped before a case was known.
    fn stopped(&self, intent: &Intent, failure: Failure, detail: &str) -> IntentResult {
        if let Failure::Invariant(invariant) = failure {
            self.sweeper.counters.invariant();
            tracing::error!(
                tenant_id = %intent.tenant_id,
                resource_type = %intent.resource_type,
                id = %intent.id,
                version_id = intent.version_id,
                invariant = invariant.as_str(),
                error = detail,
                "an intent's row does not decode; its row is kept in the outbox"
            );
        } else {
            tracing::debug!(
                tenant_id = %intent.tenant_id,
                resource_type = %intent.resource_type,
                id = %intent.id,
                version_id = intent.version_id,
                error = detail,
                "an intent's row could not be read"
            );
        }
        IntentResult {
            case: None,
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
            .map_err(|_| Failure::Invariant(Invariant::UnreadableRow))?;
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
    /// version V replaced, that is `K(V-1)` minus `K(V)` by row identity; never an insert, and no version row.
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
        // K(V-1) minus K(V), by row identity: a row version V also has (a date row both versions
        // share) was written at `ts(V)` and a delete at `ts(V)` would win the tie (IW7).
        let kept = match stored.index_keys.as_deref().map(index_keys::decode) {
            Some(Ok(keys)) => keys,
            _ => return Replay::failed(Failure::Invariant(Invariant::UnreadableKeys)),
        };
        let mut delta = index_delta(
            &previous_rows,
            VersionKeys {
                version_id: version,
                last_updated: stored.last_updated,
                fhir_version: stored
                    .fhir_version
                    .as_deref()
                    .unwrap_or_else(|| row.fhir_version_text()),
                keys: &kept,
            },
        );
        // Never an insert: version V's own rows are its request's or its replay's to write.
        for unit in &mut delta.units {
            unit.inserts.clear();
        }
        delta.units.retain(|unit| !unit.deletes.is_empty());
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


        // SW13 holds with the list full: a bug is tried three times, a transient failure holds the mark.
        let mut full = RetryList::new(1);
        let (a, b) = (intent("a", t0), intent("b", t0));
        assert!(matches!(
            full.failed(a, transient, t0),
            Queued::Retry { .. }
        ));
        assert_eq!(full.failed(b.clone(), bug, t0), Queued::Overflow);
        assert_eq!(full.failed(b.clone(), bug, t0), Queued::Overflow);
        assert_eq!(full.failed(b.clone(), bug, t0), Queued::GaveUp);
        assert!(full.owns(&b, t0));
        assert_eq!(full.len(), 1);
        let c = intent("c", t0);
        assert_eq!(full.failed(c.clone(), transient, t0), Queued::Overflow);
        assert_eq!(full.failed(c.clone(), transient, t0), Queued::Overflow);
        assert_eq!(full.failed(c.clone(), transient, t0), Queued::Overflow);
        assert!(!full.owns(&c, t0));
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

#[cfg(test)]
mod node_tests {
    use std::collections::HashSet;
    use std::future::Future;

    use super::*;
    use crate::backends::cassandra::checkpoint::Checkpoint;
    use crate::backends::cassandra::consistency::harness::{
        self, changed, check, context, patient, stored_keys, tenant_id,
    };
    use crate::backends::cassandra::consistency::{assert_consistent, version_ids};
    use crate::backends::cassandra::session::Fault;
    use crate::backends::cassandra::test_support::test_fhir_version;
    use crate::core::{ResourceStorage, SearchProvider};
    use crate::error::StorageResult;
    use crate::tenant::TenantContext;

    const SKIP: &str = "skipping: HFS_TEST_CASSANDRA_URL is not set";

    /// A backend, a fresh tenant, and `since`, taken before the first write.
    async fn start(
        prefix: &str,
    ) -> Option<(Arc<CassandraBackend>, String, TenantContext, DateTime<Utc>)> {
        let Some(backend) = harness::backend().await else {
            eprintln!("{SKIP}");
            return None;
        };
        let t = tenant_id(prefix);
        let tenant = context(&t);
        Some((
            backend,
            t,
            tenant,
            Utc::now() - chrono::Duration::seconds(5),
        ))
    }

    /// RV15/RV16: the request reaches `point` for `(t, id)` at `version` and is aborted there.
    async fn crash<T: Send + 'static>(
        backend: &CassandraBackend,
        point: Checkpoint,
        t: &str,
        id: &str,
        version: i32,
        write: impl Future<Output = StorageResult<T>> + Send + 'static,
    ) {
        let mut hold = backend.hold_at(point, t, id, version);
        let task = tokio::spawn(write);
        hold.reached().await;
        task.abort();
        match task.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("the aborted request finished"),
        }
    }

    async fn pass(
        backend: &CassandraBackend,
        tenants: &[&str],
        since: DateTime<Utc>,
    ) -> SweepReport {
        backend
            .sweep_pass(SweepOptions {
                scope: SweepScope::Tenants(tenants.iter().map(|t| t.to_string()).collect()),
                grace: Duration::ZERO,
                now: Utc::now(),
                since,
                allow_short_grace: true,
            })
            .await
            .unwrap()
    }

    async fn pending(
        backend: &CassandraBackend,
        t: &str,
        id: &str,
        since: DateTime<Utc>,
    ) -> Vec<Intent> {
        let shard = IntentKey::new(t, id, since).shard();
        let to = Utc::now() + chrono::Duration::milliseconds(1);
        let mut found = Vec::new();
        for slice in plan_slices(shard, since, to, startup_slice()) {
            let (intents, _) = backend.read_slice(&slice, Rank::Normal).await.unwrap();
            found.extend(
                intents
                    .into_iter()
                    .filter(|intent| intent.tenant_id == t && intent.id == id),
            );
        }
        found
    }

    async fn intents(
        backend: &CassandraBackend,
        t: &str,
        id: &str,
        since: DateTime<Utc>,
    ) -> Vec<i32> {
        backend.intent_versions(t, "Patient", id, since).await
    }

    async fn rbt_rows(backend: &CassandraBackend, t: &str, id: &str) -> Vec<(DateTime<Utc>, i32)> {
        let rows = backend
            .session
            .rows(
                "SELECT last_updated, id, version_id FROM resources_by_type \
                 WHERE tenant_id = ? AND resource_type = ?",
                vec![Value::from(t), Value::from("Patient")],
                StatementKind::Read,
            )
            .await
            .expect("resources_by_type");
        let mut found: Vec<(DateTime<Utc>, i32)> = rows
            .iter()
            .filter(|row| {
                let found: String = row.get_r_by_name("id").unwrap();
                found == id
            })
            .map(|row| {
                (
                    row.get_r_by_name("last_updated").unwrap(),
                    row.get_r_by_name("version_id").unwrap(),
                )
            })
            .collect();
        found.sort_by(|a, b| b.cmp(a));
        found
    }

    async fn row_of(backend: &CassandraBackend, t: &str, id: &str) -> ResourceRow {
        backend
            .read_resource_row(t, "Patient", id)
            .await
            .expect("R1")
            .expect("a row")
    }

    fn keys(row: &ResourceRow) -> Vec<IndexKey> {
        index_keys::decode(row.index_keys.as_deref().expect("index_keys")).expect("K")
    }

    fn not_produced(
        old: &[IndexKey],
        old_lu: DateTime<Utc>,
        new: &[IndexKey],
        new_lu: DateTime<Utc>,
    ) -> usize {
        let produced: HashSet<_> = new.iter().map(|key| key.row_at(new_lu)).collect();
        old.iter()
            .filter(|key| !produced.contains(&key.row_at(old_lu)))
            .count()
    }

    async fn count(backend: &CassandraBackend, tenant: &TenantContext) -> u64 {
        backend.count(tenant, Some("Patient")).await.unwrap()
    }

    fn say(name: &str, backend: &CassandraBackend, before: &SweeperStats) {
        eprintln!(
            "[sweeper case={name} {}]",
            backend.sweeper_stats().since(before)
        );
    }

    /// One version row's columns, for N3, N9 and N10.
    async fn version_row_of(
        backend: &CassandraBackend,
        t: &str,
        version: i32,
    ) -> (
        Option<String>,
        Option<Uuid>,
        Option<Vec<u8>>,
        Option<String>,
        Option<DateTime<Utc>>,
    ) {
        let rows = backend
            .session
            .rows(
                "SELECT data, write_id, index_keys, fhir_version, deleted_at \
                 FROM resource_versions WHERE tenant_id = ? AND resource_type = ? \
                 AND id = ? AND version_id = ?",
                vec![
                    Value::from(t),
                    Value::from("Patient"),
                    Value::from("p"),
                    Value::from(version),
                ],
                StatementKind::Read,
            )
            .await
            .expect("version row");
        let row = rows.first().expect("the version row exists");
        (
            row.get_by_name("data").unwrap(),
            row.get_by_name("write_id").unwrap(),
            IntoRustByName::<Blob>::get_by_name(row, "index_keys")
                .unwrap()
                .map(Blob::into_vec),
            row.get_by_name("fhir_version").unwrap(),
            row.get_by_name("deleted_at").unwrap(),
        )
    }

    /// N1 (gate): a slice is half open, so an intent at the edge is in exactly one.
    #[tokio::test]
    async fn slice_edges_are_half_open() {
        let Some((backend, t, _, _)) = start("sweeper-edges").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let edge = whole_ms(Utc::now())
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap()
            - chrono::Duration::minutes(10);
        use chrono::Timelike;
        let mut ids = Vec::new();
        let mut want = None;
        let mut n = 0;
        while ids.len() < 3 {
            let id = format!("e{n}");
            n += 1;
            let shard = IntentKey::new(&t, &id, edge).shard();
            if *want.get_or_insert(shard) == shard {
                ids.push(id);
            }
        }
        let shard = want.unwrap();
        let ms = chrono::Duration::milliseconds(1);
        for (id, at) in ids.iter().zip([edge - ms, edge, edge + ms]) {
            let key = RowKey {
                tenant_id: &t,
                resource_type: "Patient",
                id,
            };
            backend
                .write_intent(&IntentKey::new(&t, id, at), &key, Uuid::new_v4(), 1)
                .await
                .unwrap();
        }
        let minute = chrono::Duration::minutes(1);
        let slices = plan_slices(shard, edge - minute, edge + minute, startup_slice());
        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0].to, edge);
        assert_eq!(slices[1].from, edge);
        let mut found = Vec::new();
        for slice in &slices {
            let (read, _) = backend.read_slice(slice, Rank::Normal).await.unwrap();
            found.push(
                read.into_iter()
                    .filter(|intent| intent.tenant_id == t)
                    .map(|intent| intent.id)
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(found[0], vec![ids[0].clone()]);
        let mut second = found[1].clone();
        second.sort();
        assert_eq!(second, vec![ids[1].clone(), ids[2].clone()]);

        let report = backend
            .sweep_pass(SweepOptions {
                scope: SweepScope::Tenants(vec![t.clone()]),
                grace: Duration::ZERO,
                now: edge + chrono::Duration::minutes(2),
                since: edge - chrono::Duration::minutes(2),
                allow_short_grace: true,
            })
            .await
            .unwrap();
        assert_eq!((report.no_row, report.done), (3, 3), "{report}");
        for slice in &slices {
            let (read, _) = backend.read_slice(slice, Rank::Normal).await.unwrap();
            assert!(read.iter().all(|intent| intent.tenant_id != t));
        }
        say("edges", &backend, &before);
    }

    /// N2: a CAS that never ran is deleted after a serial read, and writes nothing else.
    #[tokio::test]
    async fn an_intent_whose_cas_never_ran_is_deleted_after_a_serial_read() {
        let Some((backend, t, tenant, since)) = start("sweeper-never-applied").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let version = test_fhir_version();
        let p = backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .unwrap();
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterIntent, &t, "p", 2, async move {
            b.update(&ten, &p, changed()).await
        })
        .await;
        let found = pending(&backend, &t, "p", since).await;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].version_id, 2);
        let at = backend.statement_counts();
        let result = backend.process_intent(&found[0], Rank::Normal).await;
        let spent = backend.statement_counts().since(&at);
        assert_eq!(result.case, Some(IntentCase::NeverApplied));
        assert_eq!(result.result, Ok(()));
        assert_eq!(result.index, IndexWriteStats::default());
        assert_eq!(
            (
                spent.read,
                spent.serial_read,
                spent.write,
                spent.batch,
                spent.cas
            ),
            (1, 1, 1, 0, 0),
            "{spent}"
        );
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_eq!(row_of(&backend, &t, "p").await.version_id, 1);
        assert_eq!(version_ids(&backend, &t, "Patient", "p").await, vec![1]);
        assert_consistent(&check(&backend, &t).await);
        say("never-applied", &backend, &before);
    }

    /// N3: a crash after the CAS is replayed as the current version.
    #[tokio::test]
    async fn a_crash_after_the_cas_is_replayed_as_the_current_version() {
        let Some((backend, t, tenant, since)) = start("sweeper-current").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let version = test_fhir_version();
        let p = backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .unwrap();
        let r1 = row_of(&backend, &t, "p").await;
        let k1 = stored_keys(&backend, &t, "p").await;
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 2, async move {
            b.update(&ten, &p, changed()).await
        })
        .await;
        let r2 = row_of(&backend, &t, "p").await;
        let k2 = keys(&r2);
        let found = pending(&backend, &t, "p", since).await;
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].version_id, 2);
        let at = backend.statement_counts();
        let result = backend.process_intent(&found[0], Rank::Normal).await;
        let spent = backend.statement_counts().since(&at);
        assert_eq!(result.case, Some(IntentCase::Current));
        assert_eq!(result.result, Ok(()));
        assert_eq!(result.index.rows_inserted, k2.len() as u64);
        assert_eq!(
            result.index.rows_deleted,
            not_produced(&k1, r1.last_updated, &k2, r2.last_updated) as u64
        );
        assert_eq!(
            (spent.read, spent.serial_read, spent.cas),
            (2, 0, 0),
            "{spent}"
        );
        assert_eq!(
            spent.write + spent.batch,
            result.index.batches + 2,
            "{spent}"
        );
        let (data, write_id, blob, fhir, _) = version_row_of(&backend, &t, 2).await;
        assert_eq!(data, r2.data);
        assert_eq!(write_id, r2.write_id);
        assert_eq!(blob, r2.index_keys);
        assert_eq!(fhir, r2.stored_fhir_version);
        assert_eq!(version_ids(&backend, &t, "Patient", "p").await, vec![1, 2]);
        assert_eq!(
            rbt_rows(&backend, &t, "p").await,
            vec![(r2.last_updated, 2)]
        );
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
        say("current", &backend, &before);
    }

    /// A Patient born on `date`, so its version has a date row as well as the token rows.
    fn born(date: &str) -> serde_json::Value {
        serde_json::json!({
            "resourceType": "Patient",
            "id": "p",
            "active": true,
            "birthDate": date
        })
    }

    /// The `date_index` rows of `Patient/p`'s `birthdate`: `(start, version_id)`.
    async fn birth_date_rows(backend: &CassandraBackend, t: &str) -> Vec<(DateTime<Utc>, i32)> {
        backend
            .session
            .rows(
                "SELECT start, version_id FROM date_index WHERE tenant_id = ? \
                 AND resource_type = ? AND param = ?",
                vec![
                    Value::from(t),
                    Value::from("Patient"),
                    Value::from("birthdate"),
                ],
                StatementKind::Read,
            )
            .await
            .expect("date_index")
            .iter()
            .map(|row| {
                (
                    row.get_r_by_name("start").unwrap(),
                    row.get_r_by_name("version_id").unwrap(),
                )
            })
            .collect()
    }

    /// N4: a superseded intent deletes only the rows of the version before it.
    #[tokio::test]
    async fn a_superseded_intent_deletes_only_the_rows_of_the_version_before_it() {
        let Some((backend, t, tenant, since)) = start("sweeper-superseded").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let version = test_fhir_version();
        let p = backend
            .create(&tenant, "Patient", born("1970-01-01"), version)
            .await
            .unwrap();
        let k1 = keys(&row_of(&backend, &t, "p").await);
        let lu1 = row_of(&backend, &t, "p").await.last_updated;
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 2, async move {
            b.update(&ten, &p, born("1980-02-02")).await
        })
        .await;
        let v2 = backend
            .read(&tenant, "Patient", "p")
            .await
            .unwrap()
            .unwrap();
        backend
            .update(&tenant, &v2, born("1970-01-01"))
            .await
            .unwrap();
        let lu3 = row_of(&backend, &t, "p").await.last_updated;
        assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu3, 3), (lu1, 1)]);
        assert_eq!(count(&backend, &tenant).await, 2);
        assert_eq!(
            version_ids(&backend, &t, "Patient", "p").await,
            vec![1, 2, 3]
        );
        let found = pending(&backend, &t, "p", since).await;
        assert_eq!(
            found.iter().map(|i| i.version_id).collect::<Vec<_>>(),
            vec![2]
        );
        let at = backend.statement_counts();
        let result = backend.process_intent(&found[0], Rank::Normal).await;
        let spent = backend.statement_counts().since(&at);
        assert_eq!(result.case, Some(IntentCase::Superseded));
        assert_eq!(result.result, Ok(()));
        assert!(!result.noop);
        assert_eq!(result.index.rows_inserted, 0);
        assert_eq!(result.index.rows_deleted, k1.len() as u64);
        assert_eq!(
            (spent.read, spent.serial_read, spent.cas),
            (3, 0, 0),
            "{spent}"
        );
        assert_eq!(
            spent.write + spent.batch,
            result.index.batches + 1,
            "{spent}"
        );
        assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu3, 3)]);
        // A, B, A: the date row of version 1 is not version 2's, and version 3 wrote it again.
        assert_eq!(birth_date_rows(&backend, &t).await.len(), 1);
        assert_eq!(count(&backend, &tenant).await, 1);
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_eq!(
            version_ids(&backend, &t, "Patient", "p").await,
            vec![1, 2, 3]
        );
        assert_consistent(&check(&backend, &t).await);
        say("superseded", &backend, &before);
    }

    /// A superseded replay deletes `K(V-1)` minus `K(V)`: a date row that version V also has stays.
    #[tokio::test]
    async fn a_superseded_replay_keeps_a_date_row_its_version_still_has() {
        let Some((backend, t, tenant, since)) = start("sweeper-superseded-date").await else {
            return;
        };
        let version = test_fhir_version();
        let p = backend
            .create(&tenant, "Patient", born("1970-01-01"), version)
            .await
            .unwrap();
        // Version 2 has the same date; its rows are written and its I2 is not.
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterIndex, &t, "p", 2, async move {
            b.update(&ten, &p, born("1970-01-01")).await
        })
        .await;
        // Version 3 has it too; its CAS applied and nothing else.
        let v2 = backend
            .read(&tenant, "Patient", "p")
            .await
            .unwrap()
            .unwrap();
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 3, async move {
            b.update(&ten, &v2, born("1970-01-01")).await
        })
        .await;
        let found = pending(&backend, &t, "p", since).await;
        assert_eq!(
            found.iter().map(|i| i.version_id).collect::<Vec<_>>(),
            vec![2, 3]
        );
        let first = backend.process_intent(&found[0], Rank::Normal).await;
        assert_eq!(first.case, Some(IntentCase::Superseded));
        assert_eq!(first.result, Ok(()));
        let dates = birth_date_rows(&backend, &t).await;
        assert_eq!(dates.len(), 1, "the shared date row is kept: {dates:?}");
        let second = backend.process_intent(&found[1], Rank::Normal).await;
        assert_eq!(second.case, Some(IntentCase::Current));
        assert_eq!(second.result, Ok(()));
        assert_eq!(birth_date_rows(&backend, &t).await.len(), 1);
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
    }

    /// N5: the IW24 skip, a later update, and the sweep that settles `count`.
    #[tokio::test]
    async fn the_iw24_skip_and_a_later_update_count_twice_until_the_sweep() {
        let Some((backend, t, tenant, since)) = start("sweeper-iw24").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let version = test_fhir_version();
        let p = backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .unwrap();
        let lu1 = row_of(&backend, &t, "p").await.last_updated;
        backend.session.inject_fault(
            StatementKind::Write,
            "INSERT INTO resource_versions",
            Fault::PassThrough,
        );
        for _ in 0..3 {
            backend.session.inject_fault(
                StatementKind::Write,
                "INSERT INTO resource_versions",
                Fault::TimeOutWithoutApplying,
            );
        }
        let cas = backend.cas_stats();
        let v2 = backend.update(&tenant, &p, changed()).await.unwrap();
        let stats = backend.cas_stats().since(&cas);
        assert_eq!(v2.version_id(), "2");
        assert_eq!((stats.applied, stats.post_cas_failures), (1, 1));
        assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu1, 1)]);
        assert_eq!(count(&backend, &tenant).await, 1);
        assert_eq!(version_ids(&backend, &t, "Patient", "p").await, vec![1]);
        assert_eq!(intents(&backend, &t, "p", since).await, vec![2]);

        let v3 = backend.update(&tenant, &v2, patient()).await.unwrap();
        assert_eq!(v3.version_id(), "3");
        let lu3 = row_of(&backend, &t, "p").await.last_updated;
        assert_eq!(
            version_ids(&backend, &t, "Patient", "p").await,
            vec![1, 2, 3]
        );
        assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu3, 3), (lu1, 1)]);
        assert_eq!(count(&backend, &tenant).await, 2);
        assert_eq!(intents(&backend, &t, "p", since).await, vec![2]);

        let report = pass(&backend, &[&t], since).await;
        assert_eq!((report.superseded, report.done), (1, 1), "{report}");
        assert!(report.is_complete());
        assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu3, 3)]);
        assert_eq!(count(&backend, &tenant).await, 1);
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
        say("iw24", &backend, &before);
    }

    /// N6 (exit criterion): the running loop heals a crash within poll + grace.
    #[tokio::test]
    async fn a_crash_after_the_cas_heals_within_poll_plus_grace() {
        let Some((backend, t, tenant, since)) = start("sweeper-loop").await else {
            return;
        };
        let url = std::env::var("HFS_TEST_CASSANDRA_URL").unwrap();
        let looped = Arc::new(
            CassandraBackend::connect(crate::backends::cassandra::CassandraConfig {
                contact_points: vec![url],
                request_timeout_ms: 2_500,
                sweeper_poll_interval_ms: 1_000,
                sweeper_grace_ms: 5_000,
                ..crate::backends::cassandra::test_support::test_config()
            })
            .await
            .expect("connect"),
        );
        let version = test_fhir_version();
        let p = backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .unwrap();
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 2, async move {
            b.update(&ten, &p, changed()).await
        })
        .await;
        let lu2 = row_of(&backend, &t, "p").await.last_updated;
        let crashed = std::time::Instant::now();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(looped.clone().run_sweeper_with(
            cancel.clone(),
            SweepScope::Tenants(vec![t.clone()]),
            false,
        ));
        let limit = Duration::from_secs(16);
        loop {
            if intents(&backend, &t, "p", since).await.is_empty()
                && rbt_rows(&backend, &t, "p").await == vec![(lu2, 2)]
            {
                break;
            }
            assert!(
                crashed.elapsed() < limit,
                "not healed after {:?}",
                crashed.elapsed()
            );
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        eprintln!(
            "[sweeper case=loop healed_ms={}]",
            crashed.elapsed().as_millis()
        );
        let stats = looped.sweeper_stats();
        assert!(
            stats.polls >= 1 && stats.case_current >= 1 && stats.done >= 1,
            "{stats}"
        );
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("the sweeper stops within 5 s")
            .unwrap();
        assert_eq!(version_ids(&backend, &t, "Patient", "p").await, vec![1, 2]);
        assert_consistent(&check(&backend, &t).await);
    }

    /// N7 (gate): two sweepers on one intent end in the same state (SW22).
    #[tokio::test]
    async fn two_sweepers_on_one_intent_end_in_the_same_state() {
        let Some((backend, t, tenant, since)) = start("sweeper-two").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let version = test_fhir_version();
        let p = backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .unwrap();
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 2, async move {
            b.update(&ten, &p, changed()).await
        })
        .await;
        let lu2 = row_of(&backend, &t, "p").await.last_updated;
        let other = harness::backend().await.expect("a second backend");
        let options = SweepOptions {
            scope: SweepScope::Tenants(vec![t.clone()]),
            grace: Duration::ZERO,
            now: Utc::now(),
            since,
            allow_short_grace: true,
        };
        let (a, b) = tokio::join!(
            backend.sweep_pass(options.clone()),
            other.sweep_pass(options)
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        assert!(
            a.done + b.done >= 1 && a.current + b.current >= 1,
            "{a} {b}"
        );
        assert_eq!(
            a.failed + b.failed + a.invariant + b.invariant,
            0,
            "{a} {b}"
        );
        assert_eq!(version_ids(&backend, &t, "Patient", "p").await, vec![1, 2]);
        assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu2, 2)]);
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
        let third = pass(&backend, &[&t], since).await;
        assert_eq!((third.current, third.done), (0, 0), "{third}");
        say("two-sweepers", &backend, &before);
    }

    fn search_parameter(url: &str) -> serde_json::Value {
        serde_json::json!({
            "resourceType": "SearchParameter",
            "id": "sp",
            "url": url,
            "name": "sp",
            "status": "active",
            "description": "a stored SearchParameter for the sweeper's node tests",
            "code": "sweeper-sp",
            "base": ["Patient"],
            "type": "token",
            "expression": "Patient.active",
        })
    }

    fn expression_of(
        backend: &CassandraBackend,
        tenant: &TenantContext,
        url: &str,
    ) -> Option<String> {
        backend
            .search_param_registry(tenant)
            .read()
            .get_by_url(url)
            .map(|definition| definition.expression.clone())
    }

    /// N8 (gate): a replayed SearchParameter is put back in the overlay and survives a reload (SW19).
    #[tokio::test]
    async fn a_searchparameter_replay_puts_it_back_in_the_overlay_and_the_reload() {
        let Some((backend, t, tenant, since)) = start("sweeper-sp").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let url = format!("http://example.org/SearchParameter/{t}");
        for _ in 0..3 {
            backend.session.inject_fault(
                StatementKind::Write,
                "INSERT INTO resources_by_type",
                Fault::TimeOutWithoutApplying,
            );
        }
        let cas = backend.cas_stats();
        backend
            .create(
                &tenant,
                "SearchParameter",
                search_parameter(&url),
                test_fhir_version(),
            )
            .await
            .expect("create");
        assert_eq!(backend.cas_stats().since(&cas).post_cas_failures, 1);
        assert_eq!(
            expression_of(&backend, &tenant, &url).as_deref(),
            Some("Patient.active")
        );
        assert_eq!(backend.load_stored_search_parameters(&t).await.unwrap(), 0);
        assert_eq!(expression_of(&backend, &tenant, &url), None);

        let report = pass(&backend, &[&t], since).await;
        assert_eq!((report.current, report.done), (1, 1), "{report}");
        let delta = backend.sweeper_stats().since(&before);
        assert_eq!(delta.search_parameters_followed, 1);
        assert_eq!(
            expression_of(&backend, &tenant, &url).as_deref(),
            Some("Patient.active")
        );
        assert_eq!(backend.load_stored_search_parameters(&t).await.unwrap(), 1);
        assert_eq!(
            expression_of(&backend, &tenant, &url).as_deref(),
            Some("Patient.active")
        );
        let fresh = harness::backend().await.expect("a fresh backend");
        assert_eq!(fresh.load_stored_search_parameters(&t).await.unwrap(), 1);
        assert_eq!(
            expression_of(&fresh, &tenant, &url).as_deref(),
            Some("Patient.active")
        );
        say("search-parameter", &backend, &before);
    }

    /// N9: a replayed delete writes its marker, and a revive only inserts (SW16).
    #[tokio::test]
    async fn a_replayed_delete_writes_its_marker_and_a_revive_inserts_only() {
        let Some((backend, t, tenant, since)) = start("sweeper-delete").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let version = test_fhir_version();
        backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .unwrap();
        let k1 = stored_keys(&backend, &t, "p").await;
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 2, async move {
            b.delete(&ten, "Patient", "p").await
        })
        .await;
        let lu2 = row_of(&backend, &t, "p").await.last_updated;
        let found = pending(&backend, &t, "p", since).await;
        assert_eq!(found.len(), 1);
        let result = backend.process_intent(&found[0], Rank::Normal).await;
        assert_eq!(result.case, Some(IntentCase::Current));
        assert_eq!(result.result, Ok(()));
        assert_eq!(result.index.rows_inserted, 0);
        assert_eq!(result.index.rows_deleted, k1.len() as u64);
        let (data, _, blob, _, deleted_at) = version_row_of(&backend, &t, 2).await;
        assert_eq!(data, None);
        assert_eq!(deleted_at, Some(lu2));
        assert_eq!(blob.as_deref(), Some(&index_keys::EMPTY[..]));
        assert!(rbt_rows(&backend, &t, "p").await.is_empty());
        assert_eq!(count(&backend, &tenant).await, 0);

        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 3, async move {
            b.create_or_update(&ten, "Patient", "p", patient(), version)
                .await
        })
        .await;
        let r3 = row_of(&backend, &t, "p").await;
        let k3 = keys(&r3);
        let found = pending(&backend, &t, "p", since).await;
        assert_eq!(found.len(), 1);
        let result = backend.process_intent(&found[0], Rank::Normal).await;
        assert_eq!(result.case, Some(IntentCase::Current));
        assert_eq!(result.result, Ok(()));
        assert_eq!(result.index.rows_deleted, 0);
        assert_eq!(result.index.rows_inserted, k3.len() as u64);
        assert_eq!(
            rbt_rows(&backend, &t, "p").await,
            vec![(r3.last_updated, 3)]
        );
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
        say("delete-revive", &backend, &before);
    }

    /// N10: a replay writes the stored `fhir_version` text back verbatim (RW8).
    #[tokio::test]
    async fn a_replay_writes_the_stored_fhir_version_text_back_verbatim() {
        let Some((backend, t, tenant, since)) = start("sweeper-fhir-version").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let version = test_fhir_version();
        let p = backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .unwrap();
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 2, async move {
            b.update(&ten, &p, changed()).await
        })
        .await;
        let alias = match version.as_mime_param() {
            "4.0" => "R4",
            "4.3" => "R4B",
            "5.0" => "R5",
            _ => "R6",
        };
        backend
            .session
            .execute(
                "UPDATE resources SET fhir_version = ? WHERE tenant_id = ? AND resource_type = ? \
                 AND id = ? IF version_id = 2",
                vec![
                    Value::from(alias),
                    Value::from(t.as_str()),
                    Value::from("Patient"),
                    Value::from("p"),
                ],
                StatementKind::Cas,
            )
            .await
            .expect("alias the stored text");
        let r2 = row_of(&backend, &t, "p").await;
        assert_eq!(r2.stored_fhir_version.as_deref(), Some(alias));
        assert_eq!(r2.fhir_version, version);
        let report = pass(&backend, &[&t], since).await;
        assert_eq!(report.done, 1, "{report}");
        let (_, _, _, fhir, _) = version_row_of(&backend, &t, 2).await;
        assert_eq!(fhir.as_deref(), Some(alias));
        let rows = backend
            .session
            .rows(
                "SELECT last_updated, id, fhir_version FROM resources_by_type \
                 WHERE tenant_id = ? AND resource_type = ?",
                vec![Value::from(t.as_str()), Value::from("Patient")],
                StatementKind::Read,
            )
            .await
            .unwrap();
        let text: Vec<String> = rows
            .iter()
            .filter(|row| {
                let at: DateTime<Utc> = row.get_r_by_name("last_updated").unwrap();
                at == r2.last_updated
            })
            .map(|row| row.get_r_by_name("fhir_version").unwrap())
            .collect();
        assert_eq!(text, vec![alias.to_string()]);
        assert_consistent(&check(&backend, &t).await);
        say("fhir-version", &backend, &before);
    }

    /// N11: a permanent index failure keeps the intent and counts as an invariant.
    #[tokio::test]
    async fn a_permanent_index_failure_keeps_the_intent_and_counts_as_an_invariant() {
        let Some((backend, t, tenant, since)) = start("sweeper-permanent").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let version = test_fhir_version();
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 1, async move {
            b.create(&ten, "Patient", patient(), version).await
        })
        .await;
        let lu1 = row_of(&backend, &t, "p").await.last_updated;
        backend.session.inject_fault(
            StatementKind::Write,
            "INSERT INTO resources_by_type",
            Fault::FailWithInvalid,
        );
        let found = pending(&backend, &t, "p", since).await;
        assert_eq!(found.len(), 1);
        let result = backend.process_intent(&found[0], Rank::Normal).await;
        assert_eq!(result.case, Some(IntentCase::Current));
        assert_eq!(
            result.result,
            Err(Failure::Invariant(Invariant::PermanentWrite))
        );
        assert_eq!(version_ids(&backend, &t, "Patient", "p").await, vec![1]);
        assert!(rbt_rows(&backend, &t, "p").await.is_empty());
        assert_eq!(intents(&backend, &t, "p", since).await, vec![1]);
        let delta = backend.sweeper_stats().since(&before);
        assert_eq!((delta.invariant_violations, delta.done), (1, 0));
        let again = backend.process_intent(&found[0], Rank::Normal).await;
        assert_eq!(again.result, Ok(()));
        assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu1, 1)]);
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
        say("permanent", &backend, &before);
    }

    /// N12: a transient failure keeps the intent for the next try.
    #[tokio::test]
    async fn a_transient_failure_keeps_the_intent_for_the_next_try() {
        let Some((backend, t, tenant, since)) = start("sweeper-transient").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let version = test_fhir_version();
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 1, async move {
            b.create(&ten, "Patient", patient(), version).await
        })
        .await;
        let lu1 = row_of(&backend, &t, "p").await.last_updated;
        backend.session.inject_fault(
            StatementKind::Write,
            "INSERT INTO resources_by_type",
            Fault::FailWithUnavailable,
        );
        let found = pending(&backend, &t, "p", since).await;
        let at = backend.statement_counts();
        let result = backend.process_intent(&found[0], Rank::Normal).await;
        let spent = backend.statement_counts().since(&at);
        assert_eq!(result.result, Err(Failure::Transient("index")));
        assert_eq!(
            spent.write + spent.batch,
            result.index.batches + 1,
            "{spent}"
        );
        assert_eq!(intents(&backend, &t, "p", since).await, vec![1]);
        let report = pass(&backend, &[&t], since).await;
        assert_eq!(report.done, 1, "{report}");
        assert!(report.is_complete());
        assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu1, 1)]);
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
        say("transient", &backend, &before);
    }

    /// I2 is part of an intent: a replay whose index rows were written but whose delete of the intent row
    /// failed is not finished. It is `Transient("intent delete")`, counts as failed, and the intent stays.
    #[tokio::test]
    async fn a_failed_intent_delete_leaves_the_intent_unfinished() {
        let Some((backend, t, tenant, since)) = start("sweeper-i2").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let version = test_fhir_version();
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 1, async move {
            b.create(&ten, "Patient", patient(), version).await
        })
        .await;
        let lu1 = row_of(&backend, &t, "p").await.last_updated;
        backend.session.inject_fault(
            StatementKind::Write,
            "DELETE FROM pending_index_writes",
            Fault::FailWithUnavailable,
        );
        let found = pending(&backend, &t, "p", since).await;
        let result = backend.process_intent(&found[0], Rank::Normal).await;
        assert_eq!(result.case, Some(IntentCase::Current));
        assert_eq!(result.result, Err(Failure::Transient("intent delete")));
        assert!(
            result.index.rows_inserted > 0,
            "the index rows were written"
        );
        assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu1, 1)]);
        assert_eq!(intents(&backend, &t, "p", since).await, vec![1]);
        let delta = backend.sweeper_stats().since(&before);
        assert_eq!(delta.done, 0, "{delta}");

        // Through the retry list: failed, not done, and the next try finishes it.
        let mut report = SweepReport::default();
        let state = SweepState::new(Utc::now(), CancellationToken::new());
        backend.session.inject_fault(
            StatementKind::Write,
            "DELETE FROM pending_index_writes",
            Fault::FailWithUnavailable,
        );
        let now = Utc::now();
        backend
            .run_intents(&found, Rank::Normal, Some(&state), now, &mut report)
            .await;
        assert_eq!((report.done, report.failed), (0, 1), "{report}");
        assert_eq!(intents(&backend, &t, "p", since).await, vec![1]);
        let again = backend.process_intent(&found[0], Rank::Normal).await;
        assert_eq!(again.result, Ok(()));
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
        say("i2", &backend, &before);
    }

    /// IW24 on the replay: when the version row is not written, no index statement is sent and the
    /// intent stays.
    #[tokio::test]
    async fn a_replay_whose_version_row_failed_sends_no_index_row() {
        let Some((backend, t, tenant, since)) = start("sweeper-replay-iw24").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let version = test_fhir_version();
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 1, async move {
            b.create(&ten, "Patient", patient(), version).await
        })
        .await;
        let lu1 = row_of(&backend, &t, "p").await.last_updated;
        backend.session.inject_fault(
            StatementKind::Write,
            "INSERT INTO resource_versions",
            Fault::TimeOutWithoutApplying,
        );
        backend.session.inject_fault(
            StatementKind::Write,
            "resources_by_type",
            Fault::TimeOutWithoutApplying,
        );
        let found = pending(&backend, &t, "p", since).await;
        let result = backend.process_intent(&found[0], Rank::Normal).await;
        assert_eq!(result.case, Some(IntentCase::Current));
        assert_eq!(result.result, Err(Failure::Transient("version row")));
        assert_eq!(result.index, IndexWriteStats::default());
        let error = backend
            .session
            .rows(
                "SELECT id FROM resources_by_type WHERE tenant_id = ? AND resource_type = ?",
                vec![Value::from(t.as_str()), Value::from("Patient")],
                StatementKind::Write,
            )
            .await
            .expect_err("the tripwire is still armed: no index statement was sent");
        assert!(format!("{error:?}").contains("injected fault"), "{error:?}");
        assert!(version_ids(&backend, &t, "Patient", "p").await.is_empty());
        assert_eq!(intents(&backend, &t, "p", since).await, vec![1]);

        let again = backend.process_intent(&found[0], Rank::Normal).await;
        assert_eq!(again.result, Ok(()));
        assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu1, 1)]);
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        say("replay-iw24", &backend, &before);
    }

    /// N13 (SW13): bugs are invariants, never an empty set. The planted rows' tenant is `test-planted-`.
    #[tokio::test]
    async fn missing_version_rows_and_unreadable_keys_are_invariants_never_empty_sets() {
        let Some((backend, t, _, since)) = start("planted-sweeper-invariants").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let lu = whole_ms(Utc::now());
        let plant = |id: &'static str, version: i32, blob: Vec<u8>| {
            let (backend, t) = (backend.clone(), t.clone());
            async move {
                backend
                    .session
                    .execute(
                        "INSERT INTO resources (tenant_id, resource_type, id, version_id, data, \
                         last_updated, created_at, fhir_version, write_ts, write_id, index_keys, \
                         deleted_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0) IF NOT EXISTS",
                        vec![
                            Value::from(t.as_str()),
                            Value::from("Patient"),
                            Value::from(id),
                            Value::from(version),
                            Value::from("{\"resourceType\":\"Patient\"}"),
                            Value::from(lu),
                            Value::from(lu),
                            Value::from(test_fhir_version().as_mime_param()),
                            Value::from(write_ts_of(lu)),
                            Value::from(Uuid::new_v4()),
                            Value::from(Blob::from(blob)),
                        ],
                        StatementKind::Cas,
                    )
                    .await
                    .expect("plant")
            }
        };
        plant("p", 2, index_keys::encode(&[IndexKey::ResourcesByType])).await;
        plant("q", 1, vec![0x02, 0x00]).await;
        for (id, version) in [("p", 2), ("q", 1)] {
            let key = RowKey {
                tenant_id: &t,
                resource_type: "Patient",
                id,
            };
            backend
                .write_intent(
                    &IntentKey::new(&t, id, Utc::now()),
                    &key,
                    Uuid::new_v4(),
                    version,
                )
                .await
                .unwrap();
        }
        let p = pending(&backend, &t, "p", since).await;
        let q = pending(&backend, &t, "q", since).await;
        let at = backend.statement_counts();
        let rp = backend.process_intent(&p[0], Rank::Normal).await;
        let rq = backend.process_intent(&q[0], Rank::Normal).await;
        let spent = backend.statement_counts().since(&at);
        assert_eq!(rp.case, Some(IntentCase::Current));
        assert_eq!(
            rp.result,
            Err(Failure::Invariant(Invariant::MissingVersionRow))
        );
        assert_eq!(rq.case, Some(IntentCase::Current));
        assert_eq!(
            rq.result,
            Err(Failure::Invariant(Invariant::UnreadableKeys))
        );
        assert_eq!((spent.write, spent.batch), (0, 0), "{spent}");
        assert_eq!(
            backend.sweeper_stats().since(&before).invariant_violations,
            2
        );
        assert_eq!(intents(&backend, &t, "p", since).await, vec![2]);
        assert_eq!(intents(&backend, &t, "q", since).await, vec![1]);
        for (id, intent) in [("p", &p[0]), ("q", &q[0])] {
            let key = RowKey {
                tenant_id: &t,
                resource_type: "Patient",
                id,
            };
            backend.delete_intent(&intent.key, &key).await;
        }
        say("invariants", &backend, &before);
    }

    /// N14: a scoped pass leaves other tenants' intents alone (SW28).
    #[tokio::test]
    async fn a_scoped_pass_leaves_other_tenants_intents() {
        let Some((backend, a, tenant_a, since)) = start("sweeper-scope-a").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let b = tenant_id("sweeper-scope-b");
        let tenant_b = context(&b);
        let version = test_fhir_version();
        for (t, tenant) in [(&a, &tenant_a), (&b, &tenant_b)] {
            let (backend2, ten) = (backend.clone(), tenant.clone());
            crash(&backend, Checkpoint::AfterCas, t, "p", 1, async move {
                backend2.create(&ten, "Patient", patient(), version).await
            })
            .await;
        }
        let report = pass(&backend, &[&a], since).await;
        assert_eq!((report.intents, report.done), (1, 1), "{report}");
        assert!(intents(&backend, &a, "p", since).await.is_empty());
        assert_eq!(intents(&backend, &b, "p", since).await, vec![1]);
        let report = pass(&backend, &[&b], since).await;
        assert_eq!(report.done, 1, "{report}");
        assert_consistent(&check(&backend, &a).await);
        assert_consistent(&check(&backend, &b).await);
        say("scope", &backend, &before);
    }

    /// N15: the startup scan reads old buckets in minute slices (SW9, SW10).
    #[tokio::test]
    async fn the_startup_scan_reads_old_buckets_in_minute_slices() {
        let Some((backend, t, tenant, since)) = start("sweeper-startup").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let version = test_fhir_version();
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 1, async move {
            b.create(&ten, "Patient", patient(), version).await
        })
        .await;
        let lu1 = row_of(&backend, &t, "p").await.last_updated;
        let old = whole_ms(Utc::now() - chrono::Duration::hours(2));
        let key = RowKey {
            tenant_id: &t,
            resource_type: "Patient",
            id: "p",
        };
        let old_key = IntentKey::new(&t, "p", old);
        backend
            .write_intent(&old_key, &key, Uuid::new_v4(), 1)
            .await
            .unwrap();
        let state = SweepState::new(Utc::now(), CancellationToken::new());
        let minute = chrono::Duration::minutes(1);
        let scope = SweepScope::Tenants(vec![t.clone()]);
        let report = backend
            .startup_scan(&state, old - minute, old + minute, &scope)
            .await;
        let expected: usize = (0..INTENT_SHARDS as i16)
            .map(|shard| plan_slices(shard, old - minute, old + minute, startup_slice()).len())
            .sum();
        assert_eq!(report.slices, expected as u64, "{report}");
        assert_eq!(
            (report.intents, report.current, report.done),
            (1, 1, 1),
            "{report}"
        );
        assert_eq!(
            backend.sweeper_stats().since(&before).startup_slices,
            expected as u64
        );
        assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu1, 1)]);
        let slice = &plan_slices(old_key.shard(), old - minute, old + minute, startup_slice())[0];
        let mut left = 0;
        for slice in plan_slices(slice.shard, old - minute, old + minute, startup_slice()) {
            let (read, _) = backend.read_slice(&slice, Rank::Normal).await.unwrap();
            left += read.iter().filter(|intent| intent.tenant_id == t).count();
        }
        assert_eq!(left, 0, "the old intent row is gone");
        assert_eq!(intents(&backend, &t, "p", since).await, vec![1]);
        let report = pass(&backend, &[&t], since).await;
        assert_eq!(report.done, 1, "{report}");
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
        say("startup", &backend, &before);
    }

    /// A resource row planted through a CAS, with `blob` as its `index_keys` and no version rows.
    async fn plant_resource(
        backend: &CassandraBackend,
        t: &str,
        id: &str,
        version: i32,
        blob: Vec<u8>,
    ) {
        plant_resource_with(backend, t, id, version, blob, None).await;
    }

    /// [`plant_resource`], with `write_ts` stored in place of `write_ts_of(last_updated)` when it is set.
    async fn plant_resource_with(
        backend: &CassandraBackend,
        t: &str,
        id: &str,
        version: i32,
        blob: Vec<u8>,
        write_ts: Option<i64>,
    ) {
        let lu = whole_ms(Utc::now());
        backend
            .session
            .execute(
                "INSERT INTO resources (tenant_id, resource_type, id, version_id, data, \
                 last_updated, created_at, fhir_version, write_ts, write_id, index_keys, \
                 deleted_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0) IF NOT EXISTS",
                vec![
                    Value::from(t),
                    Value::from("Patient"),
                    Value::from(id),
                    Value::from(version),
                    Value::from("{\"resourceType\":\"Patient\"}"),
                    Value::from(lu),
                    Value::from(lu),
                    Value::from(test_fhir_version().as_mime_param()),
                    Value::from(write_ts.unwrap_or_else(|| write_ts_of(lu))),
                    Value::from(Uuid::new_v4()),
                    Value::from(Blob::from(blob)),
                ],
                StatementKind::Cas,
            )
            .await
            .expect("plant");
    }

    /// Puts every mark back at `to`, as if a failed slice had held them.
    fn rewind(state: &SweepState, to: DateTime<Utc>) {
        *state.marks.lock() = [whole_ms(to); 16];
    }

    /// With a held mark, every poll re-reads the same slices; an intent the retry list holds is not run again before its backoff.
    #[tokio::test]
    async fn a_held_mark_does_not_rerun_an_intent_the_retry_list_holds() {
        let Some((backend, t, tenant, since)) = start("sweeper-held").await else {
            return;
        };
        let version = test_fhir_version();
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 1, async move {
            b.create(&ten, "Patient", patient(), version).await
        })
        .await;
        let start_mark = Utc::now() - chrono::Duration::minutes(2);
        let state = SweepState::new(start_mark, CancellationToken::new());
        let scope = SweepScope::Tenants(vec![t.clone()]);
        backend.session.inject_fault(
            StatementKind::Write,
            "INSERT INTO resources_by_type",
            Fault::FailWithUnavailable,
        );
        let now = Utc::now() + chrono::Duration::seconds(40);
        let first = backend.poll_once(&state, now, &scope).await;
        assert_eq!((first.failed, first.done), (1, 0), "{first}");
        assert_eq!(intents(&backend, &t, "p", since).await, vec![1]);

        rewind(&state, start_mark);
        let at = backend.sweeper_stats();
        let second = backend
            .poll_once(&state, now + chrono::Duration::seconds(1), &scope)
            .await;
        assert!(second.slices > 0, "{second}");
        assert_eq!((second.intents, second.current), (0, 0), "{second}");
        assert_eq!(backend.sweeper_stats().since(&at).case_current, 0);

        rewind(&state, start_mark);
        let third = backend
            .poll_once(&state, now + chrono::Duration::seconds(6), &scope)
            .await;
        assert_eq!((third.current, third.done), (1, 1), "{third}");
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
    }

    /// An intent that hits an invariant is tried three times however many polls re-read its slice (SW13).
    #[tokio::test]
    async fn an_invariant_intent_stops_after_three_tries_across_polls() {
        let Some((backend, t, _, since)) = start("planted-sweeper-held").await else {
            return;
        };
        let before = backend.sweeper_stats();
        plant_resource(
            &backend,
            &t,
            "p",
            2,
            index_keys::encode(&[IndexKey::ResourcesByType]),
        )
        .await;
        let key = RowKey {
            tenant_id: &t,
            resource_type: "Patient",
            id: "p",
        };
        let intent_key = IntentKey::new(&t, "p", Utc::now());
        backend
            .write_intent(&intent_key, &key, Uuid::new_v4(), 2)
            .await
            .unwrap();
        let start_mark = Utc::now() - chrono::Duration::minutes(2);
        let state = SweepState::new(start_mark, CancellationToken::new());
        let scope = SweepScope::Tenants(vec![t.clone()]);
        let now = Utc::now() + chrono::Duration::seconds(40);
        let mut gave_up = 0;
        for step in [0, 6, 12, 18, 24] {
            rewind(&state, start_mark);
            let report = backend
                .poll_once(&state, now + chrono::Duration::seconds(step), &scope)
                .await;
            gave_up += report.gave_up;
        }
        let delta = backend.sweeper_stats().since(&before);
        assert_eq!(delta.invariant_violations, 3, "{delta}");
        assert_eq!((delta.invariant_gave_up, gave_up), (1, 1));
        assert_eq!(intents(&backend, &t, "p", since).await, vec![2]);
        backend.delete_intent(&intent_key, &key).await;
        say("held-invariant", &backend, &before);
    }

    /// Five polls that each re-read the whole window, `now` moving 6 s a poll (backoff 5 s, then 10 s).
    /// Returns how many intents the polls gave up on.
    async fn five_polls(backend: &CassandraBackend, t: &str) -> u64 {
        let start_mark = Utc::now() - chrono::Duration::minutes(2);
        let state = SweepState::new(start_mark, CancellationToken::new());
        let scope = SweepScope::Tenants(vec![t.to_string()]);
        let now = Utc::now() + chrono::Duration::seconds(40);
        let mut gave_up = 0;
        for step in [0, 6, 12, 18, 24] {
            rewind(&state, start_mark);
            let report = backend
                .poll_once(&state, now + chrono::Duration::seconds(step), &scope)
                .await;
            gave_up += report.gave_up;
        }
        gave_up
    }

    /// SW13, `WriteTsMismatch` in both places it is raised: the current replay (`R`'s `write_ts`) and the
    /// superseded one (version row V's). Each is tried three times and its intent row is kept. The rows are
    /// planted in a `test-planted-` tenant; they decode, only the stored `write_ts` is wrong, so no scan of
    /// the node can fail on them.
    #[tokio::test]
    async fn a_write_ts_mismatch_is_tried_three_times_and_keeps_its_intent() {
        let Some((backend, t, _, since)) = start("planted-sweeper-write-ts").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let blob = index_keys::encode(&[IndexKey::ResourcesByType]);
        let lu = whole_ms(Utc::now());
        // "current": version 2 is the row, and its write_ts is not write_ts_of(its last_updated).
        plant_resource_with(
            &backend,
            &t,
            "current",
            2,
            blob.clone(),
            Some(write_ts_of(lu) + 1),
        )
        .await;
        // "superseded": the row is version 3; version row 2 carries the wrong write_ts.
        plant_resource(&backend, &t, "superseded", 3, blob.clone()).await;
        backend
            .session
            .execute(
                "INSERT INTO resource_versions (tenant_id, resource_type, id, version_id, data, \
                 last_updated, fhir_version, write_ts, write_id, index_keys) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                vec![
                    Value::from(t.as_str()),
                    Value::from("Patient"),
                    Value::from("superseded"),
                    Value::from(2),
                    Value::from("{\"resourceType\":\"Patient\"}"),
                    Value::from(lu),
                    Value::from(test_fhir_version().as_mime_param()),
                    Value::from(write_ts_of(lu) + 1),
                    Value::from(Uuid::new_v4()),
                    Value::from(Blob::from(blob)),
                ],
                StatementKind::Write,
            )
            .await
            .expect("plant the version row");
        let mut planted = Vec::new();
        for (id, version) in [("current", 2), ("superseded", 2)] {
            let key = RowKey {
                tenant_id: &t,
                resource_type: "Patient",
                id,
            };
            let intent_key = IntentKey::new(&t, id, Utc::now());
            backend
                .write_intent(&intent_key, &key, Uuid::new_v4(), version)
                .await
                .unwrap();
            planted.push((id, intent_key));
        }
        let gave_up = five_polls(&backend, &t).await;
        let delta = backend.sweeper_stats().since(&before);
        assert_eq!(delta.invariant_violations, 6, "{delta}");
        assert_eq!((delta.invariant_gave_up, gave_up), (2, 2));
        assert_eq!(
            (delta.case_current, delta.case_superseded),
            (3, 3),
            "{delta}"
        );
        assert_eq!(delta.done, 0, "{delta}");
        for (id, intent_key) in planted {
            assert_eq!(intents(&backend, &t, id, since).await, vec![2]);
            let key = RowKey {
                tenant_id: &t,
                resource_type: "Patient",
                id,
            };
            backend.delete_intent(&intent_key, &key).await;
        }
        say("write-ts", &backend, &before);
    }

    /// SW13, `UnreadableRow` where R1 is read and where R3 is: each is tried three times and its intent row
    /// is kept. A stored row that does not decode would fail the node's shared scans, so none is planted:
    /// the node answers the read with a row of another table ([`Fault::AnswerWithForeignRow`]), three times.
    #[tokio::test]
    async fn an_unreadable_row_is_tried_three_times_and_keeps_its_intent() {
        let Some((backend, t, tenant, since)) = start("sweeper-unreadable").await else {
            return;
        };
        let before = backend.sweeper_stats();
        // R1: an intent with no resource row; the read of `resources` answers with a foreign row.
        let key = RowKey {
            tenant_id: &t,
            resource_type: "Patient",
            id: "r1",
        };
        let intent_key = IntentKey::new(&t, "r1", Utc::now());
        backend
            .write_intent(&intent_key, &key, Uuid::new_v4(), 1)
            .await
            .unwrap();
        for _ in 0..3 {
            backend.session.inject_fault(
                StatementKind::Read,
                "FROM resources WHERE",
                Fault::AnswerWithForeignRow,
            );
        }
        let gave_up = five_polls(&backend, &t).await;
        let delta = backend.sweeper_stats().since(&before);
        assert_eq!(delta.invariant_violations, 3, "{delta}");
        assert_eq!((delta.invariant_gave_up, gave_up), (1, 1));
        assert_eq!(delta.done, 0, "{delta}");
        assert_eq!(intents(&backend, &t, "r1", since).await, vec![1]);
        backend.delete_intent(&intent_key, &key).await;

        // R3: a crashed update at version 2; the read of version row 1 answers with a foreign row.
        let version = test_fhir_version();
        let p = backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .unwrap();
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 2, async move {
            b.update(&ten, &p, changed()).await
        })
        .await;
        let lu2 = row_of(&backend, &t, "p").await.last_updated;
        for _ in 0..3 {
            backend.session.inject_fault(
                StatementKind::Read,
                "FROM resource_versions WHERE",
                Fault::AnswerWithForeignRow,
            );
        }
        let at = backend.sweeper_stats();
        let gave_up = five_polls(&backend, &t).await;
        let delta = backend.sweeper_stats().since(&at);
        assert_eq!(delta.invariant_violations, 3, "{delta}");
        assert_eq!((delta.invariant_gave_up, gave_up), (1, 1));
        assert_eq!(delta.done, 0, "{delta}");
        assert_eq!(intents(&backend, &t, "p", since).await, vec![2]);
        assert_eq!(version_ids(&backend, &t, "Patient", "p").await, vec![1]);

        // The faults are spent, so a pass heals it.
        let report = pass(&backend, &[&t], since).await;
        assert!(report.is_complete(), "{report}");
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu2, 2)]);
        assert_consistent(&check(&backend, &t).await);
        say("unreadable", &backend, &before);
    }

    /// A startup slice whose read failed is read again by a later poll: the marks start after the
    /// startup window, so nothing else would reach it.
    #[tokio::test]
    async fn a_failed_startup_slice_is_read_again_by_the_next_poll() {
        let Some((backend, t, tenant, since)) = start("sweeper-startup-retry").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let version = test_fhir_version();
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 1, async move {
            b.create(&ten, "Patient", patient(), version).await
        })
        .await;
        let lu1 = row_of(&backend, &t, "p").await.last_updated;
        let state = SweepState::new(Utc::now(), CancellationToken::new());
        let scope = SweepScope::Tenants(vec![t.clone()]);
        let (from, to) = (since, Utc::now() + chrono::Duration::seconds(1));
        let slices = 16 * plan_slices(0, from, to, startup_slice()).len();
        for _ in 0..slices {
            backend.session.inject_fault_on_first_page(
                StatementKind::Read,
                "FROM pending_index_writes",
                Fault::FailWithUnavailable,
            );
        }
        let report = backend.startup_scan(&state, from, to, &scope).await;
        assert_eq!(report.slice_errors, slices as u64, "{report}");
        assert_eq!(report.intents, 0, "{report}");
        assert_eq!(intents(&backend, &t, "p", since).await, vec![1]);

        // Before its backoff the slice is not read again.
        let early = backend
            .poll_once(&state, Utc::now() + chrono::Duration::seconds(2), &scope)
            .await;
        assert_eq!(early.slices, 0, "{early}");
        let later = backend
            .poll_once(&state, Utc::now() + chrono::Duration::seconds(10), &scope)
            .await;
        assert_eq!((later.current, later.done), (1, 1), "{later}");
        assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu1, 1)]);
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
        say("startup-retry", &backend, &before);
    }

    /// N17: a poll moves a mark only once its slices are read (SW8).
    #[tokio::test]
    async fn a_poll_moves_a_mark_only_once_its_slices_are_read() {
        let Some((backend, t, _, _)) = start("sweeper-marks").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let now = Utc::now();
        let start = now - chrono::Duration::minutes(2);
        let state = SweepState::new(start, CancellationToken::new());
        let scope = SweepScope::Tenants(vec![t.clone()]);
        backend.session.inject_fault_on_first_page(
            StatementKind::Read,
            "FROM pending_index_writes",
            Fault::FailWithUnavailable,
        );
        let report = backend.poll_once(&state, now, &scope).await;
        assert_eq!(report.slice_errors, 1, "{report}");
        let to = whole_ms(now - chrono::Duration::seconds(30));
        let moved = (0..16).filter(|s| state.mark(*s) == to).count();
        let held = (0..16)
            .filter(|s| state.mark(*s) == whole_ms(start))
            .count();
        assert_eq!((moved, held), (15, 1));
        let report = backend.poll_once(&state, now, &scope).await;
        assert_eq!(report.slice_errors, 0, "{report}");
        assert!((0..16).all(|s| state.mark(s) == to));
        say("marks", &backend, &before);
    }

    /// RW4: a poll over many intents never has more than `MAX_IN_FLIGHT` statements in flight on the
    /// session. The semaphore's own test cannot see a statement that takes no permit; this one counts at
    /// the session. Intents with no resource row take the read, the serial read and the delete.
    #[tokio::test]
    async fn a_poll_over_many_intents_keeps_the_session_within_32_statements() {
        let Some((backend, t, _, since)) = start("sweeper-rw4").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let writes: Vec<_> = (0..400)
            .map(|n| {
                let (backend, t) = (backend.clone(), t.clone());
                async move {
                    let id = format!("p{n}");
                    let key = RowKey {
                        tenant_id: &t,
                        resource_type: "Patient",
                        id: &id,
                    };
                    backend
                        .write_intent(
                            &IntentKey::new(&t, &id, Utc::now()),
                            &key,
                            Uuid::new_v4(),
                            1,
                        )
                        .await
                        .expect("write intent");
                }
            })
            .collect();
        futures::stream::iter(writes)
            .buffer_unordered(16)
            .collect::<Vec<_>>()
            .await;
        let state = SweepState::new(
            Utc::now() - chrono::Duration::minutes(2),
            CancellationToken::new(),
        );
        let scope = SweepScope::Tenants(vec![t.clone()]);
        let now = Utc::now() + chrono::Duration::seconds(40);
        let report = backend.poll_once(&state, now, &scope).await;
        assert_eq!((report.no_row, report.done), (400, 400), "{report}");
        let peak = backend.session.max_statements_in_flight();
        assert!(peak <= MAX_IN_FLIGHT, "{peak} statements in flight at once");
        assert!(peak > 1, "nothing ran at once, so the count proves nothing");
        assert!(intents(&backend, &t, "p0", since).await.is_empty());
        eprintln!("[sweeper case=rw4 peak_statements={peak}]");
        say("rw4", &backend, &before);
    }

    /// R1 (SW26): the outbox's tombstones stay under the node's warning threshold under load.
    /// Run by hand; the 10-minute run is the CI node's.
    #[tokio::test]
    #[ignore = "the 10-minute outbox tombstone run (SW26); run by hand"]
    async fn outbox_tombstones_stay_under_the_warning_threshold_under_load() {
        let Ok(url) = std::env::var("HFS_TEST_CASSANDRA_URL") else {
            eprintln!("{SKIP}");
            return;
        };
        let secs: u64 = std::env::var("HFS_TEST_SWEEPER_LOAD_SECS")
            .ok()
            .and_then(|raw| raw.parse().ok())
            .unwrap_or(600);
        assert!(
            !(url.trim().ends_with(":59042") && secs > 60),
            "the shared dev node takes at most 60 s of load"
        );
        let backend = harness::backend().await.expect("backend");
        let t = tenant_id("sweeper-load");
        let started = Utc::now();
        let state = SweepState::new(started, CancellationToken::new());
        let scope = SweepScope::Tenants(vec![t.clone()]);
        let before = backend.sweeper_stats();
        let mut ticker = tokio::time::interval(Duration::from_millis(100));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let (mut n, mut tick) = (0u64, 0u64);
        let deadline = std::time::Instant::now() + Duration::from_secs(secs);
        while std::time::Instant::now() < deadline {
            ticker.tick().await;
            tick += 1;
            let work: Vec<_> = (0..20)
                .map(|_| {
                    n += 1;
                    let id = format!("p{n}");
                    let (backend, t) = (backend.clone(), t.clone());
                    async move {
                        let intent = IntentKey::new(&t, &id, Utc::now());
                        let key = RowKey {
                            tenant_id: &t,
                            resource_type: "Patient",
                            id: &id,
                        };
                        backend
                            .write_intent(&intent, &key, Uuid::new_v4(), 1)
                            .await
                            .expect("write intent");
                        backend.delete_intent(&intent, &key).await;
                    }
                })
                .collect();
            futures::stream::iter(work)
                .buffer_unordered(32)
                .collect::<Vec<_>>()
                .await;
            if tick % 50 == 0 {
                backend.poll_once(&state, Utc::now(), &scope).await;
            }
        }
        let delta = backend.sweeper_stats().since(&before);
        let tombstones = backend
            .session
            .rows(
                "SELECT count, max, p99th FROM system_views.tombstones_per_read \
                 WHERE keyspace_name = ? AND table_name = 'pending_index_writes'",
                vec![Value::from(backend.config.keyspace.as_str())],
                StatementKind::Read,
            )
            .await
            .ok()
            .and_then(|rows| {
                rows.first().map(|row| {
                    let number = |name: &str| {
                        IntoRustByName::<i64>::get_by_name(row, name)
                            .ok()
                            .flatten()
                            .map(|v| v.to_string())
                            .or_else(|| {
                                IntoRustByName::<f64>::get_by_name(row, name)
                                    .ok()
                                    .flatten()
                                    .map(|v| v.to_string())
                            })
                            .unwrap_or_else(|| "unavailable".into())
                    };
                    (number("max"), number("p99th"))
                })
            })
            .unwrap_or_else(|| ("unavailable".into(), "unavailable".into()));
        eprintln!(
            "[sweeper-load secs={secs} intents={n} polls={} slices={} tombstone_warnings={} \
             tombstones_max={} tombstones_p99={}]",
            delta.polls, delta.slices_read, delta.tombstone_warnings, tombstones.0, tombstones.1
        );
        assert_eq!(delta.tombstone_warnings, 0);
    }
}

/// The post-suite step (ADR-sam-michaels-0006, RV22): after every suite has exited, one grace-0 sweep of
/// every `test-` tenant and the consistency check over every one of them but the `test-planted-` ones.
#[cfg(test)]
pub(crate) mod post_suite {
    use std::time::Instant;

    use super::*;
    use crate::backends::cassandra::backend::SELECT_RESOURCE_KEYS;
    use crate::backends::cassandra::consistency::{
        IndexScan, Registry, ScanMode, Scope, index_consistency,
    };
    use crate::backends::cassandra::test_support::test_config;

    /// RV22's exclusion: the tenants that plant defects on purpose are swept but not checked.
    pub(crate) fn checked(tenant_id: &str) -> bool {
        tenant_id.starts_with("test-") && !tenant_id.starts_with("test-planted-")
    }

    /// What the post-suite step did.
    pub(crate) struct PostSuiteReport {
        pub(crate) prefix: String,
        pub(crate) sweep: SweepReport,
        pub(crate) tenants: usize,
        pub(crate) changed: usize,
        pub(crate) defects: usize,
        pub(crate) failing: Vec<String>,
        pub(crate) rows_scanned: usize,
        pub(crate) took: Duration,
    }

    impl fmt::Display for PostSuiteReport {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            let failing = if self.failing.is_empty() {
                "-".to_string()
            } else {
                self.failing.join(",")
            };
            write!(
                f,
                "[post-suite prefix={} tenants={} changed={} defects={} failing={failing} \
                 rows_scanned={} took_ms={}] {}",
                self.prefix,
                self.tenants,
                self.changed,
                self.defects,
                self.rows_scanned,
                self.took.as_millis(),
                self.sweep
            )
        }
    }

    /// One grace-0 sweep of every tenant under `prefix`, then one shared scan and a check per checked tenant.
    pub(crate) async fn post_suite_report(
        backend: &CassandraBackend,
        prefix: &str,
        since: DateTime<Utc>,
    ) -> PostSuiteReport {
        let started = Instant::now();
        let sweep = backend
            .sweep_pass(SweepOptions {
                scope: SweepScope::TenantPrefix(prefix.to_string()),
                grace: Duration::ZERO,
                now: Utc::now(),
                since,
                allow_short_grace: true,
            })
            .await
            .expect("a grace-0 pass is allowed here");

        // Every tenant with a resource, marked changed when it has a SearchParameter (RV14).
        let keys = backend
            .session
            .rows_paged(SELECT_RESOURCE_KEYS, Vec::new(), StatementKind::Read)
            .await
            .expect("the resource keys");
        let mut registries: BTreeMap<String, Registry> = BTreeMap::new();
        for row in &keys {
            let tenant: String = row.get_r_by_name("tenant_id").expect("tenant_id");
            if !tenant.starts_with(prefix) || !checked(&tenant) {
                continue;
            }
            let resource_type: String = row.get_r_by_name("resource_type").expect("resource_type");
            let entry = registries.entry(tenant).or_insert(Registry::Fixed);
            if resource_type == "SearchParameter" {
                *entry = Registry::Changed;
            }
        }

        let own = prefix.to_string();
        let scan = IndexScan::take(backend, move |tenant| {
            tenant.starts_with(&own) && checked(tenant)
        })
        .await;
        let mut tenants: Vec<String> = registries.keys().cloned().collect();
        tenants.extend(scan.tenants().map(str::to_string));
        tenants.sort();
        tenants.dedup();

        let (mut defects, mut changed, mut failing) = (0, 0, Vec::new());
        for tenant in &tenants {
            let registry = registries.get(tenant).copied().unwrap_or(Registry::Fixed);
            if registry == Registry::Changed {
                changed += 1;
            }
            let report = index_consistency(
                backend,
                tenant,
                Scope::Tenant,
                ScanMode::Shared(&scan),
                registry,
            )
            .await;
            if !report.defects.is_empty() {
                defects += report.defects.len();
                failing.push(tenant.clone());
            }
        }
        PostSuiteReport {
            prefix: prefix.to_string(),
            sweep,
            tenants: tenants.len(),
            changed,
            defects,
            failing,
            rows_scanned: scan.scanned(),
            took: started.elapsed(),
        }
    }

    /// RV22: the CI entry point, run alone after every suite. Opt-in, and never on the shared dev node.
    #[tokio::test]
    #[ignore = "the post-suite CI step runs it alone, after every suite (RV22)"]
    async fn post_suite_sweep_and_check() {
        let Ok(url) = std::env::var("HFS_TEST_POST_SUITE_URL") else {
            eprintln!("skipping: HFS_TEST_POST_SUITE_URL is not set");
            return;
        };
        assert!(
            !url.trim().ends_with(":59042"),
            "a grace-0 sweep of every test- tenant would delete the intents of tests still running there"
        );
        let backend = CassandraBackend::connect(crate::backends::cassandra::CassandraConfig {
            contact_points: vec![url],
            ..test_config()
        })
        .await
        .expect("connect");
        let report = post_suite_report(&backend, "test-", Utc::now() - startup_window()).await;
        eprintln!("{report}");
        if std::env::var("HFS_TEST_POST_SUITE_STRICT").as_deref() == Ok("1") {
            assert!(
                report.defects == 0 && report.sweep.is_complete(),
                "{report}"
            );
        }
    }

    /// P10: the planted tenants are swept but not checked.
    #[test]
    fn the_post_suite_check_skips_planted_tenants() {
        assert!(checked("test-a"));
        assert!(!checked("test-planted-x"));
        assert!(!checked("acme"));
    }

    /// N16 (RV22): the report sweeps and checks one prefix.
    #[tokio::test]
    async fn the_post_suite_report_sweeps_and_checks_one_prefix() {
        use crate::backends::cassandra::checkpoint::Checkpoint;
        use crate::backends::cassandra::consistency::harness::{self, changed, context, patient};
        use crate::backends::cassandra::test_support::test_fhir_version;
        use crate::core::ResourceStorage;

        let Some(backend) = harness::backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let since = Utc::now() - chrono::Duration::seconds(5);
        let prefix = format!("test-postsuite-{}-", Uuid::new_v4().simple());
        let version = test_fhir_version();
        let clean = context(&format!("{prefix}clean"));
        let p = backend
            .create(&clean, "Patient", patient(), version)
            .await
            .unwrap();
        backend.update(&clean, &p, changed()).await.unwrap();

        let crashed_id = format!("{prefix}crashed");
        let crashed = context(&crashed_id);
        let p = backend
            .create(&crashed, "Patient", patient(), version)
            .await
            .unwrap();
        let mut hold = backend.hold_at(Checkpoint::AfterCas, &crashed_id, "p", 2);
        let task = tokio::spawn({
            let (backend, crashed) = (backend.clone(), crashed.clone());
            async move { backend.update(&crashed, &p, changed()).await }
        });
        hold.reached().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());

        let report = post_suite_report(&backend, &prefix, since).await;
        eprintln!("{report}");
        assert_eq!(
            (report.sweep.current, report.sweep.done),
            (1, 1),
            "{report}"
        );
        assert!(report.sweep.is_complete(), "{report}");
        assert_eq!(
            (report.tenants, report.changed, report.defects),
            (2, 0, 0),
            "{report}"
        );
        assert!(report.failing.is_empty(), "{report}");
        assert!(
            format!("{report}").starts_with("[post-suite prefix=test-postsuite-"),
            "{report}"
        );
    }
}
