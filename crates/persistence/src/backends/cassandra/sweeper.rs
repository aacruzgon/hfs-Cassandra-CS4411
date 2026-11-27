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
//! A missing version row, a key set that does not decode, and a `write_ts`
//! that is not `write_ts_of(last_updated)` are bugs, never an empty set
//! (SW13): they are logged at `error`, counted, and the intent row is kept. A
//! write the node refuses as `Invalid` is counted and logged the same way, and
//! its intent row is kept too. A `SearchParameter` that was replayed
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
//! intent row until its TTL. A write the node refused is tried 3 times 5 s
//! apart, then every 5 min until the same 24 hours: it is the node's
//! settings, not the stored rows, and an operator can fix those without
//! restarting HFS (ADR-aacruzgon-0090). Its first three tries log at `error`,
//! the third once more to say it is kept; later tries log at `warn`, the
//! request path always at `error`. The list holds at most [`RETRY_LIST_CAP`]
//! entries; when it is full the intent's shard keeps its mark, or its startup
//! or held slice stays held, so a later poll reads it again. The poll, and
//! the startup scan at most once a minute, log at `error` that the list is
//! full (SW18). A refused write the full list cannot take is not counted
//! toward its tries, though `invariant_violations` and `overflow` still count
//! it, and logs at `warn`. SW13's count of a bug's tries off the list holds
//! at most `GAVE_UP_CAP` (10,000) keys, the first counted first out: with
//! more bugs than that failing at once past a full list, a bug can be evicted
//! before its third try. How many give up then depends on how far past the
//! cap they are: in a model of the sweeper's near-fixed order, nearly all of
//! 10,001 gave up on the third poll, but at most 319 of 11,000 within five
//! (ADR-aacruzgon-0090). The rest are replayed on every poll, each try logged
//! at `error`, until the list has room. The intent row is never deleted for a
//! failure.
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
    INDEX_BATCHES_IN_FLIGHT, INTENT_SHARDS, IndexRow, IntentKey, RefusalLog, RowKey, VersionKeys,
    VersionRow, WriteFailure, bucket_of, index_delta, log_refusal, write_ts_of,
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

/// What one slice read gave: each row as an intent, or `Err` with its tenant when the row does not decode
/// and that column alone does.
#[derive(Debug)]
pub(crate) struct SliceRead {
    pub(crate) rows: Vec<Result<Intent, Option<String>>>,
    /// The node's tombstone warnings on the read (SW10).
    pub(crate) warnings: u64,
}

impl SliceRead {
    /// The rows that decoded, in every scope (tests read slices whole).
    #[cfg(test)]
    pub(crate) fn decoded(self) -> Vec<Intent> {
        self.rows.into_iter().filter_map(Result::ok).collect()
    }
}

/// The intents of `rows` in `scope`, and how many rows in scope do not decode. A row whose tenant is not
/// readable either counts only for a scope of every tenant.
pub(crate) fn in_scope(
    rows: Vec<Result<Intent, Option<String>>>,
    scope: &SweepScope,
) -> (Vec<Intent>, u64) {
    let mut intents = Vec::new();
    let mut undecodable = 0;
    for row in rows {
        match row {
            Ok(intent) if scope.covers(&intent.tenant_id) => intents.push(intent),
            Ok(_) => {}
            Err(Some(tenant)) if scope.covers(&tenant) => undecodable += 1,
            Err(None) if *scope == SweepScope::All => undecodable += 1,
            Err(_) => {}
        }
    }
    (intents, undecodable)
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
    /// A `resources` or `resource_versions` row whose columns do not decode, or a live `resources` row with no `data`.
    UnreadableRow,
    /// The version row or the index delta failed as `WriteFailure::Permanent`: no try can succeed.
    PermanentWrite,
}

impl Invariant {
    /// "missing_version_row", "unreadable_keys", "write_ts", "unreadable_row", "permanent_write".
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
    /// Tries of intents that hit an invariant while the list was full, so the third still gives up
    /// (SW13): each key with the order it was first counted in, and its count. At most `GAVE_UP_CAP`.
    /// A write the node refused is not counted here (ADR-aacruzgon-0090).
    overflow_tries: BTreeMap<RetryKey, (u64, u32)>,
    overflow_seen: u64,
}

/// Most keys of intents that gave up the retry list remembers; the oldest is forgotten first.
const GAVE_UP_CAP: usize = 10_000;

/// Least time between two of the startup scan's "retry list is full" lines.
const FULL_LOG_EVERY: std::time::Duration = std::time::Duration::from_secs(60);

/// SW18: the list is full, so `shards` keep their marks and `slices` stay held, to be read again. This
/// `error` is the operator's signal for every intent the list could not take, whose own lines are at
/// `warn` when the node refused it (ADR-aacruzgon-0090).
fn log_retry_list_full(shards: u64, slices: u64) {
    tracing::error!(
        shards_held = shards,
        slices_held = slices,
        retry_cap = RETRY_LIST_CAP,
        "the index sweeper's retry list is full; what it could not take is read again"
    );
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
    /// Kept: a write the node refused (`PermanentWrite`) for the third time or later, tried again at
    /// `next_at`, `RETRY_MAX` on (ADR-aacruzgon-0090). `first` on the third, which logs at `error`.
    Refused { next_at: DateTime<Utc>, first: bool },
}

impl RetryList {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            cap,
            gave_up: BTreeMap::new(),
            gave_up_order: std::collections::VecDeque::new(),
            overflow_tries: BTreeMap::new(),
            overflow_seen: 0,
        }
    }

    /// Whether the list already owns `intent`'s row: it is waiting for its `next_at`, or it gave up and
    /// is still inside the retry horizon. A slice read does not try such an intent again.
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
    /// an invariant failure is retried after `RETRY_FIRST` and counts toward `INVARIANT_TRIES`. A write the node
    /// refused (`PermanentWrite`) is not a bug in the stored rows but the node's settings, which an operator can
    /// fix without restarting HFS: after its third try it stays on the list, tried every `RETRY_MAX`, until the
    /// retry horizon (SW18; ADR-aacruzgon-0090). It never gives up, and a full list holds its mark as for a
    /// transient failure.
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
            // A bug is tried three times however full the list is (SW13), while at most `GAVE_UP_CAP`
            // fail at once (past that, see the module doc); a transient failure, or a write the node
            // refused, holds the mark (SW18) and is not counted toward its tries. A refused write off
            // the list logs at `warn` (`refusal_log`); the "retry list is full" line is the `error`
            // (ADR-aacruzgon-0090).
            if matches!(failure, Failure::Invariant(invariant) if invariant != Invariant::PermanentWrite)
            {
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
                entry.next_at = now + backoff(entry.failures);
            }
            Failure::Invariant(Invariant::PermanentWrite) => {
                entry.invariant_failures += 1;
                if entry.invariant_failures >= INVARIANT_TRIES {
                    entry.next_at = now + retry_max();
                    return Queued::Refused {
                        next_at: entry.next_at,
                        first: entry.invariant_failures == INVARIANT_TRIES,
                    };
                }
                entry.next_at = now + retry_first();
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
        }
    }

    /// The level the next try of `intent` logs a refused write at (ADR-aacruzgon-0090): `warn` once the
    /// list has counted `INVARIANT_TRIES` invariant failures for its entry, since the third refusal
    /// already logged at `error` that the sweeper keeps trying (only a refused write gets that far
    /// without giving up), and `warn` when it has no entry and the list is full, so a refusal could not
    /// be queued: it is not counted toward its tries (`invariant_violations` and `overflow` still count
    /// it), and the "retry list is full" line is the `error`. `error` otherwise, and the request path
    /// always logs at `error`.
    pub(crate) fn refusal_log(&self, intent: &Intent) -> RefusalLog {
        match self.entries.get(&retry_key(intent)) {
            Some(entry) if entry.invariant_failures >= INVARIANT_TRIES => RefusalLog::Warn,
            Some(_) => RefusalLog::Error,
            None if self.entries.len() >= self.cap => RefusalLog::Warn,
            None => RefusalLog::Error,
        }
    }

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
    /// Least time between two of the startup scan's "retry list is full" lines: `FULL_LOG_EVERY`,
    /// which a test may change.
    full_log_every: std::time::Duration,
}

impl SweepState {
    /// Every mark at `mark` (truncated to whole ms); an empty list of `RETRY_LIST_CAP`.
    pub(crate) fn new(mark: DateTime<Utc>, cancel: CancellationToken) -> Self {
        Self {
            marks: parking_lot::Mutex::new([whole_ms(mark); 16]),
            retry: parking_lot::Mutex::new(RetryList::new(RETRY_LIST_CAP)),
            pending: parking_lot::Mutex::new(Vec::new()),
            cancel,
            full_log_every: FULL_LOG_EVERY,
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
            Queued::Retry { .. } | Queued::Refused { .. } => Self::add(&self.retries, 1),
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
    /// Invariant failures (SW13), and undecodable intent rows in scope; their rows stay.
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

/// SW28: the end of a pass at `now` with `grace`, in whole milliseconds. `None` when the grace is past
/// what a timestamp can hold (`chrono` would panic on the subtraction): no intent is old enough, so
/// the pass is empty.
fn pass_end(now: DateTime<Utc>, grace: Duration) -> Option<DateTime<Utc>> {
    let grace = chrono::Duration::from_std(grace).ok()?;
    now.checked_sub_signed(grace).map(whole_ms)
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
        let Some(to) = pass_end(options.now, options.grace) else {
            return Ok(SweepReport::default());
        };
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

        // Held slices whose re-read the full list could not take: the startup scan's, and this poll's
        // earlier ones. They keep their hold as a shard keeps its mark, and are logged with the shards.
        let mut slices_held = 0u64;
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
                if outcome.overflow {
                    slices_held += 1;
                }
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
        if held > 0 || slices_held > 0 {
            log_retry_list_full(held, slices_held);
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
        // Slices the full list could not take, since the last "retry list is full" line, which the
        // startup scan logs at most once a minute; a slice it does not name is held, and the poll that
        // reads it again logs the line.
        let mut slices_held = 0u64;
        let mut full_logged_at: Option<std::time::Instant> = None;
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
                    if outcome.overflow {
                        slices_held += 1;
                    }
                    state.hold_slice(outcome.slice, 1, Utc::now());
                }
            }
            let every = retry.map_or(FULL_LOG_EVERY, |state| state.full_log_every);
            if slices_held > 0 && full_logged_at.is_none_or(|at| at.elapsed() >= every) {
                log_retry_list_full(0, slices_held);
                slices_held = 0;
                full_logged_at = Some(std::time::Instant::now());
            }
        }
        report
    }

    /// I3 for one slice: its rows, and the node's tombstone warnings on it (SW10). A row that does not
    /// decode is logged at `error`, counted as `invariant_violations`, and kept as `Err` with its tenant
    /// when that column alone reads, so a pass can count it in scope.
    pub(crate) async fn read_slice(
        &self,
        slice: &Slice,
        rank: Rank,
    ) -> Result<SliceRead, DriverError> {
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
        let mut decoded = Vec::with_capacity(rows.len());
        for row in &rows {
            match decode_intent(slice, row) {
                Some(intent) => decoded.push(Ok(intent)),
                None => {
                    self.sweeper.counters.invariant();
                    tracing::error!(
                        bucket = %slice.bucket,
                        shard = slice.shard,
                        invariant = "unreadable_intent",
                        "an intent row does not decode; it is left in the outbox"
                    );
                    decoded.push(Err(row.get_r_by_name("tenant_id").ok()));
                }
            }
        }
        Ok(SliceRead {
            rows: decoded,
            warnings,
        })
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
        let read = match self.read_slice(slice, rank).await {
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
        self.sweeper.counters.tombstones(read.warnings);
        outcome.report.tombstone_warnings = read.warnings;
        let (mut intents, undecodable) = in_scope(read.rows, scope);
        // An intent row that does not decode stays in the outbox, so the pass is not complete.
        outcome.report.invariant += undecodable;
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
                // ADR-aacruzgon-0090: a refused write past its third try logs at `warn`.
                let refusal = state.map_or(RefusalLog::Error, |state| {
                    state.retry.lock().refusal_log(intent)
                });
                async move {
                    // SW3: an intent not yet started is skipped once shutdown is asked for.
                    if cancel.is_some_and(|cancel| cancel.is_cancelled()) {
                        return None;
                    }
                    Some((intent, self.try_intent(intent, rank, refusal).await))
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
            Queued::Refused { first, .. } => {
                if first {
                    tracing::error!(
                        tenant_id = %intent.tenant_id,
                        resource_type = %intent.resource_type,
                        id = %intent.id,
                        version_id = intent.version_id,
                        "the node refused an intent's writes three times; the sweeper tries it again every \
                         5 minutes until 24 hours after it was enqueued (check the node's settings against \
                         crates/persistence/README.md, \"Node settings HFS depends on\")"
                    );
                }
                false
            }
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

    /// SW11 to SW16, SW19: one try of one intent, a refused write logged at `error`. The tests' entry
    /// point; the sweeper's own loop goes through [`Self::try_intent`] with its retry list's level.
    #[cfg(test)]
    pub(crate) async fn process_intent(&self, intent: &Intent, rank: Rank) -> IntentResult {
        self.try_intent(intent, rank, RefusalLog::Error).await
    }

    /// [`Self::process_intent`], with a refused write (`PermanentWrite`) logged at `refusal`
    /// ([`RetryList::refusal_log`]); every other invariant is logged at `error`.
    async fn try_intent(&self, intent: &Intent, rank: Rank, refusal: RefusalLog) -> IntentResult {
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
            (IntentCase::Current, Some(row)) => {
                self.replay_current(&key, intent, row, rank, refusal).await
            }
            (IntentCase::Superseded, Some(row)) => {
                self.replay_superseded(&key, intent, row, rank, refusal)
                    .await
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
                let level = if invariant == Invariant::PermanentWrite {
                    refusal
                } else {
                    RefusalLog::Error
                };
                log_refusal!(
                    level,
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
        // SW19: a SearchParameter that was replayed follows the overlay, from a read of its row taken
        // after the replay (ADR-aacruzgon-0099). The follow is stamped before that read, so a reload
        // that began after the stamp wins over it; a reload that began after the stamp also began
        // after the replay wrote the id's `resources_by_type` row, so it lists the id. The row the
        // replay used was read before the replay, and a reload during the replay can miss the id,
        // so following that row would either bring back a definition deleted since or, once the
        // stamp held it back, lose one the replay has just indexed.
        if matches!(case, IntentCase::Current | IntentCase::Superseded)
            && intent.resource_type == "SearchParameter"
        {
            // A test can hold the sweeper here, between R1 and the read the follow uses.
            #[cfg(test)]
            self.checkpoint(
                super::checkpoint::Checkpoint::SweeperBeforeFollow,
                &key,
                version,
            )
            .await;
            let read_at = self.search_parameter_read_stamp();
            match self.read_row(intent, StatementKind::Read, rank).await {
                Ok(fresh) => {
                    self.follow_stored_search_parameter(
                        &intent.tenant_id,
                        &intent.id,
                        fresh.as_ref(),
                        read_at,
                    );
                    self.sweeper.counters.followed();
                }
                // The replay is done and idempotent; the intent is kept, so it is replayed and
                // followed again on its next try. Logged and counted as R1's failure is.
                Err((failure, detail)) => {
                    return IntentResult {
                        case: Some(case),
                        index: replay.index,
                        noop: replay.noop,
                        ..self.stopped(intent, failure, &detail)
                    };
                }
            }
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

    /// Writes `delta` with the permits it needs, as the request does (IW17), a refused batch logged at
    /// `refusal`.
    async fn send_delta(
        &self,
        key: &RowKey<'_>,
        delta: &super::write::IndexDelta<'_>,
        ts: i64,
        rank: Rank,
        refusal: RefusalLog,
    ) -> (Result<(), Failure>, IndexWriteStats) {
        let _permit = self
            .sweeper
            .permits(rank, delta.units.len().clamp(1, INDEX_BATCHES_IN_FLIGHT))
            .await;
        let outcome = self
            .write_index_delta_logged(key, delta, ts, SWEEPER_TRIES, refusal)
            .await;
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
        refusal: RefusalLog,
    ) -> Replay {
        let version = intent.version_id;
        if row.write_ts != Some(write_ts_of(row.last_updated)) {
            return Replay::failed(Failure::Invariant(Invariant::WriteTsMismatch));
        }
        let marker = row.is_deleted();
        // A live row with no body is a bug, never an empty value (SW13).
        if !marker && row.data.is_none() {
            return Replay::failed(Failure::Invariant(Invariant::UnreadableRow));
        }
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
            self.write_version_row_logged(
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
                refusal,
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
        let (result, index) = self.send_delta(key, &delta, ts, rank, refusal).await;
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
        refusal: RefusalLog,
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
        let (result, index) = self.send_delta(key, &delta, ts, rank, refusal).await;
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
    }

    /// ADR-aacruzgon-0090: a write the node refused is tried three times at `RETRY_FIRST`, then every
    /// `RETRY_MAX` until the horizon. It never gives up, and a full list holds its mark. On the list, its
    /// first three tries log at `error` and the later ones at `warn` (`refusal_log`), also when its own
    /// entry fills the list; a full list that cannot take it does not count it toward its tries, and
    /// every try logs at `warn`. A bug never gets past `error`.
    #[test]
    fn a_refused_write_stays_on_the_retry_list_until_the_horizon() {
        let t0 = at("2026-11-22T10:00:00Z");
        let i = intent("i", t0);
        let refused = Failure::Invariant(Invariant::PermanentWrite);
        let mut list = RetryList::new(RETRY_LIST_CAP);
        let five = t0 + chrono::Duration::seconds(5);
        assert_eq!(list.refusal_log(&i), RefusalLog::Error, "try 1");
        assert_eq!(
            list.failed(i.clone(), refused, t0),
            Queued::Retry { next_at: five }
        );
        assert_eq!(list.refusal_log(&i), RefusalLog::Error, "try 2");
        assert_eq!(
            list.failed(i.clone(), refused, t0),
            Queued::Retry { next_at: five }
        );
        assert_eq!(list.refusal_log(&i), RefusalLog::Error, "try 3");
        let later = t0 + chrono::Duration::minutes(5);
        assert_eq!(
            list.failed(i.clone(), refused, t0),
            Queued::Refused {
                next_at: later,
                first: true
            }
        );
        assert_eq!(list.refusal_log(&i), RefusalLog::Warn, "try 4");
        assert!(list.owns(&i, t0));
        assert!(list.due(later - chrono::Duration::seconds(1)).is_empty());
        assert_eq!(list.due(later), vec![i.clone()]);
        let near = t0 + chrono::Duration::hours(24) - chrono::Duration::seconds(1);
        assert_eq!(
            list.failed(i.clone(), refused, near),
            Queued::Refused {
                next_at: near + chrono::Duration::minutes(5),
                first: false
            }
        );
        assert_eq!(list.len(), 1);
        assert_eq!(
            list.failed(i.clone(), refused, t0 + chrono::Duration::hours(24)),
            Queued::Expired
        );
        assert_eq!(list.len(), 0);

        let mut full = RetryList::new(1);
        let (a, b) = (intent("a", t0), intent("b", t0));
        assert!(matches!(
            full.failed(a, Failure::Transient("index"), t0),
            Queued::Retry { .. }
        ));
        for try_ in 1..=4 {
            assert_eq!(
                full.refusal_log(&b),
                RefusalLog::Warn,
                "overflowed try {try_}"
            );
            assert_eq!(full.failed(b.clone(), refused, t0), Queued::Overflow);
        }
        assert!(!full.owns(&b, t0));

        // On the list, its own entry fills a list of one: tries 1 to 3 still log at `error`, try 4 at
        // `warn`, as on a list with room.
        let mut own = RetryList::new(1);
        let c = intent("c", t0);
        for try_ in 1..=3 {
            assert_eq!(own.refusal_log(&c), RefusalLog::Error, "on-list try {try_}");
            assert!(matches!(
                own.failed(c.clone(), refused, t0),
                Queued::Retry { .. } | Queued::Refused { .. }
            ));
            assert_eq!(own.len(), 1);
        }
        assert_eq!(own.refusal_log(&c), RefusalLog::Warn, "on-list try 4");
        assert_eq!(
            own.failed(c.clone(), refused, t0),
            Queued::Refused {
                next_at: later,
                first: false
            }
        );

        // A bug gives up on its third try, and is logged at `error` on each.
        let m = intent("m", t0);
        let missing = Failure::Invariant(Invariant::MissingVersionRow);
        let mut bugs = RetryList::new(RETRY_LIST_CAP);
        for _ in 0..2 {
            assert!(matches!(
                bugs.failed(m.clone(), missing, t0),
                Queued::Retry { .. }
            ));
            assert_eq!(bugs.refusal_log(&m), RefusalLog::Error);
        }
        assert_eq!(bugs.failed(m.clone(), missing, t0), Queued::GaveUp);
        assert_eq!(bugs.refusal_log(&m), RefusalLog::Error);
    }

    /// ADR-aacruzgon-0090 and SW13 on a full list: refused writes the list cannot take are not counted
    /// toward their tries, so a data bug among them still gives up on its third try even with more
    /// refusals than `GAVE_UP_CAP` (more bugs than that may not, depending on how far past the cap
    /// they are, as the module doc says), every refusal logs at `warn`, and only the bugs are in
    /// `overflow_tries` (no insert, so no eviction scan, for a refusal). Each round, the list fails
    /// the same intents in the same order, as a held shard is read again on every poll: a bug
    /// first, the refusals, then a bug last.
    #[test]
    fn past_a_full_list_refusals_are_not_counted_and_a_bug_still_gives_up() {
        let t0 = at("2026-11-22T10:00:00Z");
        let refused = Failure::Invariant(Invariant::PermanentWrite);
        let missing = Failure::Invariant(Invariant::MissingVersionRow);
        // The control, well below the cap, and one past it.
        let mut counted = Vec::new();
        for n in [100, GAVE_UP_CAP + 1] {
            let mut list = RetryList::new(1);
            assert!(matches!(
                list.failed(intent("filler", t0), Failure::Transient("index"), t0),
                Queued::Retry { .. }
            ));
            let refusals: Vec<Intent> = (0..n).map(|k| intent(&format!("r{k:06}"), t0)).collect();
            let (first, last) = (intent("m-bug", t0), intent("z-bug", t0));
            for round in 1..=INVARIANT_TRIES {
                let bug_outcome = if round < INVARIANT_TRIES {
                    Queued::Overflow
                } else {
                    Queued::GaveUp
                };
                assert_eq!(
                    list.failed(first.clone(), missing, t0),
                    bug_outcome,
                    "n {n}, round {round}: the first bug"
                );
                for r in &refusals {
                    assert_eq!(
                        list.refusal_log(r),
                        RefusalLog::Warn,
                        "n {n}, round {round}"
                    );
                    assert_eq!(list.failed(r.clone(), refused, t0), Queued::Overflow);
                }
                assert_eq!(
                    list.failed(last.clone(), missing, t0),
                    bug_outcome,
                    "n {n}, round {round}: the last bug"
                );
                counted.push((n, round, list.overflow_tries.len()));
            }
            assert!(list.owns(&first, t0) && list.owns(&last, t0));
        }
        assert!(
            counted.iter().all(|(_, _, keys)| *keys <= 2),
            "keys counted off the list, by (n, round): {counted:?}"
        );
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

    /// SW8: a pending startup slice wholly older than the retry horizon is dropped, not read again.
    #[test]
    fn a_pending_slice_older_than_24_hours_is_dropped() {
        let now = at("2026-11-25T12:00:00Z");
        let state = SweepState::new(now, CancellationToken::new());
        let slice = |from: &str, to: &str| Slice {
            bucket: bucket_of(at(from)),
            shard: 0,
            from: at(from),
            to: at(to),
        };
        let old = slice("2026-11-24T10:00:00Z", "2026-11-24T10:01:00Z");
        let edge = slice("2026-11-24T11:59:00Z", "2026-11-24T12:00:00Z");
        let recent = slice("2026-11-25T11:00:00Z", "2026-11-25T11:01:00Z");
        for held in [&old, &edge, &recent] {
            state.hold_slice(held.clone(), 1, now - chrono::Duration::seconds(10));
        }
        let due = state.take_due_slices(now);
        let kept: Vec<_> = due.iter().map(|held| held.slice.clone()).collect();
        // `edge` ends exactly 24 hours before now: it is past the horizon too.
        assert_eq!(kept, vec![recent]);
        assert!(state.take_due_slices(now).is_empty());
    }

    /// Rows of one slice, in scope or not, decoded or not (the node plants nothing that does not decode).
    #[test]
    fn undecodable_intent_rows_count_only_in_scope() {
        let t0 = at("2026-11-25T10:00:00Z");
        let mut in_a = intent("a", t0);
        in_a.tenant_id = "test-a".into();
        let mut in_b = intent("b", t0);
        in_b.tenant_id = "other".into();
        let rows = || {
            vec![
                Ok(in_a.clone()),
                Ok(in_b.clone()),
                Err(Some("test-a".to_string())),
                Err(Some("other".to_string())),
                Err(None),
            ]
        };
        let (intents, undecodable) = in_scope(rows(), &SweepScope::Tenants(vec!["test-a".into()]));
        assert_eq!((intents, undecodable), (vec![in_a.clone()], 1));
        let (intents, undecodable) = in_scope(rows(), &SweepScope::TenantPrefix("test-".into()));
        assert_eq!((intents, undecodable), (vec![in_a.clone()], 1));
        let (intents, undecodable) = in_scope(rows(), &SweepScope::All);
        assert_eq!((intents, undecodable), (vec![in_a, in_b], 3));
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

    /// SW28: a grace past what a timestamp can hold gives an empty pass, never a panic; below that, the
    /// pass ends at `now - grace` in whole milliseconds.
    #[test]
    fn a_pass_with_a_grace_past_the_timestamp_range_ends_nowhere() {
        let now = at("2026-11-26T10:00:00.123456Z");
        assert_eq!(pass_end(now, Duration::MAX), None);
        // Inside `chrono::Duration`'s range, outside `DateTime`'s.
        assert_eq!(
            pass_end(now, Duration::from_secs(5_000_000_000_000_000)),
            None
        );
        assert_eq!(
            pass_end(now, Duration::from_secs(30)),
            Some(at("2026-11-26T09:59:30.123Z"))
        );
        assert!(pass_end(now, Duration::from_secs(1_000_000_000_000)).is_some());
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

    /// Every event's level, message and `u64` fields, recorded by a `tracing` subscriber set as this thread's default
    /// ([`Captured::install`]). A `#[tokio::test]` runs on one thread, and the sweeper polls its intents
    /// and their batches as futures inside that task, never `tokio::spawn`, so its events land here;
    /// other tests' threads keep their own. It needs no new dev-dependency.
    #[derive(Clone, Default)]
    struct Captured(Arc<parking_lot::Mutex<Vec<CapturedEvent>>>);

    /// One event: its level, its message, and its `u64` fields by name.
    struct CapturedEvent {
        level: tracing::Level,
        message: String,
        numbers: Vec<(&'static str, u64)>,
    }

    impl Captured {
        /// Sets this subscriber as the thread's default until the guard is dropped, with a second
        /// dispatcher kept registered beside it. While only one dispatcher is registered, `tracing-core`
        /// (0.1.36, `Rebuilder::JustOne`) caches a callsite's interest from the default of whichever
        /// thread hits it first. Another test's thread has none, so a callsite it hit first was cached
        /// as never enabled, and this thread's events from it were lost: under `backends::cassandra` at
        /// 24 threads, the refused-replay test failed 4 runs in 20 (the branch audit's re-check, R2).
        /// With two registered, every callsite's interest comes from both, and both always want it.
        fn install(&self) -> (tracing::subscriber::DefaultGuard, tracing::Dispatch) {
            let keep_interest = tracing::Dispatch::new(Captured::default());
            (
                tracing::subscriber::set_default(self.clone()),
                keep_interest,
            )
        }

        /// Events at `level` whose message starts with `text`.
        fn count(&self, level: tracing::Level, text: &str) -> usize {
            self.0
                .lock()
                .iter()
                .filter(|event| event.level == level && event.message.starts_with(text))
                .count()
        }

        /// The `u64` field `field` of each event at `level` whose message starts with `text`, in order
        /// (`None` where an event has no such field).
        fn numbers(&self, level: tracing::Level, text: &str, field: &str) -> Vec<Option<u64>> {
            self.0
                .lock()
                .iter()
                .filter(|event| event.level == level && event.message.starts_with(text))
                .map(|event| {
                    event
                        .numbers
                        .iter()
                        .find(|(name, _)| *name == field)
                        .map(|(_, value)| *value)
                })
                .collect()
        }

        /// The events at `warn` or `error`, for a failed assertion's message.
        fn all(&self) -> Vec<(tracing::Level, String)> {
            self.0
                .lock()
                .iter()
                .filter(|event| event.level <= tracing::Level::WARN)
                .map(|event| (event.level, event.message.clone()))
                .collect()
        }
    }

    impl tracing::Subscriber for Captured {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Message(String, Vec<(&'static str, u64)>);
            impl tracing::field::Visit for Message {
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    if field.name() == "message" {
                        self.0 = value.to_string();
                    }
                }
                fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                    self.1.push((field.name(), value));
                }
                fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
                    if field.name() == "message" {
                        self.0 = format!("{value:?}");
                    }
                }
            }
            let mut message = Message(String::new(), Vec::new());
            event.record(&mut message);
            self.0.lock().push(CapturedEvent {
                level: *event.metadata().level(),
                message: message.0,
                numbers: message.1,
            });
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

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

    /// A grace-0 pass over every intent enqueued before this call. The pass reads `[since, now)` in whole
    /// milliseconds (SW7), so its `now` is one millisecond past the call, as `pending`'s `to` is: at
    /// `Utc::now()` a pass taken in the same millisecond as an intent's stamp misses it (SW6's test, after
    /// an I1 write of under a millisecond).
    async fn pass(
        backend: &CassandraBackend,
        tenants: &[&str],
        since: DateTime<Utc>,
    ) -> SweepReport {
        backend
            .sweep_pass(SweepOptions {
                scope: SweepScope::Tenants(tenants.iter().map(|t| t.to_string()).collect()),
                grace: Duration::ZERO,
                now: Utc::now() + chrono::Duration::milliseconds(1),
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
            let intents = backend
                .read_slice(&slice, Rank::Normal)
                .await
                .unwrap()
                .decoded();
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
            let read = backend
                .read_slice(slice, Rank::Normal)
                .await
                .unwrap()
                .decoded();
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
            let read = backend
                .read_slice(slice, Rank::Normal)
                .await
                .unwrap()
                .decoded();
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
        // The request's intent and the fresh one of the unfinished write.
        assert_eq!(intents(&backend, &t, "p", since).await, vec![2, 2]);

        let v3 = backend.update(&tenant, &v2, patient()).await.unwrap();
        assert_eq!(v3.version_id(), "3");
        let lu3 = row_of(&backend, &t, "p").await.last_updated;
        assert_eq!(
            version_ids(&backend, &t, "Patient", "p").await,
            vec![1, 2, 3]
        );
        assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu3, 3), (lu1, 1)]);
        assert_eq!(count(&backend, &tenant).await, 2);
        assert_eq!(intents(&backend, &t, "p", since).await, vec![2, 2]);

        let report = pass(&backend, &[&t], since).await;
        assert_eq!((report.superseded, report.done), (2, 2), "{report}");
        assert!(report.is_complete());
        assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu3, 3)]);
        assert_eq!(count(&backend, &tenant).await, 1);
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
        say("iw24", &backend, &before);
    }

    /// SW6: a writer stalled between I1 and its CAS for longer than the grace has its intent swept as "no
    /// row". When its CAS then applies and its version row fails, `finish` leaves a fresh intent, so the
    /// sweeper still finishes the write.
    #[tokio::test]
    async fn a_writer_delayed_past_its_swept_intent_leaves_a_new_intent() {
        let Some((backend, t, tenant, since)) = start("sweeper-stalled").await else {
            return;
        };
        let version = test_fhir_version();
        let mut hold = backend.hold_at(Checkpoint::AfterIntent, &t, "p", 1);
        let (b, ten) = (backend.clone(), tenant.clone());
        let task = tokio::spawn(async move { b.create(&ten, "Patient", patient(), version).await });
        hold.reached().await;
        // The sweep takes I1 for a CAS that never ran.
        let swept = pass(&backend, &[&t], since).await;
        assert_eq!((swept.no_row, swept.done), (1, 1), "{swept}");
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        for _ in 0..3 {
            backend.session.inject_fault(
                StatementKind::Write,
                "INSERT INTO resource_versions",
                Fault::TimeOutWithoutApplying,
            );
        }
        hold.release();
        task.await.unwrap().expect("the create applied");
        assert_eq!(intents(&backend, &t, "p", since).await, vec![1]);
        assert!(rbt_rows(&backend, &t, "p").await.is_empty());
        assert_eq!(count(&backend, &tenant).await, 0);

        let report = pass(&backend, &[&t], since).await;
        assert_eq!((report.current, report.done), (1, 1), "{report}");
        assert_eq!(version_ids(&backend, &t, "Patient", "p").await, vec![1]);
        assert_eq!(rbt_rows(&backend, &t, "p").await.len(), 1);
        assert_eq!(count(&backend, &tenant).await, 1);
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
    }

    /// ADR-aacruzgon-0098: a backend whose grace (4 s) is SW4's least for its 2 s deadline, so a
    /// test can hold a writer past it in real time; with a fresh tenant and `since`. Its connect
    /// timeout (1 s) stays under the request timeout, as #121's check requires.
    async fn start_short_grace(
        prefix: &str,
    ) -> Option<(Arc<CassandraBackend>, String, TenantContext, DateTime<Utc>)> {
        let Ok(url) = std::env::var("HFS_TEST_CASSANDRA_URL") else {
            eprintln!("{SKIP}");
            return None;
        };
        if url.trim().is_empty() {
            eprintln!("{SKIP}");
            return None;
        }
        let backend = Arc::new(
            CassandraBackend::connect(crate::backends::cassandra::CassandraConfig {
                contact_points: vec![url],
                // Under the request timeout, as `connect` to a local node needs far less.
                connect_timeout_ms: 1_000,
                request_timeout_ms: 2_000,
                sweeper_poll_interval_ms: 1_000,
                sweeper_grace_ms: 4_000,
                ..crate::backends::cassandra::test_support::test_config()
            })
            .await
            .expect("connect"),
        );
        let t = tenant_id(prefix);
        let tenant = context(&t);
        Some((
            backend,
            t,
            tenant,
            Utc::now() - chrono::Duration::seconds(5),
        ))
    }

    /// ADR-aacruzgon-0098: a running sweeper's state for `t` (marks two minutes back) and its scope.
    fn running(t: &str) -> (SweepState, SweepScope) {
        (
            SweepState::new(
                Utc::now() - chrono::Duration::minutes(2),
                CancellationToken::new(),
            ),
            SweepScope::Tenants(vec![t.to_string()]),
        )
    }

    /// ADR-aacruzgon-0098: a create held at `held` for longer than the grace while a running sweeper
    /// polls, then crashed after its CAS. Returns the poll made during the hold and the CAS counters
    /// the create spent.
    async fn held_past_the_grace_then_crashed(
        backend: &Arc<CassandraBackend>,
        held: Checkpoint,
        t: &str,
        tenant: &TenantContext,
        state: &SweepState,
        scope: &SweepScope,
    ) -> (SweepReport, crate::backends::cassandra::CasStats) {
        let before = backend.cas_stats();
        let mut hold = backend.hold_at(held, t, "p", 1);
        let mut cas = backend.hold_at(Checkpoint::AfterCas, t, "p", 1);
        let (b, ten) = (backend.clone(), tenant.clone());
        let task = tokio::spawn(async move {
            b.create(&ten, "Patient", patient(), test_fhir_version())
                .await
        });
        hold.reached().await;
        // A real stall, past the grace since the stamp; the sweeper polls meanwhile.
        tokio::time::sleep(backend.config.sweeper_grace() + Duration::from_millis(200)).await;
        let during = backend.poll_once(state, Utc::now(), scope).await;
        hold.release();
        cas.reached().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        (during, backend.cas_stats().since(&before))
    }

    /// ADR-aacruzgon-0098 (CodeRabbit C6 on #107): a writer held between its intent's stamp and I1
    /// for longer than the grace writes its intent after a running sweeper's marks passed its
    /// stamp. It writes a fresh intent before its CAS, so when it crashes after the CAS the next
    /// poll still finishes the write; without the fresh intent only a restart's startup scan would.
    #[tokio::test]
    async fn a_writer_held_before_its_intent_past_the_grace_writes_a_fresh_one() {
        let Some((backend, t, tenant, since)) =
            start_short_grace("sweeper-held-before-intent").await
        else {
            return;
        };
        let (state, scope) = running(&t);
        let (during, spent) = held_past_the_grace_then_crashed(
            &backend,
            Checkpoint::AfterStamp,
            &t,
            &tenant,
            &state,
            &scope,
        )
        .await;
        assert_eq!(
            during.intents, 0,
            "nothing to see during the hold: {during}"
        );
        let left = intents(&backend, &t, "p", since).await;
        assert!(rbt_rows(&backend, &t, "p").await.is_empty());

        let grace = chrono::Duration::from_std(backend.config.sweeper_grace()).unwrap();
        let after = backend
            .poll_once(
                &state,
                Utc::now() + grace + chrono::Duration::seconds(1),
                &scope,
            )
            .await;
        assert_eq!(
            (after.current, after.done),
            (1, 1),
            "the running sweeper finishes the crashed write: {after}"
        );
        assert_eq!(rbt_rows(&backend, &t, "p").await.len(), 1);
        assert_eq!(count(&backend, &tenant).await, 1);
        assert_eq!(version_ids(&backend, &t, "Patient", "p").await, vec![1]);
        assert_eq!(spent.intent_restamps, 1, "{spent:?}");
        assert_eq!(left, vec![1, 1], "the first intent and the fresh one");
        // The first intent, behind the marks, is left for a startup scan, whose replay is harmless (SW22).
        assert_eq!(intents(&backend, &t, "p", since).await, vec![1]);
        assert_consistent(&check(&backend, &t).await);
    }

    /// ADR-aacruzgon-0098: a writer held between I1 and its CAS for longer than the grace has its
    /// intent deleted by a running sweeper as "no row". It writes a fresh intent before its CAS, so
    /// when it crashes after the CAS the next poll still finishes the write; without it nothing
    /// would, not even a startup scan, until the resource's next write.
    #[tokio::test]
    async fn a_writer_held_after_its_intent_past_the_grace_writes_a_fresh_one() {
        let Some((backend, t, tenant, since)) =
            start_short_grace("sweeper-held-after-intent").await
        else {
            return;
        };
        let (state, scope) = running(&t);
        let (during, spent) = held_past_the_grace_then_crashed(
            &backend,
            Checkpoint::AfterIntent,
            &t,
            &tenant,
            &state,
            &scope,
        )
        .await;
        assert_eq!((during.no_row, during.done), (1, 1), "{during}");
        let left = intents(&backend, &t, "p", since).await;
        assert!(rbt_rows(&backend, &t, "p").await.is_empty());

        let grace = chrono::Duration::from_std(backend.config.sweeper_grace()).unwrap();
        let after = backend
            .poll_once(
                &state,
                Utc::now() + grace + chrono::Duration::seconds(1),
                &scope,
            )
            .await;
        assert_eq!(
            (after.current, after.done),
            (1, 1),
            "the running sweeper finishes the crashed write: {after}"
        );
        assert_eq!(rbt_rows(&backend, &t, "p").await.len(), 1);
        assert_eq!(count(&backend, &tenant).await, 1);
        assert_eq!(spent.intent_restamps, 1, "{spent:?}");
        assert_eq!(left, vec![1], "the fresh intent");
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
    }

    /// ADR-aacruzgon-0098 (the audit of #123, F4): a writer held again after its fresh intent, past
    /// the grace, has that intent deleted by a running sweeper too. The check runs again and writes
    /// a second fresh intent, so the next poll after a crash still finishes the write; a single
    /// re-stamp (`if` for `while`) would leave no intent.
    #[tokio::test]
    async fn a_writer_held_again_after_its_fresh_intent_writes_another() {
        let Some((backend, t, tenant, since)) = start_short_grace("sweeper-held-twice").await
        else {
            return;
        };
        let (state, scope) = running(&t);
        let grace = backend.config.sweeper_grace();
        let timeout = Duration::from_millis(backend.config.request_timeout_ms);
        let before = backend.cas_stats();
        let mut stamp = backend.hold_at(Checkpoint::AfterStamp, &t, "p", 1);
        let mut fresh = backend.hold_at(Checkpoint::AfterFreshIntent, &t, "p", 1);
        let mut cas = backend.hold_at(Checkpoint::AfterCas, &t, "p", 1);
        let (b, ten) = (backend.clone(), tenant.clone());
        let task = tokio::spawn(async move {
            b.create(&ten, "Patient", patient(), test_fhir_version())
                .await
        });
        stamp.reached().await;
        // Long enough for the first re-stamp (held + timeout reaches the grace), not for a sweep.
        tokio::time::sleep(grace - timeout + Duration::from_millis(200)).await;
        stamp.release();
        fresh.reached().await;
        // Held again, past the grace since the fresh stamp: the sweeper deletes both intents.
        tokio::time::sleep(grace + Duration::from_millis(200)).await;
        let during = backend.poll_once(&state, Utc::now(), &scope).await;
        assert_eq!((during.no_row, during.done), (2, 2), "{during}");
        fresh.release();
        cas.reached().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let spent = backend.cas_stats().since(&before);
        let left = intents(&backend, &t, "p", since).await;

        let after = backend
            .poll_once(
                &state,
                Utc::now()
                    + chrono::Duration::from_std(grace).unwrap()
                    + chrono::Duration::seconds(1),
                &scope,
            )
            .await;
        assert_eq!(
            (after.current, after.done),
            (1, 1),
            "the running sweeper finishes the crashed write: {after}"
        );
        assert_eq!(rbt_rows(&backend, &t, "p").await.len(), 1);
        assert_eq!(count(&backend, &tenant).await, 1);
        assert_eq!(spent.intent_restamps, 2, "{spent:?}");
        assert_eq!(left, vec![1], "the second fresh intent");
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
    }

    /// ADR-aacruzgon-0098's control: a writer held only briefly writes no fresh intent, and the
    /// running sweeper finishes its crash from the one intent it has.
    #[tokio::test]
    async fn a_writer_held_briefly_writes_no_fresh_intent() {
        let Some((backend, t, tenant, since)) = start_short_grace("sweeper-held-briefly").await
        else {
            return;
        };
        let (state, scope) = running(&t);
        let before = backend.cas_stats();
        let mut hold = backend.hold_at(Checkpoint::AfterStamp, &t, "p", 1);
        let mut cas = backend.hold_at(Checkpoint::AfterCas, &t, "p", 1);
        let (b, ten) = (backend.clone(), tenant.clone());
        let task = tokio::spawn(async move {
            b.create(&ten, "Patient", patient(), test_fhir_version())
                .await
        });
        hold.reached().await;
        hold.release();
        cas.reached().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let spent = backend.cas_stats().since(&before);
        assert_eq!(spent.intent_restamps, 0, "{spent:?}");
        assert_eq!(intents(&backend, &t, "p", since).await, vec![1]);

        let grace = chrono::Duration::from_std(backend.config.sweeper_grace()).unwrap();
        let after = backend
            .poll_once(
                &state,
                Utc::now() + grace + chrono::Duration::seconds(1),
                &scope,
            )
            .await;
        assert_eq!((after.current, after.done), (1, 1), "{after}");
        assert_eq!(rbt_rows(&backend, &t, "p").await.len(), 1);
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
    }

    /// N6 (exit criterion): the running loop heals a crash within poll + grace. SW4 sets the grace (10 s) from
    /// the request timeout (5 s). With a node of three down, `connect`'s local DC check can try that node
    /// first, so the connect timeout (2 s) is kept well under the request timeout: at the default 5 s the try
    /// used the whole deadline, and `connect` failed in 1 of 3 runs.
    #[tokio::test]
    async fn a_crash_after_the_cas_heals_within_poll_plus_grace() {
        let Some((backend, t, tenant, since)) = start("sweeper-loop").await else {
            return;
        };
        let url = std::env::var("HFS_TEST_CASSANDRA_URL").unwrap();
        let looped = Arc::new(
            CassandraBackend::connect(crate::backends::cassandra::CassandraConfig {
                contact_points: vec![url],
                connect_timeout_ms: 2_000,
                request_timeout_ms: 5_000,
                sweeper_poll_interval_ms: 1_000,
                sweeper_grace_ms: 10_000,
                ..crate::backends::cassandra::test_support::test_config()
            })
            .await
            .expect("connect"),
        );
        // The loop starts before the write, so its first mark (start - grace) is older than the intent.
        let cancel = CancellationToken::new();
        let task = tokio::spawn(looped.clone().run_sweeper_with(
            cancel.clone(),
            SweepScope::Tenants(vec![t.clone()]),
            false,
        ));
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
        // Grace 10 s and a 1 s poll, so about 11 s; the bound leaves 5 s over that.
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
        // One millisecond past now, as `pass` does: the window's bound is in whole milliseconds (SW7).
        let options = SweepOptions {
            scope: SweepScope::Tenants(vec![t.clone()]),
            grace: Duration::ZERO,
            now: Utc::now() + chrono::Duration::milliseconds(1),
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
        // The request's intent and the fresh one of the unfinished write.
        assert_eq!((report.current, report.done), (2, 2), "{report}");
        let delta = backend.sweeper_stats().since(&before);
        assert_eq!(delta.search_parameters_followed, 2);
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

    /// What instance B does to the SearchParameter before instance A reloads.
    #[derive(Clone, Copy, Debug)]
    enum Change {
        /// B updates it to `status: retired`; A created it, so A held it.
        Retire,
        /// B deletes it; A created it, so A held it.
        Delete,
        /// B created it and deletes it, so A never held it, as when A's sweeper finishes B's
        /// write.
        DeleteUnheld,
    }

    /// SW19 across instances (ADR-aacruzgon-0099): B makes `change`, A reloads, then A follows a
    /// row as a replay does: the one it read before B's change (`stale`), stamped before that read,
    /// or, as the control, one it stamps and reads after its reload. Returns whether the definition
    /// is in A's registry after the follow, and after A's next reload.
    async fn follow_after_a_reload_of(change: Change, stale: bool) -> Option<(bool, bool)> {
        let (a, t, tenant, _) = start("sweeper-sp-stale").await?;
        let b = harness::backend().await.expect("instance B");
        let url = format!("http://example.org/SearchParameter/{t}");
        let creator = if matches!(change, Change::DeleteUnheld) {
            &b
        } else {
            &a
        };
        let v1 = creator
            .create(
                &tenant,
                "SearchParameter",
                search_parameter(&url),
                test_fhir_version(),
            )
            .await
            .expect("create");
        assert_eq!(
            expression_of(&a, &tenant, &url).is_some(),
            !matches!(change, Change::DeleteUnheld),
            "A holds what A created"
        );
        let stamp = a.search_parameter_read_stamp();
        let before = a
            .serial_read_resource_row(&t, "SearchParameter", "sp")
            .await
            .unwrap()
            .unwrap();
        match change {
            Change::Retire => {
                let mut retired = search_parameter(&url);
                retired["status"] = serde_json::json!("retired");
                b.update(&tenant, &v1, retired).await.expect("B's update");
            }
            Change::Delete | Change::DeleteUnheld => b
                .delete(&tenant, "SearchParameter", "sp")
                .await
                .expect("B's delete"),
        }
        assert_eq!(expression_of(&b, &tenant, &url), None);
        a.load_stored_search_parameters(&t).await.unwrap();
        assert_eq!(expression_of(&a, &tenant, &url), None, "after A's reload");
        let (row, read_at) = if stale {
            (before, stamp)
        } else {
            let read_at = a.search_parameter_read_stamp();
            let row = a
                .serial_read_resource_row(&t, "SearchParameter", "sp")
                .await
                .unwrap()
                .unwrap();
            (row, read_at)
        };
        a.follow_stored_search_parameter(&t, "sp", Some(&row), read_at);
        let followed = expression_of(&a, &tenant, &url).is_some();
        a.load_stored_search_parameters(&t).await.unwrap();
        Some((followed, expression_of(&a, &tenant, &url).is_some()))
    }

    /// Runs [`follow_after_a_reload_of`] with a stale read and with the fresh control: neither may
    /// leave the definition in force, after the follow or after the next reload.
    async fn neither_read_brings_it_back(change: Change) {
        let Some(stale) = follow_after_a_reload_of(change, true).await else {
            return;
        };
        let control = follow_after_a_reload_of(change, false).await.unwrap();
        // Each is (in force after the follow, in force after the next reload).
        assert_eq!(
            (stale, control),
            ((false, false), (false, false)),
            "{change:?}: (the stale read, a fresh read)"
        );
    }

    /// SW19 across instances: a replay's row read before another instance retired the
    /// SearchParameter does not bring it back into this instance's registry after this instance's
    /// reload saw the retirement; neither does a fresh read (the control). The next reload agrees.
    #[tokio::test]
    async fn a_stale_replay_read_does_not_bring_back_a_retired_searchparameter() {
        neither_read_brings_it_back(Change::Retire).await;
    }

    /// SW19 across instances (ADR-aacruzgon-0099): the same for a delete, which leaves the reload
    /// no `resources_by_type` row to list, so the reload records no removal; the read stamp alone
    /// holds the stale read back.
    #[tokio::test]
    async fn a_stale_replay_read_does_not_bring_back_a_deleted_searchparameter() {
        neither_read_brings_it_back(Change::Delete).await;
    }

    /// SW19 across instances (ADR-aacruzgon-0099): the same for a SearchParameter this instance
    /// never held, created and deleted by another instance, as when this instance's sweeper
    /// finishes the other's write.
    #[tokio::test]
    async fn a_stale_replay_read_does_not_put_in_a_searchparameter_deleted_elsewhere() {
        neither_read_brings_it_back(Change::DeleteUnheld).await;
    }

    /// ADR-aacruzgon-0099 (audit F1): A reloads, reading v1; B updates to v2; A reads v2 and follows
    /// it, stamped before its reload began (`early`) or, as the control, after it. The reload read
    /// the id at an older version, so A's read came after the reload's: v2 is in force either way.
    async fn follow_of_a_read_newer_than_the_reloads(early: bool) -> Option<Option<String>> {
        let (a, t, tenant, _) = start("sweeper-sp-newer").await?;
        let b = harness::backend().await.expect("instance B");
        let url = format!("http://example.org/SearchParameter/{t}");
        let v1 = a
            .create(
                &tenant,
                "SearchParameter",
                search_parameter(&url),
                test_fhir_version(),
            )
            .await
            .expect("create");
        let before = a.search_parameter_read_stamp();
        assert_eq!(
            a.load_stored_search_parameters(&t).await.unwrap(),
            1,
            "A's reload reads v1"
        );
        let after = a.search_parameter_read_stamp();
        let mut v2 = search_parameter(&url);
        v2["expression"] = serde_json::json!("Patient.gender");
        b.update(&tenant, &v1, v2).await.expect("B's update to v2");
        let row = a
            .serial_read_resource_row(&t, "SearchParameter", "sp")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.version_id, 2);
        a.follow_stored_search_parameter(&t, "sp", Some(&row), if early { before } else { after });
        Some(expression_of(&a, &tenant, &url))
    }

    #[tokio::test]
    async fn a_follow_of_a_read_newer_than_the_reloads_is_kept() {
        let Some(early) = follow_of_a_read_newer_than_the_reloads(true).await else {
            return;
        };
        let late = follow_of_a_read_newer_than_the_reloads(false)
            .await
            .unwrap();
        assert_eq!(
            (early.as_deref(), late.as_deref()),
            (Some("Patient.gender"), Some("Patient.gender")),
            "(stamped before the reload, stamped after it)"
        );
    }

    /// ADR-aacruzgon-0099 (CodeRabbit on #122): as [`follow_of_a_read_newer_than_the_reloads`],
    /// stamped before the reload began, with v1's body one the loader cannot parse (`"type":
    /// "invalid"`) or, as the control, a valid one. The follow is put after the reload merged
    /// (`merged_first`) or between its reads and its merge. The reload read the id at v1 either
    /// way, so A's read of v2 came after it, and v2 is in force.
    async fn follow_after_a_reload_read_v1(
        parses: bool,
        merged_first: bool,
    ) -> Option<Option<String>> {
        let (a, t, tenant, _) = start("sweeper-sp-unparsed").await?;
        let b = harness::backend().await.expect("instance B");
        let url = format!("http://example.org/SearchParameter/{t}");
        let mut body = search_parameter(&url);
        if !parses {
            body["type"] = serde_json::json!("invalid");
        }
        let v1 = a
            .create(&tenant, "SearchParameter", body, test_fhir_version())
            .await
            .expect("create");
        let before = a.search_parameter_read_stamp();
        let follow_v2 = async {
            let mut v2 = search_parameter(&url);
            v2["expression"] = serde_json::json!("Patient.gender");
            b.update(&tenant, &v1, v2).await.expect("B's update to v2");
            let row = a
                .serial_read_resource_row(&t, "SearchParameter", "sp")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(row.version_id, 2);
            a.follow_stored_search_parameter(&t, "sp", Some(&row), before);
        };
        if merged_first {
            a.load_stored_search_parameters(&t).await.unwrap();
            follow_v2.await;
        } else {
            a.load_stored_search_parameters_around(&t, follow_v2)
                .await
                .unwrap();
        }
        Some(expression_of(&a, &tenant, &url))
    }

    #[tokio::test]
    async fn a_follow_newer_than_an_unparsed_version_the_reload_read_is_kept() {
        let Some(first) = follow_after_a_reload_read_v1(false, true).await else {
            return;
        };
        let mut outcomes = vec![first];
        for (parses, merged_first) in [(false, false), (true, true), (true, false)] {
            outcomes.push(
                follow_after_a_reload_read_v1(parses, merged_first)
                    .await
                    .unwrap(),
            );
        }
        let gender = Some("Patient.gender".to_string());
        assert_eq!(
            outcomes,
            [gender.clone(), gender.clone(), gender.clone(), gender],
            "(v1 does not parse: reload merged first, follow put first; v1 parses: the same)"
        );
    }

    /// ADR-aacruzgon-0099 (CodeRabbit on #122): an id whose newest version is one the loader cannot
    /// parse. A reads v0 (valid, `Patient.active`); B updates it to v1, which does not parse (or, as
    /// the control, to a valid v1, `Patient.gender`); A reloads, then follows its read of v0, stamped
    /// after the reload began so `since` does not hold it back. Returns the expression in force.
    async fn stale_follow_after_a_reload_of(v1_parses: bool) -> Option<Option<String>> {
        let (a, t, tenant, _) = start("sweeper-sp-unparsed-newest").await?;
        let b = harness::backend().await.expect("instance B");
        let url = format!("http://example.org/SearchParameter/{t}");
        let v0 = b
            .create(
                &tenant,
                "SearchParameter",
                search_parameter(&url),
                test_fhir_version(),
            )
            .await
            .expect("B's create");
        let stale = a
            .serial_read_resource_row(&t, "SearchParameter", "sp")
            .await
            .unwrap()
            .unwrap();
        let mut v1 = search_parameter(&url);
        if v1_parses {
            v1["expression"] = serde_json::json!("Patient.gender");
        } else {
            v1["type"] = serde_json::json!("invalid");
        }
        b.update(&tenant, &v0, v1).await.expect("B's update to v1");
        a.load_stored_search_parameters(&t).await.unwrap();
        let after = a.search_parameter_read_stamp();
        a.follow_stored_search_parameter(&t, "sp", Some(&stale), after);
        Some(expression_of(&a, &tenant, &url))
    }

    #[tokio::test]
    async fn a_stale_follow_does_not_bring_back_a_version_an_unparsed_one_replaced() {
        let Some(unparsed) = stale_follow_after_a_reload_of(false).await else {
            return;
        };
        let control = stale_follow_after_a_reload_of(true).await.unwrap();
        assert_eq!(
            (unparsed.as_deref(), control.as_deref()),
            (None, Some("Patient.gender")),
            "(v1 does not parse, v1 parses)"
        );
    }

    /// ADR-aacruzgon-0099 (CodeRabbit on #122): two reloads of one tenant on A, called at once. The
    /// follow takes its stamp; A's first reload reads B's v1 and, before it merges, B updates to v2
    /// (`Patient.gender`), A reads v2 for the follow, B deletes the SearchParameter (`delete`) or, as
    /// the control, does not, and a second reload is started. With the delete, the first reload
    /// awaits the second for up to [`SECOND_RELOAD_WAIT`] before it merges: without the lock the
    /// second finishes in that time, whatever the load, and merges first; with it, it waits for the
    /// first. The follow is put after both merged. Returns what is in force before the follow and
    /// after it, and whether the second reload finished before the first merged.
    async fn overlapping_reloads(delete: bool) -> Option<(Option<String>, Option<String>, bool)> {
        let (a, t, tenant, _) = start("sweeper-sp-overlap").await?;
        let b = harness::backend().await.expect("instance B");
        let url = format!("http://example.org/SearchParameter/{t}");
        let v1 = b
            .create(
                &tenant,
                "SearchParameter",
                search_parameter(&url),
                test_fhir_version(),
            )
            .await
            .expect("B's create");
        let stamp = a.search_parameter_read_stamp();
        let mut row = None;
        let mut second = None;
        a.load_stored_search_parameters_around(&t, async {
            let mut v2 = search_parameter(&url);
            v2["expression"] = serde_json::json!("Patient.gender");
            b.update(&tenant, &v1, v2).await.expect("B's update to v2");
            row = a
                .serial_read_resource_row(&t, "SearchParameter", "sp")
                .await
                .unwrap();
            if delete {
                b.delete(&tenant, "SearchParameter", "sp")
                    .await
                    .expect("B's delete");
            }
            let mut task = tokio::spawn({
                let (a, t) = (a.clone(), t.clone());
                async move { a.load_stored_search_parameters(&t).await }
            });
            // A finished `JoinHandle` cannot be awaited again, so its result is kept.
            let finished = if delete {
                tokio::time::timeout(SECOND_RELOAD_WAIT, &mut task)
                    .await
                    .ok()
            } else {
                None
            };
            second = Some((task, finished));
        })
        .await
        .unwrap();
        let (task, finished) = second.unwrap();
        let overlapped = finished.is_some();
        match finished {
            Some(result) => result,
            None => task.await,
        }
        .expect("the second reload")
        .unwrap();
        let row = row.expect("v2's row");
        assert_eq!(row.version_id, 2);
        let before = expression_of(&a, &tenant, &url);
        a.follow_stored_search_parameter(&t, "sp", Some(&row), stamp);
        Some((before, expression_of(&a, &tenant, &url), overlapped))
    }

    /// How long the first reload of [`overlapping_reloads`] awaits the second, which only the lock
    /// holds back; a reload of one SearchParameter takes milliseconds.
    const SECOND_RELOAD_WAIT: std::time::Duration = std::time::Duration::from_secs(10);

    #[tokio::test]
    async fn a_reload_called_during_another_does_not_bring_back_a_deleted_searchparameter() {
        let Some(deleted) = overlapping_reloads(true).await else {
            return;
        };
        let control = overlapping_reloads(false).await.unwrap();
        let outcome = |(before, after, _): &(Option<String>, Option<String>, bool)| {
            (before.clone(), after.clone())
        };
        let gender = Some("Patient.gender".to_string());
        assert_eq!(
            (outcome(&deleted), outcome(&control)),
            ((None, None), (gender.clone(), gender)),
            "((B deleted it: before the follow, after it), (B did not: the same)); the second \
             reload finished before the first merged: {}, {}",
            deleted.2,
            control.2
        );
        assert!(!deleted.2, "the second reload waits for the first");
    }

    /// A SearchParameter created with every `resources_by_type` insert failing, so its intent is
    /// left for the sweeper, and dropped from the overlay by a reload, so only a follow puts it back.
    /// Returns the backend, the tenant, its context, the `url` and the intent.
    async fn a_searchparameter_left_for_the_sweeper(
        prefix: &str,
    ) -> Option<(Arc<CassandraBackend>, String, TenantContext, String, Intent)> {
        let (a, t, tenant, since) = start(prefix).await?;
        let url = format!("http://example.org/SearchParameter/{t}");
        for _ in 0..3 {
            a.session.inject_fault(
                StatementKind::Write,
                "INSERT INTO resources_by_type",
                Fault::TimeOutWithoutApplying,
            );
        }
        a.create(
            &tenant,
            "SearchParameter",
            search_parameter(&url),
            test_fhir_version(),
        )
        .await
        .expect("create");
        assert_eq!(a.load_stored_search_parameters(&t).await.unwrap(), 0);
        assert_eq!(expression_of(&a, &tenant, &url), None);
        let found = pending(&a, &t, "sp", since).await;
        let intent = found.first().expect("the create's intent").clone();
        Some((a, t, tenant, url, intent))
    }

    /// SW19 (ADR-aacruzgon-0099): a replay's follow uses a read taken after the replay, not R1's row.
    /// The sweeper is held between the replay and that read while B deletes the SearchParameter
    /// (`delete`), or, as the control, while nothing happens. Returns whether it is in A's registry
    /// after the intent is processed.
    async fn replay_held_before_its_follow(delete: bool) -> Option<bool> {
        let (a, t, tenant, url, intent) =
            a_searchparameter_left_for_the_sweeper("sweeper-sp-reread").await?;
        let b = harness::backend().await.expect("instance B");
        let mut hold = a.hold_at(Checkpoint::SweeperBeforeFollow, &t, "sp", intent.version_id);
        let task = tokio::spawn({
            let a = a.clone();
            async move { a.process_intent(&intent, Rank::Normal).await }
        });
        hold.reached().await;
        if delete {
            b.delete(&tenant, "SearchParameter", "sp")
                .await
                .expect("B's delete");
        }
        hold.release();
        let result = task.await.expect("the intent's task");
        assert_eq!(result.result, Ok(()), "delete={delete}");
        Some(expression_of(&a, &tenant, &url).is_some())
    }

    #[tokio::test]
    async fn a_replay_follows_a_read_taken_after_it() {
        let Some(deleted) = replay_held_before_its_follow(true).await else {
            return;
        };
        let control = replay_held_before_its_follow(false).await.unwrap();
        assert_eq!(
            (deleted, control),
            (false, true),
            "(B deleted it before the follow's read, nothing happened)"
        );
    }

    /// SW19 (ADR-aacruzgon-0099): when the read the follow uses fails (R1 passes, the second read of
    /// `resources` is refused), the intent is kept and nothing is followed; its next try replays it
    /// and follows.
    #[tokio::test]
    async fn a_failed_read_for_the_follow_keeps_the_intent() {
        let Some((a, t, tenant, url, intent)) =
            a_searchparameter_left_for_the_sweeper("sweeper-sp-reread-fails").await
        else {
            return;
        };
        let before = a.sweeper_stats();
        a.session.inject_fault(
            StatementKind::Read,
            "FROM resources WHERE",
            Fault::PassThrough,
        );
        a.session.inject_fault(
            StatementKind::Read,
            "FROM resources WHERE",
            Fault::FailWithUnavailable,
        );
        let first = a.process_intent(&intent, Rank::Normal).await;
        assert_eq!(first.case, Some(IntentCase::Current));
        assert_eq!(first.result, Err(Failure::Transient("read")));
        assert_eq!(
            a.sweeper_stats().since(&before).search_parameters_followed,
            0
        );
        assert_eq!(expression_of(&a, &tenant, &url), None, "not followed");
        let kept = a
            .intent_versions(
                &t,
                "SearchParameter",
                "sp",
                intent.enqueued - chrono::Duration::seconds(1),
            )
            .await;
        assert!(
            kept.contains(&intent.version_id),
            "the intent is kept: {kept:?}"
        );
        let second = a.process_intent(&intent, Rank::Normal).await;
        assert_eq!(second.result, Ok(()));
        assert_eq!(
            a.sweeper_stats().since(&before).search_parameters_followed,
            1
        );
        assert_eq!(
            expression_of(&a, &tenant, &url).as_deref(),
            Some("Patient.active")
        );
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

    /// ADR-aacruzgon-0090: a replay the node refuses as `Invalid` (a node set below what HFS needs) stays
    /// on the retry list, so once the node takes the write again, the same process finishes it at the next
    /// `RETRY_MAX` retry, without a restart. Before, it gave up and the resource stayed out of `count` and
    /// search until a restart or the intent's TTL. Each refused try is counted in `retries`. Tries 1 to 3
    /// log the refusal at `error`, the third once more that the sweeper keeps trying; try 4 logs it at
    /// `warn`, the batch's line and the sweeper's both. Each try's unconfirmed batches are logged as
    /// leaving the intent row in the outbox, never as kept for the sweeper, which is the request path's
    /// line.
    #[tokio::test]
    async fn a_refused_replay_is_finished_once_the_node_takes_it() {
        let Some((backend, t, tenant, since)) = start("sweeper-refused").await else {
            return;
        };
        let logs = Captured::default();
        let _logging = logs.install();
        let before = backend.sweeper_stats();
        let version = test_fhir_version();
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 1, async move {
            b.create(&ten, "Patient", patient(), version).await
        })
        .await;
        for _ in 0..4 {
            backend.session.inject_fault(
                StatementKind::Write,
                "INSERT INTO resources_by_type",
                Fault::FailWithInvalid,
            );
        }
        let start_mark = Utc::now() - chrono::Duration::minutes(2);
        let state = SweepState::new(start_mark, CancellationToken::new());
        let scope = SweepScope::Tenants(vec![t.clone()]);
        let now = Utc::now() + chrono::Duration::seconds(40);
        let mut gave_up = 0;
        // Three refused tries 6 s apart, then two polls before the 5 minute retry is due.
        for step in [0, 6, 12, 18, 24] {
            rewind(&state, start_mark);
            let report = backend
                .poll_once(&state, now + chrono::Duration::seconds(step), &scope)
                .await;
            gave_up += report.gave_up;
        }
        let refused = backend.sweeper_stats().since(&before);
        assert_eq!(gave_up, 0);
        assert_eq!(
            (
                refused.invariant_violations,
                refused.invariant_gave_up,
                refused.retries,
                refused.done
            ),
            (3, 0, 3, 0),
            "{refused}"
        );
        assert_eq!(state.retry.lock().len(), 1);
        assert_eq!(intents(&backend, &t, "p", since).await, vec![1]);
        assert!(rbt_rows(&backend, &t, "p").await.is_empty());
        assert_eq!(count(&backend, &tenant).await, 0);
        let batch = "index batch refused as invalid; not retried";
        let hit = "an intent hit an invariant; its row is kept in the outbox";
        let kept = "the node refused an intent's writes three times";
        let stays = "index batches not confirmed; the intent row stays in the outbox";
        let request = "index batches not confirmed; the intent is kept for the sweeper";
        let lines = |logs: &Captured| {
            [
                (tracing::Level::ERROR, batch),
                (tracing::Level::WARN, batch),
                (tracing::Level::ERROR, hit),
                (tracing::Level::WARN, hit),
                (tracing::Level::ERROR, kept),
                (tracing::Level::WARN, stays),
                (tracing::Level::WARN, request),
            ]
            .map(|(level, text)| logs.count(level, text))
        };
        assert_eq!(
            lines(&logs),
            [3, 0, 3, 0, 1, 3, 0],
            "tries 1 to 3: {:?}",
            logs.all()
        );

        // Try 4, the retry 5 minutes after the third, is refused again: kept, counted, logged at `warn`.
        rewind(&state, start_mark);
        let report = backend
            .poll_once(&state, now + chrono::Duration::seconds(12 + 5 * 60), &scope)
            .await;
        assert_eq!(
            (report.current, report.invariant, report.done),
            (1, 1, 0),
            "{report}"
        );
        let fourth = backend.sweeper_stats().since(&before);
        assert_eq!(
            (fourth.invariant_violations, fourth.retries),
            (4, 4),
            "{fourth}"
        );
        assert_eq!(state.retry.lock().len(), 1);
        assert_eq!(
            lines(&logs),
            [3, 1, 3, 1, 1, 4, 0],
            "try 4: {:?}",
            logs.all()
        );

        // The faults are spent: the node takes the write. The retry due 5 minutes after try 4 finishes it.
        rewind(&state, start_mark);
        let report = backend
            .poll_once(
                &state,
                now + chrono::Duration::seconds(12 + 10 * 60),
                &scope,
            )
            .await;
        assert_eq!((report.current, report.done), (1, 1), "{report}");
        assert_eq!(state.retry.lock().len(), 0);
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_eq!(rbt_rows(&backend, &t, "p").await.len(), 1);
        assert_eq!(count(&backend, &tenant).await, 1);
        assert_consistent(&check(&backend, &t).await);
        say("refused", &backend, &before);
    }

    /// ADR-aacruzgon-0090, the request path: a client `create` whose `resources_by_type` batch the node
    /// refuses logs the refusal at `error`, and its unconfirmed batches at `warn` as kept for the
    /// sweeper, never with the sweeper's line; `a_refused_replay_is_finished_once_the_node_takes_it`
    /// checks the opposite counts for the sweeper. The write still answers success (G23), and the next
    /// poll finishes the intent without logging either line.
    #[tokio::test]
    async fn a_refused_request_batch_logs_the_request_paths_line() {
        let Some((backend, t, tenant, since)) = start("request-refused").await else {
            return;
        };
        let logs = Captured::default();
        let _logging = logs.install();
        backend.session.inject_fault(
            StatementKind::Write,
            "INSERT INTO resources_by_type",
            Fault::FailWithInvalid,
        );
        let created = backend
            .create(&tenant, "Patient", patient(), test_fhir_version())
            .await;
        assert!(created.is_ok(), "{created:?}");
        let batch = "index batch refused as invalid; not retried";
        let request = "index batches not confirmed; the intent is kept for the sweeper";
        let stays = "index batches not confirmed; the intent row stays in the outbox";
        let lines = |logs: &Captured| {
            [
                (tracing::Level::ERROR, batch),
                (tracing::Level::WARN, batch),
                (tracing::Level::WARN, request),
                (tracing::Level::ERROR, request),
                (tracing::Level::WARN, stays),
            ]
            .map(|(level, text)| logs.count(level, text))
        };
        assert_eq!(lines(&logs), [1, 0, 1, 0, 0], "{:?}", logs.all());
        // I1 and the fresh intent `finish` writes for the unfinished write are left for the sweeper.
        assert_eq!(intents(&backend, &t, "p", since).await, vec![1, 1]);
        assert!(rbt_rows(&backend, &t, "p").await.is_empty());

        let state = SweepState::new(
            Utc::now() - chrono::Duration::minutes(2),
            CancellationToken::new(),
        );
        let scope = SweepScope::Tenants(vec![t.clone()]);
        let report = backend
            .poll_once(&state, Utc::now() + chrono::Duration::seconds(40), &scope)
            .await;
        assert_eq!((report.current, report.done), (2, 2), "{report}");
        assert_eq!(lines(&logs), [1, 0, 1, 0, 0], "{:?}", logs.all());
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_consistent(&check(&backend, &t).await);
    }

    /// ADR-aacruzgon-0090, the version row: a replay whose version row the node refuses as `Invalid`
    /// writes no index row (IW24) and is kept like a refused batch. Tries 1 to 3 log "post-CAS write
    /// refused as invalid" at `error`, the third once more that the sweeper keeps trying; try 4 logs it
    /// at `warn`. Once the node takes the row, the next retry finishes the intent.
    #[tokio::test]
    async fn a_refused_version_row_logs_at_warn_from_the_fourth_try() {
        let Some((backend, t, tenant, since)) = start("sweeper-refused-row").await else {
            return;
        };
        let logs = Captured::default();
        let _logging = logs.install();
        let before = backend.sweeper_stats();
        let version = test_fhir_version();
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(&backend, Checkpoint::AfterCas, &t, "p", 1, async move {
            b.create(&ten, "Patient", patient(), version).await
        })
        .await;
        for _ in 0..4 {
            backend.session.inject_fault(
                StatementKind::Write,
                "INSERT INTO resource_versions",
                Fault::FailWithInvalid,
            );
        }
        let start_mark = Utc::now() - chrono::Duration::minutes(2);
        let state = SweepState::new(start_mark, CancellationToken::new());
        let scope = SweepScope::Tenants(vec![t.clone()]);
        let now = Utc::now() + chrono::Duration::seconds(40);
        let row = "post-CAS write refused as invalid; not retried";
        let hit = "an intent hit an invariant; its row is kept in the outbox";
        let kept = "the node refused an intent's writes three times";
        let lines = |logs: &Captured| {
            [
                (tracing::Level::ERROR, row),
                (tracing::Level::WARN, row),
                (tracing::Level::ERROR, hit),
                (tracing::Level::WARN, hit),
                (tracing::Level::ERROR, kept),
            ]
            .map(|(level, text)| logs.count(level, text))
        };
        for (step, expected) in [
            (0, [1, 0, 1, 0, 0]),
            (6, [2, 0, 2, 0, 0]),
            (12, [3, 0, 3, 0, 1]),
            (12 + 5 * 60, [3, 1, 3, 1, 1]),
        ] {
            rewind(&state, start_mark);
            let report = backend
                .poll_once(&state, now + chrono::Duration::seconds(step), &scope)
                .await;
            assert_eq!(
                (report.current, report.invariant, report.done),
                (1, 1, 0),
                "+{step} s: {report}"
            );
            assert_eq!(lines(&logs), expected, "+{step} s: {:?}", logs.all());
        }
        let refused = backend.sweeper_stats().since(&before);
        assert_eq!(
            (
                refused.invariant_violations,
                refused.invariant_gave_up,
                refused.retries
            ),
            (4, 0, 4),
            "{refused}"
        );
        assert_eq!(state.retry.lock().len(), 1);
        assert!(rbt_rows(&backend, &t, "p").await.is_empty());

        // The faults are spent: the retry due 5 minutes after try 4 finishes it.
        rewind(&state, start_mark);
        let report = backend
            .poll_once(
                &state,
                now + chrono::Duration::seconds(12 + 10 * 60),
                &scope,
            )
            .await;
        assert_eq!((report.current, report.done), (1, 1), "{report}");
        assert!(intents(&backend, &t, "p", since).await.is_empty());
        assert_eq!(count(&backend, &tenant).await, 1);
        assert_consistent(&check(&backend, &t).await);
        say("refused-row", &backend, &before);
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
            let read = backend
                .read_slice(&slice, Rank::Normal)
                .await
                .unwrap()
                .decoded();
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
        // Each is refused for its `write_ts`, not for anything a replay would reach after the check: neither
        // resource has a version row 1, so a replay that skipped the check would stop at `MissingVersionRow`,
        // which the counts below cannot tell apart.
        for (id, case) in [
            ("current", IntentCase::Current),
            ("superseded", IntentCase::Superseded),
        ] {
            let found = pending(&backend, &t, id, since).await;
            let result = backend.process_intent(&found[0], Rank::Normal).await;
            assert_eq!(result.case, Some(case), "{id}");
            assert_eq!(
                result.result,
                Err(Failure::Invariant(Invariant::WriteTsMismatch)),
                "{id}"
            );
        }
        let before = backend.sweeper_stats();
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

    /// SW13: a live `resources` row with no `data` is an unreadable row, never a version row with no body. The row
    /// decodes, so it is planted in a `test-planted-` tenant as the other planted rows are.
    #[tokio::test]
    async fn a_live_row_with_no_data_is_an_unreadable_row_and_writes_nothing() {
        let Some((backend, t, _, since)) = start("planted-sweeper-no-data").await else {
            return;
        };
        let before = backend.sweeper_stats();
        let lu = whole_ms(Utc::now());
        backend
            .session
            .execute(
                "INSERT INTO resources (tenant_id, resource_type, id, version_id, last_updated, \
                 created_at, fhir_version, write_ts, write_id, index_keys, deleted_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0) IF NOT EXISTS",
                vec![
                    Value::from(t.as_str()),
                    Value::from("Patient"),
                    Value::from("p"),
                    Value::from(1),
                    Value::from(lu),
                    Value::from(lu),
                    Value::from(test_fhir_version().as_mime_param()),
                    Value::from(write_ts_of(lu)),
                    Value::from(Uuid::new_v4()),
                    Value::from(Blob::from(index_keys::encode(&[IndexKey::ResourcesByType]))),
                ],
                StatementKind::Cas,
            )
            .await
            .expect("plant");
        let key = RowKey {
            tenant_id: &t,
            resource_type: "Patient",
            id: "p",
        };
        let intent_key = IntentKey::new(&t, "p", Utc::now());
        backend
            .write_intent(&intent_key, &key, Uuid::new_v4(), 1)
            .await
            .unwrap();
        let found = pending(&backend, &t, "p", since).await;
        let result = backend.process_intent(&found[0], Rank::Normal).await;
        assert_eq!(result.case, Some(IntentCase::Current));
        assert_eq!(
            result.result,
            Err(Failure::Invariant(Invariant::UnreadableRow))
        );
        assert!(version_ids(&backend, &t, "Patient", "p").await.is_empty());
        assert_eq!(intents(&backend, &t, "p", since).await, vec![1]);
        backend.delete_intent(&intent_key, &key).await;
        say("no-data", &backend, &before);
    }

    /// SW8: a full retry list refuses an intent, and that intent's shard keeps its mark while the others move.
    #[tokio::test]
    async fn a_full_retry_list_holds_the_mark_of_the_shard_it_refused() {
        let Some((backend, t, _, _)) = start("sweeper-overflow").await else {
            return;
        };
        let now = Utc::now();
        let first = "p0".to_string();
        let first_shard = IntentKey::new(&t, &first, now).shard();
        let second = (1..)
            .map(|n| format!("p{n}"))
            .find(|id| IntentKey::new(&t, id, now).shard() != first_shard)
            .unwrap();
        let mut planted = Vec::new();
        for id in [&first, &second] {
            let key = RowKey {
                tenant_id: &t,
                resource_type: "Patient",
                id,
            };
            let intent_key = IntentKey::new(&t, id, Utc::now());
            backend
                .write_intent(&intent_key, &key, Uuid::new_v4(), 1)
                .await
                .unwrap();
            planted.push((intent_key, id.clone()));
            backend.session.inject_fault(
                StatementKind::Read,
                "FROM resources WHERE",
                Fault::FailWithUnavailable,
            );
        }
        let start_mark = Utc::now() - chrono::Duration::minutes(2);
        let state = SweepState::new(start_mark, CancellationToken::new());
        *state.retry.lock() = RetryList::new(1);
        let scope = SweepScope::Tenants(vec![t.clone()]);
        let report = backend
            .poll_once(&state, Utc::now() + chrono::Duration::seconds(40), &scope)
            .await;
        assert_eq!((report.failed, report.overflow), (2, 1), "{report}");
        let held = (0..16)
            .filter(|s| state.mark(*s) == whole_ms(start_mark))
            .count();
        assert_eq!(held, 1, "only the refused intent's shard keeps its mark");
        for (intent_key, id) in planted {
            let key = RowKey {
                tenant_id: &t,
                resource_type: "Patient",
                id: &id,
            };
            backend.delete_intent(&intent_key, &key).await;
        }
    }

    /// A list of one, kept full by an intent that is not in the outbox and is not due for 5 minutes.
    fn fill_retry_list_of_one(state: &SweepState, t: &str) {
        *state.retry.lock() = RetryList::new(1);
        let at = Utc::now();
        let filler = Intent {
            key: IntentKey::new(t, "filler", at),
            enqueued: at,
            tenant_id: t.to_string(),
            resource_type: "Patient".into(),
            id: "filler".into(),
            version_id: 1,
        };
        let mut list = state.retry.lock();
        // Seven transient failures put its next try 5 minutes out.
        for _ in 0..7 {
            assert!(matches!(
                list.failed(filler.clone(), Failure::Transient("index"), at),
                Queued::Retry { .. }
            ));
        }
    }

    /// A crashed create of `p` whose `resources_by_type` batch the node refuses on its next `tries`.
    async fn refused_create(
        backend: &Arc<CassandraBackend>,
        t: &str,
        tenant: &TenantContext,
        tries: usize,
    ) {
        let version = test_fhir_version();
        let (b, ten) = (backend.clone(), tenant.clone());
        crash(backend, Checkpoint::AfterCas, t, "p", 1, async move {
            b.create(&ten, "Patient", patient(), version).await
        })
        .await;
        for _ in 0..tries {
            backend.session.inject_fault(
                StatementKind::Write,
                "INSERT INTO resources_by_type",
                Fault::FailWithInvalid,
            );
        }
    }

    /// The "retry list is full" `error`, and the refused intent's batch and invariant lines at `error`
    /// and at `warn`.
    fn full_list_lines(logs: &Captured) -> [usize; 5] {
        let full = "the index sweeper's retry list is full; what it could not take is read again";
        let batch = "index batch refused as invalid; not retried";
        let hit = "an intent hit an invariant; its row is kept in the outbox";
        [
            (tracing::Level::ERROR, full),
            (tracing::Level::ERROR, batch),
            (tracing::Level::WARN, batch),
            (tracing::Level::ERROR, hit),
            (tracing::Level::WARN, hit),
        ]
        .map(|(level, text)| logs.count(level, text))
    }

    /// SW18, ADR-aacruzgon-0090, the control for the next test: a refused write the full list cannot
    /// take, met by the poll's main pass. Its own lines are at `warn`, and each poll logs that the list
    /// is full at `error`. Once the list has room, the next poll finishes it.
    #[tokio::test]
    async fn a_refused_write_off_a_full_list_is_logged_at_error_by_the_poll() {
        let Some((backend, t, tenant, since)) = start("sweeper-full-main").await else {
            return;
        };
        let logs = Captured::default();
        let _logging = logs.install();
        refused_create(&backend, &t, &tenant, 4).await;
        let start_mark = Utc::now() - chrono::Duration::minutes(2);
        let state = SweepState::new(start_mark, CancellationToken::new());
        fill_retry_list_of_one(&state, &t);
        let scope = SweepScope::Tenants(vec![t.clone()]);
        let now = Utc::now() + chrono::Duration::seconds(40);
        for (n, step) in [0, 6, 12, 18].into_iter().enumerate() {
            rewind(&state, start_mark);
            let report = backend
                .poll_once(&state, now + chrono::Duration::seconds(step), &scope)
                .await;
            assert_eq!(
                (report.overflow, report.invariant),
                (1, 1),
                "+{step} s: {report}"
            );
            let tries = n + 1;
            assert_eq!(
                full_list_lines(&logs),
                [tries, 0, tries, 0, tries],
                "+{step} s: {:?}",
                logs.all()
            );
        }
        *state.retry.lock() = RetryList::new(RETRY_LIST_CAP);
        rewind(&state, start_mark);
        let report = backend
            .poll_once(&state, now + chrono::Duration::seconds(24), &scope)
            .await;
        assert_eq!((report.current, report.done), (1, 1), "{report}");
        assert!(intents(&backend, &t, "p", since).await.is_empty());
    }

    /// SW18, ADR-aacruzgon-0090: the startup scan, and each poll's re-read of the slice it held, log
    /// that the list is full at `error`, as the poll's main pass does (the test above), when the full
    /// list cannot take a refused write; its own lines are at `warn`. Once the list has room, the next
    /// re-read finishes it.
    #[tokio::test]
    async fn the_startup_scan_and_held_slices_log_a_full_retry_list_at_error() {
        let Some((backend, t, tenant, since)) = start("sweeper-full-startup").await else {
            return;
        };
        let logs = Captured::default();
        let _logging = logs.install();
        refused_create(&backend, &t, &tenant, 4).await;
        // The marks start after the intent, as in `run_sweeper`: only the startup scan reaches it.
        let state = SweepState::new(Utc::now(), CancellationToken::new());
        fill_retry_list_of_one(&state, &t);
        let scope = SweepScope::Tenants(vec![t.clone()]);
        let (from, to) = (since, Utc::now() + chrono::Duration::seconds(1));
        let report = backend.startup_scan(&state, from, to, &scope).await;
        assert_eq!((report.overflow, report.invariant), (1, 1), "{report}");
        assert_eq!(state.pending.lock().len(), 1);
        assert_eq!(
            full_list_lines(&logs),
            [1, 0, 1, 0, 1],
            "startup: {:?}",
            logs.all()
        );

        // The held slice is due after 5 s, then 10 s and 20 s; each re-read is refused and held again.
        let base = Utc::now();
        for (n, step) in [6, 17, 38].into_iter().enumerate() {
            let report = backend
                .poll_once(&state, base + chrono::Duration::seconds(step), &scope)
                .await;
            // The main pass reads its own slices too (from +38 s), none of which holds the intent.
            assert_eq!(
                (report.overflow, report.invariant),
                (1, 1),
                "+{step} s: {report}"
            );
            assert_eq!(state.pending.lock().len(), 1);
            let tries = n + 2;
            assert_eq!(
                full_list_lines(&logs),
                [tries, 0, tries, 0, tries],
                "+{step} s: {:?}",
                logs.all()
            );
        }

        // The list has room, and the re-read due 40 s later finishes the intent.
        *state.retry.lock() = RetryList::new(RETRY_LIST_CAP);
        let report = backend
            .poll_once(&state, base + chrono::Duration::seconds(38 + 41), &scope)
            .await;
        assert_eq!((report.current, report.done), (1, 1), "{report}");
        assert!(state.pending.lock().is_empty());
        assert!(intents(&backend, &t, "p", since).await.is_empty());
    }

    /// SW18: the startup scan logs that the list is full at most once per `full_log_every`, each line
    /// naming the slices held since the one before. Three intents in three minutes of the scan each
    /// fail their row read once, on a list of one kept full: with the interval at 0, each minute logs
    /// its one slice; with it at "never again", only the first minute does. Once the faults are spent
    /// and the list has room, a scan finishes the three (none has a row).
    #[tokio::test]
    async fn the_startup_scan_logs_a_full_retry_list_at_most_once_per_interval() {
        let Some((backend, t, _, _)) = start("sweeper-full-interval").await else {
            return;
        };
        let logs = Captured::default();
        let _logging = logs.install();
        let now = Utc::now();
        let ids = ["q0", "q1", "q2"];
        for (id, ago) in ids.into_iter().zip([3, 2, 1]) {
            let key = RowKey {
                tenant_id: &t,
                resource_type: "Patient",
                id,
            };
            let at = now - chrono::Duration::minutes(ago);
            backend
                .write_intent(&IntentKey::new(&t, id, at), &key, Uuid::new_v4(), 1)
                .await
                .unwrap();
        }
        let full = "the index sweeper's retry list is full; what it could not take is read again";
        let scope = SweepScope::Tenants(vec![t.clone()]);
        let (from, to) = (now - chrono::Duration::minutes(5), Utc::now());
        for (every, expected) in [
            (std::time::Duration::ZERO, vec![Some(1); 3]),
            (std::time::Duration::MAX, vec![Some(1)]),
        ] {
            for _ in ids {
                backend.session.inject_fault(
                    StatementKind::Read,
                    "FROM resources WHERE",
                    Fault::FailWithUnavailable,
                );
            }
            let mut state = SweepState::new(Utc::now(), CancellationToken::new());
            state.full_log_every = every;
            fill_retry_list_of_one(&state, &t);
            let before = logs.count(tracing::Level::ERROR, full);
            let report = backend.startup_scan(&state, from, to, &scope).await;
            assert_eq!(report.overflow, 3, "{every:?}: {report}");
            let lines = logs.numbers(tracing::Level::ERROR, full, "slices_held");
            let shards = logs.numbers(tracing::Level::ERROR, full, "shards_held");
            assert_eq!(lines[before..], expected[..], "{every:?}: {:?}", logs.all());
            assert!(
                shards[before..].iter().all(|held| *held == Some(0)),
                "{shards:?}"
            );
        }

        let state = SweepState::new(Utc::now(), CancellationToken::new());
        let report = backend.startup_scan(&state, from, to, &scope).await;
        assert_eq!((report.no_row, report.done), (3, 3), "{report}");
        for id in ids {
            assert!(intents(&backend, &t, id, from).await.is_empty(), "{id}");
        }
    }

    /// SW28: `sweep_pass` with a grace of `Duration::MAX`, past what a timestamp can hold, returns an
    /// empty pass instead of panicking on the subtraction.
    #[tokio::test]
    async fn a_pass_with_the_largest_grace_is_empty() {
        let Some((backend, t, _, since)) = start("sweeper-huge-grace").await else {
            return;
        };
        let report = backend
            .sweep_pass(SweepOptions {
                scope: SweepScope::Tenants(vec![t]),
                grace: Duration::MAX,
                now: Utc::now(),
                since,
                allow_short_grace: false,
            })
            .await
            .unwrap();
        assert_eq!((report.slices, report.intents), (0, 0), "{report}");
    }

    /// SW8: a poll never reads further back than the outbox's 24 hour TTL, whatever its mark says.
    #[tokio::test]
    async fn a_poll_reads_no_further_back_than_24_hours() {
        let Some((backend, t, _, _)) = start("sweeper-clamp").await else {
            return;
        };
        let now = Utc::now();
        let state = SweepState::new(now - chrono::Duration::days(30), CancellationToken::new());
        let scope = SweepScope::Tenants(vec![t.clone()]);
        let report = backend.poll_once(&state, now, &scope).await;
        let grace = chrono::Duration::from_std(backend.config.sweeper_grace()).unwrap();
        let to = whole_ms(now - grace);
        let expected = 16 * plan_slices(0, to - retry_horizon(), to, poll_slice()).len();
        assert_eq!(report.slice_errors, 0, "{report}");
        assert_eq!(report.slices as usize, expected, "{report}");
        assert!((0..16).all(|s| state.mark(s) == to));
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

    /// How long the CI entry point waits before its grace-0 sweep (SW28): above `native_transport_timeout` (12 s), so a
    /// CAS still queued at the coordinator when its writer's process exited has applied by then.
    const POST_SUITE_SETTLE: Duration = Duration::from_secs(15);

    /// RV22's exclusion: the tenants that plant defects on purpose are swept but not checked.
    pub(crate) fn checked(tenant_id: &str) -> bool {
        tenant_id.starts_with("test-") && !tenant_id.starts_with("test-planted-")
    }

    /// What the post-suite step did.
    #[derive(Clone)]
    pub(crate) struct PostSuiteReport {
        pub(crate) prefix: String,
        pub(crate) sweep: SweepReport,
        pub(crate) tenants: usize,
        pub(crate) changed: usize,
        /// Tenants with the prefix that the planted rule skips (`checked` is false): excluded from the check and from `left`.
        /// A lower bound: it counts them from the `resources` listing only, so a planted tenant with only index or outbox rows is not counted.
        pub(crate) excluded: usize,
        pub(crate) defects: usize,
        pub(crate) failing: Vec<String>,
        /// Intents of checked tenants still in the outbox after the sweep: R4b, re-read in Rust.
        pub(crate) left: usize,
        /// Slices the re-read for `left` could not read, plus outbox rows it could not decode; any makes `left` unknown.
        pub(crate) left_errors: u64,
        /// The shared scan's failure, `"<what>: <error>"`, when it stopped early: every check is then void (ADR-sam-michaels-0007).
        pub(crate) scan_failed: Option<String>,
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
            let scan_failed = match &self.scan_failed {
                None => "-".to_string(),
                Some(text) => format!("{text:?}"),
            };
            write!(
                f,
                "[post-suite prefix={} tenants={} changed={} excluded={} defects={} failing={failing} \
                 left={} left_errors={} scan_failed={scan_failed} rows_scanned={} took_ms={}] {}",
                self.prefix,
                self.tenants,
                self.changed,
                self.excluded,
                self.defects,
                self.left,
                self.left_errors,
                self.rows_scanned,
                self.took.as_millis(),
                self.sweep
            )
        }
    }

    /// RV22's strict condition. `Err` carries the first reason, in this order: a failed scan, no tenant checked,
    /// defects, a failed re-read, intents left. `sweep.is_complete()` is not part of it: a planted tenant may leave
    /// intents the sweep cannot finish, by design.
    pub(crate) fn verdict(report: &PostSuiteReport) -> Result<(), String> {
        if let Some(s) = &report.scan_failed {
            return Err(format!(
                "the shared index scan failed ({s}), so every checked tenant's check is void: the {} failing tenants are this one failure",
                report.failing.len()
            ));
        }
        if report.tenants == 0 {
            return Err("no tenant was checked: no checked tenant has the prefix".to_string());
        }
        if report.defects > 0 {
            return Err(format!(
                "{} defects in {} tenants: {}",
                report.defects,
                report.failing.len(),
                report.failing.join(",")
            ));
        }
        if report.left_errors > 0 {
            return Err(format!(
                "the outbox re-read failed on {} slices, so the intents left are unknown",
                report.left_errors
            ));
        }
        if report.left > 0 {
            return Err(format!(
                "{} intents of checked tenants are left after the sweep (R4b)",
                report.left
            ));
        }
        Ok(())
    }

    /// R4b over `[since, now)`: every shard's one-minute slices (`plan_slices`, as `sweep_pass` reads them) through
    /// `read_slice`, 16 at a time, counting intents whose tenant starts with `prefix` and passes `keep`.
    /// Returns `(intents, slices that failed)`.
    pub(crate) async fn intents_left(
        backend: &CassandraBackend,
        prefix: &str,
        since: DateTime<Utc>,
        now: DateTime<Utc>,
        keep: impl Fn(&str) -> bool,
    ) -> (usize, u64) {
        let slices: Vec<Slice> = (0..INTENT_SHARDS as i16)
            .flat_map(|shard| plan_slices(shard, since, now, chrono::Duration::minutes(1)))
            .collect();
        let reads = slices
            .iter()
            .map(|slice| backend.read_slice(slice, Rank::Normal));
        let mut outcomes = futures::stream::iter(reads).buffer_unordered(16);
        let (mut left, mut failed) = (0usize, 0u64);
        // `read_slice` drops a row it cannot decode and counts it, so a rise here is a slice read in part.
        let invalid_before = backend.sweeper_stats().invariant_violations;
        while let Some(outcome) = outcomes.next().await {
            match outcome {
                Ok(read) => {
                    left += read
                        .decoded()
                        .iter()
                        .filter(|intent| {
                            intent.tenant_id.starts_with(prefix) && keep(&intent.tenant_id)
                        })
                        .count();
                }
                Err(_) => failed += 1,
            }
        }
        failed += backend
            .sweeper_stats()
            .invariant_violations
            .saturating_sub(invalid_before);
        (left, failed)
    }

    /// One grace-0 sweep of every tenant under `prefix`, then one shared scan and a check per checked tenant.
    pub(crate) async fn post_suite_report(
        backend: &CassandraBackend,
        prefix: &str,
        since: DateTime<Utc>,
    ) -> PostSuiteReport {
        let started = Instant::now();
        // One millisecond past now, so an intent stamped in this millisecond is in the window, whose bound
        // is in whole milliseconds (SW7); every writer has returned by now.
        let now = Utc::now() + chrono::Duration::milliseconds(1);
        let sweep = backend
            .sweep_pass(SweepOptions {
                scope: SweepScope::TenantPrefix(prefix.to_string()),
                grace: Duration::ZERO,
                now,
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
        let mut skipped: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for row in &keys {
            let tenant: String = row.get_r_by_name("tenant_id").expect("tenant_id");
            if !tenant.starts_with(prefix) {
                continue;
            }
            if !checked(&tenant) {
                skipped.insert(tenant);
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
        // The shared scan failed and no tenant's check reported it (ADR-sam-michaels-0007): with no
        // tenant to check, nothing else would.
        if tenants.is_empty()
            && let Some((what, _)) = scan.failure()
        {
            defects += 1;
            failing.push(format!("({what})"));
        }
        let (left, left_errors) = intents_left(backend, prefix, since, now, checked).await;
        PostSuiteReport {
            prefix: prefix.to_string(),
            sweep,
            tenants: tenants.len(),
            changed,
            excluded: skipped.len(),
            defects,
            failing,
            left,
            left_errors,
            scan_failed: scan
                .failed()
                .map(|(what, error)| format!("{what}: {error}")),
            rows_scanned: scan.scanned(),
            took: started.elapsed(),
        }
    }

    /// A failed shared scan fails the report even when the prefix has no tenant to check, which would
    /// otherwise report nothing; an empty scan that succeeds still passes. With one tenant (the
    /// control), its own check reports the failure, counted once.
    #[tokio::test]
    async fn a_failed_shared_scan_fails_the_post_suite_report() {
        use crate::backends::cassandra::consistency::harness::{self, context, patient};
        use crate::backends::cassandra::session::Fault;
        use crate::backends::cassandra::test_support::test_fhir_version;
        use crate::core::ResourceStorage;

        let Some(backend) = harness::backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let since = Utc::now() - chrono::Duration::seconds(5);
        let fail_scan = || {
            backend.session.inject_fault_on_first_page(
                StatementKind::Read,
                "FROM resources_by_type",
                Fault::FailWithUnavailable,
            )
        };

        let empty = format!("test-postsuite-empty-{}-", Uuid::new_v4().simple());
        let clean = post_suite_report(&backend, &empty, since).await;
        assert_eq!((clean.tenants, clean.defects), (0, 0), "{clean}");
        assert!(clean.failing.is_empty(), "{clean}");

        fail_scan();
        let failed = post_suite_report(&backend, &empty, since).await;
        assert_eq!((failed.tenants, failed.defects), (0, 1), "{failed}");
        assert_eq!(failed.failing, ["(scan resources_by_type)"], "{failed}");

        let one = format!("test-postsuite-one-{}-", Uuid::new_v4().simple());
        let tenant = context(&format!("{one}t"));
        backend
            .create(&tenant, "Patient", patient(), test_fhir_version())
            .await
            .unwrap();
        fail_scan();
        let control = post_suite_report(&backend, &one, since).await;
        assert_eq!((control.tenants, control.defects), (1, 1), "{control}");
        assert_eq!(control.failing, [format!("{one}t")], "{control}");
    }

    /// RV22: the CI entry point, run alone after every suite. Opt-in, and never on the shared dev node.
    #[tokio::test]
    #[ignore = "the post-suite CI step runs it alone, after every suite (RV22)"]
    async fn post_suite_sweep_and_check() {
        let Ok(url) = std::env::var("HFS_TEST_POST_SUITE_URL") else {
            assert!(
                std::env::var("HFS_TEST_POST_SUITE_STRICT").as_deref() != Ok("1"),
                "HFS_TEST_POST_SUITE_STRICT=1 but HFS_TEST_POST_SUITE_URL is not set: strict mode must not pass without checking"
            );
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
        tokio::time::sleep(POST_SUITE_SETTLE).await;
        let report = post_suite_report(&backend, "test-", Utc::now() - startup_window()).await;
        eprintln!("{report}");
        let verdict = verdict(&report);
        match &verdict {
            Ok(()) => eprintln!("[post-suite-verdict pass=true]"),
            Err(reason) => eprintln!("[post-suite-verdict pass=false reason={reason:?}]"),
        }
        if std::env::var("HFS_TEST_POST_SUITE_STRICT").as_deref() == Ok("1")
            && let Err(reason) = verdict
        {
            panic!("post-suite check failed: {reason}");
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
        assert_eq!(
            (
                report.excluded,
                report.left,
                report.left_errors,
                report.scan_failed.as_deref()
            ),
            (0, 0, 0, None),
            "{report}"
        );
        assert_eq!(verdict(&report), Ok(()), "{report}");
    }

    /// PS1 (rule 11): a failed scan is the verdict's first reason.
    #[test]
    fn the_post_suite_verdict_names_a_failed_scan_first() {
        let ok = PostSuiteReport {
            prefix: "test-".to_string(),
            sweep: SweepReport {
                invariant: 1,
                failed: 2,
                ..Default::default()
            },
            tenants: 3,
            changed: 1,
            excluded: 2,
            defects: 0,
            failing: vec![],
            left: 0,
            left_errors: 0,
            scan_failed: None,
            rows_scanned: 10,
            took: Duration::ZERO,
        };
        let scan = "scan uri_index: test-a-1/Patient/p: version_id: x";
        assert!(!ok.sweep.is_complete());
        assert_eq!(verdict(&ok), Ok(()));

        let scan_failed = PostSuiteReport {
            defects: 3,
            failing: vec!["test-a-1".into(), "test-b-2".into(), "test-c-3".into()],
            left: 4,
            scan_failed: Some(scan.to_string()),
            ..ok.clone()
        };
        let reason = verdict(&scan_failed).unwrap_err();
        assert!(
            reason.starts_with("the shared index scan failed (scan uri_index: test-a-1/Patient/p"),
            "{reason}"
        );
        assert!(
            reason.contains("the 3 failing tenants are this one failure"),
            "{reason}"
        );

        assert_eq!(
            verdict(&PostSuiteReport {
                tenants: 0,
                ..ok.clone()
            }),
            Err("no tenant was checked: no checked tenant has the prefix".to_string())
        );
        assert_eq!(
            verdict(&PostSuiteReport {
                defects: 2,
                failing: vec!["test-a-1".into()],
                ..ok.clone()
            }),
            Err("2 defects in 1 tenants: test-a-1".to_string())
        );
        let reason = verdict(&PostSuiteReport {
            left_errors: 1,
            ..ok.clone()
        })
        .unwrap_err();
        assert!(reason.contains("re-read failed on 1 slices"), "{reason}");
        assert_eq!(
            verdict(&PostSuiteReport {
                left: 2,
                ..ok.clone()
            }),
            Err("2 intents of checked tenants are left after the sweep (R4b)".to_string())
        );

        let line = format!("{ok}");
        assert!(
            line.contains(
                " changed=1 excluded=2 defects=0 failing=- left=0 left_errors=0 scan_failed=- rows_scanned=10 "
            ),
            "{line}"
        );
        let line = format!("{scan_failed}");
        assert!(line.contains(&format!("scan_failed=\"{scan}\"")), "{line}");
    }

    /// PS2 (rule 9): what a planted tenant leaves is swept but neither checked nor counted as left.
    #[tokio::test]
    async fn the_post_suite_check_ignores_what_a_planted_tenant_leaves() {
        use crate::backends::cassandra::consistency::harness;
        use crate::backends::cassandra::test_support::test_fhir_version;

        let Some(backend) = harness::backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let since = Utc::now() - chrono::Duration::seconds(5);
        let prefix = format!("test-planted-postsuite-{}-", Uuid::new_v4().simple());
        let t = format!("{prefix}a");
        let lu = whole_ms(Utc::now());
        let write_id = Uuid::new_v4();
        backend
            .session
            .execute(
                "INSERT INTO resources (tenant_id, resource_type, id, version_id, data, \
                 last_updated, created_at, fhir_version, write_ts, write_id, index_keys, \
                 deleted_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0) IF NOT EXISTS",
                vec![
                    Value::from(t.as_str()),
                    Value::from("Patient"),
                    Value::from("q"),
                    Value::from(1i32),
                    Value::from("{\"resourceType\":\"Patient\"}"),
                    Value::from(lu),
                    Value::from(lu),
                    Value::from(test_fhir_version().as_mime_param()),
                    Value::from(write_ts_of(lu)),
                    Value::from(write_id),
                    Value::from(Blob::from(vec![0x02, 0x00])),
                ],
                StatementKind::Cas,
            )
            .await
            .expect("plant");
        let key_i = IntentKey::new(&t, "q", Utc::now());
        let row_key = RowKey {
            tenant_id: &t,
            resource_type: "Patient",
            id: "q",
        };
        backend
            .write_intent(&key_i, &row_key, write_id, 1)
            .await
            .expect("the intent");
        // A pass reads `[since, whole_ms(now))`, so an intent enqueued in the millisecond of the
        // pass's own `now` is not read. A real writer's intent is older than the sweep by a request.
        tokio::time::sleep(Duration::from_millis(5)).await;

        let report = post_suite_report(&backend, &prefix, since).await;
        eprintln!("{report}");
        let counted = intents_left(&backend, &prefix, since, Utc::now(), |_| true).await;
        backend.delete_intent(&key_i, &row_key).await;
        assert_eq!(
            (report.sweep.current, report.sweep.invariant),
            (1, 1),
            "{report}"
        );
        assert!(!report.sweep.is_complete(), "{report}");
        assert_eq!(
            (
                report.tenants,
                report.excluded,
                report.left,
                report.left_errors
            ),
            (0, 1, 0, 0),
            "{report}"
        );
        assert_eq!(report.scan_failed, None, "{report}");
        assert_eq!(counted, (1, 0));
    }
}
