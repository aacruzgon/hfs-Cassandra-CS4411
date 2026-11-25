//! Repair on read: a search that finds index rows the tag check hides writes
//! the current version's index rows, and deletes the stale ones, itself
//! (roadmap 5.3; ADR-sam-michaels-0006, RV1 to RV9).
//!
//! # What repair does
//!
//! An index row carries the `version_id` of the version that wrote it. A
//! search reads rows, then compares each row's tag with the resource's own
//! `version_id` (R1, a `LOCAL_QUORUM` read of `resources`); a row with
//! another tag is hidden. A row below R1's version is stale: a write that
//! crashed after its CAS left it, and the sweeper has not finished the
//! write yet; or the search read the row just before an update landed.
//! Repair finishes it, from R1's own `index_keys`, K(current),
//! without reading anything else (RV4).
//!
//! - **Full replay (RV1).** When K(current) has at most
//!   [`MAX_FULL_REPAIR_KEYS`] keys, repair inserts every key of it at R1's
//!   `last_updated` and deletes every found row below R1 that K(current)
//!   does not produce (compared by [`IndexKey::row_at`], IW7).
//! - **Partial replay (RV1).** Above that, repair only touches the rows the
//!   search found. A found row whose key K(current) still produces *moves*
//!   to R1's `last_updated`, as the update itself moves it: it is deleted at
//!   its old `last_updated` and inserted at R1's (a date row, whose key has
//!   no `last_updated`, is only re-tagged in place). A found row whose key
//!   K(current) no longer produces is deleted. So a resource never loses its
//!   `resources_by_type` row to a partial repair, and the writes stay
//!   bounded by the found rows.
//! - **Skips (RV3).** Nothing is written, and the checks run in this order,
//!   the first that applies deciding: no `resources` row
//!   ([`SkipReason::NoRow`]); a found row tagged above R1 (R1 lags behind a
//!   write: [`SkipReason::TagAhead`]); no found row below R1
//!   ([`SkipReason::NotStale`]); R1's `write_ts` is not
//!   `write_ts_of(last_updated)` ([`SkipReason::WriteTsMismatch`]); R1's key
//!   set cannot be decoded ([`SkipReason::UndecodableKeys`]); the search's
//!   budget is spent ([`SkipReason::Budget`], RV5). Once the budget's 32
//!   resources are used, an entry still runs every check up to
//!   `UndecodableKeys`, then is skipped as `Budget` without a delta built.
//! - **A newer version wins.** Repair writes at `write_ts_of(R1.last_updated)`
//!   (RW10), so if the resource was updated after R1 was read, the newer
//!   version's rows carry a higher timestamp and win over this repair's:
//!   its writes are confirmed and change nothing visible (RV4).
//!
//! - **Concurrency.** Admitted repairs run up to [`REPAIR_CONCURRENCY`] at a
//!   time, none waiting behind another; each outcome is read from the
//!   repair's own slot, so a stalled repair holds up no other, and a skip's
//!   outcome never depends on a repair finishing.
//! - **One deadline.** The whole repair phase is bounded by one request
//!   deadline (`request_timeout_ms`), so a search waits at most that long
//!   for its repairs. A repair unfinished by then is reported
//!   [`WriteFailure::TriesExhausted`] and counted as failed; the next search
//!   or the sweeper repeats it.
//!
//! Repair never writes `resources`, `resource_versions` or an intent row,
//! and never sends a CAS. Its only statements are the index writer's
//! batches ([`CassandraBackend::write_index_delta`], one try, IW17).
//!
//! # Step 6's contract
//!
//! - Use one [`RepairContext`] per search request and call
//!   [`RepairContext::run`] once per response (not per page), on success and
//!   on `TooManyResults`, inside the request's task, never through
//!   `tokio::spawn`. Its result never fails the search. A filtered
//!   `search_count` does not queue repairs. The phase's deadline is meant to
//!   come out of the search's remaining time, capped at `request_timeout_ms`;
//!   Step 6 adds that entry point.
//! - Queue every candidate the tag check hides for its tag: an index row
//!   whose `version_id` is not R1's, a deleted R1, or no R1. Pass the tag
//!   check's own R1. That R1 is `SELECT_RESOURCE`, which reads the row's
//!   `index_keys`, so repair needs no other read (RV4).
//! - Build each [`StaleIndexRow`]'s key from the stored column values as
//!   they are, never through `index_text` again. Send a row the search
//!   cannot decode, or one [`StaleIndexRow::new`] refuses, to
//!   [`RepairContext::skip_undecodable`].
//! - Remove both `allow(dead_code)`s when the search calls this module: the
//!   inner attribute below, and the one on `impl RepairCounters` in
//!   `stats.rs`.

#![allow(dead_code)] // Step 6's search calls it; until then only its tests do (RV6). Step 6 removes this.

use std::future::Future;
use std::time::Duration;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use parking_lot::Mutex;

use super::backend::CassandraBackend;
use super::index_keys::{self, IndexKey, RowId};
use super::read::ResourceRow;
use super::stats::IndexWriteStats;
use super::write::{
    IndexDelta, IndexRow, RowKey, VersionKeys, WriteFailure, index_delta, index_pack_stats,
    write_ts_of,
};

/// Resources one search repairs at most (RV5, RV8): a chosen budget, not a measurement.
pub(crate) const MAX_REPAIRS_PER_SEARCH: usize = 32;
/// Index rows, inserts plus deletes, one search's repairs write at most (RV5, RV8).
pub(crate) const MAX_REPAIR_ROWS_PER_SEARCH: usize = 512;
/// Repairs of one search in flight at once (RV5, RV8): futures in the search's own task.
pub(crate) const REPAIR_CONCURRENCY: usize = 4;
/// Above this many keys in K(current), repair writes only the rows found, and the keys of
/// K(current) that name them (RV1): a chosen budget.
pub(crate) const MAX_FULL_REPAIR_KEYS: usize = 64;
/// Tries of each batch of a repair (IW17, RV7).
const REPAIR_TRIES: u32 = 1;

/// One index row a search found and the tag check hid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StaleIndexRow {
    key: IndexKey,
    /// The row's `last_updated` clustering column; `None` in the two date tables (IW7).
    last_updated: Option<DateTime<Utc>>,
    /// The row's `version_id` tag.
    version_id: i32,
}

impl StaleIndexRow {
    /// Err(message) for a non-date key without `last_updated`, which names no row.
    ///
    /// The caller builds `key` from the index row's stored column values as
    /// they are, never by passing them through `index_text` again: a stored
    /// value is already what `index_text` produced.
    pub(crate) fn new(
        key: IndexKey,
        last_updated: Option<DateTime<Utc>>,
        version_id: i32,
    ) -> Result<Self, String> {
        let is_date = matches!(key, IndexKey::Date { .. } | IndexKey::DateByEnd { .. });
        if last_updated.is_none() && !is_date {
            return Err(format!(
                "a {} row has no last_updated, so it names no row",
                key.table()
            ));
        }
        Ok(Self {
            key,
            last_updated,
            version_id,
        })
    }

    pub(crate) fn key(&self) -> &IndexKey {
        &self.key
    }

    pub(crate) fn version_id(&self) -> i32 {
        self.version_id
    }

    /// `key.row_at(last_updated)`, the Unix epoch for a date row (`row_at` ignores it there).
    fn row_id(&self) -> RowId<'_> {
        self.key.row_at(self.delete_at())
    }

    /// The `last_updated` this row is deleted at: its own, or the Unix epoch for a date row.
    fn delete_at(&self) -> DateTime<Utc> {
        self.last_updated.unwrap_or(DateTime::UNIX_EPOCH)
    }
}

impl IndexKey {
    /// The table of this key's rows. Names no indexed value, so it may be logged.
    fn table(&self) -> &'static str {
        match self {
            IndexKey::ResourcesByType => "resources_by_type",
            IndexKey::TokenCode { .. } => "token_code_index",
            IndexKey::TokenSystemCode { .. } => "token_system_code_index",
            IndexKey::TokenSystem { .. } => "token_system_index",
            IndexKey::Date { .. } => "date_index",
            IndexKey::DateByEnd { .. } => "date_index_by_end",
            IndexKey::Uri { .. } => "uri_index",
        }
    }
}

/// Why a repair wrote nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkipReason {
    /// A found row's tag is above R1's version: R1 lags behind a write (RV3).
    TagAhead,
    /// The tag check found no `resources` row (RV3).
    NoRow,
    /// R1's `write_ts` is not `write_ts_of(R1.last_updated)` (RV3, RW10).
    WriteTsMismatch,
    /// R1's `index_keys` is unset or not format 1, so K(current) is unknown (G22).
    UndecodableKeys,
    /// No found row is below R1's version.
    NotStale,
    /// Past `MAX_REPAIRS_PER_SEARCH` or `MAX_REPAIR_ROWS_PER_SEARCH` (RV5).
    Budget,
}

impl SkipReason {
    /// "tag_ahead", "no_row", "write_ts", "undecodable_keys", "not_stale", "budget".
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            SkipReason::TagAhead => "tag_ahead",
            SkipReason::NoRow => "no_row",
            SkipReason::WriteTsMismatch => "write_ts",
            SkipReason::UndecodableKeys => "undecodable_keys",
            SkipReason::NotStale => "not_stale",
            SkipReason::Budget => "budget",
        }
    }
}

/// What one repair did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepairOutcome {
    /// Every batch confirmed. `full`: K(current) whole (RV1), not only the found rows.
    Repaired { full: bool, index: IndexWriteStats },
    /// The writer's one try did not confirm every batch (RV7). Never the search's error (RV5).
    Failed {
        failure: WriteFailure,
        index: IndexWriteStats,
    },
    /// Nothing sent.
    Skipped(SkipReason),
}

/// What a repair writes, before anything is sent.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RepairPlan {
    /// Keys inserted at R1's `last_updated`: K(current) whole, or only its keys equal to a found row's key.
    next: Vec<IndexKey>,
    /// The found rows below R1's version, each at `delete_at()`.
    previous: Vec<(IndexKey, DateTime<Utc>)>,
    full: bool,
}

impl RepairPlan {
    /// `index_delta(previous, VersionKeys { version_id: row.version_id, last_updated: row.last_updated, fhir_version: row.fhir_version, keys: &next })`.
    fn delta<'a>(&'a self, row: &ResourceRow) -> IndexDelta<'a> {
        let previous: Vec<IndexRow<'a>> = self
            .previous
            .iter()
            .map(|(key, last_updated)| (key, *last_updated))
            .collect();
        index_delta(
            &previous,
            VersionKeys {
                version_id: row.version_id,
                last_updated: row.last_updated,
                fhir_version: row.fhir_version,
                keys: &self.next,
            },
        )
    }

    /// Inserts plus deletes over the delta's units.
    fn rows(&self, row: &ResourceRow) -> usize {
        delta_rows(&self.delta(row))
    }
}

/// Inserts plus deletes over the delta's units.
fn delta_rows(delta: &IndexDelta<'_>) -> usize {
    delta
        .units
        .iter()
        .map(|unit| unit.inserts.len() + unit.deletes.len())
        .sum()
}

/// RV3's checks before the key set is decoded: TagAhead, NotStale, WriteTsMismatch, in that order.
/// Returns the found rows below R1's version.
fn check_stale<'a>(
    row: &ResourceRow,
    stale: &'a [StaleIndexRow],
) -> Result<Vec<&'a StaleIndexRow>, SkipReason> {
    if stale.iter().any(|s| s.version_id > row.version_id) {
        return Err(SkipReason::TagAhead);
    }
    let below: Vec<&StaleIndexRow> = stale
        .iter()
        .filter(|s| s.version_id < row.version_id)
        .collect();
    if below.is_empty() {
        return Err(SkipReason::NotStale);
    }
    if row.write_ts != Some(write_ts_of(row.last_updated)) {
        return Err(SkipReason::WriteTsMismatch);
    }
    Ok(below)
}

/// RV1's plan, after [`check_stale`]: decodes K(current).
///
/// With at most [`MAX_FULL_REPAIR_KEYS`] keys the plan is full. Above that,
/// `next` keeps the keys of K(current) that equal the key of a found row
/// below R1 (or name the same row, for a date row): that found row moves to
/// R1's `last_updated`, or is re-tagged in place if it is a date row. A found
/// row K(current) does not produce is deleted. Writes stay bounded by the
/// found rows.
fn plan_below(row: &ResourceRow, below: &[&StaleIndexRow]) -> Result<RepairPlan, SkipReason> {
    Ok(plan_from(row, below, decode_current(row)?))
}

/// K(current), from R1's `index_keys`.
fn decode_current(row: &ResourceRow) -> Result<Vec<IndexKey>, SkipReason> {
    row.index_keys
        .as_deref()
        .and_then(|blob| index_keys::decode(blob).ok())
        .ok_or(SkipReason::UndecodableKeys)
}

fn plan_from(row: &ResourceRow, below: &[&StaleIndexRow], current: Vec<IndexKey>) -> RepairPlan {
    let full = current.len() <= MAX_FULL_REPAIR_KEYS;
    let next = if full {
        current
    } else {
        current
            .into_iter()
            .filter(|key| {
                let id = key.row_at(row.last_updated);
                below.iter().any(|s| s.key == *key || s.row_id() == id)
            })
            .collect()
    };
    let previous = below
        .iter()
        .map(|s| (s.key.clone(), s.delete_at()))
        .collect();
    RepairPlan {
        next,
        previous,
        full,
    }
}

/// RV3's checks, then RV1's plan. Pure: sends, logs and counts nothing.
fn plan_repair(row: &ResourceRow, stale: &[StaleIndexRow]) -> Result<RepairPlan, SkipReason> {
    let below = check_stale(row, stale)?;
    plan_below(row, &below)
}

/// One search's budget (RV5, RV8).
#[derive(Debug, Default)]
struct Budget {
    resources: usize,
    rows: usize,
}

impl Budget {
    /// True once `MAX_REPAIRS_PER_SEARCH` repairs are admitted: nothing more can fit.
    fn full(&self) -> bool {
        self.resources >= MAX_REPAIRS_PER_SEARCH
    }

    /// True, and counted, if one more repair of `rows` rows fits both limits.
    fn admit(&mut self, rows: usize) -> bool {
        if self.resources + 1 > MAX_REPAIRS_PER_SEARCH
            || self.rows.saturating_add(rows) > MAX_REPAIR_ROWS_PER_SEARCH
        {
            return false;
        }
        self.resources += 1;
        self.rows += rows;
        true
    }
}

/// One search's repairs (RV5, RV6): queued during the page's tag checks, run once at its end.
///
/// One context per search request, and [`RepairContext::run`] called once per
/// response, not once per page, so the budget of 32 repairs and 512 index
/// rows is per response. A filtered `search_count` does not queue repairs.
#[derive(Debug)]
pub(crate) struct RepairContext {
    tenant_id: String,
    /// One entry per (resource_type, id), in first-queued order.
    queued: Vec<QueuedRepair>,
    undecodable_rows: u64,
}

#[derive(Debug)]
struct QueuedRepair {
    resource_type: String,
    id: String,
    row: Option<ResourceRow>,
    stale: Vec<StaleIndexRow>,
}

impl QueuedRepair {
    fn key<'a>(&'a self, tenant_id: &'a str) -> RowKey<'a> {
        RowKey {
            tenant_id,
            resource_type: &self.resource_type,
            id: &self.id,
        }
    }
}

impl RepairContext {
    pub(crate) fn new(tenant_id: &str) -> Self {
        Self {
            tenant_id: tenant_id.to_string(),
            queued: Vec::new(),
            undecodable_rows: 0,
        }
    }

    /// A second call for the same (resource_type, id) appends the rows not already queued and keeps the first R1.
    pub(crate) fn queue(
        &mut self,
        resource_type: &str,
        id: &str,
        row: Option<ResourceRow>,
        stale: Vec<StaleIndexRow>,
    ) {
        match self
            .queued
            .iter_mut()
            .find(|q| q.resource_type == resource_type && q.id == id)
        {
            Some(queued) => {
                for s in stale {
                    if !queued.stale.contains(&s) {
                        queued.stale.push(s);
                    }
                }
            }
            None => self.queued.push(QueuedRepair {
                resource_type: resource_type.to_string(),
                id: id.to_string(),
                row,
                stale,
            }),
        }
    }

    /// An index row the search could not decode, or that `StaleIndexRow::new` refused: logs `error` now, counted by `run`.
    /// `error` is logged as is, so it must name no indexed value (a token code or a URI can be an identifier).
    pub(crate) fn skip_undecodable(&mut self, table: &str, error: &str) {
        tracing::error!(
            tenant_id = %self.tenant_id,
            table,
            error,
            "repair on read skipped an index row it could not decode"
        );
        self.undecodable_rows += 1;
    }

    /// Plans, admits, writes; outcomes in queue order. Never fails.
    ///
    /// Call it once per response (see [`RepairContext`]). The phase's
    /// deadline is meant to come out of the search's remaining time, capped
    /// at `request_timeout_ms`; Step 6 adds that entry point, so today `run`
    /// always gives the whole `request_timeout_ms`.
    ///
    /// The whole phase gets one request deadline (`request_timeout_ms`, the
    /// budget a request's index phase gets), so a search waits at most that
    /// long for its repairs. A repair that has not finished by then is
    /// reported [`WriteFailure::TriesExhausted`]: safe, since every repair
    /// write has a fixed timestamp and the next search or the sweeper
    /// repeats it.
    pub(crate) async fn run(self, backend: &CassandraBackend) -> Vec<RepairOutcome> {
        let deadline = phase_deadline(backend.config.request_timeout_ms);
        self.run_within(backend, deadline).await
    }

    /// [`Self::run`] with the phase's deadline given, so a test can make it short.
    async fn run_within(
        self,
        backend: &CassandraBackend,
        deadline: Duration,
    ) -> Vec<RepairOutcome> {
        let ends_at = tokio::time::Instant::now() + deadline;
        let counters = &backend.repair_counters;
        counters.queued(self.queued.len() as u64);
        counters.undecodable_rows(self.undecodable_rows);

        let tenant_id = self.tenant_id.as_str();
        let plans = plan_queue(&self.queued);
        let planned = admit_plans(&plans);
        for (queued, planned) in self.queued.iter().zip(&planned) {
            if let Err(reason) = planned {
                let version_id = queued.row.as_ref().map(|row| row.version_id);
                backend.note_repair_skip(&queued.key(tenant_id), version_id, *reason);
            }
        }

        // Only the admitted repairs run, up to `REPAIR_CONCURRENCY` at a
        // time, none waiting behind another. Each leaves what it packed
        // (before it sends) and its outcome (when it ends) in its slot, so
        // the outcomes come from the slots, by queue index, whatever the
        // order the repairs finished in or whether the deadline cut them.
        // Built into a Vec first so the stream's items are plain futures
        // (the same `Send` reason as in `write_index_delta_within`).
        let slots: Vec<Slot> = self.queued.iter().map(|_| Slot::default()).collect();
        let futures: Vec<_> = self
            .queued
            .iter()
            .zip(&planned)
            .zip(&slots)
            .filter_map(|((queued, planned), slot)| match planned {
                Ok(admitted) => Some(async move {
                    let outcome = backend
                        .send_repair(
                            &queued.key(tenant_id),
                            admitted.row,
                            admitted.plan.full,
                            &admitted.delta,
                            &slot.packed,
                        )
                        .await;
                    *slot.done.lock() = Some(outcome);
                }),
                Err(_) => None,
            })
            .collect();
        drive_within(futures, REPAIR_CONCURRENCY, ends_at).await;

        let entries = self
            .queued
            .iter()
            .zip(&planned)
            .zip(&slots)
            .map(|((queued, planned), slot)| (planned, *slot.done.lock(), (queued, slot)));
        outcomes_of(entries, |(queued, slot)| {
            let key = queued.key(tenant_id);
            tracing::warn!(
                tenant_id = %key.tenant_id,
                resource_type = key.resource_type,
                id = key.id,
                deadline_ms = deadline.as_millis() as u64,
                "repair on read passed its deadline; the repair is left for the next search or the sweeper"
            );
            let outcome = cut_outcome(*slot.packed.lock());
            if let RepairOutcome::Failed { index, .. } = &outcome {
                counters.failed(index);
            }
            outcome
        })
    }
}

/// The repair phase's one deadline: the request deadline (`request_timeout_ms`).
fn phase_deadline(request_timeout_ms: u64) -> Duration {
    Duration::from_millis(request_timeout_ms)
}

/// The outcome of a repair the deadline cut: what it packed if it started
/// (its batches may have been sent), nothing if it never started.
fn cut_outcome(packed: Option<IndexWriteStats>) -> RepairOutcome {
    RepairOutcome::Failed {
        failure: WriteFailure::TriesExhausted,
        index: packed.unwrap_or_default(),
    }
}

/// One entry's plan, or why it is skipped.
type Planned<'a> = Result<(&'a ResourceRow, RepairPlan), SkipReason>;

/// An admitted repair: its plan and the delta built from it, once.
struct Admitted<'a> {
    row: &'a ResourceRow,
    plan: &'a RepairPlan,
    delta: IndexDelta<'a>,
}

/// What a repair leaves for `run` while it runs.
#[derive(Debug, Default)]
struct Slot {
    /// What it packed, set before its first send.
    packed: Mutex<Option<IndexWriteStats>>,
    /// Its outcome, set when it ends.
    done: Mutex<Option<RepairOutcome>>,
}

/// RV3's checks, in queue order, then RV1's plan (so up to
/// `UndecodableKeys`): NoRow, TagAhead, NotStale, WriteTsMismatch,
/// UndecodableKeys. Pure: sends, logs and counts nothing.
fn plan_queue(queued: &[QueuedRepair]) -> Vec<Planned<'_>> {
    queued
        .iter()
        .map(|queued| {
            let row = queued.row.as_ref().ok_or(SkipReason::NoRow)?;
            Ok((row, plan_repair(row, &queued.stale)?))
        })
        .collect()
}

/// RV5's admission, in queue order, over the plans. Once the resource limit
/// is full, an entry that planned is skipped as `Budget` without a delta
/// built; a repair that does not fit the row limit is skipped, and a later,
/// smaller one may still be admitted. Each admitted repair's delta is built
/// once, here. Pure.
fn admit_plans<'a>(plans: &'a [Planned<'a>]) -> Vec<Result<Admitted<'a>, SkipReason>> {
    let mut budget = Budget::default();
    plans
        .iter()
        .map(|planned| {
            let (row, plan) = planned.as_ref().map_err(|reason| *reason)?;
            if budget.full() {
                return Err(SkipReason::Budget);
            }
            let delta = plan.delta(row);
            if budget.admit(delta_rows(&delta)) {
                Ok(Admitted { row, plan, delta })
            } else {
                Err(SkipReason::Budget)
            }
        })
        .collect()
}

/// One outcome per entry, in order: a skip's from its plan, an admitted
/// repair's from its slot, else `cut(context)`: the repair the deadline cut.
fn outcomes_of<'a, P: 'a, C>(
    entries: impl IntoIterator<Item = (&'a Result<P, SkipReason>, Option<RepairOutcome>, C)>,
    mut cut: impl FnMut(C) -> RepairOutcome,
) -> Vec<RepairOutcome> {
    entries
        .into_iter()
        .map(|(planned, done, context)| match planned {
            Err(reason) => RepairOutcome::Skipped(*reason),
            Ok(_) => done.unwrap_or_else(|| cut(context)),
        })
        .collect()
}

/// Runs `futures`, at most `concurrency` at once, until all finish or
/// `ends_at` passes; the ones unfinished then are dropped. None waits for
/// another to finish before it starts.
///
/// The futures are polled once before the clock is looked at, which starts
/// the first `concurrency` of them. If every one is done, that is the end. If
/// `ends_at` has already passed, it returns at once: a deadline in the past
/// cuts every future after its first poll, however fast the work behind it is
/// (a timer's own check can lag the clock by a tick, so it cannot give that).
/// Otherwise the rest runs until `ends_at`.
async fn drive_within<F: Future<Output = ()>>(
    futures: Vec<F>,
    concurrency: usize,
    ends_at: tokio::time::Instant,
) {
    let all = futures::stream::iter(futures)
        .buffer_unordered(concurrency)
        .for_each(|()| std::future::ready(()));
    let mut all = std::pin::pin!(all);
    if futures::poll!(all.as_mut()).is_ready() || tokio::time::Instant::now() >= ends_at {
        return;
    }
    let _ = tokio::time::timeout_at(ends_at, all).await;
}

impl CassandraBackend {
    /// RV6: one resource's repair. Plans (skip: log and count), else `send_repair`.
    pub(crate) async fn repair_index(
        &self,
        key: &RowKey<'_>,
        row: &ResourceRow,
        stale: &[StaleIndexRow],
    ) -> RepairOutcome {
        match plan_repair(row, stale) {
            Ok(plan) => {
                let delta = plan.delta(row);
                self.send_repair(key, row, plan.full, &delta, &Mutex::new(None))
                    .await
            }
            Err(reason) => {
                self.note_repair_skip(key, Some(row.version_id), reason);
                RepairOutcome::Skipped(reason)
            }
        }
    }

    /// Writes `plan` through `write_index_delta` at `write_ts_of(row.last_updated)`, `REPAIR_TRIES`; logs and counts.
    /// Sets `packed` to what the writer packs before it sends, so a repair cut by the deadline can report it.
    async fn send_repair(
        &self,
        key: &RowKey<'_>,
        row: &ResourceRow,
        full: bool,
        delta: &IndexDelta<'_>,
        packed: &Mutex<Option<IndexWriteStats>>,
    ) -> RepairOutcome {
        let ts = write_ts_of(row.last_updated);
        *packed.lock() = Some(index_pack_stats(key, delta, ts));
        let outcome = self.write_index_delta(key, delta, ts, REPAIR_TRIES).await;
        match outcome.result {
            Ok(()) => {
                tracing::info!(
                    tenant_id = %key.tenant_id,
                    resource_type = key.resource_type,
                    id = key.id,
                    version_id = row.version_id,
                    full,
                    index = %outcome.stats,
                    "repair on read wrote the current version's index rows"
                );
                self.repair_counters.repaired(full, &outcome.stats);
                RepairOutcome::Repaired {
                    full,
                    index: outcome.stats,
                }
            }
            Err(failure) => {
                tracing::warn!(
                    tenant_id = %key.tenant_id,
                    resource_type = key.resource_type,
                    id = key.id,
                    version_id = row.version_id,
                    failure = ?failure,
                    index = %outcome.stats,
                    "repair on read did not confirm every index batch"
                );
                self.repair_counters.failed(&outcome.stats);
                RepairOutcome::Failed {
                    failure,
                    index: outcome.stats,
                }
            }
        }
    }

    /// `error` log and the skip's counter.
    fn note_repair_skip(&self, key: &RowKey<'_>, version_id: Option<i32>, reason: SkipReason) {
        tracing::error!(
            tenant_id = %key.tenant_id,
            resource_type = key.resource_type,
            id = key.id,
            version_id = ?version_id,
            reason = reason.as_str(),
            "repair on read skipped a resource"
        );
        self.repair_counters.skipped(reason);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use cdrs_tokio::types::IntoRustByName;
    use cdrs_tokio::types::blob::Blob;
    use cdrs_tokio::types::value::Value;
    use chrono::TimeZone;
    use serde_json::{Value as Json, json};

    use super::*;
    use crate::backends::cassandra::checkpoint::Checkpoint;
    use crate::backends::cassandra::consistency::harness::*;
    use crate::backends::cassandra::consistency::{
        Registry, ScanMode, Scope, assert_consistent, index_consistency, version_ids,
    };
    use crate::backends::cassandra::session::{Fault, StatementKind};
    use crate::backends::cassandra::test_support::test_fhir_version;
    use crate::core::ResourceStorage;
    use crate::tenant::TenantContext;
    use crate::types::StoredResource;

    fn lu(n: i64) -> DateTime<Utc> {
        Utc.timestamp_millis_opt(1_700_000_000_000 + n * 1000)
            .unwrap()
    }

    /// The only `ResourceRow` literal of these tests.
    fn r1(version_id: i32, last_updated: DateTime<Utc>, keys: &[IndexKey]) -> ResourceRow {
        ResourceRow {
            version_id,
            data: Some("{}".to_string()),
            last_updated,
            created_at: None,
            deleted_at: None,
            fhir_version: test_fhir_version(),
            write_ts: Some(write_ts_of(last_updated)),
            write_id: None,
            index_keys: Some(index_keys::encode(keys)),
        }
    }

    fn token(param: &str, code: &str) -> IndexKey {
        IndexKey::TokenCode {
            param: param.to_string(),
            code: code.to_string(),
            system: String::new(),
        }
    }

    fn date(param: &str) -> IndexKey {
        IndexKey::Date {
            param: param.to_string(),
            start: 978_307_200_000,
            end: 978_393_600_000,
        }
    }

    fn s(key: IndexKey, at: Option<DateTime<Utc>>, tag: i32) -> StaleIndexRow {
        StaleIndexRow::new(key, at, tag).unwrap()
    }

    type Flat = Vec<(IndexKey, DateTime<Utc>)>;

    /// Every unit's inserts and deletes, each sorted.
    fn flat(delta: &IndexDelta<'_>) -> (Flat, Flat) {
        let mut inserts = Vec::new();
        let mut deletes = Vec::new();
        for unit in &delta.units {
            inserts.extend(unit.inserts.iter().map(|(k, t)| ((*k).clone(), *t)));
            deletes.extend(unit.deletes.iter().map(|(k, t)| ((*k).clone(), *t)));
        }
        inserts.sort();
        deletes.sort();
        (inserts, deletes)
    }

    fn sorted(mut rows: Flat) -> Flat {
        rows.sort();
        rows
    }

    #[test]
    fn the_plan_skips_as_rv3_says() {
        let rbt = IndexKey::ResourcesByType;
        let row = r1(2, lu(2), std::slice::from_ref(&rbt));
        let ahead = [
            s(rbt.clone(), Some(lu(1)), 1),
            s(rbt.clone(), Some(lu(3)), 3),
        ];
        assert_eq!(plan_repair(&row, &ahead), Err(SkipReason::TagAhead));
        let mut off = row.clone();
        off.write_ts = Some(write_ts_of(lu(2)) + 1);
        assert_eq!(plan_repair(&off, &ahead), Err(SkipReason::TagAhead));
        assert_eq!(
            plan_repair(&row, &[s(rbt.clone(), Some(lu(2)), 2)]),
            Err(SkipReason::NotStale)
        );
        assert_eq!(plan_repair(&row, &[]), Err(SkipReason::NotStale));
        let below = [s(rbt.clone(), Some(lu(1)), 1)];
        for write_ts in [Some(write_ts_of(lu(2)) + 1), None] {
            let mut bad = row.clone();
            bad.write_ts = write_ts;
            assert_eq!(plan_repair(&bad, &below), Err(SkipReason::WriteTsMismatch));
        }
        for blob in [None, Some(vec![0x02, 0x00])] {
            let mut bad = row.clone();
            bad.index_keys = blob;
            assert_eq!(plan_repair(&bad, &below), Err(SkipReason::UndecodableKeys));
        }
        assert!(plan_repair(&row, &below).is_ok());
    }

    #[test]
    fn a_full_repair_inserts_every_current_key_and_deletes_only_what_it_does_not_produce() {
        let rbt = IndexKey::ResourcesByType;
        let keys = [rbt.clone(), token("t", "a"), date("d")];
        let row = r1(2, lu(2), &keys);
        let stale = [
            s(rbt.clone(), Some(lu(1)), 1),
            s(token("t", "b"), Some(lu(1)), 1),
            s(date("d"), None, 1),
            s(token("t", "a"), Some(lu(2)), 2),
        ];
        let plan = plan_repair(&row, &stale).unwrap();
        assert!(plan.full);
        let delta = plan.delta(&row);
        assert_eq!(delta.version_id, 2);
        let (inserts, deletes) = flat(&delta);
        assert_eq!(
            inserts,
            sorted(vec![
                (rbt.clone(), lu(2)),
                (token("t", "a"), lu(2)),
                (date("d"), lu(2)),
            ])
        );
        assert_eq!(
            deletes,
            sorted(vec![(rbt, lu(1)), (token("t", "b"), lu(1))])
        );
        assert_eq!(plan.rows(&row), 5);
    }

    fn keys_of_len(len: usize) -> Vec<IndexKey> {
        let mut keys = vec![IndexKey::ResourcesByType];
        keys.extend((0..len - 2).map(|i| token("c", &format!("{i:02}"))));
        keys.push(date("d"));
        assert_eq!(keys.len(), len);
        keys
    }

    #[test]
    fn over_64_keys_moves_the_found_rows_and_retags_their_date_rows() {
        let rbt = IndexKey::ResourcesByType;
        let keys = keys_of_len(66);
        let row = r1(2, lu(2), &keys);
        let stale = [
            s(date("d"), None, 1),
            s(rbt.clone(), Some(lu(1)), 1),
            s(token("c", "x"), Some(lu(1)), 1),
            s(token("c", "00"), Some(lu(1)), 1),
            // Tagged equal to R1: not below it, so never deleted.
            s(token("c", "y"), Some(lu(2)), 2),
        ];
        let plan = plan_repair(&row, &stale).unwrap();
        assert!(!plan.full);
        assert_eq!(plan.next, vec![rbt.clone(), token("c", "00"), date("d")]);
        let (inserts, deletes) = flat(&plan.delta(&row));
        assert_eq!(
            inserts,
            sorted(vec![
                (rbt.clone(), lu(2)),
                (token("c", "00"), lu(2)),
                (date("d"), lu(2)),
            ])
        );
        // A found row K(current) produces moves: deleted at its old
        // last_updated, inserted at R1's. The date row is only re-tagged.
        assert_eq!(
            deletes,
            sorted(vec![
                (rbt, lu(1)),
                (token("c", "00"), lu(1)),
                (token("c", "x"), lu(1)),
            ])
        );
        assert!(!deletes.contains(&(token("c", "y"), lu(2))));
        assert_eq!(plan.rows(&row), 6);
    }

    #[test]
    fn exactly_65_keys_is_partial_and_64_is_full() {
        let stale = [s(token("c", "x"), Some(lu(1)), 1)];
        let plan = plan_repair(&r1(2, lu(2), &keys_of_len(64)), &stale).unwrap();
        assert!(plan.full);
        assert_eq!(plan.next.len(), 64);
        let plan = plan_repair(&r1(2, lu(2), &keys_of_len(65)), &stale).unwrap();
        assert!(!plan.full);
        assert!(plan.next.is_empty());
    }

    #[test]
    fn a_deleted_resource_repair_only_deletes() {
        let mut row = r1(3, lu(3), &[]);
        row.deleted_at = Some(lu(3));
        row.data = None;
        let stale = [s(IndexKey::ResourcesByType, Some(lu(2)), 2)];
        let plan = plan_repair(&row, &stale).unwrap();
        assert!(plan.full);
        let (inserts, deletes) = flat(&plan.delta(&row));
        assert!(inserts.is_empty());
        assert_eq!(deletes, vec![(IndexKey::ResourcesByType, lu(2))]);
    }

    #[test]
    fn a_stale_row_must_name_a_row() {
        assert!(StaleIndexRow::new(token("t", "a"), None, 1).is_err());
        assert!(StaleIndexRow::new(date("d"), None, 1).is_ok());
        assert!(StaleIndexRow::new(IndexKey::ResourcesByType, Some(lu(1)), 1).is_ok());
    }

    #[test]
    fn the_budget_admits_32_repairs_and_512_rows() {
        let mut budget = Budget::default();
        for _ in 0..MAX_REPAIRS_PER_SEARCH {
            assert!(budget.admit(1));
        }
        assert!(!budget.admit(1));
        let mut budget = Budget::default();
        assert!(budget.admit(300));
        assert!(budget.admit(200));
        assert!(!budget.admit(13));
        assert!(budget.admit(12));
        assert!(!budget.admit(1));
        let mut budget = Budget::default();
        assert!(!budget.admit(513));
    }

    #[test]
    fn a_bad_write_ts_and_keys_skip_in_the_documented_order() {
        let rbt = IndexKey::ResourcesByType;
        let mut row = r1(2, lu(2), &[]);
        row.write_ts = None;
        assert_eq!(
            plan_repair(&row, &[s(rbt.clone(), Some(lu(2)), 2)]),
            Err(SkipReason::NotStale)
        );
        row.index_keys = Some(vec![0x02, 0x00]);
        assert_eq!(
            plan_repair(&row, &[s(rbt, Some(lu(1)), 1)]),
            Err(SkipReason::WriteTsMismatch)
        );
    }

    /// Compile-time: `run`'s future is `Send`, as the storage traits' futures must be.
    fn run_is_send(context: RepairContext, backend: &CassandraBackend) {
        fn assert_send<T: Send>(_: T) {}
        assert_send(context.run(backend));
    }

    #[test]
    fn queue_merges_the_rows_of_one_resource_and_keeps_its_first_row() {
        let rbt = IndexKey::ResourcesByType;
        let (s1, s2, s3) = (
            s(rbt.clone(), Some(lu(1)), 1),
            s(token("t", "a"), Some(lu(1)), 1),
            s(token("t", "b"), Some(lu(1)), 1),
        );
        let mut context = RepairContext::new("test-repair-queue");
        context.queue("Patient", "a", Some(r1(2, lu(2), &[])), vec![s1.clone()]);
        context.queue("Patient", "b", None, vec![s2]);
        context.queue(
            "Patient",
            "a",
            Some(r1(3, lu(3), &[])),
            vec![s1.clone(), s3.clone()],
        );
        context.skip_undecodable("date_index", "x");
        let ids: Vec<&str> = context.queued.iter().map(|q| q.id.as_str()).collect();
        assert_eq!(ids, ["a", "b"]);
        assert_eq!(context.queued[0].row.as_ref().unwrap().version_id, 2);
        assert_eq!(context.queued[0].stale, vec![s1, s3]);
        assert_eq!(context.undecodable_rows, 1);
    }

    const PLANT_X5: &str = "INSERT INTO date_index (tenant_id, resource_type, param, start, id, end, version_id) VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
    const PLANT_X6: &str = "INSERT INTO date_index_by_end (tenant_id, resource_type, param, end, id, start, version_id) VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
    const PLANT_VERSION_KEYS: &str = "UPDATE resource_versions USING TIMESTAMP ? SET index_keys = ? WHERE tenant_id = ? AND resource_type = ? AND id = ? AND version_id = ?";
    /// A CAS: asserts `[applied]`.
    const PLANT_RESOURCE_KEYS: &str = "UPDATE resources SET index_keys = ? WHERE tenant_id = ? AND resource_type = ? AND id = ? IF version_id = ?";

    fn date_by_end(param: &str) -> IndexKey {
        IndexKey::DateByEnd {
            param: param.to_string(),
            end: 978_393_600_000,
            start: 978_307_200_000,
        }
    }

    async fn plant(backend: &CassandraBackend, cql: &str, values: Vec<Value>) {
        backend
            .session
            .execute(cql, values, StatementKind::Write)
            .await
            .expect("plant");
    }

    async fn plant_resource_keys(
        backend: &CassandraBackend,
        tenant: &str,
        blob: Vec<u8>,
        version_id: i32,
    ) {
        let rows = backend
            .session
            .rows(
                PLANT_RESOURCE_KEYS,
                vec![
                    Value::from(Blob::from(blob)),
                    Value::from(tenant),
                    Value::from("Patient"),
                    Value::from("p"),
                    Value::from(version_id),
                ],
                StatementKind::Cas,
            )
            .await
            .expect("plant the stored key set");
        let applied: bool = rows[0].get_r_by_name("[applied]").expect("[applied]");
        assert!(applied, "the CAS that plants resources.index_keys applied");
    }

    async fn plant_version_keys(
        backend: &CassandraBackend,
        tenant: &str,
        version_id: i32,
        blob: Vec<u8>,
        ts: i64,
    ) {
        plant(
            backend,
            PLANT_VERSION_KEYS,
            vec![
                Value::from(ts),
                Value::from(Blob::from(blob)),
                Value::from(tenant),
                Value::from("Patient"),
                Value::from("p"),
                Value::from(version_id),
            ],
        )
        .await;
    }

    /// A read of the `(tenant, Patient)` partition of `resources_by_type`:
    /// the unfiltered type search, as `(id, last_updated, version_id)`.
    async fn type_search(backend: &CassandraBackend, t: &str) -> Vec<(String, DateTime<Utc>, i32)> {
        let rows = backend
            .session
            .rows_paged(
                "SELECT last_updated, id, version_id FROM resources_by_type WHERE tenant_id = ? AND resource_type = ?",
                vec![Value::from(t), Value::from("Patient")],
                StatementKind::Read,
            )
            .await
            .expect("type search");
        rows.iter()
            .map(|row| {
                let id: String = row.get_r_by_name("id").expect("id");
                let at: DateTime<Utc> = row.get_r_by_name("last_updated").expect("last_updated");
                let tag: i32 = row.get_r_by_name("version_id").expect("version_id");
                (id, at, tag)
            })
            .collect()
    }

    /// The rows of `id` tagged below `below`, as the search hands them over.
    fn found(rows: &[(String, DateTime<Utc>, i32)], id: &str, below: i32) -> Vec<StaleIndexRow> {
        rows.iter()
            .filter(|(row_id, _, tag)| row_id == id && *tag < below)
            .map(|(_, at, tag)| {
                StaleIndexRow::new(IndexKey::ResourcesByType, Some(*at), *tag).unwrap()
            })
            .collect()
    }

    fn keys_of(row: &ResourceRow) -> Vec<IndexKey> {
        index_keys::decode(row.index_keys.as_deref().expect("index_keys")).expect("K")
    }

    /// The keys of `old` whose row is not the row of any key of `new`.
    fn not_produced(
        old: &[IndexKey],
        old_lu: DateTime<Utc>,
        new: &[IndexKey],
        new_lu: DateTime<Utc>,
    ) -> usize {
        old.iter()
            .filter(|k| {
                let row = k.row_at(old_lu);
                !new.iter().any(|n| n.row_at(new_lu) == row)
            })
            .count()
    }

    /// An update of `p` that stops after its version row: the CAS applied,
    /// the index rows were not written.
    async fn crash_update(
        backend: &Arc<CassandraBackend>,
        tenant: &TenantContext,
        t: &str,
        p: &StoredResource,
        body: Json,
    ) {
        let target: i32 = p.version_id().parse::<i32>().expect("a numeric version") + 1;
        let mut hold = backend.hold_at(Checkpoint::AfterVersionRow, t, "p", target);
        let task = tokio::spawn({
            let (backend, tenant, p) = (backend.clone(), tenant.clone(), p.clone());
            async move { backend.update(&tenant, &p, body).await }
        });
        hold.reached().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
    }

    async fn r1_of(backend: &CassandraBackend, t: &str, id: &str) -> ResourceRow {
        backend
            .read_resource_row(t, "Patient", id)
            .await
            .expect("R1")
            .expect("a row")
    }

    fn key<'a>(t: &'a str, id: &'a str) -> RowKey<'a> {
        RowKey {
            tenant_id: t,
            resource_type: "Patient",
            id,
        }
    }

    #[tokio::test]
    async fn repair_heals_a_crashed_update_at_the_first_search() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("repair-heal");
        let tenant = context(&t);
        let p = backend
            .create(&tenant, "Patient", patient(), test_fhir_version())
            .await
            .expect("create p");
        let since = Utc::now();
        let r1_first = r1_of(&backend, &t, "p").await;
        let (lu1, k1) = (r1_first.last_updated, keys_of(&r1_first));
        crash_update(&backend, &tenant, &t, &p, changed()).await;
        let row = r1_of(&backend, &t, "p").await;
        assert_eq!(row.version_id, 2);
        let (lu2, k2) = (row.last_updated, keys_of(&row));

        let stale = found(&type_search(&backend, &t).await, "p", 2);
        assert_eq!(stale.len(), 1);
        let report = check(&backend, &t).await;
        assert_eq!(
            report.kinds(),
            expect(&[
                ("missing_row", k2.len()),
                ("stray_row", not_produced(&k1, lu1, &k2, lu2)),
            ]),
            "{report}"
        );

        let mut context = RepairContext::new(&t);
        context.queue("Patient", "p", Some(row.clone()), stale);
        let (before, at) = (backend.repair_stats(), backend.statement_counts());
        let outcomes = context.run(&backend).await;
        let stats = backend.repair_stats().since(&before);
        let spent = backend.statement_counts().since(&at);
        eprintln!("[repair case=heal {stats}]");

        let [RepairOutcome::Repaired { full: true, index }] = outcomes[..] else {
            panic!("{outcomes:?}");
        };
        assert_eq!(index.rows_inserted, k2.len() as u64);
        assert_eq!(index.rows_deleted, 1);
        assert_eq!(spent.batch + spent.write, index.batches);
        assert_eq!((spent.read, spent.serial_read, spent.cas), (0, 0, 0));
        assert_eq!((stats.queued, stats.repaired, stats.failed), (1, 1, 0));
        assert_eq!(
            (stats.rows_inserted, stats.rows_deleted),
            (k2.len() as u64, 1)
        );
        let rows: Vec<_> = type_search(&backend, &t)
            .await
            .into_iter()
            .filter(|(id, ..)| id == "p")
            .map(|(_, at, tag)| (at, tag))
            .collect();
        assert_eq!(rows, [(lu2, 2)]);
        assert_eq!(r1_of(&backend, &t, "p").await, row);
        assert_eq!(version_ids(&backend, &t, "Patient", "p").await, [1, 2]);
        assert_eq!(
            backend.intent_versions(&t, "Patient", "p", since).await,
            [2]
        );
        let report = check(&backend, &t).await;
        assert_eq!(
            report.kinds(),
            expect(&[("stray_row", not_produced(&k1, lu1, &k2, lu2) - 1)]),
            "{report}"
        );
    }

    #[tokio::test]
    async fn repair_with_an_older_r1_loses_to_a_newer_version() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("repair-older-r1");
        let tenant = context(&t);
        let p = backend
            .create(&tenant, "Patient", patient(), test_fhir_version())
            .await
            .expect("create p");
        let since = Utc::now();
        let r1_first = r1_of(&backend, &t, "p").await;
        let (lu1, k1) = (r1_first.last_updated, keys_of(&r1_first));
        crash_update(&backend, &tenant, &t, &p, changed()).await;
        let r1_old = r1_of(&backend, &t, "p").await;
        assert_eq!(r1_old.version_id, 2);
        let k2 = keys_of(&r1_old);
        let stale = found(&type_search(&backend, &t).await, "p", 2);

        let v2 = backend
            .read(&tenant, "Patient", "p")
            .await
            .expect("read")
            .expect("p");
        let v3 = backend
            .update(&tenant, &v2, patient())
            .await
            .expect("update to 3");
        assert_eq!(v3.version_id(), "3");
        let r1_new = r1_of(&backend, &t, "p").await;
        let (lu3, k3) = (r1_new.last_updated, keys_of(&r1_new));

        let mut context = RepairContext::new(&t);
        context.queue("Patient", "p", Some(r1_old), stale);
        let (before, at) = (backend.repair_stats(), backend.statement_counts());
        let outcomes = context.run(&backend).await;
        let spent = backend.statement_counts().since(&at);
        assert_eq!((spent.read, spent.serial_read, spent.cas), (0, 0, 0));
        eprintln!(
            "[repair case=older-r1 {}]",
            backend.repair_stats().since(&before)
        );

        let [RepairOutcome::Repaired { full: true, index }] = outcomes[..] else {
            panic!("{outcomes:?}");
        };
        assert_eq!(index.rows_inserted, k2.len() as u64);
        assert_eq!(index.rows_deleted, 1);
        let rows: Vec<_> = type_search(&backend, &t)
            .await
            .into_iter()
            .filter(|(id, ..)| id == "p")
            .map(|(_, at, tag)| (at, tag))
            .collect();
        assert_eq!(rows, [(lu3, 3)]);
        assert_eq!(r1_of(&backend, &t, "p").await, r1_new);
        assert_eq!(version_ids(&backend, &t, "Patient", "p").await, [1, 2, 3]);
        assert_eq!(
            backend.intent_versions(&t, "Patient", "p", since).await,
            [2]
        );
        let report = check(&backend, &t).await;
        assert_eq!(
            report.kinds(),
            expect(&[("stray_row", not_produced(&k1, lu1, &k3, lu3) - 1)]),
            "{report}"
        );
    }

    /// Creates and updates `id`, so that the old `resources_by_type` row is
    /// what a search that read just before the update would find. Returns
    /// R1 (version 2) and the found row.
    async fn updated(
        backend: &CassandraBackend,
        tenant: &TenantContext,
        t: &str,
        id: &str,
    ) -> (ResourceRow, Vec<StaleIndexRow>) {
        let body = |active: bool| json!({"resourceType": "Patient", "id": id, "active": active});
        let created = backend
            .create(tenant, "Patient", body(true), test_fhir_version())
            .await
            .expect("create");
        let lu1 = r1_of(backend, t, id).await.last_updated;
        backend
            .update(tenant, &created, body(false))
            .await
            .expect("update");
        let row = r1_of(backend, t, id).await;
        assert_eq!(row.version_id, 2);
        (
            row,
            vec![StaleIndexRow::new(IndexKey::ResourcesByType, Some(lu1), 1).unwrap()],
        )
    }

    #[tokio::test]
    async fn repairs_beyond_the_budget_are_skipped_not_failed() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("repair-budget");
        let tenant = context(&t);
        let mut context = RepairContext::new(&t);
        // Entries that are skipped before the budget: they use none of it.
        let current = r1(2, lu(2), &[]);
        context.queue(
            "Patient",
            "skip-not-stale",
            Some(current.clone()),
            vec![s(IndexKey::ResourcesByType, Some(lu(1)), 2)],
        );
        context.queue(
            "Patient",
            "skip-tag-ahead",
            Some(current),
            vec![s(IndexKey::ResourcesByType, Some(lu(1)), 3)],
        );
        let skipped = 2;
        let mut k2_len = 0;
        let mut deletes = Vec::new();
        for i in 0..34 {
            let id = format!("r{i:02}");
            let (row, mut stale) = updated(&backend, &tenant, &t, &id).await;
            // Made-up rows tagged 1 before the real one: deleting each only
            // writes a tombstone, and they make every repair's size differ.
            let lu1 = stale[0].last_updated.expect("lu1");
            let extra = i % 3;
            for k in 1..=extra {
                stale.push(
                    StaleIndexRow::new(
                        IndexKey::ResourcesByType,
                        Some(lu1 - chrono::Duration::milliseconds(k)),
                        1,
                    )
                    .unwrap(),
                );
            }
            k2_len = keys_of(&row).len();
            deletes.push(1 + extra as u64);
            context.queue("Patient", &id, Some(row), stale);
        }
        let n = MAX_REPAIRS_PER_SEARCH.min(MAX_REPAIR_ROWS_PER_SEARCH / (k2_len + 3));

        let (before, at) = (backend.repair_stats(), backend.statement_counts());
        let outcomes = context.run(&backend).await;
        let stats = backend.repair_stats().since(&before);
        let spent = backend.statement_counts().since(&at);
        eprintln!("[repair case=budget {stats}]");

        assert_eq!(outcomes.len(), skipped + 34);
        assert_eq!(
            outcomes[..skipped],
            [
                RepairOutcome::Skipped(SkipReason::NotStale),
                RepairOutcome::Skipped(SkipReason::TagAhead),
            ]
        );
        let mut batches = 0;
        for (at, outcome) in outcomes[skipped..skipped + n].iter().enumerate() {
            let RepairOutcome::Repaired { full: true, index } = outcome else {
                panic!("{outcome:?}");
            };
            assert_eq!(index.rows_inserted, k2_len as u64, "position {at}");
            assert_eq!(index.rows_deleted, deletes[at], "position {at}");
            batches += index.batches;
        }
        for outcome in &outcomes[skipped + n..] {
            assert_eq!(*outcome, RepairOutcome::Skipped(SkipReason::Budget));
        }
        assert_eq!(
            (
                stats.queued,
                stats.repaired,
                stats.skipped_budget,
                stats.skipped_not_stale,
                stats.skipped_tag_ahead,
                stats.failed
            ),
            (36, n as u64, 34 - n as u64, 1, 1, 0)
        );
        assert_eq!(spent.batch + spent.write, batches);
        assert_eq!((spent.read, spent.serial_read, spent.cas), (0, 0, 0));
        assert_consistent(&check(&backend, &t).await);
    }

    #[tokio::test]
    async fn a_failed_repair_is_counted_and_never_fails_the_run() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("repair-failed");
        let tenant = context(&t);
        let mut context = RepairContext::new(&t);
        for id in ["p", "q"] {
            let (row, stale) = updated(&backend, &tenant, &t, id).await;
            context.queue("Patient", id, Some(row), stale);
        }
        backend.session.inject_fault(
            StatementKind::Write,
            "INSERT INTO resources_by_type",
            Fault::FailWithUnavailable,
        );

        let (before, at) = (backend.repair_stats(), backend.statement_counts());
        let outcomes = context.run(&backend).await;
        let stats = backend.repair_stats().since(&before);
        let spent = backend.statement_counts().since(&at);
        eprintln!("[repair case=failed {stats}]");

        assert_eq!(outcomes.len(), 2);
        let failed: Vec<_> = outcomes
            .iter()
            .filter(|o| {
                matches!(
                    o,
                    RepairOutcome::Failed {
                        failure: WriteFailure::TriesExhausted,
                        ..
                    }
                )
            })
            .collect();
        let repaired: Vec<_> = outcomes
            .iter()
            .filter(|o| matches!(o, RepairOutcome::Repaired { full: true, .. }))
            .collect();
        assert_eq!((failed.len(), repaired.len()), (1, 1), "{outcomes:?}");
        assert_eq!((stats.queued, stats.repaired, stats.failed), (2, 1, 1));
        let batches: u64 = outcomes
            .iter()
            .map(|o| match o {
                RepairOutcome::Repaired { index, .. } | RepairOutcome::Failed { index, .. } => {
                    index.batches
                }
                RepairOutcome::Skipped(_) => 0,
            })
            .sum();
        assert_eq!(spent.batch + spent.write, batches);
        assert_consistent(&check(&backend, &t).await);
    }

    #[tokio::test]
    async fn skips_and_undecodable_rows_send_nothing_and_never_fail_the_run() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("repair-skips");
        let rbt = IndexKey::ResourcesByType;
        let stale = |tag: i32| vec![s(rbt.clone(), Some(lu(1)), tag)];
        let mut bad_keys = r1(2, lu(2), &[]);
        bad_keys.index_keys = Some(vec![0x02, 0x00]);
        let mut bad_ts = r1(2, lu(2), &[]);
        bad_ts.write_ts = Some(write_ts_of(lu(2)) + 1);

        let mut context = RepairContext::new(&t);
        context.queue("Patient", "none", None, stale(1));
        context.queue("Patient", "ahead", Some(r1(2, lu(2), &[])), stale(3));
        context.queue("Patient", "keys", Some(bad_keys), stale(1));
        context.queue("Patient", "write-ts", Some(bad_ts), stale(1));
        context.queue("Patient", "current", Some(r1(2, lu(2), &[])), stale(2));
        context.skip_undecodable("date_index", "version_id: unset");
        context.skip_undecodable(
            "token_code_index",
            &StaleIndexRow::new(token("t", "a"), None, 1).unwrap_err(),
        );

        let (before, at) = (backend.repair_stats(), backend.statement_counts());
        let outcomes = context.run(&backend).await;
        let stats = backend.repair_stats().since(&before);
        eprintln!("[repair case=skips {stats}]");

        assert_eq!(
            outcomes,
            [
                RepairOutcome::Skipped(SkipReason::NoRow),
                RepairOutcome::Skipped(SkipReason::TagAhead),
                RepairOutcome::Skipped(SkipReason::UndecodableKeys),
                RepairOutcome::Skipped(SkipReason::WriteTsMismatch),
                RepairOutcome::Skipped(SkipReason::NotStale),
            ]
        );
        assert_eq!(
            (
                stats.skipped_no_row,
                stats.skipped_tag_ahead,
                stats.skipped_undecodable_keys,
                stats.skipped_write_ts,
                stats.skipped_not_stale,
                stats.skipped_budget,
            ),
            (1, 1, 1, 1, 1, 0)
        );
        assert_eq!(
            (stats.queued, stats.undecodable_rows, stats.repaired),
            (5, 2, 0)
        );
        assert_eq!(backend.statement_counts().since(&at).total(), 0);
    }

    #[tokio::test]
    async fn repair_retags_a_date_row_the_current_version_still_produces() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("planted-repair-retag");
        let tenant = context(&t);
        let p = backend
            .create(&tenant, "Patient", patient(), test_fhir_version())
            .await
            .expect("create p");
        let ts1 = r1_of(&backend, &t, "p").await.write_ts.expect("write_ts");
        backend
            .update(&tenant, &p, changed())
            .await
            .expect("update p");
        let row = r1_of(&backend, &t, "p").await;
        assert_eq!(row.version_id, 2);
        let ts2 = row.write_ts.expect("write_ts");

        let (d, de) = (date("repair-date"), date_by_end("repair-date"));
        let head = |param: &str| {
            vec![
                Value::from(t.as_str()),
                Value::from("Patient"),
                Value::from(param),
            ]
        };
        let (start, end) = (
            DateTime::from_timestamp_millis(978_307_200_000).unwrap(),
            DateTime::from_timestamp_millis(978_393_600_000).unwrap(),
        );
        let mut x5 = head("repair-date");
        x5.extend([
            Value::from(start),
            Value::from("p"),
            Value::from(end),
            Value::from(1),
            Value::from(ts1),
        ]);
        plant(&backend, PLANT_X5, x5).await;
        let mut x6 = head("repair-date");
        x6.extend([
            Value::from(end),
            Value::from("p"),
            Value::from(start),
            Value::from(1),
            Value::from(ts1),
        ]);
        plant(&backend, PLANT_X6, x6).await;

        let mut planted: BTreeSet<IndexKey> = keys_of(&row).into_iter().collect();
        planted.insert(d.clone());
        planted.insert(de);
        let blob = index_keys::encode(&planted.iter().cloned().collect::<Vec<_>>());
        plant_resource_keys(&backend, &t, blob.clone(), 2).await;
        plant_version_keys(&backend, &t, 2, blob, ts2 + 1).await;

        let row = r1_of(&backend, &t, "p").await;
        let report = index_consistency(
            &backend,
            &t,
            Scope::Tenant,
            ScanMode::Full,
            Registry::Changed,
        )
        .await;
        assert_eq!(report.kinds(), expect(&[("wrong_tag", 2)]), "{report}");

        let (before, at) = (backend.repair_stats(), backend.statement_counts());
        let outcome = backend
            .repair_index(
                &key(&t, "p"),
                &row,
                &[StaleIndexRow::new(d, None, 1).unwrap()],
            )
            .await;
        let spent = backend.statement_counts().since(&at);
        assert_eq!((spent.read, spent.serial_read, spent.cas), (0, 0, 0));
        eprintln!(
            "[repair case=retag {}]",
            backend.repair_stats().since(&before)
        );
        let RepairOutcome::Repaired { full: true, index } = outcome else {
            panic!("{outcome:?}");
        };
        assert_eq!(index.rows_inserted, planted.len() as u64);
        assert_eq!(index.rows_deleted, 0);
        for table in ["date_index", "date_index_by_end"] {
            let rows = backend
                .session
                .rows_paged(
                    &format!("SELECT id, version_id, WRITETIME(version_id) AS written FROM {table} WHERE tenant_id = ? AND resource_type = ? AND param = ?"),
                    vec![
                        Value::from(t.as_str()),
                        Value::from("Patient"),
                        Value::from("repair-date"),
                    ],
                    StatementKind::Read,
                )
                .await
                .expect("read the date rows");
            let got: Vec<(String, i32, i64)> = rows
                .iter()
                .map(|r| {
                    (
                        r.get_r_by_name("id").unwrap(),
                        r.get_r_by_name("version_id").unwrap(),
                        r.get_r_by_name("written").unwrap(),
                    )
                })
                .collect();
            assert_eq!(got, [("p".to_string(), 2, ts2)], "{table}");
        }
        let report = index_consistency(
            &backend,
            &t,
            Scope::Tenant,
            ScanMode::Full,
            Registry::Changed,
        )
        .await;
        assert_consistent(&report);
    }

    type Unit = std::pin::Pin<Box<dyn Future<Output = ()>>>;

    /// One stalled future at the head, and eight behind it that each run for 10 ms.
    #[tokio::test(start_paused = true)]
    async fn a_stalled_repair_holds_up_no_other_and_four_run_at_once() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let (in_flight, most, finished) = (
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        let enter = |in_flight: &AtomicUsize, most: &AtomicUsize| {
            let now = in_flight.fetch_add(1, SeqCst) + 1;
            most.fetch_max(now, SeqCst);
        };
        let mut futures: Vec<Unit> = Vec::new();
        {
            let (in_flight, most) = (in_flight.clone(), most.clone());
            futures.push(Box::pin(async move {
                enter(&in_flight, &most);
                std::future::pending::<()>().await;
            }));
        }
        for _ in 0..8 {
            let (in_flight, most, finished) = (in_flight.clone(), most.clone(), finished.clone());
            futures.push(Box::pin(async move {
                enter(&in_flight, &most);
                tokio::time::sleep(Duration::from_millis(10)).await;
                in_flight.fetch_sub(1, SeqCst);
                finished.fetch_add(1, SeqCst);
            }));
        }
        let started = tokio::time::Instant::now();
        drive_within(
            futures,
            REPAIR_CONCURRENCY,
            started + Duration::from_secs(10),
        )
        .await;
        assert_eq!(finished.load(SeqCst), 8);
        assert_eq!(most.load(SeqCst), 4);
        assert_eq!(started.elapsed(), Duration::from_secs(10));
    }

    /// A deadline already past cuts every future after its first poll: the
    /// first `concurrency` are started (polled once, to their first yield),
    /// none gets to run on, and the rest are never polled.
    #[tokio::test]
    async fn a_deadline_already_past_cuts_every_future_after_its_first_poll() {
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let (polled, finished) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let futures: Vec<Unit> = (0..6)
            .map(|_| {
                let (polled, finished) = (polled.clone(), finished.clone());
                Box::pin(async move {
                    polled.fetch_add(1, SeqCst);
                    tokio::task::yield_now().await;
                    finished.fetch_add(1, SeqCst);
                }) as Unit
            })
            .collect();
        drive_within(futures, 4, tokio::time::Instant::now()).await;
        assert_eq!(polled.load(SeqCst), 4);
        assert_eq!(finished.load(SeqCst), 0);
    }

    #[test]
    fn outcomes_come_from_the_plan_for_a_skip_and_from_the_slot_for_a_repair() {
        let repaired = RepairOutcome::Repaired {
            full: true,
            index: IndexWriteStats::default(),
        };
        let failed = RepairOutcome::Failed {
            failure: WriteFailure::TriesExhausted,
            index: IndexWriteStats::default(),
        };
        let planned: Vec<Result<(), SkipReason>> = vec![
            Ok(()),
            Err(SkipReason::NoRow),
            Ok(()),
            Ok(()),
            Err(SkipReason::Budget),
            Err(SkipReason::NoRow),
            Err(SkipReason::TagAhead),
        ];
        // Repair 0 stalled, 2 finished behind it, 3 stalled.
        let done = [None, None, Some(repaired), None, None, None, None];
        let mut cut = Vec::new();
        let outcomes = outcomes_of(
            planned
                .iter()
                .zip(done)
                .enumerate()
                .map(|(i, (planned, done))| (planned, done, i)),
            |i| {
                cut.push(i);
                failed
            },
        );
        assert_eq!(
            outcomes,
            [
                failed,
                RepairOutcome::Skipped(SkipReason::NoRow),
                repaired,
                failed,
                RepairOutcome::Skipped(SkipReason::Budget),
                RepairOutcome::Skipped(SkipReason::NoRow),
                RepairOutcome::Skipped(SkipReason::TagAhead),
            ]
        );
        assert_eq!(cut, [0, 3]);
    }

    #[test]
    fn a_cut_repair_reports_what_it_packed_and_one_that_never_started_nothing() {
        let mut packed = IndexWriteStats::default();
        packed.batch(5, 1, 827);
        assert_eq!(
            cut_outcome(Some(packed)),
            RepairOutcome::Failed {
                failure: WriteFailure::TriesExhausted,
                index: packed,
            }
        );
        assert_eq!(
            cut_outcome(None),
            RepairOutcome::Failed {
                failure: WriteFailure::TriesExhausted,
                index: IndexWriteStats::default(),
            }
        );
    }

    #[test]
    fn the_phase_deadline_is_the_request_deadline() {
        assert_eq!(phase_deadline(1234), Duration::from_millis(1234));
        assert_eq!(phase_deadline(0), Duration::ZERO);
    }

    fn queued(id: &str, row: Option<ResourceRow>, stale: Vec<StaleIndexRow>) -> QueuedRepair {
        QueuedRepair {
            resource_type: "Patient".to_string(),
            id: id.to_string(),
            row,
            stale,
        }
    }

    /// `run`'s planning and admission: each entry's repair size, or its skip.
    fn admitted_sizes(queue: &[QueuedRepair]) -> Vec<Result<usize, SkipReason>> {
        let plans = plan_queue(queue);
        admit_plans(&plans)
            .into_iter()
            .map(|admitted| admitted.map(|a| delta_rows(&a.delta)))
            .collect()
    }

    /// `n` found rows tagged 1, each its own row to delete.
    fn found_rows(n: usize) -> Vec<StaleIndexRow> {
        (0..n)
            .map(|i| s(token("s", &format!("{i}")), Some(lu(1)), 1))
            .collect()
    }

    #[test]
    fn a_repair_over_the_row_limit_is_skipped_and_a_later_smaller_one_is_admitted() {
        let rbt = IndexKey::ResourcesByType;
        let row = r1(2, lu(2), std::slice::from_ref(&rbt));
        let entry = |deletes: usize| queued("p", Some(row.clone()), found_rows(deletes));
        // Each repair is 1 insert plus its deletes: 501, then 601 (over the 512
        // limit), 2, 10 (503 + 10 = 513, over), 9 (512 exactly), then 2 (over).
        let queue = [
            entry(500),
            entry(600),
            entry(1),
            entry(9),
            entry(8),
            entry(1),
        ];
        assert_eq!(
            admitted_sizes(&queue),
            [
                Ok(501),
                Err(SkipReason::Budget),
                Ok(2),
                Err(SkipReason::Budget),
                Ok(9),
                Err(SkipReason::Budget),
            ]
        );
    }

    #[test]
    fn once_32_repairs_are_admitted_later_entries_still_run_the_checks_before_budget() {
        let rbt = IndexKey::ResourcesByType;
        let row = r1(2, lu(2), std::slice::from_ref(&rbt));
        let mut queue: Vec<QueuedRepair> = (0..MAX_REPAIRS_PER_SEARCH)
            .map(|_| queued("p", Some(row.clone()), found_rows(1)))
            .collect();
        let mut bad_keys = row.clone();
        bad_keys.index_keys = Some(vec![0x02, 0x00]);
        queue.push(queued("p", Some(row.clone()), found_rows(1)));
        queue.push(queued("p", Some(bad_keys), found_rows(1)));
        queue.push(queued(
            "p",
            Some(row.clone()),
            vec![s(rbt.clone(), Some(lu(3)), 3)],
        ));
        queue.push(queued(
            "p",
            Some(row.clone()),
            vec![s(rbt.clone(), Some(lu(2)), 2)],
        ));
        queue.push(queued("p", None, found_rows(1)));
        let sizes = admitted_sizes(&queue);
        assert!(sizes[..MAX_REPAIRS_PER_SEARCH].iter().all(|r| *r == Ok(2)));
        assert_eq!(
            sizes[MAX_REPAIRS_PER_SEARCH..],
            [
                Err(SkipReason::Budget),
                Err(SkipReason::UndecodableKeys),
                Err(SkipReason::TagAhead),
                Err(SkipReason::NotStale),
                Err(SkipReason::NoRow),
            ]
        );
    }

    #[test]
    fn the_message_of_a_refused_row_names_no_indexed_value() {
        let message =
            StaleIndexRow::new(token("identifier", "secret-mrn-42"), None, 1).unwrap_err();
        assert!(!message.contains("secret-mrn-42"), "{message}");
        assert!(message.contains("token_code_index"), "{message}");
    }

    /// The whole phase has one deadline. With a deadline of zero the driver
    /// polls each repair once, which starts its write, then sees the deadline
    /// has passed and drops them, so no repair can finish: the cut does not
    /// depend on the node's speed or the clock. Every repair is `Failed`,
    /// counted, and `run` returns at once. The writes that were started may
    /// have applied, so a second, ordinary run repeats them and ends
    /// consistent.
    #[tokio::test]
    async fn repair_returns_at_its_deadline_with_the_unfinished_failed() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("repair-deadline");
        let tenant = context(&t);
        let rows = [
            ("p", updated(&backend, &tenant, &t, "p").await),
            ("q", updated(&backend, &tenant, &t, "q").await),
        ];
        let queue = || {
            let mut context = RepairContext::new(&t);
            for (id, (row, stale)) in &rows {
                context.queue("Patient", id, Some(row.clone()), stale.clone());
            }
            context
        };

        let before = backend.repair_stats();
        let outcomes = queue().run_within(&backend, Duration::ZERO).await;
        let stats = backend.repair_stats().since(&before);
        eprintln!("[repair case=deadline {stats}]");
        assert_eq!(outcomes.len(), 2);
        for outcome in &outcomes {
            assert!(
                matches!(
                    outcome,
                    RepairOutcome::Failed {
                        failure: WriteFailure::TriesExhausted,
                        ..
                    }
                ),
                "{outcomes:?}"
            );
        }
        assert_eq!((stats.queued, stats.failed, stats.repaired), (2, 2, 0));

        let outcomes = queue().run(&backend).await;
        assert!(
            outcomes
                .iter()
                .all(|o| matches!(o, RepairOutcome::Repaired { full: true, .. })),
            "{outcomes:?}"
        );
        assert_consistent(&check(&backend, &t).await);
    }

    /// Five repairs and two skips with a deadline of zero. The driver polls
    /// the first four repairs once, which packs their rows and starts their
    /// writes, then drops them; the fifth never starts. So the first four are
    /// `Failed` with their packed rows, the fifth is `Failed` with none, the
    /// skips behind them stay skips, and the counters equal the sum of the
    /// outcomes.
    #[tokio::test]
    async fn skips_behind_cut_repairs_stay_skips_and_the_counters_add_up() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("repair-cut");
        let tenant = context(&t);
        let mut rows = Vec::new();
        for i in 0..5 {
            let id = format!("r{i}");
            rows.push((id.clone(), updated(&backend, &tenant, &t, &id).await));
        }
        let k2_len = keys_of(&rows[0].1.0).len() as u64;
        let queue = || {
            let mut context = RepairContext::new(&t);
            for (id, (row, stale)) in &rows {
                context.queue("Patient", id, Some(row.clone()), stale.clone());
            }
            context.queue("Patient", "none0", None, Vec::new());
            context.queue("Patient", "none1", None, Vec::new());
            context
        };

        let before = backend.repair_stats();
        let outcomes = queue().run_within(&backend, Duration::ZERO).await;
        let stats = backend.repair_stats().since(&before);
        eprintln!("[repair case=cut {stats}]");
        assert_eq!(outcomes.len(), 7);
        for outcome in &outcomes[..4] {
            let RepairOutcome::Failed { failure, index } = outcome else {
                panic!("{outcomes:?}");
            };
            assert_eq!(*failure, WriteFailure::TriesExhausted);
            assert_eq!((index.rows_inserted, index.rows_deleted), (k2_len, 1));
            assert!(index.batches >= 1);
        }
        assert_eq!(
            outcomes[4],
            RepairOutcome::Failed {
                failure: WriteFailure::TriesExhausted,
                index: IndexWriteStats::default(),
            }
        );
        assert_eq!(
            outcomes[5..],
            [RepairOutcome::Skipped(SkipReason::NoRow); 2]
        );
        assert_eq!(
            (
                stats.queued,
                stats.repaired,
                stats.failed,
                stats.skipped_no_row
            ),
            (7, 0, 5, 2)
        );
        assert_eq!((stats.rows_inserted, stats.rows_deleted), (4 * k2_len, 4));

        let outcomes = queue().run(&backend).await;
        assert!(
            outcomes[..5]
                .iter()
                .all(|o| matches!(o, RepairOutcome::Repaired { full: true, .. })),
            "{outcomes:?}"
        );
        assert_consistent(&check(&backend, &t).await);
    }

    /// A Patient with more than 64 keys whose update crashed after its
    /// version row: a partial repair moves the found `resources_by_type`
    /// row to the current version, so `count` never drops the resource.
    #[tokio::test]
    async fn a_partial_repair_moves_the_type_row_and_count_stays() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("repair-partial");
        let tenant = context(&t);
        let body = |active: bool| {
            let identifiers: Vec<Json> = (0..25)
                .map(|i| json!({"system": "urn:repair", "value": format!("v{i}")}))
                .collect();
            json!({"resourceType": "Patient", "id": "p", "active": active, "identifier": identifiers})
        };
        let p = backend
            .create(&tenant, "Patient", body(true), test_fhir_version())
            .await
            .expect("create p");
        let first = r1_of(&backend, &t, "p").await;
        let (lu1, k1) = (first.last_updated, keys_of(&first));
        crash_update(&backend, &tenant, &t, &p, body(false)).await;
        let row = r1_of(&backend, &t, "p").await;
        assert_eq!(row.version_id, 2);
        let (lu2, k2) = (row.last_updated, keys_of(&row));
        assert!(k2.len() > MAX_FULL_REPAIR_KEYS, "{} keys", k2.len());
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 1);

        let stale = found(&type_search(&backend, &t).await, "p", 2);
        assert_eq!(stale.len(), 1);
        let mut context = RepairContext::new(&t);
        context.queue("Patient", "p", Some(row.clone()), stale);
        let outcomes = context.run(&backend).await;
        let [RepairOutcome::Repaired { full: false, index }] = outcomes[..] else {
            panic!("{outcomes:?}");
        };
        assert_eq!((index.rows_inserted, index.rows_deleted), (1, 1));
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 1);
        let rows: Vec<_> = type_search(&backend, &t)
            .await
            .into_iter()
            .filter(|(id, ..)| id == "p")
            .map(|(_, at, tag)| (at, tag))
            .collect();
        assert_eq!(rows, [(lu2, 2)]);
        // Only the found row was written: the token rows wait for the sweeper.
        let report = check(&backend, &t).await;
        assert_eq!(
            report.kinds(),
            expect(&[
                ("missing_row", k2.len() - 1),
                ("stray_row", not_produced(&k1, lu1, &k2, lu2) - 1),
            ]),
            "{report}"
        );

        // The sweeper's full delta then leaves the tenant consistent.
        let previous: Vec<IndexRow<'_>> = k1.iter().map(|k| (k, lu1)).collect();
        let delta = index_delta(
            &previous,
            VersionKeys {
                version_id: 2,
                last_updated: lu2,
                fhir_version: row.fhir_version,
                keys: &k2,
            },
        );
        let done = backend
            .write_index_delta(&key(&t, "p"), &delta, write_ts_of(lu2), 3)
            .await;
        assert_eq!(done.result, Ok(()));
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 1);
        assert_consistent(&check(&backend, &t).await);
    }
}
