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
//! write yet. Repair finishes it, from R1's own `index_keys`, K(current),
//! without reading anything else (RV4).
//!
//! - **Full replay (RV1).** When K(current) has at most
//!   [`MAX_FULL_REPAIR_KEYS`] keys, repair inserts every key of it at R1's
//!   `last_updated` and deletes every found row below R1 that K(current)
//!   does not produce (compared by [`IndexKey::row_at`], IW7).
//! - **Partial replay (RV1).** Above that, repair only touches the rows the
//!   search found: a found row is re-tagged when a key of K(current) names
//!   the same row (only a date row can, since its key has no
//!   `last_updated`), and any other found row is deleted.
//! - **Skips (RV3).** Nothing is written when a found row is tagged above
//!   R1 (R1 lags behind a write: [`SkipReason::TagAhead`]), when there is no
//!   `resources` row ([`SkipReason::NoRow`]), when R1's `write_ts` is not
//!   `write_ts_of(last_updated)` ([`SkipReason::WriteTsMismatch`]), when
//!   R1's key set cannot be decoded ([`SkipReason::UndecodableKeys`]), when no
//!   found row is below R1 ([`SkipReason::NotStale`]), or when the search's
//!   budget is spent ([`SkipReason::Budget`], RV5).
//! - **A newer version wins.** Repair writes at `write_ts_of(R1.last_updated)`
//!   (RW10), so if the resource was updated after R1 was read, the newer
//!   version's rows carry a higher timestamp and win over this repair's:
//!   its writes are confirmed and change nothing visible (RV4).
//!
//! Repair never writes `resources`, `resource_versions` or an intent row,
//! and never sends a CAS. Its only statements are the index writer's
//! batches ([`CassandraBackend::write_index_delta`], one try, IW17).
//!
//! # Step 6's contract
//!
//! - Call [`RepairContext::run`] at the end of each page, on success and on
//!   `TooManyResults`, inside the request's task, never through
//!   `tokio::spawn`. Its result never fails the search.
//! - Queue every candidate the tag check hides for its tag: an index row
//!   whose `version_id` is not R1's, a deleted R1, or no R1. Pass the tag
//!   check's own R1. That R1 is `SELECT_RESOURCE`, which reads the row's
//!   `index_keys`, so repair needs no other read (RV4).
//! - Send a row the search cannot decode, or one [`StaleIndexRow::new`]
//!   refuses, to [`RepairContext::skip_undecodable`].
//! - Remove the `#![allow(dead_code)]` below when the search calls this
//!   module.

#![allow(dead_code)] // Step 6's search calls it; until then only its tests do (RV6). Step 6 removes this.

use chrono::{DateTime, Utc};
use futures::StreamExt;

use super::backend::CassandraBackend;
use super::index_keys::{self, IndexKey, RowId};
use super::read::ResourceRow;
use super::stats::IndexWriteStats;
use super::write::{
    IndexDelta, IndexRow, RowKey, VersionKeys, WriteFailure, index_delta, write_ts_of,
};

/// Resources one search repairs at most (RV5, RV8): a chosen budget, not a measurement.
pub(crate) const MAX_REPAIRS_PER_SEARCH: usize = 32;
/// Index rows, inserts plus deletes, one search's repairs write at most (RV5, RV8).
pub(crate) const MAX_REPAIR_ROWS_PER_SEARCH: usize = 512;
/// Repairs of one search in flight at once (RV5, RV8): futures in the search's own task.
pub(crate) const REPAIR_CONCURRENCY: usize = 4;
/// Above this many keys in K(current), repair writes only the rows found (RV1): a chosen budget.
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
    pub(crate) fn new(
        key: IndexKey,
        last_updated: Option<DateTime<Utc>>,
        version_id: i32,
    ) -> Result<Self, String> {
        let is_date = matches!(key, IndexKey::Date { .. } | IndexKey::DateByEnd { .. });
        if last_updated.is_none() && !is_date {
            return Err(format!(
                "index row {key:?} has no last_updated, so it names no row"
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
    /// Keys inserted at R1's `last_updated`: K(current) whole, or the found rows it still names.
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
        self.delta(row)
            .units
            .iter()
            .map(|unit| unit.inserts.len() + unit.deletes.len())
            .sum()
    }
}

/// RV3's checks, then RV1's plan. Pure: sends, logs and counts nothing.
fn plan_repair(row: &ResourceRow, stale: &[StaleIndexRow]) -> Result<RepairPlan, SkipReason> {
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
    let current = row
        .index_keys
        .as_deref()
        .and_then(|blob| index_keys::decode(blob).ok())
        .ok_or(SkipReason::UndecodableKeys)?;
    let full = current.len() <= MAX_FULL_REPAIR_KEYS;
    let next = if full {
        current
    } else {
        current
            .into_iter()
            .filter(|key| {
                let id = key.row_at(row.last_updated);
                below.iter().any(|s| s.row_id() == id)
            })
            .collect()
    };
    let previous = below
        .iter()
        .map(|s| (s.key.clone(), s.delete_at()))
        .collect();
    Ok(RepairPlan {
        next,
        previous,
        full,
    })
}

/// One search's budget (RV5, RV8).
#[derive(Debug, Default)]
struct Budget {
    resources: usize,
    rows: usize,
}

impl Budget {
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
    pub(crate) fn skip_undecodable(&mut self, table: &str, error: &str) {
        tracing::error!(
            tenant = %self.tenant_id,
            table,
            error,
            "repair on read skipped an index row it could not decode"
        );
        self.undecodable_rows += 1;
    }

    /// Plans, admits, writes; outcomes in queue order. Never fails.
    pub(crate) async fn run(self, backend: &CassandraBackend) -> Vec<RepairOutcome> {
        let counters = &backend.repair_counters;
        counters.queued(self.queued.len() as u64);
        counters.undecodable_rows(self.undecodable_rows);

        let tenant_id = self.tenant_id.as_str();
        let mut outcomes: Vec<Option<RepairOutcome>> = vec![None; self.queued.len()];
        let mut budget = Budget::default();
        let mut admitted: Vec<(usize, &QueuedRepair, &ResourceRow, RepairPlan)> = Vec::new();
        for (at, queued) in self.queued.iter().enumerate() {
            let key = RowKey {
                tenant_id,
                resource_type: &queued.resource_type,
                id: &queued.id,
            };
            let skip = match queued.row.as_ref() {
                None => Err(SkipReason::NoRow),
                Some(row) => plan_repair(row, &queued.stale).and_then(|plan| {
                    if budget.admit(plan.rows(row)) {
                        Ok((row, plan))
                    } else {
                        Err(SkipReason::Budget)
                    }
                }),
            };
            match skip {
                Ok((row, plan)) => admitted.push((at, queued, row, plan)),
                Err(reason) => {
                    let version_id = queued.row.as_ref().map(|row| row.version_id);
                    backend.note_repair_skip(&key, version_id, reason);
                    outcomes[at] = Some(RepairOutcome::Skipped(reason));
                }
            }
        }

        // Built into a Vec first so the stream's items are plain futures
        // (the same `Send` reason as in `write_index_delta_within`).
        let futures: Vec<_> = admitted
            .iter()
            .map(|(_, queued, row, plan)| async move {
                let key = RowKey {
                    tenant_id,
                    resource_type: &queued.resource_type,
                    id: &queued.id,
                };
                backend.send_repair(&key, row, plan).await
            })
            .collect();
        let sent: Vec<RepairOutcome> = futures::stream::iter(futures)
            .buffered(REPAIR_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        for ((at, ..), outcome) in admitted.iter().zip(sent) {
            outcomes[*at] = Some(outcome);
        }
        outcomes
            .into_iter()
            .map(|outcome| outcome.unwrap_or(RepairOutcome::Skipped(SkipReason::NotStale)))
            .collect()
    }
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
            Ok(plan) => self.send_repair(key, row, &plan).await,
            Err(reason) => {
                self.note_repair_skip(key, Some(row.version_id), reason);
                RepairOutcome::Skipped(reason)
            }
        }
    }

    /// Writes `plan` through `write_index_delta` at `write_ts_of(row.last_updated)`, `REPAIR_TRIES`; logs and counts.
    async fn send_repair(
        &self,
        key: &RowKey<'_>,
        row: &ResourceRow,
        plan: &RepairPlan,
    ) -> RepairOutcome {
        let delta = plan.delta(row);
        let outcome = self
            .write_index_delta(key, &delta, write_ts_of(row.last_updated), REPAIR_TRIES)
            .await;
        match outcome.result {
            Ok(()) => {
                tracing::info!(
                    tenant = %key.tenant_id,
                    resource_type = key.resource_type,
                    id = key.id,
                    version_id = row.version_id,
                    full = plan.full,
                    index = %outcome.stats,
                    "repair on read wrote the current version's index rows"
                );
                self.repair_counters.repaired(plan.full, &outcome.stats);
                RepairOutcome::Repaired {
                    full: plan.full,
                    index: outcome.stats,
                }
            }
            Err(failure) => {
                tracing::warn!(
                    tenant = %key.tenant_id,
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
            tenant = %key.tenant_id,
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
    use std::sync::Arc;

    use cdrs_tokio::types::IntoRustByName;
    use cdrs_tokio::types::value::Value;
    use chrono::TimeZone;
    use serde_json::{Value as Json, json};

    use super::*;
    use crate::backends::cassandra::checkpoint::Checkpoint;
    use crate::backends::cassandra::consistency::harness::*;
    use crate::backends::cassandra::consistency::{assert_consistent, version_ids};
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

    #[test]
    fn over_64_keys_repairs_only_the_found_rows_and_retags_their_date_rows() {
        let rbt = IndexKey::ResourcesByType;
        let mut keys = vec![rbt.clone()];
        keys.extend((0..64).map(|i| token("c", &format!("{i:02}"))));
        keys.push(date("d"));
        assert_eq!(keys.len(), 66);
        let row = r1(2, lu(2), &keys);
        let stale = [
            s(date("d"), None, 1),
            s(rbt.clone(), Some(lu(1)), 1),
            s(token("c", "x"), Some(lu(1)), 1),
        ];
        let plan = plan_repair(&row, &stale).unwrap();
        assert!(!plan.full);
        assert_eq!(plan.next, vec![date("d")]);
        let (inserts, deletes) = flat(&plan.delta(&row));
        assert_eq!(inserts, vec![(date("d"), lu(2))]);
        assert_eq!(
            deletes,
            sorted(vec![(rbt.clone(), lu(1)), (token("c", "x"), lu(1))])
        );
        assert_eq!(plan.rows(&row), 3);

        let mut keys = vec![rbt];
        keys.extend((0..63).map(|i| token("c", &format!("{i:02}"))));
        assert_eq!(keys.len(), 64);
        let plan = plan_repair(&r1(2, lu(2), &keys), &stale).unwrap();
        assert!(plan.full);
        assert_eq!(plan.next.len(), 64);
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
        let mut hold = backend.hold_at(Checkpoint::AfterVersionRow, t, "p", 2);
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
        let before = backend.repair_stats();
        let outcomes = context.run(&backend).await;
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
        let mut k2_len = 0;
        for i in 0..34 {
            let id = format!("r{i:02}");
            let (row, stale) = updated(&backend, &tenant, &t, &id).await;
            k2_len = keys_of(&row).len();
            context.queue("Patient", &id, Some(row), stale);
        }
        let rows_each = k2_len + 1;
        let n = MAX_REPAIRS_PER_SEARCH.min(MAX_REPAIR_ROWS_PER_SEARCH / rows_each);

        let (before, at) = (backend.repair_stats(), backend.statement_counts());
        let outcomes = context.run(&backend).await;
        let stats = backend.repair_stats().since(&before);
        let spent = backend.statement_counts().since(&at);
        eprintln!("[repair case=budget {stats}]");

        assert_eq!(outcomes.len(), 34);
        for outcome in &outcomes[..n] {
            let RepairOutcome::Repaired { full: true, index } = outcome else {
                panic!("{outcome:?}");
            };
            assert_eq!(index.rows_inserted, k2_len as u64);
            assert_eq!(index.rows_deleted, 1);
        }
        for outcome in &outcomes[n..] {
            assert_eq!(*outcome, RepairOutcome::Skipped(SkipReason::Budget));
        }
        assert_eq!(
            (
                stats.queued,
                stats.repaired,
                stats.skipped_budget,
                stats.failed
            ),
            (34, n as u64, 34 - n as u64, 0)
        );
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
}
