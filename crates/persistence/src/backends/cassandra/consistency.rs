//! The index consistency check, tests only (ADR-sam-michaels-0006, RV10 to
//! RV14; roadmap Step 5's exit criterion). One full scan of each index table
//! (RV12: a plain SELECT with no WHERE, which needs no ALLOW FILTERING), kept
//! to `test-` tenants in Rust, compared with each resource's stored key sets
//! and, while the tenant's registry is unchanged, with a fresh extraction by
//! the writer's own helper (RV13). Reading only the partitions the stored
//! keys name would miss a row written to a partition no key set names, the
//! bug class the check exists for (D2).
//!
//! What the check sends is reads only: the scans and the per-resource reads
//! run at `LOCAL_QUORUM` (`StatementKind::Read`). On a replication factor of
//! 3 a quorum read can trigger read repair, so the node may write replicas
//! in response; the check itself never writes.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt;
use std::time::{Duration, Instant};

use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::blob::Blob;
use cdrs_tokio::types::rows::Row;
use cdrs_tokio::types::value::Value;
use chrono::{DateTime, Utc};
use serde_json::Value as Json;

use super::backend::{CassandraBackend, SELECT_RESOURCE_KEYS};
use super::index_keys::{self, IndexKey, RowId};
use super::read::{ResourceRow, deleted_at_of};
use super::session::StatementKind;
use super::write::write_ts_of;

/// Rows per page of every scan: small pages keep the tombstones one page
/// reads far below `tombstone_failure_threshold`; a refused page is a
/// `ReadFailed` defect, never a pass.
const SCAN_PAGE_SIZE: i32 = 1000;

/// The seven index tables and their full-scan statements (RV12).
const SCANS: [(&str, &str); 7] = [
    (
        "resources_by_type",
        "SELECT tenant_id, resource_type, last_updated, id, version_id, WRITETIME(version_id) AS written FROM resources_by_type",
    ),
    (
        "token_code_index",
        "SELECT tenant_id, resource_type, param, code, last_updated, id, system, version_id, WRITETIME(version_id) AS written FROM token_code_index",
    ),
    (
        "token_system_code_index",
        "SELECT tenant_id, resource_type, param, system, code, last_updated, id, version_id, WRITETIME(version_id) AS written FROM token_system_code_index",
    ),
    (
        "token_system_index",
        "SELECT tenant_id, resource_type, param, system, last_updated, id, code, version_id, WRITETIME(version_id) AS written FROM token_system_index",
    ),
    (
        "date_index",
        "SELECT tenant_id, resource_type, param, start, id, end, version_id, WRITETIME(version_id) AS written FROM date_index",
    ),
    (
        "date_index_by_end",
        "SELECT tenant_id, resource_type, param, end, id, start, version_id, WRITETIME(version_id) AS written FROM date_index_by_end",
    ),
    (
        "uri_index",
        "SELECT tenant_id, resource_type, param, uri, last_updated, id, version_id, WRITETIME(version_id) AS written FROM uri_index",
    ),
];

/// Every version row of one resource (single partition, paged).
const SELECT_VERSION_ROWS: &str = "SELECT version_id, data, last_updated, deleted_at, \
     write_ts, index_keys FROM resource_versions \
     WHERE tenant_id = ? AND resource_type = ? AND id = ?";

/// The defects' `kind()` names, in the report line's order.
const DEFECT_KINDS: [&str; 13] = [
    "missing_row",
    "stray_row",
    "wrong_tag",
    "wrong_write_time",
    "stale_extraction",
    "version_reextraction",
    "marker_not_empty",
    "blob_mismatch",
    "missing_version_row",
    "undecodable",
    "read_failed",
    "no_type_key",
    "write_ts_mismatch",
];

/// Which resources of the tenant are checked.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Scope<'a> {
    /// Every resource with a `resources` row, found with
    /// `SELECT DISTINCT tenant_id, resource_type, id FROM resources`
    /// (`backend.rs` `SELECT_RESOURCE_KEYS`; RV12), and every index row of
    /// the tenant: a row of an id with no `resources` row is a stray.
    Tenant,
    /// Only these `(resource_type, id)`; index rows of the tenant's other
    /// ids are ignored, not reported. A `Full` scan drops them before
    /// decoding, so one it cannot decode does not fail the check. For a test that leaves one resource
    /// broken on purpose and checks the others.
    Resources(&'a [(&'a str, &'a str)]),
}

/// Where the scanned rows come from (Q2). Either way they are one full scan
/// of the seven tables (RV12).
#[derive(Clone, Copy)]
pub(crate) enum ScanMode<'a> {
    /// Scan now, keeping only this tenant's rows.
    Full,
    /// Use a scan taken earlier for many tenants (one scan, many checks:
    /// the planted test now, the post-suite step in PR 8). Panics unless the
    /// scan covers the tenant, and panics if any resource the check examines
    /// has an R1 or version row `write_ts` at or after the scan's `taken_at`
    /// (a resource written after the scan was taken, so the scan is stale for
    /// it). It cannot catch a write still in flight when the scan started
    /// whose `write_ts` is earlier but whose index rows land later, nor an
    /// index row planted by hand after the scan. Callers take the scan only
    /// after every writer has returned, as T5 does.
    ///
    /// The guard compares the writers' clock-derived `write_ts` with
    /// `taken_at`, so it also assumes the clock does not step back between
    /// the scan and the check. If it does, a write made after the scan can
    /// carry a `write_ts` below `taken_at` and pass the guard: the check then
    /// reports false `missing_row` or `stray_row` defects for that resource
    /// instead of panicking. That is loud but misattributed, and never a
    /// clean report.
    Shared(&'a IndexScan),
}

/// Whether the tenant's search-parameter registry changed during the test
/// (RV14).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Registry {
    /// Unchanged: stored key sets must also equal a fresh extraction.
    Fixed,
    /// Changed (a SearchParameter was written): the two fresh-extraction
    /// comparisons (rules 3 and 4) are skipped; everything else, the stray
    /// scan included, still runs.
    Changed,
}

/// One index row the scan read, decoded into the key it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScannedRow {
    pub(crate) table: &'static str,
    pub(crate) tenant_id: String,
    pub(crate) resource_type: String,
    pub(crate) id: String,
    pub(crate) key: IndexKey,
    /// The `last_updated` clustering column; `None` in the two date tables,
    /// whose primary key has none (IW7).
    pub(crate) last_updated: Option<DateTime<Utc>>,
    /// The `version_id` tag.
    pub(crate) version_id: i32,
    /// `WRITETIME(version_id)`, microseconds: the `USING TIMESTAMP` that left the tag.
    pub(crate) written: i64,
}

impl ScannedRow {
    /// The row as IW7's one rule names it: `key.row_at(last_updated)`, with
    /// the Unix epoch for a date row (`row_at` ignores it there).
    pub(crate) fn row_id(&self) -> RowId<'_> {
        self.key
            .row_at(self.last_updated.unwrap_or(DateTime::UNIX_EPOCH))
    }
}

/// One full scan of the seven index tables, keeping the rows of the `test-`
/// tenants `keep` accepts (RV12).
pub(crate) struct IndexScan {
    keep: Box<dyn Fn(&str) -> bool + Send + Sync>,
    rows: BTreeMap<String, Vec<ScannedRow>>,
    scanned: usize,
    /// The first read that failed, `(what, error)`; the scan stops there.
    failed: Option<(String, String)>,
    /// `Utc::now()` in whole milliseconds, as microseconds, read before the
    /// first table. A write's `write_ts` is `last_updated` floored to the
    /// millisecond (`write_ts_of`), so a write that starts in the scan's
    /// first millisecond has a `write_ts` equal to this value, and the
    /// strict `<` guard refuses that tie. A shared
    /// check refuses a resource whose `write_ts` is at or after it: a
    /// resource written after the scan was taken, as long as the clock has
    /// not stepped back since (`ScanMode::Shared`). It cannot catch a write
    /// in flight at that moment (earlier `write_ts`, index rows landing
    /// later) or an index row planted by hand after the scan; callers take
    /// the scan only after every writer has returned.
    taken_at: i64,
}

/// A column read, as the error text a scan or check reports.
fn column<T>(row: &Row, name: &str) -> Result<T, String>
where
    Row: IntoRustByName<T>,
{
    row.get_r_by_name(name)
        .map_err(|error| format!("{name}: {error:?}"))
}

/// An index row of `table`, decoded.
fn decode_scanned(table: &'static str, row: &Row) -> Result<ScannedRow, String> {
    let text = |name: &str| column::<String>(row, name);
    let timestamp = |name: &str| column::<DateTime<Utc>>(row, name);
    let (key, last_updated) = match table {
        "resources_by_type" => (IndexKey::ResourcesByType, Some(timestamp("last_updated")?)),
        "token_code_index" => (
            IndexKey::TokenCode {
                param: text("param")?,
                code: text("code")?,
                system: text("system")?,
            },
            Some(timestamp("last_updated")?),
        ),
        "token_system_code_index" => (
            IndexKey::TokenSystemCode {
                param: text("param")?,
                system: text("system")?,
                code: text("code")?,
            },
            Some(timestamp("last_updated")?),
        ),
        "token_system_index" => (
            IndexKey::TokenSystem {
                param: text("param")?,
                system: text("system")?,
                code: text("code")?,
            },
            Some(timestamp("last_updated")?),
        ),
        "date_index" => (
            IndexKey::Date {
                param: text("param")?,
                start: timestamp("start")?.timestamp_millis(),
                end: timestamp("end")?.timestamp_millis(),
            },
            None,
        ),
        "date_index_by_end" => (
            IndexKey::DateByEnd {
                param: text("param")?,
                end: timestamp("end")?.timestamp_millis(),
                start: timestamp("start")?.timestamp_millis(),
            },
            None,
        ),
        "uri_index" => (
            IndexKey::Uri {
                param: text("param")?,
                uri: text("uri")?,
            },
            Some(timestamp("last_updated")?),
        ),
        other => return Err(format!("unknown index table {other}")),
    };
    Ok(ScannedRow {
        table,
        tenant_id: text("tenant_id")?,
        resource_type: text("resource_type")?,
        id: text("id")?,
        key,
        last_updated,
        version_id: column::<i32>(row, "version_id")?,
        written: column::<i64>(row, "written")?,
    })
}

impl IndexScan {
    /// `keep` must be a pure function of the tenant id: `covers` calls it
    /// again after the scan. Recording the tenants seen while scanning would
    /// not do, because a tenant with no rows must still count as covered.
    ///
    /// Scans the tables in `SCANS` order, one at a time, through
    /// `rows_paged_keeping` at `SCAN_PAGE_SIZE`, `StatementKind::Read`.
    /// Counts every row read (a `Cell` in the keep closure) and holds only
    /// rows whose tenant starts with `test-` and passes `keep`. A failed
    /// page or an undecodable kept row ends the scan with `failed` set,
    /// `what` = `"scan <table>"`, `error` = the error's `{:?}`.
    pub(crate) async fn take(
        backend: &CassandraBackend,
        keep: impl Fn(&str) -> bool + Send + Sync + 'static,
    ) -> IndexScan {
        Self::take_only(backend, keep, None).await
    }

    /// `take`, and with `only` set, holds just the rows of those
    /// `(resource_type, id)`. The rows of every other id are dropped before
    /// they are decoded, so one the scan cannot decode is not read, and does
    /// not fail the scan.
    pub(crate) async fn take_only(
        backend: &CassandraBackend,
        keep: impl Fn(&str) -> bool + Send + Sync + 'static,
        only: Option<&[(&str, &str)]>,
    ) -> IndexScan {
        let mut scan = IndexScan {
            keep: Box::new(keep),
            rows: BTreeMap::new(),
            scanned: 0,
            failed: None,
            taken_at: Utc::now().timestamp_millis() * 1000,
        };
        for (table, cql) in SCANS {
            let seen = Cell::new(0usize);
            let accepts = &scan.keep;
            let read = backend
                .session
                .rows_paged_keeping(
                    cql,
                    Vec::new(),
                    StatementKind::Read,
                    SCAN_PAGE_SIZE,
                    |row| {
                        seen.set(seen.get() + 1);
                        // An unreadable tenant is kept, so the decode below reports it.
                        let tenant_kept = match column::<String>(row, "tenant_id") {
                            Ok(tenant) => tenant.starts_with("test-") && accepts(&tenant),
                            Err(_) => true,
                        };
                        // An unreadable id is kept too.
                        tenant_kept
                            && only.is_none_or(|only| {
                                match (
                                    column::<String>(row, "resource_type"),
                                    column::<String>(row, "id"),
                                ) {
                                    (Ok(t), Ok(i)) => {
                                        only.iter().any(|(ot, oi)| *ot == t && *oi == i)
                                    }
                                    _ => true,
                                }
                            })
                    },
                )
                .await;
            scan.scanned += seen.get();
            let rows = match read {
                Ok(rows) => rows,
                Err(error) => {
                    scan.failed = Some((format!("scan {table}"), format!("{error:?}")));
                    return scan;
                }
            };
            for row in &rows {
                match decode_scanned(table, row) {
                    Ok(scanned) => scan
                        .rows
                        .entry(scanned.tenant_id.clone())
                        .or_default()
                        .push(scanned),
                    Err(error) => {
                        scan.failed = Some((format!("scan {table}"), error));
                        return scan;
                    }
                }
            }
        }
        scan
    }

    /// Whether the scan kept `tenant_id`'s rows: a `test-` tenant `keep` accepts.
    pub(crate) fn covers(&self, tenant_id: &str) -> bool {
        tenant_id.starts_with("test-") && (self.keep)(tenant_id)
    }

    /// Rows read from all seven tables, every tenant's.
    pub(crate) fn scanned(&self) -> usize {
        self.scanned
    }
}

/// One thing the check found wrong. Implements `Display` by hand, one line
/// with every field (a field read only by a derived `Debug` still warns as
/// dead code under `-D warnings`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Defect {
    /// Rule 1: a row of a live resource's stored `K(current)`, at
    /// `lu(current)`, that the scan did not find.
    MissingRow {
        resource_type: String,
        id: String,
        key: IndexKey,
        last_updated: Option<DateTime<Utc>>,
    },
    /// Rules 1 and 2: a scanned row no live resource's `K(current)` names
    /// (a deleted, absent or unknown id, an old `last_updated`, or a key the
    /// stored set lacks).
    StrayRow { row: ScannedRow },
    /// Rule 1, RV10 clause 2 (c): a row of `K(current)` whose tag is not the current version.
    WrongTag { row: ScannedRow, current: i32 },
    /// Rule 6, RV10 clause 2 (d): `WRITETIME(version_id)` is not the `write_ts` of the version the tag names.
    WrongWriteTime { row: ScannedRow, expected: i64 },
    /// Rule 3, RV10 clause 1, `Fixed` only: stored `K(current)` differs from a fresh extraction of `resources.data`.
    StaleExtraction {
        resource_type: String,
        id: String,
        stored: Vec<IndexKey>,
        fresh: Vec<IndexKey>,
    },
    /// Rule 4, RV10 clause 2 (a), `Fixed` only: a non-current content version's stored set differs from a re-extraction of its `data`.
    VersionReextraction {
        resource_type: String,
        id: String,
        version_id: i32,
        stored: Vec<IndexKey>,
        fresh: Vec<IndexKey>,
    },
    /// Rule 4: a delete marker's stored set is not the empty set `0x01 0x00`.
    MarkerNotEmpty {
        resource_type: String,
        id: String,
        version_id: i32,
        blob: Vec<u8>,
    },
    /// Rule 5, RV10 clause 2 (b): the current version row's blob is not `resources.index_keys` byte for byte.
    BlobMismatch {
        resource_type: String,
        id: String,
        version_id: i32,
        resources: Vec<u8>,
        version_row: Vec<u8>,
    },
    /// Rule 5: the current version's row does not exist.
    MissingVersionRow {
        resource_type: String,
        id: String,
        version_id: i32,
    },
    /// Rule 7: a stored key set that is unset or not format 1; `version_id`
    /// `None` is `resources.index_keys`.
    Undecodable {
        resource_type: String,
        id: String,
        version_id: Option<i32>,
        error: String,
    },
    /// Rule 8: a read the check needed failed.
    ReadFailed { what: String, error: String },
    /// IW5, either registry: the stored key set of a live version (the
    /// `resources` row's when the current version is live, or a
    /// `resource_versions` row that is not a delete marker) lacks
    /// `IndexKey::ResourcesByType`.
    NoTypeKey {
        resource_type: String,
        id: String,
        version_id: i32,
    },
    /// Rule 6: a stored `write_ts` that is not `write_ts_of(last_updated)` of
    /// its own row, on the `resources` row (`version_id` is the current
    /// version) or on a `resource_versions` row. The writer, the sweeper and
    /// repair all derive the write time that way.
    WriteTsMismatch {
        resource_type: String,
        id: String,
        version_id: i32,
        stored: i64,
        expected: i64,
    },
}

impl Defect {
    /// The defect's name in the log line and in tests: `missing_row`,
    /// `stray_row`, `wrong_tag`, `wrong_write_time`, `stale_extraction`,
    /// `version_reextraction`, `marker_not_empty`, `blob_mismatch`,
    /// `missing_version_row`, `undecodable`, `read_failed`, `no_type_key`,
    /// `write_ts_mismatch` (this order is
    /// `DEFECT_KINDS`, the line's order).
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Defect::MissingRow { .. } => DEFECT_KINDS[0],
            Defect::StrayRow { .. } => DEFECT_KINDS[1],
            Defect::WrongTag { .. } => DEFECT_KINDS[2],
            Defect::WrongWriteTime { .. } => DEFECT_KINDS[3],
            Defect::StaleExtraction { .. } => DEFECT_KINDS[4],
            Defect::VersionReextraction { .. } => DEFECT_KINDS[5],
            Defect::MarkerNotEmpty { .. } => DEFECT_KINDS[6],
            Defect::BlobMismatch { .. } => DEFECT_KINDS[7],
            Defect::MissingVersionRow { .. } => DEFECT_KINDS[8],
            Defect::Undecodable { .. } => DEFECT_KINDS[9],
            Defect::ReadFailed { .. } => DEFECT_KINDS[10],
            Defect::NoTypeKey { .. } => DEFECT_KINDS[11],
            Defect::WriteTsMismatch { .. } => DEFECT_KINDS[12],
        }
    }
}

impl fmt::Display for Defect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: ", self.kind())?;
        match self {
            Defect::MissingRow {
                resource_type,
                id,
                key,
                last_updated,
            } => write!(
                f,
                "{resource_type}/{id} lacks {key:?} at last_updated {last_updated:?}"
            ),
            Defect::StrayRow { row } => write!(f, "{row:?}"),
            Defect::WrongTag { row, current } => {
                write!(
                    f,
                    "{row:?} is tagged {} but current is {current}",
                    row.version_id
                )
            }
            Defect::WrongWriteTime { row, expected } => write!(
                f,
                "{row:?} was written at {} but its version's write_ts is {expected}",
                row.written
            ),
            Defect::StaleExtraction {
                resource_type,
                id,
                stored,
                fresh,
            } => write!(
                f,
                "{resource_type}/{id} stored {stored:?} but extracts {fresh:?}"
            ),
            Defect::VersionReextraction {
                resource_type,
                id,
                version_id,
                stored,
                fresh,
            } => write!(
                f,
                "{resource_type}/{id} v{version_id} stored {stored:?} but extracts {fresh:?}"
            ),
            Defect::MarkerNotEmpty {
                resource_type,
                id,
                version_id,
                blob,
            } => write!(
                f,
                "{resource_type}/{id} v{version_id} is a delete marker with key set {blob:02x?}"
            ),
            Defect::BlobMismatch {
                resource_type,
                id,
                version_id,
                resources,
                version_row,
            } => write!(
                f,
                "{resource_type}/{id} v{version_id}: resources.index_keys {resources:02x?} \
                 but version row {version_row:02x?}"
            ),
            Defect::MissingVersionRow {
                resource_type,
                id,
                version_id,
            } => write!(f, "{resource_type}/{id} has no version row v{version_id}"),
            Defect::Undecodable {
                resource_type,
                id,
                version_id,
                error,
            } => write!(
                f,
                "{resource_type}/{id} version {version_id:?}: stored key set unreadable: {error}"
            ),
            Defect::ReadFailed { what, error } => write!(f, "{what}: {error}"),
            Defect::NoTypeKey {
                resource_type,
                id,
                version_id,
            } => write!(
                f,
                "{resource_type}/{id} v{version_id}: the stored key set of a live version lacks ResourcesByType"
            ),
            Defect::WriteTsMismatch {
                resource_type,
                id,
                version_id,
                stored,
                expected,
            } => write!(
                f,
                "{resource_type}/{id} v{version_id}: stored write_ts {stored} but its last_updated gives {expected}"
            ),
        }
    }
}

/// What one check found (RV13).
#[derive(Debug, Clone)]
pub(crate) struct ConsistencyReport {
    pub(crate) tenant_id: String,
    /// `"tenant"` or `"resources"`.
    pub(crate) scope: &'static str,
    /// `"full"` or `"shared"`.
    pub(crate) scan: &'static str,
    pub(crate) registry: Registry,
    /// Resources in scope; `live + deleted + absent`.
    pub(crate) resources: usize,
    pub(crate) live: usize,
    pub(crate) deleted: usize,
    /// In `Scope::Resources`, listed ids with no `resources` row.
    pub(crate) absent: usize,
    /// Version rows read.
    pub(crate) versions: usize,
    /// Rows the scan read, every tenant's (the cost of the check).
    pub(crate) rows_scanned: usize,
    /// Scanned rows of this tenant in scope.
    pub(crate) rows_checked: usize,
    pub(crate) defects: Vec<Defect>,
    /// From the call's start; a shared scan's own time is not included.
    pub(crate) took: Duration,
}

impl ConsistencyReport {
    /// Every defect's `kind()`, sorted.
    pub(crate) fn kinds(&self) -> Vec<&'static str> {
        let mut kinds: Vec<_> = self.defects.iter().map(Defect::kind).collect();
        kinds.sort_unstable();
        kinds
    }

    /// How many defects are of `kind`.
    pub(crate) fn count(&self, kind: &str) -> usize {
        self.defects.iter().filter(|d| d.kind() == kind).count()
    }
}

/// `Display` is the one `[index-consistency …]` line (RV13), parsed by
/// `ci-report.py`'s generic `tagged_lines`.
impl fmt::Display for ConsistencyReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let registry = match self.registry {
            Registry::Fixed => "fixed",
            Registry::Changed => "changed",
        };
        write!(
            f,
            "[index-consistency tenant={} scope={} scan={} registry={registry} resources={} \
             live={} deleted={} absent={} versions={} rows_scanned={} rows_checked={} defects={}",
            self.tenant_id,
            self.scope,
            self.scan,
            self.resources,
            self.live,
            self.deleted,
            self.absent,
            self.versions,
            self.rows_scanned,
            self.rows_checked,
            self.defects.len(),
        )?;
        for kind in DEFECT_KINDS {
            write!(f, " {kind}={}", self.count(kind))?;
        }
        write!(f, " took_ms={}]", self.took.as_millis())
    }
}

/// One version row of `resource_versions`, decoded.
struct VersionRow {
    version_id: i32,
    data: Option<String>,
    /// `deleted_at` set. The writer always sets it on a delete marker, so a
    /// version without it is live, whether or not its `data` is set.
    marker: bool,
    write_ts: Option<i64>,
    last_updated: Option<DateTime<Utc>>,
    blob: Option<Vec<u8>>,
}

fn decode_version_row(row: &Row) -> Result<VersionRow, String> {
    let data: Option<String> = row
        .get_by_name("data")
        .map_err(|error| format!("data: {error:?}"))?;
    let deleted = deleted_at_of(row).map_err(|error| format!("deleted_at: {error:?}"))?;
    let blob = IntoRustByName::<Blob>::get_by_name(row, "index_keys")
        .map_err(|error| format!("index_keys: {error:?}"))?
        .map(Blob::into_vec);
    Ok(VersionRow {
        version_id: column::<i32>(row, "version_id")?,
        marker: deleted.is_some(),
        data,
        write_ts: row
            .get_by_name("write_ts")
            .map_err(|error| format!("write_ts: {error:?}"))?,
        last_updated: row
            .get_by_name("last_updated")
            .map_err(|error| format!("last_updated: {error:?}"))?,
        blob,
    })
}

/// A live or deleted resource, with everything the rules compare.
struct Subject {
    resource_type: String,
    id: String,
    row: ResourceRow,
    versions: Vec<VersionRow>,
}

/// A stored key set as a sorted vector, for a defect's text.
fn sorted(keys: &BTreeSet<IndexKey>) -> Vec<IndexKey> {
    keys.iter().cloned().collect()
}

/// A fresh extraction of `data` by the writer's own helper (RV13).
fn extract(
    backend: &CassandraBackend,
    tenant: &str,
    resource_type: &str,
    data: &str,
) -> Result<BTreeSet<IndexKey>, String> {
    let body: Json = serde_json::from_str(data).map_err(|error| error.to_string())?;
    Ok(backend
        .index_keys_of(tenant, resource_type, &body)
        .into_iter()
        .collect())
}

/// Checks `tenant` (RV10, RV13) and prints the report's line once with
/// `eprintln!`. Panics unless `tenant` starts with `test-`.
pub(crate) async fn index_consistency(
    backend: &CassandraBackend,
    tenant: &str,
    scope: Scope<'_>,
    scan_mode: ScanMode<'_>,
    registry: Registry,
) -> ConsistencyReport {
    assert!(
        tenant.starts_with("test-"),
        "the consistency check runs on test- tenants only, not {tenant}"
    );
    let started = Instant::now();
    let mut report = ConsistencyReport {
        tenant_id: tenant.to_string(),
        scope: match scope {
            Scope::Tenant => "tenant",
            Scope::Resources(_) => "resources",
        },
        scan: match scan_mode {
            ScanMode::Full => "full",
            ScanMode::Shared(_) => "shared",
        },
        registry,
        resources: 0,
        live: 0,
        deleted: 0,
        absent: 0,
        versions: 0,
        rows_scanned: 0,
        rows_checked: 0,
        defects: Vec::new(),
        took: Duration::ZERO,
    };
    run(backend, tenant, scope, scan_mode, &mut report).await;
    report.took = started.elapsed();
    eprintln!("{report}");
    report
}

/// The check's body; fills `report`.
async fn run(
    backend: &CassandraBackend,
    tenant: &str,
    scope: Scope<'_>,
    scan_mode: ScanMode<'_>,
    report: &mut ConsistencyReport,
) {
    let fixed = report.registry == Registry::Fixed;

    // Rule 0: the resources in scope.
    let mut listed: Vec<(String, String)> = match scope {
        Scope::Resources(listed) => listed
            .iter()
            .map(|(t, i)| (t.to_string(), i.to_string()))
            .collect(),
        Scope::Tenant => {
            let read = backend
                .session
                .rows_paged_keeping(
                    SELECT_RESOURCE_KEYS,
                    Vec::new(),
                    StatementKind::Read,
                    SCAN_PAGE_SIZE,
                    |row| column::<String>(row, "tenant_id").is_ok_and(|t| t == tenant),
                )
                .await;
            let rows = match read {
                Ok(rows) => rows,
                Err(error) => {
                    report.defects.push(Defect::ReadFailed {
                        what: "list resources".to_string(),
                        error: format!("{error:?}"),
                    });
                    return;
                }
            };
            let mut listed = Vec::new();
            for row in &rows {
                match (
                    column::<String>(row, "resource_type"),
                    column::<String>(row, "id"),
                ) {
                    (Ok(t), Ok(i)) => listed.push((t, i)),
                    (Err(error), _) | (_, Err(error)) => {
                        report.defects.push(Defect::ReadFailed {
                            what: "list resources".to_string(),
                            error,
                        });
                        return;
                    }
                }
            }
            listed
        }
    };
    listed.sort();
    listed.dedup();

    let mut subjects: Vec<Subject> = Vec::new();
    let mut absent: BTreeSet<(String, String)> = BTreeSet::new();
    let mut skipped: BTreeSet<(String, String)> = BTreeSet::new();
    for (resource_type, id) in listed {
        let row = match backend.read_resource_row(tenant, &resource_type, &id).await {
            Ok(row) => row,
            Err(error) => {
                report.defects.push(Defect::ReadFailed {
                    what: format!("read {resource_type}/{id}"),
                    error: format!("{error:?}"),
                });
                skipped.insert((resource_type, id));
                continue;
            }
        };
        let Some(row) = row else {
            report.resources += 1;
            report.absent += 1;
            absent.insert((resource_type, id));
            continue;
        };
        let read = backend
            .session
            .rows_paged(
                SELECT_VERSION_ROWS,
                vec![
                    Value::from(tenant),
                    Value::from(resource_type.as_str()),
                    Value::from(id.as_str()),
                ],
                StatementKind::Read,
            )
            .await
            .map_err(|error| format!("{error:?}"))
            .and_then(|rows| rows.iter().map(decode_version_row).collect());
        match read {
            Ok(versions) => {
                let versions: Vec<VersionRow> = versions;
                report.versions += versions.len();
                report.resources += 1;
                if row.is_deleted() {
                    report.deleted += 1;
                } else {
                    report.live += 1;
                }
                subjects.push(Subject {
                    resource_type,
                    id,
                    row,
                    versions,
                });
            }
            Err(error) => {
                report.defects.push(Defect::ReadFailed {
                    what: format!("versions of {resource_type}/{id}"),
                    error,
                });
                skipped.insert((resource_type, id));
            }
        }
    }

    // The scan.
    let owned;
    let scan: &IndexScan = match scan_mode {
        ScanMode::Full => {
            let own = tenant.to_string();
            let only = match scope {
                Scope::Tenant => None,
                Scope::Resources(listed) => Some(listed),
            };
            owned = IndexScan::take_only(backend, move |t| t == own, only).await;
            &owned
        }
        ScanMode::Shared(shared) => {
            assert!(
                shared.covers(tenant),
                "the shared scan does not cover tenant {tenant}"
            );
            for subject in &subjects {
                // A row with no `write_ts` is dated by its `last_updated`, as
                // rule 6 does; with neither set it is not checked.
                let writes = std::iter::once(
                    subject
                        .row
                        .write_ts
                        .unwrap_or_else(|| write_ts_of(subject.row.last_updated)),
                )
                .chain(
                    subject
                        .versions
                        .iter()
                        .filter_map(|v| v.write_ts.or_else(|| v.last_updated.map(write_ts_of))),
                );
                for write_ts in writes {
                    assert!(
                        write_ts < shared.taken_at,
                        "the shared scan is stale for {}/{}: written at write_ts {write_ts}, scan taken_at {}",
                        subject.resource_type,
                        subject.id,
                        shared.taken_at
                    );
                }
            }
            shared
        }
    };
    report.rows_scanned = scan.scanned();
    let scan_ok = scan.failed.is_none();
    if let Some((what, error)) = &scan.failed {
        report.defects.push(Defect::ReadFailed {
            what: what.clone(),
            error: error.clone(),
        });
    }

    // The tenant's scanned rows by resource.
    let mut by_resource: BTreeMap<(&str, &str), Vec<&ScannedRow>> = BTreeMap::new();
    if scan_ok {
        for row in scan.rows.get(tenant).into_iter().flatten() {
            by_resource
                .entry((row.resource_type.as_str(), row.id.as_str()))
                .or_default()
                .push(row);
        }
    }
    let subject_ids: HashSet<(&str, &str)> = subjects
        .iter()
        .map(|s| (s.resource_type.as_str(), s.id.as_str()))
        .collect();
    let known = |t: &str, i: &str| {
        let pair = (t.to_string(), i.to_string());
        absent.contains(&pair) || skipped.contains(&pair) || subject_ids.contains(&(t, i))
    };
    let mut strays: Vec<&ScannedRow> = Vec::new();
    for (&(t, i), rows) in &by_resource {
        if skipped.contains(&(t.to_string(), i.to_string())) {
            continue;
        }
        if absent.contains(&(t.to_string(), i.to_string())) {
            // Rule 2: no `resources` row, so no row anywhere.
            report.rows_checked += rows.len();
            strays.extend(rows);
        } else if !known(t, i) && matches!(scope, Scope::Tenant) {
            // Rule 2: an id with no `resources` row at all.
            report.rows_checked += rows.len();
            strays.extend(rows);
        }
    }
    for row in strays {
        report.defects.push(Defect::StrayRow { row: row.clone() });
    }

    for subject in &subjects {
        let rows: &[&ScannedRow] = by_resource
            .get(&(subject.resource_type.as_str(), subject.id.as_str()))
            .map_or(&[], Vec::as_slice);
        check_subject(backend, tenant, subject, rows, fixed, scan_ok, report);
    }
}

/// Rules 1 and 3 to 7 for one resource that has a `resources` row.
fn check_subject(
    backend: &CassandraBackend,
    tenant: &str,
    subject: &Subject,
    rows: &[&ScannedRow],
    fixed: bool,
    scan_ok: bool,
    report: &mut ConsistencyReport,
) {
    let (resource_type, id) = (&subject.resource_type, &subject.id);
    let current = subject.row.version_id;
    let live = !subject.row.is_deleted();
    let current_row = subject.versions.iter().find(|v| v.version_id == current);

    // Rule 7: a live resource's stored set must decode.
    let mut stored: Option<BTreeSet<IndexKey>> = None;
    if live {
        let decoded = match subject.row.index_keys.as_deref() {
            None => Err("index_keys is unset".to_string()),
            Some(blob) => index_keys::decode(blob).map_err(|error| error.to_string()),
        };
        match decoded {
            Ok(keys) => stored = Some(keys.into_iter().collect()),
            Err(error) => report.defects.push(Defect::Undecodable {
                resource_type: resource_type.clone(),
                id: id.clone(),
                version_id: None,
                error,
            }),
        }
    }

    // Rule 6: a stored `write_ts` is the write time of its own `last_updated`.
    // This needs no scan, so it runs whatever the scan did.
    let own_write_ts = std::iter::once((
        current,
        subject.row.write_ts,
        Some(subject.row.last_updated),
    ))
    .chain(
        subject
            .versions
            .iter()
            .map(|v| (v.version_id, v.write_ts, v.last_updated)),
    );
    for (version_id, stored, last_updated) in own_write_ts {
        if let (Some(stored), Some(last_updated)) = (stored, last_updated)
            && stored != write_ts_of(last_updated)
        {
            report.defects.push(Defect::WriteTsMismatch {
                resource_type: resource_type.clone(),
                id: id.clone(),
                version_id,
                stored,
                expected: write_ts_of(last_updated),
            });
        }
    }

    // IW5: a live version's stored set holds the type key. This does not
    // depend on extraction, so it runs under either registry.
    if let Some(stored) = &stored
        && !stored.contains(&IndexKey::ResourcesByType)
    {
        report.defects.push(Defect::NoTypeKey {
            resource_type: resource_type.clone(),
            id: id.clone(),
            version_id: current,
        });
    }

    // Rows (rules 1, 2 and 6).
    if !scan_ok {
        // Rules 1, 2 and 6 need the scan; the `ReadFailed` already says so.
    } else if live && stored.is_none() {
        // Undecodable: its rows are neither stray nor missing, and not written-checked.
        report.rows_checked += rows.len();
    } else {
        report.rows_checked += rows.len();
        let expected: BTreeSet<RowId<'_>> = stored
            .iter()
            .flatten()
            .map(|key| key.row_at(subject.row.last_updated))
            .collect();
        let mut found: BTreeSet<RowId<'_>> = BTreeSet::new();
        for row in rows {
            let row_id = row.row_id();
            if expected.contains(&row_id) {
                found.insert(row_id);
                if row.version_id != current {
                    report.defects.push(Defect::WrongTag {
                        row: (*row).clone(),
                        current,
                    });
                }
            } else {
                report.defects.push(Defect::StrayRow {
                    row: (*row).clone(),
                });
            }
            // Rule 6.
            let tagged = subject
                .versions
                .iter()
                .find(|v| v.version_id == row.version_id);
            let version_ts = tagged.and_then(|v| v.write_ts);
            // When neither `write_ts` is set, the writer's own derivation from
            // `last_updated` (`write_ts_of`); with no `last_updated` either,
            // nothing is expected and the row is not checked.
            let want = version_ts
                .or_else(|| {
                    (row.version_id == current)
                        .then_some(subject.row.write_ts)
                        .flatten()
                })
                .or_else(|| {
                    tagged
                        .and_then(|v| v.last_updated)
                        .or_else(|| (row.version_id == current).then_some(subject.row.last_updated))
                        .map(write_ts_of)
                });
            if let Some(want) = want
                && want != row.written
            {
                report.defects.push(Defect::WrongWriteTime {
                    row: (*row).clone(),
                    expected: want,
                });
            }
        }
        for key in stored.iter().flatten() {
            let wanted = key.row_at(subject.row.last_updated);
            if !found.contains(&wanted) {
                report.defects.push(Defect::MissingRow {
                    resource_type: resource_type.clone(),
                    id: id.clone(),
                    key: key.clone(),
                    last_updated: wanted.last_updated(),
                });
            }
        }
    }

    // Rule 3: the current version against a fresh extraction.
    if live
        && fixed
        && let Some(stored) = &stored
    {
        match subject.row.data.as_deref() {
            None => report.defects.push(Defect::ReadFailed {
                what: format!("data of {resource_type}/{id} v{current}"),
                error: "data is unset".to_string(),
            }),
            Some(data) => match extract(backend, tenant, resource_type, data) {
                Ok(fresh) if &fresh != stored => report.defects.push(Defect::StaleExtraction {
                    resource_type: resource_type.clone(),
                    id: id.clone(),
                    stored: sorted(stored),
                    fresh: sorted(&fresh),
                }),
                Ok(_) => {}
                Err(error) => report.defects.push(Defect::ReadFailed {
                    what: format!("data of {resource_type}/{id} v{current}"),
                    error,
                }),
            },
        }
    }

    // Rule 4: every version row.
    for version in &subject.versions {
        let blob = version.blob.as_deref().unwrap_or(&[]);
        if version.marker {
            if blob != index_keys::EMPTY {
                report.defects.push(Defect::MarkerNotEmpty {
                    resource_type: resource_type.clone(),
                    id: id.clone(),
                    version_id: version.version_id,
                    blob: blob.to_vec(),
                });
            }
            continue;
        }
        if version.version_id == current {
            // Its blob is `resources.index_keys` (rule 5); only IW5 applies here.
            if let Ok(keys) = index_keys::decode(blob)
                && !keys.contains(&IndexKey::ResourcesByType)
            {
                report.defects.push(Defect::NoTypeKey {
                    resource_type: resource_type.clone(),
                    id: id.clone(),
                    version_id: version.version_id,
                });
            }
            // Its own `data` is re-extracted like an older version's. When it
            // is `resources.data` itself, rule 3 has compared it already, so
            // only a body that differs from it is compared with the stored set.
            if fixed && let Some(data) = version.data.as_deref() {
                match extract(backend, tenant, resource_type, data) {
                    Ok(fresh) => {
                        if version.data != subject.row.data
                            && let Ok(keys) = index_keys::decode(blob)
                        {
                            let keys: BTreeSet<IndexKey> = keys.into_iter().collect();
                            if fresh != keys {
                                report.defects.push(Defect::VersionReextraction {
                                    resource_type: resource_type.clone(),
                                    id: id.clone(),
                                    version_id: version.version_id,
                                    stored: sorted(&keys),
                                    fresh: sorted(&fresh),
                                });
                            }
                        }
                    }
                    Err(error) => report.defects.push(Defect::ReadFailed {
                        what: format!("data of {resource_type}/{id} v{}", version.version_id),
                        error,
                    }),
                }
            }
            continue;
        }
        let keys: BTreeSet<IndexKey> = match index_keys::decode(blob) {
            Ok(keys) => keys.into_iter().collect(),
            Err(error) => {
                report.defects.push(Defect::Undecodable {
                    resource_type: resource_type.clone(),
                    id: id.clone(),
                    version_id: Some(version.version_id),
                    error: error.to_string(),
                });
                continue;
            }
        };
        // IW5.
        if !keys.contains(&IndexKey::ResourcesByType) {
            report.defects.push(Defect::NoTypeKey {
                resource_type: resource_type.clone(),
                id: id.clone(),
                version_id: version.version_id,
            });
        }
        if !fixed {
            continue;
        }
        let Some(data) = version.data.as_deref() else {
            continue;
        };
        match extract(backend, tenant, resource_type, data) {
            Ok(fresh) if fresh != keys => report.defects.push(Defect::VersionReextraction {
                resource_type: resource_type.clone(),
                id: id.clone(),
                version_id: version.version_id,
                stored: sorted(&keys),
                fresh: sorted(&fresh),
            }),
            Ok(_) => {}
            Err(error) => report.defects.push(Defect::ReadFailed {
                what: format!("data of {resource_type}/{id} v{}", version.version_id),
                error,
            }),
        }
    }

    // Rule 5: the current version row carries `resources.index_keys`.
    match current_row {
        None => report.defects.push(Defect::MissingVersionRow {
            resource_type: resource_type.clone(),
            id: id.clone(),
            version_id: current,
        }),
        Some(version) => {
            let resources = subject.row.index_keys.clone().unwrap_or_default();
            let version_row = version.blob.clone().unwrap_or_default();
            if resources != version_row {
                report.defects.push(Defect::BlobMismatch {
                    resource_type: resource_type.clone(),
                    id: id.clone(),
                    version_id: current,
                    resources,
                    version_row,
                });
            }
        }
    }
}

/// Panics unless the report has no defect, with the line and the first 20
/// defects, one per line (`Display`).
#[track_caller]
pub(crate) fn assert_consistent(report: &ConsistencyReport) {
    if report.defects.is_empty() {
        return;
    }
    let mut message = format!("{report}");
    for defect in report.defects.iter().take(20) {
        message.push_str(&format!("\n  {defect}"));
    }
    panic!("{message}");
}

/// The version ids of the `resource_versions` rows of `(tenant,
/// resource_type, id)`, ascending (tests only; panics on a read error).
pub(crate) async fn version_ids(
    backend: &CassandraBackend,
    tenant: &str,
    resource_type: &str,
    id: &str,
) -> Vec<i32> {
    let rows = backend
        .session
        .rows_paged(
            SELECT_VERSION_ROWS,
            vec![
                Value::from(tenant),
                Value::from(resource_type),
                Value::from(id),
            ],
            StatementKind::Read,
        )
        .await
        .expect("read version rows");
    let mut ids: Vec<i32> = rows
        .iter()
        .map(|row| column::<i32>(row, "version_id").expect("version_id"))
        .collect();
    ids.sort_unstable();
    ids
}

/// The shared helpers of the node tests of this module and `checkpoint.rs`.
pub(crate) mod harness {
    use std::sync::Arc;

    use serde_json::{Value as Json, json};
    use uuid::Uuid;

    use super::*;
    use crate::backends::cassandra::CassandraConfig;
    use crate::tenant::{TenantContext, TenantId, TenantPermissions};

    /// The dev node's backend, or `None` when `HFS_TEST_CASSANDRA_URL` is unset.
    pub(crate) async fn backend() -> Option<Arc<CassandraBackend>> {
        let url = std::env::var("HFS_TEST_CASSANDRA_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())?;
        Some(Arc::new(
            CassandraBackend::connect(CassandraConfig {
                contact_points: vec![url],
                ..crate::backends::cassandra::test_support::test_config()
            })
            .await
            .expect("connect"),
        ))
    }

    /// A fresh `test-<prefix>-<uuid>` tenant.
    pub(crate) fn tenant_id(prefix: &str) -> String {
        format!("test-{prefix}-{}", Uuid::new_v4().simple())
    }

    pub(crate) fn context(tenant: &str) -> TenantContext {
        TenantContext::new(TenantId::new(tenant), TenantPermissions::full_access())
    }

    pub(crate) fn patient() -> Json {
        json!({"resourceType": "Patient", "id": "p", "active": true})
    }

    pub(crate) fn changed() -> Json {
        json!({"resourceType": "Patient", "id": "p", "active": false})
    }

    pub(crate) fn other() -> Json {
        json!({"resourceType": "Patient", "id": "q", "active": true})
    }

    /// `K(current)` of `Patient/id`, decoded from R1; none may be a date key.
    pub(crate) async fn stored_keys(
        backend: &CassandraBackend,
        tenant: &str,
        id: &str,
    ) -> Vec<IndexKey> {
        let row = backend
            .read_resource_row(tenant, "Patient", id)
            .await
            .expect("R1")
            .expect("a row");
        let keys = index_keys::decode(row.index_keys.as_deref().expect("index_keys")).expect("K");
        assert!(
            keys.iter()
                .all(|k| !matches!(k, IndexKey::Date { .. } | IndexKey::DateByEnd { .. })),
            "a date key changes this test's counts: {keys:?}"
        );
        keys
    }

    /// The default check: `Scope::Tenant`, a full scan, `Registry::Fixed`.
    pub(crate) async fn check(backend: &CassandraBackend, tenant: &str) -> ConsistencyReport {
        index_consistency(
            backend,
            tenant,
            Scope::Tenant,
            ScanMode::Full,
            Registry::Fixed,
        )
        .await
    }

    /// `kind` repeated `count` times, for comparing with `kinds()`.
    pub(crate) fn expect(parts: &[(&'static str, usize)]) -> Vec<&'static str> {
        let mut kinds: Vec<&'static str> = parts
            .iter()
            .flat_map(|&(kind, count)| std::iter::repeat_n(kind, count))
            .collect();
        kinds.sort_unstable();
        kinds
    }
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use cdrs_tokio::types::blob::Blob;
    use uuid::Uuid;

    use super::harness::*;
    use super::*;
    use crate::backends::cassandra::session::Fault;
    use crate::backends::cassandra::test_support::test_fhir_version;
    use crate::core::ResourceStorage;

    fn row(version_id: i32) -> ScannedRow {
        ScannedRow {
            table: "resources_by_type",
            tenant_id: "test-line".to_string(),
            resource_type: "Patient".to_string(),
            id: "p".to_string(),
            key: IndexKey::ResourcesByType,
            last_updated: Some(DateTime::UNIX_EPOCH),
            version_id,
            written: 1,
        }
    }

    fn one_of_each() -> Vec<Defect> {
        let (resource_type, id) = ("Patient".to_string(), "p".to_string());
        vec![
            Defect::MissingRow {
                resource_type: resource_type.clone(),
                id: id.clone(),
                key: IndexKey::ResourcesByType,
                last_updated: None,
            },
            Defect::StrayRow { row: row(1) },
            Defect::WrongTag {
                row: row(0),
                current: 1,
            },
            Defect::WrongWriteTime {
                row: row(1),
                expected: 2,
            },
            Defect::StaleExtraction {
                resource_type: resource_type.clone(),
                id: id.clone(),
                stored: vec![],
                fresh: vec![],
            },
            Defect::VersionReextraction {
                resource_type: resource_type.clone(),
                id: id.clone(),
                version_id: 1,
                stored: vec![],
                fresh: vec![],
            },
            Defect::MarkerNotEmpty {
                resource_type: resource_type.clone(),
                id: id.clone(),
                version_id: 2,
                blob: vec![1, 1],
            },
            Defect::BlobMismatch {
                resource_type: resource_type.clone(),
                id: id.clone(),
                version_id: 1,
                resources: vec![],
                version_row: vec![],
            },
            Defect::MissingVersionRow {
                resource_type: resource_type.clone(),
                id: id.clone(),
                version_id: 1,
            },
            Defect::Undecodable {
                resource_type,
                id,
                version_id: None,
                error: "x".to_string(),
            },
            Defect::ReadFailed {
                what: "w".to_string(),
                error: "e".to_string(),
            },
            Defect::NoTypeKey {
                resource_type: "Patient".to_string(),
                id: "p".to_string(),
                version_id: 1,
            },
            Defect::WriteTsMismatch {
                resource_type: "Patient".to_string(),
                id: "p".to_string(),
                version_id: 1,
                stored: 2,
                expected: 1,
            },
        ]
    }

    #[test]
    fn the_report_line_names_every_count() {
        let mut report = ConsistencyReport {
            tenant_id: "test-line".to_string(),
            scope: "tenant",
            scan: "full",
            registry: Registry::Fixed,
            resources: 3,
            live: 2,
            deleted: 1,
            absent: 0,
            versions: 5,
            rows_scanned: 40,
            rows_checked: 4,
            defects: one_of_each(),
            took: Duration::from_millis(12),
        };
        assert_eq!(
            report.to_string(),
            "[index-consistency tenant=test-line scope=tenant scan=full registry=fixed \
             resources=3 live=2 deleted=1 absent=0 versions=5 rows_scanned=40 rows_checked=4 \
             defects=13 missing_row=1 stray_row=1 wrong_tag=1 wrong_write_time=1 \
             stale_extraction=1 version_reextraction=1 marker_not_empty=1 blob_mismatch=1 \
             missing_version_row=1 undecodable=1 read_failed=1 no_type_key=1 write_ts_mismatch=1 took_ms=12]"
        );
        let mut names = DEFECT_KINDS.to_vec();
        names.sort_unstable();
        assert_eq!(report.kinds(), names);
        assert!(catch_unwind(AssertUnwindSafe(|| assert_consistent(&report))).is_err());
        report.defects.clear();
        assert_consistent(&report);
    }

    // Planting statements. Each is a `Write` unless marked.
    const PLANT_X1: &str = "INSERT INTO resources_by_type (tenant_id, resource_type, last_updated, id, version_id, fhir_version) VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
    const UNPLANT_X1: &str = "DELETE FROM resources_by_type USING TIMESTAMP ? WHERE tenant_id = ? AND resource_type = ? AND last_updated = ? AND id = ?";
    /// A type row with no `version_id`, which the scan cannot decode.
    const PLANT_X1_NO_TAG: &str = "INSERT INTO resources_by_type (tenant_id, resource_type, last_updated, id, fhir_version) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?";
    const PLANT_X2: &str = "INSERT INTO token_code_index (tenant_id, resource_type, param, code, last_updated, id, system, version_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
    const PLANT_X3: &str = "INSERT INTO token_system_code_index (tenant_id, resource_type, param, system, code, last_updated, id, version_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
    const PLANT_X4: &str = "INSERT INTO token_system_index (tenant_id, resource_type, param, system, last_updated, id, code, version_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
    const PLANT_X5: &str = "INSERT INTO date_index (tenant_id, resource_type, param, start, id, end, version_id) VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
    const PLANT_X6: &str = "INSERT INTO date_index_by_end (tenant_id, resource_type, param, end, id, start, version_id) VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
    const PLANT_X7: &str = "INSERT INTO uri_index (tenant_id, resource_type, param, uri, last_updated, id, version_id) VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
    /// A live current row with no `write_ts` column. A CAS, like every write
    /// to `resources` (the CQL audit warns on a plain one).
    const PLANT_RESOURCE_NO_TS: &str = "INSERT INTO resources (tenant_id, resource_type, id, version_id, data, last_updated, created_at, fhir_version, index_keys, deleted_at) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, 0) IF NOT EXISTS";
    /// Its version row, with no `write_ts` column.
    const PLANT_VERSION_NO_TS: &str = "INSERT INTO resource_versions (tenant_id, resource_type, id, version_id, data, last_updated, fhir_version, index_keys) VALUES (?, ?, ?, 1, ?, ?, ?, ?) USING TIMESTAMP ?";
    const PLANT_VERSION_KEYS: &str = "UPDATE resource_versions USING TIMESTAMP ? SET index_keys = ? WHERE tenant_id = ? AND resource_type = ? AND id = ? AND version_id = ?";
    /// A CAS: asserts `[applied]`.
    /// An older version row with no `data` and no `deleted_at`, written
    /// without naming either column (a null write to this table is refused).
    const PLANT_VERSION_NO_DATA: &str = "INSERT INTO resource_versions (tenant_id, resource_type, id, version_id, index_keys) VALUES (?, ?, ?, 0, ?) USING TIMESTAMP ?";
    /// A CAS: moves `resources.write_ts`.
    const PLANT_RESOURCE_WRITE_TS: &str = "UPDATE resources SET write_ts = ? WHERE tenant_id = ? AND resource_type = ? AND id = ? IF version_id = ?";
    const PLANT_VERSION_WRITE_TS: &str = "UPDATE resource_versions USING TIMESTAMP ? SET write_ts = ? WHERE tenant_id = ? AND resource_type = ? AND id = ? AND version_id = ?";
    /// Replaces a version row's `data`.
    const PLANT_VERSION_DATA: &str = "UPDATE resource_versions USING TIMESTAMP ? SET data = ? WHERE tenant_id = ? AND resource_type = ? AND id = ? AND version_id = ?";
    const PLANT_RESOURCE_KEYS: &str = "UPDATE resources SET index_keys = ? WHERE tenant_id = ? AND resource_type = ? AND id = ? IF version_id = ?";

    fn ms(millis: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(millis).expect("a timestamp")
    }

    fn planted_uri() -> IndexKey {
        IndexKey::Uri {
            param: "planted".to_string(),
            uri: "urn:planted".to_string(),
        }
    }

    async fn plant(backend: &CassandraBackend, cql: &str, values: Vec<Value>) {
        backend
            .session
            .execute(cql, values, StatementKind::Write)
            .await
            .expect("plant");
    }

    /// Sends a CAS plant and asserts it applied.
    async fn plant_cas(backend: &CassandraBackend, cql: &str, values: Vec<Value>) {
        let rows = backend
            .session
            .rows(cql, values, StatementKind::Cas)
            .await
            .expect("plant");
        let applied: bool = column(&rows[0], "[applied]").expect("[applied]");
        assert!(applied, "the CAS plant applied");
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
        let applied: bool = column(&rows[0], "[applied]").expect("[applied]");
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

    async fn plant_x1(
        backend: &CassandraBackend,
        tenant: &str,
        last_updated: DateTime<Utc>,
        id: &str,
        tag: i32,
        ts: i64,
    ) {
        plant(
            backend,
            PLANT_X1,
            vec![
                Value::from(tenant),
                Value::from("Patient"),
                Value::from(last_updated),
                Value::from(id),
                Value::from(tag),
                Value::from(test_fhir_version().as_mime_param()),
                Value::from(ts),
            ],
        )
        .await;
    }

    /// Creates `p` (v1) and returns R1's `lu1`, `ts1` and `K1`.
    async fn seeded(
        backend: &CassandraBackend,
        tenant: &str,
    ) -> (
        crate::types::StoredResource,
        DateTime<Utc>,
        i64,
        Vec<IndexKey>,
    ) {
        let stored = backend
            .create(&context(tenant), "Patient", patient(), test_fhir_version())
            .await
            .expect("create p");
        let r1 = backend
            .read_resource_row(tenant, "Patient", "p")
            .await
            .unwrap()
            .unwrap();
        let keys = stored_keys(backend, tenant, "p").await;
        (
            stored,
            r1.last_updated,
            r1.write_ts.expect("write_ts"),
            keys,
        )
    }

    #[tokio::test]
    async fn the_utility_reports_each_planted_defect() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let run = Uuid::new_v4().simple().to_string();
        let tenant_of = |case: &str| format!("test-planted-{case}-{run}");
        let all = [
            "missing-row",
            "stray-ghost",
            "stray-every-table",
            "wrong-tag",
            "wrong-write-time",
            "blob-mismatch",
            "stale-extraction",
            "version-reextraction",
            "marker-not-empty",
            "missing-version-row",
            "undecodable",
            "no-type-key",
            "no-write-ts",
            "emptied-older-version",
            "no-data-older-version",
            "write-ts-mismatch",
            "malformed-current-version-data",
        ];
        let version = test_fhir_version();

        // missing-row
        let t = tenant_of("missing-row");
        let (_, lu1, ts1, _) = seeded(&backend, &t).await;
        plant(
            &backend,
            UNPLANT_X1,
            vec![
                Value::from(ts1 + 1),
                Value::from(t.as_str()),
                Value::from("Patient"),
                Value::from(lu1),
                Value::from("p"),
            ],
        )
        .await;

        // stray-ghost
        let t = tenant_of("stray-ghost");
        let (_, lu1, ts1, _) = seeded(&backend, &t).await;
        plant_x1(&backend, &t, lu1, "ghost", 1, ts1).await;

        // stray-every-table
        let t = tenant_of("stray-every-table");
        let (_, lu1, ts1, _) = seeded(&backend, &t).await;
        let head = |t: &str| {
            [
                Value::from(t),
                Value::from("Patient"),
                Value::from("planted"),
            ]
        };
        let with = |cql: &str, mut tail: Vec<Value>| {
            let mut values = Vec::from(head(&t));
            values.append(&mut tail);
            values.push(Value::from(ts1));
            (cql.to_string(), values)
        };
        for (cql, values) in [
            with(
                PLANT_X2,
                vec![
                    Value::from("c"),
                    Value::from(lu1),
                    Value::from("p"),
                    Value::from("s"),
                    Value::from(1),
                ],
            ),
            with(
                PLANT_X3,
                vec![
                    Value::from("s"),
                    Value::from("c"),
                    Value::from(lu1),
                    Value::from("p"),
                    Value::from(1),
                ],
            ),
            with(
                PLANT_X4,
                vec![
                    Value::from("s"),
                    Value::from(lu1),
                    Value::from("p"),
                    Value::from("c"),
                    Value::from(1),
                ],
            ),
            with(
                PLANT_X5,
                vec![
                    Value::from(ms(1000)),
                    Value::from("p"),
                    Value::from(ms(2000)),
                    Value::from(1),
                ],
            ),
            with(
                PLANT_X6,
                vec![
                    Value::from(ms(2000)),
                    Value::from("p"),
                    Value::from(ms(1000)),
                    Value::from(1),
                ],
            ),
            with(
                PLANT_X7,
                vec![
                    Value::from("urn:planted"),
                    Value::from(lu1),
                    Value::from("p"),
                    Value::from(1),
                ],
            ),
        ] {
            plant(&backend, &cql, values).await;
        }

        // wrong-tag
        let t = tenant_of("wrong-tag");
        let (_, lu1, ts1, _) = seeded(&backend, &t).await;
        plant_x1(&backend, &t, lu1, "p", 0, ts1 + 1).await;

        // wrong-write-time
        let t = tenant_of("wrong-write-time");
        let (_, lu1, ts1, _) = seeded(&backend, &t).await;
        plant_x1(&backend, &t, lu1, "p", 1, ts1 + 5).await;

        // blob-mismatch
        let t = tenant_of("blob-mismatch");
        let (_, _, ts1, k1) = seeded(&backend, &t).await;
        let mut doubled = k1.clone();
        doubled.push(k1[0].clone());
        plant_version_keys(&backend, &t, 1, index_keys::encode(&doubled), ts1 + 1).await;

        // stale-extraction
        let t = tenant_of("stale-extraction");
        let (_, lu1, ts1, k1) = seeded(&backend, &t).await;
        let mut stale = k1.clone();
        stale.push(planted_uri());
        let blob = index_keys::encode(&stale);
        plant_resource_keys(&backend, &t, blob.clone(), 1).await;
        plant_version_keys(&backend, &t, 1, blob, ts1 + 1).await;
        plant(
            &backend,
            PLANT_X7,
            vec![
                Value::from(t.as_str()),
                Value::from("Patient"),
                Value::from("planted"),
                Value::from("urn:planted"),
                Value::from(lu1),
                Value::from("p"),
                Value::from(1),
                Value::from(ts1),
            ],
        )
        .await;

        // version-reextraction
        let t = tenant_of("version-reextraction");
        let (stored, _, ts1, k1) = seeded(&backend, &t).await;
        backend
            .update(&context(&t), &stored, changed())
            .await
            .expect("update p");
        let mut extra = k1.clone();
        extra.push(planted_uri());
        plant_version_keys(&backend, &t, 1, index_keys::encode(&extra), ts1 + 1).await;

        // marker-not-empty
        let t = tenant_of("marker-not-empty");
        seeded(&backend, &t).await;
        backend
            .delete(&context(&t), "Patient", "p")
            .await
            .expect("delete p");
        let ts2 = backend
            .read_resource_row(&t, "Patient", "p")
            .await
            .unwrap()
            .unwrap()
            .write_ts
            .expect("write_ts");
        let blob = index_keys::encode(&[planted_uri()]);
        plant_resource_keys(&backend, &t, blob.clone(), 2).await;
        plant_version_keys(&backend, &t, 2, blob, ts2 + 1).await;

        // missing-version-row
        let t = tenant_of("missing-version-row");
        let mut hold = backend.hold_at(
            crate::backends::cassandra::checkpoint::Checkpoint::AfterCas,
            &t,
            "p",
            1,
        );
        let task = tokio::spawn({
            let (backend, tenant) = (backend.clone(), context(&t));
            async move { backend.create(&tenant, "Patient", patient(), version).await }
        });
        hold.reached().await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        let k_missing = stored_keys(&backend, &t, "p").await;

        // undecodable
        let t = tenant_of("undecodable");
        let (_, _, ts1, _) = seeded(&backend, &t).await;
        plant_resource_keys(&backend, &t, vec![0x02, 0x00], 1).await;
        plant_version_keys(&backend, &t, 1, vec![0x02, 0x00], ts1 + 1).await;

        // no-type-key: a live resource whose stored sets are empty, and
        // whose type row is gone.
        let t = tenant_of("no-type-key");
        let (_, lu1, ts1, seeded_keys) = seeded(&backend, &t).await;
        // The type row is removed, so every other row of the seeded set is a stray.
        let strays = seeded_keys.len() - 1;
        plant_resource_keys(&backend, &t, index_keys::EMPTY.to_vec(), 1).await;
        plant_version_keys(&backend, &t, 1, index_keys::EMPTY.to_vec(), ts1 + 1).await;
        plant(
            &backend,
            UNPLANT_X1,
            vec![
                Value::from(ts1 + 1),
                Value::from(t.as_str()),
                Value::from("Patient"),
                Value::from(lu1),
                Value::from("p"),
            ],
        )
        .await;

        // no-write-ts: R1 and the version row carry no `write_ts` column.
        // `p`'s type row is one millisecond off `write_ts_of(last_updated)`;
        // `q`'s is exact.
        let t = tenant_of("no-write-ts");
        let lu = ms(1_700_000_000_000);
        let unset_ts = write_ts_of(lu);
        for (id, written) in [("p", unset_ts + 1000), ("q", unset_ts)] {
            let body = serde_json::json!({"resourceType": "Patient", "id": id, "active": true})
                .to_string();
            let blob = Value::from(Blob::from(index_keys::encode(&[IndexKey::ResourcesByType])));
            let fhir = Value::from(version.as_mime_param());
            plant_cas(
                &backend,
                PLANT_RESOURCE_NO_TS,
                vec![
                    Value::from(t.as_str()),
                    Value::from("Patient"),
                    Value::from(id),
                    Value::from(body.as_str()),
                    Value::from(lu),
                    Value::from(lu),
                    fhir.clone(),
                    blob.clone(),
                ],
            )
            .await;
            plant(
                &backend,
                PLANT_VERSION_NO_TS,
                vec![
                    Value::from(t.as_str()),
                    Value::from("Patient"),
                    Value::from(id),
                    Value::from(body.as_str()),
                    Value::from(lu),
                    fhir,
                    blob,
                    Value::from(unset_ts),
                ],
            )
            .await;
            plant_x1(&backend, &t, lu, id, 1, written).await;
        }

        // emptied-older-version: v1 is a live older version whose stored set
        // lost the type key after the update.
        let t = tenant_of("emptied-older-version");
        let (stored, _, ts1, _) = seeded(&backend, &t).await;
        backend
            .update(&context(&t), &stored, changed())
            .await
            .expect("update p");
        plant_version_keys(&backend, &t, 1, index_keys::EMPTY.to_vec(), ts1 + 1).await;

        // no-data-older-version: v0 has no `deleted_at`, so it is live although
        // its `data` is unset and its stored set is empty.
        let t = tenant_of("no-data-older-version");
        let (stored, _, ts1, _) = seeded(&backend, &t).await;
        backend
            .update(&context(&t), &stored, changed())
            .await
            .expect("update p");
        plant(
            &backend,
            PLANT_VERSION_NO_DATA,
            vec![
                Value::from(t.as_str()),
                Value::from("Patient"),
                Value::from("p"),
                Value::from(Blob::from(index_keys::EMPTY.to_vec())),
                Value::from(ts1),
            ],
        )
        .await;

        // write-ts-mismatch: R1's and v1's `write_ts` are 5 us past
        // `write_ts_of(last_updated)`, and the type row was written at that
        // time, so rule 6's index-row comparison still holds.
        let t = tenant_of("write-ts-mismatch");
        let (_, lu1, ts1, seeded_keys) = seeded(&backend, &t).await;
        plant_cas(
            &backend,
            PLANT_RESOURCE_WRITE_TS,
            vec![
                Value::from(ts1 + 5),
                Value::from(t.as_str()),
                Value::from("Patient"),
                Value::from("p"),
                Value::from(1),
            ],
        )
        .await;
        plant(
            &backend,
            PLANT_VERSION_WRITE_TS,
            vec![
                Value::from(ts1 + 1),
                Value::from(ts1 + 5),
                Value::from(t.as_str()),
                Value::from("Patient"),
                Value::from("p"),
                Value::from(1),
            ],
        )
        .await;
        // Every row of the stored set is rewritten at that time, not only the
        // type row, so no index row is off.
        let rewritten = backend
            .write_index_delta(
                &crate::backends::cassandra::write::RowKey {
                    tenant_id: &t,
                    resource_type: "Patient",
                    id: "p",
                },
                &crate::backends::cassandra::write::index_delta(
                    &[],
                    crate::backends::cassandra::write::VersionKeys {
                        version_id: 1,
                        last_updated: lu1,
                        fhir_version: version,
                        keys: &seeded_keys,
                    },
                ),
                ts1 + 5,
                3,
            )
            .await;
        assert!(rewritten.result.is_ok(), "{:?}", rewritten.result);

        // malformed-current-version-data: the current version row's own `data`
        // is not JSON, while `resources.data` is. (A valid body that differs
        // from `resources.data` shows only once extraction is real; until then
        // `index_keys_of` gives the same set for any body.)
        let t = tenant_of("malformed-current-version-data");
        let (_, _, ts1, _) = seeded(&backend, &t).await;
        plant(
            &backend,
            PLANT_VERSION_DATA,
            vec![
                Value::from(ts1 + 1),
                Value::from("{not json"),
                Value::from(t.as_str()),
                Value::from("Patient"),
                Value::from("p"),
                Value::from(1),
            ],
        )
        .await;

        // One shared scan for every case, taken after every `write_ts` above.
        // emptied-older-version's update comes last, and a scan in its
        // millisecond would be refused as stale. A second version's
        // `last_updated` can be one millisecond ahead of the clock
        // (`next_last_updated`), and no planted resource has more than two
        // versions, so every `write_ts` is at most `written_by + 1` ms.
        let written_by = Utc::now().timestamp_millis();
        while Utc::now().timestamp_millis() <= written_by + 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let scan = {
            let run = run.clone();
            IndexScan::take(&backend, move |t| t.ends_with(&run)).await
        };
        assert!(scan.failed.is_none(), "{:?}", scan.failed);
        for case in all {
            assert!(scan.covers(&tenant_of(case)), "{case}");
        }
        let checked = std::cell::RefCell::new(Vec::new());
        let check = |case: &'static str, registry: Registry| {
            checked.borrow_mut().push((case, registry));
            let backend = backend.clone();
            let t = tenant_of(case);
            let scan = &scan;
            async move {
                index_consistency(
                    &backend,
                    &t,
                    Scope::Tenant,
                    ScanMode::Shared(scan),
                    registry,
                )
                .await
            }
        };

        // Every planted tenant runs under both registries. A changed registry
        // skips only the two fresh-extraction comparisons, rules 3 and 4
        // (RV14), so it reports the same kinds as an unchanged one, less
        // `stale_extraction` and `version_reextraction`.
        for registry in [Registry::Fixed, Registry::Changed] {
            let fixed = registry == Registry::Fixed;
            let under = |kinds: &[&'static str]| -> Vec<&'static str> {
                kinds
                    .iter()
                    .copied()
                    .filter(|k| fixed || !matches!(*k, "stale_extraction" | "version_reextraction"))
                    .collect()
            };

            let report = check("missing-row", registry).await;
            assert_eq!(report.kinds(), under(&["missing_row"]), "{report}");
            let Defect::MissingRow {
                key, last_updated, ..
            } = &report.defects[0]
            else {
                unreachable!()
            };
            assert_eq!(key, &IndexKey::ResourcesByType);
            let lu1 = backend
                .read_resource_row(&tenant_of("missing-row"), "Patient", "p")
                .await
                .unwrap()
                .unwrap()
                .last_updated;
            assert_eq!(*last_updated, Some(lu1));

            let report = check("stray-ghost", registry).await;
            assert_eq!(report.kinds(), under(&["stray_row"]), "{report}");
            let Defect::StrayRow { row } = &report.defects[0] else {
                unreachable!()
            };
            assert_eq!(row.id, "ghost");

            let report = check("stray-every-table", registry).await;
            assert_eq!(report.kinds(), under(&["stray_row"; 6]), "{report}");
            let mut seen: Vec<(&str, IndexKey, Option<DateTime<Utc>>)> = report
                .defects
                .iter()
                .map(|d| match d {
                    Defect::StrayRow { row } => (row.table, row.key.clone(), row.last_updated),
                    other => panic!("{other}"),
                })
                .collect();
            seen.sort_by_key(|(table, ..)| *table);
            let tables: Vec<&str> = seen.iter().map(|(t, ..)| *t).collect();
            let mut want_tables = vec![
                "date_index",
                "date_index_by_end",
                "token_code_index",
                "token_system_code_index",
                "token_system_index",
                "uri_index",
            ];
            want_tables.sort_unstable();
            assert_eq!(tables, want_tables);
            let mut keys: Vec<IndexKey> = seen.iter().map(|(_, k, _)| k.clone()).collect();
            keys.sort();
            let (param, s, c) = ("planted".to_string(), "s".to_string(), "c".to_string());
            let mut want_keys = vec![
                IndexKey::TokenCode {
                    param: param.clone(),
                    code: c.clone(),
                    system: s.clone(),
                },
                IndexKey::TokenSystemCode {
                    param: param.clone(),
                    system: s.clone(),
                    code: c.clone(),
                },
                IndexKey::TokenSystem {
                    param: param.clone(),
                    system: s,
                    code: c,
                },
                IndexKey::Date {
                    param: param.clone(),
                    start: 1000,
                    end: 2000,
                },
                IndexKey::DateByEnd {
                    param,
                    end: 2000,
                    start: 1000,
                },
                planted_uri(),
            ];
            want_keys.sort();
            assert_eq!(keys, want_keys);
            for (table, _, last_updated) in &seen {
                assert_eq!(
                    last_updated.is_none(),
                    table.starts_with("date_index"),
                    "{table}"
                );
            }

            let report = check("wrong-tag", registry).await;
            assert_eq!(report.kinds(), under(&["wrong_tag"]), "{report}");
            let Defect::WrongTag { row, current } = &report.defects[0] else {
                unreachable!()
            };
            assert_eq!((*current, row.version_id), (1, 0));

            let report = check("wrong-write-time", registry).await;
            assert_eq!(report.kinds(), under(&["wrong_write_time"]), "{report}");
            let Defect::WrongWriteTime { row, expected } = &report.defects[0] else {
                unreachable!()
            };
            assert_eq!(row.written, *expected + 5);

            let report = check("blob-mismatch", registry).await;
            assert_eq!(report.kinds(), under(&["blob_mismatch"]), "{report}");
            let Defect::BlobMismatch { version_id, .. } = &report.defects[0] else {
                unreachable!()
            };
            assert_eq!(*version_id, 1);

            let report = check("stale-extraction", registry).await;
            assert_eq!(report.kinds(), under(&["stale_extraction"]), "{report}");

            let report = check("version-reextraction", registry).await;
            assert_eq!(report.kinds(), under(&["version_reextraction"]), "{report}");
            if fixed {
                let Defect::VersionReextraction { version_id, .. } = &report.defects[0] else {
                    unreachable!()
                };
                assert_eq!(*version_id, 1);
            }

            let report = check("marker-not-empty", registry).await;
            assert_eq!(report.kinds(), under(&["marker_not_empty"]), "{report}");
            let Defect::MarkerNotEmpty { version_id, .. } = &report.defects[0] else {
                unreachable!()
            };
            assert_eq!(*version_id, 2);

            let report = check("missing-version-row", registry).await;
            assert_eq!(
                report.kinds(),
                under(&expect(&[
                    ("missing_row", k_missing.len()),
                    ("missing_version_row", 1)
                ])),
                "{report}"
            );

            let report = check("undecodable", registry).await;
            assert_eq!(report.kinds(), under(&["undecodable"]), "{report}");
            let Defect::Undecodable { version_id, .. } = &report.defects[0] else {
                unreachable!()
            };
            assert_eq!(*version_id, None);

            // Only `p`'s type row is off; the expected time is derived from
            // `last_updated`.
            let report = check("no-write-ts", registry).await;
            // The plant stores `[ResourcesByType]` and a fresh extraction of its
            // body gives more keys, so `Fixed` also reports both rows stale.
            assert_eq!(
                report.kinds(),
                under(&expect(&[("wrong_write_time", 1), ("stale_extraction", 2)])),
                "{report}"
            );
            let Defect::WrongWriteTime { row, expected } = &report.defects[0] else {
                unreachable!()
            };
            assert_eq!(*expected, write_ts_of(ms(1_700_000_000_000)));
            assert_eq!((row.id.as_str(), row.written), ("p", *expected + 1000));

            // The resources row and the version row both lack the type key.
            let report = check("no-type-key", registry).await;
            // `seeded` created real index rows and the plant empties the stored
            // sets, so every seeded row but the type row is a stray.
            assert_eq!(
                report.kinds(),
                under(&expect(&[
                    ("no_type_key", 2),
                    ("stray_row", strays),
                    ("stale_extraction", 1)
                ])),
                "{report}"
            );
            assert!(report.defects.iter().all(|d| match d {
                Defect::NoTypeKey { version_id, .. } => *version_id == 1,
                _ => true,
            }));

            // Only the older version row lacks the type key.
            let report = check("emptied-older-version", registry).await;
            assert_eq!(
                report.kinds(),
                under(&["no_type_key", "version_reextraction"]),
                "{report}"
            );
            assert!(report.defects.iter().all(|d| match d {
                Defect::NoTypeKey { version_id, .. }
                | Defect::VersionReextraction { version_id, .. } => *version_id == 1,
                _ => true,
            }));

            // A version with no `deleted_at` is live even without `data`.
            let report = check("no-data-older-version", registry).await;
            assert_eq!(report.kinds(), ["no_type_key"], "{report}");
            assert!(report.defects.iter().all(|d| match d {
                Defect::NoTypeKey { version_id, .. } => *version_id == 0,
                _ => true,
            }));

            // R1 and v1 both carry a `write_ts` that is not their own
            // `last_updated`'s.
            let report = check("write-ts-mismatch", registry).await;
            assert_eq!(report.kinds(), ["write_ts_mismatch"; 2], "{report}");
            for defect in &report.defects {
                let Defect::WriteTsMismatch {
                    version_id,
                    stored,
                    expected,
                    ..
                } = defect
                else {
                    unreachable!()
                };
                assert_eq!(*version_id, 1);
                assert_eq!(*stored, *expected + 5);
            }

            // Only an unchanged registry re-extracts the current version row.
            let report = check("malformed-current-version-data", registry).await;
            let want: &[&str] = if fixed { &["read_failed"] } else { &[] };
            assert_eq!(report.kinds(), want, "{report}");
        }
        let checked = checked.into_inner();
        for case in all {
            for registry in [Registry::Fixed, Registry::Changed] {
                assert!(
                    checked.contains(&(case, registry)),
                    "{case} is checked under {registry:?}"
                );
            }
        }
    }

    /// A `Full` scan scoped to some ids drops the other ids' rows before
    /// decoding them, so a row of another id that cannot be decoded does not
    /// fail the check. The row is a type row of `p` with no `version_id`,
    /// planted under a `test-` tenant and deleted before the assertions.
    #[tokio::test]
    async fn a_scoped_full_check_does_not_decode_other_ids_rows() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("consistency-scope-decode");
        let version = test_fhir_version();
        for id in ["p", "q"] {
            let mut body = patient();
            body["id"] = serde_json::json!(id);
            backend
                .create(&context(&t), "Patient", body, version)
                .await
                .expect("create");
        }
        let lu = ms(1_700_000_000_000);
        let ts = write_ts_of(lu);
        plant(
            &backend,
            PLANT_X1_NO_TAG,
            vec![
                Value::from(t.as_str()),
                Value::from("Patient"),
                Value::from(lu),
                Value::from("p"),
                Value::from(version.as_mime_param()),
                Value::from(ts),
            ],
        )
        .await;
        let scoped = index_consistency(
            &backend,
            &t,
            Scope::Resources(&[("Patient", "q")]),
            ScanMode::Full,
            Registry::Fixed,
        )
        .await;
        let whole = check(&backend, &t).await;
        plant(
            &backend,
            UNPLANT_X1,
            vec![
                Value::from(ts + 1),
                Value::from(t.as_str()),
                Value::from("Patient"),
                Value::from(lu),
                Value::from("p"),
            ],
        )
        .await;
        assert_consistent(&scoped);
        assert_eq!(scoped.resources, 1, "{scoped}");
        // The row is undecodable to a scan that does read it.
        assert_eq!(whole.kinds(), ["read_failed"], "{whole}");
        assert_consistent(&check(&backend, &t).await);
    }

    /// The stale guard dates a row with no `write_ts` by its `last_updated`.
    /// `p` is planted after the scan with no `write_ts` on either row.
    #[tokio::test]
    async fn a_shared_scan_is_stale_for_a_write_with_no_write_ts() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("planted-stale-no-write-ts");
        let own = t.clone();
        let scan = IndexScan::take(&backend, move |x| x == own).await;
        assert!(scan.failed.is_none(), "{:?}", scan.failed);
        let lu = Utc::now();
        assert!(write_ts_of(lu) >= scan.taken_at);
        let body =
            serde_json::json!({"resourceType": "Patient", "id": "p", "active": true}).to_string();
        let blob = Value::from(Blob::from(index_keys::encode(&[IndexKey::ResourcesByType])));
        let fhir = Value::from(test_fhir_version().as_mime_param());
        plant_cas(
            &backend,
            PLANT_RESOURCE_NO_TS,
            vec![
                Value::from(t.as_str()),
                Value::from("Patient"),
                Value::from("p"),
                Value::from(body.as_str()),
                Value::from(lu),
                Value::from(lu),
                fhir.clone(),
                blob.clone(),
            ],
        )
        .await;
        plant(
            &backend,
            PLANT_VERSION_NO_TS,
            vec![
                Value::from(t.as_str()),
                Value::from("Patient"),
                Value::from("p"),
                Value::from(body.as_str()),
                Value::from(lu),
                fhir,
                blob,
                Value::from(write_ts_of(lu)),
            ],
        )
        .await;
        let stale = futures::FutureExt::catch_unwind(AssertUnwindSafe(index_consistency(
            &backend,
            &t,
            Scope::Tenant,
            ScanMode::Shared(&scan),
            Registry::Fixed,
        )))
        .await;
        let payload = stale.expect_err("a stale shared scan must panic");
        let message = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        assert!(
            message.contains("shared scan is stale for Patient/p"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn the_utility_passes_after_create_update_delete_revive() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("consistency-cudr");
        let (tenant, version) = (context(&t), test_fhir_version());
        let step = |report: &ConsistencyReport, shape: (usize, usize, usize, usize, usize)| {
            assert_consistent(report);
            assert_eq!(
                (
                    report.resources,
                    report.live,
                    report.deleted,
                    report.versions,
                    report.rows_checked
                ),
                shape,
                "{report}"
            );
        };

        let p = backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .expect("create p");
        backend
            .create(&tenant, "Patient", other(), version)
            .await
            .expect("create q");
        let q = stored_keys(&backend, &t, "q").await.len();
        let kp = stored_keys(&backend, &t, "p").await.len();
        step(&check(&backend, &t).await, (2, 2, 0, 2, kp + q));

        backend
            .update(&tenant, &p, changed())
            .await
            .expect("update p");
        let kp = stored_keys(&backend, &t, "p").await.len();
        step(&check(&backend, &t).await, (2, 2, 0, 3, kp + q));

        backend
            .delete(&tenant, "Patient", "p")
            .await
            .expect("delete p");
        step(&check(&backend, &t).await, (2, 1, 1, 4, q));

        let (revived, created) = backend
            .create_or_update(&tenant, "Patient", "p", patient(), version)
            .await
            .expect("revive p");
        assert!(created);
        assert_eq!(revived.version_id(), "4");
        let kp = stored_keys(&backend, &t, "p").await.len();
        step(&check(&backend, &t).await, (2, 2, 0, 5, kp + q));

        let report = index_consistency(
            &backend,
            &t,
            Scope::Resources(&[("Patient", "p")]),
            ScanMode::Full,
            Registry::Changed,
        )
        .await;
        assert_consistent(&report);
        assert_eq!((report.resources, report.versions), (1, 4));
    }

    #[tokio::test]
    async fn a_refused_scan_is_reported_never_passed() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("consistency-refused");
        backend
            .create(&context(&t), "Patient", patient(), test_fhir_version())
            .await
            .expect("create p");
        backend.session.inject_fault_on_first_page(
            StatementKind::Read,
            "FROM resources_by_type",
            Fault::FailWithTooManyTombstones,
        );
        let report = check(&backend, &t).await;
        assert_eq!(report.kinds(), ["read_failed"], "{report}");
        let Defect::ReadFailed { what, .. } = &report.defects[0] else {
            unreachable!()
        };
        assert_eq!(what, "scan resources_by_type");
        assert!(catch_unwind(AssertUnwindSafe(|| assert_consistent(&report))).is_err());
        assert_consistent(&check(&backend, &t).await);
    }

    #[tokio::test]
    async fn a_shared_scan_that_does_not_cover_the_tenant_is_refused() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("consistency-uncovered");
        let other = tenant_id("consistency-other");
        let scan = IndexScan::take(&backend, move |x| x == other).await;
        assert!(!scan.covers(&t));
        let refused = futures::FutureExt::catch_unwind(AssertUnwindSafe(index_consistency(
            &backend,
            &t,
            Scope::Tenant,
            ScanMode::Shared(&scan),
            Registry::Fixed,
        )))
        .await;
        let payload = refused.expect_err("an uncovered tenant must panic");
        let message = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        assert!(message.contains("does not cover tenant"), "{message}");
    }

    #[tokio::test]
    async fn a_shared_scan_taken_before_a_write_is_refused() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("consistency-stale-scan");
        let tenant = context(&t);
        let version = test_fhir_version();
        let p = backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .expect("create p");
        let own = t.clone();
        let scan = IndexScan::take(&backend, move |x| x == own).await;
        assert!(scan.failed.is_none(), "{:?}", scan.failed);
        assert_eq!(scan.taken_at % 1000, 0, "taken_at is a whole millisecond");
        backend
            .update(&tenant, &p, changed())
            .await
            .expect("update p");
        let stale = futures::FutureExt::catch_unwind(AssertUnwindSafe(index_consistency(
            &backend,
            &t,
            Scope::Tenant,
            ScanMode::Shared(&scan),
            Registry::Fixed,
        )))
        .await;
        let payload = stale.expect_err("a stale shared scan must panic");
        let message = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        assert!(
            message.contains("shared scan is stale for Patient/p"),
            "{message}"
        );
        assert_consistent(&check(&backend, &t).await);
    }

    /// The guard's boundary, with no race. A scan whose `taken_at` equals a
    /// resource's `write_ts` is stale for it; one whose `taken_at` is the
    /// next whole millisecond is not. `write_ts` and `taken_at` are in
    /// microseconds, so `+ 1000` is one millisecond, the smallest step
    /// `taken_at` takes. The test sets `taken_at` on a real scan, a field
    /// this module can reach, so it needs no test-only seam.
    #[tokio::test]
    async fn a_shared_scan_taken_at_a_write_ts_is_refused_and_one_ms_later_is_not() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("consistency-tie");
        backend
            .create(&context(&t), "Patient", patient(), test_fhir_version())
            .await
            .expect("create p");
        let write_ts = backend
            .read_resource_row(&t, "Patient", "p")
            .await
            .unwrap()
            .unwrap()
            .write_ts
            .expect("write_ts");
        let own = t.clone();
        let mut scan = IndexScan::take(&backend, move |x| x == own).await;
        assert!(scan.failed.is_none(), "{:?}", scan.failed);

        scan.taken_at = write_ts;
        let tie = futures::FutureExt::catch_unwind(AssertUnwindSafe(index_consistency(
            &backend,
            &t,
            Scope::Tenant,
            ScanMode::Shared(&scan),
            Registry::Fixed,
        )))
        .await;
        let payload = tie.expect_err("a write_ts equal to taken_at must panic");
        let message = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
            .unwrap_or_default();
        assert!(
            message.contains(&format!(
                "shared scan is stale for Patient/p: written at write_ts {write_ts}, scan taken_at {write_ts}"
            )),
            "{message}"
        );

        scan.taken_at = write_ts + 1000;
        let report = index_consistency(
            &backend,
            &t,
            Scope::Tenant,
            ScanMode::Shared(&scan),
            Registry::Fixed,
        )
        .await;
        assert_consistent(&report);
        assert_eq!(report.resources, 1, "{report}");
    }
}
