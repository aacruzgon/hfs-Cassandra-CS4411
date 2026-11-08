//! The write engine: every compare-and-swap on `resources` goes through it
//! (ADR-aacruzgon-0050), and `create` is its first user.
//!
//! The six steps of every write (`cassandra-discussion.md`, "Write path";
//! `docs/Cassandra/schema/walkthrough.md` 3.1). Each operation's own file
//! holds step 1 (its permission check, its pre-read, its key set) and its
//! loop; the engine, [`CassandraBackend::run_cas`], runs steps 2 to 6 for one
//! [`CasPlan`]:
//!
//! 1. The operation's own: `tenant.check_permission`; for a SearchParameter,
//!    the 128-byte bound on `code` (G38); `K(N+1)`; `K(N)` from R1's
//!    `index_keys` (G22). The engine adds the size bound on what the CAS
//!    carries ([`MAX_CAS_PAYLOAD_BYTES`], ADR-aacruzgon-0053) before anything is written.
//! 2. On update, delete and revive, V3: row `N` rewritten at `ts(N) − 1`,
//!    before the CAS (G40). Then I1, the intent row, at `LOCAL_QUORUM`,
//!    carrying this attempt's `write_id` and target version.
//! 3. The CAS, at `LOCAL_SERIAL` / `LOCAL_QUORUM`, never re-sent by the
//!    driver: C1 `INSERT … IF NOT EXISTS` (create, version 1), C2 update and
//!    C3 soft delete `IF version_id = N AND deleted_at = 0`, C4 revive
//!    `IF version_id = N`, setting `deleted_at = 0` (G39).
//! 4. V1 (content) or V2 (delete marker) for the new version,
//!    `USING TIMESTAMP ts(N+1)`.
//! 5. The index delta at `ts(N+1)`: delete the rows of `K(N)` that `K(N+1)`
//!    does not have, insert `K(N+1)`.
//! 6. I2, the intent delete.
//!
//! **What `K(V)` holds in Step 3.** Only the `resources_by_type` row (X1),
//! pulled forward from Step 5 so that `count` works and the conformance
//! seeder can run at boot. Its key includes the version's `last_updated`, so
//! an update moves it from `(lu(N), id)` to `(lu(N+1), id)` and a delete
//! removes it. The token, date and URI rows of
//! `SearchParameterExtractor::extract` join `K(V)` in Step 5; nothing here
//! changes then except the key set and the batching of step 5.
//!
//! **Failure handling** (design, "When the LWT outcome is unknown";
//! ADR-aacruzgon-0024, 0026, 0034, 0035, 0050, 0051; walkthrough 3.1):
//!
//! - V3 or I1 fails: its error, by the table for plain statements; this
//!   attempt changed nothing (V3 only rewrites row `N` at an older
//!   timestamp). Once an earlier attempt's outcome was unknown, that attempt
//!   may have applied, so the error answers as R2's does, 504 for a timeout
//!   and 503 for anything else, and the earlier attempt is left for the
//!   sweeper (ADR-aacruzgon-0067). So does a later turn's R1, the decoding
//!   of its row included ([`CasRequest::after_unknown`]).
//! - The CAS is refused before it runs (the errors [`classify_cas_failure`]
//!   takes as not applied): its error, by the table for plain statements,
//!   with no retry (ADR-aacruzgon-0050); after an earlier attempt's unknown
//!   outcome, as R2's error, 504 or 503 (ADR-aacruzgon-0067).
//! - The CAS answers not applied: a definite miss. The attempt's intent row
//!   is deleted, and the miss is mapped from the returned columns
//!   ([`CasMiss::of`]): `[applied]` alone is "no row", `deleted_at > 0` is
//!   "deleted", another `version_id` is "another version", and `IF NOT
//!   EXISTS` returning a row is "exists". The operation decides what a miss
//!   means for it (an error, a retry, or its loop's next turn).
//! - The CAS's outcome is unknown (every error except those that prove it
//!   was not executed; see [`classify_cas_failure`]): R2 at `LOCAL_SERIAL`,
//!   then [`decide`](CassandraBackend::decide) against every `write_id` the
//!   request has used. "Unchanged" re-runs the attempt, with a fresh
//!   `write_id`, `last_updated` and intent, after a 5 to 25 ms pause, at most
//!   [`UNKNOWN_OUTCOME_RERUNS`] times per request. "Another writer" is a
//!   definite miss, mapped from R2's row as the CAS's own miss would be: a
//!   deleted row is "deleted", so a delete in the gap is `NotFound` on both
//!   paths. R2's and R3's own errors are never classified as a CAS
//!   (ADR-aacruzgon-0050): a timeout is 504, and every other error is 503,
//!   since the write may have applied ([`map_lookup_error`]; D3
//!   sub-decision 4, ADR-aacruzgon-0055).
//! - **An earlier attempt may apply late** (ADR-aacruzgon-0051). Once any
//!   attempt's outcome was unknown, every later look at the row is first
//!   matched against it: a definite miss (C1's returned row, or an R2 after
//!   a C2, C3 or C4 miss), and the operation's next pre-read
//!   ([`CassandraBackend::check_earlier_attempts`]). A match is that
//!   attempt's success, and steps 4 to 6 use its values and its intent.
//! - Steps 4 and 5 fail: retried with the same timestamps; when the retries
//!   run out the write still succeeds and its intent row is left for the
//!   sweeper (G23), on every operation: a 5xx after an applied CAS would make
//!   a client's retried request write twice.
//! - Step 6 fails: ignored; the sweeper deletes the intent.

use std::sync::Arc;
use std::time::Duration;

use cdrs_tokio::error::Error as CdrsError;
use cdrs_tokio::frame::message_error::ErrorType;
use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::blob::Blob;
use cdrs_tokio::types::rows::Row;
use cdrs_tokio::types::value::Value;
use chrono::{DateTime, Timelike, Utc};
use helios_fhir::FhirVersion;
use serde_json::Value as Json;
use uuid::Uuid;

use super::backend::CassandraBackend;
use super::errors::{BACKEND_NAME, map_driver_error};
use super::index_keys::{self, IndexKey};
use super::read::{ResourceRow, SELECT_RESOURCE};
use super::session::{DriverError, NO_NODE_IN_PLAN, StatementKind};
use super::stats::{CasCounters, MissKind};
use crate::error::{
    BackendError, ConcurrencyError, ResourceError, StorageError, StorageResult, ValidationDetail,
    ValidationError, ValidationSeverity,
};
use crate::tenant::{Operation, TenantContext};
use crate::types::StoredResource;

/// I1: the intent row. TTL from the table's `default_time_to_live`.
const INSERT_INTENT: &str = "INSERT INTO pending_index_writes \
     (bucket, shard, enqueued_at, tenant_id, resource_type, id, write_id, version_id) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

/// I2: the intent delete, by I1's full key.
const DELETE_INTENT: &str = "DELETE FROM pending_index_writes \
     WHERE bucket = ? AND shard = ? AND enqueued_at = ? \
     AND tenant_id = ? AND resource_type = ? AND id = ?";

/// C1: create, version 1, `deleted_at = 0` (live; G39).
pub(super) const INSERT_RESOURCE_IF_NOT_EXISTS: &str = "INSERT INTO resources \
     (tenant_id, resource_type, id, version_id, data, last_updated, created_at, \
      fhir_version, write_ts, write_id, index_keys, deleted_at) \
     VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 0) IF NOT EXISTS";

/// C2: update of a live row, `N → N+1`; `fhir_version` is the row's, fixed
/// at create (walkthrough `:104-109`).
pub(super) const UPDATE_RESOURCE_IF_VERSION: &str = "UPDATE resources \
     SET version_id = ?, data = ?, last_updated = ?, fhir_version = ?, \
         write_ts = ?, write_id = ?, index_keys = ? \
     WHERE tenant_id = ? AND resource_type = ? AND id = ? \
     IF version_id = ? AND deleted_at = 0";

/// C3: soft delete, `N → N+1`, never a CQL `DELETE`: `deleted_at = lu(N+1)`
/// and `index_keys` the empty set, `0x0100` (walkthrough `:111-116`).
pub(super) const SOFT_DELETE_RESOURCE_IF_VERSION: &str = "UPDATE resources \
     SET version_id = ?, deleted_at = ?, last_updated = ?, write_ts = ?, write_id = ?, \
         index_keys = 0x0100 \
     WHERE tenant_id = ? AND resource_type = ? AND id = ? \
     IF version_id = ? AND deleted_at = 0";

/// C4: revive a deleted row, `N → N+1`, with `deleted_at = 0`, an ordinary
/// cell newer than the delete's, so no tombstone (G39); `fhir_version` is
/// the original, from R1 (G13; walkthrough `:118-127`).
pub(super) const REVIVE_RESOURCE_IF_VERSION: &str = "UPDATE resources \
     SET version_id = ?, data = ?, last_updated = ?, fhir_version = ?, \
         write_ts = ?, write_id = ?, index_keys = ?, deleted_at = 0 \
     WHERE tenant_id = ? AND resource_type = ? AND id = ? \
     IF version_id = ?";

/// R3, reduced to what a superseded unknown outcome needs (G36).
const SELECT_VERSION_WRITE_ID: &str = "SELECT write_id FROM resource_versions \
     WHERE tenant_id = ? AND resource_type = ? AND id = ? AND version_id = ?";

/// V1: a content version row, `USING TIMESTAMP ts(V)`. V3 of a content row
/// uses the same text at `ts(N) − 1`, leaving `deleted_at` unset.
pub(super) const INSERT_VERSION: &str = "INSERT INTO resource_versions \
     (tenant_id, resource_type, id, version_id, data, last_updated, fhir_version, \
      write_ts, write_id, index_keys) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";

/// V2: a delete marker, `data` unset and `index_keys` the empty set,
/// `USING TIMESTAMP ts(V)`. V3 of a marker uses the same text at
/// `ts(N) − 1` (walkthrough `:145-158`).
pub(super) const INSERT_DELETE_MARKER: &str = "INSERT INTO resource_versions \
     (tenant_id, resource_type, id, version_id, last_updated, deleted_at, fhir_version, \
      write_ts, write_id, index_keys) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";

/// X1: the `resources_by_type` row, `USING TIMESTAMP ts(V)` (G34: it carries
/// the resource's `fhir_version`).
const INSERT_RESOURCES_BY_TYPE: &str = "INSERT INTO resources_by_type \
     (tenant_id, resource_type, last_updated, id, version_id, fhir_version) \
     VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";

/// X1's delete, of the row at the old version's `last_updated`,
/// `USING TIMESTAMP ts(N+1)` (walkthrough `:238-239`). `resources_by_type`
/// is an index table: a CQL `DELETE` there is expected (D6).
const DELETE_RESOURCES_BY_TYPE: &str = "DELETE FROM resources_by_type USING TIMESTAMP ? \
     WHERE tenant_id = ? AND resource_type = ? AND last_updated = ? AND id = ?";

/// Re-runs per request after an unknown outcome that the serial read showed
/// unchanged (ADR-aacruzgon-0024): separate from the retries after definite
/// misses.
pub(crate) const UNKNOWN_OUTCOME_RERUNS: usize = 2;
/// Retries of `create_or_update` after a definite miss, so 4 turns in all
/// (ADR-aacruzgon-0024; walkthrough 3.5).
#[allow(dead_code)] // L3's loop.
pub(crate) const UPSERT_MISS_RETRIES: usize = 3;
/// Retries of a plain `delete` after a definite miss on a live row (G25;
/// walkthrough 3.4). `delete_versioned` never retries.
#[allow(dead_code)] // L2b's delete.
pub(crate) const PLAIN_DELETE_MISS_RETRIES: usize = 1;
/// Tries of each plain write after the CAS applied (steps 4 and 5).
const POST_CAS_WRITE_TRIES: u32 = 3;
/// The most a CAS may carry, `data` plus `index_keys`, in bytes (ADR-aacruzgon-0053): half
/// of the node's default `max_mutation_size` (16 MiB, half of the default
/// 32 MiB `commitlog_segment_size`), which leaves room for the rest of the
/// row and for Paxos's own copy of the update.
pub(crate) const MAX_CAS_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
/// Longest custom SearchParameter `code` the backend stores (G38).
const MAX_SEARCH_PARAMETER_CODE_BYTES: usize = 128;
/// Longest FHIR id (`[A-Za-z0-9\-\.]{1,64}`, R4 datatypes.html#id).
const MAX_ID_LEN: usize = 64;
/// Number of intent shards (`schema.cql`, `pending_index_writes`).
const INTENT_SHARDS: u32 = 16;

/// The full key of an intent row, kept by the request so I2 finds it even
/// after the hour rolls over.
#[derive(Debug, Clone)]
pub(super) struct IntentKey {
    bucket: String,
    shard: i16,
    enqueued_at: Uuid,
}

impl IntentKey {
    /// The key for an intent enqueued at `now` for `(tenant_id, id)`.
    pub(super) fn new(tenant_id: &str, id: &str, now: DateTime<Utc>) -> Self {
        Self {
            bucket: bucket_of(now),
            shard: shard_of(tenant_id, id),
            enqueued_at: timeuuid(now),
        }
    }
}

/// `bucket`: the UTC hour of `enqueued_at`, `'YYYY-MM-DDTHH'`.
fn bucket_of(at: DateTime<Utc>) -> String {
    at.format("%Y-%m-%dT%H").to_string()
}

/// `shard = hash(tenant_id, id) mod 16`, with 32-bit FNV-1a over the tenant
/// id, a zero byte and the id. Stable across processes and builds, so an
/// operator can find a resource's intents; the sweeper polls all 16 shards
/// and never recomputes it.
fn shard_of(tenant_id: &str, id: &str) -> i16 {
    const OFFSET: u32 = 0x811c_9dc5;
    const PRIME: u32 = 0x0100_0193;
    let mut hash = OFFSET;
    for byte in tenant_id
        .bytes()
        .chain(std::iter::once(0))
        .chain(id.bytes())
    {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    (hash % INTENT_SHARDS) as i16
}

/// A version-1 (time-based) UUID for `at`, as a `timeuuid` column needs.
/// The clock sequence and node are random, so two intents in the same 100 ns
/// tick still differ.
fn timeuuid(at: DateTime<Utc>) -> Uuid {
    // 100 ns intervals between 1582-10-15 and 1970-01-01.
    const GREGORIAN_OFFSET: u64 = 0x01b2_1dd2_1381_4000;
    let nanos = at.timestamp_nanos_opt().unwrap_or(0).max(0) as u64;
    let ticks = GREGORIAN_OFFSET + nanos / 100;
    let random = Uuid::new_v4().into_bytes();
    let counter = u16::from_be_bytes([random[0], random[1]]) & 0x3fff;
    // Multicast bit set: a random node id, never a MAC address (RFC 9562 6.10).
    let node = [
        random[2] | 0x01,
        random[3],
        random[4],
        random[5],
        random[6],
        random[7],
    ];
    uuid::Builder::from_gregorian_timestamp(ticks, counter, &node).into_uuid()
}

/// `now`, truncated to the millisecond: `last_updated` has millisecond
/// precision, and `write_ts` is derived from it (ADR-aacruzgon-0016). The
/// engine uses [`next_last_updated`], which truncates the same way.
#[cfg(test)]
fn now_millis() -> DateTime<Utc> {
    truncate_to_millis(Utc::now())
}

fn truncate_to_millis(at: DateTime<Utc>) -> DateTime<Utc> {
    at.with_nanosecond(at.nanosecond() / 1_000_000 * 1_000_000)
        .unwrap_or(at)
}

/// `lu(N+1) = max(now_ms, lu(N) + 1 ms)` (ADR-aacruzgon-0016): never behind
/// the version it replaces, even when this instance's clock is. `lu(1)` is
/// `now_ms`.
pub(crate) fn next_last_updated(
    now: DateTime<Utc>,
    previous: Option<DateTime<Utc>>,
) -> DateTime<Utc> {
    let now = truncate_to_millis(now);
    match previous {
        Some(previous) => now.max(truncate_to_millis(previous) + chrono::Duration::milliseconds(1)),
        None => now,
    }
}

/// `write_ts(V) = last_updated(V) × 1000`, in microseconds.
pub(super) fn write_ts_of(last_updated: DateTime<Utc>) -> i64 {
    last_updated.timestamp_millis() * 1000
}

/// How a compare-and-swap that did not answer `[applied]` ended.
#[derive(Debug)]
enum CasFailure {
    /// It may or may not have applied. `timed_out` decides the status of an
    /// undecidable outcome: 504 after a timeout, a dropped connection or the
    /// deadline; 503 after replicas or the node failed.
    Unknown { timed_out: bool, error: DriverError },
    /// It was never sent, or the coordinator refused it before acting.
    NotApplied(DriverError),
}

/// Classifies a CAS failure before anything maps it (`errors.rs`: a CAS must
/// not go through the generic table). Only the CAS's own error comes here:
/// an error of the serial read that follows is mapped as a plain
/// statement's (ADR-aacruzgon-0050).
///
/// Only errors that prove the CAS was not executed are `NotApplied`; every
/// other error goes to the `LOCAL_SERIAL` read, which costs one read in a
/// rare case and can never be wrong. In particular a `WriteTimeout` with
/// `write_type = SIMPLE`, which the design took as applied (the commit
/// timed out after consensus), is confirmed by that read too
/// (ADR-aacruzgon-0035): if it is ours, the read says so; if it was another
/// coordinator's proposal that our prepare completed, the read shows its
/// `write_id`, not ours. And a `Server` error or `Overloaded` can be raised
/// after the proposal was accepted, so they are unknown, not refusals.
fn classify_cas_failure(error: DriverError) -> CasFailure {
    let unknown = |timed_out, error| CasFailure::Unknown { timed_out, error };
    match &error {
        DriverError::Deadline { sent: true, .. } => unknown(true, error),
        DriverError::Deadline { sent: false, .. } => CasFailure::NotApplied(error),
        DriverError::Driver(inner) => match inner.as_ref() {
            CdrsError::Server { body, .. } => match &body.ty {
                // `CasWriteUnknown` (0x1700) is the protocol's own unknown
                // outcome; only the legacy Paxos v1 path sends it.
                ErrorType::WriteTimeout(_)
                | ErrorType::ReadTimeout(_)
                | ErrorType::CasWriteUnknown(_) => unknown(true, error),
                // Refused before execution: too few replicas alive, a
                // bootstrapping node, a statement the node rejected, or one
                // it no longer had prepared (the driver re-prepares and
                // re-sends it up to 5 times before giving up,
                // `cluster/session.rs:65-70`, `:331-377`).
                ErrorType::Unavailable(_)
                | ErrorType::IsBootstrapping
                | ErrorType::Syntax
                | ErrorType::Invalid
                | ErrorType::Unauthorized
                | ErrorType::Config
                | ErrorType::Authentication
                | ErrorType::Protocol
                | ErrorType::Unprepared(_) => CasFailure::NotApplied(error),
                // `WriteFailure`, `ReadFailure`, `CdcWriteFailure`, `Server`,
                // `Overloaded`, `Truncate`, `Unknown` (a code the driver
                // cannot name), and any variant a later driver adds.
                _ => unknown(false, error),
            },
            // Raised only while connecting (`connection_pool.rs:49-56`), so
            // before anything was sent. `InvalidProtocol` too: it comes from
            // opening the node's pool (`cluster/topology/node.rs:294-300`).
            CdrsError::Timeout(_) | CdrsError::InvalidProtocol(_) => CasFailure::NotApplied(error),
            // The plan held no node (`cluster/session.rs:892`, `:906`), or
            // the pool had no connection to the last node tried
            // (`cluster/connection_pool.rs:711-716`, returned by
            // `send_envelope.rs:66-69` before writing): the driver sent the
            // statement nowhere.
            CdrsError::General(message)
                if message == NO_NODE_IN_PLAN || message.contains("No active connections") =>
            {
                CasFailure::NotApplied(error)
            }
            // A closed connection (including one marked broken after a
            // missed heartbeat, `transport.rs:444-450`), "Write channel
            // failure!" and "Unmatched stream id" (sent to every request
            // pending on the connection, `transport.rs:531-537`, `:925-931`),
            // `Io`, and anything else after the request may have left,
            // including any variant a later driver adds: unknown.
            _ => unknown(true, error),
        },
    }
}

/// The undecidable outcome's error (design, "Which error, and why").
fn undecidable(timed_out: bool, operation: &str) -> StorageError {
    StorageError::Backend(if timed_out {
        BackendError::Timeout {
            backend_name: BACKEND_NAME.to_string(),
            message: format!("{operation}: the compare-and-swap's outcome is unknown"),
        }
    } else {
        BackendError::Unavailable {
            backend_name: BACKEND_NAME.to_string(),
            message: "Cassandra cannot serve the request right now".to_string(),
        }
    })
}

/// Reads the `[applied]` column of a CAS result; `None` when the answer has
/// none, which the caller treats as an unknown outcome.
fn applied(rows: &[Row]) -> Option<bool> {
    IntoRustByName::<bool>::get_by_name(rows.first()?, "[applied]")
        .ok()
        .flatten()
}

fn internal(message: String) -> StorageError {
    StorageError::Backend(BackendError::Internal {
        backend_name: BACKEND_NAME.to_string(),
        message,
        source: None,
    })
}

/// Validates a client-supplied id: a FHIR id is 1 to 64 of
/// `[A-Za-z0-9\-\.]`. The design bounds every key on it (64 bytes).
pub(super) fn check_id(id: &str) -> StorageResult<()> {
    let valid = !id.is_empty()
        && id.len() <= MAX_ID_LEN
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.');
    if valid {
        Ok(())
    } else {
        Err(StorageError::Validation(ValidationError::InvalidResource {
            message: "the resource id is not a valid FHIR id".to_string(),
            details: vec![ValidationDetail {
                path: "Resource.id".to_string(),
                message: format!(
                    "a FHIR id is 1 to {MAX_ID_LEN} characters of A-Z, a-z, 0-9, '-' and '.'"
                ),
                severity: ValidationSeverity::Error,
            }],
        }))
    }
}

/// The 128-byte bound on a stored SearchParameter's `code` (G38,
/// ADR-aacruzgon-0027): every index key under it carries the code as its
/// `param`, and FHIR sets no limit.
pub(super) fn check_search_parameter_code(
    resource_type: &str,
    resource: &Json,
) -> StorageResult<()> {
    if resource_type != "SearchParameter" {
        return Ok(());
    }
    let Some(code) = resource.get("code").and_then(Json::as_str) else {
        return Ok(());
    };
    if code.len() <= MAX_SEARCH_PARAMETER_CODE_BYTES {
        return Ok(());
    }
    Err(StorageError::Validation(ValidationError::InvalidResource {
        message: format!(
            "SearchParameter.code is longer than {MAX_SEARCH_PARAMETER_CODE_BYTES} bytes"
        ),
        details: vec![ValidationDetail {
            path: "SearchParameter.code".to_string(),
            message: format!(
                "the code is {} bytes in UTF-8; this server stores at most \
                 {MAX_SEARCH_PARAMETER_CODE_BYTES}",
                code.len()
            ),
            severity: ValidationSeverity::Error,
        }],
    }))
}

/// Sets `resourceType` and `id` on a resource about to be stored, and
/// serializes it: the content every version row and CAS carries.
pub(super) fn normalized_body(
    operation: &str,
    resource_type: &str,
    id: &str,
    mut resource: Json,
) -> StorageResult<(Json, String)> {
    if let Some(object) = resource.as_object_mut() {
        object.insert(
            "resourceType".to_string(),
            Json::String(resource_type.to_string()),
        );
        object.insert("id".to_string(), Json::String(id.to_string()));
    }
    let data = serde_json::to_string(&resource)
        .map_err(|error| internal(format!("{operation}: serialize resource: {error}")))?;
    Ok((resource, data))
}

/// Which compare-and-swap an attempt runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CasKind {
    /// C1: `INSERT … IF NOT EXISTS`, version 1.
    Create,
    /// C2: update of a live row.
    Update,
    /// C3: soft delete of a live row.
    Delete,
    /// C4: revive of a delete marker.
    Revive,
}

impl CasKind {
    /// The statement's CQL.
    pub(crate) fn cql(self) -> &'static str {
        match self {
            CasKind::Create => INSERT_RESOURCE_IF_NOT_EXISTS,
            CasKind::Update => UPDATE_RESOURCE_IF_VERSION,
            CasKind::Delete => SOFT_DELETE_RESOURCE_IF_VERSION,
            CasKind::Revive => REVIVE_RESOURCE_IF_VERSION,
        }
    }
}

/// Version `N`, as the operation's pre-read (R1) found it: the source of V3
/// and of `K(N)` (G22, G40).
#[derive(Debug, Clone)]
pub(crate) struct PreviousVersion {
    /// `N`, the version the CAS is conditioned on.
    pub(crate) version_id: i32,
    /// `N`'s content; `None` for a delete marker.
    pub(crate) data: Option<Arc<str>>,
    /// `lu(N)`.
    pub(crate) last_updated: DateTime<Utc>,
    /// Set (`> 0`) when `N` is a delete marker.
    pub(crate) deleted_at: Option<DateTime<Utc>>,
    /// The FHIR version the resource was created with (G13).
    pub(crate) fhir_version: FhirVersion,
    /// `w(N)`, the `write_id` of the CAS that wrote `N`; V3 carries it.
    pub(crate) write_id: Option<Uuid>,
    /// `K(N)` as stored, which V3 writes back unchanged.
    pub(crate) keys_blob: Vec<u8>,
    /// `K(N)`, decoded, for the index delta.
    pub(crate) keys: Vec<IndexKey>,
}

impl PreviousVersion {
    /// Version `N` from an R1 row. The stored `index_keys` must decode: it is
    /// never left unset, and a key set that cannot be read would leave rows
    /// nobody deletes.
    #[allow(dead_code)] // L2 and L3 build their plans from R1.
    pub(crate) fn from_row(row: &ResourceRow) -> StorageResult<Self> {
        let keys_blob = row
            .index_keys
            .clone()
            .ok_or_else(|| internal("pre-read: index_keys is unset".to_string()))?;
        let keys = index_keys::decode(&keys_blob)
            .map_err(|error| internal(format!("pre-read: {error}")))?;
        Ok(Self {
            version_id: row.version_id,
            data: row.data.as_deref().map(Arc::from),
            last_updated: row.last_updated,
            deleted_at: row.deleted_at,
            fhir_version: row.fhir_version,
            write_id: row.write_id,
            keys_blob,
            keys,
        })
    }
}

/// One operation's write, for the engine: which CAS, what the new version
/// holds, and the version it replaces. Built once per turn; every attempt of
/// the turn (re-runs included) shares it.
#[derive(Debug)]
pub(crate) struct CasPlan {
    kind: CasKind,
    /// The new version's content: the resource as returned, and serialized.
    /// `None` for a delete.
    body: Option<(Json, Arc<str>)>,
    /// The resource's FHIR version: the request's on create, the row's on
    /// every other CAS (G13).
    fhir_version: FhirVersion,
    /// `K(N+1)`, and its stored encoding.
    keys: Vec<IndexKey>,
    keys_blob: Vec<u8>,
    /// Version `N`; `None` for a create.
    previous: Option<PreviousVersion>,
}

impl CasPlan {
    /// C1: create version 1 with `body` (serialized as `data`).
    pub(crate) fn create(
        body: Json,
        data: String,
        fhir_version: FhirVersion,
        keys: Vec<IndexKey>,
    ) -> Self {
        Self::new(
            CasKind::Create,
            Some((body, data)),
            fhir_version,
            keys,
            None,
        )
    }

    /// C2: update the live version `previous` with `body`.
    #[allow(dead_code)] // L2a's update and L3's branch b.
    pub(crate) fn update(
        previous: PreviousVersion,
        body: Json,
        data: String,
        keys: Vec<IndexKey>,
    ) -> Self {
        let fhir_version = previous.fhir_version;
        Self::new(
            CasKind::Update,
            Some((body, data)),
            fhir_version,
            keys,
            Some(previous),
        )
    }

    /// C3: delete the live version `previous`; `K(N+1)` is empty.
    #[allow(dead_code)] // L2b's delete and delete_versioned.
    pub(crate) fn delete(previous: PreviousVersion) -> Self {
        let fhir_version = previous.fhir_version;
        Self::new(
            CasKind::Delete,
            None,
            fhir_version,
            Vec::new(),
            Some(previous),
        )
    }

    /// C4: revive the delete marker `previous` with `body`.
    #[allow(dead_code)] // L3's branch c.
    pub(crate) fn revive(
        previous: PreviousVersion,
        body: Json,
        data: String,
        keys: Vec<IndexKey>,
    ) -> Self {
        let fhir_version = previous.fhir_version;
        Self::new(
            CasKind::Revive,
            Some((body, data)),
            fhir_version,
            keys,
            Some(previous),
        )
    }

    fn new(
        kind: CasKind,
        body: Option<(Json, String)>,
        fhir_version: FhirVersion,
        keys: Vec<IndexKey>,
        previous: Option<PreviousVersion>,
    ) -> Self {
        let keys_blob = index_keys::encode(&keys);
        Self {
            kind,
            body: body.map(|(json, data)| (json, Arc::from(data))),
            fhir_version,
            keys,
            keys_blob,
            previous,
        }
    }

    /// Which CAS this plan runs.
    #[allow(dead_code)] // L2 and L3 read it back from `Applied::plan`.
    pub(crate) fn kind(&self) -> CasKind {
        self.kind
    }

    /// `N`, the version the CAS is conditioned on; 0 for a create.
    pub(crate) fn expected_version(&self) -> i32 {
        self.previous.as_ref().map_or(0, |p| p.version_id)
    }

    /// `V = N + 1`, the version the CAS creates.
    pub(crate) fn target_version(&self) -> i32 {
        self.expected_version() + 1
    }

    /// The new version's content as returned to the caller; `None` for a
    /// delete.
    pub(crate) fn body(&self) -> Option<&Json> {
        self.body.as_ref().map(|(json, _)| json)
    }

    /// The resource's FHIR version.
    #[allow(dead_code)] // L2 and L3 build their `StoredResource` from it.
    pub(crate) fn fhir_version(&self) -> FhirVersion {
        self.fhir_version
    }

    fn data(&self) -> Option<&str> {
        self.body.as_ref().map(|(_, data)| data.as_ref())
    }

    /// What the CAS carries that the client controls: `data` and
    /// `index_keys` (D14).
    fn payload_bytes(&self) -> usize {
        self.data().map_or(0, str::len) + self.keys_blob.len()
    }

    /// The CAS's bind values for attempt `(write_id, lu)`, in statement
    /// order. Every value is set: absent columns are not in the statement.
    fn cas_values(
        &self,
        key: &RowKey<'_>,
        write_id: Uuid,
        last_updated: DateTime<Utc>,
    ) -> Vec<Value> {
        let write_ts = write_ts_of(last_updated);
        let data = Value::from(self.data().unwrap_or_default());
        let fhir_version = Value::from(self.fhir_version.as_mime_param());
        let keys_blob = Value::from(Blob::from(self.keys_blob.clone()));
        let condition = Value::from(self.expected_version());
        let version = Value::from(self.target_version());
        match self.kind {
            CasKind::Create => vec![
                Value::from(key.tenant_id),
                Value::from(key.resource_type),
                Value::from(key.id),
                data,
                Value::from(last_updated),
                Value::from(last_updated),
                fhir_version,
                Value::from(write_ts),
                Value::from(write_id),
                keys_blob,
            ],
            CasKind::Update | CasKind::Revive => vec![
                version,
                data,
                Value::from(last_updated),
                fhir_version,
                Value::from(write_ts),
                Value::from(write_id),
                keys_blob,
                Value::from(key.tenant_id),
                Value::from(key.resource_type),
                Value::from(key.id),
                condition,
            ],
            CasKind::Delete => vec![
                version,
                Value::from(last_updated),
                Value::from(last_updated),
                Value::from(write_ts),
                Value::from(write_id),
                Value::from(key.tenant_id),
                Value::from(key.resource_type),
                Value::from(key.id),
                condition,
            ],
        }
    }

    /// Step 4's statement and binds: V2 for a delete, V1 otherwise.
    fn new_version_row(
        &self,
        key: &RowKey<'_>,
        write_id: Uuid,
        last_updated: DateTime<Utc>,
    ) -> (&'static str, Vec<Value>) {
        let write_ts = write_ts_of(last_updated);
        let head = [
            Value::from(key.tenant_id),
            Value::from(key.resource_type),
            Value::from(key.id),
            Value::from(self.target_version()),
        ];
        match self.kind {
            CasKind::Delete => (
                INSERT_DELETE_MARKER,
                head.into_iter()
                    .chain([
                        Value::from(last_updated),
                        Value::from(last_updated),
                        Value::from(self.fhir_version.as_mime_param()),
                        Value::from(write_ts),
                        Value::from(write_id),
                        Value::from(Blob::from(index_keys::EMPTY.to_vec())),
                        Value::from(write_ts),
                    ])
                    .collect(),
            ),
            _ => (
                INSERT_VERSION,
                head.into_iter()
                    .chain([
                        Value::from(self.data().unwrap_or_default()),
                        Value::from(last_updated),
                        Value::from(self.fhir_version.as_mime_param()),
                        Value::from(write_ts),
                        Value::from(write_id),
                        Value::from(Blob::from(self.keys_blob.clone())),
                        Value::from(write_ts),
                    ])
                    .collect(),
            ),
        }
    }
}

/// V3's statement and binds: row `N` as `previous` holds it, at
/// `ts(N) − 1`, so an existing row wins every cell and a missing one is
/// restored (G40). A content row leaves `deleted_at` unset, a marker leaves
/// `data` unset, and an unknown `w(N)` is left unset, never null.
fn previous_version_row(
    key: &RowKey<'_>,
    previous: &PreviousVersion,
) -> (&'static str, Vec<Value>) {
    let write_ts = write_ts_of(previous.last_updated);
    let write_id = previous.write_id.map_or(Value::NotSet, Value::from);
    let head = [
        Value::from(key.tenant_id),
        Value::from(key.resource_type),
        Value::from(key.id),
        Value::from(previous.version_id),
    ];
    let tail = [
        Value::from(previous.fhir_version.as_mime_param()),
        Value::from(write_ts),
        write_id,
        Value::from(Blob::from(previous.keys_blob.clone())),
        Value::from(write_ts - 1),
    ];
    match previous.deleted_at {
        Some(deleted_at) => (
            INSERT_DELETE_MARKER,
            head.into_iter()
                .chain([Value::from(previous.last_updated), Value::from(deleted_at)])
                .chain(tail)
                .collect(),
        ),
        None => (
            INSERT_VERSION,
            head.into_iter()
                .chain([
                    previous.data.as_deref().map_or(Value::NotSet, Value::from),
                    Value::from(previous.last_updated),
                ])
                .chain(tail)
                .collect(),
        ),
    }
}

/// One index row of a version, with the `last_updated` its key carries.
type ConcreteKey<'a> = (&'a IndexKey, DateTime<Utc>);

/// Step 5's delta: the rows of `K(N)` (at `lu(N)`) that `K(N+1)` (at
/// `lu(N+1)`) does not have, to delete, and every row of `K(N+1)`, to insert.
/// The `resources_by_type` row's key holds the version's `last_updated`, so
/// an update moves it and a delete removes it.
fn index_delta<'a>(
    previous: Option<(&'a [IndexKey], DateTime<Utc>)>,
    next: (&'a [IndexKey], DateTime<Utc>),
) -> (Vec<ConcreteKey<'a>>, Vec<ConcreteKey<'a>>) {
    let inserts: Vec<ConcreteKey<'a>> = next.0.iter().map(|key| (key, next.1)).collect();
    let deletes = previous
        .map(|(keys, at)| {
            keys.iter()
                .map(|key| (key, at))
                .filter(|old| !inserts.contains(old))
                .collect()
        })
        .unwrap_or_default();
    (deletes, inserts)
}

/// The partition key of the resource a request writes.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RowKey<'a> {
    pub(crate) tenant_id: &'a str,
    pub(crate) resource_type: &'a str,
    pub(crate) id: &'a str,
}

impl RowKey<'_> {
    fn values(&self) -> Vec<Value> {
        vec![
            Value::from(self.tenant_id),
            Value::from(self.resource_type),
            Value::from(self.id),
        ]
    }
}

/// One attempt of a request: its `write_id`, its `last_updated`, its intent
/// row, the plan it ran, and whether its outcome was unknown. Every attempt
/// is kept, because an attempt whose outcome was unknown can still apply
/// after a later one started: after a dropped connection or the client
/// deadline the coordinator may still be running it (ADR-aacruzgon-0034).
#[derive(Debug, Clone)]
struct Attempt {
    write_id: Uuid,
    last_updated: DateTime<Utc>,
    intent: IntentKey,
    plan: Arc<CasPlan>,
    /// `Some(timed_out)` once its outcome was unknown.
    unknown: Option<bool>,
    /// Its target version has been decided for good: it applied, or another
    /// writer took that version. A later look skips it. "Unchanged" never
    /// settles an attempt, which can still apply later (ADR-aacruzgon-0051).
    settled: bool,
}

/// One storage call's attempts, across every turn of its loop: the engine
/// matches each look at the row against all of them (ADR-aacruzgon-0034,
/// 0051). Built by [`CassandraBackend::cas_request`].
pub(crate) struct CasRequest<'a> {
    operation: &'static str,
    key: RowKey<'a>,
    attempts: Vec<Attempt>,
    reruns: usize,
    stats: Arc<CasCounters>,
}

impl CasRequest<'_> {
    /// The partition this request writes.
    #[allow(dead_code)] // L2's and L3's pre-reads.
    pub(crate) fn key(&self) -> RowKey<'_> {
        self.key
    }

    /// How many attempts this request has begun: each sent its CAS, unless
    /// its V3 or I1 failed first and ended the request.
    #[allow(dead_code)] // L3's loop.
    pub(crate) fn attempts(&self) -> usize {
        self.attempts.len()
    }

    /// Whether any attempt's outcome was unknown, so a later look at the row
    /// must be checked against it.
    pub(crate) fn had_unknown_outcome(&self) -> bool {
        self.attempts.iter().any(|a| a.unknown.is_some())
    }

    /// A statement's error, by the table for plain statements, until an
    /// attempt's outcome was unknown; from then on the write may have
    /// applied, so it answers as R2's error does: 504 for a timeout, 503 for
    /// anything else ([`may_have_applied`], ADR-aacruzgon-0067).
    fn statement_error(&self, error: &DriverError, operation: &str) -> StorageError {
        let mapped = map_driver_error(error, operation);
        StorageError::Backend(if self.had_unknown_outcome() {
            may_have_applied(mapped, operation)
        } else {
            mapped
        })
    }

    /// The result of a statement the operation sends between its turns, its
    /// next pre-read (R1): unchanged until an attempt's outcome was unknown;
    /// from then on a backend error answers as R2's does
    /// (ADR-aacruzgon-0067). Its other errors (a permission, a validation)
    /// are the request's own, so they are kept.
    pub(crate) fn after_unknown<T>(
        &self,
        result: StorageResult<T>,
        operation: &str,
    ) -> StorageResult<T> {
        match result {
            Err(StorageError::Backend(mapped)) if self.had_unknown_outcome() => {
                Err(StorageError::Backend(may_have_applied(mapped, operation)))
            }
            other => other,
        }
    }

    /// Counts a retry the operation takes after a definite miss.
    #[allow(dead_code)] // L2b's delete and L3's loop.
    pub(crate) fn note_miss_retry(&self) {
        self.stats.miss_retry();
    }

    /// Counts a request whose definite-miss budget ran out.
    #[allow(dead_code)] // L2b's delete and L3's loop.
    pub(crate) fn note_miss_budget_exhausted(&self) {
        self.stats.miss_budget_exhausted();
    }

    /// Marks every attempt at `target` as decided for good.
    fn settle(&mut self, target: i32) {
        for attempt in &mut self.attempts {
            if attempt.plan.target_version() == target {
                attempt.settled = true;
            }
        }
    }

    fn timed_out(&self, attempt: usize) -> bool {
        self.attempts[attempt].unknown.unwrap_or(true)
    }
}

/// A write that applied.
#[derive(Debug)]
#[allow(dead_code)] // `create` reads `last_updated` and `plan`; L2 and L3 the rest.
pub(crate) struct Applied {
    /// The version it created.
    pub(crate) version_id: i32,
    /// Its `last_updated`, `lu(V)`.
    pub(crate) last_updated: DateTime<Utc>,
    /// The `write_id` of the attempt that applied.
    pub(crate) write_id: Uuid,
    /// A later version already superseded it, so steps 4 and 5 were skipped
    /// and the intent row is left for the sweeper.
    pub(crate) superseded: bool,
    /// Steps 4 and 5 all succeeded and the intent row was deleted. `false`
    /// leaves the intent row for the sweeper (G23); the write still
    /// succeeded.
    pub(crate) post_cas_complete: bool,
    /// The plan of the attempt that applied, whose values the operation
    /// returns.
    pub(crate) plan: Arc<CasPlan>,
}

/// A definite miss, mapped from the columns the CAS (or the serial read)
/// returned (walkthrough section 2, "How it maps"; D4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CasMiss {
    /// `[applied]` alone: no row (C2 to C4). `NotFound`.
    NoRow,
    /// The row is deleted (`deleted_at > 0`; C2, C3). `NotFound`.
    Deleted,
    /// Another version holds the row (C2 to C4, or the serial read showed
    /// another writer's `write_id` at the target). `VersionConflict`.
    VersionMoved {
        /// The row's version, when it was readable.
        current: Option<i32>,
    },
    /// C1 found a row. `AlreadyExists`.
    Exists,
}

impl CasMiss {
    /// Maps a not-applied CAS result. `row` is its first row, holding
    /// `[applied]` and, unless the row is absent, the `IF` columns (spike
    /// A1). The node returns `[applied]` alone for a missing row
    /// (`ModificationStatement.java:624-627`), so "no row" is the condition
    /// columns being absent from the result, not a null `version_id`.
    pub(crate) fn of(kind: CasKind, row: Option<&Row>) -> Self {
        if kind == CasKind::Create {
            return CasMiss::Exists;
        }
        let Some(row) = row.filter(|row| row.contains_column("version_id")) else {
            return CasMiss::NoRow;
        };
        if row_is_deleted(row) {
            return CasMiss::Deleted;
        }
        CasMiss::VersionMoved {
            current: IntoRustByName::<i32>::get_by_name(row, "version_id")
                .ok()
                .flatten(),
        }
    }

    /// The storage error a miss is, for an operation that does not retry.
    /// `expected` is `N`, the version the CAS was conditioned on.
    pub(crate) fn into_error(self, resource_type: &str, id: &str, expected: i32) -> StorageError {
        match self {
            CasMiss::NoRow | CasMiss::Deleted => StorageError::Resource(ResourceError::NotFound {
                resource_type: resource_type.to_string(),
                id: id.to_string(),
            }),
            CasMiss::VersionMoved { current } => {
                StorageError::Concurrency(ConcurrencyError::VersionConflict {
                    resource_type: resource_type.to_string(),
                    id: id.to_string(),
                    expected_version: expected.to_string(),
                    actual_version: current
                        .map_or_else(|| "unknown".to_string(), |v| v.to_string()),
                })
            }
            CasMiss::Exists => already_exists(resource_type, id),
        }
    }

    fn counter(self) -> MissKind {
        match self {
            CasMiss::NoRow => MissKind::NoRow,
            CasMiss::Deleted => MissKind::Deleted,
            CasMiss::VersionMoved { .. } => MissKind::VersionConflict,
            CasMiss::Exists => MissKind::AlreadyExists,
        }
    }
}

/// `deleted_at > 0` in a row that returned the column: every reader that
/// decides "deleted" tests that (G39).
fn row_is_deleted(row: &Row) -> bool {
    row.contains_column("deleted_at")
        && IntoRustByName::<DateTime<Utc>>::get_by_name(row, "deleted_at")
            .ok()
            .flatten()
            .is_some_and(|at| at.timestamp_millis() > 0)
}

/// What one plan's attempts came to.
#[derive(Debug)]
pub(crate) enum CasOutcome {
    /// One of the request's attempts applied; steps 4 to 6 ran (or were
    /// skipped, when superseded).
    Applied(Applied),
    /// A definite miss: no attempt of the request applied.
    Missed(CasMiss),
}

/// What the row (and, when needed, row `V`) says about a request's attempts
/// at target version `V` (walkthrough 3.1's table).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Decision {
    /// Attempt `n` of the request is version `V`.
    Applied(usize),
    /// Attempt `n` was version `V`, and a later version superseded it.
    AppliedSuperseded(usize),
    /// The row is still at `N` (no row, for `V = 1`): not applied when the
    /// read ran.
    Unchanged,
    /// Another request wrote `V`.
    AnotherWriter,
    /// Nothing tells the cases apart.
    Undecidable,
}

/// The two cells a decision reads, `None` when missing or undecodable
/// (which then decides nothing), and whether the row is deleted, which only
/// maps a miss that another writer's version caused: deleted in the gap is
/// `NotFound`, as a definite miss of C2 or C3 is ([`CasMiss::of`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CurrentRow {
    pub(crate) version_id: Option<i32>,
    pub(crate) write_id: Option<Uuid>,
    pub(crate) deleted: bool,
}

impl CurrentRow {
    fn of(row: &Row) -> Self {
        Self {
            version_id: IntoRustByName::<i32>::get_by_name(row, "version_id")
                .ok()
                .flatten(),
            write_id: IntoRustByName::<Uuid>::get_by_name(row, "write_id")
                .ok()
                .flatten(),
            deleted: row_is_deleted(row),
        }
    }
}

impl From<&ResourceRow> for CurrentRow {
    fn from(row: &ResourceRow) -> Self {
        Self {
            version_id: Some(row.version_id),
            write_id: row.write_id,
            deleted: row.is_deleted(),
        }
    }
}

/// The first half of a decision, from the row alone: `None` means row `V`
/// must be read (the row is past `V`). Pure, so the whole table is unit
/// tested.
fn decide_from_row(
    target: i32,
    expected: i32,
    write_ids: &[Uuid],
    row: Option<CurrentRow>,
) -> Option<Decision> {
    let Some(row) = row else {
        // A create's "no row" is its `N = 0`; any other CAS's row exists
        // once it has been read at `N`, and nothing deletes it.
        return Some(if expected == 0 {
            Decision::Unchanged
        } else {
            Decision::Undecidable
        });
    };
    let Some(version) = row.version_id else {
        return Some(Decision::Undecidable);
    };
    Some(if version == target {
        by_write_id(write_ids, row.write_id, Decision::Applied)
    } else if version > target {
        return None;
    } else if version == expected {
        Decision::Unchanged
    } else {
        // Below `N`: versions only grow, so the cell is not what it seems.
        Decision::Undecidable
    })
}

/// The second half: row `V`'s `write_id` decides a superseded target (G36).
fn decide_from_version_row(write_ids: &[Uuid], row_v_write_id: Option<Uuid>) -> Decision {
    by_write_id(write_ids, row_v_write_id, Decision::AppliedSuperseded)
}

/// Ours, another writer's, or (missing or undecodable) undecidable: never
/// "another writer" for a cell that cannot be read, which would be a false
/// 409.
fn by_write_id(
    write_ids: &[Uuid],
    write_id: Option<Uuid>,
    ours: fn(usize) -> Decision,
) -> Decision {
    match write_id {
        Some(write_id) => match write_ids.iter().position(|w| *w == write_id) {
            Some(n) => ours(n),
            None => Decision::AnotherWriter,
        },
        None => Decision::Undecidable,
    }
}

/// The miss an unknown outcome is once R2 shows another writer's version:
/// mapped from the row R2 read, as a definite miss of the CAS would be from
/// its condition columns (step 3). So another request's delete in the gap
/// is `NotFound` (404) whether our CAS missed outright or its outcome was
/// unknown (walkthrough 3.3 and 3.4, G9), and only a live version is
/// `VersionConflict` (409).
fn another_writers_miss(kind: CasKind, row: Option<CurrentRow>, target: i32) -> CasMiss {
    match row {
        _ if kind == CasKind::Create => CasMiss::Exists,
        Some(row) if row.deleted => CasMiss::Deleted,
        row => CasMiss::VersionMoved {
            current: row.and_then(|r| r.version_id).or(Some(target)),
        },
    }
}

/// A random pause of 5 to 25 ms before a re-run (ADR-aacruzgon-0024).
fn rerun_pause() -> Duration {
    let random = Uuid::new_v4().as_u128() as u64;
    Duration::from_millis(5 + random % 21)
}

fn already_exists(resource_type: &str, id: &str) -> StorageError {
    StorageError::Resource(ResourceError::AlreadyExists {
        resource_type: resource_type.to_string(),
        id: id.to_string(),
    })
}

/// The error of R2 or R3, which run only after a CAS whose outcome is
/// unknown, so the write may have applied (D3 sub-decision 4,
/// ADR-aacruzgon-0055). A timeout keeps the plain table's 504, and an
/// unavailable node, a replica failure or a lost connection its 503.
/// Whatever the plain table would make 500 (`Internal` or `QueryError`: a
/// `Server` error, an unmatched stream id, a row that cannot be decoded) is
/// 503 too, never reported as a server bug for a write that may be stored.
/// The client-facing message names no host; [`map_driver_error`] logs the
/// driver's detail.
fn map_lookup_error(error: &DriverError, operation: &str) -> StorageError {
    StorageError::Backend(unavailable_unless_timeout(
        map_driver_error(error, operation),
        || {
            format!(
                "the write's outcome could not be read back ({operation} failed); \
                 it may have been stored"
            )
        },
    ))
}

/// The rule of [`map_lookup_error`] for any statement a request sends once
/// an earlier attempt's outcome was unknown (ADR-aacruzgon-0067): that
/// attempt may have applied, so a 500 of the plain table is 503.
fn may_have_applied(mapped: BackendError, operation: &str) -> BackendError {
    unavailable_unless_timeout(mapped, || {
        format!(
            "{operation} failed after an earlier attempt's outcome was unknown; \
             the write may have been stored"
        )
    })
}

/// An error already mapped by the table for plain statements: a timeout
/// (504), an unavailable node or a lost connection (503) is kept, and
/// anything else is `Unavailable` (503) with `message`, which names no host
/// (REST sends it to the client verbatim).
fn unavailable_unless_timeout(
    mapped: BackendError,
    message: impl FnOnce() -> String,
) -> BackendError {
    match mapped {
        BackendError::Timeout { .. }
        | BackendError::Unavailable { .. }
        | BackendError::ConnectionFailed { .. } => mapped,
        _ => BackendError::Unavailable {
            backend_name: BACKEND_NAME.to_string(),
            message: message(),
        },
    }
}

/// ADR-aacruzgon-0053: refuses, before anything is written, a write whose CAS would carry
/// more than [`MAX_CAS_PAYLOAD_BYTES`]. A larger mutation cannot succeed,
/// and under Paxos v2 the node may refuse it only at the commit, after
/// consensus, which would read as an unknown outcome (503) for a request
/// that can never apply.
fn check_cas_size(plan: &CasPlan) -> StorageResult<()> {
    let bytes = plan.payload_bytes();
    if bytes <= MAX_CAS_PAYLOAD_BYTES {
        return Ok(());
    }
    Err(StorageError::Validation(ValidationError::InvalidResource {
        message: format!(
            "the resource is too large to store: {bytes} bytes, over this server's \
             {MAX_CAS_PAYLOAD_BYTES}"
        ),
        details: vec![ValidationDetail {
            path: "Resource".to_string(),
            message: format!(
                "the serialized resource and its index keys are {bytes} bytes; this server \
                 stores at most {MAX_CAS_PAYLOAD_BYTES} bytes per version"
            ),
            severity: ValidationSeverity::Error,
        }],
    }))
}

impl CassandraBackend {
    /// A new request on the partition `key`; `operation` names it in errors
    /// and logs.
    pub(crate) fn cas_request<'a>(
        &self,
        operation: &'static str,
        key: RowKey<'a>,
    ) -> CasRequest<'a> {
        CasRequest {
            operation,
            key,
            attempts: Vec::new(),
            reruns: 0,
            stats: self.cas_counters.clone(),
        }
    }

    /// Steps 2 to 6 of one plan (see the module docs): V3, I1, the CAS, and
    /// on success V1/V2, the index delta and I2. An unknown outcome is
    /// settled here, re-runs included, and a definite miss is first checked
    /// against the request's earlier unknown attempts (ADR-aacruzgon-0051).
    pub(crate) async fn run_cas(
        &self,
        request: &mut CasRequest<'_>,
        plan: CasPlan,
    ) -> StorageResult<CasOutcome> {
        if let Err(error) = check_cas_size(&plan) {
            request.stats.too_large();
            return Err(error);
        }
        let plan = Arc::new(plan);
        let key = request.key;
        let operation = request.operation;
        loop {
            let lu_n = plan.previous.as_ref().map(|p| p.last_updated);
            let attempt = Attempt {
                write_id: Uuid::new_v4(),
                last_updated: next_last_updated(Utc::now(), lu_n),
                intent: IntentKey::new(key.tenant_id, key.id, Utc::now()),
                plan: plan.clone(),
                unknown: None,
                settled: false,
            };
            request.attempts.push(attempt.clone());
            let index = request.attempts.len() - 1;

            // Step 2.
            if let Some(previous) = &plan.previous {
                let (cql, values) = previous_version_row(&key, previous);
                self.session
                    .execute(cql, values, StatementKind::Write)
                    .await
                    .map_err(|error| request.statement_error(&error, "previous version row"))?;
            }
            self.write_intent(
                &attempt.intent,
                &key,
                attempt.write_id,
                plan.target_version(),
            )
            .await
            .map_err(|error| request.statement_error(&error, "intent row"))?;

            // Step 3.
            request.stats.attempt();
            let failure = match self
                .session
                .rows(
                    plan.kind.cql(),
                    plan.cas_values(&key, attempt.write_id, attempt.last_updated),
                    StatementKind::Cas,
                )
                .await
            {
                Ok(rows) => match applied(&rows) {
                    Some(true) => {
                        return Ok(CasOutcome::Applied(
                            self.finish(request, index, false).await,
                        ));
                    }
                    Some(false) => {
                        // This attempt missed, and its intent can go.
                        self.delete_intent(&attempt.intent, &key).await;
                        return self.definite_miss(request, rows.first()).await;
                    }
                    // An answer without a readable `[applied]`: as unknown.
                    None => {
                        request.stats.unknown_no_applied_column();
                        CasFailure::Unknown {
                            timed_out: true,
                            error: DriverError::from(CdrsError::General(
                                "compare-and-swap answer without [applied]".to_string(),
                            )),
                        }
                    }
                },
                Err(error) => {
                    let failure = classify_cas_failure(error);
                    if let CasFailure::Unknown { error, .. } = &failure {
                        request.stats.unknown(error);
                    }
                    failure
                }
            };

            let (timed_out, error) = match failure {
                CasFailure::NotApplied(error) => {
                    // Nothing was decided; the intent is harmless and the
                    // sweeper deletes it. No retry, `Unavailable` included
                    // (ADR-aacruzgon-0050). After an earlier attempt's
                    // unknown outcome, that attempt may have applied, so the
                    // error answers as R2's does (ADR-aacruzgon-0067).
                    request.stats.not_applied();
                    return Err(request.statement_error(&error, operation));
                }
                CasFailure::Unknown { timed_out, error } => (timed_out, error),
            };
            request.attempts[index].unknown = Some(timed_out);
            tracing::warn!(
                operation,
                resource_type = key.resource_type,
                id = %key.id,
                error = ?error,
                "compare-and-swap outcome unknown; reading it back at LOCAL_SERIAL"
            );
            let row = self.serial_read(request).await?;
            let target = plan.target_version();
            match self
                .decide(request, target, plan.expected_version(), row)
                .await?
            {
                Decision::Applied(n) => {
                    return Ok(CasOutcome::Applied(self.finish(request, n, false).await));
                }
                Decision::AppliedSuperseded(n) => {
                    return Ok(CasOutcome::Applied(self.finish(request, n, true).await));
                }
                Decision::Unchanged if request.reruns < UNKNOWN_OUTCOME_RERUNS => {
                    request.reruns += 1;
                    request.stats.rerun();
                    tokio::time::sleep(rerun_pause()).await;
                }
                Decision::Unchanged | Decision::Undecidable => {
                    request.stats.undecidable();
                    return Err(undecidable(timed_out, operation));
                }
                Decision::AnotherWriter => {
                    request.settle(target);
                    // An attempt of an earlier turn, at another target, may
                    // still be ours (ADR-aacruzgon-0051).
                    if let Some(applied) = self.late_attempt(request, row, Some(target)).await? {
                        return Ok(CasOutcome::Applied(applied));
                    }
                    let miss = another_writers_miss(plan.kind, row, target);
                    request.stats.miss(miss.counter());
                    return Ok(CasOutcome::Missed(miss));
                }
            }
        }
    }

    /// The operation's pre-read check (ADR-aacruzgon-0051): before acting on
    /// its R1 row (`None` for no row), an operation whose request already had
    /// an unknown outcome asks whether that row is in fact one of its own
    /// earlier attempts, applied late. `Some` is that attempt's success, with
    /// steps 4 to 6 already run with its values and intent; `None` means act
    /// on the row as usual. Costs nothing unless an outcome was unknown.
    #[allow(dead_code)] // L2 and L3's loops call it after each R1.
    pub(crate) async fn check_earlier_attempts(
        &self,
        request: &mut CasRequest<'_>,
        row: Option<&ResourceRow>,
    ) -> StorageResult<Option<Applied>> {
        if !request.had_unknown_outcome() {
            return Ok(None);
        }
        self.late_attempt(request, row.map(CurrentRow::from), None)
            .await
    }

    /// A definite miss of the request's last attempt. Before it is reported,
    /// it is checked against the request's earlier unknown attempts: C1's
    /// miss returns the whole row; a C2, C3 or C4 miss returns only the `IF`
    /// columns, so R2 reads the row's `write_id` (ADR-aacruzgon-0051).
    async fn definite_miss(
        &self,
        request: &mut CasRequest<'_>,
        row: Option<&Row>,
    ) -> StorageResult<CasOutcome> {
        let kind = request
            .attempts
            .last()
            .map_or(CasKind::Create, |a| a.plan.kind);
        let miss = CasMiss::of(kind, row);
        if request.had_unknown_outcome() {
            let look = if kind == CasKind::Create {
                row.map(CurrentRow::of)
            } else {
                self.serial_read(request).await?
            };
            if let Some(applied) = self.late_attempt(request, look, None).await? {
                return Ok(CasOutcome::Applied(applied));
            }
        }
        request.stats.miss(miss.counter());
        Ok(CasOutcome::Missed(miss))
    }

    /// Matches `row` against every earlier attempt of the request whose
    /// outcome was unknown and is not settled, one target version at a time
    /// (`skip` is a target already decided). A match finishes that attempt.
    /// An undecidable target fails the request only once every other target
    /// has been checked and none applied.
    async fn late_attempt(
        &self,
        request: &mut CasRequest<'_>,
        row: Option<CurrentRow>,
        skip: Option<i32>,
    ) -> StorageResult<Option<Applied>> {
        let mut decided: Vec<i32> = skip.into_iter().collect();
        let open: Vec<(usize, i32, i32)> = request
            .attempts
            .iter()
            .enumerate()
            .filter(|(_, a)| a.unknown.is_some() && !a.settled)
            .map(|(n, a)| (n, a.plan.target_version(), a.plan.expected_version()))
            .collect();
        let mut undecided: Option<usize> = None;
        for (n, target, expected) in open {
            if decided.contains(&target) {
                continue;
            }
            decided.push(target);
            match self.decide(request, target, expected, row).await? {
                Decision::Applied(k) => return Ok(Some(self.finish(request, k, false).await)),
                Decision::AppliedSuperseded(k) => {
                    return Ok(Some(self.finish(request, k, true).await));
                }
                Decision::AnotherWriter => request.settle(target),
                Decision::Undecidable => {
                    undecided.get_or_insert(n);
                }
                Decision::Unchanged => {}
            }
        }
        match undecided {
            Some(n) => {
                request.stats.undecidable();
                Err(undecidable(request.timed_out(n), request.operation))
            }
            None => Ok(None),
        }
    }

    /// `decide(V)` (walkthrough 3.1's table; D3): from the row, then, past
    /// `V`, from row `V` of `resource_versions` (R3).
    async fn decide(
        &self,
        request: &CasRequest<'_>,
        target: i32,
        expected: i32,
        row: Option<CurrentRow>,
    ) -> StorageResult<Decision> {
        let write_ids: Vec<Uuid> = request.attempts.iter().map(|a| a.write_id).collect();
        if let Some(decision) = decide_from_row(target, expected, &write_ids, row) {
            return Ok(decision);
        }
        request.stats.version_read();
        let mut values = request.key.values();
        values.push(Value::from(target));
        let rows = self
            .session
            .rows(SELECT_VERSION_WRITE_ID, values, StatementKind::Read)
            .await
            .map_err(|error| map_lookup_error(&error, "version read"))?;
        let row_v = rows.first().map(CurrentRow::of).and_then(|r| r.write_id);
        Ok(decide_from_version_row(&write_ids, row_v))
    }

    /// R2 at `LOCAL_SERIAL`. Its own error is never classified as a CAS
    /// (ADR-aacruzgon-0050): 504 for a timeout, `WriteTimeout`/CAS included,
    /// which Paxos v2's read can raise (`Paxos.java:893-901`), and 503 for
    /// anything else ([`map_lookup_error`]).
    async fn serial_read(&self, request: &CasRequest<'_>) -> StorageResult<Option<CurrentRow>> {
        let result = self
            .session
            .rows(
                SELECT_RESOURCE,
                request.key.values(),
                StatementKind::SerialRead,
            )
            .await;
        request.stats.serial_read(result.is_err());
        let rows = result.map_err(|error| map_lookup_error(&error, "serial read"))?;
        Ok(rows.first().map(CurrentRow::of))
    }

    /// Steps 4 to 6 for attempt `n`, which applied, with its own values and
    /// intent; skipped when superseded, leaving the intent for the sweeper,
    /// whose superseded case deletes `K(V-1)`.
    async fn finish(&self, request: &mut CasRequest<'_>, n: usize, superseded: bool) -> Applied {
        let target = request.attempts[n].plan.target_version();
        request.settle(target);
        let attempt = &request.attempts[n];
        let plan = attempt.plan.clone();
        let key = request.key;
        let version_id = plan.target_version();
        let complete = if superseded {
            false
        } else {
            let (cql, values) = plan.new_version_row(&key, attempt.write_id, attempt.last_updated);
            let version_written = self.write_with_retries(cql, values, "version row").await;
            let indexed = self
                .write_index_delta(&key, &plan, attempt.last_updated)
                .await;
            if version_written && indexed {
                self.delete_intent(&attempt.intent, &key).await;
                true
            } else {
                request.stats.post_cas_failure();
                tracing::warn!(
                    operation = request.operation,
                    resource_type = key.resource_type,
                    id = %key.id,
                    version_id,
                    "write applied, but its version or index rows could not be written; \
                     the sweeper finishes it from the intent row"
                );
                false
            }
        };
        request
            .stats
            .applied(n + 1 != request.attempts.len(), superseded);
        Applied {
            version_id,
            last_updated: attempt.last_updated,
            write_id: attempt.write_id,
            superseded,
            post_cas_complete: complete,
            plan,
        }
    }

    /// I1.
    async fn write_intent(
        &self,
        intent: &IntentKey,
        key: &RowKey<'_>,
        write_id: Uuid,
        version_id: i32,
    ) -> Result<(), DriverError> {
        self.session
            .execute(
                INSERT_INTENT,
                vec![
                    Value::from(intent.bucket.as_str()),
                    Value::from(intent.shard),
                    Value::from(intent.enqueued_at),
                    Value::from(key.tenant_id),
                    Value::from(key.resource_type),
                    Value::from(key.id),
                    Value::from(write_id),
                    Value::from(version_id),
                ],
                StatementKind::Write,
            )
            .await
            .map(|_| ())
    }

    /// I2, best effort: a failure leaves the intent for the sweeper.
    async fn delete_intent(&self, intent: &IntentKey, key: &RowKey<'_>) {
        let result = self
            .session
            .execute(
                DELETE_INTENT,
                vec![
                    Value::from(intent.bucket.as_str()),
                    Value::from(intent.shard),
                    Value::from(intent.enqueued_at),
                    Value::from(key.tenant_id),
                    Value::from(key.resource_type),
                    Value::from(key.id),
                ],
                StatementKind::Write,
            )
            .await;
        if let Err(error) = result {
            tracing::debug!(
                resource_type = key.resource_type,
                id = key.id,
                error = ?error,
                "intent delete failed; the sweeper deletes it"
            );
        }
    }

    /// Step 5: the delta from `K(N)` to `K(N+1)` at `ts(N+1)`. `true` once
    /// every row is written.
    ///
    /// The inserts go first, so a concurrent `count` never sees the resource
    /// missing while its `resources_by_type` row moves. The deletes of
    /// `K(N) \ K(N+1)` are sent only once every insert is acknowledged. If an
    /// insert still fails after its tries, the old rows are kept and this
    /// returns `false`: the write succeeds with I1 left (G23), and the
    /// sweeper, which reads `K(N)` from row `N` and `K(N+1)` from the
    /// `resources` row, writes the missing rows and deletes the old ones.
    /// The walkthrough's batch template (section 2, "Index rows") sends a
    /// version's deletes and inserts in one logged batch, all or nothing, so
    /// this keeps what it guarantees: no old row is deleted without its
    /// replacement written.
    async fn write_index_delta(
        &self,
        key: &RowKey<'_>,
        plan: &CasPlan,
        last_updated: DateTime<Utc>,
    ) -> bool {
        let write_ts = write_ts_of(last_updated);
        let previous = plan
            .previous
            .as_ref()
            .map(|p| (p.keys.as_slice(), p.last_updated));
        let (deletes, inserts) = index_delta(previous, (&plan.keys, last_updated));
        let mut inserted = true;
        for (index_key, at) in inserts {
            inserted &= self
                .write_index_row(key, plan, write_ts, index_key, at, true)
                .await;
        }
        if !inserted {
            tracing::warn!(
                resource_type = key.resource_type,
                id = key.id,
                version_id = plan.target_version(),
                "an index insert failed after its tries; the old index rows are kept \
                 for the sweeper"
            );
            return false;
        }
        let mut deleted = true;
        for (index_key, at) in deletes {
            deleted &= self
                .write_index_row(key, plan, write_ts, index_key, at, false)
                .await;
        }
        deleted
    }

    /// One row of step 5's delta, an insert or a delete at `write_ts`, with
    /// its retries. `false` if it was not written.
    async fn write_index_row(
        &self,
        key: &RowKey<'_>,
        plan: &CasPlan,
        write_ts: i64,
        index_key: &IndexKey,
        at: DateTime<Utc>,
        insert: bool,
    ) -> bool {
        let (cql, values) = match (index_key, insert) {
            (IndexKey::ResourcesByType, true) => (
                INSERT_RESOURCES_BY_TYPE,
                vec![
                    Value::from(key.tenant_id),
                    Value::from(key.resource_type),
                    Value::from(at),
                    Value::from(key.id),
                    Value::from(plan.target_version()),
                    Value::from(plan.fhir_version.as_mime_param()),
                    Value::from(write_ts),
                ],
            ),
            (IndexKey::ResourcesByType, false) => (
                DELETE_RESOURCES_BY_TYPE,
                vec![
                    Value::from(write_ts),
                    Value::from(key.tenant_id),
                    Value::from(key.resource_type),
                    Value::from(at),
                    Value::from(key.id),
                ],
            ),
            (other, _) => {
                // Step 5 adds these rows; Steps 2 to 4 never have them.
                tracing::error!(key = ?other, "index row kind not written before Step 5");
                return false;
            }
        };
        self.write_with_retries(cql, values, "index row").await
    }

    /// A plain, idempotent write with a fixed timestamp, tried up to
    /// [`POST_CAS_WRITE_TRIES`] times.
    async fn write_with_retries(&self, cql: &str, values: Vec<Value>, what: &str) -> bool {
        for attempt in 1..=POST_CAS_WRITE_TRIES {
            match self
                .session
                .execute(cql, values.clone(), StatementKind::Write)
                .await
            {
                Ok(_) => return true,
                Err(error) => {
                    tracing::warn!(attempt, what, error = ?error, "post-CAS write failed");
                    if attempt < POST_CAS_WRITE_TRIES {
                        tokio::time::sleep(Duration::from_millis(10 * u64::from(attempt))).await;
                    }
                }
            }
        }
        false
    }

    /// `ResourceStorage::create` (walkthrough 3.2): C1 through the engine.
    pub(super) async fn create_resource(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        resource: Json,
        fhir_version: FhirVersion,
    ) -> StorageResult<StoredResource> {
        // Step 1.
        tenant.check_permission(Operation::Create, resource_type)?;
        check_search_parameter_code(resource_type, &resource)?;

        let id = match resource.get("id").and_then(Json::as_str) {
            Some(id) => {
                check_id(id)?;
                id.to_string()
            }
            None => crate::types::new_resource_id(),
        };
        let (resource, data) = normalized_body("create", resource_type, &id, resource)?;
        let tenant_id = tenant.tenant_id().as_str();
        let key = RowKey {
            tenant_id,
            resource_type,
            id: &id,
        };
        let plan = CasPlan::create(
            resource,
            data,
            fhir_version,
            vec![IndexKey::ResourcesByType],
        );

        let mut request = self.cas_request("create", key);
        let outcome = match self.run_cas(&mut request, plan).await {
            Ok(CasOutcome::Applied(applied)) => Ok(applied),
            Ok(CasOutcome::Missed(miss)) => Err(miss.into_error(resource_type, &id, 0)),
            Err(error) => Err(error),
        };
        // An undecided SearchParameter create: the overlay follows the row
        // (ADR-aacruzgon-0068).
        self.settle_overlay_after_unknown(&request, &outcome).await;
        let applied = outcome?;

        let resource = applied.plan.body().cloned().unwrap_or(Json::Null);
        if resource_type == "SearchParameter" {
            self.note_stored_search_parameter(tenant_id, &resource, applied.last_updated);
        }
        Ok(StoredResource::from_storage(
            resource_type,
            &id,
            "1",
            tenant.tenant_id().clone(),
            resource,
            applied.last_updated,
            applied.last_updated,
            None,
            fhir_version,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdrs_tokio::consistency::Consistency;
    use cdrs_tokio::frame::Version;
    use cdrs_tokio::frame::message_error::{
        CasWriteUnknownError, ErrorBody, FailureInfo, UnavailableError, WriteFailureError,
        WriteTimeoutError, WriteType,
    };
    use cdrs_tokio::frame::message_result::{
        BodyResResultRows, ColSpec, ColType, ColTypeOption, RowsMetadata, RowsMetadataFlags,
    };
    use cdrs_tokio::types::CBytes;

    fn server(ty: ErrorType) -> DriverError {
        DriverError::from(CdrsError::Server {
            body: ErrorBody::new("x", ty),
            addr: "127.0.0.1:9042".parse().unwrap(),
        })
    }

    fn write_timeout(write_type: WriteType) -> ErrorType {
        ErrorType::WriteTimeout(WriteTimeoutError::new(
            Consistency::LocalQuorum,
            0,
            1,
            write_type,
            None,
        ))
    }

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .with_timezone(&Utc)
    }

    /// A one-row result with these columns, as the node would send it.
    fn row(columns: &[(&str, ColType, Option<Vec<u8>>)]) -> Row {
        let metadata = RowsMetadata::new(
            RowsMetadataFlags::empty(),
            columns.len() as i32,
            None,
            None,
            None,
            columns
                .iter()
                .map(|(name, ty, _)| {
                    ColSpec::new(
                        None,
                        *name,
                        ColTypeOption {
                            id: *ty,
                            value: None,
                        },
                    )
                })
                .collect(),
        );
        let content = columns
            .iter()
            .map(|(_, _, value)| value.clone().map_or_else(CBytes::new_null, CBytes::new))
            .collect();
        Row::from_body(BodyResResultRows::new(
            metadata,
            1,
            vec![content],
            Version::V5,
        ))
        .remove(0)
    }

    fn applied_false() -> (&'static str, ColType, Option<Vec<u8>>) {
        ("[applied]", ColType::Boolean, Some(vec![0]))
    }

    fn int(value: i32) -> Option<Vec<u8>> {
        Some(value.to_be_bytes().to_vec())
    }

    fn timestamp(millis: i64) -> Option<Vec<u8>> {
        Some(millis.to_be_bytes().to_vec())
    }

    fn previous(version_id: i32, deleted: bool) -> PreviousVersion {
        let keys = if deleted {
            Vec::new()
        } else {
            vec![IndexKey::ResourcesByType]
        };
        PreviousVersion {
            version_id,
            data: (!deleted).then(|| Arc::from(r#"{"resourceType":"Patient","id":"p"}"#)),
            last_updated: at("2026-11-06T10:00:00.123Z"),
            deleted_at: deleted.then(|| at("2026-11-06T10:00:00.123Z")),
            fhir_version: FhirVersion::default_enabled(),
            write_id: Some(Uuid::new_v4()),
            keys_blob: index_keys::encode(&keys),
            keys,
        }
    }

    fn body() -> (Json, String) {
        normalized_body(
            "test",
            "Patient",
            "p",
            serde_json::json!({"resourceType": "Patient"}),
        )
        .unwrap()
    }

    fn key() -> RowKey<'static> {
        RowKey {
            tenant_id: "acme",
            resource_type: "Patient",
            id: "p",
        }
    }

    fn every_plan() -> Vec<CasPlan> {
        let (json, data) = body();
        vec![
            CasPlan::create(
                json.clone(),
                data.clone(),
                FhirVersion::default_enabled(),
                vec![IndexKey::ResourcesByType],
            ),
            CasPlan::update(
                previous(3, false),
                json.clone(),
                data.clone(),
                vec![IndexKey::ResourcesByType],
            ),
            CasPlan::delete(previous(3, false)),
            CasPlan::revive(
                previous(3, true),
                json,
                data,
                vec![IndexKey::ResourcesByType],
            ),
        ]
    }

    #[test]
    fn cas_failures_are_classified_as_the_design_says() {
        let unknown = |error, expect_timeout: bool| match classify_cas_failure(error) {
            CasFailure::Unknown { timed_out, .. } => assert_eq!(timed_out, expect_timeout),
            other => panic!("expected unknown, got {other:?}"),
        };
        unknown(server(write_timeout(WriteType::Cas)), true);
        unknown(
            server(ErrorType::WriteFailure(WriteFailureError::new(
                Consistency::LocalQuorum,
                0,
                1,
                FailureInfo::NumFailures(1),
                WriteType::Cas,
            ))),
            false,
        );
        unknown(
            DriverError::from(CdrsError::General(
                "Connection closed while waiting for response!".to_string(),
            )),
            true,
        );
        unknown(
            DriverError::from(CdrsError::Io(std::io::Error::other("reset"))),
            true,
        );
        unknown(
            server(ErrorType::CasWriteUnknown(CasWriteUnknownError::new(
                Consistency::LocalSerial,
                0,
                1,
            ))),
            true,
        );
        unknown(
            server(crate::backends::cassandra::errors::tests::undecoded_error_type()),
            false,
        );
        unknown(
            DriverError::from(CdrsError::General(
                "Transport processing task panicked!".to_string(),
            )),
            true,
        );
        unknown(
            DriverError::Deadline {
                after: Duration::from_secs(10),
                sent: true,
            },
            true,
        );

        // Confirmed by the serial read, never taken on trust.
        unknown(server(write_timeout(WriteType::Simple)), true);
        // Can be raised after the proposal was accepted.
        unknown(server(ErrorType::Server), false);
        unknown(server(ErrorType::Overloaded), false);
        // Paxos v2 raises only write-type errors for a CAS (`Paxos.java:551-567`),
        // and a read timeout comes only from the v1 path; but none of these
        // proves the CAS was not executed, and taking one as "not applied"
        // would skip the serial read for a write that may have applied.
        unknown(
            server(ErrorType::ReadTimeout(
                cdrs_tokio::frame::message_error::ReadTimeoutError::new(
                    Consistency::LocalSerial,
                    0,
                    1,
                    false,
                ),
            )),
            true,
        );
        unknown(
            server(ErrorType::ReadFailure(
                cdrs_tokio::frame::message_error::ReadFailureError::new(
                    Consistency::LocalSerial,
                    0,
                    1,
                    FailureInfo::NumFailures(1),
                    false,
                ),
            )),
            false,
        );
        unknown(server(ErrorType::Truncate), false);
        unknown(server(ErrorType::CdcWriteFailure), false);
        for not_sent in [
            DriverError::Deadline {
                after: Duration::from_secs(10),
                sent: false,
            },
            DriverError::from(CdrsError::Timeout("connect".to_string())),
            DriverError::from(CdrsError::InvalidProtocol(
                "127.0.0.1:9042".parse().unwrap(),
            )),
            DriverError::from(CdrsError::General(NO_NODE_IN_PLAN.to_string())),
            server(ErrorType::Unavailable(UnavailableError::new(
                Consistency::LocalSerial,
                1,
                0,
            ))),
            DriverError::from(CdrsError::General(
                "No active connections to: 127.0.0.1:9042".to_string(),
            )),
            server(ErrorType::IsBootstrapping),
            server(ErrorType::Invalid),
            server(ErrorType::Syntax),
            server(ErrorType::Unauthorized),
            server(ErrorType::Config),
            server(ErrorType::Authentication),
            server(ErrorType::Protocol),
            server(ErrorType::Unprepared(
                cdrs_tokio::frame::message_error::UnpreparedError::new(
                    cdrs_tokio::types::CBytesShort::new(vec![1]),
                ),
            )),
        ] {
            assert!(
                matches!(classify_cas_failure(not_sent), CasFailure::NotApplied(_)),
                "should be not applied"
            );
        }
    }

    /// The two `General` errors `cdrs-tokio` 10.0.0 sends to every request
    /// pending on a connection, some of them already on the wire
    /// (`transport.rs:531-537`, `:684`, `:769`, `:925-931`).
    #[test]
    fn a_failed_write_channel_and_an_unmatched_stream_are_unknown() {
        for message in ["Write channel failure!", "Unmatched stream id: 17"] {
            match classify_cas_failure(DriverError::from(CdrsError::General(message.to_string()))) {
                CasFailure::Unknown { timed_out, .. } => assert!(timed_out, "{message}"),
                other => panic!("{message}: expected unknown, got {other:?}"),
            }
        }
    }

    /// D3 sub-decision 4: the serial read's own error goes through the plain
    /// table, so a `WriteTimeout`/CAS there (Paxos v2's read can raise it) is
    /// the read timing out, 504, and a failure is 503; neither is an unknown
    /// write that would re-run the attempt.
    #[test]
    fn a_serial_read_error_is_mapped_as_a_read_never_as_a_cas() {
        assert!(matches!(
            map_driver_error(&server(write_timeout(WriteType::Cas)), "serial read"),
            BackendError::Timeout { .. }
        ));
        assert!(matches!(
            map_driver_error(
                &server(ErrorType::ReadFailure(
                    cdrs_tokio::frame::message_error::ReadFailureError::new(
                        Consistency::LocalSerial,
                        0,
                        1,
                        FailureInfo::NumFailures(1),
                        false,
                    )
                )),
                "serial read"
            ),
            BackendError::Unavailable { .. }
        ));
    }

    /// ADR-aacruzgon-0055: after an unknown outcome, R2's and R3's own
    /// errors are 504 for a timeout and 503 for everything else; what the
    /// plain table makes 500 (`Server`, `Invalid`, an unmatched stream id, a
    /// decode error) is 503, since the write may have applied.
    #[test]
    fn an_r2_or_r3_error_is_504_for_a_timeout_and_503_otherwise() {
        let read_timeout =
            ErrorType::ReadTimeout(cdrs_tokio::frame::message_error::ReadTimeoutError::new(
                Consistency::LocalSerial,
                0,
                1,
                false,
            ));
        let timeouts = [
            server(write_timeout(WriteType::Cas)),
            server(read_timeout),
            DriverError::Deadline {
                after: Duration::from_millis(12_000),
                sent: true,
            },
        ];
        let others = [
            server(ErrorType::Server),
            server(ErrorType::Invalid),
            server(ErrorType::Overloaded),
            DriverError::from(CdrsError::General("Unmatched stream id: 17".to_string())),
            DriverError::from(CdrsError::General(
                "Connection closed when writing data!".to_string(),
            )),
            DriverError::from(CdrsError::General("cannot decode column data".to_string())),
        ];
        for operation in ["serial read", "version read"] {
            for error in &timeouts {
                assert!(
                    matches!(
                        map_lookup_error(error, operation),
                        StorageError::Backend(BackendError::Timeout { .. })
                    ),
                    "{operation}: {error:?}"
                );
            }
            for error in &others {
                match map_lookup_error(error, operation) {
                    StorageError::Backend(
                        BackendError::Unavailable { message, .. }
                        | BackendError::ConnectionFailed { message, .. },
                    ) => assert!(!message.contains("127.0.0.1"), "{message}"),
                    other => panic!("{operation}: {error:?} gave {other:?}"),
                }
            }
        }
        // The plain table itself is unchanged: the same `Server` error on any
        // other statement is still 500.
        assert!(matches!(
            map_driver_error(&server(ErrorType::Server), "intent row"),
            BackendError::Internal { .. }
        ));
    }

    #[test]
    fn undecidable_is_504_after_a_timeout_and_503_after_a_failure() {
        assert!(matches!(
            undecidable(true, "create"),
            StorageError::Backend(BackendError::Timeout { .. })
        ));
        assert!(matches!(
            undecidable(false, "create"),
            StorageError::Backend(BackendError::Unavailable { .. })
        ));
    }

    #[test]
    fn intent_keys_follow_the_schema() {
        let at = at("2026-10-14T10:00:00.123Z");
        let key = IntentKey::new("acme", "p1", at);
        assert_eq!(key.bucket, "2026-10-14T10");
        assert!((0..16).contains(&key.shard));
        assert_eq!(key.shard, shard_of("acme", "p1"), "stable");
        assert_eq!(key.enqueued_at.get_version_num(), 1, "a timeuuid");
        let (seconds, nanos) = key
            .enqueued_at
            .get_timestamp()
            .expect("v1 has a timestamp")
            .to_unix();
        assert_eq!((seconds, nanos), (at.timestamp() as u64, 123_000_000));
        assert_ne!(
            IntentKey::new("acme", "p1", at).enqueued_at,
            key.enqueued_at,
            "same instant, different timeuuid"
        );
    }

    #[test]
    fn shards_spread_and_do_not_depend_on_concatenation() {
        let shards: std::collections::BTreeSet<i16> = (0..200)
            .map(|i| shard_of("acme", &format!("p{i}")))
            .collect();
        assert_eq!(shards.len(), 16);
        assert_ne!(shard_of("ab", "c"), shard_of("a", "bc"));
    }

    #[test]
    fn last_updated_is_whole_milliseconds_and_write_ts_is_derived() {
        let lu = now_millis();
        assert_eq!(lu.nanosecond() % 1_000_000, 0);
        assert_eq!(write_ts_of(lu), lu.timestamp_millis() * 1000);
    }

    /// `lu(N+1) = max(now_ms, lu(N) + 1 ms)` (ADR-aacruzgon-0016).
    #[test]
    fn the_next_last_updated_never_falls_behind_the_version_it_replaces() {
        let lu_n = at("2026-11-06T10:00:00.500Z");
        // A clock behind lu(N), as on another instance: 1 ms after lu(N).
        assert_eq!(
            next_last_updated(at("2026-11-06T09:59:59.000Z"), Some(lu_n)),
            at("2026-11-06T10:00:00.501Z")
        );
        // The same millisecond: still 1 ms later.
        assert_eq!(
            next_last_updated(at("2026-11-06T10:00:00.500900Z"), Some(lu_n)),
            at("2026-11-06T10:00:00.501Z")
        );
        // A clock ahead: now, truncated to the millisecond.
        assert_eq!(
            next_last_updated(at("2026-11-06T10:00:02.250777Z"), Some(lu_n)),
            at("2026-11-06T10:00:02.250Z")
        );
        // lu(1).
        assert_eq!(
            next_last_updated(at("2026-11-06T10:00:02.250777Z"), None),
            at("2026-11-06T10:00:02.250Z")
        );
    }

    #[test]
    fn ids_and_search_parameter_codes_are_bounded() {
        assert!(check_id("p1").is_ok());
        assert!(check_id(&"a".repeat(64)).is_ok());
        for bad in ["", "a/b", "a b", "ä", &"a".repeat(65)] {
            assert!(check_id(bad).is_err(), "{bad:?}");
        }
        let code =
            |n| serde_json::json!({"resourceType": "SearchParameter", "code": "c".repeat(n)});
        assert!(check_search_parameter_code("SearchParameter", &code(128)).is_ok());
        assert!(matches!(
            check_search_parameter_code("SearchParameter", &code(129)),
            Err(StorageError::Validation(
                ValidationError::InvalidResource { .. }
            ))
        ));
        assert!(check_search_parameter_code("Patient", &code(500)).is_ok());
    }

    /// D14: what a CAS carries is bounded before anything is written.
    #[test]
    fn what_a_cas_carries_is_bounded_at_8_mib() {
        let keys = vec![IndexKey::ResourcesByType];
        let blob = index_keys::encode(&keys).len();
        let plan = |n: usize| {
            CasPlan::create(
                Json::Null,
                "x".repeat(n),
                FhirVersion::default_enabled(),
                keys.clone(),
            )
        };
        assert!(check_cas_size(&plan(MAX_CAS_PAYLOAD_BYTES - blob)).is_ok());
        assert!(matches!(
            check_cas_size(&plan(MAX_CAS_PAYLOAD_BYTES - blob + 1)),
            Err(StorageError::Validation(
                ValidationError::InvalidResource { .. }
            ))
        ));
        // A delete carries only its empty key set.
        assert!(check_cas_size(&CasPlan::delete(previous(1, false))).is_ok());
    }

    #[test]
    fn c1_binds_no_null_and_no_unset_value() {
        let plan = &every_plan()[0];
        let values = plan.cas_values(&key(), Uuid::new_v4(), now_millis());
        // Ten markers in C1; version_id and deleted_at are literals.
        assert_eq!(values.len(), 10);
        assert!(
            values.iter().all(|value| matches!(value, Value::Some(_))),
            "{values:?}"
        );
    }

    /// Every marker of C2 to C4 and of V1 and V2 is bound, to a value: no
    /// null (a tombstone) and no unset cell. The count matches the CQL's
    /// markers.
    #[test]
    fn every_cas_and_version_row_binds_every_marker_to_a_value() {
        for plan in every_plan() {
            let values = plan.cas_values(&key(), Uuid::new_v4(), now_millis());
            assert_eq!(
                values.len(),
                plan.kind().cql().matches('?').count(),
                "{:?}",
                plan.kind()
            );
            assert!(
                values.iter().all(|value| matches!(value, Value::Some(_))),
                "{:?}: {values:?}",
                plan.kind()
            );
            let (cql, values) = plan.new_version_row(&key(), Uuid::new_v4(), now_millis());
            assert_eq!(values.len(), cql.matches('?').count(), "{:?}", plan.kind());
            assert!(
                values.iter().all(|value| matches!(value, Value::Some(_))),
                "{:?}: {values:?}",
                plan.kind()
            );
        }
        let delete = CasPlan::delete(previous(3, false));
        assert_eq!(
            delete
                .new_version_row(&key(), Uuid::new_v4(), now_millis())
                .0,
            INSERT_DELETE_MARKER
        );
        assert_eq!((delete.expected_version(), delete.target_version()), (3, 4));
    }

    /// V3 rewrites row `N` one microsecond older than the original, from what
    /// R1 read, with its `write_id` (G40; D5). A marker is a V2 row, a
    /// content row a V1 row; neither binds a null.
    #[test]
    fn v3_rewrites_row_n_at_its_timestamp_minus_one() {
        for deleted in [false, true] {
            let previous = previous(3, deleted);
            let (cql, values) = previous_version_row(&key(), &previous);
            assert_eq!(
                cql,
                if deleted {
                    INSERT_DELETE_MARKER
                } else {
                    INSERT_VERSION
                }
            );
            assert_eq!(values.len(), cql.matches('?').count());
            let ts = write_ts_of(previous.last_updated);
            assert_eq!(
                values[values.len() - 1],
                Value::from(ts - 1),
                "USING TIMESTAMP"
            );
            assert_eq!(values[values.len() - 4], Value::from(ts), "write_ts column");
            assert_eq!(values[3], Value::from(3_i32), "row N");
            assert_eq!(
                values[values.len() - 3],
                Value::from(previous.write_id.unwrap())
            );
            assert!(
                values.iter().all(|value| matches!(value, Value::Some(_))),
                "{values:?}"
            );
        }
        // An unknown w(N) is left unset, never bound to null.
        let mut unknown = previous(3, false);
        unknown.write_id = None;
        let (_, values) = previous_version_row(&key(), &unknown);
        assert_eq!(values[values.len() - 3], Value::NotSet);
        assert!(!values.iter().any(|value| matches!(value, Value::Null)));
    }

    /// Step 5's delta: an update moves the `resources_by_type` row from
    /// `(lu(N), id)` to `(lu(N+1), id)`; a delete removes every key of
    /// `K(N)`; a create only inserts.
    #[test]
    fn the_key_delta_deletes_what_the_old_version_wrote_and_inserts_the_new_set() {
        let x1 = [IndexKey::ResourcesByType];
        let lu_n = at("2026-11-06T10:00:00.000Z");
        let lu_v = at("2026-11-06T10:00:00.001Z");
        let (deletes, inserts) = index_delta(Some((&x1, lu_n)), (&x1, lu_v));
        assert_eq!(deletes, vec![(&x1[0], lu_n)]);
        assert_eq!(inserts, vec![(&x1[0], lu_v)]);

        let (deletes, inserts) = index_delta(Some((&x1, lu_n)), (&[], lu_v));
        assert_eq!(deletes, vec![(&x1[0], lu_n)]);
        assert!(inserts.is_empty());

        let (deletes, inserts) = index_delta(None, (&x1, lu_v));
        assert!(deletes.is_empty());
        assert_eq!(inserts, vec![(&x1[0], lu_v)]);

        // A row both versions hold at the same key is not deleted (only
        // possible for a key without `last_updated`, which no table has; the
        // set difference is still exact).
        let (deletes, _) = index_delta(Some((&x1, lu_v)), (&x1, lu_v));
        assert!(deletes.is_empty());
    }

    /// D4: the miss mapping from the returned columns (spike A1). A missing
    /// row returns `[applied]` alone, so "no row" is the condition columns
    /// being absent, not a null `version_id`.
    #[test]
    fn a_definite_miss_is_mapped_from_the_returned_columns() {
        let alone = row(&[applied_false()]);
        for kind in [CasKind::Update, CasKind::Delete, CasKind::Revive] {
            assert_eq!(CasMiss::of(kind, Some(&alone)), CasMiss::NoRow, "{kind:?}");
            assert_eq!(CasMiss::of(kind, None), CasMiss::NoRow, "{kind:?}");
        }

        let deleted = row(&[
            applied_false(),
            ("version_id", ColType::Int, int(4)),
            (
                "deleted_at",
                ColType::Timestamp,
                timestamp(1_762_400_000_000),
            ),
        ]);
        assert_eq!(
            CasMiss::of(CasKind::Update, Some(&deleted)),
            CasMiss::Deleted
        );
        assert_eq!(
            CasMiss::of(CasKind::Delete, Some(&deleted)),
            CasMiss::Deleted
        );

        let moved = row(&[
            applied_false(),
            ("version_id", ColType::Int, int(5)),
            ("deleted_at", ColType::Timestamp, timestamp(0)),
        ]);
        assert_eq!(
            CasMiss::of(CasKind::Update, Some(&moved)),
            CasMiss::VersionMoved { current: Some(5) }
        );
        // C4's IF has `version_id` only.
        let moved_revive = row(&[applied_false(), ("version_id", ColType::Int, int(5))]);
        assert_eq!(
            CasMiss::of(CasKind::Revive, Some(&moved_revive)),
            CasMiss::VersionMoved { current: Some(5) }
        );
        // A null version_id is not "no row": the column is there.
        let null_version = row(&[applied_false(), ("version_id", ColType::Int, None)]);
        assert_eq!(
            CasMiss::of(CasKind::Update, Some(&null_version)),
            CasMiss::VersionMoved { current: None }
        );
        // IF NOT EXISTS returned a row.
        assert_eq!(CasMiss::of(CasKind::Create, Some(&moved)), CasMiss::Exists);

        let not_found = |miss: CasMiss| {
            matches!(
                miss.into_error("Patient", "p", 3),
                StorageError::Resource(ResourceError::NotFound { .. })
            )
        };
        assert!(not_found(CasMiss::NoRow));
        assert!(not_found(CasMiss::Deleted));
        match (CasMiss::VersionMoved { current: Some(5) }).into_error("Patient", "p", 3) {
            StorageError::Concurrency(ConcurrencyError::VersionConflict {
                expected_version,
                actual_version,
                ..
            }) => assert_eq!(
                (expected_version.as_str(), actual_version.as_str()),
                ("3", "5")
            ),
            other => panic!("expected a version conflict, got {other:?}"),
        }
        assert!(matches!(
            CasMiss::Exists.into_error("Patient", "p", 0),
            StorageError::Resource(ResourceError::AlreadyExists { .. })
        ));
    }

    /// ADR-aacruzgon-0067: until an attempt's outcome is unknown, a
    /// statement's error is the plain table's, 500 included; from then on a
    /// timeout stays 504, and everything else is 503. A non-backend error of
    /// the request (here a permission) is kept as it is.
    #[test]
    fn after_an_unknown_outcome_no_statement_error_is_500() {
        let key = RowKey {
            tenant_id: "t",
            resource_type: "Patient",
            id: "p",
        };
        let mut request = CasRequest {
            operation: "update",
            key,
            attempts: Vec::new(),
            reruns: 0,
            stats: Arc::new(CasCounters::default()),
        };
        let server_error = server(ErrorType::Server);
        assert!(matches!(
            request.statement_error(&server_error, "intent row"),
            StorageError::Backend(BackendError::Internal { .. })
        ));
        let internal = || {
            Err::<(), _>(StorageError::Backend(map_driver_error(
                &server_error,
                "pre-read",
            )))
        };
        assert!(matches!(
            request.after_unknown(internal(), "pre-read"),
            Err(StorageError::Backend(BackendError::Internal { .. }))
        ));

        let mut attempt = Attempt {
            write_id: Uuid::new_v4(),
            last_updated: Utc::now(),
            intent: IntentKey::new("t", "p", Utc::now()),
            plan: Arc::new(CasPlan::create(
                Json::Null,
                "{}".to_string(),
                FhirVersion::default_enabled(),
                vec![IndexKey::ResourcesByType],
            )),
            unknown: None,
            settled: false,
        };
        attempt.unknown = Some(true);
        request.attempts.push(attempt);
        for error in [server(ErrorType::Server), server(ErrorType::Invalid)] {
            assert!(
                matches!(
                    request.statement_error(&error, "intent row"),
                    StorageError::Backend(BackendError::Unavailable { .. })
                ),
                "{error:?}"
            );
        }
        assert!(matches!(
            request.statement_error(&server(write_timeout(WriteType::Simple)), "intent row"),
            StorageError::Backend(BackendError::Timeout { .. })
        ));
        assert!(matches!(
            request.after_unknown(internal(), "pre-read"),
            Err(StorageError::Backend(BackendError::Unavailable { .. }))
        ));
        let denied = Err::<(), _>(StorageError::Tenant(
            crate::error::TenantError::OperationNotPermitted {
                tenant_id: crate::tenant::TenantId::new("t"),
                operation: "delete".to_string(),
            },
        ));
        assert!(matches!(
            request.after_unknown(denied, "pre-read"),
            Err(StorageError::Tenant(_))
        ));
    }

    /// "Another writer" after an unknown outcome maps from R2's row as a
    /// definite miss maps from the CAS's columns: deleted is `Deleted`
    /// (`NotFound`), a live version `VersionMoved`, and a create `Exists`.
    #[test]
    fn another_writers_miss_maps_from_the_row_as_a_definite_miss_does() {
        let row = |version_id, deleted| {
            Some(CurrentRow {
                version_id,
                write_id: Some(Uuid::new_v4()),
                deleted,
            })
        };
        for kind in [CasKind::Update, CasKind::Delete, CasKind::Revive] {
            assert_eq!(
                another_writers_miss(kind, row(Some(2), true), 2),
                CasMiss::Deleted,
                "{kind:?}: deleted in the gap"
            );
            assert_eq!(
                another_writers_miss(kind, row(Some(5), false), 2),
                CasMiss::VersionMoved { current: Some(5) },
                "{kind:?}"
            );
            assert_eq!(
                another_writers_miss(kind, row(None, false), 2),
                CasMiss::VersionMoved { current: Some(2) },
                "{kind:?}: an undecodable version is the target's"
            );
        }
        assert_eq!(
            another_writers_miss(CasKind::Create, row(Some(1), true), 1),
            CasMiss::Exists
        );
    }

    /// `decide(V)` from the row (walkthrough 3.1's table), for `V = 1`
    /// (create, `N = 0`) and `V = 4` (`N = 3`).
    #[test]
    fn decide_follows_the_walkthrough_table_for_version_1_and_later_versions() {
        let (ours, earlier, theirs) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let tried = [earlier, ours];
        let at = |version: Option<i32>, write_id: Option<Uuid>| {
            Some(CurrentRow {
                version_id: version,
                write_id,
                deleted: false,
            })
        };
        for (target, expected) in [(1, 0), (4, 3)] {
            let decide = |row| decide_from_row(target, expected, &tried, row);
            assert_eq!(
                decide(at(Some(target), Some(ours))),
                Some(Decision::Applied(1))
            );
            assert_eq!(
                decide(at(Some(target), Some(earlier))),
                Some(Decision::Applied(0)),
                "an earlier attempt that applied late is still ours"
            );
            assert_eq!(
                decide(at(Some(target), Some(theirs))),
                Some(Decision::AnotherWriter)
            );
            // An undecodable cell decides nothing, never "another writer".
            assert_eq!(decide(at(Some(target), None)), Some(Decision::Undecidable));
            assert_eq!(decide(at(None, Some(ours))), Some(Decision::Undecidable));
            // Past V: row V decides.
            assert_eq!(decide(at(Some(target + 1), Some(theirs))), None);
            assert_eq!(decide(at(Some(target + 7), None)), None);
        }
        // Unchanged: no row for a create, version N for the rest.
        assert_eq!(
            decide_from_row(1, 0, &tried, None),
            Some(Decision::Unchanged)
        );
        assert_eq!(
            decide_from_row(4, 3, &tried, at(Some(3), Some(theirs))),
            Some(Decision::Unchanged)
        );
        // No row after a pre-read found one, or a version below N: neither
        // can happen, so neither is taken as "unchanged".
        assert_eq!(
            decide_from_row(4, 3, &tried, None),
            Some(Decision::Undecidable)
        );
        assert_eq!(
            decide_from_row(4, 3, &tried, at(Some(2), Some(theirs))),
            Some(Decision::Undecidable)
        );
        // Row V of resource_versions, for a superseded target.
        assert_eq!(
            decide_from_version_row(&tried, Some(earlier)),
            Decision::AppliedSuperseded(0)
        );
        assert_eq!(
            decide_from_version_row(&tried, Some(theirs)),
            Decision::AnotherWriter
        );
        assert_eq!(decide_from_version_row(&tried, None), Decision::Undecidable);
    }

    #[test]
    fn rerun_pauses_are_5_to_25_ms() {
        for _ in 0..200 {
            let pause = rerun_pause();
            assert!((5..=25).contains(&pause.as_millis()), "{pause:?}");
        }
    }

    /// The string literals of a Rust source file, with `\`-newline
    /// continuations folded as the compiler folds them. Comments are skipped.
    fn string_literals(source: &str) -> Vec<String> {
        let chars: Vec<char> = source.chars().collect();
        let mut literals = Vec::new();
        let mut i = 0;
        while i < chars.len() {
            match chars[i] {
                '/' if chars.get(i + 1) == Some(&'/') => {
                    while i < chars.len() && chars[i] != '\n' {
                        i += 1;
                    }
                }
                '/' if chars.get(i + 1) == Some(&'*') => {
                    i += 2;
                    while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                        i += 1;
                    }
                    i += 2;
                }
                '\'' => {
                    // A char literal ('x', '\n', '"') or a lifetime ('a).
                    if chars.get(i + 1) == Some(&'\\') {
                        // Past the backslash and the escaped character,
                        // which may itself be a quote ('\'').
                        i += 3;
                        while i < chars.len() && chars[i] != '\'' {
                            i += 1;
                        }
                        i += 1;
                    } else if chars.get(i + 2) == Some(&'\'') {
                        i += 3;
                    } else {
                        i += 1;
                    }
                }
                'r' if matches!(chars.get(i + 1), Some('"') | Some('#'))
                    && (i == 0 || !(chars[i - 1].is_alphanumeric() || chars[i - 1] == '_')) =>
                {
                    let mut j = i + 1;
                    let mut hashes = 0;
                    while chars.get(j) == Some(&'#') {
                        hashes += 1;
                        j += 1;
                    }
                    if chars.get(j) != Some(&'"') {
                        i += 1;
                        continue;
                    }
                    j += 1;
                    let start = j;
                    let closing: String = std::iter::once('"')
                        .chain(std::iter::repeat_n('#', hashes))
                        .collect();
                    let rest: String = chars[start..].iter().collect();
                    let end = rest.find(&closing).unwrap_or(rest.len());
                    literals.push(rest[..end].to_string());
                    i = start + rest[..end].chars().count() + closing.chars().count();
                }
                '"' => {
                    let mut literal = String::new();
                    i += 1;
                    while i < chars.len() && chars[i] != '"' {
                        if chars[i] == '\\' {
                            match chars.get(i + 1) {
                                Some('\n') => {
                                    i += 2;
                                    while i < chars.len() && chars[i].is_whitespace() {
                                        i += 1;
                                    }
                                    continue;
                                }
                                Some(escaped) => literal.push(*escaped),
                                None => {}
                            }
                            i += 2;
                            continue;
                        }
                        literal.push(chars[i]);
                        i += 1;
                    }
                    literals.push(literal);
                    i += 1;
                }
                _ => i += 1,
            }
        }
        literals
    }

    /// The statements a scan of `literals` refuses: a CQL `DELETE` on
    /// `resources` or `resource_versions`, or a null written there. Deletes
    /// of index tables (`resources_by_type`) and intent rows are expected.
    fn forbidden_statements(literals: &[String]) -> Vec<String> {
        let delete = regex::Regex::new(
            r#"(?is)\bDELETE\b[^;]*?\bFROM\s+(\w+\.)?"?(resources|resource_versions)"?(\s|;|$)"#,
        )
        .unwrap();
        let writes_to = regex::Regex::new(
            r#"(?is)\b(INSERT\s+INTO|UPDATE)\s+(\w+\.)?"?(resources|resource_versions)"?(\s|\()"#,
        )
        .unwrap();
        let null = regex::Regex::new(r"(?i)\bnull\b").unwrap();
        literals
            .iter()
            .filter(|cql| delete.is_match(cql) || (writes_to.is_match(cql) && null.is_match(cql)))
            .cloned()
            .collect()
    }

    /// D6, layer 1: no CQL `DELETE` is ever written against `resources` or
    /// `resource_versions`, and no null either, in any file of this backend
    /// (`read_dir`, so a new file is never missed). A delete is a soft delete
    /// (C3) and a revive writes `deleted_at = 0` (G39), so these tables never
    /// hold a tombstone.
    #[test]
    fn no_statement_deletes_from_or_writes_null_to_resources_or_resource_versions() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/backends/cassandra");
        let mut scanned = 0;
        let mut found = Vec::new();
        let mut pending = vec![dir.clone()];
        let mut files = Vec::new();
        while let Some(next) = pending.pop() {
            for entry in std::fs::read_dir(&next).expect("the backend's directory") {
                let path = entry.expect("a directory entry").path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                    files.push(path);
                }
            }
        }
        for path in files {
            let source = std::fs::read_to_string(&path).expect("a source file");
            scanned += 1;
            for statement in forbidden_statements(&string_literals(&source)) {
                found.push(format!("{}: {statement}", path.display()));
            }
        }
        assert!(scanned >= 15, "scanned only {scanned} files in {dir:?}");
        assert!(found.is_empty(), "{found:#?}");

        // The scan itself: what it refuses and what it lets through. Built
        // from pieces, so this file's own literals never match.
        let refused = |cql: &str| !forbidden_statements(&[cql.to_string()]).is_empty();
        let verb = ["DE", "LETE"].concat();
        assert!(refused(&format!("{verb} FROM resources WHERE id = ?")));
        assert!(refused(&format!(
            "{verb} data FROM resource_versions WHERE id = ?"
        )));
        assert!(refused(&format!(
            "UPDATE resources SET deleted_at = {} WHERE id = ?",
            ["nu", "ll"].concat()
        )));
        assert!(!refused(&format!(
            "{verb} FROM resources_by_type USING TIMESTAMP ? WHERE id = ?"
        )));
        assert!(!refused(&format!(
            "{verb} FROM pending_index_writes WHERE id = ?"
        )));
        assert!(!refused(SOFT_DELETE_RESOURCE_IF_VERSION));
        assert!(refused(&format!(
            "BEGIN BATCH {verb} FROM resources WHERE id = ?; APPLY BATCH"
        )));
        assert!(refused(&format!(
            "{verb} FROM helios.resources WHERE id = ?"
        )));
        assert!(refused(&format!(
            "{verb} FROM \"resource_versions\" WHERE id = ?"
        )));
        assert!(refused(&format!(
            "INSERT INTO helios.resources (id, data) VALUES (?, {})",
            ["NU", "LL"].concat()
        )));
        // A char literal holding an escaped quote opens no string.
        assert_eq!(
            string_literals("let q = ['\\'', '\"']; let s = \"x\";"),
            vec!["x".to_string()]
        );
        let folded = string_literals("const A: &str = \"DE\\\n     LETE\";");
        assert_eq!(folded, vec![verb.clone()], "a continuation folds");
    }
}

/// The decision table of an unknown outcome and the engine's paths, against
/// the node at `HFS_TEST_CASSANDRA_URL` (skipped when unset). Each case sets
/// up its own rows under its own tenant, every write to `resources` as a CAS.
#[cfg(test)]
mod node_tests {
    use super::*;
    use crate::backends::cassandra::CassandraConfig;
    use crate::backends::cassandra::session::Fault;
    use crate::core::ResourceStorage;
    use crate::tenant::{TenantId, TenantPermissions};

    async fn backend() -> Option<CassandraBackend> {
        let url = std::env::var("HFS_TEST_CASSANDRA_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())?;
        Some(
            CassandraBackend::connect(CassandraConfig {
                contact_points: vec![url],
                data_dir: Some(concat!(env!("CARGO_MANIFEST_DIR"), "/../../data").into()),
                ..CassandraConfig::default()
            })
            .await
            .expect("connect"),
        )
    }

    /// An attempt of a create with `write_id`, for the decision table.
    fn tried(write_id: Uuid) -> Attempt {
        Attempt {
            write_id,
            last_updated: now_millis(),
            intent: IntentKey::new("t", "p", Utc::now()),
            plan: Arc::new(CasPlan::create(
                Json::Null,
                "{}".to_string(),
                FhirVersion::default_enabled(),
                vec![IndexKey::ResourcesByType],
            )),
            unknown: Some(true),
            settled: false,
        }
    }

    /// A version-1 row whose `write_id` is `writer`, through C1.
    async fn create_row(backend: &CassandraBackend, tenant: &str, writer: Uuid) {
        let plan = CasPlan::create(
            Json::Null,
            r#"{"resourceType":"Patient","id":"p"}"#.to_string(),
            FhirVersion::default_enabled(),
            vec![IndexKey::ResourcesByType],
        );
        let key = RowKey {
            tenant_id: tenant,
            resource_type: "Patient",
            id: "p",
        };
        let rows = backend
            .session
            .rows(
                INSERT_RESOURCE_IF_NOT_EXISTS,
                plan.cas_values(&key, writer, now_millis()),
                StatementKind::Cas,
            )
            .await
            .expect("C1");
        assert_eq!(applied(&rows), Some(true));
    }

    /// Moves the row to version 2 (a CAS), as a later writer would.
    async fn supersede(backend: &CassandraBackend, tenant: &str) {
        let rows = backend
            .session
            .rows(
                "UPDATE resources SET version_id = 2, write_id = ? \
                 WHERE tenant_id = ? AND resource_type = ? AND id = ? IF version_id = 1",
                vec![
                    Value::from(Uuid::new_v4()),
                    Value::from(tenant),
                    Value::from("Patient"),
                    Value::from("p"),
                ],
                StatementKind::Cas,
            )
            .await
            .expect("supersede");
        assert_eq!(applied(&rows), Some(true));
    }

    /// Row 1 of `resource_versions` with `writer` as its `write_id`.
    async fn version_one(backend: &CassandraBackend, tenant: &str, writer: Uuid) {
        let lu = now_millis();
        backend
            .session
            .execute(
                INSERT_VERSION,
                vec![
                    Value::from(tenant),
                    Value::from("Patient"),
                    Value::from("p"),
                    Value::from(1_i32),
                    Value::from("{}"),
                    Value::from(lu),
                    Value::from(FhirVersion::default_enabled().as_mime_param()),
                    Value::from(write_ts_of(lu)),
                    Value::from(writer),
                    Value::from(Blob::from(index_keys::encode(&[IndexKey::ResourcesByType]))),
                    Value::from(write_ts_of(lu)),
                ],
                StatementKind::Write,
            )
            .await
            .expect("V1");
    }

    /// R2 then `decide(1)`, as a create's unknown outcome runs them.
    async fn resolve(backend: &CassandraBackend, tenant: &str, attempts: &[Uuid]) -> Decision {
        let key = RowKey {
            tenant_id: tenant,
            resource_type: "Patient",
            id: "p",
        };
        let mut request = backend.cas_request("create", key);
        request.attempts = attempts.iter().copied().map(tried).collect();
        let row = backend.serial_read(&request).await.expect("R2");
        backend.decide(&request, 1, 0, row).await.expect("decide")
    }

    fn tenant(case: &str) -> String {
        format!("s2-resolve-{case}-{}", Uuid::new_v4().simple())
    }

    fn tenant_context(case: &str) -> TenantContext {
        TenantContext::new(
            TenantId::new(format!("s3-engine-{case}-{}", Uuid::new_v4().simple())),
            TenantPermissions::full_access(),
        )
    }

    #[tokio::test]
    async fn unknown_create_outcomes_follow_the_walkthrough_table() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let (ours, earlier, theirs) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());

        // No row: not applied when the serial read ran.
        let t = tenant("none");
        assert_eq!(resolve(&backend, &t, &[ours]).await, Decision::Unchanged);

        // Version 1 with one of our write_ids: applied, by that attempt.
        let t = tenant("v1-ours");
        create_row(&backend, &t, ours).await;
        assert_eq!(resolve(&backend, &t, &[ours]).await, Decision::Applied(0));
        assert_eq!(
            resolve(&backend, &t, &[earlier, ours]).await,
            Decision::Applied(1),
            "an earlier attempt that applied late is still ours"
        );
        assert_eq!(
            resolve(&backend, &t, &[theirs]).await,
            Decision::AnotherWriter
        );

        // Past version 1: row 1's write_id decides.
        let t = tenant("superseded-ours");
        create_row(&backend, &t, ours).await;
        version_one(&backend, &t, ours).await;
        supersede(&backend, &t).await;
        assert_eq!(
            resolve(&backend, &t, &[ours]).await,
            Decision::AppliedSuperseded(0)
        );
        assert_eq!(
            resolve(&backend, &t, &[theirs]).await,
            Decision::AnotherWriter
        );

        // Past version 1 with no row 1: nothing tells them apart.
        let t = tenant("superseded-unknown");
        create_row(&backend, &t, ours).await;
        supersede(&backend, &t).await;
        assert_eq!(resolve(&backend, &t, &[ours]).await, Decision::Undecidable);
    }

    fn patient() -> Json {
        serde_json::json!({"resourceType": "Patient", "id": "p", "active": true})
    }

    /// The row R1 reads, for a plan built from it.
    async fn current(backend: &CassandraBackend, tenant: &TenantContext) -> ResourceRow {
        backend
            .read_resource_row(tenant.tenant_id().as_str(), "Patient", "p")
            .await
            .expect("R1")
            .expect("a row")
    }

    async fn version_rows(
        backend: &CassandraBackend,
        tenant: &TenantContext,
    ) -> Vec<(i32, Option<Uuid>, bool)> {
        let rows = backend
            .session
            .rows(
                "SELECT version_id, write_id, deleted_at FROM resource_versions \
                 WHERE tenant_id = ? AND resource_type = ? AND id = ?",
                vec![
                    Value::from(tenant.tenant_id().as_str()),
                    Value::from("Patient"),
                    Value::from("p"),
                ],
                StatementKind::Read,
            )
            .await
            .expect("version rows");
        rows.iter()
            .map(|row| {
                (
                    row.get_r_by_name("version_id").unwrap(),
                    row.get_by_name("write_id").unwrap(),
                    IntoRustByName::<DateTime<Utc>>::get_by_name(row, "deleted_at")
                        .unwrap()
                        .is_some(),
                )
            })
            .collect()
    }

    fn key_of(tenant: &TenantContext) -> RowKey<'_> {
        RowKey {
            tenant_id: tenant.tenant_id().as_str(),
            resource_type: "Patient",
            id: "p",
        }
    }

    fn applied_of(outcome: CasOutcome) -> Applied {
        match outcome {
            CasOutcome::Applied(applied) => applied,
            other => panic!("expected applied, got {other:?}"),
        }
    }

    /// C2, C3 and C4 through the engine on one resource: V3 before each, the
    /// new version row, the `resources_by_type` row moved, removed and put
    /// back (so `count` follows), the miss mapping on the node, and the
    /// statements each costs.
    #[tokio::test]
    async fn the_engine_updates_deletes_and_revives_through_c2_c3_and_c4() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = tenant_context("cycle");
        backend
            .create(
                &tenant,
                "Patient",
                patient(),
                FhirVersion::default_enabled(),
            )
            .await
            .expect("create");
        let count = || backend.count(&tenant, Some("Patient"));
        assert_eq!(count().await.unwrap(), 1);

        // C2: 1 -> 2.
        let row1 = current(&backend, &tenant).await;
        let previous = PreviousVersion::from_row(&row1).unwrap();
        let (json, data) = normalized_body("update", "Patient", "p", patient()).unwrap();
        let before = backend.statement_counts();
        let mut request = backend.cas_request("update", key_of(&tenant));
        let applied = applied_of(
            backend
                .run_cas(
                    &mut request,
                    CasPlan::update(
                        previous.clone(),
                        json.clone(),
                        data.clone(),
                        vec![IndexKey::ResourcesByType],
                    ),
                )
                .await
                .unwrap(),
        );
        let spent = backend.statement_counts().since(&before);
        eprintln!("[statements op=c2 {spent}]");
        // V3, I1, V1, X1 delete, X1 insert, I2; one CAS; no read.
        assert_eq!(
            (spent.write, spent.cas, spent.read, spent.serial_read),
            (6, 1, 0, 0)
        );
        assert_eq!(applied.version_id, 2);
        assert!(applied.post_cas_complete && !applied.superseded);
        assert!(applied.last_updated > row1.last_updated);
        let row2 = current(&backend, &tenant).await;
        assert_eq!(
            (row2.version_id, row2.write_id),
            (2, Some(applied.write_id))
        );
        assert_eq!(count().await.unwrap(), 1, "X1 moved, not duplicated");

        // A stale plan (still at 1) misses: another version.
        let mut stale = backend.cas_request("update", key_of(&tenant));
        match backend
            .run_cas(
                &mut stale,
                CasPlan::update(
                    previous,
                    json.clone(),
                    data.clone(),
                    vec![IndexKey::ResourcesByType],
                ),
            )
            .await
            .unwrap()
        {
            CasOutcome::Missed(CasMiss::VersionMoved { current: Some(2) }) => {}
            other => panic!("expected a version conflict, got {other:?}"),
        }

        // C3: 2 -> 3, a delete marker.
        let row2 = current(&backend, &tenant).await;
        let mut request = backend.cas_request("delete", key_of(&tenant));
        let deleted = applied_of(
            backend
                .run_cas(
                    &mut request,
                    CasPlan::delete(PreviousVersion::from_row(&row2).unwrap()),
                )
                .await
                .unwrap(),
        );
        assert_eq!(deleted.version_id, 3);
        let row3 = current(&backend, &tenant).await;
        assert!(row3.is_deleted());
        assert_eq!(row3.deleted_at, Some(deleted.last_updated));
        assert_eq!(row3.keys().unwrap(), Vec::<IndexKey>::new());
        assert_eq!(count().await.unwrap(), 0, "X1 removed");
        assert!(matches!(
            backend.read(&tenant, "Patient", "p").await,
            Err(StorageError::Resource(ResourceError::Gone { .. }))
        ));

        // C2 from a pre-read of version 2, after the delete: deleted in the
        // gap, which `deleted_at > 0` in the returned columns says.
        let mut in_the_gap = backend.cas_request("update", key_of(&tenant));
        match backend
            .run_cas(
                &mut in_the_gap,
                CasPlan::update(
                    PreviousVersion::from_row(&row2).unwrap(),
                    json.clone(),
                    data.clone(),
                    vec![IndexKey::ResourcesByType],
                ),
            )
            .await
            .unwrap()
        {
            CasOutcome::Missed(CasMiss::Deleted) => {}
            other => panic!("expected deleted, got {other:?}"),
        }

        // C4: 3 -> 4, live again.
        let mut request = backend.cas_request("create_or_update", key_of(&tenant));
        let revived = applied_of(
            backend
                .run_cas(
                    &mut request,
                    CasPlan::revive(
                        PreviousVersion::from_row(&row3).unwrap(),
                        json.clone(),
                        data.clone(),
                        vec![IndexKey::ResourcesByType],
                    ),
                )
                .await
                .unwrap(),
        );
        assert_eq!(revived.version_id, 4);
        let row4 = current(&backend, &tenant).await;
        assert!(!row4.is_deleted());
        assert_eq!(row4.created_at, row1.created_at, "C4 keeps created_at");
        assert_eq!(count().await.unwrap(), 1, "X1 back");
        let read = backend
            .read(&tenant, "Patient", "p")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read.version_id(), "4");

        // Every version row exists, each with the write_id of its CAS; row 3
        // is the marker.
        let mut rows = version_rows(&backend, &tenant).await;
        rows.sort_by_key(|(version, _, _)| *version); // stored newest first
        assert_eq!(
            rows.iter()
                .map(|(v, _, deleted)| (*v, *deleted))
                .collect::<Vec<_>>(),
            vec![(1, false), (2, false), (3, true), (4, false)]
        );
        assert_eq!(rows[0].1, row1.write_id, "V3 carried w(1) from R1");
        assert!(rows.iter().all(|(_, w, _)| w.is_some()));

        // C2 on a row that never existed: [applied] alone.
        let ghost = tenant_context("ghost");
        let mut request = backend.cas_request("update", key_of(&ghost));
        match backend
            .run_cas(
                &mut request,
                CasPlan::update(
                    PreviousVersion::from_row(&row4).unwrap(),
                    json,
                    data,
                    vec![IndexKey::ResourcesByType],
                ),
            )
            .await
            .unwrap()
        {
            CasOutcome::Missed(CasMiss::NoRow) => {}
            other => panic!("expected no row, got {other:?}"),
        }
    }

    /// The fault seam's three modes on C1, and the late apply
    /// (ADR-aacruzgon-0051): each ends as the walkthrough's table says.
    #[tokio::test]
    async fn unknown_create_outcomes_through_the_fault_seam() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let version = FhirVersion::default_enabled();

        // Applied, reply lost: R2 finds our write_id. One attempt.
        let tenant = tenant_context("lost-reply");
        let before = backend.cas_stats();
        backend.session.inject_fault(
            StatementKind::Cas,
            "INSERT INTO resources",
            Fault::ApplyThenTimeOut,
        );
        let created = backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .unwrap();
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=lost-reply {stats}]");
        assert_eq!((stats.attempts, stats.applied, stats.reruns), (1, 1, 0));
        assert_eq!(stats.unknown_write_timeout_cas, 1);
        assert_eq!(created.version_id(), "1");
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 1);

        // Not applied: R2 finds no row, one re-run applies.
        let tenant = tenant_context("rerun");
        let before = backend.cas_stats();
        backend.session.inject_fault(
            StatementKind::Cas,
            "INSERT INTO resources",
            Fault::TimeOutWithoutApplying,
        );
        backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .unwrap();
        let stats = backend.cas_stats().since(&before);
        assert_eq!((stats.attempts, stats.applied, stats.reruns), (2, 1, 1));

        // The serial read itself times out with WriteTimeout/CAS: 504, no
        // re-run (D3 sub-decision 4).
        let tenant = tenant_context("r2-timeout");
        let before = backend.cas_stats();
        backend.session.inject_fault(
            StatementKind::Cas,
            "INSERT INTO resources",
            Fault::TimeOutWithoutApplying,
        );
        backend.session.inject_fault(
            StatementKind::SerialRead,
            "FROM resources",
            Fault::FailWithCasTimeout,
        );
        match backend.create(&tenant, "Patient", patient(), version).await {
            Err(StorageError::Backend(BackendError::Timeout { .. })) => {}
            other => panic!("expected 504, got {other:?}"),
        }
        let stats = backend.cas_stats().since(&before);
        assert_eq!(
            (stats.attempts, stats.reruns, stats.serial_read_failures),
            (1, 0, 1)
        );

        // Attempt 1 applies late, after R2 said "no row" and before the
        // re-run's C1: the re-run misses on attempt 1's row, which is ours.
        let tenant = tenant_context("late-c1");
        let before = backend.cas_stats();
        backend.session.inject_fault(
            StatementKind::Cas,
            "INSERT INTO resources",
            Fault::ApplyLate {
                before: StatementKind::Cas,
            },
        );
        let created = backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .unwrap();
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=late-c1 {stats}]");
        assert_eq!(
            (stats.attempts, stats.applied, stats.applied_late),
            (2, 1, 1)
        );
        assert_eq!(stats.miss_already_exists, 0, "not a 409 against itself");
        let row = current(&backend, &tenant).await;
        assert_eq!(
            row.last_updated,
            created.last_modified(),
            "attempt 1's values"
        );
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 1);
    }

    /// ADR-aacruzgon-0051 on C2: attempt 1 times out, R2 shows version 1,
    /// attempt 1 then applies late, and the re-run's C2 misses on it. The
    /// miss is followed by R2 and matched against attempt 1: success, with
    /// attempt 1's values, not a version conflict.
    #[tokio::test]
    async fn a_late_earlier_attempt_behind_a_c2_miss_is_our_success() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = tenant_context("late-c2");
        backend
            .create(
                &tenant,
                "Patient",
                patient(),
                FhirVersion::default_enabled(),
            )
            .await
            .unwrap();
        let row1 = current(&backend, &tenant).await;
        let (json, data) = normalized_body("update", "Patient", "p", patient()).unwrap();
        backend.session.inject_fault(
            StatementKind::Cas,
            "UPDATE resources",
            Fault::ApplyLate {
                before: StatementKind::Cas,
            },
        );
        let before = backend.cas_stats();
        let mut request = backend.cas_request("update", key_of(&tenant));
        let applied = applied_of(
            backend
                .run_cas(
                    &mut request,
                    CasPlan::update(
                        PreviousVersion::from_row(&row1).unwrap(),
                        json,
                        data,
                        vec![IndexKey::ResourcesByType],
                    ),
                )
                .await
                .unwrap(),
        );
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=late-c2 {stats}]");
        assert_eq!(request.attempts(), 2);
        assert_eq!(applied.write_id, request.attempts[0].write_id, "attempt 1");
        assert_eq!(
            (
                stats.applied,
                stats.applied_late,
                stats.miss_version_conflict
            ),
            (1, 1, 0)
        );
        assert_eq!(
            stats.serial_reads, 2,
            "after the timeout, and after the miss"
        );
        let row2 = current(&backend, &tenant).await;
        assert_eq!(
            (row2.version_id, row2.write_id),
            (2, Some(applied.write_id))
        );
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 1);

        // Once applied, attempt 1 is settled: a later look neither finds it
        // again nor writes its rows twice.
        let before = backend.cas_stats();
        assert!(
            backend
                .check_earlier_attempts(&mut request, Some(&row2))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(backend.cas_stats().since(&before).applied, 0);
    }

    /// The budgets and the statuses an unknown outcome ends in, through the
    /// fault seam: re-runs exhausted after timeouts is 504, after replica
    /// failures 503; `Unavailable` is not applied and never retried.
    #[tokio::test]
    async fn unknown_outcomes_end_in_504_or_503_and_unavailable_is_not_retried() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let version = FhirVersion::default_enabled();
        let c1 = "INSERT INTO resources";

        // Three timeouts, each "unchanged": two re-runs, then 504.
        let tenant = tenant_context("reruns-504");
        for _ in 0..3 {
            backend
                .session
                .inject_fault(StatementKind::Cas, c1, Fault::TimeOutWithoutApplying);
        }
        let before = backend.cas_stats();
        match backend.create(&tenant, "Patient", patient(), version).await {
            Err(StorageError::Backend(BackendError::Timeout { .. })) => {}
            other => panic!("expected 504, got {other:?}"),
        }
        let stats = backend.cas_stats().since(&before);
        assert_eq!((stats.attempts, stats.reruns, stats.undecidable), (3, 2, 1));
        assert!(
            backend
                .read(&tenant, "Patient", "p")
                .await
                .unwrap()
                .is_none()
        );

        // Three replica failures: the same budget, then 503.
        let tenant = tenant_context("reruns-503");
        for _ in 0..3 {
            backend
                .session
                .inject_fault(StatementKind::Cas, c1, Fault::FailWithWriteFailure);
        }
        let before = backend.cas_stats();
        match backend.create(&tenant, "Patient", patient(), version).await {
            Err(StorageError::Backend(BackendError::Unavailable { .. })) => {}
            other => panic!("expected 503, got {other:?}"),
        }
        let stats = backend.cas_stats().since(&before);
        assert_eq!((stats.attempts, stats.unknown_write_failure), (3, 3));

        // Unavailable: refused before anything, 503 at once, no serial read
        // and no retry (ADR-aacruzgon-0050).
        let tenant = tenant_context("unavailable");
        backend
            .session
            .inject_fault(StatementKind::Cas, c1, Fault::FailWithUnavailable);
        let before = backend.cas_stats();
        match backend.create(&tenant, "Patient", patient(), version).await {
            Err(StorageError::Backend(BackendError::Unavailable { .. })) => {}
            other => panic!("expected 503, got {other:?}"),
        }
        let stats = backend.cas_stats().since(&before);
        assert_eq!(
            (
                stats.attempts,
                stats.not_applied,
                stats.serial_reads,
                stats.reruns
            ),
            (1, 1, 0, 0)
        );
    }

    /// G23 on every operation (ADR-aacruzgon-0034, 0050): the CAS applied,
    /// the version row fails all three tries, and the write still succeeds,
    /// with the intent row left for the sweeper (no I2).
    #[tokio::test]
    async fn a_post_cas_failure_still_succeeds_and_leaves_the_intent() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = tenant_context("post-cas");
        for _ in 0..POST_CAS_WRITE_TRIES {
            backend.session.inject_fault(
                StatementKind::Write,
                "INSERT INTO resource_versions",
                Fault::TimeOutWithoutApplying,
            );
        }
        let (before, counted) = (backend.cas_stats(), backend.statement_counts());
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient(),
                FhirVersion::default_enabled(),
            )
            .await
            .expect("success after an applied CAS");
        let stats = backend.cas_stats().since(&before);
        let spent = backend.statement_counts().since(&counted);
        assert_eq!(created.version_id(), "1");
        assert_eq!((stats.applied, stats.post_cas_failures), (1, 1));
        // I1, three tries of V1, X1; no I2.
        assert_eq!((spent.write, spent.cas), (5, 1), "{spent}");
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 1);
    }

    /// An update whose new `resources_by_type` row fails all three tries
    /// keeps the old row: its delete is never sent, so the live resource
    /// stays counted, and the write succeeds with I1 left for the sweeper
    /// (G23). Reproduced first by W3 (L2b): before this, the delete still ran
    /// and the id vanished from `resources_by_type` at version 2.
    #[tokio::test]
    async fn a_failed_index_insert_keeps_the_old_row_and_the_intent() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = tenant_context("index-insert");
        backend
            .create(
                &tenant,
                "Patient",
                patient(),
                FhirVersion::default_enabled(),
            )
            .await
            .unwrap();
        let row1 = current(&backend, &tenant).await;
        let (json, data) = normalized_body("update", "Patient", "p", patient()).unwrap();
        let plan = CasPlan::update(
            PreviousVersion::from_row(&row1).unwrap(),
            json,
            data,
            vec![IndexKey::ResourcesByType],
        );
        for _ in 0..POST_CAS_WRITE_TRIES {
            backend.session.inject_fault(
                StatementKind::Write,
                "INSERT INTO resources_by_type",
                Fault::TimeOutWithoutApplying,
            );
        }
        let (before, counted) = (backend.cas_stats(), backend.statement_counts());
        let mut request = backend.cas_request("update", key_of(&tenant));
        let applied = applied_of(backend.run_cas(&mut request, plan).await.unwrap());
        assert_eq!(applied.version_id, 2);
        assert!(!applied.post_cas_complete && !applied.superseded);
        let stats = backend.cas_stats().since(&before);
        let spent = backend.statement_counts().since(&counted);
        assert_eq!((stats.applied, stats.post_cas_failures), (1, 1));
        // V3, I1, V1 and three tries of X1's insert; no X1 delete, no I2.
        assert_eq!((spent.write, spent.cas), (6, 1), "{spent}");

        // The old row is still there, at version 1's `last_updated`.
        let by_type = backend
            .session
            .rows(
                "SELECT last_updated, id, version_id FROM resources_by_type \
                 WHERE tenant_id = ? AND resource_type = ?",
                vec![
                    Value::from(tenant.tenant_id().as_str()),
                    Value::from("Patient"),
                ],
                StatementKind::Read,
            )
            .await
            .expect("resources_by_type");
        let by_type: Vec<(DateTime<Utc>, String, i32)> = by_type
            .iter()
            .map(|row| {
                (
                    row.get_r_by_name("last_updated").unwrap(),
                    row.get_r_by_name("id").unwrap(),
                    row.get_r_by_name("version_id").unwrap(),
                )
            })
            .collect();
        assert_eq!(by_type, vec![(row1.last_updated, "p".to_string(), 1)]);
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 1);

        // I1 is left for the sweeper.
        let intent = &request.attempts[0].intent;
        let intents = backend
            .session
            .rows(
                "SELECT version_id FROM pending_index_writes \
                 WHERE bucket = ? AND shard = ? AND enqueued_at = ? \
                 AND tenant_id = ? AND resource_type = ? AND id = ?",
                vec![
                    Value::from(intent.bucket.as_str()),
                    Value::from(intent.shard),
                    Value::from(intent.enqueued_at),
                    Value::from(tenant.tenant_id().as_str()),
                    Value::from("Patient"),
                    Value::from("p"),
                ],
                StatementKind::Read,
            )
            .await
            .expect("pending_index_writes");
        let versions: Vec<i32> = intents
            .iter()
            .map(|row| row.get_r_by_name("version_id").unwrap())
            .collect();
        assert_eq!(versions, vec![2]);
    }

    /// A superseded attempt, found by the pre-read check: the row is past
    /// the attempt's target, and row `V` (R3) carries its `write_id`. The
    /// write is ours, with steps 4 and 5 skipped and the intent left.
    #[tokio::test]
    async fn a_superseded_earlier_attempt_is_ours_with_its_rows_left_to_the_sweeper() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = tenant_context("superseded");
        backend
            .create(
                &tenant,
                "Patient",
                patient(),
                FhirVersion::default_enabled(),
            )
            .await
            .unwrap();
        let (json, data) = normalized_body("update", "Patient", "p", patient()).unwrap();
        let update = |row: &ResourceRow| {
            CasPlan::update(
                PreviousVersion::from_row(row).unwrap(),
                json.clone(),
                data.clone(),
                vec![IndexKey::ResourcesByType],
            )
        };
        // Version 2, written by the engine.
        let row1 = current(&backend, &tenant).await;
        let mut first = backend.cas_request("update", key_of(&tenant));
        let v2 = applied_of(backend.run_cas(&mut first, update(&row1)).await.unwrap());
        // Version 3 by another request; its V3 rewrites row 2 with w(2).
        let row2 = current(&backend, &tenant).await;
        let mut other = backend.cas_request("update", key_of(&tenant));
        applied_of(backend.run_cas(&mut other, update(&row2)).await.unwrap());
        let row3 = current(&backend, &tenant).await;

        // A request that sent version 2's CAS and never heard back.
        let mut request = backend.cas_request("update", key_of(&tenant));
        request.attempts.push(Attempt {
            write_id: v2.write_id,
            last_updated: v2.last_updated,
            intent: IntentKey::new(tenant.tenant_id().as_str(), "p", Utc::now()),
            plan: Arc::new(update(&row1)),
            unknown: Some(true),
            settled: false,
        });
        let before = backend.cas_stats();
        let found = backend
            .check_earlier_attempts(&mut request, Some(&row3))
            .await
            .unwrap()
            .expect("ours, superseded");
        assert_eq!((found.version_id, found.write_id), (2, v2.write_id));
        assert!(found.superseded && !found.post_cas_complete);
        let stats = backend.cas_stats().since(&before);
        assert_eq!((stats.version_reads, stats.applied_superseded), (1, 1));
    }

    /// An earlier attempt whose row cannot be decided answers by that
    /// attempt's trigger (ADR-aacruzgon-0051): 503 after a replica failure,
    /// 504 after a timeout. No statement is needed for a row at the target
    /// with no readable `write_id`.
    #[tokio::test]
    async fn an_undecidable_earlier_attempt_answers_by_its_own_trigger() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let key = RowKey {
            tenant_id: "s3-engine-undecidable",
            resource_type: "Patient",
            id: "p",
        };
        let row = Some(CurrentRow {
            version_id: Some(1),
            write_id: None,
            deleted: false,
        });
        for (timed_out, want_timeout) in [(false, false), (true, true)] {
            let mut request = backend.cas_request("create", key);
            let mut attempt = tried(Uuid::new_v4());
            attempt.unknown = Some(timed_out);
            request.attempts.push(attempt);
            match backend.late_attempt(&mut request, row, None).await {
                Err(StorageError::Backend(BackendError::Timeout { .. })) if want_timeout => {}
                Err(StorageError::Backend(BackendError::Unavailable { .. })) if !want_timeout => {}
                other => panic!("timed_out={timed_out}: {other:?}"),
            }
        }
    }

    /// D14, measured on the node: what Cassandra answers to a C1 and to a
    /// plain V1 at 9 MiB and 17 MiB, against the backend's own 8 MiB bound,
    /// which refuses before writing anything.
    #[tokio::test]
    async fn the_size_bound_refuses_before_the_node_would() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = tenant_context("size");
        let big = |mib: usize| {
            serde_json::json!({
                "resourceType": "Patient",
                "id": "p",
                "text": {"status": "generated", "div": "x".repeat(mib * 1024 * 1024)}
            })
        };
        let before = backend.statement_counts();
        match backend
            .create(&tenant, "Patient", big(9), FhirVersion::default_enabled())
            .await
        {
            Err(StorageError::Validation(ValidationError::InvalidResource { .. })) => {}
            other => panic!("expected 400, got {other:?}"),
        }
        assert_eq!(
            backend.statement_counts().since(&before).total(),
            0,
            "refused before any statement"
        );

        // The node itself, past the bound: measured once for the ADR, and
        // only when asked, so the required job does not write 9 MiB rows on
        // every run.
        if std::env::var("HFS_TEST_CASSANDRA_SIZE_PROBE")
            .ok()
            .as_deref()
            != Some("1")
        {
            eprintln!("not run: the 9 and 17 MiB probe (set HFS_TEST_CASSANDRA_SIZE_PROBE=1)");
            return;
        }
        for mib in [9, 17] {
            let t = format!("{}-{mib}", tenant.tenant_id().as_str());
            let key = RowKey {
                tenant_id: &t,
                resource_type: "Patient",
                id: "p",
            };
            let plan = CasPlan::create(
                Json::Null,
                serde_json::to_string(&big(mib)).unwrap(),
                FhirVersion::default_enabled(),
                vec![IndexKey::ResourcesByType],
            );
            let lu = now_millis();
            let cas = backend
                .session
                .rows(
                    INSERT_RESOURCE_IF_NOT_EXISTS,
                    plan.cas_values(&key, Uuid::new_v4(), lu),
                    StatementKind::Cas,
                )
                .await;
            let after_cas = backend.read_resource_row(&t, "Patient", "p").await;
            let (cql, values) = plan.new_version_row(&key, Uuid::new_v4(), lu);
            let plain = backend
                .session
                .execute(cql, values, StatementKind::Write)
                .await;
            eprintln!(
                "[size mib={mib} cas={} row_after_cas={} plain_v1={}]",
                match &cas {
                    Ok(rows) => format!("applied={:?}", applied(rows)),
                    Err(error) => format!("{error:?}"),
                },
                match &after_cas {
                    Ok(row) => format!("{}", row.is_some()),
                    Err(error) => format!("{error:?}"),
                },
                match &plain {
                    Ok(_) => "ok".to_string(),
                    Err(error) => format!("{error:?}"),
                }
            );
        }
    }

    /// Fresh `test-` tenants for the hardening tests (ADR-aacruzgon-0020).
    fn hardening_tenant(case: &str) -> TenantContext {
        TenantContext::new(
            TenantId::new(format!(
                "test-s3-hardening-{case}-{}",
                Uuid::new_v4().simple()
            )),
            TenantPermissions::full_access(),
        )
    }

    /// Another request's C3 of the current version, sent now but held back
    /// by the fault seam until just before the next statement of `before`:
    /// a delete that lands in a chosen gap of the request that follows.
    async fn hold_back_delete(
        backend: &CassandraBackend,
        tenant: &TenantContext,
        before: StatementKind,
    ) {
        let row = current(backend, tenant).await;
        let plan = CasPlan::delete(PreviousVersion::from_row(&row).unwrap());
        backend.session.inject_fault(
            StatementKind::Cas,
            "deleted_at = ?, last_updated",
            Fault::ApplyLate { before },
        );
        let lu = next_last_updated(Utc::now(), Some(row.last_updated));
        let held = backend
            .session
            .rows(
                SOFT_DELETE_RESOURCE_IF_VERSION,
                plan.cas_values(&key_of(tenant), Uuid::new_v4(), lu),
                StatementKind::Cas,
            )
            .await;
        assert!(held.is_err(), "the other request's delete is held back");
    }

    /// Another request deletes v1 between our pre-read and our CAS's
    /// decision. That is `NotFound` (404) on every path, as walkthrough 3.3
    /// and 3.4 and the design's step 3 say ("deleted in the gap", G9):
    /// (a) and (c) when our C2 or C3 misses outright, and (b) and (d) when
    /// its outcome was unknown and R2 shows the delete. In every case the
    /// row ends deleted at v2, by the other request.
    #[tokio::test]
    async fn a_delete_in_the_gap_is_not_found_whether_or_not_our_outcome_was_unknown() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let version = FhirVersion::default_enabled();
        let not_found = |r: &StorageResult<StoredResource>| {
            matches!(
                r,
                Err(StorageError::Resource(ResourceError::NotFound { .. }))
            )
        };
        let not_found_unit = |r: &StorageResult<()>| {
            matches!(
                r,
                Err(StorageError::Resource(ResourceError::NotFound { .. }))
            )
        };

        // (a) update: the delete lands just before our C2, which misses.
        let tenant = hardening_tenant("gap-update-c2");
        let stored = backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .unwrap();
        hold_back_delete(&backend, &tenant, StatementKind::Cas).await;
        let a = backend.update(&tenant, &stored, patient()).await;
        let row = current(&backend, &tenant).await;
        assert!(row.is_deleted() && row.version_id == 2, "(a) row {row:?}");
        assert!(not_found(&a), "(a) a definite miss: {a:?}");

        // (b) update: our C2 times out without applying, and the same delete
        // lands before R2.
        let tenant = hardening_tenant("gap-update-r2");
        let stored = backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .unwrap();
        hold_back_delete(&backend, &tenant, StatementKind::SerialRead).await;
        backend.session.inject_fault(
            StatementKind::Cas,
            "SET version_id = ?, data = ?",
            Fault::TimeOutWithoutApplying,
        );
        let before = backend.cas_stats();
        let b = backend.update(&tenant, &stored, patient()).await;
        let stats = backend.cas_stats().since(&before);
        let row = current(&backend, &tenant).await;
        assert!(row.is_deleted() && row.version_id == 2, "(b) row {row:?}");
        eprintln!("[gap case=update-r2 result={b:?} stats={stats}]");
        assert!(not_found(&b), "(b) after an unknown outcome: {b:?}");

        // (c) delete_versioned: the delete lands just before our C3.
        let tenant = hardening_tenant("gap-delete-c3");
        backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .unwrap();
        hold_back_delete(&backend, &tenant, StatementKind::Cas).await;
        let c = backend.delete_versioned(&tenant, "Patient", "p", "1").await;
        assert!(not_found_unit(&c), "(c) a definite miss: {c:?}");

        // (d) delete_versioned: our C3 times out without applying, and the
        // same delete lands before R2.
        let tenant = hardening_tenant("gap-delete-r2");
        backend
            .create(&tenant, "Patient", patient(), version)
            .await
            .unwrap();
        hold_back_delete(&backend, &tenant, StatementKind::SerialRead).await;
        backend.session.inject_fault(
            StatementKind::Cas,
            "deleted_at = ?, last_updated",
            Fault::TimeOutWithoutApplying,
        );
        let d = backend.delete_versioned(&tenant, "Patient", "p", "1").await;
        let row = current(&backend, &tenant).await;
        assert!(row.is_deleted() && row.version_id == 2, "(d) row {row:?}");
        assert!(not_found_unit(&d), "(d) after an unknown outcome: {d:?}");
    }

    /// After an unknown outcome, the write may be stored: an error of any
    /// later statement must answer 503 or 504, never 500 (ADR-aacruzgon-0067).
    fn is_503_or_504<T: std::fmt::Debug>(result: &StorageResult<T>) -> bool {
        matches!(
            result,
            Err(StorageError::Backend(
                BackendError::Unavailable { .. } | BackendError::Timeout { .. }
            ))
        )
    }

    /// Attempt 1's C1 is reported as timed out and applies late, after R2
    /// said "no row"; the re-run's I1 then fails with a node-side `Server`
    /// error. The resource is stored, by attempt 1, so the request answers
    /// 503, not the plain table's 500.
    #[tokio::test]
    async fn a_reruns_intent_error_after_an_unknown_outcome_is_503() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = hardening_tenant("rerun-i1");
        let intent = "INSERT INTO pending_index_writes";
        backend
            .session
            .inject_fault(StatementKind::Write, intent, Fault::PassThrough);
        backend
            .session
            .inject_fault(StatementKind::Write, intent, Fault::FailWithServerError);
        backend.session.inject_fault(
            StatementKind::Cas,
            "INSERT INTO resources",
            Fault::ApplyLate {
                before: StatementKind::Write,
            },
        );
        let before = backend.cas_stats();
        let result = backend
            .create(
                &tenant,
                "Patient",
                patient(),
                FhirVersion::default_enabled(),
            )
            .await;
        let stats = backend.cas_stats().since(&before);
        let row = backend
            .read_resource_row(tenant.tenant_id().as_str(), "Patient", "p")
            .await
            .unwrap();
        eprintln!("[e2 case=rerun-i1 result={result:?} stats={stats}]");
        assert_eq!(stats.reruns, 1, "{stats}");
        assert_eq!(row.map(|r| r.version_id), Some(1), "attempt 1 stored it");
        assert!(is_503_or_504(&result), "{result:?}");
    }

    /// An update's C2 times out without applying, R2 shows the row
    /// unchanged, and the re-run's V3 fails with a `Server` error. Attempt 1
    /// may still apply (it did not here), so the request answers 503.
    #[tokio::test]
    async fn a_reruns_previous_version_row_error_after_an_unknown_outcome_is_503() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = hardening_tenant("rerun-v3");
        let stored = backend
            .create(
                &tenant,
                "Patient",
                patient(),
                FhirVersion::default_enabled(),
            )
            .await
            .unwrap();
        let v3 = "INSERT INTO resource_versions";
        backend
            .session
            .inject_fault(StatementKind::Write, v3, Fault::PassThrough);
        backend
            .session
            .inject_fault(StatementKind::Write, v3, Fault::FailWithServerError);
        backend.session.inject_fault(
            StatementKind::Cas,
            "SET version_id = ?, data = ?",
            Fault::TimeOutWithoutApplying,
        );
        let before = backend.cas_stats();
        let result = backend.update(&tenant, &stored, patient()).await;
        let stats = backend.cas_stats().since(&before);
        eprintln!("[e2 case=rerun-v3 result={result:?} stats={stats}]");
        assert_eq!(stats.reruns, 1, "{stats}");
        assert_eq!(current(&backend, &tenant).await.version_id, 1);
        assert!(is_503_or_504(&result), "{result:?}");
    }

    /// A plain delete's C3 times out without applying, and R2 shows another
    /// request's update in the gap, so the delete takes its retry; that
    /// turn's R1 fails with a `Server` error. The first attempt's outcome was
    /// unknown, so the request answers 503.
    #[tokio::test]
    async fn a_later_turns_pre_read_error_after_an_unknown_outcome_is_503() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = hardening_tenant("later-r1");
        backend
            .create(
                &tenant,
                "Patient",
                patient(),
                FhirVersion::default_enabled(),
            )
            .await
            .unwrap();
        // Another request's C2 of v1, held back until just before our R2.
        let row = current(&backend, &tenant).await;
        let (body, data) = normalized_body("update", "Patient", "p", patient()).unwrap();
        let plan = CasPlan::update(
            PreviousVersion::from_row(&row).unwrap(),
            body,
            data,
            vec![IndexKey::ResourcesByType],
        );
        backend.session.inject_fault(
            StatementKind::Cas,
            "SET version_id = ?, data = ?",
            Fault::ApplyLate {
                before: StatementKind::SerialRead,
            },
        );
        let lu = next_last_updated(Utc::now(), Some(row.last_updated));
        let held = backend
            .session
            .rows(
                plan.kind.cql(),
                plan.cas_values(&key_of(&tenant), Uuid::new_v4(), lu),
                StatementKind::Cas,
            )
            .await;
        assert!(held.is_err(), "the other request's update is held back");
        backend.session.inject_fault(
            StatementKind::Cas,
            "deleted_at = ?, last_updated",
            Fault::TimeOutWithoutApplying,
        );
        let r1 = "SELECT version_id, data, last_updated";
        backend
            .session
            .inject_fault(StatementKind::Read, r1, Fault::PassThrough);
        backend
            .session
            .inject_fault(StatementKind::Read, r1, Fault::FailWithServerError);
        let before = backend.cas_stats();
        let result = backend.delete(&tenant, "Patient", "p").await;
        let stats = backend.cas_stats().since(&before);
        eprintln!("[e2 case=later-r1 result={result:?} stats={stats}]");
        assert_eq!(stats.miss_retries, 1, "the delete took its retry: {stats}");
        assert_eq!(current(&backend, &tenant).await.version_id, 2);
        assert!(is_503_or_504(&result), "{result:?}");
    }

    /// Attempt 1's C1 is reported as timed out and applies late, after R2
    /// said "no row"; the re-run's own C1 is then refused before it runs,
    /// with a node-side `Protocol` error, which the engine takes as not
    /// applied. The resource is stored, by attempt 1, so the request answers
    /// 503, not the plain table's 500 (ADR-aacruzgon-0067).
    #[tokio::test]
    async fn a_reruns_cas_refused_before_it_runs_after_an_unknown_outcome_is_503() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = hardening_tenant("rerun-cas-refused");
        backend.session.inject_fault(
            StatementKind::Cas,
            "INSERT INTO resources",
            Fault::ApplyLate {
                before: StatementKind::Cas,
            },
        );
        backend.session.inject_fault(
            StatementKind::Cas,
            "INSERT INTO resources",
            Fault::FailWithProtocolError,
        );
        let before = backend.cas_stats();
        let result = backend
            .create(
                &tenant,
                "Patient",
                patient(),
                FhirVersion::default_enabled(),
            )
            .await;
        let stats = backend.cas_stats().since(&before);
        let row = backend
            .read_resource_row(tenant.tenant_id().as_str(), "Patient", "p")
            .await
            .unwrap();
        eprintln!("[e2 case=rerun-cas-refused result={result:?} stats={stats}]");
        assert_eq!((stats.reruns, stats.not_applied), (1, 1), "{stats}");
        assert_eq!(row.map(|r| r.version_id), Some(1), "attempt 1 stored it");
        assert!(
            matches!(
                result,
                Err(StorageError::Backend(BackendError::Unavailable { .. }))
            ),
            "{result:?}"
        );
    }
}
