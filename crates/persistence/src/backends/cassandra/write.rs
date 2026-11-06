//! The write path's building blocks, and `create`.
//!
//! The six steps of every write (`cassandra-discussion.md`, "Write path";
//! `docs/Cassandra/schema/walkthrough.md` 3.1), as `create` runs them
//! (walkthrough 3.2):
//!
//! 1. `tenant.check_permission(Operation::Create, type)`; for a
//!    SearchParameter, the 128-byte bound on `code` (G38); `K(1)`.
//! 2. I1, the intent row, at `LOCAL_QUORUM`, carrying this attempt's
//!    `write_id` and version 1. A create has no V3 (no version before it).
//! 3. C1, `INSERT … IF NOT EXISTS` with `deleted_at = 0`, at `LOCAL_SERIAL` /
//!    `LOCAL_QUORUM`, never re-sent by the driver.
//! 4. V1, the `resource_versions` row for version 1, `USING TIMESTAMP ts(1)`.
//! 5. The index rows of `K(1)`, `USING TIMESTAMP ts(1)`.
//! 6. I2, the intent delete.
//!
//! **What `K(1)` holds in Step 2.** Only the `resources_by_type` row (X1):
//! the orchestrator pulled it forward from Step 5 so that `count` works and
//! the conformance seeder can run at boot (ADR draft "create writes
//! `resources_by_type`"). The token, date and URI rows of
//! `SearchParameterExtractor::extract` join `K(1)` in Step 5; nothing here
//! changes then except the key set, because the CAS, V1 and the index step
//! already carry whatever `K(1)` is, and the stored blob already uses the
//! final format.
//!
//! **Failure handling** (design, "When the LWT outcome is unknown";
//! ADR-aacruzgon-0024, 0026; walkthrough 3.1, 3.2):
//!
//! - I1 fails: its error (503 or 504); nothing changed.
//! - C1 not applied: the row exists, so `AlreadyExists` (409); this attempt's
//!   intent row is deleted.
//! - C1's outcome unknown (`WriteTimeout`/`WriteFailure` with `write_type =
//!   CAS`, a dropped connection, the client deadline after sending,
//!   `CAS_WRITE_UNKNOWN`): R2 at `LOCAL_SERIAL` decides, per the
//!   walkthrough's table. "No row" re-runs the create, same id, after a 5 to
//!   25 ms pause, at most twice. A `WriteTimeout` with `write_type = SIMPLE`
//!   means the CAS was decided and only its commit timed out: applied.
//! - Steps 4 and 5 fail: retried with the same timestamps; when the retries
//!   run out the create still succeeds and its intent row is left for the
//!   sweeper (G23), because a 5xx after an applied CAS would make a client's
//!   retried POST create a second resource.
//! - Step 6 fails: ignored; the sweeper deletes the intent.

use std::time::Duration;

use cdrs_tokio::error::Error as CdrsError;
use cdrs_tokio::frame::message_error::{ErrorType, WriteType};
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
use super::session::{DriverError, StatementKind};
use crate::error::{
    BackendError, ResourceError, StorageError, StorageResult, ValidationDetail, ValidationError,
    ValidationSeverity,
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
const INSERT_RESOURCE_IF_NOT_EXISTS: &str = "INSERT INTO resources \
     (tenant_id, resource_type, id, version_id, data, last_updated, created_at, \
      fhir_version, write_ts, write_id, index_keys, deleted_at) \
     VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 0) IF NOT EXISTS";

/// R2: the current row at `LOCAL_SERIAL`, which completes or cancels an
/// in-flight proposal before answering. Only what an unknown outcome needs.
const SELECT_RESOURCE_SERIAL: &str = "SELECT version_id, write_id FROM resources \
     WHERE tenant_id = ? AND resource_type = ? AND id = ?";

/// R3, reduced to what a superseded unknown outcome needs (G36).
const SELECT_VERSION_WRITE_ID: &str = "SELECT write_id FROM resource_versions \
     WHERE tenant_id = ? AND resource_type = ? AND id = ? AND version_id = ?";

/// V1: a content version row, `USING TIMESTAMP ts(V)`.
const INSERT_VERSION: &str = "INSERT INTO resource_versions \
     (tenant_id, resource_type, id, version_id, data, last_updated, fhir_version, \
      write_ts, write_id, index_keys) \
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";

/// X1: the `resources_by_type` row, `USING TIMESTAMP ts(V)` (G34: it carries
/// the resource's `fhir_version`).
const INSERT_RESOURCES_BY_TYPE: &str = "INSERT INTO resources_by_type \
     (tenant_id, resource_type, last_updated, id, version_id, fhir_version) \
     VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";

/// Re-runs after an unknown outcome that the serial read proved did not
/// apply (ADR-aacruzgon-0024): separate from the 3 retries after definite
/// misses, which a create does not have.
const UNKNOWN_OUTCOME_RERUNS: usize = 2;
/// Tries of each plain write after the CAS applied (steps 4 and 5).
const POST_CAS_WRITE_TRIES: u32 = 3;
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
/// precision, and `write_ts` is derived from it (ADR-aacruzgon-0016).
fn now_millis() -> DateTime<Utc> {
    let now = Utc::now();
    now.with_nanosecond(now.nanosecond() / 1_000_000 * 1_000_000)
        .unwrap_or(now)
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
    /// deadline; 503 after replicas failed.
    Unknown { timed_out: bool, error: DriverError },
    /// `WriteTimeout` with `write_type = SIMPLE`: decided, only the commit
    /// timed out.
    Applied,
    /// It was never sent, or the node refused it before acting.
    NotApplied(DriverError),
}

/// Classifies a CAS failure before anything maps it (`errors.rs`: a CAS must
/// not go through the generic table).
fn classify_cas_failure(error: DriverError) -> CasFailure {
    let unknown_timeout = |error| CasFailure::Unknown {
        timed_out: true,
        error,
    };
    match &error {
        DriverError::Deadline { sent: true, .. } => unknown_timeout(error),
        DriverError::Deadline { sent: false, .. } => CasFailure::NotApplied(error),
        DriverError::Driver(inner) => match inner.as_ref() {
            CdrsError::Server { body, .. } => match &body.ty {
                ErrorType::WriteTimeout(timeout) if timeout.write_type == WriteType::Simple => {
                    CasFailure::Applied
                }
                ErrorType::WriteTimeout(_) | ErrorType::ReadTimeout(_) => unknown_timeout(error),
                ErrorType::WriteFailure(_) | ErrorType::ReadFailure(_) => CasFailure::Unknown {
                    timed_out: false,
                    error,
                },
                // Unavailable, Overloaded, IsBootstrapping, Syntax, …: the
                // coordinator refused before it acted.
                _ => CasFailure::NotApplied(error),
            },
            // Raised only while connecting (`connection_pool.rs`), so before
            // anything was sent.
            CdrsError::Timeout(_) => CasFailure::NotApplied(error),
            CdrsError::General(message)
                if message.contains("No nodes available")
                    || message.contains("No active connections") =>
            {
                CasFailure::NotApplied(error)
            }
            // A closed connection, `Io`, `CAS_WRITE_UNKNOWN` (0x1700, which
            // cassandra-protocol 3.3.0 cannot decode), and anything else after
            // the request may have left: unknown.
            _ => unknown_timeout(error),
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

/// What R2 (and, when needed, R3) says about a create attempt.
#[derive(Debug, PartialEq, Eq)]
enum CreateOutcome {
    /// Our write_id is version 1.
    Applied,
    /// Ours was version 1 and a later version superseded it.
    AppliedSuperseded,
    /// No row: ours did not apply and never will.
    NotApplied,
    /// Another writer created it.
    AnotherWriter,
    /// Nothing tells the two apart.
    Undecidable,
}

/// Reads the `[applied]` column of a CAS result.
fn applied(rows: &[Row]) -> Result<bool, StorageError> {
    let row = rows.first().ok_or_else(|| {
        StorageError::Backend(BackendError::Internal {
            backend_name: BACKEND_NAME.to_string(),
            message: "compare-and-swap returned no [applied] row".to_string(),
            source: None,
        })
    })?;
    IntoRustByName::<bool>::get_r_by_name(row, "[applied]").map_err(|error| {
        StorageError::Backend(BackendError::Internal {
            backend_name: BACKEND_NAME.to_string(),
            message: format!("unreadable [applied]: {error}"),
            source: None,
        })
    })
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
fn check_id(id: &str) -> StorageResult<()> {
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

/// One create attempt's fixed values.
struct CreateAttempt<'a> {
    tenant_id: &'a str,
    resource_type: &'a str,
    id: &'a str,
    data: &'a str,
    fhir_version: &'static str,
    last_updated: DateTime<Utc>,
    keys_blob: &'a [u8],
}

impl CassandraBackend {
    /// `ResourceStorage::create`; see the module docs.
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
        let mut resource = resource;
        if let Some(object) = resource.as_object_mut() {
            object.insert(
                "resourceType".to_string(),
                Json::String(resource_type.to_string()),
            );
            object.insert("id".to_string(), Json::String(id.clone()));
        }
        let data = serde_json::to_string(&resource)
            .map_err(|error| internal(format!("create: serialize resource: {error}")))?;

        let keys = vec![IndexKey::ResourcesByType];
        let keys_blob = index_keys::encode(&keys);
        let tenant_id = tenant.tenant_id().as_str();

        let mut reruns = 0;
        loop {
            let last_updated = now_millis();
            let attempt = CreateAttempt {
                tenant_id,
                resource_type,
                id: &id,
                data: &data,
                fhir_version: fhir_version.as_mime_param(),
                last_updated,
                keys_blob: &keys_blob,
            };
            let write_id = Uuid::new_v4();
            let intent = IntentKey::new(tenant_id, &id, Utc::now());

            // Step 2.
            self.write_intent(&intent, tenant_id, resource_type, &id, write_id, 1)
                .await?;

            // Step 3.
            let outcome = match self
                .session
                .rows(
                    INSERT_RESOURCE_IF_NOT_EXISTS,
                    attempt.c1_values(write_id),
                    StatementKind::Cas,
                )
                .await
            {
                Ok(rows) => {
                    if applied(&rows)? {
                        CreateOutcome::Applied
                    } else {
                        // `IF NOT EXISTS` returned the existing row: a
                        // definite miss. This attempt's intent can go.
                        self.delete_intent(&intent, tenant_id, resource_type, &id)
                            .await;
                        return Err(already_exists(resource_type, &id));
                    }
                }
                Err(error) => match classify_cas_failure(error) {
                    CasFailure::Applied => CreateOutcome::Applied,
                    CasFailure::NotApplied(error) => {
                        // Nothing was decided; the intent is harmless and the
                        // sweeper deletes it.
                        return Err(StorageError::Backend(map_driver_error(&error, "create")));
                    }
                    CasFailure::Unknown { timed_out, error } => {
                        tracing::warn!(
                            resource_type,
                            id = %id,
                            error = ?error,
                            "create: compare-and-swap outcome unknown; reading it back at LOCAL_SERIAL"
                        );
                        match self
                            .resolve_unknown_create(tenant_id, resource_type, &id, write_id)
                            .await?
                        {
                            CreateOutcome::NotApplied if reruns < UNKNOWN_OUTCOME_RERUNS => {
                                reruns += 1;
                                tokio::time::sleep(rerun_pause()).await;
                                continue;
                            }
                            CreateOutcome::NotApplied | CreateOutcome::Undecidable => {
                                return Err(undecidable(timed_out, "create"));
                            }
                            CreateOutcome::AnotherWriter => {
                                return Err(already_exists(resource_type, &id));
                            }
                            other => other,
                        }
                    }
                },
            };

            if outcome == CreateOutcome::Applied {
                // Steps 4 to 6.
                let version_written = self.write_version_row(&attempt, write_id).await;
                let indexed = self.write_index_rows(&attempt, &keys).await;
                if version_written && indexed {
                    self.delete_intent(&intent, tenant_id, resource_type, &id)
                        .await;
                } else {
                    tracing::warn!(
                        resource_type,
                        id = %id,
                        "create applied, but its version or index rows could not be written; \
                         the sweeper finishes it from the intent row"
                    );
                }
            }
            // `AppliedSuperseded`: a later version exists, so V1 and X1 are
            // skipped (a late insert of K(1) could outlive the later version's
            // tombstones) and the intent is left for the sweeper.

            if resource_type == "SearchParameter" {
                self.note_stored_search_parameter(tenant_id, &resource);
            }
            return Ok(StoredResource::from_storage(
                resource_type,
                &id,
                "1",
                tenant.tenant_id().clone(),
                resource,
                last_updated,
                last_updated,
                None,
                fhir_version,
            ));
        }
    }

    /// I1.
    async fn write_intent(
        &self,
        intent: &IntentKey,
        tenant_id: &str,
        resource_type: &str,
        id: &str,
        write_id: Uuid,
        version_id: i32,
    ) -> StorageResult<()> {
        self.session
            .execute(
                INSERT_INTENT,
                vec![
                    Value::from(intent.bucket.as_str()),
                    Value::from(intent.shard),
                    Value::from(intent.enqueued_at),
                    Value::from(tenant_id),
                    Value::from(resource_type),
                    Value::from(id),
                    Value::from(write_id),
                    Value::from(version_id),
                ],
                StatementKind::Write,
            )
            .await
            .map(|_| ())
            .map_err(|error| StorageError::Backend(map_driver_error(&error, "intent row")))
    }

    /// I2, best effort: a failure leaves the intent for the sweeper.
    async fn delete_intent(
        &self,
        intent: &IntentKey,
        tenant_id: &str,
        resource_type: &str,
        id: &str,
    ) {
        let result = self
            .session
            .execute(
                DELETE_INTENT,
                vec![
                    Value::from(intent.bucket.as_str()),
                    Value::from(intent.shard),
                    Value::from(intent.enqueued_at),
                    Value::from(tenant_id),
                    Value::from(resource_type),
                    Value::from(id),
                ],
                StatementKind::Write,
            )
            .await;
        if let Err(error) = result {
            tracing::debug!(
                resource_type,
                id,
                error = ?error,
                "intent delete failed; the sweeper deletes it"
            );
        }
    }

    /// R2 then, for a superseded version, R3 (walkthrough 3.1's table).
    async fn resolve_unknown_create(
        &self,
        tenant_id: &str,
        resource_type: &str,
        id: &str,
        write_id: Uuid,
    ) -> StorageResult<CreateOutcome> {
        let key = vec![
            Value::from(tenant_id),
            Value::from(resource_type),
            Value::from(id),
        ];
        let rows = self
            .session
            .rows(
                SELECT_RESOURCE_SERIAL,
                key.clone(),
                StatementKind::SerialRead,
            )
            .await
            .map_err(|error| StorageError::Backend(map_driver_error(&error, "serial read")))?;
        let Some(row) = rows.first() else {
            return Ok(CreateOutcome::NotApplied);
        };
        let version: i32 = row
            .get_r_by_name("version_id")
            .map_err(|error| internal(format!("serial read: version_id: {error}")))?;
        let current_writer: Option<Uuid> = row.get_by_name("write_id").ok().flatten();
        if version == 1 {
            return Ok(if current_writer == Some(write_id) {
                CreateOutcome::Applied
            } else {
                CreateOutcome::AnotherWriter
            });
        }

        let mut values = key;
        values.push(Value::from(1_i32));
        let rows = self
            .session
            .rows(SELECT_VERSION_WRITE_ID, values, StatementKind::Read)
            .await
            .map_err(|error| StorageError::Backend(map_driver_error(&error, "version read")))?;
        let version_one_writer: Option<Uuid> = rows
            .first()
            .and_then(|row| row.get_by_name("write_id").ok().flatten());
        Ok(match version_one_writer {
            Some(writer) if writer == write_id => CreateOutcome::AppliedSuperseded,
            Some(_) => CreateOutcome::AnotherWriter,
            None => CreateOutcome::Undecidable,
        })
    }

    /// Step 4: V1 for the new version. `true` once it is written.
    async fn write_version_row(&self, attempt: &CreateAttempt<'_>, write_id: Uuid) -> bool {
        let write_ts = write_ts_of(attempt.last_updated);
        let values = vec![
            Value::from(attempt.tenant_id),
            Value::from(attempt.resource_type),
            Value::from(attempt.id),
            Value::from(1_i32),
            Value::from(attempt.data),
            Value::from(attempt.last_updated),
            Value::from(attempt.fhir_version),
            Value::from(write_ts),
            Value::from(write_id),
            Value::from(Blob::from(attempt.keys_blob.to_vec())),
            Value::from(write_ts),
        ];
        self.write_with_retries(INSERT_VERSION, values, "version row")
            .await
    }

    /// Step 5: the inserts of `K(1)` (a create deletes nothing). `true` once
    /// every row is written.
    async fn write_index_rows(&self, attempt: &CreateAttempt<'_>, keys: &[IndexKey]) -> bool {
        let write_ts = write_ts_of(attempt.last_updated);
        let mut all = true;
        for key in keys {
            let (cql, values) = match key {
                IndexKey::ResourcesByType => (
                    INSERT_RESOURCES_BY_TYPE,
                    vec![
                        Value::from(attempt.tenant_id),
                        Value::from(attempt.resource_type),
                        Value::from(attempt.last_updated),
                        Value::from(attempt.id),
                        Value::from(1_i32),
                        Value::from(attempt.fhir_version),
                        Value::from(write_ts),
                    ],
                ),
                other => {
                    // Step 5 adds these rows; Step 2's key set never has them.
                    tracing::error!(key = ?other, "index row kind not written before Step 5");
                    all = false;
                    continue;
                }
            };
            all &= self.write_with_retries(cql, values, "index row").await;
        }
        all
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
}

impl CreateAttempt<'_> {
    /// C1's bind values, in statement order.
    fn c1_values(&self, write_id: Uuid) -> Vec<Value> {
        vec![
            Value::from(self.tenant_id),
            Value::from(self.resource_type),
            Value::from(self.id),
            Value::from(self.data),
            Value::from(self.last_updated),
            Value::from(self.last_updated),
            Value::from(self.fhir_version),
            Value::from(write_ts_of(self.last_updated)),
            Value::from(write_id),
            Value::from(Blob::from(self.keys_blob.to_vec())),
        ]
    }
}

fn already_exists(resource_type: &str, id: &str) -> StorageError {
    StorageError::Resource(ResourceError::AlreadyExists {
        resource_type: resource_type.to_string(),
        id: id.to_string(),
    })
}

/// A random pause of 5 to 25 ms before a re-run (ADR-aacruzgon-0024).
fn rerun_pause() -> Duration {
    let random = Uuid::new_v4().as_u128() as u64;
    Duration::from_millis(5 + random % 21)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdrs_tokio::consistency::Consistency;
    use cdrs_tokio::frame::message_error::{
        ErrorBody, FailureInfo, UnavailableError, WriteFailureError, WriteTimeoutError,
    };

    fn server(ty: ErrorType) -> DriverError {
        DriverError::from(CdrsError::Server {
            body: ErrorBody {
                message: "x".to_string(),
                ty,
            },
            addr: "127.0.0.1:9042".parse().unwrap(),
        })
    }

    fn write_timeout(write_type: WriteType) -> ErrorType {
        ErrorType::WriteTimeout(WriteTimeoutError {
            cl: Consistency::LocalQuorum,
            received: 0,
            block_for: 1,
            write_type,
            contentions: None,
        })
    }

    #[test]
    fn cas_failures_are_classified_as_the_design_says() {
        let unknown = |error, expect_timeout: bool| match classify_cas_failure(error) {
            CasFailure::Unknown { timed_out, .. } => assert_eq!(timed_out, expect_timeout),
            other => panic!("expected unknown, got {other:?}"),
        };
        unknown(server(write_timeout(WriteType::Cas)), true);
        unknown(
            server(ErrorType::WriteFailure(WriteFailureError {
                cl: Consistency::LocalQuorum,
                received: 0,
                block_for: 1,
                failure_info: FailureInfo::NumFailures(1),
                write_type: WriteType::Cas,
            })),
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
            DriverError::from(CdrsError::UnexpectedErrorCode(0x1700)),
            true,
        );
        unknown(
            DriverError::Deadline {
                after: Duration::from_secs(10),
                sent: true,
            },
            true,
        );

        assert!(matches!(
            classify_cas_failure(server(write_timeout(WriteType::Simple))),
            CasFailure::Applied
        ));
        for not_sent in [
            DriverError::Deadline {
                after: Duration::from_secs(10),
                sent: false,
            },
            DriverError::from(CdrsError::Timeout("connect".to_string())),
            DriverError::from(CdrsError::General(
                "No nodes available in query plan!".to_string(),
            )),
            server(ErrorType::Unavailable(UnavailableError {
                cl: Consistency::LocalSerial,
                required: 1,
                alive: 0,
            })),
        ] {
            assert!(
                matches!(classify_cas_failure(not_sent), CasFailure::NotApplied(_)),
                "should be not applied"
            );
        }
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
        let at = DateTime::parse_from_rfc3339("2026-10-14T10:00:00.123Z")
            .unwrap()
            .with_timezone(&Utc);
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

    #[test]
    fn c1_binds_no_null_and_no_unset_value() {
        let keys = index_keys::encode(&[IndexKey::ResourcesByType]);
        let attempt = CreateAttempt {
            tenant_id: "acme",
            resource_type: "Patient",
            id: "p1",
            data: "{}",
            fhir_version: FhirVersion::default_enabled().as_mime_param(),
            last_updated: now_millis(),
            keys_blob: &keys,
        };
        let values = attempt.c1_values(Uuid::new_v4());
        // Ten markers in C1; version_id and deleted_at are literals.
        assert_eq!(values.len(), 10);
        assert!(
            values.iter().all(|value| matches!(value, Value::Some(_))),
            "{values:?}"
        );
    }

    #[test]
    fn rerun_pauses_are_5_to_25_ms() {
        for _ in 0..200 {
            let pause = rerun_pause();
            assert!((5..=25).contains(&pause.as_millis()), "{pause:?}");
        }
    }
}
