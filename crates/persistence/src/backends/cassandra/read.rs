//! `read` and `count`.
//!
//! **`read`** (`docs/Cassandra/schema/walkthrough.md` 4.1): R1 at
//! `LOCAL_QUORUM` on `resources (T, P, id)`. No row is `Ok(None)` (404); a
//! row with `deleted_at > 0` is `ResourceError::Gone`, which REST always
//! answers with 410 (`crates/rest/src/error.rs`, `From<ResourceError>`;
//! `HFS_RETURN_GONE` is read only by `vread`, `handlers/vread.rs`); otherwise
//! the row is built with
//! `StoredResource::from_storage`, with `deleted_at` 0 passed as `None`
//! (walkthrough section 2, "`deleted_at` and its readers"; G39). The version
//! id is the `int` column as a numeric string, so the ETag is `W/"1"`,
//! `W/"2"`, ….
//!
//! **`count`** (walkthrough 5.9; design, "`_count=0` and `count`"): a
//! `COUNT(*)` of the `(tenant, type)` partition of `resources_by_type`, which
//! the node pages internally, with no tag check. So it counts writes still in
//! flight and orphans the sweeper has not reached, which the design accepts
//! (G7, ADR-aacruzgon-0027). `count(tenant, None)` sums it over the resource
//! type names of every enabled FHIR version, each name once, as
//! `ResourceTypeScope` judges type names
//! (`crate::search::type_qualifier`), not over the registry's types, which
//! would include `Resource` and miss `Binary`.

use std::collections::BTreeSet;
use std::sync::OnceLock;

use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::blob::Blob;
use cdrs_tokio::types::rows::Row;
use cdrs_tokio::types::value::Value;
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt};
use helios_fhir::{FhirResourceTypeProvider, FhirVersion};
use uuid::Uuid;

use super::backend::CassandraBackend;
use super::errors::{BACKEND_NAME, map_driver_error};
use super::index_keys::{self, IndexKey};
use super::session::StatementKind;
use crate::error::{BackendError, ResourceError, StorageError, StorageResult};
use crate::tenant::TenantContext;
use crate::types::StoredResource;

/// R1: the current row (walkthrough section 2, "Reads"). It selects what
/// every pre-read needs, so `update`, `delete` and `create_or_update` (Step
/// 3) reuse the same prepared statement.
pub(super) const SELECT_RESOURCE: &str = "SELECT version_id, data, last_updated, created_at, \
     deleted_at, fhir_version, write_ts, write_id, index_keys \
     FROM resources WHERE tenant_id = ? AND resource_type = ? AND id = ?";

/// `count(tenant, Some(type))`.
const COUNT_BY_TYPE: &str =
    "SELECT COUNT(*) FROM resources_by_type WHERE tenant_id = ? AND resource_type = ?";

/// Partitions `count(tenant, None)` scans at once.
const COUNT_ALL_CONCURRENCY: usize = 8;

fn internal(message: String) -> StorageError {
    StorageError::Backend(BackendError::Internal {
        backend_name: BACKEND_NAME.to_string(),
        message,
        source: None,
    })
}

/// The live/deleted state of an R1 row: `deleted_at > 0` means deleted.
/// An unset cell, which the backend never writes, reads as live.
pub(super) fn deleted_at_of(row: &Row) -> StorageResult<Option<DateTime<Utc>>> {
    let deleted_at: Option<DateTime<Utc>> = row
        .get_by_name("deleted_at")
        .map_err(|error| internal(format!("read: deleted_at: {error}")))?;
    Ok(deleted_at.filter(|at| at.timestamp_millis() > 0))
}

/// The stored FHIR version, falling back to the build's default as
/// PostgreSQL and MongoDB do.
pub(super) fn fhir_version_of(row: &Row) -> FhirVersion {
    IntoRustByName::<String>::get_by_name(row, "fhir_version")
        .ok()
        .flatten()
        .and_then(|stored| FhirVersion::from_storage(&stored))
        .unwrap_or_else(FhirVersion::default_enabled)
}

/// An R1 row of `resources`, decoded: the pre-read every Step 3 write runs
/// (`update`'s read of `K(N)`, the pre-reads of `delete`, `delete_versioned`
/// and each `create_or_update` turn; walkthrough section 2, "Reads", and
/// G22). [`CassandraBackend::read_resource_row`] reads it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ResourceRow {
    /// `N`.
    pub(crate) version_id: i32,
    /// The stored JSON; unset only if the row was written without it, which
    /// the backend never does on `resources`.
    pub(crate) data: Option<String>,
    /// `lu(N)`.
    pub(crate) last_updated: DateTime<Utc>,
    /// When the resource was created; `None` if unset.
    pub(crate) created_at: Option<DateTime<Utc>>,
    /// `Some` only when `deleted_at > 0`: a delete marker. The live sentinel
    /// 0 and an unset cell are both `None` (G39).
    pub(crate) deleted_at: Option<DateTime<Utc>>,
    /// The FHIR version the resource was created with, falling back to the
    /// build's default as [`fhir_version_of`] does.
    pub(crate) fhir_version: FhirVersion,
    /// `write_ts(N)`, in microseconds.
    pub(crate) write_ts: Option<i64>,
    /// `w(N)`, the `write_id` of the CAS that wrote `N`.
    pub(crate) write_id: Option<Uuid>,
    /// `K(N)`, in the stored encoding (`index_keys.rs`).
    pub(crate) index_keys: Option<Vec<u8>>,
}

impl ResourceRow {
    /// Decodes a row of [`SELECT_RESOURCE`] (R1, or R2 at `LOCAL_SERIAL`).
    /// `version_id` and `last_updated` must be readable; the rest may be
    /// unset.
    pub(crate) fn decode(row: &Row) -> StorageResult<Self> {
        let version_id: i32 = row
            .get_r_by_name("version_id")
            .map_err(|error| internal(format!("pre-read: version_id: {error}")))?;
        let last_updated: DateTime<Utc> = row
            .get_r_by_name("last_updated")
            .map_err(|error| internal(format!("pre-read: last_updated: {error}")))?;
        let optional = |name: &str| -> StorageResult<()> {
            if row.contains_column(name) {
                Ok(())
            } else {
                Err(internal(format!("pre-read: no {name} column")))
            }
        };
        for name in ["data", "created_at", "write_ts", "write_id", "index_keys"] {
            optional(name)?;
        }
        Ok(Self {
            version_id,
            data: row
                .get_by_name("data")
                .map_err(|error| internal(format!("pre-read: data: {error}")))?,
            last_updated,
            created_at: row
                .get_by_name("created_at")
                .map_err(|error| internal(format!("pre-read: created_at: {error}")))?,
            deleted_at: deleted_at_of(row)?,
            fhir_version: fhir_version_of(row),
            write_ts: row
                .get_by_name("write_ts")
                .map_err(|error| internal(format!("pre-read: write_ts: {error}")))?,
            write_id: row
                .get_by_name("write_id")
                .map_err(|error| internal(format!("pre-read: write_id: {error}")))?,
            index_keys: IntoRustByName::<Blob>::get_by_name(row, "index_keys")
                .map_err(|error| internal(format!("pre-read: index_keys: {error}")))?
                .map(Blob::into_vec),
        })
    }

    /// Whether the row is a delete marker (`deleted_at > 0`).
    pub(crate) fn is_deleted(&self) -> bool {
        self.deleted_at.is_some()
    }

    /// `K(N)`, decoded from the stored `index_keys`. An unset or undecodable
    /// key set is an error: it is never left unset, and guessing it would
    /// leave index rows nobody deletes (G22).
    #[allow(dead_code)] // Read by tests; the writes decode `K(N)` in `PreviousVersion::from_row`.
    pub(crate) fn keys(&self) -> StorageResult<Vec<IndexKey>> {
        let blob = self
            .index_keys
            .as_deref()
            .ok_or_else(|| internal("pre-read: index_keys is unset".to_string()))?;
        index_keys::decode(blob).map_err(|error| internal(format!("pre-read: {error}")))
    }
}

impl CassandraBackend {
    /// `ResourceStorage::read`; see the module docs.
    pub(super) async fn read_resource(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<Option<StoredResource>> {
        let rows = self
            .session
            .rows(
                SELECT_RESOURCE,
                vec![
                    Value::from(tenant.tenant_id().as_str()),
                    Value::from(resource_type),
                    Value::from(id),
                ],
                StatementKind::Read,
            )
            .await
            .map_err(|error| StorageError::Backend(map_driver_error(&error, "read")))?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };

        if let Some(deleted_at) = deleted_at_of(row)? {
            return Err(StorageError::Resource(ResourceError::Gone {
                resource_type: resource_type.to_string(),
                id: id.to_string(),
                deleted_at: Some(deleted_at),
            }));
        }

        let version_id: i32 = row
            .get_r_by_name("version_id")
            .map_err(|error| internal(format!("read: version_id: {error}")))?;
        let data: String = row
            .get_r_by_name("data")
            .map_err(|error| internal(format!("read: data: {error}")))?;
        let content = serde_json::from_str(&data)
            .map_err(|error| internal(format!("read: stored JSON: {error}")))?;
        let last_updated: DateTime<Utc> = row
            .get_r_by_name("last_updated")
            .map_err(|error| internal(format!("read: last_updated: {error}")))?;
        let created_at: DateTime<Utc> = row
            .get_by_name("created_at")
            .ok()
            .flatten()
            .unwrap_or(last_updated);

        Ok(Some(StoredResource::from_storage(
            resource_type,
            id,
            version_id.to_string(),
            tenant.tenant_id().clone(),
            content,
            created_at,
            last_updated,
            None,
            fhir_version_of(row),
        )))
    }

    /// R1 at `LOCAL_QUORUM`: the current row of `(tenant_id, resource_type,
    /// id)`, decoded, or `None` when there is none. A deleted row is
    /// returned, with [`ResourceRow::deleted_at`] set; deciding what that
    /// means is the caller's. Its failure is mapped as a plain read's (504
    /// timed out, 503 failed).
    pub(crate) async fn read_resource_row(
        &self,
        tenant_id: &str,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<Option<ResourceRow>> {
        let rows = self
            .session
            .rows(
                SELECT_RESOURCE,
                vec![
                    Value::from(tenant_id),
                    Value::from(resource_type),
                    Value::from(id),
                ],
                StatementKind::Read,
            )
            .await
            .map_err(|error| StorageError::Backend(map_driver_error(&error, "pre-read")))?;
        rows.first().map(ResourceRow::decode).transpose()
    }

    /// `ResourceStorage::count`; see the module docs.
    pub(super) async fn count_resources(
        &self,
        tenant: &TenantContext,
        resource_type: Option<&str>,
    ) -> StorageResult<u64> {
        let tenant_id = tenant.tenant_id().as_str();
        match resource_type {
            Some(resource_type) => self.count_type(tenant_id, resource_type).await,
            None => {
                let scans: Vec<_> = all_resource_type_names()
                    .iter()
                    .map(|resource_type| self.count_type(tenant_id, resource_type))
                    .collect();
                // Sum as the scans finish and stop at the first error:
                // dropping the stream cancels the scans still in flight,
                // so a down node costs at most `COUNT_ALL_CONCURRENCY`
                // timed-out requests, not one for every resource type.
                let mut counts = stream::iter(scans).buffer_unordered(COUNT_ALL_CONCURRENCY);
                let mut total = 0u64;
                while let Some(count) = counts.next().await {
                    total += count?;
                }
                Ok(total)
            }
        }
    }

    async fn count_type(&self, tenant_id: &str, resource_type: &str) -> StorageResult<u64> {
        let rows = self
            .session
            .rows(
                COUNT_BY_TYPE,
                vec![Value::from(tenant_id), Value::from(resource_type)],
                StatementKind::Read,
            )
            .await
            .map_err(|error| StorageError::Backend(map_driver_error(&error, "count")))?;
        let count: i64 = rows
            .first()
            .ok_or_else(|| internal("count: no row".to_string()))?
            .get_r_by_name("count")
            .map_err(|error| internal(format!("count: {error}")))?;
        Ok(count.max(0) as u64)
    }
}

/// Every resource type name of every FHIR version this build enables, each
/// once.
pub(super) fn all_resource_type_names() -> &'static BTreeSet<&'static str> {
    static NAMES: OnceLock<BTreeSet<&'static str>> = OnceLock::new();
    NAMES.get_or_init(|| {
        FhirVersion::enabled_versions()
            .iter()
            .flat_map(|version| resource_type_names(*version))
            .collect()
    })
}

fn resource_type_names(version: FhirVersion) -> Vec<&'static str> {
    match version {
        #[cfg(feature = "R4")]
        FhirVersion::R4 => helios_fhir::r4::Resource::get_resource_type_names(),
        #[cfg(feature = "R4B")]
        FhirVersion::R4B => helios_fhir::r4b::Resource::get_resource_type_names(),
        #[cfg(feature = "R5")]
        FhirVersion::R5 => helios_fhir::r5::Resource::get_resource_type_names(),
        #[cfg(feature = "R6")]
        FhirVersion::R6 => helios_fhir::r6::Resource::get_resource_type_names(),
        // helios-fhir can have a version enabled (by another crate in the
        // build) that this crate's own features do not name, as
        // `type_qualifier.rs` notes.
        #[allow(unreachable_patterns)]
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_enabled_version_contributes_its_type_names() {
        let names = all_resource_type_names();
        for version in FhirVersion::enabled_versions() {
            for name in resource_type_names(*version) {
                assert!(names.contains(name), "{name} of {version:?}");
            }
        }
        assert!(names.contains("Patient"));
        assert!(names.contains("Binary"));
        assert!(!names.contains("Resource"));
    }
}
