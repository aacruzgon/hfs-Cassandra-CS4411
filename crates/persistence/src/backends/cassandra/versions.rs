//! `vread`, `list_versions` and `delete_with_match` (pulled into roadmap
//! Step 3, layer L2c, because the race suite needs them: Step 3 plan D1), and
//! the `exists` and `read_batch` overrides (D10).
//!
//! - **`vread`** (`docs/Cassandra/schema/walkthrough.md` 4.2): R3 at
//!   `LOCAL_QUORUM`, one row of `resource_versions`. The version id is
//!   parsed first: anything but the canonical decimal of a positive `int`
//!   (`"abc"`, `"1.0"`, `"01"`, `"+1"`, `"0"`) is `Ok(None)` without a query,
//!   which is also what SQLite and PostgreSQL, comparing it as text, answer
//!   for `"01"`. No row is `Ok(None)` (404). A content row is its stored
//!   JSON. A delete marker (`deleted_at > 0`, `data` unset; walkthrough
//!   section 2, "`deleted_at` and its readers") comes back as a
//!   `StoredResource` with `deleted_at` set and content `Value::Null`, as
//!   PostgreSQL's, which the REST vread handler answers with 410 (404 when
//!   `HFS_RETURN_GONE` is false). `resource_versions` has no `created_at`,
//!   so `last_updated` stands in for it, as on PostgreSQL (G14).
//! - **`list_versions`** (4.3): R4 at `LOCAL_QUORUM`, every `version_id` of
//!   the partition, oldest first. `version_id` is an `int`, so `"10"` follows
//!   `"9"`. An unknown id is an empty list. Paged, since a resource's version
//!   count has no bound.
//! - **`delete_with_match`** takes the client's `If-Match` field value
//!   (`W/"1"`, a list, `*`), not a bare version. It checks
//!   `Operation::Delete`, pre-reads R1 (absent or deleted: `NotFound`),
//!   compares the field with the current version through
//!   [`if_match_field_satisfied`], and then calls `delete_versioned` with the
//!   bare current version, as SQLite, PostgreSQL and MongoDB do. A mismatch
//!   is `VersionConflict` with nothing written. The compare-and-swap is
//!   `delete_versioned`'s, so a writer landing after the comparison makes
//!   that CAS miss rather than being deleted with it (#1404).
//! - **`update_with_match`** (roadmap Step 4) has the same shape. It checks
//!   `Operation::Update` (the SearchParameter `code` bound, G38, is
//!   `update`'s, before any statement there),
//!   pre-reads the resource with `read` (absent or deleted: `NotFound`),
//!   compares the field with its version as `delete_with_match` does (a
//!   mismatch is `VersionConflict`, 409, with nothing written), and then
//!   hands the write to `update`, which runs the whole update path itself.
//!   A writer landing between the pre-read and `update`'s compare-and-swap
//!   is `update`'s miss: `VersionConflict` for an update, `NotFound` for a
//!   delete.
//! - **`exists`**: an R1 that selects only `deleted_at`, so a yes/no answer
//!   does not ship the resource. No row, or `deleted_at > 0`, is `false`.
//! - **`current_version`** (roadmap Step 4): an R1 that selects only
//!   `version_id` and `deleted_at`, as `exists` does. No row, or
//!   `deleted_at > 0`, is `None`, as the trait documents ("if the resource
//!   doesn't exist or is deleted"). The trait default reads through `read`,
//!   which answers `Gone` for a deleted resource, so the default would
//!   answer `Gone` there too, as it does on every other backend.
//! - **`read_batch`**: plain `read`s, [`BULK_WRITE_CONCURRENCY`] at a time
//!   (`buffered`, so the output keeps the input's order). Missing and deleted
//!   ids are skipped; any other error fails the batch, and dropping the
//!   stream cancels the reads still in flight. Never at `LOCAL_SERIAL`: a
//!   serial read is a Paxos round per partition.
//!
//! None of them checks `Operation::Read`: `read` does not, nor do the other
//! backends' versions of these methods.

use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::rows::Row;
use cdrs_tokio::types::value::Value;
use chrono::{DateTime, Utc};
use futures::stream::{self, StreamExt};
use serde_json::Value as Json;

use super::backend::CassandraBackend;
use super::errors::{BACKEND_NAME, map_driver_error};
use super::read::{deleted_at_of, fhir_version_of};
use super::session::{PAGE_SIZE, StatementKind};
use super::storage::BULK_WRITE_CONCURRENCY;
use crate::core::{if_match_field_satisfied, normalize_etag};
use crate::error::{BackendError, ConcurrencyError, ResourceError, StorageError, StorageResult};
use crate::tenant::{Operation, TenantContext};
use crate::types::StoredResource;

/// R3, reduced to what `vread` needs (walkthrough section 2, "Reads").
const SELECT_VERSION: &str = "SELECT data, last_updated, deleted_at, fhir_version \
     FROM resource_versions \
     WHERE tenant_id = ? AND resource_type = ? AND id = ? AND version_id = ?";

/// R4: every version number of one resource, oldest first. The table
/// clusters `version_id DESC`; a single-partition query may reverse it.
const SELECT_VERSION_IDS: &str = "SELECT version_id FROM resource_versions \
     WHERE tenant_id = ? AND resource_type = ? AND id = ? \
     ORDER BY version_id ASC";

/// R1, reduced to the one column `exists` needs.
const SELECT_DELETED_AT: &str =
    "SELECT deleted_at FROM resources WHERE tenant_id = ? AND resource_type = ? AND id = ?";

/// R1, reduced to the two columns `current_version` needs.
const SELECT_VERSION_AND_DELETED_AT: &str = "SELECT version_id, deleted_at FROM resources \
     WHERE tenant_id = ? AND resource_type = ? AND id = ?";

fn internal(message: String) -> StorageError {
    StorageError::Backend(BackendError::Internal {
        backend_name: BACKEND_NAME.to_string(),
        message,
        source: None,
    })
}

/// The `int` a vread's version id names, or `None` when it names none: only
/// the canonical decimal of a positive `int` is a version this backend
/// writes (`"1"`, `"2"`, …), so `"01"` and `"+1"` are unknown, as on the
/// backends that compare version ids as text.
fn version_number(version_id: &str) -> Option<i32> {
    version_id
        .parse::<i32>()
        .ok()
        .filter(|number| *number >= 1 && number.to_string() == version_id)
}

/// The bare current version `delete_with_match` hands to `delete_versioned`,
/// or `VersionConflict` when the `If-Match` field value names another one.
fn matched_version(
    resource_type: &str,
    id: &str,
    if_match: &str,
    current: i32,
) -> StorageResult<String> {
    let current = current.to_string();
    if if_match_field_satisfied(if_match, &current) {
        Ok(current)
    } else {
        Err(StorageError::Concurrency(
            ConcurrencyError::VersionConflict {
                resource_type: resource_type.to_string(),
                id: id.to_string(),
                expected_version: normalize_etag(if_match).to_string(),
                actual_version: current,
            },
        ))
    }
}

fn not_found(resource_type: &str, id: &str) -> StorageError {
    StorageError::Resource(ResourceError::NotFound {
        resource_type: resource_type.to_string(),
        id: id.to_string(),
    })
}

/// A row of [`SELECT_VERSION`] as a `StoredResource`: a marker with
/// `deleted_at` set and no content, or the stored JSON.
fn stored_version(
    tenant: &TenantContext,
    resource_type: &str,
    id: &str,
    version_id: &str,
    row: &Row,
) -> StorageResult<StoredResource> {
    let last_updated: DateTime<Utc> = row
        .get_r_by_name("last_updated")
        .map_err(|error| internal(format!("vread: last_updated: {error}")))?;
    let deleted_at = deleted_at_of(row)?;
    let content = match deleted_at {
        Some(_) => serde_json::Value::Null,
        None => {
            let data: String = row
                .get_r_by_name("data")
                .map_err(|error| internal(format!("vread: data: {error}")))?;
            serde_json::from_str(&data)
                .map_err(|error| internal(format!("vread: stored JSON: {error}")))?
        }
    };
    Ok(StoredResource::from_storage(
        resource_type,
        id,
        version_id,
        tenant.tenant_id().clone(),
        content,
        last_updated,
        last_updated,
        deleted_at,
        fhir_version_of(row),
    ))
}

impl CassandraBackend {
    /// `VersionedStorage::vread`; see the module docs.
    pub(super) async fn vread_resource(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        version_id: &str,
    ) -> StorageResult<Option<StoredResource>> {
        let Some(number) = version_number(version_id) else {
            return Ok(None);
        };
        let rows = self
            .session
            .rows(
                SELECT_VERSION,
                vec![
                    Value::from(tenant.tenant_id().as_str()),
                    Value::from(resource_type),
                    Value::from(id),
                    Value::from(number),
                ],
                StatementKind::Read,
            )
            .await
            .map_err(|error| StorageError::Backend(map_driver_error(&error, "vread")))?;
        rows.first()
            .map(|row| stored_version(tenant, resource_type, id, version_id, row))
            .transpose()
    }

    /// `VersionedStorage::list_versions`; see the module docs.
    pub(super) async fn list_resource_versions(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<Vec<String>> {
        self.list_versions_paged_by(tenant, resource_type, id, PAGE_SIZE)
            .await
    }

    /// [`Self::list_resource_versions`], `page_size` rows a page, so a test
    /// can cross a page boundary of the reversed clustering order.
    async fn list_versions_paged_by(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        page_size: i32,
    ) -> StorageResult<Vec<String>> {
        let rows = self
            .session
            .rows_paged_by(
                SELECT_VERSION_IDS,
                vec![
                    Value::from(tenant.tenant_id().as_str()),
                    Value::from(resource_type),
                    Value::from(id),
                ],
                StatementKind::Read,
                page_size,
            )
            .await
            .map_err(|error| StorageError::Backend(map_driver_error(&error, "list_versions")))?;
        rows.iter()
            .map(|row| {
                IntoRustByName::<i32>::get_r_by_name(row, "version_id")
                    .map(|version| version.to_string())
                    .map_err(|error| internal(format!("list_versions: version_id: {error}")))
            })
            .collect()
    }

    /// `VersionedStorage::delete_with_match`; see the module docs.
    pub(super) async fn delete_with_match_resource(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        expected_version: &str,
    ) -> StorageResult<()> {
        tenant.check_permission(Operation::Delete, resource_type)?;
        let row = self
            .read_resource_row(tenant.tenant_id().as_str(), resource_type, id)
            .await?
            .filter(|row| !row.is_deleted())
            .ok_or_else(|| not_found(resource_type, id))?;
        let current = matched_version(resource_type, id, expected_version, row.version_id)?;
        self.delete_versioned_resource(tenant, resource_type, id, &current)
            .await
    }

    /// `VersionedStorage::update_with_match`; see the module docs.
    pub(super) async fn update_with_match_resource(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        expected_version: &str,
        resource: Json,
    ) -> StorageResult<StoredResource> {
        // Again in `update`, before any statement there too. The
        // SearchParameter `code` bound (G38) is left to `update`, so this file
        // does not depend on `write.rs`'s check, which Step 5's extraction
        // pull request removes (ADR-aacruzgon-0079).
        tenant.check_permission(Operation::Update, resource_type)?;
        let current = match self.read_resource(tenant, resource_type, id).await {
            Ok(Some(current)) => current,
            Ok(None) | Err(StorageError::Resource(ResourceError::Gone { .. })) => {
                return Err(not_found(resource_type, id));
            }
            Err(error) => return Err(error),
        };
        let version = current
            .version_id()
            .parse::<i32>()
            .map_err(|error| internal(format!("update_with_match: version_id: {error}")))?;
        matched_version(resource_type, id, expected_version, version)?;
        self.update_resource(tenant, &current, resource).await
    }

    /// `ResourceStorage::exists`; see the module docs. A deleted resource is
    /// `false`, never an error.
    pub(super) async fn exists_resource(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<bool> {
        let rows = self
            .session
            .rows(
                SELECT_DELETED_AT,
                vec![
                    Value::from(tenant.tenant_id().as_str()),
                    Value::from(resource_type),
                    Value::from(id),
                ],
                StatementKind::Read,
            )
            .await
            .map_err(|error| StorageError::Backend(map_driver_error(&error, "exists")))?;
        match rows.first() {
            Some(row) => Ok(deleted_at_of(row)?.is_none()),
            None => Ok(false),
        }
    }

    /// `VersionedStorage::current_version`; see the module docs.
    pub(super) async fn current_version_of(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<Option<String>> {
        let rows = self
            .session
            .rows(
                SELECT_VERSION_AND_DELETED_AT,
                vec![
                    Value::from(tenant.tenant_id().as_str()),
                    Value::from(resource_type),
                    Value::from(id),
                ],
                StatementKind::Read,
            )
            .await
            .map_err(|error| StorageError::Backend(map_driver_error(&error, "current_version")))?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        if deleted_at_of(row)?.is_some() {
            return Ok(None);
        }
        let version_id: i32 = row
            .get_r_by_name("version_id")
            .map_err(|error| internal(format!("current_version: version_id: {error}")))?;
        Ok(Some(version_id.to_string()))
    }

    /// `ResourceStorage::read_batch`; see the module docs.
    pub(super) async fn read_batch_resources(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        ids: &[&str],
    ) -> StorageResult<Vec<StoredResource>> {
        // Futures do nothing until polled, so building them all first starts
        // no read; `buffered` polls `BULK_WRITE_CONCURRENCY` at a time.
        let reads: Vec<_> = ids
            .iter()
            .map(|id| self.read_resource(tenant, resource_type, id))
            .collect();
        let mut reads = stream::iter(reads).buffered(BULK_WRITE_CONCURRENCY);
        let mut results = Vec::with_capacity(ids.len());
        while let Some(read) = reads.next().await {
            match read {
                Ok(Some(resource)) => results.push(resource),
                Ok(None) | Err(StorageError::Resource(ResourceError::Gone { .. })) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_canonical_decimal_of_a_positive_int_is_a_version() {
        assert_eq!(version_number("1"), Some(1));
        assert_eq!(version_number("10"), Some(10));
        assert_eq!(version_number("2147483647"), Some(i32::MAX));
        for not_one in [
            "",
            "0",
            "-1",
            "01",
            "+1",
            " 1",
            "1 ",
            "1.0",
            "1e1",
            "abc",
            "W/\"1\"",
            "2147483648",
        ] {
            assert_eq!(version_number(not_one), None, "{not_one:?}");
        }
    }

    #[test]
    fn the_if_match_field_is_compared_with_the_bare_current_version() {
        // Every spelling of the current version, a list naming it, and `*`.
        for field in ["W/\"2\"", "\"2\"", "2", "W/\"1\", W/\"2\"", "*"] {
            assert_eq!(
                matched_version("Patient", "p", field, 2).unwrap(),
                "2",
                "{field}"
            );
        }
        // Another version, never a prefix or suffix match.
        for (field, current) in [("W/\"1\"", 2), ("W/\"1\"", 12), ("W/\"12\"", 1), ("3", 2)] {
            match matched_version("Patient", "p", field, current) {
                Err(StorageError::Concurrency(ConcurrencyError::VersionConflict {
                    expected_version,
                    actual_version,
                    ..
                })) => {
                    assert_eq!(expected_version, normalize_etag(field), "{field}");
                    assert_eq!(actual_version, current.to_string(), "{field}");
                }
                other => panic!("{field} against {current}: {other:?}"),
            }
        }
    }
}

/// Live tests on the node `HFS_TEST_CASSANDRA_URL` names (each skips without
/// it), each under its own tenant. Markers and versions past 9 are written
/// through the write engine itself ([`CassandraBackend::run_cas`] with
/// [`CasPlan::update`] and [`CasPlan::delete`]), as `write.rs`'s
/// `the_engine_updates_deletes_and_revives_through_c2_c3_and_c4` does, so
/// these tests need neither L2a's `update` nor L2b's `delete`. The
/// exceptions go through what the method under test hands its write to:
/// `delete_with_match_deletes_the_version_it_compared` through L2b's
/// `delete_versioned`, and Step 4's `update_with_match` tests through L2a's
/// `update`.
#[cfg(test)]
mod node_tests {
    use serde_json::{Value as Json, json};
    use uuid::Uuid;

    use super::*;
    use crate::backends::cassandra::index_keys::IndexKey;
    use crate::backends::cassandra::read::ResourceRow;
    use crate::backends::cassandra::session::{Fault, TimeoutWriteType};
    use crate::backends::cassandra::write::{
        CasOutcome, CasPlan, PreviousVersion, RowKey, normalized_body,
    };
    use crate::core::{ResourceStorage, VersionedStorage};
    use crate::error::TenantError;
    use crate::tenant::{TenantId, TenantPermissions};

    async fn backend() -> Option<CassandraBackend> {
        let url = std::env::var("HFS_TEST_CASSANDRA_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())?;
        Some(
            CassandraBackend::connect(crate::backends::cassandra::CassandraConfig {
                contact_points: vec![url],
                ..crate::backends::cassandra::test_support::test_config()
            })
            .await
            .expect("connect"),
        )
    }

    /// A fresh tenant with the `test-` prefix of ADR-aacruzgon-0029.
    fn tenant(case: &str) -> TenantContext {
        TenantContext::new(
            TenantId::new(format!("test-l2c-{case}-{}", Uuid::new_v4().simple())),
            TenantPermissions::full_access(),
        )
    }

    fn patient(id: &str, family: &str) -> Json {
        json!({"resourceType": "Patient", "id": id, "name": [{"family": family}]})
    }

    async fn create(backend: &CassandraBackend, tenant: &TenantContext, id: &str, family: &str) {
        backend
            .create(
                tenant,
                "Patient",
                patient(id, family),
                crate::backends::cassandra::test_support::test_fhir_version(),
            )
            .await
            .expect("create");
    }

    async fn current(backend: &CassandraBackend, tenant: &TenantContext, id: &str) -> ResourceRow {
        backend
            .read_resource_row(tenant.tenant_id().as_str(), "Patient", id)
            .await
            .expect("R1")
            .expect("a row")
    }

    async fn through_the_engine(
        backend: &CassandraBackend,
        tenant: &TenantContext,
        id: &str,
        plan: impl FnOnce(PreviousVersion) -> CasPlan,
    ) -> i32 {
        let row = current(backend, tenant, id).await;
        let key = RowKey {
            tenant_id: tenant.tenant_id().as_str(),
            resource_type: "Patient",
            id,
        };
        let mut request = backend.cas_request("l2c-test", key);
        match backend
            .run_cas(&mut request, plan(PreviousVersion::from_row(&row).unwrap()))
            .await
            .expect("run_cas")
        {
            CasOutcome::Applied(applied) => applied.version_id,
            other => panic!("expected the CAS to apply, got {other:?}"),
        }
    }

    /// C2 through the engine: the next version, with `family`.
    async fn engine_update(
        backend: &CassandraBackend,
        tenant: &TenantContext,
        id: &str,
        family: &str,
    ) -> i32 {
        let (body, data) = normalized_body("update", "Patient", id, patient(id, family)).unwrap();
        through_the_engine(backend, tenant, id, |previous| {
            CasPlan::update(previous, body, data, vec![IndexKey::ResourcesByType])
        })
        .await
    }

    /// C3 through the engine: the next version is a delete marker.
    async fn engine_delete(backend: &CassandraBackend, tenant: &TenantContext, id: &str) -> i32 {
        through_the_engine(backend, tenant, id, CasPlan::delete).await
    }

    fn family(resource: &StoredResource) -> &str {
        resource.content()["name"][0]["family"]
            .as_str()
            .unwrap_or_else(|| panic!("no family: {}", resource.content()))
    }

    /// Walkthrough 4.2, every row of its table, and the version a
    /// `Location` names (D19).
    #[tokio::test]
    async fn vread_answers_each_row_of_the_walkthrough_table() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant("vread");
        create(&backend, &t, "p", "One").await;
        let v1 = current(&backend, &t, "p").await;
        assert_eq!(engine_update(&backend, &t, "p", "Two").await, 2);
        assert_eq!(engine_delete(&backend, &t, "p").await, 3);
        let v3 = current(&backend, &t, "p").await;

        // A content row: its own JSON, version and instant, not the
        // current row's.
        let one = backend
            .vread(&t, "Patient", "p", "1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (one.id(), one.version_id(), family(&one)),
            ("p", "1", "One")
        );
        assert_eq!(one.content()["id"], "p");
        assert!(!one.is_deleted());
        assert_eq!(one.last_modified(), v1.last_updated);
        assert_eq!(one.created_at(), v1.last_updated, "no created_at: lu (G14)");
        assert_eq!(
            one.fhir_version(),
            crate::backends::cassandra::test_support::test_fhir_version()
        );
        assert_eq!(one.tenant_id(), t.tenant_id());
        let two = backend
            .vread(&t, "Patient", "p", "2")
            .await
            .unwrap()
            .unwrap();
        assert_eq!((two.version_id(), family(&two)), ("2", "Two"));
        assert!(two.last_modified() > one.last_modified());

        // The marker: deleted_at set, no content. REST's vread answers it
        // with 410 because `is_deleted()` (`rest/src/handlers/vread.rs`).
        let marker = backend
            .vread(&t, "Patient", "p", "3")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(marker.version_id(), "3");
        assert!(marker.is_deleted());
        assert_eq!(marker.deleted_at(), v3.deleted_at);
        assert_eq!(marker.last_modified(), v3.last_updated);
        assert_eq!(marker.content(), &Json::Null);
        // `read` of the same resource is Gone; vread of the version before
        // the delete still serves it.
        assert!(matches!(
            backend.read(&t, "Patient", "p").await,
            Err(StorageError::Resource(ResourceError::Gone { .. }))
        ));

        // No such row: a version past the current, an unknown id, another
        // tenant's resource, another type.
        assert!(
            backend
                .vread(&t, "Patient", "p", "4")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            backend
                .vread(&t, "Patient", "nobody", "1")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            backend
                .vread(&tenant("vread-other"), "Patient", "p", "1")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            backend
                .vread(&t, "Observation", "p", "1")
                .await
                .unwrap()
                .is_none()
        );

        // Not a version this backend writes: `Ok(None)` without a query.
        let before = backend.statement_counts();
        for not_one in ["abc", "1.0", "01", "+1", "0", "-1", "2147483648"] {
            assert!(
                backend
                    .vread(&t, "Patient", "p", not_one)
                    .await
                    .unwrap()
                    .is_none(),
                "{not_one}"
            );
        }
        assert_eq!(backend.statement_counts().since(&before).total(), 0);
    }

    /// Walkthrough 4.3: numeric order past 9, a marker included, and an
    /// unknown id's empty list.
    #[tokio::test]
    async fn list_versions_is_numeric_past_nine() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant("list");
        create(&backend, &t, "p", "v1").await;
        for version in 2..=11 {
            assert_eq!(
                engine_update(&backend, &t, "p", &format!("v{version}")).await,
                version
            );
        }
        assert_eq!(engine_delete(&backend, &t, "p").await, 12);
        let expected: Vec<String> = (1..=12).map(|v| v.to_string()).collect();
        assert_eq!(
            backend.list_versions(&t, "Patient", "p").await.unwrap(),
            expected
        );
        // Three rows a page: four pages of the reversed order, then an
        // empty last page or none, still oldest first.
        assert_eq!(
            backend
                .list_versions_paged_by(&t, "Patient", "p", 3)
                .await
                .unwrap(),
            expected
        );
        // vread agrees with it past 9.
        let ten = backend
            .vread(&t, "Patient", "p", "10")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(family(&ten), "v10");
        assert!(
            backend
                .list_versions(&t, "Patient", "nobody")
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            backend
                .list_versions(&tenant("list-other"), "Patient", "p")
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// What `delete_with_match` refuses, each with nothing written: a stale
    /// tag (409), an absent or deleted resource (`NotFound`), and a tenant
    /// without delete permission (before any read).
    #[tokio::test]
    async fn delete_with_match_refuses_without_writing() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant("dwm-refuse");
        create(&backend, &t, "p", "One").await;
        assert_eq!(engine_update(&backend, &t, "p", "Two").await, 2);

        let before = backend.statement_counts();
        for stale in ["W/\"1\"", "\"1\"", "1", "W/\"3\"", "W/\"1\", W/\"3\""] {
            match backend.delete_with_match(&t, "Patient", "p", stale).await {
                Err(StorageError::Concurrency(ConcurrencyError::VersionConflict {
                    expected_version,
                    actual_version,
                    ..
                })) => {
                    assert_eq!(expected_version, normalize_etag(stale));
                    assert_eq!(actual_version, "2");
                }
                other => panic!("{stale}: expected a version conflict, got {other:?}"),
            }
        }
        let spent = backend.statement_counts().since(&before);
        assert_eq!(
            (spent.write, spent.cas, spent.read),
            (0, 0, 5),
            "R1 each, nothing else"
        );
        assert_eq!(current(&backend, &t, "p").await.version_id, 2);
        assert_eq!(
            backend.list_versions(&t, "Patient", "p").await.unwrap(),
            ["1", "2"]
        );

        // Absent, and deleted: NotFound, never a CAS (G9).
        assert_eq!(engine_delete(&backend, &t, "p").await, 3);
        let before = backend.statement_counts();
        for (id, field) in [
            ("nobody", "W/\"1\""),
            ("p", "W/\"2\""),
            ("p", "W/\"3\""),
            ("p", "*"),
        ] {
            match backend.delete_with_match(&t, "Patient", id, field).await {
                Err(StorageError::Resource(ResourceError::NotFound { .. })) => {}
                other => panic!("{id} {field}: expected NotFound, got {other:?}"),
            }
        }
        let spent = backend.statement_counts().since(&before);
        assert_eq!((spent.write, spent.cas), (0, 0));
        assert_eq!(
            backend.list_versions(&t, "Patient", "p").await.unwrap(),
            ["1", "2", "3"]
        );

        // No delete permission: refused before the pre-read, so the answer
        // says nothing about whether the resource exists.
        let read_only = TenantContext::new(t.tenant_id().clone(), TenantPermissions::read_only());
        let before = backend.statement_counts();
        for id in ["p", "nobody"] {
            match backend
                .delete_with_match(&read_only, "Patient", id, "*")
                .await
            {
                Err(StorageError::Tenant(TenantError::OperationNotPermitted { .. })) => {}
                other => panic!("{id}: expected a permission refusal, got {other:?}"),
            }
        }
        assert_eq!(backend.statement_counts().since(&before).total(), 0);
    }

    /// A matching tag deletes exactly the version it named, through L2b's
    /// `delete_versioned` with the bare version: `W/"2"` reaches it as `2`
    /// (Step 3 plan D1).
    #[tokio::test]
    async fn delete_with_match_deletes_the_version_it_compared() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant("dwm-match");
        for (id, field) in [
            ("a", "W/\"2\""),
            ("b", "\"2\""),
            ("c", "2"),
            ("d", "W/\"1\", W/\"2\""),
            ("e", "*"),
        ] {
            create(&backend, &t, id, "One").await;
            assert_eq!(engine_update(&backend, &t, id, "Two").await, 2);
            backend
                .delete_with_match(&t, "Patient", id, field)
                .await
                .unwrap_or_else(|error| panic!("{id} {field}: {error:?}"));
            assert!(matches!(
                backend.read(&t, "Patient", id).await,
                Err(StorageError::Resource(ResourceError::Gone { .. }))
            ));
            assert_eq!(
                backend.list_versions(&t, "Patient", id).await.unwrap(),
                ["1", "2", "3"],
                "{id}"
            );
            let marker = backend
                .vread(&t, "Patient", id, "3")
                .await
                .unwrap()
                .unwrap();
            assert!(marker.is_deleted(), "{id}");
            // Again: deleted now, so NotFound and no fourth version.
            assert!(matches!(
                backend
                    .delete_with_match(&t, "Patient", id, "W/\"3\"")
                    .await,
                Err(StorageError::Resource(ResourceError::NotFound { .. }))
            ));
            assert_eq!(
                backend
                    .list_versions(&t, "Patient", id)
                    .await
                    .unwrap()
                    .len(),
                3
            );
        }
    }

    /// Another writer's compare-and-swap of `Patient/{id}` from version
    /// `from`, outside the engine: it writes `resources` only, with no
    /// version row, as `upsert.rs`'s `another_writer_late` does, and
    /// `deleting` makes it a delete. The fault seam holds it back and applies
    /// it just before the next plain write, which in `update` and
    /// `delete_versioned` is the rewrite of row `N` (V3, G40): after their
    /// reads and before their own compare-and-swap.
    async fn another_writer_before_the_next_write(
        backend: &CassandraBackend,
        tenant: &TenantContext,
        id: &str,
        from: i32,
        deleting: bool,
    ) {
        const ANOTHER_WRITER: &str = "UPDATE resources SET version_id = ?, deleted_at = ? \
             WHERE tenant_id = ? AND resource_type = ? AND id = ? IF version_id = ?";
        backend.session.inject_fault(
            StatementKind::Cas,
            ANOTHER_WRITER,
            Fault::ApplyLate {
                before: StatementKind::Write,
                write_type: TimeoutWriteType::Cas,
            },
        );
        let deleted_at = if deleting {
            Utc::now()
        } else {
            DateTime::<Utc>::UNIX_EPOCH
        };
        let values = vec![
            Value::from(from + 1),
            Value::from(deleted_at),
            Value::from(tenant.tenant_id().as_str()),
            Value::from("Patient"),
            Value::from(id),
            Value::from(from),
        ];
        let sent = backend
            .session
            .rows(ANOTHER_WRITER, values, StatementKind::Cas)
            .await;
        assert!(
            sent.is_err(),
            "the seam answers the held-back CAS with a timeout"
        );
    }

    /// A matching tag updates exactly the version it named, through L2a's
    /// `update`: every form of the `If-Match` field `delete_with_match`
    /// accepts, with exactly the pre-read, `update`'s R1 and one
    /// compare-and-swap, the new body as version 3, `created_at` kept (G13),
    /// and the same tag refused once it is stale.
    #[tokio::test]
    async fn update_with_match_updates_the_version_it_compared() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant("uwm-match");
        for (id, field) in [
            ("a", "W/\"2\""),
            ("b", "\"2\""),
            ("c", "2"),
            ("d", "W/\"1\", W/\"2\""),
            ("e", "*"),
        ] {
            create(&backend, &t, id, "One").await;
            assert_eq!(engine_update(&backend, &t, id, "Two").await, 2);
            let created_at = backend
                .read(&t, "Patient", id)
                .await
                .unwrap()
                .unwrap()
                .created_at();
            let before = backend.statement_counts();
            let updated = backend
                .update_with_match(&t, "Patient", id, field, patient(id, "Three"))
                .await
                .unwrap_or_else(|error| panic!("{id} {field}: {error:?}"));
            // Exactly the pre-read and `update`'s own R1, then one CAS. A
            // second read whose row `update` builds on would reopen the
            // lost-update window #57 closed in REST, so any extra read fails
            // here. (The plain writes are not pinned: Step 5's batches, #90
            // onward, change them.)
            let spent = backend.statement_counts().since(&before);
            assert_eq!(
                (spent.read, spent.serial_read, spent.cas),
                (2, 0, 1),
                "{id} {field}: the pre-read, update's R1 and one CAS"
            );
            assert_eq!(
                (updated.version_id(), family(&updated)),
                ("3", "Three"),
                "{id}"
            );
            assert_eq!(updated.created_at(), created_at, "{id}");
            let live = backend.read(&t, "Patient", id).await.unwrap().unwrap();
            assert_eq!((live.version_id(), family(&live)), ("3", "Three"), "{id}");
            assert_eq!(
                backend.list_versions(&t, "Patient", id).await.unwrap(),
                ["1", "2", "3"],
                "{id}"
            );
            let three = backend
                .vread(&t, "Patient", id, "3")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(family(&three), "Three", "{id}");

            // The same tag again is stale now (`*` always matches): 409,
            // and no fourth version.
            if field != "*" {
                assert!(
                    matches!(
                        backend
                            .update_with_match(&t, "Patient", id, field, patient(id, "Four"))
                            .await,
                        Err(StorageError::Concurrency(
                            ConcurrencyError::VersionConflict { .. }
                        ))
                    ),
                    "{id} {field}"
                );
                assert_eq!(
                    backend
                        .list_versions(&t, "Patient", id)
                        .await
                        .unwrap()
                        .len(),
                    3,
                    "{id}"
                );
            }
        }
    }

    /// What `update_with_match` refuses, each with nothing written, as
    /// `delete_with_match` does: a stale tag (409), an absent or deleted
    /// resource (`NotFound`); a tenant without update permission, before any
    /// statement; and a SearchParameter `code` over 128 bytes (G38), which
    /// `update` refuses after the pre-read and before any statement of its
    /// own.
    #[tokio::test]
    async fn update_with_match_refuses_without_writing() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant("uwm-refuse");
        create(&backend, &t, "p", "One").await;
        assert_eq!(engine_update(&backend, &t, "p", "Two").await, 2);

        let before = backend.statement_counts();
        for stale in ["W/\"1\"", "\"1\"", "1", "W/\"3\"", "W/\"1\", W/\"3\""] {
            match backend
                .update_with_match(&t, "Patient", "p", stale, patient("p", "Three"))
                .await
            {
                Err(StorageError::Concurrency(ConcurrencyError::VersionConflict {
                    expected_version,
                    actual_version,
                    ..
                })) => {
                    assert_eq!(expected_version, normalize_etag(stale));
                    assert_eq!(actual_version, "2");
                }
                other => panic!("{stale}: expected a version conflict, got {other:?}"),
            }
        }
        let spent = backend.statement_counts().since(&before);
        assert_eq!(
            (spent.write, spent.cas, spent.read),
            (0, 0, 5),
            "the pre-read each, nothing else"
        );
        let live = backend.read(&t, "Patient", "p").await.unwrap().unwrap();
        assert_eq!((live.version_id(), family(&live)), ("2", "Two"));
        assert_eq!(
            backend.list_versions(&t, "Patient", "p").await.unwrap(),
            ["1", "2"]
        );

        // Absent, and deleted: NotFound, never a CAS (G9).
        assert_eq!(engine_delete(&backend, &t, "p").await, 3);
        let before = backend.statement_counts();
        for (id, field) in [
            ("nobody", "W/\"1\""),
            ("p", "W/\"2\""),
            ("p", "W/\"3\""),
            ("p", "*"),
        ] {
            match backend
                .update_with_match(&t, "Patient", id, field, patient(id, "Three"))
                .await
            {
                Err(StorageError::Resource(ResourceError::NotFound { .. })) => {}
                other => panic!("{id} {field}: expected NotFound, got {other:?}"),
            }
        }
        let spent = backend.statement_counts().since(&before);
        assert_eq!((spent.write, spent.cas), (0, 0));
        assert_eq!(
            backend.list_versions(&t, "Patient", "p").await.unwrap(),
            ["1", "2", "3"]
        );

        // No update permission: refused before the pre-read, so the answer
        // says nothing about whether the resource exists.
        let read_only = TenantContext::new(t.tenant_id().clone(), TenantPermissions::read_only());
        let before = backend.statement_counts();
        for id in ["p", "nobody"] {
            match backend
                .update_with_match(&read_only, "Patient", id, "*", patient(id, "Three"))
                .await
            {
                Err(StorageError::Tenant(TenantError::OperationNotPermitted { .. })) => {}
                other => panic!("{id}: expected a permission refusal, got {other:?}"),
            }
        }
        assert_eq!(backend.statement_counts().since(&before).total(), 0);

        // A body over the 8 MiB CAS bound (ADR-aacruzgon-0053): `update`
        // refuses it before any write, so only reads are spent (the pre-read and
        // `update`'s own read) and nothing is written. (A SearchParameter `code` over 128 bytes no
        // longer serves: ADR-aacruzgon-0079 removed that refusal.) The
        // SearchParameter exists, so the pre-read finds it.
        let search_parameter = |code: String| {
            json!({
                "resourceType": "SearchParameter",
                "id": "sp",
                "url": "http://example.org/sp",
                "status": "active",
                "code": code,
                "base": ["Patient"],
                "type": "token",
                "expression": "Patient.gender"
            })
        };
        backend
            .create(
                &t,
                "SearchParameter",
                search_parameter("short".to_string()),
                crate::backends::cassandra::test_support::test_fhir_version(),
            )
            .await
            .expect("create the SearchParameter");
        let before = backend.statement_counts();
        match backend
            .update_with_match(&t, "SearchParameter", "sp", "*", {
                let mut oversized = search_parameter("short".to_string());
                oversized["description"] = json!("d".repeat(9 * 1024 * 1024));
                oversized
            })
            .await
        {
            Err(StorageError::Validation(_)) => {}
            other => panic!("expected a validation refusal, got {other:?}"),
        }
        let spent = backend.statement_counts().since(&before);
        assert_eq!(
            (spent.read, spent.total()),
            (2, 2),
            "the pre-read and `update`'s own read of the row, nothing written"
        );
        assert_eq!(
            backend
                .current_version(&t, "SearchParameter", "sp")
                .await
                .unwrap()
                .as_deref(),
            Some("1")
        );
    }

    /// Another writer landing between the pre-read and the compare-and-swap,
    /// for both `_with_match` methods: their compare-and-swap misses, so
    /// nothing of theirs is written. Another update is 409; a delete is
    /// `NotFound`, the answer for a deleted resource. The resource is left
    /// at the other writer's version 3, with no version 4.
    #[tokio::test]
    async fn with_match_loses_to_a_writer_between_its_read_and_its_write() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant("wm-between");
        for (method, deleting) in [
            ("update", false),
            ("update", true),
            ("delete", false),
            ("delete", true),
        ] {
            let id = format!("{method}-{}", if deleting { "deleted" } else { "updated" });
            let context = format!("{method}_with_match, another writer's {}", {
                if deleting { "delete" } else { "update" }
            });
            create(&backend, &t, &id, "One").await;
            assert_eq!(engine_update(&backend, &t, &id, "Two").await, 2);
            another_writer_before_the_next_write(&backend, &t, &id, 2, deleting).await;

            let before = backend.statement_counts();
            let result = if method == "update" {
                backend
                    .update_with_match(&t, "Patient", &id, "W/\"2\"", patient(&id, "Ours"))
                    .await
                    .map(|_| ())
            } else {
                backend
                    .delete_with_match(&t, "Patient", &id, "W/\"2\"")
                    .await
            };
            match (deleting, &result) {
                (
                    false,
                    Err(StorageError::Concurrency(ConcurrencyError::VersionConflict { .. })),
                )
                | (true, Err(StorageError::Resource(ResourceError::NotFound { .. }))) => {}
                _ => panic!("{context}: {result:?}"),
            }
            // Its own compare-and-swap was sent, so the other writer landed
            // after the reads, not before them: a pre-read that saw it
            // would have refused with no CAS at all.
            assert!(
                backend.statement_counts().since(&before).cas >= 1,
                "{context}: the compare-and-swap ran and missed"
            );

            // The other writer's version 3 stands, and ours never landed.
            let row = current(&backend, &t, &id).await;
            assert_eq!(row.version_id, 3, "{context}");
            assert_eq!(row.is_deleted(), deleting, "{context}");
            if !deleting {
                let live = backend.read(&t, "Patient", &id).await.unwrap().unwrap();
                assert_eq!(family(&live), "Two", "{context}");
            }
            assert_eq!(
                backend.list_versions(&t, "Patient", &id).await.unwrap(),
                ["1", "2"],
                "{context}: the other writer wrote no version row, and ours none"
            );
        }
    }

    /// `exists`: one R1 of `deleted_at` only, true for a live row, false for
    /// a deleted or unknown one, never an error for either; true again once
    /// revived, and false once deleted again.
    #[tokio::test]
    async fn exists_reads_only_deleted_at() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant("exists");
        create(&backend, &t, "live", "Live").await;
        create(&backend, &t, "gone", "Gone").await;
        assert_eq!(engine_delete(&backend, &t, "gone").await, 2);
        for (id, expected) in [("live", true), ("gone", false), ("nobody", false)] {
            let before = backend.statement_counts();
            assert_eq!(
                backend.exists(&t, "Patient", id).await.unwrap(),
                expected,
                "{id}"
            );
            let spent = backend.statement_counts().since(&before);
            assert_eq!((spent.read, spent.total()), (1, 1), "{id}");
        }
        assert!(
            !backend
                .exists(&tenant("exists-other"), "Patient", "live")
                .await
                .unwrap()
        );
        // Revived through the engine: live again.
        let (body, data) = normalized_body(
            "create_or_update",
            "Patient",
            "gone",
            patient("gone", "Back"),
        )
        .unwrap();
        assert_eq!(
            through_the_engine(&backend, &t, "gone", |previous| {
                CasPlan::revive(previous, body, data, vec![IndexKey::ResourcesByType])
            })
            .await,
            3
        );
        assert!(backend.exists(&t, "Patient", "gone").await.unwrap());
        // Deleted again after the revive: false again.
        assert_eq!(engine_delete(&backend, &t, "gone").await, 4);
        assert!(!backend.exists(&t, "Patient", "gone").await.unwrap());
    }

    /// `current_version`: one R1 of `version_id` and `deleted_at` only. The
    /// live row's version; `None` for a deleted or unknown resource, as the
    /// trait documents, where the trait default's `read` would answer
    /// `Gone`; the new version again once the resource is revived, and
    /// `None` once it is deleted again.
    #[tokio::test]
    async fn current_version_is_none_for_a_deleted_resource() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant("current");
        create(&backend, &t, "live", "One").await;
        assert_eq!(engine_update(&backend, &t, "live", "Two").await, 2);
        create(&backend, &t, "gone", "Gone").await;
        assert_eq!(engine_delete(&backend, &t, "gone").await, 2);
        for (id, expected) in [("live", Some("2")), ("gone", None), ("nobody", None)] {
            let before = backend.statement_counts();
            assert_eq!(
                backend
                    .current_version(&t, "Patient", id)
                    .await
                    .unwrap()
                    .as_deref(),
                expected,
                "{id}"
            );
            let spent = backend.statement_counts().since(&before);
            assert_eq!((spent.read, spent.total()), (1, 1), "{id}");
        }
        // What the trait default, which reads through `read`, would answer.
        assert!(matches!(
            backend.read(&t, "Patient", "gone").await,
            Err(StorageError::Resource(ResourceError::Gone { .. }))
        ));
        assert_eq!(
            backend
                .current_version(&tenant("current-other"), "Patient", "live")
                .await
                .unwrap(),
            None
        );
        // Revived through the engine: its new version.
        let (body, data) = normalized_body(
            "create_or_update",
            "Patient",
            "gone",
            patient("gone", "Back"),
        )
        .unwrap();
        assert_eq!(
            through_the_engine(&backend, &t, "gone", |previous| {
                CasPlan::revive(previous, body, data, vec![IndexKey::ResourcesByType])
            })
            .await,
            3
        );
        assert_eq!(
            backend
                .current_version(&t, "Patient", "gone")
                .await
                .unwrap()
                .as_deref(),
            Some("3")
        );
        // Deleted again after the revive: `None` again, as for the first
        // delete.
        assert_eq!(engine_delete(&backend, &t, "gone").await, 4);
        assert_eq!(
            backend
                .current_version(&t, "Patient", "gone")
                .await
                .unwrap(),
            None
        );
    }

    /// `read_batch` over more ids than it reads at once: the input order,
    /// with unknown and deleted ids left out, duplicates kept, as the trait
    /// default.
    #[tokio::test]
    async fn read_batch_keeps_the_input_order_over_a_mix() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant("batch");
        let ids: Vec<String> = (0..20).map(|n| format!("p{n:02}")).collect();
        for id in &ids {
            create(&backend, &t, id, id).await;
        }
        for deleted in ["p03", "p08", "p15"] {
            assert_eq!(engine_delete(&backend, &t, deleted).await, 2);
        }
        assert_eq!(engine_update(&backend, &t, "p11", "eleven").await, 2);

        // Reversed, with unknown ids and a duplicate mixed in.
        let mut asked: Vec<&str> = ids.iter().rev().map(String::as_str).collect();
        asked.insert(4, "nobody");
        asked.insert(10, "p19");
        asked.push("also-nobody");
        let expected: Vec<&str> = asked
            .iter()
            .copied()
            .filter(|id| !["nobody", "also-nobody", "p03", "p08", "p15"].contains(id))
            .collect();

        let before = backend.statement_counts();
        let found = backend.read_batch(&t, "Patient", &asked).await.unwrap();
        let spent = backend.statement_counts().since(&before);
        assert_eq!(found.iter().map(|r| r.id()).collect::<Vec<_>>(), expected);
        assert_eq!(spent.read as usize, asked.len(), "one plain read per id");
        assert_eq!(spent.serial_read, 0, "never at LOCAL_SERIAL");
        let eleven = found.iter().find(|r| r.id() == "p11").unwrap();
        assert_eq!((eleven.version_id(), family(eleven)), ("2", "eleven"));
        assert!(found.iter().all(|r| !r.is_deleted()));

        assert!(
            backend
                .read_batch(&t, "Patient", &[])
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            backend
                .read_batch(&tenant("batch-other"), "Patient", &asked)
                .await
                .unwrap()
                .is_empty()
        );
    }

    /// A failed read is mapped as a plain read's (`errors.rs`): timed out
    /// 504, failed 503, never `Ok`. `read_batch` fails as a whole on any
    /// such error. (`list_versions` pages, and the seam does not reach paged
    /// statements.)
    #[tokio::test]
    async fn read_failures_are_mapped_as_reads() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant("faults");
        create(&backend, &t, "p", "One").await;
        create(&backend, &t, "q", "Two").await;
        let timed_out = |result: &str, error: StorageError| match error {
            StorageError::Backend(BackendError::Timeout { .. }) => {}
            other => panic!("{result}: expected a timeout, got {other:?}"),
        };
        let unavailable = |result: &str, error: StorageError| match error {
            StorageError::Backend(BackendError::Unavailable { .. }) => {}
            other => panic!("{result}: expected unavailable, got {other:?}"),
        };

        backend.session.inject_fault(
            StatementKind::Read,
            "FROM resource_versions",
            Fault::TimeOutWithoutApplying,
        );
        timed_out(
            "vread",
            backend.vread(&t, "Patient", "p", "1").await.unwrap_err(),
        );
        backend.session.inject_fault(
            StatementKind::Read,
            "FROM resource_versions",
            Fault::FailWithUnavailable,
        );
        unavailable(
            "vread",
            backend.vread(&t, "Patient", "p", "1").await.unwrap_err(),
        );

        backend.session.inject_fault(
            StatementKind::Read,
            "SELECT deleted_at FROM resources",
            Fault::TimeOutWithoutApplying,
        );
        timed_out(
            "exists",
            backend.exists(&t, "Patient", "p").await.unwrap_err(),
        );
        backend.session.inject_fault(
            StatementKind::Read,
            "SELECT deleted_at FROM resources",
            Fault::FailWithWriteFailure,
        );
        unavailable(
            "exists",
            backend.exists(&t, "Patient", "p").await.unwrap_err(),
        );

        backend.session.inject_fault(
            StatementKind::Read,
            "SELECT version_id, deleted_at FROM resources",
            Fault::TimeOutWithoutApplying,
        );
        timed_out(
            "current_version",
            backend
                .current_version(&t, "Patient", "p")
                .await
                .unwrap_err(),
        );
        backend.session.inject_fault(
            StatementKind::Read,
            "SELECT version_id, deleted_at FROM resources",
            Fault::FailWithUnavailable,
        );
        unavailable(
            "current_version",
            backend
                .current_version(&t, "Patient", "p")
                .await
                .unwrap_err(),
        );

        backend.session.inject_fault(
            StatementKind::Read,
            "FROM resources WHERE",
            Fault::FailWithUnavailable,
        );
        unavailable(
            "delete_with_match",
            backend
                .delete_with_match(&t, "Patient", "p", "W/\"1\"")
                .await
                .unwrap_err(),
        );

        // One of the batch's reads fails: the batch fails.
        backend.session.inject_fault(
            StatementKind::Read,
            "FROM resources WHERE",
            Fault::TimeOutWithoutApplying,
        );
        timed_out(
            "read_batch",
            backend
                .read_batch(&t, "Patient", &["p", "q", "nobody"])
                .await
                .unwrap_err(),
        );
        // The fault fired once; the node is fine.
        assert_eq!(
            backend
                .read_batch(&t, "Patient", &["p", "q"])
                .await
                .unwrap()
                .len(),
            2
        );
    }
}
