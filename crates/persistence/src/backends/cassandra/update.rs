//! `update` (roadmap Step 3, layer L2a), and the one C2 attempt
//! `create_or_update`'s branch b shares with it.
//!
//! The walkthrough's 3.3 (`docs/Cassandra/schema/walkthrough.md`), with
//! plain `update`'s V3 built from R1 (Step 3 plan D5):
//!
//! 1. **Step 1, the operation's own.** `tenant.check_permission(
//!    Operation::Update, type)`, then the G38 bound on a SearchParameter's
//!    `code` ([`check_search_parameter_code`]): either refusal comes before
//!    any statement.
//! 2. **R1** ([`CassandraBackend::read_resource_row`]) of `resources`, for
//!    `K(N)` (G22) and for V3's `data` and `w(N)` (D5). Its version decides
//!    what happens next ([`r1_case`], walkthrough `:176-180`):
//!    - at `N` and live: the attempt;
//!    - past `N`: the miss C2 would give, without running it: `NotFound` if
//!      the row is deleted, otherwise `VersionConflict`;
//!    - below `N`: `N`'s compare-and-swap is still committing. R2, the same
//!      read at `LOCAL_SERIAL`, completes it ([`CassandraBackend::
//!      serial_resource_row`]), and its row is judged the same way. A row
//!      still below `N` after R2 is a `VersionConflict`;
//!    - no row, or deleted at `N`: `NotFound` (G9), with nothing written.
//! 3. **The pre-read check** of ADR-aacruzgon-0051
//!    ([`CassandraBackend::check_earlier_attempts`]). `update` uses one
//!    request and sends at most one plan, so it is free; it is called so that
//!    the rule holds however the request is used.
//! 4. **[`CassandraBackend::update_attempt`]**: one C2 through the engine
//!    ([`CasPlan::update`], whose V3 is R1's row `N`, `w(N)` included). The
//!    engine runs V3, I1, C2, the unknown-outcome table, V1, the X1 move and
//!    I2.
//! 5. **The answer.** An applied attempt is returned with its version, the
//!    content the attempt wrote, `current`'s `created_at` (G14) and
//!    `current`'s `fhir_version` (G13; C2 binds R1's, which is the same).
//!    A definite miss is its error ([`CasMiss::into_error`]): `NotFound` for
//!    no row or a deleted one, `VersionConflict` for another version.
//!    `update` never retries: its caller named the version it read
//!    (ADR-aacruzgon-0050). A stored SearchParameter is replaced in this
//!    process's overlay (D9).

use cdrs_tokio::types::value::Value;
use serde_json::Value as Json;

use super::backend::CassandraBackend;
use super::errors::map_driver_error;
use super::index_keys::IndexKey;
use super::read::{ResourceRow, SELECT_RESOURCE};
use super::session::StatementKind;
use super::write::{
    CasMiss, CasOutcome, CasPlan, CasRequest, PreviousVersion, RowKey, check_search_parameter_code,
    normalized_body,
};
use crate::error::{ConcurrencyError, StorageError, StorageResult};
use crate::tenant::{Operation, TenantContext};
use crate::types::StoredResource;

/// What `update` does with the row R1 (or R2) returned, for `current` at
/// version `N` (walkthrough `:176-180`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum R1Case {
    /// The row is live at `N`: run the attempt from it.
    Attempt,
    /// The row is below `N`: read it again at `LOCAL_SERIAL`.
    SerialRead,
    /// The miss C2 would give, without running it.
    Miss(CasMiss),
}

/// The pure half of R1's version cases: `row` against `expected`, the `N`
/// of `current`.
fn r1_case(row: Option<&ResourceRow>, expected: i32) -> R1Case {
    let Some(row) = row else {
        return R1Case::Miss(CasMiss::NoRow);
    };
    if row.version_id < expected {
        return R1Case::SerialRead;
    }
    if row.is_deleted() {
        return R1Case::Miss(CasMiss::Deleted);
    }
    if row.version_id > expected {
        return R1Case::Miss(CasMiss::VersionMoved {
            current: Some(row.version_id),
        });
    }
    R1Case::Attempt
}

/// The case after R2: as [`r1_case`], except that a row still below `N` is
/// not read again. R2 has completed any proposal in flight, so the row is
/// what it is: `NotFound` if it is deleted, otherwise a version conflict.
fn r2_case(row: Option<&ResourceRow>, expected: i32) -> R1Case {
    match (r1_case(row, expected), row) {
        (R1Case::SerialRead, Some(row)) if row.is_deleted() => R1Case::Miss(CasMiss::Deleted),
        (R1Case::SerialRead, Some(row)) => R1Case::Miss(CasMiss::VersionMoved {
            current: Some(row.version_id),
        }),
        (case, _) => case,
    }
}

impl CassandraBackend {
    /// `ResourceStorage::update`; see the module docs.
    pub(super) async fn update_resource(
        &self,
        tenant: &TenantContext,
        current: &StoredResource,
        resource: Json,
    ) -> StorageResult<StoredResource> {
        let resource_type = current.resource_type();
        let id = current.id();

        // Step 1.
        tenant.check_permission(Operation::Update, resource_type)?;
        check_search_parameter_code(resource_type, &resource)?;

        let tenant_id = tenant.tenant_id().as_str();
        let Ok(expected) = current.version_id().parse::<i32>() else {
            // Not a version this backend wrote, so no row can be at it.
            return Err(self
                .unversioned_current(tenant_id, resource_type, id, current.version_id())
                .await);
        };

        // R1, and R2 when R1 is behind `current`.
        let mut row = self.read_resource_row(tenant_id, resource_type, id).await?;
        let mut case = r1_case(row.as_ref(), expected);
        if case == R1Case::SerialRead {
            row = self
                .serial_resource_row(tenant_id, resource_type, id)
                .await?;
            case = r2_case(row.as_ref(), expected);
        }
        let row = match (case, row) {
            (R1Case::Attempt, Some(row)) => row,
            (R1Case::Miss(miss), _) => return Err(miss.into_error(resource_type, id, expected)),
            // `r1_case` answers `Attempt` only for a row, and `r2_case`
            // never answers `SerialRead`.
            (R1Case::Attempt | R1Case::SerialRead, _) => {
                return Err(CasMiss::NoRow.into_error(resource_type, id, expected));
            }
        };

        let key = RowKey {
            tenant_id,
            resource_type,
            id,
        };
        let mut request = self.cas_request("update", key);
        let applied = match self
            .check_earlier_attempts(&mut request, Some(&row))
            .await?
        {
            Some(applied) => applied,
            None => match self.update_attempt(&mut request, &row, resource).await? {
                CasOutcome::Applied(applied) => applied,
                CasOutcome::Missed(miss) => {
                    return Err(miss.into_error(resource_type, id, expected));
                }
            },
        };

        let content = applied.plan.body().cloned().unwrap_or(Json::Null);
        if resource_type == "SearchParameter" {
            self.replace_stored_search_parameter(tenant_id, id, &content, applied.last_updated);
        }
        Ok(StoredResource::from_storage(
            resource_type,
            id,
            applied.version_id.to_string(),
            tenant.tenant_id().clone(),
            content,
            current.created_at(),
            applied.last_updated,
            None,
            current.fhir_version(),
        ))
    }

    /// One C2 attempt of `request`: update the live row `row` (R1, at version
    /// `N`) to `N + 1` with `resource`, through
    /// [`run_cas`](Self::run_cas). Returns the engine's outcome: `Applied`
    /// with the attempt's values, or the definite miss, which the caller maps
    /// (`update`) or answers with its next turn (`create_or_update`).
    ///
    /// The caller has already checked the permission and the G38 bound, and
    /// has run [`check_earlier_attempts`](Self::check_earlier_attempts) on
    /// `row`.
    ///
    /// The plan's V3 is `row` itself: `N`'s `data`, `lu(N)`, `w(N)` and
    /// `K(N)` (D5, G22, G40), and its `fhir_version` is `row`'s (G13). `K(N+1)`
    /// is the `resources_by_type` row until Step 5.
    pub(crate) async fn update_attempt(
        &self,
        request: &mut CasRequest<'_>,
        row: &ResourceRow,
        resource: Json,
    ) -> StorageResult<CasOutcome> {
        let key = request.key();
        let previous = PreviousVersion::from_row(row)?;
        let (body, data) = normalized_body("update", key.resource_type, key.id, resource)?;
        let plan = CasPlan::update(previous, body, data, vec![IndexKey::ResourcesByType]);
        self.run_cas(request, plan).await
    }

    /// R2 for `update`: R1's text at `LOCAL_SERIAL`, which completes or
    /// cancels a proposal still in flight on the partition (walkthrough
    /// `:179`, `:300-301`). It runs before this update's own CAS, not after
    /// an unknown outcome, so nothing this request wrote can be stored yet,
    /// and its error goes through the plain table (`errors.rs`,
    /// [`map_driver_error`]): 504 for a timeout; 503 for `Unavailable`, a
    /// replica failure or a lost connection; 500 for a node-side exception,
    /// a refused statement or a decoding error. It is never classified as a
    /// CAS's (ADR-aacruzgon-0050): a `WriteTimeout`/CAS from a Paxos v2
    /// serial read is the read timing out, so 504.
    async fn serial_resource_row(
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
                StatementKind::SerialRead,
            )
            .await
            .map_err(|error| {
                StorageError::Backend(map_driver_error(&error, "update serial read"))
            })?;
        rows.first().map(ResourceRow::decode).transpose()
    }

    /// The error for a `current` whose version is not a number, so not one
    /// this backend wrote: R1 tells `NotFound` (no row, or deleted) from a
    /// conflict with the row's real version. Nothing is written.
    async fn unversioned_current(
        &self,
        tenant_id: &str,
        resource_type: &str,
        id: &str,
        expected: &str,
    ) -> StorageError {
        match self.read_resource_row(tenant_id, resource_type, id).await {
            Err(error) => error,
            Ok(Some(row)) if !row.is_deleted() => {
                StorageError::Concurrency(ConcurrencyError::VersionConflict {
                    resource_type: resource_type.to_string(),
                    id: id.to_string(),
                    expected_version: expected.to_string(),
                    actual_version: row.version_id.to_string(),
                })
            }
            Ok(_) => CasMiss::NoRow.into_error(resource_type, id, 0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ResourceError;
    use chrono::{TimeZone, Utc};
    use helios_fhir::FhirVersion;

    fn row(version_id: i32, deleted: bool) -> ResourceRow {
        let at = Utc.timestamp_millis_opt(1_000).single().expect("time");
        ResourceRow {
            version_id,
            data: (!deleted).then(|| "{}".to_string()),
            last_updated: at,
            created_at: Some(at),
            deleted_at: deleted.then_some(at),
            fhir_version: FhirVersion::default_enabled(),
            write_ts: Some(1_000_000),
            write_id: None,
            index_keys: Some(vec![0x01, 0x00]),
        }
    }

    /// Walkthrough `:176-180`, for `current` at version 3.
    #[test]
    fn r1_version_cases_follow_the_walkthrough() {
        let moved = |v| R1Case::Miss(CasMiss::VersionMoved { current: Some(v) });
        for (case, row, expected) in [
            ("no row", None, R1Case::Miss(CasMiss::NoRow)),
            ("at N, live", Some(row(3, false)), R1Case::Attempt),
            (
                "at N, deleted",
                Some(row(3, true)),
                R1Case::Miss(CasMiss::Deleted),
            ),
            ("past N, live", Some(row(4, false)), moved(4)),
            (
                "past N, deleted",
                Some(row(5, true)),
                R1Case::Miss(CasMiss::Deleted),
            ),
            ("below N, live", Some(row(2, false)), R1Case::SerialRead),
            ("below N, deleted", Some(row(2, true)), R1Case::SerialRead),
        ] {
            assert_eq!(r1_case(row.as_ref(), 3), expected, "R1 {case}");
        }
    }

    /// After R2, a row still below `N` is not read a third time.
    #[test]
    fn r2_never_asks_for_another_read() {
        let moved = |v| R1Case::Miss(CasMiss::VersionMoved { current: Some(v) });
        for (case, row, expected) in [
            ("no row", None, R1Case::Miss(CasMiss::NoRow)),
            ("completed to N", Some(row(3, false)), R1Case::Attempt),
            (
                "completed to a delete at N",
                Some(row(3, true)),
                R1Case::Miss(CasMiss::Deleted),
            ),
            ("past N", Some(row(4, false)), moved(4)),
            ("still below N", Some(row(2, false)), moved(2)),
            (
                "still below N, deleted",
                Some(row(2, true)),
                R1Case::Miss(CasMiss::Deleted),
            ),
        ] {
            assert_eq!(r2_case(row.as_ref(), 3), expected, "R2 {case}");
        }
    }

    /// The miss mapping `update` returns: `NotFound` for no row and for a
    /// deleted one (G9, never `Gone`), and `VersionConflict` naming `N` and
    /// the row's version.
    #[test]
    fn misses_map_to_not_found_or_a_version_conflict() {
        for miss in [CasMiss::NoRow, CasMiss::Deleted] {
            assert!(matches!(
                miss.into_error("Patient", "p", 3),
                StorageError::Resource(ResourceError::NotFound { .. })
            ));
        }
        match (CasMiss::VersionMoved { current: Some(4) }).into_error("Patient", "p", 3) {
            StorageError::Concurrency(ConcurrencyError::VersionConflict {
                expected_version,
                actual_version,
                ..
            }) => assert_eq!(
                (expected_version.as_str(), actual_version.as_str()),
                ("3", "4")
            ),
            other => panic!("expected a version conflict, got {other:?}"),
        }
    }
}

/// Live tests against the node at `HFS_TEST_CASSANDRA_URL` (skipped when
/// unset), in the style of `write.rs`'s `node_tests`. Each test writes under
/// its own tenant (ADR-aacruzgon-0020).
#[cfg(test)]
mod node_tests {
    use cdrs_tokio::types::IntoRustByName;
    use cdrs_tokio::types::blob::Blob;
    use chrono::{DateTime, Utc};
    use helios_fhir::FhirVersion;
    use uuid::Uuid;

    use super::*;
    use crate::backends::cassandra::CassandraConfig;
    use crate::backends::cassandra::index_keys;
    use crate::backends::cassandra::session::Fault;
    use crate::backends::cassandra::write::{
        SOFT_DELETE_RESOURCE_IF_VERSION, UPDATE_RESOURCE_IF_VERSION, write_ts_of,
    };
    use crate::core::{ResourceStorage, SearchProvider};
    use crate::error::{BackendError, ResourceError, TenantError};
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

    macro_rules! backend_or_skip {
        () => {
            match backend().await {
                Some(backend) => backend,
                None => {
                    eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
                    return;
                }
            }
        };
    }

    fn tenant_context(case: &str) -> TenantContext {
        TenantContext::new(
            TenantId::new(format!("test-update-{case}-{}", Uuid::new_v4().simple())),
            TenantPermissions::full_access(),
        )
    }

    fn patient(family: &str) -> Json {
        serde_json::json!({
            "resourceType": "Patient",
            "id": "p",
            "name": [{"family": family}]
        })
    }

    async fn create(backend: &CassandraBackend, tenant: &TenantContext) -> StoredResource {
        backend
            .create(
                tenant,
                "Patient",
                patient("Smith"),
                FhirVersion::default_enabled(),
            )
            .await
            .expect("create")
    }

    /// R1 of `Patient/p`.
    async fn current_row(backend: &CassandraBackend, tenant: &TenantContext) -> ResourceRow {
        backend
            .read_resource_row(tenant.tenant_id().as_str(), "Patient", "p")
            .await
            .expect("R1")
            .expect("a row")
    }

    /// A `current` claiming version `version` of `Patient/p`, as REST would
    /// hand it over after its own read.
    fn claimed(tenant: &TenantContext, version: i32) -> StoredResource {
        let now = Utc::now();
        StoredResource::from_storage(
            "Patient",
            "p",
            version.to_string(),
            tenant.tenant_id().clone(),
            patient("Smith"),
            now,
            now,
            None,
            FhirVersion::default_enabled(),
        )
    }

    /// Every version row of `Patient/p`: `(version_id, write_id, deleted)`.
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
        let mut versions: Vec<(i32, Option<Uuid>, bool)> = rows
            .iter()
            .map(|row| {
                (
                    row.get_r_by_name("version_id").unwrap(),
                    row.get_by_name("write_id").unwrap(),
                    IntoRustByName::<DateTime<Utc>>::get_by_name(row, "deleted_at")
                        .unwrap()
                        .is_some(),
                )
            })
            .collect();
        versions.sort();
        versions
    }

    /// C2's bind values for moving `Patient/p` from `from` to `from + 1`
    /// with `write_id`, outside the engine (another writer).
    fn c2_values(
        tenant: &TenantContext,
        from: i32,
        write_id: Uuid,
        lu: DateTime<Utc>,
    ) -> Vec<Value> {
        vec![
            Value::from(from + 1),
            Value::from(patient("Other").to_string()),
            Value::from(lu),
            Value::from(FhirVersion::default_enabled().as_mime_param()),
            Value::from(write_ts_of(lu)),
            Value::from(write_id),
            Value::from(Blob::from(index_keys::encode(&[IndexKey::ResourcesByType]))),
            Value::from(tenant.tenant_id().as_str()),
            Value::from("Patient"),
            Value::from("p"),
            Value::from(from),
        ]
    }

    /// Sends another writer's C2 from `from`, which the fault seam holds back
    /// and applies just before the next statement of kind `before`.
    async fn another_writer_late(
        backend: &CassandraBackend,
        tenant: &TenantContext,
        from: i32,
        write_id: Uuid,
        before: StatementKind,
    ) {
        backend.session.inject_fault(
            StatementKind::Cas,
            "UPDATE resources",
            Fault::ApplyLate { before },
        );
        let lu = super::super::write::next_last_updated(Utc::now(), None)
            + chrono::Duration::milliseconds(50);
        let sent = backend
            .session
            .rows(
                UPDATE_RESOURCE_IF_VERSION,
                c2_values(tenant, from, write_id, lu),
                StatementKind::Cas,
            )
            .await;
        assert!(
            sent.is_err(),
            "the seam answers the held-back CAS with a timeout"
        );
    }

    /// Sends another writer's C3 (soft delete from `from`), which the fault
    /// seam holds back and applies just before the next statement of kind
    /// `before`.
    async fn another_deleter_late(
        backend: &CassandraBackend,
        tenant: &TenantContext,
        from: i32,
        before: StatementKind,
    ) {
        backend.session.inject_fault(
            StatementKind::Cas,
            "UPDATE resources",
            Fault::ApplyLate { before },
        );
        let lu = super::super::write::next_last_updated(Utc::now(), None)
            + chrono::Duration::milliseconds(50);
        let sent = backend
            .session
            .rows(
                SOFT_DELETE_RESOURCE_IF_VERSION,
                vec![
                    Value::from(from + 1),
                    Value::from(lu),
                    Value::from(lu),
                    Value::from(write_ts_of(lu)),
                    Value::from(Uuid::new_v4()),
                    Value::from(tenant.tenant_id().as_str()),
                    Value::from("Patient"),
                    Value::from("p"),
                    Value::from(from),
                ],
                StatementKind::Cas,
            )
            .await;
        assert!(
            sent.is_err(),
            "the seam answers the held-back CAS with a timeout"
        );
    }

    /// Walkthrough 3.3's C2 table, as real CAS misses: R1 saw `N` live, and
    /// another writer's C2 or C3 landed between R1 and our C2 (held back by
    /// the seam until just before it). `[applied] False` with another
    /// `version_id` is `VersionConflict`; with `deleted_at > 0` it is
    /// `NotFound`, never `Gone` (G9). No serial read: the miss is definite.
    #[tokio::test]
    async fn c2_misses_through_update_map_to_a_conflict_or_not_found() {
        let backend = backend_or_skip!();

        let tenant = tenant_context("miss-moved");
        let current = create(&backend, &tenant).await;
        let theirs = Uuid::new_v4();
        another_writer_late(&backend, &tenant, 1, theirs, StatementKind::Cas).await;
        let before = backend.cas_stats();
        let counts = backend.statement_counts();
        match backend.update(&tenant, &current, patient("Mine")).await {
            Err(StorageError::Concurrency(ConcurrencyError::VersionConflict {
                expected_version,
                actual_version,
                ..
            })) => assert_eq!(
                (expected_version.as_str(), actual_version.as_str()),
                ("1", "2")
            ),
            other => panic!("expected a version conflict, got {other:?}"),
        }
        let stats = backend.cas_stats().since(&before);
        let spent = backend.statement_counts().since(&counts);
        eprintln!("[cas case=c2-miss-moved {stats}]");
        assert_eq!(
            (stats.attempts, stats.applied, stats.miss_version_conflict),
            (1, 0, 1)
        );
        assert_eq!(spent.serial_read, 0, "a definite miss needs no R2");
        assert_eq!(current_row(&backend, &tenant).await.write_id, Some(theirs));

        let tenant = tenant_context("miss-deleted");
        let current = create(&backend, &tenant).await;
        another_deleter_late(&backend, &tenant, 1, StatementKind::Cas).await;
        let before = backend.cas_stats();
        assert!(matches!(
            backend.update(&tenant, &current, patient("Mine")).await,
            Err(StorageError::Resource(ResourceError::NotFound { .. }))
        ));
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c2-miss-deleted {stats}]");
        assert_eq!((stats.attempts, stats.miss_deleted), (1, 1));
        assert!(current_row(&backend, &tenant).await.is_deleted());
    }

    /// The applied path: version `N + 1`, `current`'s `created_at` and
    /// `fhir_version`, the X1 row moved, and V3 built from R1 with `w(N)`
    /// (D5), which restores a missing row `N` (G40).
    #[tokio::test]
    async fn update_writes_n_plus_one_from_r1() {
        let backend = backend_or_skip!();
        let tenant = tenant_context("applied");
        let current = create(&backend, &tenant).await;
        let row1 = current_row(&backend, &tenant).await;
        tokio::time::sleep(std::time::Duration::from_millis(3)).await;

        let before = backend.statement_counts();
        let mut body = patient("Jones");
        body["meta"] =
            serde_json::json!({"versionId": "99", "lastUpdated": "2001-01-01T00:00:00Z"});
        let updated = backend
            .update(&tenant, &current, body)
            .await
            .expect("update");
        let spent = backend.statement_counts().since(&before);
        eprintln!("[statements op=update {spent}]");
        // R1; V3, I1, V1, X1 delete, X1 insert, I2; one C2.
        assert_eq!(
            (spent.read, spent.serial_read, spent.write, spent.cas),
            (1, 0, 6, 1)
        );

        assert_eq!(updated.version_id(), "2");
        assert_eq!(
            updated.created_at(),
            current.created_at(),
            "created_at untouched"
        );
        assert_eq!(
            updated.fhir_version(),
            current.fhir_version(),
            "fhir_version kept"
        );
        assert!(updated.last_modified() > current.last_modified());
        assert_eq!(updated.content()["name"][0]["family"], "Jones");
        let meta = updated.content_with_meta();
        assert_eq!(
            meta["meta"]["versionId"], "2",
            "the client's versionId is ignored"
        );

        let row2 = current_row(&backend, &tenant).await;
        assert_eq!(row2.version_id, 2);
        assert_eq!(
            row2.created_at, row1.created_at,
            "C2 does not write created_at"
        );
        assert_eq!(row2.fhir_version, row1.fhir_version);
        assert_eq!(row2.last_updated, updated.last_modified());
        assert_eq!(
            backend.count(&tenant, Some("Patient")).await.unwrap(),
            1,
            "X1 moved"
        );
        let read = backend
            .read(&tenant, "Patient", "p")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read.version_id(), "2");
        assert_eq!(read.content()["name"][0]["family"], "Jones");
        assert_eq!(
            version_rows(&backend, &tenant).await,
            vec![(1, row1.write_id, false), (2, row2.write_id, false)]
        );
    }

    /// D5 and G40 on the node: row 1 was never written (its writer stopped
    /// after its CAS), and `update`'s V3 restores it from R1, `write_id`
    /// included.
    #[tokio::test]
    async fn v3_restores_a_missing_row_n_with_its_write_id() {
        let backend = backend_or_skip!();
        let tenant = tenant_context("v3");
        let writer = Uuid::new_v4();
        let lu = super::super::write::next_last_updated(Utc::now(), None);
        let rows = backend
            .session
            .rows(
                super::super::write::INSERT_RESOURCE_IF_NOT_EXISTS,
                vec![
                    Value::from(tenant.tenant_id().as_str()),
                    Value::from("Patient"),
                    Value::from("p"),
                    Value::from(patient("Smith").to_string()),
                    Value::from(lu),
                    Value::from(lu),
                    Value::from(FhirVersion::default_enabled().as_mime_param()),
                    Value::from(write_ts_of(lu)),
                    Value::from(writer),
                    Value::from(Blob::from(index_keys::encode(&[IndexKey::ResourcesByType]))),
                ],
                StatementKind::Cas,
            )
            .await
            .expect("C1 alone");
        assert!(!rows.is_empty());
        assert!(version_rows(&backend, &tenant).await.is_empty());

        let current = backend
            .read(&tenant, "Patient", "p")
            .await
            .unwrap()
            .unwrap();
        backend
            .update(&tenant, &current, patient("Jones"))
            .await
            .expect("update");
        let versions = version_rows(&backend, &tenant).await;
        assert_eq!(versions.len(), 2);
        assert_eq!(
            versions[0],
            (1, Some(writer), false),
            "V3 carries w(N) (D5)"
        );
    }

    /// Walkthrough `:176-180` on the node: R1 at a higher version, deleted,
    /// absent, or lower (completed by R2, or still lower after it).
    #[tokio::test]
    async fn r1_version_cases_on_the_node() {
        let backend = backend_or_skip!();

        // Past N, live: VersionConflict, without a CAS or a write.
        let tenant = tenant_context("past");
        let v1 = create(&backend, &tenant).await;
        let v2 = backend.update(&tenant, &v1, patient("Two")).await.unwrap();
        let before = backend.statement_counts();
        match backend.update(&tenant, &v1, patient("Stale")).await {
            Err(StorageError::Concurrency(ConcurrencyError::VersionConflict {
                expected_version,
                actual_version,
                ..
            })) => assert_eq!(
                (expected_version.as_str(), actual_version.as_str()),
                ("1", "2")
            ),
            other => panic!("expected a version conflict, got {other:?}"),
        }
        let spent = backend.statement_counts().since(&before);
        assert_eq!(
            (spent.read, spent.write, spent.cas),
            (1, 0, 0),
            "nothing written"
        );

        // Past N, deleted: NotFound, without a CAS (G9: a write never says Gone).
        let row2 = current_row(&backend, &tenant).await;
        let key = RowKey {
            tenant_id: tenant.tenant_id().as_str(),
            resource_type: "Patient",
            id: "p",
        };
        let mut request = backend.cas_request("delete", key);
        let deleted = backend
            .run_cas(
                &mut request,
                CasPlan::delete(PreviousVersion::from_row(&row2).unwrap()),
            )
            .await
            .unwrap();
        assert!(matches!(deleted, CasOutcome::Applied(_)));
        let before = backend.statement_counts();
        assert!(matches!(
            backend.update(&tenant, &v2, patient("Late")).await,
            Err(StorageError::Resource(ResourceError::NotFound { .. }))
        ));
        // At N, deleted (REST read the marker's version): NotFound too.
        assert!(matches!(
            backend
                .update(&tenant, &claimed(&tenant, 3), patient("Late"))
                .await,
            Err(StorageError::Resource(ResourceError::NotFound { .. }))
        ));
        let spent = backend.statement_counts().since(&before);
        assert_eq!((spent.write, spent.cas), (0, 0), "nothing written");
        assert_eq!(current_row(&backend, &tenant).await.version_id, 3);

        // No row: NotFound, after R1 alone.
        let tenant = tenant_context("absent");
        let before = backend.statement_counts();
        assert!(matches!(
            backend
                .update(&tenant, &claimed(&tenant, 1), patient("Ghost"))
                .await,
            Err(StorageError::Resource(ResourceError::NotFound { .. }))
        ));
        let spent = backend.statement_counts().since(&before);
        assert_eq!((spent.read, spent.write, spent.cas), (1, 0, 0));

        // Below N, and N's CAS still committing: R2 completes it, and the
        // update goes ahead from R2's row. Another writer's C2 (1 -> 2) is
        // held back by the seam and applied just before the serial read.
        let tenant = tenant_context("below");
        create(&backend, &tenant).await;
        let theirs = Uuid::new_v4();
        another_writer_late(&backend, &tenant, 1, theirs, StatementKind::SerialRead).await;
        assert_eq!(
            current_row(&backend, &tenant).await.version_id,
            1,
            "not yet"
        );
        let before = backend.statement_counts();
        let updated = backend
            .update(&tenant, &claimed(&tenant, 2), patient("Three"))
            .await
            .expect("update after R2");
        let spent = backend.statement_counts().since(&before);
        eprintln!("[statements op=update-below {spent}]");
        assert_eq!(updated.version_id(), "3");
        assert_eq!((spent.read, spent.serial_read, spent.cas), (1, 1, 1));
        let versions = version_rows(&backend, &tenant).await;
        assert_eq!(
            versions.iter().find(|v| v.0 == 2).map(|v| v.1),
            Some(Some(theirs)),
            "V3 rewrote row 2 from R2's row, with the other writer's write_id"
        );

        // Below N even after R2: VersionConflict, nothing written.
        let before = backend.statement_counts();
        match backend
            .update(&tenant, &claimed(&tenant, 7), patient("Ahead"))
            .await
        {
            Err(StorageError::Concurrency(ConcurrencyError::VersionConflict {
                expected_version,
                actual_version,
                ..
            })) => assert_eq!(
                (expected_version.as_str(), actual_version.as_str()),
                ("7", "3")
            ),
            other => panic!("expected a version conflict, got {other:?}"),
        }
        let spent = backend.statement_counts().since(&before);
        assert_eq!(
            (spent.read, spent.serial_read, spent.write, spent.cas),
            (1, 1, 0, 0)
        );
    }

    /// Step 1 refuses before any statement: the permission, and G38's bound.
    #[tokio::test]
    async fn step_one_refuses_before_any_statement() {
        let backend = backend_or_skip!();
        let tenant = tenant_context("step1");
        let current = create(&backend, &tenant).await;

        let read_only =
            TenantContext::new(tenant.tenant_id().clone(), TenantPermissions::read_only());
        let before = backend.statement_counts();
        assert!(matches!(
            backend.update(&read_only, &current, patient("No")).await,
            Err(StorageError::Tenant(
                TenantError::OperationNotPermitted { .. }
            ))
        ));
        let long_code = serde_json::json!({
            "resourceType": "SearchParameter",
            "id": "sp",
            "url": "http://example.org/sp",
            "status": "active",
            "code": "c".repeat(129),
            "base": ["Patient"],
            "type": "token",
            "expression": "Patient.gender"
        });
        let sp = claimed(&tenant, 1);
        let sp = StoredResource::from_storage(
            "SearchParameter",
            "sp",
            "1",
            tenant.tenant_id().clone(),
            long_code.clone(),
            sp.created_at(),
            sp.last_modified(),
            None,
            FhirVersion::default_enabled(),
        );
        assert!(matches!(
            backend.update(&tenant, &sp, long_code).await,
            Err(StorageError::Validation(_))
        ));
        let spent = backend.statement_counts().since(&before);
        assert_eq!(spent.total(), 0, "{spent}");
    }

    /// Walkthrough 3.1's unknown-outcome table on C2, through `update` and
    /// the fault seam. "Past V with row V ours" needs a superseding V3 that
    /// knows this attempt's `write_id`, which a one-shot fault cannot stage;
    /// the engine's `decide` tests cover it.
    #[tokio::test]
    async fn unknown_c2_outcomes_through_the_fault_seam() {
        let backend = backend_or_skip!();

        // Applied, reply lost: R2 finds our write_id at V. One attempt.
        let tenant = tenant_context("lost-reply");
        let current = create(&backend, &tenant).await;
        backend.session.inject_fault(
            StatementKind::Cas,
            "UPDATE resources",
            Fault::ApplyThenTimeOut,
        );
        let before = backend.cas_stats();
        let updated = backend
            .update(&tenant, &current, patient("Two"))
            .await
            .unwrap();
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c2-lost-reply {stats}]");
        assert_eq!(updated.version_id(), "2");
        assert_eq!((stats.attempts, stats.applied, stats.reruns), (1, 1, 0));
        assert_eq!(stats.unknown_write_timeout_cas, 1);
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 1);

        // Not applied: R2 finds N, one re-run applies, and no version is
        // written twice.
        let tenant = tenant_context("rerun");
        let current = create(&backend, &tenant).await;
        backend.session.inject_fault(
            StatementKind::Cas,
            "UPDATE resources",
            Fault::TimeOutWithoutApplying,
        );
        let before = backend.cas_stats();
        let updated = backend
            .update(&tenant, &current, patient("Two"))
            .await
            .unwrap();
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c2-rerun {stats}]");
        assert_eq!(updated.version_id(), "2");
        assert_eq!((stats.attempts, stats.applied, stats.reruns), (2, 1, 1));
        let versions: Vec<i32> = version_rows(&backend, &tenant)
            .await
            .iter()
            .map(|v| v.0)
            .collect();
        assert_eq!(versions, [1, 2]);

        // Another writer at V: R2 shows V with its write_id, a definite
        // miss, so 409 from update.
        let tenant = tenant_context("another");
        let current = create(&backend, &tenant).await;
        let theirs = Uuid::new_v4();
        another_writer_late(&backend, &tenant, 1, theirs, StatementKind::SerialRead).await;
        backend.session.inject_fault(
            StatementKind::Cas,
            "UPDATE resources",
            Fault::TimeOutWithoutApplying,
        );
        let before = backend.cas_stats();
        match backend.update(&tenant, &current, patient("Mine")).await {
            Err(StorageError::Concurrency(ConcurrencyError::VersionConflict {
                expected_version,
                actual_version,
                ..
            })) => assert_eq!(
                (expected_version.as_str(), actual_version.as_str()),
                ("1", "2")
            ),
            other => panic!("expected a version conflict, got {other:?}"),
        }
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c2-another {stats}]");
        assert_eq!(
            (stats.attempts, stats.applied, stats.miss_version_conflict),
            (1, 0, 1)
        );
        assert_eq!(current_row(&backend, &tenant).await.write_id, Some(theirs));

        // Past V, row V's write_id unset: undecidable, 504 after a timeout.
        // Our C2 applies (1 -> 2) and its reply is lost; another writer's
        // C2 (2 -> 3), held back, lands before R2, and row 2 was never
        // written, since our step 4 never ran.
        let tenant = tenant_context("undecidable");
        let current = create(&backend, &tenant).await;
        another_writer_late(
            &backend,
            &tenant,
            2,
            Uuid::new_v4(),
            StatementKind::SerialRead,
        )
        .await;
        backend.session.inject_fault(
            StatementKind::Cas,
            "UPDATE resources",
            Fault::ApplyThenTimeOut,
        );
        let before = backend.cas_stats();
        match backend.update(&tenant, &current, patient("Mine")).await {
            Err(StorageError::Backend(BackendError::Timeout { .. })) => {}
            other => panic!("expected 504, got {other:?}"),
        }
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c2-undecidable {stats}]");
        assert_eq!(
            (stats.attempts, stats.version_reads, stats.undecidable),
            (1, 1, 1)
        );

        // R2 itself times out with WriteTimeout/CAS: 504, no re-run, the row
        // unchanged.
        let tenant = tenant_context("r2-timeout");
        let current = create(&backend, &tenant).await;
        backend.session.inject_fault(
            StatementKind::Cas,
            "UPDATE resources",
            Fault::TimeOutWithoutApplying,
        );
        backend.session.inject_fault(
            StatementKind::SerialRead,
            "FROM resources",
            Fault::FailWithCasTimeout,
        );
        let before = backend.cas_stats();
        match backend.update(&tenant, &current, patient("Two")).await {
            Err(StorageError::Backend(BackendError::Timeout { .. })) => {}
            other => panic!("expected 504, got {other:?}"),
        }
        let stats = backend.cas_stats().since(&before);
        assert_eq!(
            (stats.attempts, stats.reruns, stats.serial_read_failures),
            (1, 0, 1)
        );
        assert_eq!(current_row(&backend, &tenant).await.version_id, 1);

        // Attempt 1 applies late, after R2 said "unchanged" and before the
        // re-run's C2: the re-run misses on attempt 1's version, which is
        // ours (ADR-aacruzgon-0051). Success, not a 409 against itself.
        let tenant = tenant_context("late");
        let current = create(&backend, &tenant).await;
        backend.session.inject_fault(
            StatementKind::Cas,
            "UPDATE resources",
            Fault::ApplyLate {
                before: StatementKind::Cas,
            },
        );
        let before = backend.cas_stats();
        let updated = backend
            .update(&tenant, &current, patient("Two"))
            .await
            .unwrap();
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c2-late {stats}]");
        assert_eq!(updated.version_id(), "2");
        assert_eq!(
            (
                stats.attempts,
                stats.applied,
                stats.applied_late,
                stats.miss_version_conflict
            ),
            (2, 1, 1, 0)
        );
        let row = current_row(&backend, &tenant).await;
        assert_eq!(
            row.last_updated,
            updated.last_modified(),
            "attempt 1's values"
        );
        let versions: Vec<i32> = version_rows(&backend, &tenant)
            .await
            .iter()
            .map(|v| v.0)
            .collect();
        assert_eq!(versions, [1, 2], "the body is written once");
    }

    fn search_parameter(id: &str, url: &str, status: &str, expression: &str) -> Json {
        serde_json::json!({
            "resourceType": "SearchParameter",
            "id": id,
            "url": url,
            "name": id,
            "status": status,
            "description": "test",
            "code": id,
            "base": ["Patient"],
            "type": "token",
            "expression": expression
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
            .map(|d| d.expression.clone())
    }

    /// D9 on the node: an update replaces its id's definition in the
    /// overlay, a deactivating update removes it (and invalidates), and of
    /// two definitions of one `url` the other comes back into force. A new
    /// process's reload agrees.
    #[tokio::test]
    async fn the_overlay_follows_an_update() {
        let backend = backend_or_skip!();
        let tenant = tenant_context("overlay");
        let version = FhirVersion::default_enabled();
        let url = format!(
            "http://example.org/SearchParameter/{}",
            tenant.tenant_id().as_str()
        );
        let url = url.as_str();

        let v1 = backend
            .create(
                &tenant,
                "SearchParameter",
                search_parameter("sp", url, "active", "Patient.gender"),
                version,
            )
            .await
            .unwrap();
        assert_eq!(
            expression_of(&backend, &tenant, url).as_deref(),
            Some("Patient.gender")
        );

        // A new expression, same url: replaced at once.
        let v2 = backend
            .update(
                &tenant,
                &v1,
                search_parameter("sp", url, "active", "Patient.birthDate"),
            )
            .await
            .unwrap();
        assert_eq!(
            expression_of(&backend, &tenant, url).as_deref(),
            Some("Patient.birthDate")
        );

        // A new url: the old one leaves the registry, the new one enters.
        let moved = format!("{url}-moved");
        let v3 = backend
            .update(
                &tenant,
                &v2,
                search_parameter("sp", &moved, "active", "Patient.birthDate"),
            )
            .await
            .unwrap();
        assert_eq!(expression_of(&backend, &tenant, url), None);
        assert!(expression_of(&backend, &tenant, &moved).is_some());

        // Deactivated: removed, and the registry rebuilt without it.
        backend
            .update(
                &tenant,
                &v3,
                search_parameter("sp", &moved, "retired", "Patient.birthDate"),
            )
            .await
            .unwrap();
        assert_eq!(expression_of(&backend, &tenant, &moved), None);

        // Two ids, one url: retiring the one in force brings the other back.
        let shared = format!("{url}-shared");
        backend
            .create(
                &tenant,
                "SearchParameter",
                search_parameter("older", &shared, "active", "Patient.gender"),
                version,
            )
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(3)).await;
        let newer = backend
            .create(
                &tenant,
                "SearchParameter",
                search_parameter("newer", &shared, "active", "Patient.active"),
                version,
            )
            .await
            .unwrap();
        assert_eq!(
            expression_of(&backend, &tenant, &shared).as_deref(),
            Some("Patient.active")
        );
        backend
            .update(
                &tenant,
                &newer,
                search_parameter("newer", &shared, "draft", "Patient.active"),
            )
            .await
            .unwrap();
        assert_eq!(
            expression_of(&backend, &tenant, &shared).as_deref(),
            Some("Patient.gender"),
            "the older definition of the url is in force again, not lost"
        );

        // A new process's reload reaches the same registry.
        let restarted = backend_or_skip!();
        restarted
            .load_stored_search_parameters(tenant.tenant_id().as_str())
            .await
            .unwrap();
        assert_eq!(expression_of(&restarted, &tenant, url), None);
        assert_eq!(expression_of(&restarted, &tenant, &moved), None);
        assert_eq!(
            expression_of(&restarted, &tenant, &shared).as_deref(),
            Some("Patient.gender")
        );
    }

    /// D9's race on the node: a reload that read a SearchParameter before
    /// this process deactivated it must not put it back. The reload's read
    /// is staged by noting the old body as a reload would have read it,
    /// then letting the update record its removal before the reload merges.
    #[tokio::test]
    async fn a_reload_racing_a_deactivation_does_not_restore_it() {
        let backend = backend_or_skip!();
        let tenant = tenant_context("overlay-race");
        let url = format!(
            "http://example.org/SearchParameter/{}",
            tenant.tenant_id().as_str()
        );
        let v1 = backend
            .create(
                &tenant,
                "SearchParameter",
                search_parameter("sp", &url, "active", "Patient.gender"),
                FhirVersion::default_enabled(),
            )
            .await
            .unwrap();
        // The reload begins (it would read v1 here) ...
        let reload = backend.reload_since_for_test();
        // ... the update deactivates it ...
        backend
            .update(
                &tenant,
                &v1,
                search_parameter("sp", &url, "draft", "Patient.gender"),
            )
            .await
            .unwrap();
        // ... and the reload merges what it read before the update.
        backend.merge_reload_for_test(tenant.tenant_id().as_str(), &v1, reload);
        assert_eq!(expression_of(&backend, &tenant, &url), None);
    }
}
