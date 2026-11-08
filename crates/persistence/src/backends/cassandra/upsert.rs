//! `create_or_update`, including revive (roadmap Step 3, layer L3).
//!
//! The walkthrough's 3.5 (`docs/Cassandra/schema/walkthrough.md`): a bounded
//! loop of turns over the engine in `write.rs`, all of them in one
//! [`CasRequest`](super::write::CasRequest), so that every look at the row is
//! matched against every earlier attempt (ADR-aacruzgon-0034, 0051).
//!
//! 1. **`check_id`** on the URL id, before any statement (Step 3 plan D11):
//!    REST never checks it, and every key the design bounds holds it.
//! 2. **Each turn** (at most `1 + UPSERT_MISS_RETRIES`, so 4):
//!    1. R1 ([`CassandraBackend::read_resource_row`]), whose `index_keys`
//!       is `K(N)` (G22);
//!    2. the pre-read check ([`CassandraBackend::check_earlier_attempts`]):
//!       an earlier attempt of this request that applied late is this
//!       request's success, so the body is never written twice (D13);
//!    3. the branch R1 picks ([`Branch::of`]), then its own step 1: the
//!       permission, as SQLite checks it inside `create`, `update` and
//!       `restore_deleted`, then the G38 bound on a SearchParameter's `code`:
//!       - **a**, no row: C1 ([`CasPlan::create`]) with the request's
//!         `fhir_version`, permission `Create`;
//!       - **b**, a live row: C2 through [`CassandraBackend::update_attempt`]
//!         (V3 from R1, `w(N)` included, D5), permission `Update`;
//!       - **c**, a delete marker (`deleted_at > 0`, G39): C4
//!         ([`CasPlan::revive`]), which writes `deleted_at = 0` and keeps R1's
//!         `fhir_version` (G13), with V3 the marker (G40), permission
//!         `Update`;
//!    4. the engine's answer. `Applied` ends the call. `Missed`, a definite
//!       miss already checked against the earlier attempts, starts the next
//!       turn at once, with no pause.
//! 3. **Four definite misses** are `VersionConflict`, 409 (walkthrough
//!    `:440`), whatever the last one was.
//!
//! **The answer** comes from the attempt that applied, which may be an
//! earlier turn's:
//! - a: version 1, `created_at` its `last_updated`, `created = true`;
//! - b: `created = false`;
//! - c: `created = true`, 201 in every FHIR version (D16).
//!
//! b and c keep the row's `created_at` (G14), which no CAS after C1 writes.
//! `fhir_version` is the plan's. A stored SearchParameter goes to this
//! process's overlay (D9): a created one as `create` notes it, an updated or
//! revived one through `replace_stored_search_parameter`.

use chrono::{DateTime, Utc};
use helios_fhir::FhirVersion;
use serde_json::Value as Json;

use super::backend::CassandraBackend;
use super::errors::BACKEND_NAME;
use super::index_keys::IndexKey;
use super::read::ResourceRow;
use super::write::{
    Applied, CasKind, CasMiss, CasOutcome, CasPlan, PreviousVersion, RowKey, UPSERT_MISS_RETRIES,
    check_id, check_search_parameter_code, normalized_body,
};
use crate::error::{BackendError, StorageError, StorageResult};
use crate::tenant::{Operation, TenantContext};
use crate::types::StoredResource;

/// Which write a turn makes, from its R1 (walkthrough 3.5's table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Branch {
    /// a: no row, so C1 creates version 1.
    Create,
    /// b: a live row, so C2 updates it.
    Update,
    /// c: a delete marker, so C4 revives it.
    Revive,
}

impl Branch {
    fn of(row: Option<&ResourceRow>) -> Self {
        match row {
            None => Branch::Create,
            Some(row) if row.is_deleted() => Branch::Revive,
            Some(_) => Branch::Update,
        }
    }

    /// The permission the branch needs, as the other backends check it
    /// inside `create`, `update` and `restore_deleted`.
    fn operation(self) -> Operation {
        match self {
            Branch::Create => Operation::Create,
            Branch::Update | Branch::Revive => Operation::Update,
        }
    }
}

/// The error after the last turn's definite miss: always `VersionConflict`
/// (walkthrough `:440`), against that turn's `N` (0 for a create). The
/// row's version is kept when the miss carried it.
fn budget_exhausted(miss: CasMiss, resource_type: &str, id: &str, expected: i32) -> StorageError {
    let current = match miss {
        CasMiss::VersionMoved { current } => current,
        CasMiss::NoRow | CasMiss::Deleted | CasMiss::Exists => None,
    };
    CasMiss::VersionMoved { current }.into_error(resource_type, id, expected)
}

/// A test's step between a turn's R1 and its compare-and-swap, to stage
/// another writer or a fault in one particular turn: the fault seam alone
/// fires every held-back write at the first statement of its kind. Outside
/// tests it is empty and does nothing.
#[derive(Clone, Copy, Default)]
struct Seam<'h> {
    #[cfg(test)]
    before_cas: Option<&'h (dyn Fn(usize) -> futures::future::BoxFuture<'h, ()> + Send + Sync)>,
    #[cfg(not(test))]
    _unused: std::marker::PhantomData<&'h ()>,
}

impl Seam<'_> {
    async fn before_cas(&self, _turn: usize) {
        #[cfg(test)]
        if let Some(step) = self.before_cas {
            step(_turn).await;
        }
    }
}

impl CassandraBackend {
    /// `ResourceStorage::create_or_update`; see the module docs. Returns the
    /// stored resource and whether it was created (`true` for branches a
    /// and c).
    pub(super) async fn create_or_update_resource(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        resource: Json,
        fhir_version: FhirVersion,
    ) -> StorageResult<(StoredResource, bool)> {
        self.upsert(
            tenant,
            resource_type,
            id,
            resource,
            fhir_version,
            Seam::default(),
        )
        .await
    }

    async fn upsert(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        resource: Json,
        fhir_version: FhirVersion,
        seam: Seam<'_>,
    ) -> StorageResult<(StoredResource, bool)> {
        check_id(id)?;
        let tenant_id = tenant.tenant_id().as_str();
        let key = RowKey {
            tenant_id,
            resource_type,
            id,
        };
        let mut request = self.cas_request("create_or_update", key);
        // The row's `created_at`, from the last R1 that found a row. No CAS
        // after C1 writes it, so every R1 agrees once the row exists. Unset,
        // the answer takes the new version's `last_updated`, as `read` would.
        let mut created_at: Option<DateTime<Utc>> = None;
        let mut last_miss = (CasMiss::VersionMoved { current: None }, 0);
        let result = async {
            for turn in 0..=UPSERT_MISS_RETRIES {
                if turn > 0 {
                    request.note_miss_retry();
                }
                // After an unknown outcome, R1's error answers as R2's does
                // (ADR-aacruzgon-0067).
                let row = request.after_unknown(
                    self.read_resource_row(tenant_id, resource_type, id).await,
                    "pre-read",
                )?;
                if let Some(row) = &row {
                    created_at = row.created_at;
                }
                if let Some(applied) = self
                    .check_earlier_attempts(&mut request, row.as_ref())
                    .await?
                {
                    return self.upserted(tenant, resource_type, id, applied, created_at);
                }

                let branch = Branch::of(row.as_ref());
                tenant.check_permission(branch.operation(), resource_type)?;
                check_search_parameter_code(resource_type, &resource)?;
                seam.before_cas(turn).await;

                let body = resource.clone();
                let (outcome, expected) = match (branch, &row) {
                    (Branch::Update, Some(row)) => (
                        self.update_attempt(&mut request, row, body).await?,
                        row.version_id,
                    ),
                    (Branch::Revive, Some(row)) => {
                        let previous =
                            request.after_unknown(PreviousVersion::from_row(row), "pre-read")?;
                        let (body, data) =
                            normalized_body("create_or_update", resource_type, id, body)?;
                        let plan =
                            CasPlan::revive(previous, body, data, vec![IndexKey::ResourcesByType]);
                        (self.run_cas(&mut request, plan).await?, row.version_id)
                    }
                    // Branch a, the only one `Branch::of` picks for no row.
                    _ => {
                        let (body, data) =
                            normalized_body("create_or_update", resource_type, id, body)?;
                        let plan = CasPlan::create(
                            body,
                            data,
                            fhir_version,
                            vec![IndexKey::ResourcesByType],
                        );
                        (self.run_cas(&mut request, plan).await?, 0)
                    }
                };
                match outcome {
                    CasOutcome::Applied(applied) => {
                        return self.upserted(tenant, resource_type, id, applied, created_at);
                    }
                    CasOutcome::Missed(miss) => last_miss = (miss, expected),
                }
            }

            request.note_miss_budget_exhausted();
            let (miss, expected) = last_miss;
            Err(budget_exhausted(miss, resource_type, id, expected))
        }
        .await;
        // An undecided SearchParameter write: the overlay follows the row
        // (ADR-aacruzgon-0068).
        self.settle_overlay_after_unknown(&request, &result).await;
        result
    }

    /// The answer for the attempt that applied, from its own plan (it may be
    /// an earlier turn's), and the overlay note for a SearchParameter.
    fn upserted(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        applied: Applied,
        created_at: Option<DateTime<Utc>>,
    ) -> StorageResult<(StoredResource, bool)> {
        let plan = &applied.plan;
        let kind = plan.kind();
        let (created_at, created) = match kind {
            CasKind::Create => (applied.last_updated, true),
            CasKind::Update | CasKind::Revive => (
                created_at.unwrap_or(applied.last_updated),
                kind == CasKind::Revive,
            ),
            CasKind::Delete => {
                return Err(StorageError::Backend(BackendError::Internal {
                    backend_name: BACKEND_NAME.to_string(),
                    message: "create_or_update: a delete plan applied".to_string(),
                    source: None,
                }));
            }
        };
        let content = plan.body().cloned().unwrap_or(Json::Null);
        let tenant_id = tenant.tenant_id().as_str();
        if resource_type == "SearchParameter" {
            if kind == CasKind::Create {
                // As `create` notes it: a body the overlay does not hold
                // changes nothing and drops no registry (#667's seeding).
                self.note_stored_search_parameter(tenant_id, &content, applied.last_updated);
            } else {
                self.replace_stored_search_parameter(tenant_id, id, &content, applied.last_updated);
            }
        }
        Ok((
            StoredResource::from_storage(
                resource_type,
                id,
                applied.version_id.to_string(),
                tenant.tenant_id().clone(),
                content,
                created_at,
                applied.last_updated,
                None,
                plan.fhir_version(),
            ),
            created,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ConcurrencyError;

    fn row(version_id: i32, deleted: bool) -> ResourceRow {
        let at = DateTime::from_timestamp_millis(1_791_972_000_000).unwrap();
        ResourceRow {
            version_id,
            data: (!deleted).then(|| "{}".to_string()),
            last_updated: at,
            created_at: Some(at),
            deleted_at: deleted.then_some(at),
            fhir_version: FhirVersion::default_enabled(),
            write_ts: Some(at.timestamp_millis() * 1000),
            write_id: None,
            index_keys: Some(vec![1, 0]),
        }
    }

    /// Walkthrough 3.5's branch table, and the permission each branch
    /// checks (SQLite `create`, `update`, `restore_deleted`).
    #[test]
    fn r1_picks_the_branch_and_its_permission() {
        assert_eq!(Branch::of(None), Branch::Create);
        assert_eq!(Branch::of(Some(&row(3, false))), Branch::Update);
        assert_eq!(Branch::of(Some(&row(3, true))), Branch::Revive);
        assert_eq!(Branch::Create.operation(), Operation::Create);
        assert_eq!(Branch::Update.operation(), Operation::Update);
        assert_eq!(Branch::Revive.operation(), Operation::Update);
    }

    /// Walkthrough `:440`: whatever the fourth definite miss was, the answer
    /// is `VersionConflict` against that turn's `N`, with the row's version
    /// when the miss carried it.
    #[test]
    fn a_fourth_miss_is_always_a_version_conflict() {
        let cases = [
            (CasMiss::VersionMoved { current: Some(5) }, 4, "4", "5"),
            (CasMiss::VersionMoved { current: None }, 4, "4", "unknown"),
            (CasMiss::Deleted, 2, "2", "unknown"),
            (CasMiss::NoRow, 2, "2", "unknown"),
            (CasMiss::Exists, 0, "0", "unknown"),
        ];
        for (miss, expected, want_expected, want_actual) in cases {
            match budget_exhausted(miss, "Patient", "p", expected) {
                StorageError::Concurrency(ConcurrencyError::VersionConflict {
                    resource_type,
                    id,
                    expected_version,
                    actual_version,
                }) => assert_eq!(
                    (
                        resource_type.as_str(),
                        id.as_str(),
                        expected_version.as_str(),
                        actual_version.as_str()
                    ),
                    ("Patient", "p", want_expected, want_actual),
                    "{miss:?}"
                ),
                other => panic!("{miss:?}: expected a version conflict, got {other:?}"),
            }
        }
    }
}

/// Live tests against `HFS_TEST_CASSANDRA_URL`, in the style of
/// `write.rs`'s: each uses its own tenant, and skips when no node is set.
#[cfg(test)]
mod node_tests {
    use cdrs_tokio::types::IntoRustByName;
    use cdrs_tokio::types::blob::Blob;
    use cdrs_tokio::types::value::Value;
    use futures::FutureExt;
    use futures::future::BoxFuture;
    use uuid::Uuid;

    use super::*;
    use crate::backends::cassandra::CassandraConfig;
    use crate::backends::cassandra::index_keys;
    use crate::backends::cassandra::session::{Fault, StatementKind};
    use crate::backends::cassandra::write::{
        INSERT_VERSION, REVIVE_RESOURCE_IF_VERSION, UPDATE_RESOURCE_IF_VERSION, next_last_updated,
        write_ts_of,
    };
    use crate::core::{ResourceStorage, SearchProvider};
    use crate::error::{ConcurrencyError, TenantError, ValidationError};
    use crate::tenant::{TenantId, TenantPermissions};

    /// Our C2 and C4, told apart for the fault seam.
    const C2: &str = "index_keys = ? WHERE";
    const C4: &str = "deleted_at = 0 WHERE";

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
        tenant_with(case, TenantPermissions::full_access())
    }

    fn tenant_with(case: &str, permissions: TenantPermissions) -> TenantContext {
        TenantContext::new(
            TenantId::new(format!("s3-upsert-{case}-{}", Uuid::new_v4().simple())),
            permissions,
        )
    }

    fn patient(family: &str) -> Json {
        serde_json::json!({
            "resourceType": "Patient",
            "id": "p",
            "name": [{"family": family}]
        })
    }

    fn family(stored: &StoredResource) -> &str {
        stored.content()["name"][0]["family"].as_str().unwrap()
    }

    /// `create_or_update` of `Patient/p`, as REST's PUT calls it.
    async fn put(
        backend: &CassandraBackend,
        tenant: &TenantContext,
        body: Json,
    ) -> StorageResult<(StoredResource, bool)> {
        backend
            .create_or_update(tenant, "Patient", "p", body, FhirVersion::default_enabled())
            .await
    }

    /// The same, with `step` run after each turn's R1 and before its CAS.
    async fn put_with<'h>(
        backend: &'h CassandraBackend,
        tenant: &'h TenantContext,
        body: Json,
        step: &'h (dyn Fn(usize) -> BoxFuture<'h, ()> + Send + Sync),
    ) -> StorageResult<(StoredResource, bool)> {
        backend
            .upsert(
                tenant,
                "Patient",
                "p",
                body,
                FhirVersion::default_enabled(),
                Seam {
                    before_cas: Some(step),
                },
            )
            .await
    }

    /// R1 of `Patient/p`.
    async fn current_row(backend: &CassandraBackend, tenant: &TenantContext) -> ResourceRow {
        backend
            .read_resource_row(tenant.tenant_id().as_str(), "Patient", "p")
            .await
            .expect("R1")
            .expect("a row")
    }

    fn key(tenant: &TenantContext) -> RowKey<'_> {
        RowKey {
            tenant_id: tenant.tenant_id().as_str(),
            resource_type: "Patient",
            id: "p",
        }
    }

    /// Deletes `Patient/p` through the engine's C3, as L2b's `delete` will.
    async fn delete(backend: &CassandraBackend, tenant: &TenantContext) {
        let row = current_row(backend, tenant).await;
        let mut request = backend.cas_request("delete", key(tenant));
        let plan = CasPlan::delete(PreviousVersion::from_row(&row).unwrap());
        match backend.run_cas(&mut request, plan).await.expect("C3") {
            CasOutcome::Applied(_) => {}
            CasOutcome::Missed(miss) => panic!("the test's delete missed: {miss:?}"),
        }
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

    fn numbers(rows: &[(i32, Option<Uuid>, bool)]) -> Vec<i32> {
        rows.iter().map(|row| row.0).collect()
    }

    /// Another writer's C2 or C4 of `Patient/p` from `from`, outside the
    /// engine: it writes `resources` only, with no version row. The fault
    /// seam holds it back and applies it just before the next statement of
    /// kind `before`.
    async fn another_writer_late(
        backend: &CassandraBackend,
        tenant: &TenantContext,
        cql: &str,
        from: i32,
        write_id: Uuid,
        before: StatementKind,
    ) {
        let marker = if cql == REVIVE_RESOURCE_IF_VERSION {
            C4
        } else {
            C2
        };
        backend
            .session
            .inject_fault(StatementKind::Cas, marker, Fault::ApplyLate { before });
        let lu = next_last_updated(Utc::now(), None)
            + chrono::Duration::milliseconds(50 * i64::from(from));
        let values = vec![
            Value::from(from + 1),
            Value::from(patient("Theirs").to_string()),
            Value::from(lu),
            Value::from(FhirVersion::default_enabled().as_mime_param()),
            Value::from(write_ts_of(lu)),
            Value::from(write_id),
            Value::from(Blob::from(index_keys::encode(&[IndexKey::ResourcesByType]))),
            Value::from(tenant.tenant_id().as_str()),
            Value::from("Patient"),
            Value::from("p"),
            Value::from(from),
        ];
        let sent = backend.session.rows(cql, values, StatementKind::Cas).await;
        assert!(
            sent.is_err(),
            "the seam answers the held-back CAS with a timeout"
        );
    }

    /// A marker at version 2: create, then delete.
    async fn deleted(backend: &CassandraBackend, case: &str) -> TenantContext {
        let tenant = tenant_context(case);
        put(backend, &tenant, patient("Smith")).await.unwrap();
        delete(backend, &tenant).await;
        tenant
    }

    /// Walkthrough 3.5's answers per branch: a is version 1 and created; b
    /// is `N + 1`, not created, with the row's `created_at` (G14); c, after
    /// a delete, is the next version and created (D16), with `deleted_at`
    /// back to 0 (G39), the `created_at` of version 1, a marker V3 (G40) and
    /// the X1 row back. Client `meta` is ignored (U-3).
    #[tokio::test]
    async fn each_branch_answers_its_version_and_created_flag() {
        let backend = backend_or_skip!();
        let tenant = tenant_context("branches");

        let counts = backend.statement_counts();
        let (v1, created) = put(&backend, &tenant, patient("One")).await.unwrap();
        let spent = backend.statement_counts().since(&counts);
        eprintln!("[cas case=upsert-a statements {spent}]");
        assert_eq!((v1.version_id(), created), ("1", true));
        assert_eq!(v1.created_at(), v1.last_modified());
        assert_eq!(v1.fhir_version(), FhirVersion::default_enabled());
        assert_eq!((spent.read, spent.serial_read, spent.cas), (1, 0, 1));

        let mut body = patient("Two");
        body["meta"] =
            serde_json::json!({"versionId": "77", "lastUpdated": "2001-01-01T00:00:00Z"});
        let counts = backend.statement_counts();
        let (v2, created) = put(&backend, &tenant, body).await.unwrap();
        let spent = backend.statement_counts().since(&counts);
        eprintln!("[cas case=upsert-b statements {spent}]");
        assert_eq!((v2.version_id(), created), ("2", false));
        assert_eq!(v2.created_at(), v1.created_at(), "G14");
        assert!(v2.last_modified() > v1.last_modified());
        assert_eq!(family(&v2), "Two");
        assert_eq!(v2.content_with_meta()["meta"]["versionId"], "2", "U-3");
        assert_eq!((spent.read, spent.serial_read, spent.cas), (1, 0, 1));

        delete(&backend, &tenant).await;
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 0);
        let marker = current_row(&backend, &tenant).await;
        assert!(marker.is_deleted());

        let counts = backend.statement_counts();
        let (v4, created) = put(&backend, &tenant, patient("Four")).await.unwrap();
        let spent = backend.statement_counts().since(&counts);
        eprintln!("[cas case=upsert-c statements {spent}]");
        assert_eq!((v4.version_id(), created), ("4", true));
        assert_eq!(v4.created_at(), v1.created_at(), "G14");
        assert_eq!(v4.fhir_version(), marker.fhir_version, "G13");
        assert_eq!((spent.read, spent.serial_read, spent.cas), (1, 0, 1));
        let row = current_row(&backend, &tenant).await;
        assert_eq!((row.version_id, row.deleted_at), (4, None), "G39");
        assert_eq!(row.created_at, Some(v1.created_at()));
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 1);
        let read = backend
            .read(&tenant, "Patient", "p")
            .await
            .unwrap()
            .unwrap();
        assert_eq!((read.version_id(), family(&read)), ("4", "Four"));

        // V3 rewrote the marker as a marker (G40): 1, 2, the marker 3, 4.
        let versions = version_rows(&backend, &tenant).await;
        assert_eq!(
            versions.iter().map(|v| (v.0, v.2)).collect::<Vec<_>>(),
            [(1, false), (2, false), (3, true), (4, false)]
        );
        assert!(versions.iter().all(|v| v.1.is_some()), "{versions:?}");
    }

    /// The shared suite's numbering (`crud/delete_tests.rs:343`: create,
    /// delete, restore is "3"), T-9 (`created = true`) and T-13 (`count`
    /// is 2 after deleting 1 of 3, and 3 again after the revive).
    #[tokio::test]
    async fn revive_continues_the_version_chain_and_the_count() {
        let backend = backend_or_skip!();
        let tenant = tenant_context("revive");
        for id in ["q", "r"] {
            backend
                .create_or_update(
                    &tenant,
                    "Patient",
                    id,
                    patient("Other"),
                    FhirVersion::default_enabled(),
                )
                .await
                .unwrap();
        }
        put(&backend, &tenant, patient("Smith")).await.unwrap();
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 3);
        delete(&backend, &tenant).await;
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 2);

        let (restored, created) = put(&backend, &tenant, patient("Restored")).await.unwrap();
        assert_eq!((restored.version_id(), created), ("3", true));
        assert_eq!(family(&restored), "Restored");
        assert!(backend.exists(&tenant, "Patient", "p").await.unwrap());
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 3);
    }

    /// D11, T-3: a URL id that is not a FHIR id is 400 `invalid`, before any
    /// statement, so no intent row either. A 64-character id is accepted.
    #[tokio::test]
    async fn an_invalid_id_is_refused_before_any_statement() {
        let backend = backend_or_skip!();
        let tenant = tenant_context("check-id");
        let long = "a".repeat(65);
        for id in ["a_b", long.as_str(), ""] {
            let counts = backend.statement_counts();
            let result = backend
                .create_or_update(
                    &tenant,
                    "Patient",
                    id,
                    patient("Smith"),
                    FhirVersion::default_enabled(),
                )
                .await;
            assert!(
                matches!(
                    result,
                    Err(StorageError::Validation(
                        ValidationError::InvalidResource { .. }
                    ))
                ),
                "{id:?}: {result:?}"
            );
            let spent = backend.statement_counts().since(&counts);
            assert_eq!(spent.total(), 0, "{id:?}: {spent}");
        }
        let longest = "a".repeat(64);
        let (stored, created) = backend
            .create_or_update(
                &tenant,
                "Patient",
                &longest,
                patient("Smith"),
                FhirVersion::default_enabled(),
            )
            .await
            .unwrap();
        assert_eq!((stored.id(), created), (longest.as_str(), true));
    }

    /// Each branch checks its own permission after R1 and before writing:
    /// `Create` on a, `Update` on b and c. G38 refuses a long
    /// SearchParameter `code` the same way, with nothing written.
    #[tokio::test]
    async fn each_branch_checks_its_own_permission() {
        let backend = backend_or_skip!();
        let only = |operations: Vec<Operation>| {
            TenantPermissions::builder()
                .allow_operations(operations)
                .build()
        };
        let refused =
            |result: StorageResult<(StoredResource, bool)>, operation: Operation| match result {
                Err(StorageError::Tenant(TenantError::OperationNotPermitted {
                    operation: refused,
                    ..
                })) => assert_eq!(refused, operation.to_string()),
                other => panic!("expected {operation} refused, got {other:?}"),
            };

        // a without `Create`: refused after R1, nothing written.
        let tenant = tenant_with("perm", only(vec![Operation::Read, Operation::Update]));
        let counts = backend.statement_counts();
        refused(
            put(&backend, &tenant, patient("No")).await,
            Operation::Create,
        );
        let spent = backend.statement_counts().since(&counts);
        assert_eq!((spent.read, spent.write, spent.cas), (1, 0, 0), "{spent}");

        // b and c without `Update`, on the same tenant id.
        let full = TenantContext::new(tenant.tenant_id().clone(), TenantPermissions::full_access());
        let create_only = TenantContext::new(
            tenant.tenant_id().clone(),
            only(vec![Operation::Read, Operation::Create]),
        );
        put(&backend, &create_only, patient("Created"))
            .await
            .unwrap();
        let counts = backend.statement_counts();
        refused(
            put(&backend, &create_only, patient("No")).await,
            Operation::Update,
        );
        delete(&backend, &full).await;
        refused(
            put(&backend, &create_only, patient("No")).await,
            Operation::Update,
        );
        let spent = backend.statement_counts().since(&counts);
        // The two refusals' R1s, and the test's own delete (R1, V3, I1, C3,
        // V2, X1 delete, I2).
        assert_eq!((spent.read, spent.cas), (3, 1), "{spent}");
        let row = current_row(&backend, &full).await;
        assert_eq!((row.version_id, row.is_deleted()), (2, true));

        // b and c with `Update` but not `Create`.
        let update_only = TenantContext::new(
            tenant.tenant_id().clone(),
            only(vec![Operation::Read, Operation::Update]),
        );
        let (revived, created) = put(&backend, &update_only, patient("Back")).await.unwrap();
        assert_eq!((revived.version_id(), created), ("3", true));
        let (updated, created) = put(&backend, &update_only, patient("Again")).await.unwrap();
        assert_eq!((updated.version_id(), created), ("4", false));

        // G38, before any write.
        let long_code = serde_json::json!({
            "resourceType": "SearchParameter",
            "url": "http://example.org/sp",
            "status": "active",
            "code": "c".repeat(129),
            "base": ["Patient"],
            "type": "token",
            "expression": "Patient.gender"
        });
        let counts = backend.statement_counts();
        assert!(matches!(
            backend
                .create_or_update(
                    &full,
                    "SearchParameter",
                    "sp",
                    long_code,
                    FhirVersion::default_enabled()
                )
                .await,
            Err(StorageError::Validation(_))
        ));
        let spent = backend.statement_counts().since(&counts);
        assert_eq!((spent.read, spent.write, spent.cas), (1, 0, 0), "{spent}");
    }

    /// The loop budget (walkthrough 3.5; plan §6): another writer wins
    /// between R1 and the CAS in every turn, once per miss kind (C1's
    /// `AlreadyExists`, C2's `VersionConflict` and `NotFound`, C4's
    /// `VersionConflict`). After 4 turns the answer is `VersionConflict`.
    /// With one turn left free, the fourth turn succeeds.
    #[tokio::test]
    async fn the_loop_takes_four_turns_then_answers_a_conflict() {
        let backend = backend_or_skip!();

        for free_turn in [None, Some(3)] {
            let tenant = tenant_context("budget");
            let (b, t) = (&backend, &tenant);
            let step = move |turn: usize| -> BoxFuture<'_, ()> {
                async move {
                    if Some(turn) == free_turn {
                        return;
                    }
                    match turn {
                        0 => {
                            b.create(
                                t,
                                "Patient",
                                patient("Theirs1"),
                                FhirVersion::default_enabled(),
                            )
                            .await
                            .unwrap();
                        }
                        1 => {
                            let current = b.read(t, "Patient", "p").await.unwrap().unwrap();
                            b.update(t, &current, patient("Theirs2")).await.unwrap();
                        }
                        2 => delete(b, t).await,
                        3 => {
                            let (_, created) = put(b, t, patient("Theirs4")).await.unwrap();
                            assert!(created, "their revive");
                        }
                        _ => panic!("a fifth turn"),
                    }
                }
                .boxed()
            };
            let before = backend.cas_stats();
            let result = put_with(&backend, &tenant, patient("Mine"), &step).await;
            let stats = backend.cas_stats().since(&before);
            eprintln!("[cas case=upsert-budget free={free_turn:?} {stats}]");
            match free_turn {
                None => {
                    match result {
                        Err(StorageError::Concurrency(ConcurrencyError::VersionConflict {
                            expected_version,
                            actual_version,
                            ..
                        })) => assert_eq!(
                            (expected_version.as_str(), actual_version.as_str()),
                            ("3", "4")
                        ),
                        other => panic!("expected a version conflict, got {other:?}"),
                    }
                    // Ours: 4 attempts, 4 misses, 3 retries, one exhausted
                    // budget. Theirs, on the same counters: create, update,
                    // delete and revive, each one applied attempt.
                    assert_eq!((stats.attempts, stats.applied), (4 + 4, 4));
                    assert_eq!(
                        (
                            stats.miss_already_exists,
                            stats.miss_version_conflict,
                            stats.miss_deleted,
                            stats.miss_retries,
                            stats.miss_budget_exhausted,
                        ),
                        (1, 2, 1, 3, 1)
                    );
                    let row = current_row(&backend, &tenant).await;
                    assert_eq!(row.version_id, 4);
                    let read = backend
                        .read(&tenant, "Patient", "p")
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(family(&read), "Theirs4", "nothing of ours was written");
                }
                Some(_) => {
                    let (stored, created) = result.unwrap();
                    assert_eq!((stored.version_id(), created), ("4", true));
                    assert_eq!(family(&stored), "Mine");
                    // Ours: 3 misses, then the fourth turn applies.
                    // Theirs: 3 applied attempts.
                    assert_eq!((stats.attempts, stats.applied), (4 + 3, 1 + 3));
                    assert_eq!((stats.miss_retries, stats.miss_budget_exhausted), (3, 0));
                }
            }
            assert_eq!(
                numbers(&version_rows(&backend, &tenant).await),
                [1, 2, 3, 4],
                "no gap, no duplicate"
            );
        }
    }

    /// Walkthrough 3.1's unknown-outcome table on C4, through
    /// `create_or_update` and the fault seam, each from a marker at 2. In
    /// `create_or_update`, "another writer won V" is the next turn
    /// (walkthrough `:358`). "Past V with row V ours" needs a superseding V3
    /// that knows this attempt's `write_id`, which a one-shot fault cannot
    /// stage; the engine's `decide` tests cover it.
    #[tokio::test]
    async fn unknown_c4_outcomes_through_the_fault_seam() {
        let backend = backend_or_skip!();

        // Applied, reply lost: R2 finds our write_id at V.
        let tenant = deleted(&backend, "c4-lost-reply").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C4, Fault::ApplyThenTimeOut);
        let before = backend.cas_stats();
        let (stored, created) = put(&backend, &tenant, patient("Mine")).await.unwrap();
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c4-lost-reply {stats}]");
        assert_eq!((stored.version_id(), created), ("3", true));
        assert_eq!((stats.attempts, stats.applied, stats.reruns), (1, 1, 0));
        assert_eq!(
            (stats.unknown_write_timeout_cas, stats.serial_reads),
            (1, 1)
        );
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 1);

        // Not applied: R2 shows the marker, so a re-run, which applies.
        let tenant = deleted(&backend, "c4-rerun").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C4, Fault::TimeOutWithoutApplying);
        let before = backend.cas_stats();
        let (stored, created) = put(&backend, &tenant, patient("Mine")).await.unwrap();
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c4-rerun {stats}]");
        assert_eq!((stored.version_id(), created), ("3", true));
        assert_eq!((stats.attempts, stats.applied, stats.reruns), (2, 1, 1));
        assert_eq!(numbers(&version_rows(&backend, &tenant).await), [1, 2, 3]);

        // Another writer won V: the next turn finds it live and updates it.
        let tenant = deleted(&backend, "c4-another").await;
        let theirs = Uuid::new_v4();
        another_writer_late(
            &backend,
            &tenant,
            REVIVE_RESOURCE_IF_VERSION,
            2,
            theirs,
            StatementKind::SerialRead,
        )
        .await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C4, Fault::TimeOutWithoutApplying);
        let before = backend.cas_stats();
        let (stored, created) = put(&backend, &tenant, patient("Mine")).await.unwrap();
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c4-another {stats}]");
        assert_eq!((stored.version_id(), created), ("4", false));
        assert_eq!(
            (
                stats.attempts,
                stats.applied,
                stats.miss_version_conflict,
                stats.miss_retries
            ),
            (2, 1, 1, 1)
        );
        // Row 3 was never written by its writer: the second turn's V3
        // restored it from R1, with their write_id (D5, G40).
        let versions = version_rows(&backend, &tenant).await;
        assert_eq!(numbers(&versions), [1, 2, 3, 4]);
        assert_eq!(versions[2].1, Some(theirs));

        // Past V with row V unset: undecidable, 504 after a timeout and 503
        // after a write failure. Nothing is retried.
        for (fault, timed_out) in [
            (Fault::TimeOutWithoutApplying, true),
            (Fault::FailWithWriteFailure, false),
        ] {
            let tenant = deleted(&backend, "c4-undecidable").await;
            another_writer_late(
                &backend,
                &tenant,
                REVIVE_RESOURCE_IF_VERSION,
                2,
                Uuid::new_v4(),
                StatementKind::SerialRead,
            )
            .await;
            another_writer_late(
                &backend,
                &tenant,
                UPDATE_RESOURCE_IF_VERSION,
                3,
                Uuid::new_v4(),
                StatementKind::SerialRead,
            )
            .await;
            backend.session.inject_fault(StatementKind::Cas, C4, fault);
            let before = backend.cas_stats();
            let result = put(&backend, &tenant, patient("Mine")).await;
            let stats = backend.cas_stats().since(&before);
            eprintln!("[cas case=c4-undecidable timed_out={timed_out} {stats}]");
            match (result, timed_out) {
                (Err(StorageError::Backend(BackendError::Timeout { .. })), true)
                | (Err(StorageError::Backend(BackendError::Unavailable { .. })), false) => {}
                (other, _) => panic!("expected 504 or 503, got {other:?}"),
            }
            assert_eq!(
                (stats.attempts, stats.version_reads, stats.undecidable),
                (1, 1, 1)
            );
            assert_eq!(current_row(&backend, &tenant).await.version_id, 4);
        }

        // Past V with row V another writer's: a definite miss, so the next
        // turn, which updates their version 4 to 5. Row 3 is written here
        // directly; R1 reads `resources` only, so it does not see it.
        let tenant = deleted(&backend, "c4-past-theirs").await;
        let theirs = Uuid::new_v4();
        another_writer_late(
            &backend,
            &tenant,
            REVIVE_RESOURCE_IF_VERSION,
            2,
            theirs,
            StatementKind::SerialRead,
        )
        .await;
        another_writer_late(
            &backend,
            &tenant,
            UPDATE_RESOURCE_IF_VERSION,
            3,
            Uuid::new_v4(),
            StatementKind::SerialRead,
        )
        .await;
        let lu = next_last_updated(Utc::now(), None);
        backend
            .session
            .execute(
                INSERT_VERSION,
                vec![
                    Value::from(tenant.tenant_id().as_str()),
                    Value::from("Patient"),
                    Value::from("p"),
                    Value::from(3),
                    Value::from(patient("Theirs").to_string()),
                    Value::from(lu),
                    Value::from(FhirVersion::default_enabled().as_mime_param()),
                    Value::from(write_ts_of(lu)),
                    Value::from(theirs),
                    Value::from(Blob::from(index_keys::encode(&[IndexKey::ResourcesByType]))),
                    Value::from(write_ts_of(lu)),
                ],
                StatementKind::Write,
            )
            .await
            .expect("their row 3");
        backend
            .session
            .inject_fault(StatementKind::Cas, C4, Fault::TimeOutWithoutApplying);
        let before = backend.cas_stats();
        let (stored, created) = put(&backend, &tenant, patient("Mine")).await.unwrap();
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c4-past-theirs {stats}]");
        assert_eq!((stored.version_id(), created), ("5", false));
        assert_eq!(
            (
                stats.attempts,
                stats.version_reads,
                stats.miss_version_conflict,
                stats.miss_retries,
                stats.undecidable
            ),
            (2, 1, 1, 1, 0)
        );
        assert_eq!(
            numbers(&version_rows(&backend, &tenant).await),
            [1, 2, 3, 4, 5]
        );

        // R2 fails: 503, from R2's own error; nothing written.
        let tenant = deleted(&backend, "c4-r2-failure").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C4, Fault::TimeOutWithoutApplying);
        backend.session.inject_fault(
            StatementKind::SerialRead,
            "FROM resources",
            Fault::FailWithWriteFailure,
        );
        let before = backend.cas_stats();
        assert!(matches!(
            put(&backend, &tenant, patient("Mine")).await,
            Err(StorageError::Backend(BackendError::Unavailable { .. }))
        ));
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c4-r2-failure {stats}]");
        assert_eq!((stats.attempts, stats.serial_read_failures), (1, 1));
        assert!(current_row(&backend, &tenant).await.is_deleted());

        // R2 itself times out: 504, from R2's own error.
        let tenant = deleted(&backend, "c4-r2-timeout").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C4, Fault::TimeOutWithoutApplying);
        backend.session.inject_fault(
            StatementKind::SerialRead,
            "FROM resources",
            Fault::FailWithCasTimeout,
        );
        let before = backend.cas_stats();
        assert!(matches!(
            put(&backend, &tenant, patient("Mine")).await,
            Err(StorageError::Backend(BackendError::Timeout { .. }))
        ));
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c4-r2-timeout {stats}]");
        assert_eq!((stats.attempts, stats.serial_read_failures), (1, 1));
        assert!(current_row(&backend, &tenant).await.is_deleted());

        // Refused with `Unavailable`: 503 at once, never retried.
        let tenant = deleted(&backend, "c4-unavailable").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C4, Fault::FailWithUnavailable);
        let before = backend.cas_stats();
        assert!(matches!(
            put(&backend, &tenant, patient("Mine")).await,
            Err(StorageError::Backend(BackendError::Unavailable { .. }))
        ));
        let stats = backend.cas_stats().since(&before);
        assert_eq!(
            (stats.attempts, stats.not_applied, stats.serial_reads),
            (1, 1, 0)
        );
        assert!(current_row(&backend, &tenant).await.is_deleted());

        // An earlier attempt applies late, just before the re-run's CAS
        // (ADR-aacruzgon-0051): the re-run misses on our own version, and
        // the check finds attempt 1. One turn, one new version.
        let tenant = deleted(&backend, "c4-late").await;
        backend.session.inject_fault(
            StatementKind::Cas,
            C4,
            Fault::ApplyLate {
                before: StatementKind::Cas,
            },
        );
        let before = backend.cas_stats();
        let (stored, created) = put(&backend, &tenant, patient("Mine")).await.unwrap();
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c4-late {stats}]");
        assert_eq!((stored.version_id(), created), ("3", true));
        assert_eq!(
            (
                stats.attempts,
                stats.applied,
                stats.applied_late,
                stats.miss_retries,
                stats.miss_version_conflict
            ),
            (2, 1, 1, 0, 0)
        );
        assert_eq!(numbers(&version_rows(&backend, &tenant).await), [1, 2, 3]);
        let read = backend
            .read(&tenant, "Patient", "p")
            .await
            .unwrap()
            .unwrap();
        assert_eq!((read.version_id(), family(&read)), ("3", "Mine"));
    }

    /// The case L1 left to L3 (`report-L1.md`, audit item 11): the late
    /// check on the "another writer" path, across two turns' targets. Turns
    /// 1 and 2 each have an unknown outcome that R2 shows another writer
    /// won, so turn 2's check runs over turn 1's attempt (decided, so
    /// skipped) as well as its own. Turn 3 applies. Nothing is written twice
    /// and the numbering has no gap.
    ///
    /// The same check can never *find* an earlier turn's attempt: a turn
    /// ends in a miss only after a look at or past its target, which
    /// decides every attempt of the turn, and an attempt whose condition
    /// names a version the row has left can never apply. The second half of
    /// this test stages the nearest shape, an attempt that applies late
    /// within its turn, behind another turn's miss, and checks that it is
    /// found there, with no further turn.
    #[tokio::test]
    async fn a_late_attempt_across_turns_behind_another_writer() {
        let backend = backend_or_skip!();
        let tenant = tenant_context("across-turns");
        put(&backend, &tenant, patient("Smith")).await.unwrap();

        let theirs = [Uuid::new_v4(), Uuid::new_v4()];
        let (b, t) = (&backend, &tenant);
        let step = move |turn: usize| -> BoxFuture<'_, ()> {
            async move {
                if turn < 2 {
                    // Turn k runs C2 from version k + 1. Another writer's C2
                    // lands before our R2, and ours never applies.
                    another_writer_late(
                        b,
                        t,
                        UPDATE_RESOURCE_IF_VERSION,
                        i32::try_from(turn).unwrap() + 1,
                        theirs[turn],
                        StatementKind::SerialRead,
                    )
                    .await;
                    b.session
                        .inject_fault(StatementKind::Cas, C2, Fault::TimeOutWithoutApplying);
                }
            }
            .boxed()
        };
        let before = backend.cas_stats();
        let (stored, created) = put_with(&backend, &tenant, patient("Mine"), &step)
            .await
            .unwrap();
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=upsert-across-turns {stats}]");
        assert_eq!((stored.version_id(), created), ("4", false));
        assert_eq!(family(&stored), "Mine");
        assert_eq!(
            (
                stats.attempts,
                stats.applied,
                stats.applied_late,
                stats.serial_reads,
                stats.miss_version_conflict,
                stats.miss_retries,
                stats.undecidable
            ),
            (3, 1, 0, 2, 2, 2, 0)
        );
        let versions = version_rows(&backend, &tenant).await;
        assert_eq!(numbers(&versions), [1, 2, 3, 4]);
        assert_eq!(
            (versions[1].1, versions[2].1),
            (Some(theirs[0]), Some(theirs[1]))
        );

        // Turn 1 misses to another writer (no unknown outcome); turn 2's
        // first attempt applies late, just before its re-run's CAS. The
        // re-run's miss finds it: success in turn 2, no third turn.
        let tenant = tenant_context("across-turns-late");
        put(&backend, &tenant, patient("Smith")).await.unwrap();
        let (b, t) = (&backend, &tenant);
        let step = move |turn: usize| -> BoxFuture<'_, ()> {
            async move {
                match turn {
                    0 => {
                        let current = b.read(t, "Patient", "p").await.unwrap().unwrap();
                        b.update(t, &current, patient("Theirs")).await.unwrap();
                    }
                    1 => b.session.inject_fault(
                        StatementKind::Cas,
                        C2,
                        Fault::ApplyLate {
                            before: StatementKind::Cas,
                        },
                    ),
                    _ => panic!("a third turn"),
                }
            }
            .boxed()
        };
        let before = backend.cas_stats();
        let (stored, created) = put_with(&backend, &tenant, patient("Mine"), &step)
            .await
            .unwrap();
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=upsert-across-turns-late {stats}]");
        assert_eq!((stored.version_id(), created), ("3", false));
        // Ours: turn 1's miss, then turn 2's two attempts. Theirs: one
        // update, counted by the same backend.
        assert_eq!(
            (
                stats.attempts,
                stats.applied,
                stats.applied_late,
                stats.miss_retries,
                stats.miss_version_conflict
            ),
            (1 + 3, 1 + 1, 1, 1, 1)
        );
        assert_eq!(
            numbers(&version_rows(&backend, &tenant).await),
            [1, 2, 3],
            "the body is written once"
        );
    }

    fn search_parameter(url: &str, status: &str, expression: &str) -> Json {
        serde_json::json!({
            "resourceType": "SearchParameter",
            "id": "sp",
            "url": url,
            "name": "sp",
            "status": status,
            "description": "test",
            "code": "sp",
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

    /// D9 through each branch: a creates the definition, b replaces and
    /// then deactivates it, and c, after a delete, brings it back. A new
    /// process's reload agrees.
    #[tokio::test]
    async fn the_overlay_follows_each_branch() {
        let backend = backend_or_skip!();
        let tenant = tenant_context("overlay");
        let url = format!(
            "http://example.org/SearchParameter/{}",
            tenant.tenant_id().as_str()
        );
        let url = url.as_str();
        let put_sp = |body: Json| {
            backend.create_or_update(
                &tenant,
                "SearchParameter",
                "sp",
                body,
                FhirVersion::default_enabled(),
            )
        };

        let (_, created) = put_sp(search_parameter(url, "active", "Patient.gender"))
            .await
            .unwrap();
        assert!(created);
        assert_eq!(
            expression_of(&backend, &tenant, url).as_deref(),
            Some("Patient.gender")
        );

        let (_, created) = put_sp(search_parameter(url, "active", "Patient.birthDate"))
            .await
            .unwrap();
        assert!(!created);
        assert_eq!(
            expression_of(&backend, &tenant, url).as_deref(),
            Some("Patient.birthDate")
        );

        put_sp(search_parameter(url, "retired", "Patient.birthDate"))
            .await
            .unwrap();
        assert_eq!(expression_of(&backend, &tenant, url), None);

        // Delete it (as L2b will, with its overlay removal), then revive it
        // active.
        let row = backend
            .read_resource_row(tenant.tenant_id().as_str(), "SearchParameter", "sp")
            .await
            .unwrap()
            .unwrap();
        let mut request = backend.cas_request(
            "delete",
            RowKey {
                tenant_id: tenant.tenant_id().as_str(),
                resource_type: "SearchParameter",
                id: "sp",
            },
        );
        let deleted_at = match backend
            .run_cas(
                &mut request,
                CasPlan::delete(PreviousVersion::from_row(&row).unwrap()),
            )
            .await
            .unwrap()
        {
            CasOutcome::Applied(applied) => applied.last_updated,
            CasOutcome::Missed(miss) => panic!("delete missed: {miss:?}"),
        };
        backend.forget_stored_search_parameter(tenant.tenant_id().as_str(), "sp", deleted_at);

        let (revived, created) = put_sp(search_parameter(url, "active", "Patient.name"))
            .await
            .unwrap();
        assert_eq!((revived.version_id(), created), ("5", true));
        assert_eq!(
            expression_of(&backend, &tenant, url).as_deref(),
            Some("Patient.name")
        );

        let fresh = backend_or_skip!();
        fresh
            .load_stored_search_parameters(tenant.tenant_id().as_str())
            .await
            .unwrap();
        assert_eq!(
            expression_of(&fresh, &tenant, url).as_deref(),
            Some("Patient.name")
        );
    }
}
