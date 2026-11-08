//! `delete` and `delete_versioned` (roadmap Step 3, layer L2b): the soft
//! delete, C3, through the write engine in `write.rs` (ADR-aacruzgon-0050).
//!
//! Each turn follows `docs/Cassandra/schema/walkthrough.md` 3.4:
//!
//! 0. R1 at `LOCAL_QUORUM`, which gives `N`, `data`, `lu(N)` and `K(N)`
//!    (G22). An absent row, or one with `deleted_at > 0`, is
//!    `ResourceError::NotFound` with nothing written: no V3, no intent, no
//!    version. `delete_versioned` with `N` other than the caller's version is
//!    `ConcurrencyError::VersionConflict`, also with nothing written.
//! 1. `Operation::Delete`, checked once before the first R1. `K(N+1)` is
//!    empty and `deleted_at = lu(N+1)`.
//! 2. to 6. [`CassandraBackend::run_cas`] with
//!    [`CasPlan::delete`](super::write::CasPlan::delete): V3 rewrites row `N`
//!    from R1 (G40), I1, C3 (`IF version_id = N AND deleted_at = 0`), V2 (the
//!    marker: `data` unset, `index_keys` the empty set), the delete of every
//!    key of `K(N)` at `ts(N+1)` (in Step 3 only the `resources_by_type` row,
//!    X1), and I2.
//!
//! **What a miss means** (walkthrough 3.4, G9, G25; ADR-aacruzgon-0024):
//! `[applied]` alone or `deleted_at > 0` is `NotFound`, for both. Another
//! live version is, for a plain `delete`, one more turn from step 0, at once
//! and with a new intent ([`PLAIN_DELETE_MISS_RETRIES`]): "delete whatever is
//! current" still means the same after the other writer committed, as
//! MongoDB does (#1405). A second miss is `VersionConflict` (409), and a turn
//! whose R1 finds the resource deleted is `NotFound`. `delete_versioned`
//! never retries: its caller named the version it saw.
//!
//! **Statuses.** Storage's `NotFound` is how a caller tells "removed nothing"
//! from "removed something". The REST layer answers a plain `delete`'s
//! `NotFound` with the spec's no-op, 204 (HeliosSoftware/hfs#2529, carried by
//! ADR-aacruzgon-0052). A `delete_versioned` meets `NotFound` only when
//! another delete won the race after REST's own `If-Match` read, and REST
//! answers that 404. The engine's errors keep their own statuses: 503 or 504
//! for a failed statement or an undecidable outcome.
//!
//! **A late earlier attempt** (ADR-aacruzgon-0051). One request covers every
//! turn, so a turn's R1 is first matched against every earlier attempt whose
//! outcome was unknown ([`CassandraBackend::check_earlier_attempts`]), and
//! the engine does the same for a miss. A delete that applied late is then
//! the success it is, never `NotFound` or 409 against itself.
//!
//! **Stored SearchParameters** (D9). A delete of a `SearchParameter` is
//! recorded in the in-process overlay (`forget_stored_search_parameter`),
//! unless a later version already superseded it: after a delete marker the
//! only later version is a revive, which restores the definition.
//!
//! `delete_versioned` is its own compare-and-swap: the trait default reads,
//! compares and then calls `delete`, which is not atomic (#1404).

use std::future::Future;

use super::backend::CassandraBackend;
use super::read::ResourceRow;
use super::write::{
    Applied, CasMiss, CasOutcome, CasPlan, PLAIN_DELETE_MISS_RETRIES, PreviousVersion, RowKey,
};
use crate::error::{ConcurrencyError, ResourceError, StorageError, StorageResult};
use crate::tenant::{Operation, TenantContext};

/// Which delete a call is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect<'a> {
    /// `delete`: whatever version is current.
    Current,
    /// `delete_versioned`: only this version, a bare version id (`3`).
    Version(&'a str),
}

impl Expect<'_> {
    /// The operation's name in errors and logs.
    fn operation(self) -> &'static str {
        match self {
            Expect::Current => "delete",
            Expect::Version(_) => "delete_versioned",
        }
    }

    /// The turns after a definite miss on another live version (G25).
    fn miss_retries(self) -> usize {
        match self {
            Expect::Current => PLAIN_DELETE_MISS_RETRIES,
            Expect::Version(_) => 0,
        }
    }
}

/// What a turn does after a definite miss.
#[derive(Debug)]
enum AfterMiss {
    /// Another live version won: one more turn from step 0.
    Retry,
    /// The call's answer.
    Fail(StorageError),
}

fn not_found(resource_type: &str, id: &str) -> StorageError {
    StorageError::Resource(ResourceError::NotFound {
        resource_type: resource_type.to_string(),
        id: id.to_string(),
    })
}

/// Step 0 (walkthrough 3.4): the live row a turn may delete, or the call's
/// answer when there is none to delete. An absent or deleted row is
/// `NotFound`; for `delete_versioned`, another current version is
/// `VersionConflict`. Neither writes anything. The version is compared as the
/// string the caller gave, as MongoDB does, so an `expected` that is not a
/// number is a conflict, not a parse error.
fn deletable<'r>(
    row: Option<&'r ResourceRow>,
    expect: Expect<'_>,
    resource_type: &str,
    id: &str,
) -> StorageResult<&'r ResourceRow> {
    let row = match row {
        Some(row) if !row.is_deleted() => row,
        _ => return Err(not_found(resource_type, id)),
    };
    if let Expect::Version(expected) = expect {
        let current = row.version_id.to_string();
        if current != expected {
            return Err(StorageError::Concurrency(
                ConcurrencyError::VersionConflict {
                    resource_type: resource_type.to_string(),
                    id: id.to_string(),
                    expected_version: expected.to_string(),
                    actual_version: current,
                },
            ));
        }
    }
    Ok(row)
}

/// A definite miss of C3 conditioned on `N = expected`, with `may_retry`
/// when the call has a turn left (walkthrough 3.4's CAS table).
fn after_miss(
    miss: CasMiss,
    may_retry: bool,
    resource_type: &str,
    id: &str,
    expected: i32,
) -> AfterMiss {
    match miss {
        CasMiss::VersionMoved { .. } if may_retry => AfterMiss::Retry,
        miss => AfterMiss::Fail(miss.into_error(resource_type, id, expected)),
    }
}

impl CassandraBackend {
    /// `ResourceStorage::delete`; see the module docs.
    pub(super) async fn delete_resource(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<()> {
        tenant.check_permission(Operation::Delete, resource_type)?;
        let tenant_id = tenant.tenant_id().as_str();
        self.delete_turns(tenant_id, resource_type, id, Expect::Current, move || {
            self.read_resource_row(tenant_id, resource_type, id)
        })
        .await
    }

    /// `ResourceStorage::delete_versioned`; see the module docs.
    /// `expected_version` is a bare version id; it never retries.
    pub(super) async fn delete_versioned_resource(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        expected_version: &str,
    ) -> StorageResult<()> {
        tenant.check_permission(Operation::Delete, resource_type)?;
        let tenant_id = tenant.tenant_id().as_str();
        self.delete_turns(
            tenant_id,
            resource_type,
            id,
            Expect::Version(expected_version),
            move || self.read_resource_row(tenant_id, resource_type, id),
        )
        .await
    }

    /// The turns of one delete call, all in one request. `pre_read` is the
    /// turn's R1: [`Self::read_resource_row`] in production, and in the node
    /// tests R1 followed by another writer, which is how a test puts a write
    /// between a turn's R1 and its C3 (the fault seam can defer a statement
    /// only to the next statement of a kind, and the first turn already sends
    /// every kind).
    async fn delete_turns<F, Fut>(
        &self,
        tenant_id: &str,
        resource_type: &str,
        id: &str,
        expect: Expect<'_>,
        mut pre_read: F,
    ) -> StorageResult<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = StorageResult<Option<ResourceRow>>>,
    {
        let key = RowKey {
            tenant_id,
            resource_type,
            id,
        };
        let mut request = self.cas_request(expect.operation(), key);
        let mut retries_left = expect.miss_retries();
        let result: StorageResult<()> = async {
            loop {
                // Step 0. After an unknown outcome, R1's error answers as R2's
                // does (ADR-aacruzgon-0067).
                let row = request.after_unknown(pre_read().await, "pre-read")?;
                if let Some(applied) = self
                    .check_earlier_attempts(&mut request, row.as_ref())
                    .await?
                {
                    self.deleted(tenant_id, resource_type, id, &applied);
                    return Ok(());
                }
                // A SearchParameter already deleted, by an earlier request
                // whose outcome this instance never learned or by another
                // instance: the overlay catches up before the NotFound
                // (ADR-aacruzgon-0068).
                if resource_type == "SearchParameter"
                    && row.as_ref().is_some_and(ResourceRow::is_deleted)
                {
                    self.follow_stored_search_parameter(tenant_id, id, row.as_ref());
                }
                let row = deletable(row.as_ref(), expect, resource_type, id)?;
                let expected = row.version_id;

                // Steps 2 to 6.
                let previous = request.after_unknown(PreviousVersion::from_row(row), "pre-read")?;
                let plan = CasPlan::delete(previous);
                match self.run_cas(&mut request, plan).await? {
                    CasOutcome::Applied(applied) => {
                        self.deleted(tenant_id, resource_type, id, &applied);
                        return Ok(());
                    }
                    CasOutcome::Missed(miss) => {
                        match after_miss(miss, retries_left > 0, resource_type, id, expected) {
                            AfterMiss::Retry => {
                                retries_left -= 1;
                                request.note_miss_retry();
                            }
                            AfterMiss::Fail(error) => {
                                if matches!(miss, CasMiss::VersionMoved { .. })
                                    && expect.miss_retries() > 0
                                {
                                    request.note_miss_budget_exhausted();
                                }
                                // Another writer's marker made C3 miss: the
                                // overlay catches up with it (ADR-aacruzgon-0068).
                                // After an unknown outcome the serial read at the
                                // end of the call does that instead.
                                if resource_type == "SearchParameter"
                                    && miss == CasMiss::Deleted
                                    && !request.had_unknown_outcome()
                                {
                                    self.catch_up_with_a_deleted_miss(tenant_id, id).await;
                                }
                                return Err(error);
                            }
                        }
                    }
                }
            }
        }
        .await;
        self.settle_overlay_after_unknown(&request, &result).await;
        result
    }

    /// A SearchParameter delete whose C3 missed with `deleted_at > 0`:
    /// another writer deleted it between this call's R1 and its C3, and the
    /// overlay records that removal now, not at the TTL refresh
    /// (ADR-aacruzgon-0068). One R1 at `LOCAL_QUORUM` reads the marker's
    /// `last_updated`: the miss came from a Paxos round on this partition
    /// that read the marker as committed, and Paxos v2's prepare first
    /// brings a quorum up to the latest commit (`PaxosPrepare.java:852-857`),
    /// so a quorum read sees it, or a newer version, which the overlay
    /// follows as well. A removal already held is not recorded again
    /// ([`CassandraBackend::follow_stored_search_parameter`]). A failed read
    /// is logged and changes nothing; the call's `NotFound` is unchanged.
    async fn catch_up_with_a_deleted_miss(&self, tenant_id: &str, id: &str) {
        match self
            .read_resource_row(tenant_id, "SearchParameter", id)
            .await
        {
            Ok(row) => self.follow_stored_search_parameter(tenant_id, id, row.as_ref()),
            Err(error) => tracing::warn!(
                tenant_id,
                id,
                error = %error,
                "a SearchParameter delete met another writer's delete and could not read \
                 its marker; the overlay keeps its entry until the next refresh or write of it"
            ),
        }
    }

    /// After a delete applied: a stored SearchParameter leaves the overlay
    /// (D9), unless a later version already superseded the marker. After a
    /// marker the next version can only be a revive (C2 and C3 need
    /// `deleted_at = 0`), which restores the definition; recording this
    /// removal after it would drop a live parameter. If that revive was
    /// deleted again in turn, that later delete records its own removal.
    fn deleted(&self, tenant_id: &str, resource_type: &str, id: &str, applied: &Applied) {
        if resource_type == "SearchParameter" && !applied.superseded {
            self.forget_stored_search_parameter(tenant_id, id, applied.last_updated);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use helios_fhir::FhirVersion;

    fn row(version_id: i32, deleted: bool) -> ResourceRow {
        let at = DateTime::<Utc>::from_timestamp_millis(1_791_972_000_123).unwrap();
        ResourceRow {
            version_id,
            data: Some("{}".to_string()),
            last_updated: at,
            created_at: Some(at),
            deleted_at: deleted.then_some(at),
            fhir_version: FhirVersion::default_enabled(),
            write_ts: Some(at.timestamp_millis() * 1000),
            write_id: None,
            index_keys: Some(super::super::index_keys::EMPTY.to_vec()),
        }
    }

    fn is_not_found(result: &StorageResult<&ResourceRow>) -> bool {
        matches!(
            result,
            Err(StorageError::Resource(ResourceError::NotFound { .. }))
        )
    }

    /// Step 0: absent and deleted are `NotFound` for both; only
    /// `delete_versioned` compares the version, as a string.
    #[test]
    fn step_0_refuses_what_there_is_nothing_to_delete_and_another_version() {
        let live = row(2, false);
        let marker = row(3, true);
        for expect in [Expect::Current, Expect::Version("2")] {
            assert!(is_not_found(&deletable(None, expect, "Patient", "p")));
            assert!(is_not_found(&deletable(
                Some(&marker),
                expect,
                "Patient",
                "p"
            )));
            assert_eq!(
                deletable(Some(&live), expect, "Patient", "p")
                    .unwrap()
                    .version_id,
                2
            );
        }
        // A marker is `NotFound` even when its version is the one named.
        assert!(is_not_found(&deletable(
            Some(&marker),
            Expect::Version("3"),
            "Patient",
            "p"
        )));
        for stale in ["1", "3", "02", "abc", ""] {
            match deletable(Some(&live), Expect::Version(stale), "Patient", "p") {
                Err(StorageError::Concurrency(ConcurrencyError::VersionConflict {
                    expected_version,
                    actual_version,
                    ..
                })) => assert_eq!(
                    (expected_version.as_str(), actual_version.as_str()),
                    (stale, "2")
                ),
                other => panic!("{stale:?}: expected a version conflict, got {other:?}"),
            }
        }
    }

    /// Walkthrough 3.4's CAS table: another live version retries once
    /// (plain) and is then 409; no row and deleted are `NotFound`.
    #[test]
    fn a_miss_retries_only_on_another_live_version_and_only_while_a_turn_is_left() {
        let moved = CasMiss::VersionMoved { current: Some(3) };
        assert!(matches!(
            after_miss(moved, true, "Patient", "p", 2),
            AfterMiss::Retry
        ));
        match after_miss(moved, false, "Patient", "p", 2) {
            AfterMiss::Fail(StorageError::Concurrency(ConcurrencyError::VersionConflict {
                expected_version,
                actual_version,
                ..
            })) => assert_eq!(
                (expected_version.as_str(), actual_version.as_str()),
                ("2", "3")
            ),
            other => panic!("expected 409, got {other:?}"),
        }
        for miss in [CasMiss::NoRow, CasMiss::Deleted] {
            for may_retry in [true, false] {
                assert!(
                    matches!(
                        after_miss(miss, may_retry, "Patient", "p", 2),
                        AfterMiss::Fail(StorageError::Resource(ResourceError::NotFound { .. }))
                    ),
                    "{miss:?}, may_retry {may_retry}"
                );
            }
        }
    }

    #[test]
    fn a_plain_delete_has_one_retry_and_a_versioned_one_none() {
        assert_eq!(Expect::Current.miss_retries(), 1);
        assert_eq!(Expect::Version("1").miss_retries(), 0);
        assert_eq!(Expect::Current.operation(), "delete");
        assert_eq!(Expect::Version("1").operation(), "delete_versioned");
    }
}

/// The delete paths against the node at `HFS_TEST_CASSANDRA_URL` (skipped
/// when unset), in the style of `write.rs`'s `node_tests`: each case under
/// its own tenant, every write to `resources` a CAS, and the engine's
/// counters printed as `[cas case=… …]`.
#[cfg(test)]
mod node_tests {
    use cdrs_tokio::types::IntoRustByName;
    use cdrs_tokio::types::blob::Blob;
    use cdrs_tokio::types::value::Value;
    use chrono::{DateTime, Utc};
    use helios_fhir::FhirVersion;
    use serde_json::Value as Json;
    use uuid::Uuid;

    use super::*;
    use crate::backends::cassandra::CassandraConfig;
    use crate::backends::cassandra::index_keys::{self, IndexKey};
    use crate::backends::cassandra::session::{Fault, StatementKind};
    use crate::backends::cassandra::write::normalized_body;
    use crate::core::{ResourceStorage, SearchProvider};
    use crate::error::BackendError;
    use crate::tenant::{TenantId, TenantPermissions};

    /// A substring only C3 has, so a fault meant for the delete's CAS never
    /// fires on another writer's C2.
    const C3_ONLY: &str = "index_keys = 0x0100";

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

    fn tenant_context(case: &str) -> TenantContext {
        TenantContext::new(
            TenantId::new(format!("s3-delete-{case}-{}", Uuid::new_v4().simple())),
            TenantPermissions::full_access(),
        )
    }

    fn patient(id: &str) -> Json {
        serde_json::json!({"resourceType": "Patient", "id": id, "active": true})
    }

    async fn create(backend: &CassandraBackend, tenant: &TenantContext, id: &str) {
        backend
            .create(
                tenant,
                "Patient",
                patient(id),
                FhirVersion::default_enabled(),
            )
            .await
            .expect("create");
    }

    async fn r1(backend: &CassandraBackend, tenant: &str) -> Option<ResourceRow> {
        backend
            .read_resource_row(tenant, "Patient", "p")
            .await
            .expect("R1")
    }

    fn key(tenant: &str) -> RowKey<'_> {
        RowKey {
            tenant_id: tenant,
            resource_type: "Patient",
            id: "p",
        }
    }

    /// Another writer's update of `p`, from its own R1, through the engine.
    async fn another_update(backend: &CassandraBackend, tenant: &str) {
        let row = r1(backend, tenant).await.expect("a row to update");
        let (json, data) = normalized_body("update", "Patient", "p", patient("p")).unwrap();
        let mut request = backend.cas_request("update", key(tenant));
        let plan = CasPlan::update(
            PreviousVersion::from_row(&row).unwrap(),
            json,
            data,
            vec![IndexKey::ResourcesByType],
        );
        match backend.run_cas(&mut request, plan).await.unwrap() {
            CasOutcome::Applied(_) => {}
            other => panic!("the other writer's update: {other:?}"),
        }
    }

    /// Another writer's delete of `p`, from its own R1, through the engine.
    async fn another_delete(backend: &CassandraBackend, tenant: &str) {
        let row = r1(backend, tenant).await.expect("a row to delete");
        let mut request = backend.cas_request("delete", key(tenant));
        let plan = CasPlan::delete(PreviousVersion::from_row(&row).unwrap());
        match backend.run_cas(&mut request, plan).await.unwrap() {
            CasOutcome::Applied(_) => {}
            other => panic!("the other writer's delete: {other:?}"),
        }
    }

    /// What another writer does in a turn: after the turn's R1, before its C3.
    #[derive(Clone, Copy, Debug)]
    enum Interleave {
        Nothing,
        /// `n` updates of the row.
        Update(usize),
        /// A delete of the row.
        Delete,
        /// Before the turn's R1: a delete of the row.
        DeleteFirst,
        /// Row 2 written without a `write_id`, then `resources` moved to
        /// version 3 by a CAS: a version past the target whose row cannot
        /// decide.
        UndecidableRowTwo,
    }

    /// One delete call of `p`, with `turns[k]` happening around turn `k`'s
    /// R1. Runs the same turns as `delete` and `delete_versioned`.
    async fn delete_with(
        backend: &CassandraBackend,
        tenant: &TenantContext,
        expect: Expect<'_>,
        turns: &[Interleave],
    ) -> StorageResult<()> {
        let tenant_id = tenant.tenant_id().as_str();
        let mut turn = 0;
        backend
            .delete_turns(tenant_id, "Patient", "p", expect, move || {
                let interleave = turns.get(turn).copied().unwrap_or(Interleave::Nothing);
                turn += 1;
                async move {
                    if let Interleave::DeleteFirst = interleave {
                        another_delete(backend, tenant_id).await;
                    }
                    let row = backend.read_resource_row(tenant_id, "Patient", "p").await;
                    match interleave {
                        Interleave::Update(n) => {
                            for _ in 0..n {
                                another_update(backend, tenant_id).await;
                            }
                        }
                        Interleave::Delete => another_delete(backend, tenant_id).await,
                        Interleave::UndecidableRowTwo => {
                            undecidable_row_two(backend, tenant_id).await
                        }
                        Interleave::Nothing | Interleave::DeleteFirst => {}
                    }
                    row
                }
            })
            .await
    }

    /// See [`Interleave::UndecidableRowTwo`]. The row is at version 1.
    async fn undecidable_row_two(backend: &CassandraBackend, tenant: &str) {
        let row = r1(backend, tenant).await.unwrap();
        let lu = row.last_updated + chrono::Duration::milliseconds(1);
        let ts = lu.timestamp_millis() * 1000;
        let blob = || Value::from(Blob::from(row.index_keys.clone().unwrap()));
        backend
            .session
            .execute(
                "INSERT INTO resource_versions \
                 (tenant_id, resource_type, id, version_id, data, last_updated, \
                  fhir_version, write_ts, index_keys) \
                 VALUES (?, ?, ?, 2, '{}', ?, ?, ?, ?) USING TIMESTAMP ?",
                vec![
                    Value::from(tenant),
                    Value::from("Patient"),
                    Value::from("p"),
                    Value::from(lu),
                    Value::from(row.fhir_version.as_mime_param()),
                    Value::from(ts),
                    blob(),
                    Value::from(ts),
                ],
                StatementKind::Write,
            )
            .await
            .expect("row 2 without a write_id");
        let applied = backend
            .session
            .rows(
                "UPDATE resources SET version_id = 3, last_updated = ?, write_ts = ?, \
                 write_id = ? \
                 WHERE tenant_id = ? AND resource_type = ? AND id = ? \
                 IF version_id = 1 AND deleted_at = 0",
                vec![
                    Value::from(lu + chrono::Duration::milliseconds(1)),
                    Value::from(ts + 1000),
                    Value::from(Uuid::new_v4()),
                    Value::from(tenant),
                    Value::from("Patient"),
                    Value::from("p"),
                ],
                StatementKind::Cas,
            )
            .await
            .expect("move to version 3");
        assert_eq!(
            IntoRustByName::<bool>::get_by_name(&applied[0], "[applied]").unwrap(),
            Some(true)
        );
    }

    /// One row of `resource_versions`, as T-8 reads it.
    #[derive(Debug)]
    struct VersionRow {
        data: Option<String>,
        last_updated: DateTime<Utc>,
        deleted_at: Option<DateTime<Utc>>,
        fhir_version: Option<String>,
        write_id: Option<Uuid>,
        index_keys: Option<Vec<u8>>,
    }

    async fn version_row(backend: &CassandraBackend, tenant: &str, version: i32) -> VersionRow {
        let rows = backend
            .session
            .rows(
                "SELECT data, last_updated, deleted_at, fhir_version, write_id, index_keys \
                 FROM resource_versions \
                 WHERE tenant_id = ? AND resource_type = ? AND id = ? AND version_id = ?",
                vec![
                    Value::from(tenant),
                    Value::from("Patient"),
                    Value::from("p"),
                    Value::from(version),
                ],
                StatementKind::Read,
            )
            .await
            .expect("R3");
        let row = rows.first().expect("the version row");
        VersionRow {
            data: row.get_by_name("data").unwrap(),
            last_updated: row.get_r_by_name("last_updated").unwrap(),
            deleted_at: row.get_by_name("deleted_at").unwrap(),
            fhir_version: row.get_by_name("fhir_version").unwrap(),
            write_id: row.get_by_name("write_id").unwrap(),
            index_keys: IntoRustByName::<Blob>::get_by_name(row, "index_keys")
                .unwrap()
                .map(Blob::into_vec),
        }
    }

    async fn versions(backend: &CassandraBackend, tenant: &str) -> Vec<(i32, bool)> {
        let rows = backend
            .session
            .rows(
                "SELECT version_id, deleted_at FROM resource_versions \
                 WHERE tenant_id = ? AND resource_type = ? AND id = ?",
                vec![
                    Value::from(tenant),
                    Value::from("Patient"),
                    Value::from("p"),
                ],
                StatementKind::Read,
            )
            .await
            .expect("R4");
        let mut versions: Vec<(i32, bool)> = rows
            .iter()
            .map(|row| {
                (
                    row.get_r_by_name("version_id").unwrap(),
                    IntoRustByName::<DateTime<Utc>>::get_by_name(row, "deleted_at")
                        .unwrap()
                        .is_some(),
                )
            })
            .collect();
        versions.sort();
        versions
    }

    async fn ids_by_type(backend: &CassandraBackend, tenant: &str) -> Vec<String> {
        let rows = backend
            .session
            .rows(
                "SELECT id FROM resources_by_type WHERE tenant_id = ? AND resource_type = ?",
                vec![Value::from(tenant), Value::from("Patient")],
                StatementKind::Read,
            )
            .await
            .expect("X1 rows");
        let mut ids: Vec<String> = rows
            .iter()
            .map(|row| row.get_r_by_name("id").unwrap())
            .collect();
        ids.sort();
        ids
    }

    fn is_not_found(result: &StorageResult<()>) -> bool {
        matches!(
            result,
            Err(StorageError::Resource(ResourceError::NotFound { .. }))
        )
    }

    fn version_conflict(result: StorageResult<()>) -> (String, String) {
        match result {
            Err(StorageError::Concurrency(ConcurrencyError::VersionConflict {
                expected_version,
                actual_version,
                ..
            })) => (expected_version, actual_version),
            other => panic!("expected a version conflict, got {other:?}"),
        }
    }

    async fn assert_live_at(backend: &CassandraBackend, tenant: &TenantContext, version: &str) {
        let read = backend
            .read(tenant, "Patient", "p")
            .await
            .expect("read")
            .expect("still live");
        assert_eq!(read.version_id(), version);
    }

    async fn assert_gone(backend: &CassandraBackend, tenant: &TenantContext) {
        assert!(matches!(
            backend.read(tenant, "Patient", "p").await,
            Err(StorageError::Resource(ResourceError::Gone { .. }))
        ));
    }

    /// T-8, T-13, the statements a delete costs, and no CQL `DELETE` on
    /// `resources` or `resource_versions`: tripwires on every write and CAS
    /// that names either table in a `FROM` clause (only a `DELETE` does) are
    /// still armed afterwards, so the delete sent none. Its one `DELETE` is
    /// X1's, on `resources_by_type`.
    #[tokio::test]
    async fn a_delete_writes_the_marker_and_removes_only_the_x1_row() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = tenant_context("marker");
        let t = tenant.tenant_id().as_str();
        for id in ["a", "p", "z"] {
            create(&backend, &tenant, id).await;
        }
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 3);
        let row1 = r1(&backend, t).await.unwrap();

        let tripwires = [
            (StatementKind::Write, "FROM resources "),
            (StatementKind::Write, "FROM resource_versions"),
            (StatementKind::Cas, "FROM resources "),
            (StatementKind::Cas, "FROM resource_versions"),
        ];
        for (kind, cql) in tripwires {
            backend
                .session
                .inject_fault(kind, cql, Fault::TimeOutWithoutApplying);
        }
        let before = (backend.statement_counts(), backend.cas_stats());
        backend
            .delete(&tenant, "Patient", "p")
            .await
            .expect("delete");
        let spent = backend.statement_counts().since(&before.0);
        let stats = backend.cas_stats().since(&before.1);
        eprintln!("[statements op=delete {spent}]");
        eprintln!("[cas case=delete {stats}]");
        // R1; V3, I1, V2, the X1 delete, I2; one C3.
        assert_eq!(
            (spent.read, spent.write, spent.cas, spent.serial_read),
            (1, 5, 1, 0)
        );
        assert_eq!(
            (stats.attempts, stats.applied, stats.post_cas_failures),
            (1, 1, 0)
        );
        for (kind, cql) in tripwires {
            let probe = format!(
                "SELECT version_id {cql}{}WHERE tenant_id = ? AND resource_type = ? AND id = ?",
                if cql.ends_with(' ') { "" } else { " " }
            );
            let error = backend
                .session
                .rows(
                    &probe,
                    vec![Value::from(t), Value::from("Patient"), Value::from("p")],
                    kind,
                )
                .await
                .expect_err("the tripwire is still armed");
            assert!(
                format!("{error:?}").contains("injected fault"),
                "{kind:?} {cql:?}: {error:?}"
            );
        }

        // T-13: count is 2, and only p's X1 row went.
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 2);
        assert_eq!(ids_by_type(&backend, t).await, vec!["a", "z"]);

        // `resources`: version 2, deleted at lu(2), the empty key set, and
        // the old `data` kept (G29).
        let row2 = r1(&backend, t).await.unwrap();
        assert_eq!(row2.version_id, 2);
        assert_eq!(row2.deleted_at, Some(row2.last_updated));
        assert!(row2.last_updated > row1.last_updated);
        assert_eq!(row2.index_keys.as_deref(), Some(&index_keys::EMPTY[..]));
        assert_eq!(row2.data, row1.data, "G29");
        assert_eq!(row2.created_at, row1.created_at);

        // T-8: the marker has `deleted_at` set, `data` unset, and the
        // delete's instant as `last_updated`, with C3's `write_id`.
        let marker = version_row(&backend, t, 2).await;
        assert_eq!(marker.data, None);
        assert_eq!(marker.deleted_at, Some(row2.last_updated));
        assert_eq!(marker.last_updated, row2.last_updated);
        assert_eq!(marker.write_id, row2.write_id);
        assert_eq!(marker.index_keys.as_deref(), Some(&index_keys::EMPTY[..]));
        assert_eq!(
            marker.fhir_version.as_deref(),
            Some(row1.fhir_version.as_mime_param())
        );
        // Row 1 as create wrote it: V3 at ts(1) - 1 changed nothing.
        let content = version_row(&backend, t, 1).await;
        assert_eq!(content.data, row1.data);
        assert_eq!(content.deleted_at, None);
        assert_eq!(content.write_id, row1.write_id);
        assert_eq!(content.index_keys, row1.index_keys);
        assert_gone(&backend, &tenant).await;
        assert!(!backend.exists(&tenant, "Patient", "p").await.unwrap());

        // Again, and an id that never existed: `NotFound` after R1 alone,
        // nothing written (R4 #delete: "has no effect").
        let before = (backend.statement_counts(), backend.cas_stats());
        assert!(is_not_found(&backend.delete(&tenant, "Patient", "p").await));
        assert!(is_not_found(
            &backend.delete(&tenant, "Patient", "never").await
        ));
        let spent = backend.statement_counts().since(&before.0);
        assert_eq!((spent.read, spent.total()), (2, 2), "{spent}");
        assert_eq!(backend.cas_stats().since(&before.1).attempts, 0);
        assert_eq!(versions(&backend, t).await, vec![(1, false), (2, true)]);
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 2);
    }

    /// A tenant without delete permission is refused before any statement.
    #[tokio::test]
    async fn a_delete_without_permission_sends_nothing() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = tenant_context("perm");
        create(&backend, &tenant, "p").await;
        let read_only =
            TenantContext::new(tenant.tenant_id().clone(), TenantPermissions::read_only());
        let before = backend.statement_counts();
        assert!(matches!(
            backend.delete(&read_only, "Patient", "p").await,
            Err(StorageError::Tenant(_))
        ));
        assert!(matches!(
            backend
                .delete_versioned(&read_only, "Patient", "p", "1")
                .await,
            Err(StorageError::Tenant(_))
        ));
        assert_eq!(backend.statement_counts().since(&before).total(), 0);
        assert_live_at(&backend, &tenant, "1").await;
    }

    /// G25: a plain delete that loses to an update retries once from R1 and
    /// deletes the new version; a second loss is 409; a retry whose R1 finds
    /// the resource deleted is `NotFound`; a loss to a delete is `NotFound`
    /// at once.
    #[tokio::test]
    async fn a_plain_delete_retries_once_after_losing_to_an_update() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };

        // One loss, then the retry deletes version 2 as version 3.
        let tenant = tenant_context("retry");
        let t = tenant.tenant_id().as_str();
        create(&backend, &tenant, "p").await;
        let before = backend.cas_stats();
        delete_with(&backend, &tenant, Expect::Current, &[Interleave::Update(1)])
            .await
            .expect("the retry deletes");
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=delete-retry {stats}]");
        // The other writer's C2 is counted too: 3 attempts, 2 applied.
        assert_eq!((stats.attempts, stats.applied), (3, 2));
        assert_eq!(
            (
                stats.miss_version_conflict,
                stats.miss_retries,
                stats.miss_budget_exhausted
            ),
            (1, 1, 0)
        );
        assert_eq!(
            versions(&backend, t).await,
            vec![(1, false), (2, false), (3, true)]
        );
        assert_gone(&backend, &tenant).await;
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 0);

        // Two losses: 409, and the resource is still live at version 3.
        let tenant = tenant_context("twice");
        create(&backend, &tenant, "p").await;
        let before = backend.cas_stats();
        let conflict = version_conflict(
            delete_with(
                &backend,
                &tenant,
                Expect::Current,
                &[Interleave::Update(1), Interleave::Update(1)],
            )
            .await,
        );
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=delete-twice {stats}]");
        assert_eq!(conflict, ("2".to_string(), "3".to_string()));
        assert_eq!(
            (
                stats.miss_version_conflict,
                stats.miss_retries,
                stats.miss_budget_exhausted
            ),
            (2, 1, 1)
        );
        assert_live_at(&backend, &tenant, "3").await;
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 1);

        // One loss, then the retry's R1 finds the resource deleted.
        let tenant = tenant_context("then-gone");
        let t = tenant.tenant_id().as_str();
        create(&backend, &tenant, "p").await;
        let before = backend.cas_stats();
        let result = delete_with(
            &backend,
            &tenant,
            Expect::Current,
            &[Interleave::Update(1), Interleave::DeleteFirst],
        )
        .await;
        assert!(is_not_found(&result), "{result:?}");
        let stats = backend.cas_stats().since(&before);
        assert_eq!((stats.miss_retries, stats.miss_budget_exhausted), (1, 0));
        assert_eq!(
            versions(&backend, t).await,
            vec![(1, false), (2, false), (3, true)],
            "only the other writer's marker"
        );

        // A loss to a delete: `NotFound` at once, no retry.
        let tenant = tenant_context("gap");
        let t = tenant.tenant_id().as_str();
        create(&backend, &tenant, "p").await;
        let before = backend.cas_stats();
        let result = delete_with(&backend, &tenant, Expect::Current, &[Interleave::Delete]).await;
        assert!(is_not_found(&result), "{result:?}");
        let stats = backend.cas_stats().since(&before);
        assert_eq!((stats.miss_deleted, stats.miss_retries), (1, 0));
        assert_eq!(versions(&backend, t).await, vec![(1, false), (2, true)]);
    }

    /// `delete_versioned`: another version at R1 is 409 with nothing
    /// written; a loss between R1 and C3 is 409 with no retry; the current
    /// version deletes; a repeat is `NotFound` (G9).
    #[tokio::test]
    async fn a_versioned_delete_never_retries() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = tenant_context("versioned");
        let t = tenant.tenant_id().as_str();
        create(&backend, &tenant, "p").await;
        another_update(&backend, t).await;

        let before = (backend.statement_counts(), backend.cas_stats());
        for stale in ["1", "3", "abc"] {
            let conflict = version_conflict(
                backend
                    .delete_versioned(&tenant, "Patient", "p", stale)
                    .await,
            );
            assert_eq!(conflict, (stale.to_string(), "2".to_string()));
        }
        let spent = backend.statement_counts().since(&before.0);
        assert_eq!((spent.read, spent.total()), (3, 3), "R1 only: {spent}");
        assert_eq!(backend.cas_stats().since(&before.1).attempts, 0);
        assert_live_at(&backend, &tenant, "2").await;

        // A writer between R1 and C3: 409 against the version it wrote.
        let before = backend.cas_stats();
        let conflict = version_conflict(
            delete_with(
                &backend,
                &tenant,
                Expect::Version("2"),
                &[Interleave::Update(1)],
            )
            .await,
        );
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=delete-versioned-race {stats}]");
        assert_eq!(conflict, ("2".to_string(), "3".to_string()));
        assert_eq!(
            (
                stats.miss_version_conflict,
                stats.miss_retries,
                stats.miss_budget_exhausted
            ),
            (1, 0, 0),
            "one turn only"
        );
        assert_live_at(&backend, &tenant, "3").await;

        // A delete between R1 and C3: `NotFound`, no retry.
        let gap = tenant_context("versioned-gap");
        create(&backend, &gap, "p").await;
        let before = backend.cas_stats();
        let result = delete_with(&backend, &gap, Expect::Version("1"), &[Interleave::Delete]).await;
        assert!(is_not_found(&result), "{result:?}");
        let stats = backend.cas_stats().since(&before);
        assert_eq!((stats.miss_deleted, stats.miss_retries), (1, 0));
        assert_eq!(
            versions(&backend, gap.tenant_id().as_str()).await,
            vec![(1, false), (2, true)]
        );

        // An unknown outcome, and R2 shows another writer at version 2: a
        // definite miss, 409, never retried.
        let unknown = tenant_context("versioned-unk");
        create(&backend, &unknown, "p").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C3_ONLY, Fault::TimeOutWithoutApplying);
        let before = backend.cas_stats();
        let conflict = version_conflict(
            delete_with(
                &backend,
                &unknown,
                Expect::Version("1"),
                &[Interleave::Update(1)],
            )
            .await,
        );
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=delete-versioned-unknown {stats}]");
        assert_eq!(conflict, ("1".to_string(), "2".to_string()));
        assert_eq!(
            (
                stats.unknown_write_timeout_cas,
                stats.miss_version_conflict,
                stats.miss_retries,
                stats.reruns
            ),
            (1, 1, 0, 0)
        );
        assert_live_at(&backend, &unknown, "2").await;

        backend
            .delete_versioned(&tenant, "Patient", "p", "3")
            .await
            .expect("the current version deletes");
        assert_gone(&backend, &tenant).await;
        assert_eq!(
            versions(&backend, t).await,
            vec![(1, false), (2, false), (3, false), (4, true)]
        );
        for version in ["3", "4"] {
            let again = backend
                .delete_versioned(&tenant, "Patient", "p", version)
                .await;
            assert!(is_not_found(&again), "{version}: {again:?}");
        }
        assert!(is_not_found(
            &backend
                .delete_versioned(&tenant, "Patient", "never", "1")
                .await
        ));
        assert_eq!(versions(&backend, t).await.len(), 4);
    }

    /// Walkthrough 3.1's unknown-outcome table on C3, through `delete`, with
    /// the `cfg(test)` fault seam and, where a row needs another writer, the
    /// interleaved turns.
    #[tokio::test]
    async fn unknown_delete_outcomes_follow_the_walkthrough_table() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };

        // Applied, reply lost: R2 shows version 2 with our write_id.
        let tenant = tenant_context("lost-reply");
        let t = tenant.tenant_id().as_str();
        create(&backend, &tenant, "p").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C3_ONLY, Fault::ApplyThenTimeOut);
        let before = backend.cas_stats();
        backend
            .delete(&tenant, "Patient", "p")
            .await
            .expect("applied");
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c3-lost-reply {stats}]");
        assert_eq!(
            (
                stats.attempts,
                stats.applied,
                stats.unknown_write_timeout_cas,
                stats.serial_reads,
                stats.reruns
            ),
            (1, 1, 1, 1, 0)
        );
        assert_eq!(versions(&backend, t).await, vec![(1, false), (2, true)]);
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 0);

        // Not applied: R2 shows version 1, and one re-run deletes.
        let tenant = tenant_context("rerun");
        let t = tenant.tenant_id().as_str();
        create(&backend, &tenant, "p").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C3_ONLY, Fault::TimeOutWithoutApplying);
        let before = backend.cas_stats();
        backend
            .delete(&tenant, "Patient", "p")
            .await
            .expect("re-run");
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c3-rerun {stats}]");
        assert_eq!((stats.attempts, stats.applied, stats.reruns), (2, 1, 1));
        assert_eq!(versions(&backend, t).await, vec![(1, false), (2, true)]);

        // Unchanged three times: the 2 re-runs run out, 504, nothing deleted.
        let tenant = tenant_context("reruns-out");
        create(&backend, &tenant, "p").await;
        for _ in 0..3 {
            backend.session.inject_fault(
                StatementKind::Cas,
                C3_ONLY,
                Fault::TimeOutWithoutApplying,
            );
        }
        let before = backend.cas_stats();
        let result = backend.delete(&tenant, "Patient", "p").await;
        assert!(
            matches!(
                result,
                Err(StorageError::Backend(BackendError::Timeout { .. }))
            ),
            "{result:?}"
        );
        let stats = backend.cas_stats().since(&before);
        assert_eq!((stats.attempts, stats.reruns, stats.undecidable), (3, 2, 1));
        assert_live_at(&backend, &tenant, "1").await;

        // Replicas fail three times: the re-runs run out, and after a
        // WriteFailure the answer is 503, not 504.
        let tenant = tenant_context("reruns-503");
        create(&backend, &tenant, "p").await;
        for _ in 0..3 {
            backend
                .session
                .inject_fault(StatementKind::Cas, C3_ONLY, Fault::FailWithWriteFailure);
        }
        let before = backend.cas_stats();
        let result = backend.delete(&tenant, "Patient", "p").await;
        assert!(
            matches!(
                result,
                Err(StorageError::Backend(BackendError::Unavailable { .. }))
            ),
            "{result:?}"
        );
        let stats = backend.cas_stats().since(&before);
        assert_eq!(
            (stats.attempts, stats.reruns, stats.unknown_write_failure),
            (3, 2, 3)
        );
        assert_live_at(&backend, &tenant, "1").await;

        // Unavailable: refused before any replica acted, so not applied and
        // never retried (ADR-aacruzgon-0050): 503 after one attempt, no R2.
        let tenant = tenant_context("unavailable");
        create(&backend, &tenant, "p").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C3_ONLY, Fault::FailWithUnavailable);
        let before = backend.cas_stats();
        let result = backend.delete(&tenant, "Patient", "p").await;
        assert!(
            matches!(
                result,
                Err(StorageError::Backend(BackendError::Unavailable { .. }))
            ),
            "{result:?}"
        );
        let stats = backend.cas_stats().since(&before);
        assert_eq!(
            (
                stats.attempts,
                stats.not_applied,
                stats.serial_reads,
                stats.miss_retries
            ),
            (1, 1, 0, 0)
        );
        assert_live_at(&backend, &tenant, "1").await;

        // R2 itself times out with WriteTimeout/CAS: 504, no re-run.
        let tenant = tenant_context("r2-timeout");
        create(&backend, &tenant, "p").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C3_ONLY, Fault::TimeOutWithoutApplying);
        backend.session.inject_fault(
            StatementKind::SerialRead,
            "FROM resources",
            Fault::FailWithCasTimeout,
        );
        let before = backend.cas_stats();
        let result = backend.delete(&tenant, "Patient", "p").await;
        assert!(
            matches!(
                result,
                Err(StorageError::Backend(BackendError::Timeout { .. }))
            ),
            "{result:?}"
        );
        let stats = backend.cas_stats().since(&before);
        assert_eq!(
            (stats.attempts, stats.reruns, stats.serial_read_failures),
            (1, 0, 1)
        );
        assert_live_at(&backend, &tenant, "1").await;

        // Attempt 1 applies late, after R2 said version 1 and before the
        // re-run's C3, which misses on attempt 1's marker: our success, not
        // `NotFound` against ourselves (ADR-aacruzgon-0051).
        let tenant = tenant_context("late");
        let t = tenant.tenant_id().as_str();
        create(&backend, &tenant, "p").await;
        backend.session.inject_fault(
            StatementKind::Cas,
            C3_ONLY,
            Fault::ApplyLate {
                before: StatementKind::Cas,
            },
        );
        let before = backend.cas_stats();
        backend.delete(&tenant, "Patient", "p").await.expect("late");
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c3-late {stats}]");
        assert_eq!(
            (
                stats.attempts,
                stats.applied,
                stats.applied_late,
                stats.reruns
            ),
            (2, 1, 1, 1)
        );
        assert_eq!(
            (
                stats.miss_deleted,
                stats.miss_version_conflict,
                stats.miss_retries
            ),
            (0, 0, 0)
        );
        assert_eq!(versions(&backend, t).await, vec![(1, false), (2, true)]);
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 0);

        // Another writer won version 2 (R2 shows another write_id at V): a
        // definite miss, so the plain delete's retry deletes version 2.
        let tenant = tenant_context("another-at-v");
        let t = tenant.tenant_id().as_str();
        create(&backend, &tenant, "p").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C3_ONLY, Fault::TimeOutWithoutApplying);
        let before = backend.cas_stats();
        delete_with(&backend, &tenant, Expect::Current, &[Interleave::Update(1)])
            .await
            .expect("the retry deletes");
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c3-another-at-v {stats}]");
        assert_eq!(
            (
                stats.unknown_write_timeout_cas,
                stats.miss_version_conflict,
                stats.miss_retries,
                stats.reruns
            ),
            (1, 1, 1, 0)
        );
        assert_eq!(
            versions(&backend, t).await,
            vec![(1, false), (2, false), (3, true)]
        );

        // Past V, row V another writer's (R3): a definite miss, and the
        // retry deletes version 3.
        let tenant = tenant_context("past-v");
        let t = tenant.tenant_id().as_str();
        create(&backend, &tenant, "p").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C3_ONLY, Fault::TimeOutWithoutApplying);
        let before = backend.cas_stats();
        delete_with(&backend, &tenant, Expect::Current, &[Interleave::Update(2)])
            .await
            .expect("the retry deletes");
        let stats = backend.cas_stats().since(&before);
        eprintln!("[cas case=c3-past-v {stats}]");
        // Row 2 is read once, for the outcome. Attempt 1 is then settled as
        // another writer's, so the retry's pre-read check does not read it
        // again.
        assert_eq!(
            (
                stats.version_reads,
                stats.miss_version_conflict,
                stats.miss_retries
            ),
            (1, 1, 1)
        );
        assert_eq!(
            versions(&backend, t).await,
            vec![(1, false), (2, false), (3, false), (4, true)]
        );

        // Past V, row V without a write_id: undecidable, 504, no retry.
        let tenant = tenant_context("undecidable");
        create(&backend, &tenant, "p").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C3_ONLY, Fault::TimeOutWithoutApplying);
        let before = backend.cas_stats();
        let result = delete_with(
            &backend,
            &tenant,
            Expect::Current,
            &[Interleave::UndecidableRowTwo],
        )
        .await;
        assert!(
            matches!(
                result,
                Err(StorageError::Backend(BackendError::Timeout { .. }))
            ),
            "{result:?}"
        );
        let stats = backend.cas_stats().since(&before);
        assert_eq!(
            (stats.undecidable, stats.version_reads, stats.miss_retries),
            (1, 1, 0)
        );
    }

    /// A stored, `active` SearchParameter `id` on Patient, with `url` and
    /// `expression`.
    fn search_parameter(id: &str, url: &str, expression: &str) -> Json {
        serde_json::json!({
            "resourceType": "SearchParameter",
            "id": id,
            "url": url,
            "name": id,
            "status": "active",
            "description": "a stored SearchParameter for delete.rs's node tests",
            "code": format!("delete-{id}"),
            "base": ["Patient"],
            "type": "token",
            "expression": expression,
        })
    }

    /// The expression in force for `url` in `tenant`'s registry, which a
    /// search would use.
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

    /// D9 with L2a's overlay, through `delete`: deleting the SearchParameter
    /// in force for a `url` brings back the other definition of that `url`,
    /// and deleting that one as well leaves none. The removals hold: a
    /// reload in this process, and one in a new process, bring nothing back.
    #[tokio::test]
    async fn a_searchparameter_delete_takes_it_out_of_the_overlay() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = tenant_context("sp-overlay");
        let t = tenant.tenant_id().as_str();
        let url = format!("http://example.org/SearchParameter/{t}");
        let version = FhirVersion::default_enabled();
        for (id, expression) in [("a", "Patient.gender"), ("b", "Patient.birthDate")] {
            backend
                .create(
                    &tenant,
                    "SearchParameter",
                    search_parameter(id, &url, expression),
                    version,
                )
                .await
                .expect("create");
        }
        // `b` is the later write (and the higher id), so it is in force.
        assert_eq!(
            expression_of(&backend, &tenant, &url).as_deref(),
            Some("Patient.birthDate")
        );

        backend
            .delete(&tenant, "SearchParameter", "b")
            .await
            .expect("delete b");
        assert_eq!(
            expression_of(&backend, &tenant, &url).as_deref(),
            Some("Patient.gender"),
            "deleting b brings a back"
        );
        backend
            .delete(&tenant, "SearchParameter", "a")
            .await
            .expect("delete a");
        assert_eq!(expression_of(&backend, &tenant, &url), None);

        let held = backend
            .load_stored_search_parameters(t)
            .await
            .expect("reload");
        assert_eq!(
            (held, expression_of(&backend, &tenant, &url)),
            (0, None),
            "reload in this process"
        );
        let restarted = self::backend().await.expect("connect");
        let held = restarted
            .load_stored_search_parameters(t)
            .await
            .expect("reload");
        assert_eq!(
            (held, expression_of(&restarted, &tenant, &url)),
            (0, None),
            "reload in a new process"
        );
    }

    /// The `!applied.superseded` gate in [`CassandraBackend::deleted`]. This
    /// process's delete wrote marker 2, and before it learned so, another
    /// instance revived the parameter as version 3 (C4), so the engine
    /// answers the delete applied and superseded (G36). This process's
    /// overlay still holds version 1 and the id is live: recording the
    /// removal would take its `url` out of force until the next reload.
    ///
    /// The engine's superseded answer is W1's, tested in `write.rs`
    /// (`a_superseded_earlier_attempt_is_ours_with_its_rows_left_to_the_sweeper`).
    /// The fault seam has no point between a delete's C3 and its R2 at which
    /// another writer can revive, and an earlier attempt can be built only
    /// inside `write.rs`. So this test writes the marker and the revive
    /// through the engine and hands `deleted` the C3's own `Applied`, once as
    /// superseded and once as it came back.
    #[tokio::test]
    async fn a_superseded_searchparameter_delete_leaves_the_overlay_alone() {
        let (Some(backend), Some(other)) = (backend().await, backend().await) else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = tenant_context("sp-superseded");
        let t = tenant.tenant_id().as_str();
        let url = format!("http://example.org/SearchParameter/{t}");
        backend
            .create(
                &tenant,
                "SearchParameter",
                search_parameter("s", &url, "Patient.gender"),
                FhirVersion::default_enabled(),
            )
            .await
            .expect("create");
        assert_eq!(
            expression_of(&backend, &tenant, &url).as_deref(),
            Some("Patient.gender")
        );
        let key = RowKey {
            tenant_id: t,
            resource_type: "SearchParameter",
            id: "s",
        };

        // This process's C3, through the engine but not through `delete`, so
        // `deleted` has not run yet.
        let row = backend
            .read_resource_row(t, "SearchParameter", "s")
            .await
            .expect("R1")
            .expect("version 1");
        let mut request = backend.cas_request("delete", key);
        let plan = CasPlan::delete(PreviousVersion::from_row(&row).unwrap());
        let applied = match backend.run_cas(&mut request, plan).await.unwrap() {
            CasOutcome::Applied(applied) => applied,
            other => panic!("the delete: {other:?}"),
        };
        assert_eq!((applied.version_id, applied.superseded), (2, false));

        // Another instance, with its own overlay, revives it as version 3.
        let marker = other
            .read_resource_row(t, "SearchParameter", "s")
            .await
            .expect("R1")
            .expect("the marker");
        let (json, data) = normalized_body(
            "create",
            "SearchParameter",
            "s",
            search_parameter("s", &url, "Patient.birthDate"),
        )
        .unwrap();
        let plan = CasPlan::revive(
            PreviousVersion::from_row(&marker).unwrap(),
            json,
            data,
            vec![IndexKey::ResourcesByType],
        );
        let mut revive = other.cas_request("create", key);
        match other.run_cas(&mut revive, plan).await.unwrap() {
            CasOutcome::Applied(revived) => assert_eq!(revived.version_id, 3),
            outcome => panic!("the revive: {outcome:?}"),
        }

        // Superseded: the overlay is left alone, and `url` stays in force.
        let superseded = Applied {
            version_id: applied.version_id,
            last_updated: applied.last_updated,
            write_id: applied.write_id,
            superseded: true,
            post_cas_complete: false,
            plan: applied.plan.clone(),
        };
        backend.deleted(t, "SearchParameter", "s", &superseded);
        assert_eq!(
            expression_of(&backend, &tenant, &url).as_deref(),
            Some("Patient.gender"),
            "a superseded delete leaves the overlay alone"
        );
        // The same delete, not superseded, takes `url` out of force: the gate
        // is what kept it.
        backend.deleted(t, "SearchParameter", "s", &applied);
        assert_eq!(expression_of(&backend, &tenant, &url), None);

        // A reload reads version 3, and the removal at lu(2) does not hold it
        // back.
        let held = backend
            .load_stored_search_parameters(t)
            .await
            .expect("reload");
        assert_eq!(
            (held, expression_of(&backend, &tenant, &url).as_deref()),
            (1, Some("Patient.birthDate"))
        );
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

    /// A tenant holding the stored SearchParameter `s` for its own `url`, in
    /// force with `Patient.gender`.
    async fn tenant_with_parameter(
        backend: &CassandraBackend,
        case: &str,
    ) -> (TenantContext, String) {
        let tenant = hardening_tenant(case);
        let url = format!(
            "http://example.org/SearchParameter/{}",
            tenant.tenant_id().as_str()
        );
        backend
            .create(
                &tenant,
                "SearchParameter",
                search_parameter("s", &url, "Patient.gender"),
                FhirVersion::default_enabled(),
            )
            .await
            .expect("create");
        assert_eq!(
            expression_of(backend, &tenant, &url).as_deref(),
            Some("Patient.gender")
        );
        (tenant, url)
    }

    /// ADR-aacruzgon-0068. A SearchParameter delete whose C3 applied but
    /// whose outcome R2 could not read answers 503; one serial read of the
    /// row then shows the marker, so the parameter is out of force at once,
    /// not at the TTL refresh.
    #[tokio::test]
    async fn an_undecided_searchparameter_delete_that_applied_leaves_the_overlay() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let (tenant, url) = tenant_with_parameter(&backend, "sp-undecided-delete").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C3_ONLY, Fault::ApplyThenTimeOut);
        backend.session.inject_fault(
            StatementKind::SerialRead,
            "FROM resources",
            Fault::FailWithUnavailable,
        );
        let result = backend.delete(&tenant, "SearchParameter", "s").await;
        assert!(
            matches!(
                result,
                Err(StorageError::Backend(BackendError::Unavailable { .. }))
            ),
            "{result:?}"
        );
        assert!(
            !backend
                .exists(&tenant, "SearchParameter", "s")
                .await
                .unwrap()
        );
        assert_eq!(expression_of(&backend, &tenant, &url), None);
    }

    /// The same undecided delete, where C3 did not apply: the row read back
    /// is live, so the parameter stays in force, as it must.
    #[tokio::test]
    async fn an_undecided_searchparameter_delete_that_did_not_apply_stays_in_force() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let (tenant, url) = tenant_with_parameter(&backend, "sp-undecided-kept").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C3_ONLY, Fault::TimeOutWithoutApplying);
        backend.session.inject_fault(
            StatementKind::SerialRead,
            "FROM resources",
            Fault::FailWithUnavailable,
        );
        let result = backend.delete(&tenant, "SearchParameter", "s").await;
        assert!(
            matches!(result, Err(StorageError::Backend(_))),
            "{result:?}"
        );
        assert!(
            backend
                .exists(&tenant, "SearchParameter", "s")
                .await
                .unwrap()
        );
        assert_eq!(
            expression_of(&backend, &tenant, &url).as_deref(),
            Some("Patient.gender")
        );
    }

    /// When that serial read fails too, the overlay cannot know, and keeps
    /// the parameter; the client's retried DELETE finds the marker in its
    /// pre-read, answers `NotFound` (REST 204), and takes it out of force.
    #[tokio::test]
    async fn a_retried_searchparameter_delete_catches_the_overlay_up() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let (tenant, url) = tenant_with_parameter(&backend, "sp-retried-delete").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C3_ONLY, Fault::ApplyThenTimeOut);
        for _ in 0..2 {
            backend.session.inject_fault(
                StatementKind::SerialRead,
                "FROM resources",
                Fault::FailWithUnavailable,
            );
        }
        let result = backend.delete(&tenant, "SearchParameter", "s").await;
        assert!(
            matches!(result, Err(StorageError::Backend(_))),
            "{result:?}"
        );
        assert_eq!(
            expression_of(&backend, &tenant, &url).as_deref(),
            Some("Patient.gender"),
            "neither read could say, so the overlay is unchanged"
        );
        let retry = backend.delete(&tenant, "SearchParameter", "s").await;
        assert!(
            matches!(
                retry,
                Err(StorageError::Resource(ResourceError::NotFound { .. }))
            ),
            "{retry:?}"
        );
        assert_eq!(expression_of(&backend, &tenant, &url), None);
    }

    /// Another instance deleted the parameter: this one's DELETE finds the
    /// marker in its pre-read and takes it out of force here too, before the
    /// refresh would.
    #[tokio::test]
    async fn a_delete_of_a_parameter_another_instance_deleted_catches_up() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let (tenant, url) = tenant_with_parameter(&backend, "sp-other-instance").await;
        let other = self::backend().await.expect("connect");
        other
            .delete(&tenant, "SearchParameter", "s")
            .await
            .expect("the other instance's delete");
        assert_eq!(
            expression_of(&backend, &tenant, &url).as_deref(),
            Some("Patient.gender"),
            "this instance has not heard of it"
        );
        let here = backend.delete(&tenant, "SearchParameter", "s").await;
        assert!(
            matches!(
                here,
                Err(StorageError::Resource(ResourceError::NotFound { .. }))
            ),
            "{here:?}"
        );
        assert_eq!(expression_of(&backend, &tenant, &url), None);
    }

    /// An update that deactivates the parameter (status `retired`), whose C2
    /// applied but whose outcome R2 could not read: the row read back holds
    /// the retired body, so the parameter is out of force at once.
    #[tokio::test]
    async fn an_undecided_deactivating_update_leaves_the_overlay() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let (tenant, url) = tenant_with_parameter(&backend, "sp-undecided-update").await;
        let current = backend
            .read(&tenant, "SearchParameter", "s")
            .await
            .unwrap()
            .expect("stored");
        let mut retired = search_parameter("s", &url, "Patient.gender");
        retired["status"] = Json::from("retired");
        backend.session.inject_fault(
            StatementKind::Cas,
            "SET version_id = ?, data = ?",
            Fault::ApplyThenTimeOut,
        );
        backend.session.inject_fault(
            StatementKind::SerialRead,
            "FROM resources",
            Fault::FailWithUnavailable,
        );
        let result = backend.update(&tenant, &current, retired).await;
        assert!(
            matches!(result, Err(StorageError::Backend(_))),
            "{result:?}"
        );
        let stored = backend
            .read(&tenant, "SearchParameter", "s")
            .await
            .unwrap()
            .expect("stored");
        assert_eq!(stored.version_id(), "2", "the update applied");
        assert_eq!(expression_of(&backend, &tenant, &url), None);
    }

    /// A plain delete's C3 times out without applying, R2 shows another
    /// request's update, and the delete takes its retry; that turn's R1 row
    /// has an `index_keys` that does not decode, so no plan can be built. The first
    /// attempt's outcome was unknown, so the request answers 503, not the
    /// plain table's 500 (ADR-aacruzgon-0067).
    #[tokio::test]
    async fn a_later_turns_undecodable_pre_read_after_an_unknown_outcome_is_503() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = hardening_tenant("later-r1-undecodable");
        let t = tenant.tenant_id().as_str();
        create(&backend, &tenant, "p").await;
        backend
            .session
            .inject_fault(StatementKind::Cas, C3_ONLY, Fault::TimeOutWithoutApplying);
        let backend = &backend;
        let mut turn = 0;
        let result = backend
            .delete_turns(t, "Patient", "p", Expect::Current, move || {
                turn += 1;
                let first = turn == 1;
                async move {
                    if !first {
                        // `index_keys` undecodable on the row the turn reads.
                        backend
                            .session
                            .rows(
                                "UPDATE resources SET index_keys = 0xff \
                                 WHERE tenant_id = ? AND resource_type = ? AND id = ? \
                                 IF version_id = 2",
                                vec![Value::from(t), Value::from("Patient"), Value::from("p")],
                                StatementKind::Cas,
                            )
                            .await
                            .expect("an undecodable index_keys");
                    }
                    let row = backend.read_resource_row(t, "Patient", "p").await;
                    if first {
                        another_update(backend, t).await;
                    }
                    row
                }
            })
            .await;
        eprintln!("[e2 case=later-r1-undecodable result={result:?}]");
        assert_eq!(r1(backend, t).await.map(|r| r.version_id), Some(2));
        assert!(
            matches!(
                result,
                Err(StorageError::Backend(BackendError::Unavailable { .. }))
            ),
            "{result:?}"
        );
    }

    /// ADR-aacruzgon-0068, the in-gap case: another instance deletes the
    /// parameter between this delete's pre-read (live) and its C3, which
    /// then misses with `deleted_at > 0`. The call answers `NotFound`
    /// (REST 204), with no unknown outcome, and the overlay drops the
    /// parameter at once. For `delete` and for `delete_versioned`.
    #[tokio::test]
    async fn a_searchparameter_deleted_in_the_gap_by_another_instance_leaves_the_overlay() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let other = self::backend().await.expect("connect");
        for (case, expect) in [
            ("sp-in-gap-delete", Expect::Current),
            ("sp-in-gap-versioned", Expect::Version("1")),
        ] {
            let (tenant, url) = tenant_with_parameter(&backend, case).await;
            let t = tenant.tenant_id().as_str();
            let (backend_ref, other_ref, tenant_ref) = (&backend, &other, &tenant);
            let result = backend
                .delete_turns(t, "SearchParameter", "s", expect, move || async move {
                    let row = backend_ref
                        .read_resource_row(t, "SearchParameter", "s")
                        .await;
                    if row
                        .as_ref()
                        .is_ok_and(|r| r.as_ref().is_some_and(|r| !r.is_deleted()))
                    {
                        other_ref
                            .delete(tenant_ref, "SearchParameter", "s")
                            .await
                            .expect("the other instance's delete");
                    }
                    row
                })
                .await;
            assert!(
                matches!(
                    result,
                    Err(StorageError::Resource(ResourceError::NotFound { .. }))
                ),
                "{case}: {result:?}"
            );
            assert!(
                !backend
                    .exists(&tenant, "SearchParameter", "s")
                    .await
                    .unwrap()
            );
            assert_eq!(expression_of(&backend, &tenant, &url), None, "{case}");
        }
    }
}
