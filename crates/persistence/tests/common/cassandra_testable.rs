//! [`TestableBackend`] for the Cassandra backend (roadmap item 2.5).
//!
//! `#[path]`-included by `tests/cassandra_tests.rs`, next to the shared
//! harness (`common/harness.rs`, `fixtures.rs`, `capabilities.rs`), which that
//! binary compiles as its `common` module. No other test target declares
//! `mod common;`, so this is the first backend to implement the trait.
//!
//! # One node, many instances: reset rotates the tenant
//!
//! Every instance works against the one node every test in the binary
//! shares ([`node`](crate::cassandra_node::node)), so `reset` cannot "clear all data" the
//! way an in-memory SQLite can: it must never truncate or drop a table, and a
//! CQL `DELETE` against `resources` must never exist (`development-roadmap.md`
//! section 4, Step 1, "Statements that must never exist"; `cassandra-discussion.md`,
//! soft deletes only). Cleaning up between tests was rejected in
//! ADR-aacruzgon-0020 for the tombstones and the locking it would need.
//!
//! Instead each instance owns two tenants ([`unique_tenant`], `test-<label>-<uuid>`):
//! [`tenant`](CassandraTestBackend::tenant), which `seed` writes under, and
//! [`secondary_tenant`](CassandraTestBackend::secondary_tenant), for isolation
//! tests. `reset` replaces both with fresh ones and issues no statement. The
//! old rows stay on the node under tenants nobody names again, which is what
//! ADR-aacruzgon-0020's rule 2 already accepts for every Cassandra test. So:
//!
//! - after `reset`, nothing an instance wrote is visible under its tenants;
//! - two instances never see each other's data, since their tenants differ;
//! - a re-run on a reused node never meets an earlier run's fixtures, although
//!   their ids (`patient-1`, …) are fixed.
//!
//! **Read the tenants from the backend, not from a stored copy.** They change
//! on every `reset`. A [`TestContext`] holds copies: build one with
//! [`into_context`](CassandraTestBackend::into_context), not `TestContext::new`
//! (whose `test-tenant-1` would be shared by every test on the node), and reset
//! it with [`reset_and_seed`], which re-points the copies. The harness's own
//! `TestContext::reset_and_seed` would seed the new tenant while `ctx.tenant`
//! still named the old one.
//!
//! [`TenantContext::system`] is the same `__system__` partition for every test
//! on the node; a test that writes there must use its own ids
//! ([`unique_id`](crate::cassandra_node::unique_id)).
//!
//! # Seeding
//!
//! `seed` writes every fixture under [`tenant`](CassandraTestBackend::tenant)
//! with the backend's real `create`, one at a time, keeping each fixture's id.
//! Every [`TestFixtures`] entry is a create, so seeding needs no `update` or
//! `delete`, which a suite calls itself when it needs them. Seeding the same
//! tenant twice fails with the backend's `AlreadyExists`, as it should: reset
//! first. A
//! `reset` while a `seed` is running does not move it: the seed keeps the
//! tenant it started with, so its fixtures land, whole, under the old tenant.
//!
//! # Forwarding
//!
//! The wrapper forwards **every** [`ResourceStorage`] and [`VersionedStorage`]
//! method to the backend, defaulted ones included (`exists`, `read_batch`,
//! `create_many`, `current_version`, …), so that the backend's overrides
//! (`readiness_check`, `is_cluster_shared`, `delete_versioned`,
//! `bulk_write_concurrency`, and Step 3's `exists` and `read_batch`) are what
//! the tests exercise, never the trait's defaults running over the wrapper.
//! `VersionedStorage` is forwarded for the shared race suite
//! (`search/versioned_write_race_suite.rs`), whose bound it is. The other
//! traits (`SearchProvider`, …) are reached through
//! [`inner`](CassandraTestBackend::inner).

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use helios_fhir::FhirVersion;
use helios_persistence::backends::cassandra::CassandraBackend;
use helios_persistence::core::sof_terminology::SofTerminologyStore;
use helios_persistence::core::{
    ActivityCell, Backend, BackendCapability, BackendKind, CountBasis, DailyResourceCount,
    DiscoveryRequest, ResourceCountDelta, ResourceStorage, SofRunner, TenantDiscovery,
    TenantRecord, VersionedStorage, WriteMarker,
};
use helios_persistence::error::{BackendError, StorageError, StorageResult};
use helios_persistence::search::ReindexTarget;
use helios_persistence::sof::in_process::ResourceScan;
use helios_persistence::tenant::TenantContext;
use helios_persistence::types::StoredResource;
use serde_json::Value;

use crate::backend_skeleton::config_for;
use crate::cassandra_node::{CassandraNode, unique_tenant};
use crate::common::fixtures::TestFixtures;
use crate::common::harness::{TestContext, TestContextBuilder, TestableBackend};

/// The two tenants an instance currently owns.
#[derive(Clone)]
struct Tenants {
    primary: TenantContext,
    secondary: TenantContext,
}

impl Tenants {
    fn fresh(label: &str) -> Self {
        Self {
            primary: unique_tenant(label),
            secondary: unique_tenant(&format!("2-{label}")),
        }
    }
}

/// A [`CassandraBackend`] connected to the test node, with tenants of its own
/// that [`reset`](TestableBackend::reset) rotates. See the module docs.
pub(crate) struct CassandraTestBackend {
    inner: CassandraBackend,
    label: String,
    tenants: RwLock<Tenants>,
}

impl CassandraTestBackend {
    /// Connects to `node` exactly as the other Cassandra tests do
    /// ([`config_for`]: its contact point and the repository's `data/`
    /// directory), checks the ten tables with `initialize`, and takes two
    /// fresh tenants labelled `label` (usually the test's name; the secondary
    /// one is labelled `2-<label>`, and only the first 26 bytes of either
    /// label are kept, the UUID telling them apart). Panics when
    /// the node refuses, like [`crate::backend_skeleton::backend`].
    pub(crate) async fn connect(node: &CassandraNode, label: &str) -> Self {
        let inner = CassandraBackend::connect(config_for(node))
            .await
            .unwrap_or_else(|e| panic!("connect to {}: {e}", node.contact_point()));
        inner
            .initialize()
            .await
            .unwrap_or_else(|e| panic!("initialize on {}: {e}", node.contact_point()));
        Self {
            inner,
            label: label.to_owned(),
            tenants: RwLock::new(Tenants::fresh(label)),
        }
    }

    /// The backend itself, for the traits this wrapper does not forward.
    pub(crate) fn inner(&self) -> &CassandraBackend {
        &self.inner
    }

    /// The tenant `seed` writes under, until the next `reset`.
    pub(crate) fn tenant(&self) -> TenantContext {
        self.tenants().primary
    }

    /// A second tenant of this instance, for isolation tests; also replaced by
    /// `reset`.
    pub(crate) fn secondary_tenant(&self) -> TenantContext {
        self.tenants().secondary
    }

    fn tenants(&self) -> Tenants {
        // A poisoned lock only means a test panicked while holding it; the
        // tenants inside are still whole.
        self.tenants
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    /// A [`TestContext`] whose primary and secondary tenants are this
    /// instance's, holding `fixtures` (not yet seeded). Use it instead of
    /// `TestContext::new`, whose fixed `test-tenant-1` every test on the node
    /// would share; reset it with [`reset_and_seed`].
    pub(crate) fn into_context(self, fixtures: TestFixtures) -> TestContext<Self> {
        let tenants = self.tenants();
        TestContextBuilder::new(self)
            .fixtures(fixtures)
            .tenant_id(tenants.primary.tenant_id().as_str())
            .secondary_tenant_id(tenants.secondary.tenant_id().as_str())
            .build()
    }
}

/// `TestContext::reset_and_seed` for Cassandra: resets the backend, points
/// `ctx.tenant` and `ctx.secondary_tenant` at its new tenants, then seeds
/// `ctx.fixtures` there. The harness's own method cannot re-point them.
pub(crate) async fn reset_and_seed(
    ctx: &mut TestContext<CassandraTestBackend>,
) -> StorageResult<()> {
    ctx.backend.reset().await?;
    ctx.tenant = ctx.backend.tenant();
    ctx.secondary_tenant = ctx.backend.secondary_tenant();
    ctx.backend.seed(&ctx.fixtures).await
}

#[async_trait]
impl TestableBackend for CassandraTestBackend {
    fn backend_kind(&self) -> BackendKind {
        self.inner.kind()
    }

    /// What the backend declares, from the one list it keeps
    /// ([`CassandraBackend::declared_capabilities`]).
    fn supported_capabilities(&self) -> HashSet<BackendCapability> {
        CassandraBackend::declared_capabilities()
            .into_iter()
            .collect()
    }

    /// Rotates to two fresh tenants. Issues no statement; see the module docs
    /// for why it never deletes.
    async fn reset(&self) -> StorageResult<()> {
        let fresh = Tenants::fresh(&self.label);
        *self
            .tenants
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = fresh;
        Ok(())
    }

    /// Creates every fixture under [`tenant`](CassandraTestBackend::tenant),
    /// in [`TestFixtures::all_resources`] order, and stops at the first
    /// failure: it logs the fixture to stderr and returns the backend's own
    /// error unchanged, so a caller can still match `AlreadyExists`.
    async fn seed(&self, fixtures: &TestFixtures) -> StorageResult<()> {
        let tenant = self.tenant();
        let fhir_version = crate::cassandra_node::test_fhir_version();
        for (resource_type, id, resource) in fixtures.all_resources() {
            let stored = self
                .inner
                .create(&tenant, resource_type, resource, fhir_version)
                .await
                .inspect_err(|e| {
                    eprintln!(
                        "seeding {resource_type}/{id} under {}: {e}",
                        tenant.tenant_id()
                    )
                })?;
            if stored.id() != id {
                return Err(StorageError::Backend(BackendError::Internal {
                    backend_name: "cassandra".to_string(),
                    message: format!(
                        "seeding {resource_type}/{id} stored it as {resource_type}/{}",
                        stored.id()
                    ),
                    source: None,
                }));
            }
        }
        Ok(())
    }
}

/// Every method forwards to the backend; see the module docs ("Forwarding").
#[async_trait]
impl ResourceStorage for CassandraTestBackend {
    fn backend_name(&self) -> &'static str {
        self.inner.backend_name()
    }

    fn is_cluster_shared(&self) -> bool {
        self.inner.is_cluster_shared()
    }

    async fn readiness_check(&self) -> Result<(), BackendError> {
        self.inner.readiness_check().await
    }

    async fn create(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        resource: Value,
        fhir_version: FhirVersion,
    ) -> StorageResult<StoredResource> {
        self.inner
            .create(tenant, resource_type, resource, fhir_version)
            .await
    }

    async fn create_many(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        resources: Vec<Value>,
        fhir_version: FhirVersion,
    ) -> Vec<StorageResult<StoredResource>> {
        self.inner
            .create_many(tenant, resource_type, resources, fhir_version)
            .await
    }

    async fn create_or_update(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        resource: Value,
        fhir_version: FhirVersion,
    ) -> StorageResult<(StoredResource, bool)> {
        self.inner
            .create_or_update(tenant, resource_type, id, resource, fhir_version)
            .await
    }

    fn as_search_index(&self) -> Option<&dyn ReindexTarget> {
        self.inner.as_search_index()
    }

    async fn read(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<Option<StoredResource>> {
        self.inner.read(tenant, resource_type, id).await
    }

    async fn update(
        &self,
        tenant: &TenantContext,
        current: &StoredResource,
        resource: Value,
    ) -> StorageResult<StoredResource> {
        self.inner.update(tenant, current, resource).await
    }

    async fn delete(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<()> {
        self.inner.delete(tenant, resource_type, id).await
    }

    async fn delete_versioned(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        expected_version: &str,
    ) -> StorageResult<()> {
        self.inner
            .delete_versioned(tenant, resource_type, id, expected_version)
            .await
    }

    async fn exists(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<bool> {
        self.inner.exists(tenant, resource_type, id).await
    }

    async fn read_batch(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        ids: &[&str],
    ) -> StorageResult<Vec<StoredResource>> {
        self.inner.read_batch(tenant, resource_type, ids).await
    }

    async fn count(
        &self,
        tenant: &TenantContext,
        resource_type: Option<&str>,
    ) -> StorageResult<u64> {
        self.inner.count(tenant, resource_type).await
    }

    fn sof_runner(&self) -> Option<Arc<dyn SofRunner>> {
        self.inner.sof_runner()
    }

    fn sof_terminology_store(&self) -> Option<Arc<dyn SofTerminologyStore>> {
        self.inner.sof_terminology_store()
    }

    fn resource_scan(&self) -> Option<Arc<dyn ResourceScan>> {
        self.inner.resource_scan()
    }

    async fn count_by_types(
        &self,
        tenant: &TenantContext,
        resource_types: &[&str],
    ) -> StorageResult<Vec<(String, u64)>> {
        self.inner.count_by_types(tenant, resource_types).await
    }

    async fn count_by_day(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        since: DateTime<Utc>,
    ) -> StorageResult<Vec<DailyResourceCount>> {
        self.inner.count_by_day(tenant, resource_type, since).await
    }

    async fn count_deltas_by_bucket(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        since: DateTime<Utc>,
        bucket_seconds: i64,
    ) -> StorageResult<Vec<ResourceCountDelta>> {
        self.inner
            .count_deltas_by_bucket(tenant, resource_type, since, bucket_seconds)
            .await
    }

    async fn count_deltas_by_type_and_bucket(
        &self,
        tenant: &TenantContext,
        resource_types: &[&str],
        since: DateTime<Utc>,
        bucket_seconds: i64,
    ) -> StorageResult<Vec<(String, ResourceCountDelta)>> {
        self.inner
            .count_deltas_by_type_and_bucket(tenant, resource_types, since, bucket_seconds)
            .await
    }

    async fn activity_histogram(
        &self,
        tenant: &TenantContext,
        since: DateTime<Utc>,
    ) -> StorageResult<Vec<ActivityCell>> {
        self.inner.activity_histogram(tenant, since).await
    }

    async fn count_all_types(&self, tenant: &TenantContext) -> StorageResult<Vec<(String, u64)>> {
        self.inner.count_all_types(tenant).await
    }

    fn supports_type_counts(&self) -> bool {
        self.inner.supports_type_counts()
    }

    fn type_count_basis(&self) -> Option<CountBasis> {
        self.inner.type_count_basis()
    }

    async fn latest_write_marker(
        &self,
        tenant: &TenantContext,
        recent_since: Option<DateTime<Utc>>,
    ) -> StorageResult<Option<WriteMarker>> {
        self.inner.latest_write_marker(tenant, recent_since).await
    }

    async fn count_by_tenant(&self) -> StorageResult<Vec<(String, u64)>> {
        self.inner.count_by_tenant().await
    }

    async fn discover_tenants(&self, req: &DiscoveryRequest) -> StorageResult<TenantDiscovery> {
        self.inner.discover_tenants(req).await
    }

    async fn tenant_has_resources(&self, tenant: &TenantContext) -> StorageResult<bool> {
        self.inner.tenant_has_resources(tenant).await
    }

    fn bulk_write_concurrency(&self) -> usize {
        self.inner.bulk_write_concurrency()
    }

    fn supports_tenant_registry(&self) -> bool {
        self.inner.supports_tenant_registry()
    }

    async fn list_tenants(&self) -> StorageResult<Vec<TenantRecord>> {
        self.inner.list_tenants().await
    }

    async fn get_tenant(&self, id: &str) -> StorageResult<Option<TenantRecord>> {
        self.inner.get_tenant(id).await
    }

    fn ensure_canonical_tenant_id(&self, id: &str) -> StorageResult<()> {
        self.inner.ensure_canonical_tenant_id(id)
    }

    async fn register_tenant(
        &self,
        id: &str,
        display_name: Option<&str>,
    ) -> StorageResult<TenantRecord> {
        self.inner.register_tenant(id, display_name).await
    }

    async fn deregister_tenant(&self, id: &str) -> StorageResult<bool> {
        self.inner.deregister_tenant(id).await
    }

    async fn purge_tenant_data(&self, id: &str) -> StorageResult<u64> {
        self.inner.purge_tenant_data(id).await
    }

    fn tenant_registry_unsupported(&self) -> StorageError {
        self.inner.tenant_registry_unsupported()
    }
}

/// Every method forwards to the backend, `current_version` included; see the
/// module docs ("Forwarding").
#[async_trait]
impl VersionedStorage for CassandraTestBackend {
    async fn vread(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        version_id: &str,
    ) -> StorageResult<Option<StoredResource>> {
        self.inner
            .vread(tenant, resource_type, id, version_id)
            .await
    }

    async fn update_with_match(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        expected_version: &str,
        resource: Value,
    ) -> StorageResult<StoredResource> {
        self.inner
            .update_with_match(tenant, resource_type, id, expected_version, resource)
            .await
    }

    async fn delete_with_match(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        expected_version: &str,
    ) -> StorageResult<()> {
        self.inner
            .delete_with_match(tenant, resource_type, id, expected_version)
            .await
    }

    async fn current_version(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<Option<String>> {
        self.inner.current_version(tenant, resource_type, id).await
    }

    async fn list_versions(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<Vec<String>> {
        self.inner.list_versions(tenant, resource_type, id).await
    }
}
