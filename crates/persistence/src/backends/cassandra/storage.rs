//! [`ResourceStorage`] and [`VersionedStorage`] for [`CassandraBackend`]:
//! one-line delegations to the file that implements each method, so that
//! each of roadmap Step 3's parts owns its own file (`development-roadmap.md`
//! ground rule "One owner per shared file").
//!
//! | Methods | File | Status |
//! | --- | --- | --- |
//! | `create` | `write.rs`, on the shared write engine | real |
//! | `read`, `count` | `read.rs` | real |
//! | `update` | `update.rs`, on the write engine | real (Step 3, L2a) |
//! | `delete`, `delete_versioned` | `delete.rs`, on the write engine | real (Step 3, L2b) |
//! | `create_or_update` | `upsert.rs` | stub (Step 3, L3) |
//! | `vread`, `list_versions`, `delete_with_match` | `versions.rs` | real (Step 3, L2c; pulled forward from Step 4, D1) |
//! | `exists`, `read_batch` | `versions.rs` | real (Step 3, L2c): `exists` reads only `deleted_at`, `read_batch` reads concurrently (D10) |
//! | `update_with_match` | here | stub (Step 4) |
//!
//! A stub returns
//! [`BackendError::UnsupportedCapability`](crate::error::BackendError), which
//! the REST layer answers with 501. `count` is pulled forward from Step 3
//! with the `resources_by_type` row every create writes, so the conformance
//! seeder, which counts before it creates, works at boot.
//!
//! The provided defaults the design lists as wrong for a networked store are
//! overridden here (`cassandra-discussion.md`, "Defaults we must override"):
//! `readiness_check`, `is_cluster_shared`, `bulk_write_concurrency` and
//! `delete_versioned`. `create_many` keeps its default, which calls
//! `create`; `exists` and `read_batch` are overridden too (D10).

use async_trait::async_trait;
use helios_fhir::FhirVersion;
use serde_json::Value;

use super::backend::CassandraBackend;
use super::errors::{BACKEND_NAME, unsupported};
use crate::core::{Backend, ResourceStorage, VersionedStorage};
use crate::error::{BackendError, StorageResult};
use crate::tenant::TenantContext;
use crate::types::StoredResource;

/// The concurrency `create_many` and batch Bundles use: PostgreSQL's and
/// MongoDB's. Batch caps it at `HFS_BATCH_MAX_CONCURRENCY` and forces 1 when
/// any entry is conditional.
pub(super) const BULK_WRITE_CONCURRENCY: usize = 8;

#[async_trait]
impl ResourceStorage for CassandraBackend {
    fn backend_name(&self) -> &'static str {
        BACKEND_NAME
    }

    /// Cassandra is a networked store every instance shares.
    fn is_cluster_shared(&self) -> bool {
        true
    }

    /// Touches the node, so `/_readiness` reports down while Cassandra is
    /// down (the default answers `Ok` unconditionally).
    async fn readiness_check(&self) -> Result<(), BackendError> {
        self.health_check().await
    }

    async fn create(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        resource: Value,
        fhir_version: FhirVersion,
    ) -> StorageResult<StoredResource> {
        self.create_resource(tenant, resource_type, resource, fhir_version)
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
        self.create_or_update_resource(tenant, resource_type, id, resource, fhir_version)
            .await
    }

    async fn read(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<Option<StoredResource>> {
        self.read_resource(tenant, resource_type, id).await
    }

    async fn exists(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<bool> {
        self.exists_resource(tenant, resource_type, id).await
    }

    async fn read_batch(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        ids: &[&str],
    ) -> StorageResult<Vec<StoredResource>> {
        self.read_batch_resources(tenant, resource_type, ids).await
    }

    async fn update(
        &self,
        tenant: &TenantContext,
        current: &StoredResource,
        resource: Value,
    ) -> StorageResult<StoredResource> {
        self.update_resource(tenant, current, resource).await
    }

    async fn delete(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<()> {
        self.delete_resource(tenant, resource_type, id).await
    }

    /// Overridden, never the default: the default reads, compares and then
    /// calls `delete`, which is not atomic, so a writer between the read and
    /// the delete would be deleted along with the version the client named
    /// (#1404). `delete.rs` gives it its own compare-and-swap.
    async fn delete_versioned(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        expected_version: &str,
    ) -> StorageResult<()> {
        self.delete_versioned_resource(tenant, resource_type, id, expected_version)
            .await
    }

    async fn count(
        &self,
        tenant: &TenantContext,
        resource_type: Option<&str>,
    ) -> StorageResult<u64> {
        self.count_resources(tenant, resource_type).await
    }

    fn bulk_write_concurrency(&self) -> usize {
        BULK_WRITE_CONCURRENCY
    }
}

#[async_trait]
impl VersionedStorage for CassandraBackend {
    async fn vread(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        version_id: &str,
    ) -> StorageResult<Option<StoredResource>> {
        self.vread_resource(tenant, resource_type, id, version_id)
            .await
    }

    async fn update_with_match(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
        _expected_version: &str,
        _resource: Value,
    ) -> StorageResult<StoredResource> {
        Err(unsupported("update_with_match"))
    }

    async fn delete_with_match(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
        expected_version: &str,
    ) -> StorageResult<()> {
        self.delete_with_match_resource(tenant, resource_type, id, expected_version)
            .await
    }

    async fn list_versions(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<Vec<String>> {
        self.list_resource_versions(tenant, resource_type, id).await
    }
}
