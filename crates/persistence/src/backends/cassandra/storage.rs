//! [`ResourceStorage`] and [`VersionedStorage`] for [`CassandraBackend`].
//!
//! Roadmap Step 2 serves `create` (`write.rs`), `read` and `count`
//! (`read.rs`); Step 3 and Step 4 fill in the rest. A stub returns
//! [`BackendError::UnsupportedCapability`](crate::error::BackendError), which
//! the REST layer answers with 501. `count` is pulled forward from Step 3
//! with the `resources_by_type` row every create writes, so the conformance
//! seeder, which counts before it creates, works at boot.
//!
//! The provided defaults the design lists as wrong for a networked store are
//! overridden here (`cassandra-discussion.md`, "Defaults we must override"):
//! `readiness_check`, `is_cluster_shared`, `bulk_write_concurrency` and
//! `delete_versioned`. `read_batch` and `create_many` keep their defaults for
//! now: both only call `read` and `create`, so they are correct, and Step 3
//! makes `read_batch` concurrent.

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
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
        _resource: Value,
        _fhir_version: FhirVersion,
    ) -> StorageResult<(StoredResource, bool)> {
        Err(unsupported("create_or_update"))
    }

    async fn read(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<Option<StoredResource>> {
        self.read_resource(tenant, resource_type, id).await
    }

    async fn update(
        &self,
        _tenant: &TenantContext,
        _current: &StoredResource,
        _resource: Value,
    ) -> StorageResult<StoredResource> {
        Err(unsupported("update"))
    }

    async fn delete(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
    ) -> StorageResult<()> {
        Err(unsupported("delete"))
    }

    /// Refused outright until Step 3 gives it its compare-and-swap. The
    /// default reads, compares and then calls `delete`, which is not atomic:
    /// a writer between the read and the delete would be deleted along with
    /// the version the client named (#1404).
    async fn delete_versioned(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
        _expected_version: &str,
    ) -> StorageResult<()> {
        Err(unsupported("delete_versioned"))
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
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
        _version_id: &str,
    ) -> StorageResult<Option<StoredResource>> {
        Err(unsupported("vread"))
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
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
        _expected_version: &str,
    ) -> StorageResult<()> {
        Err(unsupported("delete_with_match"))
    }

    async fn list_versions(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
    ) -> StorageResult<Vec<String>> {
        Err(unsupported("list_versions"))
    }
}
