//! `vread`, `list_versions` and `delete_with_match` (pulled into roadmap
//! Step 3, layer L2c, because the race suite needs them), and `exists` and
//! `read_batch`.
//!
//! - `vread`, `list_versions` and `delete_with_match` are still stubs that
//!   return [`BackendError::UnsupportedCapability`](crate::error::BackendError),
//!   which the REST layer answers with 501. L2c replaces them with the
//!   walkthrough's 4.2 and 4.3 (`docs/Cassandra/schema/walkthrough.md`), and
//!   `delete_with_match` with the `If-Match` parse and `delete_versioned`.
//! - `exists` and `read_batch` keep the trait defaults' logic
//!   (`crate::core::storage`), on top of `read`. L2c overrides them (D10):
//!   `exists` reading only `deleted_at`, `read_batch` with concurrent point
//!   reads.

use super::backend::CassandraBackend;
use super::errors::unsupported;
use crate::core::ResourceStorage;
use crate::error::{ResourceError, StorageError, StorageResult};
use crate::tenant::TenantContext;
use crate::types::StoredResource;

impl CassandraBackend {
    /// `VersionedStorage::vread`; see the module docs.
    pub(super) async fn vread_resource(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
        _version_id: &str,
    ) -> StorageResult<Option<StoredResource>> {
        Err(unsupported("vread"))
    }

    /// `VersionedStorage::list_versions`; see the module docs.
    pub(super) async fn list_resource_versions(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
    ) -> StorageResult<Vec<String>> {
        Err(unsupported("list_versions"))
    }

    /// `VersionedStorage::delete_with_match`; see the module docs.
    pub(super) async fn delete_with_match_resource(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
        _expected_version: &str,
    ) -> StorageResult<()> {
        Err(unsupported("delete_with_match"))
    }

    /// `ResourceStorage::exists`: the trait default's logic. A deleted
    /// resource is `false`, never an error.
    pub(super) async fn exists_resource(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        id: &str,
    ) -> StorageResult<bool> {
        match self.read(tenant, resource_type, id).await {
            Ok(found) => Ok(found.is_some()),
            Err(StorageError::Resource(ResourceError::Gone { .. })) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// `ResourceStorage::read_batch`: the trait default's logic, one read
    /// after another. Missing and deleted ids are skipped; any other error
    /// fails the batch.
    pub(super) async fn read_batch_resources(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        ids: &[&str],
    ) -> StorageResult<Vec<StoredResource>> {
        let mut results = Vec::with_capacity(ids.len());
        for id in ids {
            match self.read(tenant, resource_type, id).await {
                Ok(Some(resource)) => results.push(resource),
                Ok(None) => {}
                Err(StorageError::Resource(ResourceError::Gone { .. })) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(results)
    }
}
