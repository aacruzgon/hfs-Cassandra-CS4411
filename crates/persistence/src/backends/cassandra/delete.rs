//! `delete` and `delete_versioned` (roadmap Step 3, layer L2b).
//!
//! Both are still stubs that return
//! [`BackendError::UnsupportedCapability`](crate::error::BackendError), which
//! the REST layer answers with 501. L2b replaces the bodies with the
//! walkthrough's 3.4 (`docs/Cassandra/schema/walkthrough.md`): R1, V3 from
//! R1's row, I1, C3 through the engine in `write.rs`
//! ([`CasPlan::delete`](super::write::CasPlan::delete)), V2, the delete of
//! every key of `K(N)`, I2. A plain `delete` retries once after a definite
//! miss ([`PLAIN_DELETE_MISS_RETRIES`](super::write::PLAIN_DELETE_MISS_RETRIES),
//! G25); `delete_versioned` never retries. A delete of an absent or deleted
//! row is `NotFound`, which REST answers with 204 for a plain delete
//! (ADR-aacruzgon-0052).
//!
//! `delete_versioned` must stay its own compare-and-swap: the trait default
//! reads, compares and then calls `delete`, which is not atomic (#1404).

use super::backend::CassandraBackend;
use super::errors::unsupported;
use crate::error::StorageResult;
use crate::tenant::TenantContext;

impl CassandraBackend {
    /// `ResourceStorage::delete`; see the module docs.
    pub(super) async fn delete_resource(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
    ) -> StorageResult<()> {
        Err(unsupported("delete"))
    }

    /// `ResourceStorage::delete_versioned`; see the module docs. It refuses
    /// before reading, so it never reaches the trait default.
    pub(super) async fn delete_versioned_resource(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
        _expected_version: &str,
    ) -> StorageResult<()> {
        Err(unsupported("delete_versioned"))
    }
}
