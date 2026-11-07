//! `create_or_update`, including revive (roadmap Step 3, layer L3).
//!
//! Still a stub that returns
//! [`BackendError::UnsupportedCapability`](crate::error::BackendError), which
//! the REST layer answers with 501. L3 replaces the body with the
//! walkthrough's 3.5 (`docs/Cassandra/schema/walkthrough.md`), a bounded loop
//! of turns over the engine in `write.rs`: each turn runs R1, checks it
//! against the request's earlier unknown attempts
//! ([`CassandraBackend::check_earlier_attempts`],
//! ADR-aacruzgon-0051), and picks
//!
//! - branch a, an absent row: C1 ([`CasPlan::create`](super::write::CasPlan::create)),
//!   permission `Create`;
//! - branch b, a live row: C2 through
//!   [`CassandraBackend::update_attempt`],
//!   permission `Update`;
//! - branch c, a delete marker: C4, the revive
//!   ([`CasPlan::revive`](super::write::CasPlan::revive), `deleted_at = 0`,
//!   G39), permission `Update`.
//!
//! A definite miss starts the next turn at once, for at most
//! [`UPSERT_MISS_RETRIES`](super::write::UPSERT_MISS_RETRIES) retries; an
//! unknown outcome is settled by the engine from its own budget of re-runs.
//! `check_id` runs first (D11).

use helios_fhir::FhirVersion;
use serde_json::Value;

use super::backend::CassandraBackend;
use super::errors::unsupported;
use crate::error::StorageResult;
use crate::tenant::TenantContext;
use crate::types::StoredResource;

impl CassandraBackend {
    /// `ResourceStorage::create_or_update`; see the module docs. Returns the
    /// stored resource and whether it was created (`true` for branches a
    /// and c).
    pub(super) async fn create_or_update_resource(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
        _resource: Value,
        _fhir_version: FhirVersion,
    ) -> StorageResult<(StoredResource, bool)> {
        Err(unsupported("create_or_update"))
    }
}
