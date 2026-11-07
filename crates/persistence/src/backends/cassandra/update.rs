//! `update` (roadmap Step 3, layer L2a), and the one C2 attempt
//! `create_or_update`'s branch b shares with it.
//!
//! Still stubs that return
//! [`BackendError::UnsupportedCapability`](crate::error::BackendError), which
//! the REST layer answers with 501. L2a replaces the bodies with the
//! walkthrough's 3.3 (`docs/Cassandra/schema/walkthrough.md`):
//!
//! - `update`: `Operation::Update`, the G38 `code` bound
//!   ([`check_search_parameter_code`](super::write::check_search_parameter_code)),
//!   then R1 ([`CassandraBackend::read_resource_row`]) for `K(N)` (G22) and
//!   R1's version cases (walkthrough `:176-180`): at `N`, the attempt; higher,
//!   the miss without a CAS; lower, R2 first; absent, `NotFound`. Then
//!   [`CassandraBackend::update_attempt`]. `current`'s `fhir_version` is kept
//!   and `created_at` untouched. A definite miss is the error
//!   ([`CasMiss::into_error`](super::write::CasMiss::into_error)); `update`
//!   never retries.
//! - [`CassandraBackend::update_attempt`]: one C2 turn from an R1 row at `N`,
//!   through the engine ([`CasPlan::update`](super::write::CasPlan::update),
//!   whose V3 is built from R1 and so carries `w(N)`, D5). L3's branch b
//!   calls it with the row of its own R1.

use serde_json::Value;

use super::backend::CassandraBackend;
use super::errors::unsupported;
use super::read::ResourceRow;
use super::write::{CasOutcome, CasRequest};
use crate::error::StorageResult;
use crate::tenant::TenantContext;
use crate::types::StoredResource;

impl CassandraBackend {
    /// `ResourceStorage::update`; see the module docs.
    pub(super) async fn update_resource(
        &self,
        _tenant: &TenantContext,
        _current: &StoredResource,
        _resource: Value,
    ) -> StorageResult<StoredResource> {
        Err(unsupported("update"))
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
    #[allow(dead_code)] // L2a's update and L3's branch b.
    pub(crate) async fn update_attempt(
        &self,
        _request: &mut CasRequest<'_>,
        _row: &ResourceRow,
        _resource: Value,
    ) -> StorageResult<CasOutcome> {
        Err(unsupported("update"))
    }
}
