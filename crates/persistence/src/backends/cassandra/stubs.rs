//! The traits the server's bound requires but the Cassandra backend does not
//! serve (`cassandra-discussion.md`, "Traits, structs and enums").
//!
//! Each method refuses explicitly ("refuse explicitly", Discussion #28): it
//! returns `UnsupportedCapability { backend_name: "cassandra", capability }`,
//! which the REST layer answers with 501, except `BundleProvider`, which
//! refuses a transaction Bundle with `TransactionError::AtomicityUnsupported`
//! as S3 does. None of these is planned for this project:
//!
//! - **Conditional operations** (✗): `supports_conditional` is `false` for
//!   every interaction, overriding the default that claims all of them.
//! - **`_include` and `_revinclude`** (✗): REST resolves includes through
//!   `SearchProvider`, never through these traits, so the refusal that counts
//!   is in `search()` (Step 6). These exist so the server compiles.
//! - **History** (✗): instance, type and system history, and the two
//!   history-deletion operations whose defaults name no backend.
//! - **Transaction Bundles** (✗): batch Bundles do not use this trait; REST
//!   runs each entry against `ResourceStorage`.
//! - **Bulk export** (out of scope).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use helios_fhir::FhirVersion;
use serde_json::Value;

use super::backend::CassandraBackend;
use super::errors::{BACKEND_NAME, unsupported};
use crate::core::{
    BundleEntry, BundleProvider, BundleResult, ConditionalCreateResult, ConditionalDeleteResult,
    ConditionalInteraction, ConditionalStorage, ConditionalUpdateResult, EntityTagPrecondition,
    ExportDataProvider, ExportRequest, GroupExportProvider, HistoryPage, HistoryParams,
    IncludeProvider, InstanceHistoryProvider, NdjsonBatch, PatchCandidateValidator,
    PatientExportProvider, RevincludeProvider, SystemHistoryProvider, TypeHistoryProvider,
};
use crate::error::{StorageResult, TransactionError};
use crate::tenant::TenantContext;
use crate::types::{IncludeDirective, StoredResource};

#[async_trait]
impl ConditionalStorage for CassandraBackend {
    /// No conditional interaction is supported; the default answers `true`
    /// for create, update and delete.
    fn supports_conditional(&self, _interaction: ConditionalInteraction) -> bool {
        false
    }

    async fn conditional_create(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _resource: Value,
        _search_params: &str,
        _fhir_version: FhirVersion,
    ) -> StorageResult<ConditionalCreateResult> {
        Err(unsupported("conditional_create"))
    }

    async fn conditional_update(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _resource: Value,
        _search_params: &str,
        _upsert: bool,
        _fhir_version: FhirVersion,
        _if_match: &EntityTagPrecondition,
    ) -> StorageResult<ConditionalUpdateResult> {
        Err(unsupported("conditional_update"))
    }

    async fn conditional_delete(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _search_params: &str,
        _if_match: &EntityTagPrecondition,
    ) -> StorageResult<ConditionalDeleteResult> {
        Err(unsupported("conditional_delete"))
    }
}

#[async_trait]
impl IncludeProvider for CassandraBackend {
    async fn resolve_includes(
        &self,
        _tenant: &TenantContext,
        _resources: &[StoredResource],
        _includes: &[IncludeDirective],
    ) -> StorageResult<Vec<StoredResource>> {
        Err(unsupported("_include"))
    }
}

#[async_trait]
impl RevincludeProvider for CassandraBackend {
    async fn resolve_revincludes(
        &self,
        _tenant: &TenantContext,
        _resources: &[StoredResource],
        _revincludes: &[IncludeDirective],
    ) -> StorageResult<Vec<StoredResource>> {
        Err(unsupported("_revinclude"))
    }
}

#[async_trait]
impl InstanceHistoryProvider for CassandraBackend {
    async fn history_instance(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
        _params: &HistoryParams,
    ) -> StorageResult<HistoryPage> {
        Err(unsupported("history-instance"))
    }

    async fn history_instance_count(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
    ) -> StorageResult<u64> {
        Err(unsupported("history-instance"))
    }

    async fn delete_instance_history(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
    ) -> StorageResult<u64> {
        Err(unsupported("delete_instance_history"))
    }

    async fn delete_version(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _id: &str,
        _version_id: &str,
    ) -> StorageResult<()> {
        Err(unsupported("delete_version"))
    }
}

#[async_trait]
impl TypeHistoryProvider for CassandraBackend {
    async fn history_type(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
        _params: &HistoryParams,
    ) -> StorageResult<HistoryPage> {
        Err(unsupported("history-type"))
    }

    async fn history_type_count(
        &self,
        _tenant: &TenantContext,
        _resource_type: &str,
    ) -> StorageResult<u64> {
        Err(unsupported("history-type"))
    }
}

#[async_trait]
impl SystemHistoryProvider for CassandraBackend {
    async fn history_system(
        &self,
        _tenant: &TenantContext,
        _params: &HistoryParams,
    ) -> StorageResult<HistoryPage> {
        Err(unsupported("history-system"))
    }

    async fn history_system_count(&self, _tenant: &TenantContext) -> StorageResult<u64> {
        Err(unsupported("history-system"))
    }
}

#[async_trait]
impl BundleProvider for CassandraBackend {
    /// A `resources` row and its index rows live in different partitions, and
    /// Cassandra has no multi-partition transaction (Accord is not generally
    /// available), so a transaction Bundle cannot be all-or-nothing.
    fn supports_atomic_transactions(&self) -> bool {
        false
    }

    fn supports_conditional_in_transaction(&self) -> bool {
        false
    }

    async fn process_transaction_with_patch_validator(
        &self,
        _tenant: &TenantContext,
        _entries: Vec<BundleEntry>,
        _fhir_version: FhirVersion,
        _validator: Option<&dyn PatchCandidateValidator>,
    ) -> Result<BundleResult, TransactionError> {
        // Refused before any work, as S3 does: a partial commit is worse than
        // a rejection.
        Err(TransactionError::AtomicityUnsupported {
            backend_name: BACKEND_NAME.to_string(),
        })
    }
}

#[async_trait]
impl ExportDataProvider for CassandraBackend {
    async fn list_export_types(
        &self,
        _tenant: &TenantContext,
        _request: &ExportRequest,
    ) -> StorageResult<Vec<String>> {
        Err(unsupported("bulk_export"))
    }

    async fn count_export_resources(
        &self,
        _tenant: &TenantContext,
        _request: &ExportRequest,
        _resource_type: &str,
    ) -> StorageResult<u64> {
        Err(unsupported("bulk_export"))
    }

    async fn fetch_export_batch(
        &self,
        _tenant: &TenantContext,
        _request: &ExportRequest,
        _resource_type: &str,
        _cursor: Option<&str>,
        _batch_size: u32,
    ) -> StorageResult<NdjsonBatch> {
        Err(unsupported("bulk_export"))
    }
}

#[async_trait]
impl PatientExportProvider for CassandraBackend {
    async fn list_patient_ids(
        &self,
        _tenant: &TenantContext,
        _request: &ExportRequest,
        _cursor: Option<&str>,
        _batch_size: u32,
    ) -> StorageResult<(Vec<String>, Option<String>)> {
        Err(unsupported("bulk_export"))
    }

    async fn fetch_patient_compartment_batch(
        &self,
        _tenant: &TenantContext,
        _request: &ExportRequest,
        _resource_type: &str,
        _patient_ids: &[String],
        _cursor: Option<&str>,
        _batch_size: u32,
    ) -> StorageResult<NdjsonBatch> {
        Err(unsupported("bulk_export"))
    }
}

#[async_trait]
impl GroupExportProvider for CassandraBackend {
    async fn get_group_members(
        &self,
        _tenant: &TenantContext,
        _group_id: &str,
    ) -> StorageResult<Vec<String>> {
        Err(unsupported("bulk_export"))
    }

    async fn resolve_group_patient_ids(
        &self,
        _tenant: &TenantContext,
        _group_id: &str,
    ) -> StorageResult<Vec<String>> {
        Err(unsupported("bulk_export"))
    }

    async fn get_group_members_with_periods(
        &self,
        _tenant: &TenantContext,
        _group_id: &str,
    ) -> StorageResult<Vec<(String, Option<DateTime<Utc>>)>> {
        Err(unsupported("bulk_export"))
    }
}
