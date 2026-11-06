//! [`SearchProvider`] and [`SearchCapabilityProvider`] for
//! [`CassandraBackend`].
//!
//! `search` and `search_count` are Step 6's; until then they return
//! `UnsupportedCapability` (501). `search_param_registry` is real from Step 2
//! on, because the CapabilityStatement, batch, compartment, `$everything`,
//! bulk export and SQL-on-FHIR all call it, with or without search
//! (roadmap Step 2, item 2.3).
//!
//! The capability declaration is the design's: token, date and URI
//! parameters with no modifiers, `_id`, `_lastUpdated`, `_tag`, `_profile` and
//! `_security`, `_count` and cursor paging, `_total`; no `_include`,
//! `_revinclude`, offset paging or sorting (`cassandra-discussion.md`,
//! "Structs and enums": `SearchParamFullCapability`, `PaginationCapability`).

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::RwLock;

use super::backend::CassandraBackend;
use super::errors::unsupported;
use crate::core::capabilities::{
    GlobalSearchCapabilities, ResourceSearchCapabilities, SearchCapabilityProvider,
};
use crate::core::{SearchProvider, SearchResult};
use crate::error::StorageResult;
use crate::search::SearchParameterRegistry;
use crate::tenant::TenantContext;
use crate::types::{
    PaginationCapability, ResultModeCapability, SearchParamFullCapability, SearchParamType,
    SearchQuery, SpecialSearchParam,
};

/// The parameter types Cassandra serves (design, scope table).
pub(super) const SUPPORTED_PARAM_TYPES: [SearchParamType; 3] = [
    SearchParamType::Token,
    SearchParamType::Date,
    SearchParamType::Uri,
];

/// `_count`'s ceiling, which REST already applies (`HFS_MAX_PAGE_SIZE`,
/// default 1000); the backend adds none of its own (C6, ADR-aacruzgon-0027).
const MAX_PAGE_SIZE: u32 = 1000;
/// Page size when the request names none, as every other backend declares.
const DEFAULT_PAGE_SIZE: u32 = 20;

/// The special parameters Cassandra serves: `_id` and `_lastUpdated`, and the
/// token and URI metadata parameters (C1, ADR-aacruzgon-0027). `_source` is
/// served too but has no variant here.
fn special_params() -> Vec<SpecialSearchParam> {
    vec![
        SpecialSearchParam::Id,
        SpecialSearchParam::LastUpdated,
        SpecialSearchParam::Tag,
        SpecialSearchParam::Profile,
        SpecialSearchParam::Security,
    ]
}

fn pagination() -> Vec<PaginationCapability> {
    vec![
        PaginationCapability::Count,
        PaginationCapability::Cursor,
        PaginationCapability::MaxPageSize(MAX_PAGE_SIZE),
        PaginationCapability::DefaultPageSize(DEFAULT_PAGE_SIZE),
    ]
}

#[async_trait]
impl SearchProvider for CassandraBackend {
    async fn search(
        &self,
        _tenant: &TenantContext,
        _query: &SearchQuery,
    ) -> StorageResult<SearchResult> {
        Err(unsupported("search"))
    }

    async fn search_count(
        &self,
        _tenant: &TenantContext,
        _query: &SearchQuery,
    ) -> StorageResult<u64> {
        Err(unsupported("search_count"))
    }

    /// The tenant's registry: the shared base plus that tenant's stored
    /// overlay, built on first use and cached.
    fn search_param_registry(
        &self,
        tenant: &TenantContext,
    ) -> Arc<RwLock<SearchParameterRegistry>> {
        self.registries.for_tenant(tenant.tenant_id().as_str())
    }
}

impl SearchCapabilityProvider for CassandraBackend {
    fn resource_search_capabilities(
        &self,
        resource_type: &str,
    ) -> Option<ResourceSearchCapabilities> {
        let (params, common_params) = {
            let registry = self.registries.base().read();
            (
                registry.get_active_params(resource_type),
                registry.get_active_params("Resource"),
            )
        };
        if params.is_empty() && common_params.is_empty() {
            return None;
        }

        let mut search_params: Vec<SearchParamFullCapability> = Vec::new();
        for param in params.iter().chain(common_params.iter()) {
            if !SUPPORTED_PARAM_TYPES.contains(&param.param_type)
                || search_params.iter().any(|p| p.name == param.code)
            {
                continue;
            }
            // No modifiers: every one is refused with 400 (design, rejection).
            search_params.push(
                SearchParamFullCapability::new(&param.code, param.param_type)
                    .with_definition(&param.url),
            );
        }

        Some(
            ResourceSearchCapabilities::new(resource_type)
                .with_special_params(special_params())
                .with_pagination_capabilities(pagination())
                .with_result_mode_capabilities(vec![ResultModeCapability::Total])
                .with_param_list(search_params),
        )
    }

    fn global_search_capabilities(&self) -> GlobalSearchCapabilities {
        GlobalSearchCapabilities::new()
            .with_special_params(special_params())
            .with_pagination(pagination())
    }
}
