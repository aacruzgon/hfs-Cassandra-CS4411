//! [`SearchProvider`] and [`SearchCapabilityProvider`] for
//! [`CassandraBackend`].
//!
//! `search` and `search_count` are Step 6's; until then they return
//! `UnsupportedCapability` (501), and `supports_search` is `false`, so
//! `/metadata` lists no `search-type` and no search parameters (roadmap Step 7,
//! ADR-aacruzgon-0085). `search_param_registry` is real from Step 2 on, because
//! the CapabilityStatement, batch, compartment, `$everything`, bulk export and
//! SQL-on-FHIR all call it, with or without search (roadmap Step 2, item 2.3).
//!
//! The capability declaration is the design's: token, date and URI
//! parameters with no modifiers, `_id`, `_lastUpdated`, `_tag`, `_profile` and
//! `_security`, `_count` and cursor paging, `_total`; no `_include`,
//! `_revinclude`, offset paging or sorting (`cassandra-discussion.md`,
//! "Structs and enums": `SearchParamFullCapability`, `PaginationCapability`).
//!
//! # The interface the per-type modules implement
//!
//! Step 6 is written by several people in parallel (#127): this file is the
//! framework (#128), and `search/token.rs` (6a, #130), `search/date.rs` (6b,
//! #132), `search/uri.rs` (6c, #131) and `search/paging.rs` (6d, #129) plug
//! into it. The contract is ADR-anastasiabryz-0001:
//!
//! - each module turns one [`SearchParameter`](crate::types::SearchParameter), with all its comma values,
//!   into a [`ParamPlan`] through its `compile` function;
//! - the framework picks the driving plan by [`Drive`] (C5's order), streams
//!   its [`Candidate`]s, checks the others with [`ParamPlan::contains`], runs
//!   the tag check, the residuals ([`ParamPlan::residual`]) and the candidate
//!   cap ([`CandidateBudget`]), and queues repair (walkthrough 5.1, 5.8);
//! - only `paging.rs` encodes or decodes a cursor.
//!
//! A change to this contract goes through #128 and is announced on every
//! module issue before it merges.

mod date;
mod paging;
mod token;
mod uri;

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use parking_lot::{Mutex, RwLock};

use super::backend::CassandraBackend;
use super::errors::unsupported;
use super::index_keys::IndexKey;
use super::read::ResourceRow;
use super::session::CassandraSession;
use crate::core::capabilities::{
    GlobalSearchCapabilities, ResourceSearchCapabilities, SearchCapabilityProvider,
};
use crate::core::{SearchProvider, SearchResult};
use crate::error::{SearchError, StorageResult};
use crate::search::{ExtractedValue, SearchParameterDefinition, SearchParameterRegistry};
use crate::tenant::TenantContext;
use crate::types::{
    PaginationCapability, ResultModeCapability, SearchParamFullCapability, SearchParamType,
    SearchQuery, SpecialSearchParam,
};
use paging::{OrderKey, Position};

/// One index row as read, for the tag check and for repair.
///
/// `key` is built from the stored columns, never passed through `index_text`
/// again: a stored value is already what `index_text` produced
/// (ADR-aacruzgon-0079). The fields are the arguments of
/// [`StaleIndexRow::new`](super::repair::StaleIndexRow::new).
#[allow(dead_code)] // The search engine (#128) reads it; until then only the modules' types do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FoundRow {
    pub(crate) key: IndexKey,
    /// The row's `last_updated`; `None` for `date_index` and
    /// `date_index_by_end`, whose key has none.
    pub(crate) last_updated: Option<DateTime<Utc>>,
    /// The row's tag, checked against `resources.version_id`.
    pub(crate) version_id: i32,
}

/// One resource a plan's stream yields: where it sits in the stream's order,
/// its id, and every index row of it read at that order key (for example its
/// rows in both the named system's and the `IMPLICIT` partition).
#[allow(dead_code)] // The search engine (#128) reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Candidate {
    pub(crate) order: OrderKey,
    pub(crate) id: String,
    pub(crate) rows: Vec<FoundRow>,
}

/// A candidate after the tag check: its R1 row, and the values the framework
/// extracted from it once (under the resource's own `fhir_version` and the
/// tenant's registry, on the write path's large-stack thread in debug
/// builds), which the date and compartment residuals both read. `extracted`
/// is empty when no residual needs it.
#[allow(dead_code)] // The search engine (#128) builds it.
#[derive(Debug, Clone)]
pub(crate) struct LoadedResource {
    pub(crate) row: ResourceRow,
    pub(crate) extracted: Vec<ExtractedValue>,
}

/// How strongly a plan can drive a search with several parameters: C5's
/// fixed order (walkthrough 5.8, ADR-aacruzgon-0027), most specific first.
/// The planner takes the lowest across the whole query; ties go by query
/// order.
#[allow(dead_code)] // The modules' plans return it once they exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Drive {
    /// The framework's own `_id` plan (walkthrough 5.4).
    Id,
    /// Token `system|code` and `|code`.
    SystemCode,
    /// URI: a canonical resource's own `url` (walkthrough 5.8's list).
    IdentityUri,
    /// Token `code`; `_profile`, `_source` and every other URI.
    Shared,
    /// Token `system|`.
    SystemOnly,
    /// The framework's `_lastUpdated` plan (5.5): drives only when nothing
    /// above is present.
    LastUpdated,
    /// Date: drives only when every parameter is a date (G20); otherwise it
    /// is a residual.
    DateOnly,
}

/// Counts the distinct candidates a search point-reads (R1) for one page or
/// one `search_count`, against the candidate cap (G27, ADR-anastasiabryz-0002).
///
/// Only the framework charges it, at the one place R1 is read for a
/// candidate. Modules never do: neither their index reads nor their
/// `contains` point reads are candidates point-read. A search is of one
/// resource type, so the id alone tells candidates apart.
#[allow(dead_code)] // The search engine (#128) charges it.
#[derive(Debug)]
pub(crate) struct CandidateBudget {
    max: usize,
    seen: Mutex<HashSet<String>>,
}

#[allow(dead_code)] // The search engine (#128) charges it.
impl CandidateBudget {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            max,
            seen: Mutex::new(HashSet::new()),
        }
    }

    /// Counts one R1 read of `id`; reading the same id again costs nothing.
    /// Past the cap it is `TooManyResults`, which REST answers with 422.
    pub(crate) fn charge(&self, id: &str) -> Result<(), SearchError> {
        let mut seen = self.seen.lock();
        seen.insert(id.to_string());
        if seen.len() > self.max {
            return Err(SearchError::TooManyResults {
                count: seen.len(),
                max: self.max,
            });
        }
        Ok(())
    }

    /// How many distinct candidates have been charged.
    pub(crate) fn used(&self) -> usize {
        self.seen.lock().len()
    }
}

/// What every read of a search needs.
#[allow(dead_code)] // The search engine (#128) builds it.
pub(crate) struct ReadCtx<'a> {
    pub(crate) tenant: &'a TenantContext,
    pub(crate) resource_type: &'a str,
    pub(crate) session: &'a CassandraSession,
    pub(crate) budget: &'a CandidateBudget,
}

/// What a module's `compile` gets besides the parameter itself.
#[allow(dead_code)] // The search engine (#128) builds it.
pub(crate) struct CompileCtx<'a> {
    pub(crate) resource_type: &'a str,
    /// The parameter's registered definition, when the tenant's registry has
    /// one (an identity URI is recognised by its expression, walkthrough 5.8).
    pub(crate) definition: Option<&'a SearchParameterDefinition>,
}

/// One parameter of a search, compiled by its module (ADR-anastasiabryz-0001).
#[allow(dead_code)] // The modules implement it and the search engine (#128) calls it.
#[async_trait]
pub(crate) trait ParamPlan: Send + Sync {
    /// The parameter's name.
    fn param(&self) -> &str;

    /// Its rank as a driver.
    fn drive(&self) -> Drive;

    /// Its candidates strictly after `from`, in `from`'s order, merged across
    /// its partitions and OR values and collapsed on the order key. Each CQL
    /// read takes a `LIMIT` batch (a batch size, not the page size) through
    /// [`CassandraSession::rows`].
    fn stream<'a>(
        &'a self,
        ctx: &'a ReadCtx<'a>,
        from: &'a Position,
    ) -> BoxStream<'a, StorageResult<Candidate>>;

    /// Whether a candidate another plan drives matches this parameter: the
    /// clustering point reads at `last_updated = ? AND id = ?`, one per
    /// partition and per OR value, ORed (walkthrough 5.8).
    async fn contains(
        &self,
        ctx: &ReadCtx<'_>,
        last_updated: DateTime<Utc>,
        id: &str,
    ) -> StorageResult<bool>;

    /// The residual check on the loaded resource. Default: it matches.
    fn residual(&self, _loaded: &LoadedResource) -> bool {
        true
    }
}

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
    /// From the declared [`BackendCapability::BasicSearch`], as S3 answers
    /// (#514): one declaration, which `/metadata` reads. Cassandra does not
    /// declare it while `search` refuses everything, so `/metadata` lists no
    /// `search-type`, `searchParam`, `searchInclude` or `searchRevInclude`.
    /// Step 6's last commit declares it, which turns this `true`, and
    /// overrides `serves_search_param` with [`SUPPORTED_PARAM_TYPES`]
    /// (roadmap Step 7, row 7.2).
    ///
    /// [`BackendCapability::BasicSearch`]: crate::core::BackendCapability::BasicSearch
    fn supports_search(&self) -> bool {
        crate::core::Backend::supports(self, crate::core::BackendCapability::BasicSearch)
    }

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

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::error::BackendError;
    use crate::error::StorageError;
    use crate::types::SearchParameter;

    /// A plan that matches nothing: proves a module can implement
    /// [`ParamPlan`] as written (the interface PR's compile-only check).
    struct NoMatches;

    #[async_trait]
    impl ParamPlan for NoMatches {
        fn param(&self) -> &str {
            "status"
        }

        fn drive(&self) -> Drive {
            Drive::Shared
        }

        fn stream<'a>(
            &'a self,
            _ctx: &'a ReadCtx<'a>,
            _from: &'a Position,
        ) -> BoxStream<'a, StorageResult<Candidate>> {
            futures::stream::empty().boxed()
        }

        async fn contains(
            &self,
            _ctx: &ReadCtx<'_>,
            _last_updated: DateTime<Utc>,
            _id: &str,
        ) -> StorageResult<bool> {
            Ok(false)
        }
    }

    #[test]
    fn a_module_can_implement_the_plan_trait() {
        let plan: Box<dyn ParamPlan> = Box::new(NoMatches);
        assert_eq!(plan.param(), "status");
        assert_eq!(plan.drive(), Drive::Shared);
    }

    #[test]
    fn drive_ranks_in_c5_order() {
        let ranks = [
            Drive::Id,
            Drive::SystemCode,
            Drive::IdentityUri,
            Drive::Shared,
            Drive::SystemOnly,
            Drive::LastUpdated,
            Drive::DateOnly,
        ];
        assert!(ranks.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(ranks.iter().min(), Some(&Drive::Id));
    }

    #[test]
    fn the_budget_counts_distinct_candidates_and_stops_past_the_cap() {
        let budget = CandidateBudget::new(2);
        budget.charge("a").unwrap();
        budget.charge("a").unwrap();
        budget.charge("b").unwrap();
        assert_eq!(budget.used(), 2);
        let error = budget.charge("c").unwrap_err();
        assert!(matches!(
            error,
            SearchError::TooManyResults { count: 3, max: 2 }
        ));
    }

    #[test]
    fn every_module_stub_refuses_until_its_issue_lands() {
        let param = SearchParameter {
            name: "status".to_string(),
            param_type: SearchParamType::Token,
            ..Default::default()
        };
        let ctx = CompileCtx {
            resource_type: "Observation",
            definition: None,
        };
        for (module, compiled) in [
            ("token", token::compile(&param, &ctx)),
            ("uri", uri::compile(&param, &ctx)),
            ("date", date::compile(&param, &ctx)),
        ] {
            assert!(
                matches!(
                    compiled,
                    Err(StorageError::Backend(
                        BackendError::UnsupportedCapability { .. }
                    ))
                ),
                "{module}::compile should refuse until its issue lands"
            );
        }
    }
}
