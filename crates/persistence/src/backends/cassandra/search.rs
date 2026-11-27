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
use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::rows::Row;
use cdrs_tokio::types::value::Value;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream, StreamExt};
use parking_lot::{Mutex, RwLock};

use super::backend::CassandraBackend;
use super::errors::{BACKEND_NAME, map_driver_error, unsupported};
use super::index_keys::IndexKey;
use super::read::ResourceRow;
use super::repair::{RepairContext, StaleIndexRow};
use super::session::{CassandraSession, StatementKind};
use crate::core::capabilities::{
    GlobalSearchCapabilities, ResourceSearchCapabilities, SearchCapabilityProvider,
};
use crate::core::{SearchProvider, SearchResult};
use crate::error::{BackendError, SearchError, StorageError, StorageResult};
use crate::search::{ExtractedValue, SearchParameterDefinition, SearchParameterRegistry};
use crate::tenant::TenantContext;
use crate::types::{
    IncludeType, Page, PageInfo, PaginationCapability, ResultModeCapability,
    SearchParamFullCapability, SearchParamType, SearchQuery, SortDirection, SpecialSearchParam,
    StoredResource, SummaryMode,
};
use paging::{Order, OrderKey, Position};

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

    /// The order its stream is in, which the framework passes to
    /// `paging::decode` for the request's cursor and uses to start the first
    /// page (#128, item 4: "the plan says which"). Default:
    /// `(last_updated DESC, id DESC)`; a date plan returns its table's order.
    fn order(&self) -> Order {
        Order::Default
    }

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

/// Entry validation, which `search()` and `search_count()` both run before
/// anything is read (roadmap Step 6, "Entry validation in order";
/// `coverage.md` section 6). The first refusal wins, in this order:
///
/// 1. the shared gates, so a value is refused here as on every backend:
///    `validate_date_values` (400 `InvalidDateValue`),
///    `validate_value_presence` (400 `EmptyValue`) and
///    `reject_unsupported_metadata_modifier` (400 `UnsupportedModifier`);
/// 2. `_text` and `_content` (501 `TextSearchNotAvailable`) and `_filter`
///    (501 `UnsupportedCapability`), by name and before the type check,
///    which would otherwise answer 400 for R4's string-typed `_text` and
///    `_content` (C10, C11);
/// 3. a parameter type other than token, date and URI (400
///    `UnsupportedParameterType`);
/// 4. any modifier (400 `UnsupportedModifier`);
/// 5. `_include` or `_revinclude` (501 `IncludeNotSupported`);
/// 6. `_offset`, and a `_sort` other than the search's native order (400
///    `QueryParseError`; C9).
///
/// `query.compartment` and `query.fhir_version` are residual filters and are
/// never refused (ADR-aacruzgon-0026).
pub(super) fn validate(query: &SearchQuery) -> StorageResult<()> {
    crate::search::validate_date_values(query)?;
    crate::search::validate_value_presence(query)?;
    crate::search::reject_unsupported_metadata_modifier(query)?;

    for param in &query.parameters {
        match param.name.as_str() {
            "_text" | "_content" => return Err(SearchError::TextSearchNotAvailable.into()),
            "_filter" => return Err(unsupported("_filter")),
            _ => {}
        }
    }

    if let Some(param) = query
        .parameters
        .iter()
        .find(|param| !SUPPORTED_PARAM_TYPES.contains(&param.param_type))
    {
        return Err(SearchError::UnsupportedParameterType {
            param_type: param.param_type.to_string(),
        }
        .into());
    }

    if let Some((param, modifier)) = query
        .parameters
        .iter()
        .find_map(|param| param.modifier.as_ref().map(|modifier| (param, modifier)))
    {
        return Err(SearchError::UnsupportedModifier {
            modifier: modifier.to_string(),
            param_type: param.param_type.to_string(),
        }
        .into());
    }

    if let Some(include) = query.includes.first() {
        let operation = match include.include_type {
            IncludeType::Include => "_include",
            IncludeType::Revinclude => "_revinclude",
        };
        return Err(SearchError::IncludeNotSupported {
            operation: operation.to_string(),
        }
        .into());
    }

    if query.offset.is_some() {
        return Err(SearchError::QueryParseError {
            message: "_offset is not supported: Cassandra pages by cursor only".to_string(),
        }
        .into());
    }
    if !sort_is_native(query) {
        return Err(SearchError::QueryParseError {
            message: "_sort is not supported: Cassandra returns its native order only \
                      (none, or -_lastUpdated where that is the order)"
                .to_string(),
        }
        .into());
    }
    Ok(())
}

/// Whether `query`'s `_sort` asks for the order the search returns anyway
/// (C9): no `_sort`, or exactly `-_lastUpdated` when the order is
/// `(last_updated DESC, id DESC)`. That is every search except a date-only
/// one sliced on a date table: one whose parameters are all dates, none of
/// them `_lastUpdated` (which, when present, is sliced instead; G20).
fn sort_is_native(query: &SearchQuery) -> bool {
    match query.sort.as_slice() {
        [] => true,
        [sort] => {
            let date_table = !query.parameters.is_empty()
                && query.parameters.iter().all(|param| {
                    param.param_type == SearchParamType::Date && param.name != "_lastUpdated"
                });
            sort.parameter == "_lastUpdated"
                && sort.direction == SortDirection::Descending
                && !date_table
        }
        _ => false,
    }
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
        tenant: &TenantContext,
        query: &SearchQuery,
    ) -> StorageResult<SearchResult> {
        validate(query)?;
        if !is_unfiltered(query) {
            return Err(unsupported("search"));
        }
        // `_count=0` behaves as `_summary=count` (`cassandra-discussion.md`,
        // "`_count=0` and `count`"): a total and no entries, so no page is
        // read. REST turns `_summary=count` into `_total=accurate`.
        let count_only = query.count == Some(0) || query.summary == Some(SummaryMode::Count);
        let mut result = if count_only {
            SearchResult::new(Page::new(Vec::new(), PageInfo::end()))
        } else {
            self.unfiltered_search(tenant, query).await?
        };
        // The page as usual, plus one `search_count` of the same query
        // (walkthrough 5.9); `_total=none` or absent computes none.
        if count_only || query.wants_total() {
            result.total = Some(self.search_count(tenant, query).await?);
        }
        Ok(result)
    }

    async fn search_count(
        &self,
        tenant: &TenantContext,
        query: &SearchQuery,
    ) -> StorageResult<u64> {
        validate(query)?;
        if is_unfiltered(query) {
            return match query.stored_fhir_version() {
                // `count(tenant, Some(type))`, no tag check (walkthrough 5.9, G7).
                None => {
                    self.count_resources(tenant, Some(&query.resource_type))
                        .await
                }
                Some(version) => {
                    self.count_type_version(tenant, &query.resource_type, version)
                        .await
                }
            };
        }
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
        assert_eq!(plan.order(), Order::Default);
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

/// The candidate cap until `HFS_CASSANDRA_SEARCH_MAX_CANDIDATES` is read from
/// the configuration (ADR-anastasiabryz-0002; the setting goes in Sam's
/// `config.rs`).
const DEFAULT_MAX_CANDIDATES: usize = 100_000;
/// Rows one index read asks for: a batch size, not the page size
/// (walkthrough 5.1, step 6).
const BATCH_ROWS: usize = 100;
/// R1 point reads in flight at once, for one batch of index rows.
const R1_CONCURRENCY: usize = 8;

/// Walkthrough 5.6's read of `resources_by_type`, without its keyset and
/// `LIMIT` (added per read).
const SELECT_TYPE_ROWS: &str = "SELECT last_updated, id, version_id, fhir_version \
     FROM resources_by_type WHERE tenant_id = ? AND resource_type = ?";

/// A search with no parameters, chains, `_has`, `_list` or compartment: the
/// unfiltered type search of walkthrough 5.6. A compartment (a compartment
/// search, or any search under a `patient/` scope) is a residual filter
/// (5.11, G33), so a search with one is never unfiltered: answering it from
/// `resources_by_type` alone would return the whole type.
fn is_unfiltered(query: &SearchQuery) -> bool {
    query.parameters.is_empty()
        && query.reverse_chains.is_empty()
        && query.list.is_empty()
        && query.compartment.is_none()
}

/// `search_count` with no filters and a FHIR version named (G34): the whole
/// partition's `fhir_version` column, counted in Rust.
const SELECT_TYPE_VERSIONS: &str =
    "SELECT fhir_version FROM resources_by_type WHERE tenant_id = ? AND resource_type = ?";
/// Rows per page of that scan.
const VERSION_SCAN_PAGE: i32 = 5000;

fn internal(message: String) -> StorageError {
    StorageError::Backend(BackendError::Internal {
        backend_name: BACKEND_NAME.to_string(),
        message,
        source: None,
    })
}

/// One row of `resources_by_type`.
#[derive(Debug, Clone)]
struct TypeRow {
    last_updated: DateTime<Utc>,
    id: String,
    /// The row's tag; `None` only for a row written without one.
    version_id: Option<i32>,
    /// The resource's `fhir_version`, as `resources` stores it (G34).
    fhir_version: Option<String>,
}

impl TypeRow {
    fn decode(row: &Row) -> StorageResult<Self> {
        let field = |name: &str, error: cdrs_tokio::error::Error| {
            internal(format!("search: resources_by_type.{name}: {error}"))
        };
        Ok(Self {
            last_updated: row
                .get_r_by_name("last_updated")
                .map_err(|error| field("last_updated", error))?,
            id: row
                .get_r_by_name("id")
                .map_err(|error| field("id", error))?,
            version_id: row
                .get_by_name("version_id")
                .map_err(|error| field("version_id", error))?,
            fhir_version: row
                .get_by_name("fhir_version")
                .map_err(|error| field("fhir_version", error))?,
        })
    }
}

/// The resource an R1 row holds, as a search returns it.
fn stored_resource(
    tenant: &TenantContext,
    resource_type: &str,
    id: &str,
    row: &ResourceRow,
) -> StorageResult<StoredResource> {
    let data = row
        .data
        .as_deref()
        .ok_or_else(|| internal("search: a live resources row has no data".to_string()))?;
    let content = serde_json::from_str(data)
        .map_err(|error| internal(format!("search: stored JSON: {error}")))?;
    Ok(StoredResource::from_storage(
        resource_type,
        id,
        row.version_id.to_string(),
        tenant.tenant_id().clone(),
        content,
        row.created_at.unwrap_or(row.last_updated),
        row.last_updated,
        None,
        row.fhir_version,
    ))
}

impl CassandraBackend {
    /// `GET /Patient`: one partition of `resources_by_type`, keyset-paged
    /// from the cursor, each row tag-checked against R1 (walkthrough 5.1,
    /// 5.6). One repair context for the response, run once at its end, on
    /// success and on `TooManyResults` (`repair.rs`, "Step 6's contract").
    async fn unfiltered_search(
        &self,
        tenant: &TenantContext,
        query: &SearchQuery,
    ) -> StorageResult<SearchResult> {
        let position = paging::decode(query.cursor.as_deref(), Order::Default)?;
        let budget = CandidateBudget::new(DEFAULT_MAX_CANDIDATES);
        let mut repairs = RepairContext::new(tenant.tenant_id().as_str());
        let page = self
            .type_page(tenant, query, position, &budget, &mut repairs)
            .await;
        if matches!(
            &page,
            Ok(_) | Err(StorageError::Search(SearchError::TooManyResults { .. }))
        ) {
            repairs.run(self).await;
        }
        page
    }

    /// The page itself: read batches of `resources_by_type` after
    /// `position`, keep the rows that pass the tag check, and stop at
    /// `_count + 1` kept; the extra one decides whether `next` exists.
    async fn type_page(
        &self,
        tenant: &TenantContext,
        query: &SearchQuery,
        position: Position,
        budget: &CandidateBudget,
        repairs: &mut RepairContext,
    ) -> StorageResult<SearchResult> {
        let page_size = query.count.unwrap_or(DEFAULT_PAGE_SIZE) as usize;
        if page_size == 0 {
            return Ok(SearchResult::new(Page::new(Vec::new(), PageInfo::end())));
        }
        let tenant_id = tenant.tenant_id().as_str();
        let resource_type = query.resource_type.as_str();
        let wanted_version = query.stored_fhir_version();

        let mut kept: Vec<(DateTime<Utc>, StoredResource)> = Vec::new();
        let mut after = position;
        'reading: loop {
            let rows = self.type_rows(tenant_id, resource_type, &after).await?;
            let Some(last) = rows.last() else {
                break;
            };
            let more = rows.len() == BATCH_ROWS;
            after = Position::After {
                order: Order::Default,
                at: last.last_updated,
                id: last.id.clone(),
                table: None,
            };

            // G34: another FHIR version is dropped from the row's own
            // column, before any point read.
            let candidates = rows.into_iter().filter(|row| match wanted_version {
                Some(wanted) => row
                    .fhir_version
                    .as_deref()
                    .is_none_or(|stored| stored == wanted),
                None => true,
            });
            let mut checked = stream::iter(candidates)
                .map(|row| async move {
                    budget.charge(&row.id)?;
                    let r1 = self
                        .read_resource_row(tenant_id, resource_type, &row.id)
                        .await?;
                    Ok::<_, StorageError>((row, r1))
                })
                .buffered(R1_CONCURRENCY);
            while let Some(checked_row) = checked.next().await {
                let (row, r1) = checked_row?;
                match r1 {
                    Some(r1) if !r1.is_deleted() && Some(r1.version_id) == row.version_id => {
                        if query.matches_fhir_version(r1.fhir_version) {
                            let resource = stored_resource(tenant, resource_type, &row.id, &r1)?;
                            kept.push((row.last_updated, resource));
                            if kept.len() > page_size {
                                break 'reading;
                            }
                        }
                    }
                    r1 => queue_hidden(repairs, resource_type, &row, r1),
                }
            }
            if !more {
                break;
            }
        }

        let has_next = kept.len() > page_size;
        kept.truncate(page_size);
        let next_cursor = match kept.last() {
            Some((at, resource)) if has_next => Some(paging::mint(
                &Candidate {
                    order: OrderKey {
                        at: *at,
                        table: None,
                    },
                    id: resource.id().to_string(),
                    rows: Vec::new(),
                },
                Order::Default,
            )?),
            _ => None,
        };
        let page_info = PageInfo {
            next_cursor,
            has_next,
            ..PageInfo::end()
        };
        let items = kept.into_iter().map(|(_, resource)| resource).collect();
        Ok(SearchResult::new(Page::new(items, page_info)))
    }

    /// `search_count` with no filters and `query.fhir_version` set: the rows
    /// of `(tenant, resource_type)` whose `fhir_version` is `version`, read
    /// from `resources_by_type` alone, with no point reads (walkthrough 5.9,
    /// G34). Like `count`, it has no tag check.
    async fn count_type_version(
        &self,
        tenant: &TenantContext,
        resource_type: &str,
        version: &str,
    ) -> StorageResult<u64> {
        let rows = self
            .session
            .rows_paged_keeping(
                SELECT_TYPE_VERSIONS,
                vec![
                    Value::from(tenant.tenant_id().as_str()),
                    Value::from(resource_type),
                ],
                StatementKind::Read,
                VERSION_SCAN_PAGE,
                |row| {
                    IntoRustByName::<String>::get_by_name(row, "fhir_version")
                        .ok()
                        .flatten()
                        .is_some_and(|stored| stored == version)
                },
            )
            .await
            .map_err(|error| StorageError::Backend(map_driver_error(&error, "count")))?;
        Ok(rows.len() as u64)
    }

    /// One batch of `resources_by_type` rows strictly after `after`.
    async fn type_rows(
        &self,
        tenant_id: &str,
        resource_type: &str,
        after: &Position,
    ) -> StorageResult<Vec<TypeRow>> {
        let mut values = vec![Value::from(tenant_id), Value::from(resource_type)];
        let cql = match paging::keyset(after) {
            None => format!("{SELECT_TYPE_ROWS} LIMIT ?"),
            Some((relation, bound)) => {
                values.extend(bound);
                format!("{SELECT_TYPE_ROWS} AND {relation} LIMIT ?")
            }
        };
        values.push(Value::from(BATCH_ROWS as i32));
        let rows = self
            .session
            .rows(&cql, values, StatementKind::Read)
            .await
            .map_err(|error| StorageError::Backend(map_driver_error(&error, "search")))?;
        rows.iter().map(TypeRow::decode).collect()
    }
}

/// A `resources_by_type` row the tag check hid: queued for repair with the
/// tag check's own R1, or counted as undecodable when it has no tag
/// (`repair.rs`, "Step 6's contract").
fn queue_hidden(
    repairs: &mut RepairContext,
    resource_type: &str,
    row: &TypeRow,
    r1: Option<ResourceRow>,
) {
    let Some(tag) = row.version_id else {
        repairs.skip_undecodable("resources_by_type", "an index row with no version_id tag");
        return;
    };
    match StaleIndexRow::new(IndexKey::ResourcesByType, Some(row.last_updated), tag) {
        Ok(stale) => repairs.queue(resource_type, &row.id, r1, vec![stale]),
        Err(error) => repairs.skip_undecodable("resources_by_type", &error),
    }
}

/// Entry validation (roadmap row 6.1): each documented refusal, its error,
/// and the order the refusals run in (`coverage.md` section 6).
#[cfg(test)]
mod validation_tests {
    use super::*;
    use crate::error::{BackendError, StorageError};
    use crate::types::{
        CompartmentMembership, IncludeDirective, SearchModifier, SearchParameter, SearchValue,
        SortDirective,
    };

    fn param(name: &str, param_type: SearchParamType, value: &str) -> SearchParameter {
        SearchParameter {
            name: name.to_string(),
            param_type,
            values: vec![SearchValue::parse(value)],
            ..Default::default()
        }
    }

    fn with_modifier(mut param: SearchParameter, modifier: SearchModifier) -> SearchParameter {
        param.modifier = Some(modifier);
        param
    }

    fn query(params: Vec<SearchParameter>) -> SearchQuery {
        params
            .into_iter()
            .fold(SearchQuery::new("Patient"), SearchQuery::with_parameter)
    }

    fn gender() -> SearchParameter {
        param("gender", SearchParamType::Token, "female")
    }

    fn birthdate() -> SearchParameter {
        param("birthdate", SearchParamType::Date, "ge1980-01-01")
    }

    fn refusal(query: &SearchQuery) -> StorageError {
        validate(query).expect_err("validate should refuse this query")
    }

    fn include(include_type: IncludeType) -> IncludeDirective {
        IncludeDirective {
            include_type,
            source_type: "Observation".to_string(),
            search_param: "patient".to_string(),
            target_type: None,
            iterate: false,
        }
    }

    #[test]
    fn a_served_query_passes() {
        let mut served = query(vec![
            gender(),
            birthdate(),
            param("_profile", SearchParamType::Uri, "http://example.org/p"),
            param("_id", SearchParamType::Token, "abc"),
            param("_lastUpdated", SearchParamType::Date, "gt2020-01-01"),
        ]);
        // Residual filters, never refused (ADR-aacruzgon-0026).
        served.compartment = Some(CompartmentMembership {
            params: vec!["subject".to_string()],
            reference: "Patient/123".to_string(),
        });
        validate(&served).unwrap();
        validate(&SearchQuery::new("Patient")).unwrap();
    }

    #[test]
    fn the_shared_gates_refuse_first() {
        let bad_date = param("birthdate", SearchParamType::Date, "not-a-date");
        assert!(matches!(
            refusal(&query(vec![bad_date])),
            StorageError::Search(SearchError::InvalidDateValue { .. })
        ));
        let empty = param("gender", SearchParamType::Token, "");
        assert!(matches!(
            refusal(&query(vec![empty])),
            StorageError::Search(SearchError::EmptyValue { .. })
        ));
        let last_updated_not = with_modifier(
            param("_lastUpdated", SearchParamType::Date, "gt2020-01-01"),
            SearchModifier::Not,
        );
        assert!(matches!(
            refusal(&query(vec![last_updated_not])),
            StorageError::Search(SearchError::UnsupportedModifier { .. })
        ));
    }

    #[test]
    fn text_and_content_are_501_by_name_before_the_type_check() {
        for name in ["_text", "_content"] {
            // R4 types both as string, which the type check would refuse
            // with 400 if it ran first (C10).
            let text = param(name, SearchParamType::String, "fever");
            assert!(
                matches!(
                    refusal(&query(vec![text])),
                    StorageError::Search(SearchError::TextSearchNotAvailable)
                ),
                "{name}"
            );
        }
    }

    #[test]
    fn filter_is_501_unsupported_capability_before_the_type_check() {
        let filter = param("_filter", SearchParamType::String, "name eq smith");
        assert!(matches!(
            refusal(&query(vec![filter])),
            StorageError::Backend(BackendError::UnsupportedCapability { .. })
        ));
    }

    #[test]
    fn every_other_parameter_type_is_400() {
        for (name, param_type, value) in [
            ("name", SearchParamType::String, "smith"),
            (
                "general-practitioner",
                SearchParamType::Reference,
                "Practitioner/1",
            ),
            ("value-quantity", SearchParamType::Quantity, "5.4"),
            (
                "code-value-quantity",
                SearchParamType::Composite,
                "8480-6$100",
            ),
            ("probability", SearchParamType::Number, "0.8"),
            ("near", SearchParamType::Special, "1|2|3|km"),
        ] {
            match refusal(&query(vec![gender(), param(name, param_type, value)])) {
                StorageError::Search(SearchError::UnsupportedParameterType {
                    param_type: found,
                }) => {
                    assert_eq!(found, param_type.to_string(), "{name}");
                }
                other => panic!("{name}: expected UnsupportedParameterType, got {other:?}"),
            }
        }
    }

    #[test]
    fn every_modifier_is_400() {
        for (served, modifier) in [
            (gender(), SearchModifier::Missing),
            (gender(), SearchModifier::Not),
            (gender(), SearchModifier::Text),
            (gender(), SearchModifier::OfType),
            (gender(), SearchModifier::CodeText),
            (gender(), SearchModifier::TextAdvanced),
            (birthdate(), SearchModifier::Missing),
            (
                param("_profile", SearchParamType::Uri, "http://x"),
                SearchModifier::Below,
            ),
            (
                param("_profile", SearchParamType::Uri, "http://x"),
                SearchModifier::Above,
            ),
            (
                param("_profile", SearchParamType::Uri, "http://x"),
                SearchModifier::Contains,
            ),
            (
                param("_id", SearchParamType::Token, "abc"),
                SearchModifier::Not,
            ),
            (
                param("_id", SearchParamType::Token, "abc"),
                SearchModifier::Missing,
            ),
        ] {
            let name = served.name.clone();
            match refusal(&query(vec![with_modifier(served, modifier.clone())])) {
                StorageError::Search(SearchError::UnsupportedModifier {
                    modifier: found, ..
                }) => {
                    assert_eq!(found, modifier.to_string(), "{name}");
                }
                other => panic!("{name}:{modifier}: expected UnsupportedModifier, got {other:?}"),
            }
        }
    }

    #[test]
    fn include_and_revinclude_are_501() {
        for (include_type, operation) in [
            (IncludeType::Include, "_include"),
            (IncludeType::Revinclude, "_revinclude"),
        ] {
            match refusal(&query(vec![gender()]).with_include(include(include_type))) {
                StorageError::Search(SearchError::IncludeNotSupported { operation: found }) => {
                    assert_eq!(found, operation);
                }
                other => panic!("{operation}: expected IncludeNotSupported, got {other:?}"),
            }
        }
    }

    #[test]
    fn offset_is_400() {
        let mut offset = query(vec![gender()]);
        offset.offset = Some(20);
        assert!(matches!(
            refusal(&offset),
            StorageError::Search(SearchError::QueryParseError { .. })
        ));
    }

    #[test]
    fn only_the_native_sort_is_accepted() {
        let sorted = |params: Vec<SearchParameter>, sorts: &[&str]| {
            sorts.iter().fold(query(params), |q, sort| {
                q.with_sort(SortDirective::parse(sort))
            })
        };
        // The default order is (last_updated DESC, id DESC).
        validate(&sorted(vec![gender()], &["-_lastUpdated"])).unwrap();
        validate(&sorted(vec![], &["-_lastUpdated"])).unwrap();
        // With _lastUpdated present, a date-only search slices it (G20).
        validate(&sorted(
            vec![
                birthdate(),
                param("_lastUpdated", SearchParamType::Date, "gt2020-01-01"),
            ],
            &["-_lastUpdated"],
        ))
        .unwrap();

        for (case, refused) in [
            ("_sort=birthdate", sorted(vec![gender()], &["birthdate"])),
            (
                "_sort=_lastUpdated",
                sorted(vec![gender()], &["_lastUpdated"]),
            ),
            (
                "two sorts",
                sorted(vec![gender()], &["-_lastUpdated", "_id"]),
            ),
            // A date-only search on a date table is in its table's order.
            ("date-only", sorted(vec![birthdate()], &["-_lastUpdated"])),
        ] {
            assert!(
                matches!(
                    refusal(&refused),
                    StorageError::Search(SearchError::QueryParseError { .. })
                ),
                "{case}"
            );
        }
    }

    #[test]
    fn the_refusals_run_in_the_documented_order() {
        let string = param("name", SearchParamType::String, "smith");
        let bad_date = param("birthdate", SearchParamType::Date, "not-a-date");
        let text = param("_text", SearchParamType::String, "fever");
        let modified = with_modifier(gender(), SearchModifier::Missing);

        // A date value, before a type.
        assert!(matches!(
            refusal(&query(vec![string.clone(), bad_date])),
            StorageError::Search(SearchError::InvalidDateValue { .. })
        ));
        // _text by name, before a type that comes earlier in the query.
        assert!(matches!(
            refusal(&query(vec![string.clone(), text])),
            StorageError::Search(SearchError::TextSearchNotAvailable)
        ));
        // A type, before a modifier that comes earlier in the query.
        assert!(matches!(
            refusal(&query(vec![modified.clone(), string])),
            StorageError::Search(SearchError::UnsupportedParameterType { .. })
        ));
        // A modifier, before an include.
        let modified_with_include =
            query(vec![modified]).with_include(include(IncludeType::Include));
        assert!(matches!(
            refusal(&modified_with_include),
            StorageError::Search(SearchError::UnsupportedModifier { .. })
        ));
        // An include, before _offset and _sort.
        let mut include_and_offset = query(vec![gender()])
            .with_include(include(IncludeType::Revinclude))
            .with_sort(SortDirective::parse("birthdate"));
        include_and_offset.offset = Some(5);
        assert!(matches!(
            refusal(&include_and_offset),
            StorageError::Search(SearchError::IncludeNotSupported { .. })
        ));
    }
}

/// `search()` and `search_count()` on a backend connected to the node at
/// `HFS_TEST_CASSANDRA_URL` (skipped when unset).
#[cfg(test)]
mod node_tests {
    use super::*;
    use crate::backends::cassandra::test_support::test_config;
    use crate::error::{BackendError, StorageError};
    use crate::tenant::{TenantId, TenantPermissions};

    async fn backend() -> Option<CassandraBackend> {
        let url = std::env::var("HFS_TEST_CASSANDRA_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())?;
        Some(
            CassandraBackend::connect(crate::backends::cassandra::CassandraConfig {
                contact_points: vec![url],
                ..test_config()
            })
            .await
            .expect("connect"),
        )
    }

    /// Both validate first, so a refused query gets its own error, and a
    /// query the engine does not serve yet still gets 501.
    #[tokio::test]
    async fn search_and_search_count_validate_before_anything_else() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = TenantContext::new(
            TenantId::new(format!("test-validation-{}", uuid::Uuid::new_v4().simple())),
            TenantPermissions::full_access(),
        );
        let mut refused = SearchQuery::new("Patient");
        refused.offset = Some(20);
        // A token parameter: served by 6a, so still 501 until it lands.
        let served = SearchQuery::new("Patient").with_parameter(crate::types::SearchParameter {
            name: "gender".to_string(),
            param_type: SearchParamType::Token,
            values: vec![crate::types::SearchValue::eq("female")],
            ..Default::default()
        });

        assert!(matches!(
            backend.search(&tenant, &refused).await,
            Err(StorageError::Search(SearchError::QueryParseError { .. }))
        ));
        assert!(matches!(
            backend.search_count(&tenant, &refused).await,
            Err(StorageError::Search(SearchError::QueryParseError { .. }))
        ));
        assert!(matches!(
            backend.search(&tenant, &served).await,
            Err(StorageError::Backend(
                BackendError::UnsupportedCapability { .. }
            ))
        ));
        assert!(matches!(
            backend.search_count(&tenant, &served).await,
            Err(StorageError::Backend(
                BackendError::UnsupportedCapability { .. }
            ))
        ));
    }

    fn fresh_tenant(label: &str) -> TenantContext {
        TenantContext::new(
            TenantId::new(format!("{label}-{}", uuid::Uuid::new_v4().simple())),
            TenantPermissions::full_access(),
        )
    }

    async fn create_patients(
        backend: &CassandraBackend,
        tenant: &TenantContext,
        n: usize,
    ) -> Vec<String> {
        let mut ids = Vec::new();
        for _ in 0..n {
            let stored = crate::core::ResourceStorage::create(
                backend,
                tenant,
                "Patient",
                serde_json::json!({ "resourceType": "Patient" }),
                crate::backends::cassandra::test_support::test_fhir_version(),
            )
            .await
            .expect("create");
            ids.push(stored.id().to_string());
        }
        ids
    }

    /// Every page of `query`, following `next` to the end.
    async fn every_page(
        backend: &CassandraBackend,
        tenant: &TenantContext,
        mut query: SearchQuery,
    ) -> Vec<SearchResult> {
        let mut pages = Vec::new();
        loop {
            let page = backend.search(tenant, &query).await.expect("search");
            let next = page.resources.page_info.next_cursor.clone();
            pages.push(page);
            match next {
                Some(cursor) => query.cursor = Some(cursor),
                None => break,
            }
        }
        pages
    }

    /// `GET /Patient?_count=10` over 25 Patients, 3 of them deleted: 10, 10
    /// and 2, newest first, each live one once; `next` on every page but
    /// the last; no `previous` (walkthrough 5.6, 5.10).
    #[tokio::test]
    async fn an_unfiltered_search_pages_every_live_resource_once() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = fresh_tenant("test-search-unfiltered");
        let ids = create_patients(&backend, &tenant, 25).await;
        for id in &ids[..3] {
            crate::core::ResourceStorage::delete(&backend, &tenant, "Patient", id)
                .await
                .expect("delete");
        }
        let live: std::collections::BTreeSet<String> = ids[3..].iter().cloned().collect();

        let pages = every_page(
            &backend,
            &tenant,
            SearchQuery::new("Patient").with_count(10),
        )
        .await;
        let sizes: Vec<usize> = pages.iter().map(|page| page.resources.len()).collect();
        assert_eq!(sizes, [10, 10, 2]);
        for (n, page) in pages.iter().enumerate() {
            let info = &page.resources.page_info;
            assert_eq!(info.has_next, n < 2, "page {n}: has_next");
            assert_eq!(info.next_cursor.is_some(), n < 2, "page {n}: next");
            assert!(info.previous_cursor.is_none(), "page {n}: no previous");
        }
        let found: Vec<&StoredResource> = pages
            .iter()
            .flat_map(|page| &page.resources.items)
            .collect();
        let distinct: std::collections::BTreeSet<String> = found
            .iter()
            .map(|resource| resource.id().to_string())
            .collect();
        assert_eq!(found.len(), distinct.len(), "no resource on two pages");
        assert_eq!(distinct, live, "every live Patient, no deleted one");
        assert!(
            found
                .windows(2)
                .all(|pair| (pair[0].last_modified(), pair[0].id())
                    >= (pair[1].last_modified(), pair[1].id())),
            "newest first: (last_updated DESC, id DESC)"
        );
    }

    /// An index row with no `resources` row is a candidate the tag check
    /// hides: it is never returned. Planted, so the tenant is
    /// `test-planted-` (#127).
    #[tokio::test]
    async fn an_orphan_index_row_is_hidden() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = fresh_tenant("test-planted-search-orphan");
        let ids = create_patients(&backend, &tenant, 3).await;
        backend
            .session
            .execute(
                "INSERT INTO resources_by_type \
                 (tenant_id, resource_type, last_updated, id, version_id, fhir_version) \
                 VALUES (?, ?, ?, ?, ?, ?)",
                vec![
                    Value::from(tenant.tenant_id().as_str()),
                    Value::from("Patient"),
                    Value::from(Utc::now() + chrono::Duration::hours(1)),
                    Value::from("orphan"),
                    Value::from(1_i32),
                    Value::from("4.0"),
                ],
                StatementKind::Write,
            )
            .await
            .expect("plant");

        let page = backend
            .search(&tenant, &SearchQuery::new("Patient"))
            .await
            .expect("search");
        let found: std::collections::BTreeSet<String> = page
            .resources
            .items
            .iter()
            .map(|resource| resource.id().to_string())
            .collect();
        assert_eq!(found, ids.into_iter().collect());
        assert!(page.resources.page_info.next_cursor.is_none());
    }

    /// A cursor `mint` did not make is 400 `InvalidCursor` (`coverage.md`
    /// section 5).
    #[tokio::test]
    async fn a_malformed_cursor_is_invalid() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let mut query = SearchQuery::new("Patient");
        query.cursor = Some("garbage".to_string());
        assert!(matches!(
            backend
                .search(&fresh_tenant("test-search-cursor"), &query)
                .await,
            Err(StorageError::Search(SearchError::InvalidCursor { .. }))
        ));
    }

    /// Plants a `resources_by_type` row with no `resources` row behind it.
    async fn plant_type_row(
        backend: &CassandraBackend,
        tenant: &TenantContext,
        id: &str,
        fhir_version: &str,
    ) {
        backend
            .session
            .execute(
                "INSERT INTO resources_by_type \
                 (tenant_id, resource_type, last_updated, id, version_id, fhir_version) \
                 VALUES (?, ?, ?, ?, ?, ?)",
                vec![
                    Value::from(tenant.tenant_id().as_str()),
                    Value::from("Patient"),
                    Value::from(Utc::now()),
                    Value::from(id),
                    Value::from(1_i32),
                    Value::from(fhir_version),
                ],
                StatementKind::Write,
            )
            .await
            .expect("plant");
    }

    /// `search_count` with no filters is `count(tenant, Some(type))`: the
    /// live rows of `resources_by_type`, with no tag check, so a planted
    /// orphan row counts too (walkthrough 5.9, G7). With a FHIR version
    /// named, only that version's rows count (G34). Planted, so the tenant
    /// is `test-planted-` (#127).
    #[tokio::test]
    async fn an_unfiltered_count_counts_the_type_and_its_version() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = fresh_tenant("test-planted-search-count");
        let ids = create_patients(&backend, &tenant, 5).await;
        crate::core::ResourceStorage::delete(&backend, &tenant, "Patient", &ids[0])
            .await
            .expect("delete");
        // Two rows of a version no resource here has.
        plant_type_row(&backend, &tenant, "other-1", "0.0-planted").await;
        plant_type_row(&backend, &tenant, "other-2", "0.0-planted").await;

        let all = SearchQuery::new("Patient");
        assert_eq!(
            backend.search_count(&tenant, &all).await.unwrap(),
            6,
            "4 live + 2 planted"
        );

        let version = crate::backends::cassandra::test_support::test_fhir_version();
        let one_version = SearchQuery::new("Patient").with_fhir_version(version);
        assert_eq!(
            backend.search_count(&tenant, &one_version).await.unwrap(),
            4,
            "only the live rows of {version:?}"
        );
        assert_eq!(
            backend
                .search_count(&fresh_tenant("test-search-count-empty"), &all)
                .await
                .unwrap(),
            0
        );
    }

    /// A compartment is a residual filter (5.11), so a search or count
    /// with one is never answered from `resources_by_type` alone: until the
    /// compartment residual lands, it is refused rather than returning the
    /// whole type.
    #[tokio::test]
    async fn a_compartment_is_never_read_as_unfiltered() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = fresh_tenant("test-search-compartment");
        create_patients(&backend, &tenant, 2).await;
        let mut query = SearchQuery::new("Patient");
        query.compartment = Some(crate::types::CompartmentMembership {
            params: vec!["link".to_string()],
            reference: "Patient/someone-else".to_string(),
        });
        assert!(matches!(
            backend.search(&tenant, &query).await,
            Err(StorageError::Backend(
                BackendError::UnsupportedCapability { .. }
            ))
        ));
        assert!(matches!(
            backend.search_count(&tenant, &query).await,
            Err(StorageError::Backend(
                BackendError::UnsupportedCapability { .. }
            ))
        ));
    }

    /// `_total=accurate` (and `estimate`) adds the count to the page;
    /// `_total=none` or absent adds none; `_count=0` and `_summary=count`
    /// give the total and no entries (walkthrough 5.9).
    #[tokio::test]
    async fn totals_and_count_zero_carry_the_count() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = fresh_tenant("test-search-total");
        create_patients(&backend, &tenant, 7).await;
        let page_of = |total: Option<crate::types::TotalMode>| {
            let mut query = SearchQuery::new("Patient").with_count(3);
            query.total = total;
            query
        };

        for total in [
            crate::types::TotalMode::Accurate,
            crate::types::TotalMode::Estimate,
        ] {
            let result = backend
                .search(&tenant, &page_of(Some(total)))
                .await
                .unwrap();
            assert_eq!(result.resources.len(), 3, "{total:?}: the page as usual");
            assert_eq!(result.total, Some(7), "{total:?}");
        }
        for total in [None, Some(crate::types::TotalMode::None)] {
            let result = backend.search(&tenant, &page_of(total)).await.unwrap();
            assert_eq!(result.resources.len(), 3);
            assert_eq!(result.total, None, "{total:?}: no total computed");
        }

        let count_zero = SearchQuery::new("Patient").with_count(0);
        let mut summary_count = SearchQuery::new("Patient");
        summary_count.summary = Some(SummaryMode::Count);
        summary_count.total = Some(crate::types::TotalMode::Accurate);
        for (case, query) in [("_count=0", count_zero), ("_summary=count", summary_count)] {
            let result = backend.search(&tenant, &query).await.unwrap();
            assert!(result.resources.is_empty(), "{case}: no entries");
            assert!(
                result.resources.page_info.next_cursor.is_none(),
                "{case}: no next"
            );
            assert_eq!(result.total, Some(7), "{case}");
        }
    }
}
