//! The search loop over a module's [`ParamPlan`] (roadmap row 6.3;
//! walkthrough 5.1; ADR-anastasiabryz-0001).
//!
//! The driving plan streams its candidates in its own order, already merged
//! and collapsed on the order key. For each one the framework:
//!
//! 1. charges the candidate cap and point-reads R1 (5.1, steps 4 and 7);
//! 2. runs the tag check: kept only if R1 exists, is live, and one of the
//!    candidate's index rows carries R1's `version_id`. Every row tagged
//!    otherwise is stale and is queued for repair with this R1 (`repair.rs`,
//!    "Step 6's contract"), so a hidden candidate and a kept one with a
//!    leftover row are both repaired;
//! 3. applies the residuals: the FHIR version (G34), `_lastUpdated` on
//!    `resources.last_updated` (5.8), the compartment (5.11) and the plan's
//!    own `residual`, from one extraction that is made only when a residual
//!    reads values (a compartment, or a date parameter);
//! 4. stops at `_count + 1` kept, and mints `next` in the plan's order.
//!
//! A candidate repeated with the same order key, or a resource already kept
//! on this page, is kept once. The planner (`planner.rs`) picks the driver;
//! the other plans of an AND come next (6.3f).
//!
//! This is the framework's own file (ADR-anastasiabryz-0001; #128).

use std::collections::HashSet;

use futures::StreamExt;

use super::last_updated::LastUpdatedFilter;
use super::paging::{self, OrderKey};
use super::planner::Checks;
use super::{
    CandidateBudget, DEFAULT_PAGE_SIZE, ParamPlan, R1_CONCURRENCY, ReadCtx, finish_page_in,
    max_candidates, stored_resource,
};
use crate::backends::cassandra::backend::CassandraBackend;
use crate::backends::cassandra::index_keys::IndexKey;
use crate::backends::cassandra::repair::{RepairContext, StaleIndexRow};
use crate::core::SearchResult;
use crate::error::{SearchError, StorageError, StorageResult};
use crate::tenant::TenantContext;
use crate::types::{Page, PageInfo, SearchQuery, StoredResource};

/// What a plan scan found: the resources it kept, in the plan's order, and
/// how many candidates passed every check.
#[derive(Debug, Default)]
pub(super) struct PlanScan {
    pub(super) kept: Vec<(OrderKey, StoredResource)>,
    pub(super) matched: usize,
}

impl CassandraBackend {
    /// One page of `driver`'s matches: the cursor decoded in the plan's
    /// order, the scan, and the repairs it queued, run once at the end, on
    /// success and on `TooManyResults` (`repair.rs`, "Step 6's contract").
    pub(super) async fn plan_search(
        &self,
        tenant: &TenantContext,
        query: &SearchQuery,
        driver: &dyn ParamPlan,
        checks: &Checks<'_>,
    ) -> StorageResult<SearchResult> {
        let order = driver.order();
        let position = paging::decode(query.cursor.as_deref(), order)?;
        let page_size = query.count.unwrap_or(DEFAULT_PAGE_SIZE) as usize;
        if page_size == 0 {
            return Ok(SearchResult::new(Page::new(Vec::new(), PageInfo::end())));
        }
        let filter = LastUpdatedFilter::of(query)?;
        let budget = CandidateBudget::new(max_candidates());
        let mut repairs = RepairContext::new(tenant.tenant_id().as_str());
        let scan = self
            .plan_scan(
                tenant,
                query,
                driver,
                checks,
                &filter,
                &position,
                &budget,
                &mut repairs,
                Some(page_size + 1),
            )
            .await;
        if matches!(
            &scan,
            Ok(_) | Err(StorageError::Search(SearchError::TooManyResults { .. }))
        ) {
            repairs.run(self).await;
        }
        finish_page_in(scan?.kept, page_size, order)
    }

    /// Every match of `driver`, counted: the stream run to the end, with
    /// tag checks, under the cap; a filtered `search_count` queues no
    /// repairs (walkthrough 5.9).
    pub(super) async fn plan_count(
        &self,
        tenant: &TenantContext,
        query: &SearchQuery,
        driver: &dyn ParamPlan,
        checks: &Checks<'_>,
    ) -> StorageResult<u64> {
        let filter = LastUpdatedFilter::of(query)?;
        let budget = CandidateBudget::new(max_candidates());
        let mut unrun = RepairContext::new(tenant.tenant_id().as_str());
        let first = paging::Position::First(driver.order());
        let scan = self
            .plan_scan(
                tenant, query, driver, checks, &filter, &first, &budget, &mut unrun, None,
            )
            .await?;
        Ok(scan.matched as u64)
    }

    /// The loop of the module docs. With `keep` set, returns the first
    /// `keep` kept resources; with `None`, reads to the end and only counts.
    #[allow(clippy::too_many_arguments)]
    async fn plan_scan(
        &self,
        tenant: &TenantContext,
        query: &SearchQuery,
        driver: &dyn ParamPlan,
        checks: &Checks<'_>,
        filter: &LastUpdatedFilter,
        position: &paging::Position,
        budget: &CandidateBudget,
        repairs: &mut RepairContext,
        keep: Option<usize>,
    ) -> StorageResult<PlanScan> {
        let tenant_id = tenant.tenant_id().as_str();
        let resource_type = query.resource_type.as_str();
        let ctx = ReadCtx {
            tenant,
            resource_type,
            session: &self.session,
            budget,
        };

        let mut scan = PlanScan::default();
        let mut previous: Option<(OrderKey, String)> = None;
        let mut kept_ids: HashSet<String> = HashSet::new();
        let mut checked = driver
            .stream(&ctx, position)
            .map(|candidate| async move {
                let candidate = candidate?;
                budget.charge(&candidate.id)?;
                let r1 = self
                    .read_resource_row(tenant_id, resource_type, &candidate.id)
                    .await?;
                Ok::<_, StorageError>((candidate, r1))
            })
            .buffered(R1_CONCURRENCY);

        while let Some(next) = checked.next().await {
            let (candidate, r1) = next?;
            let key = (candidate.order.clone(), candidate.id.clone());
            if previous.as_ref() == Some(&key) || kept_ids.contains(&candidate.id) {
                continue;
            }
            previous = Some(key);

            // The tag check (5.1, step 4).
            let live_version = r1
                .as_ref()
                .filter(|r1| !r1.is_deleted())
                .map(|r1| r1.version_id);
            let stale: Vec<StaleIndexRow> = candidate
                .rows
                .iter()
                .filter(|row| Some(row.version_id) != live_version)
                .filter_map(|row| {
                    StaleIndexRow::new(row.key.clone(), row.last_updated, row.version_id)
                        .map_err(|error| repairs.skip_undecodable(table_of(&row.key), &error))
                        .ok()
                })
                .collect();
            let tagged = candidate
                .rows
                .iter()
                .any(|row| Some(row.version_id) == live_version);
            if !stale.is_empty() {
                repairs.queue(resource_type, &candidate.id, r1.clone(), stale);
            }
            let Some(r1) = r1.filter(|_| tagged) else {
                continue;
            };

            // The residuals (5.1, step 5).
            if !query.matches_fhir_version(r1.fhir_version) || !filter.matches(r1.last_updated) {
                continue;
            }
            let resource = stored_resource(tenant, resource_type, &candidate.id, &r1)?;
            let loaded = self.loaded(&ctx, query, r1, &resource);
            if !driver.residual(&loaded)
                || !self
                    .passes(&ctx, query, checks, &loaded, &candidate.id)
                    .await?
            {
                continue;
            }

            scan.matched += 1;
            if let Some(keep) = keep {
                kept_ids.insert(candidate.id.clone());
                scan.kept.push((candidate.order, resource));
                if scan.kept.len() >= keep {
                    break;
                }
            }
        }
        Ok(scan)
    }
}

/// The table an index key's row is in, for a log line.
fn table_of(key: &IndexKey) -> &'static str {
    match key {
        IndexKey::ResourcesByType => "resources_by_type",
        IndexKey::TokenCode { .. } => "token_code_index",
        IndexKey::TokenSystemCode { .. } => "token_system_code_index",
        IndexKey::TokenSystem { .. } => "token_system_index",
        IndexKey::Date { .. } => "date_index",
        IndexKey::DateByEnd { .. } => "date_index_by_end",
        IndexKey::Uri { .. } => "uri_index",
    }
}

/// The loop against the node at `HFS_TEST_CASSANDRA_URL` (skipped when
/// unset), driven by two fake plans until the modules land: one that streams
/// `resources_by_type` itself, in the default order, and one that streams a
/// fixed list in a date order.
#[cfg(test)]
mod node_tests {
    use super::*;
    use crate::backends::cassandra::search::paging::{DateTable, Order, Position};
    use crate::backends::cassandra::search::{Candidate, FoundRow};
    use crate::backends::cassandra::session::StatementKind;
    use crate::backends::cassandra::test_support::{test_config, test_fhir_version};
    use crate::tenant::{TenantId, TenantPermissions};
    use crate::types::{CursorValue, PageCursor};
    use async_trait::async_trait;
    use cdrs_tokio::types::IntoRustByName;
    use cdrs_tokio::types::value::Value;
    use chrono::{DateTime, Utc};
    use futures::stream::{self, BoxStream};

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

    fn fresh_tenant(label: &str) -> TenantContext {
        TenantContext::new(
            TenantId::new(format!("{label}-{}", uuid::Uuid::new_v4().simple())),
            TenantPermissions::full_access(),
        )
    }

    /// Patients created one by one, with their stored `last_updated`.
    async fn create_patients(
        backend: &CassandraBackend,
        tenant: &TenantContext,
        n: usize,
    ) -> Vec<(DateTime<Utc>, String)> {
        let mut made = Vec::new();
        for _ in 0..n {
            let stored = crate::core::ResourceStorage::create(
                backend,
                tenant,
                "Patient",
                serde_json::json!({ "resourceType": "Patient" }),
                test_fhir_version(),
            )
            .await
            .expect("create");
            made.push((stored.last_modified(), stored.id().to_string()));
        }
        made
    }

    /// A plan streaming `resources_by_type` after `from`, newest first, one
    /// candidate per row, as a token or URI plan streams its own table.
    struct TypeRowsPlan;

    #[async_trait]
    impl ParamPlan for TypeRowsPlan {
        fn param(&self) -> &str {
            "fake-type-rows"
        }
        fn drive(&self) -> super::super::Drive {
            super::super::Drive::Shared
        }
        fn stream<'a>(
            &'a self,
            ctx: &'a ReadCtx<'a>,
            from: &'a Position,
        ) -> BoxStream<'a, StorageResult<super::super::Candidate>> {
            let read = async move {
                let mut values = vec![
                    Value::from(ctx.tenant.tenant_id().as_str()),
                    Value::from(ctx.resource_type),
                ];
                let mut cql = "SELECT last_updated, id, version_id FROM resources_by_type \
                               WHERE tenant_id = ? AND resource_type = ?"
                    .to_string();
                if let Some((relation, bound)) = paging::keyset(from) {
                    cql.push_str(" AND ");
                    cql.push_str(relation);
                    values.extend(bound);
                }
                let rows = ctx
                    .session
                    .rows(&cql, values, StatementKind::Read)
                    .await
                    .expect("fake plan read");
                rows.iter()
                    .map(|row| {
                        let at: DateTime<Utc> = row.get_r_by_name("last_updated").unwrap();
                        let id: String = row.get_r_by_name("id").unwrap();
                        let version_id: i32 = row.get_r_by_name("version_id").unwrap();
                        Ok(Candidate {
                            order: OrderKey { at, table: None },
                            id,
                            rows: vec![FoundRow {
                                key: IndexKey::ResourcesByType,
                                last_updated: Some(at),
                                version_id,
                            }],
                        })
                    })
                    .collect::<Vec<_>>()
            };
            stream::once(read).flat_map(stream::iter).boxed()
        }
        async fn contains(
            &self,
            _ctx: &ReadCtx<'_>,
            _last_updated: DateTime<Utc>,
            _id: &str,
        ) -> StorageResult<bool> {
            unreachable!("the fake plan only drives")
        }
    }

    /// A plan streaming a fixed list in `date_index`'s order, strictly after
    /// `from`, as a date plan streams its slice.
    struct ListPlan(Vec<Candidate>);

    #[async_trait]
    impl ParamPlan for ListPlan {
        fn param(&self) -> &str {
            "fake-list"
        }
        fn drive(&self) -> super::super::Drive {
            super::super::Drive::DateOnly
        }
        fn order(&self) -> Order {
            Order::DateStart
        }
        fn stream<'a>(
            &'a self,
            _ctx: &'a ReadCtx<'a>,
            from: &'a Position,
        ) -> BoxStream<'a, StorageResult<Candidate>> {
            let after = |candidate: &&Candidate| match from {
                Position::First(_) => true,
                Position::After { at, id, .. } => {
                    (candidate.order.at, candidate.id.as_str()) > (*at, id.as_str())
                }
            };
            stream::iter(
                self.0
                    .iter()
                    .filter(after)
                    .cloned()
                    .map(Ok)
                    .collect::<Vec<_>>(),
            )
            .boxed()
        }
        async fn contains(
            &self,
            _ctx: &ReadCtx<'_>,
            _last_updated: DateTime<Utc>,
            _id: &str,
        ) -> StorageResult<bool> {
            unreachable!("the fake plan only drives")
        }
    }

    fn date_candidate(at: DateTime<Utc>, id: &str, version_id: i32) -> Candidate {
        Candidate {
            order: OrderKey {
                at,
                table: Some(DateTable::Start),
            },
            id: id.to_string(),
            rows: vec![FoundRow {
                key: IndexKey::Date {
                    param: "birthdate".to_string(),
                    start: at.timestamp_millis(),
                    end: at.timestamp_millis() + 86_400_000,
                },
                last_updated: None,
                version_id,
            }],
        }
    }

    /// Every page of `driver`'s search, following `next`.
    async fn every_page(
        backend: &CassandraBackend,
        tenant: &TenantContext,
        driver: &dyn ParamPlan,
        count: u32,
    ) -> Vec<SearchResult> {
        let mut query = SearchQuery::new("Patient").with_count(count);
        let mut pages = Vec::new();
        loop {
            let page = backend
                .plan_search(tenant, &query, driver, &Checks::none())
                .await
                .expect("plan search");
            let next = page.resources.page_info.next_cursor.clone();
            pages.push(page);
            match next {
                Some(cursor) => query.cursor = Some(cursor),
                None => break,
            }
        }
        pages
    }

    fn ids(pages: &[SearchResult]) -> Vec<String> {
        pages
            .iter()
            .flat_map(|page| page.resources.items.iter().map(|r| r.id().to_string()))
            .collect()
    }

    /// A default-order plan: every live resource once, newest first, in
    /// pages of 3 through the cursor; counted to the end.
    #[tokio::test]
    async fn a_plan_pages_its_candidates_through_the_tag_check() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = fresh_tenant("test-engine-pages");
        let mut made = create_patients(&backend, &tenant, 7).await;
        made.sort_by(|a, b| b.cmp(a));
        let expected: Vec<String> = made.into_iter().map(|(_, id)| id).collect();

        let pages = every_page(&backend, &tenant, &TypeRowsPlan, 3).await;
        let sizes: Vec<usize> = pages.iter().map(|page| page.resources.len()).collect();
        assert_eq!(sizes, [3, 3, 1]);
        assert_eq!(ids(&pages), expected);
        assert_eq!(
            backend
                .plan_count(
                    &tenant,
                    &SearchQuery::new("Patient"),
                    &TypeRowsPlan,
                    &Checks::none()
                )
                .await
                .unwrap(),
            7
        );
    }

    /// A date-order plan: pages in its own ascending order; `next` names the
    /// table, as the design's cursor does; a candidate whose tag is not R1's
    /// is hidden and queued for repair; a repeated candidate is kept once.
    #[tokio::test]
    async fn a_date_order_plan_pages_in_its_order_and_hides_a_stale_row() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = fresh_tenant("test-engine-date-order");
        let made = create_patients(&backend, &tenant, 4).await;
        let day = |n: i64| {
            DateTime::<Utc>::from_timestamp(0, 0).unwrap() + chrono::Duration::days(10_000 + n)
        };
        let (a, b, c, d) = (&made[0].1, &made[1].1, &made[2].1, &made[3].1);
        let plan = ListPlan(vec![
            date_candidate(day(1), c, 1),
            date_candidate(day(1), c, 1), // a repeat
            date_candidate(day(2), a, 1),
            date_candidate(day(3), d, 99), // stale: R1 is at version 1
            date_candidate(day(4), b, 1),
        ]);
        let before = backend.repair_stats();

        let pages = every_page(&backend, &tenant, &plan, 2).await;
        assert_eq!(ids(&pages), [c.clone(), a.clone(), b.clone()]);
        assert_eq!(pages.len(), 2);

        let first_next = pages[0].resources.page_info.next_cursor.as_deref().unwrap();
        let cursor = PageCursor::decode(first_next).unwrap();
        assert!(matches!(
            cursor.sort_values(),
            [_, CursorValue::Null, CursorValue::String(table)] if table == "start"
        ));
        assert_eq!(cursor.resource_id(), a);

        let ran = backend.repair_stats().since(&before);
        // Page 1 reads past its end to decide `next`, so both pages read the
        // stale row, and each response runs its own repair phase.
        assert_eq!(
            ran.queued, 2,
            "the stale row was queued by each page that read it: {ran:?}"
        );
    }

    /// A non-driving token or URI plan: `contains` is true for these ids,
    /// and only when asked at the resource's own `last_updated`, as the
    /// clustering point read is (walkthrough 5.8).
    struct MemberAt(std::collections::HashMap<String, DateTime<Utc>>);

    #[async_trait]
    impl ParamPlan for MemberAt {
        fn param(&self) -> &str {
            "fake-member"
        }
        fn drive(&self) -> super::super::Drive {
            super::super::Drive::Shared
        }
        fn stream<'a>(
            &'a self,
            _ctx: &'a ReadCtx<'a>,
            _from: &'a Position,
        ) -> BoxStream<'a, StorageResult<Candidate>> {
            unreachable!("the fake check never drives")
        }
        async fn contains(
            &self,
            _ctx: &ReadCtx<'_>,
            last_updated: DateTime<Utc>,
            id: &str,
        ) -> StorageResult<bool> {
            Ok(self.0.get(id) == Some(&last_updated))
        }
    }

    /// A non-driving date plan: its `residual` keeps a female Patient.
    struct Female;

    #[async_trait]
    impl ParamPlan for Female {
        fn param(&self) -> &str {
            "fake-female"
        }
        fn drive(&self) -> super::super::Drive {
            super::super::Drive::DateOnly
        }
        fn stream<'a>(
            &'a self,
            _ctx: &'a ReadCtx<'a>,
            _from: &'a Position,
        ) -> BoxStream<'a, StorageResult<Candidate>> {
            unreachable!("the fake residual never drives")
        }
        async fn contains(
            &self,
            _ctx: &ReadCtx<'_>,
            _last_updated: DateTime<Utc>,
            _id: &str,
        ) -> StorageResult<bool> {
            unreachable!("a date beside another parameter is a residual")
        }
        fn residual(&self, loaded: &super::super::LoadedResource) -> bool {
            loaded
                .row
                .data
                .as_deref()
                .is_some_and(|data| data.contains("\"female\""))
        }
    }

    /// 6 Patients, alternately female and male, newest last; with their
    /// stored `last_updated`.
    async fn create_genders(
        backend: &CassandraBackend,
        tenant: &TenantContext,
    ) -> Vec<(DateTime<Utc>, String, bool)> {
        let mut made = Vec::new();
        for n in 0..6 {
            let female = n % 2 == 0;
            let stored = crate::core::ResourceStorage::create(
                backend,
                tenant,
                "Patient",
                serde_json::json!({
                    "resourceType": "Patient",
                    "gender": if female { "female" } else { "male" },
                }),
                test_fhir_version(),
            )
            .await
            .expect("create");
            made.push((stored.last_modified(), stored.id().to_string(), female));
        }
        made
    }

    /// The ids of `made` that `keep` accepts, newest first.
    fn newest_first(
        made: &[(DateTime<Utc>, String, bool)],
        keep: impl Fn(&(DateTime<Utc>, String, bool)) -> bool,
    ) -> Vec<String> {
        let mut kept: Vec<_> = made.iter().filter(|m| keep(m)).collect();
        kept.sort_by(|a, b| (b.0, &b.1).cmp(&(a.0, &a.1)));
        kept.into_iter().map(|(_, id, _)| id.clone()).collect()
    }

    fn page_ids(page: &SearchResult) -> Vec<String> {
        page.resources
            .items
            .iter()
            .map(|r| r.id().to_string())
            .collect()
    }

    /// AND, whichever path drives: a module plan, `_id`, or the type scan.
    /// Each candidate that passes the tag check is checked by every other
    /// token or URI plan's `contains`, at `resources.last_updated`, and by
    /// every date plan's `residual` (walkthrough 5.8).
    #[tokio::test]
    async fn and_checks_every_candidate_against_the_other_parameters() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = fresh_tenant("test-engine-and");
        let made = create_genders(&backend, &tenant).await;
        // Members: the first four created, at their real `last_updated`.
        let members: std::collections::HashMap<String, DateTime<Utc>> = made[..4]
            .iter()
            .map(|(at, id, _)| (id.clone(), *at))
            .collect();
        let member_ids: std::collections::HashSet<String> = members.keys().cloned().collect();
        let member = MemberAt(members);
        let both = Checks {
            contains: vec![&member],
            residuals: vec![&Female],
        };
        let expected = newest_first(&made, |(_, id, female)| *female && member_ids.contains(id));
        assert_eq!(expected.len(), 2);
        let query = SearchQuery::new("Patient").with_count(10);

        // A module plan drives.
        let page = backend
            .plan_search(&tenant, &query, &TypeRowsPlan, &both)
            .await
            .unwrap();
        assert_eq!(page_ids(&page), expected, "a module plan drives");
        assert_eq!(
            backend
                .plan_count(&tenant, &query, &TypeRowsPlan, &both)
                .await
                .unwrap(),
            2
        );

        // `_id` drives.
        let ids: Vec<String> = made.iter().map(|(_, id, _)| id.clone()).collect();
        let page = backend
            .id_search(&tenant, &query, &ids, &both)
            .await
            .unwrap();
        assert_eq!(page_ids(&page), expected, "_id drives");
        assert_eq!(
            backend
                .id_count(&tenant, &query, &ids, &both)
                .await
                .unwrap(),
            2
        );

        // The type scan drives (a date-only search with `_lastUpdated`).
        let page = backend.type_search(&tenant, &query, &both).await.unwrap();
        assert_eq!(page_ids(&page), expected, "the type scan drives");
        assert_eq!(backend.type_count(&tenant, &query, &both).await.unwrap(), 2);

        // A `contains` asked at another `last_updated` does not match.
        let moved = MemberAt(
            made.iter()
                .map(|(at, id, _)| (id.clone(), *at + chrono::Duration::milliseconds(1)))
                .collect(),
        );
        let wrong_time = Checks {
            contains: vec![&moved],
            residuals: Vec::new(),
        };
        let page = backend
            .plan_search(&tenant, &query, &TypeRowsPlan, &wrong_time)
            .await
            .unwrap();
        assert!(page_ids(&page).is_empty());
    }

    /// AND pages through the cursor like any search: 2 a page over the 3
    /// females, each once, newest first.
    #[tokio::test]
    async fn and_pages_through_the_cursor() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = fresh_tenant("test-engine-and-paging");
        let made = create_genders(&backend, &tenant).await;
        let checks = Checks {
            contains: Vec::new(),
            residuals: vec![&Female],
        };
        let mut query = SearchQuery::new("Patient").with_count(2);
        let mut found = Vec::new();
        let mut pages = 0;
        loop {
            let page = backend
                .plan_search(&tenant, &query, &TypeRowsPlan, &checks)
                .await
                .unwrap();
            pages += 1;
            found.extend(page_ids(&page));
            match page.resources.page_info.next_cursor {
                Some(cursor) => query.cursor = Some(cursor),
                None => break,
            }
        }
        assert_eq!(found, newest_first(&made, |(_, _, female)| *female));
        assert_eq!(pages, 2);
    }
}
