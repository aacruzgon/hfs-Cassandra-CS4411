//! Roadmap 6d (#129): upstream's `tests/search/pagination_tests.rs` as an
//! adapted copy that runs on the Cassandra node, the way
//! `tests/cassandra/crud.rs` and `versioning.rs` copy theirs
//! (ADR-aacruzgon-0057).
//!
//! Upstream's file is wired to SQLite only (`create_sqlite_backend`, every
//! test `#[cfg(feature = "sqlite")]`) and seeds a fixed `test-tenant`, which
//! would collide across tests and runs on the shared node
//! (ADR-aacruzgon-0020, rule 2). So this file holds the same test names and
//! bodies in [`pagination_tests`], with these changes only:
//!
//! - **the backend** is a [`CassandraBackend`](helios_persistence::backends::cassandra::CassandraBackend)
//!   on the test node ([`crate::backend_skeleton::backend`]), and a test skips
//!   when there is no node, as every Cassandra test does;
//! - **every tenant is fresh** ([`unique_tenant`](crate::cassandra_node::unique_tenant)),
//!   in place of `test-tenant`;
//! - **[`test_fhir_version`](crate::cassandra_node::test_fhir_version)** in
//!   place of upstream's `FhirVersion::default()` (ADR-aacruzgon-0065);
//! - **the two sort tests are not copied.** `test_pagination_with_sort` and
//!   `test_pagination_with_asc_sort` sort by `name`, a string; Cassandra has
//!   only its native order and refuses `_sort` (design, "Sorting is ✗").
//!   [`parity`] lists them on `EXCLUDED`, with the reason;
//! - **`test_pagination_with_filter` is `#[ignore]`d until date search
//!   answers** (#132): it pages a date-only `birthdate=sa1970-01-01`
//!   search. Its `#[ignore]` goes in the commit that makes it pass. The
//!   other 8 pass on the framework's unfiltered search (#128, row 6.2);
//! - **`test_pagination_total` checks nothing on Cassandra.** It asserts
//!   only inside `if let Some(total)`, and Cassandra leaves `total` unset
//!   unless `_total` asks for one, as search.html#total allows, so it
//!   passes whatever the count. Totals are tested by the framework's own
//!   tests, and will be by the copy of `total_mode_suite.rs` that Step 6's
//!   exit criterion asks for.
//!
//! **Not compiled in CI yet:** `tests/cassandra_tests.rs` (Sara's file) has
//! no `#[path] mod` line for this file, which has been asked of her, so CI
//! neither builds nor runs it until then.
//!
//! [`parity`] reads `tests/search/pagination_tests.rs` when it runs, so an
//! upstream sync that adds, renames or removes a test there, or changes the
//! file, fails it until this copy follows.

/// The Cassandra backend on the test node, or a skip (`return`) when there is
/// no node. Stands in for upstream's `create_sqlite_backend()`.
macro_rules! cassandra_backend {
    ($test:literal) => {{
        match crate::cassandra_node::node().await {
            Some(node) => crate::backend_skeleton::backend(node).await,
            None => {
                eprintln!(
                    "Skipping {} ({} unset and no Docker)",
                    $test,
                    crate::cassandra_node::URL_VAR
                );
                return;
            }
        }
    }};
}

pub mod pagination_tests {
    //! Tests for search pagination.
    //!
    //! This module tests cursor-based and offset-based pagination
    //! including _count, page navigation, and result ordering.

    use serde_json::json;

    use helios_persistence::backends::cassandra::CassandraBackend;
    use helios_persistence::core::{ResourceStorage, SearchProvider};
    use helios_persistence::tenant::TenantContext;
    use helios_persistence::types::{SearchParamType, SearchParameter, SearchQuery, SearchValue};

    use crate::cassandra_node::{test_fhir_version, unique_tenant};

    fn create_tenant() -> TenantContext {
        unique_tenant("paging")
    }

    async fn seed_many_patients(backend: &CassandraBackend, tenant: &TenantContext, count: usize) {
        for i in 0..count {
            let patient = json!({
                "resourceType": "Patient",
                "name": [{"family": format!("Patient{:03}", i)}],
                "birthDate": format!("19{:02}-01-01", 50 + (i % 50))
            });
            backend
                .create(tenant, "Patient", patient, test_fhir_version())
                .await
                .unwrap();
        }
    }

    // ============================================================================
    // Basic Pagination Tests
    // ============================================================================

    /// Test _count limits results.
    #[tokio::test]
    async fn test_pagination_count() {
        let backend = cassandra_backend!("test_pagination_count");
        let tenant = create_tenant();
        seed_many_patients(&backend, &tenant, 20).await;

        let query = SearchQuery::new("Patient").with_count(5);
        let result = backend.search(&tenant, &query).await.unwrap();

        assert_eq!(result.resources.len(), 5);
    }

    /// Test total count is returned.
    #[tokio::test]
    async fn test_pagination_total() {
        let backend = cassandra_backend!("test_pagination_total");
        let tenant = create_tenant();
        seed_many_patients(&backend, &tenant, 20).await;

        let query = SearchQuery::new("Patient").with_count(5);
        let result = backend.search(&tenant, &query).await.unwrap();

        // If total is supported, it should be 20
        if let Some(total) = result.total {
            assert_eq!(total, 20);
        }
    }

    // ============================================================================
    // Cursor Pagination Tests
    // ============================================================================

    /// Test cursor-based pagination forward.
    #[tokio::test]
    async fn test_cursor_pagination_forward() {
        let backend = cassandra_backend!("test_cursor_pagination_forward");
        let tenant = create_tenant();
        seed_many_patients(&backend, &tenant, 20).await;

        // Get first page
        let query = SearchQuery::new("Patient").with_count(5);
        let page1 = backend.search(&tenant, &query).await.unwrap();

        assert_eq!(page1.resources.len(), 5);
        assert!(
            page1.resources.page_info.next_cursor.is_some(),
            "Should have next cursor"
        );

        // Get second page
        if let Some(cursor) = page1.resources.page_info.next_cursor.clone() {
            let mut query2 = SearchQuery::new("Patient").with_count(5);
            query2.cursor = Some(cursor);
            let page2 = backend.search(&tenant, &query2).await.unwrap();

            assert_eq!(page2.resources.len(), 5);

            // Pages should not overlap
            let page1_ids: std::collections::HashSet<_> =
                page1.resources.items.iter().map(|r| r.id()).collect();
            for resource in &page2.resources.items {
                assert!(
                    !page1_ids.contains(resource.id()),
                    "Pages should not overlap"
                );
            }
        }
    }

    /// Test paginating through all results.
    #[tokio::test]
    async fn test_paginate_all_results() {
        let backend = cassandra_backend!("test_paginate_all_results");
        let tenant = create_tenant();
        seed_many_patients(&backend, &tenant, 25).await;

        let mut all_ids = std::collections::HashSet::new();
        let mut cursor: Option<String> = None;

        loop {
            let mut query = SearchQuery::new("Patient").with_count(10);
            query.cursor = cursor.clone();
            let page = backend.search(&tenant, &query).await.unwrap();

            for resource in &page.resources.items {
                let inserted = all_ids.insert(resource.id().to_string());
                assert!(inserted, "Should not see duplicate resources");
            }

            match page.resources.page_info.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }

        // Should have seen all 25 patients
        assert_eq!(all_ids.len(), 25);
    }

    /// Test last page has no next cursor.
    #[tokio::test]
    async fn test_last_page_no_cursor() {
        let backend = cassandra_backend!("test_last_page_no_cursor");
        let tenant = create_tenant();
        seed_many_patients(&backend, &tenant, 8).await;

        // First page of 5
        let query = SearchQuery::new("Patient").with_count(5);
        let page1 = backend.search(&tenant, &query).await.unwrap();
        assert!(page1.resources.page_info.next_cursor.is_some());

        // Second page of remaining 3
        if let Some(cursor) = page1.resources.page_info.next_cursor.clone() {
            let mut query2 = SearchQuery::new("Patient").with_count(5);
            query2.cursor = Some(cursor);
            let page2 = backend.search(&tenant, &query2).await.unwrap();

            assert_eq!(page2.resources.len(), 3);
            assert!(
                page2.resources.page_info.next_cursor.is_none(),
                "Last page should have no cursor"
            );
        }
    }

    // ============================================================================
    // Edge Cases
    // ============================================================================

    /// Test pagination with no results.
    #[tokio::test]
    async fn test_pagination_empty_results() {
        let backend = cassandra_backend!("test_pagination_empty_results");
        let tenant = create_tenant();

        let query = SearchQuery::new("Patient").with_count(10);
        let result = backend.search(&tenant, &query).await.unwrap();

        assert!(result.resources.is_empty());
        assert!(result.resources.page_info.next_cursor.is_none());
    }

    /// Test pagination with count larger than results.
    #[tokio::test]
    async fn test_pagination_count_larger_than_results() {
        let backend = cassandra_backend!("test_pagination_count_larger_than_results");
        let tenant = create_tenant();
        seed_many_patients(&backend, &tenant, 5).await;

        let query = SearchQuery::new("Patient").with_count(100);
        let result = backend.search(&tenant, &query).await.unwrap();

        assert_eq!(result.resources.len(), 5);
        assert!(result.resources.page_info.next_cursor.is_none());
    }

    /// Test pagination count of 1.
    #[tokio::test]
    async fn test_pagination_count_one() {
        let backend = cassandra_backend!("test_pagination_count_one");
        let tenant = create_tenant();
        seed_many_patients(&backend, &tenant, 5).await;

        let mut seen = 0;
        let mut cursor: Option<String> = None;

        loop {
            let mut query = SearchQuery::new("Patient").with_count(1);
            query.cursor = cursor.clone();
            let page = backend.search(&tenant, &query).await.unwrap();

            assert!(page.resources.len() <= 1);
            seen += page.resources.len();

            match page.resources.page_info.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }

        assert_eq!(seen, 5);
    }

    // ============================================================================
    // Filtered Pagination Tests
    // ============================================================================

    /// Test pagination with search filter.
    #[tokio::test]
    #[ignore = "a date-only search answers 501 until date search (#132)"]
    async fn test_pagination_with_filter() {
        let backend = cassandra_backend!("test_pagination_with_filter");
        let tenant = create_tenant();
        seed_many_patients(&backend, &tenant, 50).await;

        let mut total_found = 0;
        let mut cursor: Option<String> = None;

        loop {
            // Search for patients with birth year starting with "19"
            let mut query = SearchQuery::new("Patient")
                .with_parameter(SearchParameter {
                    name: "birthdate".to_string(),
                    param_type: SearchParamType::Date,
                    modifier: None,
                    values: vec![SearchValue::new(
                        helios_persistence::types::SearchPrefix::Sa,
                        "1970-01-01",
                    )],
                    chain: vec![],
                    components: vec![],
                })
                .with_count(10);
            query.cursor = cursor.clone();
            let page = backend.search(&tenant, &query).await.unwrap();
            total_found += page.resources.len();

            match page.resources.page_info.next_cursor {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }

        // Should have found some subset of patients
        assert!(total_found > 0);
    }
}

/// The copy against upstream's file: the same tests but the excluded ones,
/// and upstream's content pinned, so a sync that changes it is noticed.
pub mod parity {
    use std::collections::BTreeSet;
    use std::path::Path;

    /// Upstream tests deliberately left out, with the reason.
    const EXCLUDED: &[(&str, &str)] = &[
        (
            "test_pagination_with_sort",
            "_sort=-name sorts by a string; Cassandra has only its native order and \
             refuses _sort (design, \"Sorting is ✗\")",
        ),
        (
            "test_pagination_with_asc_sort",
            "_sort=name sorts by a string; Cassandra has only its native order and \
             refuses _sort (design, \"Sorting is ✗\")",
        ),
    ];

    /// Upstream's file as a 64-bit FNV-1a hash of its text with `\r\n` read
    /// as `\n` (the same hash as `crud.rs`'s and `versioning.rs`'s). A changed
    /// file fails here until the copy follows it: bring the change into
    /// [`super::pagination_tests`] (or check the exclusions still hold), then
    /// update the hash. Baseline: `main` at `883f9b1e0`.
    const UPSTREAM_CONTENT: u64 = 0xbd21_01da_67a6_5804;

    fn read(path: &Path) -> String {
        std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()))
    }

    fn fnv1a(text: &str) -> u64 {
        text.replace("\r\n", "\n")
            .bytes()
            .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
            })
    }

    /// The test functions of `source`: every `fn` whose nearest preceding
    /// line that is not a comment, an `#[ignore…]` or a `#[cfg(…)]` is a test
    /// attribute (as `versioning.rs`'s `parity::tests` finds them).
    fn tests(source: &str) -> BTreeSet<String> {
        let lines: Vec<&str> = source.lines().map(str::trim_start).collect();
        let mut found = BTreeSet::new();
        for (i, line) in lines.iter().enumerate() {
            let Some(rest) = line
                .strip_prefix("async fn ")
                .or_else(|| line.strip_prefix("fn "))
            else {
                continue;
            };
            let name = rest.split(['(', '<']).next().unwrap_or("").to_owned();
            for above in lines[..i].iter().rev() {
                if above.starts_with("//")
                    || above.starts_with("#[ignore")
                    || above.starts_with("#[cfg(")
                {
                    continue;
                }
                if *above == "#[test]" || above.starts_with("#[tokio::test") {
                    found.insert(name.clone());
                }
                break;
            }
        }
        found
    }

    #[test]
    fn the_copy_has_every_upstream_test_but_the_excluded_ones() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
        let upstream = read(&root.join("search/pagination_tests.rs"));
        let copy = read(&root.join("cassandra/search_paging.rs"));
        let copy = copy
            .split("pub mod parity")
            .next()
            .expect("the copy's tests");

        let excluded: BTreeSet<String> =
            EXCLUDED.iter().map(|(name, _)| name.to_string()).collect();
        let upstream_tests = tests(&upstream);
        assert!(
            excluded.is_subset(&upstream_tests),
            "an excluded test is no longer upstream: {:?}",
            excluded.difference(&upstream_tests).collect::<Vec<_>>()
        );
        let expected: BTreeSet<String> = upstream_tests.difference(&excluded).cloned().collect();
        assert_eq!(tests(copy), expected);
        assert_eq!(expected.len(), 9, "the 9 tests #129 keeps");
    }

    #[test]
    fn upstreams_file_is_the_one_the_copy_follows() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/search/pagination_tests.rs");
        assert_eq!(
            fnv1a(&read(&path)),
            UPSTREAM_CONTENT,
            "tests/search/pagination_tests.rs changed: bring the change into \
             search_paging.rs's pagination_tests, then update UPSTREAM_CONTENT"
        );
    }
}
