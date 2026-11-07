//! Cassandra backend tests (roadmap Step 2 onward).
//!
//! Against the shared node (`docker/cassandra`, schema applied):
//!
//! ```text
//! HFS_TEST_CASSANDRA_URL=127.0.0.1:59042 \
//!     cargo test -p helios-persistence --features cassandra --test cassandra_tests
//! ```
//!
//! With `HFS_TEST_CASSANDRA_URL` unset the harness starts its own container and
//! applies the schema to it; without Docker the node tests skip. See
//! `common/cassandra.rs` for both paths, and for the isolation rule every test
//! here follows: its own tenant ([`cassandra_node::unique_tenant`]) or its own
//! ids ([`cassandra_node::unique_id`]), never a cleanup (ADR-aacruzgon-0020).
//!
//! Later steps add their tests, and the shared suites they include
//! (`development-roadmap.md` section 5), to this binary.

#![cfg(feature = "cassandra")]

#[path = "common/container_cleanup.rs"]
mod container_cleanup;

#[path = "common/cassandra.rs"]
mod cassandra_node;

use std::collections::HashSet;

use cassandra_node::{
    DATA_CENTER, KEYSPACE, MAX_FHIR_ID_LEN, RELEASE_VERSION, TABLES, TEST_TENANT_PREFIX, URL_VAR,
    cql_statements, parse_url, read_schema_file, schema_listing, select, text, unique_id,
    unique_tenant, unique_tenant_id,
};
use helios_persistence::tenant::{MAX_TENANT_ID_LEN, TenantId};

/// The shared node, or a skip when no node can be had (see
/// [`cassandra_node::node`]).
macro_rules! node_or_skip {
    ($test:literal) => {
        match cassandra_node::node().await {
            Some(node) => node,
            None => {
                eprintln!("Skipping {} ({URL_VAR} unset and no Docker)", $test);
                return;
            }
        }
    };
}

/// Step 2: the backend's connection, `health_check`, `initialize`, the stubs
/// and the search-parameter registry (items 2.1 and 2.3).
#[path = "cassandra/backend_skeleton.rs"]
mod backend_skeleton;

/// Step 2: `create`, `read` and `count` (item 2.1).
#[path = "cassandra/create_read.rs"]
mod create_read;

/// Step 2: stored SearchParameters reloaded after a restart (item 2.2).
#[path = "cassandra/stored_search_parameters.rs"]
mod stored_search_parameters;

/// CI's per-operation latency report; runs only when
/// `HFS_TEST_LATENCY_REPORT` names a directory.
#[path = "cassandra/latency_report.rs"]
mod latency_report;

/// The shared backend-test harness, `tests/common/{harness, fixtures,
/// capabilities}.rs`, compiled here as `crate::common` because
/// [`TestableBackend`](common::harness::TestableBackend) lives there and
/// `harness.rs` names its two siblings. No other test target declares
/// `mod common;`, so this binary is the first to compile them. They stay as
/// upstream wrote them: what Cassandra does not use may stay unused, `rustfmt`
/// skips them, and the two Clippy lints `capabilities.rs` trips (its hand-written
/// `Default` for `SupportLevel`, and an `or_insert_with(HashMap::new)` in
/// `set_backend_capabilities`) are allowed here rather than fixed in a file the
/// capability-contract layer owns.
#[allow(
    dead_code,
    unused_imports,
    clippy::derivable_impls,
    clippy::unwrap_or_default
)]
mod common {
    #[rustfmt::skip]
    pub mod capabilities;
    #[rustfmt::skip]
    pub mod fixtures;
    #[rustfmt::skip]
    pub mod harness;
}

/// Step 2: `TestableBackend` for Cassandra (item 2.5).
#[path = "common/cassandra_testable.rs"]
mod cassandra_testable;

/// The seeding writes of #2435, which the shared suites below reach as
/// `crate::seeding`. Their retry is MongoDB's alone, so on Cassandra every
/// seeding write runs once.
#[path = "common/seeding.rs"]
mod seeding;

/// Step 3: upstream's `tests/crud/` as an adapted copy, its parity test and
/// the Cassandra-strict additions (rows 3.3 and 3.4).
#[path = "cassandra/crud.rs"]
mod crud;

/// Step 3: the backend-agnostic race suite for version-aware writes (#1404,
/// #1405, #2111), the compare-and-swap contract (row 3.3). Wrappers in
/// [`step3_suites`].
#[path = "search/versioned_write_race_suite.rs"]
mod versioned_write_race_suite;

/// Step 3: the multitenancy suites' tests that need only CRUD (roadmap
/// section 5). Wrappers in [`step3_suites`].
#[path = "multitenancy/tenant_id_fidelity_suite.rs"]
mod tenant_id_fidelity_suite;

#[path = "multitenancy/tenant_existence_suite.rs"]
mod tenant_existence_suite;

/// The node holds the signed-off schema: the ten tables, and every column,
/// key position, clustering order and table option recorded in
/// `docs/Cassandra/schema/expected-schema.txt`. The CI job makes the same
/// comparison with `cqlsh`; this one goes through the driver the backend uses.
#[tokio::test]
async fn node_has_the_signed_off_schema() {
    let node = node_or_skip!("node_has_the_signed_off_schema");
    let session = node.session().await;

    let rows = select(
        &session,
        &format!("SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{KEYSPACE}'"),
    )
    .await
    .unwrap();
    let mut tables: Vec<String> = rows
        .iter()
        .map(|row| text(row, "table_name").unwrap())
        .collect();
    tables.sort();
    assert_eq!(tables, TABLES, "tables of keyspace {KEYSPACE}");

    let actual = schema_listing(&session).await.unwrap();
    let expected = read_schema_file("expected-schema.txt").replace("\r\n", "\n");
    if actual != expected {
        let expected_lines: HashSet<&str> = expected.lines().collect();
        let actual_lines: HashSet<&str> = actual.lines().collect();
        let mut missing: Vec<&str> = expected_lines.difference(&actual_lines).copied().collect();
        let mut extra: Vec<&str> = actual_lines.difference(&expected_lines).copied().collect();
        missing.sort_unstable();
        extra.sort_unstable();
        panic!(
            "the schema on {} ({:?}) differs from docs/Cassandra/schema/expected-schema.txt\n\
             missing from the node:\n  {}\nonly on the node:\n  {}\n\
             A node reached through {URL_VAR} keeps an old schema until keyspace {KEYSPACE} \
             is dropped and applied again (ADR-aacruzgon-0020).",
            node.contact_point(),
            node.origin(),
            missing.join("\n  "),
            extra.join("\n  "),
        );
    }
}

/// The node runs `docker/cassandra`'s configuration: the pinned release,
/// `paxos_variant: v2` (ADR-aacruzgon-0001) and the DC the keyspace replicates
/// to (ADR-aacruzgon-0003).
#[tokio::test]
async fn node_runs_the_docker_cassandra_configuration() {
    let node = node_or_skip!("node_runs_the_docker_cassandra_configuration");
    let session = node.session().await;

    let local = select(
        &session,
        "SELECT release_version, data_center, rack FROM system.local",
    )
    .await
    .unwrap();
    let [local] = local.as_slice() else {
        panic!("system.local returned {} rows", local.len());
    };
    assert_eq!(text(local, "release_version").unwrap(), RELEASE_VERSION);
    assert_eq!(text(local, "data_center").unwrap(), DATA_CENTER);
    assert_eq!(text(local, "rack").unwrap(), "helios-rack1");

    let paxos = select(
        &session,
        "SELECT value FROM system_views.settings WHERE name = 'paxos_variant'",
    )
    .await
    .unwrap();
    let [paxos] = paxos.as_slice() else {
        panic!(
            "system_views.settings returned {} paxos_variant rows",
            paxos.len()
        );
    };
    assert_eq!(text(paxos, "value").unwrap(), "v2");

    // The driver keeps the contact point as the node's address: it does not
    // switch to the container IP that system.local reports as rpc_address
    // (docker/cassandra/README.md).
    let addresses: Vec<std::net::SocketAddr> = session
        .cluster_metadata()
        .nodes()
        .values()
        .map(|node| node.broadcast_rpc_address())
        .collect();
    let contact_point: Vec<std::net::SocketAddr> = tokio::net::lookup_host(node.contact_point())
        .await
        .unwrap()
        .collect();
    assert_eq!(addresses.len(), 1, "one node, no peers: {addresses:?}");
    assert!(
        contact_point.contains(&addresses[0]),
        "the driver addresses the node as {addresses:?}, not as {} ({contact_point:?})",
        node.contact_point()
    );
}

#[test]
fn unique_tenants_are_valid_distinct_and_marked() {
    let long_label = "a_test_name::with::module::paths_that_is_much_longer_than_sixty_four_bytes";
    let ids: Vec<TenantId> = (0..100)
        .map(|_| unique_tenant_id(long_label))
        .chain(["", "---", "résumé", "Patient/1"].map(unique_tenant_id))
        .collect();
    for id in &ids {
        assert!(id.as_str().len() <= MAX_TENANT_ID_LEN, "{id:?}");
        assert!(id.as_str().starts_with(TEST_TENANT_PREFIX), "{id:?}");
        assert!(id.is_canonical(), "{id:?}");
    }
    let distinct: HashSet<&str> = ids.iter().map(TenantId::as_str).collect();
    assert_eq!(distinct.len(), ids.len());
    assert_eq!(
        unique_tenant("x").tenant_id().as_str().len(),
        TEST_TENANT_PREFIX.len() + 1 + 1 + 32
    );
}

#[test]
fn unique_ids_are_valid_fhir_ids() {
    let ids: Vec<String> = (0..100)
        .map(|_| unique_id("Observation with a long label that will not fit in sixty-four"))
        .chain(["", "pt", "a.b"].map(unique_id))
        .collect();
    for id in &ids {
        assert!(!id.is_empty() && id.len() <= MAX_FHIR_ID_LEN, "{id:?}");
        assert!(
            id.chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.'),
            "{id:?}"
        );
    }
    let distinct: HashSet<&String> = ids.iter().collect();
    assert_eq!(distinct.len(), ids.len());
}

#[test]
fn the_url_is_one_host_and_port() {
    assert_eq!(parse_url("127.0.0.1:59042").unwrap(), "127.0.0.1:59042");
    assert_eq!(parse_url(" cassandra:9042\n").unwrap(), "cassandra:9042");
    assert_eq!(parse_url("[::1]:9042").unwrap(), "[::1]:9042");
    for invalid in [
        "",
        "127.0.0.1",
        "127.0.0.1:",
        ":9042",
        "127.0.0.1:0",
        "127.0.0.1:65536",
        "127.0.0.1:port",
        "cassandra://127.0.0.1:9042",
        "a:9042,b:9042",
        "::1:9042",
    ] {
        assert!(parse_url(invalid).is_err(), "{invalid:?} was accepted");
    }
}

#[test]
fn the_schema_files_split_into_their_statements() {
    let keyspace = cql_statements(&read_schema_file("keyspace-single-node.cql"));
    assert_eq!(keyspace.len(), 2, "{keyspace:#?}");
    assert!(keyspace[0].starts_with(&format!("CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}")));
    assert!(keyspace[1].starts_with("ALTER KEYSPACE system_traces"));

    let schema = cql_statements(&read_schema_file("schema.cql"));
    let created: Vec<&str> = schema
        .iter()
        .map(|statement| {
            statement
                .strip_prefix("CREATE TABLE IF NOT EXISTS ")
                .and_then(|rest| rest.split_whitespace().next())
                .unwrap_or_else(|| panic!("not a CREATE TABLE IF NOT EXISTS: {statement}"))
        })
        .collect();
    let mut sorted = created.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, TABLES);
}

#[test]
fn cql_comments_and_strings_do_not_split_statements() {
    let script = "-- a; comment\nCREATE TABLE t (a int PRIMARY KEY) WITH comment = 'x; -- y''s';\n\
                  /* block; */ SELECT \"we;ird\" FROM t // trailing; comment\n;;  ";
    assert_eq!(
        cql_statements(script),
        vec![
            "CREATE TABLE t (a int PRIMARY KEY) WITH comment = 'x; -- y''s'",
            "SELECT \"we;ird\" FROM t",
        ]
    );
}

/// Item 2.5: the Cassandra [`TestableBackend`](common::harness::TestableBackend)
/// on the test node.
mod testable_backend {
    use std::collections::HashSet;

    use helios_persistence::backends::cassandra::CassandraBackend;
    use helios_persistence::core::{BackendCapability, BackendKind, ResourceStorage};
    use helios_persistence::error::{ResourceError, StorageError};
    use helios_persistence::tenant::TenantContext;
    use serde_json::json;

    use crate::cassandra_node::{self, URL_VAR, unique_id};
    use crate::cassandra_testable::{CassandraTestBackend, reset_and_seed};
    use crate::common::fixtures::TestFixtures;
    use crate::common::harness::TestableBackend;

    /// Asserts every fixture reads back under `tenant` as version 1 of
    /// itself, and that `tenant` holds nothing else.
    async fn assert_seeded(
        backend: &CassandraTestBackend,
        tenant: &TenantContext,
        fixtures: &TestFixtures,
    ) {
        for (resource_type, id, expected) in fixtures.all_resources() {
            let stored = backend
                .read(tenant, resource_type, &id)
                .await
                .unwrap_or_else(|e| panic!("read {resource_type}/{id}: {e}"))
                .unwrap_or_else(|| panic!("{resource_type}/{id} not under {}", tenant.tenant_id()));
            assert_eq!(stored.id(), id);
            assert_eq!(stored.version_id(), "1", "{resource_type}/{id}");
            assert_eq!(stored.tenant_id(), tenant.tenant_id());
            for (key, value) in expected.as_object().unwrap() {
                assert_eq!(&stored.content()[key], value, "{resource_type}/{id} .{key}");
            }
        }
        assert_eq!(
            backend.count(tenant, None).await.unwrap(),
            fixtures.total_count() as u64,
            "resources under {}",
            tenant.tenant_id()
        );
    }

    /// Asserts no fixture is visible under `tenant`, and that it is empty.
    async fn assert_empty(
        backend: &CassandraTestBackend,
        tenant: &TenantContext,
        fixtures: &TestFixtures,
    ) {
        for (resource_type, id, _) in fixtures.all_resources() {
            let found = backend.read(tenant, resource_type, &id).await.unwrap();
            assert!(
                found.is_none(),
                "{resource_type}/{id} is visible under {}",
                tenant.tenant_id()
            );
        }
        assert_eq!(backend.count(tenant, None).await.unwrap(), 0);
    }

    /// The brief's sequence: build, seed, read back, reset, nothing visible
    /// under the new tenant, and two instances never see each other's data.
    #[tokio::test]
    async fn testable_backend_seeds_resets_and_isolates() {
        let node = node_or_skip!("testable_backend_seeds_resets_and_isolates");
        let a = CassandraTestBackend::connect(node, "testable-a").await;
        let b = CassandraTestBackend::connect(node, "testable-b").await;
        let fixtures = TestFixtures::rich();
        // Every fixture kind is present, so seeding covers them all.
        let kinds: HashSet<&str> = fixtures
            .all_resources()
            .iter()
            .map(|(t, _, _)| *t)
            .collect();
        assert_eq!(kinds.len(), 5, "{kinds:?}");

        // What the backend declares, and nothing hard-coded here.
        assert_eq!(a.backend_kind(), BackendKind::Cassandra);
        let declared: HashSet<BackendCapability> = CassandraBackend::declared_capabilities()
            .into_iter()
            .collect();
        assert_eq!(a.supported_capabilities(), declared);
        // Each other capability is declared by the step that implements it
        // (ADR-aacruzgon-0037); the contract test pins the list.
        assert!(a.supports(BackendCapability::SharedSchema));
        assert!(!a.supports(BackendCapability::Transactions));

        // Two instances, four tenants, all distinct and all test tenants.
        let tenants: HashSet<String> = [&a, &b]
            .iter()
            .flat_map(|x| [x.tenant(), x.secondary_tenant()])
            .map(|t| t.tenant_id().as_str().to_owned())
            .collect();
        assert_eq!(tenants.len(), 4, "{tenants:?}");
        assert!(
            tenants
                .iter()
                .all(|t| t.starts_with(cassandra_node::TEST_TENANT_PREFIX))
        );

        // Seed `a`; read every fixture back.
        a.seed(&fixtures).await.expect("seed a");
        assert_seeded(&a, &a.tenant(), &fixtures).await;
        assert_empty(&a, &a.secondary_tenant(), &fixtures).await;

        // `b` sees none of it, through its own tenants or its own connection,
        // and seeds the same fixed ids without meeting `a`'s.
        assert_empty(&b, &b.tenant(), &fixtures).await;
        assert_empty(&b, &b.secondary_tenant(), &fixtures).await;
        b.seed(&fixtures).await.expect("seed b, same ids as a");
        assert_seeded(&b, &b.tenant(), &fixtures).await;
        let extra = unique_id("testable-extra");
        b.create(
            &b.tenant(),
            "Patient",
            json!({"resourceType": "Patient", "id": extra}),
            crate::cassandra_node::test_fhir_version(),
        )
        .await
        .unwrap();
        assert!(
            a.read(&a.tenant(), "Patient", &extra)
                .await
                .unwrap()
                .is_none()
        );
        assert_seeded(&a, &a.tenant(), &fixtures).await;

        // Seeding a tenant twice is refused by the backend's real create.
        match a.seed(&fixtures).await {
            Err(StorageError::Resource(ResourceError::AlreadyExists { .. })) => {}
            other => panic!("second seed without reset: {other:?}"),
        }

        // Reset: both tenants replaced; nothing visible under the new ones.
        let (old, old_secondary) = (a.tenant(), a.secondary_tenant());
        a.reset().await.expect("reset");
        assert_ne!(a.tenant().tenant_id(), old.tenant_id());
        assert_ne!(a.secondary_tenant().tenant_id(), old_secondary.tenant_id());
        assert_ne!(a.tenant().tenant_id(), a.secondary_tenant().tenant_id());
        assert_empty(&a, &a.tenant(), &fixtures).await;
        assert_empty(&a, &a.secondary_tenant(), &fixtures).await;
        // Reset deleted nothing: the old tenant still holds its rows, which
        // nobody names again (ADR-aacruzgon-0020).
        assert_seeded(&a, &old, &fixtures).await;
        // `b` is untouched by `a`'s reset.
        assert_eq!(
            b.count(&b.tenant(), None).await.unwrap(),
            fixtures.total_count() as u64 + 1
        );

        // The fresh tenant seeds again.
        a.seed(&fixtures).await.expect("seed after reset");
        assert_seeded(&a, &a.tenant(), &fixtures).await;
    }

    /// A `TestContext` built from the backend follows its resets.
    #[tokio::test]
    async fn testable_backend_context_follows_reset() {
        let node = node_or_skip!("testable_backend_context_follows_reset");
        let mut ctx = CassandraTestBackend::connect(node, "testable-ctx")
            .await
            .into_context(TestFixtures::minimal());
        assert_eq!(ctx.tenant.tenant_id(), ctx.backend.tenant().tenant_id());
        assert_eq!(
            ctx.secondary_tenant.tenant_id(),
            ctx.backend.secondary_tenant().tenant_id()
        );
        assert!(ctx.supports(BackendCapability::SharedSchema));

        ctx.backend.seed(&ctx.fixtures).await.expect("seed");
        assert_seeded(&ctx.backend, &ctx.tenant, &ctx.fixtures).await;
        let before = ctx.tenant.clone();

        reset_and_seed(&mut ctx).await.expect("reset_and_seed");
        assert_ne!(ctx.tenant.tenant_id(), before.tenant_id());
        assert_eq!(ctx.tenant.tenant_id(), ctx.backend.tenant().tenant_id());
        assert_eq!(
            ctx.secondary_tenant.tenant_id(),
            ctx.backend.secondary_tenant().tenant_id()
        );
        assert_seeded(&ctx.backend, &ctx.tenant, &ctx.fixtures).await;
        assert_empty(&ctx.backend, &ctx.secondary_tenant, &ctx.fixtures).await;
        // A read-only copy of the current tenant reads the fixtures too.
        let read_only = ctx.read_only_tenant();
        assert!(
            ctx.backend
                .read(&read_only, "Patient", "patient-1")
                .await
                .unwrap()
                .is_some()
        );
    }

    /// The wrapper forwards to the backend's overrides, not the trait's
    /// defaults.
    #[tokio::test]
    async fn testable_backend_forwards_the_backend_overrides() {
        let node = node_or_skip!("testable_backend_forwards_the_backend_overrides");
        let backend = CassandraTestBackend::connect(node, "testable-fwd").await;
        let inner = backend.inner();
        assert_eq!(backend.backend_name(), "cassandra");
        // The default answers false; the backend overrides it to true.
        assert!(backend.is_cluster_shared());
        assert_eq!(backend.is_cluster_shared(), inner.is_cluster_shared());
        assert_eq!(
            backend.bulk_write_concurrency(),
            inner.bulk_write_concurrency()
        );
        assert_ne!(
            backend.bulk_write_concurrency(),
            1,
            "the trait default; the backend overrides it"
        );
        // A smoke check only: the default also answers Ok, and telling the two
        // apart needs a node that is down (L7's `/_readiness` check).
        backend.readiness_check().await.expect("readiness_check");

        // `delete_versioned` reaches the backend's compare-and-swap, not the
        // trait default, which reads and then calls `delete`: that would be
        // two pre-reads, where the backend's own needs one.
        let tenant = backend.tenant();
        let id = unique_id("testable-delete");
        backend
            .create(
                &tenant,
                "Patient",
                json!({"resourceType": "Patient", "id": id}),
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .expect("create");
        let before = inner.statement_counts();
        backend
            .delete_versioned(&tenant, "Patient", &id, "1")
            .await
            .expect("delete_versioned");
        let spent = inner.statement_counts().since(&before);
        assert_eq!((spent.read, spent.cas), (1, 1), "{spent}");
        match backend.delete_versioned(&tenant, "Patient", &id, "1").await {
            Err(StorageError::Resource(ResourceError::NotFound { .. })) => {}
            other => panic!("a repeated delete_versioned is NotFound: {other:?}"),
        }

        // `create_many` reaches the backend's `create`.
        let ids: Vec<String> = (0..3).map(|_| unique_id("testable-many")).collect();
        let results = backend
            .create_many(
                &tenant,
                "Patient",
                ids.iter()
                    .map(|id| json!({"resourceType": "Patient", "id": id}))
                    .collect(),
                crate::cassandra_node::test_fhir_version(),
            )
            .await;
        assert!(results.iter().all(Result::is_ok), "{results:?}");
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 3);
        let id_refs: Vec<&str> = ids.iter().map(String::as_str).collect();
        assert_eq!(
            backend
                .read_batch(&tenant, "Patient", &id_refs)
                .await
                .unwrap()
                .len(),
            3
        );
        assert!(backend.exists(&tenant, "Patient", &ids[0]).await.unwrap());
    }
}

/// Step 3's wrappers for the shared suites included above: the race suite
/// (row 3.3) and the two multitenancy tests that need only CRUD.
mod step3_suites {
    use std::sync::Arc;

    use helios_persistence::backends::cassandra::CasStats;

    use crate::cassandra_node::{self, CassandraNode, TEST_TENANT_PREFIX, URL_VAR};
    use crate::cassandra_testable::CassandraTestBackend;
    use crate::{tenant_existence_suite, tenant_id_fidelity_suite, versioned_write_race_suite};

    /// Rounds per race test: PostgreSQL's and MongoDB's 10, not SQLite's 40,
    /// on a node where each round is eight contending Paxos writers.
    const RACE_ROUNDS: usize = 10;

    /// The longest label [`unique_base`] takes.
    const MAX_BASE_LABEL: usize = 8;

    /// A base no other test or run has used, for the shared suites that
    /// derive their tenants from one: `test-<label>-<32 hex digits>`, at most
    /// 46 bytes, so that the race suite's `{base}-plain-delete-race` and the
    /// existence suite's `{base}-parent/child` stay within the 64-byte tenant
    /// bound (`tenant/id.rs`). PostgreSQL's `unique_base` with the `test-`
    /// prefix of every Cassandra test tenant (ADR-aacruzgon-0020, 0029).
    pub(crate) fn unique_base(label: &str) -> String {
        assert!(
            label.len() <= MAX_BASE_LABEL
                && label.bytes().all(|b| b.is_ascii_lowercase() || b == b'-'),
            "a short label: {label:?}"
        );
        format!(
            "{TEST_TENANT_PREFIX}{label}-{}",
            uuid::Uuid::new_v4().simple()
        )
    }

    #[test]
    fn unique_bases_fit_every_derived_tenant() {
        let longest_suffix = [
            "-plain-delete-race".len(),
            "-parent/child".len(),
            "/resources".len(),
        ]
        .into_iter()
        .max()
        .unwrap();
        let base = unique_base("abcdefgh");
        assert!(base.starts_with(TEST_TENANT_PREFIX), "{base}");
        assert!(
            base.len() + longest_suffix <= helios_persistence::tenant::MAX_TENANT_ID_LEN,
            "{base}"
        );
        assert_ne!(unique_base("a"), unique_base("a"));
    }

    /// The race suite's backend: the test wrapper, which forwards
    /// `VersionedStorage` as well as `ResourceStorage`.
    async fn race_backend(node: &CassandraNode, label: &str) -> Arc<CassandraTestBackend> {
        Arc::new(CassandraTestBackend::connect(node, label).await)
    }

    /// Prints the race's `[cas case=race:<test> …]` line: the write engine's
    /// counters over the race (`CasStats`'s `Display`), for CI's CAS outcome
    /// report (Step 3 plan R3, `ci-report.py cas`). Each wrapper connects its
    /// own backend, so the counters are the race's alone.
    fn print_cas(test: &str, backend: &CassandraTestBackend, before: &CasStats) {
        eprintln!(
            "[cas case=race:{test} {}]",
            backend.inner().cas_stats().since(before)
        );
    }

    /// #1404: of eight writers holding the same version, one `update` writes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn cassandra_concurrent_updates_from_the_same_version_admit_one() {
        let node = node_or_skip!("cassandra_concurrent_updates_from_the_same_version_admit_one");
        let backend = race_backend(node, "race-update").await;
        let before = backend.inner().cas_stats();
        versioned_write_race_suite::concurrent_updates_from_the_same_version_admit_one(
            Arc::clone(&backend),
            &unique_base("upd"),
            RACE_ROUNDS,
        )
        .await;
        print_cas(
            "cassandra_concurrent_updates_from_the_same_version_admit_one",
            &backend,
            &before,
        );
    }

    /// #1404: an update and a versioned delete of the same version: one wins.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn cassandra_concurrent_update_and_versioned_delete_admit_one() {
        let node = node_or_skip!("cassandra_concurrent_update_and_versioned_delete_admit_one");
        let backend = race_backend(node, "race-vdelete").await;
        let before = backend.inner().cas_stats();
        versioned_write_race_suite::concurrent_update_and_versioned_delete_admit_one(
            Arc::clone(&backend),
            &unique_base("vdel"),
            RACE_ROUNDS,
        )
        .await;
        print_cas(
            "cassandra_concurrent_update_and_versioned_delete_admit_one",
            &backend,
            &before,
        );
    }

    /// #1404, #1405: an update racing an unconditional delete leaves a
    /// contiguous history; the plain delete retries once (G25).
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn cassandra_concurrent_update_and_plain_delete_stay_consistent() {
        let node = node_or_skip!("cassandra_concurrent_update_and_plain_delete_stay_consistent");
        let backend = race_backend(node, "race-pdelete").await;
        let before = backend.inner().cas_stats();
        versioned_write_race_suite::concurrent_update_and_plain_delete_stay_consistent(
            Arc::clone(&backend),
            &unique_base("pdel"),
            RACE_ROUNDS,
        )
        .await;
        print_cas(
            "cassandra_concurrent_update_and_plain_delete_stay_consistent",
            &backend,
            &before,
        );
    }

    /// #2111: four unconditional PUTs racing onto one delete marker all
    /// succeed, at versions 3 to 6, exactly one of them `created`, and the
    /// marker stays in the history (the roadmap's Step 3 exit criterion).
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn cassandra_concurrent_puts_onto_a_tombstone_all_succeed() {
        let node = node_or_skip!("cassandra_concurrent_puts_onto_a_tombstone_all_succeed");
        let backend = race_backend(node, "race-restore").await;
        let before = backend.inner().cas_stats();
        versioned_write_race_suite::concurrent_puts_onto_a_tombstone_all_succeed(
            Arc::clone(&backend),
            &unique_base("rest"),
            RACE_ROUNDS,
        )
        .await;
        print_cas(
            "cassandra_concurrent_puts_onto_a_tombstone_all_succeed",
            &backend,
            &before,
        );
    }

    /// #1404: `delete_versioned` compares and deletes in one step. No race,
    /// so a plain runtime, as on every backend.
    #[tokio::test]
    async fn cassandra_versioned_delete_is_a_compare_and_swap() {
        let node = node_or_skip!("cassandra_versioned_delete_is_a_compare_and_swap");
        let backend = CassandraTestBackend::connect(node, "race-cas").await;
        let before = backend.inner().cas_stats();
        versioned_write_race_suite::versioned_delete_is_a_compare_and_swap(
            &backend,
            &unique_base("cas"),
        )
        .await;
        print_cas(
            "cassandra_versioned_delete_is_a_compare_and_swap",
            &backend,
            &before,
        );
    }

    /// On a current-thread runtime the race tests pass without racing
    /// (`versioned_write_race_suite.rs`, module docs), so each of the four
    /// must keep its multi-thread attribute. Read from this file's source.
    #[test]
    fn race_wrappers_run_on_eight_worker_threads() {
        let source = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/cassandra_tests.rs"
        ))
        .expect("read tests/cassandra_tests.rs");
        let lines: Vec<&str> = source.lines().map(str::trim).collect();
        for name in [
            "cassandra_concurrent_updates_from_the_same_version_admit_one",
            "cassandra_concurrent_update_and_versioned_delete_admit_one",
            "cassandra_concurrent_update_and_plain_delete_stay_consistent",
            "cassandra_concurrent_puts_onto_a_tombstone_all_succeed",
        ] {
            let at = lines
                .iter()
                .position(|l| *l == format!("async fn {name}() {{"))
                .unwrap_or_else(|| panic!("no wrapper {name}"));
            assert_eq!(
                lines[at - 1],
                "#[tokio::test(flavor = \"multi_thread\", worker_threads = 8)]",
                "{name}"
            );
        }
    }

    /// #447: distinct tenant ids (`base`, `/base`, `base/`, `//base`,
    /// `base/resources`) never share data. Cassandra matches the tenant id
    /// whole, as the first column of every partition key, so this is the
    /// "identity" row of the suite's table, asserted rather than read.
    #[tokio::test]
    async fn cassandra_distinct_tenant_ids_never_share_data() {
        let node = node_or_skip!("cassandra_distinct_tenant_ids_never_share_data");
        let backend = crate::backend_skeleton::backend(node).await;
        tenant_id_fidelity_suite::distinct_tenant_ids_never_share_data(
            &backend,
            &unique_base("fidelity"),
        )
        .await;
    }

    /// #447, #1912: a child tenant's data is not its parent's. The suite's
    /// discovery half compares `count_by_tenant` with the expected `false`
    /// for the parent; Cassandra keeps that method's default, an empty list,
    /// so that half holds trivially here and only `tenant_has_resources` (the
    /// default, over `count`) is really tested.
    /// `tenant_has_resources_matches_discovery` is not run: it needs
    /// `register_tenant`, `deregister_tenant` and `purge_tenant_data`, which
    /// Cassandra refuses (the trait defaults, `tenant_registry_unsupported`;
    /// `$purge` is 501, ADR-aacruzgon-0040), and neither is the fidelity
    /// suite's purge test, for `PurgableStorage`.
    #[tokio::test]
    async fn cassandra_child_data_is_not_the_parents() {
        let node = node_or_skip!("cassandra_child_data_is_not_the_parents");
        let backend = crate::backend_skeleton::backend(node).await;
        tenant_existence_suite::child_data_is_not_the_parents(&backend, &unique_base("exist"))
            .await;
    }
}
