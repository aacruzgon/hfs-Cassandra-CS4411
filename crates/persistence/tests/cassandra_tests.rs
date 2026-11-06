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

    use helios_fhir::FhirVersion;
    use helios_persistence::backends::cassandra::CassandraBackend;
    use helios_persistence::core::{BackendCapability, BackendKind, ResourceStorage};
    use helios_persistence::error::{BackendError, ResourceError, StorageError};
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
        assert!(a.supports_all(&[BackendCapability::Crud, BackendCapability::SharedSchema]));
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
            FhirVersion::default_enabled(),
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
        assert!(ctx.supports(BackendCapability::Crud));

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

        // The default `delete_versioned` reads first and answers NotFound for a
        // missing id; the backend's stub refuses before reading.
        let tenant = backend.tenant();
        match backend
            .delete_versioned(&tenant, "Patient", &unique_id("missing"), "1")
            .await
        {
            Err(StorageError::Backend(BackendError::UnsupportedCapability {
                backend_name,
                ..
            })) => assert_eq!(backend_name, "cassandra"),
            other => panic!("delete_versioned went to the default: {other:?}"),
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
                FhirVersion::default_enabled(),
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
