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
