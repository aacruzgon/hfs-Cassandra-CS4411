//! Roadmap Step 2 (items 2.1 and 2.3) against a running node: the
//! connection, `health_check`, `initialize` on the ten tables, the stubs and
//! the search-parameter registry.
//!
//! The node comes from [`test_node_url`]: `HFS_TEST_CASSANDRA_URL`
//! (`host:port`), with the schema already applied (ADR-aacruzgon-0020). Every
//! test here reads only, or names its own tenant, so it can share the node with
//! any other test (rule 2).

use helios_fhir::FhirVersion;
use helios_persistence::backends::cassandra::{CassandraBackend, CassandraConfig, TABLES};
use helios_persistence::core::{
    Backend, BackendKind, BundleProvider, ConditionalInteraction, ConditionalStorage,
    ResourceStorage, SearchProvider, VersionedStorage,
};
use helios_persistence::error::{BackendError, StorageError, TransactionError};
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use helios_persistence::types::SearchQuery;
use serde_json::json;

/// The node to test against, or `None` to skip.
fn test_node_url() -> Option<String> {
    std::env::var("HFS_TEST_CASSANDRA_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
}

fn config_for(url: &str) -> CassandraConfig {
    CassandraConfig {
        contact_points: vec![url.to_string()],
        data_dir: Some(concat!(env!("CARGO_MANIFEST_DIR"), "/../../data").into()),
        ..CassandraConfig::default()
    }
}

/// Connects to the test node, or returns `None` (and says why) to skip.
async fn backend() -> Option<CassandraBackend> {
    let Some(url) = test_node_url() else {
        eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
        return None;
    };
    Some(
        CassandraBackend::connect(config_for(&url))
            .await
            .unwrap_or_else(|e| panic!("connect to {url}: {e}")),
    )
}

/// A tenant no other test uses (ADR-aacruzgon-0020, rule 2).
fn own_tenant(test: &str) -> TenantContext {
    let id = format!("s2-skeleton-{test}-{}", uuid::Uuid::new_v4().simple());
    TenantContext::new(TenantId::new(id), TenantPermissions::full_access())
}

fn is_unsupported(error: &StorageError) -> bool {
    matches!(
        error,
        StorageError::Backend(BackendError::UnsupportedCapability { backend_name, .. })
            if backend_name == "cassandra"
    )
}

#[tokio::test]
async fn cassandra_health_check_and_readiness_answer_from_the_node() {
    let Some(backend) = backend().await else {
        return;
    };
    assert_eq!(backend.kind(), BackendKind::Cassandra);
    assert_eq!(backend.name(), "cassandra");
    backend.health_check().await.expect("health_check");
    backend.readiness_check().await.expect("readiness_check");
    // Prepared once, then reused.
    backend.health_check().await.expect("second health_check");
}

#[tokio::test]
async fn cassandra_initialize_accepts_a_keyspace_with_the_ten_tables() {
    let Some(backend) = backend().await else {
        return;
    };
    assert_eq!(TABLES.len(), 10);
    backend.initialize().await.expect("initialize on helios");
    backend.migrate().await.expect("migrate only checks");
}

#[tokio::test]
async fn cassandra_initialize_refuses_a_keyspace_without_the_tables() {
    let Some(url) = test_node_url() else {
        eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
        return;
    };
    // `system_traces` exists on every node and holds none of the ten tables,
    // so this needs no keyspace of its own and writes nothing.
    let backend = CassandraBackend::connect(CassandraConfig {
        keyspace: "system_traces".to_string(),
        ..config_for(&url)
    })
    .await
    .expect("connect with keyspace system_traces");
    match backend.initialize().await {
        Err(BackendError::MigrationError { message }) => {
            assert!(message.contains("system_traces"), "{message}");
            assert!(message.contains("keyspace-single-node.cql"), "{message}");
        }
        other => panic!("expected MigrationError, got {other:?}"),
    }
}

#[tokio::test]
async fn cassandra_connect_to_a_closed_port_fails_as_connection_failed() {
    if test_node_url().is_none() {
        eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
        return;
    }
    let started = std::time::Instant::now();
    let result = CassandraBackend::connect(CassandraConfig {
        contact_points: vec!["127.0.0.1:1".to_string()],
        connect_timeout_ms: 2_000,
        ..CassandraConfig::default()
    })
    .await;
    match result {
        Err(StorageError::Backend(
            BackendError::ConnectionFailed { .. } | BackendError::Timeout { .. },
        )) => {}
        other => panic!("expected a connection failure, got {other:?}"),
    }
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "connect took {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn cassandra_stubs_refuse_with_unsupported_capability() {
    let Some(backend) = backend().await else {
        return;
    };
    let tenant = own_tenant("stubs");
    let version = FhirVersion::default_enabled();

    let err = backend
        .create(
            &tenant,
            "Patient",
            json!({"resourceType": "Patient"}),
            version,
        )
        .await
        .unwrap_err();
    assert!(is_unsupported(&err), "create: {err:?}");
    let err = backend.read(&tenant, "Patient", "p1").await.unwrap_err();
    assert!(is_unsupported(&err), "read: {err:?}");
    let err = backend
        .delete_versioned(&tenant, "Patient", "p1", "1")
        .await
        .unwrap_err();
    assert!(is_unsupported(&err), "delete_versioned: {err:?}");
    let err = backend.count(&tenant, Some("Patient")).await.unwrap_err();
    assert!(is_unsupported(&err), "count: {err:?}");
    let err = backend
        .vread(&tenant, "Patient", "p1", "1")
        .await
        .unwrap_err();
    assert!(is_unsupported(&err), "vread: {err:?}");
    let err = backend
        .search(&tenant, &SearchQuery::new("Patient"))
        .await
        .unwrap_err();
    assert!(is_unsupported(&err), "search: {err:?}");

    match backend
        .process_transaction(&tenant, Vec::new(), version)
        .await
    {
        Err(TransactionError::AtomicityUnsupported { backend_name }) => {
            assert_eq!(backend_name, "cassandra")
        }
        other => panic!("expected AtomicityUnsupported, got {other:?}"),
    }
    assert!(!backend.supports_atomic_transactions());
    assert!(!backend.supports_conditional_in_transaction());
    for interaction in [
        ConditionalInteraction::Create,
        ConditionalInteraction::Update,
        ConditionalInteraction::Delete,
        ConditionalInteraction::Patch,
    ] {
        assert!(
            !backend.supports_conditional(interaction),
            "{interaction:?}"
        );
    }
}

#[tokio::test]
async fn cassandra_overrides_the_networked_store_defaults() {
    let Some(backend) = backend().await else {
        return;
    };
    assert!(backend.is_cluster_shared());
    assert_eq!(backend.bulk_write_concurrency(), 8);
    assert!(!backend.supports_tenant_registry());
}

#[tokio::test]
async fn cassandra_search_param_registry_is_per_tenant_and_populated() {
    let Some(backend) = backend().await else {
        return;
    };
    let a = own_tenant("registry-a");
    let b = own_tenant("registry-b");
    let registry_a = backend.search_param_registry(&a);
    assert!(
        registry_a.read().get_param("Patient", "gender").is_some(),
        "Patient.gender is in the base registry"
    );
    // Cached per tenant: the same tenant gets the same registry back.
    assert!(std::sync::Arc::ptr_eq(
        &registry_a,
        &backend.search_param_registry(&a)
    ));
    assert!(!std::sync::Arc::ptr_eq(
        &registry_a,
        &backend.search_param_registry(&b)
    ));
    backend
        .refresh_stored_search_parameters()
        .await
        .expect("refresh");
    // The refresh drops the cache, so the tenant's registry is rebuilt.
    assert!(!std::sync::Arc::ptr_eq(
        &registry_a,
        &backend.search_param_registry(&a)
    ));
}

#[tokio::test]
async fn cassandra_connect_refuses_a_local_dc_the_cluster_does_not_have() {
    let Some(url) = test_node_url() else {
        eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
        return;
    };
    let result = CassandraBackend::connect(CassandraConfig {
        local_dc: "no-such-dc".to_string(),
        ..config_for(&url)
    })
    .await;
    match result {
        Err(StorageError::Backend(
            BackendError::Internal { message, .. } | BackendError::ConnectionFailed { message, .. },
        )) => assert!(message.contains("no-such-dc"), "{message}"),
        other => panic!("expected the local DC to be refused, got {other:?}"),
    }
}

#[tokio::test]
async fn cassandra_connect_to_a_missing_keyspace_fails_naming_it() {
    let Some(url) = test_node_url() else {
        eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
        return;
    };
    let result = CassandraBackend::connect(CassandraConfig {
        keyspace: "s2_skeleton_no_such_keyspace".to_string(),
        connect_timeout_ms: 2_000,
        ..config_for(&url)
    })
    .await;
    match result {
        Err(StorageError::Backend(BackendError::ConnectionFailed { message, .. })) => {
            assert!(
                message.contains("s2_skeleton_no_such_keyspace"),
                "{message}"
            )
        }
        other => panic!("expected ConnectionFailed naming the keyspace, got {other:?}"),
    }
}

/// `helios_rest::create_app`'s bound (`crates/rest/src/lib.rs`), checked here
/// so a missing trait fails this layer's build, not the wiring layer's.
#[allow(dead_code)]
fn assert_server_bound<S>()
where
    S: ResourceStorage
        + ConditionalStorage
        + SearchProvider
        + helios_persistence::core::IncludeProvider
        + helios_persistence::core::RevincludeProvider
        + helios_persistence::core::InstanceHistoryProvider
        + helios_persistence::core::TypeHistoryProvider
        + helios_persistence::core::SystemHistoryProvider
        + BundleProvider
        + helios_persistence::core::ExportDataProvider
        + helios_persistence::core::PatientExportProvider
        + helios_persistence::core::GroupExportProvider
        + helios_persistence::core::SearchCapabilityProvider
        + Backend
        + Send
        + Sync
        + 'static,
{
}

#[test]
fn cassandra_backend_meets_the_server_bound() {
    assert_server_bound::<CassandraBackend>();
}
