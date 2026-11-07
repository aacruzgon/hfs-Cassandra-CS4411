//! Roadmap Step 2 (items 2.1 and 2.3) against a running node: the
//! connection, `health_check`, `initialize` on the ten tables, the stubs and
//! the search-parameter registry. `create`, `read` and `count` are in
//! `create_read.rs` (layer L3).
//!
//! Included by `tests/cassandra_tests.rs`. The node comes from
//! [`cassandra_node::node`] (`HFS_TEST_CASSANDRA_URL`, or the harness's own
//! container). Every test here reads only, or names its own tenant
//! ([`unique_tenant`]), so it can share the node with any other test
//! (ADR-aacruzgon-0020, rule 2).

use helios_fhir::FhirVersion;
use helios_persistence::backends::cassandra::{CassandraBackend, CassandraConfig, TABLES};
use helios_persistence::core::{
    Backend, BackendKind, BundleProvider, ConditionalInteraction, ConditionalStorage,
    ResourceStorage, SearchProvider, VersionedStorage,
};
use helios_persistence::error::{BackendError, StorageError, TransactionError};
use helios_persistence::types::SearchQuery;
use serde_json::json;

use crate::cassandra_node::{self, CassandraNode, URL_VAR, unique_tenant};

/// The backend's configuration for the test node: its contact point, which
/// has `HFS_TEST_CASSANDRA_URL`'s `host:port` form, and the repository's
/// `data/` directory for the search-parameter registry.
pub(crate) fn config_for(node: &CassandraNode) -> CassandraConfig {
    CassandraConfig {
        contact_points: vec![node.contact_point().to_string()],
        data_dir: Some(concat!(env!("CARGO_MANIFEST_DIR"), "/../../data").into()),
        ..CassandraConfig::default()
    }
}

/// Connects the backend to the test node; panics when it cannot.
pub(crate) async fn backend(node: &CassandraNode) -> CassandraBackend {
    CassandraBackend::connect(config_for(node))
        .await
        .unwrap_or_else(|e| panic!("connect to {}: {e}", node.contact_point()))
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
    let node = node_or_skip!("cassandra_health_check_and_readiness_answer_from_the_node");
    let backend = backend(node).await;
    assert_eq!(backend.kind(), BackendKind::Cassandra);
    assert_eq!(backend.name(), "cassandra");
    backend.health_check().await.expect("health_check");
    backend.readiness_check().await.expect("readiness_check");
    // Prepared once, then reused.
    backend.health_check().await.expect("second health_check");
}

#[tokio::test]
async fn cassandra_initialize_accepts_a_keyspace_with_the_ten_tables() {
    let node = node_or_skip!("cassandra_initialize_accepts_a_keyspace_with_the_ten_tables");
    let backend = backend(node).await;
    assert_eq!(TABLES.len(), 10);
    backend.initialize().await.expect("initialize on helios");
    backend.migrate().await.expect("migrate only checks");
}

#[tokio::test]
async fn cassandra_initialize_refuses_a_keyspace_without_the_tables() {
    let node = node_or_skip!("cassandra_initialize_refuses_a_keyspace_without_the_tables");
    // `system_traces` exists on every node and holds none of the ten tables,
    // so this needs no keyspace of its own and writes nothing.
    let backend = CassandraBackend::connect(CassandraConfig {
        keyspace: "system_traces".to_string(),
        ..config_for(node)
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
    let _node = node_or_skip!("cassandra_connect_to_a_closed_port_fails_as_connection_failed");
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
    let node = node_or_skip!("cassandra_stubs_refuse_with_unsupported_capability");
    let backend = backend(node).await;
    let tenant = unique_tenant("stubs");
    let version = FhirVersion::default_enabled();

    // `create`, `read` and `count` are real from L3 on (`create_read.rs`).
    // `update` is real from Step 3's L2a: on an id that was never created it
    // is `NotFound` (walkthrough `:180`), no longer 501.
    let err = backend
        .update(
            &tenant,
            &helios_persistence::types::StoredResource::new(
                "Patient",
                "p1",
                tenant.tenant_id().clone(),
                json!({"resourceType": "Patient", "id": "p1"}),
                version,
            ),
            json!({"resourceType": "Patient", "id": "p1"}),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            StorageError::Resource(helios_persistence::error::ResourceError::NotFound { .. })
        ),
        "update: {err:?}"
    );
    let err = backend.delete(&tenant, "Patient", "p1").await.unwrap_err();
    assert!(is_unsupported(&err), "delete: {err:?}");
    let err = backend
        .delete_versioned(&tenant, "Patient", "p1", "1")
        .await
        .unwrap_err();
    assert!(is_unsupported(&err), "delete_versioned: {err:?}");
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
    let node = node_or_skip!("cassandra_overrides_the_networked_store_defaults");
    let backend = backend(node).await;
    assert!(backend.is_cluster_shared());
    assert_eq!(backend.bulk_write_concurrency(), 8);
    assert!(!backend.supports_tenant_registry());
}

#[tokio::test]
async fn cassandra_search_param_registry_is_per_tenant_and_populated() {
    let node = node_or_skip!("cassandra_search_param_registry_is_per_tenant_and_populated");
    let backend = backend(node).await;
    let a = unique_tenant("registry-a");
    let b = unique_tenant("registry-b");
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
    let node = node_or_skip!("cassandra_connect_refuses_a_local_dc_the_cluster_does_not_have");
    let result = CassandraBackend::connect(CassandraConfig {
        local_dc: "no-such-dc".to_string(),
        ..config_for(node)
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
    let node = node_or_skip!("cassandra_connect_to_a_missing_keyspace_fails_naming_it");
    let result = CassandraBackend::connect(CassandraConfig {
        keyspace: "s2_skeleton_no_such_keyspace".to_string(),
        connect_timeout_ms: 2_000,
        ..config_for(node)
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
