//! Roadmap Step 2, item 2.2 (layer L6): a tenant's stored SearchParameters
//! reach its registry again after a restart, from the `resources_by_type`
//! rows `create` writes.
//!
//! A second [`CassandraBackend`] on the same node stands for the server after
//! a restart: its overlay starts empty. Each test writes under its own tenant
//! ([`unique_tenant`]).

use helios_fhir::FhirVersion;
use helios_persistence::backends::cassandra::CassandraBackend;
use helios_persistence::core::{ResourceStorage, SearchProvider};
use helios_persistence::tenant::TenantContext;
use serde_json::json;

use crate::backend_skeleton::backend;
use crate::cassandra_node::{self, URL_VAR, unique_tenant};

fn search_parameter(id: &str, url: &str, status: &str) -> serde_json::Value {
    json!({
        "resourceType": "SearchParameter",
        "id": id,
        "url": url,
        "name": id,
        "status": status,
        "description": "test",
        "code": id,
        "base": ["Patient"],
        "type": "token",
        "expression": "Patient.gender"
    })
}

/// Whether `tenant`'s registry on `backend` holds a definition with `url`.
fn has_url(backend: &CassandraBackend, tenant: &TenantContext, url: &str) -> bool {
    backend
        .search_param_registry(tenant)
        .read()
        .get_by_url(url)
        .is_some()
}

/// Stores an active, a draft and a spec-url SearchParameter in a fresh
/// tenant; returns the tenant and the active one's url.
async fn store_three(writer: &CassandraBackend, label: &str) -> (TenantContext, String) {
    let tenant = unique_tenant(label);
    let version = FhirVersion::default_enabled();
    let url = format!(
        "http://example.org/SearchParameter/{}",
        tenant.tenant_id().as_str()
    );
    for (id, url, status) in [
        ("activesp", url.as_str(), "active"),
        (
            "draftsp",
            "http://example.org/SearchParameter/draft",
            "draft",
        ),
        // The shared base already holds this url; the overlay skips it.
        (
            "specsp",
            "http://hl7.org/fhir/SearchParameter/individual-gender",
            "active",
        ),
    ] {
        writer
            .create(
                &tenant,
                "SearchParameter",
                search_parameter(id, url, status),
                version,
            )
            .await
            .unwrap_or_else(|e| panic!("create {id}: {e}"));
    }
    (tenant, url)
}

#[tokio::test]
async fn cassandra_stored_search_parameters_load_after_a_restart() {
    let node = node_or_skip!("cassandra_stored_search_parameters_load_after_a_restart");
    let writer = backend(node).await;
    let (tenant, url) = store_three(&writer, "stored-sp").await;
    assert!(
        has_url(&writer, &tenant, &url),
        "the writing process sees it at once"
    );

    let restarted = backend(node).await;
    assert!(
        !has_url(&restarted, &tenant, &url),
        "a new process starts with an empty overlay"
    );
    let loaded = restarted
        .load_stored_search_parameters(tenant.tenant_id().as_str())
        .await
        .expect("load");
    assert_eq!(loaded, 1, "only the active, non-spec SearchParameter");
    assert!(has_url(&restarted, &tenant, &url));
    assert!(!has_url(
        &restarted,
        &tenant,
        "http://example.org/SearchParameter/draft"
    ));

    let other = unique_tenant("stored-sp-other");
    assert!(!has_url(&restarted, &other, &url), "tenants stay apart");
}

#[tokio::test]
async fn cassandra_refresh_loads_a_tenant_seen_since_start() {
    let node = node_or_skip!("cassandra_refresh_loads_a_tenant_seen_since_start");
    let writer = backend(node).await;
    let (tenant, url) = store_three(&writer, "refresh-sp").await;

    let restarted = backend(node).await;
    // The tenant's first request builds its registry from the shared base.
    assert!(!has_url(&restarted, &tenant, &url));
    let stored = restarted
        .refresh_stored_search_parameters()
        .await
        .expect("refresh");
    assert_eq!(stored, 1, "the one tenant this process has seen");
    assert!(has_url(&restarted, &tenant, &url));
}
