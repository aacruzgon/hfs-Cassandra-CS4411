//! Roadmap Step 2, item 2.2 (layer L6): a tenant's stored SearchParameters
//! reach its registry again after a restart, from the `resources_by_type`
//! rows `create` writes.
//!
//! A second [`CassandraBackend`] on the same node stands for the server after
//! a restart: its overlay starts empty. Each test writes under its own tenant
//! ([`unique_tenant`]), which is never the server's default tenant.

use std::collections::HashSet;

use helios_fhir::FhirVersion;
use helios_persistence::backends::cassandra::CassandraBackend;
use helios_persistence::core::patient_compartment::PatientCompartmentMatcher;
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

/// The server's startup load (`start_cassandra`) covers a tenant this
/// process has never seen: no request, no refresh. With
/// `HFS_SEARCH_PARAM_CACHE_TTL=0` no refresh ever runs, so this load is the
/// only one; the refresh runs the same function.
#[tokio::test]
async fn cassandra_startup_load_covers_an_unseen_tenant_without_a_refresh() {
    let node = node_or_skip!("cassandra_startup_load_covers_an_unseen_tenant_without_a_refresh");
    let writer = backend(node).await;
    let (tenant, url) = store_three(&writer, "startup-sp").await;

    let restarted = backend(node).await;
    let listed = restarted
        .stored_search_parameter_tenants()
        .await
        .expect("tenant scan");
    assert!(
        listed.contains(tenant.tenant_id().as_str()),
        "the partition-key scan lists the tenant"
    );
    let reload = restarted
        .load_all_stored_search_parameters()
        .await
        .expect("startup load");
    assert!(reload.tenants >= 1 && reload.definitions >= 1, "{reload:?}");
    // The tenant's first registry use, after the load and before any refresh.
    assert!(has_url(&restarted, &tenant, &url));
    assert!(!has_url(
        &restarted,
        &tenant,
        "http://example.org/SearchParameter/draft"
    ));
}

#[tokio::test]
async fn cassandra_refresh_picks_up_another_process_write() {
    let node = node_or_skip!("cassandra_refresh_picks_up_another_process_write");
    let restarted = backend(node).await;
    let writer = backend(node).await;
    // The tenant's first request builds its registry from the shared base,
    // and only then does another process store the SearchParameter.
    let tenant = unique_tenant("refresh-sp");
    let url = format!(
        "http://example.org/SearchParameter/{}",
        tenant.tenant_id().as_str()
    );
    assert!(!has_url(&restarted, &tenant, &url));
    writer
        .create(
            &tenant,
            "SearchParameter",
            search_parameter("activesp", &url, "active"),
            FhirVersion::default_enabled(),
        )
        .await
        .expect("create");
    assert!(
        !has_url(&restarted, &tenant, &url),
        "cached until a refresh"
    );
    let stored = restarted
        .refresh_stored_search_parameters()
        .await
        .expect("refresh");
    assert!(stored >= 1, "{stored}");
    assert!(has_url(&restarted, &tenant, &url));
}

/// A stored SearchParameter that changes patient-compartment membership
/// applies from a non-default tenant's first use after a restart. The REST
/// layer's patient-compartment check (`crates/rest/src/patient_compartment.rs`,
/// `PatientCompartment`) builds this same [`PatientCompartmentMatcher`] over
/// `search_param_registry(tenant)`, so a wrong registry here is a wrong 403
/// (or a wrong allow) there.
#[tokio::test]
async fn cassandra_a_stored_compartment_parameter_applies_after_a_restart() {
    let node = node_or_skip!("cassandra_a_stored_compartment_parameter_applies_after_a_restart");
    let version = FhirVersion::default_enabled();
    let writer = backend(node).await;
    let tenant = unique_tenant("stored-sp-compartment");
    // Shadows the spec's Observation `subject` (a Patient-compartment
    // parameter) with a new url whose expression reads `focus` instead.
    writer
        .create(
            &tenant,
            "SearchParameter",
            json!({
                "resourceType": "SearchParameter",
                "id": "obs-subject-focus",
                "url": format!(
                    "http://example.org/SearchParameter/{}-subject",
                    tenant.tenant_id().as_str()
                ),
                "name": "subject",
                "status": "active",
                "description": "test: Observation subject read from focus",
                "code": "subject",
                "base": ["Observation"],
                "type": "reference",
                "expression": "Observation.focus",
                "target": ["Patient"]
            }),
            version,
        )
        .await
        .expect("create");
    let observation = json!({
        "resourceType": "Observation",
        "status": "final",
        "code": {"text": "x"},
        "subject": {"reference": "Patient/subject-a"},
        "focus": [{"reference": "Patient/focus-b"}]
    });
    let member = |backend: &CassandraBackend, patient: &str| {
        let matcher = PatientCompartmentMatcher::new(
            backend.search_param_registry(&tenant),
            version,
            "Observation",
        );
        matcher.is_member(&observation, &HashSet::from([format!("Patient/{patient}")]))
    };
    assert!(member(&writer, "focus-b"), "the writer applies it at once");
    assert!(!member(&writer, "subject-a"));

    // This tenant's load only, which the startup load runs for every listed
    // tenant (see the test above), so other workers' tenants on a shared node
    // cannot change the result.
    let restarted = backend(node).await;
    restarted
        .load_stored_search_parameters(tenant.tenant_id().as_str())
        .await
        .expect("load");
    assert!(
        member(&restarted, "focus-b"),
        "after a restart, the tenant's first use sees the stored parameter"
    );
    assert!(!member(&restarted, "subject-a"));
}

/// Two stored SearchParameters with the same `url` and different ids: the one
/// with the newest `last_updated` is in force, for the process that stored
/// them and after a restart, however the reload's reads finish. The older one
/// has the higher id, so the id tie-break would pick it: only `last_updated`
/// decides here.
#[tokio::test]
async fn cassandra_the_newest_of_two_same_url_search_parameters_wins() {
    let node = node_or_skip!("cassandra_the_newest_of_two_same_url_search_parameters_wins");
    let writer = backend(node).await;
    let tenant = unique_tenant("stored-sp-dup-url");
    let url = format!(
        "http://example.org/SearchParameter/{}-dup",
        tenant.tenant_id().as_str()
    );
    for id in ["zzz-older", "aaa-newer"] {
        let mut resource = search_parameter(id, &url, "active");
        resource["code"] = json!(id);
        writer
            .create(
                &tenant,
                "SearchParameter",
                resource,
                FhirVersion::default_enabled(),
            )
            .await
            .unwrap_or_else(|e| panic!("create {id}: {e}"));
        // Distinct `last_updated` milliseconds.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let code = |backend: &CassandraBackend| {
        backend
            .search_param_registry(&tenant)
            .read()
            .get_by_url(&url)
            .map(|definition| definition.code.clone())
    };
    assert_eq!(code(&writer).as_deref(), Some("aaa-newer"), "the writer");

    for _ in 0..5 {
        let restarted = backend(node).await;
        restarted
            .load_stored_search_parameters(tenant.tenant_id().as_str())
            .await
            .expect("load");
        assert_eq!(
            code(&restarted).as_deref(),
            Some("aaa-newer"),
            "after a restart"
        );
    }
}
