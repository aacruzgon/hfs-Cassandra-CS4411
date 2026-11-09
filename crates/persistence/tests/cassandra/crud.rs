//! Roadmap Step 3 (rows 3.3 and 3.4): upstream's CRUD suite, `tests/crud/`,
//! as an adapted copy that runs on the Cassandra node (D2 of the Step 3
//! plan, recorded in its ADR).
//!
//! Upstream's `crud_suite.rs` is `#![cfg(feature = "sqlite")]`, and every test
//! in `tests/crud/` builds its own `SqliteBackend::in_memory()` under fixed
//! tenants (`test-tenant`, `tenant-1`, `my-tenant`, …) and counts from an
//! empty store. On the shared node those tenants would collide across tests
//! and across runs (ADR-aacruzgon-0020, rule 2), and ADR-aacruzgon-0038 left
//! to Step 3 how the suite reaches Cassandra. So this file holds one module
//! per upstream file, with the same test names and bodies, and these changes
//! only:
//!
//! - **the backend** is a [`CassandraBackend`](helios_persistence::backends::cassandra::CassandraBackend)
//!   on the test node ([`crate::backend_skeleton::backend`]), and a test skips
//!   when there is no node, as every Cassandra test does;
//! - **every tenant is fresh** ([`unique_tenant`](crate::cassandra_node::unique_tenant),
//!   [`unique_tenant_id`](crate::cassandra_node::unique_tenant_id)), so the
//!   absolute counts hold, and an assertion on a literal tenant id compares
//!   with the tenant the test made instead;
//! - **`test_read_system_tenant`** keeps [`TenantContext::system`](helios_persistence::tenant::TenantContext::system),
//!   which is one `__system__` partition for every test on the node, and gives
//!   its ValueSet an id of its own ([`unique_id`](crate::cassandra_node::unique_id),
//!   ADR-aacruzgon-0038);
//! - **the two tests upstream ignores for #306** run: SQLite has no
//!   `created_at` column, Cassandra does (`docs/Cassandra/schema/schema.cql`);
//! - **[`test_fhir_version`](crate::cassandra_node::test_fhir_version)** in
//!   place of upstream's `FhirVersion::default()`, which exists only with the
//!   `R4` feature, so the copy also compiles in the R4B-only check
//!   (`r4b-compile-check.yml`) and runs under the version
//!   `HFS_TEST_FHIR_VERSION` names (ADR-aacruzgon-0065). Unset, it is
//!   `FhirVersion::default_enabled()`, which is R4 when `R4` is on, so the
//!   tests run as upstream's do.
//!
//! Where an upstream assertion accepts two answers, the copy keeps it as it
//! is, for parity, and [`strict`] adds a `cassandra_strict_*` test that pins
//! Cassandra's one answer (the Step 3 matrix's T-4, T-9 and T-14).
//!
//! [`parity`] reads `tests/crud/` when it runs, so an upstream sync that adds,
//! renames or removes a CRUD test, or a whole file, fails it until this copy
//! follows.
//!
//! Every test here runs against the real Step 3 backend. The methods they
//! call are `update`, `delete` and `delete_versioned` (roadmap Step 3's write
//! path), `exists` and `read_batch` (Step 3, L2c), and `create_or_update`, as
//! well as Step 2's `create`, `read` and `count`.

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

pub mod create_tests {
    //! Tests for resource creation operations.
    //!
    //! This module tests the `create` and `create_or_update` methods of the
    //! ResourceStorage trait.

    use serde_json::json;

    use helios_persistence::core::ResourceStorage;
    use helios_persistence::error::StorageError;
    use helios_persistence::tenant::{TenantContext, TenantPermissions};

    use crate::cassandra_node::{unique_tenant, unique_tenant_id};

    // ============================================================================
    // Helper Functions
    // ============================================================================

    fn create_tenant() -> TenantContext {
        unique_tenant("crud")
    }

    fn create_patient_json(id: Option<&str>) -> serde_json::Value {
        let mut patient = json!({
            "resourceType": "Patient",
            "name": [{"family": "Smith", "given": ["John"]}],
            "active": true
        });
        if let Some(id) = id {
            patient["id"] = json!(id);
        }
        patient
    }

    // ============================================================================
    // Create Tests - Basic
    // ============================================================================

    /// Test that creating a new resource succeeds and returns the stored resource.
    #[tokio::test]
    async fn test_create_resource_success() {
        let backend = cassandra_backend!("test_create_resource_success");
        let tenant = create_tenant();
        let patient = create_patient_json(None);

        let result = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await;

        assert!(result.is_ok(), "Create should succeed");
        let stored = result.unwrap();
        assert_eq!(stored.resource_type(), "Patient");
        assert!(!stored.id().is_empty(), "ID should be assigned");
        assert_eq!(stored.version_id(), "1", "Initial version should be 1");
        assert!(!stored.is_deleted(), "Resource should not be deleted");
    }

    /// Test that the created resource has proper FHIR metadata.
    #[tokio::test]
    async fn test_create_resource_has_metadata() {
        let backend = cassandra_backend!("test_create_resource_has_metadata");
        let tenant = create_tenant();
        let patient = create_patient_json(None);

        let stored = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .expect("Create should succeed");

        // Check content has id and meta
        let content = stored.content();
        assert!(content.get("id").is_some(), "Content should have id");
        assert_eq!(
            content["resourceType"], "Patient",
            "resourceType should be preserved"
        );

        // Check ETag
        assert!(stored.etag().starts_with("W/\""), "ETag should be weak");

        // Check timestamps
        assert!(
            stored.created_at() <= chrono::Utc::now(),
            "created_at should be set"
        );
        assert!(
            stored.last_modified() <= chrono::Utc::now(),
            "last_modified should be set"
        );
    }

    /// Test that the tenant ID is properly associated with the created resource.
    #[tokio::test]
    async fn test_create_resource_tenant_association() {
        let backend = cassandra_backend!("test_create_resource_tenant_association");
        let tenant_id = unique_tenant_id("my-tenant");
        let tenant = TenantContext::new(tenant_id.clone(), TenantPermissions::full_access());
        let patient = create_patient_json(None);

        let stored = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .expect("Create should succeed");

        assert_eq!(stored.tenant_id(), &tenant_id, "Tenant ID should match");
    }

    /// Test creating resources of different types.
    #[tokio::test]
    async fn test_create_different_resource_types() {
        let backend = cassandra_backend!("test_create_different_resource_types");
        let tenant = create_tenant();

        // Create a Patient
        let patient = create_patient_json(None);
        let patient_stored = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        assert_eq!(patient_stored.resource_type(), "Patient");

        // Create an Observation
        let observation = json!({
            "resourceType": "Observation",
            "status": "final",
            "code": {
                "coding": [{"system": "http://loinc.org", "code": "8867-4"}]
            }
        });
        let obs_stored = backend
            .create(
                &tenant,
                "Observation",
                observation,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        assert_eq!(obs_stored.resource_type(), "Observation");

        // Create an Organization
        let organization = json!({
            "resourceType": "Organization",
            "name": "Test Hospital"
        });
        let org_stored = backend
            .create(
                &tenant,
                "Organization",
                organization,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        assert_eq!(org_stored.resource_type(), "Organization");
    }

    // ============================================================================
    // Create Tests - Error Cases
    // ============================================================================

    /// Test that creating without create permission fails.
    #[tokio::test]
    async fn test_create_without_permission_fails() {
        let backend = cassandra_backend!("test_create_without_permission_fails");
        let tenant = TenantContext::new(unique_tenant_id("test"), TenantPermissions::read_only());
        let patient = create_patient_json(None);

        let result = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await;

        assert!(result.is_err(), "Create without permission should fail");
        match result {
            Err(StorageError::Tenant(_)) => {}
            Err(e) => panic!("Expected TenantError, got {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    // ============================================================================
    // Create or Update Tests
    // ============================================================================

    /// Test that create_or_update creates a new resource when it doesn't exist.
    #[tokio::test]
    async fn test_create_or_update_creates_new() {
        let backend = cassandra_backend!("test_create_or_update_creates_new");
        let tenant = create_tenant();
        let patient = create_patient_json(Some("new-patient-123"));

        let (stored, created) = backend
            .create_or_update(
                &tenant,
                "Patient",
                "new-patient-123",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .expect("create_or_update should succeed");

        assert!(created, "Should indicate resource was created");
        assert_eq!(stored.id(), "new-patient-123");
        assert_eq!(stored.version_id(), "1");
    }

    /// Test that create_or_update updates an existing resource.
    #[tokio::test]
    async fn test_create_or_update_updates_existing() {
        let backend = cassandra_backend!("test_create_or_update_updates_existing");
        let tenant = create_tenant();

        // First create a resource
        let patient1 = create_patient_json(Some("patient-456"));
        let (stored1, created1) = backend
            .create_or_update(
                &tenant,
                "Patient",
                "patient-456",
                patient1,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        assert!(created1);
        assert_eq!(stored1.version_id(), "1");

        // Now update it
        let mut patient2 = create_patient_json(Some("patient-456"));
        patient2["active"] = json!(false);

        let (stored2, created2) = backend
            .create_or_update(
                &tenant,
                "Patient",
                "patient-456",
                patient2,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        assert!(
            !created2,
            "Should indicate resource was updated, not created"
        );
        assert_eq!(stored2.id(), "patient-456");
        assert_eq!(stored2.version_id(), "2", "Version should be incremented");
        assert_eq!(stored2.content()["active"], false);
    }

    /// Test that create_or_update preserves resource history.
    #[tokio::test]
    // Un-ignored on Cassandra, which stores `created_at` (`schema.cql`):
    // upstream ignores it for SQLite only (#306, PR #361).
    async fn test_create_or_update_preserves_created_timestamp() {
        let backend = cassandra_backend!("test_create_or_update_preserves_created_timestamp");
        let tenant = create_tenant();

        // Create initial resource
        let patient1 = create_patient_json(Some("patient-789"));
        let (stored1, _) = backend
            .create_or_update(
                &tenant,
                "Patient",
                "patient-789",
                patient1,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let created_at = stored1.created_at();

        // Wait a tiny bit
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Update it
        let patient2 = create_patient_json(Some("patient-789"));
        let (stored2, _) = backend
            .create_or_update(
                &tenant,
                "Patient",
                "patient-789",
                patient2,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        assert_eq!(
            stored2.created_at(),
            created_at,
            "created_at should be preserved"
        );
        assert!(
            stored2.last_modified() >= created_at,
            "last_modified should be updated"
        );
    }

    // ============================================================================
    // Create Tests - Multiple Resources
    // ============================================================================

    /// Test creating multiple resources of the same type.
    #[tokio::test]
    async fn test_create_multiple_resources() {
        let backend = cassandra_backend!("test_create_multiple_resources");
        let tenant = create_tenant();

        let mut ids = Vec::new();
        for i in 0..10 {
            let patient = json!({
                "resourceType": "Patient",
                "name": [{"family": format!("Patient{}", i)}]
            });
            let stored = backend
                .create(
                    &tenant,
                    "Patient",
                    patient,
                    crate::cassandra_node::test_fhir_version(),
                )
                .await
                .unwrap();
            ids.push(stored.id().to_string());
        }

        // Verify all IDs are unique
        let unique_ids: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(ids.len(), unique_ids.len(), "All IDs should be unique");
    }

    /// Test that resources in different tenants are isolated.
    #[tokio::test]
    async fn test_create_tenant_isolation() {
        let backend = cassandra_backend!("test_create_tenant_isolation");

        let tenant1 = TenantContext::new(
            unique_tenant_id("tenant-1"),
            TenantPermissions::full_access(),
        );
        let tenant2 = TenantContext::new(
            unique_tenant_id("tenant-2"),
            TenantPermissions::full_access(),
        );

        // Create same patient in both tenants
        let patient = create_patient_json(None);

        let stored1 = backend
            .create(
                &tenant1,
                "Patient",
                patient.clone(),
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let stored2 = backend
            .create(
                &tenant2,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Verify different tenant IDs
        assert_eq!(stored1.tenant_id(), tenant1.tenant_id());
        assert_eq!(stored2.tenant_id(), tenant2.tenant_id());

        // Each should have its own count
        let count1 = backend.count(&tenant1, Some("Patient")).await.unwrap();
        let count2 = backend.count(&tenant2, Some("Patient")).await.unwrap();
        assert_eq!(count1, 1);
        assert_eq!(count2, 1);
    }

    // ============================================================================
    // Create Tests - Content Validation
    // ============================================================================

    /// Test that resource content is preserved correctly.
    #[tokio::test]
    async fn test_create_preserves_content() {
        let backend = cassandra_backend!("test_create_preserves_content");
        let tenant = create_tenant();

        let patient = json!({
            "resourceType": "Patient",
            "name": [{
                "family": "Smith",
                "given": ["John", "Jacob"],
                "prefix": ["Mr."]
            }],
            "birthDate": "1980-01-15",
            "gender": "male",
            "active": true,
            "address": [{
                "line": ["123 Main St"],
                "city": "Boston",
                "state": "MA",
                "postalCode": "02101"
            }],
            "identifier": [{
                "system": "http://example.org/mrn",
                "value": "MRN12345"
            }]
        });

        let stored = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let content = stored.content();

        assert_eq!(content["name"][0]["family"], "Smith");
        assert_eq!(content["name"][0]["given"][0], "John");
        assert_eq!(content["name"][0]["given"][1], "Jacob");
        assert_eq!(content["birthDate"], "1980-01-15");
        assert_eq!(content["gender"], "male");
        assert_eq!(content["address"][0]["city"], "Boston");
        assert_eq!(content["identifier"][0]["value"], "MRN12345");
    }

    /// Test that nested objects are preserved.
    #[tokio::test]
    async fn test_create_preserves_nested_objects() {
        let backend = cassandra_backend!("test_create_preserves_nested_objects");
        let tenant = create_tenant();

        let observation = json!({
            "resourceType": "Observation",
            "status": "final",
            "code": {
                "coding": [{
                    "system": "http://loinc.org",
                    "code": "8867-4",
                    "display": "Heart rate"
                }],
                "text": "Heart rate"
            },
            "subject": {
                "reference": "Patient/123"
            },
            "valueQuantity": {
                "value": 72,
                "unit": "beats/minute",
                "system": "http://unitsofmeasure.org",
                "code": "/min"
            },
            "component": [{
                "code": {
                    "coding": [{
                        "system": "http://loinc.org",
                        "code": "8867-4"
                    }]
                },
                "valueQuantity": {
                    "value": 72
                }
            }]
        });

        let stored = backend
            .create(
                &tenant,
                "Observation",
                observation,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let content = stored.content();

        assert_eq!(content["code"]["coding"][0]["code"], "8867-4");
        assert_eq!(content["valueQuantity"]["value"], 72);
        assert_eq!(content["component"][0]["valueQuantity"]["value"], 72);
    }

    // ============================================================================
    // Create Tests - Special Cases
    // ============================================================================

    /// Test creating a resource with an empty array field.
    #[tokio::test]
    async fn test_create_with_empty_arrays() {
        let backend = cassandra_backend!("test_create_with_empty_arrays");
        let tenant = create_tenant();

        let patient = json!({
            "resourceType": "Patient",
            "name": [],
            "identifier": []
        });

        let stored = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let content = stored.content();

        assert!(content["name"].as_array().unwrap().is_empty());
        assert!(content["identifier"].as_array().unwrap().is_empty());
    }

    /// Test creating a resource with null fields.
    #[tokio::test]
    async fn test_create_with_null_fields() {
        let backend = cassandra_backend!("test_create_with_null_fields");
        let tenant = create_tenant();

        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": "Smith"}],
            "birthDate": null
        });

        let stored = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let content = stored.content();

        // Null fields may or may not be preserved depending on implementation
        assert_eq!(content["name"][0]["family"], "Smith");
    }

    /// Test creating resources with Unicode content.
    #[tokio::test]
    async fn test_create_with_unicode() {
        let backend = cassandra_backend!("test_create_with_unicode");
        let tenant = create_tenant();

        let patient = json!({
            "resourceType": "Patient",
            "name": [{
                "family": "日本語",
                "given": ["太郎"]
            }],
            "address": [{
                "city": "東京都"
            }]
        });

        let stored = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let content = stored.content();

        assert_eq!(content["name"][0]["family"], "日本語");
        assert_eq!(content["name"][0]["given"][0], "太郎");
        assert_eq!(content["address"][0]["city"], "東京都");
    }

    /// Test creating a resource with large content.
    #[tokio::test]
    async fn test_create_with_large_content() {
        let backend = cassandra_backend!("test_create_with_large_content");
        let tenant = create_tenant();

        // Create a patient with many names
        let mut names = Vec::new();
        for i in 0..100 {
            names.push(json!({
                "family": format!("Family{}", i),
                "given": [format!("Given{}", i)]
            }));
        }

        let patient = json!({
            "resourceType": "Patient",
            "name": names
        });

        let stored = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let content = stored.content();

        assert_eq!(content["name"].as_array().unwrap().len(), 100);
    }
}

pub mod read_tests {
    //! Tests for resource read operations.
    //!
    //! This module tests the `read`, `exists`, `read_batch`, and `count` methods
    //! of the ResourceStorage trait.

    use serde_json::json;

    use helios_persistence::core::ResourceStorage;
    use helios_persistence::error::{ResourceError, StorageError};
    use helios_persistence::tenant::{TenantContext, TenantPermissions};

    use crate::cassandra_node::{unique_id, unique_tenant, unique_tenant_id};

    // ============================================================================
    // Helper Functions
    // ============================================================================

    fn create_tenant() -> TenantContext {
        unique_tenant("crud")
    }

    fn create_patient_json(name: &str) -> serde_json::Value {
        json!({
            "resourceType": "Patient",
            "name": [{"family": name}],
            "active": true
        })
    }

    // ============================================================================
    // Read Tests - Basic
    // ============================================================================

    /// Test reading a resource that exists.
    #[tokio::test]
    async fn test_read_existing_resource() {
        let backend = cassandra_backend!("test_read_existing_resource");
        let tenant = create_tenant();

        // Create a resource first
        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Read it back
        let result = backend
            .read(&tenant, "Patient", created.id())
            .await
            .unwrap();

        assert!(result.is_some());
        let read = result.unwrap();
        assert_eq!(read.id(), created.id());
        assert_eq!(read.version_id(), created.version_id());
        assert_eq!(read.resource_type(), "Patient");
    }

    /// Test that reading returns the correct content.
    #[tokio::test]
    async fn test_read_returns_correct_content() {
        let backend = cassandra_backend!("test_read_returns_correct_content");
        let tenant = create_tenant();

        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": "Smith", "given": ["John"]}],
            "birthDate": "1980-01-15"
        });
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        let read = backend
            .read(&tenant, "Patient", created.id())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read.content()["name"][0]["family"], "Smith");
        assert_eq!(read.content()["name"][0]["given"][0], "John");
        assert_eq!(read.content()["birthDate"], "1980-01-15");
    }

    /// Test reading a resource that doesn't exist.
    #[tokio::test]
    async fn test_read_nonexistent_resource() {
        let backend = cassandra_backend!("test_read_nonexistent_resource");
        let tenant = create_tenant();

        let result = backend
            .read(&tenant, "Patient", "nonexistent-id")
            .await
            .unwrap();

        assert!(
            result.is_none(),
            "Reading nonexistent resource should return None"
        );
    }

    /// Test reading returns correct metadata.
    #[tokio::test]
    async fn test_read_returns_metadata() {
        let backend = cassandra_backend!("test_read_returns_metadata");
        let tenant = create_tenant();

        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        let read = backend
            .read(&tenant, "Patient", created.id())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read.etag(), created.etag());
        assert_eq!(read.created_at(), created.created_at());
        assert_eq!(read.last_modified(), created.last_modified());
        assert_eq!(read.tenant_id(), created.tenant_id());
    }

    // ============================================================================
    // Read Tests - Tenant Isolation
    // ============================================================================

    /// Test that reading only returns resources from the correct tenant.
    #[tokio::test]
    async fn test_read_tenant_isolation() {
        let backend = cassandra_backend!("test_read_tenant_isolation");

        let tenant1 = TenantContext::new(
            unique_tenant_id("tenant-1"),
            TenantPermissions::full_access(),
        );
        let tenant2 = TenantContext::new(
            unique_tenant_id("tenant-2"),
            TenantPermissions::full_access(),
        );

        // Create resource in tenant1
        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant1,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Try to read from tenant2 - should not find it
        let result = backend
            .read(&tenant2, "Patient", created.id())
            .await
            .unwrap();

        assert!(
            result.is_none(),
            "Should not be able to read another tenant's resource"
        );
    }

    /// Test that system tenant can be used for shared resources.
    #[tokio::test]
    async fn test_read_system_tenant() {
        let backend = cassandra_backend!("test_read_system_tenant");
        let system = TenantContext::system();

        // Create a shared resource in system tenant. `__system__` is one
        // partition for every test on the node, so the resource takes an id of
        // its own (ADR-aacruzgon-0038); `create` keeps a client id.
        let value_set = json!({
            "resourceType": "ValueSet",
            "id": unique_id("crud-system-valueset"),
            "name": "TestValueSet"
        });
        let created = backend
            .create(
                &system,
                "ValueSet",
                value_set,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Read from system tenant
        let read = backend
            .read(&system, "ValueSet", created.id())
            .await
            .unwrap();

        assert!(read.is_some());
        assert!(read.unwrap().tenant_id().is_system());
    }

    // ============================================================================
    // Exists Tests
    // ============================================================================

    /// Test exists returns true for existing resources.
    #[tokio::test]
    async fn test_exists_returns_true() {
        let backend = cassandra_backend!("test_exists_returns_true");
        let tenant = create_tenant();

        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        let exists = backend
            .exists(&tenant, "Patient", created.id())
            .await
            .unwrap();

        assert!(exists, "exists should return true for existing resource");
    }

    /// Test exists returns false for nonexistent resources.
    #[tokio::test]
    async fn test_exists_returns_false() {
        let backend = cassandra_backend!("test_exists_returns_false");
        let tenant = create_tenant();

        let exists = backend
            .exists(&tenant, "Patient", "nonexistent-id")
            .await
            .unwrap();

        assert!(
            !exists,
            "exists should return false for nonexistent resource"
        );
    }

    /// Test exists respects tenant isolation.
    #[tokio::test]
    async fn test_exists_tenant_isolation() {
        let backend = cassandra_backend!("test_exists_tenant_isolation");

        let tenant1 = TenantContext::new(
            unique_tenant_id("tenant-1"),
            TenantPermissions::full_access(),
        );
        let tenant2 = TenantContext::new(
            unique_tenant_id("tenant-2"),
            TenantPermissions::full_access(),
        );

        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant1,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Check from different tenant
        let exists = backend
            .exists(&tenant2, "Patient", created.id())
            .await
            .unwrap();

        assert!(
            !exists,
            "exists should return false for other tenant's resource"
        );
    }

    // ============================================================================
    // Read Batch Tests
    // ============================================================================

    /// Test reading multiple resources at once.
    #[tokio::test]
    async fn test_read_batch_success() {
        let backend = cassandra_backend!("test_read_batch_success");
        let tenant = create_tenant();

        // Create multiple resources
        let mut ids = Vec::new();
        for i in 0..5 {
            let patient = create_patient_json(&format!("Patient{}", i));
            let created = backend
                .create(
                    &tenant,
                    "Patient",
                    patient,
                    crate::cassandra_node::test_fhir_version(),
                )
                .await
                .unwrap();
            ids.push(created.id().to_string());
        }

        // Read them all
        let id_refs: Vec<&str> = ids.iter().map(|s| s.as_str()).collect();
        let results = backend
            .read_batch(&tenant, "Patient", &id_refs)
            .await
            .unwrap();

        assert_eq!(results.len(), 5, "Should return all 5 resources");
    }

    /// Test read_batch returns only existing resources.
    #[tokio::test]
    async fn test_read_batch_partial() {
        let backend = cassandra_backend!("test_read_batch_partial");
        let tenant = create_tenant();

        // Create 2 resources
        let patient1 = create_patient_json("Smith");
        let created1 = backend
            .create(
                &tenant,
                "Patient",
                patient1,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let patient2 = create_patient_json("Jones");
        let created2 = backend
            .create(
                &tenant,
                "Patient",
                patient2,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Read with some nonexistent IDs
        let ids = vec![
            created1.id(),
            "nonexistent-1",
            created2.id(),
            "nonexistent-2",
        ];
        let results = backend.read_batch(&tenant, "Patient", &ids).await.unwrap();

        assert_eq!(results.len(), 2, "Should return only existing resources");
    }

    /// A soft-deleted (Gone) id in a batch is skipped, not fatal: its live
    /// siblings of the same type must still be returned (#1119).
    #[tokio::test]
    async fn test_read_batch_skips_soft_deleted() {
        let backend = cassandra_backend!("test_read_batch_skips_soft_deleted");
        let tenant = create_tenant();

        let created1 = backend
            .create(
                &tenant,
                "Patient",
                create_patient_json("Live"),
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let created2 = backend
            .create(
                &tenant,
                "Patient",
                create_patient_json("Doomed"),
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Soft-delete the second so a bare read of it returns Gone.
        backend
            .delete(&tenant, "Patient", created2.id())
            .await
            .unwrap();

        let ids = vec![created1.id(), created2.id()];
        let results = backend.read_batch(&tenant, "Patient", &ids).await.unwrap();

        assert_eq!(
            results.len(),
            1,
            "the deleted id is skipped, the live sibling still returned"
        );
        assert_eq!(results[0].id(), created1.id());
    }

    /// Test read_batch with empty list.
    #[tokio::test]
    async fn test_read_batch_empty() {
        let backend = cassandra_backend!("test_read_batch_empty");
        let tenant = create_tenant();

        let results: Vec<helios_persistence::types::StoredResource> =
            backend.read_batch(&tenant, "Patient", &[]).await.unwrap();

        assert!(
            results.is_empty(),
            "Empty input should return empty results"
        );
    }

    /// Test read_batch respects tenant isolation.
    #[tokio::test]
    async fn test_read_batch_tenant_isolation() {
        let backend = cassandra_backend!("test_read_batch_tenant_isolation");

        let tenant1 = TenantContext::new(
            unique_tenant_id("tenant-1"),
            TenantPermissions::full_access(),
        );
        let tenant2 = TenantContext::new(
            unique_tenant_id("tenant-2"),
            TenantPermissions::full_access(),
        );

        // Create resources in tenant1
        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant1,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Try to read from tenant2
        let results = backend
            .read_batch(&tenant2, "Patient", &[created.id()])
            .await
            .unwrap();

        assert!(
            results.is_empty(),
            "Should not find other tenant's resources"
        );
    }

    // ============================================================================
    // Count Tests
    // ============================================================================

    /// Test counting resources by type.
    #[tokio::test]
    async fn test_count_by_type() {
        let backend = cassandra_backend!("test_count_by_type");
        let tenant = create_tenant();

        // Create some patients
        for i in 0..3 {
            let patient = create_patient_json(&format!("Patient{}", i));
            backend
                .create(
                    &tenant,
                    "Patient",
                    patient,
                    crate::cassandra_node::test_fhir_version(),
                )
                .await
                .unwrap();
        }

        // Create some observations
        for i in 0..2 {
            let obs = json!({
                "resourceType": "Observation",
                "status": "final",
                "code": {"coding": [{"code": format!("code-{}", i)}]}
            });
            backend
                .create(
                    &tenant,
                    "Observation",
                    obs,
                    crate::cassandra_node::test_fhir_version(),
                )
                .await
                .unwrap();
        }

        let patient_count = backend.count(&tenant, Some("Patient")).await.unwrap();
        let obs_count = backend.count(&tenant, Some("Observation")).await.unwrap();

        assert_eq!(patient_count, 3);
        assert_eq!(obs_count, 2);
    }

    /// Test counting all resources.
    #[tokio::test]
    async fn test_count_all() {
        let backend = cassandra_backend!("test_count_all");
        let tenant = create_tenant();

        // Create mixed resources
        for i in 0..3 {
            let patient = create_patient_json(&format!("Patient{}", i));
            backend
                .create(
                    &tenant,
                    "Patient",
                    patient,
                    crate::cassandra_node::test_fhir_version(),
                )
                .await
                .unwrap();
        }
        for i in 0..2 {
            let obs = json!({
                "resourceType": "Observation",
                "status": "final",
                "code": {"coding": [{"code": format!("code-{}", i)}]}
            });
            backend
                .create(
                    &tenant,
                    "Observation",
                    obs,
                    crate::cassandra_node::test_fhir_version(),
                )
                .await
                .unwrap();
        }

        let total = backend.count(&tenant, None).await.unwrap();
        assert_eq!(total, 5);
    }

    /// Test count returns zero for empty storage.
    #[tokio::test]
    async fn test_count_empty() {
        let backend = cassandra_backend!("test_count_empty");
        let tenant = create_tenant();

        let count = backend.count(&tenant, Some("Patient")).await.unwrap();
        assert_eq!(count, 0);
    }

    /// Test count respects tenant isolation.
    #[tokio::test]
    async fn test_count_tenant_isolation() {
        let backend = cassandra_backend!("test_count_tenant_isolation");

        let tenant1 = TenantContext::new(
            unique_tenant_id("tenant-1"),
            TenantPermissions::full_access(),
        );
        let tenant2 = TenantContext::new(
            unique_tenant_id("tenant-2"),
            TenantPermissions::full_access(),
        );

        // Create resources in tenant1
        for i in 0..5 {
            let patient = create_patient_json(&format!("Patient{}", i));
            backend
                .create(
                    &tenant1,
                    "Patient",
                    patient,
                    crate::cassandra_node::test_fhir_version(),
                )
                .await
                .unwrap();
        }

        // Create resources in tenant2
        for i in 0..3 {
            let patient = create_patient_json(&format!("Patient{}", i));
            backend
                .create(
                    &tenant2,
                    "Patient",
                    patient,
                    crate::cassandra_node::test_fhir_version(),
                )
                .await
                .unwrap();
        }

        let count1 = backend.count(&tenant1, Some("Patient")).await.unwrap();
        let count2 = backend.count(&tenant2, Some("Patient")).await.unwrap();

        assert_eq!(count1, 5);
        assert_eq!(count2, 3);
    }

    // ============================================================================
    // Read Tests - After Update
    // ============================================================================

    /// Test that read returns the latest version after update.
    #[tokio::test]
    async fn test_read_after_update() {
        let backend = cassandra_backend!("test_read_after_update");
        let tenant = create_tenant();

        // Create resource
        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Update it
        let mut updated_content = created.content().clone();
        updated_content["name"][0]["family"] = json!("Jones");
        let updated = backend
            .update(&tenant, &created, updated_content)
            .await
            .unwrap();

        // Read it
        let read = backend
            .read(&tenant, "Patient", created.id())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(read.version_id(), "2");
        assert_eq!(read.content()["name"][0]["family"], "Jones");
        assert_eq!(read.version_id(), updated.version_id());
    }

    // ============================================================================
    // Read Tests - After Delete
    // ============================================================================

    /// Test that read returns None after delete.
    #[tokio::test]
    async fn test_read_after_delete() {
        let backend = cassandra_backend!("test_read_after_delete");
        let tenant = create_tenant();

        // Create resource
        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Delete it
        backend
            .delete(&tenant, "Patient", created.id())
            .await
            .unwrap();

        // Soft delete: `read` surfaces a deleted resource as `Gone` (SQLite/PG/S3)
        // or as `None`; either satisfies "not readable". Mirrors sqlite_tests.rs.
        match backend.read(&tenant, "Patient", created.id()).await {
            Ok(None) => {}
            Err(StorageError::Resource(ResourceError::Gone { .. })) => {}
            other => panic!(
                "Read after delete should not return a resource, got: {:?}",
                other
            ),
        }
    }

    /// Test that exists returns false after delete.
    #[tokio::test]
    async fn test_exists_after_delete() {
        let backend = cassandra_backend!("test_exists_after_delete");
        let tenant = create_tenant();

        // Create resource
        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Delete it
        backend
            .delete(&tenant, "Patient", created.id())
            .await
            .unwrap();

        // Check existence
        let exists = backend
            .exists(&tenant, "Patient", created.id())
            .await
            .unwrap();

        assert!(!exists, "exists after delete should return false");
    }

    /// Test that count excludes deleted resources.
    #[tokio::test]
    async fn test_count_excludes_deleted() {
        let backend = cassandra_backend!("test_count_excludes_deleted");
        let tenant = create_tenant();

        // Create 3 resources
        let mut ids = Vec::new();
        for i in 0..3 {
            let patient = create_patient_json(&format!("Patient{}", i));
            let created = backend
                .create(
                    &tenant,
                    "Patient",
                    patient,
                    crate::cassandra_node::test_fhir_version(),
                )
                .await
                .unwrap();
            ids.push(created.id().to_string());
        }

        // Initial count
        let initial_count = backend.count(&tenant, Some("Patient")).await.unwrap();
        assert_eq!(initial_count, 3);

        // Delete one
        backend.delete(&tenant, "Patient", &ids[0]).await.unwrap();

        // Count should decrease
        let final_count = backend.count(&tenant, Some("Patient")).await.unwrap();
        assert_eq!(final_count, 2);
    }

    // ============================================================================
    // Read Tests - Permissions
    // ============================================================================

    /// Test that read with read-only permissions succeeds.
    #[tokio::test]
    async fn test_read_with_read_only_permissions() {
        let backend = cassandra_backend!("test_read_with_read_only_permissions");
        let tenant_id = unique_tenant_id("test-tenant");
        let full_access = TenantContext::new(tenant_id.clone(), TenantPermissions::full_access());
        let read_only = TenantContext::new(tenant_id, TenantPermissions::read_only());

        // Create resource with full access
        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &full_access,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Read with read-only permissions
        let read = backend
            .read(&read_only, "Patient", created.id())
            .await
            .unwrap();

        assert!(
            read.is_some(),
            "Should be able to read with read-only permissions"
        );
    }
}

pub mod update_tests {
    //! Tests for resource update operations.
    //!
    //! This module tests the `update` method of the ResourceStorage trait
    //! and related versioning behavior.

    use serde_json::json;

    use helios_persistence::core::ResourceStorage;
    use helios_persistence::error::{ConcurrencyError, ResourceError, StorageError};
    use helios_persistence::tenant::{TenantContext, TenantPermissions};

    use crate::cassandra_node::{unique_tenant, unique_tenant_id};

    // ============================================================================
    // Helper Functions
    // ============================================================================

    fn create_tenant() -> TenantContext {
        unique_tenant("crud")
    }

    fn create_patient_json(name: &str) -> serde_json::Value {
        json!({
            "resourceType": "Patient",
            "name": [{"family": name}],
            "active": true
        })
    }

    // ============================================================================
    // Update Tests - Basic
    // ============================================================================

    /// Test that updating a resource succeeds and returns the updated resource.
    #[tokio::test]
    async fn test_update_resource_success() {
        let backend = cassandra_backend!("test_update_resource_success");
        let tenant = create_tenant();

        // Create a resource
        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Update it
        let mut updated_content = created.content().clone();
        updated_content["name"][0]["family"] = json!("Jones");

        let updated = backend
            .update(&tenant, &created, updated_content)
            .await
            .unwrap();

        assert_eq!(updated.id(), created.id(), "ID should remain the same");
        assert_eq!(updated.version_id(), "2", "Version should be incremented");
        assert_eq!(updated.content()["name"][0]["family"], "Jones");
    }

    /// Test that update increments version correctly.
    #[tokio::test]
    async fn test_update_increments_version() {
        let backend = cassandra_backend!("test_update_increments_version");
        let tenant = create_tenant();

        let patient = create_patient_json("Smith");
        let v1 = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        assert_eq!(v1.version_id(), "1");

        let v2 = backend
            .update(&tenant, &v1, v1.content().clone())
            .await
            .unwrap();
        assert_eq!(v2.version_id(), "2");

        let v3 = backend
            .update(&tenant, &v2, v2.content().clone())
            .await
            .unwrap();
        assert_eq!(v3.version_id(), "3");

        let v4 = backend
            .update(&tenant, &v3, v3.content().clone())
            .await
            .unwrap();
        assert_eq!(v4.version_id(), "4");
    }

    /// Test that update updates the last_modified timestamp.
    #[tokio::test]
    // Un-ignored on Cassandra, which stores `created_at` (`schema.cql`):
    // upstream ignores it for SQLite only (#306, PR #361).
    async fn test_update_updates_timestamp() {
        let backend = cassandra_backend!("test_update_updates_timestamp");
        let tenant = create_tenant();

        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let created_time = created.last_modified();

        // Small delay to ensure timestamp difference
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let updated = backend
            .update(&tenant, &created, created.content().clone())
            .await
            .unwrap();

        assert!(
            updated.last_modified() >= created_time,
            "last_modified should be updated"
        );
        assert_eq!(
            updated.created_at(),
            created.created_at(),
            "created_at should be preserved"
        );
    }

    /// Test that update generates new ETag.
    #[tokio::test]
    async fn test_update_generates_new_etag() {
        let backend = cassandra_backend!("test_update_generates_new_etag");
        let tenant = create_tenant();

        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        let updated = backend
            .update(&tenant, &created, created.content().clone())
            .await
            .unwrap();

        assert_ne!(
            updated.etag(),
            created.etag(),
            "ETag should change after update"
        );
        assert!(
            updated.etag().contains("2"),
            "ETag should contain version 2"
        );
    }

    // ============================================================================
    // Update Tests - Content Changes
    // ============================================================================

    /// Test updating multiple fields at once.
    #[tokio::test]
    async fn test_update_multiple_fields() {
        let backend = cassandra_backend!("test_update_multiple_fields");
        let tenant = create_tenant();

        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": "Smith", "given": ["John"]}],
            "active": true,
            "gender": "male"
        });
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        let mut updated_content = created.content().clone();
        updated_content["name"][0]["family"] = json!("Jones");
        updated_content["active"] = json!(false);
        updated_content["gender"] = json!("unknown");

        let updated = backend
            .update(&tenant, &created, updated_content)
            .await
            .unwrap();

        assert_eq!(updated.content()["name"][0]["family"], "Jones");
        assert_eq!(updated.content()["active"], false);
        assert_eq!(updated.content()["gender"], "unknown");
    }

    /// Test adding new fields during update.
    #[tokio::test]
    async fn test_update_add_new_fields() {
        let backend = cassandra_backend!("test_update_add_new_fields");
        let tenant = create_tenant();

        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": "Smith"}]
        });
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        let mut updated_content = created.content().clone();
        updated_content["birthDate"] = json!("1980-01-15");
        updated_content["gender"] = json!("male");
        updated_content["address"] = json!([{"city": "Boston"}]);

        let updated = backend
            .update(&tenant, &created, updated_content)
            .await
            .unwrap();

        assert_eq!(updated.content()["birthDate"], "1980-01-15");
        assert_eq!(updated.content()["gender"], "male");
        assert_eq!(updated.content()["address"][0]["city"], "Boston");
    }

    /// Test removing fields during update.
    #[tokio::test]
    async fn test_update_remove_fields() {
        let backend = cassandra_backend!("test_update_remove_fields");
        let tenant = create_tenant();

        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": "Smith"}],
            "birthDate": "1980-01-15",
            "gender": "male"
        });
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Create content without birthDate and gender
        let updated_content = json!({
            "resourceType": "Patient",
            "name": [{"family": "Smith"}]
        });

        let updated = backend
            .update(&tenant, &created, updated_content)
            .await
            .unwrap();

        assert!(
            updated.content().get("birthDate").is_none()
                || updated.content()["birthDate"].is_null(),
            "birthDate should be removed or null"
        );
    }

    /// Test updating nested objects.
    #[tokio::test]
    async fn test_update_nested_objects() {
        let backend = cassandra_backend!("test_update_nested_objects");
        let tenant = create_tenant();

        let observation = json!({
            "resourceType": "Observation",
            "status": "preliminary",
            "code": {
                "coding": [{
                    "system": "http://loinc.org",
                    "code": "8867-4",
                    "display": "Heart rate"
                }]
            },
            "valueQuantity": {
                "value": 70,
                "unit": "bpm"
            }
        });
        let created = backend
            .create(
                &tenant,
                "Observation",
                observation,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        let mut updated_content = created.content().clone();
        updated_content["status"] = json!("final");
        updated_content["valueQuantity"]["value"] = json!(75);

        let updated = backend
            .update(&tenant, &created, updated_content)
            .await
            .unwrap();

        assert_eq!(updated.content()["status"], "final");
        assert_eq!(updated.content()["valueQuantity"]["value"], 75);
    }

    // ============================================================================
    // Update Tests - Error Cases
    // ============================================================================

    /// Test that updating a nonexistent resource fails.
    #[tokio::test]
    async fn test_update_nonexistent_fails() {
        let backend = cassandra_backend!("test_update_nonexistent_fails");
        let tenant = create_tenant();

        // Create a fake stored resource
        let fake_resource = helios_persistence::types::StoredResource::new(
            "Patient",
            "nonexistent-id",
            tenant.tenant_id().clone(),
            json!({"resourceType": "Patient"}),
            crate::cassandra_node::test_fhir_version(),
        );

        let result = backend
            .update(&tenant, &fake_resource, json!({"resourceType": "Patient"}))
            .await;

        assert!(result.is_err());
        match result {
            Err(StorageError::Resource(ResourceError::NotFound { .. })) => {}
            Err(e) => panic!("Expected NotFound error, got {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    /// Test that updating without permission fails.
    #[tokio::test]
    async fn test_update_without_permission_fails() {
        let backend = cassandra_backend!("test_update_without_permission_fails");
        let tenant_id = unique_tenant_id("test-tenant");
        let full_access = TenantContext::new(tenant_id.clone(), TenantPermissions::full_access());
        let read_only = TenantContext::new(tenant_id, TenantPermissions::read_only());

        // Create with full access
        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &full_access,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Try to update with read-only
        let result = backend
            .update(&read_only, &created, created.content().clone())
            .await;

        assert!(result.is_err());
        match result {
            Err(StorageError::Tenant(_)) => {}
            Err(e) => panic!("Expected TenantError, got {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    /// Test that updating a deleted resource fails.
    #[tokio::test]
    async fn test_update_deleted_fails() {
        let backend = cassandra_backend!("test_update_deleted_fails");
        let tenant = create_tenant();

        // Create and delete a resource
        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        backend
            .delete(&tenant, "Patient", created.id())
            .await
            .unwrap();

        // Try to update the deleted resource
        let result = backend
            .update(&tenant, &created, created.content().clone())
            .await;

        assert!(result.is_err());
        // Could be NotFound or Gone depending on implementation
        match result {
            Err(StorageError::Resource(ResourceError::NotFound { .. }))
            | Err(StorageError::Resource(ResourceError::Gone { .. })) => {}
            Err(e) => panic!("Expected NotFound or Gone error, got {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    /// Test updating from wrong tenant fails.
    #[tokio::test]
    async fn test_update_wrong_tenant_fails() {
        let backend = cassandra_backend!("test_update_wrong_tenant_fails");

        let tenant1 = TenantContext::new(
            unique_tenant_id("tenant-1"),
            TenantPermissions::full_access(),
        );
        let tenant2 = TenantContext::new(
            unique_tenant_id("tenant-2"),
            TenantPermissions::full_access(),
        );

        // Create in tenant1
        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant1,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Try to update from tenant2
        let result = backend
            .update(&tenant2, &created, created.content().clone())
            .await;

        assert!(result.is_err());
    }

    // ============================================================================
    // Update Tests - Version Conflicts
    // ============================================================================

    /// Test that concurrent updates with stale version fail (optimistic locking).
    #[tokio::test]
    async fn test_update_version_conflict() {
        let backend = cassandra_backend!("test_update_version_conflict");
        let tenant = create_tenant();

        // Create a resource
        let patient = create_patient_json("Smith");
        let v1 = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Do a legitimate update
        let mut content2 = v1.content().clone();
        content2["name"][0]["family"] = json!("Jones");
        let _v2 = backend.update(&tenant, &v1, content2).await.unwrap();

        // Try to update again using stale v1
        let mut content3 = v1.content().clone();
        content3["name"][0]["family"] = json!("Williams");
        let result = backend.update(&tenant, &v1, content3).await;

        // Should fail due to version conflict
        assert!(result.is_err());
        match result {
            Err(StorageError::Concurrency(ConcurrencyError::VersionConflict { .. })) => {}
            Err(StorageError::Resource(ResourceError::NotFound { .. })) => {
                // Some implementations may return NotFound for stale versions
            }
            Err(e) => panic!("Expected VersionConflict or NotFound error, got {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    // ============================================================================
    // Update Tests - Sequential Updates
    // ============================================================================

    /// Test that sequential updates work correctly.
    #[tokio::test]
    async fn test_sequential_updates() {
        let backend = cassandra_backend!("test_sequential_updates");
        let tenant = create_tenant();

        let patient = create_patient_json("Name0");
        let mut current = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        for i in 1..=10 {
            let mut content = current.content().clone();
            content["name"][0]["family"] = json!(format!("Name{}", i));
            current = backend.update(&tenant, &current, content).await.unwrap();
            assert_eq!(current.version_id(), (i + 1).to_string());
        }

        // Verify final state
        let read = backend
            .read(&tenant, "Patient", current.id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read.version_id(), "11");
        assert_eq!(read.content()["name"][0]["family"], "Name10");
    }

    // ============================================================================
    // Update Tests - Idempotency
    // ============================================================================

    /// Test that updating with same content still increments version.
    #[tokio::test]
    async fn test_update_same_content_increments_version() {
        let backend = cassandra_backend!("test_update_same_content_increments_version");
        let tenant = create_tenant();

        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Update with identical content
        let updated = backend
            .update(&tenant, &created, created.content().clone())
            .await
            .unwrap();

        // Version should still increment (this is standard FHIR behavior)
        assert_eq!(updated.version_id(), "2");
    }

    // ============================================================================
    // Update Tests - Special Content
    // ============================================================================

    /// Test updating with Unicode content.
    #[tokio::test]
    async fn test_update_with_unicode() {
        let backend = cassandra_backend!("test_update_with_unicode");
        let tenant = create_tenant();

        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        let mut updated_content = created.content().clone();
        updated_content["name"][0]["family"] = json!("日本語");
        updated_content["name"][0]["given"] = json!(["太郎"]);

        let updated = backend
            .update(&tenant, &created, updated_content)
            .await
            .unwrap();

        assert_eq!(updated.content()["name"][0]["family"], "日本語");
        assert_eq!(updated.content()["name"][0]["given"][0], "太郎");
    }

    /// Test updating with large content.
    #[tokio::test]
    async fn test_update_with_large_content() {
        let backend = cassandra_backend!("test_update_with_large_content");
        let tenant = create_tenant();

        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Create large update with many names
        let mut names = Vec::new();
        for i in 0..100 {
            names.push(json!({
                "family": format!("Family{}", i),
                "given": [format!("Given{}", i)]
            }));
        }

        let updated_content = json!({
            "resourceType": "Patient",
            "name": names
        });

        let updated = backend
            .update(&tenant, &created, updated_content)
            .await
            .unwrap();

        assert_eq!(updated.content()["name"].as_array().unwrap().len(), 100);
    }
}

pub mod delete_tests {
    //! Tests for resource delete operations.
    //!
    //! This module tests the `delete` method of the ResourceStorage trait
    //! and related soft delete behavior.

    use serde_json::json;

    use helios_persistence::core::ResourceStorage;
    use helios_persistence::error::{ResourceError, StorageError};
    use helios_persistence::tenant::{TenantContext, TenantPermissions};

    use crate::cassandra_node::{unique_tenant, unique_tenant_id};

    // ============================================================================
    // Helper Functions
    // ============================================================================

    fn create_tenant() -> TenantContext {
        unique_tenant("crud")
    }

    fn create_patient_json(name: &str) -> serde_json::Value {
        json!({
            "resourceType": "Patient",
            "name": [{"family": name}],
            "active": true
        })
    }

    // ============================================================================
    // Delete Tests - Basic
    // ============================================================================

    /// Test that deleting a resource succeeds.
    #[tokio::test]
    async fn test_delete_resource_success() {
        let backend = cassandra_backend!("test_delete_resource_success");
        let tenant = create_tenant();

        // Create a resource
        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Delete it
        let result = backend.delete(&tenant, "Patient", created.id()).await;

        assert!(result.is_ok(), "Delete should succeed");
    }

    /// Test that resource is not readable after delete.
    #[tokio::test]
    async fn test_delete_makes_resource_unreadable() {
        let backend = cassandra_backend!("test_delete_makes_resource_unreadable");
        let tenant = create_tenant();

        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let id = created.id().to_string();

        backend.delete(&tenant, "Patient", &id).await.unwrap();

        // Soft delete: `read` surfaces a deleted resource as `Gone` (SQLite/PG/S3)
        // or as `None`; either satisfies "not readable". Mirrors sqlite_tests.rs.
        match backend.read(&tenant, "Patient", &id).await {
            Ok(None) => {}
            Err(StorageError::Resource(ResourceError::Gone { .. })) => {}
            other => panic!("Deleted resource should not be readable, got: {:?}", other),
        }
    }

    /// Test that exists returns false after delete.
    #[tokio::test]
    async fn test_delete_makes_exists_false() {
        let backend = cassandra_backend!("test_delete_makes_exists_false");
        let tenant = create_tenant();

        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let id = created.id().to_string();

        backend.delete(&tenant, "Patient", &id).await.unwrap();

        let exists = backend.exists(&tenant, "Patient", &id).await.unwrap();
        assert!(!exists, "Deleted resource should not exist");
    }

    /// Test that count decreases after delete.
    #[tokio::test]
    async fn test_delete_decreases_count() {
        let backend = cassandra_backend!("test_delete_decreases_count");
        let tenant = create_tenant();

        // Create 3 resources
        let mut ids = Vec::new();
        for i in 0..3 {
            let patient = create_patient_json(&format!("Patient{}", i));
            let created = backend
                .create(
                    &tenant,
                    "Patient",
                    patient,
                    crate::cassandra_node::test_fhir_version(),
                )
                .await
                .unwrap();
            ids.push(created.id().to_string());
        }

        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 3);

        // Delete one
        backend.delete(&tenant, "Patient", &ids[0]).await.unwrap();
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 2);

        // Delete another
        backend.delete(&tenant, "Patient", &ids[1]).await.unwrap();
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 1);
    }

    // ============================================================================
    // Delete Tests - Error Cases
    // ============================================================================

    /// Test that deleting a nonexistent resource fails.
    #[tokio::test]
    async fn test_delete_nonexistent_fails() {
        let backend = cassandra_backend!("test_delete_nonexistent_fails");
        let tenant = create_tenant();

        let result = backend.delete(&tenant, "Patient", "nonexistent-id").await;

        assert!(result.is_err());
        match result {
            Err(StorageError::Resource(ResourceError::NotFound { .. })) => {}
            Err(e) => panic!("Expected NotFound error, got {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    /// Test that deleting an already deleted resource fails.
    #[tokio::test]
    async fn test_delete_already_deleted_fails() {
        let backend = cassandra_backend!("test_delete_already_deleted_fails");
        let tenant = create_tenant();

        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let id = created.id().to_string();

        // Delete once
        backend.delete(&tenant, "Patient", &id).await.unwrap();

        // Try to delete again
        let result = backend.delete(&tenant, "Patient", &id).await;

        assert!(result.is_err());
        // One contract for "nothing to delete", never stored or already deleted:
        // `NotFound` (#2523). The REST layer answers it as a `204` no-op.
        match result {
            Err(StorageError::Resource(ResourceError::NotFound { .. })) => {}
            Err(e) => panic!("Expected NotFound error, got {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    /// Test that deleting without permission fails.
    #[tokio::test]
    async fn test_delete_without_permission_fails() {
        let backend = cassandra_backend!("test_delete_without_permission_fails");
        let tenant_id = unique_tenant_id("test-tenant");
        let full_access = TenantContext::new(tenant_id.clone(), TenantPermissions::full_access());
        let read_only = TenantContext::new(tenant_id, TenantPermissions::read_only());

        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &full_access,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        let result = backend.delete(&read_only, "Patient", created.id()).await;

        assert!(result.is_err());
        match result {
            Err(StorageError::Tenant(_)) => {}
            Err(e) => panic!("Expected TenantError, got {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    /// Test that deleting from wrong tenant fails.
    #[tokio::test]
    async fn test_delete_wrong_tenant_fails() {
        let backend = cassandra_backend!("test_delete_wrong_tenant_fails");

        let tenant1 = TenantContext::new(
            unique_tenant_id("tenant-1"),
            TenantPermissions::full_access(),
        );
        let tenant2 = TenantContext::new(
            unique_tenant_id("tenant-2"),
            TenantPermissions::full_access(),
        );

        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant1,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        let result = backend.delete(&tenant2, "Patient", created.id()).await;

        assert!(result.is_err());
        match result {
            Err(StorageError::Resource(ResourceError::NotFound { .. })) => {}
            Err(e) => panic!("Expected NotFound error, got {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    // ============================================================================
    // Delete Tests - Tenant Isolation
    // ============================================================================

    /// Test that delete only affects the specified tenant.
    #[tokio::test]
    async fn test_delete_tenant_isolation() {
        let backend = cassandra_backend!("test_delete_tenant_isolation");

        let tenant1 = TenantContext::new(
            unique_tenant_id("tenant-1"),
            TenantPermissions::full_access(),
        );
        let tenant2 = TenantContext::new(
            unique_tenant_id("tenant-2"),
            TenantPermissions::full_access(),
        );

        // Create same-id resource in both tenants using create_or_update
        let patient = create_patient_json("Smith");
        backend
            .create_or_update(
                &tenant1,
                "Patient",
                "shared-id",
                patient.clone(),
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        backend
            .create_or_update(
                &tenant2,
                "Patient",
                "shared-id",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Delete from tenant1
        backend
            .delete(&tenant1, "Patient", "shared-id")
            .await
            .unwrap();

        // Tenant1 should not have it
        assert!(
            !backend
                .exists(&tenant1, "Patient", "shared-id")
                .await
                .unwrap()
        );

        // Tenant2 should still have it
        assert!(
            backend
                .exists(&tenant2, "Patient", "shared-id")
                .await
                .unwrap()
        );
    }

    // ============================================================================
    // Delete Tests - Soft Delete Behavior
    // ============================================================================

    /// Test that delete is a soft delete (resource can potentially be restored).
    /// This test verifies the soft delete semantics described in the design.
    #[tokio::test]
    async fn test_delete_is_soft_delete() {
        let backend = cassandra_backend!("test_delete_is_soft_delete");
        let tenant = create_tenant();

        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let id = created.id().to_string();

        backend.delete(&tenant, "Patient", &id).await.unwrap();

        // Normal read reports the deleted resource as `Gone` (or `None`)
        match backend.read(&tenant, "Patient", &id).await {
            Ok(None) => {}
            Err(StorageError::Resource(ResourceError::Gone { .. })) => {}
            other => panic!("Deleted resource should not be readable, got: {:?}", other),
        }

        // But create_or_update with same ID creates a new version
        let patient2 = create_patient_json("Restored");
        let (restored, _created_new) = backend
            .create_or_update(
                &tenant,
                "Patient",
                &id,
                patient2,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // This should create a new resource (after deletion)
        assert!(backend.exists(&tenant, "Patient", &id).await.unwrap());
        assert_eq!(restored.content()["name"][0]["family"], "Restored");

        // The restore continues the version chain rather than resetting to "1":
        // v1 create, v2 delete, v3 restore.
        assert_eq!(
            restored.version_id(),
            "3",
            "restore should continue the version chain"
        );
    }

    // ============================================================================
    // Delete Tests - Multiple Deletes
    // ============================================================================

    /// Test deleting multiple resources.
    #[tokio::test]
    async fn test_delete_multiple_resources() {
        let backend = cassandra_backend!("test_delete_multiple_resources");
        let tenant = create_tenant();

        // Create 5 resources
        let mut ids = Vec::new();
        for i in 0..5 {
            let patient = create_patient_json(&format!("Patient{}", i));
            let created = backend
                .create(
                    &tenant,
                    "Patient",
                    patient,
                    crate::cassandra_node::test_fhir_version(),
                )
                .await
                .unwrap();
            ids.push(created.id().to_string());
        }

        // Delete them all
        for id in &ids {
            backend.delete(&tenant, "Patient", id).await.unwrap();
        }

        // Verify all are deleted
        for id in &ids {
            assert!(!backend.exists(&tenant, "Patient", id).await.unwrap());
        }

        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 0);
    }

    /// Test deleting resources of different types.
    #[tokio::test]
    async fn test_delete_different_types() {
        let backend = cassandra_backend!("test_delete_different_types");
        let tenant = create_tenant();

        // Create various resources
        let patient = create_patient_json("Smith");
        let patient_created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        let observation = json!({
            "resourceType": "Observation",
            "status": "final",
            "code": {"coding": [{"code": "test"}]}
        });
        let obs_created = backend
            .create(
                &tenant,
                "Observation",
                observation,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Delete patient
        backend
            .delete(&tenant, "Patient", patient_created.id())
            .await
            .unwrap();

        // Patient should be gone
        assert!(
            !backend
                .exists(&tenant, "Patient", patient_created.id())
                .await
                .unwrap()
        );

        // Observation should still exist
        assert!(
            backend
                .exists(&tenant, "Observation", obs_created.id())
                .await
                .unwrap()
        );
    }

    // ============================================================================
    // Delete Tests - After Update
    // ============================================================================

    /// Test deleting a resource that has been updated.
    #[tokio::test]
    async fn test_delete_after_update() {
        let backend = cassandra_backend!("test_delete_after_update");
        let tenant = create_tenant();

        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Update multiple times
        let mut current = created;
        for i in 1..=5 {
            let mut content = current.content().clone();
            content["name"][0]["family"] = json!(format!("Name{}", i));
            current = backend.update(&tenant, &current, content).await.unwrap();
        }

        // Now delete
        backend
            .delete(&tenant, "Patient", current.id())
            .await
            .unwrap();

        // Should be deleted
        assert!(
            !backend
                .exists(&tenant, "Patient", current.id())
                .await
                .unwrap()
        );
    }

    // ============================================================================
    // Delete Tests - Idempotency Check
    // ============================================================================

    /// Verify delete is not idempotent (second delete fails).
    #[tokio::test]
    async fn test_delete_not_idempotent() {
        let backend = cassandra_backend!("test_delete_not_idempotent");
        let tenant = create_tenant();

        let patient = create_patient_json("Smith");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let id = created.id().to_string();

        // First delete succeeds
        assert!(backend.delete(&tenant, "Patient", &id).await.is_ok());

        // Second delete fails
        assert!(backend.delete(&tenant, "Patient", &id).await.is_err());
    }
}

/// Cassandra's one answer where an upstream assertion accepts two (the Step 3
/// matrix's T-4, T-9 and T-14). Each is on [`parity`]'s list of additions.
pub mod strict {
    use helios_persistence::core::ResourceStorage;
    use helios_persistence::error::{ResourceError, StorageError};
    use serde_json::json;

    use crate::cassandra_node::{unique_id, unique_tenant};

    fn patient(family: &str) -> serde_json::Value {
        json!({"resourceType": "Patient", "name": [{"family": family}], "active": true})
    }

    /// T-4 ([R4 http.html#read](https://hl7.org/fhir/R4/http.html#read): "a
    /// GET for a deleted resource returns a 410 status code, whereas a GET for
    /// an unknown resource returns 404"). `read_tests::test_read_after_delete`
    /// accepts `Ok(None)` or `Gone`; Cassandra's `read` answers `Gone` for a
    /// delete marker, which REST turns into 410 `deleted`, and `Ok(None)` only
    /// for an id it never stored.
    #[tokio::test]
    async fn cassandra_strict_read_after_delete_is_gone() {
        let backend = cassandra_backend!("cassandra_strict_read_after_delete_is_gone");
        let tenant = unique_tenant("strict-gone");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient("Smith"),
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        backend
            .delete(&tenant, "Patient", created.id())
            .await
            .unwrap();

        match backend.read(&tenant, "Patient", created.id()).await {
            Err(StorageError::Resource(ResourceError::Gone {
                resource_type, id, ..
            })) => {
                assert_eq!(resource_type, "Patient");
                assert_eq!(id, created.id());
            }
            other => panic!("read after delete must be Gone, got {other:?}"),
        }
        assert!(
            !backend
                .exists(&tenant, "Patient", created.id())
                .await
                .unwrap()
        );
        // An id that was never stored is absent, not gone.
        assert!(
            backend
                .read(&tenant, "Patient", &unique_id("never-stored"))
                .await
                .unwrap()
                .is_none()
        );
    }

    /// T-14 ([R4 http.html#delete](https://hl7.org/fhir/R4/http.html#delete):
    /// "if the resource does not exist at all" and "already deleted has no
    /// effect"). Storage has one answer for "nothing to delete", `NotFound`,
    /// as SQLite, PostgreSQL and MongoDB give, and REST answers it as a 204
    /// no-op (HeliosSoftware/hfs#2529, carried by ADR-aacruzgon-0052). This
    /// pins Cassandra to it for both cases, for `delete` and for
    /// `delete_versioned`, and checks that neither wrote a version: the
    /// marker stays at version 2 and the unknown id stays unknown.
    #[tokio::test]
    async fn cassandra_strict_nothing_to_delete_is_not_found() {
        let backend = cassandra_backend!("cassandra_strict_nothing_to_delete_is_not_found");
        let tenant = unique_tenant("strict-nf");

        let unknown = unique_id("never-stored");
        match backend.delete(&tenant, "Patient", &unknown).await {
            Err(StorageError::Resource(ResourceError::NotFound { .. })) => {}
            other => panic!("delete of an unknown id must be NotFound, got {other:?}"),
        }
        match backend
            .delete_versioned(&tenant, "Patient", &unknown, "1")
            .await
        {
            Err(StorageError::Resource(ResourceError::NotFound { .. })) => {}
            other => panic!("delete_versioned of an unknown id must be NotFound, got {other:?}"),
        }
        assert!(
            backend
                .read(&tenant, "Patient", &unknown)
                .await
                .unwrap()
                .is_none(),
            "a delete of an unknown id writes no marker"
        );

        let created = backend
            .create(
                &tenant,
                "Patient",
                patient("Twice"),
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        backend
            .delete(&tenant, "Patient", created.id())
            .await
            .unwrap();
        match backend.delete(&tenant, "Patient", created.id()).await {
            Err(StorageError::Resource(ResourceError::NotFound { .. })) => {}
            other => panic!("a second delete must be NotFound, not Gone: {other:?}"),
        }
        match backend
            .delete_versioned(&tenant, "Patient", created.id(), "2")
            .await
        {
            Err(StorageError::Resource(ResourceError::NotFound { .. })) => {}
            other => panic!("delete_versioned of a marker must be NotFound: {other:?}"),
        }
        // Neither second delete wrote a version: a revive lands on 3.
        let (revived, created_again) = backend
            .create_or_update(
                &tenant,
                "Patient",
                created.id(),
                json!({"resourceType": "Patient", "id": created.id()}),
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        assert!(created_again);
        assert_eq!(revived.version_id(), "3", "the marker was version 2");
    }

    /// T-9 ([R4 http.html#update](https://hl7.org/fhir/R4/http.html#update):
    /// "a 201 Created status code if the resource was created (or brought back
    /// to life/re-created)"; D16 keeps that for every FHIR version).
    /// `delete_tests::test_delete_is_soft_delete` checks the revive's version
    /// but not `created`. Here a resource at version N = 2 is deleted (the
    /// marker is N+1) and revived: `created` is true, the version is N+2, the
    /// body is the revive's, `created_at` and the FHIR version are the
    /// original's, and the resource reads and counts again.
    #[tokio::test]
    async fn cassandra_strict_revive_is_created_at_n_plus_2() {
        let backend = cassandra_backend!("cassandra_strict_revive_is_created_at_n_plus_2");
        let tenant = unique_tenant("strict-revive");
        let id = unique_id("revive");
        let (v1, created) = backend
            .create_or_update(
                &tenant,
                "Patient",
                &id,
                json!({"resourceType": "Patient", "id": id, "name": [{"family": "One"}]}),
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        assert!(created);
        let v2 = backend
            .update(
                &tenant,
                &v1,
                json!({"resourceType": "Patient", "id": id, "name": [{"family": "Two"}]}),
            )
            .await
            .unwrap();
        assert_eq!(v2.version_id(), "2");
        backend.delete(&tenant, "Patient", &id).await.unwrap();
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 0);

        let (revived, created) = backend
            .create_or_update(
                &tenant,
                "Patient",
                &id,
                json!({"resourceType": "Patient", "id": id, "name": [{"family": "Back"}]}),
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        assert!(created, "a revive counts as created");
        assert_eq!(
            revived.version_id(),
            "4",
            "N = 2, the marker 3, the revive 4"
        );
        assert_eq!(revived.content()["name"][0]["family"], "Back");
        assert_eq!(revived.created_at(), v1.created_at(), "created_at is kept");
        assert_eq!(revived.fhir_version(), v1.fhir_version());
        assert!(!revived.is_deleted());

        let read = backend
            .read(&tenant, "Patient", &id)
            .await
            .unwrap()
            .expect("live again after the revive");
        assert_eq!(read.version_id(), "4");
        assert_eq!(read.content()["name"][0]["family"], "Back");
        assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 1);
    }

    /// D8 and G9: an `update` of a deleted resource is `NotFound`, never
    /// `Gone`, as SQLite, PostgreSQL and MongoDB answer; REST then answers
    /// 404 for an `If-Match` PUT that raced a delete. `update_tests::
    /// test_update_deleted_fails` accepts either. The marker is untouched.
    #[tokio::test]
    async fn cassandra_strict_update_of_deleted_is_not_found() {
        let backend = cassandra_backend!("cassandra_strict_update_of_deleted_is_not_found");
        let tenant = unique_tenant("strict-upd");
        let created = backend
            .create(
                &tenant,
                "Patient",
                patient("Smith"),
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        backend
            .delete(&tenant, "Patient", created.id())
            .await
            .unwrap();

        match backend
            .update(&tenant, &created, created.content().clone())
            .await
        {
            Err(StorageError::Resource(ResourceError::NotFound { .. })) => {}
            other => panic!("update of a deleted resource must be NotFound: {other:?}"),
        }
        match backend.read(&tenant, "Patient", created.id()).await {
            Err(StorageError::Resource(ResourceError::Gone { .. })) => {}
            other => panic!("the refused update left the marker: {other:?}"),
        }
    }
}

/// The copy against upstream's `tests/crud/`, read when the test runs.
pub mod parity {
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::Path;

    /// Upstream tests deliberately left out of the copy, with the reason.
    /// None today: every upstream CRUD test runs on Cassandra.
    const EXCLUDED: &[(&str, &str, &str)] = &[];

    /// Tests in the copy that upstream does not have, with the reason.
    const ADDITIONS: &[(&str, &str, &str)] = &[
        (
            "strict",
            "cassandra_strict_read_after_delete_is_gone",
            "T-4: read_tests::test_read_after_delete accepts None or Gone",
        ),
        (
            "strict",
            "cassandra_strict_nothing_to_delete_is_not_found",
            "T-14: one answer, NotFound, for an unknown and a deleted id, writing nothing",
        ),
        (
            "strict",
            "cassandra_strict_revive_is_created_at_n_plus_2",
            "T-9: delete_tests::test_delete_is_soft_delete does not assert created",
        ),
        (
            "strict",
            "cassandra_strict_update_of_deleted_is_not_found",
            "D8, G9: update_tests::test_update_deleted_fails accepts NotFound or Gone",
        ),
    ];

    /// The content of each upstream file this copy was made from, as a
    /// 64-bit FNV-1a hash of its text with `\r\n` read as `\n`. A test can
    /// change upstream without changing its name (PR #55 rewrote
    /// `test_delete_already_deleted_fails`'s expectation that way), so a
    /// changed file fails here until the copy follows it: bring the change
    /// into the module of the same name, then update the hash. Baseline:
    /// `main` at `e32b02184`; the sync of 2026-11-09 removed `conditional_tests`
    /// with upstream's file (HeliosSoftware/hfs#2522) and re-pinned `mod`.
    const UPSTREAM_CONTENT: &[(&str, u64)] = &[
        ("create_tests", 0x9d81_515d_caeb_fa4f),
        ("delete_tests", 0xc658_fd7c_28d4_4415),
        ("mod", 0x6636_1b8e_7a00_9806),
        ("read_tests", 0xb791_7f63_e15a_4fb2),
        ("update_tests", 0x7fdf_5021_c306_ab09),
    ];

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

    /// The test functions of `source`: every `fn` (async or not) whose
    /// nearest preceding line that is not a comment is a test attribute
    /// (`#[test]`, `#[tokio::test]` or `#[tokio::test(…)]`), whatever its
    /// name, with `ignored` set when an `#[ignore…]` line sits between.
    fn tests(source: &str) -> BTreeMap<String, bool> {
        let lines: Vec<&str> = source.lines().map(str::trim_start).collect();
        let mut found = BTreeMap::new();
        for (i, line) in lines.iter().enumerate() {
            let Some(rest) = line
                .strip_prefix("async fn ")
                .or_else(|| line.strip_prefix("fn "))
            else {
                continue;
            };
            let name = rest.split(['(', '<']).next().unwrap_or("").to_owned();
            let mut ignored = false;
            for above in lines[..i].iter().rev() {
                if above.starts_with("//") {
                    continue;
                }
                if above.starts_with("#[ignore") {
                    ignored = true;
                    continue;
                }
                if above.starts_with("#[cfg(") {
                    continue;
                }
                if *above == "#[test]" || above.starts_with("#[tokio::test") {
                    found.insert(name.clone(), ignored);
                }
                break;
            }
        }
        found
    }

    /// The text of each `pub mod <name> {` block of the copy, up to the next
    /// one: the copy is one module per upstream file, at the top level.
    fn copy_modules(copy: &str) -> BTreeMap<String, String> {
        let mut modules = BTreeMap::new();
        let mut current: Option<(String, String)> = None;
        for line in copy.lines() {
            if let Some(rest) = line.strip_prefix("pub mod ") {
                if let Some((name, text)) = current.take() {
                    modules.insert(name, text);
                }
                let name = rest.trim_end_matches(" {").trim().to_owned();
                current = Some((name, String::new()));
            } else if let Some((_, text)) = current.as_mut() {
                text.push_str(line);
                text.push('\n');
            }
        }
        if let Some((name, text)) = current {
            modules.insert(name, text);
        }
        modules
    }

    /// Every upstream CRUD test has a counterpart in the module of the same
    /// name here, or a reason on [`EXCLUDED`]; every test here is upstream's
    /// or on [`ADDITIONS`]; none is ignored, the two #306 tests included; and
    /// no upstream file changed since the copy was made ([`UPSTREAM_CONTENT`]).
    #[test]
    fn crud_copy_matches_upstream() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
        let copy = read(&root.join("cassandra/crud.rs"));
        let modules = copy_modules(&copy);

        let mut upstream_files = BTreeSet::new();
        let mut upstream_total = 0;
        let mut copied_total = 0;
        let entries = std::fs::read_dir(root.join("crud")).expect("read tests/crud");
        for entry in entries {
            let path = entry.expect("a directory entry").path();
            let Some(stem) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .filter(|_| path.extension().is_some_and(|e| e == "rs"))
            else {
                continue;
            };
            let upstream = read(&path);
            let pinned = UPSTREAM_CONTENT
                .iter()
                .find(|(file, _)| *file == stem)
                .map(|(_, hash)| *hash);
            assert_eq!(
                pinned,
                Some(fnv1a(&upstream)),
                "tests/crud/{stem}.rs is new or changed since tests/cassandra/crud.rs was \
                 copied from it: bring the change into the copy, then update UPSTREAM_CONTENT"
            );
            if stem == "mod" {
                continue;
            }
            upstream_files.insert(stem.to_owned());
            let Some(module) = modules.get(stem) else {
                panic!("tests/crud/{stem}.rs has no `pub mod {stem}` in tests/cassandra/crud.rs");
            };
            let ours = tests(module);
            let theirs = tests(&upstream);
            upstream_total += theirs.len();
            copied_total += ours.len();
            for name in theirs.keys() {
                let excluded = EXCLUDED.iter().any(|(f, t, _)| *f == stem && t == name);
                assert!(
                    ours.contains_key(name) != excluded,
                    "tests/crud/{stem}.rs::{name} needs exactly one of: a copy in \
                     tests/cassandra/crud.rs::{stem}, or a reason on EXCLUDED"
                );
            }
            for (name, ignored) in &ours {
                assert!(
                    theirs.contains_key(name),
                    "crud.rs::{stem}::{name} is not in tests/crud/{stem}.rs: renamed or removed \
                     upstream?"
                );
                assert!(!ignored, "crud.rs::{stem}::{name} is ignored");
            }
        }
        for (file, _) in UPSTREAM_CONTENT {
            assert!(
                *file == "mod" || upstream_files.contains(*file),
                "tests/crud/{file}.rs is gone upstream"
            );
        }

        // The copy has no module upstream lacks, beyond the two of this file.
        for name in modules.keys() {
            assert!(
                upstream_files.contains(name) || name == "strict" || name == "parity",
                "crud.rs::{name} matches no file in tests/crud/"
            );
        }
        let strict = tests(&modules["strict"]);
        for (module, name, reason) in ADDITIONS {
            assert_eq!(*module, "strict");
            assert!(!reason.is_empty());
            assert_eq!(strict.get(*name), Some(&false), "the addition {name}");
        }
        assert_eq!(
            strict.len(),
            ADDITIONS.len(),
            "every strict test is on ADDITIONS: {strict:?}"
        );
        // 88 at the 2026-11-06 baseline; the count follows upstream.
        assert_eq!(copied_total + EXCLUDED.len(), upstream_total);

        // No `#[ignore]` anywhere in the copy's test modules.
        for (name, text) in &modules {
            if name != "parity" {
                assert!(
                    !text.lines().any(|l| l.trim_start().starts_with("#[ignore")),
                    "crud.rs::{name}: no copied CRUD test is ignored on Cassandra"
                );
            }
        }
    }

    /// The discovery finds tests by attribute, not by name, and sees an
    /// `#[ignore]`.
    #[test]
    fn test_discovery_reads_attributes() {
        let source = "/// doc\n#[tokio::test]\nasync fn plain_name() {}\n\
                      #[test]\n#[ignore = \"why\"]\nfn sync_one() {}\n\
                      #[tokio::test(flavor = \"multi_thread\")]\n// note\nasync fn flavoured() {}\n\
                      fn helper() {}\n";
        let found = tests(source);
        assert_eq!(found.len(), 3, "{found:?}");
        assert!(!found["plain_name"]);
        assert!(found["sync_one"]);
        assert!(!found["flavoured"]);
        assert!(!found.contains_key("helper"));
    }
}
