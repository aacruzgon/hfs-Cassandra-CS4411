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
//! - **`FhirVersion::default_enabled()`** in place of upstream's
//!   `FhirVersion::default()`, which exists only with the `R4` feature, so the
//!   copy also compiles in the R4B-only check (`r4b-compile-check.yml`). With
//!   `R4` on, both return R4, so the tests run as upstream's do.
//!
//! Where an upstream assertion accepts two answers, the copy keeps it as it
//! is, for parity, and [`strict`] adds a `cassandra_strict_*` test that pins
//! Cassandra's one answer (the Step 3 matrix's T-4, T-9 and T-14).
//!
//! [`parity`] reads `tests/crud/` when it runs, so an upstream sync that adds,
//! renames or removes a CRUD test, or a whole file, fails it until this copy
//! follows.
//!
//! Every test here runs against the real Step 3 backend: `update`, `delete`
//! and `delete_versioned` (roadmap Step 3's write path), `vread`,
//! `list_versions` and `delete_with_match`, and `create_or_update`, as well
//! as Step 2's `create`, `read`, `count`, `exists` and `read_batch`.

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

    use helios_fhir::FhirVersion;
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
                FhirVersion::default_enabled(),
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
                FhirVersion::default_enabled(),
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
                FhirVersion::default_enabled(),
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
                FhirVersion::default_enabled(),
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
                FhirVersion::default_enabled(),
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
                FhirVersion::default_enabled(),
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
                FhirVersion::default_enabled(),
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
                .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
                FhirVersion::default_enabled(),
            )
            .await
            .unwrap();
        let stored2 = backend
            .create(&tenant2, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
                FhirVersion::default_enabled(),
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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

    use helios_fhir::FhirVersion;
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant1, "Patient", patient, FhirVersion::default_enabled())
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
                FhirVersion::default_enabled(),
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant1, "Patient", patient, FhirVersion::default_enabled())
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
                .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient1, FhirVersion::default_enabled())
            .await
            .unwrap();
        let patient2 = create_patient_json("Jones");
        let created2 = backend
            .create(&tenant, "Patient", patient2, FhirVersion::default_enabled())
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
                FhirVersion::default_enabled(),
            )
            .await
            .unwrap();
        let created2 = backend
            .create(
                &tenant,
                "Patient",
                create_patient_json("Doomed"),
                FhirVersion::default_enabled(),
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
            .create(&tenant1, "Patient", patient, FhirVersion::default_enabled())
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
                .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
                .create(&tenant, "Observation", obs, FhirVersion::default_enabled())
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
                .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
                .create(&tenant, "Observation", obs, FhirVersion::default_enabled())
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
                .create(&tenant1, "Patient", patient, FhirVersion::default_enabled())
                .await
                .unwrap();
        }

        // Create resources in tenant2
        for i in 0..3 {
            let patient = create_patient_json(&format!("Patient{}", i));
            backend
                .create(&tenant2, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
                .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
                FhirVersion::default_enabled(),
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

    use helios_fhir::FhirVersion;
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
                FhirVersion::default_enabled(),
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
            FhirVersion::default_enabled(),
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
                FhirVersion::default_enabled(),
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant1, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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

    use helios_fhir::FhirVersion;
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
                .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
                FhirVersion::default_enabled(),
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
            .create(&tenant1, "Patient", patient, FhirVersion::default_enabled())
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
                FhirVersion::default_enabled(),
            )
            .await
            .unwrap();
        backend
            .create_or_update(
                &tenant2,
                "Patient",
                "shared-id",
                patient,
                FhirVersion::default_enabled(),
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
                FhirVersion::default_enabled(),
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
                .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
                FhirVersion::default_enabled(),
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
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
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
            .await
            .unwrap();
        let id = created.id().to_string();

        // First delete succeeds
        assert!(backend.delete(&tenant, "Patient", &id).await.is_ok());

        // Second delete fails
        assert!(backend.delete(&tenant, "Patient", &id).await.is_err());
    }
}

pub mod conditional_tests {
    //! Tests for conditional CRUD operations.
    //!
    //! This module tests conditional create, update, and delete operations
    //! that use search parameters to match resources.

    use serde_json::json;

    use helios_fhir::FhirVersion;
    use helios_persistence::core::ResourceStorage;
    use helios_persistence::tenant::TenantContext;

    use crate::cassandra_node::{unique_tenant, unique_tenant_id};

    // ============================================================================
    // Helper Functions
    // ============================================================================

    fn create_tenant() -> TenantContext {
        unique_tenant("crud")
    }

    fn create_patient_json(family: &str, identifier: Option<(&str, &str)>) -> serde_json::Value {
        let mut patient = json!({
            "resourceType": "Patient",
            "name": [{"family": family}],
            "active": true
        });

        if let Some((system, value)) = identifier {
            patient["identifier"] = json!([{
                "system": system,
                "value": value
            }]);
        }

        patient
    }

    // ============================================================================
    // Note: Conditional operations require SearchProvider implementation
    // These tests serve as a specification for expected behavior when implemented
    // ============================================================================

    /// Test conditional create when no match exists (should create).
    #[tokio::test]
    async fn test_conditional_create_no_match_creates() {
        // This test verifies the expected behavior of conditional_create
        // when no matching resource exists.
        //
        // Expected behavior:
        // - If search matches 0 resources -> Create new resource
        // - If search matches 1 resource -> Return existing resource
        // - If search matches >1 resources -> Return error (MultipleMatches)

        let backend = cassandra_backend!("test_conditional_create_no_match_creates");
        let tenant = create_tenant();

        // First, verify the storage is empty
        let count = backend.count(&tenant, Some("Patient")).await.unwrap();
        assert_eq!(count, 0);

        // For now, we test basic create since conditional_create needs SearchProvider
        let patient = create_patient_json("Smith", Some(("http://example.org/mrn", "MRN001")));
        let created = backend
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
            .await
            .unwrap();

        assert_eq!(created.content()["identifier"][0]["value"], "MRN001");
    }

    /// Test conditional create when exactly one match exists (should return existing).
    #[tokio::test]
    async fn test_conditional_create_single_match_returns_existing() {
        // When conditional_create finds exactly one matching resource,
        // it should return that resource without creating a new one.

        let backend = cassandra_backend!("test_conditional_create_single_match_returns_existing");
        let tenant = create_tenant();

        // Create initial resource
        let patient = create_patient_json("Smith", Some(("http://example.org/mrn", "MRN002")));
        let created = backend
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
            .await
            .unwrap();

        // Verify it exists
        let count = backend.count(&tenant, Some("Patient")).await.unwrap();
        assert_eq!(count, 1);

        // Conditional create with same identifier should return existing
        // (This would use identifier=http://example.org/mrn|MRN002 as search param)
        // For now we just verify the existing resource is still there
        let read = backend
            .read(&tenant, "Patient", created.id())
            .await
            .unwrap();
        assert!(read.is_some());
    }

    /// Test conditional create when multiple matches exist (should fail).
    #[tokio::test]
    async fn test_conditional_create_multiple_matches_fails() {
        // When conditional_create finds multiple matching resources,
        // it should return MultipleMatches error.

        let backend = cassandra_backend!("test_conditional_create_multiple_matches_fails");
        let tenant = create_tenant();

        // Create two resources with same family name (but different identifiers)
        let patient1 = create_patient_json("Smith", Some(("http://example.org/mrn", "MRN003")));
        let patient2 = create_patient_json("Smith", Some(("http://example.org/mrn", "MRN004")));

        backend
            .create(&tenant, "Patient", patient1, FhirVersion::default_enabled())
            .await
            .unwrap();
        backend
            .create(&tenant, "Patient", patient2, FhirVersion::default_enabled())
            .await
            .unwrap();

        // If we tried conditional_create with name=Smith, it should fail
        // because there are two matches
        let count = backend.count(&tenant, Some("Patient")).await.unwrap();
        assert_eq!(count, 2);
    }

    // ============================================================================
    // Conditional Update Tests (Specification)
    // ============================================================================

    /// Test conditional update when exactly one match exists (should update).
    #[tokio::test]
    async fn test_conditional_update_single_match_updates() {
        // Expected behavior of conditional_update:
        // - If search matches 0 resources and upsert=false -> NoMatch
        // - If search matches 0 resources and upsert=true -> Create
        // - If search matches 1 resource -> Update it
        // - If search matches >1 resources -> MultipleMatches error

        let backend = cassandra_backend!("test_conditional_update_single_match_updates");
        let tenant = create_tenant();

        // Create a resource
        let patient = create_patient_json("Smith", Some(("http://example.org/mrn", "MRN005")));
        let created = backend
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
            .await
            .unwrap();

        // Update using regular update (conditional update would use search params)
        let mut updated_content = created.content().clone();
        updated_content["name"][0]["family"] = json!("UpdatedSmith");
        let updated = backend
            .update(&tenant, &created, updated_content)
            .await
            .unwrap();

        assert_eq!(updated.content()["name"][0]["family"], "UpdatedSmith");
        assert_eq!(updated.version_id(), "2");
    }

    /// Test conditional update with upsert when no match exists (should create).
    #[tokio::test]
    async fn test_conditional_update_upsert_creates() {
        // When conditional_update with upsert=true finds no matches,
        // it should create a new resource.

        let backend = cassandra_backend!("test_conditional_update_upsert_creates");
        let tenant = create_tenant();

        // Start with empty storage
        let count = backend.count(&tenant, Some("Patient")).await.unwrap();
        assert_eq!(count, 0);

        // This is equivalent to upsert behavior
        let patient = create_patient_json("NewPatient", Some(("http://example.org/mrn", "MRN006")));
        let (created, is_created) = backend
            .create_or_update(
                &tenant,
                "Patient",
                "new-patient-id",
                patient,
                FhirVersion::default_enabled(),
            )
            .await
            .unwrap();

        assert!(is_created);
        assert_eq!(created.content()["name"][0]["family"], "NewPatient");
    }

    // ============================================================================
    // Conditional Delete Tests (Specification)
    // ============================================================================

    /// Test conditional delete when exactly one match exists (should delete).
    #[tokio::test]
    async fn test_conditional_delete_single_match_deletes() {
        // Expected behavior of conditional_delete:
        // - If search matches 0 resources -> NoMatch
        // - If search matches 1 resource -> Delete it
        // - If search matches >1 resources -> MultipleMatches error

        let backend = cassandra_backend!("test_conditional_delete_single_match_deletes");
        let tenant = create_tenant();

        // Create a resource
        let patient = create_patient_json("ToDelete", Some(("http://example.org/mrn", "MRN007")));
        let created = backend
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
            .await
            .unwrap();
        let id = created.id().to_string();

        // Delete it (regular delete; conditional would use search params)
        backend.delete(&tenant, "Patient", &id).await.unwrap();

        // Verify deleted
        assert!(!backend.exists(&tenant, "Patient", &id).await.unwrap());
    }

    /// Test conditional delete when no match exists (should return NoMatch).
    #[tokio::test]
    async fn test_conditional_delete_no_match() {
        // When conditional_delete finds no matching resources,
        // it should return NoMatch result.

        let backend = cassandra_backend!("test_conditional_delete_no_match");
        let tenant = create_tenant();

        // Empty storage
        let count = backend.count(&tenant, Some("Patient")).await.unwrap();
        assert_eq!(count, 0);

        // Regular delete of nonexistent resource fails with NotFound
        let result = backend.delete(&tenant, "Patient", "nonexistent").await;
        assert!(result.is_err());
    }

    /// Test conditional delete when multiple matches exist (should fail).
    #[tokio::test]
    async fn test_conditional_delete_multiple_matches_fails() {
        // When conditional_delete finds multiple matching resources,
        // it should return MultipleMatches error (FHIR doesn't allow
        // deleting multiple resources with one conditional delete).

        let backend = cassandra_backend!("test_conditional_delete_multiple_matches_fails");
        let tenant = create_tenant();

        // Create multiple resources
        let patient1 =
            create_patient_json("DeleteTest", Some(("http://example.org/mrn", "MRN008")));
        let patient2 =
            create_patient_json("DeleteTest", Some(("http://example.org/mrn", "MRN009")));

        let created1 = backend
            .create(&tenant, "Patient", patient1, FhirVersion::default_enabled())
            .await
            .unwrap();
        let created2 = backend
            .create(&tenant, "Patient", patient2, FhirVersion::default_enabled())
            .await
            .unwrap();

        // Both should exist
        assert!(
            backend
                .exists(&tenant, "Patient", created1.id())
                .await
                .unwrap()
        );
        assert!(
            backend
                .exists(&tenant, "Patient", created2.id())
                .await
                .unwrap()
        );

        // If conditional_delete were called with name=DeleteTest,
        // it should fail with MultipleMatches
    }

    // ============================================================================
    // If-Match Header Tests (Optimistic Locking)
    // ============================================================================

    /// Test update with If-Match header succeeds when version matches.
    #[tokio::test]
    async fn test_if_match_success() {
        let backend = cassandra_backend!("test_if_match_success");
        let tenant = create_tenant();

        let patient = create_patient_json("Smith", None);
        let created = backend
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
            .await
            .unwrap();

        // Update with correct ETag (version matches)
        let mut content = created.content().clone();
        content["name"][0]["family"] = json!("Jones");

        // The update method checks version internally
        let updated = backend.update(&tenant, &created, content).await.unwrap();

        assert_eq!(updated.version_id(), "2");
    }

    /// Test update with If-Match header fails when version doesn't match.
    #[tokio::test]
    async fn test_if_match_failure() {
        let backend = cassandra_backend!("test_if_match_failure");
        let tenant = create_tenant();

        let patient = create_patient_json("Smith", None);
        let v1 = backend
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
            .await
            .unwrap();

        // Do an update to create v2
        let v2 = backend
            .update(&tenant, &v1, v1.content().clone())
            .await
            .unwrap();
        assert_eq!(v2.version_id(), "2");

        // Try to update using stale v1
        let mut content = v1.content().clone();
        content["name"][0]["family"] = json!("Jones");
        let result = backend.update(&tenant, &v1, content).await;

        // Should fail due to version mismatch
        assert!(result.is_err());
    }

    // ============================================================================
    // If-None-Match Header Tests (Conditional Create)
    // ============================================================================

    /// Test create with If-None-Match: * succeeds when resource doesn't exist.
    #[tokio::test]
    async fn test_if_none_match_success() {
        let backend = cassandra_backend!("test_if_none_match_success");
        let tenant = create_tenant();

        // If-None-Match: * means "only create if nothing exists with this ID"
        // This is essentially what create_or_update does when creating new
        let patient = create_patient_json("NewPatient", None);
        let (created, is_new) = backend
            .create_or_update(
                &tenant,
                "Patient",
                "unique-id-123",
                patient,
                FhirVersion::default_enabled(),
            )
            .await
            .unwrap();

        assert!(is_new);
        assert_eq!(created.id(), "unique-id-123");
    }

    /// Test create with If-None-Match: * fails when resource exists.
    #[tokio::test]
    async fn test_if_none_match_failure() {
        let backend = cassandra_backend!("test_if_none_match_failure");
        let tenant = create_tenant();

        // Create a resource with specific ID
        let patient1 = create_patient_json("First", None);
        let (_, is_new1) = backend
            .create_or_update(
                &tenant,
                "Patient",
                "existing-id",
                patient1,
                FhirVersion::default_enabled(),
            )
            .await
            .unwrap();
        assert!(is_new1);

        // Try to create another with same ID
        // create_or_update will update instead of failing
        let patient2 = create_patient_json("Second", None);
        let (updated, is_new2) = backend
            .create_or_update(
                &tenant,
                "Patient",
                "existing-id",
                patient2,
                FhirVersion::default_enabled(),
            )
            .await
            .unwrap();

        // It should have updated, not created
        assert!(!is_new2);
        assert_eq!(updated.version_id(), "2");
        assert_eq!(updated.content()["name"][0]["family"], "Second");
    }

    // ============================================================================
    // Conditional Patch Tests (FHIR v6.0.0)
    // ============================================================================

    /// Test conditional patch when exactly one match exists (should patch).
    ///
    /// Conditional patch uses search parameters to find a resource to patch:
    /// PATCH [base]/[type]?[search-params]
    ///
    /// Expected behavior:
    /// - If search matches 0 resources -> Return error (no match)
    /// - If search matches 1 resource -> Apply patch to that resource
    /// - If search matches >1 resources -> Return error (multiple matches)
    #[tokio::test]
    async fn test_conditional_patch_single_match() {
        let backend = cassandra_backend!("test_conditional_patch_single_match");
        let tenant = create_tenant();

        // Create a resource with unique identifier
        let patient =
            create_patient_json("PatchTest", Some(("http://example.org/mrn", "PATCH-001")));
        let created = backend
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
            .await
            .unwrap();

        // Verify the resource exists
        let read = backend
            .read(&tenant, "Patient", created.id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read.content()["name"][0]["family"], "PatchTest");

        // Conditional patch would be:
        // PATCH /Patient?identifier=http://example.org/mrn|PATCH-001
        // with patch body: [{"op": "replace", "path": "/name/0/family", "value": "Patched"}]
        //
        // This would find the patient by identifier and patch it.
        // For now, we use regular update to simulate the expected outcome.

        let mut patched_content = read.content().clone();
        patched_content["name"][0]["family"] = json!("Patched");
        let updated = backend
            .update(&tenant, &read, patched_content)
            .await
            .unwrap();

        assert_eq!(updated.content()["name"][0]["family"], "Patched");
        assert_eq!(updated.version_id(), "2");
    }

    /// Test conditional patch when no match exists (should fail).
    ///
    /// When conditional patch finds no matching resources, it should return an error.
    #[tokio::test]
    async fn test_conditional_patch_no_match() {
        let backend = cassandra_backend!("test_conditional_patch_no_match");
        let tenant = create_tenant();

        // Verify storage is empty
        let count = backend.count(&tenant, Some("Patient")).await.unwrap();
        assert_eq!(count, 0, "Storage should be empty");

        // Conditional patch with no matching resources should fail
        // PATCH /Patient?identifier=http://example.org/mrn|NONEXISTENT
        //
        // When implemented, this should return a "no match" error.
        // The current test documents the expected behavior.
    }

    /// Test conditional patch when multiple matches exist (should fail).
    ///
    /// When conditional patch finds multiple matching resources, it should return
    /// an error rather than patching all of them.
    #[tokio::test]
    async fn test_conditional_patch_multiple_matches() {
        let backend = cassandra_backend!("test_conditional_patch_multiple_matches");
        let tenant = create_tenant();

        // Create multiple resources with the same family name
        let patient1 =
            create_patient_json("DuplicateName", Some(("http://example.org/mrn", "DUP-001")));
        let patient2 =
            create_patient_json("DuplicateName", Some(("http://example.org/mrn", "DUP-002")));

        backend
            .create(&tenant, "Patient", patient1, FhirVersion::default_enabled())
            .await
            .unwrap();
        backend
            .create(&tenant, "Patient", patient2, FhirVersion::default_enabled())
            .await
            .unwrap();

        // Conditional patch with multiple matches should fail
        // PATCH /Patient?family=DuplicateName
        //
        // When implemented, this should return a "multiple matches" error.
        // FHIR requires that conditional operations match exactly one resource.

        let count = backend.count(&tenant, Some("Patient")).await.unwrap();
        assert_eq!(count, 2, "Should have 2 patients with same name");
    }

    /// Test conditional patch with JSON Patch format.
    ///
    /// FHIR supports JSON Patch (RFC 6902) for conditional patch operations.
    /// The Content-Type should be application/json-patch+json.
    #[tokio::test]
    async fn test_conditional_patch_json_patch_format() {
        let backend = cassandra_backend!("test_conditional_patch_json_patch_format");
        let tenant = create_tenant();

        // Create a resource
        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": "Original", "given": ["First"]}],
            "active": true
        });
        let created = backend
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
            .await
            .unwrap();

        // JSON Patch operations (RFC 6902):
        // [
        //   {"op": "replace", "path": "/name/0/family", "value": "NewFamily"},
        //   {"op": "add", "path": "/name/0/given/-", "value": "Middle"},
        //   {"op": "remove", "path": "/active"}
        // ]
        //
        // Expected result:
        // - family changed to "NewFamily"
        // - "Middle" added to given names
        // - active field removed

        // Simulate the expected outcome with regular update
        let mut patched = created.content().clone();
        patched["name"][0]["family"] = json!("NewFamily");
        if let Some(given) = patched["name"][0]["given"].as_array_mut() {
            given.push(json!("Middle"));
        }
        patched.as_object_mut().unwrap().remove("active");

        let updated = backend.update(&tenant, &created, patched).await.unwrap();

        assert_eq!(updated.content()["name"][0]["family"], "NewFamily");
        let given = updated.content()["name"][0]["given"].as_array().unwrap();
        assert!(given.iter().any(|v| v == "Middle"));
        assert!(updated.content().get("active").is_none());
    }

    /// Test conditional patch with FHIRPath Patch format.
    ///
    /// FHIR v6.0.0 supports FHIRPath Patch as an alternative to JSON Patch.
    /// The Content-Type should be application/fhir+json with a Parameters resource.
    #[tokio::test]
    async fn test_conditional_patch_fhirpath_format() {
        let backend = cassandra_backend!("test_conditional_patch_fhirpath_format");
        let tenant = create_tenant();

        // Create a resource
        let patient = json!({
            "resourceType": "Patient",
            "name": [{"family": "Original"}],
            "birthDate": "1990-01-15"
        });
        let created = backend
            .create(&tenant, "Patient", patient, FhirVersion::default_enabled())
            .await
            .unwrap();

        // FHIRPath Patch uses a Parameters resource with operations:
        // {
        //   "resourceType": "Parameters",
        //   "parameter": [
        //     {
        //       "name": "operation",
        //       "part": [
        //         {"name": "type", "valueCode": "replace"},
        //         {"name": "path", "valueString": "Patient.name.family"},
        //         {"name": "value", "valueString": "FHIRPathPatched"}
        //       ]
        //     }
        //   ]
        // }
        //
        // This would use FHIRPath expressions for the path.

        // Simulate the expected outcome with regular update
        let mut patched = created.content().clone();
        patched["name"][0]["family"] = json!("FHIRPathPatched");

        let updated = backend.update(&tenant, &created, patched).await.unwrap();
        assert_eq!(updated.content()["name"][0]["family"], "FHIRPathPatched");
    }

    /// Test conditional patch respects tenant isolation.
    #[tokio::test]
    async fn test_conditional_patch_tenant_isolation() {
        let backend = cassandra_backend!("test_conditional_patch_tenant_isolation");

        let tenant1 = helios_persistence::tenant::TenantContext::new(
            unique_tenant_id("patch-tenant-1"),
            helios_persistence::tenant::TenantPermissions::full_access(),
        );
        let tenant2 = helios_persistence::tenant::TenantContext::new(
            unique_tenant_id("patch-tenant-2"),
            helios_persistence::tenant::TenantPermissions::full_access(),
        );

        // Create resource in tenant1
        let patient =
            create_patient_json("TenantPatch", Some(("http://example.org/mrn", "T1-001")));
        let created = backend
            .create(&tenant1, "Patient", patient, FhirVersion::default_enabled())
            .await
            .unwrap();
        let id = created.id().to_string();

        // Verify tenant2 cannot read it
        let read_t2 = backend.read(&tenant2, "Patient", &id).await.unwrap();
        assert!(
            read_t2.is_none(),
            "Tenant2 should not see tenant1's resource"
        );

        // Conditional patch from tenant2 should not find the resource
        // PATCH /Patient?identifier=http://example.org/mrn|T1-001
        // (from tenant2's context)
        //
        // This should return "no match" because the resource belongs to tenant1.

        // Verify tenant1 can still access it
        let read_t1 = backend.read(&tenant1, "Patient", &id).await.unwrap();
        assert!(read_t1.is_some(), "Tenant1 should see their own resource");
    }
}

/// Cassandra's one answer where an upstream assertion accepts two (the Step 3
/// matrix's T-4, T-9 and T-14). Each is on [`parity`]'s list of additions.
pub mod strict {
    use helios_fhir::FhirVersion;
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
                FhirVersion::default_enabled(),
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
                FhirVersion::default_enabled(),
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
                FhirVersion::default_enabled(),
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
                FhirVersion::default_enabled(),
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
                FhirVersion::default_enabled(),
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
                FhirVersion::default_enabled(),
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
    /// `main` at `e32b02184`.
    const UPSTREAM_CONTENT: &[(&str, u64)] = &[
        ("conditional_tests", 0xdd39_0a52_5f94_05fa),
        ("create_tests", 0x9d81_515d_caeb_fa4f),
        ("delete_tests", 0xc658_fd7c_28d4_4415),
        ("mod", 0xd810_8d28_a3c0_423e),
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
