//! Roadmap Step 4 (row 4.1): upstream's versioning suite, `tests/versioning/`,
//! as an adapted copy that runs on the Cassandra node, the way
//! `tests/cassandra/crud.rs` copies `tests/crud/` (ADR-aacruzgon-0057).
//!
//! Upstream's `versioning_suite.rs` is `#![cfg(feature = "sqlite")]`, and every
//! test in `tests/versioning/` builds its own `SqliteBackend::in_memory()`
//! under fixed tenants (`test-tenant`, `tenant-1`, `tenant-2`). On the shared
//! node those tenants would collide across tests and across runs
//! (ADR-aacruzgon-0020, rule 2). So this file holds one module per upstream
//! file it copies, with the same test names and bodies, and these changes
//! only:
//!
//! - **the backend** is a [`CassandraBackend`](helios_persistence::backends::cassandra::CassandraBackend)
//!   on the test node ([`crate::backend_skeleton::backend`]), and a test skips
//!   when there is no node, as every Cassandra test does;
//! - **every tenant is fresh** ([`unique_tenant`](crate::cassandra_node::unique_tenant)),
//!   in place of `test-tenant`, `tenant-1` and `tenant-2`;
//! - **[`test_fhir_version`](crate::cassandra_node::test_fhir_version)** in
//!   place of upstream's `FhirVersion::default()`, as in the CRUD copy
//!   (ADR-aacruzgon-0065);
//! - **`test_vread_returns_correct_metadata` is not copied.** Upstream
//!   ignores it for #306 (SQLite has no `created_at` column). On Cassandra
//!   every assertion but its last holds; the last requires versions 1 and 2
//!   to share `created_at`, but `resource_versions` has no `created_at`, so
//!   `vread` gives each version its own `last_updated` as its `created_at`,
//!   as PostgreSQL does (walkthrough G14, signed off in ADR-aacruzgon-0028).
//!   `versions.rs`'s `vread_answers_each_row_of_the_walkthrough_table` pins
//!   that answer. [`parity`] lists it on `EXCLUDED`, with the reason.
//!
//! **`history_tests.rs` is not copied.** Its 18 tests call
//! `history_instance`, `history_type` and `history_system`, which Cassandra
//! refuses by design (`backends/cassandra/stubs.rs`; roadmap Step 4,
//! "Traps"). [`parity`] lists it, with the reason, and still pins its
//! content, so a change upstream is noticed.
//!
//! [`parity`] reads `tests/versioning/` when it runs, so an upstream sync that
//! adds, renames or removes a versioning test, or changes a file, fails it
//! until this copy follows.
//!
//! The methods these tests call are `vread`, `update_with_match` and
//! `current_version`'s neighbours on `VersionedStorage` (Steps 3 and 4), and
//! `create`, `read`, `update`, `delete` and `create_or_update`.

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

pub mod vread_tests {
    //! Tests for version read (vread) operations.
    //!
    //! This module tests the `vread` method of the VersionedStorage trait
    //! which reads a specific version of a resource.

    use serde_json::json;

    use helios_persistence::core::{ResourceStorage, VersionedStorage};
    use helios_persistence::tenant::TenantContext;

    use crate::cassandra_node::unique_tenant;

    // ============================================================================
    // Helper Functions
    // ============================================================================

    fn create_tenant() -> TenantContext {
        unique_tenant("versioning")
    }

    fn create_patient_json(name: &str) -> serde_json::Value {
        json!({
            "resourceType": "Patient",
            "name": [{"family": name}],
            "active": true
        })
    }

    // ============================================================================
    // VRead Tests - Basic
    // ============================================================================

    /// Test reading a specific version of a resource.
    #[tokio::test]
    async fn test_vread_specific_version() {
        let backend = cassandra_backend!("test_vread_specific_version");
        let tenant = create_tenant();

        // Create a resource (version 1)
        let patient = create_patient_json("Version1");
        let v1 = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Update to create version 2
        let mut content2 = v1.content().clone();
        content2["name"][0]["family"] = json!("Version2");
        let v2 = backend.update(&tenant, &v1, content2).await.unwrap();

        // Update to create version 3
        let mut content3 = v2.content().clone();
        content3["name"][0]["family"] = json!("Version3");
        let _v3 = backend.update(&tenant, &v2, content3).await.unwrap();

        // VRead version 1
        let read_v1 = backend
            .vread(&tenant, "Patient", v1.id(), "1")
            .await
            .unwrap();
        assert!(read_v1.is_some());
        let read_v1 = read_v1.unwrap();
        assert_eq!(read_v1.version_id(), "1");
        assert_eq!(read_v1.content()["name"][0]["family"], "Version1");

        // VRead version 2
        let read_v2 = backend
            .vread(&tenant, "Patient", v1.id(), "2")
            .await
            .unwrap();
        assert!(read_v2.is_some());
        let read_v2 = read_v2.unwrap();
        assert_eq!(read_v2.version_id(), "2");
        assert_eq!(read_v2.content()["name"][0]["family"], "Version2");

        // VRead version 3
        let read_v3 = backend
            .vread(&tenant, "Patient", v1.id(), "3")
            .await
            .unwrap();
        assert!(read_v3.is_some());
        let read_v3 = read_v3.unwrap();
        assert_eq!(read_v3.version_id(), "3");
        assert_eq!(read_v3.content()["name"][0]["family"], "Version3");
    }

    // ============================================================================
    // VRead Tests - Error Cases
    // ============================================================================

    /// Test vread of nonexistent resource.
    #[tokio::test]
    async fn test_vread_nonexistent_resource() {
        let backend = cassandra_backend!("test_vread_nonexistent_resource");
        let tenant = create_tenant();

        let result = backend
            .vread(&tenant, "Patient", "nonexistent", "1")
            .await
            .unwrap();

        assert!(result.is_none());
    }

    /// Test vread of nonexistent version.
    #[tokio::test]
    async fn test_vread_nonexistent_version() {
        let backend = cassandra_backend!("test_vread_nonexistent_version");
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

        // Try to read version 99 which doesn't exist
        let result = backend
            .vread(&tenant, "Patient", v1.id(), "99")
            .await
            .unwrap();

        assert!(result.is_none());
    }

    /// Test vread with invalid version format.
    #[tokio::test]
    async fn test_vread_invalid_version_format() {
        let backend = cassandra_backend!("test_vread_invalid_version_format");
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

        // Try various invalid version formats
        let result = backend.vread(&tenant, "Patient", v1.id(), "invalid").await;

        // Should either return None or an error, depending on implementation
        // Most implementations will return None for unparseable versions
        match result {
            Ok(None) => {}
            Ok(Some(_)) => panic!("Should not find resource with invalid version"),
            Err(_) => {} // Also acceptable
        }
    }

    // ============================================================================
    // VRead Tests - Tenant Isolation
    // ============================================================================

    /// Test that vread respects tenant isolation.
    #[tokio::test]
    async fn test_vread_tenant_isolation() {
        let backend = cassandra_backend!("test_vread_tenant_isolation");

        let tenant1 = unique_tenant("versioning-1");
        let tenant2 = unique_tenant("versioning-2");

        // Create resource in tenant1
        let patient = create_patient_json("Smith");
        let v1 = backend
            .create(
                &tenant1,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        // Try to vread from tenant2
        let result = backend
            .vread(&tenant2, "Patient", v1.id(), "1")
            .await
            .unwrap();

        assert!(result.is_none(), "Should not see other tenant's resource");
    }

    // ============================================================================
    // VRead Tests - After Delete
    // ============================================================================

    /// Test vread of deleted resource (should still return version).
    #[tokio::test]
    async fn test_vread_after_delete() {
        let backend = cassandra_backend!("test_vread_after_delete");
        let tenant = create_tenant();

        // Create and update a resource
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
        let _v2 = backend
            .update(&tenant, &v1, v1.content().clone())
            .await
            .unwrap();

        // Delete the resource
        backend.delete(&tenant, "Patient", v1.id()).await.unwrap();

        // VRead should still return the historical versions
        // (This is a key feature of FHIR vread - history is preserved)
        let read_v1 = backend
            .vread(&tenant, "Patient", v1.id(), "1")
            .await
            .unwrap();
        let read_v2 = backend
            .vread(&tenant, "Patient", v1.id(), "2")
            .await
            .unwrap();

        // Note: Behavior may vary by implementation
        // Some backends preserve history, others don't
        // This test documents expected FHIR behavior
        if let Some(v1_read) = read_v1 {
            assert_eq!(v1_read.version_id(), "1");
        }
        if let Some(v2_read) = read_v2 {
            assert_eq!(v2_read.version_id(), "2");
        }
    }

    /// Test that vread of deleted version returns deleted marker.
    #[tokio::test]
    async fn test_vread_deleted_version() {
        let backend = cassandra_backend!("test_vread_deleted_version");
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
        backend.delete(&tenant, "Patient", v1.id()).await.unwrap();

        // The delete creates a new version (v2) with deleted marker
        let deleted_version = backend
            .vread(&tenant, "Patient", v1.id(), "2")
            .await
            .unwrap();

        // If history is preserved, the deleted version should be readable
        // and marked as deleted
        if let Some(deleted) = deleted_version {
            assert!(deleted.is_deleted());
            assert_eq!(deleted.version_id(), "2");
        }
    }

    // ============================================================================
    // VRead Tests - Many Versions
    // ============================================================================

    /// Test vread with many versions.
    #[tokio::test]
    async fn test_vread_many_versions() {
        let backend = cassandra_backend!("test_vread_many_versions");
        let tenant = create_tenant();

        // Create initial version
        let patient = create_patient_json("Version0");
        let mut current = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let id = current.id().to_string();

        // Create 19 more versions (total 20)
        for i in 1..20 {
            let mut content = current.content().clone();
            content["name"][0]["family"] = json!(format!("Version{}", i));
            current = backend.update(&tenant, &current, content).await.unwrap();
        }

        // Verify we can read all versions
        for version in 1..=20 {
            let read = backend
                .vread(&tenant, "Patient", &id, &version.to_string())
                .await
                .unwrap();
            assert!(read.is_some(), "Version {} should exist", version);
            let read = read.unwrap();
            assert_eq!(read.version_id(), version.to_string());
            assert_eq!(
                read.content()["name"][0]["family"],
                format!("Version{}", version - 1)
            );
        }

        // Version 21 should not exist
        let read = backend.vread(&tenant, "Patient", &id, "21").await.unwrap();
        assert!(read.is_none());
    }

    // ============================================================================
    // VRead Tests - Resource Types
    // ============================================================================

    /// Test vread works across different resource types.
    #[tokio::test]
    async fn test_vread_different_resource_types() {
        let backend = cassandra_backend!("test_vread_different_resource_types");
        let tenant = create_tenant();

        // Create Patient with multiple versions
        let patient = create_patient_json("Smith");
        let patient_v1 = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let _patient_v2 = backend
            .update(&tenant, &patient_v1, patient_v1.content().clone())
            .await
            .unwrap();

        // Create Observation with multiple versions
        let obs = json!({
            "resourceType": "Observation",
            "status": "preliminary",
            "code": {"coding": [{"code": "test"}]}
        });
        let obs_v1 = backend
            .create(
                &tenant,
                "Observation",
                obs,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let mut obs_content = obs_v1.content().clone();
        obs_content["status"] = json!("final");
        let _obs_v2 = backend.update(&tenant, &obs_v1, obs_content).await.unwrap();

        // VRead both versions of Patient
        let p_read_v1 = backend
            .vread(&tenant, "Patient", patient_v1.id(), "1")
            .await
            .unwrap()
            .unwrap();
        let p_read_v2 = backend
            .vread(&tenant, "Patient", patient_v1.id(), "2")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(p_read_v1.resource_type(), "Patient");
        assert_eq!(p_read_v2.resource_type(), "Patient");

        // VRead both versions of Observation
        let o_read_v1 = backend
            .vread(&tenant, "Observation", obs_v1.id(), "1")
            .await
            .unwrap()
            .unwrap();
        let o_read_v2 = backend
            .vread(&tenant, "Observation", obs_v1.id(), "2")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(o_read_v1.resource_type(), "Observation");
        assert_eq!(o_read_v1.content()["status"], "preliminary");
        assert_eq!(o_read_v2.resource_type(), "Observation");
        assert_eq!(o_read_v2.content()["status"], "final");
    }
}

pub mod optimistic_locking_tests {
    //! Tests for optimistic locking via If-Match and ETag.
    //!
    //! This module tests the `update_with_match` method of the VersionedStorage trait
    //! which implements HTTP If-Match semantics for concurrent update prevention.

    use serde_json::json;

    use helios_persistence::core::{ResourceStorage, VersionedStorage};
    use helios_persistence::error::{ConcurrencyError, StorageError};
    use helios_persistence::tenant::TenantContext;

    use crate::cassandra_node::unique_tenant;

    // ============================================================================
    // Helper Functions
    // ============================================================================

    fn create_tenant() -> TenantContext {
        unique_tenant("versioning")
    }

    fn create_patient_json(name: &str) -> serde_json::Value {
        json!({
            "resourceType": "Patient",
            "name": [{"family": name}],
            "active": true
        })
    }

    // ============================================================================
    // Optimistic Locking Tests - Basic
    // ============================================================================

    /// Test that update_with_match succeeds when ETag matches.
    #[tokio::test]
    async fn test_update_with_match_success() {
        let backend = cassandra_backend!("test_update_with_match_success");
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
        let etag = created.etag().to_string();

        // Update with matching ETag
        let mut content = created.content().clone();
        content["name"][0]["family"] = json!("Jones");

        let result = backend
            .update_with_match(&tenant, "Patient", created.id(), &etag, content)
            .await;

        assert!(result.is_ok());
        let updated = result.unwrap();
        assert_eq!(updated.version_id(), "2");
        assert_eq!(updated.content()["name"][0]["family"], "Jones");
    }

    /// Test that update_with_match fails when ETag doesn't match.
    #[tokio::test]
    async fn test_update_with_match_etag_mismatch() {
        let backend = cassandra_backend!("test_update_with_match_etag_mismatch");
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

        // Update to create v2
        let v2 = backend
            .update(&tenant, &v1, v1.content().clone())
            .await
            .unwrap();

        // Try to update using stale v1 ETag
        let mut content = v2.content().clone();
        content["name"][0]["family"] = json!("Jones");

        let result = backend
            .update_with_match(&tenant, "Patient", v1.id(), v1.etag(), content)
            .await;

        assert!(result.is_err());
        match result {
            Err(StorageError::Concurrency(ConcurrencyError::VersionConflict { .. })) => {}
            Err(e) => panic!("Expected VersionConflict error, got {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    /// Test various ETag formats are accepted.
    #[tokio::test]
    async fn test_update_with_match_etag_formats() {
        let backend = cassandra_backend!("test_update_with_match_etag_formats");
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

        // Test different ETag formats that should all be equivalent
        let etag_formats = vec![
            r#"W/"1""#.to_string(), // Weak ETag with quotes
            r#""1""#.to_string(),   // Strong ETag with quotes
            "1".to_string(),        // Just the version
        ];

        for etag in etag_formats {
            let read = backend
                .read(&tenant, "Patient", created.id())
                .await
                .unwrap()
                .unwrap();

            let result = backend
                .update_with_match(
                    &tenant,
                    "Patient",
                    created.id(),
                    &etag,
                    read.content().clone(),
                )
                .await;

            // Should succeed with any valid format
            if result.is_ok() {
                // Reset for next test
                break; // Only need one to succeed to validate format handling
            }
        }
    }

    // ============================================================================
    // Optimistic Locking Tests - Concurrent Updates
    // ============================================================================

    /// Test that concurrent updates are properly serialized.
    #[tokio::test]
    async fn test_concurrent_update_serialization() {
        let backend = cassandra_backend!("test_concurrent_update_serialization");
        let tenant = create_tenant();

        let patient = create_patient_json("Original");
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

        // Simulate two users reading at the same time
        let user1_read = backend
            .read(&tenant, "Patient", &id)
            .await
            .unwrap()
            .unwrap();
        let user2_read = backend
            .read(&tenant, "Patient", &id)
            .await
            .unwrap()
            .unwrap();

        // User 1 updates first
        let mut user1_content = user1_read.content().clone();
        user1_content["name"][0]["family"] = json!("User1Edit");
        let user1_result = backend
            .update_with_match(&tenant, "Patient", &id, user1_read.etag(), user1_content)
            .await;
        assert!(user1_result.is_ok());

        // User 2 tries to update with stale ETag
        let mut user2_content = user2_read.content().clone();
        user2_content["name"][0]["family"] = json!("User2Edit");
        let user2_result = backend
            .update_with_match(&tenant, "Patient", &id, user2_read.etag(), user2_content)
            .await;

        // User 2 should fail
        assert!(user2_result.is_err());
        match user2_result {
            Err(StorageError::Concurrency(_)) => {}
            Err(e) => panic!("Expected ConcurrencyError, got {:?}", e),
            Ok(_) => panic!("Expected error"),
        }

        // Final state should be User1's edit
        let final_state = backend
            .read(&tenant, "Patient", &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_state.content()["name"][0]["family"], "User1Edit");
    }

    /// Test that the second concurrent updater can retry with fresh ETag.
    #[tokio::test]
    async fn test_concurrent_update_retry() {
        let backend = cassandra_backend!("test_concurrent_update_retry");
        let tenant = create_tenant();

        let patient = create_patient_json("Original");
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

        // User 1 reads
        let user1_read = backend
            .read(&tenant, "Patient", &id)
            .await
            .unwrap()
            .unwrap();

        // User 2 reads
        let user2_read = backend
            .read(&tenant, "Patient", &id)
            .await
            .unwrap()
            .unwrap();

        // User 1 updates
        let mut user1_content = user1_read.content().clone();
        user1_content["name"][0]["family"] = json!("User1Edit");
        backend
            .update_with_match(&tenant, "Patient", &id, user1_read.etag(), user1_content)
            .await
            .unwrap();

        // User 2's first attempt fails
        let mut user2_content = user2_read.content().clone();
        user2_content["name"][0]["family"] = json!("User2Edit");
        let first_attempt = backend
            .update_with_match(
                &tenant,
                "Patient",
                &id,
                user2_read.etag(),
                user2_content.clone(),
            )
            .await;
        assert!(first_attempt.is_err());

        // User 2 refreshes and retries
        let user2_refresh = backend
            .read(&tenant, "Patient", &id)
            .await
            .unwrap()
            .unwrap();
        let mut retry_content = user2_refresh.content().clone();
        retry_content["name"][0]["family"] = json!("User2Edit");

        let retry_result = backend
            .update_with_match(&tenant, "Patient", &id, user2_refresh.etag(), retry_content)
            .await;

        assert!(retry_result.is_ok());
        let final_state = backend
            .read(&tenant, "Patient", &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_state.content()["name"][0]["family"], "User2Edit");
        assert_eq!(final_state.version_id(), "3");
    }

    // ============================================================================
    // Optimistic Locking Tests - Error Cases
    // ============================================================================

    /// Test update_with_match on nonexistent resource.
    #[tokio::test]
    async fn test_update_with_match_nonexistent() {
        let backend = cassandra_backend!("test_update_with_match_nonexistent");
        let tenant = create_tenant();

        let result = backend
            .update_with_match(
                &tenant,
                "Patient",
                "nonexistent",
                "W/\"1\"",
                create_patient_json("Smith"),
            )
            .await;

        assert!(result.is_err());
    }

    /// Test update_with_match on deleted resource.
    #[tokio::test]
    async fn test_update_with_match_deleted() {
        let backend = cassandra_backend!("test_update_with_match_deleted");
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
        let etag = created.etag().to_string();

        // Delete the resource
        backend.delete(&tenant, "Patient", &id).await.unwrap();

        // Try to update with old ETag
        let result = backend
            .update_with_match(&tenant, "Patient", &id, &etag, create_patient_json("Jones"))
            .await;

        assert!(result.is_err());
    }

    // ============================================================================
    // ETag Matching Tests
    // ============================================================================

    /// Test that ETag is correctly generated for each version.
    #[tokio::test]
    async fn test_etag_generation() {
        let backend = cassandra_backend!("test_etag_generation");
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
        let v2 = backend
            .update(&tenant, &v1, v1.content().clone())
            .await
            .unwrap();
        let v3 = backend
            .update(&tenant, &v2, v2.content().clone())
            .await
            .unwrap();

        // Each version should have a unique ETag
        assert!(v1.etag().contains("1"));
        assert!(v2.etag().contains("2"));
        assert!(v3.etag().contains("3"));

        assert_ne!(v1.etag(), v2.etag());
        assert_ne!(v2.etag(), v3.etag());
    }

    /// Test ETag matching helper method.
    #[tokio::test]
    async fn test_stored_resource_matches_etag() {
        let backend = cassandra_backend!("test_stored_resource_matches_etag");
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

        // Should match various formats
        assert!(created.matches_etag("W/\"1\""));
        assert!(created.matches_etag("\"1\""));
        assert!(created.matches_etag("1"));

        // Should not match wrong versions
        assert!(!created.matches_etag("W/\"2\""));
        assert!(!created.matches_etag("\"2\""));
        assert!(!created.matches_etag("2"));
    }

    // ============================================================================
    // Optimistic Locking Tests - Sequential Updates
    // ============================================================================

    /// Test sequential updates with proper ETag management.
    #[tokio::test]
    async fn test_sequential_updates_with_etag() {
        let backend = cassandra_backend!("test_sequential_updates_with_etag");
        let tenant = create_tenant();

        let patient = create_patient_json("Version0");
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
            content["name"][0]["family"] = json!(format!("Version{}", i));

            current = backend
                .update_with_match(&tenant, "Patient", current.id(), current.etag(), content)
                .await
                .unwrap_or_else(|e| panic!("Update {i} should succeed: {e:?}"));

            assert_eq!(current.version_id(), (i + 1).to_string());
        }
    }

    // ============================================================================
    // If-None-Match Tests (Conditional Create)
    // ============================================================================

    /// Test If-None-Match: * semantics (only create if not exists).
    #[tokio::test]
    async fn test_if_none_match_create_only() {
        let backend = cassandra_backend!("test_if_none_match_create_only");
        let tenant = create_tenant();

        // First create succeeds (resource doesn't exist)
        let patient1 = create_patient_json("First");
        let (created, is_new) = backend
            .create_or_update(
                &tenant,
                "Patient",
                "test-id",
                patient1,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        assert!(is_new);
        assert_eq!(created.version_id(), "1");

        // Second create with same ID updates instead
        let patient2 = create_patient_json("Second");
        let (updated, is_new2) = backend
            .create_or_update(
                &tenant,
                "Patient",
                "test-id",
                patient2,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();

        assert!(!is_new2);
        assert_eq!(updated.version_id(), "2");
    }

    // ============================================================================
    // Stress Tests
    // ============================================================================

    /// Test rapid sequential updates maintain correct versioning.
    #[tokio::test]
    async fn test_rapid_sequential_updates() {
        let backend = cassandra_backend!("test_rapid_sequential_updates");
        let tenant = create_tenant();

        let patient = create_patient_json("Initial");
        let mut current = backend
            .create(
                &tenant,
                "Patient",
                patient,
                crate::cassandra_node::test_fhir_version(),
            )
            .await
            .unwrap();
        let id = current.id().to_string();

        // Do 100 rapid updates
        for i in 1..=100 {
            let mut content = current.content().clone();
            content["active"] = json!(i % 2 == 0);

            current = backend
                .update_with_match(&tenant, "Patient", &id, current.etag(), content)
                .await
                .unwrap_or_else(|e| panic!("Update {i} should succeed: {e:?}"));
        }

        // Final version should be 101
        let final_state = backend
            .read(&tenant, "Patient", &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(final_state.version_id(), "101");
    }
}

/// Keeps the copy in step with `tests/versioning/` (as `crud.rs`'s `parity`
/// does for `tests/crud/`).
pub mod parity {
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::Path;

    /// Upstream files deliberately left out of the copy, with the reason.
    const EXCLUDED_FILES: &[(&str, &str)] = &[(
        "history_tests",
        "every test calls history_instance, history_type or history_system, which \
         Cassandra refuses by design (backends/cassandra/stubs.rs; roadmap Step 4, Traps)",
    )];

    /// Upstream tests of a copied file deliberately left out, with the
    /// reason.
    const EXCLUDED: &[(&str, &str, &str)] = &[(
        "vread_tests",
        "test_vread_returns_correct_metadata",
        "G14: resource_versions has no created_at, so vread gives each version its own \
         last_updated as created_at, as PostgreSQL does; the test's last assertion needs \
         versions 1 and 2 to share it. versions.rs's \
         vread_answers_each_row_of_the_walkthrough_table pins Cassandra's answer",
    )];

    /// The content of each upstream file, copied or excluded, as a 64-bit
    /// FNV-1a hash of its text with `\r\n` read as `\n` (the same hash as
    /// `crud.rs`'s `UPSTREAM_CONTENT`). A changed file fails here until the
    /// copy follows it: bring the change into the module of the same name
    /// (or check the exclusion still holds), then update the hash. Baseline:
    /// `main` at `e5e8ea67f`.
    const UPSTREAM_CONTENT: &[(&str, u64)] = &[
        ("history_tests", 0xcb81_d113_95cc_428b),
        ("mod", 0xdb24_0c93_e1f6_f71e),
        ("optimistic_locking_tests", 0xcf66_4819_1c31_7f1d),
        ("vread_tests", 0xd54e_83fb_d307_8ee7),
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

    /// The test functions of `source`, as `crud.rs`'s `parity::tests` finds
    /// them: every `fn` whose nearest preceding line that is not a comment,
    /// an `#[ignore…]` or a `#[cfg(…)]` is a test attribute, with `ignored`
    /// set when an `#[ignore…]` line sits between.
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

    /// The text of each top-level `pub mod <name> {` block of the copy, up to
    /// the next one.
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

    /// Every upstream versioning test is either copied, by name, into the
    /// module of its file, or excluded with a reason (its own, or its whole
    /// file's); the copy has no test upstream lacks, and none is ignored; and
    /// no upstream file changed since the copy was made ([`UPSTREAM_CONTENT`]).
    #[test]
    fn versioning_copy_matches_upstream() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
        let copy = read(&root.join("cassandra/versioning.rs"));
        let modules = copy_modules(&copy);

        let mut upstream_files = BTreeSet::new();
        let mut upstream_total = 0;
        let mut copied_total = 0;
        let mut excluded_total = 0;
        let entries = std::fs::read_dir(root.join("versioning")).expect("read tests/versioning");
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
                "tests/versioning/{stem}.rs is new or changed since \
                 tests/cassandra/versioning.rs was copied from it: bring the change into the \
                 copy (or check its exclusion still holds), then update UPSTREAM_CONTENT"
            );
            if stem == "mod" {
                continue;
            }
            upstream_files.insert(stem.to_owned());
            let theirs = tests(&upstream);
            upstream_total += theirs.len();
            if EXCLUDED_FILES.iter().any(|(file, _)| *file == stem) {
                assert!(
                    !modules.contains_key(stem),
                    "tests/versioning/{stem}.rs is excluded, but the copy has `pub mod {stem}`"
                );
                excluded_total += theirs.len();
                continue;
            }
            let Some(module) = modules.get(stem) else {
                panic!(
                    "tests/versioning/{stem}.rs has no `pub mod {stem}` in \
                     tests/cassandra/versioning.rs, and is not on EXCLUDED_FILES"
                );
            };
            let ours = tests(module);
            copied_total += ours.len();
            for name in theirs.keys() {
                let excluded = EXCLUDED.iter().any(|(f, t, _)| *f == stem && t == name);
                excluded_total += usize::from(excluded);
                assert!(
                    ours.contains_key(name) != excluded,
                    "tests/versioning/{stem}.rs::{name} needs exactly one of: a copy in \
                     tests/cassandra/versioning.rs::{stem}, or a reason on EXCLUDED"
                );
            }
            for (name, ignored) in &ours {
                assert!(
                    theirs.contains_key(name),
                    "versioning.rs::{stem}::{name} is not in tests/versioning/{stem}.rs: \
                     renamed or removed upstream?"
                );
                assert!(!ignored, "versioning.rs::{stem}::{name} is ignored");
            }
        }
        for (file, _) in UPSTREAM_CONTENT {
            assert!(
                *file == "mod" || upstream_files.contains(*file),
                "tests/versioning/{file}.rs is gone upstream"
            );
        }
        for (file, reason) in EXCLUDED_FILES {
            assert!(!reason.is_empty() && upstream_files.contains(*file));
        }
        for (file, test, reason) in EXCLUDED {
            assert!(
                !reason.is_empty() && upstream_files.contains(*file),
                "EXCLUDED {file}::{test} needs a reason and an upstream file"
            );
        }

        // The copy has no module upstream lacks, beyond this one.
        for name in modules.keys() {
            assert!(
                upstream_files.contains(name) || name == "parity",
                "versioning.rs::{name} matches no file in tests/versioning/"
            );
        }
        // 40 at the 2026-11-20 baseline: 21 copied; 19 excluded, the 18 of
        // history_tests.rs and test_vread_returns_correct_metadata.
        assert_eq!(copied_total + excluded_total, upstream_total);
    }
}
