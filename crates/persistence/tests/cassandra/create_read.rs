//! Roadmap Step 2, item 2.1 (layer L4): `create`, `read` and `count`
//! against a running node.
//!
//! The node is `HFS_TEST_CASSANDRA_URL` (`host:port`) with the schema applied
//! (ADR-aacruzgon-0020). Every test writes under its own tenant, so tests in
//! parallel and data left by earlier runs never meet (rule 2). Where a test
//! must look at cells, it reads them with its own `cdrs-tokio` session, and
//! the one setup write it makes (a delete marker, until Step 3 has `delete`)
//! is itself a compare-and-swap, as every write to `resources` must be.

use std::collections::BTreeSet;

use cdrs_tokio::cluster::session::{Session, SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::{NodeTcpConfigBuilder, TcpConnectionManager};
use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::frame::Version;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::query::QueryValues;
use cdrs_tokio::statement::StatementParamsBuilder;
use cdrs_tokio::transport::TransportTcp;
use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::blob::Blob;
use cdrs_tokio::types::rows::Row;
use cdrs_tokio::types::value::Value;
use chrono::{DateTime, Utc};
use helios_fhir::FhirVersion;
use helios_persistence::backends::cassandra::{CassandraBackend, CassandraConfig};
use helios_persistence::core::ResourceStorage;
use helios_persistence::error::{ResourceError, StorageError, TenantError, ValidationError};
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use serde_json::json;

type CqlSession = Session<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

/// A non-null column, typed.
fn col<T>(row: &Row, name: &str) -> T
where
    Row: IntoRustByName<T>,
{
    IntoRustByName::<T>::get_r_by_name(row, name).unwrap_or_else(|e| panic!("{name}: {e}"))
}

/// A column that may be null, typed.
fn opt<T>(row: &Row, name: &str) -> Option<T>
where
    Row: IntoRustByName<T>,
{
    IntoRustByName::<T>::get_by_name(row, name).unwrap_or_else(|e| panic!("{name}: {e}"))
}

fn test_node_url() -> Option<String> {
    std::env::var("HFS_TEST_CASSANDRA_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
}

/// The backend and a raw CQL session on the same node, or `None` to skip.
async fn setup() -> Option<(CassandraBackend, CqlSession)> {
    let Some(url) = test_node_url() else {
        eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
        return None;
    };
    let backend = CassandraBackend::connect(CassandraConfig {
        contact_points: vec![url.clone()],
        data_dir: Some(concat!(env!("CARGO_MANIFEST_DIR"), "/../../data").into()),
        ..CassandraConfig::default()
    })
    .await
    .unwrap_or_else(|e| panic!("connect to {url}: {e}"));
    let node = NodeTcpConfigBuilder::new()
        .with_contact_points(vec![url.as_str().into()])
        .with_version(Version::V5)
        .build()
        .await
        .expect("node config");
    let cql = TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), node)
        .with_keyspace(backend.config().keyspace.clone())
        .build()
        .await
        .expect("CQL session");
    Some((backend, cql))
}

fn own_tenant(test: &str) -> TenantContext {
    let id = format!("s2-create-{test}-{}", uuid::Uuid::new_v4().simple());
    TenantContext::new(TenantId::new(id), TenantPermissions::full_access())
}

/// Runs `cql` at `consistency` and returns its rows.
async fn query(
    session: &CqlSession,
    cql: &str,
    values: Vec<Value>,
    consistency: Consistency,
) -> Vec<Row> {
    let params = StatementParamsBuilder::new()
        .with_consistency(consistency)
        .with_values(QueryValues::SimpleValues(values))
        .build();
    session
        .query_with_params(cql, params)
        .await
        .unwrap_or_else(|e| panic!("{cql}: {e}"))
        .response_body()
        .expect("body")
        .into_rows()
        .unwrap_or_default()
}

fn key(tenant: &TenantContext, resource_type: &str, id: &str) -> Vec<Value> {
    vec![
        Value::from(tenant.tenant_id().as_str()),
        Value::from(resource_type),
        Value::from(id),
    ]
}

fn patient(id: &str) -> serde_json::Value {
    json!({
        "resourceType": "Patient",
        "id": id,
        "gender": "male",
        "birthDate": "1980-05-01",
        "identifier": [{"system": "http://hospital.example/mrn", "value": "12345"}]
    })
}

#[tokio::test]
async fn cassandra_create_then_read_round_trips() {
    let Some((backend, _cql)) = setup().await else {
        return;
    };
    let tenant = own_tenant("round-trip");
    let version = FhirVersion::default_enabled();

    let created = backend
        .create(&tenant, "Patient", patient("p1"), version)
        .await
        .expect("create");
    assert_eq!(created.id(), "p1");
    assert_eq!(created.version_id(), "1");
    assert_eq!(created.etag(), "W/\"1\"");
    assert_eq!(created.fhir_version(), version);
    assert_eq!(
        created.last_modified().timestamp_subsec_nanos() % 1_000_000,
        0,
        "last_updated has millisecond precision"
    );

    let read = backend
        .read(&tenant, "Patient", "p1")
        .await
        .expect("read")
        .expect("found");
    assert_eq!(read.id(), "p1");
    assert_eq!(read.version_id(), "1");
    assert_eq!(read.etag(), "W/\"1\"");
    assert_eq!(read.fhir_version(), version);
    assert_eq!(read.last_modified(), created.last_modified());
    assert_eq!(read.content(), created.content());
    assert_eq!(read.content()["gender"], "male");
    assert!(read.deleted_at().is_none());
    // REST serves meta from the row, overriding any client value
    // (R4 http.html#create: the server SHALL ignore client meta.versionId).
    let served = read.content_with_meta();
    assert_eq!(served["meta"]["versionId"], "1");

    // Another tenant does not see it.
    let other = own_tenant("round-trip-other");
    assert!(
        backend
            .read(&other, "Patient", "p1")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn cassandra_create_assigns_an_id_when_the_resource_has_none() {
    let Some((backend, _cql)) = setup().await else {
        return;
    };
    let tenant = own_tenant("assigned-id");
    let created = backend
        .create(
            &tenant,
            "Patient",
            json!({"resourceType": "Patient"}),
            FhirVersion::default_enabled(),
        )
        .await
        .expect("create");
    assert!(!created.id().is_empty());
    assert_eq!(created.content()["id"], created.id());
    let read = backend
        .read(&tenant, "Patient", created.id())
        .await
        .unwrap()
        .expect("found");
    assert_eq!(read.id(), created.id());
}

#[tokio::test]
async fn cassandra_duplicate_create_is_already_exists() {
    let Some((backend, _cql)) = setup().await else {
        return;
    };
    let tenant = own_tenant("duplicate");
    let version = FhirVersion::default_enabled();
    backend
        .create(&tenant, "Patient", patient("dup"), version)
        .await
        .expect("first create");
    match backend
        .create(&tenant, "Patient", patient("dup"), version)
        .await
    {
        Err(StorageError::Resource(ResourceError::AlreadyExists { resource_type, id })) => {
            assert_eq!((resource_type.as_str(), id.as_str()), ("Patient", "dup"));
        }
        other => panic!("expected AlreadyExists, got {other:?}"),
    }
    // The first version is untouched.
    let read = backend
        .read(&tenant, "Patient", "dup")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.version_id(), "1");
}

#[tokio::test]
async fn cassandra_concurrent_creates_of_one_id_make_exactly_one_resource() {
    let Some((backend, _cql)) = setup().await else {
        return;
    };
    let tenant = own_tenant("race");
    let version = FhirVersion::default_enabled();
    let attempts = (0..8).map(|i| {
        let mut body = patient("race");
        body["gender"] = json!(if i % 2 == 0 { "male" } else { "female" });
        backend.create(&tenant, "Patient", body, version)
    });
    let results = futures::future::join_all(attempts).await;
    let created = results.iter().filter(|r| r.is_ok()).count();
    let refused = results
        .iter()
        .filter(|r| {
            matches!(
                r,
                Err(StorageError::Resource(ResourceError::AlreadyExists { .. }))
            )
        })
        .count();
    assert_eq!((created, refused), (1, 7), "{results:?}");
    assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 1);
}

#[tokio::test]
async fn cassandra_read_of_a_missing_id_is_none() {
    let Some((backend, _cql)) = setup().await else {
        return;
    };
    let tenant = own_tenant("missing");
    assert!(
        backend
            .read(&tenant, "Patient", "never-created")
            .await
            .expect("read")
            .is_none()
    );
}

#[tokio::test]
async fn cassandra_read_of_a_deleted_resource_is_gone() {
    let Some((backend, cql)) = setup().await else {
        return;
    };
    let tenant = own_tenant("gone");
    backend
        .create(
            &tenant,
            "Patient",
            patient("gone"),
            FhirVersion::default_enabled(),
        )
        .await
        .expect("create");

    // Until Step 3 has `delete`: a delete marker set through CQL, as C3 would
    // set it, and as a compare-and-swap, since every write to `resources` is.
    let deleted_at = Utc::now();
    let mut values = vec![Value::from(deleted_at), Value::from(2_i32)];
    values.extend(key(&tenant, "Patient", "gone"));
    values.push(Value::from(1_i32));
    let params = StatementParamsBuilder::new()
        .with_consistency(Consistency::LocalQuorum)
        .with_serial_consistency(Consistency::LocalSerial)
        .with_values(QueryValues::SimpleValues(values))
        .build();
    let applied = cql
        .query_with_params(
            "UPDATE resources SET deleted_at = ?, version_id = ? \
             WHERE tenant_id = ? AND resource_type = ? AND id = ? \
             IF version_id = ? AND deleted_at = 0",
            params,
        )
        .await
        .expect("mark deleted")
        .response_body()
        .unwrap()
        .into_rows()
        .unwrap();
    assert!(col::<bool>(&applied[0], "[applied]"));

    match backend.read(&tenant, "Patient", "gone").await {
        Err(StorageError::Resource(ResourceError::Gone {
            resource_type,
            id,
            deleted_at: Some(at),
        })) => {
            assert_eq!((resource_type.as_str(), id.as_str()), ("Patient", "gone"));
            assert_eq!(at.timestamp_millis(), deleted_at.timestamp_millis());
        }
        other => panic!("expected Gone, got {other:?}"),
    }
    assert!(!backend.exists(&tenant, "Patient", "gone").await.unwrap());
}

#[tokio::test]
async fn cassandra_create_writes_every_cell_and_no_null() {
    let Some((backend, cql)) = setup().await else {
        return;
    };
    let tenant = own_tenant("cells");
    let version = FhirVersion::default_enabled();
    let created = backend
        .create(&tenant, "Patient", patient("cells"), version)
        .await
        .expect("create");
    let lu_ms = created.last_modified().timestamp_millis();
    let ts = lu_ms * 1000;

    // `resources`: every column set; deleted_at is the live sentinel 0 (G39).
    let rows = query(
        &cql,
        "SELECT version_id, data, last_updated, created_at, deleted_at, fhir_version, \
         write_ts, write_id, index_keys \
         FROM resources WHERE tenant_id = ? AND resource_type = ? AND id = ?",
        key(&tenant, "Patient", "cells"),
        Consistency::LocalQuorum,
    )
    .await;
    let row = &rows[0];
    assert_eq!(col::<i32>(row, "version_id"), 1);
    assert!(opt::<String>(row, "data").is_some());
    let last_updated: DateTime<Utc> = row.get_r_by_name("last_updated").unwrap();
    assert_eq!(last_updated.timestamp_millis(), lu_ms);
    let created_at: DateTime<Utc> = row.get_r_by_name("created_at").unwrap();
    assert_eq!(created_at.timestamp_millis(), lu_ms);
    let deleted_at: DateTime<Utc> = row
        .get_by_name("deleted_at")
        .unwrap()
        .expect("deleted_at is written, never left unset");
    assert_eq!(deleted_at.timestamp_millis(), 0);
    assert_eq!(
        col::<String>(row, "fhir_version"),
        version.as_mime_param(),
        "fhir_version is stored as as_mime_param()"
    );
    assert_eq!(col::<i64>(row, "write_ts"), ts);
    let write_id: uuid::Uuid = row.get_r_by_name("write_id").unwrap();
    let index_keys = col::<Blob>(row, "index_keys").into_vec();
    assert_eq!(
        index_keys,
        vec![0x01, 0x01, 0x01],
        "K(1) = {{resources_by_type}}"
    );

    // `resource_versions` row 1: V1 at ts(1), deleted_at unset (not null).
    let mut version_key = key(&tenant, "Patient", "cells");
    version_key.push(Value::from(1_i32));
    let rows = query(
        &cql,
        "SELECT data, last_updated, deleted_at, fhir_version, write_ts, write_id, index_keys, \
         writetime(data) AS data_ts, writetime(index_keys) AS keys_ts \
         FROM resource_versions \
         WHERE tenant_id = ? AND resource_type = ? AND id = ? AND version_id = ?",
        version_key,
        Consistency::LocalQuorum,
    )
    .await;
    let row = &rows[0];
    assert_eq!(
        col::<String>(row, "data"),
        serde_json::to_string(created.content()).unwrap()
    );
    let version_lu: DateTime<Utc> = row.get_r_by_name("last_updated").unwrap();
    assert_eq!(version_lu.timestamp_millis(), lu_ms);
    assert!(
        opt::<DateTime<Utc>>(row, "deleted_at").is_none(),
        "a content row leaves deleted_at unset"
    );
    assert_eq!(col::<String>(row, "fhir_version"), version.as_mime_param());
    assert_eq!(col::<i64>(row, "write_ts"), ts);
    assert_eq!(col::<uuid::Uuid>(row, "write_id"), write_id);
    assert_eq!(col::<Blob>(row, "index_keys").into_vec(), index_keys);
    assert_eq!(col::<i64>(row, "data_ts"), ts, "USING TIMESTAMP ts(1)");
    assert_eq!(col::<i64>(row, "keys_ts"), ts);

    // `resources_by_type`: X1 at ts(1), tag 1, the resource's fhir_version.
    let rows = query(
        &cql,
        "SELECT id, last_updated, version_id, fhir_version, writetime(version_id) AS row_ts \
         FROM resources_by_type WHERE tenant_id = ? AND resource_type = ?",
        vec![
            Value::from(tenant.tenant_id().as_str()),
            Value::from("Patient"),
        ],
        Consistency::LocalQuorum,
    )
    .await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(col::<String>(row, "id"), "cells");
    let row_lu: DateTime<Utc> = row.get_r_by_name("last_updated").unwrap();
    assert_eq!(row_lu.timestamp_millis(), lu_ms);
    assert_eq!(col::<i32>(row, "version_id"), 1);
    assert_eq!(col::<String>(row, "fhir_version"), version.as_mime_param());
    assert_eq!(col::<i64>(row, "row_ts"), ts);

    // The intent row is gone: no row of this tenant in any shard of this hour
    // or the previous one.
    let now = Utc::now();
    let mut buckets = BTreeSet::new();
    for at in [now, now - chrono::Duration::hours(1)] {
        buckets.insert(at.format("%Y-%m-%dT%H").to_string());
    }
    for bucket in &buckets {
        for shard in 0..16_i16 {
            let rows = query(
                &cql,
                "SELECT tenant_id FROM pending_index_writes WHERE bucket = ? AND shard = ?",
                vec![Value::from(bucket.as_str()), Value::from(shard)],
                Consistency::LocalQuorum,
            )
            .await;
            assert!(
                rows.iter()
                    .all(|row| { col::<String>(row, "tenant_id") != tenant.tenant_id().as_str() }),
                "an intent row of this tenant is left in ({bucket}, {shard})"
            );
        }
    }
}

#[tokio::test]
async fn cassandra_count_scans_resources_by_type() {
    let Some((backend, _cql)) = setup().await else {
        return;
    };
    let tenant = own_tenant("count");
    let version = FhirVersion::default_enabled();
    assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 0);
    assert_eq!(backend.count(&tenant, None).await.unwrap(), 0);
    for id in ["c1", "c2", "c3"] {
        backend
            .create(&tenant, "Patient", patient(id), version)
            .await
            .expect("create");
    }
    backend
        .create(
            &tenant,
            "Observation",
            json!({"resourceType": "Observation", "status": "final",
                   "code": {"coding": [{"system": "http://loinc.org", "code": "8867-4"}]}}),
            version,
        )
        .await
        .expect("create");
    assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 3);
    assert_eq!(
        backend.count(&tenant, Some("Observation")).await.unwrap(),
        1
    );
    assert_eq!(backend.count(&tenant, Some("Encounter")).await.unwrap(), 0);
    assert_eq!(backend.count(&tenant, None).await.unwrap(), 4);
}

#[tokio::test]
async fn cassandra_create_many_seeds_through_create() {
    let Some((backend, _cql)) = setup().await else {
        return;
    };
    let tenant = own_tenant("many");
    let bodies: Vec<_> = (0..20).map(|i| patient(&format!("m{i}"))).collect();
    let results = backend
        .create_many(&tenant, "Patient", bodies, FhirVersion::default_enabled())
        .await;
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(backend.count(&tenant, Some("Patient")).await.unwrap(), 20);
}

#[tokio::test]
async fn cassandra_create_checks_permission_before_writing() {
    let Some((backend, _cql)) = setup().await else {
        return;
    };
    let writer = own_tenant("permission");
    let reader = TenantContext::new(writer.tenant_id().clone(), TenantPermissions::read_only());
    match backend
        .create(
            &reader,
            "Patient",
            patient("ro"),
            FhirVersion::default_enabled(),
        )
        .await
    {
        Err(StorageError::Tenant(TenantError::OperationNotPermitted { .. })) => {}
        other => panic!("expected OperationNotPermitted, got {other:?}"),
    }
    assert!(
        backend
            .read(&writer, "Patient", "ro")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(backend.count(&writer, Some("Patient")).await.unwrap(), 0);
}

#[tokio::test]
async fn cassandra_search_parameter_code_is_bounded_at_128_bytes() {
    let Some((backend, _cql)) = setup().await else {
        return;
    };
    let tenant = own_tenant("sp-code");
    let search_parameter = |id: &str, code: String| {
        json!({
            "resourceType": "SearchParameter",
            "id": id,
            "url": format!("http://example.org/SearchParameter/{id}"),
            "name": id,
            "status": "active",
            "description": "test",
            "code": code,
            "base": ["Patient"],
            "type": "token",
            "expression": "Patient.gender"
        })
    };
    let version = FhirVersion::default_enabled();
    match backend
        .create(
            &tenant,
            "SearchParameter",
            search_parameter("long", "c".repeat(129)),
            version,
        )
        .await
    {
        Err(StorageError::Validation(ValidationError::InvalidResource { details, .. })) => {
            assert_eq!(details[0].path, "SearchParameter.code");
        }
        other => panic!("expected InvalidResource, got {other:?}"),
    }
    assert!(
        backend
            .read(&tenant, "SearchParameter", "long")
            .await
            .unwrap()
            .is_none()
    );
    backend
        .create(
            &tenant,
            "SearchParameter",
            search_parameter("max", "c".repeat(128)),
            version,
        )
        .await
        .expect("a 128-byte code is stored");
}
