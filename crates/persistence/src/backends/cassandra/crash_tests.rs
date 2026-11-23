//! Step 5's in-process crash tests: the rows of the roadmap's section 6 that pause or kill Helios
//! (ADR-sam-michaels-0006, RV17, RV18), and issue #77's applied-then-504 write (RV19).
//!
//! A crash is made with a checkpoint (RV15): `hold_at` holds the request at a point of the write path, and
//! `abort()` on its task is the crash, while `release()` is a pause. The fault seam is used only for the 504
//! of issue #77, which needs a statement that applies and then times out.
//!
//! Every test ends the same way (RV19): one `sweep_pass` scoped to its own tenant at grace 0, then the full
//! version history, the right `count`, no intent left and a consistent index. Nothing here sends a batch
//! the node refuses (IW20).
//!
//! The `kill -9`, restart, node-kill and tombstone rows stay in the roadmap's row 8.1, and the cluster rows
//! in 8.3.

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::blob::Blob;
use cdrs_tokio::types::value::Value;
use chrono::{DateTime, Utc};
use serde_json::{Value as Json, json};
use tokio_util::sync::CancellationToken;

use super::backend::CassandraBackend;
use super::checkpoint::{Checkpoint, Hold};
use super::consistency::harness::{self, context, tenant_id};
use super::consistency::{
    IndexScan, Registry, ScanMode, ScannedRow, Scope, assert_consistent, index_consistency,
    version_ids,
};
use super::index_keys;
use super::read::ResourceRow;
use super::session::{Fault, StatementKind};
use super::sweeper::{Intent, Rank, SweepOptions, SweepReport, SweepScope, plan_slices};
use super::test_support::{test_config, test_fhir_version};
use super::write::{IntentKey, RowKey, write_ts_of};
use crate::core::search::SearchProvider;
use crate::core::{ResourceStorage, VersionedStorage};
use crate::error::{BackendError, ConcurrencyError, StorageError, StorageResult};
use crate::tenant::TenantContext;
use crate::types::StoredResource;

const SKIP: &str = "skipping: HFS_TEST_CASSANDRA_URL is not set";

/// A body for `Patient/{id}`: `active` and `gender` are indexed (token) once extraction is real, so each version's
/// key set differs; no date element, so no row is shared across versions by identity except through `row_at`.
fn body(id: &str, active: bool, gender: &str) -> Json {
    json!({"resourceType": "Patient", "id": id, "active": active, "gender": gender})
}

/// The `gender` of a stored resource.
fn gender(resource: &StoredResource) -> String {
    resource.content()["gender"]
        .as_str()
        .expect("a gender")
        .to_string()
}

/// Holds `write` at `point` for `(t, id, version)`, waits until it gets there, and aborts it: a crash (RV15).
async fn crash<T: Send + 'static>(
    backend: &Arc<CassandraBackend>,
    point: Checkpoint,
    t: &str,
    id: &str,
    version: i32,
    write: impl Future<Output = StorageResult<T>> + Send + 'static,
) {
    let mut hold = backend.hold_at(point, t, id, version);
    let task = tokio::spawn(write);
    hold.reached().await;
    task.abort();
    match task.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("the aborted request finished"),
    }
}

/// Holds `write` at `point` and returns it held: a paused writer. The caller releases the `Hold` and awaits the task.
async fn pause<T: Send + 'static>(
    backend: &Arc<CassandraBackend>,
    point: Checkpoint,
    t: &str,
    id: &str,
    version: i32,
    write: impl Future<Output = StorageResult<T>> + Send + 'static,
) -> (Hold, tokio::task::JoinHandle<StorageResult<T>>) {
    let mut hold = backend.hold_at(point, t, id, version);
    let task = tokio::spawn(write);
    hold.reached().await;
    (hold, task)
}

/// The one scoped sweep (SW28, RV19): `Tenants([t])`, grace 0, `allow_short_grace`, `now = Utc::now()`. Unwraps.
async fn pass(backend: &CassandraBackend, t: &str, since: DateTime<Utc>) -> SweepReport {
    backend
        .sweep_pass(SweepOptions {
            scope: SweepScope::Tenants(vec![t.to_string()]),
            grace: Duration::ZERO,
            now: Utc::now(),
            since,
            allow_short_grace: true,
        })
        .await
        .expect("a grace-0 pass is allowed here")
}

/// `(last_updated, version_id)` of `id`'s `resources_by_type` rows in `(t, "Patient")`, newest first.
async fn rbt_rows(backend: &CassandraBackend, t: &str, id: &str) -> Vec<(DateTime<Utc>, i32)> {
    let rows = backend
        .session
        .rows(
            "SELECT last_updated, id, version_id FROM resources_by_type \
             WHERE tenant_id = ? AND resource_type = ?",
            vec![Value::from(t), Value::from("Patient")],
            StatementKind::Read,
        )
        .await
        .expect("resources_by_type");
    let mut found: Vec<(DateTime<Utc>, i32)> = rows
        .iter()
        .filter(|row| {
            let found: String = row.get_r_by_name("id").unwrap();
            found == id
        })
        .map(|row| {
            (
                row.get_r_by_name("last_updated").unwrap(),
                row.get_r_by_name("version_id").unwrap(),
            )
        })
        .collect();
    found.sort_by(|a, b| b.cmp(a));
    found
}

/// R1 of `Patient/{id}`; panics when there is none.
async fn r1(backend: &CassandraBackend, t: &str, id: &str) -> ResourceRow {
    backend
        .read_resource_row(t, "Patient", id)
        .await
        .expect("R1")
        .expect("a row")
}

/// Every index row of `Patient/{id}` in `t`, from one `IndexScan` keeping only `t` (`IndexScan::rows_of`).
async fn index_rows(backend: &CassandraBackend, t: &str, id: &str) -> Vec<ScannedRow> {
    let own = t.to_string();
    let scan = IndexScan::take(backend, move |tenant| tenant == own).await;
    assert!(scan.failed().is_none(), "{:?}", scan.failed());
    scan.rows_of(t)
        .iter()
        .filter(|row| row.resource_type == "Patient" && row.id == id)
        .cloned()
        .collect()
}

/// The distinct `version_id` tags of `index_rows(..)`.
async fn tags(backend: &CassandraBackend, t: &str, id: &str) -> BTreeSet<i32> {
    index_rows(backend, t, id)
        .await
        .iter()
        .map(|row| row.version_id)
        .collect()
}

/// `resource_versions.index_keys` of `Patient/{id}` version `v` (`StatementKind::Read`); `None` when unset or no row.
async fn version_index_keys(
    backend: &CassandraBackend,
    t: &str,
    id: &str,
    v: i32,
) -> Option<Vec<u8>> {
    let rows = backend
        .session
        .rows(
            "SELECT index_keys FROM resource_versions \
             WHERE tenant_id = ? AND resource_type = ? AND id = ? AND version_id = ?",
            vec![
                Value::from(t),
                Value::from("Patient"),
                Value::from(id),
                Value::from(v),
            ],
            StatementKind::Read,
        )
        .await
        .expect("resource_versions");
    let row = rows.first()?;
    IntoRustByName::<Blob>::get_by_name(row, "index_keys")
        .expect("index_keys")
        .map(Blob::into_vec)
}

/// Intents of `(t, id)` from `since`, read through I3: `plan_slices(IntentKey::new(t, id, since).shard(), since,
/// Utc::now() + 1 ms, chrono::Duration::minutes(1))` and `read_slice(.., Rank::Normal)`, keeping `(t, id)`.
async fn pending(
    backend: &CassandraBackend,
    t: &str,
    id: &str,
    since: DateTime<Utc>,
) -> Vec<Intent> {
    let shard = IntentKey::new(t, id, since).shard();
    let to = Utc::now() + chrono::Duration::milliseconds(1);
    let mut found = Vec::new();
    for slice in plan_slices(shard, since, to, chrono::Duration::minutes(1)) {
        let (intents, _) = backend
            .read_slice(&slice, Rank::Normal)
            .await
            .expect("read a slice");
        found.extend(
            intents
                .into_iter()
                .filter(|intent| intent.tenant_id == t && intent.id == id),
        );
    }
    found
}

/// The versions of `(t, id)`'s intents from `since`, ascending.
async fn intents(backend: &CassandraBackend, t: &str, id: &str, since: DateTime<Utc>) -> Vec<i32> {
    let mut versions = backend.intent_versions(t, "Patient", id, since).await;
    versions.sort_unstable();
    versions
}

/// RV19's end state: `version_ids == versions`, `count(Some("Patient")) == count`, no intent of `(t, id)` since
/// `since`, and `assert_consistent(index_consistency(backend, t, Scope::Tenant, ScanMode::Full, registry))`.
async fn assert_healed(
    backend: &CassandraBackend,
    t: &str,
    id: &str,
    since: DateTime<Utc>,
    versions: &[i32],
    count: u64,
    registry: Registry,
) {
    assert_eq!(
        version_ids(backend, t, "Patient", id).await,
        versions,
        "the version history of {t}/{id}"
    );
    assert_eq!(
        backend
            .count(&context(t), Some("Patient"))
            .await
            .expect("count"),
        count,
        "the count of {t}"
    );
    assert!(
        pending(backend, t, id, since).await.is_empty(),
        "an intent of {t}/{id} is left"
    );
    assert_consistent(
        &index_consistency(backend, t, Scope::Tenant, ScanMode::Full, registry).await,
    );
}

/// R1's fields that a crash before the CAS must leave alone.
fn assert_same_row(now: &ResourceRow, then: &ResourceRow) {
    assert_eq!(now.version_id, then.version_id);
    assert_eq!(now.last_updated, then.last_updated);
    assert_eq!(now.write_ts, then.write_ts);
    assert_eq!(now.write_id, then.write_id);
    assert_eq!(now.index_keys, then.index_keys);
    assert_eq!(now.data, then.data);
}

/// A stored, `active` SearchParameter `id` on Patient, with `url` and `expression`.
fn search_parameter(id: &str, url: &str, expression: &str) -> Json {
    json!({
        "resourceType": "SearchParameter",
        "id": id,
        "url": url,
        "name": id,
        "status": "active",
        "description": "a stored SearchParameter for crash_tests.rs",
        "code": format!("crash-{id}"),
        "base": ["Patient"],
        "type": "token",
        "expression": expression,
    })
}

/// The expression in force for `url` in `tenant`'s registry, which a search would use.
fn expression_of(backend: &CassandraBackend, tenant: &TenantContext, url: &str) -> Option<String> {
    backend
        .search_param_registry(tenant)
        .read()
        .get_by_url(url)
        .map(|definition| definition.expression.clone())
}

/// Starts a case: a backend, `since` before the first write, and the sweeper counters to subtract at the end.
async fn start() -> Option<(
    Arc<CassandraBackend>,
    DateTime<Utc>,
    super::sweeper::SweeperStats,
)> {
    let Some(backend) = harness::backend().await else {
        eprintln!("{SKIP}");
        return None;
    };
    let before = backend.sweeper_stats();
    Some((backend, Utc::now() - chrono::Duration::seconds(5), before))
}

/// Row "Intent then crash": the request dies after its intent (I1), before the CAS. Nothing of it exists but
/// the intent, and the sweep finds the CAS never applied and only deletes the intent.
#[tokio::test]
async fn intent_then_crash_is_swept_and_changes_nothing() {
    let Some((backend, since, before)) = start().await else {
        return;
    };
    let t = tenant_id("crash-intent");
    let tenant = context(&t);
    let version = test_fhir_version();
    let v1 = backend
        .create(&tenant, "Patient", body("p", true, "male"), version)
        .await
        .unwrap();
    let r1_0 = r1(&backend, &t, "p").await;
    let rows_0 = index_rows(&backend, &t, "p").await;
    let rbt_0 = rbt_rows(&backend, &t, "p").await;
    let (b, ten) = (backend.clone(), tenant.clone());
    crash(&backend, Checkpoint::AfterIntent, &t, "p", 2, async move {
        b.update(&ten, &v1, body("p", false, "female")).await
    })
    .await;

    assert_eq!(intents(&backend, &t, "p", since).await, vec![2]);
    assert_same_row(&r1(&backend, &t, "p").await, &r1_0);
    let at = backend.statement_counts();
    let report = pass(&backend, &t, since).await;
    assert_eq!((report.never_applied, report.done), (1, 1), "{report}");
    assert_eq!(report.current + report.superseded, 0, "{report}");
    assert!(report.is_complete(), "{report}");
    let spent = backend.statement_counts().since(&at);
    assert_eq!((spent.write, spent.batch, spent.cas), (1, 0, 0), "{spent}");

    assert_same_row(&r1(&backend, &t, "p").await, &r1_0);
    assert_eq!(index_rows(&backend, &t, "p").await, rows_0);
    let rbt = rbt_rows(&backend, &t, "p").await;
    assert_eq!(rbt, rbt_0);
    assert_eq!(rbt, vec![(r1_0.last_updated, 1)]);
    assert_healed(&backend, &t, "p", since, &[1], 1, Registry::Fixed).await;
    eprintln!(
        "[crash case=intent {}]",
        backend.sweeper_stats().since(&before)
    );
}

/// Row "CAS then crash" on a create: the resource is readable at once, and the sweep writes its version row
/// and its index rows.
#[tokio::test]
async fn cas_then_crash_on_create_is_readable_at_once_and_indexed_after_the_sweep() {
    let Some((backend, since, before)) = start().await else {
        return;
    };
    let t = tenant_id("crash-cas-create");
    let tenant = context(&t);
    let version = test_fhir_version();
    let (b, ten) = (backend.clone(), tenant.clone());
    crash(&backend, Checkpoint::AfterCas, &t, "p", 1, async move {
        b.create(&ten, "Patient", body("p", true, "male"), version)
            .await
    })
    .await;

    let read = backend
        .read(&tenant, "Patient", "p")
        .await
        .unwrap()
        .expect("readable at once");
    assert_eq!(read.version_id(), "1");
    assert_eq!(read.content()["active"], json!(true));
    assert!(
        backend
            .vread(&tenant, "Patient", "p", "1")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        version_ids(&backend, &t, "Patient", "p").await,
        Vec::<i32>::new()
    );
    assert!(rbt_rows(&backend, &t, "p").await.is_empty());
    assert_eq!(intents(&backend, &t, "p", since).await, vec![1]);

    let report = pass(&backend, &t, since).await;
    assert_eq!((report.current, report.done), (1, 1), "{report}");
    assert!(report.is_complete(), "{report}");

    let stored = r1(&backend, &t, "p").await;
    let keys = index_keys::decode(stored.index_keys.as_deref().expect("K1")).expect("K1");
    let vread = backend
        .vread(&tenant, "Patient", "p", "1")
        .await
        .unwrap()
        .expect("the version row");
    assert_eq!(vread.content(), read.content());
    assert_eq!(
        rbt_rows(&backend, &t, "p").await,
        vec![(stored.last_updated, 1)]
    );
    assert_eq!(index_rows(&backend, &t, "p").await.len(), keys.len());
    assert_eq!(tags(&backend, &t, "p").await, BTreeSet::from([1]));
    assert_healed(&backend, &t, "p", since, &[1], 1, Registry::Fixed).await;
    eprintln!(
        "[crash case=cas-create {}]",
        backend.sweeper_stats().since(&before)
    );
}

/// Row "CAS then crash, then superseded": a later writer lands before the sweep. It rewrites the crashed
/// version's row from R1 (G40), so no version is lost, and the sweep removes the old index row.
#[tokio::test]
async fn cas_then_crash_then_superseded_loses_no_version_and_leaves_no_old_row() {
    let Some((backend, since, before)) = start().await else {
        return;
    };
    let t = tenant_id("crash-superseded");
    let tenant = context(&t);
    let version = test_fhir_version();
    let v1 = backend
        .create(&tenant, "Patient", body("p", true, "male"), version)
        .await
        .unwrap();
    let lu1 = r1(&backend, &t, "p").await.last_updated;
    let (b, ten) = (backend.clone(), tenant.clone());
    crash(&backend, Checkpoint::AfterCas, &t, "p", 2, async move {
        b.update(&ten, &v1, body("p", false, "female")).await
    })
    .await;
    let v2 = backend
        .read(&tenant, "Patient", "p")
        .await
        .unwrap()
        .expect("v2");
    let v3 = backend
        .update(&tenant, &v2, body("p", true, "other"))
        .await
        .unwrap();
    assert_eq!(v3.version_id(), "3");
    let lu3 = r1(&backend, &t, "p").await.last_updated;

    let vread2 = backend
        .vread(&tenant, "Patient", "p", "2")
        .await
        .unwrap()
        .expect("row 2");
    assert_eq!(gender(&vread2), "female");
    assert_eq!(
        version_ids(&backend, &t, "Patient", "p").await,
        vec![1, 2, 3]
    );
    assert_eq!(intents(&backend, &t, "p", since).await, vec![2]);
    assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu3, 3), (lu1, 1)]);

    let report = pass(&backend, &t, since).await;
    assert_eq!((report.superseded, report.done), (1, 1), "{report}");
    assert!(report.is_complete(), "{report}");

    assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu3, 3)]);
    assert_eq!(tags(&backend, &t, "p").await, BTreeSet::from([3]));
    let vread2 = backend
        .vread(&tenant, "Patient", "p", "2")
        .await
        .unwrap()
        .expect("row 2");
    assert_eq!(gender(&vread2), "female");
    assert_healed(&backend, &t, "p", since, &[1, 2, 3], 1, Registry::Fixed).await;
    eprintln!(
        "[crash case=superseded {}]",
        backend.sweeper_stats().since(&before)
    );
}

/// The same row, G40: two crashes in a row (versions 2 and 3, each after its CAS) lose no version either.
#[tokio::test]
async fn two_crashes_in_a_row_lose_nothing() {
    let Some((backend, since, before)) = start().await else {
        return;
    };
    let t = tenant_id("crash-twice");
    let tenant = context(&t);
    let version = test_fhir_version();
    let v1 = backend
        .create(&tenant, "Patient", body("p", true, "male"), version)
        .await
        .unwrap();
    let lu1 = r1(&backend, &t, "p").await.last_updated;
    let (b, ten) = (backend.clone(), tenant.clone());
    crash(&backend, Checkpoint::AfterCas, &t, "p", 2, async move {
        b.update(&ten, &v1, body("p", false, "female")).await
    })
    .await;
    let v2 = backend
        .read(&tenant, "Patient", "p")
        .await
        .unwrap()
        .expect("v2");
    let (b, ten) = (backend.clone(), tenant.clone());
    crash(&backend, Checkpoint::AfterCas, &t, "p", 3, async move {
        b.update(&ten, &v2, body("p", true, "other")).await
    })
    .await;

    assert_eq!(version_ids(&backend, &t, "Patient", "p").await, vec![1, 2]);
    assert_eq!(r1(&backend, &t, "p").await.version_id, 3);
    assert_eq!(intents(&backend, &t, "p", since).await, vec![2, 3]);
    let vread2 = backend
        .vread(&tenant, "Patient", "p", "2")
        .await
        .unwrap()
        .expect("row 2");
    assert_eq!(gender(&vread2), "female");
    assert!(
        backend
            .vread(&tenant, "Patient", "p", "3")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu1, 1)]);

    let report = pass(&backend, &t, since).await;
    assert_eq!(
        (report.superseded, report.current, report.done),
        (1, 1, 2),
        "{report}"
    );
    assert!(report.is_complete(), "{report}");

    let lu3 = r1(&backend, &t, "p").await.last_updated;
    assert_eq!(
        version_ids(&backend, &t, "Patient", "p").await,
        vec![1, 2, 3]
    );
    for (v, expected) in [("1", "male"), ("2", "female"), ("3", "other")] {
        let stored = backend
            .vread(&tenant, "Patient", "p", v)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("version {v}"));
        assert_eq!(gender(&stored), expected, "version {v}");
    }
    assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu3, 3)]);
    assert_eq!(tags(&backend, &t, "p").await, BTreeSet::from([3]));
    assert_healed(&backend, &t, "p", since, &[1, 2, 3], 1, Registry::Fixed).await;
    eprintln!(
        "[crash case=twice {}]",
        backend.sweeper_stats().since(&before)
    );
}

/// Row "Sweeper outage": two crashed writes whose intents are two hours old are processed by the loop's
/// startup scan (SW9), with no poll before it.
#[tokio::test]
async fn sweeper_outage_processes_two_hour_old_intents_on_startup() {
    let Some((backend, since, _)) = start().await else {
        return;
    };
    let url = std::env::var("HFS_TEST_CASSANDRA_URL").unwrap();
    let t = tenant_id("crash-sweeper-outage");
    let tenant = context(&t);
    let version = test_fhir_version();
    let q1 = backend
        .create(&tenant, "Patient", body("q", true, "male"), version)
        .await
        .unwrap();
    let (b, ten) = (backend.clone(), tenant.clone());
    crash(&backend, Checkpoint::AfterCas, &t, "p", 1, async move {
        b.create(&ten, "Patient", body("p", true, "male"), version)
            .await
    })
    .await;
    let (b, ten) = (backend.clone(), tenant.clone());
    crash(&backend, Checkpoint::AfterCas, &t, "q", 2, async move {
        b.update(&ten, &q1, body("q", false, "female")).await
    })
    .await;

    // Make both intents two hours old: a new intent row at `old`, the fresh one deleted.
    let old_at = Utc::now() - chrono::Duration::minutes(125);
    let old = DateTime::from_timestamp_millis(old_at.timestamp_millis()).unwrap();
    for (id, v) in [("p", 1), ("q", 2)] {
        let fresh = pending(&backend, &t, id, since).await;
        assert_eq!(fresh.len(), 1, "{id}: {fresh:?}");
        assert_eq!(fresh[0].version_id, v);
        let key = RowKey {
            tenant_id: &t,
            resource_type: "Patient",
            id,
        };
        let write_id = r1(&backend, &t, id).await.write_id.unwrap();
        backend
            .write_intent(&IntentKey::new(&t, id, old), &key, write_id, v)
            .await
            .expect("an old intent");
        backend.delete_intent(&fresh[0].key, &key).await;
    }
    let from = old - chrono::Duration::minutes(1);
    assert_eq!(
        backend.intent_versions(&t, "Patient", "p", from).await,
        vec![1]
    );
    assert_eq!(
        backend.intent_versions(&t, "Patient", "q", from).await,
        vec![2]
    );
    for id in ["p", "q"] {
        assert!(pending(&backend, &t, id, since).await.is_empty(), "{id}");
    }
    let lu_p1 = r1(&backend, &t, "p").await.last_updated;
    let lu_q2 = r1(&backend, &t, "q").await.last_updated;

    let looped = Arc::new(
        CassandraBackend::connect(super::CassandraConfig {
            contact_points: vec![url],
            request_timeout_ms: 2_500,
            sweeper_poll_interval_ms: 1_000,
            sweeper_grace_ms: 5_000,
            ..test_config()
        })
        .await
        .expect("connect"),
    );
    let looped_before = looped.sweeper_stats();
    let cancel = CancellationToken::new();
    let started = std::time::Instant::now();
    let task = tokio::spawn(looped.clone().run_sweeper_with(
        cancel.clone(),
        SweepScope::Tenants(vec![t.clone()]),
        true,
    ));
    loop {
        if backend
            .intent_versions(&t, "Patient", "p", from)
            .await
            .is_empty()
            && backend
                .intent_versions(&t, "Patient", "q", from)
                .await
                .is_empty()
            && rbt_rows(&backend, &t, "p").await == vec![(lu_p1, 1)]
            && rbt_rows(&backend, &t, "q").await == vec![(lu_q2, 2)]
        {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(120),
            "not healed after {:?}",
            started.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    eprintln!(
        "[crash case=sweeper-outage healed_ms={}]",
        started.elapsed().as_millis()
    );
    let stats = looped.sweeper_stats().since(&looped_before);
    assert!(
        stats.startup_slices >= 1 && stats.case_current >= 2 && stats.done >= 2,
        "{stats}"
    );
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the sweeper stops within 5 s")
        .unwrap();

    assert_healed(&backend, &t, "p", since, &[1], 2, Registry::Fixed).await;
    assert_healed(&backend, &t, "q", since, &[1, 2], 2, Registry::Fixed).await;
}

/// Row "Stale batch ordering": a writer paused after its CAS finishes after a successor has landed. Its
/// late index rows must not outrank the successor's.
#[tokio::test]
async fn stale_batch_ordering_ignores_the_late_writer() {
    let Some((backend, since, before)) = start().await else {
        return;
    };
    let t = tenant_id("crash-stale-batch");
    let tenant = context(&t);
    let version = test_fhir_version();
    let v1 = backend
        .create(&tenant, "Patient", body("p", true, "male"), version)
        .await
        .unwrap();
    let (b, ten) = (backend.clone(), tenant.clone());
    let (hold, task) = pause(&backend, Checkpoint::AfterCas, &t, "p", 2, async move {
        b.update(&ten, &v1, body("p", false, "female")).await
    })
    .await;
    let k2 = r1(&backend, &t, "p").await.index_keys;
    let v2 = backend
        .read(&tenant, "Patient", "p")
        .await
        .unwrap()
        .expect("v2");
    let v3 = backend
        .update(&tenant, &v2, body("p", true, "other"))
        .await
        .unwrap();
    assert_eq!(v3.version_id(), "3");
    hold.release();
    let late = tokio::time::timeout(Duration::from_secs(30), task)
        .await
        .expect("the late writer finishes within 30 s")
        .unwrap()
        .expect("the late writer");
    assert_eq!(late.version_id(), "2");

    let current = r1(&backend, &t, "p").await;
    let lu3 = current.last_updated;
    assert!(intents(&backend, &t, "p", since).await.is_empty());
    assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu3, 3)]);
    assert_eq!(tags(&backend, &t, "p").await, BTreeSet::from([3]));
    for row in index_rows(&backend, &t, "p").await {
        assert_eq!(row.written, write_ts_of(lu3), "{row:?}");
    }
    let read = backend
        .read(&tenant, "Patient", "p")
        .await
        .unwrap()
        .expect("current");
    assert_eq!(gender(&read), "other");
    let vread2 = backend
        .vread(&tenant, "Patient", "p", "2")
        .await
        .unwrap()
        .expect("row 2");
    assert_eq!(gender(&vread2), "female");
    assert_eq!(version_index_keys(&backend, &t, "p", 2).await, k2);

    let report = pass(&backend, &t, since).await;
    assert_eq!((report.intents, report.done), (0, 0), "{report}");
    assert!(report.is_complete(), "{report}");
    assert_healed(&backend, &t, "p", since, &[1, 2, 3], 1, Registry::Fixed).await;
    eprintln!(
        "[crash case=stale-batch {}]",
        backend.sweeper_stats().since(&before)
    );
}

/// Row "Paused writer, then a crashed successor" (G40): version 2 is paused after its CAS, version 3 crashes
/// after its. The sweep, run while writer 2 is still held because that is the row, finishes both intents;
/// writer 2 then resumes and leaves no row of its version.
#[tokio::test]
async fn a_paused_writer_after_a_crashed_successor_leaves_no_row_of_its_version() {
    let Some((backend, since, before)) = start().await else {
        return;
    };
    let t = tenant_id("crash-paused-writer");
    let tenant = context(&t);
    let version = test_fhir_version();
    let v1 = backend
        .create(&tenant, "Patient", body("p", true, "male"), version)
        .await
        .unwrap();
    let (b, ten) = (backend.clone(), tenant.clone());
    let (hold, task) = pause(&backend, Checkpoint::AfterCas, &t, "p", 2, async move {
        b.update(&ten, &v1, body("p", false, "female")).await
    })
    .await;
    let k2 = r1(&backend, &t, "p").await.index_keys;
    let v2 = backend
        .read(&tenant, "Patient", "p")
        .await
        .unwrap()
        .expect("v2");
    let (b, ten) = (backend.clone(), tenant.clone());
    crash(&backend, Checkpoint::AfterCas, &t, "p", 3, async move {
        b.update(&ten, &v2, body("p", true, "other")).await
    })
    .await;
    let current = r1(&backend, &t, "p").await;
    let (k3, lu3) = (current.index_keys.clone(), current.last_updated);

    assert_eq!(intents(&backend, &t, "p", since).await, vec![2, 3]);
    assert_eq!(version_ids(&backend, &t, "Patient", "p").await, vec![1, 2]);
    // The sweep runs while writer 2 is held: that is the row under test.
    let report = pass(&backend, &t, since).await;
    assert_eq!(
        (report.superseded, report.current, report.done),
        (1, 1, 2),
        "{report}"
    );
    assert!(report.is_complete(), "{report}");
    assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu3, 3)]);
    assert_eq!(tags(&backend, &t, "p").await, BTreeSet::from([3]));

    hold.release();
    let late = tokio::time::timeout(Duration::from_secs(30), task)
        .await
        .expect("the paused writer finishes within 30 s")
        .unwrap()
        .expect("the paused writer");
    assert_eq!(late.version_id(), "2");

    assert_eq!(rbt_rows(&backend, &t, "p").await, vec![(lu3, 3)]);
    assert_eq!(tags(&backend, &t, "p").await, BTreeSet::from([3]));
    assert_eq!(version_index_keys(&backend, &t, "p", 2).await, k2);
    assert_eq!(version_index_keys(&backend, &t, "p", 3).await, k3);
    let vread2 = backend
        .vread(&tenant, "Patient", "p", "2")
        .await
        .unwrap()
        .expect("row 2");
    assert_eq!(gender(&vread2), "female");
    assert_healed(&backend, &t, "p", since, &[1, 2, 3], 1, Registry::Fixed).await;
    eprintln!(
        "[crash case=paused-writer {}]",
        backend.sweeper_stats().since(&before)
    );
}

/// The same row after a registry change: a SearchParameter is stored while writer 2 is paused, so the late
/// writer's key set was extracted under the old registry, and the successor's under the new one.
#[tokio::test]
async fn stale_batch_ordering_after_a_registry_change_leaves_no_row_of_the_late_version() {
    let Some((backend, since, before)) = start().await else {
        return;
    };
    let t = tenant_id("crash-registry-change");
    let tenant = context(&t);
    let version = test_fhir_version();
    let url = format!("http://example.org/SearchParameter/{t}");
    let v1 = backend
        .create(&tenant, "Patient", body("p", true, "male"), version)
        .await
        .unwrap();
    let (b, ten) = (backend.clone(), tenant.clone());
    let (hold, task) = pause(&backend, Checkpoint::AfterCas, &t, "p", 2, async move {
        b.update(&ten, &v1, body("p", false, "female")).await
    })
    .await;
    backend
        .create(
            &tenant,
            "SearchParameter",
            search_parameter("crash-sp", &url, "Patient.gender"),
            version,
        )
        .await
        .unwrap();
    assert_eq!(
        expression_of(&backend, &tenant, &url).as_deref(),
        Some("Patient.gender")
    );
    let v2 = backend
        .read(&tenant, "Patient", "p")
        .await
        .unwrap()
        .expect("v2");
    let v3 = backend
        .update(&tenant, &v2, body("p", true, "other"))
        .await
        .unwrap();
    assert_eq!(v3.version_id(), "3");
    let k3 = r1(&backend, &t, "p").await.index_keys;
    hold.release();
    let late = tokio::time::timeout(Duration::from_secs(30), task)
        .await
        .expect("the late writer finishes within 30 s")
        .unwrap()
        .expect("the late writer");
    assert_eq!(late.version_id(), "2");

    let current = r1(&backend, &t, "p").await;
    assert_eq!(tags(&backend, &t, "p").await, BTreeSet::from([3]));
    assert_eq!(
        rbt_rows(&backend, &t, "p").await,
        vec![(current.last_updated, 3)]
    );
    assert_eq!(version_index_keys(&backend, &t, "p", 3).await, k3);
    assert_eq!(k3, current.index_keys);

    let report = pass(&backend, &t, since).await;
    assert_eq!(report.intents, 0, "{report}");
    assert!(report.is_complete(), "{report}");
    // The tenant wrote a SearchParameter, so rules 3 and 4 are skipped (RV14).
    assert_healed(&backend, &t, "p", since, &[1, 2, 3], 1, Registry::Changed).await;
    eprintln!(
        "[crash case=registry-change {}]",
        backend.sweeper_stats().since(&before)
    );
}

/// Row "Concurrent updates": 32 unconditional PUTs of one resource released together. Every result is a
/// version or a `VersionConflict` after the retry budget, and the index ends equal to a fresh extraction.
#[tokio::test]
async fn concurrent_updates_end_with_the_index_equal_to_a_fresh_extraction() {
    let Some((backend, since, _)) = start().await else {
        return;
    };
    let t = tenant_id("crash-concurrent");
    let tenant = context(&t);
    let version = test_fhir_version();
    backend
        .create(&tenant, "Patient", body("p", true, "male"), version)
        .await
        .unwrap();
    let at = backend.cas_stats();
    let barrier = Arc::new(tokio::sync::Barrier::new(32));
    let mut tasks = Vec::new();
    for n in 0..32usize {
        let (b, ten, gate) = (backend.clone(), tenant.clone(), barrier.clone());
        tasks.push(tokio::spawn(async move {
            gate.wait().await;
            let resource = body(
                "p",
                n % 2 == 0,
                ["male", "female", "other", "unknown"][n % 4],
            );
            b.create_or_update(&ten, "Patient", "p", resource, version)
                .await
        }));
    }
    let (mut oks, mut conflicts) = (Vec::new(), 0usize);
    for task in tasks {
        match task.await.unwrap() {
            Ok((stored, false)) => oks.push(stored.version_id().to_string()),
            Err(StorageError::Concurrency(ConcurrencyError::VersionConflict { .. })) => {
                conflicts += 1
            }
            other => panic!("a concurrent update ended with {other:?}"),
        }
    }
    let distinct: BTreeSet<&String> = oks.iter().collect();
    assert_eq!(distinct.len(), oks.len(), "versions are distinct: {oks:?}");
    assert!(!oks.is_empty(), "no update succeeded");
    let last = 1 + oks.len() as i32;
    let spent = backend.cas_stats().since(&at);
    assert_eq!(spent.client_side_unknowns(), 0, "{spent}");
    let current = r1(&backend, &t, "p").await;
    assert_eq!(current.version_id, last);
    assert_eq!(
        version_ids(&backend, &t, "Patient", "p").await,
        (1..=last).collect::<Vec<i32>>()
    );

    let report = pass(&backend, &t, since).await;
    assert!(report.is_complete(), "{report}");
    let current = r1(&backend, &t, "p").await;
    assert_eq!(
        rbt_rows(&backend, &t, "p").await,
        vec![(current.last_updated, current.version_id)]
    );
    assert_eq!(
        tags(&backend, &t, "p").await,
        BTreeSet::from([current.version_id])
    );
    let versions: Vec<i32> = (1..=last).collect();
    assert_healed(&backend, &t, "p", since, &versions, 1, Registry::Fixed).await;
    eprintln!(
        "[crash case=concurrent oks={} conflicts={conflicts} {spent}]",
        oks.len()
    );
}

/// Issue #77: a write whose CAS applied but whose answer timed out, and whose serial read timed out too,
/// answers 504 while its row is stored. The sweep finishes it, on create, update and delete.
#[tokio::test]
async fn an_applied_write_answered_504_is_finished_by_the_sweeper() {
    let Some((backend, since, _)) = start().await else {
        return;
    };
    let version = test_fhir_version();
    for op in ["create", "update", "delete"] {
        let t = tenant_id(&format!("crash-504-{op}"));
        let tenant = context(&t);
        let stored = if op == "create" {
            None
        } else {
            Some(
                backend
                    .create(&tenant, "Patient", body("p", true, "male"), version)
                    .await
                    .unwrap(),
            )
        };
        let cas = match op {
            "create" => "INSERT INTO resources",
            "update" => "SET version_id = ?, data = ?",
            _ => "deleted_at = ?, last_updated",
        };
        backend
            .session
            .inject_fault(StatementKind::Cas, cas, Fault::ApplyThenTimeOut);
        backend.session.inject_fault(
            StatementKind::SerialRead,
            "FROM resources",
            Fault::FailWithCasTimeout,
        );
        let at = backend.cas_stats();
        let result: StorageResult<()> = match &stored {
            None => backend
                .create(&tenant, "Patient", body("p", true, "male"), version)
                .await
                .map(|_| ()),
            Some(v1) if op == "update" => backend
                .update(&tenant, v1, body("p", false, "female"))
                .await
                .map(|_| ()),
            Some(_) => backend.delete(&tenant, "Patient", "p").await,
        };
        let spent = backend.cas_stats().since(&at);
        match result {
            Err(StorageError::Backend(BackendError::Timeout { .. })) => {}
            other => panic!("{op}: expected 504, got {other:?}"),
        }
        assert_eq!(
            (spent.attempts, spent.reruns, spent.serial_read_failures),
            (1, 0, 1),
            "{op}: {spent}"
        );
        assert_eq!(spent.client_side_unknowns(), 0, "{op}: {spent}");

        let target = if op == "create" { 1 } else { 2 };
        let current = r1(&backend, &t, "p").await;
        assert_eq!(current.version_id, target, "{op}");
        assert_eq!(
            intents(&backend, &t, "p", since).await,
            vec![target],
            "{op}"
        );
        let before_sweep: Vec<i32> = if op == "create" { vec![] } else { vec![1] };
        assert_eq!(
            version_ids(&backend, &t, "Patient", "p").await,
            before_sweep,
            "{op}"
        );

        let report = pass(&backend, &t, since).await;
        assert_eq!((report.current, report.done), (1, 1), "{op}: {report}");
        assert!(report.is_complete(), "{op}: {report}");

        let (versions, count): (Vec<i32>, u64) = match op {
            "create" => (vec![1], 1),
            "update" => (vec![1, 2], 1),
            _ => (vec![1, 2], 0),
        };
        let current = r1(&backend, &t, "p").await;
        let expected = if op == "delete" {
            Vec::new()
        } else {
            vec![(current.last_updated, target)]
        };
        assert_eq!(rbt_rows(&backend, &t, "p").await, expected, "{op}");
        assert_healed(&backend, &t, "p", since, &versions, count, Registry::Fixed).await;
        eprintln!("[crash case=504-{op} {spent}]");
    }
}
