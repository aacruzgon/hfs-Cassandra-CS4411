//! Crash checkpoints in the write path, tests only (ADR-sam-michaels-0006,
//! RV15, RV16). A test arms a hold on a point for one tenant, id and target
//! version; the request that reaches it stops there until the test releases
//! it (a paused writer) or aborts the task running it (a crash: the future is
//! dropped at that await, so nothing after the point is sent, IW12). The
//! fault seam's statement faults (`session.rs`) stay for issue #77's 504s;
//! they cannot cut cleanly after step 4 once batches run concurrently, nor
//! pause a writer and resume it.

use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::oneshot;

use super::backend::CassandraBackend;
use super::write::RowKey;

/// How long [`Hold::reached`] waits before it fails the test.
const REACH_TIMEOUT: Duration = Duration::from_secs(30);

/// A point in the write path where a test can hold a request (RV16). Each is
/// passed whatever the step before it returned, so a test can also hold a
/// request whose version row or index rows failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
// The names are RV16's; each says where the request is, so the shared prefix stays.
#[allow(clippy::enum_variant_names)]
pub(crate) enum Checkpoint {
    /// After I1, before the CAS (`run_cas`): the intent exists, nothing else changed.
    AfterIntent,
    /// After a CAS that applied, before step 4 (`finish`), superseded or not.
    AfterCas,
    /// After step 4's version row, before step 5 (`finish`).
    AfterVersionRow,
    /// After step 5's index rows, before I2 (`finish`).
    AfterIndex,
}

/// The holds armed on one backend, each one-shot.
#[derive(Default)]
pub(crate) struct Checkpoints {
    armed: Mutex<Vec<Armed>>,
}

/// One armed hold, as the write path finds it.
struct Armed {
    point: Checkpoint,
    tenant_id: String,
    id: String,
    version_id: i32,
    reached: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}

/// A test's handle on one armed hold (RV16: `Hold { reached, release }`).
/// Dropping it without `release` lets the request go on, so a forgotten hold
/// never hangs a test.
pub(crate) struct Hold {
    point: Checkpoint,
    /// `"<tenant>/<id> v<version>"`, for messages.
    what: String,
    reached: Option<oneshot::Receiver<()>>,
    release: oneshot::Sender<()>,
}

impl Checkpoints {
    /// Arms a one-shot hold: the first request of `tenant_id` and `id` whose
    /// target version is `version_id` to reach `point` stops there.
    pub(crate) fn arm(
        &self,
        point: Checkpoint,
        tenant_id: &str,
        id: &str,
        version_id: i32,
    ) -> Hold {
        let (reached_tx, reached_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        self.armed.lock().push(Armed {
            point,
            tenant_id: tenant_id.to_string(),
            id: id.to_string(),
            version_id,
            reached: reached_tx,
            release: release_rx,
        });
        Hold {
            point,
            what: format!("{tenant_id}/{id} v{version_id}"),
            reached: Some(reached_rx),
            release: release_tx,
        }
    }

    /// Called at each point: takes the first matching armed hold that is still
    /// held (a hold whose `Hold` was dropped is skipped, so arming the same
    /// point and key again works), removing it, signals `reached`, then waits
    /// for `release` (or for the `Hold` to be dropped). Without a match it
    /// returns at once. The lock is never held across the await.
    pub(crate) async fn pass(&self, point: Checkpoint, tenant_id: &str, id: &str, version_id: i32) {
        let taken = {
            let mut armed = self.armed.lock();
            // A `Hold` that was dropped closed its `reached` receiver.
            armed.retain(|a| !a.reached.is_closed());
            armed
                .iter()
                .position(|a| {
                    a.point == point
                        && a.tenant_id == tenant_id
                        && a.id == id
                        && a.version_id == version_id
                })
                .map(|index| armed.remove(index))
        };
        if let Some(hold) = taken {
            // A dropped `Hold` closes both channels; either way go on.
            let _ = hold.reached.send(());
            let _ = hold.release.await;
        }
    }
}

impl Hold {
    /// Waits until a request reaches the point; panics after
    /// [`REACH_TIMEOUT`] with `"checkpoint {point:?} for {what} was not reached"`,
    /// and if awaited twice.
    pub(crate) async fn reached(&mut self) {
        let receiver = self.reached.take().unwrap_or_else(|| {
            panic!(
                "checkpoint {:?} for {}: reached awaited twice",
                self.point, self.what
            )
        });
        match tokio::time::timeout(REACH_TIMEOUT, receiver).await {
            Ok(Ok(())) => {}
            _ => panic!(
                "checkpoint {:?} for {} was not reached",
                self.point, self.what
            ),
        }
    }

    /// Lets the held request go on: a pause, not a crash (RV15).
    pub(crate) fn release(self) {
        let _ = self.release.send(());
    }
}

impl CassandraBackend {
    /// [`Checkpoints::arm`] on this backend's write path (tests only).
    pub(crate) fn hold_at(
        &self,
        point: Checkpoint,
        tenant_id: &str,
        id: &str,
        version_id: i32,
    ) -> Hold {
        self.checkpoints.arm(point, tenant_id, id, version_id)
    }

    /// The write path's call at each point: `key`'s tenant and id, and the
    /// attempt's target version.
    pub(super) async fn checkpoint(&self, point: Checkpoint, key: &RowKey<'_>, version_id: i32) {
        self.checkpoints
            .pass(point, key.tenant_id, key.id, version_id)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::Utc;

    use super::*;
    use crate::backends::cassandra::consistency::harness::*;
    use crate::backends::cassandra::consistency::{
        Registry, ScanMode, Scope, assert_consistent, index_consistency, version_ids,
    };
    use crate::backends::cassandra::index_keys;
    use crate::backends::cassandra::test_support::test_fhir_version;
    use crate::core::ResourceStorage;

    const SECOND: Duration = Duration::from_secs(1);

    async fn within<F: std::future::Future>(what: F) -> F::Output {
        tokio::time::timeout(SECOND, what)
            .await
            .expect("completes within a second")
    }

    #[tokio::test]
    async fn a_hold_pauses_only_its_own_point_and_key() {
        let checkpoints = Arc::new(Checkpoints::default());
        let mut hold = checkpoints.arm(Checkpoint::AfterCas, "t", "p", 2);
        within(checkpoints.pass(Checkpoint::AfterCas, "t", "p", 1)).await;
        within(checkpoints.pass(Checkpoint::AfterIntent, "t", "p", 2)).await;
        within(checkpoints.pass(Checkpoint::AfterCas, "t", "q", 2)).await;
        within(checkpoints.pass(Checkpoint::AfterCas, "u", "p", 2)).await;

        let task = tokio::spawn({
            let checkpoints = checkpoints.clone();
            async move { checkpoints.pass(Checkpoint::AfterCas, "t", "p", 2).await }
        });
        hold.reached().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!task.is_finished(), "the request is held");
        hold.release();
        within(task).await.expect("the request goes on");
        // One-shot: the same key now passes at once.
        within(checkpoints.pass(Checkpoint::AfterCas, "t", "p", 2)).await;

        // A dropped hold never hangs a request.
        let hold = checkpoints.arm(Checkpoint::AfterIndex, "t", "p", 2);
        drop(hold);
        within(checkpoints.pass(Checkpoint::AfterIndex, "t", "p", 2)).await;

        // Dropping a hold and arming the same point and key again: the new
        // hold sees the request.
        let dropped = checkpoints.arm(Checkpoint::AfterIndex, "t", "p", 3);
        drop(dropped);
        let mut rearmed = checkpoints.arm(Checkpoint::AfterIndex, "t", "p", 3);
        let task = tokio::spawn({
            let checkpoints = checkpoints.clone();
            async move { checkpoints.pass(Checkpoint::AfterIndex, "t", "p", 3).await }
        });
        rearmed.reached().await;
        rearmed.release();
        within(task).await.expect("the request goes on");
    }

    #[tokio::test]
    async fn each_checkpoint_holds_a_create_until_released() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let t = tenant_id("crash-checkpoint-hold");
        let mut holds: Vec<Hold> = [
            Checkpoint::AfterIntent,
            Checkpoint::AfterCas,
            Checkpoint::AfterVersionRow,
            Checkpoint::AfterIndex,
        ]
        .into_iter()
        .map(|point| backend.hold_at(point, &t, "p", 1))
        .collect();
        let since = Utc::now();
        let task = tokio::spawn({
            let (backend, tenant) = (backend.clone(), context(&t));
            async move {
                backend
                    .create(&tenant, "Patient", patient(), test_fhir_version())
                    .await
            }
        });
        let mut next = holds.drain(..);

        let mut hold = next.next().unwrap();
        hold.reached().await;
        assert!(
            backend
                .read_resource_row(&t, "Patient", "p")
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            backend.intent_versions(&t, "Patient", "p", since).await,
            [1]
        );
        hold.release();

        let mut hold = next.next().unwrap();
        hold.reached().await;
        let r1 = backend
            .read_resource_row(&t, "Patient", "p")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r1.version_id, 1);
        assert_eq!(
            version_ids(&backend, &t, "Patient", "p").await,
            [] as [i32; 0]
        );
        hold.release();

        let mut hold = next.next().unwrap();
        hold.reached().await;
        assert_eq!(version_ids(&backend, &t, "Patient", "p").await, [1]);
        let keys = stored_keys(&backend, &t, "p").await;
        let report = check(&backend, &t).await;
        assert_eq!(
            report.kinds(),
            expect(&[("missing_row", keys.len())]),
            "{report}"
        );
        hold.release();

        let mut hold = next.next().unwrap();
        hold.reached().await;
        assert_consistent(&check(&backend, &t).await);
        assert_eq!(
            backend.intent_versions(&t, "Patient", "p", since).await,
            [1]
        );
        hold.release();

        let created = within_long(task).await.expect("joined").expect("created");
        assert_eq!(created.version_id(), "1");
        assert_eq!(
            backend.intent_versions(&t, "Patient", "p", since).await,
            [] as [i32; 0]
        );
        assert_consistent(&check(&backend, &t).await);
    }

    async fn within_long<F: std::future::Future>(what: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(30), what)
            .await
            .expect("completes")
    }

    #[tokio::test]
    async fn aborting_at_each_checkpoint_leaves_its_state() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        for (point, name) in [
            (Checkpoint::AfterIntent, "after-intent"),
            (Checkpoint::AfterCas, "after-cas"),
            (Checkpoint::AfterVersionRow, "after-version-row"),
            (Checkpoint::AfterIndex, "after-index"),
        ] {
            let t = tenant_id(&format!("crash-checkpoint-{name}"));
            let tenant = context(&t);
            let version = test_fhir_version();
            let p = backend
                .create(&tenant, "Patient", patient(), version)
                .await
                .expect("create p");
            backend
                .create(&tenant, "Patient", other(), version)
                .await
                .expect("create q");
            let k1 = stored_keys(&backend, &t, "p").await.len();

            let mut hold = backend.hold_at(point, &t, "p", 2);
            let since = Utc::now();
            let task = tokio::spawn({
                let (backend, tenant) = (backend.clone(), tenant.clone());
                async move { backend.update(&tenant, &p, changed()).await }
            });
            hold.reached().await;
            let at = backend.statement_counts();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled(), "{name}");
            let spent = backend.statement_counts().since(&at);
            assert_eq!(
                (spent.write, spent.batch, spent.cas),
                (0, 0, 0),
                "{name}: {spent}"
            );

            let r1 = backend
                .read_resource_row(&t, "Patient", "p")
                .await
                .unwrap()
                .unwrap();
            let report = check(&backend, &t).await;
            let (r1_version, versions, want): (i32, Vec<i32>, Vec<&str>) = match point {
                Checkpoint::AfterIntent => (1, vec![1], vec![]),
                Checkpoint::AfterCas | Checkpoint::AfterVersionRow => {
                    let k2 = index_keys::decode(r1.index_keys.as_deref().unwrap())
                        .unwrap()
                        .len();
                    let mut want = vec![("missing_row", k2), ("stray_row", k1)];
                    let rows = if point == Checkpoint::AfterCas {
                        want.push(("missing_version_row", 1));
                        vec![1]
                    } else {
                        vec![1, 2]
                    };
                    (2, rows, expect(&want))
                }
                Checkpoint::AfterIndex => (2, vec![1, 2], vec![]),
            };
            assert_eq!(r1.version_id, r1_version, "{name}");
            assert_eq!(
                version_ids(&backend, &t, "Patient", "p").await,
                versions,
                "{name}"
            );
            assert_eq!(
                backend.intent_versions(&t, "Patient", "p", since).await,
                [2],
                "{name}"
            );
            assert_eq!(report.kinds(), want, "{name}: {report}");

            let only_q = index_consistency(
                &backend,
                &t,
                Scope::Resources(&[("Patient", "q")]),
                ScanMode::Full,
                Registry::Fixed,
            )
            .await;
            assert_consistent(&only_q);
            assert_eq!(only_q.resources, 1, "{name}");
        }
    }
}
