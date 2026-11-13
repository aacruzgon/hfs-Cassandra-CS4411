//! Backend-agnostic race suite for version-aware writes (issues #1404, #1405).
//!
//! `ResourceStorage::update` and `ResourceStorage::delete_versioned` are
//! compare-and-swaps on the version the caller holds: of any number of writers
//! holding the same version, exactly one writes, and every other one is told
//! it lost with a `ConcurrencyError` — not with a backend `Internal` error, and
//! never with a success that silently overwrote the winner.
//!
//! The writers are real tasks on a multi-threaded runtime, released together
//! by a barrier and repeated over many resources, so the interleaving these
//! tests need happens by volume rather than by timing. Each test's caller must
//! therefore run it with `#[tokio::test(flavor = "multi_thread")]`; on a
//! current-thread runtime a synchronous backend (SQLite) never interleaves and
//! the assertions hold vacuously.
//!
//! Included by `#[path]` into each backend's test binary, the same arrangement
//! as `conditional_if_match_suite.rs`.
//!
//! Each race test has a `_with` variant that takes a [`RacePolicy`]; the plain
//! function runs it with [`RacePolicy::strict`]. Only a backend whose
//! compare-and-swap can answer an unknown outcome under contention by design
//! passes anything else: Cassandra at RF 3 (issue #77).

#![allow(dead_code)]

use std::sync::Arc;

use serde_json::{Value, json};
use tokio::sync::Barrier;

use helios_fhir::FhirVersion;
use helios_persistence::core::{ResourceStorage, VersionedStorage};
use helios_persistence::error::{BackendError, ResourceError, StorageError};
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use helios_persistence::types::StoredResource;

use crate::seeding::SeedingStorage;

const WRITERS: usize = 8;

/// Finds a write that applied but whose request ended before the version row
/// was written: the state a 504'd writer can leave (issue #77). Implemented
/// by each backend that accepts timeouts, since only it knows where such a
/// write is recorded.
#[async_trait::async_trait]
pub trait PendingWrites: Send + Sync {
    /// The version of `Patient/{id}` that applied and is not in the history
    /// yet, if there is one.
    async fn pending_version(&self, tenant: &TenantContext, id: &str) -> Option<String>;
}

/// What the race checks accept from a writer answered
/// `BackendError::Timeout` (504).
pub enum RacePolicy {
    /// A 504 fails the test, as any other error does.
    Strict,
    /// A 504 is an unknown outcome, which may have applied. The checks then
    /// decide from the stored body which write landed, and allow the history
    /// to lack its newest version only when `PendingWrites` reports that
    /// version as pending.
    AcceptTimeouts(Arc<dyn PendingWrites>),
}

impl RacePolicy {
    pub fn strict() -> Self {
        RacePolicy::Strict
    }

    pub fn accepts_timeouts(&self) -> bool {
        matches!(self, RacePolicy::AcceptTimeouts(_))
    }

    /// Whether this error is an unknown outcome under this policy.
    fn is_unknown(&self, error: &StorageError) -> bool {
        self.accepts_timeouts()
            && matches!(error, StorageError::Backend(BackendError::Timeout { .. }))
    }
}

/// The history of `Patient/{id}`, with the version [`PendingWrites`] reports
/// appended when it is the next one, and whether it was.
async fn history<S: VersionedStorage>(
    backend: &S,
    policy: &RacePolicy,
    t: &TenantContext,
    id: &str,
) -> (Vec<String>, bool) {
    let mut versions = backend
        .list_versions(t, "Patient", id)
        .await
        .expect("list versions");
    if let RacePolicy::AcceptTimeouts(pending) = policy
        && let Some(version) = pending.pending_version(t, id).await
    {
        let next = (versions.len() + 1).to_string();
        if version == next {
            versions.push(version);
            return (versions, true);
        }
    }
    (versions, false)
}

/// `["1", …, "n"]`.
fn versions_up_to(n: usize) -> Vec<String> {
    (1..=n).map(|v| v.to_string()).collect()
}

fn tenant(base: &str, label: &str) -> TenantContext {
    TenantContext::new(
        TenantId::new(format!("{base}-{label}")),
        TenantPermissions::full_access(),
    )
}

fn patient(id: &str, family: &str) -> Value {
    json!({
        "resourceType": "Patient",
        "id": id,
        "name": [{"family": family}]
    })
}

fn family(stored: &StoredResource) -> String {
    stored.content()["name"][0]["family"]
        .as_str()
        .unwrap_or("?")
        .to_string()
}

/// Creates `Patient/{id}` and proves it reads back at version 1 — the positive
/// control: every writer below holds exactly this `StoredResource`.
async fn seed<S: ResourceStorage>(backend: &S, tenant: &TenantContext, id: &str) -> StoredResource {
    backend
        .seed_create(
            tenant,
            "Patient",
            patient(id, "Seed"),
            FhirVersion::default_enabled(),
        )
        .await
        .expect("seed patient");
    let current = backend
        .read(tenant, "Patient", id)
        .await
        .expect("read seeded patient")
        .expect("seeded patient exists");
    assert_eq!(current.version_id(), "1");
    assert_eq!(family(&current), "Seed");
    current
}

/// What one racing writer did.
#[derive(Debug)]
enum Outcome {
    Updated(StoredResource),
    Deleted,
    Failed(StorageError),
}

fn assert_losers_are_concurrency_errors(
    outcomes: &[Outcome],
    deleted: bool,
    policy: &RacePolicy,
    context: &str,
) {
    for outcome in outcomes {
        match outcome {
            Outcome::Updated(_) | Outcome::Deleted => {}
            Outcome::Failed(StorageError::Concurrency(_)) => {}
            // A writer that lost to a *delete* finds no live resource at all.
            Outcome::Failed(StorageError::Resource(ResourceError::NotFound { .. })) if deleted => {}
            Outcome::Failed(StorageError::Resource(ResourceError::Gone { .. })) if deleted => {}
            Outcome::Failed(e) if policy.is_unknown(e) => {}
            other => panic!("{context}: a loser is a concurrency refusal, got {other:?}"),
        }
    }
}

/// The writers whose outcome is unknown under `policy`, by index.
fn unknown_writers(outcomes: &[Outcome], policy: &RacePolicy) -> Vec<usize> {
    outcomes
        .iter()
        .enumerate()
        .filter(|(_, o)| matches!(o, Outcome::Failed(e) if policy.is_unknown(e)))
        .map(|(n, _)| n)
        .collect()
}

/// With no write landed, nobody can have been refused for losing to one: every
/// writer must be an unknown outcome.
fn assert_nobody_lost(outcomes: &[Outcome], unknown: &[usize], context: &str) {
    assert_eq!(
        unknown.len(),
        outcomes.len(),
        "{context}: no write landed, so no writer can have lost to one: {outcomes:?}"
    );
}

/// `WRITERS` tasks all hold version 1 of the same resource and `update` it at
/// once, `rounds` times over. Exactly one writes version 2; the rest are
/// concurrency errors; the stored resource and the history are the winner's.
pub async fn concurrent_updates_from_the_same_version_admit_one<S>(
    backend: Arc<S>,
    base: &str,
    rounds: usize,
) where
    S: ResourceStorage + VersionedStorage + Send + Sync + 'static,
{
    concurrent_updates_from_the_same_version_admit_one_with(
        backend,
        base,
        rounds,
        &RacePolicy::strict(),
    )
    .await;
}

/// [`concurrent_updates_from_the_same_version_admit_one`] under `policy`.
/// With timeouts accepted, the 504 can reach the winner: then no writer
/// reports success, and version 2 must hold the body of one 504'd writer, or
/// nothing landed and no writer was refused.
pub async fn concurrent_updates_from_the_same_version_admit_one_with<S>(
    backend: Arc<S>,
    base: &str,
    rounds: usize,
    policy: &RacePolicy,
) where
    S: ResourceStorage + VersionedStorage + Send + Sync + 'static,
{
    let t = tenant(base, "update-race");

    for round in 0..rounds {
        let id = format!("race-{round}");
        let current = seed(backend.as_ref(), &t, &id).await;
        let barrier = Arc::new(Barrier::new(WRITERS));

        let mut tasks = Vec::new();
        for n in 0..WRITERS {
            let (backend, t, current, barrier, id) = (
                backend.clone(),
                t.clone(),
                current.clone(),
                barrier.clone(),
                id.clone(),
            );
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                match backend
                    .update(&t, &current, patient(&id, &format!("Writer{n}")))
                    .await
                {
                    Ok(stored) => Outcome::Updated(stored),
                    Err(e) => Outcome::Failed(e),
                }
            }));
        }
        let mut outcomes = Vec::new();
        for task in tasks {
            outcomes.push(task.await.expect("writer task"));
        }
        let context = format!("round {round}");

        let winners: Vec<&StoredResource> = outcomes
            .iter()
            .filter_map(|o| match o {
                Outcome::Updated(stored) => Some(stored),
                _ => None,
            })
            .collect();
        let unknown = unknown_writers(&outcomes, policy);
        assert!(
            winners.len() == 1 || (winners.is_empty() && !unknown.is_empty()),
            "{context}: exactly one of {WRITERS} writers holding version 1 may write: {outcomes:?}"
        );
        assert_losers_are_concurrency_errors(&outcomes, false, policy, &context);

        let stored = backend
            .read(&t, "Patient", &id)
            .await
            .expect("read after race")
            .expect("still live");
        if let Some(winner) = winners.first() {
            assert_eq!(winner.version_id(), "2", "{context}");
            assert_eq!(stored.version_id(), "2", "{context}");
            assert_eq!(family(&stored), family(winner), "{context}");
        } else if stored.version_id() == "1" {
            assert_nobody_lost(&outcomes, &unknown, &context);
        } else {
            assert_eq!(stored.version_id(), "2", "{context}");
            assert!(
                unknown
                    .iter()
                    .any(|n| family(&stored) == format!("Writer{n}")),
                "{context}: with no writer told it won, version 2 is a 504'd writer's: \
                 {} after {outcomes:?}",
                family(&stored)
            );
        }

        let landed = stored.version_id() == "2";
        let (versions, pending) = history(backend.as_ref(), policy, &t, &id).await;
        assert_eq!(
            versions,
            versions_up_to(if landed { 2 } else { 1 }),
            "{context}: history has no gap and no duplicate"
        );
        if landed && !pending {
            let v2 = backend
                .vread(&t, "Patient", &id, "2")
                .await
                .expect("vread 2")
                .expect("version 2 exists");
            assert_eq!(
                family(&v2),
                family(&stored),
                "{context}: history's version 2 is the winner's content"
            );
        }
    }
}

/// Half the tasks `update` from version 1 and half `delete_versioned` version
/// 1, all at once. Exactly one of them wins: either version 2 is an update and
/// the resource is live, or version 2 is the tombstone — never an update *and*
/// a delete of "version 1", which is the delete removing a version its caller
/// never saw.
pub async fn concurrent_update_and_versioned_delete_admit_one<S>(
    backend: Arc<S>,
    base: &str,
    rounds: usize,
) where
    S: ResourceStorage + VersionedStorage + Send + Sync + 'static,
{
    concurrent_update_and_versioned_delete_admit_one_with(
        backend,
        base,
        rounds,
        &RacePolicy::strict(),
    )
    .await;
}

/// [`concurrent_update_and_versioned_delete_admit_one`] under `policy`. Which
/// write landed is read from the stored state: a live version 2 is an update
/// (an even writer), nothing live is the delete (an odd writer), and a live
/// version 1 is nothing at all.
pub async fn concurrent_update_and_versioned_delete_admit_one_with<S>(
    backend: Arc<S>,
    base: &str,
    rounds: usize,
    policy: &RacePolicy,
) where
    S: ResourceStorage + VersionedStorage + Send + Sync + 'static,
{
    let t = tenant(base, "delete-race");

    for round in 0..rounds {
        let id = format!("race-{round}");
        let current = seed(backend.as_ref(), &t, &id).await;
        let barrier = Arc::new(Barrier::new(WRITERS));

        let mut tasks = Vec::new();
        for n in 0..WRITERS {
            let (backend, t, current, barrier, id) = (
                backend.clone(),
                t.clone(),
                current.clone(),
                barrier.clone(),
                id.clone(),
            );
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                if n % 2 == 0 {
                    match backend
                        .update(&t, &current, patient(&id, &format!("Writer{n}")))
                        .await
                    {
                        Ok(stored) => Outcome::Updated(stored),
                        Err(e) => Outcome::Failed(e),
                    }
                } else {
                    match backend.delete_versioned(&t, "Patient", &id, "1").await {
                        Ok(()) => Outcome::Deleted,
                        Err(e) => Outcome::Failed(e),
                    }
                }
            }));
        }
        let mut outcomes = Vec::new();
        for task in tasks {
            outcomes.push(task.await.expect("writer task"));
        }
        let context = format!("round {round}");

        let winners: Vec<&Outcome> = outcomes
            .iter()
            .filter(|o| matches!(o, Outcome::Updated(_) | Outcome::Deleted))
            .collect();
        let unknown = unknown_writers(&outcomes, policy);
        assert!(
            winners.len() == 1 || (winners.is_empty() && !unknown.is_empty()),
            "{context}: exactly one of {WRITERS} writers holding version 1 may write: {outcomes:?}"
        );

        let after = backend.read(&t, "Patient", &id).await;
        let deleted = matches!(
            after,
            Ok(None) | Err(StorageError::Resource(ResourceError::Gone { .. }))
        );
        assert_losers_are_concurrency_errors(&outcomes, deleted, policy, &context);
        let live = if deleted {
            None
        } else {
            Some(after.expect("read after race").expect("still live"))
        };
        match (winners.first(), &live) {
            (Some(Outcome::Updated(winner)), Some(stored)) => {
                assert_eq!(stored.version_id(), "2", "{context}");
                assert_eq!(family(stored), family(winner), "{context}");
            }
            (Some(Outcome::Updated(_)), None) => {
                panic!("{context}: the update won, but nothing is live: {outcomes:?}")
            }
            (Some(_), live) => assert!(
                live.is_none(),
                "{context}: the delete won, so nothing is live: {live:?}"
            ),
            (None, None) => assert!(
                unknown.iter().any(|n| n % 2 == 1),
                "{context}: nothing is live, but no delete can have landed: {outcomes:?}"
            ),
            (None, Some(stored)) if stored.version_id() == "1" => {
                assert_nobody_lost(&outcomes, &unknown, &context)
            }
            (None, Some(stored)) => {
                assert_eq!(stored.version_id(), "2", "{context}");
                assert!(
                    unknown
                        .iter()
                        .any(|n| n % 2 == 0 && family(stored) == format!("Writer{n}")),
                    "{context}: with no writer told it won, version 2 is a 504'd \
                     update's: {} after {outcomes:?}",
                    family(stored)
                );
            }
        }

        let landed = live
            .as_ref()
            .is_none_or(|stored| stored.version_id() == "2");
        let (versions, _) = history(backend.as_ref(), policy, &t, &id).await;
        assert_eq!(
            versions,
            versions_up_to(if landed { 2 } else { 1 }),
            "{context}: one write landed, as version 2"
        );
    }
}

/// Half the tasks `update` from version 1 and half `delete` with no
/// precondition. A plain delete removes whatever is current, so an update and
/// a delete may both land (as versions 2 and 3) — but only one of each, every
/// other task is refused cleanly, and the history is contiguous. On MongoDB
/// this is the path with the one bounded retry of a write conflict (#1405).
pub async fn concurrent_update_and_plain_delete_stay_consistent<S>(
    backend: Arc<S>,
    base: &str,
    rounds: usize,
) where
    S: ResourceStorage + VersionedStorage + Send + Sync + 'static,
{
    concurrent_update_and_plain_delete_stay_consistent_with(
        backend,
        base,
        rounds,
        &RacePolicy::strict(),
    )
    .await;
}

/// [`concurrent_update_and_plain_delete_stay_consistent`] under `policy`. The
/// writes that landed are counted from the history and the stored state; each
/// beyond those reported as successes needs a 504'd writer of its kind.
pub async fn concurrent_update_and_plain_delete_stay_consistent_with<S>(
    backend: Arc<S>,
    base: &str,
    rounds: usize,
    policy: &RacePolicy,
) where
    S: ResourceStorage + VersionedStorage + Send + Sync + 'static,
{
    let t = tenant(base, "plain-delete-race");

    for round in 0..rounds {
        let id = format!("race-{round}");
        let current = seed(backend.as_ref(), &t, &id).await;
        let barrier = Arc::new(Barrier::new(WRITERS));

        let mut tasks = Vec::new();
        for n in 0..WRITERS {
            let (backend, t, current, barrier, id) = (
                backend.clone(),
                t.clone(),
                current.clone(),
                barrier.clone(),
                id.clone(),
            );
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                if n % 2 == 0 {
                    match backend
                        .update(&t, &current, patient(&id, &format!("Writer{n}")))
                        .await
                    {
                        Ok(stored) => Outcome::Updated(stored),
                        Err(e) => Outcome::Failed(e),
                    }
                } else {
                    match backend.delete(&t, "Patient", &id).await {
                        Ok(()) => Outcome::Deleted,
                        Err(e) => Outcome::Failed(e),
                    }
                }
            }));
        }
        let mut outcomes = Vec::new();
        for task in tasks {
            outcomes.push(task.await.expect("writer task"));
        }
        let context = format!("round {round}");

        let updates = outcomes
            .iter()
            .filter(|o| matches!(o, Outcome::Updated(_)))
            .count();
        let deletes = outcomes
            .iter()
            .filter(|o| matches!(o, Outcome::Deleted))
            .count();
        assert!(
            updates <= 1,
            "{context}: one update from version 1: {outcomes:?}"
        );
        assert!(
            deletes <= 1,
            "{context}: a resource is deleted once: {outcomes:?}"
        );
        let unknown = unknown_writers(&outcomes, policy);
        assert!(
            updates + deletes >= 1 || !unknown.is_empty(),
            "{context}: somebody wins: {outcomes:?}"
        );
        assert_losers_are_concurrency_errors(&outcomes, true, policy, &context);

        let after = backend.read(&t, "Patient", &id).await;
        let deleted = matches!(
            after,
            Ok(None) | Err(StorageError::Resource(ResourceError::Gone { .. }))
        );
        assert!(
            deletes == 0 || deleted,
            "{context}: a delete landed, so nothing is live: {after:?}"
        );
        let (versions, _) = history(backend.as_ref(), policy, &t, &id).await;
        let landed_deletes = usize::from(deleted);
        let landed_updates = versions.len().saturating_sub(1 + landed_deletes);
        let unknown_updates = unknown.iter().filter(|n| *n % 2 == 0).count();
        let unknown_deletes = unknown.len() - unknown_updates;
        assert!(
            landed_updates <= 1
                && (updates..=updates + unknown_updates).contains(&landed_updates)
                && (deletes..=deletes + unknown_deletes).contains(&landed_deletes),
            "{context}: one version per successful write, no gap, no duplicate: \
             {versions:?} after {outcomes:?}"
        );
        assert_eq!(
            versions,
            versions_up_to(1 + landed_updates + landed_deletes),
            "{context}: one version per successful write, no gap, no duplicate: {outcomes:?}"
        );
        if landed_updates + landed_deletes == 0 {
            assert_nobody_lost(&outcomes, &unknown, &context);
        }

        if !deleted {
            let stored = after.expect("read after race").expect("still live");
            assert_eq!(
                stored.version_id(),
                (1 + landed_updates).to_string(),
                "{context}"
            );
        }
    }
}

/// How many unconditional PUTs race onto one tombstone. `create_or_update`
/// retries a lost race three times, and each lost attempt means another racer
/// committed after this one's read — each racer commits once and then stops —
/// so four racers are the most for which every PUT succeeding is guaranteed
/// rather than likely.
const RESTORE_WRITERS: usize = 4;

/// `RESTORE_WRITERS` tasks race an unconditional `create_or_update` onto the
/// same soft-deleted id (#2111). Every PUT succeeds: exactly one restores the
/// tombstone (`created`, version 3) and the rest converge through the update
/// arm, each under its own version. The history is contiguous with no
/// duplicate, and its newest entry carries the live body. A restore that is a
/// read-then-write instead lets two racers both compute version 3: the second
/// history insert violates the primary key (an `Internal` error, a 500), and
/// the live row and history version 3 hold different bodies.
pub async fn concurrent_puts_onto_a_tombstone_all_succeed<S>(
    backend: Arc<S>,
    base: &str,
    rounds: usize,
) where
    S: ResourceStorage + VersionedStorage + Send + Sync + 'static,
{
    concurrent_puts_onto_a_tombstone_all_succeed_with(backend, base, rounds, &RacePolicy::strict())
        .await;
}

/// [`concurrent_puts_onto_a_tombstone_all_succeed`] under `policy`. A 504'd
/// PUT may or may not have landed, so the history ends anywhere from the
/// successes' count to that plus the 504s, and the restore may be among the
/// 504s; the successes still each have their own version within it.
pub async fn concurrent_puts_onto_a_tombstone_all_succeed_with<S>(
    backend: Arc<S>,
    base: &str,
    rounds: usize,
    policy: &RacePolicy,
) where
    S: ResourceStorage + VersionedStorage + Send + Sync + 'static,
{
    let t = tenant(base, "restore-race");

    for round in 0..rounds {
        let id = format!("race-{round}");
        seed(backend.as_ref(), &t, &id).await;
        backend
            .seed_delete(&t, "Patient", &id)
            .await
            .expect("delete seeded patient");
        assert_eq!(
            backend
                .list_versions(&t, "Patient", &id)
                .await
                .expect("list versions"),
            vec!["1".to_string(), "2".to_string()],
            "the tombstone is version 2"
        );
        let barrier = Arc::new(Barrier::new(RESTORE_WRITERS));

        let mut tasks = Vec::new();
        for n in 0..RESTORE_WRITERS {
            let (backend, t, barrier, id) =
                (backend.clone(), t.clone(), barrier.clone(), id.clone());
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                backend
                    .create_or_update(
                        &t,
                        "Patient",
                        &id,
                        patient(&id, &format!("Writer{n}")),
                        FhirVersion::default_enabled(),
                    )
                    .await
            }));
        }
        let mut puts = Vec::new();
        for task in tasks {
            puts.push(task.await.expect("writer task"));
        }
        let context = format!("round {round}");

        let mut unknown = 0;
        let puts: Vec<(StoredResource, bool)> = puts
            .into_iter()
            .filter_map(|r| match r {
                Ok(put) => Some(put),
                Err(e) if policy.is_unknown(&e) => {
                    unknown += 1;
                    None
                }
                Err(e) => panic!("{context}: every PUT must succeed: {e:?}"),
            })
            .collect();
        let created = puts.iter().filter(|(_, created)| *created).count();
        assert!(
            created == 1 || (created == 0 && unknown > 0),
            "{context}: exactly one PUT restores the tombstone"
        );
        let mut versions: Vec<u64> = puts
            .iter()
            .map(|(stored, _)| stored.version_id().parse().expect("numeric version"))
            .collect();
        versions.sort_unstable();
        versions.dedup();

        let (chain, pending) = history(backend.as_ref(), policy, &t, &id).await;
        let landed = chain.len().saturating_sub(2);
        assert!(
            (puts.len().max(1)..=puts.len() + unknown).contains(&landed),
            "{context}: history has no gap and no duplicate: {chain:?}, \
             {} PUTs succeeded and {unknown} answered 504",
            puts.len()
        );
        assert_eq!(
            chain,
            versions_up_to(2 + landed),
            "{context}: history has no gap and no duplicate"
        );
        assert!(
            versions.len() == puts.len()
                && versions.iter().all(|v| (3..=2 + landed as u64).contains(v)),
            "{context}: each PUT gets its own version, continuing the chain: {versions:?}"
        );

        let newest = (2 + landed).to_string();
        let stored = backend
            .read(&t, "Patient", &id)
            .await
            .expect("read after race")
            .expect("live after restore");
        assert_eq!(stored.version_id(), newest, "{context}");

        if !pending {
            let latest = backend
                .vread(&t, "Patient", &id, &newest)
                .await
                .expect("vread newest")
                .expect("newest version exists");
            assert_eq!(
                latest.content(),
                stored.content(),
                "{context}: the newest history entry is the live body"
            );
        }
    }
}

/// `delete_versioned` is a compare-and-swap on the *current* version: a stale
/// version is refused and deletes nothing, the current one deletes, and a
/// second delete of the same version finds nothing live.
pub async fn versioned_delete_is_a_compare_and_swap<S>(backend: &S, base: &str)
where
    S: ResourceStorage + VersionedStorage,
{
    let t = tenant(base, "delete-cas");
    let v1 = seed(backend, &t, "cas").await;
    let v2 = backend
        .seed_update(&t, &v1, patient("cas", "Updated"))
        .await
        .expect("update to v2");
    assert_eq!(v2.version_id(), "2");

    // The version the client saw is gone: refuse, and leave v2 live.
    let stale = backend.delete_versioned(&t, "Patient", "cas", "1").await;
    assert!(
        matches!(stale, Err(StorageError::Concurrency(_))),
        "a stale versioned delete is a concurrency error, got {stale:?}"
    );
    let live = backend
        .read(&t, "Patient", "cas")
        .await
        .expect("read")
        .expect("a refused delete deletes nothing");
    assert_eq!(live.version_id(), "2");
    assert_eq!(family(&live), "Updated");

    // `delete_with_match` takes an `If-Match` field value and goes the same way.
    let stale = backend
        .delete_with_match(&t, "Patient", "cas", "W/\"1\"")
        .await;
    assert!(
        matches!(stale, Err(StorageError::Concurrency(_))),
        "{stale:?}"
    );

    backend
        .delete_versioned(&t, "Patient", "cas", "2")
        .await
        .expect("the current version deletes");
    let after = backend.read(&t, "Patient", "cas").await;
    assert!(
        matches!(
            after,
            Ok(None) | Err(StorageError::Resource(ResourceError::Gone { .. }))
        ),
        "{after:?}"
    );
    assert_eq!(
        backend
            .list_versions(&t, "Patient", "cas")
            .await
            .expect("list versions"),
        vec!["1".to_string(), "2".to_string(), "3".to_string()],
        "the tombstone is version 3"
    );

    let again = backend.delete_versioned(&t, "Patient", "cas", "2").await;
    assert!(
        matches!(
            again,
            Err(StorageError::Resource(ResourceError::NotFound { .. }))
        ),
        "nothing live is left to delete: {again:?}"
    );
}
