# ADR-aacruzgon-0044: Upgrade `cdrs-tokio` to 10.0.0, and drop the workarounds it makes unnecessary

- **Date:** 2026-11-06
- **Status:** accepted (Alan decided the upgrade on 2026-11-06; the workaround choices below are the worker's, approved by Alan). The session-rebuild record written for PR #37 was not committed: the rebuild is the alternative that lost, below. ADR-aacruzgon-0032 (driver settings) was rewritten for 10.0.0 alongside this entry.
- **Context:**
    - The backend was built on `cdrs-tokio` 8.1.9 (the design's "Decision: driver", `cassandra-discussion.md:229`: "We stay on `cdrs-tokio` 8 as the crate already declares it"; no ADR recorded the driver choice). 9.1.0 and 10.0.0 were released on 2026-11-04 ([changelog](https://github.com/krojew/cdrs-tokio/blob/master/changelog.md)).
    - 9.1.0 fixes the bug that made a single-node session fail for good after a Cassandra restart: `clone_with_node_info` now compares `broadcast_rpc_address` with the refreshed `broadcast_rpc_address`, and a down node listed in the peers table is promoted to up (10.0.0, `cluster/topology/node.rs:376-413`). PR #37 worked around that bug by rebuilding the driver session.
    - 9.1.0 also fixes a correctness bug of 8.1.9: the stream id of a request in flight could be reused, so a response could reach the wrong caller. A stream id is now handed out again only after its response or error was delivered (`transport.rs:874-958`). On a backend where one CAS answer decides 201 against 409, that is not optional.
    - A third 8.1.9 bug, found while building the stored-SearchParameter reload (ADR-aacruzgon-0041): a response larger than one v5 frame is lost. It was fixed in 9.0.2, and the fix is in 9.1.0 and 10.0.0.
    - Three more workarounds in PR #37 answered 8.1.9 behaviour: the `LocalDcOnly` distance evaluator, the startup refusal of contact points outside `HFS_CASSANDRA_LOCAL_DC`, and the connect deadline with its TCP probe. 9.1.0 changes the behaviour each one answers (dc-local plans, contact-point distances, `SessionInitFailed`), so each had to be decided again, from the source and from runs, not from the changelog.
- **Decision:**
    - **A deviation from the design.** This replaces the design's "stay on `cdrs-tokio` 8" (`cassandra-discussion.md:229`). The rest of that paragraph still holds:
        - the project is essentially single-maintainer;
        - the two advisories `audit.toml` ignores for it (`RUSTSEC-2024-0388` `derivative`, `RUSTSEC-2025-0057` `fxhash`, `audit.toml:8-10`) remain at 10.0.0: `derivative` is a dependency of `cassandra-protocol` 5.0.0, and `derivative` and `fxhash` of `cdrs-tokio` 10.0.0 (`Cargo.lock`);
        - it exposes per-statement serial consistency, typed write timeouts with `write_type`, prepared statements and manual paging.

      `scylla` stays the named fallback. The paragraph, in this entry's commit, reads: "**Decision: driver.** We use `cdrs-tokio` 10 (locked 10.0.0, upgraded from 8.1.9 on 2026-11-06, ADR-aacruzgon-0044: 9.1.0 fixed a single-node session lost after a restart and the reuse of in-flight stream ids). It is essentially a single-maintainer project, and the two advisories `audit.toml` ignores for it (`derivative` and `fxhash`, both "unmaintained") remain in 10.0.0, but it exposes everything this design needs: per-statement serial consistency, typed write-timeout errors with `write_type`, prepared statements and manual paging. The `scylla` driver … [unchanged]".

      The design's `CAS_WRITE_UNKNOWN` sentence (`:291`) then reads "`cdrs-tokio` 10 decodes it as `ErrorType::CasWriteUnknown`, and the backend treats it as unknown", and walkthrough 3.1 (`walkthrough.md:339`) is amended to match, in the same commit. ADR-aacruzgon-0024 names 8.1.9 too (its lines 59-62, 65, 69 and 160); under the log's append-only rule it is not edited, and this entry supersedes those statements.
    - **Versions.** `cdrs-tokio` 10.0.0 and `cassandra-protocol` 5.0.0, the newest releases on 2026-11-06; the workarounds kept below were checked against 8.1.9 and 10.0.0. `uuid` moves from 1.18.1 to 1.19.0, the lowest version the driver accepts (`cargo update -p uuid --precise 1.19.0`). `cdrs-tokio-helpers-derive` is dropped instead of bumped to 6.0.0: nothing in the workspace derives `TryFromRow`, `TryFromUdt`, `IntoCdrsValue` or `DbMirror` (`git grep`), and the build passes without it. No fallback to 9.1.0 was needed: 10.0.0 had no blocker.
    - **Error mapping.** `CAS_WRITE_UNKNOWN` now decodes, as `ErrorType::CasWriteUnknown`, where 8.1.9 raised `Error::UnexpectedErrorCode`. The CAS path names it as an unknown outcome (504 when the serial read cannot decide, as before), and the generic table gives it 504. `CdcWriteFailure` is 503 (no table uses CDC). `ErrorType::Unknown` (a code the driver cannot name) is 500 in the generic table and an unknown outcome on a CAS. `InvalidProtocol`, raised only while opening a node's pool (`cluster/connection_manager.rs:63`, passed on at `cluster/topology/node.rs:294-300`), is now classed as not sent on a CAS (audit; ADR-aacruzgon-0034). `Error` and `ErrorType` are `#[non_exhaustive]`; the wildcard arms give `Internal` (500) in the table and "unknown, confirm by the serial read" on a CAS, the safe answers.
    - **Session rebuild: removed.** With the rebuild switched off by a temporary local switch (never committed), the stop/start scenario recovered on its own in 10 runs out of 10 (table below). The holder, its single-flight task, backoff, drop guard and TCP probe go; `CassandraSession` holds one driver session and its prepared cache. The restart test stays as the regression test for the driver's recovery: it now asserts that the same session, with its prepared statements, serves throughout.
    - **`LocalDcOnly`: kept.** Since 9.1.0 a topology-aware plan leaves remote nodes out for a dc-local consistency on a `NetworkTopologyStrategy` keyspace (`load_balancing/topology_aware.rs:264-272`). But a `SimpleStrategy` or unknown-strategy keyspace still ends its plan with every unignored node, local or remote (`:292-330`), and the driver's own evaluator would mark remote nodes `Remote`, not ignored. The evaluator costs nothing and keeps `LOCAL_*` requests in one DC whatever the keyspace says (ADR-aacruzgon-0032).
    - **Remote-DC contact-point refusal at `connect`: removed.** In 8.1.9 a contact point kept the `Local` distance it started with. In 10.0.0:
        - the metadata starts empty (`cluster/cluster_metadata_manager.rs:341`);
        - every node in it is built from `system.local` / `system.peers_v2` with the evaluator's distance, contact points included (`metadata_builder.rs:12-80`, `topology/node.rs:421-441`), as are nodes added by events (`cluster_metadata_manager.rs:540-560`);
        - the contact points themselves, with their `Local` distance, are a query plan only while the metadata holds no node (`load_balancing/initializing_wrapper.rs:21-31`), that is, before the first refresh, before `build()` returns.

      With `LocalDcOnly`, every node with a distance is therefore in the local DC, so the check could never fire.

      **One behaviour change:** a contact point in another DC is now accepted, where PR #37 refused it. The driver learns the whole cluster from it, and routes only to the local-DC nodes it lists. That is safe, since no request reaches the remote node, but it is a change [inferred from the source above; not run, since there is no two-DC cluster here].

      If the cluster has no node in the local DC at all, `connect` still fails in `verify_local_dc`: every node is ignored, so its query fails, and the error names `HFS_CASSANDRA_LOCAL_DC` (the test `cassandra_connect_refuses_a_local_dc_the_cluster_does_not_have` covers that path with a wrong DC name).
    - **Connect deadline: kept; the TCP probe: removed.**
        - The driver now fails the build by itself when no contact point connects (`cluster/control_connection.rs:170-176`, `SessionBuildError::SessionInitFailed`, `cluster/session.rs:1005`), and `connect_timeout` applies to the control connection.
        - But when a contact point connects and the metadata refresh fails, it retries for good (`control_connection.rs:166-169`), so `connect` keeps one deadline over resolving and building (ADR-aacruzgon-0031).
        - Cutting the build off no longer leaks: the control-connection task is held in an `AbortOnDropHandle`, aborted when the build fails or its future is dropped (`cluster/session.rs:996-1006`). The probe existed only to avoid that leak for a closed port, so it goes.
    - **Kept as it was:** `LOCAL_QUORUM` / `LOCAL_SERIAL` on every statement; a CAS non-idempotent and carrying `FallthroughRetryPolicy` (`retry/retry_policy.rs:52-69`). The default retry session still re-sends any statement after `Unavailable` and `IsBootstrapping`, and once on the same node after a `ReadTimeout`, now when it retrieved no data (`:139-156`, 9.1.0's "read timeouts are retried"; 8.1.9 re-sent only when data was present, `:120-137`). That re-send is safe for `Read` and `SerialRead`, and a CAS never gets it.
        - No speculative execution, and the client deadline: the driver still has no request timeout.
        - The R2 `LOCAL_SERIAL` settle and the 503/504 split are unchanged.
- **Alternatives considered:**
    - **Patch or fork `cdrs-tokio` 8.1.9:** lost. The single-node bug is one comparison (8.1.9 compared `broadcast_address` with the refreshed `broadcast_rpc_address`), and its fix shipped in 9.1.0 (`topology/node.rs:255-270` at 9.1.0, `:376-413` at 10.0.0), together with the stream-id fix. A fork would be a dependency the project maintains, for a bug upstream has already fixed.
    - **Stay on 8.1.9 with PR #37's workarounds:** lost. The session rebuild covers only the lost node. Stream ids of requests in flight can still be reused at 8.1.9, so a response can reach the wrong caller (9.1.0 changelog), and no workaround of ours can fix that.
    - **9.1.0.** It already has the node fix and the stream-id fix. But 10.0.0 adds pools kept across refreshes and an immediate reconnect when a node is marked up, which should shorten recovery [inferred, not measured on 9.1.0], and the port cost only constructors in test code. 9.1.0 stays the fallback if 10.0.0 shows a blocker.
    - **Keep the session rebuild (PR #37's `CassandraSession` holder, `5cba87d6a`) as defence in depth:** lost. It rebuilt the driver session when 8.1.9 lost its only node, retried an idempotent statement once, never a CAS, one rebuild at a time with a backoff. Its reasons are gone at 10.0.0:
        - the bug itself: `topology/node.rs:376-413`;
        - the task of its own, written because a cancelled build orphaned the control-connection task: the build now holds that task in an `AbortOnDropHandle` (`cluster/session.rs:996-1006`);
        - the metadata check, which guarded against the empty-plan text following an exhausted retry: an exhausted plan now returns the last node's error (`send_envelope.rs:33-75`).

      With the rebuild switched off the driver recovered in 10 restart runs of 10 (table below). Kept, it would be about 450 lines (a spawned task, a lock, a watch channel and a panic guard) that never run, and so are never tested. If Step 8's failure tests show the driver losing the node again, it can come back from `5cba87d6a`.
    - **Keep the remote-DC refusal.** It can no longer fire: see above. A check that cannot fail tells an operator nothing.
    - **Keep the TCP probe.** It made a closed port fail fast and spawned nothing. The driver now does both itself.
    - **Bump `cdrs-tokio-helpers-derive` to 6.0.0.** Nothing uses it, and 6.0.0's generated code needs `cdrs-tokio` as a direct dependency anyway. Dropping it removes a dependency.
- **Consequences:**
    - The backend no longer delivers a response to the wrong caller under concurrency (the 8.1.9 stream-id bug).
    - Recovery after a restart is the driver's job, so it takes the driver's reconnection time: 11.1 to 29.6 s after `docker start` in the 10 runs, mean 24.2 s. With the rebuild on 8.1.9 it took 11.9 to 16.1 s (the worker's local restart runs; CI measured 14.0 and 16.1 s, ADR-aacruzgon-0042). Both are well inside the readiness budget (240 s, `cassandra_milestone_a.rs` `UP_TIMEOUT`). This is the re-run ADR-aacruzgon-0042 asked for.
    - The design and the roadmap named `cdrs-tokio` 8 / 8.1.9 and said `CAS_WRITE_UNKNOWN` would not decode (`cassandra-discussion.md:229`, `:279`, `:289`, `:291`; walkthrough 3.1; roadmap Step 2). The behaviour is the same, an unknown outcome; the text gets the version and the variant name in this entry's commit.
    - `docker/cassandra/README.md` and `crates/hfs/tests/cassandra_milestone_a.rs:12-13` described the 8.1.9 restart failure and the rebuild; PR #45 updates both (commit `e55f741da`).
    - **Older than PR #45, more relevant now** (audit): the pool's `connect_timeout` is the whole `HFS_CASSANDRA_CONNECT_TIMEOUT_MS`, and since 9.1.0 it also bounds each control-connection attempt (`cluster/connection_pool.rs:224-237`). Those attempts try the contact points in order. So a first contact point that drops packets silently uses the whole connect deadline, and startup fails even if later contact points are healthy. A closed port, which refuses at once, does not have this problem. To revisit with Step 8's failure tests: a per-connection timeout of the deadline divided by the number of contact points, or a setting of its own.
    - Not exercised: a three-node cluster, and a restart under write load (Step 8's failure tests).
- **AI involvement:** Written up by a Claude Code worker (`s2-backend`, Opus 5.5) for roadmap Step 2, PR #45. Alan decided the upgrade; the worker read the 10.0.0, cassandra-protocol 4.1.0 and 5.0.0, 9.1.0 and 9.0.x changelog sections in full and the 10.0.0 source at each path cited above. Verified:
    - the build, clippy and the R4B check against 10.0.0;
    - the unit tests, which include the new error variants;
    - `cassandra_tests` and `backend_capability_contract` against the shared 5.0.9 node;
    - both Milestone A tests;
    - 10 restart runs with the rebuild switched off, on a node of the worker's own (Compose project `s2l8-restart`, port 59442, removed afterwards).
    - **What Alan changed:** Alan reviewed every Step 2 decision record on 2026-11-06, before it was committed. From his own review he found that several alternatives were stale, because `cdrs-tokio` 9.1.0 and 10.0.0 (released 2026-11-04) had already fixed driver behaviour the records relied on. He found that some alternatives were not real options or did not make sense: strawmen, restatements of the decision, and the workers' own debugging steps listed as alternatives. And he found that some records cited sources outside the repository. He had a review agent re-audit every alternative against the code, the design documents and `cdrs-tokio` 10.0.0, accepted its recommendations, and had each record rewritten from them. For this entry: Alan decided the upgrade himself on 2026-11-06 ("lets upgrade the dependency if it has been fixed already"), over a patch or a fork of the driver, after finding the session-rebuild record's alternatives stale. The worker then decided each workaround by restart runs and the 10.0.0 source. This entry carries the session rebuild as the alternative that lost, in place of the session-rebuild record, which was considered and dropped, not committed. "Patch or fork" and "stay on 8.1.9" were added as alternatives.
- **Links:** PR #45 (branch `cassandra/step2-driver-upgrade`, on `cassandra/step2-milestone-a` `bb6cc8a3d`: `d972a3867`, `f358be3cc`, `9c2e720cb`, `e55f741da`, `2c50ae427`; before the restack the first three were `8d8b8a627`, `dcdf350c6`, `a2b5cfa96`); ADR-aacruzgon-0003, 0024, 0031, 0032, 0034, 0041, 0042; `cassandra-discussion.md`, "Decision: driver", "When the LWT outcome is unknown" and "Which error, and why"; `docs/Cassandra/schema/walkthrough.md` 3.1; `development-roadmap.md` Step 2 (connection setup, Cassandra docs).

## Restart runs, rebuild switched off

All on a node of the worker's own (`docker/cassandra` Compose file, project `s2l8-restart`, `127.0.0.1:59442`, schema applied), each a fresh process of `backend.rs` `restart_tests` with a temporary local switch set (never committed). Each run:
- `docker stop`, then 16 concurrent `health_check`s every 500 ms until all fail;
- `docker start`, then the same polls until all pass, within 240 s;
- 120 s of one `health_check` and one read every second, all of which must pass.

With the switch, `execute` returned the driver's error instead of rebuilding. The test asserted that the session generation stayed 0 and that no rebuild was attempted.

| Run | Down after the stop | Up after the start | Polls of 16 callers | After recovery | Prepared before / after |
| --- | --- | --- | --- | --- | --- |
| 1 | 0.20 ms | 28.1 s | 57 | 120 s, 119 checks + reads, all ok | 5 / 5 |
| 2 | 0.18 ms | 11.1 s | 23 | 120 s, 119 checks + reads, all ok | 5 / 5 |
| 3 | 0.16 ms | 25.1 s | 51 | 120 s, 119 checks + reads, all ok | 5 / 5 |
| 4 | 0.18 ms | 27.6 s | 56 | 120 s, 119 checks + reads, all ok | 5 / 5 |
| 5 | 0.26 ms | 29.6 s | 60 | 120 s, 119 checks + reads, all ok | 5 / 5 |
| 6 | 0.20 ms | 26.1 s | 53 | 120 s, 119 checks + reads, all ok | 5 / 5 |
| 7 | 0.26 ms | 26.6 s | 54 | 120 s, 119 checks + reads, all ok | 5 / 5 |
| 8 | 0.17 ms | 22.2 s | 45 | 120 s, 119 checks + reads, all ok | 5 / 5 |
| 9 | 0.92 ms | 23.1 s | 47 | 120 s, 119 checks + reads, all ok | 5 / 5 |
| 10 | 0.18 ms | 22.6 s | 46 | 120 s, 119 checks + reads, all ok | 5 / 5 |

**10 of 10 recovered on their own:** up 11.1 to 29.6 s after the start (mean 24.2 s). Never a failure after recovery: 8.1.9 lost the node 40 s after recovery in PR #36's reproduction.

Afterwards, on the committed code with no switch, rebased onto the parent (then `8c18196b2`, now `bb6cc8a3d`):
- the restart test passed once more: up 29.2 s after the start, 120 s stable, 5 / 5 prepared;
- the Milestone A readiness test passed on the same node: `/_readiness` down 1.5 ms after the stop, up 29.3 s after the start.

The node was removed with `down -v`.
