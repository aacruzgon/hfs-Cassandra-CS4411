# ADR-aacruzgon-0083: A report-only nightly job runs the backend's tests and five pause scenarios on three nodes, with the tests in a container on the cluster's network

- **Date:** 2026-11-20
- **Status:** accepted
- **Context:**
    - **No CI job ran Cassandra on three nodes.** Every Cassandra job used one node: the required `Cassandra schema (single node)`, coverage and the FHIR-versions job. The roadmap's failure tests run "against the single node first, then the three-node cluster" (`development-roadmap.md` section 6, `:612`), and row 8.3 (`:108`) gives the three-node ones to Step 8.
    - **Some behaviour exists only on three nodes.** The PR #90 review ran the cluster by hand on `acruzdebian` on 2026-11-19 (`report-pr90-review.md`, "Three nodes"):
        - a one-partition logged batch that times out reports `SIMPLE`, `received 1`, `block_for 2`, and has applied on the coordinator;
        - once two replicas are marked down, the same batch gets `Unavailable`;
        - a two-partition logged batch with one replica paused times out as `BATCH_LOG`, and a batchlog replay still applies it.
    - **Issue #77.** On RF 3 a concurrent write can answer 504 (ADR-aacruzgon-0072, decision 4). The race suites still panic on it. The fix, reading the `helios` keyspace's RF rather than an env var or the node count (my comment on #77, 2026-11-12), is PR #83, still open; `main` at `e5e8ea67f` has neither an env var nor node-count detection. So on `main` every #77 504 is a failing test.
    - **The runners' Docker is rootless,** with `slirp4netns` and the `builtin` port driver (ADR-sam-michaels-0004, ADR-aacruzgon-0023). The nodes' addresses on the cluster network live in rootlesskit's network namespace, so the runner reaches only the published ports. The backend's driver learns every node's address from `system.peers_v2` and routes to all three (`session.rs`: `TopologyAwareLoadBalancingStrategy` with `LocalDcOnly`). `docker/cassandra/README.md` already says the same for Docker Desktop.
    - **The heavy label is changing.** PR #100 (`ci-mutation-flake`) splits the runners into light, medium and heavy, with heavy on `acruzdebian` (40G) only. Until it merges, heavy also matches `sam-7900x`, so a heavy job must still fit its 20G (ADR-aacruzgon-0080).
- **Decision:**
    - **A new workflow, `.github/workflows/cassandra-three-node.yml`, report-only.**
        - **Triggers:** `schedule` daily at 10:00 UTC, and `workflow_dispatch` with inputs `ref`, `test_filter` (for `cassandra_tests`), `lib_filter` (default `node_tests`) and `repeats` (default 5, 1 to 20). No `pull_request` trigger.
        - **Why 10:00 UTC:** of the fork's last 1,000 runs (2026-11-04 to 2026-11-20, Actions API), none started between 10:00 and 12:59 UTC and 3 at 09:00, against 45 to 108 an hour in the evening. It is clear of the canary (02:00) and the Saturday prune (20:00).
        - **The job:** `Cassandra three-node tests (report only)`, in no ruleset, on `[self-hosted, hfs-cassandra-heavy]`, `timeout-minutes: 120`, with the fork guard. `concurrency` is one group for every ref, `cassandra-three-node`, without cancelling, so two runs never overlap.
    - **The cluster.**
        - `docker-compose.cluster.yml` with a new override, `docker-compose.cluster.heap512m.yml`: 512M heap, 256M direct memory, `mem_limit` and `memswap_limit` 1700m, as the review used. 5.1 GiB of limits in all.
        - Its own Compose project `hfs-cassandra-3n`, subnet `10.59.142.0/24` and host ports 59642 to 59644.
        - `keyspace-three-node.cql` (RF 3), then `schema.cql`; the job waits until node 1 sees three nodes `UN` and every node reports one schema version.
        - `down -v` on that project at the end, under `always()`, so also after a failure, a timeout or a cancel, and once more at the start for a run that was killed.
    - **The tests run in a container on the cluster's network.** `run-tests.sh` starts the pinned Cassandra image (Debian 13, glibc 2.41, `libssl3`, already on the runner), with the checkout mounted read-only and the target directory read-write at their own paths, and runs the binary that the build step made. `HFS_TEST_CASSANDRA_URL` is node 1's cluster address, `10.59.142.11:9042`.
        - **Step 1:** `tests/cassandra_tests.rs`, every test.
        - **Step 2:** the `--lib` tests filtered to `node_tests` (write.rs, backend.rs, delete.rs, update.rs, upsert.rs, versions.rs). `restart_tests` and `tombstone_limit_tests` skip, as they need variables of their own.
        - **Each binary runs `repeats` times.** A pass of both takes about 10 s, and #77's 504 is a rate.
        - **No test changes.** A failing test fails its step only (`continue-on-error`), and the job succeeds.
    - **Five pause scenarios, in raw CQL from `cqlsh` on node 1** (`pause-scenarios.sh`), after the tests. No backend code runs, so nothing is added to the backend's test tree.
        - **s1:** a one-partition logged batch (an update's index delta: insert v2 and delete v1 in one `(tenant, Patient)` partition), node 2 paused, sent at once.
        - **s2:** the same batch with nodes 2 and 3 paused, sent at once, while node 1 still sees them `UN`.
        - **s3:** the same batch with nodes 2 and 3 marked `DN`.
        - **s4a and s4b:** a two-partition logged batch (Patient and Observation) with node 2 paused, sent at once and then after it is marked `DN`.

        Each answer, its write type and counts, its time, and whether it applied (read at `ALL` once every node is `UN` again and 25 s have passed) are compared with the review's results. They are recorded, not asserted.
    - **Reports.** The job writes these to the step summary and the artifact `ci-report-three-node` (14 days):
        - from `ci-report.py`: the build (peak `anon` and `memory.current`, OOM kills, `--timings`), the start and schema apply, the test counts and skips of every repeat, the CAS counters, the intent rows left behind, each node's statistics, the target directory's size, and where the time went;
        - from `report.py`:
            - every failing test with the first line of its failure, how many runs it failed in, and its cause: a #77 504, another 504, a 503, expected on three nodes, or other;
            - the pause scenarios;
            - each container's `memory.peak` and OOM kills, and a `docker stats` sample every 5 s;
        - in the artifact: the full test logs, `nodetool status` and `describecluster` from every node, each node's `system.log` tail and container log, and the scenarios' raw `cqlsh` output.
    - **Limits.** Its own target directory, `target/cassandra-three-node`, with the required Cassandra job's 15 GB cap and reset rule (ADR-sam-michaels-0002), and the cargo `CACHEDIR.TAG` (ADR-aacruzgon-0070). The test container is capped at 6 GiB, and each run of a binary is stopped after 8 minutes (6 for `--lib`).
    - **Row 8.3 is unchanged.** This job runs the existing tests and measures; it does not own or replace Step 8's failure tests. They can run here once they exist.
- **Alternatives considered:**
    - **Run the tests on the runner, through the published ports:** lost. The runner does not reach the nodes' cluster addresses (measured in run 44681897890: the probe step's TCP connect from the runner to `10.59.142.11`, `.12` and `.13` on port 9042 did not complete within 3 s, for all three), and the driver routes to all three, so a request routed to node 2 or 3 would time out. Restricting the driver to its contact point would be a change to the backend.
    - **Enter rootlesskit's network namespace with `nsenter`:** lost. It depends on rootless Docker's internals (the daemon's child PID and its user namespace). The container is the route `docker/cassandra/README.md` already documents.
    - **A Rust image, or `cargo test` inside the container:** lost. A Rust image is another pull, and its glibc can differ from the build host's. The Cassandra image is already on the runner and matches Debian 13's glibc; running the built binary directly needs no toolchain in the container.
    - **The base file's limits (768M heap, 2.5 GiB per node, 7.5 GiB):** lost. They fit 20G too, but 1700 MiB is what the review and #77's twelve runs used, and the nodes peaked at 1,162 to 1,249 MiB (run 44680155092).
    - **A `pull_request` trigger:** lost for this first version. The job holds the heavy runner for about 8 minutes, and it measures rather than gates.
    - **A job in `fmt-clippy-cassandra.yml`:** lost. That workflow carries four of the five required checks, and PR #100 is changing it.
    - **The pause scenarios as a backend test (the review's `audit_3n`):** lost. It needed a scratch test in `write.rs`'s `node_tests` and a change to `LocalDcOnly` so every request goes to node 1 (`3n-scratch.diff`). Raw CQL from node 1 gets the same coordinator answers without backend code, and matched the review in every scenario.
    - **Subnet `172.29.x` or port 59542:** lost. `172.29.0.0/16` is inside Docker's default address pools (ADR-aacruzgon-0072), and 59542 is the benchmark harness's default (PR #76). `10.59.142.0/24` is outside the default pools and clear of `slirp4netns`'s own `10.0.2.0/24`, and ports 59642 to 59644 were free on `acruzdebian` (`ss -ltn`, 2026-11-20).
    - **Sharing `target/cassandra-tests`:** lost. It builds the same two binaries, but a nightly run would change what the required job finds warm.
    - **One pass of each binary:** lost. A pass takes seconds, and a single pass gives #77 as yes or no.
    - **An `HFS_TEST_CASSANDRA_CLUSTER` variable for the suites:** not added. The job changes no test, and #83 reads the RF instead.
- **Consequences:**
    - **Easier.**
        - Every night, the #77 rate on `main` and every other failure on three nodes, with its cause. When #83 merges, those 504s should become passes, and the job will show whether they do.
        - The three coordinator answers the review found by hand are checked every night.
        - Steps 4, 5 and 8 have a running cluster job to add their own three-node tests to.
    - **Harder.**
        - The heavy runner is busy for about 8 to 10 minutes a night, and disk use on each runner grows by up to 15 GB.
        - `workflow_dispatch` only works once the file is on `main` (GitHub answers HTTP 404 otherwise). Until then a temporary `push` trigger, limited to `cassandra/ci-three-node` and the job's own files, ran it; it comes out before the pull request leaves draft.
    - **What the first runs measured** (`acruzdebian`, Cassandra 5.0.9, `write_request_timeout` 2000 ms):
        - **Run 44680155092** (one pass each, commit `cc7e97834`), 8 min 11 s from start to end:

          | Step | Time |
          | --- | --- |
          | Cold build | 132 s, peak `anon` 4,716 MiB under the 40 GiB cap, no OOM kill |
          | Three nodes to healthy | 200 s (node 1's CQL after 88 s) |
          | Keyspace and schema apply, with agreement | 12 s |
          | `cassandra_tests` | 7 s: 152 passed, 2 failed |
          | `--lib` `node_tests` | 3 s: 62 passed |
          | Pause scenarios | 77 s |
          | `down -v` | 28 s |

          - **The 2 failures:**
              - `node_runs_the_docker_cassandra_configuration`, the rack assertion at `cassandra_tests.rs:185`, as the README says;
              - `step3_suites::cassandra_concurrent_update_and_plain_delete_stay_consistent`, a #77 504 at `versioned_write_race_suite.rs:96`: "serial read: CAS operation timed out: received 0 of 2 required responses after 0 contention retries".

            So 1 #77 504 and no other failure.
          - **Memory:** the nodes peaked at 1,249, 1,162 and 1,187 MiB, under their 1,700 MiB limit, and each test container at 342 to 352 MiB. No OOM kill, and the host kept 57 GiB available.
          - **Pause scenarios:** all five matched the review: s1 success; s2 `WriteTimeout` `SIMPLE` 1 of 2, applied; s3 `Unavailable` 1 of 2 alive, not applied; s4a `WriteTimeout` `BATCH_LOG` 1 of 2 in 4.4 s, applied; s4b success. Node 1 marked a paused node `DN` after 16 s.
        - **Run 44681897890** (five passes each, commit `7ffdddfa4`): 7 min 23 s, on `acruzdebian`.
          - **Time:** warm build 38 s (peak `anon` 4,040 MiB, no OOM kill), nodes healthy 200 s, schema 11 s, five passes of `cassandra_tests` 29 s, five of `--lib` 15 s, pause scenarios 75 s, `down -v` 29 s.
          - **Routing:** the runner reached none of `10.59.142.11` to `.13:9042` (the probe step), which confirms the reason the tests run in a container.
          - **Issue #77:** a 504 in 4 of the 5 passes of `cassandra_tests`, 8 failures in all, across three race tests, all at `versioned_write_race_suite.rs:96` with "received 0 of 2 required responses": `cassandra_concurrent_update_and_plain_delete_stay_consistent` (3 of 5), `cassandra_concurrent_updates_from_the_same_version_admit_one` (3 of 5) and `cassandra_concurrent_update_and_versioned_delete_admit_one` (2 of 5). No other 504, no 503, no other failure. `--lib` `node_tests` passed 62 of 62 every time.
          - **Memory:** the nodes peaked at 1,275, 1,200 and 1,217 MiB, the test containers at 331 to 370 MiB, no OOM kill. The `docker stats` sampler took 51 samples: the job's containers together peaked at 3,808 MiB, and the host's `MemAvailable` never fell below 53,692 MiB.
          - **Pause scenarios:** all five matched the review again; node 1 marked node 2 `DN` after 13 s, and nodes 2 and 3 after 16 s.
          - **Intent rows:** five passes left 376 for `test-` tenants (Step 5's sweeper, #89).
    - **Expected failures.** `node_runs_the_docker_cassandra_configuration` always fails on three nodes. The report labels it as expected rather than leaving it out.
    - **Not in the job:** `backend_capability_contract`, the Milestone A tests through `hfs`, `restart_tests` and `tombstone_limit_tests`. Each could be added as another `run-tests.sh` call.
    - **Intent rows.** Run 1 left 79 intent rows for `test-` tenants. That is Step 5's area (#89), and the job reports it every night.
    - **`ci-report.py node-stats`** says "before the readiness test restarts the node" in every section. That is true of the required job, not of this one; the wording is in my file.
    - **To revisit:**
        - when #83 merges;
        - when Step 8 writes the three-node failure tests (row 8.3);
        - whether to add a `pull_request` trigger;
        - after PR #100, whether `medium` is enough: the job needs about 5 GiB for the build, with the cluster outside the runner's cap.
    - **`CLAUDE.md`** is left to my own session, after #100 merges, because PR #100 rewrites the same CI paragraph. It should say: one more report-only workflow, `cassandra-three-node.yml`, not required, nightly and by dispatch, on heavy; its Compose project `hfs-cassandra-3n`, ports 59642 to 59644 and subnet `10.59.142.0/24` are reserved; and never rename its job to a required check's name.
- **AI involvement:**
    - **Tool and model:** Claude Code (Opus 5.5), as the Rex worker `ci-three-node`.
    - **What the AI proposed:** everything above:
        - the workflow, the override, the three scripts and the report;
        - running the tests in a container;
        - the hour, the subnet and the ports;
        - the repeats and their default of 5;
        - the scenarios' raw-CQL form;
        - the temporary push trigger.
    - **What the human changed:** After the PR #90 review's hand runs on three nodes, I asked for a CI job that runs the cluster ("should we also do the 3 node one?"), with the scope my session briefed: report-only, nightly and by dispatch, no `pull_request` trigger in this first version, no test changes, and the pause scenarios only if they need no backend test code. I approved the draft's choices as written: the container route, 10:00 UTC, the reserved subnet and ports, and 5 repeats.
    - **Verified:**
        - **The two CI runs above:** their logs and their artifact `ci-report-three-node`.
        - **`report.py`:** run locally on synthetic libtest logs (a #77 504, an expected failure, another failure), on the review's own `cqlsh` outputs (`3n-cqlsh*-rep1.txt`), and on run 1's real logs.
        - **Lint:** `actionlint` 1.7.12 reports only `runner.workspace`, which it also reports three times in the required `fmt-clippy-cassandra.yml`. `shellcheck` is clean.
        - **The image:** `docker run` of the pinned image showed `libssl.so.3`, `libcrypto.so.3` and glibc 2.41. On Linux, `cargo tree -i openssl-sys` showed the binaries link `openssl` through `native-tls`.
        - **The ports:** `ss -ltn` on `acruzdebian` (read-only, in a visible pane).
        - **The schedule:** the Actions API's run start times.
        - **#77 and #83:** read in full; `tests/common/cassandra.rs` on `e5e8ea67f` has no RF or node-count check.
- **Links:**
    - PR #101 (this job); issue #77; PR #83; PR #90 and its review (`report-pr90-review.md`, "Three nodes"); PR #100.
    - Runs 44680155092 and 44681897890.
    - `development-roadmap.md`: section 1 "Machines", the CI paragraph (row 0.5's), row 8.3, section 6's first paragraph, and section 8's file lists.
    - `docker/cassandra/README.md`: "Run", the client paragraph, and "Recorded configuration".
    - ADR-aacruzgon-0002, 0018, 0019, 0021, 0022, 0023, 0070, 0072, 0080, 0081; ADR-sam-michaels-0002, 0004.
