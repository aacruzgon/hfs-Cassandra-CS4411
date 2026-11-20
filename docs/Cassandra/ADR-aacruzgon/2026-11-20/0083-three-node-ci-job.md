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
    - **The heavy label is changing.** PR #100 (`ci-mutation-flake`) splits the runners into light, medium and heavy, with heavy on `acruzdebian` (40G) only. Until it merges, heavy also matches `sam-7900x`, so a heavy job's build must still fit its 20G (ADR-aacruzgon-0080). The cap covers the build only: this job's containers run in `gh-runner`'s rootless Docker, outside the runner service's cgroup. So #101 merges after #100 and after `hfs-cassandra-heavy` is removed from `sam-7900x`, and the job runs on `acruzdebian` only.
- **Decision:**
    - **A new workflow, `.github/workflows/cassandra-three-node.yml`, report-only.**
        - **Triggers:** `schedule` daily at 10:37 UTC, and `workflow_dispatch` with inputs `ref`, `test_filter` (for `cassandra_tests`), `lib_filter` (default `node_tests`) and `repeats` (default 5, 1 to 20). No `pull_request` trigger.
        - **Why 10:37 UTC:** picked from when runs really start (`run_started_at`, with the scheduled runs counted where they started, not at their cron).
            - **The measurement:** the fork's 1,000 most recent runs, 2026-11-08T01:09 to 2026-11-20T14:24 UTC (Actions API, its cap for a filtered list).
                - None started between 10:00 and 12:59 UTC.
                - 14 started at 08:00, 19 at 09:00 and 39 at 13:00.
                - From 16:00 to 22:00, 32 to 107 an hour.
            - **GitHub starts this fork's scheduled runs hours late:**
                - the canary's `0 2 * * *` was created at 08:16 to 09:52 every day, and on 2026-11-20 its job started at 09:59 on `acruzdebian`;
                - the Saturday prune's `0 20 * * 6` was created at 23:28 to 23:42.
            - **Off the hour (`:37`):** GitHub's documentation names the start of every hour as its peak load for schedules.
            - **If it runs late:** a late run only queues (`concurrency`).
            - **Correction of the first version:** it said 10:00 UTC, "clear of the canary (02:00)". It gave its window as 2026-11-04 to 2026-11-20 (it was 2026-11-07 to 11-20) and its evening range as 45 to 108 (it was 32 to 108). The independent audit of #101 found all three.
        - **The job:** `Cassandra three-node tests (report only)`, in no ruleset, on `[self-hosted, hfs-cassandra-heavy]`, `timeout-minutes: 120`, with the fork guard. `concurrency` is one group for every ref, `cassandra-three-node`, with `cancel-in-progress: false`, so two runs never overlap. GitHub keeps at most one pending run per group, so a third run (a dispatch while the nightly is queued) cancels the pending one.
        - **Two checkouts.** The code under test (`ref`, or the run's own commit) goes at the workspace root, and the schema `docs/Cassandra/schema/schema.cql` comes with it. This workflow's own commit's `.github/` and `docker/` go in `.three-node-job/`: the job's scripts, the Compose files and `ci-report.py`. So `ref` can name a branch without this job's files, such as #83's. `ref` refuses `refs/pull/`, and, before any checkout, a commit SHA that no branch of this repository contains (the compare API, each branch head against the SHA): a fork pull request's code, by ref or by commit, must not run on a self-hosted runner.
        - **`repeats`:** a whole number from 1 to 20, with no leading zero, checked before the checkouts.
    - **The cluster.**
        - `docker-compose.cluster.yml` with a new override, `docker-compose.cluster.heap512m.yml`: 512M heap, 256M direct memory, `mem_limit` and `memswap_limit` 1700m, as the review used. 5,100 MiB (4.98 GiB) of limits in all.
        - Its own Compose project `hfs-cassandra-3n`, subnet `10.59.142.0/24` and host ports 59642 to 59644.
        - `keyspace-three-node.cql` (RF 3), then `schema.cql`; the job waits until node 1 sees three nodes `UN` and every node reports one schema version.
        - `down -v` on that project at the end, under `always()`, so also after a failure, a timeout or a cancel, and once more at the start for a run that was killed.
    - **The tests run in a container on the cluster's network.** `run-tests.sh` starts the pinned Cassandra image (Debian 13, glibc 2.41, `libssl3`, already on the runner), with the checkout mounted read-only and the target directory read-write at their own paths, and runs the binary that the build step made. `HFS_TEST_CASSANDRA_URL` is node 1's cluster address, `10.59.142.11:9042`.
        - **Step 1:** `tests/cassandra_tests.rs`, every test.
        - **Step 2:** the `--lib` tests filtered to `node_tests` (write.rs, backend.rs, delete.rs, update.rs, upsert.rs, versions.rs). `restart_tests` and `tombstone_limit_tests` skip, as they need variables of their own.
        - **Each binary runs `repeats` times.** A pass of both takes about 10 s, and #77's 504 is a rate.
        - **`--nocapture`:** libtest prints each panic as it happens, under the test's thread name. So a run the timeout stops keeps the messages of the tests that failed before it, and `ci-report.py` still finds a printed skip.
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
            - every failing test with the first line of its failure, how many runs it failed in, and its cause. The cause is read from the panic's own first line, and only when that line's subject is the backend error (`got Failed(Backend(Timeout { … }))`, an unwrap's `` `Err` value: ``, or a printed `Err(Backend(…))`), and the line ends with it. The causes are:
                - **a #77 504:** a `Timeout` whose message starts `serial read: CAS operation timed out`;
                - **another 504:** any other timeout, including a CAS write's (`create: CAS operation timed out`);
                - **a 503:** `Unavailable`;
                - **expected on three nodes:** `node_runs_the_docker_cassandra_configuration` at its rack assertion or at `one node, no peers`, and nowhere else (a 503 or 504 in it keeps its own class);
                - **timed out:** a test still running when the timeout stopped the run;
                - **killed:** a test still running when the binary was killed (exit 137, an OOM kill or the timeout's `SIGKILL`);
                - **cause unknown:** a failure whose output a stopped run never printed;
                - **other:** everything else. An invariant panic that lists the writers' outcomes, a 504 among them (PR #83's), is "other", with a note.
            - the pause scenarios;
            - each container's `memory.peak` and OOM kills, and a `docker stats` sample every 5 s;
        - in the artifact: the full test logs, `nodetool status` and `describecluster` from every node, each node's `system.log` tail and container log, and the scenarios' raw `cqlsh` output.
    - **Limits.** Its own target directory, `target/cassandra-three-node`, with the required Cassandra job's 15 GB cap and reset rule (ADR-sam-michaels-0002), and the cargo `CACHEDIR.TAG` (ADR-aacruzgon-0070). The test container is capped at 6 GiB.
        - **Each run of a binary** is stopped after 8 minutes (6 for `--lib`), cut short so it ends about two minutes before its step's own timeout (a deadline a minute early, less another minute in `run-tests.sh`). A run with under two minutes left is not started. So the step's timeout never has to kill a `docker run` client and leave its container running.
        - **A cancel** is cleaned up by the `always()` step "Stop the cluster", which removes every `hfs-cassandra-3n-tests-*` container. The runner signals only the step's shell, not `run-tests.sh`. `run-tests.sh` also removes its container on a `SIGINT`, `SIGTERM` or `SIGHUP` sent to the script itself.
        - **How a run ended:** coreutils `timeout`'s 124 is reported as a timeout. 137 is reported as "killed", with the container's OOM kills saying whether the kernel did it.
        - **Leftovers of a killed run** are removed before the build.
    - **Row 8.3 is unchanged.** This job runs the existing tests and measures; it does not own or replace Step 8's failure tests. They can run here once they exist.
- **Alternatives considered:**
    - **Run the tests on the runner, through the published ports:** lost. The runner does not reach the nodes' cluster addresses (measured in run 44681897890: the probe step's TCP connect from the runner to `10.59.142.11`, `.12` and `.13` on port 9042 did not complete within 3 s, for all three), and the driver routes to all three, so a request routed to node 2 or 3 would time out. Restricting the driver to its contact point would be a change to the backend.
    - **Enter rootlesskit's network namespace with `nsenter`:** lost. It depends on rootless Docker's internals (the daemon's child PID and its user namespace). The container is the route `docker/cassandra/README.md` already documents.
    - **A Rust image, or `cargo test` inside the container:** lost. A Rust image is another pull, and its glibc can differ from the build host's. The Cassandra image is already on the runner and matches Debian 13's glibc; running the built binary directly needs no toolchain in the container.
    - **The base file's limits (768M heap, 2.5 GiB per node, 7.5 GiB):** lost. They fit `acruzdebian`, but 1700 MiB is what the review and #77's twelve runs used, and the nodes peaked at 1,162 to 1,249 MiB (run 44680155092).
    - **A `pull_request` trigger:** lost for this first version. The job holds the heavy runner for about 8 minutes, and it measures rather than gates.
    - **A job in `fmt-clippy-cassandra.yml`:** lost. That workflow carries four of the five required checks, and PR #100 is changing it.
    - **The pause scenarios as a backend test (the review's `audit_3n`):** lost. It needed a scratch test in `write.rs`'s `node_tests` and a change to `LocalDcOnly` so every request goes to node 1 (`3n-scratch.diff`). Raw CQL from node 1 gets the same coordinator answers without backend code, and matched the review in every scenario.
    - **Subnet `172.29.x` or port 59542:** lost. `172.29.0.0/16` is inside Docker's default address pools (ADR-aacruzgon-0072), and 59542 is the benchmark harness's default (PR #76). `10.59.142.0/24` is outside the default pools and clear of `slirp4netns`'s own `10.0.2.0/24`, and ports 59642 to 59644 were free on `acruzdebian` (`ss -ltn`, 2026-11-20).
    - **Sharing `target/cassandra-tests`:** lost. It builds the same two binaries, but a nightly run would change what the required job finds warm.
    - **One checkout of `ref`, as in the first version:** lost. Every file the steps use came from `ref`, so a branch without this job's files failed at its first `docker compose` (the audit reproduced it on #83, #90 and #93).
    - **`--show-output` (capture mode), as the required job runs:** lost. libtest prints a failure's output only in its closing `failures:` section, which a stopped run never reaches.
    - **One pass of each binary:** lost. A pass takes seconds, and a single pass gives #77 as yes or no.
    - **An `HFS_TEST_CASSANDRA_CLUSTER` variable for the suites:** not added. The job changes no test, and #83 reads the RF instead.
- **Consequences:**
    - **Easier.**
        - Every night, the #77 rate on `main` and every other failure on three nodes, with its cause. When #83 merges, those 504s should become passes, and the job will show whether they do.
        - The three coordinator answers the review found by hand are checked every night.
        - Steps 4, 5 and 8 have a running cluster job to add their own three-node tests to.
    - **Harder.**
        - The heavy runner is busy for about 8 to 10 minutes a night, and disk use on each runner grows by up to 15 GB.
        - `workflow_dispatch` only works once the file is on `main` (GitHub answers HTTP 404 otherwise). While it was not, a temporary `push` trigger, limited to `cassandra/ci-three-node` and the job's own files, ran it. It was removed in `09cd1988b`, added back once to run the job with the audit's fixes, and removed again; #101 merges with no `push` trigger.
    - **What the first runs measured** (`acruzdebian`, Cassandra 5.0.9, `write_request_timeout` 2000 ms):
        - **Run 44680155092** (one pass each, commit `cc7e97834`), 8 min 13 s from the job's start to its end (Actions API):

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
              - `node_runs_the_docker_cassandra_configuration`, the rack assertion at `cassandra_tests.rs:185`, as the README says. Run 44681897890 failed at `one node, no peers` (`:214`) in all five passes. Which one fails depends on the node that answers `system.local`, and either is the expected one;
              - `step3_suites::cassandra_concurrent_update_and_plain_delete_stay_consistent`, a #77 504 at `versioned_write_race_suite.rs:96`: "serial read: CAS operation timed out: received 0 of 2 required responses after 0 contention retries".

            So 1 #77 504 and no other failure.
          - **Memory:** the nodes peaked at 1,249, 1,162 and 1,187 MiB, under their 1,700 MiB limit, and each test container at 342 to 352 MiB. No OOM kill. The sampler took no sample in this run, but `free -m` at the end showed 57,610 MiB (56.3 GiB) available.
          - **Pause scenarios:** all five matched the review: s1 success; s2 `WriteTimeout` `SIMPLE` 1 of 2, applied; s3 `Unavailable` 1 of 2 alive, not applied; s4a `WriteTimeout` `BATCH_LOG` 1 of 2 in 4.4 s, applied; s4b success. Node 1 marked a paused node `DN` after 16 s.
        - **Run 44681897890** (five passes each, commit `7ffdddfa4`): 7 min 23 s, on `acruzdebian`.
          - **Time:** warm build 38 s (peak `anon` 4,040 MiB, no OOM kill), nodes healthy 200 s, schema 11 s, five passes of `cassandra_tests` 29 s, five of `--lib` 15 s, pause scenarios 75 s, `down -v` 29 s.
          - **Routing:** the runner reached none of `10.59.142.11` to `.13:9042` (the probe step), which confirms the reason the tests run in a container.
          - **Issue #77:** a 504 in 4 of the 5 passes of `cassandra_tests`, 8 failures in all, across three race tests, all at `versioned_write_race_suite.rs:96` with "received 0 of 2 required responses": `cassandra_concurrent_update_and_plain_delete_stay_consistent` (3 of 5), `cassandra_concurrent_updates_from_the_same_version_admit_one` (3 of 5) and `cassandra_concurrent_update_and_versioned_delete_admit_one` (2 of 5). No other 504, no 503, no other failure. `--lib` `node_tests` passed 62 of 62 every time.
          - **Memory:** the nodes peaked at 1,275, 1,200 and 1,217 MiB, the test containers at 331 to 370 MiB, no OOM kill. The `docker stats` sampler took 51 samples: the job's containers together peaked at 3,808 MiB, and the host's `MemAvailable` never fell below 53,692 MiB.
          - **Pause scenarios:** all five matched the review again; node 1 marked node 2 `DN` after 13 s, and nodes 2 and 3 after 16 s.
          - **Intent rows:** five passes left 376 for `test-` tenants (Step 5's sweeper, #89).
        - **Run 44758590016,** after the independent audit's fixes: commit `42e590831`, with a temporary checkout override that tested PR #83's branch at `ebc3e520d`. It took 7 min 30 s on `acruzdebian`.
          - **Checkouts:** the code under test was #83's merge commit, which has none of this job's files, and the scripts and Compose files came from `42e590831`.
          - **Time:** build 41 s (peak `anon` 3,981 MiB, no OOM kill), nodes healthy 200 s, five passes of `cassandra_tests` 31 s, five of `--lib` 14 s, pause scenarios 74 s, `down -v` 29 s.
          - **`cassandra_tests`:** 155 passed and 1 failed in each of the five passes. The failure was the expected rack assertion (`cassandra_tests.rs:186` on #83). With #83's race check there was no #77 504 and no other failure. Every run ended with an exit, not a timeout or a kill.
          - **`--lib` `node_tests`:** passed 62 of 62 every time.
          - **Memory:** the containers together peaked at 3,780 MiB, and the host's `MemAvailable` never fell below 57,146 MiB.
          - **Pause scenarios:** all five matched the review for the third time.
          - **Intent rows:** 416 were left.
    - **Expected failures.** `node_runs_the_docker_cassandra_configuration` always fails on three nodes. The report labels it as expected rather than leaving it out, but only at its rack assertion or `one node, no peers`. A release, data-centre or `paxos_variant` mismatch is "other".
    - **Not in the job:** `backend_capability_contract`, the Milestone A tests through `hfs`, `restart_tests` and `tombstone_limit_tests`. Each could be added as another `run-tests.sh` call.
    - **Intent rows.** Run 1 left 79 intent rows for `test-` tenants. That is Step 5's area (#89), and the job reports it every night.
    - **`ci-report.py node-stats`** says "before the readiness test restarts the node" in every section. That is true of the required job, not of this one; the wording is in my file.
    - **To revisit:**
        - when #83 merges;
        - when Step 8 writes the three-node failure tests (row 8.3);
        - whether to add a `pull_request` trigger;
        - after PR #100, whether `medium` is enough: the job needs about 5 GiB for the build, with the cluster outside the runner's cap;
        - when the first scheduled runs really start (`run_started_at` and the job's start), and whether 10:37 UTC still lands in the quiet hours;
        - after #100 merges, adding `test_cassandra_three_node_report` to the fmt job's unittest step in `fmt-clippy-cassandra.yml`. #100 edits that same line, so #101 runs the tests locally only;
        - linting the new workflow and scripts (`actionlint`, `shellcheck`) in CI, as `fhir-bench-tests.yml` does for the benchmark workflows.
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
    - **What the human changed:** After the PR #90 review's hand runs on three nodes, I asked for a CI job that runs the cluster ("should we also do the 3 node one?"), with the scope my session briefed: report-only, nightly and by dispatch, no `pull_request` trigger in this first version, no test changes, and the pause scenarios only if they need no backend test code. I approved the draft's choices as written: the container route, 10:00 UTC, the reserved subnet and ports, and 5 repeats. After the independent audit of #101 (2026-11-20), I had its findings fixed before undraft: the failure classes read from the panic's own first line, a stopped run's hung test and earlier messages, `ref` with two checkouts, the hour picked again from the runs' real start times (10:37 UTC), and its minor findings except M9 (linting in CI), which is under "To revisit". After the delta audit, I had its minors fixed too: a 504 inside `Err(…)`, the configuration test's 503 and 504, an OOM-killed run's tests, a commit SHA on no branch, and the trap's wording.
    - **Verified:**
        - **The CI runs above:** their logs and their artifact `ci-report-three-node`.
        - **`report.py`:** `.github/scripts/test_cassandra_three_node_report.py`, 23 tests, run locally. The classes it covers:
            - a #77 504, through the race suites and through a `--lib` unwrap;
            - a plain write's 504, a CAS write's 504 and an unknown-outcome 504, each "another 504";
            - a 503;
            - the rack and `one node, no peers` failures, "expected";
            - a release mismatch and a data-centre mismatch, "other";
            - a plain assertion;
            - a #77 504 and a 503 printed inside `Err(…)`, and a 504 followed by more text ("other");
            - a 504 and a 503 in the configuration test, which keep their class;
            - a test still running at an OOM kill, "killed";
            - two invariant panics listing a #77 504, "other" with a note;
            - a run stopped by the timeout, in capture mode and with `--nocapture`;
            - a finished `--nocapture` run;
            - the section's "Ended" column for a timeout and an OOM kill.

          It also matches the classes of run 1's and run 2's real logs, and was run on the review's own `cqlsh` outputs (`3n-cqlsh*-rep1.txt`).
        - **Lint:** `actionlint` 1.7.12 reports only `runner.workspace`, which it also reports three times in the required `fmt-clippy-cassandra.yml`. `shellcheck` is clean.
        - **The image:** `docker run` of the pinned image showed `libssl.so.3`, `libcrypto.so.3` and glibc 2.41. On Linux, `cargo tree -i openssl-sys` showed the binaries link `openssl` through `native-tls`.
        - **The ports:** `ss -ltn` on `acruzdebian` (read-only, in a visible pane).
        - **The schedule:** the Actions API's `run_started_at` of the 1,000 most recent runs, and the canary's and prune's creation times.
        - **`ref` without this job's files:**
            - in CI, run 44758590016 tested #83's branch through a temporary checkout override;
            - locally, `docker compose config` on #83's tree failed with the old single-checkout layout (`stat …/docker-compose.cluster.heap512m.yml: no such file or directory`), and with the new layout it resolved the image, the 512M heap and the subnet.
        - **The commit-SHA guard:** the step's script run locally with `bash -eo pipefail`, as the runner runs it, against this repository's API.
            - It accepted `main`'s head, a commit 40 back on `main`, and #83's head (on its branch, after 46 s).
            - It refused upstream `HeliosSoftware/hfs`'s `main` head (in the fork's network, on no branch of the fork, after 81 s) and a random SHA.
            - It passed a branch name straight through.
        - **`run-tests.sh`:** run locally against a fake `docker`.
            - Exits 101, 124 and 137 were recorded as an exit, a timeout and a kill.
            - A deadline 100 s away skipped the run with exit 125, and one 300 s away cut the timeout to 240 s.
            - A `SIGTERM` removed the container and exited 143 within about 2 s.
        - **#77 and #83:** read in full; `tests/common/cassandra.rs` on `e5e8ea67f` has no RF or node-count check.
- **Links:**
    - PR #101 (this job); issue #77; PR #83; PR #90 and its review (`report-pr90-review.md`, "Three nodes"); PR #100.
    - Runs 44680155092, 44681897890 and 44758590016.
    - `development-roadmap.md`: section 1 "Machines", the CI paragraph (row 0.5's), row 8.3, section 6's first paragraph, and section 8's file lists.
    - `docker/cassandra/README.md`: "Run", the client paragraph, and "Recorded configuration".
    - ADR-aacruzgon-0002, 0018, 0019, 0021, 0022, 0023, 0070, 0072, 0080, 0081; ADR-sam-michaels-0002, 0004.
