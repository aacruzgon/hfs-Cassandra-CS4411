# ADR-aacruzgon-0045: CI reports measured figures on every run, starting with the boot time; they never fail a job

- **Date:** 2026-11-06
- **Status:** accepted (written after Alan approved the other Step 2 records; he approved it on 2026-11-06)
- **Context:**
    - **Alan asked for measured figures from CI,** for his supervisor: "I want to build as many reports as possible during CI in everything that we can so we can measure things properly." The first is the boot time of roadmap Step 2, item 2.6 (`development-roadmap.md:198`: "Confirm the conformance seeder runs at boot (about 1,400 LWT creates per tenant) and record boot time").
    - **The budget.** Roadmap section 7 (`development-roadmap.md:445`) moves `create_many` to a concurrent implementation when the seeder's boot time is above the startup budget. Alan adopted 10 s of seeding per fresh tenant on the single node on 2026-11-06, provisionally: he will discuss it with his supervisor (ADR-aacruzgon-0043). Nothing enforces it.
    - **Item 2.6 was measured by hand,** with a script outside the repository, on a node of its own, in two rounds: 1.3 to 4.5 s of seed per fresh tenant (ADR-aacruzgon-0043). Nothing re-measures it as the code changes.
    - **Other figures CI already had but only printed:**
        - the Cassandra job's build peaks (`memory.current`, `anon`, OOM kills), which ADR-aacruzgon-0023 asked for to size the light runners (ADR-aacruzgon-0030);
        - the readiness test's down and up times (the Step 2 exit criterion, ADR-aacruzgon-0042).

      The clippy jobs, whose peaks put them on the heavy runner (ADR-sam-michaels-0003), print none.
    - **A skip passes.** libtest counts a test that returns early as passed, so a live Cassandra test that skipped once looked like a pass in Step 2.
    - **Tombstones.** The design watches them: "tombstones scanned per read above 1,000" is the trigger for time-bucketing a partition (`cassandra-discussion.md:211`), "We … record tombstones scanned per read" (`:343`), and `pending_index_writes` is keyed to avoid scanning them (`:187`).
    - **The constraints:**
        - `Cassandra schema (single node)` is a required check (ADR-aacruzgon-0021), on the light label (ADR-aacruzgon-0023);
        - no `paths:` filter, no rename (`CLAUDE.md`, "Commits and CI");
        - every action pinned by full SHA (ADR-aacruzgon-0022);
        - the readiness test restarts the job's node, so it runs last (ADR-aacruzgon-0042).
- **Decision:**
    - **Report only.** A figure never fails a job.
        - A figure past a threshold, or a skipped live test, is a `::warning::` annotation on the run.
        - A report that cannot be read is a warning too: its step has `continue-on-error: true`, and the helper exits 0.
        - Only the boot-time step can fail the job: when `hfs` does not boot, or a log line the measurement needs is missing. That is a broken server or a broken measurement, not a slow one. (ADR-aacruzgon-0046 adds a second such step, latency, under the same rule.)
    - **One mechanism, `.github/scripts/ci-report.py`** (Python 3, standard library only; `python3` is on both runners, since their `cqlsh` needs it). Each report is one section:
        - `## <title>` and its Markdown are appended to `$GITHUB_STEP_SUMMARY`;
        - `<id>.md` and `<id>.json` are written to the job's report directory, `$RUNNER_TEMP/ci-report`. The JSON is `{"id", "title", "context", "data"}`;
        - each string in the data's `"warnings"` list becomes an annotation.

      The job's last step uploads the directory as the artifact `ci-report-<job>`, with `if: always()`, through `actions/upload-artifact@cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7.0.2`. That is the latest release (2026-10-07); its notes list only a retry fix since v7.0.1. `overwrite: true` lets a re-run attempt replace the first attempt's artifact rather than fail.

      **The context** is the same in every file of a job:
        - the commit, ref, run and job;
        - `RUNNER_NAME`;
        - the build profile (`CI_REPORT_PROFILE`);
        - the CPUs, and the cgroup's `cpu.max` and `memory.max`;
        - `rustc -V` and `cargo -V`;
        - the node's Cassandra version (`nodetool version`), once the node is up.

      A later pull request, or the Step 8 benchmarks, adds a section with `ci-report.py add <id> <title> --markdown … --json …`, or with a new subcommand.
    - **The reports, all inside the jobs that produce their data:**
        - **Boot time and conformance seed (item 2.6).** A test, `boot_time_report` in `crates/hfs/tests/cassandra_milestone_a.rs`, runs only when `HFS_TEST_BOOT_REPORT` names a directory, and skips otherwise, so `cargo test` is unchanged.
            - It boots `hfs` with the seed three times, each on a fresh `test-` tenant, then once warm, on the third tenant, against the job's node.
            - Each span comes from `hfs`'s own log lines, matched in one place (`mod startup_line`):
                - launch to the first `/metadata` 200, polled every 50 ms;
                - launch to "Initializing Cassandra backend";
                - connect plus the base registry: to "Connected to Cassandra", which `CassandraBackend::connect` logs after the session, the local-DC check and the spec file's SearchParameter parse;
                - `initialize` plus seed: from "Connected to Cassandra" to the start of the stored-SearchParameter load, which is the "Stored SearchParameters loaded" line's time minus its `elapsed_ms`. It includes the ten-table check, which logs no line of its own;
                - the load's `elapsed_ms`, `tenants` and `stored` (ADR-aacruzgon-0041);
                - the resources created, from the seeder's `created=` fields;
                - launch to "Server listening".
            - The span of the earlier hand measurement, "Connected" to the last "Seeded …" line, is kept beside it for comparison.
            - The worst fresh-tenant seed is compared with 10 s.
            - The step runs after the round trip and before the readiness test, and uses the binaries already built. It has a 10-minute timeout. The readiness test and the node snapshot run whenever the round trip passed, so a failed boot report does not hide the exit criterion; it still fails the job. It writes `boot-time.json` and `boot-time.md`, plus each boot's whole `hfs` log as the figures' source.
        - **Build: wall time and peak memory,** for the Cassandra job's two builds and both clippy jobs. `ci-report.py build -- <command>` replaces the step's shell sampler (ADR-aacruzgon-0030) and keeps its method: the runner cgroup is sampled every second for its `memory.current` and `anon` peaks, and its OOM kills are counted. It now also records the wall time and the margin to `memory.max`, and exits with the command's status.
        - **Tests:** every test step tees its output; the persistence-tests and round-trip steps run with `--show-output`, so each passing test's captured output is printed under its name, and the boot-time and readiness steps with `--nocapture`. The parser counts a skip in either mode. The report gives, per suite:
            - passed, failed, ignored and filtered out;
            - libtest's own `finished in`;
            - every test that printed a "Skipping …" line, by name, with a warning. The Cassandra job has a node, so it should never skip.
        - **Readiness recovery:** the readiness test prints `[stop_ms=… stopped_to_503_ms=…]` and `[start_ms=… started_to_200_ms=…]`. A step after it, with `if: always()`, reports them.
        - **Node statistics,** read before the readiness test, because its restart resets them:
            - `nodetool tablestats helios`: partitions, SSTables, memtable cells, droppable tombstone ratio;
            - `system_views.local_read_latency`, `local_write_latency`, `tombstones_per_read`, `rows_per_read` and `max_partition_size`;
            - `nodetool tpstats`: pools and dropped messages;
            - the lightweight-transaction figures: `nodetool proxyhistograms` (CAS latency) and the `ClientRequest` CASWrite/CASRead counts through `nodetool sjk mx` (`Latency` count, `ConditionNotMet`, `ContentionHistogram`, `UnfinishedCommit`, `UnknownResult`, `Timeouts`).

          The raw outputs are kept in the artifact.
- **Alternatives considered:**
    - **Fail the job when the seed passes the budget:** lost. The budget is provisional and may change after Alan's supervisor meeting. The figure comes from a debug build on a runner that is also a developer's desktop (`sam-7900x`) or another person's machine, so a slow run would block a merge for noise. Section 7 makes the budget a trigger for work, not a merge rule.
    - **A separate, non-required job for the reports:** lost. It would build the binaries a second time and start a second node. It could not report the required job's own build peaks, test counts or skips, since those exist only in that job.
    - **A measurement script outside cargo** (the earlier hand measurement's approach): lost. The Milestone A test file already starts the real binary with a clean environment and a fresh tenant (`Hfs::start`, `unique_tenant`), and a second harness would duplicate them.
    - **Reading the seed's size from CQL counts** instead of the seeder's `created=` fields: not chosen. The log line carries the same numbers for the same run without a CQL client in `helios-hfs`'s tests. The earlier hand measurement checked once that the counts agree (1,372 + 5).
    - **A shell and `jq` helper:** lost. `jq` is not known to be on both runners, while `python3` is, since their `cqlsh` needs it. The JSON and the parsing are easier to keep correct in Python.
    - **`cargo-nextest`, for per-test times and JUnit output:** not now. It is a new tool to install and pin on two runners. It runs each test in its own process, which changes the per-process fallback of the test-node harness (ADR-aacruzgon-0020 names nextest as a revisit point). Per-test time is a candidate for a later report.
    - **libtest's `--report-time` and `--format json`:** lost. Both are unstable (`-Z unstable-options`) and need a nightly or `RUSTC_BOOTSTRAP`.
    - **Node statistics after the readiness test:** lost, because the restart resets every metric. Before it, they cover the schema checks, the tests and the boot report's three seeds.
- **Consequences:**
    - **Every run's summary page** carries the boot-time table and the budget verdict, the build peaks of every compiling job, the test counts and skips, the readiness times and the node's statistics. Each job's artifact, `ci-report-cassandra`, `ci-report-clippy-fhir` and `ci-report-clippy-workspace`, holds the same as JSON.
    - **Time added to the required job: about 13 s** (run 42255106011 on `acruzdebian`): context 1 s, boot report 8 s, node statistics 3 s, the tests and readiness reports under 1 s, upload 1 s. That is well under a minute, so nothing needs cutting. The build wrapper adds no measurable time. Each clippy job's upload takes 2 s.
    - **The boot figures are a debug build on a shared runner.** They are comparable run to run on one runner, not across runners. The context names the runner, and a release-build measurement stays with the Step 8 benchmarks.
    - **The stored-SearchParameter load reads every tenant on the node that has a SearchParameter partition** (`load_all_stored_search_parameters`, ADR-aacruzgon-0041, after a scan of every `resources_by_type` partition key). On a fresh CI node it covers a few tenants. On the shared development node, with 67 to 86 tenants, it took 0.8 to 2.1 s per boot, more than the seed itself (local runs below); in CI, with 6 to 8 tenants, 0.2 to 0.6 s. It grows with the number of tenants that ever wrote a SearchParameter, and is a candidate for its own budget (ADR-aacruzgon-0048 gives it a warning line).
    - **A reworded log line breaks the boot step** with a message naming the line, rather than producing a wrong figure. `mod startup_line` is the one place to update. The rework of the stored-SearchParameter load in PR #43 (ADR-aacruzgon-0041) is below PR #46 and kept the line it reads ("Stored SearchParameters loaded", with `tenants`, `stored` and `elapsed_ms`).
    - **The test steps now print each passing test's captured output.** The logs are longer, and so is the artifact.
    - **Later pull requests add sections without redesign:** `ci-report.py add` for anything that writes Markdown and JSON, or a new subcommand for a new parser (ADR-aacruzgon-0046, 0047, 0048).
    - **The roadmap** records the mechanism in its section 8 CI line, in this entry's commit.
- **AI involvement:**
    - **Tool and model:** Claude Code (Opus 5.5), as the Step 2 worker `s2-testnode` (PR #46).
    - **What the AI proposed:** the helper and its section format, the spans and where each starts and ends, the skip detection through `--show-output`, the node-statistics sources, and the step order.
    - **What Alan changed:** Alan asked for the boot-time reports to be built in CI on every run, then widened the request to as many measured reports as CI can give. He set the provisional 10 s budget and the rule that a report never fails a job for a figure. This entry was written after his approval of the other Step 2 records; he approved it later on 2026-11-06.
    - **Verified:**
        - Locally, against the shared node `127.0.0.1:59042` (Apple M3 Pro, 11 cores, debug build, Cassandra 5.0.9), two runs of the boot report. The worst fresh seed was 1.34 s and 1.36 s, the warm seed 0.16 to 0.17 s, and launch to `/metadata` 2.7 to 5.4 s. 1,372 SearchParameter and 5 CompartmentDefinition were created per fresh tenant.
        - Each span of `fresh 2` was recomputed by hand from its raw log: connect 166.5 ms, seed 1,190.0 ms, cross-check 1,189.2 ms.
        - The round trip passed against the shared node. The readiness test passed on a container of its own, which was removed afterwards: docker stop 4.78 s, 503 within 1 ms of the stop returning, 200 10.20 s after the start.
        - With no variables set, the three tests passed, and the test report named `boot_time_report` as skipped.
        - `ci-report.py node-stats` ran read-only against the shared node in 5 s; `build` with a failing command passed its exit status through.
        - `actionlint`: only the `runner.workspace` false positive of the existing jobs. Clippy on `cassandra_milestone_a.rs` with CI's flags: no hits.
        - CI on `7dde4e50e` (before the restack; now `76f8ec736`, the same patch): R4B compile check run 42255101638 and fmt/clippy/Cassandra run 42255106011 (https://github.com/aacruzgon/hfs-Cassandra-CS4411/actions/runs/42255106011); all five jobs succeeded.
            - Artifacts: `ci-report-cassandra` (29 KB), `ci-report-clippy-fhir` and `ci-report-clippy-workspace`.
            - The boot report on `acruzdebian`: worst fresh seed 1.02 s, warm 0.21 s, launch to `/metadata` 1.28 to 2.01 s.
            - The build peaks (`anon`): 4,592 and 4,895 MiB of the 9,216 MiB cap on `acruzdebian`; clippy 13,477 and 13,853 MiB of 20,480 MiB on `sam-7900x`.
            - Readiness: 503 within 4.86 s of the stop being issued, 200 28.43 s after the start.
            - No skips, no warnings.
        - An audit subagent reviewed the diff. It found nothing blocking. All ten of its findings are fixed in `7dde4e50e` (now `76f8ec736`). Among them: the connect span was relabelled "Connect + base registry", because `CassandraBackend::connect` also parses the spec file; a failed boot report no longer skips the readiness test; the boot step and the node snapshot have timeouts; pull-request reports name the head commit; and the JSON is strict, with no NaN.
- **Links:**
    - PR #46 (branch `cassandra/step2-boot-report`); `.github/scripts/ci-report.py`; `.github/workflows/fmt-clippy-cassandra.yml` (jobs `cassandra`, `clippy-fhir`, `clippy-workspace`); `crates/hfs/tests/cassandra_milestone_a.rs` (`boot_time_report`).
    - `development-roadmap.md:198` (Step 2, seeder bullet), `:445` (section 7 trigger), section 8 (CI); `cassandra-discussion.md:187`, `:211`, `:343` (tombstones).
    - ADR-aacruzgon-0018 to 0023, 0030, 0041, 0042, 0043; ADR-sam-michaels-0002, 0003. Extended by ADR-aacruzgon-0046, 0047 and 0048.
