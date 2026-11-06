# ADR-aacruzgon-0048: More CI reports: the R4B build, cargo timings, the node's start, its JVM and container, hfs's memory, and the stored-SearchParameter load

- **Date:** 2026-11-06
- **Status:** accepted (written after Alan approved the other Step 2 records; he approved it, with the 5 s load threshold, on 2026-11-06)
- **Context:**
    - **The mechanism is in place.** ADR-aacruzgon-0045 (PR #46) made `.github/scripts/ci-report.py` the one place reports go: a summary section and JSON in a per-job artifact, and reports never fail a job. Its reports are boot time, build peaks, tests, readiness and node statistics. ADR-aacruzgon-0046 (PR #47, `cassandra/step2-ci-reports`) added latency, dependency freshness, advisories, sizes and step times, and records whether a target dir was reset, that is, a cold or warm build.
    - **Alan asked for every report that can be built so far:** "if there are any other CI reports that can be built so far, let's send them to be built and add them to ADR". These are the further reports CI could build at this point that PR #47 does not cover.
    - **What was only printed or not measured:**
        - the R4B check's build cost;
        - where a cold build's time goes;
        - Cassandra's start and the schema apply;
        - the node's heap, GC and compactions, and the container's memory against its 3 GiB limit (ADR-aacruzgon-0002);
        - `hfs`'s own memory at boot;
        - the stored-SearchParameter load, which ADR-aacruzgon-0045 found grows with the node's tenants.
    - **What that load reads** (ADR-aacruzgon-0041). `load_all_stored_search_parameters` (`crates/persistence/src/backends/cassandra/backend.rs:313` at PR #49's head) runs before the server serves, and the same reload runs on every TTL refresh. It first finds the tenants with a SearchParameter partition (`stored_search_parameter_tenants`, `:346`, a `SELECT DISTINCT` over the whole of `resources_by_type`'s partition keys, `:42`), plus the tenants already in the overlay. For each of them, `load_stored_search_parameters` (`:404`) does two things:
        - it scans the partition's ids;
        - it point-reads each one, 16 at a time (`buffer_unordered(STORED_SEARCH_PARAMETER_READS)`, `:439`) and drops those whose `url` the base already holds.

      A seeded tenant has 1,372 spec SearchParameters, so it costs 1,372 reads on every boot for definitions that never enter the overlay. Tenants that were never seeded hold a few rows and cost little. On the shared node, 14 of 95 tenants were seeded; the others held 1 to 3 SearchParameters (a read-only count by the audit subagent).
- **Decision:**
    - **The R4B check's `cargo check` runs under `ci-report.py build`,** like the other compiling commands. Its wall time, `anon` and `memory.current` peaks and OOM kills join PR #47's `ci-report-r4b-check` artifact. That keeps PR #47's name rather than a new `ci-report-r4b`.
    - **`cargo --timings` on every compiling command:**
        - the Cassandra job's two `cargo test --no-run` builds;
        - both clippy jobs;
        - the R4B check.

      `ci-report.py build` handles the output:
        - it keeps `cargo-timing.html` in the artifact as `<id>-timings.html`;
        - it reads the unit data and lists the ten slowest units, with the unit count, the fresh units and cargo's total;
        - it deletes the older timestamped charts, since the target directories are persistent.

      `--timings` changes no fingerprint, so the test steps after the build, which run without it, rebuild nothing.
    - **Cassandra's start and the schema apply.** A new subcommand, `ci-report.py timed GROUP LABEL -- COMMAND`, runs a command, exits with its status, and records its wall time. It wraps `docker compose up --wait` and each schema file's `cqlsh -f`.

      A step after the schema checks (report only: `continue-on-error: true`, `timeout-minutes: 2`, and run unless cancelled once the build succeeded) writes the section through `ci-report.py node-start`. It also gives the container's start to its "Starting listening for CQL clients" line, from `docker inspect` `StartedAt` and `docker logs --timestamps`. Unlike the start step's wall time, that figure is not rounded up by the healthcheck's 10 s interval.
    - **The node's JVM and container,** in the node-statistics step, before the readiness test restarts the node:
        - `nodetool info`: heap used and total, off heap, uptime;
        - `nodetool gcstats`: collections, total and maximum pause, since the node started, because this is the first call;
        - `nodetool compactionstats`: pending, active, completed and aborted;
        - the container's own cgroup v2 files, read with `docker exec`: `memory.peak`, `memory.current`, `memory.max`, `memory.stat` `anon` and `file`, and `memory.events` `oom_kill`.

      The peak is stated as a share of the limit. A peak over 90 %, or any OOM kill, is a warning.
    - **`hfs`'s peak RSS per boot,** in `boot_time_report`: `VmHWM` from `/proc/<pid>/status`, read once `hfs` serves, so after the seed and the load, and before it is stopped. Linux only; elsewhere it reads "n/a".
    - **The stored-SearchParameter load against the node's tenants,** as its own table in the boot report. Per run it gives the tenants with a SearchParameter, the stored (non-spec) definitions, the load, and the change from the run before. Each fresh run adds one seeded tenant, so that change is the cost of one. There is no "per tenant" column: dividing by every tenant with a SearchParameter understates the seeded tenants' cost several times over.

      **A warning above 5 s per boot.** 5 s is half the 10 s seeding budget (roadmap section 7, ADR-aacruzgon-0043). The seed is paid once per tenant; the load on every boot, warm restarts included, and on every TTL refresh, so it gets the stricter line. At the about 0.2 s per seeded tenant measured so far (CI: 210, 422 and 593 ms with 1, 2 and 3 seeded tenants; locally about 0.18 s), that is about 25 seeded tenants on one node; at the 0.2 to 0.55 s ADR-aacruzgon-0043 measured on a loaded machine, about 9 to 25.

      CI's fresh node only ever has three seeded tenants, so there the line catches a slowdown per tenant, about 8×, rather than growth in their number. Only a long-lived node, such as the shared development node, approaches it by accumulating tenants.
- **Alternatives considered:**
    - **Report the load per tenant only, without a threshold:** lost. A table nobody is warned about does not reach a review. The roadmap's own trigger (section 7) needs a line to be checkable, as the seeding budget did.
    - **The same 10 s line as the seed:** lost. A load of 10 s would double a warm boot (1.3 s in CI) many times over, on every restart and refresh, while the seed's 10 s is paid once per tenant.
    - **A per-tenant threshold** (for example 100 ms per seeded tenant): not chosen. The test cannot tell seeded tenants from others without a CQL client. The figure also swings with the node's other load: +0.16 s and +0.41 s for one tenant in the same local run. The total is what delays the first request.
    - **The container's memory from `docker stats`:** lost. It gives the current figure, not the peak, and needs a second sample loop. `memory.peak` is the kernel's own high-water mark.
    - **Taking the JVM and container figures after the readiness test:** lost. The restart resets the GC counters and, through the new cgroup, the memory peak. Before it they cover the schema, the tests, the boot report's three seeds and PR #47's latency runs.
    - **`/usr/bin/time -v` around each boot for the peak RSS:** lost. It is not installed on every machine, macOS has a different `time`, and the boot test already holds the child's pid.
    - **Repeating PR #47's cold or warm line in each build section:** dropped. PR #47's "Target directory size" section already gives it, with the reason.
    - **A separate `ci-report-r4b` artifact,** as first requested: not chosen. PR #47 already uploads `ci-report-r4b-check` from that job, and a second artifact from one job would split its reports.
- **Consequences:**
    - **Time added: about 2 s** to the required job (run 42265211023 on `acruzdebian`):
        - the node-statistics step took 5 s, against 3 s in PR #46's run, for its three `nodetool` calls and one `docker exec`;
        - the node-start report, under 1 s;
        - `--timings`, nothing measurable: the warm build took 13 s.

      The R4B and clippy jobs gain nothing measurable either.
    - **The artifacts grow** by one timings chart per compiling command: 28 to 190 KB each on warm builds, about 0.5 to 2 MB on cold ones [the cold figure is an estimate].
    - **The stored-SearchParameter load is now visible on every run.** On the shared development node, with 93 to 95 tenants, it was 2.3 to 2.9 s per boot, more than the seed. If it passes 5 s, the fix belongs to the backend, for example scoping the load to the tenants this server serves, or not reading definitions the base already holds. That is not a CI change.
    - **The container's peak is read before the snapshot's `nodetool` calls.** Each is a JVM inside the container's cgroup with the compose file's `MAX_HEAP_SIZE`; one `nodetool info` raised `memory.current` by about 136 MB. The healthcheck's `cqlsh` and every `docker exec` still count against the same limit.
    - **`gcstats` resets its counters on each call,** so a later report that calls it again sees only the interval since this one.
    - **Peak RSS is Linux only.** Locally on macOS it reads "n/a".
- **AI involvement:**
    - **Tool and model:** Claude Code (Opus 5.5), as the Step 2 worker `s2-testnode` (PR #49).
    - **What the AI proposed:** the `timed` and `node-start` subcommands, the timings parsing and clean-up, the cgroup source for the container's peak, `VmHWM` for `hfs`, and the 5 s load threshold and its reasoning.
    - **What Alan changed:** Alan asked for every CI report that can be built so far to be built and recorded in an ADR, once PR #46's work had named them as candidates. He did not set the load threshold; it was put to him for review, and he approved it on 2026-11-06. This entry was written after his approval of the other Step 2 records; he approved it later on 2026-11-06.
    - **Verified:**
        - **Locally:**
            - `ci-report.py node-stats` against the shared node: heap 220.77 / 1024 MB, 49 collections (491 ms in all, 65 ms max), compactions 0 pending, 58 completed, and the container's peak 2,159 of 3,072 MiB (70 %).
            - `node-start` read the shared node's start → CQL line as 57.8 s.
            - A cold `--timings` build of the Milestone A test: 640 units, 135 s in all, `helios-fhir` 46.9 s and `helios-fhirpath` 44.0 s the slowest.
            - The boot report on the shared node: the tenant table rendered, with 93 to 95 tenants and a load of 2.27 to 2.85 s; peak RSS "n/a" on macOS.
            - Clippy on `cassandra_milestone_a.rs` with CI's flags: no hits. actionlint: only the existing `runner.workspace` false positives.
        - **CI** on `1c84a1a04` (before the restack, then on PR #47 without PR #48; now `edbd4cfd3`, the same patch): R4B check run 42265207023, fmt/clippy/Cassandra run 42265211023 (https://github.com/aacruzgon/hfs-Cassandra-CS4411/actions/runs/42265211023), all five jobs green.
            - **Start:** compose up 62.9 s, but container start → CQL only 56.6 s. The rest is the healthcheck's polling.
            - **Schema apply:** the keyspace 0.3 s, `schema.cql` 0.9 s.
            - **Node, after the tests:** heap 497 / 1,024 MB, 3 GC collections (58 ms total, 33 ms max), 13 compactions completed, none pending.
            - **Container memory:** peak 1,952 of 3,072 MiB (64 %), no OOM kill.
            - **`hfs` peak RSS:** 104 to 109 MiB per boot.
            - **Stored-SearchParameter load:** 0.21, 0.42 and 0.59 s with 1, 2 and 3 seeded tenants, that is +0.21 s and +0.18 s per seeded tenant.
            - **Timings charts:** 28 to 190 KB each, in every compiling job's artifact (warm builds, so few units compiled).
        - A wording fix to the timings line followed in `0e60f5f45` (now `cd10b66d0`), re-run as 42265738650 (R4B, success) and 42265742758 (superseded by the restack after #36).
        - **The audit subagent** found nothing blocking. All nine of its findings are fixed:
            - the per-tenant column, which understated the cost;
            - the container's peak, which now is read before the snapshot's own `nodetool` JVMs;
            - an active compaction counted twice;
            - this entry's per-tenant figures;
            - wording that named the pull request's position in the stack;
            - a cgroup read that was all or nothing;
            - `node-start`'s missing timeout, its noise after a failed build, and a restarted container's earlier boot;
            - the cold/warm line, which repeated PR #47's report;
            - the unit mode labels.
- **Links:**
    - PR #49 (branch `cassandra/step2-more-reports`); `.github/scripts/ci-report.py` (`build` timings, `timed`, `node-start`, `node-stats`); `.github/workflows/fmt-clippy-cassandra.yml`; `.github/workflows/r4b-compile-check.yml`; `crates/hfs/tests/cassandra_milestone_a.rs` (`STORED_LOAD_WARN`, `Hfs::peak_rss_kib`).
    - ADR-aacruzgon-0002, 0023, 0041, 0043, 0045, 0046; roadmap section 7.
