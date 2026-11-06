# ADR-aacruzgon-0030: The Cassandra job builds its tests before starting the node, and stays on the light runners

- **Date:** 2026-11-06
- **Status:** accepted
- **Context:**
    - **The plan.** Roadmap item 0.5 (Step 0, the CI-order bullet) said the Cassandra job applies the schema "until Step 2 adds `tests/cassandra_tests.rs`, which then runs with `HFS_TEST_CASSANDRA_URL` pointing at that container" (ADR-aacruzgon-0020). ADR-aacruzgon-0021 records the same plan. Step 2 item 2.0 adds that binary (ADR-aacruzgon-0029).
    - **The job name is fixed.** `Cassandra schema (single node)` is a required check (ADR-aacruzgon-0021), so renaming it would break the ruleset.
    - **Memory.** The job carries `hfs-cassandra-light`, so it also lands on `acruzdebian`, whose runner service is capped at 9 GiB. ADR-aacruzgon-0023 ("To revisit at Step 2") asked for the build's peak to be measured there once the job compiles, and for the job to move to `hfs-cassandra-heavy` if it does not fit.
    - **Speed.** Until now the job built nothing, so it had no Rust toolchain step and no target directory.
- **Decision:**
    - **Run the tests inside the existing job,** as ADR-aacruzgon-0021 already planned ("From Step 2 it also runs the Cassandra test binary against that node"), under its unchanged name, after the schema checks pass. The new decisions are the ones below. `HFS_TEST_CASSANDRA_URL=127.0.0.1:$CASSANDRA_HOST_PORT` (59142) points the tests at the job's own `hfs-cassandra-ci` node, through the published port, the same path `cqlsh` uses.
    - **Build first, then start Cassandra.** `cargo test -p helios-persistence --features cassandra --test cassandra_tests --no-run` runs before the container starts, and the later step only runs the binary. The build's memory peak and the 3 GiB container therefore never overlap.
    - **The job stays light.** A cold build on `acruzdebian` peaked at 4,817 MiB of anonymous memory against the 9 GiB (9,216 MiB) cap, with no OOM kill; see Consequences.
    - **The build step prints its own peaks.** A background loop samples the job's cgroup every second. The cgroup is `/sys/fs/cgroup` plus the path in `/proc/self/cgroup`, that is, the runner service's. The step prints, with the runner's name:
        - the peak of `memory.current`;
        - the peak of `memory.stat`'s `anon`;
        - `memory.max`;
        - the OOM kills during the build, from `memory.events`.

      An EXIT trap prints them on a failed build too. `anon` is the figure that must fit. `memory.current` includes page cache, which fills the cgroup up to its cap and is reclaimed there: on the first run it read 9,190 MiB, a full cgroup and not a near miss. The margin stays visible on every run, as ADR-sam-michaels-0003 asked of the clippy jobs. The build runs under `nice -n 10`, as clippy does, because `sam-7900x` is Sam's desktop.
    - **A persistent target directory,** `${{ runner.workspace }}/target/cassandra-tests`, with the stamp and size reset step of ADR-sam-michaels-0002 and a 15 GB cap, not `Swatinem/rust-cache`, which that ADR rejected for every compiling job. Add `CARGO_INCREMENTAL=0` for the job: the incremental cache would grow with every commit until the cap wipes the whole directory. A cold build leaves about 5 GB, so the cap leaves room for the tests of the pull requests above it. It is 15 GB rather than the R4B check's 10 GB because this is a debug build with debug info (codegen and linking), not a `cargo check`. ADR-sam-michaels-0002 asks each compiling job to record its cap, and this entry is that record.
    - **`timeout-minutes` goes from 15 to 40,** for a cold build plus Cassandra's start.
- **Alternatives considered:**
    - **A separate job with its own node:** lost. ADR-aacruzgon-0021 and roadmap item 0.5 already put the tests in this job. A new job would start a second Cassandra on the runner, and it would gate merges only if it were added to the ruleset as a sixth required check. A second job that `needs:` this one cannot reuse its node either, because the node is removed when the job ends.
    - **Start the node first, then build (start, apply, build, test):** lost. A build failure would then still report the schema result, but the compile peak would overlap the 3 GiB container. On `acruzdebian` that container sits outside the runner's 9 GiB cap but inside the machine's 15 GB (ADR-aacruzgon-0023).
    - **Move the job to `hfs-cassandra-heavy`:** lost while the measured peak stays well under 9 GiB. It would make every pull request wait for `sam-7900x`, even when only `acruzdebian` is on.
    - **Build with `--no-default-features --features cassandra,R4`:** not chosen. It would skip `sqlite`, but CI would then build a different feature set from the command developers run, which the roadmap names.
- **Consequences:**
    - **Every pull request now builds and runs the Cassandra tests,** and the tests of the pull requests above it run there as they land. On `acruzdebian` the job's time is a 118 s cold build, seconds when the target directory is warm, 63 to 64 s for Cassandra to turn healthy, and the tests' own time (0.02 s at PR #36). The first run took 3 min 13 s in all (run 42178035007).
    - **Measured peaks** (the build step's own output, on the runner cgroup, page cache included):

      | Run | Runner | Build | `anon` peak | `memory.current` peak | OOM kills | Cap |
      | --- | --- | --- | --- | --- | --- | --- |
      | 42178035007 | `acruzdebian` | cold (fresh dir) | not sampled yet | 9,190 MiB | not sampled; the build succeeded | 9,216 MiB |
      | 42178609043 | `acruzdebian` | cold (dir reset by a temporary `CAP_GB: 0`) | 4,817 MiB | 9,215 MiB | 0 | 9,216 MiB |
      | 42179035900 | `sam-7900x` | cold (first run there) | 4,511 MiB | 10,108 MiB | 0 | 20,480 MiB |

      Margin: about 4.3 GiB of anonymous memory under the cap, at cargo's default 16 jobs. Run 42178609043 came from a temporary commit that pinned the job to `hfs-runner-acruzdebian` and set `CAP_GB: 0` to force a cold build. The commit was then dropped from the branch.

      A local estimate came first: a cold build at `CARGO_BUILD_JOBS=16` on an Apple-silicon laptop peaked at 4.53 GiB, the summed RSS of `rustc`, `cargo`, `cc` and `ld` sampled every 0.5 s, with no page cache counted.
    - **If later pull requests push the peak toward the cap,** the first fix is `CARGO_BUILD_JOBS` in the build step, as ADR-sam-michaels-0003 did for clippy. Moving to heavy comes after that. ADR-aacruzgon-0042 re-measured the peak with `hfs` added to the build (4,872 MiB).
    - **A schema failure now also hides the test result,** and a build failure hides the schema result, because the steps run in order. Both are red either way.
    - **Each runner keeps its own warm `target/cassandra-tests`,** as for the R4B check (ADR-aacruzgon-0023).
    - **The roadmap's Step 0 CI-order bullet** no longer says "until Step 2 adds …"; it is updated in this entry's commit.
- **AI involvement:**
    - **Tool and model:** Claude Code (Opus 5.5), as the Step 2 worker `s2-testnode`.
    - **What the AI proposed:** running inside the existing job (already planned by ADR-aacruzgon-0021 and roadmap item 0.5), building before Cassandra starts, the in-step memory sampler, the target directory, `CARGO_INCREMENTAL=0` and the 40-minute timeout. The audit subagent asked for the EXIT trap and `nice`. The first CI run showed that `memory.current` alone cannot answer the fit question, which led to sampling `anon`.
    - **What Alan changed:** Alan reviewed every Step 2 decision record on 2026-11-06, before it was committed. From his own review he found that several alternatives were stale, because `cdrs-tokio` 9.1.0 and 10.0.0 (released 2026-11-04) had already fixed driver behaviour the records relied on. He found that some alternatives were not real options or did not make sense: strawmen, restatements of the decision, and the workers' own debugging steps listed as alternatives. And he found that some records cited sources outside the repository. He had a review agent re-audit every alternative against the code, the design documents and `cdrs-tokio` 10.0.0, accepted its recommendations, and had each record rewritten from them. For this entry: of its six alternatives, two were kept, two were rewritten, none was moved and two were dropped. The separate-job alternative was rewritten and now also covers a job that `needs:` this one, which was dropped as a separate entry. "Build after the schema checks" was rewritten as "start the node first, then build", with the cost stated plainly. `Swatinem/rust-cache` was dropped, because ADR-sam-michaels-0002 already decided it for every compiling job; the target-directory Decision bullet now cites that ADR for it. The entry was retitled to its new decisions (build before the node, stay light), and the first Decision bullet now says ADR-aacruzgon-0021 already planned running the tests in this job. AI involvement no longer cites a source outside the repository.
    - **Verified:**
        - `actionlint` reports only the `runner.workspace` false positive that the existing clippy jobs also get;
        - the workflow parses, and its step order was read back;
        - the local estimate above;
        - the dispatched runs: https://github.com/aacruzgon/hfs-Cassandra-CS4411/actions/runs/42178035007 and https://github.com/aacruzgon/hfs-Cassandra-CS4411/actions/runs/42178609043 (both on `acruzdebian`) and https://github.com/aacruzgon/hfs-Cassandra-CS4411/actions/runs/42179035900 (`sam-7900x`, at `9f71b0665`, the last workflow change of PR #36; its final head `1711529f7` changes only `docker/cassandra/README.md`). All three passed 24 tests against the job's node; the peaks are as printed by the step;
        - the step's script under `bash -e` with a fake cgroup directory and a failing build: it printed the peaks and still exited 1.
- **Links:**
    - PR #36 (branch `cassandra/step2-test-node`, merged as `fb16da9d1`); `.github/workflows/fmt-clippy-cassandra.yml`, job `cassandra`.
    - ADR-aacruzgon-0019, 0020, 0021, 0023, 0029; ADR-sam-michaels-0002, 0003. Refined by ADR-aacruzgon-0042 (Milestone A in the same job) and 0045 (the shell sampler becomes `ci-report.py build`).
    - `development-roadmap.md` Step 0 CI-order bullet and row 0.5; Step 2 row 2.0.
