# ADR-aacruzgon-0101: In the report-only Cassandra jobs, coverage runs the 65,535-statement batch test alone, mutation testing skips it, and the flakiness and three-node jobs are unchanged (amends ADR-aacruzgon-0095)

- **Date:** 2026-11-26
- **Status:** proposed
- **Context:**
    - **What ADR-aacruzgon-0095 left open.** It took `session::node_tests::a_batch_over_the_statement_limit_is_refused_and_sends_nothing` out of the required job's `--lib` step and the FHIR versions job's lib runs (PR #119, `04c2db43a`). Its "Revisit if" named the report-only jobs that still ran it: "`cassandra-coverage.yml` (light, report only) still runs the test inside its whole lib run, so it can still time out there on `sam-7900x`. So can `cassandra-flaky.yml` (medium, which `sam-7900x` takes) and the heavy report-only jobs (mutants, three-node), which never run on `sam-7900x`."
    - **The coverage job failed on it.** Both runs were "Cassandra coverage (report only)" on `sam-7900x`, and the test timed out inside the whole lib run, with the rest of it passing:
        - run 45840184803, on #106: `[at-limit-batch statements=65535 ms=2703 sent=false]`, 326 passed, 1 failed;
        - run 45846259659, on #122: `ms=2563`, 329 passed, 1 failed.

      A failed lib step skips the Milestone A steps after it, so the coverage figures of those runs miss them. Alan asked on 2026-11-26 that CI pass on the open PRs.
    - **Each job runs the test differently.** I read the four workflows on `main` (`ad67b9320`) in full:
        - **coverage** runs the whole `backends::cassandra` lib suite in one process, at libtest's default thread count;
        - **flaky** does not. It runs only the tests a pull request's diff adds or changes, each 20 times, one test per process, `--exact`, `--test-threads 1`, one process at a time (`.github/scripts/flaky-tests.py`, `run`). The test is then alone on the node, as in the required job's own step;
        - **mutants** runs the whole lib suite once per mutant, in `MUTANTS_JOBS` (3) build directories at once, all against one node;
        - **three-node** runs the lib binary with the filter `node_tests` (every `node_tests` module, this test's included), five repeats, against three nodes at RF 3, 2 CPUs and a 512M heap each (`docker-compose.cluster.yml`, `docker-compose.cluster.heap512m.yml`), on `acruzdebian` only.
    - **Why it is a decision.** It changes what tests two CI jobs run, and for mutants it trades a test against a false result. ADR-aacruzgon-0095 is the decision it amends.
- **Decision:**
    - **Coverage** (`cassandra-coverage.yml`) does what the required job does:
        - `AT_LIMIT_TEST` is in the job's env;
        - the lib step first checks that `--skip` drops exactly one test, from the `--list` counts with and without it, and fails if not;
        - the lib run takes `--skip "$AT_LIMIT_TEST"`;
        - the step after it runs the test alone with `--exact`, fails unless exactly one test passed, and writes the `[at-limit-batch …]` line to a report section whatever the result.

      The listing's profiles go to a scratch path (`LLVM_PROFILE_FILE`), out of the one directory `cargo llvm-cov report` reads. The new step's profile counts: `show-env`'s `LLVM_PROFILE_FILE` (`…/hfs-Cassandra-CS4411-%p-%24m.profraw` in run 45840184803) gives each process its own file, and the report merges every profile in that directory.
    - **Flaky** (`cassandra-flaky.yml`) is unchanged. It already runs the test alone, one process at a time, when a diff touches it. On PR #119's head (`c834b1d3f`, run 45816527685, on `sam-7900x`) it passed 20 of 20, median 0.35 s, max 0.63 s; its log's batch times were 255 to 540 ms. On `32dac4c3f` (run 45801537631, `acruzdebian`), 20 of 20, median 0.45 s. Skipping it there would stop the one job that measures its flakiness.
    - **Mutants** (`cassandra-mutants.yml`) skips the test in every run (`-- -- backends::cassandra --skip "$AT_LIMIT_TEST"`) and does not run it alone.
        - cargo-mutants has one test command per mutant, so there is no step of its own to move it to.
        - cargo-mutants 27.1.0 counts any failing test under a mutant as caught (`src/outcome.rs`, `mutant_caught`), and a failed baseline stops the run with no mutant tested (`src/lab.rs`). Its book says it "can only help if the test suite is hermetic: if the tests are flaky or non-deterministic, or depend on external state, it will draw the wrong conclusions" (`book/src/limitations.md`), and that parallel jobs against an external database "may cause test flakes" (`book/src/parallelism.md`). A timeout on load would be a false kill, or a run that tests nothing.
        - A step before the run checks the skip the same way, from a build of its own at the CI budget's 4 processes, in the scratch directory the run step empties. The job is report only, so a failed check warns and the report says how many tests the skip dropped.
    - **Three-node** (`cassandra-three-node.yml`) is unchanged. It does not time out there.
        - **Inside the whole `node_tests` run,** on `main` (`ad67b9320`, run 45852723226, `acruzdebian`), it passed in 5 of 5 repeats, in 1,264 to 1,932 ms.
        - **Alone** (a dispatch with `lib_filter` set to the test, run 45858868162, `acruzdebian`), 5 of 5, in 391 to 790 ms.

      The job is nightly and by dispatch, with no `pull_request` trigger, and its lib step is `continue-on-error`. A timeout there reports a failed test and fails neither the job nor a pull request, and the job's purpose is to report what fails on three nodes (ADR-aacruzgon-0083). Its lib filter is a dispatch input, so a skip would also have to compose with whatever filter a dispatch gives.
- **Alternatives considered:**
    - **Coverage: skip the test with no step of its own.** Lost: its lines in `session.rs` would no longer be counted, and the batch's time on the coverage job's runner would not be recorded.
    - **Coverage: a longer `write_request_timeout` on its node.** Lost, for ADR-aacruzgon-0095's reasons: the node would differ from the documented configuration every other job uses.
    - **Flaky: skip the test.** Lost. The job runs it alone, which is the configuration the required job uses, and it measured it there (above).
    - **Mutants: keep the test.** Lost. Under three concurrent lib runs against one node it can fail on load, which cargo-mutants reads as a caught mutant or a failed baseline. It has not failed there yet, but no mutants run has included it: the test arrived on 2026-11-25 (`96d9c7724`), after the last run (45566799539, `23c5066c6`).
    - **Mutants: run it with `--test-threads 1`, or `--jobs 1`.** Lost. Either slows every mutant's run and only narrows the margin, as ADR-aacruzgon-0095 found for fewer threads.
    - **Mutants: a static guard (`grep` for the test's name) instead of a listing build.** Lost. The required job's check is the `--list` count, and a name check would miss a test in a submodule whose path contains the name.
    - **Three-node: skip it in the `node_tests` run now and run it alone after, as coverage does.** Lost for now. It has not timed out there in 5 of 5 repeats, the job is report only and never gates a pull request, and a timeout there is a three-node result the job exists to report. Revisit below.
- **Consequences:**
    - **Easier.** The coverage job no longer runs this batch inside its whole lib run, where it failed twice on `sam-7900x`, and it records the batch's time on each runner. A mutants run can no longer be stopped, or shown a false kill, by it.
    - **Mutants keep their kills without it.** On a single 5.0.9 node on a Mac (`docker/cassandra/docker-compose.yml`, 2.3 GiB limit), cargo-mutants 27.1.0 on `session.rs:637:29` (the `>` of `CassandraSession::batch`'s limit check) with the skip: the baseline ran 326 tests, 2,027 filtered out, and all three mutants were caught. `>=` by `write::node_tests::a_unit_over_the_piece_bytes_lands_and_a_count_cut_piece_is_refused`, `<` and `==` by others. The control, the same run without the skip: 327 tests, 2,026 filtered out, so the skip dropped exactly this one, and the same three mutants caught. There the batch took 433 ms inside the whole lib run.
    - **Harder.** Renaming the test must now change `AT_LIMIT_TEST` in four workflows, or the coverage step fails and the mutants check warns. The mutants job builds the `--lib` tests once more before its run.
    - **Three nodes: the margin is small.** Inside the whole run, the slowest repeat left 68 ms of the 2 s. Alone, the slowest left about 1.2 s. So most of the time is load from the other tests, on nodes of 2 CPUs and a 512M heap each, at RF 3. A nightly may report this test as failed on load alone.
    - **Revisit if:**
        - the flakiness job shows it flaky;
        - the three-node nightly reports it timed out: then give it the coverage job's treatment there, a `--skip` in the `node_tests` run and a run of its own after it;
        - ADR-aacruzgon-0095's load source is isolated: a narrower fix there may let the whole runs keep the test.
- **AI involvement:** Claude Code (Claude Opus 5.5), a Rex worker (`ci-at-limit-report`) started by the orchestrator session.
    - **What the AI proposed.** It read the four workflows and found that flaky already runs the test alone. It proposed the change for each job and wrote the commits and this entry.
    - **What was verified, and how:**
        - the two coverage failures, from the job logs of runs 45840184803 and 45846259659 (runner, the `[at-limit-batch …]` lines, the lib results and `LLVM_PROFILE_FILE`);
        - the flaky record, from the artifacts of runs 45816527685 and 45801537631;
        - that no mutants or three-node run on `main` had included the test (run list against `96d9c7724`);
        - cargo-mutants 27.1.0's source and book, read from the crate;
        - a local cargo-mutants run, with and without the skip, on a 5.0.9 node;
        - two dispatches of the three-node job on `main`, the whole `node_tests` run and the test alone;
        - `actionlint` 1.7.12 (with `shellcheck`) and `zizmor` on each changed workflow and on `main`'s copy: the same one `runner.workspace` finding on coverage as `main`, none on mutants; `zizmor` reports no findings on either;
        - PR #125's CI: the coverage job passed in three attempts of run 45855797677, on `acruzdebian`, `acruzdebian-2` and `acruzdebian-2`. Each lib run had 326 tests, 2,027 filtered out. The test passed alone in 366, 384 and 353 ms. Its lines are counted in the first attempt's `lcov.info` (`session.rs:1793`, `:1800`, `:1814`, one hit each, from the new step only, since the lib run skipped it). No attempt ran on `sam-7900x`, so the new step is not yet observed there. Alan said on 2026-11-26, through the orchestrator, that this is fine.
    - **What the human changed:** Alan asked on 2026-11-26 that CI pass on the open PRs ('lets also make sure CI is passing in this PRs i noticed some jobs failing'), under his approval of the open items' recommendations; he has not yet confirmed this entry's text.
- **Links:**
    - PR #125 (`ci/cassandra-at-limit-report-jobs`), with commits `ac27c6851` (coverage) and `cec43f696` (mutants);
    - amends [ADR-aacruzgon-0095](0095-at-limit-test-own-step.md);
    - see [ADR-aacruzgon-0047](../2026-11-06/0047-coverage-report-in-ci.md) (the coverage job), [ADR-aacruzgon-0082](../2026-11-20/0082-mutation-flakiness-medium-tier.md) (mutants and flaky), [ADR-aacruzgon-0083](../2026-11-20/0083-three-node-ci-job.md) (three nodes) and [ADR-aacruzgon-0089](../2026-11-25/0089-index-unit-split-at-statement-limit.md) (the test);
    - runs 45840184803, 45846259659, 45816527685, 45801537631, 45852723226, 45858868162 and 45855797677;
    - roadmap row 0.13.
