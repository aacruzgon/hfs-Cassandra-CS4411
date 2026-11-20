# ADR-aacruzgon-0082: CI gets report-only mutation testing and flakiness jobs, and the runners split into light, medium and heavy

- **Date:** 2026-11-20
- **Status:** accepted
- **Context:**
    - **Reviewers were finding test gaps by hand.** On Sam's Step 5 pull requests (#87, #90) each review broke the code on purpose to see whether a test failed, and re-ran each new test 50 times. On #90 the first review (at `f7911aed3`) left 2 of its 9 mutations surviving: `index_batches` packing single statements, which splits a unit, and `buffer_unordered(usize::MAX)`, the in-flight bound. The second left 2 of its 5, both in the wiring of the batch's retry policy (`session.rs:541`, `:420`). I want each pull request to show its surviving mutants and its unstable tests without a reviewer having to look. Hand review stays for what a tool cannot do: mutations specific to the design, bypasses of the source scans, and comments checked against the ADRs.
    - **A new tool in CI is a dependency decision.** `cargo-mutants` would be the first mutation-testing tool here. Its 27.1.0 changelog (`NEWS.md`) covers what the job needs:
        - `--in-diff`; an empty diff is not an error, and a diff that matches no mutant exits 0;
        - `--package` and `--features` passed through to cargo;
        - workspace support, with each mutant tested only by its own package's tests;
        - since 26.0.0, any function with an attribute whose path ends in `test`, `#[tokio::test]` included, is skipped rather than mutated.
    - **Every job had to fit 20G.** Both runners carried `hfs-cassandra-light` and `hfs-cassandra-heavy`, so a heavy job could land on `sam-7900x`, whose service is capped at 20G, while `acruzdebian` has 40G (ADR-aacruzgon-0080). cargo-mutants builds one cold copy of the workspace per job.
- **Decision:**
    - **Three runner tiers.**

      | Label | Runners | A job must fit | Jobs |
      | --- | --- | --- | --- |
      | `hfs-cassandra-light` | both | about 9 GB (0023) | fmt, the R4B check, the Cassandra job, coverage, the canary, the R5 and R6 checks, the dataset check |
      | `hfs-cassandra-medium` (new) | both | 20G, `sam-7900x`'s cap | the clippy pair, `Cassandra tests (all FHIR versions)`, `Cassandra flaky tests (report only)` |
      | `hfs-cassandra-heavy` | `acruzdebian` only | 40G, `acruzdebian`'s cap | `Cassandra mutants (report only)` |

      - **The old heavy jobs move to medium.** Every job that targeted heavy fitted 20G by 0080's rule, so all three moved to medium.
      - **No required check changes.** No job's `name:` changed, and no `paths:` filter was added to `r4b-compile-check.yml` or `fmt-clippy-cassandra.yml`.
      - **No required check may be heavy,** since only one runner takes heavy jobs.
      - **The clippy job count still works.** `cargo clippy (helios-fhir)` still reads its cargo job count from the cgroup of the runner it lands on, which does not depend on the label.
    - **The labels change through the API only, in two steps.**
        - On 2026-11-20, before the workflow change was pushed, `hfs-cassandra-medium` was added to both runners: `POST .../actions/runners/22/labels` and `.../21/labels`.
        - Removing `hfs-cassandra-heavy` from `sam-7900x` is Alan's session's first step right after PR #100 merges: `DELETE /repos/aacruzgon/hfs-Cassandra-CS4411/actions/runners/21/labels/hfs-cassandra-heavy` (id 21 is `sam-7900x`), then a check with `GET .../actions/runners` that it carries `hfs-cassandra, hfs-cassandra-light, hfs-cassandra-medium, hfs-runner-sam-7900x`.
            - Not before the merge: until then `main`'s clippy pair still targets heavy, so the DELETE would take two required checks off `sam-7900x`.
            - Not later: until the DELETE a heavy job, such as a pull request labelled `mutants`, can still land on `sam-7900x`, at up to 15.4 GiB `anon` under its 20G cap and 32.9 GiB of scratch disk on Sam's desktop.
        - Every job stays runnable at each step. Nothing on `sam-7900x` itself changes.
    - **Mutation testing, report only: `.github/workflows/cassandra-mutants.yml`, job `Cassandra mutants (report only)`.**
        - **The tool:** `cargo-mutants` 27.1.0, released 2026-06-02, built once per runner into its tool cache with `cargo install --locked`.
        - **What it mutates:** the pull request's changed lines under `crates/persistence/src/backends/cassandra/` (`--in-diff` on `git diff origin/main...HEAD` of that directory), in `helios-persistence` with `--features cassandra`.
        - **The tests:** each mutant runs the `--lib` tests filtered to `backends::cassandra`, against a Cassandra node of the job's own (project `hfs-cassandra-mutants`, port 59842, which no Compose file, workflow or benchmark harness publishes: rootless Docker publishes on the host itself, and the first version's 59342 was the three-node Compose file's default).
        - **Runner and limits:**
            - heavy, with 4 jobs and cargo-mutants' jobserver sharing the 16 CPUs between them;
            - incremental builds;
            - a 600 s build timeout;
            - a 130-minute run budget, after which `timeout -s INT` stops the run and the report counts the rest as untested;
            - a 160-minute job timeout, and cancel-in-progress per pull request.
        - **The build directories** go under `target/mutants-scratch` and are removed after every run.
        - **`RUSTFLAGS=-D long_running_const_eval`** makes a mutant that loops a `const fn` forever unviable within seconds, instead of holding a job for the whole build timeout.
        - **When it runs:** only on a pull request labelled `mutants` that touches the backend (a `paths:` filter, fine here), and on `workflow_dispatch`, with inputs for the head to mutate, the base, a range or `none` in place of base...head, the node, and the job count.
            - The `pull_request` trigger takes `opened`, `synchronize`, `reopened` and `labeled`, so adding the label starts a run and each later push runs it again while the label stays.
            - The job's `if:` keeps the fork guard and adds `contains(github.event.pull_request.labels.*.name, 'mutants')` and `github.event.action != 'labeled' || github.event.label.name == 'mutants'`, so a `labeled` event for any other label runs nothing.
            - Such a run gets a concurrency group of its own (`other-label-<run id>`): in the pull request's group, with cancel-in-progress, it would cancel a run in flight.
            - A dispatch needs no label.
        - **The report:**
            - the summary counts caught, missed, unviable and timed-out mutants, and lists every missed one with its `file:line`, function and replacement (`.github/scripts/mutants-report.py`, with its tests);
            - `mutants.out/` is the `mutants-out` artifact;
            - the job never fails on what survives.
    - **Flakiness, report only: `.github/workflows/cassandra-flaky.yml`, job `Cassandra flaky tests (report only)`.**
        - **Finding the tests:** `.github/scripts/flaky-tests.py` (with its tests) finds the `#[test]` and `#[tokio::test]` functions the diff adds or changes in the backend's `--lib` tests and in `tests/cassandra/`. A test counts when any line the diff adds or removes falls between its first attribute and its closing brace. Strings, chars and comments are blanked first, so a test quoted in a source scan's string is not a test.
        - **Running them:** the job builds the two test binaries once and starts its own node (project `hfs-cassandra-flaky`, port 59442). It runs each test 20 times (a dispatch input for more), one test per process with `--exact`, with the tombstone tests and the latency report opted in.
        - **The report:** one row per test, with runs, passes, failures, timeouts, status and the first failure's message. A test that failed some of its runs is a warning annotation; the job never fails on it.
        - **Medium:** its build peaked at 4,116 MiB `anon`.
    - **A fix in `ci-report.py`.** The `context` command writes its own section to `context.json`, which is also the file the job's context is kept in. So every section after it nested the whole context section as its context (run 44655683717's `node-stats.json` and `tests.json`), and a `build` report after it failed with `KeyError('runner')`, which the new jobs hit. `context()` now unwraps that section, and a new test in `test_ci_report.py` fails without the fix.
- **Alternatives considered:**
    - **The mutation job on every push to a backend pull request:** lost. It was the first version. It holds `acruzdebian` for about 26 minutes a push, and while `sam-7900x` is offline every required check queues behind it on the one runner left: on 2026-11-20 the queue reached 14 runs.
    - **Keep two tiers and let the mutation job run heavy on either runner:** lost.
        - At 4 jobs it peaked at 14.5–15.4 GiB `anon`, which leaves under 5 GiB of `sam-7900x`'s 20G (0080's margin).
        - Its build directories reach 32.9 GiB of disk.
        - It would hold Sam's desktop for 25 minutes a run.
    - **A label of its own for the mutation job, with heavy left on both runners:** lost. "Heavy" would then mean 20G on one runner and 40G on the other, the ambiguity this removes.
    - **`--in-place` with one job and a warm, persistent target directory:** lost. cargo-mutants copies the source tree but not `target/`, so every job starts cold; but with incremental builds a mutant still took a median 26 s to rebuild on #90, so 153 mutants one at a time would take over an hour against 25 minutes with 4 jobs. One `CARGO_TARGET_DIR` shared between the copies is unsafe: cargo-mutants does not set it, so the copies would share one lock and one set of artifacts.
    - **No node:** lost. It was the first version. On `f7911aed3` the node took the missed mutants from 18 to 8 (score 81.1 % to 91.6 %) for 4 more minutes; on #90's head from 21 to 8 (78.8 % to 91.9 %). Every mutant only node tests can catch otherwise shows as missed: of `write.rs`'s 14 missed mutants on #90 without a node (run 44689049080), the node caught 8; the other 6, the retry pauses, are missed either way (run 44714518327).
    - **Mutating the whole backend on every pull request:** lost. That is 1,327 mutants, more than the budget; a dispatch with `in_diff: none` still does it.
    - **`--cap-lints=true`:** dropped after the first #90 run. It also caps `-D long_running_const_eval`, and the crate denies no warning that would otherwise make a mutant unviable.
    - **The flakiness job on light:** possible by memory (its build is the required Cassandra job's), but it holds a runner for minutes of repeated runs; medium, the smallest tier I wanted for the new report jobs, keeps it on either runner.
    - **Re-run every test of a changed file:** lost. #90's changed files hold 111 tests; the 36 it adds or changes are what a reviewer re-ran by hand.
- **Consequences:**
    - **A backend pull request labelled `mutants` shows its missed mutants; every backend pull request shows its unstable tests.** On #90's head: 153 mutants, 91 caught, 8 missed, 54 unviable, 0 timed out, in 24.6 minutes. Its 36 added or changed tests passed 720 of 720 runs, in 3.9 minutes.
    - **A missed mutant is a lead, not a verdict, and the tool does not find everything.**
        - It changes operators and function bodies. It did not express the reviewers' four survivors: repacking statements one by one inserts code, `usize::MAX` replaces a literal argument, and `Retries::OfKind` for `Never` swaps an enum variant.
        - It found 8 survivors the reviewers did not list, the same 8 at `f7911aed3` and at #90's head:
            - the pauses between tries (ADR-aacruzgon-0034's 10 ms and 20 ms) are not tested, in `retry_pause` and in the `attempt < tries` checks of `send_index_batch` and `write_with_retries` (6 mutants);
            - `StatementCounts::total` with a non-zero `serial_read`;
            - `has_at`'s bound at the end of the text, which would leave the source scans blind to CQL that ends in a table name.
        - The 8 are posted on #90 as leads ([comment](https://github.com/aacruzgon/hfs-Cassandra-CS4411/pull/90#issuecomment-7041338305)).
    - **29 of #90's 54 unviable mutants are caught by the compiler.** They are in the two `const fn`s, `writes_an_index_table` and `has_at`, that `batch.rs`'s compile-time assertions evaluate, so a mutant that breaks an assertion does not build.
    - **Mutation runs wait whenever `acruzdebian` is off.** They are reports, so nothing else waits on them.
    - **Only a labelled pull request holds `acruzdebian` for about 26 minutes a push.** Cancel-in-progress drops an older run. Light and medium jobs queue behind it there; with `sam-7900x` on, they go to it. A pull request without the label pays nothing: its run is skipped before it takes a runner.
    - **Someone has to ask for it.** The label is added by hand, by the author or a reviewer of a backend pull request.
    - **To revisit:**
        - the job count: kept at 4, with room for about 6 under 40G at 14.6 GiB `anon`;
        - the 32.9 GiB of transient disk, which grows with the job count;
        - the bare `hfs-cassandra` label, which only `sam-7900x` carries and no `runs-on:` uses (it is in `.github/actionlint.yaml` and the docs).
    - **Sam needs to know that GitHub keeps the labels, not the runner.**
        - A restart keeps them.
        - Re-registering `sam-7900x` with `config.sh`, with or without `--replace`, sets only the labels passed with `--labels`. GitHub's labels page: "If you replace an existing runner, then you must reassign any custom labels."
        - Its list is then `hfs-cassandra,hfs-cassandra-light,hfs-cassandra-medium,hfs-runner-sam-7900x`, without `hfs-cassandra-heavy`.
- **AI involvement:**
    - **Tool and model:** Claude Code (Opus 5.5), a worker of my orchestrating session, on branch `cassandra/ci-mutation-flake`.
    - **What the AI proposed:**
        - the two workflows and their scripts;
        - the test detection rule: any added or removed line from the first attribute to the closing brace, on blanked source;
        - 4 jobs, the 600 s build timeout, the 130-minute budget and the scratch directory;
        - `-D long_running_const_eval` in place of `--cap-lints`, after the first run lost about 100 job-minutes to four `const fn` mutants that looped forever;
        - turning incremental builds back on, after finding `CARGO_INCREMENTAL=0` from `dtolnay/rust-toolchain` in the job's environment;
        - the node on by default, from the measured cost;
        - the label's `if:` and the separate concurrency group for other labels;
        - the `ci-report.py` fix;
        - the upload retry, after one upload was lost to a DNS error;
        - the job-to-tier table.
    - **What the human changed:** I chose the three tiers and the order of the label calls; report-only for both jobs; the scope (`--in-diff` on the backend, `--lib`, 20 runs); and heavy for the mutation job. After the first version ran pure tests only, I asked for the node tests to be included, which took the missed mutants on #90 from 21 to 8. I chose a `mutants` label over running on every push, so the required checks don't queue behind a 26-minute job while `acruzdebian` is the only runner online. I kept 4 jobs, left the bare `hfs-cassandra` label for a later cleanup, and had the 8 survivors posted on #90 as leads.
    - **Verified:**
        - **Labels:** after the two `POST` calls, `gh api .../actions/runners` lists `acruzdebian` (online) with `hfs-cassandra-light, hfs-runner-acruzdebian, hfs-cassandra-heavy, hfs-cassandra-medium` and `sam-7900x` (offline) with `hfs-cassandra, hfs-cassandra-light, hfs-cassandra-heavy, hfs-runner-sam-7900x, hfs-cassandra-medium`.
        - **The `mutants` label,** on head `10d03db50` of #100, which touches no backend file:
            - the push without the label: run 44749191602 `skipped`;
            - adding `mutants`: run 44749302167 queued, then ran and passed (0 files under the backend, 17 s);
            - adding `documentation` while that run was queued: run 44749353272 `skipped`, and 44749302167 was not cancelled; both labels were then removed;
            - a dispatch with no label: run 44749420210 ran and passed.
        - **The move to medium:** the first push's runs, with the clippy pair and the all-versions job on medium, passed on `acruzdebian`, and no job sat queued without a runner.
        - **Lint and tests:** `actionlint` 1.7.12 with `shellcheck` passes on every workflow (bar its known false positive on `runner.workspace`). The scripts' unit tests pass: 11 for `mutants-report.py`, 28 for `flaky-tests.py`, and the new one among `ci-report.py`'s 62, all run by the fmt job.
        - **The mutation runs,** all on `acruzdebian`, 4 jobs (memory from `ci-report.py build` on the runner's cgroup; scratch disk sampled every 30 s):

          | Run | Head | Node | Mutants | Caught | Missed | Unviable | Timed out | Score | Mutation step | Peak `anon` | Scratch peak |
          | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
          | 44680805685 | #90 `998427a19` | no | 153 | 78 | 21 | 50 | 4 | 75.7 % | 59.4 min | not recorded | 22.2 GiB |
          | 44689049080 | #90 | no | 153 | 78 | 21 | 54 | 0 | 78.8 % | 37.6 min | 15,419 MiB | 22.2 GiB |
          | 44689058508 | `f7911aed3` | no | 144 | 77 | 18 | 49 | 0 | 81.1 % | 35.5 min | 15,384 MiB | 22.1 GiB |
          | 44689068123 | `f7911aed3` | yes | 144 | 87 | 8 | 49 | 0 | 91.6 % | 38.4 min | 14,523 MiB | 22.2 GiB |
          | 44689103712 | #90 | yes | 153 | 91 | 8 | 54 | 0 | 91.9 % | 40.9 min | 15,323 MiB | 22.2 GiB |
          | 44714518327 | #90 | yes, incremental | 153 | 91 | 8 | 54 | 0 | 91.9 % | 24.6 min | 14,573 MiB | 32.9 GiB |

          The first run had `--cap-lints=true` and a 1,500 s build timeout; its four timeouts were the `+=` into `*=` mutants of the `const fn` scans, and its memory report hit the `ci-report.py` bug. No OOM kill in any run. `memory.current` reached the 40 GiB cap in each, which is page cache. The node's 3 GiB container runs outside the runner's cgroup (0080).
        - **The flakiness runs:**
            - 44680052059: 36 tests found in #90, both `a_stalled_index_phase_starts_no_try_after_its_deadline` and `an_oversized_unit_is_sent_as_one_batch` among them; 720 of 720 runs passed. It showed that `latency_report::cassandra_latency_report` returns at once without `HFS_TEST_LATENCY_REPORT`, which the job now sets.
            - 44689253658: 720 of 720 passed, the latency report at a median 2.4 s a run. The build took 38 s at 4,116 MiB `anon`, the runs 113 s at 183 MiB, and the whole job 3.9 min.
        - **The comparison with the hand reviews:** the reviewers' mutation diffs in `pr90-review/evidence/` and `pr90-delta/evidence/`, matched against cargo-mutants' mutants of the same functions (`index_batches`, `pack`, `CassandraSession::batch`, `send_index_batch`).
- **Links:**
    - PR #100; the 8 survivors on #90: https://github.com/aacruzgon/hfs-Cassandra-CS4411/pull/90#issuecomment-7041338305.
    - ADR-aacruzgon-0023 and 0080 (amended), 0018, 0019, 0021, 0022, 0034, 0045; ADR-sam-michaels-0001, 0002, 0003.
    - `.github/workflows/cassandra-mutants.yml`, `cassandra-flaky.yml`, `fmt-clippy-cassandra.yml`, `fhir-versions.yml`, `cassandra-coverage.yml`.
    - `.github/scripts/mutants-report.py`, `flaky-tests.py`, `ci-report.py`, and their tests.
    - `development-roadmap.md` section 1 ("Machines"), the Step 0 CI bullet, row 0.13 and the upstream touch list; `CLAUDE.md` and `AGENTS.md` "Commits and CI"; `.coderabbit.yaml`'s CI rules.
    - cargo-mutants 27.1.0: `NEWS.md` and the book pages `in-diff.md`, `build-dirs.md`, `parallelism.md`, `jobserver.md` and `exit-codes.md`, at tag `v27.1.0`.
    - GitHub: "Using labels with self-hosted runners", and the REST API for self-hosted runners.
