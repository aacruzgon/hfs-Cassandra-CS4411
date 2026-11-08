# ADR-aacruzgon-0071: The all-versions job fails unless every version really ran its tests; the required fmt job runs the report's tests; the job's target-dir caps and its two builds are recorded

- **Date:** 2026-11-08
- **Status:** accepted (Alan approved it on 2026-11-08). It amends ADR-aacruzgon-0065: the job's report gains a gate, and the sentence "none edits a required workflow" no longer holds for one `run:` line. The ADR commit appends " Amended by ADR-aacruzgon-0071." to 0065's Status line, keeping its text, and 0065's index row links this entry, as 0047's does 0070.
- **Context:**
    - **What ADR-0065 said PR C would be.** Three report-only jobs in `fhir-versions.yml`, "none edits a required workflow". `Cassandra tests (all FHIR versions)` "builds once", and its results, "with each binary's selector line checked", go in the step summary and the artifact. "Their CI figures come with PR C, in that PR's own ADR entry if they change a decision."
    - **The independent audit of PR #68 (`pr68-review`, 2026-11-08) found four things that change that.** Its report and evidence are in `~/.claude/rex-agent/runs/pr68-review/`.
    - **The job could be green having tested the wrong version.** The test step failed on a non-zero exit, and for `rest` on a count other than 2. Only the report read the selector lines and the `lib` count, and the report exits 0 from a `continue-on-error` step. Run from the step's own text against the shared node:
        - `--lib -- backends::cassandra_renamed`: `0 passed; 1970 filtered out`, step exit 0;
        - a selector that reads a variable the step never sets: 131 + 182 + 2 tests pass as R4 under the name R5, no selector line in any log, step exit 0.

      ADR-0065 rejected exactly this for a bad selector value: "A typo or a misconfigured job would run R4 and report green."
    - **The script's tests ran in no job.** `test_fhir_versions_report.py` is in no workflow. ADR-aacruzgon-0070 (PR #66, merged) says a new script's tests are added by module name to the required fmt job's "Test the CI report script".
    - **The caps were recorded nowhere.** ADR-sam-michaels-0002: "every new job that compiles Rust must pick a cap and record it here or in its own ADR".
    - **It is two builds.** The persistence and hfs test crates have different feature sets, so `helios-fhir` compiles twice: two `libhelios_fhir-*.rlib` of 978 MB each after a cold run.
    - **Why it is a decision:** a report-only job gets a step that fails it, ADR-0045 says a figure never fails a job, and a required workflow's `run:` line changes.
- **Decision:**
    - **A step after the report fails the job unless the report's JSON has `all_ok` true** ("Fail unless every version really ran its tests"). `all_ok` requires that every run finished with exit status 0 and no failed test, that every suite ran more than zero tests and passed, and that the selector line names the version the step set in each binary that ran a test, except `backend_capability_contract`, which never asks for a version. Only `rest` has an exact count (3). (Alan's choice, as recommended, 2026-11-08.)
        - It has no `if:`, so it runs only when every step before it passed: a failed test step has already failed the job, and a second red step would add nothing.
        - **A report that cannot be read fails the job too:** it confirms nothing about the versions.
        - The report step stays `continue-on-error`, as ADR-0045 asks, and the `if: always()` reports after the gate still upload.
        - **It is not a figure failing a job.** It is a broken or missing test run, the case ADR-0045 already lets the boot-time step fail on ("a broken server or a broken measurement, not a slow one").
        - The job stays report-only: its name is in no ruleset.
    - **The required fmt job runs `test_fhir_versions_report` too,** added to the module list of its step "Test the CI report script" in `fmt-clippy-cassandra.yml`, as ADR-0070 says. Only that `run:` line changes; the job and step names do not, and there is no `paths:` filter. This is the one edit to a required workflow, against ADR-0065's "none edits a required workflow". (Alan's choice, as recommended, 2026-11-08.)
    - **The `rest` suite also runs `the_startup_check_matches_the_whole_version`,** PR #67's unit test of the check on `hfs`'s startup line: the third name in its `--exact` list, so it runs once per FHIR version. Every other CI run of that binary names its tests with `--exact`, so the test ran in no job (the audit's finding B-1). The suite's exactly-once count goes from two to three. A rename fails the step and the gate's report. No required workflow changes. This was added after Alan's answers of 2026-11-08.
    - **The caps,** per runner (ADR-aacruzgon-0023):

      | Job | Target dir | Cap | Measured cold, locally |
      | --- | --- | --- | --- |
      | `cargo check (cassandra, R5)` | `target/fhir-versions-r5` | 10 GB | 1.6 GB |
      | `cargo check (cassandra, R6)` | `target/fhir-versions-r6` | 10 GB | 1.5 GB |
      | `Cassandra tests (all FHIR versions)` | `target/fhir-versions-all` | 25 GB | 14 GB |

      They stay as the workflow has them (Alan's choice, as recommended, 2026-11-08).
    - **The all-versions job is two builds, one per test crate.** The comments and the report's text now say so. It is one build per crate, never one per version, so the point of ADR-0065 stands. (Alan's choice to record it, as recommended, 2026-11-08.)
- **Alternatives considered:**
    - **Leave the selector and count checks as warnings:** lost. It keeps the outcome ADR-0065 rejected.
    - **Fail on the JSON from inside the report step:** lost. That step is `continue-on-error` under ADR-0045, so its failure would change nothing.
    - **`if: !cancelled()` on the gate:** not adopted. After a failed test step the gate would add a second red step and repeat what the step's own errors say. A step with no `if:` also does not run after a cancel.
    - **A shell `grep` in `run_suite` for the selector line:** lost. The check is per binary, and `backend_capability_contract` prints no line, which the report already handles.
    - **An exact test count for every suite, not only `rest`:** not taken, on the orchestrating session's recommendation, because those counts change with every added test. So the gate fails a suite that ran no test, but not one that lost tests: on `ea3f49666`, with the step's filters narrowed so that `--lib` ran 3 tests and `cassandra_tests` none, the contract's 3 passing tests kept the `tests` suite green and the gate exited 0 (`evidence/followup2/g.summary` in `~/.claude/rex-agent/runs/pr67-review/`).
    - **Run the script's tests in `fhir-versions.yml` instead of the fmt job:** lost, as a recommendation. It would leave ADR-0065's sentence true, but the tests would run only in a report-only job, against ADR-0070's convention.
    - **A `cargo test` of the startup-line check of its own, once per job:** lost. The report would need a suite that runs once and prints no selector line, for a test that takes no measurable time.
    - **One `cargo test` over both crates, to build `helios-fhir` once:** not tried. The cost is one more compile of a report-only job, and 14 GB is within the cap.
- **Consequences:**
    - **A version that did not really run turns the job red.** Proof from the step text on real logs of a twelve-run job, with one fault swapped in at a time (`evidence/f1-gate-proof.txt`): all-pass exits 0; `lib` with 0 tests exits 1; a broken selector exits 1; an unreadable report exits 1. On the tree with PR #67 merged in (`ed77baa64`), the all-pass and the `lib` case were run again from new logs (`evidence/m-run-summary.txt`), and on `ed77baa64` plus the uncommitted edit that became `ea3f49666`, with three names in the `rest` suite, the all-pass and a renamed test (`evidence/b1-run-summary.txt`). The delta audit by the Rex worker `pr67-review` ran the job's steps on `ea3f49666` itself against the shared node: the all-pass exits 0, `lib` with 0 tests exits 1, a broken selector exits 1, and a renamed test fails the test step (`evidence/followup2/` in `~/.claude/rex-agent/runs/pr67-review/`).
    - **The fmt step runs 85 tests,** and one broken test in any of its three modules makes it exit non-zero (`evidence/f2-fmt-module-proof.txt`).
    - **Disk on the runners.** The three caps add up to 45 GB on `sam-7900x`, and 20 GB on `acruzdebian`, which takes only the two light jobs. `acruzdebian` had 771 GB free on 2026-11-08.
    - **Memory, locally and not yet on CI** (a Mac, the summed RSS of the build; a cold R4B check there reads 2.88 GiB against CI's 3,339 MiB `anon`, so about 1.13×): R5 check 3.08 GiB (3.79–4.14 GiB when `pr67-review` repeated it twice with the same sampler), R6 check 3.71 GiB, all-versions 7.19 and 6.89 GiB. By that factor about 3.5, 4.2 and 8.1 GiB, and about 4.7 GiB for the R5 check at that highest reading. The conclusion holds: all are inside 9 and 20 GiB. CI's peak on `sam-7900x` may be higher at its 24 jobs.
    - **The CI figures ADR-0065 promised are still to come:** none of the three jobs has run on a runner, because a workflow triggers only on PRs into `main`. Record them when #68 has run, with the week's revisit ADR-0065 already sets.
    - **Required checks can queue behind the heavy job,** on the one `sam-7900x` executor (ADR-sam-michaels-0003), which delays them and cannot change their result. Unmeasured until the job runs; worth including in that revisit.
- **AI involvement:** Claude Code (Opus 5.5), as the Rex worker `pr68-review`. The Rex worker `pr67-review` delta-audited PR #68 and the draft of this entry and made its second version, and `pr68-review` audited that version.
    - **What the AI proposed:** the audit's findings, the gate's design and every recommendation above, and this draft.
    - **What the human changed:** He approved this entry on 2026-11-08.
    - **What was verified and how:**
        - `fhir-versions.yml`'s test, report and gate steps, run from their own text under `bash -e`: twelve real runs against the shared node (`evidence/step-baseline.txt`), and real runs with the filter renamed and with the selector broken (a local edit, restored).
        - The cold builds' peaks and target sizes, sampled per second (`evidence/builds.txt`).
        - The CI baseline for the 1.13× factor, from the artifacts of runs 42341372671 and 42308911620.
        - `actionlint` (in the repository; it exits 1 on six notes that `runner.workspace` is undefined, three in `fhir-versions.yml` and three in `fmt-clippy-cassandra.yml`, as `main`'s workflows do, and reports nothing else), `zizmor`, `yamllint`, `shellcheck` and `bash -n`, on `ea3f49666`, in the runs of the Rex worker `pr67-review` (`~/.claude/rex-agent/runs/pr67-review/evidence/followup2/fmt-and-linters.txt` and `ci-rules.txt` beside it).
        - Not verified: anything that needs a runner.
- **Links:**
    - PR #68 (`cassandra/fhir-versions-ci`), commits `530c14354` (gate), `b52b5977e` (fmt step), `8aef02d64` (report script), `55b2cf673` (two builds), `ea3f49666` (the startup-line check), and the merge of PR #67 `ed77baa64`. The audit: `~/.claude/rex-agent/runs/pr68-review/report-pr68-review.md`.
    - ADR-aacruzgon-0065 (amended), 0070, 0045, 0023; ADR-sam-michaels-0002, 0003.
    - `development-roadmap.md` row 0.12 and section 8.
    - `main` at `7efb359b4` when this entry was committed.
