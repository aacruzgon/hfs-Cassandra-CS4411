# ADR-aacruzgon-0095: The 65,535-statement batch test runs alone, in a step of its own (amends ADR-aacruzgon-0054)

- **Date:** 2026-11-26
- **Status:** accepted (by the orchestrator, which applied Alan's delegation of 2026-11-25 to this pull request; see AI involvement)
- **Context:**
    - **The test.** `a_batch_over_the_statement_limit_is_refused_and_sends_nothing` (`session.rs`, ADR-aacruzgon-0089) sends one batch of exactly 65,535 statements, all writing one `resources_by_type` row. ADR-aacruzgon-0054 put it in the required job's step "cargo test --lib backends::cassandra against this node", where it runs alongside the rest of the suite, at libtest's default thread count.
    - **The failure.** On `sam-7900x` the batch timed out with `WriteTimeout` ("received only 0 responses", `LOCAL_QUORUM`, write type `SIMPLE`). The CQL audit check then failed as well ("cannot rule out a CQL DELETE: a failed native-protocol batch"), as ADR-aacruzgon-0058 foresaw for a timed-out native batch. The record of the schema job, from 2026-11-25 to 2026-11-26:
        - **`sam-7900x`:** 8 of 9 attempts failed, at 24 test threads, in attempts that ran 231 to 325 lib tests. One failure was a push to `main` (`1be5d2a97`, run 45738993055). Two of the failing SHAs, `8569c663b` and `6e33d4357`, passed on `acruzdebian-2` when re-run.
        - **`acruzdebian` and `acruzdebian-2`:** 0 of 17 attempts failed, at 16 test threads.
        - **The FHIR versions job:** 2 of 40 lib runs on `sam-7900x` failed the same way, both in R4, its first lib run (run 45783764882 on `main`, and run 45804008357).

      The independent audit of PR #119 recounted these from the job logs.
    - **Why the batch is slow.** In Cassandra 5.0.9, `write_request_timeout` (2000 ms, `Config.java:151`) counts from when the node begins the request (`cql_start_time: REQUEST`, `Config.java:1391`; `Dispatcher.RequestTime.computeTimeout`; `AbstractWriteResponseHandler.currentTimeoutNanos`). That window includes building the batch's one mutation from 65,535 statements (`BatchStatement.executeWithoutConditions`, `getMutations`). With a single partition the batch is not atomic, so it goes through `StorageProxy.mutate`, hence `SIMPLE`. The node's slowest coordinator write (`nodetool proxyhistograms`) was:
        - on `sam-7900x`'s one pass (run 45723640399): the bucket ending at 1,955,666 µs;
        - on two Debian passes: 263 ms and 316 ms.

      Local per-table write latency stayed under 0.1 ms on the same nodes, so the time is coordinator-side.
    - **Why it is a decision.** It changes what a required check runs (ADR-aacruzgon-0054 counts that as a decision, under ADR-aacruzgon-0021). It is a choice between workable designs.
- **Decision:**
    - The required job's `--lib` step skips this one test (`--skip`). The step right after it runs the test alone with `--exact`, still inside the CQL audit window.
        - It fails unless exactly one test passed.
        - It reports the batch's time, which the test now prints as `[at-limit-batch statements=65535 ms=… sent=…]`.
    - The FHIR versions job's four lib runs skip the test, which binds the FHIR version only as a column's text.
    - The test's name is in each job's env as `AT_LIMIT_TEST`.
    - Each lib run that skips the test first checks that `--skip` drops exactly one test. It compares the `--list` counts with and without the skip, because libtest's `--skip` matches by substring unless `--exact` is given, and `--exact` would make the `backends::cassandra` filter exact too (`6a6fff10b`).
    - The new step writes its report section whatever the test's result, then exits with cargo's status (`00acc2555`).
- **Alternatives considered:**
    - **A longer `write_request_timeout` on CI's node:** lost.
        - It changes the node every test runs against, away from the documented configuration that the dev node and the benchmarks also use (ADR-aacruzgon-0020; `docker/cassandra/entrypoint.sh` is shared with them).
        - It would hide slow writes elsewhere.
        - `crates/persistence/README.md`, "Node settings HFS depends on", keeps the 2 s default below `HFS_CASSANDRA_REQUEST_TIMEOUT_MS`.
    - **A cheaper at-limit batch** (a smaller row, or an unlogged batch): lost.
        - For one partition, a logged batch already runs as not atomic (`mutateAtomic` is false).
        - The cost is per statement, and the test's point is 65,535 of them.
    - **Fewer test threads for the whole lib run:** lost. It slows every test and only narrows the margin.
    - **Running the test before the audit log is on:** lost. The audit check would no longer cover its statements.
    - **A separate job:** lost, for ADR-aacruzgon-0054's reasons: a node of its own, and a new required check name.
- **Consequences:**
    - **Measured on PR #119, run 45801538029.** The batch took, alone:
        - 490 ms on `acruzdebian` (attempt 1);
        - 358 ms and 355 ms on `sam-7900x` (attempts 2 and 3);
        - 490 ms on `acruzdebian-2` (attempt 4).

      Every attempt passed the job, the CQL audit check included. On a single 5.0.9 node on a Mac (the same Compose file, the audit log on), the batch took 0.28 to 0.40 s alone, against 0.43 to 0.71 s under the whole lib run at 11, 16 and 24 threads. The audit check over that node's log passed, with each at-limit batch logged as 65,536 entries.
    - **Easier.** The required job no longer fails on this batch on `sam-7900x`, and each attempt records the batch's margin against the 2 s timeout.
    - **Harder.** There is one more `--exact` step, of about 1 s. Renaming the test must change `AT_LIMIT_TEST` in both workflows, or the step fails (roadmap Step 5, "Four required steps no longer pass on zero tests").
    - **The audit check still fails closed.** A timed-out batch in the new step still fails it.
    - **Revisit if:**
        - **The coverage job fails on it.** `cassandra-coverage.yml` (light, report only) still runs the test inside its whole lib run, so it can still time out there on `sam-7900x`. So can `cassandra-flaky.yml` (medium, which `sam-7900x` takes) and the heavy report-only jobs (mutants, three-node), which never run on `sam-7900x`.
        - **The load source is isolated.** Which load pushes the batch past 2 s on `sam-7900x` is not isolated. Candidates: the 24 test threads; the tombstone tests' 2 × 101,000 DELETEs, which ran throughout the lib run (in the failed run 45794643261, 3,000 to 5,000 audit entries per 500 ms in the 2 s before the time-out); a freshly started node (the FHIR versions job failed only in its first lib run); or other load on Sam's desktop. If it is isolated, a narrower fix may replace this one.
        - **The margin shrinks.** If the batch alone nears 1 s on any runner.
        - **The 31,185-statement batches fail.** The byte-cut tests' 31,185-statement batches (`write.rs`) stay in the lib run. They have about half the statements, have not failed so far, and their margin on `sam-7900x` is not measured.
- **AI involvement:** Claude Code (Claude Opus 5.5), a Rex worker (`at-limit-headroom`) started by the orchestrator session.
    - **What the AI proposed.** It found the cause, laid out the five options above and recommended this one. It wrote the three commits and this entry.
    - **What was verified, and how:**
        - the CI record, from the job logs of every schema-job attempt, by runner, result and tombstone times;
        - the node statistics (`proxyhistograms`, `gcstats`, system_views) from the CI artifacts of runs 45723640399, 45783764776 and 45782096927;
        - the failed run's CQL audit log, the load per 500 ms;
        - Cassandra 5.0.9's `Config.java`, `Dispatcher.java`, `AbstractWriteResponseHandler.java`, `BatchStatement.java`, `StorageProxy.java` and `conf/cassandra_latest.yaml`, read at tag `cassandra-5.0.9`, and `system_views.settings` on a node;
        - local runs on a 5.0.9 node, before and after the change, including `ci-report.py cql-audit --check` on that node's log;
        - PR #119's CI, four attempts;
        - `actionlint` (the same 3 `runner.workspace` findings as on `main`) and `zizmor` (no findings) on both workflows.

      An independent audit of PR #119, by a reviewer the author did not brief, recounted the CI record (8 of 9 and 17 of 17, and the second FHIR versions failure) and found nothing blocking.
    - **What the human changed:** Alan's delegation of 2026-11-25 ('go with any recommendations when it comes to make the decisions so you can merge the PR') was given for PRs #107, #106 and #109. The orchestrator applied it to this pull request on 2026-11-26, because #106's required check cannot pass without it. Alan has not yet read this entry, and may amend or supersede it in a later entry.
- **Links:**
    - PR #119 (`ci/cassandra-at-limit-headroom`), with commits `0204d817b` (the test prints its batch time), `746f78a0a` (the required job's new step), `32dac4c3f` (the FHIR versions skip), `6a6fff10b` (the `--skip` guard in both workflows) and `00acc2555` (the report section written on a failed test too);
    - amends [ADR-aacruzgon-0054](../2026-11-06/0054-required-job-lib-step.md) (the `--lib` step's command);
    - see [ADR-aacruzgon-0058](../2026-11-06/0058-cql-audit-check.md) (the audit check on a failed native batch) and [ADR-aacruzgon-0089](../2026-11-25/0089-index-unit-split-at-statement-limit.md) (the test);
    - runs 45794643261, 45723640399, 45738993055, 45783764882, 45804008357 and 45801538029;
    - roadmap row 0.5.
