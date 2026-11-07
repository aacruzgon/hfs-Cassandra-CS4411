# ADR-aacruzgon-0059: Step 3's CI reports: latency per operation, CAS outcomes, statements, intent rows, tombstones, DML and the HL7 rows

- **Date:** 2026-11-06
- **Status:** accepted (Alan approved it on 2026-11-07, with the latency report's 501 tolerance dropped).
- **Context:**
    - **Alan's standing rule** (ADR-aacruzgon-0045, 0046, 0048): CI publishes as many measured reports as it can, each in the step summary and in the job's artifact, and a report never fails a job over a figure.
    - **The plan's ten reports.** Section 7 of the Step 3 plan lists R1 to R10. D3 to D24 were accepted as recommended (`decisions.md`). How each report reads its data was left to the CI layer (L4-ci).
    - **What did not exist when L4-ci started.**
        - **The engine's counters,** which are layer L1's. W1 fixed them the same day:
            - `CassandraBackend::cas_stats() -> CasStats`, with `pairs()`, `since()` and a `Display` of `key=value …`;
            - `statement_counts() -> StatementCounts`, with `read`, `serial_read`, `write` and `cas`.

          L1's node tests print `[cas case=… <Display>]`.
        - **The Step 3 REST test** (`step3_crud_through_rest`), which is L4-tests'.
        - **Most Step 3 operations.** They were 501 stubs until layers L2 and L3.
    - **Constraints:**
        - one mechanism, `.github/scripts/ci-report.py`, stdlib only (ADR-aacruzgon-0045);
        - the required job stays light, and its readiness test stays last (ADR-aacruzgon-0023, 0042);
        - no new build and no new job (Step 3 plan, section 6).
- **Decision:**
    - **R1 and R5, latency and statements per operation:** `crates/persistence/tests/cassandra/latency_report.rs`, extended.
        - **What it measures.** `exists`, `read_batch` (10 and 100 ids), `update`, `vread`, `list_versions`, the three `create_or_update` branches, `delete` and `delete_versioned`, next to Step 2's `create`, `read` and `count`.
            - Each Step 3 operation gets 100 samples, after 3 unmeasured calls.
            - Each operation works on its own ids, on one fresh tenant.
        - **Any error fails it, a 501 (`UnsupportedCapability`) included.** Every operation it measures is real once Step 3 is assembled, so a 501 can only be a regression.
            - Until layer L4, a 501 on an operation's first call was listed as "not implemented yet", so that the report could run at every layer of the stack.
            - Alan dropped that tolerance on 2026-11-07.
        - **It checks each answer against the trait's contract:**
            - a new version on `update`;
            - the `created` flag on each `create_or_update` branch (a revive is 201, D16);
            - a found `vread`.

          A wrong answer fails the step, as a failed call does. That reads ADR-aacruzgon-0046's "fails only if an operation fails" as including an operation that answers wrongly.
        - **Counters.** Around each measured stretch, it reads the engine's counters through one adapter, `mod counters`. The adapter turns `statement_counts()` and `cas_stats().pairs()` into name-to-count maps, and is the only code that knows L1's types.
        - **Printed lines.** It prints a `[cas case=latency-report:<operation> key=value …]` line for each operation, and one for the whole report: the same shape as L1's own node tests.
    - **R2, R3 and R8, the CAS outcome counters:** a new `ci-report.py cas` command.
        - **What it reads.** Every `[cas case=… key=value …]` line in four logs: the persistence tests, the `--lib` tests, the latency report and the Step 3 REST test. A value is a double-quoted string, or a bare token that may hold quoted runs.
        - **What it shows.** One row per line, never summed, since each row is its own backend instance. `attempts_per_applied` is added as the contention figure (R3).
        - **The race lines.** The race wrappers in `cassandra_tests.rs` print one `[cas case=race:<test> …]` line each, from `inner().cas_stats().since(&before)` (ADR-aacruzgon-0057).
        - **Warnings:**
            - **A counter above 0** whose name contains `undecidable`, `exhausted` or `post_cas`: in L1's names, `undecidable`, `miss_budget_exhausted` and `post_cas_failures`.
                - Rows from the `--lib` log (`persistence-lib-tests`) are tabulated but never warned on. Its engine tests provoke those outcomes on purpose, through the fault seam.
            - **A missing log.**
            - **A run with no `race:` case, or no `latency-report:` case.**
    - **R4, intent rows left behind:** a new `ci-report.py intents` command.
        - **The read.** A `cqlsh` read, inside the container, of `pending_index_writes` for the current hour's bucket and the two before it, all 16 shards, filtered to `test-` tenants.
        - **The classification.** Each intent is set against its resource's `resources.version_id` as "at or below the current version", "above it" or "no resource row".
        - **The output** is grouped by the test's tenant label. It is report only: an intent left after an unknown outcome is by design (`cassandra-discussion.md:296-307`).
    - **R6, tombstones.** The node-statistics section gains a table for `resources`, `resource_versions` and `resources_by_type`, from `system_views.tombstones_per_read`.
        - It warns if either of the first two reports a maximum above 1 (G39).
        - The histogram's floor is 1. Verified on a 5.0.9 node: a read of a live row in a table never deleted from reports 1.0, and so does a read of a missing partition. So the plan's "max 0" cannot be told apart from one tombstone, and the report says so.
    - **R7, DML by table and type,** is in the CQL audit section (ADR-aacruzgon-0058).
    - **R9, the Step 3 REST test's suite counts,** is a separate `tests` section, so the existing tests step is untouched.
    - **R10, the HL7 rows observed,** is a new `ci-report.py spec` command. It tabulates the test's `[spec row=… expected=… got=…]` lines, and warns on each mismatch and when the log has none. A weak ETag such as `W/"1"` is one value.
    - **The required Step 3 REST step** (D24) goes after the Milestone A round trip, and before the boot report and the readiness test.
        - **What it runs:** `--exact step3_crud_through_rest`, on the binary already built.
        - **It fails unless libtest prints `test result: ok. 1 passed`,** because `--exact` on a missing name passes with 0 tests. That line is grepped from a process substitution, so that grep stopping early cannot fail the step through a SIGPIPE under `pipefail`.
        - **It fails on every event if the test is missing** from `crates/hfs/tests/cassandra_milestone_a.rs`.
        - **When it runs:** when the round trip passed (`!cancelled() && steps.round-trip.outcome == 'success'`), with a 10-minute timeout.
            - The boot report after it had the default `if: success()`, and now gets the same condition, so a REST failure costs only this step, not the reports after it.
            - Verified on CI with steps that fail on purpose: before the boot step got its condition, a failed REST step skipped the boot report (run 42338334340); with it, boot and every later step ran (run 42339210088); and a failed round trip skips the REST step (run 42338719925).
    - **The script tests run in the required `cargo fmt --check` job** ("Test the CI report script", about a second, no node). That changes the content of a required check (ADR-aacruzgon-0021), which this entry records.
    - **The order in the job:**
        1. enable the audit log, after the schema checks;
        2. the persistence tests, the `--lib` tests, the round trip, the Step 3 REST test, the boot report and the latency report;
        3. the audit check, the intent report, the node statistics, and the readiness test (last);
        4. the parsed reports.
- **Alternatives considered:**
    - **Wait for L1's signatures before writing R1, R2 and R5:** lost. L4-ci starts on day 0 (Step 3 plan, section 5). With the adapter, the change when the signatures landed was one module (`mod counters`), plus the line's tag.
    - **Keep the latency report's 501 tolerance once every operation is real:** lost (Alan, 2026-11-07).
        - It let the report run at every layer below L4 without edits, as the stack merges bottom-up (D12).
        - From L4 on, it would only turn a regression into a gap in the table. It also applied to Step 2's `read`, `count`, `exists` and `read_batch`, which had failed on any error before.
    - **Sum the `[cas …]` rows into a run total:** lost. Each row is a separate backend instance over a separate scope, and some rows are subsets of others (each latency operation against the report's total), so a sum would double-count.
    - **Warn on every row, the `--lib` log's included:** lost. On a healthy run it gave five warnings, all from the fault-seam tests, which would hide a real undecidable write in a race or in the latency report.
    - **R4 through a Rust test with the driver:** lost. It would need new code in a test binary, while the job already reaches the node with `docker exec … cqlsh`, as node-stats does.
    - **Add the Step 3 REST log to the existing tests section:** not chosen. It would edit the same lines as L1's `--lib` step, and a separate section keeps the hunks apart.
    - **Let a manual dispatch pass without the REST test:** dropped at L4. It let the lower layers be dispatched before the test existed; once the test exists, it could only hide a removed test.
- **Consequences:**
    - **The time the reports cost the required job,** in CI run [42382031387](https://github.com/aacruzgon/hfs-Cassandra-CS4411/actions/runs/42382031387) on `sam-7900x`, layer L4, with every Step 3 operation real:

      | Step | Time | What ran |
      | --- | ---: | --- |
      | Step 3 REST | 1 s | the test in 0.72 s, 109 HL7 rows |
      | Latency | 4 s | the backend report took 1.8 s in all |
      | Intents | 1 s | |
      | Parsed reports | 1 s | |
      | Script tests (fmt job) | under 1 s | |

    - **What the reports showed there:**
        - **R1/R5:** every operation was measured, for example:

          | Operation | p50 | Statements per call |
          | --- | ---: | --- |
          | `create` | 1.477 ms | cas 1, write 4 |
          | `update` | 2.221 ms | cas 1, read 1, write 6 |
          | `delete` | 1.854 ms | cas 1, read 1, write 5 |
          | `vread` | 0.29 ms | read 1 |
          | `read_batch` (100 ids) | 6.468 ms | read 100 |
          | revive | 1.913 ms | cas 1, read 1, write 5 |

        - **R2/R3/R8:** 49 rows and no warning. The 5 race rows have `attempts_per_applied` from 1.0 to 4.5, and no undecidable write, exhausted budget or failed post-CAS write.
        - **R4:** 0 intents left behind.
        - **R6:** a maximum of 1.0 for `resources` and `resource_versions`, the floor, so no warning. The other two tables report, for information: `resources_by_type` 103, and `pending_index_writes` 446.
        - **R10:** 109 rows and 0 mismatches.
    - **A failed persistence or `--lib` step hides later signals; this is accepted (Alan, 2026-11-07).** The Milestone A round trip keeps the default `if: success()`. So after such a failure, the round trip, the Step 3 REST test, the boot report, the latency report and the readiness test are all skipped. The job still fails, so nothing passes falsely; only those steps' signals are lost for that run.
    - **The counter names become report columns.** A field added to `CasStats` appears without any change here. A rename that drops `post_cas`, `exhausted` or `undecidable` from a name silently drops R8's warning for it.
    - **The latency report needs an edit** whenever an operation it measures is turned off on purpose.
- **AI involvement:**
    - **Tool and model:** Claude Code (Opus 5.5).
        - The Step 3 worker `s3-ci` (W5, L4-ci) wrote the reports.
        - Worker `s3-suites` (W4) assembled layer L4, made the changes after its audit, and brought this entry up to date with them.
    - **What the AI proposed:** the stub tolerance (since dropped), the adapter, the line formats, the intent classes, the tombstone table and the step order.
    - **The L4 assembly's audit findings that changed the design,** each reproduced before it was fixed:
        - **The `spec` parser cut `W/"1"` to `W/`,** so every ETag row read as matching, and a real mismatch gave no warning. Seen in the run's own `spec.md`.
        - **The `cas` report warned five times on every healthy run,** all from the `--lib` fault-seam tests. Seen on this branch's `--lib --show-output` log. It was also silent on a missing log, and with no race lines at all.
        - **The REST step's `workflow_dispatch` exemption,** and its `sed | grep -q` under `pipefail`.
        - **The latency report's 501 tolerance.** Its removal was validated by a temporary edit that made `vread` of version 1 return `UnsupportedCapability`: the report then failed (rc 101, "vread (version 1) …: capability 'vread' not supported by cassandra"). With the edit undone, it passes with every operation measured.
    - **What Alan changed:**
        - he dropped the 501 tolerance;
        - he kept the round trip's default `if:` (F6), with the consequence recorded above.
    - **Verified:**
        - `test_ci_report.py` (35 tests);
        - `intents` and `node-stats` against a node of W5's own (5.0.9, port 59342);
        - the latency report against the shared node, locally, with every operation measured;
        - clippy on `cassandra_tests` with CI's flags;
        - CI runs [42336353732](https://github.com/aacruzgon/hfs-Cassandra-CS4411/actions/runs/42336353732) and [42337070761](https://github.com/aacruzgon/hfs-Cassandra-CS4411/actions/runs/42337070761) (before L2 and L3), and 42382031387 (L4): every report was in the summary and in the artifact;
        - **the R6 floor:** the first run warned on a maximum of 1.0 for `resources` and `resource_versions`, while the audit log showed no DELETE. Reproduced: one read of a live row in a never-deleted table reports 1.0, and 2 deleted rows report 2.0.
- **Links:**
    - Step 3 plan, section 7 (R1 to R10), section 6 ("CI changes") and D24;
    - ADR-aacruzgon-0021, 0023, 0042, 0045, 0046, 0048, 0057, 0058;
    - `.github/scripts/ci-report.py`, `.github/scripts/test_ci_report.py`, `crates/persistence/tests/cassandra/latency_report.rs`, `.github/workflows/fmt-clippy-cassandra.yml`;
    - `development-roadmap.md`, section 8's CI line, updated in this commit;
    - branch `cassandra/step3-suites`, PR #60.
