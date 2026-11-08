# ADR-aacruzgon-0058: The node's CQL audit log proves that no CQL `DELETE` reaches `resources` or `resource_versions`, as a check in the required Cassandra job

- **Date:** 2026-11-06
- **Status:** accepted (Alan chose D6 of the Step 3 plan on 2026-11-06 and approved this entry on 2026-11-07); amended by ADR-aacruzgon-0066
- **Context:**
    - **The exit criterion.** Roadmap Step 3's exit criterion and row 3.4 said "`cqlsh` shows no CQL `DELETE` was ever issued against `resources`". That cannot be done literally: Cassandra keeps no table of past statements (Step 3 plan, D6).
    - **The design forbids such a DELETE.**
        - `schema.cql:23`: "never a CQL DELETE against resources".
        - `schema.cql:202-204`, G39 and ADR-aacruzgon-0027 (`:165`): "neither `resources` nor `resource_versions` holds any tombstone".
        - Step 3 does delete legitimately from two other tables:
            - `resources_by_type`, on every update and delete (the X1 move, `walkthrough.md:230-239`);
            - `pending_index_writes`, on every finished write (I2, `walkthrough.md:65-90`).
    - **What Alan decided.** D6, accepted as recommended (`decisions.md`), has three layers:
        1. a source scan in `--lib`, which is layer L1's;
        2. a hard audit-log check in CI, which is this entry;
        3. a tombstone report, which is ADR-aacruzgon-0059's.

      It left open how the node is configured, the exact matching rule, and the measured cost.
    - **What the node offers** (Cassandra 5.0.9).
        - The audit log can be turned on at runtime with `nodetool enableauditlog` ([audit_logging](https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/audit_logging.html), [enableauditlog](https://cassandra.apache.org/doc/5.0/cassandra/managing/tools/nodetool/enableauditlog.html)).
        - It writes binary Chronicle files, which `/opt/cassandra/tools/bin/auditlogviewer` prints. That tool is in the image, but not on its PATH.
        - Each entry is `user:…|host:…|source:…|port:…|timestamp:…|type:…|category:…[|batch:…][|ks:…][|scope:…][|operation:<CQL>]` (`AuditLogEntry.java:74-104` at `cassandra-5.0.9`). The CQL text comes last.
    - **The node's yaml cannot carry it.** On every boot, `docker/cassandra/entrypoint.sh:40` copies `cassandra_latest.yaml` over `cassandra.yaml` (ADR-aacruzgon-0001), and that file sets `audit_logging_options.enabled: false`. The developers' shared node runs the same entrypoint.
- **Decision:**
    - **Turn it on at runtime, in the job.** A step after the schema checks and before any test step runs:

      ```bash
      docker exec -e MAX_HEAP_SIZE=128m hfs-cassandra-ci-cassandra-1 nodetool enableauditlog \
        --excluded-keyspaces system,system_schema,system_auth,system_distributed,system_traces,system_views,system_virtual_schema \
        --included-categories DML,ERROR,PREPARE,DDL --blocking true
      ```

        - **It logs every keyspace but the system ones, rather than `--included-keyspaces helios`.**
            - An entry's keyspace is the statement's, or else the session's current one (`AuditLogEntry.Builder(state)`).
            - A failed native batch logs an entry with none, and so does a failed unprepared statement from a session with no keyspace. An include list drops such an entry (`AuditLogFilter.isFiltered`).
            - Verified on 5.0.9: with the include list, five failed native batches from a keyspace-less session left no entry at all.
        - **The defaults stay:** BinAuditLogger, `/opt/cassandra/logs/audit`, HOURLY rolling, a 256 MiB queue and 16 GiB. `--blocking true` makes the default explicit: a full queue slows writers rather than dropping entries.
        - **The JVM heaps are bounded.** Each `docker exec` JVM would otherwise inherit the container's `MAX_HEAP_SIZE=1G`, so each gets its own script's default instead: 128m for nodetool and 256M for `auditlogviewer`.
        - **Nothing in `docker/cassandra/` changes,** so the shared node is untouched.
        - **The categories:**
            - DML is the statements.
            - ERROR is the failed ones, since a DELETE that timed out may still have applied.
            - PREPARE is what was prepared, and is reported only.
            - DDL catches a `TRUNCATE` or `DROP TABLE`. Verified live: `type:TRUNCATE|category:DDL|ks:helios|scope:resource_versions`.
    - **The check runs after every step that writes,** and before the node statistics and the readiness test. The steps that write are the persistence tests, the Milestone A round trip, the Step 3 REST test, the boot report and the latency report. The check:
        1. **Confirms the log is still on.** `nodetool getauditlog` must still show `enabled true`, so that the log covered the whole window.
            - Its output is written to `getauditlog.txt` first and grepped from the file.
            - A `getauditlog | tee | grep -q` pipeline under `pipefail` could fail falsely: grep stops at the first line, and tee then dies of SIGPIPE (exit 141).
        2. **Stops the log.** `nodetool disableauditlog` stops and flushes it (`BinLog.stop` drains its queue).
        3. **Dumps it.** `auditlogviewer`, gzipped into the artifact.
        4. **Checks it.** `ci-report.py cql-audit … --check` exits 1 on any of:
            - **(a)** an entry with `type:DELETE` and a scope of **exactly** `resources` or `resource_versions`. A prefix never matches, so `resources_by_type` and `pending_index_writes` pass.
            - **(b)** any entry whose CQL text holds a `DELETE … FROM [<ks>.]resources` or `resource_versions` statement, quoted or not, with the table name ending there.
            - **(b′)** a `TRUNCATE` or `DROP_TABLE` entry with either scope.
            - **(c)** an empty or missing log, or one with no DML entry in `helios`, which would prove nothing.
            - **(d)** a failed native-protocol batch: a `REQUEST_FAILURE` entry whose text is "BATCH of N statements at consistency X; …".
                - The rule covers an entry with `ks:helios`, and one with none.
                - Such an entry is **unverifiable**: it has no scope and no child text (`AuditLogManager.batchFailure`).
                - It may also have applied. Verified on 5.0.9: under a 1 ms write timeout, a LOGGED batch of 400 DELETEs on `resources` failed with WriteTimeout, and the batchlog applied all 400 about 70 s later. Without rule (d), the check passed on that log.
                - The backend sends no native batch, so this never fails on the backend's traffic.

      The rules apply to entries whose `ks` is `helios` or empty.
    - **Why rule (b) is needed.** D6 assumed that "a batch logs one entry per child statement". Verified live on 5.0.9, that holds only for a native-protocol BATCH message.
        - **A CQL-text batch** (`BEGIN BATCH … APPLY BATCH`, sent as one statement) logs one scope-less `type:BATCH` entry, with every child statement in its text.
        - **A failed statement** logs as `type:REQUEST_FAILURE` (`AuditLogManager.java:149-166`). It has no scope when it was not prepared (`queryFailure`, `:254-260`), while a prepared one keeps its scope (`executeFailure`, `:285-306`).

      Rule (a) alone would miss both. A failed **native** batch has no text either, hence rule (d).
    - **It is a check, not a report.** The step has no `continue-on-error`. That is consistent with ADR-aacruzgon-0045, because it asserts the exit criterion and measures nothing.
        - Its section also carries report R7, DML by table and type.
        - It also carries report-only counts of two statements `schema/README.md:43` says must never exist, which the log can show: a plain (non-`IF`) write to `resources`, and `ALLOW FILTERING`.
        - A bound null cannot be seen, because the log omits values.
    - **The script's own tests** (`.github/scripts/test_ci_report.py`, stdlib `unittest`, about a second) run in the `cargo fmt --check` job. Their fixtures are real `auditlogviewer` output from a 5.0.9 node.
- **Alternatives considered:**
    - **`audit_logging_options` in the yaml, through `entrypoint.sh`:** lost.
        - It would also turn the audit log on for every developer's shared node (`127.0.0.1:59042`), writing every statement to its container layer.
        - The entrypoint is shared by everyone, not a CI file.
    - **FileAuditLogger** (plain text through logback): lost. It needs a logback appender in the image's `logback.xml`, which again changes the shared container setup. BinAuditLogger with `auditlogviewer` needs nothing.
    - **Only rule (a), as D6 first wrote it:** lost, because of the CQL-text batch and the failed statement above.
    - **`--included-keyspaces helios`, the first version of this entry:** lost. It drops every keyspace-less entry, a failed native batch from a session with no keyspace included (verified, above).
    - **A failed native batch as a warning, not a failure:** lost. The entry hides statements that may have applied (the 400-DELETE case), so passing the check on it would assert what the log cannot show.
    - **Only rule (b), a text match:** lost. Rule (a) is the plan's exact rule, and the more robust one for single statements; keeping both costs nothing.
    - **Category DML only:** lost. A `DELETE` that fails logs under ERROR, and a timed-out one may still have applied.
    - **Read the log after the readiness test:** lost.
        - A restart turns the runtime setting off (`AuditLogManager.java:71-86` reads only the yaml).
        - The job's later steps write nothing worth checking.
    - **`sstabledump` on the flushed SSTables:** not in CI. Both sstable tools want the node stopped (Step 3 plan D6). It can be one manual check.
- **Consequences:**
    - **The Step 3 exit criterion runs on every push and every pull request,** as part of a required check (ADR-aacruzgon-0021). It also catches a statement built at run time, which the source scan cannot see.
    - **On the assembled Step 3 stack it passes.** CI run [42382031387](https://github.com/aacruzgon/hfs-Cassandra-CS4411/actions/runs/42382031387) (layer L4, PR #60, `sam-7900x`), with the real `update`, `delete`, `delete_versioned` and `create_or_update`:
        - "Check: passed" on 32,627 entries, 31,125 of them DML in `helios`, parsed in 0.08 s;
        - DML by table:

          | Table | DELETE | UPDATE |
          | --- | ---: | ---: |
          | `resources` | – | 6,063 |
          | `resource_versions` | – | 6,856 |
          | `resources_by_type` | 668 | 5,422 |
          | `pending_index_writes` | 6,039 | 6,077 |

        - 1,501 prepares, no plain write to `resources`, and no `ALLOW FILTERING`;
        - one failed statement, a warning only: the Step 2 test that `USE`s a keyspace that does not exist on purpose (`USE "s2_skeleton_no_such_keyspace"`).
    - **The required job's content changes** (ADR-aacruzgon-0021, 0030): two new steps, one of which can fail it.
        - **The cost: about 2 s a run,** all of it in the two new steps. In run 42382031387, both took 1 s.
        - **The test steps show no measurable difference** with and without the audit log. Measured before layers L2 and L3, with two runs of the same code (`8a6082ab0`) on the same runner (`acruzdebian`): audit on in [42337070761](https://github.com/aacruzgon/hfs-Cassandra-CS4411/actions/runs/42337070761), and off in [42337434648](https://github.com/aacruzgon/hfs-Cassandra-CS4411/actions/runs/42337434648) (a temporary commit, since dropped).

          | Figure | Audit on | Audit off |
          | --- | --- | --- |
          | `cassandra_tests` suite | 2.06 s | 2.07 s |
          | backend `--lib` suite | 1.41 s | 1.42 s |
          | boot-time report suite | 7.49 s | 7.30 s |
          | boot: worst fresh seed | 1,106.9 ms | 1,058.4 ms |
          | latency report (backend) suite | 1.42 s | 1.40 s |
          | backend `create` p50 | 1.709 ms | 1.705 ms |
          | backend `read_batch` (100 ids) p50 | 30.14 ms | 30.05 ms |
          | REST `POST /Patient` p50 | 2.815 ms | 2.702 ms |

          There has been no audit-off run on the assembled stack.
    - **Known limits, accepted:**
        - **A failed native-protocol batch cannot be checked, so it fails the check (rule d).**
            - A future layer that sends native batches would see the check fail on a timed-out batch, even a legitimate one.
            - That is the right trade for an exit criterion: the failure says "cannot rule out", and the dump in the artifact shows the entry.
            - Today the backend sends none: there is no `BatchType` or `BEGIN BATCH` in `backends/cassandra/`.
        - **Rule (b) is not tied to a statement's start.** A string literal or comment naming such a DELETE would also match. That is a false failure, never a missed one, and the backend binds values with `?`, so it cannot happen today.
        - **An entry whose `ks` is a keyspace other than `helios` and the system ones is not checked.**
            - A failed, unprepared `DELETE FROM helios.resources`, sent from a session whose current keyspace is another one, logs that keyspace (`queryFailure`), so the rules skip it. Verified on the parser with a synthetic entry: `ks:other`, `REQUEST_FAILURE`, `DELETE FROM helios.resources` gave no violation. Not reproduced on a node.
            - No CI session uses another keyspace, so this does not arise today.
            - **The one-line hardening, for a later CI pass:** apply rule (b) whatever an entry's `ks` is, when its text names `helios.`.
        - **Plain `SELECT`s are category QUERY, which is not logged.** So the `ALLOW FILTERING` count sees only prepared and DML statements.
    - **The boot-time and latency trends move here.** The audit log covers the boot report and the latency report, because both write and must be checked. Their figures from this change on are measured with the audit log on. The overhead measured for those steps is within noise (the table above).
    - **`block: true` is the default.** If the audit queue ever fills, writers block rather than lose entries, which is the right trade for a check. The job writes about 33,000 entries (run 42382031387), against a 256 MiB queue. Locally, 5,843 entries took 2.1 MB in the audit directory.
    - **The audit files live in the container's writable layer,** and go with `down -v`. The gzipped dump in the artifact is what remains.
- **AI involvement:**
    - **Tool and model:** Claude Code (Opus 5.5).
        - The Step 3 worker `s3-ci` (W5, layer L4-ci) wrote the check, with a read-only research subagent for the documentation sweep and a review subagent.
        - Worker `s3-suites` (W4) assembled layer L4 and brought this entry up to date with it.
    - **What the AI proposed:** the runtime enable, the categories, rules (b), (b′) and (c), disabling before the dump, the gzipped dump, the script tests in the fmt job, and the report-only counts.
    - **W5's review subagent's findings,** each adopted: DDL, the final `enabled` check, explicit `--blocking true`, bounded JVM heaps, the known limits, the trend note, and the script tests in the required fmt job.
    - **Its code-reading findings, reproduced later** under Alan's validation rule:
        - F4, the failed native batch, turned out to hide an applied DELETE. That led to rule (d) and the keyspace filter.
        - F5, the prepared failure's scope, held as read.
    - **The L4 assembly's audit findings that changed the design:**
        - **The `getauditlog` pipeline (F2).** It could fail the required check falsely. Reproduced: the same pipeline shape gave `PIPESTATUS 141 141 0` on every one of 10 runs, although grep matched. Fixed by writing the file first: the new shape passes 20 of 20 runs on "enabled true", and fails 20 of 20 on "enabled false".
        - **The non-`helios` keyspace limit (F8),** recorded above.
    - **What Alan changed:** he accepted the non-`helios` keyspace limit (F8) as a known limit, with its hardening left to a later CI pass.
    - **Verified:**
        - **On a Cassandra 5.0.9 node of W5's own.** It was Compose project `s3ci-audit` on port 59342, from the repository's compose file, and was removed afterwards. With cqlsh:
            - a DELETE on `resources_by_type` logs `scope:resources_by_type`;
            - a DELETE on `resources` logs `scope:resources`;
            - a quoted `"resource_versions"` logs `scope:resource_versions`;
            - a CQL-text batch logs one scope-less BATCH entry;
            - an invalid DELETE logs a scope-less REQUEST_FAILURE.
        - **On a second node of W5's own,** with the image's Python driver sending native batches. It was Compose project `s3ci-f4` on port 59343, at 2560m, and was removed afterwards.
            - A successful batch logs one scoped entry per child, which rule (a) catches.
            - Failed batches each log one scope-less `REQUEST_FAILURE` "BATCH of N statements …", and none at all under `--included-keyspaces helios` when the session has no keyspace.
            - The timed-out batch's 400 DELETEs applied about 70 s later.
            - The check before rule (d) passed on that log (rc 0); after it, the check fails (rc 1).
        - **In the 5.0.9 source:** `AuditLogEntry.java:74-104`, `AuditLogManager.java:71-86`, `:149-166`, `:262-345`, and `AuditLogViewer.java:131-138`, `:240-243`.
        - **The script's tests:** 35 pass, including recorded failed native batches.
        - **actionlint:** no findings beyond the three `runner.workspace` ones already there.
        - **CI:** the check passed on runs 42336353732, 42337070761 and 42339210088 (before layers L2 and L3), and on 42382031387 (layer L4).
- **Links:**
    - Step 3 plan D6;
    - `development-roadmap.md`: row 3.4, Step 3's exit criterion and section 8's CI line, all updated in this commit;
    - `schema.cql:23`, `:202-204`; `schema/README.md:43`;
    - ADR-aacruzgon-0001, 0021, 0027, 0042, 0045, 0059;
    - `.github/workflows/fmt-clippy-cassandra.yml`: job `cassandra`, steps "Turn on the CQL audit log" and "Check the CQL audit log …", and job `fmt`, "Test the CI report script";
    - `.github/scripts/ci-report.py` (`cql-audit`), `.github/scripts/test_ci_report.py`;
    - branch `cassandra/step3-suites`, PR #60.
