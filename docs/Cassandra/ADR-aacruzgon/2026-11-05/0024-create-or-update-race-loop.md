# ADR-aacruzgon-0024: `create_or_update` is a bounded CAS loop on every branch, with one intent row per attempt and a fixed rule for unknown outcomes

- **Date:** 2026-11-05
- **Status:** accepted
- **Amended 2026-11-05 after review on PR #32.** CodeRabbit raised three points, and all three held:
    - `WriteFailure` with `write_type = CAS` also leaves the outcome open;
    - plain `create` (POST) was missing from the unknown-outcome rule;
    - the bound is now stated as 3 retries, or 4 attempts.

  In the same amendment, a client-side timeout or a dropped connection after the CAS was sent is treated like a server `WriteTimeout`/CAS. That case was found while verifying the review, and Alan decided it.

  The Decision and "Verified" below are updated, with each change marked "(amended)".
- **Context:**
    - **The upstream sync brought in a new contract.** PR #28, the 2026-11-04 sync, added a race contract to `ResourceStorage::create_or_update` (`crates/persistence/src/core/storage.rs:516-533`, upstream #2111 and #905). An unconditional PUT is last-writer-wins:
        - callers racing on one id, on one instance or across instances, must all succeed, with distinct versions and a complete history;
        - a lost race is retried a bounded number of times from a fresh read;
        - `VersionConflict`, `AlreadyExists` and `NotFound` from a lost race never reach the caller;
        - "Restoring a soft-deleted resource counts as created."
    - **The shared test makes it concrete.** `concurrent_puts_onto_a_tombstone_all_succeed` (`tests/search/versioned_write_race_suite.rs:395`) races 4 PUTs onto a delete marker at version 2. It requires:
        - versions 3 to 6;
        - exactly one `created = true`;
        - `list_versions` = 1 to 6, with the marker kept;
        - the newest history row equal to the live body.
    - **The bound the test relies on.** Its comment (lines 380-385) assumes 3 retries. SQLite, PostgreSQL and MongoDB all use `MAX_CAS_RETRIES = 3`:
        - `backends/sqlite/storage.rs:564`;
        - `backends/postgres/storage.rs:1289`;
        - `backends/mongodb/storage.rs:2843`.
    - **Our documents disagree with the contract and with each other.**
        - `cassandra-discussion.md` described the retry only under "Batch is different".
        - Step 3 mapped a missed compare-and-swap straight to 404 or 409. Read literally, an unconditional single PUT that lost a race on a live resource or a delete marker would get 409 or 404.
        - `schema/walkthrough.md` §3.5 already looped over all three branches. But it answered an unknown CAS outcome that was "not ours" with 503, although that is also a lost race.
        - The intent row was both deleted on a miss (step 3) and rewritten under the same key per attempt.
        - 503 and 504 were used inconsistently for timeouts (G26).
        - A plain delete that lost to an update was open (G25).
        - Revive (G13) had two open details:
            - whether it keeps the original `fhir_version` or takes the request's;
            - whether C4's literal `SET deleted_at = null` may break the "never bind null" rule.
    - **Every backend fixes a resource's FHIR version at create.**
        - `create_or_update` takes the request's `fhir_version` but uses it only to create:
            - SQLite: `backends/sqlite/storage.rs:546`, used at line 588;
            - PostgreSQL: `backends/postgres/storage.rs:1268`, used at 1313;
            - MongoDB: `backends/mongodb/storage.rs:2829`, used at 2865.
        - On a delete marker, each calls `restore_deleted(tenant, type, id, body)` (sqlite 596, postgres 1321, mongodb 2873), which takes no version and keeps the stored one:
            - SQLite and PostgreSQL select `version_id, fhir_version … AND is_deleted` and write that `fhir_version` into the history row;
            - MongoDB's `restore_attempt` reads `fhir_version` from the deleted document ("The restore keeps the FHIR version the resource was stored under", `mongodb/storage.rs:4981-4987`), writes it into the history row (5111), and leaves it out of the `$set`.
        - `update(tenant, &current, body)` takes no version either. It writes `current.fhir_version()` (sqlite 734, postgres 1429).
    - **Why this is a decision.** It changes the write path and the failure handling, and there are workable alternatives. The revive condition `IF version_id = N` was already correct; what was missing was the loop around it.
- **Decision:**
    - **One loop for every `create_or_update`.** It covers a single PUT without `If-Match` and every batch PUT, on a missing, live or deleted row.
        - Each attempt reads the row, takes a fresh `write_id`, computes `last_updated` and `write_ts` from that read, writes its own intent row with its own `enqueued_at`, and runs C1, C2 or C4.
        - A definite miss deletes that attempt's own intent row and retries at once, with no pause, for at most 3 retries, so 4 attempts in all. A fourth definite miss is `VersionConflict` (409) (wording amended: a writer may lose three times and succeed on its fourth attempt, which the upstream test relies on).
        - A revive keeps `IF version_id = N` and returns `created = true` (201).
        - **A revive keeps the resource's original `fhir_version`** (G13). C4 binds the `fhir_version` from the row the attempt read, not the request's. C2 and every version row (V1, V2, V3) likewise bind the version the resource was created with. As in every other backend, a resource's FHIR version is fixed when it is created.
        - **C4's literal `SET deleted_at = null` is the one allowed exception to "never bind null".** It is part of the statement, not a bound value. It writes exactly one cell tombstone, on purpose, to clear the delete (checked on a 5.0.9 node, 2026-10-13). `schema.cql`, the walkthrough and the roadmap's Step 1 checklist say so.
    - **An unknown CAS outcome** on **every** compare-and-swap (`create`, `create_or_update`, `update`, `delete` and `delete_versioned`) is resolved by a `LOCAL_SERIAL` read before anything else happens. It is any of:
        - a `WriteTimeout` with `write_type = CAS`;
        - a `WriteFailure` with `write_type = CAS` (amended);
        - `CAS_WRITE_UNKNOWN`;
        - (amended) a client-side timeout or a dropped connection after the CAS was sent, treated exactly like a server `WriteTimeout`/CAS (504 if still undecidable). In cdrs-tokio 8.1.9 that is:
            - `Error::General("Connection closed when writing data!")` or `Error::General("Connection closed while waiting for response!")` (`transport.rs:353`, `357`);
            - `Error::Io`;
            - `Error::Timeout` (raised only while connecting, `cluster/connection_pool.rs:46`, so before anything was sent; the serial read then shows the row unchanged);
            - the backend's own request deadline elapsing around the CAS.

      **CAS statements are sent as non-idempotent (amended).** The driver's default retry session re-sends a statement marked idempotent to the next node after `Io`, `General`, `Overloaded`, `Server` or `Truncate` (`retry/retry_policy.rs:72-104`). Speculative execution also runs only for idempotent statements (`cluster/session.rs:667-668`). A CAS re-sent that way after it may have applied is exactly the duplicate this rule prevents, so the backend's own resolution is the only retry.

      **Why `WriteFailure` is included (amended).** With `paxos_variant: v2`, Cassandra 5.0.9 throws it when replicas answer with failures, including in the commit phase after the proposal has reached consensus (`Paxos.java:794-799`, `markAndThrowAsTimeoutOrFailure`).

      **`CAS_WRITE_UNKNOWN` cannot occur with Paxos v2.** Only the legacy v1 path sends it (`StorageProxy.java:771`). cdrs-tokio 8.1.9's decoder (`cassandra-protocol` 3.3.0) has no variant for code 0x1700 and would report `Error::UnexpectedErrorCode`; the backend treats that code as unknown too.

      **What cdrs-tokio surfaces:**
        - `error::Error::Server` with `ErrorType::WriteTimeout(WriteTimeoutError { write_type: WriteType::Cas, .. })`;
        - or `error::Error::Server` with `ErrorType::WriteFailure(WriteFailureError { write_type: WriteType::Cas, .. })`.

      | The read shows | Action |
      | --- | --- |
      | our `write_id` at our version | applied: continue (a `create` returns 201) |
      | unchanged (no row, for a `create`) | not applied: run again after a random 5 to 25 ms pause, from a separate budget of 2 re-runs; a `create` keeps the same `id`, so a retry can never make a second resource (amended) |
      | another writer at our version | a definite miss |
      | a version past ours | no retry: `BackendError::Timeout` (504) after a `WriteTimeout`, a client-side timeout or a dropped connection; `BackendError::Unavailable` (503) after a `WriteFailure` (amended) |
      | the read times out, or fails | 504 if it timed out; 503 if it failed (`ReadFailure`, `Unavailable`) (amended) |

      In every case except "applied", the intent row of the attempt is kept for the sweeper. When the 2 re-runs are used up, the last attempt's error decides the status the same way.

      **Why the status follows the error (amended).** `Write_failure` is "a non-timeout exception during a write request" (native protocol v5, 0x1500): replicas answered with failures. That is the 503 family in `error.rs` ("this instance cannot serve right now"), not a timeout. So the existing rule (504 for a timeout, 503 for unavailable) gives 503 here, while a `WriteTimeout` keeps 504.

      What "a definite miss" means depends on the operation:
        - `create` (amended): `AlreadyExists`, which REST answers with 409 (`crates/rest/src/error.rs:945-949`, `ResourceError::AlreadyExists` to `RestError::VersionConflict`);
        - `create_or_update`: the next attempt;
        - a plain `delete`: its one retry;
        - `update` and `delete_versioned`: `VersionConflict` (409), or `NotFound` (404) when the read shows the row deleted in the gap, as step 3 maps a miss.
    - **Timeouts and unavailability** follow the mapping Helios already has in `crates/rest/src/error.rs`:
        - `BackendError::Timeout` is 504 (module table, line 22; `From<BackendError>`, line 1233);
        - `Unavailable`, `ConnectionFailed` and `PoolExhausted` are 503 (line 21; lines 1219-1225);
        - a replica failure (`WriteFailure`, `ReadFailure`) is `Unavailable`, 503 (amended).

      This resolves G26 for every step.
    - **A plain `delete` that loses to an update retries once,** at once, from its pre-read, as MongoDB does (`backends/mongodb/storage.rs:4578-4594`, #1405). A second miss is 409. `delete_versioned` never retries. This resolves G25.
- **Alternatives considered:**
    - **Add only the retry, and keep the unknown-outcome rule as it was:** lost.
        - Under contention a CAS can time out with nothing committed. A 503 for that is a lost race the contract forbids.
        - Retrying without first resolving the outcome can write our body twice: an extra version, and a wrong `created` flag.
    - **Rewrite one intent row per request, with each attempt's version:** lost. A rewrite of one key with a new version, or a delete and then a re-insert, is ordered by coordinator timestamps, and a skewed coordinator can shadow the newer row. The sweeper then works from the wrong version, or from none. One row per attempt needs no timestamp discipline, and ADR-aacruzgon-0013 already makes a stale intent harmless. Explicit monotonic `USING TIMESTAMP` would also work, but it is one more rule to get wrong.
    - **For the undecidable unknown outcome:**
        - *500, "outcome unknown"* (like `TransactionError::CommitOutcomeUnknown`): lost, because Alan chose the existing timeout mapping, 504, for consistency.
        - *Retry anyway:* lost, because a second version with our body breaks the contract's version and history counts.
    - **Pause after every retry, or after none:** lost. A definite miss means the winner has committed, so the next read sees it and an immediate retry is right; SQLite, PostgreSQL and MongoDB do not pause there either. A contended Paxos round can time out with nobody committed, and an immediate retry would contend again; MongoDB pauses 25 ms in that kind of case.
    - **Plain delete returns 409 on a lost race, with no retry:** lost. "Delete whatever is current" means the same after the other writer commits, and MongoDB already retries once.
    - **Serialise revive with a lock or lease row,** as PostgreSQL's advisory lock does: lost. It costs one more LWT on another partition per write, and a crashed holder leaves the lock behind.
    - **Make revive unconditional:** lost. That is the #2111 bug.
    - **Accord multi-partition transactions:** lost. They are not generally available in 4.x or 5.0.
- **Consequences:**
    - **Easier:**
        - The whole shared race suite is a Step 3 exit criterion that Cassandra can pass, including two batch entries on one id, which race inside one request with `bulk_write_concurrency = 8`.
        - G13 is settled. A revive is 201, by R4 and R4B http.html#update: "201 Created … (or brought back to life/re-created)". It keeps the original `fhir_version`, as every other backend does. Its literal `deleted_at = null` is a documented exception.
        - G25 and G26 are closed.
    - **Harder:**
        - A contended PUT can take up to 4 compare-and-swaps plus 2 serial reads and pauses.
        - In one rare case (an unknown outcome that has already been superseded), a write that may in fact have applied answers 504. The sweeper still finishes that write's index rows, and history keeps it through the next writer's V3 rewrite.
    - **When to revisit the budgets** (3 definite-miss retries, so 4 attempts; 2 unknown-outcome re-runs; the 5 to 25 ms pause): only if the benchmarks show the retries running out.
        - The benchmark that would show it is the hot-key write benchmark in roadmap Step 8: 2, 4, 8 and 16 clients sending unconditional PUTs to one id, on a live resource and on a delete marker, against both backends.
        - An unconditional-PUT 409 means the 3 retries were exhausted (a fourth definite miss). A 504 or 503 from an unresolved CAS outcome means the 2 re-runs were exhausted, or the outcome was undecidable.
        - Any of either at 4 clients or fewer is the trigger in roadmap section 7.
    - **Unchanged:**
        - the tables in `schema.cql`; only comments change there;
        - the spec position. Helios is stricter than FHIR, which permits 409 for a concurrent unconditional PUT ("a 409 Conflict can be returned when the server detects the update cannot be done", http.html#concurrency) and partial history ("Servers may choose to only record successful interactions", http.html#history). That is allowed, and recorded here.
- **AI involvement:**
    - **Tools:** Claude Code (Opus 5.5), working as a worker agent in its own worktree, with web-fetch subagents for the HL7 and Cassandra pages.
    - **What the AI proposed:**
        - The AI stated the contract and the four race tests as rules.
        - It compared the loops in SQLite, PostgreSQL and MongoDB, and traced the REST paths, including that batch entries run concurrently.
        - It went through the design case by case, and found the gaps above.
        - It recommended the loop over all branches with 3 retries, one intent row per attempt, and per-attempt `write_id`.
        - It proposed the unknown-outcome decision table, with a separate budget of 2 and a pause.
        - It offered 504, 500 or a retry for the undecidable case.
    - **What the human changed:**
        - Alan chose 504 for the undecidable case and for a timed-out serial read, to match how Helios already maps timeouts. He also asked that every 503-for-a-timeout in the documents become 504, which closes G26.
        - He kept the budgets, to be revisited only if the benchmarks show the retries running out.
        - He kept one intent row per attempt.
        - He limited the pause to timed-out attempts.
        - He decided that a plain delete losing to an update retries once (G25).
        - He split the `readHistory` finding into ADR-aacruzgon-0025.
        - **In a second round, he:**
            - approved extending the unknown-outcome table from `create_or_update` to every compare-and-swap (`update`, `delete` and `delete_versioned`), which the AI had applied in the documents and flagged;
            - closed G13 here: a revive keeps the original `fhir_version`, as every backend does, and C4's literal `deleted_at = null` is an allowed exception to "never bind null";
            - accepted the ADR.
        - **After CodeRabbit's review on PR #32 (2026-11-05)**, Alan asked for each of its three findings to be checked against the source, and for the ADR to be amended with a dated line rather than rewritten. The AI verified them and proposed:
            - adding `WriteFailure`/CAS to the unknown-outcome rule, with 503 when it leaves the outcome undecidable;
            - applying the rule to `create`, keeping the same `id` on a re-run;
            - wording the bound as 3 retries, or 4 attempts.
        - Alan agreed to keep 503 for an undecidable `WriteFailure`, and 504 for timeouts. He also decided that a client-side driver timeout, or a dropped connection after the CAS was sent, is treated exactly like a server `WriteTimeout`/CAS: resolved by the `LOCAL_SERIAL` read, re-run from the budget of 2 if unchanged, 504 if still undecidable, intent row kept. The AI had reported that gap after the review. It added, from the driver source, that CAS statements must be sent as non-idempotent so the driver never re-sends one itself.
    - **Verified:**
        - **Code:** every file:line above, read at `5b69583be`, including the `fhir_version` handling of `create_or_update`, `restore_deleted`/`restore_attempt` and `update` in the three backends.
        - **Tests:** `cargo test -p helios-persistence --test sqlite_tests -- concurrent_` passed all 4 race tests, including `sqlite_concurrent_puts_onto_a_tombstone_all_succeed`.
        - **Upstream:** the commits `5a32f0baf` and `4927af5db`, and #2111 and #905, read with `git log` and `gh issue view`.
        - **FHIR:** R4, R4B and R5 http.html (#update, #delete, #concurrency, #history), quoted verbatim from hl7.org.
        - **Cassandra:**
            - the native protocol v5 text for `CAS_WRITE_UNKNOWN` and `Write_timeout`/`CAS`, from the trunk spec;
            - amended: on the `cassandra-5.0.9` branch, `Paxos.java` (`markAndThrowAsTimeoutOrFailure` at 551-567, commit-phase call at 794-799, and `useV2()` at 1210), `StorageProxy.java` (326-328, the v2/v1 switch; 771, `CasWriteUnknownResultException` only on the v1 path) and `native_protocol_v5.spec` (`Write_failure` 0x1500, writeType `CAS`).
        - **The driver (amended):** cdrs-tokio 8.1.9 and `cassandra-protocol` 3.3.0 from the local cargo registry:
            - `ErrorType::WriteFailure(WriteFailureError { write_type, .. })` and `WriteType::Cas`;
            - no 0x1700 variant (`message_error.rs:154-176`);
            - the default retry policy does not retry either CAS error (`retry/retry_policy.rs:138-155`);
            - (amended) a connection lost mid-request is `Error::General` (`transport.rs:353`, `357`). `Error::Timeout` is raised only when connecting (`cluster/connection_pool.rs:46`). `Io` and `General` are re-sent to the next node only for idempotent statements (`retry/retry_policy.rs:72-104`), as is speculative execution (`cluster/session.rs:667-668`).
    - **Not verified:**
        - the `cas_contention_timeout` default, which came from a page summary;
        - the Paxos visibility argument for the 3-retry guarantee, and the timestamp-shadowing risk, which are reasoning only;
        - none of it was run on a Cassandra node.
- **Links:**
    - PR #28; upstream HeliosSoftware/hfs#2111 and #905;
    - `crates/persistence/src/core/storage.rs:497-541`; `crates/persistence/tests/search/versioned_write_race_suite.rs:380-493`; `crates/rest/src/error.rs:21-22`, `1219-1225` and `1233`;
    - `cassandra-discussion.md`: "FHIR versions" (version fixed at create), and "Consistency, failure handling and rejection" ("`create_or_update` absorbs the race", step 2, step 3 with "A plain `delete`" and "Revive", "When the LWT outcome is unknown", and the open questions G26/G27 and G28, with G13 removed);
    - `development-roadmap.md`: the Step 1 checklist (`deleted_at`), Step 3 tasks and exit criterion, progress rows 3.2 and 3.3, the Step 8 hot-key benchmark, and the last row of section 7;
    - `schema/walkthrough.md` §2 ("Compare-and-swap on `resources`", version rows), §3.1, §3.4, §3.5, G13, G24, G25, G26 and G28;
    - `schema/README.md` (the 0024 row; G13 removed from the open decisions);
    - `schema/schema.cql` (the "never bind null" exception and the `write_id` and `pending_index_writes` comments);
    - ADR-aacruzgon-0012, 0013 and 0016.
