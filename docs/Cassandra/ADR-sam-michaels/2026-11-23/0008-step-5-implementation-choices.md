# ADR-sam-michaels-0008: Step 5's implementation choices, pull requests 5 to 8

- **Date:** 2026-11-23
- **Status:** accepted
- **Context:**
  - ADR-sam-michaels-0006 and the plan fix Step 5's design. They leave some choices open: a
    constant, a budget, a tie between two decisions.
  - Others arose while building. Two decisions turned out to conflict, and one worked example
    in the walkthrough was wrong.
  - This entry records those choices for pull requests 5 (extraction), 6 (sweeper), 7 (repair)
    and 8 (crash tests and the post-suite check). Each pull request's description lists its own,
    with the mechanics this entry skips (file placement, commit order, test names).
- **Decision:** the choices below. IDs are ADR-sam-michaels-0006's.
  - **Pull request 5, extraction (roadmap row 5.1)**
    1. **The walkthrough's worked example is corrected to 10 keys, not 8.** R4's `deceased`
       (`Patient.deceased.exists() and Patient.deceased != false`) evaluates to `false` for a
       Patient with no `deceased[x]`, so the writer also extracts the token `deceased = false`.
       That adds a `TokenCode` and a `TokenSystemCode` key. The iwbench evidence agrees: 29
       `token_code_index` rows for 5 Patients, 24 without `deceased`. K(1) is 10 keys in 323
       bytes. An update deletes 8 rows and inserts 10, a delete removes 10, and a revive inserts
       10. The walkthrough's "8 rows, not 9" became "10, not 11". This deviates from the
       walkthrough's section 7.
    2. **Extraction uses `extract_with_missing`, not `extract`** (IW1, D6). It returns the
       failure count that IW1's counter needs, and an `Err` when the whole resource cannot be
       extracted.
    3. **The 128-byte SearchParameter-code refusal is removed** (`check_search_parameter_code`,
       its constant, its three calls and their tests), as ADR-aacruzgon-0079 assigns. It
       removes one Step 3 integration test, which the spec named.
    4. **Statement-count pins are written in terms of `k`,** the decoded size of the stored key
       set. The fhir-versions job runs `--lib` under R4B, R5 and R6, where a literal count can
       differ. The mapping itself is pinned by pure tests.
    5. **The mapping matches on `(param_type, value)`,** as the iwbench code that produced
       IW2's and IW3's row counts does. An empty code is dropped whatever its system (IW2).
    6. **`index_statement` is infallible,** since every row kind now has a statement. The
       extraction and payload counters join `CasStats`.
    7. **`ApplyLate` carries a `write_type`.** The new `Copy` enum `TimeoutWriteType` mirrors
       the driver's `WriteType`, which is not `Copy`. Date bounds are bound as `i64`
       milliseconds, which are the bytes cdrs would send.
  - **Pull request 6, sweeper (row 5.2)**
    1. **SW13 against SW18.** SW13 says "try 3 times"; SW18 says the list never drops an
       intent before `enqueued_at` + 24 h. Both stand: transient failures (`TriesExhausted`,
       `InBatchlog`) stay on the retry list under SW18, and bugs (a `Permanent` index or
       version-row write, an unreadable row, a `write_ts` that is unset or not
       `write_ts_of(last_updated)`) give up after 3 tries and leave the in-memory list. Neither
       ever deletes the intent row, which stays until its TTL.
    2. **The retry list holds at most 10,000 entries.** The design gives no figure; this is a
       chosen budget. The gave-up set is also capped at 10,000 keys and forgets a key after
       24 h. A poll's window starts at `max(mark, to - 24 h)`, so a held mark cannot widen it
       past a day.
    3. **The startup scan's failed or overflowed slices go to the poll.** They join a
       pending-slice list on `SweepState`, retried with a 5 s to 5 min backoff and dropped when
       wholly older than 24 h. A held mark skips intents the retry list owns or has given up on.
    4. **RW4's ranking:** a second semaphore caps the startup scan at 8 of the 32 permits.
       A read, the version row or I2 takes 1 permit; an index delta takes `min(units, 8)`.
    5. **IW24's skip also applies on the replay.** When the sweeper's own version-row write
       fails, it sends no index statement, as the request path does.
    6. **#89 is met with `CasStats::client_side_unknowns()`,** the sum of `unknown_deadline`
       and `unknown_connection`. That is #89's wider form (ADR-aacruzgon-0081) and includes
       SW28's "HFS's own deadline". No new error classification is needed.
    7. **The sweeper keeps its own counter macro and gauges** (`lag_ms`, `retry_len`,
       `startup_scan_ms`), so it and pull request 7's `RepairStats` edit no common lines.
    8. **RW8's shape:** `ResourceRow` gains `stored_fhir_version`, and the writer's version
       parameters become `&str`, so a replay carries the stored text verbatim.
  - **Pull request 7, repair (row 5.3)**
    1. **`MAX_FULL_REPAIR_KEYS = 64`** gives RV1's "about 64 keys". Above it a replay is
       partial: a found row is re-tagged only when a key of K(current) names the same row by
       `row_at` (only a date row can); any other found row is deleted.
    2. **Budget admission is in queue order.** A repair's size is its delta's inserts plus
       deletes. One that does not fit is skipped (`Budget`), and a later, smaller one may still
       be admitted. Skips for any other reason use no budget.
    3. **One overall deadline for the repair phase, equal to `request_timeout_ms`.** `run` is
       `run_within(backend, request_timeout_ms)` over `buffered(4)`, so outcomes keep queue
       order. A repair the deadline cuts is `Failed { TriesExhausted }`, counted and logged.
    4. **Undecodable index rows are skipped and counted** (`undecodable_rows`, logged at
       `error`). Nothing returns an error or panics. An unset or undecodable `index_keys` on
       R1 skips the resource. No undecodable row is planted on a node, since one would fail
       pull request 8's shared scan for every tenant (ADR-sam-michaels-0007).
    5. **One found row tagged above R1 skips the whole resource** (`TagAhead`): R1 lags a
       write. RV3's "a `write_ts` that does not match" means R1's `write_ts` is not
       `write_ts_of(R1.last_updated)`.
    6. **`RepairStats` gets its own counter macro,** a copy of `cas_counters!`, rather than a
       generalised one. Folding the macros together is a later clean-up.
    7. **Pull request 7 merges pull request 5's branch,** so its date re-tag test runs on the
       node: without pull request 5 the writer has no statement for a date key. Two planning
       tests also cover the rule without a node.
  - **Pull request 8, crash tests and the post-suite check (row 5.4)**
    1. **Ten crash tests, not eight.** The plan's list leaves out two of RV17's rows that
       pause or kill Helios: "Paused writer, then a crashed successor (G40)" and "Stale batch
       ordering after a registry change". Both are added, so every RV17 row Step 5 owns has a
       test.
    2. **The strict verdict fails on any of four conditions:** a failed shared scan;
       `tenants == 0`; `defects > 0`; or an intent of a checked tenant left after the sweep
       (R4b, re-read in Rust). It excludes only tenants named `test-planted-` by prefix, which
       stays inside RV22, and the report prints `excluded=`, the count of tenants skipped. It
       does not require `sweep.is_complete()`: planted tenants leave intents the sweep cannot
       finish, by design. This replaces pull request 6's placeholder `defects == 0 &&
       sweep.is_complete()`.
    3. **R4 and the post-suite step move before the CQL audit,** so the sweep's writes are
       audited (rule (d), RV20). The audit check is again "after every step that writes", and
       R4 stays just before the post-suite step (RV21).
    4. **Fixtures store the real key set instead of being renamed to `test-planted-`.** The 21
       tenants that stored a hand-built key set take the set from `backend.index_keys_of`, so
       they are checked rather than hidden by the planted rule: 15 in Step 5's own tests, and
       6 in `versions.rs`, changed by that file's owner. The rename stays only for cases that
       plant a defect on purpose.
    5. **One undecodable index row still voids the shared scan** for every tenant; falling
       back to a `Full` check per tenant would cost seven table scans each. It is made loud:
       the scan error names `tenant/type/id`, `scan_failed` carries it, and the verdict names
       it first.
    6. **The CI entry point waits 15 s before its grace-0 sweep** (SW28), above
       `native_transport_timeout` (12 s), because a CAS still queued when its writer's process
       exited can apply after that exit.
    7. **SW26's 600 s run:** against a private single node started from CI's compose file,
       `tombstones_max = 72` and the average was about 70, against the 1,000-tombstone bound.
- **Alternatives considered:**
  - Keep the walkthrough's 8 keys and special-case `deceased`: lost, since the writer extracts
    what FHIRPath gives and the evidence already counts those rows.
  - SW13 and SW18, drop the intent row after 3 tries: lost, it would make a bug permanent data
    loss, and the retry list is only in memory.
  - Repair budget by FIFO cut-off, stopping at the first repair that does not fit: lost, one
    large repair would starve the small ones behind it. Per-repair deadlines instead of one
    overall deadline: lost, the phase must fit inside one request.
  - Rename every inconsistent fixture to `test-planted-`: lost, it would exclude real
    tenants from the strict check, and the planted rule would hide genuine defects.
  - Fall back to a per-tenant `Full` check on a failed shared scan: lost, seven table scans per
    tenant.
  - Generalise `cas_counters!` for sweeper and repair counters: lost for now, because two
    pull requests would edit the same lines.
- **Consequences:**
  - Roadmap row 5.1 and the walkthrough now state 10 keys; any other document that still says
    8 is stale.
  - Dropping the 128-byte refusal means a long code takes the hashed-row path (IW2).
  - The 10,000 cap, `MAX_FULL_REPAIR_KEYS = 64`, the 8-of-32 permits and the 15 s settle are
    budgets with no measured ground; revisit them if a run shows pressure.
  - The strict verdict trusts the `test-planted-` prefix: a test that plants a defect under
    another name fails the run, and a clean tenant under that prefix is never checked.
  - Pull request 7 merges after pull request 5, whose date statements its re-tag test needs.
  - The SW26 result is for one node on one host. The 1,000 bound applies to every node in
    CI, so recheck if the compose file's node settings change.
- **AI involvement:**
  - Claude Code (Claude Opus planning and reviewing, Claude Sonnet implementing) planned and
    built pull requests 5 to 8 from ADR-sam-michaels-0006's decisions and proposed the choices
    above. Independent AI reviews checked each pull request against its spec. The reviews
    found, among other things, the startup scan's lost slices, the repair phase's missing
    overall deadline and a one-day error in the extraction tests' date constants.
  - Verified by test runs against the dev node, among them `--lib -- backends::cassandra`
    (283 passed, 2 ignored on pull request 8, ten runs), `cassandra_tests` (179 passed), the
    new crash tests (10 passed) and the extraction tests under R4B, R5 and R6.
  - Sam Michaels chose to run SW26 on a private node started from CI's compose file, not the
    shared dev node, and to have each fixture in another collaborator's file changed by its
    owner.
- **Links:**
  - Pull requests 5 to 8: #108 (extraction), #107 (the sweeper), #106 (repair) and #109
    (crash tests and the post-suite check).
  - ADR-sam-michaels-0006: IW1, IW2, IW3, IW24, RW4, RW8, SW13, SW18, SW26, SW28, RV1, RV3,
    RV17, RV20 to RV22. ADR-sam-michaels-0007. ADR-aacruzgon-0079 and -0081. Issue #89.
  - Roadmap rows 5.1 to 5.4; `walkthrough.md` section 7.
