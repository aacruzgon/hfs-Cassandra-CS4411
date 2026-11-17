# ADR-sam-michaels-0006: Step 5's design: index rows by partition, the version row first, a quorum-first sweeper, bounded repair and a report-first CI check

- **Date:** 2026-11-16
- **Status:** accepted
- **Context:**
    - I own roadmap Step 5, rows 5.1 to 5.4: index rows on every write, the sweeper over `pending_index_writes`, repair on read, and the index consistency check. Alan built Steps 2 and 3, so all six steps of the write path already run on `main` (`b4b6d6ddd`). Step 5 of the path writes only the `resources_by_type` row, because the key set is hard-coded to `[IndexKey::ResourcesByType]` at `write.rs:1936`, `update.rs:206` and `upsert.rs:205` and `:216`. Sara owns Step 4, which does not block this.
    - My plan of 2026-11-05 was written before Alan's Steps 2 and 3 and is wrong on several facts. The biggest: every old key set is now read from a stored `index_keys` column and never re-extracted (G22, G40, ADR-aacruzgon-0027). Step 5 had to be planned again against the real code.
    - The design documents leave several things open or get them wrong for the code as built:
        - **Order.** The order of a write's index deletes and inserts. The walkthrough's batch template sends one logged batch; the Step 3 handover says every insert first.
        - **The tie bug.** Today's index diff compares `(key, last_updated)` pairs, but the two date tables have no `last_updated` in their primary key. Once date keys exist, an update that keeps a date deletes that row at the same timestamp it re-inserts it, and Cassandra lets the delete win the tie (0 rows left, verified on the dev node).
        - **The sweeper's read.** The walkthrough (`:705`, `:794`) and the design (`cassandra-discussion.md:347`) make every sweeper read `LOCAL_SERIAL`. On three nodes a serial read can push a live write into issue #77's 504.
        - **Old data.** Nothing heals a resource written before 5.1 and never written again.
    - The plan has 74 decisions: 49 on architecture and 25 on implementation. Six of them were real choices between workable designs, and the other 68 were recommendations. I went through the six one at a time on 2026-11-16 and approved the 68 as written. The table at the end lists all 74 by the plan's IDs, which code comments cite as "ADR-sam-michaels-0006, IW7" and so on.
- **Decision:** Step 5 is built as the table below says, in eight pull requests (build order at the end). These decisions change or extend the design documents, or were real choices:
    1. **The order of a write's index rows: by partition (IW9, B).**
        - Every row of one partition key travels in one unit, its inserts and deletes together, and the units go out at once. A unit is one batch, and Cassandra applies a batch's rows for one partition whole on each replica. The intent is deleted last.
        - **Why:** `count` is `COUNT(*)` over the `(tenant, type)` partition of `resources_by_type` (`read.rs:53`), with no version-tag check. An update moves the resource's row in that partition from `last_updated` N to N+1, so under B no reader sees both rows and `count` never counts a resource twice.
        - The same rule applies to the request, the sweeper and repair, through one shared writer (IW17, SW14).
        - Speed is secondary. B saves one round trip on the index step, about 0.25 ms p50 per update on the dev node, and there is no throughput target behind it.
    2. **The version row before the index rows, and no index rows without it (IW24).**
        - Step 4's version row is acknowledged before step 5 is sent, so anything search finds can be opened with vread and `_history`.
        - New: when the version row fails all its tries, step 5 is skipped, where today it runs anyway (`write.rs:1703-1707`). The intent stays, and the sweeper writes both.
    3. **One row-identity rule (IW7).** One function in `index_keys.rs` decides whether two index rows are the same row, by each table's own primary key. It fixes the tie bug, and it must merge before extraction is switched on. The writer, the sweeper, repair and the consistency check all call it.
    4. **The sweeper reads at quorum first (SW12). This departs from the signed-off design.**
        - The sweeper reads the resource row at `LOCAL_QUORUM`. If that shows version V or later, V is committed, so it replays with no Paxos promise. Only if there is no row, or the version is below V, does it fall back to `LOCAL_SERIAL`, which finishes a compare-and-swap that is still in flight before the intent is deleted.
        - **Why not a serial read for every intent:** a `LOCAL_SERIAL` read is not a plain read. It runs a Paxos prepare round and stores a promise on the row. The trace shows "Promising read ballot", a commit-log append and a Paxos-memtable write. A live compare-and-swap on that row with a lower ballot is then refused and must retry its round, and on three nodes that retry is what times out into issue #77's 504. After an outage the sweeper drains many intents for busy rows just as traffic comes back, so every-intent serial reads would hit live writes when they can least afford it. When quorum already shows V or later, the serial read cannot change the answer.
        - Otherwise this is ADR-aacruzgon-0013 rewritten for stored key sets:
            - Row V below: the compare-and-swap never applied, so the intent is deleted.
            - V current: V's version row is written, K(V) inserted and K(V−1) minus K(V) deleted.
            - V superseded: K(V−1) is deleted.
            - Everything is written at `write_ts_of(last_updated)`.
        - The sweeper never drops an intent before its 24 h TTL (SW18). Several Helios processes each sweep, with no lease (SW22).
    5. **Data written before 5.1 is dropped and reloaded, once (RW6).** Every keyspace written before 5.1 is dropped and reloaded: the dev node's `helios` and any teammate's or benchmark dataset. CI starts fresh containers. `crates/persistence/README.md` and the 5.1 pull request say so, so Alan and Sara reload theirs.
    6. **Repair (RV1, RV3, RV4, RV5).**
        - **What it writes:** repair re-runs step 5 for the current version through the shared writer, as ADR-aacruzgon-0017 says. A resource with more than about 64 keys is the exception: there, repair fixes only the rows the search found and leaves the full replay to the sweeper.
        - **When it acts:** only on a row whose tag is below the current version. It never writes `resources`, `resource_versions` or intents.
        - **No serial read first (RV4):** the tag check's `LOCAL_QUORUM` read is enough. A current version seen at quorum is committed, which is the same argument as in point 4.
        - **Limits:** repair runs at the end of the search's page, at most 32 resources and 512 rows per search, 4 at a time. A failed repair never fails the search.
    7. **The post-suite check in CI is report-first (RV22).**
        - **What it runs:** after every suite, it sweeps every `test-` tenant at grace 0, runs the consistency check over them, and reports the intents left after the sweep (R4b).
        - **When it becomes required:** it only reports from the sweeper pull request (6). The pull request that closes Step 5 (8) makes it fail the job, after three clean runs in a row.
        - **Why:** "Cassandra schema (single node)" is required on `main`, so an unsettled step would block Sara's and Alan's merges.
    8. **Batches are counted separately (IW14).**
        - `StatementCounts` gains `batch` (batch requests) and `batched` (statements inside them). `write` stays single statements, and `total()` counts round trips.
        - So the update test reads `read=1 serial_read=0 write=4 batch=1 batched=2 cas=1`, and the CI report's write-amplification table shows both requests and rows.
    9. **Two index rows the design keeps are now dropped (IW2, RW17, IW3).**
        - **P5 (ADR-aacruzgon-0027) goes further.** No `TokenSystem` row for an implicit or empty system, and no row for an empty code or an empty URI. No search reads them. This saves about 5% of rows on Synthea.
        - **New: the parts of composite parameters are not indexed.** Cassandra serves no composite search (`coverage.md` C7), and keeping them adds 104,820 unread rows on 10k Synthea, +53% rows and tombstones.
    10. **What a client sees from extraction (IW1, IW6).**
        - **A parameter that fails to extract:** that parameter is skipped, logged at warn and counted, and the write still succeeds. Refusing it would let one bad stored SearchParameter block every write of its type. SQLite skips the same way (`backends/sqlite/storage.rs:1869`).
        - **A value too long:** a value over 2,048 bytes, or a parameter name over 128 bytes (G35), answers 400 `InvalidResource` before anything is written.
- **Alternatives considered:**
    - **IW9:**
        - **A, every insert acknowledged before any delete** (the Step 3 handover's rule, and today's code): lost. `count` and a search's `_total` count an updated resource twice until the delete lands. That is one round trip normally, and up to the sweeper's 35 s if Helios crashes between the two.
        - **B+, B with the `resources_by_type` pair and changed-value deletes held until every insert is acknowledged:** lost. It keeps a repair trigger at the cost of A's second round trip. The trigger it keeps matters only if an intent is lost, and SW18 never drops one.
    - **IW24, steps 4 and 5 at once** (the index-writes research's proposal): lost. It saves 0.12 ms p50 per write. But after a crash, search could show N+1 while vread and `_history` answer 404 until the sweeper runs, and the crash tests' `AfterVersionRow` checkpoint would need a redesign.
    - **SW12, a serial read for every intent** (the design as signed off, and the sweeper research's proposal): lost, for the reason under Decision 4. Measured on the dev node: quorum 0.30 ms, serial 0.48 ms per read. That timing was not the reason.
    - **RW6, leaving old data:** lost. Search silently misses each such resource until its next write.
    - **RW6, a reindex:** lost. It is a compare-and-swap per resource, because every index row must appear in the stored `index_keys` (G22). That is a feature of its own, outside Step 5.
    - **RV22, a hard check from the first day:** lost. A gap in the list of tests that plant broken states, or a flaky case, would block every pull request until fixed.
    - **IW14, a batch counted as one `write`:** lost. It keeps today's four-number pins, but a reader cannot tell which writes carried several rows.
    - **SW22, a lease row so that one process sweeps:** lost. It needs a compare-and-swap per poll, the #77 path again, and it stalls on a crashed holder (ADR-aacruzgon-0027).
    - **IW10, plain statements instead of batches:** lost. They are correct and need no batch API, but they lose IW9's per-partition unit, so `count` could see both rows of an update again, and the index step alone ran at about half the rate on the dev node (5.4k against 9.7k writes/s).
    - **IW20, demoting the CI audit's failed-batch rule (d) to a warning:** lost. It would amend ADR-aacruzgon-0058 and 0066 and trust the code being checked.
- **Consequences:**
    - **`count` stays exact** through updates, and every search hit can be opened by version, including after a crash.
    - **B gives something up.** If a changed value's new row fails for good and the intent is then lost, no stale row is left to trigger repair. Search then misses the new value until the resource's next write. Never dropping an intent (SW18) is what closes this, so SW18 must not be weakened later.
    - **Reloads.** Every keyspace written before 5.1 needs one. Mine on the dev node needs the schema re-applied anyway.
    - **A CI gap.** Between pull requests 6 and 8, a Step 5 bug can merge and show only as a line in the run's report, so I read that report on each of my pull requests.
    - **Composite search later.** If anyone adds composite search, the composite rows have to come back, and every resource must be rewritten to get them.
    - **Measure-first switches, each kept behind one constant or function:**
        - unlogged batches of at most 10 partitions, if Step 8 shows the batchlog costing over 20% of throughput (IW10);
        - the node's own batch-size formula instead of counting every byte (IW11);
        - repair's limits as settings (RV8);
        - compressing `index_keys` in the compare-and-swap (IW21).
    - **Cross-team:**
        - **Alan:** `main.rs` starts the sweeper, and the milestone launches set `HFS_CASSANDRA_SWEEPER_ENABLED=false` (SW1, SW23).
        - **Sara:** gets `sweep_pass` for her race suites' "sweep, then require the full history" check, which her PR #83 names as its follow-up. Grace is at least 2 s on three nodes (SW28, RV19).
        - **The team:** names an owner for roadmap row 8.1. Step 5 takes only the in-process crash tests (RV17).
        - **Upstream:** filing the registry generation counter there (IW19) is a separate call.
- **AI involvement:**
    - **Tool and model:** Claude Code, Claude Opus 5.5, as an orchestrator with worker sessions.
    - **What the AI proposed:**
        - **Research, 2026-11-12 and 13.** Three worker sessions researched index writes, the sweeper, and repair with the check. They worked against `main` at `7ca9144fa` and ran experiments on the dev node in their own keyspaces. A fourth reviewed their work and found 8 major and 12 minor points, all folded into the plan.
        - **The plan.** The orchestrator wrote the 74 decisions into my Step 5 plan, with a recommendation for each. On 2026-11-16 it re-checked the plan against `b4b6d6ddd`: no decision changed, only line numbers.
        - **The six choices.** For each, it recommended the option I picked.
        - **This entry.** It drafted it.
    - **What the human changed:**
        - **IW9.** I asked why throughput mattered for IW9. The 9.7k-against-6.2k writes/s figure timed the index step alone, on one node, with no target behind it. The plan had led with it, and I had the decision reworded to lead with `count`, which is the real reason.
        - **SW12.** I asked that the plan and this entry say why the sweeper does not use a serial read for every intent.
        - **The other 68** I approved as recommended.
    - **What was verified, and how:**
        - **On the dev node (Cassandra 5.0.9, single node), by the research workers:**
            - the date-key tie (delete wins, 0 rows);
            - the serial read's Paxos promise in a trace;
            - quorum against serial read latency;
            - the batch size the node compares with `batch_size_warn_threshold`.
        - **On the repository's fixtures and 10k Synthea:**
            - 0 extraction failures;
            - the row counts behind IW2 and IW3;
            - the size of `index_keys`.
        - **By reading the code:** cdrs-tokio 10's batch consistency default of `ONE`, read in its source.
        - **Not yet measured:** the three-node figures. Steps 5 and 8 measure them.
- **Links:**
    - **Roadmap and design, edited in the same commit as this entry:** roadmap Step 5 (rows 5.1 and 5.2, the sweeper task and the handover's ordering rule); `cassandra-discussion.md` (index rows, steps 1, 5 and 6, the sweeper and repair on read); walkthrough sections 1, 2 and 6 and the worked example's crash cases; `coverage.md`. The reload note goes in `crates/persistence/README.md` with pull request 5, which switches extraction on.
    - **ADRs:** ADR-aacruzgon-0013 (the sweeper), 0017 (repair), 0027 (G22, P5, the lease), 0058 and 0066 (the CQL audit).
    - **Issues and pull requests:** issue #77; Sara's PR #83.
    - **My plan:** the "Step 5 plan" tab of my Cassandra Write Path Plan (a Claude Doc).
    - **This entry's pull request:** #86.

## The 74 decisions

**Build order.** Eight pull requests, one roadmap concern each:

1. This entry and the doc edits.
2. Row identity and the shared writer, with no behaviour change (IW7, IW9, IW17, IW24).
3. Batches in the session (IW10, IW11, IW13, IW14, IW20).
4. Crash checkpoints and the consistency check (RV10 to RV16).
5. Extraction switched on, item 5.1.
6. The sweeper, item 5.2.
7. Repair, item 5.3.
8. The crash tests and the required post-suite step, item 5.4 and Step 5's exit.

**3. Index writes (5.1): architecture**

| ID | Decision | What is built |
| --- | --- | --- |
| IW1 | Extraction, and what a failing parameter does | Run the shared extractor's `extract` on the normalized body with the tenant's registry. A parameter that fails is skipped, logged at warn and counted (`extraction_failures`); the write goes ahead. |
| IW2 + RW17 | Token and URI mapping, with the P5 drops | `TokenCode` and `TokenSystemCode` rows for every non-empty code; a `TokenSystem` row only for a real system (not IMPLICIT, not `''`); booleans get system `''`. Drop empty codes and empty URIs. |
| IW3 | Drop composite component values (a new drop) | Drop every extracted value that belongs to a composite parameter. |
| IW5 | A version's key set is sorted and deduplicated | `BTreeSet<IndexKey>`, encoded in order; the `resources_by_type` key is always present for a live version. |
| IW7 = RV2 | One row-identity rule: the date-key fix | One function in `index_keys.rs` decides whether two index rows are the same row, by each table's primary key (date rows without `last_updated`). The writer, the sweeper, repair and the 5.4 check all call it. |
| IW8 | Where extraction runs | One helper right after `normalized_body`, called at the four sites that hard-code the key set today (inside `update_attempt`); re-extract on every `create_or_update` turn. No signature changes. |
| IW9 + RW1 | Order of a write's deletes and inserts | B, by partition: every row of one partition key, inserts and deletes, travels in one unit (one batch, applied whole on each replica); all units go at once; the intent is deleted last. Chosen so `count` never sees an updated resource twice (Decision, 1). |
| IW10 | Logged batches | Logged multi-partition batches, as the design says. If the three-node benchmark shows the batchlog costing over 20% of throughput, switch to unlogged batches of at most 10 partitions; the packing code stays the same. |
| IW12 | Concurrent batches run inside the request's task | Futures with a bound of 8, never `tokio::spawn`. |
| IW15 | What each batch failure means | A `BATCH` write timeout: no retry, keep the intent. `BATCH_LOG`, `SIMPLE` and transient errors: retry. `Invalid`: no retry, an error. Any batch not confirmed: the write still answers success with its intent kept (G23). |
| IW24 + RW20 | Version row before index rows | Version row first: step 4 is acknowledged before step 5 is sent, and step 5 is skipped when the version row failed all its tries; the intent stays for the sweeper (Decision, 2). |
| IW19 + RW16 | The registry generation counter (overlay limitation 1) | A per-tenant generation number in `TenantSearchRegistries`; a registry build is cached only if the generation did not change while it was built. `search/tenant_registries.rs` is upstream's shared file, owned for Step 5 by role 5s, which is mine; filing the counter upstream is a separate call. |
| IW20 = RV20 | The CI audit rule for failed batches stays strict | Keep rule (d), which fails the job on any failed native batch, and add a source scan proving the batch API accepts only index-table statements. A genuine batch failure in CI: re-run once and record it. |
| RW6 | Data written before 5.1 | Drop and reload every keyspace written before 5.1, once (Decision, 5). |
| IW23 | The latency target for 5.1 | Expect +0.2 to +0.6 ms p50 per create, update or revive on the single node; compare the latency report before and after. A report, not a gate. |

**4. The sweeper (5.2): architecture**

| ID | Decision | What is built |
| --- | --- | --- |
| SW1 | Who starts the sweeper | `start_cassandra` in `crates/hfs/src/main.rs` (Alan's file) spawns it inside the server's shutdown-managed worker pool when it is enabled. `connect` never starts one. |
| SW3 | How it stops and how errors show | Cancelled between intents on shutdown. It never exits on an error: a failing intent goes to the retry list, a failing poll keeps its mark. Readiness is not affected. |
| SW5 | Keep the outbox exactly as on `main` | UTC-hour bucket, FNV-1a shard mod 16, v1 timeuuid, as I1 writes them today; the sweeper reuses `bucket_of`, never a copy. |
| SW6 = IW18 | Stamp the intent just before I1 | Take `enqueued_at` after V3 and immediately before I1 (today it is taken before V3). |
| SW8 | Which partitions a poll reads | Only the hour buckets overlapping `[mark, now − grace)`: 16 partitions normally, 32 for about 35 s after each hour turns. A mark moves only once its whole slice is done. |
| SW9 | The startup scan runs in the background | After a restart, scan the last 24 h and 1 min in one-minute slices alongside the normal loop; never hold back HTTP. |
| SW11 + RW10 | What happens to one intent for version V | Read the resource row. Below V: the CAS never applied, delete the intent. V current: write V's version row, insert K(V), delete K(V−1) minus K(V). V superseded: delete K(V−1) only. All at `write_ts_of(last_updated)`, checked equal to the stored `write_ts`. Never compare `write_id`. |
| SW12 + RW4 | Consistency of the sweeper's row read | Quorum first: a `LOCAL_QUORUM` read; only no row or a version below V falls back to `LOCAL_SERIAL` (Decision, 4). |
| RW4 | A limit on how hard the sweeper loads the node | One semaphore of 32 in-flight statements for the whole sweeper, with the startup scan ranked below the normal loop. |
| SW13 | A missing version row or an unreadable key set is a bug, never an empty set | Log at error, keep the intent, try 3 times, count it. |
| SW14 = IW17 | The sweeper and repair reuse the request's writers | The version-row writer and the index writer take plain inputs (keys, old rows, timestamp), so the request, the sweeper and repair all call the same code. |
| SW15 | Order inside a replay | IW9's rule (B, by partition), through the shared writer; the intent is deleted only after every part succeeded. |
| SW16 | Delete markers and revives | A replayed delete writes the marker version row with no `data` and deletes all of K(V−1); a replayed revive writes the version row and inserts only. |
| SW18 + RW3 | The retry list never drops an intent | Back off from 5 s to 5 min; give up only at `enqueued_at` + 24 h. When the list is full, stop advancing the marks of the partitions it could not queue, and log at error. |
| SW19 | SearchParameter replays update the overlay | After replaying a SearchParameter, call `follow_stored_search_parameter` with the row it read. |
| SW20 | The sweeper is a system actor | No `TenantContext` and no permission check; every statement uses the intent's own `tenant_id`. |
| SW21 | The backlog on first deploy | Process every leftover intent the same way, `test-` tenants included; a tenant that no longer exists has no row, so its intent is just deleted. |
| SW22 | Several Helios processes (G12) | Accept duplicate work; every sweeper write is idempotent at a fixed timestamp. k processes cost k × 3.2 queries/s and k startup scans. |
| RW8 | Replays keep the stored FHIR version text | Carry the stored `fhir_version` string in `ResourceRow` and write it back verbatim in every replay and repair. |
| SW28 + RW15 | A public one-pass sweep for tests | `sweep_pass(SweepOptions { scope, grace, now, since })`, public for Sara's integration tests but `#[doc(hidden)]`; a grace below the configured one is refused unless the options say `allow_short_grace`. |
| SW23 | The `hfs` servers CI launches run with the sweeper off | `HFS_CASSANDRA_SWEEPER_ENABLED=false` in those launches (both workers now agree). A REST test that expects healed rows runs one `sweep_pass` itself. |

**5. Repair (5.3), the consistency check (5.4) and crash tests: architecture**

| ID | Decision | What is built |
| --- | --- | --- |
| RV1 + RW2 | What a repair writes | Re-run step 5 for the current version through the shared writer (insert all of K(current), delete the stale rows found), as ADR-aacruzgon-0017 says. For a resource with more than about 64 keys, repair only the rows found (delete those the current version does not produce, re-tag the date rows it does) and leave the full replay to the sweeper. |
| RV3 | When repair acts, and what it never touches | Only when a row's tag is below the current version. A tag ahead, no row, or a `write_ts` that does not match: skip and log. Repair never writes `resources`, `resource_versions` or intents. |
| RV4 | No second, serial read before repairing | Trust the tag check's `LOCAL_QUORUM` read of the row (Decision, 6). |
| RV5 + RW2 | Repair runs inline, with a budget | The search queues repairs for its page and runs them at the end, on success and on `TooManyResults`: at most 32 resources and 512 rows per search, 4 at a time. A failed repair never fails the search. |
| RV10 | What the 5.4 check means | Clause 1: every live resource's index rows equal its stored K(current), and also a fresh extraction when the tenant's registry has not changed. Clause 2, the version rows, becomes four checks: re-extraction gives the stored set; the current version's blob equals `resources.index_keys` byte for byte; no live row carries an old tag; each row's write time equals its version's timestamp. |
| RV11 | The check lives inside the crate, tests only | A `cfg(test)` module `consistency.rs` in the backend, reached by `--lib` tests. |
| RV12 | Scanning without `ALLOW FILTERING` | Read only the partitions named by every version's stored keys, plus `resources_by_type`; find a tenant's resources with `DISTINCT` on `resources`, never on `resources_by_type`. |
| RV14 | Tests that change the registry | A test declares the registry `Fixed` or `Changed`; `Changed` skips only the fresh-extraction comparison. The post-suite step detects tenants with stored SearchParameters itself. |
| RV15 | How a test simulates "Helios crashed here" | `cfg(test)` checkpoints in the write path (after the intent, the CAS, the version row, the index rows) hold the request; `abort()` is the crash, release is a pause. The fault seam's existing 504 recipes stay for issue #77. |
| RV17 | Which failure tests are Step 5's | Step 5 owns in-process versions of the eight section 6 rows that kill or pause Helios. Row 8.1 keeps the `kill -9` scripts, search visibility, `If-Match`, restart, node kill and tombstone rows; 8.3 the cluster rows. |
| RV19 | Issue #77 in Step 5's tests | Strict on the single node: after one scoped sweep, every test expects the full history, the right `count`, no intent left and a consistent index. Sara owns the race suites' check on three nodes (with a grace of at least 2 s there, SW28). |
| RV22 + RW7 | The post-suite check in CI | Report first: after every suite, sweep every `test-` tenant at grace 0, run the 5.4 check and R4b; report-only from PR 6, failing the job from PR 8 after three clean runs (Decision, 7). |
| RV25 | No search suite runs at 5.1 | Search stays 501 until Step 6, so the Step 3 suites are Step 5's regression gate and every search suite waits for Step 6. |

**Index writes (5.1)**

| ID | Decision | What is built |
| --- | --- | --- |
| IW4 | Date rows | `indexed_range(value, Millis)` gives one `Date` and one `DateByEnd` row in milliseconds. Open ends use sentinels, never null: start −62135596800000 (year 1), end 253402300799999 (year 9999). An unparseable date is dropped, as in the other backends. |
| IW6 | Size bounds checked before anything is written | Each value at most 2,048 bytes and each parameter name at most 128 bytes (G35), checked after extraction and before V3 and I1. Over the bound: 400 `InvalidResource`, one detail per value, naming the parameter's expression. |
| IW11 | Batch size and packing | Count every wire byte of a batch's statements, at most 4,864 per batch (under the 5 KiB warning). A unit (one partition's rows) is never split; a unit too big on its own goes alone. |
| IW13 | One batch call on the session | `CassandraSession::batch`, with the prepared-statement cache, the counters, the fault seam and the deadline; always logged and always `LOCAL_QUORUM`; a single statement goes as a plain execute. |
| IW14 + RW5 | How statements are counted | Separate fields: `StatementCounts` gains `batch` and `batched`; `write` stays single statements; `total()` counts round trips (Decision, 8). |
| IW16 | Retries for index batches | 3 tries per batch (10 ms, then 20 ms pauses), inside an index-phase deadline of `request_timeout_ms`; whatever is left then goes to the sweeper (G23). |
| IW17 + RW9 + RW10 | The shared writer's signature | `index_delta(previous: &[(IndexKey, last_updated)], next)`, `write_index_delta(key, delta, ts, tries)` and `version_row(..)`, all on plain inputs. Tries: 3 for the request, 1 per pass for the sweeper, 1 for repair. Every caller computes `ts = write_ts_of(last_updated)`. An `Invalid` error is permanent. |
| IW21 | `index_keys` in the CAS | Accept it and report `cas_payload_bytes`; compress only if Step 8 shows a need. |
| IW22 | Extraction runs inline | On the request's own task, with `extract_us` added to the latency report. |

**The sweeper (5.2)**

| ID | Decision | What is built |
| --- | --- | --- |
| SW2 | The sweeper's API | `run_sweeper(Arc<Self>, CancellationToken)`, a thin loop over `&self` steps (`poll_once`, `startup_scan`, `process_intent`) that take an explicit `now` and a scope. |
| SW4 + RW11 | Settings and validation | `HFS_CASSANDRA_SWEEPER_ENABLED` (true), `HFS_CASSANDRA_SWEEPER_POLL_INTERVAL_MS` (5000), `HFS_CASSANDRA_SWEEPER_GRACE_MS` (30000) in `CassandraConfig`. Refuse a grace under 2 × `request_timeout_ms`, of 1 h or more, or shorter than the poll interval, and a poll interval of 0. Everything else is a constant. |
| SW7 | The outbox read (I3) | `enqueued_at >= minTimeuuid(?) AND enqueued_at < minTimeuuid(?)` with millisecond bounds, `LOCAL_QUORUM`, page size 1,000, no `LIMIT`. |
| SW10 | One-minute startup slices | Fixed at one minute; count the server's tombstone warnings in the stats in case slices must adapt later. |
| SW17 | Concurrency inside a poll | 16 slices and 8 intents per slice at once, all under the 32-statement limit (RW4). |
| SW24 | Where the sweeper's tests live | Mechanics tests in `sweeper.rs`, scoped to their tenant, at a fake `now`, with old intents written through `IntentKey::new`; the crash rows in `crash_tests.rs` (RV18). |
| SW25 + RW18 + RW19 | Counters and logs an operator can read | Sweeper counters in the style of the CAS counters: polls, errors, intents by case, retries, overflow, invariant violations, SearchParameters followed, startup timings, plus `lag_ms`, `retry_len` and `replays_noop`. One `info` summary per minute when there is work; one `warn` when lag exceeds 2 × (poll + grace); per-intent failures summed per poll. `/metrics` export waits for Step 8. |
| SW26 | Proving the 10-minute tombstone criterion | Record the hand run (59.8 average, 86 maximum), re-run it through the backend on the CI node, and add the outbox's `tablestats` to the post-suite step rather than a separate load step. |

**Repair and the check (5.3, 5.4)**

| ID | Decision | What is built |
| --- | --- | --- |
| RV6 | Repair's file and signature | `repair.rs`: `repair_index(key, &ResourceRow, &[StaleIndexRow]) -> RepairOutcome`, plus `RepairContext` for the per-search budget; `#[allow(dead_code)]` until Step 6 calls it, since CI's clippy rejects an unused item. |
| RV7 | How repair writes | Through the shared writer at `LOCAL_QUORUM`, with the stale rows as `previous` (each with its own `last_updated`), at the current version's timestamp, one try. |
| RV8 | Repair's limits are constants | `MAX_REPAIRS_PER_SEARCH = 32`, `MAX_REPAIR_ROWS_PER_SEARCH = 512`, `REPAIR_CONCURRENCY = 4`. They become settings only if Step 8 measures a need. |
| RV9 | Repair's logs and counters | `info` per repair, `error` per skip, `RepairStats` beside the CAS stats, `[repair …]` lines for the CI report. No AuditEvent. |
| RV13 | The check's API | `index_consistency(backend, tenant, scope, scan_mode, registry) -> ConsistencyReport`, one `[index-consistency …]` line, and `assert_consistent`. It uses the writer's own extraction, and a golden test pins the walkthrough's worked example. |
| RV16 | The checkpoints | `Checkpoint { AfterIntent, AfterCas, AfterVersionRow, AfterIndex }`, keyed by tenant, id and version; `Hold { reached, release }`. |
| RV18 + RW12 | Test files | `repair.rs` tests, `consistency.rs` and `crash_tests.rs`, all in the existing required `--lib` step, with tenants `test-crash-…` and `test-repair-…`. Any test that uses the fault seam or a checkpoint lives in `--lib`. |
| RV21 | CI changes | No new build or job. The six `--exact` test names stay unchanged. The tombstone report adds the index tables, the report gains repair and consistency sections, and the audit parse is timed against its 5-minute timeout. The leftover-intent report (R4) moves to just before the post-suite step (RW7). |
