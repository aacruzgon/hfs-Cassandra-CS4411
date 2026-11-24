# ADR-sam-michaels-0006: Step 5's design: index rows by partition, the version row first, a quorum-first sweeper, bounded repair and a report-first CI check

- **Date:** 2026-11-16
- **Status:** accepted; amended on 2026-11-17 and 2026-11-18 after Alan's two reviews on #86 (see "Amendments after review")
- **Context:**
    - I own roadmap Step 5, rows 5.1 to 5.4: index rows on every write, the sweeper over `pending_index_writes`, repair on read, and the index consistency check. Alan built Steps 2 and 3, so all six steps of the write path already run on `main` (`b4b6d6ddd`). Step 5 of the path writes only the `resources_by_type` row, because the key set is hard-coded to `[IndexKey::ResourcesByType]` at `write.rs:1936`, `update.rs:206` and `upsert.rs:205` and `:216`. Sara owns Step 4, which does not block this.
    - My plan of 2026-11-05 was written before Alan's Steps 2 and 3 and is wrong on several facts. The biggest: every old key set is now read from a stored `index_keys` column and never re-extracted (G22, G40, ADR-aacruzgon-0027). Step 5 had to be planned again against the real code.
    - The design documents leave several things open or get them wrong for the code as built:
        - **Order.** The order of a write's index deletes and inserts. The walkthrough's batch template sends one logged batch; the Step 3 handover says every insert first.
        - **The tie bug.** Today's index diff compares `(key, last_updated)` pairs, but the two date tables have no `last_updated` in their primary key. Once date keys exist, an update that keeps a date deletes that row at the same timestamp it re-inserts it, and Cassandra lets the delete win the tie (0 rows left, verified on the dev node).
        - **The sweeper's read.** The walkthrough (`:705`, `:794`) and the design (`cassandra-discussion.md:347`) make every sweeper read `LOCAL_SERIAL`. On three nodes a serial read can push a live write into issue #77's 504.
        - **Old data.** Nothing heals a resource written before 5.1 and never written again.
    - The plan has 74 decisions: 49 on architecture and 25 on implementation. Six of them were real choices between workable designs, and the other 68 were recommendations. I went through the six one at a time on 2026-11-16 and approved the 68 as written. The table at the end lists all 74 by the plan's IDs, which code comments cite as "ADR-sam-michaels-0006, IW7" and so on.
    - **Evidence.** The scripts and outputs behind the figures below are in `0006-evidence/` (its `README.md` says what each file is and how to run it). All runs were on the single-node dev node on 2026-11-12 and 13.
        - **Writes per second** (`throughput-out.txt`: 32 writers, 16 rows per write, two passes, forward then reverse): 9.7k and 7.1k is "logged one batch ins+del" (9,682 and 7,149), which was one 5,248-byte batch, above IW11's 4,864-byte bound, so B as packed would send that write as two batches (Alan, #86); 6.2k and 4.2k is "logged ins+del (A: 2 batches)" (6,170 and 4,152); 5.4k and 3.5k is "sequential" (5,422 and 3,472). **6.2k is IW9's A against B** (9.7k); **5.4k is IW10's plain statements against batches.**
        - **Latency** (`latency-out.txt`, p50, 300 samples per scenario): 0.25 ms is B's one batch against A's two at 16 rows per write (0.29 ms against 0.54 ms). 0.12 ms is one statement's round trip ("sequential" at 1 row per write), the cost IW24 puts in a separate step.
        - **Rows** (`iwbench-out.txt` and `iwbench-opt.txt`, 10k Synthea, 9,982 Observations and 18 Patients): 104,820 composite values dropped ("composite 104820"). The +53% is relative to the 197,433 rows that remain without them (the "rows per table" lines added up), so keeping them would add 53% more rows and tombstones. "About 5%" is the 10,018 `TokenSystem` rows dropped for an implicit or empty system ("ts-index implicit/empty 10018"), 4.8% of the 207,451 rows there would be without that drop. Both percentages are derived from those lines.
        - **Measured once, output not kept:** quorum 0.30 ms against serial 0.48 ms per read (SW12); and the sweeper tombstone run, 59.8 average and 86 maximum (SW26), done by hand with `sw/exp.py`. SW26 re-runs the second through the backend in CI.
        - **Chosen budgets, not measurements:** RV1's 64 keys, RV5's 32 resources, 512 rows and 4 at a time, and IW16's 10 and 20 ms pauses (the pauses reuse ADR-aacruzgon-0034's).
- **Decision:** Step 5 is built as the table below says, in eight pull requests (build order at the end). These decisions change or extend the design documents, or were real choices:
    1. **The order of a write's index rows: by partition (IW9, B).**
        - Every row of one partition key travels in one unit, its inserts and deletes together, and the units go out at once. A unit is one batch, and Cassandra applies a batch's rows for one partition whole on each replica. The intent is deleted last.
        - **Why:** `count` is `COUNT(*)` over the `(tenant, type)` partition of `resources_by_type` (`read.rs:53`), with no version-tag check. An update moves the resource's row in that partition from `last_updated` N to N+1, so under B no reader sees both rows once the batch has reached a quorum, and that update alone cannot make `count` count the resource twice.
        - **What it does not promise.** `count` is off by one either way, twice or not at all, in these cases:
            - **Twice, after a write whose index step was skipped or did not finish,** until the sweeper or repair finishes it (G23). This happens on one node too: Decision 2's case, an update whose version row fails and a later update on top, and any write whose index batches fail with the intent kept (Alan, #86; #89 reproduced it with the fault seam).
            - **Twice, on RF 3, after a write that reached fewer than a quorum.** The default `read_repair = 'BLOCKING'` repairs only the rows a read covered (Cassandra 5.0 docs, read_repair.html, "Read Repair Options"; `BlockingReadRepair.java:45-49`), so a read of N+1 alone can repair it onto a replica that still holds N, and a quorum `COUNT(*)` then returns 2 (Alan reproduced this on three 5.0.9 nodes).
            - **Not at all, through read repair.** On three 5.0.9 nodes Alan wrote row N, applied the update batch on node 1 only, and ran a QUORUM slice covering only N on nodes 1 and 2. That repaired N's tombstone onto node 2, and a QUORUM `COUNT(*)` on nodes 2 and 3 then returned 0 for a live resource; with `read_repair = 'NONE'` it returned 1 (Alan, #86; Alan's `read_repair` entry, to follow).
            - **Paging.** `count`'s internal paging (10,000 rows per page, `SelectStatement.java:338`, `:899-902`) can miss a resource updated between pages, under A or B. `resources_by_type` is `last_updated DESC`, so a paged count can only miss, never double.
        - The same rule applies to the request, the sweeper and repair, through one shared writer (IW17, SW14).
        - Speed is secondary. B saves one round trip on the index step, about 0.25 ms p50 per update on the dev node (`latency-out.txt`), and there is no throughput target behind it.
    2. **The version row before the index rows, and no index rows without it (IW24).**
        - Step 4's version row is acknowledged before step 5 is sent, so anything search finds can be opened with vread and `_history`.
        - New: when the version row fails all its tries, step 5 is skipped, where today it runs anyway (`write.rs:1703-1707`). The intent stays, and the sweeper writes both.
        - **The skip lands with the sweeper, in pull request 6, not in pull request 2** (decided 2026-11-17). Without a sweeper it does more harm than good. Take an update to N+1 whose version row fails, and a later update to N+2. N+2's index step takes N+1's keys as the rows to delete, so it never deletes N's `resources_by_type` row, and `count` shows the resource twice until the sweeper (whose superseded case deletes N's keys) or repair runs. Without the skip, the index moves to N+1, N+2 cleans up normally, and only vread of N+1 answers 404 until N+2 rewrites that row. Vread of the failed version answers 404 either way, since it reads only `resource_versions`.
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
    5. **Data written before 5.1 is dropped and reloaded, once (RW6).** Every keyspace written before 5.1 is dropped and reloaded: the dev node's `helios` and any teammate's or benchmark dataset. CI starts fresh containers. `crates/persistence/README.md` and the 5.1 pull request say so, so Alan and Sara reload theirs. Alan accepted this on #86 on 2026-11-17: he reloads the dev node and the benchmark datasets when Step 6's search lands, or before his next benchmark run, and the README note still says every keyspace written before 5.1 needs the reload.
    6. **Repair (RV1, RV3, RV4, RV5).**
        - **What it writes:** repair re-runs step 5 for the current version through the shared writer, as ADR-aacruzgon-0017 says. A resource with more than about 64 keys is the exception: there, repair fixes only the rows the search found and leaves the full replay to the sweeper.
        - **When it acts:** only on a row whose tag is below the current version. It never writes `resources`, `resource_versions` or intents.
        - **No serial read first (RV4):** the tag check's `LOCAL_QUORUM` read is enough. A current version seen at quorum is committed, which is the same argument as in point 4.
        - **Limits:** repair runs at the end of the search's page, at most 32 resources and 512 rows per search, 4 at a time. A failed repair never fails the search.
    7. **The post-suite check in CI is report-first (RV22).**
        - **What it runs:** after every suite, it sweeps every `test-` tenant at grace 0, runs the consistency check over them, and reports the intents left after the sweep (R4b). It runs as a CI step after the test process has exited, inside the existing "Cassandra schema (single node)" job, which is not renamed (the `main` ruleset requires that check by name, ADR-aacruzgon-0019, 0021).
        - **When it becomes required:** it only reports from the sweeper pull request (6). The pull request that closes Step 5 (8) makes it fail the job, after three clean runs in a row.
        - **Why:** "Cassandra schema (single node)" is required on `main`, so an unsettled step would block Sara's and Alan's merges.
    8. **Batches are counted separately (IW14).**
        - `StatementCounts` gains `batch` (batch requests) and `batched` (statements inside them). `write` stays single statements, and `total()` counts round trips.
        - So the update test reads `read=1 serial_read=0 write=4 batch=1 batched=2 cas=1`, and the CI report's write-amplification table shows both requests and rows.
    9. **Two index rows the design keeps are now dropped (IW2, RW17, IW3).**
        - **P5 (ADR-aacruzgon-0027) goes further.** No `TokenSystem` row for an implicit or empty system, and no row for an empty code or an empty URI. Only invalid content makes a row with a system and an empty code (`"code": ""` in a CodeableConcept's coding, `converters.rs:470`). SQLite and PostgreSQL would match it with `system|` (`token.rs:101`); dropping it is still fine. The drops save about 5% of rows on Synthea (`iwbench-out.txt`, see Evidence).
        - **New: the parts of composite parameters are not indexed.** Cassandra serves no composite search (`coverage.md` C7), and keeping them adds 104,820 unread rows on 10k Synthea, +53% rows and tombstones (`iwbench-out.txt`, `iwbench-opt.txt`; see Evidence).
    10. **What a client sees from extraction (IW1, IW6).**
        - **A parameter that fails to extract:** that parameter is skipped, logged at warn and counted, and the write still succeeds. Refusing it would let one bad stored SearchParameter block every write of its type. The per-parameter skip is in the shared extractor (`search/extractor.rs:502-515`), which also records `extraction.failures` for the counter.
        - **A resource whose whole extraction fails:** the write goes ahead with only the `resources_by_type` row, logged at warn and counted, as SQLite falls back to `_id` and `_lastUpdated` (`sqlite/storage.rs:2073-2083`).
        - **A value too long:** nothing is refused for its length. G35 as amended by ADR-aacruzgon-0079: a token system or code, a uri or a parameter name over 1,024 bytes, or one that starts with `sha256:`, is stored as `sha256:` plus its SHA-256 in lowercase hex (`index_text`), and a search maps its query values the same way. It is applied exactly once (ADR-aacruzgon-0079): `index_text` goes on K(N+1) at step 1, on RV10's fresh extraction and on parsed query values, and never on K(N), SW11's replay or RV1's stored keys, since mapping a digest again hashes it again.
- **Alternatives considered:**
    - **IW9:**
        - **A, every insert acknowledged before any delete** (the Step 3 handover's rule, and today's code): lost. `count` and a search's `_total` count an updated resource twice until the delete lands. That is one round trip normally, and up to the sweeper's 35 s if Helios crashes between the two.
        - **B+, B with the `resources_by_type` pair and changed-value deletes held until every insert is acknowledged:** lost. It keeps a repair trigger at the cost of A's second round trip. The trigger it keeps matters only if an intent is lost, and SW18 never drops one.
    - **IW24, steps 4 and 5 at once** (the index-writes research's proposal): lost. It saves 0.12 ms p50 per write (`latency-out.txt`, see Evidence). But after a crash, search could show N+1 while vread and `_history` answer 404 until the sweeper runs, and the crash tests' `AfterVersionRow` checkpoint would need a redesign.
    - **SW12, a serial read for every intent** (the design as signed off, and the sweeper research's proposal): lost, for the reason under Decision 4. Measured on the dev node: quorum 0.30 ms, serial 0.48 ms per read (measured once, output not kept). That timing was not the reason.
    - **RW6, leaving old data:** lost. Search silently misses each such resource until its next write.
    - **RW6, a reindex:** lost. It is a compare-and-swap per resource, because every index row must appear in the stored `index_keys` (G22). That is a feature of its own, outside Step 5.
    - **RV22, a hard check from the first day:** lost. A gap in the list of tests that plant broken states, or a flaky case, would block every pull request until fixed.
    - **IW14, a batch counted as one `write`:** lost. It keeps today's four-number pins, but a reader cannot tell which writes carried several rows.
    - **SW22, a lease row so that one process sweeps:** lost. It needs a compare-and-swap per poll, the #77 path again, and it stalls on a crashed holder (ADR-aacruzgon-0027).
    - **IW10, plain statements instead of batches:** lost. They are correct and need no batch API, but they lose IW9's per-partition unit, so `count` could see both rows of an update again, and the index step alone ran at about half the rate on the dev node (5.4k against 9.7k writes/s, `throughput-out.txt`).
    - **IW20, demoting the CI audit's failed-batch rule (d) to a warning:** lost. It would amend ADR-aacruzgon-0058 and 0066 and trust the code being checked.
- **Consequences:**
    - **An update alone never makes `count` see both rows once its batch has reached a quorum**, and every search hit can be opened by version, including after a crash. `count` is not exact: it is off by one either way, twice or not at all, after a write whose index step did not finish (until the sweeper or repair finishes it) and through read repair on RF 3 (Decision 1). So the failure tests must not assert an exact `count` right after a failed write, and the RF 3 ones must not assert it while replicas disagree.
    - **B gives something up.** If a changed value's new row fails for good and the intent is then lost, no stale row is left to trigger repair. Search then misses the new value until the resource's next write. Never dropping an intent (SW18) is what closes this, so SW18 must not be weakened later.
    - **Reloads.** Every keyspace written before 5.1 needs one. Mine on the dev node needs the schema re-applied anyway.
    - **A CI gap.** Between pull requests 6 and 8, a Step 5 bug can merge and show only as a line in the run's report, so I read that report on each of my pull requests.
    - **Composite search later.** If anyone adds composite search, the composite rows have to come back, and every resource must be rewritten to get them.
    - **Measure-first switches, each kept behind one constant or function:**
        - `read_repair = 'NONE'` on `resources_by_type`, which would remove Decision 1's read-repair cases of a wrong `count` on RF 3, at the cost of monotonic reads on that table. It is a schema change, which Alan records in his log as a measure-first switch for Step 8 (Alan's `read_repair` entry, to follow);
        - unlogged batches of at most 10 partitions, if Step 8 shows the batchlog costing over 20% of throughput (IW10);
        - the node's own batch-size formula instead of counting every byte (IW11);
        - repair's limits as settings (RV8);
        - compressing `index_keys` in the compare-and-swap (IW21).
    - **Cross-team.** Alan accepted all six on #86 on 2026-11-17, with these conditions:
        - **Alan, SW1 and SW23:** accepted. `main.rs` starts the sweeper (the `start_cassandra` edit lands in PR 6, and Alan reviews it as that file's owner), and the milestone launches set `HFS_CASSANDRA_SWEEPER_ENABLED=false`. A REST test running its own `sweep_pass` is fine.
        - **Alan, RV22:** accepted: report-only from PR 6, failing the job from PR 8 after three clean runs, inside the existing "Cassandra schema (single node)" job, not renamed (the `main` ruleset requires it by name, ADR-aacruzgon-0019, 0021).
        - **Alan, IW20:** accepted as written: rule (d) stays strict, with the added source scan, and a genuine batch failure in CI gets one re-run that is recorded.
        - **Alan, RW6:** accepted: Alan reloads the dev node's keyspace and the benchmark datasets when Step 6's search lands, or before his next benchmark run (search answers 501 until then). PR 5's `crates/persistence/README.md` note still says every keyspace written before 5.1 needs the reload.
        - **The team, RV17:** accepted: Step 5 takes the in-process crash tests, row 8.1 keeps the rest, and its owner stays X until the group settles it.
        - **Upstream, IW19:** accepted, as long as the change to upstream's `search/tenant_registries.rs` stays small and isolated. It is listed in roadmap section 8. Filing it upstream is a separate call, later.
        - **Sara:** gets `sweep_pass` for her race suites' "sweep, then require the full history" check, which her PR #83 names as its follow-up. She runs it scoped to her tests' own tenants, after every writer has returned and after asserting that no writer's error was HFS's own deadline, at grace 0 (issue #89; SW28, RV19).
- **AI involvement:**
    - **Tool and model:** Claude Code, Claude Opus 5.5, as an orchestrator with worker sessions.
    - **What the AI proposed:**
        - **The amendments.** Claude Code (Opus) read and summarised Alan's review of #86 and recommended an option for each of D1 to D6. A Sonnet worker wrote the edits, and the evidence files were recovered from earlier session scratchpads.
        - **Research, 2026-11-12 and 13.** Three worker sessions researched index writes, the sweeper, and repair with the check. They worked against `main` at `7ca9144fa` and ran experiments on the dev node in their own keyspaces. A fourth reviewed their work and found 8 major and 12 minor points, all folded into the plan.
        - **The plan.** The orchestrator wrote the 74 decisions into my Step 5 plan, with a recommendation for each. On 2026-11-16 it re-checked the plan against `b4b6d6ddd`: no decision changed, only line numbers.
        - **The six choices.** For each, it recommended the option I picked.
        - **This entry.** It drafted it.
    - **What the human changed:**
        - **IW9.** I asked why throughput mattered for IW9. The 9.7k-against-6.2k writes/s figure timed the index step alone, on one node, with no target behind it. The plan had led with it, and I had the decision reworded to lead with `count`, which is the real reason.
        - **SW12.** I asked that the plan and this entry say why the sweeper does not use a serial read for every intent.
        - **IW24's timing.** The plan put the skip in pull request 2. The worker building it found the `count` orphan above, and I moved the skip to pull request 6.
        - **The other 68** I approved as recommended.
        - **After Alan's second review (2026-11-18).** Claude Code drafted the changes; I chose Alan's fix for point 4 (the race suites assert that no writer's error was HFS's own deadline). The two source cites (`BatchStatement.java:444-445`, `StorageProxy.java:885`) were checked against Cassandra 5.0.9's source.
        - **After Alan's review (2026-11-17).** I chose D1 to D6 (see Amendments), each the option the assistant recommended, after it explained each choice by what a client or test sees.
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
        - **After Alan's review:** cdrs-tokio 10.0.0 takes a batch's consistency from `BatchQueryBuilder` (`cluster/session.rs:536`), and `retry_policy.rs` re-sends an idempotent `BATCH_LOG` timeout once, which is why batches get `FallthroughRetryPolicy`. The throughput and latency numbers matched the recovered output files, except the two noted as measured once.
        - **Not yet measured:** the three-node figures. Steps 5 and 8 measure them.
- **Links:**
    - **Evidence:** `0006-evidence/` (this folder), with its `README.md`.
    - **Review:** Alan's review and follow-ups on #86; ADR-aacruzgon-0079 (G35 amended); Alan's `read_repair` entry, to follow.
    - **Roadmap and design, edited in the same commits as this entry:** roadmap Step 5 (rows 5.1 and 5.2, the sweeper task, the handover's ordering rule, the exit criterion and the `count` wording in the notes), section 6 (the failure table and the `count` rule) and section 8 (the upstream-file list); `cassandra-discussion.md` (index rows, steps 1, 5 and 6, the sweeper and repair on read); walkthrough sections 1, 2 and 6 (the `count` wording and the failure table), G12 and the worked example's crash cases; `coverage.md`. The reload note goes in `crates/persistence/README.md` with pull request 5, which switches extraction on.
    - **ADRs:** ADR-aacruzgon-0013 (the sweeper), 0017 (repair), 0027 (G22, P5, the lease), 0058 and 0066 (the CQL audit).
    - **Issues and pull requests:** issue #77; Sara's PR #83.
    - **My plan:** the "Step 5 plan" tab of my Cassandra Write Path Plan (a Claude Doc).
    - **This entry's pull request:** #86.

## Amendments after review (2026-11-17)

Alan reviewed this entry on #86. Sam chose D1 to D6; the mechanical fixes follow his other points. Alan accepted the six cross-team decisions on #86 on 2026-11-17, with the conditions under Cross-team.

- **D1, IW9's `count` guarantee:** B stays, reworded: never twice once the batch has reached a quorum, transiently possible on RF 3 after a write that reached fewer (Decision 1, IW9). RF 3 failure tests assert no exact `count` after a failed write. `read_repair = 'NONE'` is listed as a measure-first switch.
- **D2, RV12 is a full table scan** of each index table, filtered to `test-` tenants in Rust (RV12, RV14).
- **D3, grace 0** for the race suites' one scoped sweep after every writer returned, replacing "at least 2 s" (SW28, RV19, RV22).
- **D4, evidence committed** in `0006-evidence/` and cited, with unkept outputs marked, and the budgets labelled as chosen (Context, Evidence).
- **D5, IW15 and IW13:** `BATCH_LOG` is a timeout, not a failure; batches use `FallthroughRetryPolicy`; the batch's consistency comes from `BatchQueryBuilder`.
- **D6, IW1:** the per-parameter skip is in the shared extractor, and a whole-resource extraction failure writes only the `resources_by_type` row.
- **IDs:** each ID heads exactly one row (SW11, SW12, SW14, RV5 lose the duplicate).
- **Cross-team:** SW1, SW23, RV22, IW20, RW6, RV17 and IW19 marked accepted, with conditions.
- **IW6 and Decision 10:** the 400 is replaced by ADR-aacruzgon-0079's hashing.
- **IW4, IW2, IW3:** wording fixed (IW3 now says `composite_group`).
- **Leftovers:** the roadmap's sweeper text and walkthrough G12 aligned with SW8, SW9 and SW12.
- **Log rule:** `ADR-sam-michaels.md` says how a later entry supersedes one ID.

**(2026-11-18)** Alan's second review on #86:

- **Point 1, `count` after an unfinished write:** added to "What it does not promise" and IW9 (off after a write whose index step was skipped or did not finish, until the sweeper or repair finishes it, G23); Consequences no longer says "exact".
- **Point 2, off either way:** "off by one either way, twice or not at all", with the read-repair case (Alan's `read_repair` entry, to follow), in Decision 1, IW9, Consequences, walkthrough section 2, and roadmap Step 5 and section 6; the paging caveat stays.
- **Point 3, RV12:** a check inside a test keeps only its own tenant, the post-suite step keeps the `test-` tenants, pages of 1,000 rows.
- **Point 4, SW28 and RV19:** the claim that HFS's deadline is above the node's own timeouts is replaced by the coordinator's queue wait (`native_transport_timeout`, 12 s) and stalls; the race suites assert, before the grace-0 sweep, that no writer's error was HFS's own deadline (#89 makes the same change).
- **Point 5, IW6 and Decision 10:** ADR-aacruzgon-0079's "applied exactly once" added.
- **RV5:** duplicate "(RW2)" head removed.
- **The 9.7k figure:** one 5,248-byte batch, above IW11's 4,864-byte bound.
- **IW23:** "+0.2 to +0.6 ms" labelled as an estimate.
- **The batchlog cite (IW15):** "without the batchlog" is `BatchStatement.java:444-445`; `StorageProxy.java:885` stays for the `SIMPLE` type. Both checked against 5.0.9.
- **Index and Status:** the index row follows the Status line; the Links line lists the roadmap's exit criterion, sections 6 and 8, and the walkthrough's failure table and G12.
- **`read_repair`:** cites Alan's entry on it, whose number follows when he records it (0080 went to the `acruzdebian` runner, #91).

*Update 2026-11-19:* Alan's `read_repair` entry, cited above as "to follow" (Decision 1, Consequences, Links, IW9's row and Amendments point 2) and as "whose number follows" (Amendments, `read_repair`), is [ADR-aacruzgon-0081](../../ADR-aacruzgon/2026-11-18/0081-what-step5-design-changes.md), merged in #97. Its decision 2 corrects `count`'s bound after an unfinished write, which Decision 1, Consequences and IW9's row give as "twice": after an unfinished update or delete, `count` is too high, by up to one per such write; after an unfinished create or revive, it is one too low; "twice or not at all" is only the read-repair case on RF 3, after a write that reached fewer than a quorum.

*Update 2026-11-23:* SW28 and RV19 state the precondition for the race suites' grace-0 sweep too narrowly. Before that sweep, no attempt of any writer, re-runs included, may have ended in a client-side error: HFS's own deadline after the CAS was sent (`HFS_CASSANDRA_REQUEST_TIMEOUT_MS`), or a dropped connection or other driver error after the CAS may have left. Both leave the same unknown, since the coordinator may not have started the CAS yet. The check reads `cas_stats()` on the round's own backend before and after the races, and requires both the `unknown_deadline` and the `unknown_connection` deltas to be 0 (`CasStats::client_side_unknowns`, which the sweeper's pull request adds). Two kinds of error don't block the sweep. An unknown outcome the node itself answered (`WriteTimeout` with `write_type = CAS`, `WriteFailure`, or a server error) is settled by SW12's `LOCAL_SERIAL` read. `Unavailable` proves the CAS never ran. Issue #98 has the detail; #89 already states this precondition.

## The 74 decisions

**Build order.** Eight pull requests, one roadmap concern each:

1. This entry and the doc edits.
2. Row identity and the shared writer, with no behaviour change (IW7, IW9, IW17).
3. Batches in the session (IW10, IW11, IW13, IW14, IW20).
4. Crash checkpoints and the consistency check (RV10 to RV16).
5. Extraction switched on, item 5.1.
6. The sweeper, item 5.2, with IW24's skip.
7. Repair, item 5.3.
8. The crash tests and the required post-suite step, item 5.4 and Step 5's exit.

**3. Index writes (5.1): architecture**

| ID | Decision | What is built |
| --- | --- | --- |
| IW1 | Extraction, and what a failing parameter does | Run the shared extractor's `extract` on the normalized body with the tenant's registry. A parameter that fails is skipped, logged at warn and counted (`extraction_failures`); the write goes ahead. The skip is in the shared extractor (`search/extractor.rs:502-515`), which also records `extraction.failures` for the counter. If extraction fails for the whole resource, the write goes ahead with only the `resources_by_type` row, logged at warn and counted, as SQLite falls back to `_id` and `_lastUpdated` (`sqlite/storage.rs:2073-2083`). |
| IW2 + RW17 | Token and URI mapping, with the P5 drops | `TokenCode` and `TokenSystemCode` rows for every non-empty code; a `TokenSystem` row only for a real system (not IMPLICIT, not `''`); booleans get system `''`. Drop empty codes and empty URIs. Only invalid content makes a row with a system and an empty code (`"code": ""` in a CodeableConcept's coding, `converters.rs:470`); SQLite and PostgreSQL would match it with `system|` (`token.rs:101`), and dropping it is still fine. |
| IW3 | Drop composite component values (a new drop) | Drop every extracted value with `composite_group` set (`extractor.rs:740-760`), so PR 5 cannot drop a composite parameter's own non-component rows such as `code`'s. |
| IW5 | A version's key set is sorted and deduplicated | `BTreeSet<IndexKey>`, encoded in order; the `resources_by_type` key is always present for a live version. |
| IW7 = RV2 | One row-identity rule: the date-key fix | One function in `index_keys.rs` decides whether two index rows are the same row, by each table's primary key (date rows without `last_updated`). The writer, the sweeper, repair and the 5.4 check all call it. |
| IW8 | Where extraction runs | One helper right after `normalized_body`, called at the four sites that hard-code the key set today (inside `update_attempt`); re-extract on every `create_or_update` turn. No signature changes. |
| IW9 + RW1 | Order of a write's deletes and inserts | B, by partition: every row of one partition key, inserts and deletes, travels in one unit (one batch, applied whole on each replica); all units go at once; the intent is deleted last. Chosen so that an update alone never lets `count` see both of a resource's rows once the batch has reached a quorum (Decision, 1). `count` is off by one either way, twice or not at all: twice after a write whose index step was skipped or did not finish, until the sweeper or repair finishes it (G23), and on RF 3 after a write that reached fewer than a quorum; not at all when read repair copies a delete onto a replica that missed the insert, because the default `read_repair = 'BLOCKING'` repairs only the rows a read covered (`BlockingReadRepair.java:45-49`; Alan's `read_repair` entry, to follow). `count`'s internal paging (10,000 rows per page, `SelectStatement.java:338`, `:899-902`) can also miss a resource updated between pages, under A or B. |
| IW10 | Logged batches | Logged multi-partition batches, as the design says. If the three-node benchmark shows the batchlog costing over 20% of throughput, switch to unlogged batches of at most 10 partitions; the packing code stays the same. |
| IW12 | Concurrent batches run inside the request's task | Futures with a bound of 8, never `tokio::spawn`. |
| IW15 | What each batch failure means | A `BATCH` write timeout: no retry, keep the intent. `BATCH_LOG` means the batchlog write timed out, not that it failed: the batch may still be applied by a batchlog replay (Alan saw `WriteTimeout`, `write_type: BATCH_LOG`, `consistency: TWO` on three nodes, then node 2's replay applied it), and retrying is still right. `UNLOGGED_BATCH` (IW10's fallback), `WriteFailure`, a single-partition batch timing out as `SIMPLE` (the node runs a one-partition logged batch without the batchlog, `BatchStatement.java:444-445`, and a one-mutation write has type `SIMPLE`, `StorageProxy.java:885`) and transient errors are all retried. `Invalid`: no retry, an error. Batches use `FallthroughRetryPolicy`, as CAS does, so the driver never re-sends one by itself (`cdrs-tokio-10.0.0/src/retry/retry_policy.rs:157-174`) and each of IW16's 3 tries is exactly one send. Any batch not confirmed: the write still answers success with its intent kept (G23). |
| IW24 + RW20 | Version row before index rows | Version row first: step 4 is acknowledged before step 5 is sent, and step 5 is skipped when the version row failed all its tries; the intent stays for the sweeper (Decision, 2). |
| IW19 + RW16 | The registry generation counter (overlay limitation 1) | A per-tenant generation number in `TenantSearchRegistries`; a registry build is cached only if the generation did not change while it was built. `search/tenant_registries.rs` is upstream's shared file, owned for Step 5 by role 5s, which is mine; filing the counter upstream is a separate call. Accepted by Alan on #86, 2026-11-17, as long as the change stays small and isolated; listed in roadmap section 8. |
| IW20 = RV20 | The CI audit rule for failed batches stays strict | Keep rule (d), which fails the job on any failed native batch, and add a source scan proving the batch API accepts only index-table statements. A genuine batch failure in CI: re-run once and record it. Accepted by Alan on #86, 2026-11-17, as written. |
| RW6 | Data written before 5.1 | Drop and reload every keyspace written before 5.1, once (Decision, 5). Accepted by Alan on #86, 2026-11-17: he reloads when Step 6's search lands or before his next benchmark run, and PR 5's `crates/persistence/README.md` note still says every keyspace written before 5.1 needs the reload. |
| IW23 | The latency target for 5.1 | An estimate, not a measurement: expect +0.2 to +0.6 ms p50 per create, update or revive on the single node; compare the latency report before and after. A report, not a gate. |

**4. The sweeper (5.2): architecture**

| ID | Decision | What is built |
| --- | --- | --- |
| SW1 | Who starts the sweeper | `start_cassandra` in `crates/hfs/src/main.rs` (Alan's file) spawns it inside the server's shutdown-managed worker pool when it is enabled. `connect` never starts one. Accepted by Alan on #86, 2026-11-17: the `start_cassandra` edit lands in PR 6 and he reviews it as that file's owner. |
| SW3 | How it stops and how errors show | Cancelled between intents on shutdown. It never exits on an error: a failing intent goes to the retry list, a failing poll keeps its mark. Readiness is not affected. |
| SW5 | Keep the outbox exactly as on `main` | UTC-hour bucket, FNV-1a shard mod 16, v1 timeuuid, as I1 writes them today; the sweeper reuses `bucket_of`, never a copy. |
| SW6 = IW18 | Stamp the intent just before I1 | Take `enqueued_at` after V3 and immediately before I1 (today it is taken before V3). |
| SW8 | Which partitions a poll reads | Only the hour buckets overlapping `[mark, now − grace)`: 16 partitions normally, 32 for about 35 s after each hour turns. A mark moves only once its whole slice is done. |
| SW9 | The startup scan runs in the background | After a restart, scan the last 24 h and 1 min in one-minute slices alongside the normal loop; never hold back HTTP. |
| SW11 | What happens to one intent for version V | Read the resource row. Below V: the CAS never applied, delete the intent. V current: write V's version row, insert K(V), delete K(V−1) minus K(V). V superseded: delete K(V−1) only. All at `write_ts_of(last_updated)` (RW10), checked equal to the stored `write_ts`. Never compare `write_id`. |
| SW12 | Consistency of the sweeper's row read | Quorum first: a `LOCAL_QUORUM` read; only no row or a version below V falls back to `LOCAL_SERIAL` (Decision, 4). |
| RW4 | A limit on how hard the sweeper loads the node | One semaphore of 32 in-flight statements for the whole sweeper, with the startup scan ranked below the normal loop. |
| SW13 | A missing version row or an unreadable key set is a bug, never an empty set | Log at error, keep the intent, try 3 times, count it. |
| SW14 | The sweeper and repair reuse the request's writers | Reuses IW17's writers: the version-row writer and the index writer take plain inputs (keys, old rows, timestamp), so the request, the sweeper and repair all call the same code. |
| SW15 | Order inside a replay | IW9's rule (B, by partition), through the shared writer; the intent is deleted only after every part succeeded. |
| SW16 | Delete markers and revives | A replayed delete writes the marker version row with no `data` and deletes all of K(V−1); a replayed revive writes the version row and inserts only. |
| SW18 + RW3 | The retry list never drops an intent | Back off from 5 s to 5 min; give up only at `enqueued_at` + 24 h. When the list is full, stop advancing the marks of the partitions it could not queue, and log at error. |
| SW19 | SearchParameter replays update the overlay | After replaying a SearchParameter, call `follow_stored_search_parameter` with the row it read. |
| SW20 | The sweeper is a system actor | No `TenantContext` and no permission check; every statement uses the intent's own `tenant_id`. |
| SW21 | The backlog on first deploy | Process every leftover intent the same way, `test-` tenants included; a tenant that no longer exists has no row, so its intent is just deleted. |
| SW22 | Several Helios processes (G12) | Accept duplicate work; every sweeper write is idempotent at a fixed timestamp. k processes cost k × 3.2 queries/s and k startup scans. |
| RW8 | Replays keep the stored FHIR version text | Carry the stored `fhir_version` string in `ResourceRow` and write it back verbatim in every replay and repair. |
| SW28 + RW15 | A public one-pass sweep for tests | `sweep_pass(SweepOptions { scope, grace, now, since })`, public for Sara's integration tests but `#[doc(hidden)]`; a grace below the configured one is refused unless the options say `allow_short_grace`. Sara's race suites run one `sweep_pass` scoped to the test's own tenants, after every writer has returned, at grace 0 (issue #89), through `allow_short_grace`. The reason is not that HFS's deadline is above the node's own timeouts. The node's CAS and write timeouts start only when it processes a request; before that a request can wait in the coordinator's queue for up to `native_transport_timeout` (12 s by default), and longer if the node process stalls. So HFS's 10 s deadline (`HFS_CASSANDRA_REQUEST_TIMEOUT_MS`, `config.rs:21`) can return a writer before its compare-and-swap has run: on three nodes, with node 1 paused, a CAS sent to it timed out at the client after 10 s, quorum and serial reads through node 2 found no row, and once node 1 resumed the CAS applied (Alan, #86). So the race suites' check first asserts that no writer's error was HFS's own deadline, and only then runs the grace-0 sweep; #89 makes the same change. An unscoped sweep at grace 0, or one while other tests still write, could delete another test's intent. |
| SW23 | The `hfs` servers CI launches run with the sweeper off | `HFS_CASSANDRA_SWEEPER_ENABLED=false` in those launches (both workers now agree). A REST test that expects healed rows runs one `sweep_pass` itself. Accepted by Alan on #86, 2026-11-17, with the `start_cassandra` edit in PR 6 (SW1). |

**5. Repair (5.3), the consistency check (5.4) and crash tests: architecture**

| ID | Decision | What is built |
| --- | --- | --- |
| RV1 + RW2 | What a repair writes | Re-run step 5 for the current version through the shared writer (insert all of K(current), delete the stale rows found), as ADR-aacruzgon-0017 says. For a resource with more than about 64 keys (a chosen budget, not a measurement), repair only the rows found (delete those the current version does not produce, re-tag the date rows it does) and leave the full replay to the sweeper. |
| RV3 | When repair acts, and what it never touches | Only when a row's tag is below the current version. A tag ahead, no row, or a `write_ts` that does not match: skip and log. Repair never writes `resources`, `resource_versions` or intents. |
| RV4 | No second, serial read before repairing | Trust the tag check's `LOCAL_QUORUM` read of the row (Decision, 6). |
| RV5 | Repair runs inline, with a budget | The search queues repairs for its page and runs them at the end, on success and on `TooManyResults`: at most 32 resources and 512 rows per search, 4 at a time (chosen budgets, not measurements). A failed repair never fails the search. |
| RV10 | What the 5.4 check means | Clause 1: every live resource's index rows equal its stored K(current), and also a fresh extraction when the tenant's registry has not changed. Clause 2, the version rows, becomes four checks: re-extraction gives the stored set; the current version's blob equals `resources.index_keys` byte for byte; no live row carries an old tag; each row's write time equals its version's timestamp. |
| RV11 | The check lives inside the crate, tests only | A `cfg(test)` module `consistency.rs` in the backend, reached by `--lib` tests. |
| RV12 | Scanning without `ALLOW FILTERING` | Page through every row of each index table with a plain `SELECT` with no `WHERE` (it needs no `ALLOW FILTERING`; only a partial partition-key restriction does, checked by Alan on 5.0.9), pages of 1,000 rows (a page of 5,000 with update churn logged a tombstone warning on every page; Alan, #86), keep only `test-` tenants in Rust when the post-suite step runs it, but only the test's own tenant when a check runs inside a test while other tests write, and compare with a fresh extraction of every live resource. Still find a tenant's live resources with `DISTINCT` on `resources`, never on `resources_by_type`. Why: reading only the partitions named by stored keys cannot see a row written to a partition no key set names, which is the bug class the check exists for. |
| RV14 | Tests that change the registry | A test declares the registry `Fixed` or `Changed`; `Changed` tests skip only the fresh-extraction comparison; the stray-row scan still runs. The post-suite step detects tenants with stored SearchParameters itself. |
| RV15 | How a test simulates "Helios crashed here" | `cfg(test)` checkpoints in the write path (after the intent, the CAS, the version row, the index rows) hold the request; `abort()` is the crash, release is a pause. The fault seam's existing 504 recipes stay for issue #77. |
| RV17 | Which failure tests are Step 5's | Step 5 owns in-process versions of the eight section 6 rows that kill or pause Helios. Row 8.1 keeps the `kill -9` scripts, search visibility, `If-Match`, restart, node kill and tombstone rows; 8.3 the cluster rows. Accepted by Alan on #86, 2026-11-17; row 8.1's owner stays X until the group settles it. |
| RV19 | Issue #77 in Step 5's tests | Strict on the single node: after one scoped sweep, every test expects the full history, the right `count`, no intent left and a consistent index. Sara owns the race suites' check on three nodes: one `sweep_pass` scoped to the test's own tenants, after every writer has returned, at grace 0 through `allow_short_grace` (SW28, issue #89). Before that sweep the check asserts that no writer's error was HFS's own deadline, because a CAS a stalled node has not yet processed can still apply after the writer returned (SW28); #89 makes the same change. |
| RV22 + RW7 | The post-suite check in CI | Report first: after every suite, sweep every `test-` tenant at grace 0, run the 5.4 check and R4b; report-only from PR 6, failing the job from PR 8 after three clean runs (Decision, 7). The grace-0 sweep of every `test-` tenant runs as a CI step after the test process has exited, never as a test inside the same binary while others run. It stays inside the existing "Cassandra schema (single node)" job without renaming it (the `main` ruleset requires that check by name, ADR-aacruzgon-0019, 0021). Accepted by Alan on #86, 2026-11-17. |
| RV25 | No search suite runs at 5.1 | Search stays 501 until Step 6, so the Step 3 suites are Step 5's regression gate and every search suite waits for Step 6. |

**Index writes (5.1)**

| ID | Decision | What is built |
| --- | --- | --- |
| IW4 | Date rows | `indexed_range(value, Millis)` gives one `Date` and one `DateByEnd` row in milliseconds. Open ends use sentinels, never null: start −62135596800000 (year 1), end 253402300799999 (year 9999). An unparseable date is dropped: PostgreSQL drops it too (`postgres/search/writer.rs:675-690`); SQLite keeps a row with a NULL range (`sqlite/search/writer.rs:50-52`), which only `:missing` could see, and Cassandra refuses `:missing`. |
| IW6 | Long indexed values are hashed, not refused | G35 as amended by ADR-aacruzgon-0079: map each token system, token code, uri and parameter name through `index_text` after extraction and before V3 and I1. Over 1,024 bytes, or starting with `sha256:`, it is stored as `sha256:` plus its SHA-256 in hex. Nothing is refused for its length. Applied exactly once (ADR-aacruzgon-0079): `index_text` goes on K(N+1) at step 1, on RV10's fresh extraction and on parsed query values; it never goes on K(N), SW11's replay or RV1's stored keys, since mapping a digest again hashes it again. |
| IW11 | Batch size and packing | Count every wire byte of a batch's statements, at most 4,864 per batch (under the 5 KiB warning). A unit (one partition's rows) is never split; a unit too big on its own goes alone. |
| IW13 | One batch call on the session | `CassandraSession::batch`, with the prepared-statement cache, the counters, the fault seam and the deadline; always logged and always `LOCAL_QUORUM`; a single statement goes as a plain execute. cdrs-tokio takes a batch's consistency only from `BatchQueryBuilder` (`cluster/session.rs:536`), whose default is `ONE`; `StatementParams`' consistency is ignored for batches. PR 3's test `an_index_batch_is_logged_at_local_quorum_with_no_batch_timestamp` checks the built batch. |
| IW14 + RW5 | How statements are counted | Separate fields: `StatementCounts` gains `batch` and `batched`; `write` stays single statements; `total()` counts round trips (Decision, 8). |
| IW16 | Retries for index batches | 3 tries per batch (10 ms, then 20 ms pauses, chosen budgets that reuse ADR-aacruzgon-0034's), inside an index-phase deadline of `request_timeout_ms`; whatever is left then goes to the sweeper (G23). |
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
| SW26 | Proving the 10-minute tombstone criterion | Record the hand run (59.8 average, 86 maximum; measured once by hand with `sw/exp.py`, output not kept), re-run it through the backend on the CI node, and add the outbox's `tablestats` to the post-suite step rather than a separate load step. |

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
