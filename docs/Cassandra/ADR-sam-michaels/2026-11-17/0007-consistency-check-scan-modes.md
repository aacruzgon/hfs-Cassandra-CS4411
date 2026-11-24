# ADR-sam-michaels-0007: The consistency check's scan modes, and the extraction helper's seat in pull request 4

- **Date:** 2026-11-17
- **Status:** accepted
- **Context:**
  - Pull request 4 of Step 5 builds the consistency check (RV10 to RV16) before extraction exists. Extraction comes in pull request 5.
  - The planner's spec raised two questions that the signed-off wording in ADR-sam-michaels-0006 no longer answered.
  - **The fresh extraction.**
    - RV13 says the check uses "the writer's own extraction". IW8 places that extraction in one helper, called at the four sites that hard-code the key set.
    - In pull request 4 the writer has no helper yet, only four literal `vec![IndexKey::ResourcesByType]`.
  - **What `scan_mode` means.**
    - RV13's `scan_mode` chose between the old RV12's two ways of finding index rows: derived partitions or a full table scan.
    - D2 (ADR-sam-michaels-0006, after Alan's review on #86) made every scan a full scan of each index table. So `scan_mode` no longer chooses anything.
- **Decision:** supersedes ADR-sam-michaels-0006, RV13.
  1. **IW8's helper lands in pull request 4.**
     - `index_keys_of(tenant, resource_type, body)` is added now, and the four production plans call it: create, update and the two upsert branches.
     - Its body stays `[ResourcesByType]`, so no client sees a change. Pull request 5 only replaces the body.
     - IW8's decision itself (one helper, the four call sites, no signature changes) stands. Only its pull request moves.
  2. **RV13's `scan_mode` becomes `Full` or `Shared(&IndexScan)`.**
     - `Full` scans the seven index tables now, for this one tenant.
     - `Shared` reuses one full scan for many tenants: the planted-defects test now, and roadmap 5.4's post-suite step later.
     - Whenever a `Shared` check doesn't panic, both modes report the same defects, as long as four things hold: no write to the tenant was in flight when the shared scan was taken; no index row was written after it without a `resources` write (a hand plant, and later the sweeper or repair); the clock didn't step back between the scan and the check (see Consequences); and no write to the tenant lands while the check runs. A `Full` check reads `resources` before it scans, so a resource created in between can give it a false `stray_row`, while `Shared` is clean. One exception: an undecodable index row in any tenant the shared scan covers. The shared scan fails on it, so every `Shared` check taken from it reports `read_failed`, while a `Full` check is clean unless the row is in its own tenant (for a `Scope::Resources` check, of an id in scope), since `Full` drops other rows before decoding them (see Consequences).
     - A `Shared` check panics in two cases: the scan didn't keep its tenant, or a resource it checks was written at or after the scan was taken. The second assumes the clock didn't step back between the scan and the check (see Consequences).
  3. **RV13's golden test of the walkthrough's worked example moves to pull request 5.** It needs real extraction.

  The rest of RV13 stands: the signature `index_consistency(backend, tenant, scope, scan_mode, registry) -> ConsistencyReport`, one `[index-consistency …]` line, and `assert_consistent`.
- **Alternatives considered:**
  - Fresh extraction:
    - (B) Add the helper, but only the check calls it until pull request 5. Lost: until then, the check would test a function the writer doesn't use.
    - (C) The check keeps a one-line stand-in, swapped in pull request 5. Lost: it is one more step in pull request 5 that could be forgotten, and for one pull request the check doesn't use the writer's own extraction.
  - Scan mode:
    - (B) `Full` only, a one-value enum until pull request 8 adds `Shared`. Lost: the planted test would scan the seven tables 11 times, not once, and the design would be put off rather than settled.
    - (C) Drop the parameter. Lost: pull request 8's post-suite step would then cost one full scan of seven tables per `test-` tenant, over a hundred per suite, or need a new parameter or function.
- **Consequences:**
  - From pull request 4 on, the check and the writer call the same function, so the check cannot drift from what the writer stores.
  - `update.rs` and `upsert.rs` change one pull request early. Their signatures don't change, so Sara's `update_with_match` is unaffected.
  - The `Shared` guard compares client-clock `write_ts` values with the scan's start time, so it assumes the scan and the writers run on one machine, as they do in CI.
  - It also assumes that clock doesn't step back between the scan and the check. If it does, a write made after the scan passes the guard, and the check compares that resource with rows from before the write: it can report false `missing_row` or `stray_row` defects, and it can miss a defect of that write.
  - The guard cannot see two things:
    - a write still in flight when the scan started;
    - an index row written after the scan with no `resources` write: a hand plant, and later the sweeper or repair.
  - So callers take a shared scan only after every writer has returned.
  - Two notes for later pull requests:
    - Pull request 8's report should count checks run with `Registry::Changed`, since those skip rules 3 and 4.
    - One undecodable index row fails a shared scan for every tenant it covers. That is loud, not silent, but it matters if pull request 7 plants such rows.
- **AI involvement:**
  - Claude Code (Opus 5.5 as orchestrator, an Opus planner, a Sonnet implementer, an Opus reviewer).
  - The planner wrote `SPEC-pr4.md` and proposed both options A, which Sam chose.
  - The reviewer found two things:
    - the missing guard against a stale shared scan, which Sam chose to add in this pull request, not in pull request 8;
    - a bug in the test helper `intent_versions`: hour buckets kept their fraction of a second, so the helper could read no bucket. It was fixed in the same pull request.
  - Verified by test runs against the dev node:
    - `--lib -- backends::cassandra`: 188 passed;
    - the new modules alone: three runs, no failure;
    - `cassandra_tests`: 154 passed.
  - CI's clippy was run on each commit, and the reviewer read the whole diff against the spec.
- **Links:**
  - Pull request 4, #93 (`cassandra/step5-consistency-check`).
  - ADR-sam-michaels-0006: RV13 (superseded by this entry), IW8, RV12 and D2.
  - Roadmap row 5.4.
