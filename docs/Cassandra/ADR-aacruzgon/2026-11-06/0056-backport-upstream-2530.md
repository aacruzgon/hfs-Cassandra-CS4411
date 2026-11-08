# ADR-aacruzgon-0056: A batch `ifMatch` PUT or DELETE is pinned to the version its gate checked, through upstream's fix (HeliosSoftware/hfs#2530) carried as a `-x` cherry-pick

- **Date:** 2026-11-06
- **Status:** accepted (Alan approved it on 2026-11-07)
- **Context:**
    - **The race.** A `batch` `PUT` or `DELETE` entry carrying `request.ifMatch` read the resource, checked the tag, dropped the row, and then wrote unconditionally (`create_or_update` or `delete`). A writer that landed between the check and the write went undetected, so the entry:
        - deleted a version the client never saw;
        - overwrote a version the client never saw;
        - or brought back (`201`) a resource deleted after the check.

      The same requests outside a Bundle answer `409` or `404`, because #1404 / #1421 made the instance `PUT` and `DELETE` with `If-Match` compare-and-swap in storage. The batch gate predates that and was never moved over (HeliosSoftware/hfs#2527, reproduced on upstream `main` `1e6bb3013`). It is the shared REST layer, so every backend is affected.
    - **The spec.** R4, R4B and R5 http.html#transaction say batch entries are "processed separately as if they were an individual interaction … subject to the normal processing for each, including … version aware updates". [`Bundle.entry.request.ifMatch`](https://hl7.org/fhir/R4/bundle-definitions.html#Bundle.entry.request.ifMatch) points at "Managing Resource Contention", whose purpose is preventing lost updates.
    - **Where the fork met it.**
        - The Step 3 matrix rows CC-9 and CC-10 (batch `ifMatch` PUT and DELETE are check-then-act) and B-2.
        - The walkthrough's G28 ("Batch `ifMatch` plus the `create_or_update` retry is last-writer-wins"). ADR-aacruzgon-0024 recorded it as an upstream limitation that the fork's bounded retry widens.
        - Step 3 plan D22 drafted it as UP-5+6 and planned to carry the fix in the fork as layer L6, as ADR-aacruzgon-0049 did.
    - **The upstream fix.** Alan filed HeliosSoftware/hfs#2527 and opened HeliosSoftware/hfs#2530: one signed commit, `9683e47f8`, on branch `fix/2527-batch-if-match-cas`, on upstream `main` `1e6bb3013`. On 2026-11-06 it is open, with no review yet. It touches 7 files:
        - `crates/rest/src/handlers/batch.rs`;
        - `crates/rest/tests/if_match_write_race.rs` and `validation_enforcement_tests.rs`;
        - `crates/rest/README.md`, the book's HFS server page, and both `run-hfs-server` skill mirrors.
    - **Why it is a decision.**
        - It changes what the server answers on the wire, for every backend.
        - It closes G28, which the design and ADR-aacruzgon-0024 recorded as open upstream.
        - Like ADR-aacruzgon-0049 and 0052, it carries an upstream change ahead of upstream. The fork is behind upstream `main`, and its last sync is `efab948ed` (upstream `81f992588`).
- **Decision:**
    - **Pin the write to the checked version.** A batch `PUT` with `ifMatch` calls `ResourceStorage::update(tenant, &current, resource)` against the row its gate evaluated:
        - a version written since is `VersionConflict` → `409`, and that version survives;
        - a delete since is `404`, not a revival.

      A batch `DELETE` with `ifMatch` calls `core::delete_under_precondition` with that row. Entries without `ifMatch` are unchanged: last writer wins, and a `PUT` on a deleted resource revives it with `201`. A tag that does not match at the gate is still `412`, evaluated before write validation.
    - **Backport now, as a `-x` pick of `9683e47f8`, unchanged except for one line.** It is on branch `cassandra/step3-backport-2530`, commit `9315c43f2` (first `696ef3b58`, rebased onto `main` `9c8c8f4f3` once Step 3's L1 to L4 merged, with the same patch), plus one fork-only test commit, in its own PR, following ADR-aacruzgon-0049 and 0052.
        - **One textual conflict, in `batch.rs`'s `use helios_persistence::core::{…}` list.** The fork also imports `conditional_update_entry`, from its backport of #2518 (`0ca71445f`, upstream `717df7af5`, ADR-aacruzgon-0049). It was resolved as upstream's list plus that name. No upstream commit touched `batch.rs` between the fork's last synced upstream commit `81f992588` and `1e6bb3013`, so no hunk depends on an upstream commit the fork lacks.
        - **Patch-ids** (`git patch-id --stable`, per file): 6 of 7 files are the same as upstream's. `batch.rs` differs only by the import line. Both have 6 hunks, and their changed lines are identical once that line is set aside. The whole-commit patch-id therefore differs (`465a8023…` upstream, `070d16d5…` in the fork).
        - **One fork-only commit on top, `5cde38cbf`** (first `cc69142ca`). Upstream's new `batch_put_with_a_satisfied_if_match_still_updates` asserts `location` `"Patient/ok"`. In the fork, every batch update answers the versioned location since the #2518 backport, and the test failed with `"Patient/ok/_history/2"` (13 of 14 passed). The assertion now takes the fork's answer. It is a separate commit, as ADR-aacruzgon-0049's Milestone A change was, so the pick stays upstream's in that file. The hfs-2527 neighbour check covered #2529 only, so it could not see this interaction.
    - **The Cassandra backend needs no change.** Its `update` is already the CAS that `handlers::update` uses (Step 3 L2a). A batch `ifMatch` PUT now reaches it instead of `create_or_update`'s loop, so the bounded retry of ADR-aacruzgon-0024 serves only entries without `ifMatch`, where last-writer-wins is the upstream contract (#2111). Its `delete_with_match` / `delete_versioned` (L2b, L2c) serve the batch `DELETE`.
- **Alternatives considered:**
    - **Wait for #2530 to merge upstream and come in with a sync:** lost, as for ADR-aacruzgon-0049 and 0052.
        - Its merge date is in upstream's hands, and syncs are manual.
        - Step 3's `update` and `create_or_update` would ship with G28 still open.
        - Step 3 plan D22 already chose to carry it.
    - **A Cassandra-only fix, retrying `create_or_update` less, or not at all, after a batch `ifMatch`:** lost.
        - The backend cannot tell that a `create_or_update` call came from a satisfied `ifMatch`: the router drops the row and the tag.
        - It would leave SQLite, PostgreSQL and MongoDB with the same race.
        - ADR-aacruzgon-0024 found the retry mandatory under #2111.
    - **Reshape the pick**, for example by adopting the fork's versioned `location` in upstream's test: lost as the default. The pick stays upstream's, so the next sync sees the same change. A fork-only difference, if one is needed, goes in a separate commit that names its cause.
- **Consequences:**
    - **Behaviour change for clients of the fork.**
        - A batch `PUT` or `DELETE` entry with `ifMatch` that loses a race now answers `409` and writes nothing. It used to answer `200` / `204` over a version the client had not seen.
        - A batch `PUT` with `ifMatch` on a resource deleted after the check now answers `404`. It used to revive it with `201`.
        - Two entries in one batch with the same `ifMatch` for one id now write once. The other answers `409`, or `412` if it was checked after the first wrote, which is always the case on SQLite.
    - **The success shape in the fork.** Through the result handling the fork already has from #2518, a satisfied `ifMatch` PUT answers `200`, the new `etag`, and a **versioned** `location` (`Type/id/_history/vid`), as every other update entry in the fork does. Upstream's text for #2530 says `location` `Type/id`, because upstream does not have #2518 yet. Whichever of #2518 and #2530 merges upstream second must change that test's line the same way (`5cde38cbf`). At the next sync:
        - if #2530 merged first, upstream's `"Patient/ok"` line meets the fork's;
        - if #2518 merged first, upstream's line already matches, and the sync takes no change.
    - **#2529's comment goes slightly stale, and is not edited here.** The batch DELETE arm's comment from the #2529 backport (ADR-aacruzgon-0052) says "`ifMatch` cannot reach here for an absent resource; its gate above has already answered `412`". With this fix, an entry whose target was deleted *after* its gate passed does reach that arm. The guard `if if_match.is_none()` still does the right thing: the pinned delete keeps its `404`, which `batch_delete_with_if_match_on_a_resource_deleted_after_the_check_is_not_found` pins. Upstream rewords the comment in whichever of #2529 and #2530 merges second. The hfs-2527 worker suggested "…an `ifMatch` entry reaches here only when the target was deleted after its gate passed, and that pinned delete keeps its 404". The fork takes the rewording with that sync.
    - **What the next upstream sync sees.** These were checked with throwaway merges in a scratch worktree, removed afterwards, from the fork's commit `696ef3b58`, before the rebase. The rebased `9315c43f2` has the same patch-id for each of the 7 files:
        - **Merging upstream's fix base `1e6bb3013`.** One conflict, in `.github/disabled-workflows/ci.yml`. It comes from drift, the same conflict ADR-aacruzgon-0049 and 0052 recorded, not from this pick.
        - **What differs after it.** 5 of the fix's 7 files differ from `9683e47f8`: both `SKILL.md` copies, the book page, `crates/rest/README.md` and `batch.rs`. Each difference is the fork-only content already there before this pick: the Cassandra rows, and the #2518 and #2529 backports. Compared file by file with the same merge from `e32b02184` (without this pick), the changed lines are the same. The context differs in the book page, and `batch.rs` differs only by `conditional_update_entry` sitting in the new import list.
        - **Then merging `fix/2527-batch-if-match-cas`.** One conflict, the same import line. Resolved with the fork's side, the merge changes nothing (an empty `git diff` against the base merge). A simulated squash of #2530 gave the same tree.
        - **Then also merging `fix/2523-delete-absent-is-success` (#2529), the case where upstream merges both.** No conflict. The only change is the SQLite test hunk that ADR-aacruzgon-0052 left out, as that entry predicts. Upstream's own combination (`git merge-tree --merge-base 1e6bb3013 4dbc3c7f0 9683e47f8`) is clean.
    - **If #2530 changes in review:**
        - each later upstream commit arrives in the sync as an ordinary change on top of the backport;
        - a hunk that rewrites lines the backport changed conflicts. Resolve it by taking upstream's side in the fix's files, then re-run `if_match_write_race`, `validation_enforcement_tests`, `batch_if_match`, `batch_conformance` and `delete_nothing_to_delete`.
    - **If #2530 is closed without merging, or reverted before a sync:** the three-way merge keeps the backport silently, as ADR-aacruzgon-0049 records.
    - **What it obliges at each sync.** Check the backport against what merged:
        - `git log --grep 'cherry picked from commit 9683e47f8'` finds the pick;
        - `git diff upstream/main HEAD -- <the 7 files>` after the merge is empty, or every difference is explained (today: the Cassandra rows and the #2518 and #2529 backports);
        - the import line is resolved with both names;
        - if upstream dropped the fix, the sync reverts the backport explicitly, or a new entry records keeping it.

      Step 3 plan `:989` marks `batch.rs` and `if_match_write_race.rs` "carried ahead of upstream, drop at the sync that brings UP-5+6".
    - **The Cassandra REST case is in this PR, as its own commit `b7c42a14c`,** added once L4 merged, since the file is L4's (`crates/hfs/tests/cassandra_milestone_a.rs`). It is Step 3 plan §5 L6 and checklist 13. `step3_crud_through_rest` gains `batch_if_match_through_rest`, with spec rows `CC-9:*` and `CC-10:*`:
        - one batch: an `ifMatch` PUT through `update`, a plain PUT through `create_or_update`, and an `ifMatch` DELETE through `delete_under_precondition` and so `delete_versioned`;
        - a second batch with two same-id `ifMatch: W/"1"` PUTs: exactly one writes, the other answers `409` or `412`, and the resource ends at version 2.

      Only the second batch tells the fix apart: the first answers the same with or without it. Without the backport's `batch.rs` change, both twins wrote (`CC-9:twins-one-write` expected 1, got 2), in each of 3 runs on the local node.
    - **The design is updated in the same commit as this entry:**
        - the design's "`create_or_update` absorbs the race" (a batch PUT with `ifMatch` now goes through `update`);
        - its open-items line on G28;
        - the walkthrough's 3.5 opening and G28.

      ADR-aacruzgon-0024 is not edited. This entry supersedes its G28 note.
- **AI involvement:**
    - **Who.** The Claude Code worker s3-ci (W5, Opus 5.5) made the branch and the pick, resolved the conflict, ran the sync merges and the checks, drafted the design edits, and drafted this entry. The upstream fix itself was written by the worker hfs-2527 in a worktree of upstream.
    - **What it proposed:**
        - the conflict resolution;
        - not editing #2529's comment (as the brief said);
        - deferring the Cassandra REST case to L4's file;
        - the extra same-id twin batch in that case.
    - **What Alan changed:** nothing. On 2026-11-07 he accepted both recommendations:
        - `cc69142ca` (`5cde38cbf` after the rebase), the fork-only `location` assertion, stays a separate commit, not squashed into the pick;
        - #2529's slightly stale comment in the batch DELETE arm stays unedited, as noted above, and is reworded with the upstream sync.
    - **What it verified, and how:**
        - the upstream issue and PR in full (`gh issue view 2527`, `gh pr view 2530`), and the hfs-2527 worker's report, including its neighbour check against #2529;
        - that no upstream commit touched `batch.rs` between `81f992588` and `1e6bb3013` (`git log`), and that `conditional_update_entry` came from the #2518 backport (`git log -S`);
        - the patch-ids, per file and whole, and the line-by-line comparison of the two `batch.rs` patches;
        - the sync merges above, in a scratch worktree removed afterwards;
        - that the fork's result handling gives a satisfied `ifMatch` PUT a versioned `location` (`batch.rs`, the `Ok((stored, created))` arm);
        - the checks, with `serial-cargo`, on `cc69142ca`, all passing:
            - `helios-rest`, with default features (SQLite): the lib tests, 997; `if_match_write_race` 14/14 (13/14 on the pick alone, the one failure above); `validation_enforcement_tests` 8; `rest_conformance` 113; `delete_nothing_to_delete` 9; `batch_if_match` 13; `batch_conformance` 109; `bundle_patch` 10; `batch_entry_issue_codes` 5;
            - the required Cassandra job's tests against the shared node (`HFS_TEST_CASSANDRA_URL=127.0.0.1:59042`): `cassandra_tests` 68/68, `backend_capability_contract` 10/10, Milestone A's `milestone_a_create_then_read_through_rest` 1/1;
            - `rustfmt --edition 2024 --check` on the three `.rs` files; clippy `-p helios-rest --all-targets --all-features --no-deps`, with `-D warnings` and CI's eight `-A` flags: clean;
            - the R4B check passes, with 3 dead-code warnings in `core/bulk_submit*.rs` that were there before this pick;
        - after the rebase onto `main` `9c8c8f4f3`, on `b7c42a14c`, all passing:
            - the same `helios-rest` suites, with the same counts;
            - against the shared node: `cassandra_tests`, `backend_capability_contract`, the backend's `--lib` tests and Milestone A;
            - `step3_crud_through_rest`, 132 spec rows (110 before the case), 0 mismatches in the HL7 rows report, passing in 3 of 3 runs;
            - the same case with the pick's `batch.rs` change reverted: failing in 3 of 3 runs, as above;
            - fmt, clippy with CI's flags, the R4B check, and the CI script's tests.
- **Links:**
    - branch `cassandra/step3-backport-2530`, commits `9315c43f2` (the pick), `5cde38cbf` (the fork's location assertion) and `b7c42a14c` (the Cassandra REST case), first `696ef3b58` and `cc69142ca` before the rebase;
    - HeliosSoftware/hfs#2527 (issue) and HeliosSoftware/hfs#2530 (PR, commit `9683e47f8`); HeliosSoftware/hfs#2529 and #2518, the neighbouring backports;
    - [R4 http.html#transaction](https://hl7.org/fhir/R4/http.html#transaction), [#concurrency](https://hl7.org/fhir/R4/http.html#concurrency), [Bundle.entry.request.ifMatch](https://hl7.org/fhir/R4/bundle-definitions.html#Bundle.entry.request.ifMatch);
    - [cassandra-discussion.md](../../cassandra-discussion.md), "`create_or_update` absorbs the race"; `schema/walkthrough.md` 3.5 and G28;
    - Step 3 plan D22 and §5 L6;
    - ADR-aacruzgon-0024 (G28's first record), 0049 and 0052 (the backport precedents).
