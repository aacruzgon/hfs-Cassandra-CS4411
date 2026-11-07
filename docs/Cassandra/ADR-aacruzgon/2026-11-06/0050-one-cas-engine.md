# ADR-aacruzgon-0050: Every compare-and-swap goes through one write engine, under the rules of ADR-aacruzgon-0034 and 0035

- **Date:** 2026-11-06
- **Status:** accepted (Alan approved it on 2026-11-06, as drafted)
- **Context:**
    - **The question.** Roadmap Step 3 (CRUD core) adds four compare-and-swaps to Step 2's one: C2 (`update`), C3 (`delete` and `delete_versioned`) and C4 (revive), beside C1 (`create`), with `create_or_update` looping over C1, C2 and C4. ADR-aacruzgon-0024 already extended the unknown-outcome rule to "every compare-and-swap", and ADR-aacruzgon-0035 says "On every CAS (C1 for `create` now; C2 to C4 in Step 3)". But Step 2 implemented these rules only inside `create`'s own loop (`crates/persistence/src/backends/cassandra/write.rs:467-559`). The code that decides an outcome is hard-wired to version 1:
        - `CreateAttempt` and `write_version_row` bind version 1 (`write.rs:759`);
        - `write_index_rows` inserts only, and only X1 (`:786`, `:791-796`);
        - `decide_create` reads row 1 (`:735`);
        - `CreateOutcome` is walkthrough 3.1's table for `V = 1`.

      So Step 3 has to choose how the six-step write path is shared, and whether each new CAS takes the same rules.
    - **Three questions the earlier entries left open.**
        - ADR-aacruzgon-0032 left it to Step 3 whether a CAS is retried after `Unavailable`.
        - ADR-aacruzgon-0034's rule for a post-CAS failure (return success and leave the intent for the sweeper, G23) was decided for `create` only.
        - The Step 3 plan's HL7 and Cassandra matrix (§2d, gap 1) found that on Paxos v2 the serial read R2 can itself fail with `WriteTimeout`/CAS (`Paxos.java:893-901`, in `Paxos.read`, which throws `CasWriteTimeoutException(WriteType.CAS, …)` through `markAndThrowAsTimeoutOrFailure(true, …)`). Run through the CAS classification, that would read as a new unknown write.
    - **The documents were behind the code.** PR #50 merged with CodeRabbit comments that the Step 3 plan (D20) checked against `main` at `86d72fcfa` and found still true:
        - r4497056786 (design `:276`) and r4497056852 (walkthrough `:344`): the unknown-outcome trigger lists leave out `Server`, `Overloaded` and an answer with no readable `[applied]`, which ADR-aacruzgon-0034 and `classify_cas_failure` (`write.rs:218-268`) send to the serial read. A worker who implements C2 to C4 from the list instead of the helper would map an applied write to an error.
      Two more of PR #50's comments, r4497056805 and r4497056906, are about a late earlier attempt. They are recorded in ADR-aacruzgon-0051. The Step 3 plan also found two places where the roadmap contradicts the design and an earlier entry (D21 rows 1 and 2, below).
- **Decision:**
    - **One engine.** Every compare-and-swap, C1 to C4, runs through one engine in `write.rs`. Each operation hands the engine a plan:
        - the CAS statement and its binds;
        - the target version `V = N + 1`;
        - `K(N)`, decoded from R1's `index_keys`, and `K(N+1)`;
        - the version row, V1 or V2, and the V3 source;
        - what a definite miss does: return the error (`update`, `delete_versioned`), retry once from the pre-read (plain `delete`, G25), or take the next turn (`create_or_update`).

      The engine runs V3 (step 2, before I1, G40), I1, the CAS, the classification, R2 and R3 and the decision, the post-CAS V1 or V2 and the X1 delta, and I2. It returns the attempt that applied, or a typed miss. Each operation keeps only its pre-read, its miss mapping and its loop, in its own file: `update.rs`, `delete.rs`, `upsert.rs`, `versions.rs`. `create` moves onto the engine with no change in behaviour, and its existing tests, `unknown_create_outcomes_follow_the_walkthrough_table` among them, are the check.
    - **The decision is one function of the target version.** `decide(V, tried, row)` generalises `decide_create`. It follows walkthrough 3.1's table:
        - R2 at `V` with one of the request's `write_id`s: applied;
        - R2 at `N` (or no row for C1): not applied yet, so re-run;
        - R2 at `V` with another `write_id`: a definite miss;
        - R2 past `V`: R3 on row `V` decides (ours: applied and superseded; another: a definite miss; unset or undecodable: undecidable).

      A cell that is missing or cannot be decoded decides nothing, never "another writer" (ADR-aacruzgon-0034).
    - **The miss mapping is a pure function of the columns returned:**
        - `[applied] False` alone: `NotFound`;
        - `deleted_at > 0`: `NotFound`;
        - another `version_id`: `VersionConflict`;
        - a row on `IF NOT EXISTS`: `AlreadyExists`.

      A missing row returns `[applied]` alone (`ModificationStatement.java:624-627`), so "no row" is detected by the condition columns being absent from the answer (`Row::contains_column`), not by a null value.
    - **ADR-aacruzgon-0034's and 0035's rules hold on every CAS.**
        - Every attempt's `write_id` is kept for the whole request.
        - Every error that does not prove the CAS was not executed goes to R2. That includes `WriteTimeout` with `write_type = SIMPLE` (ADR-aacruzgon-0035), `Server`, `Overloaded`, an unreadable `[applied]`, and any other driver error after the CAS was sent, such as `General("Write channel failure!")` and `General("Unmatched stream id: …")` (matrix §2d, gap 2). The wildcard arms of `classify_cas_failure` already send these to R2 (`write.rs:245`, `:265`).
        - Only the errors that prove nothing was sent, or that the coordinator refused first, are mapped directly.
    - **Three sub-decisions:**
        - **No CAS retry after `Unavailable`.** The native protocol says the coordinator refused before acting, so the 503 is accurate and fast. Single-node CI cannot exercise a retry, and the three-node "Partition, RF 3" failure test expects 503 (roadmap section 6). Revisit only if Step 8 (item 8.3) shows needless 503s.
        - **Post-CAS failures return success on every operation.** If V1 or V2, or the X1 delta, still fails after its 3 tries, `update`, `delete`, `delete_versioned` and a `create_or_update` turn return success and leave I1 for the sweeper (G23), as `create` does (ADR-aacruzgon-0034). Until the Step 5 sweeper exists, such a write's X1 row stays stale, which skews `count` only, and only under faults.
        - **An R2 error is R2's own timeout or failure.** It is mapped by the table for plain statements (`errors.rs`), as `resolve_unknown_create` does today (`write.rs:691`), never by `classify_cas_failure`. A `WriteTimeout`/CAS raised by R2 on Paxos v2 is R2 timing out (504). A unit test pins this, and the `cfg(test)` fault seam gets a mode that fails the next R2 with `WriteTimeout`/CAS.
    - **The documents follow the code.** These edits are in this entry's commit (no edit in the commit is to a table, key, column type or table option):
        - **Design:**
            - step 3: what a miss has written (V3 and I1, not "nothing"), and how "no row" is detected;
            - "When the LWT outcome is unknown": ADR-aacruzgon-0034's full trigger list and its list of direct mappings, and no retry after `Unavailable` (PR #50 r4497056786);
            - "The serial read times out, or fails": R2's error is a plain statement's;
            - step 6: post-CAS failures return success on every operation.
        - **Walkthrough 3.1:**
            - the same trigger list and direct mappings (PR #50 r4497056852);
            - the R2-failure row;
            - step 4's row: retries exhausted return success;
            - 3.2's unknown row points to 3.1's triggers.
        - **Roadmap Step 3, "CAS outcome handling":**
            - the trigger list;
            - the past-the-target line, which said "past the target version … 504 … 503". It now says that R3 on row V decides, and only an unset `write_id` or a failed R2 is 504 or 503 (D21 row 1; CodeRabbit's outside-diff comment on PR #33, never applied);
            - the R2-error line;
            - the post-CAS line.
        - **Roadmap section 6, "Concurrent updates":** "every other gets 409" contradicted ADR-aacruzgon-0024, where an unconditional PUT is last-writer-wins and every racer succeeds. It now says: with `If-Match`, exactly one 200 per version and the rest 409 or 412; without it, every PUT succeeds with a distinct version and a gap-free history, and 409 only after four definite misses in a row, which is the retry budget running out (D21 row 2).
- **Alternatives considered:**
    - **Four functions, each a copy of `create`'s loop:** lost. Each copy would carry its own tried-list, its own superseded rule and its own not-applied list, so the subtleties of ADR-aacruzgon-0034 would be kept four times. `create`'s loop is already the hardest code in the backend.
    - **A trait object per operation:** lost. The operations differ only in data (statement, binds, versions, keys, miss policy), so a plan value carries them. A trait adds dynamic dispatch and lifetimes over borrowed request data, and keeps nothing apart that a value does not.
    - **Apply the rules per operation, and take `WriteTimeout`/`SIMPLE` as applied on update and delete:** lost. ADR-aacruzgon-0035 showed that `SIMPLE` can come from committing another coordinator's proposal, and ADR-aacruzgon-0024 extended the rule to every CAS.
    - **Retry a CAS after `Unavailable`:** lost, for the reasons above. It would also be an untested path: one node cannot show a coordinator that refuses and then recovers.
    - **Return 5xx after a post-CAS failure on update and delete:** lost. After an applied CAS, a client's retried PUT would write an extra version, and a retried If-Match PUT would get a 412 for a write that succeeded.
    - **Classify R2's error like a CAS's:** lost. R2 writes nothing of ours. Taking its `WriteTimeout`/CAS as a new unknown write would re-run, or report undecidable, a request whose real problem is that the read could not answer.
- **Consequences:**
    - **Easier:** the L2 and L3 workers write only a pre-read, a miss mapping and a loop. The rules of ADR-aacruzgon-0034 and 0035 live in one place and are tested once. `storage.rs` becomes one-line delegations, so the parallel workers never edit the same file (roadmap ground rule "One owner per shared file").
    - **Harder:** `create` has to move onto the engine without a change in behaviour. Its existing unit and node tests are the net, and they now gate the PR through the new `--lib` step in the required job, which gets its own entry with its measured cost (Step 3 plan §9 #10).
    - **The walkthrough** is part of the schema Alan signed off at `67f9e0532` (ADR-aacruzgon-0028). These edits change its error handling, not a table, key, column type or table option, so they need no new sign-off under that entry's rule, as with ADR-aacruzgon-0034 and 0035.
    - **To measure:** R2 and R3 under the race suite's contention, through the engine's outcome counters (`cas_stats()`, Step 3 CI report R2). If the 503s after `Unavailable` show up as needless in Step 8 (item 8.3), the no-retry rule is revisited.
    - **Not verified until Step 8:** a real mid-CAS failure on a cluster. The fault seam covers the engine's applied, re-run and R2-failure branches on one node.
- **AI involvement:** Drafted by a Claude Code worker (W1, `s3-engine`, Opus 5.5) for roadmap Step 3, layer L1 (branch `cassandra/step3-write-engine`). It worked from the Step 3 plan (decisions D3, D4, D20 and D21, written by the `s3-plan` worker, and reconciled with the `s3-spec` worker's HL7 matrix). Verified:
    - every line number cited, against `main` at `86d72fcfa`;
    - the code paths, by reading `write.rs` (`classify_cas_failure`, `resolve_unknown_create`, `decide_create`) and `errors.rs`;
    - the driver strings, in cdrs-tokio 10.0.0's `transport.rs` (`:487`, `:491`, `:684`, `:769`, `:921`);
    - that `Paxos.read` throws `CasWriteTimeoutException(WriteType.CAS, …)`, in the downloaded `cassandra-5.0.9` `Paxos.java` (`:551-567`, `:893`);
    - the PR #50 comment text, through `gh api repos/aacruzgon/hfs-Cassandra-CS4411/pulls/50/comments`.

  The engine itself is in the commits after this one, with its tests.
    - **What Alan changed:** Alan accepted D3, D4, D20 and D21 as the Step 3 plan recommended them on 2026-11-06, and these decisions bear on this entry:
        - **D8:** a delete of an unknown or already-deleted id follows the spec (200 or 204). The fix is in the shared REST layer, upstream in HeliosSoftware/hfs#2523, and the fork will carry it. The Cassandra backend returns `NotFound` for that case, as SQLite, PostgreSQL and MongoDB do, so the miss mapping above is unchanged.
        - **D5:** plain `update`'s V3 is built from R1 and carries a `write_id`. That is recorded in its own entry. It makes the "row `V`'s `write_id` unset" case of `decide(V)` unreachable in practice.
        - **D12:** each Step 3 PR is opened against `main` after the one below it merges, so this entry lands with the engine in L1.
- **Links:** Step 3 layer L1, branch `cassandra/step3-write-engine`; PR #50 comments [r4497056786](https://github.com/aacruzgon/hfs-Cassandra-CS4411/pull/50#discussion_r4497056786) and [r4497056852](https://github.com/aacruzgon/hfs-Cassandra-CS4411/pull/50#discussion_r4497056852); ADR-aacruzgon-0012, 0024, 0026, 0027 (G22, G39, G40), 0028, 0032, 0034, 0035, 0051; `cassandra-discussion.md`, "Write path" steps 3 and 6, "When the LWT outcome is unknown"; `docs/Cassandra/schema/walkthrough.md` 3.1 and 3.2; `development-roadmap.md` Step 3 "CAS outcome handling" and section 6 "Concurrent updates"; `crates/persistence/src/backends/cassandra/write.rs`, `errors.rs`.
