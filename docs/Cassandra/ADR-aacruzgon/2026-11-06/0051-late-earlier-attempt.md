# ADR-aacruzgon-0051: After an unknown outcome, a later attempt checks every earlier attempt before it acts on what it sees

- **Date:** 2026-11-06
- **Status:** accepted (Alan approved it on 2026-11-06, as drafted, including the check on the pre-read)
- **Context:**
    - **How a request gets more than one attempt.** A compare-and-swap whose outcome is unknown is resolved by the serial read R2. If R2 shows the row unchanged (version `N`, or no row for C1), the same write runs again as a new attempt, from a budget of 2 re-runs (ADR-aacruzgon-0024). ADR-aacruzgon-0034 keeps every attempt's `write_id` for the whole request. Its reason is that after a dropped connection or the client deadline, the coordinator may still be running attempt 1 (retrying under `cas_contention_timeout`, or still queued), so attempt 1 can apply after R2 reported it unchanged.
    - **Why `create` is safe and Step 3's CASes are not.**
        - `create`'s C1 is `IF NOT EXISTS`, which returns the whole existing row on a miss, `write_id` included. So a re-run that misses on attempt 1's late row finds attempt 1's `write_id` there and reports success (`write.rs:500-519`).
        - C2, C3 and C4 are conditioned on `version_id` (and C2 and C3 on `deleted_at`), and a miss returns only those columns (`ModificationStatement.java:645-677`; walkthrough "What a CAS returns when not applied").

      Suppose attempt 1 of a `create_or_update`, `update`, `delete` or `delete_versioned` timed out, R2 said "unchanged", attempt 2 was sent, and attempt 1 then applied late. The request then reports a lost race against itself:
        - attempt 2's CAS misses on attempt 1's own version, and the miss mapping sees "another version": `VersionConflict` (409) from `update` and `delete_versioned`, a retry from a plain `delete`, which then finds the resource deleted (`NotFound`), and one more turn of `create_or_update`, which writes the body a second time and breaks the race suite's version count;
        - the same happens one step earlier, if attempt 1 applies before attempt 2's pre-read R1. R1 then shows `N+1` with attempt 1's `write_id`, and the operation acts on it as if another writer had written it: `update` maps it to a miss without a CAS (walkthrough section 2), `create_or_update` updates `N+1` to `N+2` with the same body, and `delete` finds the resource deleted.
    - **The documents claimed it could not happen.** Three passages said so:
        - walkthrough 3.1, `:356`: "not applied, and never will be";
        - the design, `:296`: "after the serial read it never will";
        - the design's step 3, `:243`: "If it is not applied, nothing else has been written". That is false twice over: the miss's attempt wrote V3 and I1, as the design's own `:270` says ("A compare-and-swap that misses wastes its rewrite, which changes nothing"), and an earlier attempt may have applied.

      CodeRabbit flagged the first two on PR #50 (r4497056906, Major; r4497056805), and the Step 3 plan's HL7 matrix flagged the third (DS-12). All were unresolved when #50 merged.
- **Decision:**
    - **The check.** Once an attempt `k` of a request had an unknown outcome (target version `V_k`, `write_id` `w_k`), every later look at the row in that request is matched against every such attempt before it is acted on. There are two kinds of look:
        - the next attempt's pre-read R1 of `resources`, which returns `version_id` and `write_id`;
        - a definite miss of a later C2, C3 or C4, which is followed by R2, since the miss carries no `write_id`. C1's miss needs no read, because it returns the whole row.
    - **How the look is decided,** for each earlier unknown attempt `k`, by the engine's `decide(V_k)` (ADR-aacruzgon-0050):
        - the row at `V_k` with `w_k`: attempt `k` applied late. The request returns its success with attempt `k`'s values, and steps 4 to 6 use attempt `k`'s intent;
        - the row past `V_k`: R3 on row `V_k` decides, as in walkthrough 3.1. Ours means applied and superseded, so steps 4 and 5 are skipped; another `write_id` means attempt `k` did not apply; unset or undecodable means undecidable, 504 or 503 by attempt `k`'s trigger;
        - anything else: attempt `k` did not apply, and the look is acted on as usual. The miss is mapped, or the attempt goes ahead.
    - **No CQL change.** The statements stay as the catalogue the schema sign-off covered (ADR-aacruzgon-0028).
    - **The wording is corrected,** in this entry's commit:
        - walkthrough 3.1's "unchanged" row now says "not applied when R2 ran; it can still apply later", and that the attempt's `write_id` stays in the request's list;
        - a new paragraph and table under walkthrough 3.1 state the check;
        - the design's "Version still N" bullet says the same;
        - a new design paragraph, "A later attempt first checks the earlier ones", states the check;
        - the design's step 3 says what a miss has written (V3 and I1), and points to the check;
        - roadmap Step 3 "CAS outcome handling" gains a line for it.
- **Alternatives considered:**
    - **Add `AND write_id = ?` to C2's and C3's conditions,** bound to R1's `w(N)`, so that a miss returns `write_id`. C4 would need the same. Lost. `version_id` already fixes `write_id`, so the outcomes are unchanged, but the CQL changes after the schema sign-off. It also covers only the miss: a late attempt seen by the next pre-read still needs the check.
    - **Check only on a definite miss,** the Step 3 plan's wording of D13. Lost, as a refinement of the same option. Attempt 1 can apply before attempt 2's pre-read just as well as after it. The pre-read already returns `version_id` and `write_id`, so the match costs nothing there, and R3 is needed only when the row is past `V_k`.
    - **Ignore it as too rare:** lost. The race suite runs 8 writers on one node, and that is where unknown outcomes cluster: `PaxosState` lock timeouts throw `WriteTimeout`/CAS before anything is proposed (`PaxosState.java:406`, `:420`, as the Step 3 plan's §4a cites them; the worker did not re-read that file). One duplicate version fails `concurrent_puts_onto_a_tombstone_all_succeed`'s count of versions 3 to 6.
- **Consequences:**
    - **What it guarantees.** An applied write is never reported as 409 or `NotFound`, and `create_or_update` never writes one body twice, whichever look first sees a late attempt.
    - **What it costs.** Nothing on the common path, since it runs only after an unknown outcome. On that path it costs at most one R2 per miss and one R3 per earlier unknown attempt whose target the row has passed. A request has at most 2 re-runs, so at most 2 such R3s.
    - **What stays undecidable.** Undecidable stays possible only when row `V_k` has no readable `write_id`. With plain `update`'s V3 built from R1 (D5, its own entry), every V3 carries one, and row `V_k` exists once a later CAS applied (G40). So this case is unreachable in practice.
    - **The walkthrough** is part of the schema signed off at `67f9e0532` (ADR-aacruzgon-0028). The edits change its error handling only, so no new sign-off is needed.
    - **Tests.** The check gets a pure test of the decision, and a node test through the `cfg(test)` fault seam: an attempt that executes and reports `WriteTimeout`/CAS, then a forced re-run. Step 3's L2 and L3 extend that test to their operations.
- **AI involvement:** Drafted by a Claude Code worker (W1, `s3-engine`, Opus 5.5) for roadmap Step 3, layer L1 (branch `cassandra/step3-write-engine`). It worked from the Step 3 plan's D13 and D20, the HL7 matrix's DS-12, and PR #50's comments r4497056805 and r4497056906, read through the GitHub API. While writing it, the worker found the pre-read case: a late attempt can be seen first by the next attempt's R1, not only by its CAS miss. It widened the check from the plan's "on a definite miss" to cover both looks. That is a refinement of the option Alan accepted, and it is put to him here. Verified:
    - every cited line, against `main` at `86d72fcfa`;
    - `create`'s whole-row match, in `write.rs:500-519` and `decide_create` (`:706-750`);
    - what C2 to C4 return when they miss, in walkthrough "What a CAS returns when not applied" (spike A1, checked on a 5.0.9 node).

  The check's code and tests are in the commits after this one.
    - **What Alan changed:** Alan accepted D13 as the Step 3 plan recommended it on 2026-11-06: keep the CQL, and check the earlier attempts with `decide(V_k)` before calling a miss a lost race. He also accepted D20, under which these corrections come in L1's docs commit, before the engine code. Two more decisions bear on this entry:
        - **D5:** `update`'s V3 carries R1's `write_id`, which leaves undecidable unreachable here;
        - **D3:** one engine, so the check is written once, in `decide(V)`, and every operation gets it.

      Alan also confirmed the widening described above under AI involvement: the check runs on the next attempt's pre-read as well as on a definite miss.
- **Links:** Step 3 layer L1, branch `cassandra/step3-write-engine`; PR #50 comments [r4497056805](https://github.com/aacruzgon/hfs-Cassandra-CS4411/pull/50#discussion_r4497056805) and [r4497056906](https://github.com/aacruzgon/hfs-Cassandra-CS4411/pull/50#discussion_r4497056906); ADR-aacruzgon-0024, 0028, 0034, 0050; `cassandra-discussion.md`, "Write path" step 3, "When the LWT outcome is unknown"; `docs/Cassandra/schema/walkthrough.md` 3.1; `development-roadmap.md` Step 3 "CAS outcome handling".
