# ADR-aacruzgon-0035: A CAS `WriteTimeout` with `write_type = SIMPLE` is confirmed by the serial read, not taken as applied

- **Date:** 2026-11-06
- **Status:** accepted
- **Context:**
    - Every compare-and-swap whose outcome the driver cannot report is resolved by the `LOCAL_SERIAL` read R2 (ADR-aacruzgon-0024, ADR-aacruzgon-0026; walkthrough 3.1, "Unknown CAS outcome").
    - The design makes one exception: "A `WriteTimeout` with `write_type = SIMPLE` means the CAS was decided and only the commit timed out, so the write is applied" (`docs/Cassandra/cassandra-discussion.md:314`, "Which error, and why"). Walkthrough 3.1 repeats it: "`write_type = SIMPLE`: the CAS was decided and only the commit timed out, so treat it as applied" (`docs/Cassandra/schema/walkthrough.md:359`), and so does roadmap Step 3 ("`SIMPLE` is treated as applied").
    - Taking an answer as applied without a check is safe only if the timed-out commit is always our own proposal. Implementing `create` (roadmap Step 2, item 2.1; PR #38, `cassandra/step2-create-read`) showed it need not be:
        - a Paxos round whose prepare finds another coordinator's accepted but uncommitted proposal must commit that proposal first;
        - an empty proposal is committed after a failed condition.
    - A `SIMPLE` timeout during that commit would then be read as our success [inferred from the Paxos protocol; not verified in Cassandra 5.0.9's `Paxos.java`].
    - Taken as applied, `create` would return 201, then write version row V1 and the `resources_by_type` row X1 with our body, over the winner's version 1. Changing this is a deviation from the design, so it needs its own entry and a design edit in the same commit.
- **Decision:**
    - On every CAS (C1 for `create` now; C2 to C4 in Step 3), a `WriteTimeout` with `write_type = SIMPLE` is an unknown outcome, timed out (504 when the read cannot decide), like `WriteTimeout` with `write_type = CAS`. R2 decides it, by the table in walkthrough 3.1:
        - our `write_id` at the version we targeted: applied, the design's answer whenever its reading of `SIMPLE` is right;
        - another `write_id`: a definite miss;
        - no row: a re-run.
    - In the code: `classify_cas_failure` in `crates/persistence/src/backends/cassandra/write.rs` sends every `WriteTimeout`, whatever its `write_type`, to `CasFailure::Unknown { timed_out: true }`.
    - **Design edits, in this entry's commit:**
        - `docs/Cassandra/cassandra-discussion.md`, "When the LWT outcome is unknown": the first kind of error becomes "a `WriteTimeout` with `write_type = CAS`, or with `write_type = SIMPLE` (ADR-aacruzgon-0035)".
        - `docs/Cassandra/cassandra-discussion.md:314`, the sentence is replaced with: "A `WriteTimeout` with `write_type = SIMPLE` is treated as an unknown outcome and confirmed by the serial read, like `WriteTimeout`/CAS. It usually means the CAS was decided and only its commit timed out, but that commit can be another coordinator's proposal that our prepare had to finish first, so only the read tells whose `write_id` was applied (ADR-aacruzgon-0035)."
        - `docs/Cassandra/schema/walkthrough.md:337`, the "Triggers" line: "`WriteTimeout` or `WriteFailure` with `write_type = CAS`" becomes "`WriteTimeout` with `write_type = CAS` or `SIMPLE`, or `WriteFailure` with `write_type = CAS`".
        - `docs/Cassandra/schema/walkthrough.md:359`, the last sentence ("`write_type = SIMPLE`: the CAS was decided and only the commit timed out, so treat it as applied.") becomes: "`write_type = SIMPLE` is a trigger like `WriteTimeout`/CAS, confirmed by R2, never taken as applied (ADR-aacruzgon-0035)."
        - `docs/Cassandra/development-roadmap.md`, Step 3 "CAS outcome handling": "`SIMPLE` is treated as applied" becomes "`WriteTimeout` with `write_type = SIMPLE` is resolved by the same `LOCAL_SERIAL` read, never taken as applied (ADR-aacruzgon-0035)".
- **Alternatives considered:**
    - **Take `WriteTimeout`/`SIMPLE` as applied, as the design says:** lost. A `SIMPLE` timeout may also come from committing another coordinator's proposal [inferred, not verified in `Paxos.java`]. We would then write V1 and X1 with our body over the winner's version 1, and answer 201 for a resource another request created. The read that rules this out costs one `LOCAL_SERIAL` read, only after a commit timeout.
- **Consequences:**
    - A commit timeout on a CAS costs one serial read more than the design planned, and its answer can now be 409 (another writer) or 504 (undecidable), where the design gave 201.
    - Step 3's `update`, `delete` and `create_or_update` follow the same rule, because they share `classify_cas_failure`.
    - If a reading of `Paxos.java` later shows that a `SIMPLE` timeout always belongs to our own proposal, the extra read is harmless and can stay.
    - The walkthrough is part of the schema Alan signed off at `67f9e0532` (ADR-aacruzgon-0028). These edits change its error handling, not a table, key, column type or table option, so they need no new sign-off under that entry's rule.
- **AI involvement:** Written up by a Claude Code worker (`s2-backend`, Opus 5.5) for roadmap Step 2, PR #38 (create, read, count). Its audit subagent questioned the design's reading of `SIMPLE`, and the worker chose to confirm it by the read rather than take it on trust. Verified:
    - by the unit test `cas_failures_are_classified_as_the_design_says` (`write.rs`), which asserts that `WriteTimeout`/`SIMPLE` is an unknown, timed-out outcome;
    - by the node-gated decision-table tests (`write.rs`, `node_tests`: no row; version 1 ours, as the only attempt and as the second of two; another's; superseded with row 1 ours or another's; superseded with no row 1), each under its own tenant on the shared 5.0.9 node.

  Not verified: that a `SIMPLE` timeout can come from another coordinator's proposal (no `Paxos.java` reading, and no fault injection until Step 8's failure tests).
    - **What Alan changed:** Alan reviewed every Step 2 decision record on 2026-11-06, before it was committed. From his own review he found that several alternatives were stale, because `cdrs-tokio` 9.1.0 and 10.0.0 (released 2026-11-04) had already fixed driver behaviour the records relied on. He found that some alternatives were not real options or did not make sense: strawmen, restatements of the decision, and the workers' own debugging steps listed as alternatives. And he found that some records cited sources outside the repository. He had a review agent re-audit every alternative against the code, the design documents and `cdrs-tokio` 10.0.0, accepted its recommendations, and had each record rewritten from them. For this entry: the review found that this deviation from the design was buried in the entry on `create`'s details (now ADR-aacruzgon-0034) and needed its own entry, since it changes `cassandra-discussion.md` and the walkthrough in the same commit. Alan accepted the split. This entry was split out, with the exact replacement text for both documents. Its one alternative, "take it as applied, as the design says", was missing before and was added from the review.
- **Links:** PR #38 (branch `cassandra/step2-create-read`); `crates/persistence/src/backends/cassandra/write.rs`, `classify_cas_failure`; `cassandra-discussion.md`, "Which error, and why" and "When the LWT outcome is unknown"; `docs/Cassandra/schema/walkthrough.md` 3.1; `development-roadmap.md` Step 3; ADR-aacruzgon-0024, 0026, 0028, 0034.
