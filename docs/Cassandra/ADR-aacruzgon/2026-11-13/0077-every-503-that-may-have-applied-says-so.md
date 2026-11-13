# ADR-aacruzgon-0077: Every 503 the backend returns while the write may have applied says the write may have been stored

- **Date:** 2026-11-13
- **Status:** accepted (Alan approved it on 2026-11-13, as drafted). It amends the message rule of ADR-aacruzgon-0055 (only the errors it turns from 500 into 503 said the write may have been stored) and the same rule in ADR-aacruzgon-0067 (`may_have_applied`); their status lines and index rows say so (see "Edits in the same commit" below).
- **Context:**
    - **The question (issue #77).** On 2026-11-13 Sara (`sara-lahourpour`) ran `Fault::ApplyThenTimeOut` on the compare-and-swap and then `FailWithWriteFailure` or `FailWithUnavailable` on `StatementKind::SerialRead`, for all seven writes. The caller got 503 with the text "Cassandra cannot serve the request right now", and the state was the same as after the 504 of the same case: the write applied, the history lacks its version, and the intent row is kept with the row's `write_id`. She asked whether the 503 should say the write may have applied, as the 504's documentation does.
    - **Why the text differed.** ADR-aacruzgon-0055 gave the text "the write's outcome could not be read back (… failed); it may have been stored" only to the errors the table for plain statements would have made 500 (`map_lookup_error`, `write.rs`). `unavailable_unless_timeout` kept a genuine `Unavailable` or `ConnectionFailed` as it was, so the client got the table's own 503 text, "Cassandra cannot serve the request right now" or "no Cassandra node could be reached" (`errors.rs`, `unavailable()`, `connection_failed()`). ADR-aacruzgon-0067's `may_have_applied` has the same shape. The undecidable outcome after a `WriteFailure` (`undecidable`, design `:334`, `:336`) used the same "cannot serve" text. So the same state told the client two different things.
    - **Why only the 503.** REST sends a 503's message to the client verbatim (`crates/rest/src/error.rs:627-631`, #2303) and never a 504's (`:298-301`, sanitized). `crates/rest/` is upstream's, so the 504 keeps saying it only in the design and ADR-aacruzgon-0072 decision 4.
    - **Alan's decision**, posted on #77 (https://github.com/aacruzgon/hfs-Cassandra-CS4411/issues/77#issuecomment-6860666752): keep both statuses; every 503 the backend returns while the write may have applied says that the write may have been stored.
    - **Why it is an entry.** It changes failure handling on the write path and amends two accepted entries (ADR-aacruzgon.md, "What counts as a decision worth an entry").
- **Decision:**
    - **The rule.** Once a compare-and-swap's outcome is unknown, every 503 the request returns says that the write may have been stored, and names no host. The variant is kept: a lost connection stays `ConnectionFailed`, everything else is `Unavailable`, so the status (503) and `Retry-After` do not change. A timeout stays `Timeout` (504) with its message as it is. A 503 returned before any outcome was unknown, when nothing can have been stored, keeps the plain table's text.
    - **The paths it covers** (`crates/persistence/src/backends/cassandra/write.rs` as changed by this entry's PR, #81):

      | Path | Today's 503 text (genuine `Unavailable` / `WriteFailure` / lost connection) | New text |
      | --- | --- | --- |
      | R2 fails, after the CAS (`serial_read`, `:1617-1629`, from `run_cas` `:1460` and `definite_miss` `:1532`); ADR-0055 | "Cassandra cannot serve the request right now" / "no Cassandra node could be reached" | "the write's outcome could not be read back (serial read failed); it may have been stored" |
      | R3 fails (`decide`, `:1590-1611`, the read at `:1608`); ADR-0055 | the same | "the write's outcome could not be read back (version read failed); it may have been stored" |
      | A re-run's V3 or I1, or a re-run's CAS the node refuses before it runs (`statement_error`, `:1387`, `:1396`, `:1448`); ADR-0067 | the same | "<previous version row / intent row / operation> failed after an earlier attempt's outcome was unknown; the write may have been stored" |
      | A later turn's R1 (`after_unknown`: `delete.rs:220`, `upsert.rs:174`); ADR-0067 | the same | "pre-read failed after an earlier attempt's outcome was unknown; the write may have been stored" |
      | Undecidable after a `WriteFailure`, and the re-runs used up after one (`undecidable`, `:391`, from `:1477-1480` and `late_attempt` `:1580-1583`); design `:334`, `:336` | "Cassandra cannot serve the request right now" | "<operation>: the compare-and-swap's outcome is unknown; the write may have been stored" |

      The errors that were already 503 with the "may have been stored" text (a `Server` error, an unmatched stream id, a decoding error on these paths) keep that text.
    - **The paths it does not touch**, where the write cannot have applied: the first attempt's V3 or I1 (no CAS sent yet); a first attempt's CAS refused before acting (`Unavailable`, `IsBootstrapping`, no connection; ADR-aacruzgon-0050); R1 of the first turn, and `update`'s own serial read before its CAS (`update.rs:221-243`); every read (`read`, `vread`, `count`, history). They keep "Cassandra cannot serve the request right now" or "no Cassandra node could be reached".
    - **The code.** One helper holds the rule: `unavailable_unless_timeout` now keeps only `Timeout` as it is, and gives `ConnectionFailed` and `Unavailable` (and everything else, as before) the caller's message. `map_lookup_error` (R2, R3) and `may_have_applied` (ADR-0067) keep their texts. `undecidable`'s 503 adds "; the write may have been stored" to the 504's text. The module docs of `write.rs` and `errors.rs` ("Messages") say so.
    - **What does not change.** Every status and variant; `Retry-After`; the plain table; which errors are 504 and which 503 (ADR-0055, 0067); the intent row kept for the sweeper; the 504's message.
- **Alternatives considered:**
    - **Leave the text as it is, and document that a 503 may also have applied:** lost. Alan chose to make the 503 say it (#77). The client reads the message; a document it never sees doesn't help it decide whether to retry a create.
    - **Answer 504 for every outcome that may have applied:** lost on #77. 504 means a timeout everywhere else in the backend (G26), and REST hides a 504's message, so the client would learn less, not more.
    - **Turn a lost connection into `Unavailable` too:** not taken. Both are 503 with `Retry-After`, so the client sees no difference, but the variant tells an operator or a caller of the storage API which kind of failure it was, and the decision on #77 changes only the message.
    - **A new message per cause (an unavailable node, a replica failure, a lost connection):** not taken. The cause is in the log (`map_driver_error` logs the driver's detail); the client needs one fact, that the write may be stored.
    - **Say it only while an unknown attempt is still unsettled** (`had_unknown_outcome()` asks whether any attempt was ever unknown, `write.rs:937-939`; an attempt settled as "another writer" can never apply, since its CAS was conditioned on a version that has moved on): not taken here. It would also change ADR-aacruzgon-0067's 500→503 rule, which uses the same check, and "may" stays true as a hedge. Left to Alan (see "Consequences").
    - **Put the text in REST, where `BackendError` becomes a 503:** lost. `crates/rest/` is upstream's file, and REST cannot know whether a write may have applied.
- **Consequences:**
    - **What it guarantees.** The same state (a write that may be stored, its intent row kept) tells the client the same thing whatever error ended the request with a 503, so a client knows to check before retrying a create.
    - **Where it over-states.** Once any attempt was unknown, the text is given even after that attempt was settled as not applied. Reproduced on a single node: a plain delete's C3 times out, R2 shows another writer's version 2 (a definite miss, the attempt settled), the delete retries, and the retry's R1 fails with `Unavailable`: the 503 says "pre-read failed after an earlier attempt's outcome was unknown; the write may have been stored", though the only attempt, conditioned on version 1, cannot apply. ADR-aacruzgon-0067's 500→503 rule has the same trigger (`had_unknown_outcome()`), so this is not new behaviour of the rule, only of the text.
    - **What becomes harder.** The client no longer sees "Cassandra cannot serve the request right now" on these paths; the server's log still says which error it was.
    - **Tests** (each fails without the change; the mutation evidence is in the PR's report):
        - pure, new: `a_genuine_503_on_r2_or_r3_says_the_write_may_have_been_stored` (an `Unavailable`, a `WriteFailure`/CAS, a `ReadFailure`, `Overloaded`, an `Io` reset and a closed connection on R2 and R3 keep their variant and say the write may have been stored; the plain table on another statement keeps its own text) and `after_an_unknown_outcome_every_503_says_the_write_may_have_been_stored` (`statement_error` and `after_unknown`: the plain text before any unknown outcome, the new text after);
        - pure, extended: `an_r2_or_r3_error_is_504_for_a_timeout_and_503_otherwise` (every 503 says it), `undecidable_is_504_after_a_timeout_and_503_after_a_failure` (the 503 says it);
        - node, new: `a_503_from_r2_after_an_applied_cas_says_the_write_may_have_been_stored`: `ApplyThenTimeOut` on the CAS and `FailWithUnavailable` or `FailWithWriteFailure` on R2, for create, update and delete; the CAS applied, and the 503 says the write may have been stored;
        - node, extended: `unknown_outcomes_end_in_504_or_503_and_unavailable_is_not_retried` (the re-runs used up after `WriteFailure` say it; a CAS refused with `Unavailable` keeps the plain text).
    - **No REST-level test reaches these 503s:** the fault seam is `cfg(test)` inside `helios-persistence`. REST passes the message through unchanged (`crates/rest/src/error.rs:1237-1239`, `:627-631`).
    - **To revisit.** If upstream ever lets a 504's message through, the 504 could say the same.
- **AI involvement:**
    - **Who and what.** A Claude Code worker (`fix-77-503-message`, Opus 5.5) listed every 503 path of the write engine and whether the write may have applied there, wrote the change, the tests and this draft, following Alan's decision on #77.
    - **What Alan changed:** nothing; he approved the draft as written on 2026-11-13.
    - **Verified:**
        - The 503 paths, by reading every error return of `run_cas`, `definite_miss`, `late_attempt`, `decide`, `serial_read`, and the loops of `update.rs`, `delete.rs` and `upsert.rs`; an audit subagent (not briefed with the conclusion) walked them again and found no path left with the plain text, and none where the write cannot have applied that gets the new text.
        - The Cassandra `--lib` tests (142) on the worker's own single node (Compose project `fix77-cassandra`, port 59142), all passing; the node test's six cases print the CAS applied and the new text.
        - Each new or extended test fails with the code change reverted and the tests kept (6 failed of 142), and passes with it.
        - The over-statement above, by changing the fault of `a_later_turns_pre_read_error_after_an_unknown_outcome_is_503` to `FailWithUnavailable` in a scratch copy (restored).
        - The required checks on draft PR #81: four pass; `cargo clippy (workspace)` fails on rustc 1.100.0 lints in PostgreSQL and MongoDB files the PR does not touch (run 43479557585).
    - **Not verified:** a `ConnectionFailed` 503 through the fault seam (it has no lost-connection fault; the pure tests cover it); a real (not injected) `Unavailable` on R2; any REST-level test of these 503s (none exists; REST passes the message through, `crates/rest/src/error.rs:1237-1239`).
- **Edits in the same commit** (CLAUDE.md, "Decision records"):
    - `2026-11-06/0055-r2-r3-errors-503.md`, the status line. Before:
      `- **Status:** accepted (Alan chose 503 on 2026-11-06, as recommended, and approved this entry on 2026-11-07). It amends ADR-aacruzgon-0050's sub-decision "An R2 error is R2's own timeout or failure"; this entry's "What does not change" is amended by ADR-aacruzgon-0067`
      After:
      `- **Status:** accepted (Alan chose 503 on 2026-11-06, as recommended, and approved this entry on 2026-11-07). It amends ADR-aacruzgon-0050's sub-decision "An R2 error is R2's own timeout or failure"; this entry's "What does not change" is amended by ADR-aacruzgon-0067, and its message rule (only the errors turned from 500 into 503 say the write may have been stored) by ADR-aacruzgon-0077`
    - `2026-11-07/0067-errors-after-unknown-outcome-503.md`, the status line. Before:
      `- **Status:** accepted (Alan approved it on 2026-11-07). It amends ADR-aacruzgon-0055's "What does not change".`
      After:
      `- **Status:** accepted (Alan approved it on 2026-11-07). It amends ADR-aacruzgon-0055's "What does not change"; its message rule is amended by ADR-aacruzgon-0077.`
    - `ADR-aacruzgon.md`, the index. Row 0055's status `accepted; amended by [0067](2026-11-07/0067-errors-after-unknown-outcome-503.md)` becomes `accepted; amended by [0067](2026-11-07/0067-errors-after-unknown-outcome-503.md) and [0077](2026-11-13/0077-every-503-that-may-have-applied-says-so.md)`; row 0067's status `accepted` becomes `accepted; amended by [0077](2026-11-13/0077-every-503-that-may-have-applied-says-so.md)`; and, after row 0075 (0076 is reserved for PR #76, which adds its own row), a new row:
      `| [0077](2026-11-13/0077-every-503-that-may-have-applied-says-so.md) | 2026-11-13 | Every 503 the backend returns while the write may have applied says the write may have been stored | accepted |`
    - `cassandra-discussion.md`, "Which error, and why", a bullet after `:336`. Before (`:335-336`):
      ```
      - a serial read that times out is 504, and one that fails (`ReadFailure`, `Unavailable`) is 503;
      - when the 2 re-runs are used up, the last attempt's error decides the same way: 504 after a `WriteTimeout`, 503 after a `WriteFailure`.
      ```
      After:
      ```
      - a serial read that times out is 504, and one that fails (`ReadFailure`, `Unavailable`) is 503;
      - when the 2 re-runs are used up, the last attempt's error decides the same way: 504 after a `WriteTimeout`, 503 after a `WriteFailure`;
      - every 503 returned once an outcome was unknown, these and the serial read's, the version read's and every later statement's (step 2 above), says that the write may have been stored. REST sends a 503's message to the client and never a 504's, so the 504 cannot say it (ADR-aacruzgon-0077).
      ```
    - No change to the walkthrough (`docs/Cassandra/schema/walkthrough.md:372-373`, `:377`) or to `development-roadmap.md:227-228`: they state statuses, not texts, and the statuses do not change.
- **Links:** issue #77 and Alan's comment above; draft PR #81 (`cassandra/fix-77-503-may-have-applied`); ADR-aacruzgon-0050, 0055 and 0067 (amended here), 0072 decision 4; `cassandra-discussion.md:319`, `:332-336`; walkthrough 3.1's undecidable and R2 rows (`walkthrough.md:372-373`), which state the status, not the text, and are unchanged.
