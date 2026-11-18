# ADR-aacruzgon-0078: The error a 503 replaces after an unknown outcome is logged once, and a later turn's R1 keeps its statement apart from its decoding

- **Date:** 2026-11-15
- **Status:** accepted (Alan approved it on 2026-11-15). It updates how ADR-aacruzgon-0067 and ADR-aacruzgon-0077 describe a later turn's R1 path; their status lines and index rows say so (see "Edits in the same commit" below).
- **Context:**
    - **The question (issue #84).** Once a request has had an unknown compare-and-swap outcome, `CasRequest::after_unknown` (`crates/persistence/src/backends/cassandra/write.rs:981-988` at `69d85e386`) turns a later turn's backend error into the 503 `MAY_HAVE_APPLIED_503`, "the write's outcome is unknown and a later step failed; it may have been stored" (ADR-aacruzgon-0067, 0077), and drops the error's own message.
        - A driver error has already been logged by `map_driver_error` (`errors.rs:97-111`), with its statement's label.
        - An error the backend builds itself had not been logged. These are `PreviousVersion::from_row` ("pre-read: index_keys is unset", "pre-read: <key set decode error>", `write.rs:571-588` at `69d85e386`) and `ResourceRow::decode` ("pre-read: version_id: ...", "pre-read: no <column> column", `read.rs:118-156` at `69d85e386`). Both build their errors through `internal()` (`write.rs:436`, `read.rs:58` at `69d85e386`), which logs nothing.
        - Without an unknown outcome the same error is a 500, and REST logs a 500 (`crates/rest/src/error.rs:684-688`). After one, an operator saw a 503 and no trace of a corrupt row.
        - Reproduced at `69d85e386` by a pure test for both `from_row` and `decode`: each answers the 503, and no captured log line holds its detail.
    - **Why the variant does not settle it.** The issue suggested logging in `may_have_applied` or `after_unknown` "when it's one the driver mapping hasn't already logged: `Internal`, `QueryError` and `SerializationError`".
        - `map_driver_error` produces both of the first two itself:
            - `Internal` for a `Server`, `Protocol`, `Truncate`, `Unprepared` or unknown error, or a driver decoding error (`errors.rs:151`, `:159`);
            - `QueryError` for `Syntax`, `Invalid`, `Unauthorized`, `Config`, `AlreadyExists` or `FunctionFailure` (`:138-145`).
        - A later turn's R1 brought both kinds to `after_unknown` in one `StorageResult`: `read_resource_row` (`read.rs:244-264` at `69d85e386`) mapped the statement's error through `map_driver_error`, then ran `ResourceRow::decode`.
        - So no rule on the variant can log the decoding error without logging a driver `Server` or `Unauthorized` error on that R1 a second time.
    - **Why it is an entry.** The log line alone would be routine. But:
        - the fix is a choice between workable designs;
        - it changes how a later turn's R1 reaches the replacement, which ADR-aacruzgon-0067 (`:14`) and ADR-aacruzgon-0077 (`:20`) describe as `after_unknown`;
        - its level departs from the issue's suggested `warn` (ADR-aacruzgon.md, "What counts as a decision worth an entry").
- **Decision:** `after_unknown` logs the error it replaces, once, at `error`. Only errors the backend built can reach it:
    - a later turn's R1 is split into its statement and its decoding;
    - `after_unknown` is private to `write.rs`, so driver errors are kept out by construction.
    - **The log line.** `after_unknown` (`write.rs:1015-1033` at `08d36382e`) logs the error before `may_have_applied` replaces its message.
        - Call: `tracing::error!` (`:1021`) with `operation`, `resource_type`, `id` and `error = %` the `BackendError`. These are the fields of `run_cas`'s unknown-outcome `warn` (`:1512-1518`).
        - Text: "a later step failed after a compare-and-swap outcome was unknown; answering that the write may have been stored".
        - Example: `ERROR helios_persistence::backends::cassandra::write: a later step failed after a compare-and-swap outcome was unknown; answering that the write may have been stored operation="delete" resource_type="Patient" id=p error=internal error in cassandra: pre-read: index_keys is unset`.
        - Neither the row's body nor any other cell value is logged: the messages name a column, a type or a byte count.
    - **The level is `error`**, not the issue's `warn`. Alan confirmed it on 2026-11-15.
        - It is the level `map_driver_error` gives an `Internal` or `QueryError` it maps (`errors.rs:107`), and the level REST gives the same error when it is a 500.
        - A row that cannot be decoded is a server fault, whatever status the client gets.
        - At `warn` it would sit beside the unknown-outcome lines, which are expected under load.
    - **The R1 split.**
        - `CassandraBackend::select_resource_rows` (`read.rs:265-282`) runs the statement and returns the driver's error unmapped.
        - `CasRequest::pre_read_row` (`write.rs:986-992`) maps that error with `statement_error`, as a re-run's V3 and I1 are mapped, then decodes the row through `after_unknown`.
        - `delete_turns`' pre-read closure now returns the statement's rows (`delete.rs:209`, `:222`), and `create_or_update`'s loop reads R1 the same way (`upsert.rs:175`).
        - `read_resource_row` (`read.rs:247`) keeps its signature for its other callers (`update`'s own R1 and `unversioned_current`, `delete_with_match`, the SearchParameter catch-up). None of them runs after an unknown outcome in the same request.
    - **The guard.** `after_unknown` is a private method of `CasRequest`. The three `PreviousVersion::from_row` sites (`delete.rs:243`, `upsert.rs:201`, `update.rs:204`) call `CasRequest::previous_version(row)` (`write.rs:999-1001`), which is `after_unknown(PreviousVersion::from_row(row))`.
        - So its only callers are `pre_read_row` and `previous_version`, both in `write.rs`, and both pass errors the backend built.
        - A call from another file does not compile (E0624, "method `after_unknown` is private"). That was checked by changing `delete.rs:243` back to `request.after_unknown(PreviousVersion::from_row(row))`.
    - **What does not change.** Every client text (ADR-0077), variant, status and `Retry-After`.
        - For a driver error on a later turn's R1 the result is what it was: `map_driver_error` with the "pre-read" label, then `may_have_applied` once an outcome was unknown.
        - Before any outcome was unknown, a decoding error is still the plain 500, and the backend logs nothing (REST does).
        - `statement_error`, `map_lookup_error` (R2, R3) and `undecidable` log nothing new: their errors are driver errors that `map_driver_error` has logged, or, for `undecidable`, its own `warn`.
- **Alternatives considered:**
    - **Log in `after_unknown` or `may_have_applied` for `Internal`, `QueryError` and `SerializationError` (the issue's suggestion):** lost.
        - It logs a later turn's R1 driver error twice: once at `error` by `map_driver_error`, and once by the new line.
        - Shown by mutating `pre_read_row` back to sending R1's mapped driver error through `after_unknown`: the driver-error test counts 8 extra lines for 8 errors.
    - **Log in `may_have_applied` or `unavailable_unless_timeout`, for every error:** lost.
        - `statement_error` calls `may_have_applied` with a driver error that `map_driver_error` has just logged, so a re-run's V3 or I1 error would be logged twice.
        - Those functions don't have the request's operation, type or id.
        - Shown by the same kind of mutation: 16 extra lines, and no `operation="delete"` on the line.
    - **Log where the error is built (`from_row`, `ResourceRow::decode`), always:** lost. Before an unknown outcome the error is a 500 that REST already logs, so the plain path would log it twice.
    - **Make the decoding errors `SerializationError`, which `map_driver_error` never produces, and log that variant:** not taken.
        - It changes the variant that every caller of `read_resource_row` and of `PreviousVersion::from_row`, and their tests, see for a decoding error.
        - The rule would hold only while no driver mapping ever returns that variant.
    - **Keep `after_unknown` `pub(crate)` and rely on its doc comment** (the first version of PR #85): not taken, after the independent audit (`pr85-review`, M1). A later caller in `delete.rs` or `upsert.rs` could pass a statement's mapped error and log it twice, and only review would catch it. The private method costs one three-line wrapper.
    - **`warn`, as the issue suggested:** not taken, for the reason under "The level". Alan confirmed `error` on 2026-11-15.
- **Consequences:**
    - **Easier:**
        - After an unknown outcome, an operator can tell a corrupt row (a key set that does not decode, an unset `version_id`, a cell of the wrong type) from an outage, from one `error` line with the operation, type and id.
        - `after_unknown` has one kind of input, and the compiler holds that, not a comment.
    - **Harder:** a later turn's R1 is two calls (`select_resource_rows`, `pre_read_row`) instead of `read_resource_row` inside `after_unknown`.
    - **Revisit:** if Step 4 or 5 adds a statement between turns, it goes through `statement_error`. A new step the backend runs itself between turns gets a wrapper in `write.rs`, as `previous_version` is.
    - **Tests** (pure, `write.rs`, PR #85):
        - `after_an_unknown_outcome_a_row_that_cannot_be_decoded_is_logged_once` (`:2985`) covers three decoding errors:
            - `decode` of a null `version_id` and of a `data` cell of the wrong type that holds the body, both through `pre_read_row`;
            - `from_row` with `index_keys` unset, through `previous_version`.

          Before an unknown outcome each is the plain 500, and the backend logs nothing. After one, each is `Unavailable` with exactly `MAY_HAVE_APPLIED_503`, its detail is logged once at `ERROR` with the three fields, and the body is never logged, though the body is the cell that failed.
        - `after_an_unknown_outcome_a_statements_driver_error_is_logged_once` (`:3105`) sends `Server`, `Invalid` and every plain-table 503 through `pre_read_row` and `statement_error`. Each is logged once, by `map_driver_error`, and never by the new line.
        - Mutations, each failing one of the two tests:
            - the author's six: no log; R1's driver error through `after_unknown`; the log in `may_have_applied`; the variant kept; the detail in the client text; the log only in `pre_read_row`;
            - the audit's four: logging before an unknown outcome too; `warn`; no `id` field; R1 mapped by the plain table;
            - the audit's body check: `read.rs` echoes the `data` cell's bytes in its error, and the test fails with "the row's body is never logged", left 1, right 0.
        - The node tests of the changed closures pass on a single node.
- **AI involvement:**
    - **The `fix-84-log` worker** (Claude Code, Opus 5.5):
        - reproduced #84 on `69d85e386` for both decoding paths, with a temporary pure test that captures tracing output;
        - mapped every path into the replacement (`statement_error`, `after_unknown`, `map_lookup_error`, `may_have_applied`, `unavailable_unless_timeout`) and which errors each can carry;
        - found that the variant cannot tell a driver error from a built one;
        - proposed the R1 split and the `error` level, wrote the tests, and ran six mutations (each run, failing, then restored).
    - **The independent audit** (Claude Code, Opus 5.5, the `pr85-review` worker, not briefed by the author):
        - re-ran two of those mutations and four of its own;
        - ran the node tests on its own single node;
        - showed on that node, with tracing captured, that both decoding sites log once and answer the exact 503;
        - probed four corrupt cells holding the body (none logged a value);
        - with Alan's approval, made its four minor fixes: the private `after_unknown` with `previous_version` (`b74af6f63`), the body assertion on a failing `data` cell (`08d36382e`), the line citations of the draft of this entry, and the pointer to ADR-0067.
    - **The delta audit** (Claude Code, Opus 5.5, a subagent not briefed by the author) audited `b74af6f63` and `08d36382e`. It reproduced E0624 at `delete.rs:243`, showed that the new test fails when `read.rs:142` echoes the `data` cell's bytes and that the test as it was before `08d36382e` passed under the same change, ran clippy with CI's workspace flags scoped to `-p helios-persistence` (exit 0), and re-checked every line citation at `08d36382e`. It found nothing blocking or should-fix.
    - **What Alan changed:** on 2026-11-15 he confirmed that the log level stays `error`, not the issue's `warn`, and he approved the entry as drafted on 2026-11-15.
- **Edits in the same commit** (CLAUDE.md, "Decision records"):
    - `2026-11-07/0067-errors-after-unknown-outcome-503.md`. Two of its lines no longer describe the code:
        - its `:14` ("`CasRequest::after_unknown` maps a later turn's R1 result, and the decoding of that row into the next plan ...");
        - its `:25` ("What becomes harder": an operator can't tell a server bug from an outage by the status; "`map_driver_error` still logs the driver's detail ..."), which is true only of driver errors, and is the gap this entry closes.

      Its rule (which errors are 503) is unchanged. Status line `:4`:
        - before: `- **Status:** accepted (Alan approved it on 2026-11-07). It amends ADR-aacruzgon-0055's "What does not change"; its message rule is amended by ADR-aacruzgon-0077.`
        - after: `- **Status:** accepted (Alan approved it on 2026-11-07). It amends ADR-aacruzgon-0055's "What does not change"; its message rule is amended by ADR-aacruzgon-0077. Since ADR-aacruzgon-0078, a later turn's R1 and its decoding reach the rule through `pre_read_row` and `previous_version`, not `after_unknown` directly ("The code", below), and the error the 503 replaces is logged ("What becomes harder").`

      Index row (`ADR-aacruzgon.md:114`), its status cell:
        - before: `accepted; amended by [0077](2026-11-13/0077-every-503-that-may-have-applied-says-so.md)`
        - after: `accepted; amended by [0077](2026-11-13/0077-every-503-that-may-have-applied-says-so.md); its R1 path and logging updated by [0078](2026-11-15/0078-log-the-replaced-error.md)`
    - `2026-11-13/0077-every-503-that-may-have-applied-says-so.md`. Two of its lines are stale in the same way:
        - its `:20` (the later-turn R1 row cites `after_unknown` at `delete.rs:220`, `:241`, `upsert.rs:174-175`, `:198`, `update.rs:205`; those sites are now `pre_read_row` at `delete.rs:222`, `upsert.rs:175` and `previous_version` at `delete.rs:243`, `upsert.rs:201`, `update.rs:204`);
        - its `:25` ("Operators still see the names: `map_driver_error` logs the failing statement's label with the driver's error", true only of driver errors).

      Its message rule is unchanged. Status line `:4`:
        - before: `- **Status:** accepted (Alan approved it on 2026-11-13, and approved two follow-ups to it, from PR #81's independent audit, the same day; see "What Alan changed"). It amends the message rule of ADR-aacruzgon-0055 (only the errors it turns from 500 into 503 said the write may have been stored) and the same rule in ADR-aacruzgon-0067 (`may_have_applied`); their status lines and index rows say so (see "Edits in the same commit" below).`
        - after: the same, followed by ` Since ADR-aacruzgon-0078, a later turn's R1 and its decoding reach `after_unknown` through `pre_read_row` and `previous_version` (the R1 row of the table below cites the sites before that change), and the error a "may have been stored" 503 replaces is logged even when no statement logged it.`

      Index row (`ADR-aacruzgon.md:123`), its status cell:
        - before: `accepted`
        - after: `accepted; see [0078](2026-11-15/0078-log-the-replaced-error.md)`
    - `ADR-aacruzgon.md`, the index: this entry's row is added after 0077's:
      `| [0078](2026-11-15/0078-log-the-replaced-error.md) | 2026-11-15 | The error a 503 replaces after an unknown outcome is logged once, and a later turn's R1 keeps its statement apart from its decoding | accepted |`
    - No change to `cassandra-discussion.md` or `development-roadmap.md`: they say nothing about logging.
- **Links:**
    - issue #84;
    - PR #85 (`cassandra/fix-84-log-replaced-error`): commits `d30132ac1`, `90d06d82a`, `b74af6f63`, `08d36382e`;
    - ADR-aacruzgon-0055, 0067, 0077;
    - design: `cassandra-discussion.md`, "When the LWT outcome is unknown" (`:278`) and "Which error, and why" (`:332-338`). They say nothing about logging and are unchanged.
