# ADR-aacruzgon-0084: The mutation job spells `StorageResult<T>` as a `Result` in its own checkout, so cargo-mutants tries `Ok(…)`, and its report warns when a run tested little

- **Date:** 2026-11-20
- **Status:** accepted
- **Context:**
    - **The run on #102 tested almost nothing.** A reviewer dispatched `cassandra-mutants.yml` (ADR-aacruzgon-0082) against PR #102 (run 44801901198, `head=912b7d6c2`, `base=main`). It generated 30 mutants: 1 caught, 0 missed, 29 unviable. The score said 100 %, of one viable mutant.
        - **28 of the 29** replaced the body of `update_with_match`, `update_with_match_resource`, `current_version` or `current_version_of` with `StorageResult::new(…)`, `::from(…)` or `::from_iter(…)`. rustc refused each, by the first error in each log: E0599 (12), E0277 (8), E0271 (6) and E0283 (2).
        - **The 29th** was `vec![Default::default()]` for `Vec<BackendCapability>`, which has no `Default`.
    - **The cause is in cargo-mutants, not in #102.** `StorageResult<T>` is an alias for `Result<T, StorageError>` (`crates/persistence/src/error.rs:1068`).
        - cargo-mutants 27.1.0 treats a return type as a `Result` only when its last path segment is the identifier `Result`: `path_ends_with(path, "Result")` at `src/fnvalue.rs:77`, with `path_ends_with` at `src/fnvalue.rs:263`. Only then does it generate `Ok(…)` of each value it can make for the `Ok` type, and `Err(…)` of each `--error` value.
        - `StorageResult<T>` falls through to its guess for an unknown generic type with one argument, `maybe_collection_or_container` (`src/fnvalue.rs:150-163`): `T::new()`, `T::from_iter([v])`, `T::new(v)` and `T::from(v)`, none of which exist for a `Result`.
    - **It is most of the backend.** Listed on `main` at `aa6d6297c` with `cargo mutants --list`, the whole backend has 1,327 mutants, and 559 of them (42 %) are this kind. 90 return types in 10 files are `-> StorageResult<…>`.
    - **#90 was not affected.** The brief guessed that the alias explained many of #90's 54 unviable mutants (run 44714518327). It explains none: #90's diff (`e5e8ea67f..998427a19`, +2,350/−149) changes no function that returns `StorageResult`, and its 54 are compile-time assertions (32), missing `Default` impls (14), `Range::new`/`::from`/`::from_iter` (7) and one `Duration * Duration`.
    - **Nothing upstream fixes it.**
        - **Releases:** 27.1.0 (2026-06-02) is the newest. `NEWS.md`'s "Unreleased" section has nothing on aliases, and `src/fnvalue.rs` on `main` is the same as at the tag (checked 2026-11-20).
        - **Issues and pull requests:** none on type aliases of `Result` (searched "alias", "Result alias", "type Result", "anyhow::Result", "io::Result").
        - **Configuration:** `.cargo/mutants.toml` (27.1.0's `src/config.rs`) has `error_values`, `examine_re`/`exclude_re`, `skip_calls` and build/test options. None names a return type or maps one type to another. `error_values` only adds `Err(…)` to the types already recognised as `Result`.
- **Decision:**
    - **The job rewrites the return types in its own checkout.** A new step, after the diff is taken and before cargo-mutants runs, calls `.github/scripts/mutants-result-alias.py --alias StorageResult=crate::error::StorageError`.
        - **What it rewrites:** each `-> StorageResult<T>` (or `-> some::path::StorageResult<T>`) in the `.rs` files under `crates/persistence/src/backends/cassandra/`, into `-> ::core::result::Result<T, crate::error::StorageError>`. That is the same type, spelled with `Result` last, so cargo-mutants generates `Ok(…)`.
        - **What it leaves alone:** comments, strings and char literals; every other use of `StorageResult` (locals, parameters, closures' arguments, `Future<Output = …>`); and the alias and the backend's code in the repository. It never commits.
        - **Why `::core::result::Result` and `crate::error::StorageError`:** both resolve in any module of `helios-persistence`, whatever that module imports; a bare `Result` or `StorageError` could be shadowed or not in scope.
        - **No line is added or removed,** so every `file:line` in the report is still the line the pull request shows.
        - **The diff is realigned.** cargo-mutants checks that the new side of the `--in-diff` diff matches the tree (`src/in_diff.rs:159`, `check_diff_new_text_matches`), so the script replaces each new-side line of the diff (context and added lines, counted from each hunk header) with the rewritten file's line of the same number. Removed lines and headers stay.
        - **Report only.** The script rewrites every file in memory first and writes nothing if one fails (an unbalanced `<`). If the step still fails, it restores the backend directory with `git checkout` and the diff from a copy, warns, and the run goes on as before.
        - **The names are spelled back.** `mutants-report.py --alias StorageResult=crate::error::StorageError` turns `::core::result::Result<T, crate::error::StorageError>` back into `StorageResult<T>` in the mutants' names. The diffs and logs in `mutants.out/` keep the rewritten spelling, and the summary's first line says so.
    - **The report says why each unviable mutant did not compile.** It reads the first rustc error in each unviable mutant's log and groups them in a new table, "Why the unviable mutants did not compile":
        - a `Result` type alias cargo-mutants does not see through (`X::new`, `::from` or `::from_iter` on a type whose name ends in `Result`);
        - rejected at compile time by a const evaluation (E0080, or `constant evaluation is taking a long time` under `-D long_running_const_eval`): as good as caught;
        - `Default::default()` for a type with no `Default` (E0277 naming `Default`);
        - a guessed constructor the type does not have (E0599, E0277, E0271 or E0283 on a `T::new`, `::from` or `::from_iter`);
        - otherwise the rustc error itself, with its code.
    - **The report warns when a run tested little.** When at least half of a run's mutants did not compile for a reason other than a const evaluation, the summary starts with a warning that says how many and points at the table, and the warning becomes an annotation. The job stays report-only and never fails.
        - **Why half:** on #102 the share was 29 of 30 (97 %); on #90 it was 22 of 153 (14 %; 54 unviable, 32 of them const evaluation). Half is far from both, and below it most of a run's mutants were tested.
        - **Why const evaluations do not count:** in `batch.rs`, the const fns' mutants are rejected by compile-time assertions. That is a test catching the mutant at build time, not a mutant that tested nothing.
    - **cargo-mutants stays at 27.1.0,** and no `.cargo/mutants.toml` is added.
    - **How it amends ADR-aacruzgon-0082:** 0082's job and its decisions (heavy, label-gated, report-only, the node, incremental builds) stay. This adds the rewrite step before the run, the unviable-reasons table and the low-test warning to its report, and `mutants-result-alias.py` to its `paths:` filter. 0082's per-run numbers for #90 still hold: #90's mutants are the same with the rewrite.
- **Alternatives considered:**
    - **A newer cargo-mutants.** None exists past 27.1.0, and `main` has not changed the recognition.
    - **`.cargo/mutants.toml`.** No option names a type or maps an alias, as above.
    - **`--exclude-re 'StorageResult::(new|from|from_iter)'`.** It skips the 559 unviable builds (each costs a build, a median 7 s on #102 and 11 s on #90 with incremental builds) but adds no `Ok(…)` mutant, so those functions' bodies would still go untested. It lost to the rewrite, which does both.
    - **Changing `StorageResult` or the backend's signatures to `Result<T, StorageError>`.** Out of scope: it changes production code for a tool, across every backend that uses the alias.
    - **An upstream change to cargo-mutants** (for example an option listing `Result` aliases, or a configurable error type). It is the right fix in the long run, but it would not be in a release two weeks old for some time. It is an open question below, not part of this decision.
    - **`--error` values with the rewrite** (`Err(StorageError::…)`). Lost for now: an error value applies to every `Result` the tool recognises, and the backend also returns 44 `Result<T, E>` with other error types (`DriverError`, `BackendError`, `DecodeError`, `WriteFailure` and others), where `Err(StorageError::…)` would be unviable.
    - **Only a warning, no rewrite.** It would say that the run tested little without making it test more.
- **Consequences:**
    - **Easier:** a pull request that changes a backend function returning `StorageResult` now gets `Ok(…)` body mutants that build and run. On #102's head (run 44825415773): 10 mutants instead of 30, all 6 `Ok(…)` mutants of `current_version` and `current_version_of` built, ran and were caught, and the score rests on 7 viable mutants instead of 1. On the whole backend: 926 mutants instead of 1,327, with 158 `Ok(…)` replacements in place of the 559 unviable ones.
    - **Harder:**
        - The mutants' diffs and logs in `mutants.out/` show the rewritten return type, not the source's, and a mutant on the same line as a rewritten signature has its column shifted.
        - A new `Result` alias in the backend needs its own `--alias` in `MUTANTS_ALIAS`; the report's alias reason names it when it appears.
        - The rewrite leaves `StorageResult` imports unused in 7 places, which rustc only warns about (the crate denies no warnings; checked with `cargo check --tests` on #102's head, rewritten, 1 min 51 s, no errors).
    - **Still unviable after the fix** (grouped by cause, with the runs below):
        - **Const evaluations** (32 on #90, all in `batch.rs`'s const fns under compile-time assertions): caught at build time; not to be avoided.
        - **`Default::default()` for a type with no `Default`** (14 on #90, 3 on #102: `Value`, `Envelope`, `QueryBatch`, `Row`, `IndexStatement`, `IndexBatches`, `Applied`, `IndexOutcome`, `BatchFailure`, `StoredResource`, `BackendCapability`): not cheap to avoid, since an `exclude_re` would have to name each type; no `Default` impls are added to production types.
        - **Guessed constructors** (7 on #90, `Range<usize>`; 31 on the whole backend with `DateTime` and `TcpSessionBuilder`): one `exclude_re` would avoid them, but at a median 11 s per unviable mutant (run 44826753307) it saves under a minute on a #90-sized diff, so it is left out.
        - **Other rustc errors** (1 on #90, E0369 `Duration * Duration`).
    - **To revisit** (the four open questions, left open by Alan on 2026-11-20):
        - **An upstream issue or pull request** on `sourcefrog/cargo-mutants` for `Result` aliases (a configured list of alias names, or reading `type X<T> = Result<T, E>` in the crate). Once a release with it is two weeks old, a version bump drops the rewrite step. Nothing has been posted upstream.
        - **`Err(…)` mutants:** `--error 'crate::error::StorageError::…'` would add an "always fails" mutant to every `StorageResult` function, but would be unviable in the 44 backend functions that return `Result<T, E>` with another error type.
        - **The guessed-constructor `exclude_re`:** `with .*\b(DateTime|Range|TcpSessionBuilder)::(new|from|from_iter)\(` in a new `.cargo/mutants.toml` would skip 31 unviable mutants on the whole backend, for under a minute of wall time on a #90-sized diff; left out for now.
        - **The threshold:** 50 % of the mutants not compiling for a reason other than a const evaluation; counting the const evaluations too would put #90 at 35 %, still under.
- **AI involvement:** Claude Code (Claude Opus 5.5), as a `rex-agent` worker on branch `cassandra/ci-mutants-alias`, briefed by Alan's orchestrating session.
    - **What the assistant did:**
        - checked the cargo-mutants releases, `NEWS.md`, the open and closed issues and pull requests, and `main`'s `src/fnvalue.rs`;
        - read 27.1.0's `src/fnvalue.rs`, `src/in_diff.rs` and `src/config.rs` (a clone of tag `v27.1.0`, commit `8ab1dc786`) for the cause and the options;
        - reproduced run 44801901198's 30 mutants with a local `cargo mutants --list` on #102's head (arm64 27.1.0, built into the session's scratch directory), and listed them again after the rewrite;
        - proposed the CI-only rewrite over an `--exclude-re` and over an upstream change, and the 50 % threshold from runs 44801901198 and 44714518327;
        - wrote `mutants-result-alias.py`, the report changes and 13 new tests in `test_mutants_report.py` (27 in all), and checked that the new tests fail when the comment mask, the hunk counting, the threshold or the const-evaluation reason is broken;
        - ran the dispatches below.
    - **What Alan changed:** Approved the draft as written (2026-11-20). The four open questions stay open.
    - **Verified:**
        - `cargo check --tests --package helios-persistence --features cassandra` on #102's head after the rewrite;
        - `--list` before and after on #102's head, #90's full diff, `a18bec9be~1..a18bec9be` (5 mutants either way) and the whole backend;
        - `PYTHONPATH=.github/scripts python3 -m unittest test_mutants_report` (27 tests, OK); `actionlint` on the workflow;
        - dispatches, one at a time on `acruzdebian`:

          | Run | Workflow | Target | Mutants | Caught | Missed | Unviable | Timed out | Run time | Peak `anon` |
          | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
          | 44801901198 | `main` (before) | #102 `912b7d6c2`, `base=main` | 30 | 1 | 0 | 29 | 0 | 386 s (job 7.9 min) | 10,188 MiB |
          | 44825415773 | this branch (after) | #102 `912b7d6c2`, `base=main` | 10 | 7 | 0 | 3 | 0 | 407 s (job 8.2 min) | 15,202 MiB |
          | 44830435140 | `main` (before) | #90 `e5e8ea67f..998427a19` | 153 | 91 | 8 | 54 | 0 | 1,433 s (job 25.4 min) | 14,602 MiB |
          | 44826753307 | this branch (after) | #90 `e5e8ea67f..998427a19` | 153 | 91 | 8 | 54 | 0 | 1,430 s (job 25.3 min) | 15,199 MiB |

          On #90 the caught, missed and unviable lists are identical before and after; the rewrite step took 1 s in each after run.
- **Links:** PR #103; ADR-aacruzgon-0082 (amended); run 44801901198 and the PR #102 review (`report-pr102-review.md`, "CI mutation job"); cargo-mutants 27.1.0 `src/fnvalue.rs:77`, `:150-163`, `:263`, `src/in_diff.rs:159`; `crates/persistence/src/error.rs:1068`.
