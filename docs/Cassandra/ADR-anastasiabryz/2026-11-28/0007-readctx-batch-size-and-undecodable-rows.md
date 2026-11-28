# ADR-anastasiabryz-0007: `ReadCtx` gives every plan one batch size and a place for undecodable index rows; a date plan beside another parameter is a residual only

- **Date:** 2026-11-28
- **Status:** accepted
- **Context:**
    - ADR-anastasiabryz-0001 fixed the interface the search modules implement. Its `ReadCtx` carries the tenant, the resource type, the session and the candidate budget. `C/` is `crates/persistence/src/backends/cassandra/`.
    - Alan's first audit of #133 (2026-11-27, https://github.com/aacruzgon/hfs-Cassandra-CS4411/pull/133#issuecomment-7223031169) named three gaps in it, "for whenever they fit":
        - "a module has no way to hand an undecodable row to repair (`skip_undecodable`)". The repair contract counts such rows through `RepairContext::skip_undecodable` (`C/repair.rs`), but only the framework holds the request's `RepairContext`, behind a `&mut` that a plan's stream cannot share;
        - "the read context passes no batch or page size, so each module would pick its own `LIMIT`";
        - "what a date plan does when it doesn't drive, and when values are extracted, aren't documented yet".
    - His audit of #141 (2026-11-28) adds, for the date plan: "`contains()` can't do a date point read (no `last_updated` key), so it should return `true` and leave the test to `residual(…, None)`".
- **Decision:**
    - `ReadCtx` gains two fields:
        - `batch_rows: usize`, the rows each index read asks for (`LIMIT`): a batch size, not the page size (walkthrough 5.1, step 6). The framework sets it to `BATCH_ROWS`, 100, the type scan's own, so every plan reads in the same batches;
        - `undecodable: &Undecodable`, where a plan records an index row it could not decode (`Undecodable::skip(table, error)`). The plan skips the row, never returns it and never fails the search for it. When the stages finish, the framework hands each recorded row to the request's `RepairContext::skip_undecodable`, which logs and counts it. `_id` and counts run no repair phase, so their rows are logged through an unrun context, as the counts' hidden rows already are.
    - `ParamPlan`'s docs now say:
        - only the driving plan streams, and a date plan drives only in a search of dates alone (G20);
        - a date plan beside another parameter is checked by `residual(loaded, None)` alone, so the framework never calls its `contains`, which can return `true`;
        - `loaded.extracted` is filled once per candidate after the tag check, and only when a residual reads values (a compartment, or a date parameter other than `_lastUpdated`);
        - a residual decides from its arguments alone, because a resumed page compiles a new plan and candidates are checked concurrently (ADR-anastasiabryz-0005);
        - within one page, the framework keeps a resource once whatever the residuals return, as bundle.html#bundle-unique requires.
    - **Counts dedupe like pages.** `plan_count` now counts a resource once even when the driving stream yields it at several order keys, as a page keeps it once. Before, a date driver that didn't apply "first matching row" could count a resource twice that its pages showed once (the audit's finding 11).
- **Alternatives considered:**
    - **Let a module return an error for an undecodable row.** Lost because one bad index row would then fail every search that reads it, where the repair contract skips and counts it.
    - **Give `ReadCtx` the `RepairContext` itself.** Lost because the repair context is `&mut` and the R1 and check stages run concurrently. Sharing it would need a lock around every queue, where the tag check now queues in one ordered step.
    - **Leave the batch size to each module.** Lost because the reads in flight (ADR 0005) and the cap's cost are reasoned about at one batch size, and different `LIMIT`s per module would make a search's cost depend on which module drives. (Alan's measurements used batches of 50; ours is 100, not measured separately.)
- **Consequences:**
    - The trait surface changes: `ReadCtx` has two more fields. No module implements the trait yet, so no code breaks. Sara and Diya are told on #130, #131 and #132, as ADR 0001 requires for every interface change.
    - The batch size is a constant in `C/search.rs`; to be revisited with Step 8's measurements.
    - The large-stack extraction stays in `C/search/residual.rs`, not a shared helper. The write path's `extract_index_keys` returns index keys, not the values a residual reads, and keeps its stack size private to `C/index_keys.rs`, Sam's file. Sharing one helper is his change to make, if he wants it.
- **AI involvement:** Claude Code (Claude Opus 5.5) proposed the two fields, the `Undecodable` sink and the count dedupe from Alan's audit points and the committed-work audit, and wrote the docs and the tests:
    - `a_plans_undecodable_row_reaches_repair`, a fake plan that records one row: the search answers, and the repair phase counts it;
    - the count assertion in `a_resource_is_kept_once_and_its_leftover_row_repaired`;
    - the default-`residual` assertion in `a_module_can_implement_the_plan_trait`.

    Each fails under a reverted mutation; the Cassandra lib tests pass (428). Reviewed and accepted by me (Anastasia Bryzgalova) on 2026-11-28.
- **Links:** #128; ADR-anastasiabryz-0001 (which this extends), 0005; Alan's audits on #133 (2026-11-27) and #141 (2026-11-28); `C/repair.rs` (`skip_undecodable`); walkthrough 5.1, 5.8, G20; PR #143.
