# ADR-aacruzgon-0079: Oversized index values are hashed, not refused (G35 amended)

- **Date:** 2026-11-17
- **Status:** accepted (Alan approved it on 2026-11-17, and chose to retire G38's refusal). It amends G35 in ADR-aacruzgon-0026 and the G38 refusal in ADR-aacruzgon-0027 and ADR-aacruzgon-0034; their status lines and index rows say so (see "Edits in the same commit" below).
- **Context:**
    - **The rule being amended.** G35 (ADR-aacruzgon-0026, design step 1 "Size check", walkthrough 3.1) refuses a write with 400 `InvalidResource` when any `code`, `system` or `uri` value of its new index rows is over 2,048 bytes in UTF-8. G38 (ADR-aacruzgon-0027, 0034) refuses to store a SearchParameter whose `code` is over 128 bytes, also with 400, and the step 1 check measures every row's `param` as a backstop.
        - **What it protects.** Cassandra refuses a key over 65,535 bytes ("Key length of 70021 is longer than maximum of 65535", 5.0.9). An index batch with such a key would fail at step 5, after the compare-and-swap applied, and nothing could finish the write.
        - **What is built so far.** G38's refusal is built (`check_search_parameter_code`, `crates/persistence/src/backends/cassandra/write.rs:471-497`, `MAX_SEARCH_PARAMETER_CODE_BYTES` at `:205`). G35's check is not: no index value is extracted before Step 5 (`write.rs:1936`).
    - **Why it is reopened.**
        - **The refusals reject conformant resources.** FHIR strings may reach 1 MB: [R4 datatypes.html#string](https://hl7.org/fhir/R4/datatypes.html#string), "strings SHALL NOT exceed 1MB (1024*1024 characters) in size"; R5 says the same in characters. An `Identifier.value`, `Coding.code`, `meta.source` or canonical URL of 3,000 characters is valid FHIR. So is a SearchParameter `code` of 200 bytes: the [`code`](https://hl7.org/fhir/R4/datatypes.html#code) type has no length limit.
        - **400 is the wrong status for a business rule.** [R4 http.html#create](https://hl7.org/fhir/R4/http.html#create) gives "400 Bad Request - resource could not be parsed or failed basic FHIR validation rules" and "422 Unprocessable Entity - the proposed resource violated applicable FHIR profiles or server business rules". A server's own length limit is a business rule. R4B and R5 have the same text.
        - **The other backends don't refuse.** SQLite accepts any length. PostgreSQL has no check of its own (`crates/persistence/src/backends/postgres/schema.rs:377-383`, plain btree indexes on the TEXT values).
        - **Raised by the independent audit of PR #86** (ADR-sam-michaels-0006, IW6, which restates G35), and taken by Alan on 2026-11-17 ("Decision 1").
    - **Why hashing loses nothing here.** Every token and uri form Cassandra serves is an exact match:
        - `[code]`, `[system]|[code]`, `|[code]` and `[system]|` (`docs/Cassandra/schema/coverage.md` section 2);
        - a whole-URI match (section 4).
        Everything that would need the original text is refused:
        - `:above`, `:below` and `:contains` on uri;
        - `:text`, `:of-type`, `:missing` and `:not` on token;
        - string, composite and the other types (section 6).
        An exact match on a value is an exact match on a collision-resistant hash of it.
    - **What 0026 said against hashing, and the answer.** 0026 lost the hash because "the key columns would change meaning, and every search would need a residual comparison".
        - The meaning changes only for values over the threshold, which carry a marker.
        - With SHA-256 no residual comparison is needed (below).
- **Decision:** a long indexed value is stored as its SHA-256 digest, and nothing is refused for its length.
    - **The mapping.** One function, `index_text(v)`, is applied to each of the four text components of an index key that a client can make long:
        - a token `system` and a token `code`;
        - a `uri`;
        - the `param` (a SearchParameter's `code`).

      The rule:
        - if `v` is at most **1,024 bytes** in UTF-8 and does not start with `sha256:`, it is stored as it is;
        - otherwise it is stored as `sha256:` followed by the 64 lowercase hex digits of SHA-256 over `v`'s UTF-8 bytes, 71 bytes in all;
        - no other normalisation is done: case, Unicode form and whitespace are kept, so exact, case-sensitive matching is unchanged.
    - **Why every value that starts with `sha256:` is also hashed.** It keeps the mapping one-to-one, collisions apart. A real 71-byte value `sha256:<64 hex>`, for example a container-image digest, then cannot be mistaken for the digest of a different long value.
    - **Why 1,024 bytes.**
        - **Every row fits in one batch.** The largest partition key, `token_system_code_index`'s, has five components:

          | Component | Bound | Bytes |
          | --- | --- | --- |
          | `tenant_id` | `MAX_TENANT_ID_LEN` | 64 |
          | `resource_type` | longest type name | 34 |
          | `param` | the mapping | 1,024 |
          | `system` | the mapping | 1,024 |
          | `code` | the mapping | 1,024 |
          | Framing | 3 per component | 15 |
          | **Total** | | **3,185** |

          With the clustering columns (`last_updated` 8, `id` up to 64) and `version_id`, a row stays under the 4,864-byte batch bound of ADR-sam-michaels-0006 (IW11). One row always fits one batch.
        - **Why not keep 2,048.** With `param` also allowed to 1,024 bytes, a 2,048-byte limit on values would allow a 5,233-byte key, over that bound.
        - **The key limit is far away.** Both figures are far below Cassandra's 65,535-byte key limit, which was G35's reason. G35's other reason, "the same for every tenant", still holds: the bound depends only on the value, not on the tenant id.
        - **What it costs.** Real data rarely has an identifier, code or URL over 1 KB, so the hash is rare. When it applies, a long value costs 71 bytes in every key that carries it, instead of its own length.
    - **Searching.** Step 6 applies the same `index_text` to every token `system` and `code` and `uri` value in a query, and to the `param`, before it builds a partition key or a clustering bound. A query value over 1,024 bytes, or one that starts with `sha256:`, then finds exactly the rows its value was stored under. Forms that need the original text are refused already (`coverage.md` section 6), so no form is lost.
    - **Collisions.** None is known for SHA-256, and a chance collision needs about 2^128 values. The design treats a collision as impossible and adds no residual comparison.
        - Even if one happened, a search would return one extra candidate. It could never return wrong content: the tag check reads the `resources` row, and the client gets that row's own body.
    - **What `index_keys` holds.** The stored, mapped form, exactly as in the rows' primary keys. Deletes then reproduce each row's key byte for byte from `resources.index_keys` and each version row's `index_keys` (G22, ADR-aacruzgon-0027). The original long value is never in `index_keys`, which also keeps the compare-and-swap smaller.
    - **The tag check** is unchanged. It compares the index row's `version_id` with the `resources` row's, and never reads an indexed value back.
    - **What is still refused, and with which status.**
        - **Nothing is refused for an index value's or a parameter code's length.** G35's refusal and G38's 128-byte refusal (on `create`, `update`, `update_with_match` and `create_or_update` of a SearchParameter, `write.rs:471-497`) are retired. A conformant resource is never refused for the length of an indexed value or of a custom SearchParameter's `code`.
        - **The 8 MiB payload bound stays** (ADR-aacruzgon-0053), with 400, unchanged by this entry. It protects the node's 16 MiB message limit and `max_mutation_size`, not a key.
        - **The status of any future server refusal.** If a refusal on server-business-rule grounds is ever reinstated (for example the alternative below), it answers 422, not 400 ([R4 http.html#create](https://hl7.org/fhir/R4/http.html#create)). No `ValidationError` maps to 422 today: every variant becomes 400 (`crates/rest/src/error.rs:1048-1066`). So such a refusal needs a new variant mapped to `RestError::UnprocessableEntity` (`error.rs:591-595`), in shared upstream files.
    - **Where it lives.** `index_text` goes in `crates/persistence/src/backends/cassandra/index_keys.rs`, beside the row-identity rule (ADR-sam-michaels-0006, IW7). The writer, the sweeper, repair, the 5.4 consistency check's fresh extraction and Step 6's query builder all call it.
    - **Who builds it.** Step 5's extraction pull request (ADR-sam-michaels-0006, PR 5, row 5.1), which also removes `check_search_parameter_code`. Step 6 applies it to queries.
    - **ADR-sam-michaels-0006's IW6** ("Size bounds checked before anything is written … Over the bound: 400 `InvalidResource`") is to cite this entry instead of restating G35. Alan's review of PR #86 asks for that.
- **Alternatives considered:**
    - **Keep G35's refusal, with 422 instead of 400.** This lost: it still refuses a conformant resource for a limit of the server's own storage, when the limit can be met without refusing. It also needs a new `ValidationError` variant in upstream's shared `crates/persistence/src/error.rs` and `crates/rest/src/error.rs`.
    - **Keep G38's refusal of a SearchParameter `code` over 128 bytes, with 422.** This lost too, for the same reasons. With `param` mapped like the values, the bound protects nothing: the key stays at 3,185 bytes or less. Every spec code is 46 bytes or less (`data/search-parameters-*.json`), so neither choice affects a real parameter.
        - **Had it been kept,** the refusal would have stayed where it is built (`write.rs:471-497`), answered 422 through a new variant, and kept the threshold for `param` at 128. The values' threshold could then have been 2,048, with G35's largest key of 4,337 bytes.
    - **Hashing at 2,048 bytes, as G35's limit was.** This lost: with `param` mapped at the same threshold, the largest key would be 5,233 bytes, over the 4,864-byte batch bound (IW11). Two thresholds (`param` 128, values 2,048) would also work, but one rule is simpler, and it hashes only values that are rare anyway.
    - **Truncating long values to a prefix.** This lost: two different long values with the same prefix would match each other. That needs the residual comparison 0026 objected to, and the search would have to re-extract every candidate.
    - **A shorter or non-cryptographic hash** (64-bit FNV, or a truncated SHA-256). This lost: collisions become plausible across a large tenant. 71 bytes per long value is cheap.
    - **The marker `urn:x-helios:sha256:`, as `IMPLICIT_TOKEN_SYSTEM` is `urn:x-helios:implicit code system`.** This lost narrowly to `sha256:`, which is shorter. Either is one-to-one under the "starts with the marker" rule.
- **Consequences:**
    - **Easier:**
        - No conformant resource is refused for the length of an indexed value or of a custom SearchParameter's `code`. This matches SQLite and PostgreSQL, which accept them.
        - The largest index key and row shrink from about 4.3 KB to about 3.2 KB, so batches pack more rows.
        - One rule replaces two refusals, and the step 1 size check disappears.
    - **Harder:**
        - Every place that builds a key must call `index_text`, or a long value would be written raw and miss: the writer, the sweeper, repair, the consistency check and the query builder. One function in `index_keys.rs`, and a unit test per caller with a 1,025-byte value and with a short value starting `sha256:`, keep that honest.
        - An operator reading an index table sees a digest for long values. `index_keys` holds the digest too, so the original value is only in the resource body.
        - If Cassandra ever serves `:above` or `:below` on uri, or `:contains`, a hashed value cannot be prefix-matched. A value over 1,024 bytes would then need a residual on the loaded resource. Nothing planned serves them (`coverage.md` section 6).
    - **To measure:** Step 5's fixture report ("Measure and record `index_keys` sizes per fixture resource", roadmap Step 5) also counts values over 1,024 bytes. None is expected.
    - **To revisit:** the 8 MiB payload bound's 400 (ADR-aacruzgon-0053) is a server limit too. It is left as it is here.
- **AI involvement:**
    - **Tool and model:** Claude Code, Claude Opus 5.5, as a worker session (`pr86-audit`): the independent audit of PR #86, a cross-check against the persistence README, and then this entry.
    - **What the AI proposed:**
        - In the audit report (`report-pr86-audit.md`, finding F2), hashing instead of refusing, "say 1,024 bytes", `sha256:` plus hex, and 422 for any refusal that stays.
        - In this draft:
            - the "starts with `sha256:`" rule;
            - retiring G38's refusal;
            - 1,024 bytes for all four components, from the batch bound.
    - **What the AI verified, and how:**
        - **FHIR:** R4, R4B and R5 `datatypes.html#string` and `http.html#create`, fetched.
        - **Code read at `b4b6d6ddd`:**
            - every `ValidationError` maps to 400 (`crates/rest/src/error.rs:1048-1066`), and the only 422 paths are `RestError::UnprocessableEntity` (`:591-595`) and `SearchError::TooManyResults` (`:1123`);
            - G38's check (`write.rs:471-497`);
            - G35 is unbuilt;
            - Cassandra's served forms (`coverage.md` sections 2, 4 and 6).
        - **The key arithmetic:** from walkthrough 3.1's framing of 3 bytes per component.
        - **The persistence README** (`crates/persistence/README.md`, read in full on `origin/main` `b4b6d6ddd` and upstream `c52ff36ad` in the audit's follow-up 4). It states no length limit, refusal or hashing of index values for any backend, so this entry needs no README edit.
        - **Not run:** no code was written, and no long value was stored. Nothing in this entry was reproduced on a node; the audit's experiments (read repair, Paxos reads, batch timeouts) concern ADR-sam-michaels-0006, not this rule.
    - **What Alan changed:** he took the recommendation to hash rather than refuse, or to answer 422 (Decision 1, 2026-11-17). He decided to retire G38's refusal after weighing it against keeping a 400 or a 422, because:
        - no other backend limits a SearchParameter `code` (SQLite and PostgreSQL accept any length), and the persistence README states no length limit for any backend;
        - a refusal would contradict G35's own reasoning (400) or need a new upstream error variant (422);
        - the 65,535-byte key limit it protected is now bounded by the hash.
- **Edits in the same commit** (CLAUDE.md, "Decision records"):
    - **`docs/Cassandra/cassandra-discussion.md`,** design step 1:
        - the "Size check", "Why it is needed", "Why each value is measured" and "What is not checked" bullets become the mapping;
        - the G38 bullet says the code is mapped, not refused.
    - **`docs/Cassandra/schema/walkthrough.md`:**
        - section 1's `K(V)` row (`:37`);
        - 3.1's step 1 row (`:336`) and its "Size check" and "SearchParameter `code` bound" paragraphs (`:343`, `:347`);
        - G35 (`:860-865`) and G38's "Decided" bullet (`:984`).
    - **`docs/Cassandra/schema/coverage.md`:**
        - section 6's two size rows (`:205-206`);
        - section 7's G35 and G38 rows (`:239`, `:242`).
    - **`docs/Cassandra/schema/schema.cql`:** the two size rules in the header comment (`:57-63`). Comment only; no table changes.
    - **Status lines and index rows:**
        - `2026-11-05/0026-schema-audit-fixes.md:4`:
            - before: `- **Status:** accepted`
            - after: `- **Status:** accepted; G35's refusal is replaced by hashing in ADR-aacruzgon-0079`
          Its index row's status cell: `accepted` becomes `accepted; G35 amended by [0079](2026-11-17/0079-hash-oversized-index-values.md)`.
        - `2026-11-05/0027-step-1-open-items-decided.md:4`:
            - before: `- **Status:** accepted`
            - after: `- **Status:** accepted; G38's refusal is retired by ADR-aacruzgon-0079 (a long `param` is hashed instead)`
          Its index row's status cell gains `; G38 amended by [0079](2026-11-17/0079-hash-oversized-index-values.md)`.
        - `2026-11-06/0034-create-details.md:4`:
            - before: `- **Status:** accepted`
            - after: `- **Status:** accepted; its SearchParameter `code` refusal is retired by ADR-aacruzgon-0079`
          Its index row's status cell: `accepted` becomes `accepted; its code check retired by [0079](2026-11-17/0079-hash-oversized-index-values.md)`.
    - **`ADR-aacruzgon.md`, the index:** this entry's row is added after 0078's.
    - **`development-roadmap.md`, Step 5's tasks (`:442`):** "Measure each row's `param` in the step 1 size check, as a backstop for the 128-byte bound …" becomes the mapping task, the removal of the 128-byte refusal, and a count of hashed values beside the `index_keys` sizes. PR #86 edits other lines of the same section; a trial merge with its branch (`git merge-tree`) is clean.
- **Links:**
    - ADR-aacruzgon-0026 (G35), 0027 (G38, G22), 0034, 0053;
    - ADR-sam-michaels-0006, IW6 and IW11 (PR #86);
    - Alan's review of PR #86, pullrequestreview-5941266315;
    - design step 1;
    - walkthrough 3.1, G35 and G38;
    - `coverage.md` sections 2, 4, 6 and 7;
    - [R4 datatypes.html#string](https://hl7.org/fhir/R4/datatypes.html#string), [R4 http.html#create](https://hl7.org/fhir/R4/http.html#create).
