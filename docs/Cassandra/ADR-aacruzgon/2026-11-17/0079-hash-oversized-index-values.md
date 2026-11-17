# ADR-aacruzgon-0079: Oversized index values are hashed, not refused (G35 amended)

- **Date:** 2026-11-17
- **Status:** accepted (Alan approved it on 2026-11-17, and chose to retire G38's refusal). It amends G35 in ADR-aacruzgon-0026 and the G38 refusal in ADR-aacruzgon-0027 and ADR-aacruzgon-0034; their status lines and index rows say so (see "Edits with this entry" below).
- **Context:**
    - **The rule being amended.** G35 (ADR-aacruzgon-0026, design step 1 "Size check", walkthrough 3.1) refuses a write with 400 `InvalidResource` when any `code`, `system` or `uri` value of its new index rows is over 2,048 bytes in UTF-8. G38 (ADR-aacruzgon-0027, 0034) refuses to store a SearchParameter whose `code` is over 128 bytes, also with 400, and the step 1 check measures every row's `param` as a backstop.
        - **What it protects.** Cassandra refuses a key over 65,535 bytes ("Key length of 70021 is longer than maximum of 65535", 5.0.9). An index batch with such a key would fail at step 5, after the compare-and-swap applied, and nothing could finish the write.
        - **What is built so far.** G38's refusal is built (`check_search_parameter_code`, `crates/persistence/src/backends/cassandra/write.rs:471-497`, `MAX_SEARCH_PARAMETER_CODE_BYTES` at `:205`). G35's check is not: no index value is extracted before Step 5 (`write.rs:1936`).
    - **Why it is reopened.**
        - **The refusals reject conformant resources.** FHIR strings may reach 1 MB: [R4 datatypes.html#string](https://hl7.org/fhir/R4/datatypes.html#string), "strings SHALL NOT exceed 1MB (1024*1024 characters) in size"; R5 says the same in characters. An `Identifier.value`, `Coding.code`, `meta.source` or canonical URL of 3,000 characters is valid FHIR. So is a SearchParameter `code` of 200 bytes: the [`code`](https://hl7.org/fhir/R4/datatypes.html#code) type has no length limit.
        - **400 is the wrong status for a business rule.** [R4 http.html#create](https://hl7.org/fhir/R4/http.html#create) gives "400 Bad Request - resource could not be parsed or failed basic FHIR validation rules" and "422 Unprocessable Entity - the proposed resource violated applicable FHIR profiles or server business rules". A server's own length limit is a business rule. R4B and R5 have the same text.
        - **What the other backends do.** Neither has a length rule, and neither answers 400. Reproduced on `origin/main` `b4b6d6ddd` on 2026-11-17, with a Patient carrying one long random `Identifier.value` or `meta.profile`, then a search for it:
            - **SQLite** stores and finds token values of 1,000, 2,000, 2,600, 2,800 and 3,000 bytes, and a 3,000-byte uri.
            - **PostgreSQL** (16.16) has no length check of its own, but it stores the values as btree key columns, and "an index entry cannot exceed approximately one-third of a page (after TOAST compression, if applicable)" ([btree.html#BTREE-INTRO](https://www.postgresql.org/docs/current/btree.html#BTREE-INTRO)), 2,704 bytes at 8 KB pages. The live indexes are `idx_search_token_code` (`crates/persistence/src/backends/postgres/schema.rs:5605-5609`, migration v21) and `idx_search_uri` (`:5784-5787`, v19). Up to 2,600 bytes a value is stored and found. A 2,800-byte or 3,000-byte token value, or a 3,000-byte uri, answers 500, with nothing stored: "index row size 2904 exceeds btree version 4 maximum 2704 for index \"idx_search_token_code\"". A compressible 3,000-byte value (`a` repeated) is stored and found.
        - **Raised by the independent audit of PR #86** (ADR-sam-michaels-0006, IW6, which restates G35), and taken by Alan on 2026-11-17 ("Decision 1").
    - **Why hashing loses nothing here.** Every token and uri form Cassandra serves is an exact match:
        - `[code]`, `[system]|[code]`, `|[code]` and `[system]|` (`docs/Cassandra/schema/coverage.md` section 2);
        - a whole-URI match (section 4);
        - token `:in`, `:above` and `:below` with a terminology server, which REST expands into a plain `system|code` OR before the backend sees it (section 6, C8), so each expanded value is mapped like any other.
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
        - **A single-row batch stays under IW11's bound, and batches pack more rows.** The largest partition key, `token_system_code_index`'s, has five components:

          | Component | Bound | Bytes |
          | --- | --- | --- |
          | `tenant_id` | `MAX_TENANT_ID_LEN` | 64 |
          | `resource_type` | longest type name | 34 |
          | `param` | the mapping | 1,024 |
          | `system` | the mapping | 1,024 |
          | `code` | the mapping | 1,024 |
          | Framing | 3 per component | 15 |
          | **Total** | | **3,185** |

          With the clustering columns (`last_updated` 8, `id` up to 64) and `version_id` 4, a row is 3,261 bytes of key and column data. IW11 (ADR-sam-michaels-0006) counts wire bytes, at most 4,864 per batch, so the comparison is on the wire: as a prepared INSERT in a CQL v5 `BATCH` (1 kind byte, an 18-byte id, a 2-byte value count, a 4-byte length per value), the row is 1 + 18 + 2 + 8 × 4 + (64 + 34 + 3 × 1,024 + 8 + 64 + 4) = 3,299 bytes, and its DELETE (7 values) 3,291. A single-row unit always fits one batch.
        - **A packing preference, not a correctness bound.** An update that keeps a maximal value sends a DELETE (version N) and an INSERT (N+1) to the same partition, one IW9 unit of about 6,590 bytes, over 4,864. IW11 already sends such a unit alone, as it would have sent G35's (about 8.9 KB). That is harmless: Cassandra 5.0.9 skips its batch size check for a batch of one mutation (`BatchStatement.java:333-335`, "We only warn for batch spanning multiple mutations (#10876)"), and its measure excludes partition-key bytes (walkthrough section 2, "Index rows", checked on a 5.0.9 node). The threshold only decides how many rows share a batch.
        - **Why not keep 2,048.** With `param` at 1,024 bytes, a 2,048-byte limit on values would allow a 5,233-byte key (64 + 34 + 1,024 + 2 × 2,048 + 15), so even a single-row unit would go over IW11's bound and be sent alone.
        - **The key limit is far away.** Both figures are far below Cassandra's 65,535-byte key limit, which was G35's reason. G35's other reason, "the same for every tenant", still holds: the bound depends only on the value, not on the tenant id.
        - **What it costs.** Real data rarely has an identifier, code or URL over 1 KB, so the hash is rare. When it applies, a long value costs 71 bytes in every key that carries it, instead of its own length.
    - **Searching.** Step 6 applies the same `index_text` to the parsed value of every token `system` and `code` and `uri` in a query, and to the `param`, before it builds a partition key or a clustering bound. The parsed value is the one after percent-decoding, after FHIR's `\,` `\|` `\$` `\\` unescaping and after the split on `|`, never the raw parameter text: a stored 1,020-byte code with five commas is sent as `code=…\,…`, 1,025 raw bytes, and mapping the raw text would hash it and miss. A query value over 1,024 bytes, or one that starts with `sha256:`, then finds exactly the rows its value was stored under. Forms that need the original text are refused already (`coverage.md` section 6), so no form is lost.
    - **Collisions.** None is known for SHA-256, and a chance collision needs about 2^128 values. The design treats a collision as impossible and adds no residual comparison.
        - Even if one happened, a search would return one non-matching resource, with its own body, never another resource's: the tag check compares only `version_id` and never reads the value back, and the client gets that row's own body.
    - **What `index_keys` holds.** The stored, mapped form, exactly as in the rows' primary keys. Deletes then reproduce each row's key byte for byte from `resources.index_keys` and each version row's `index_keys` (G22, ADR-aacruzgon-0027). The original long value is never in `index_keys`, which also keeps the compare-and-swap smaller.
    - **The tag check** is unchanged. It compares the index row's `version_id` with the `resources` row's, and never reads an indexed value back.
    - **What is still refused, and with which status.**
        - **Nothing is refused for an index value's or a parameter code's length.** G35's refusal and G38's 128-byte refusal (on `create`, `update`, `update_with_match` and `create_or_update` of a SearchParameter, `write.rs:471-497`) are retired. A conformant resource is never refused for the length of an indexed value or of a custom SearchParameter's `code`.
        - **The 8 MiB payload bound stays** (ADR-aacruzgon-0053), with 400, unchanged by this entry. It protects the node's 16 MiB message limit and `max_mutation_size`, not a key.
        - **The status of any future server refusal.** If a refusal on server-business-rule grounds is ever reinstated (for example the alternative below), it answers 422, not 400 ([R4 http.html#create](https://hl7.org/fhir/R4/http.html#create)). No `ValidationError` variant a backend can use for a business rule maps to 422 today: they become 400 (`crates/rest/src/error.rs:1048-1066`). The one 422 through `ValidationError` is `ValidationError::Patch` (`:1065`), whose `PatchError::TestFailed` becomes `UnprocessableEntity` (`:1081-1083`); it reports a failed FHIRPatch `test` operation, and a storage limit is not one. So such a refusal needs a new variant mapped to `RestError::UnprocessableEntity` (`error.rs:591-595`), in shared upstream files.
    - **Where it lives.** `index_text` goes in `crates/persistence/src/backends/cassandra/index_keys.rs`, beside the row-identity rule (ADR-sam-michaels-0006, IW7).
    - **Applied exactly once to each value.** The mapping is not idempotent: every digest starts with `sha256:`, so mapping a digest again hashes it again, and for a long `v`, `index_text(index_text(v))` differs from `index_text(v)`. So it is applied:
        - to values fresh from `SearchParameterExtractor::extract`: the writer's step 1 (`K(N+1)`) and the 5.4 consistency check's fresh extraction (ADR-sam-michaels-0006, RV10);
        - to parsed query values, in Step 6 (above);
        - never to keys decoded from `index_keys` or read from index rows. The writer's `K(N)`, the sweeper's replay (SW11, "insert K(V), delete K(V−1) minus K(V)") and repair (RV1) take the stored keys as they are (G22). Mapping them again would send the sweeper's inserts to the wrong partitions and make its deletes miss, leaving orphans and missed hits only for long values.
    - **Who builds it.** `index_text` lands no later than the first Step 5 pull request that extracts or compares fresh keys: PR 4 of ADR-sam-michaels-0006's build order, whose 5.4 check compares a fresh extraction (RV10), or at the latest PR 5 (row 5.1), which switches extraction on. PR 5 also removes `check_search_parameter_code` and its three calls (`write.rs:1916`, `update.rs:112`, `upsert.rs:191`). Until then `main` still refuses a SearchParameter `code` over 128 bytes with 400, and `coverage.md` section 6 and walkthrough 3.1 say so. Step 6 applies the mapping to queries.
    - **ADR-sam-michaels-0006's IW6** ("Size bounds checked before anything is written … Over the bound: 400 `InvalidResource`") is to cite this entry instead of restating G35. Alan's review of PR #86 asks for that.
- **Alternatives considered:**
    - **Keep G35's refusal, with 422 instead of 400.** This lost: it still refuses a conformant resource for a limit of the server's own storage, when the limit can be met without refusing. It also needs a new `ValidationError` variant in upstream's shared `crates/persistence/src/error.rs` and `crates/rest/src/error.rs`.
    - **Keep G38's refusal of a SearchParameter `code` over 128 bytes, with 422.** This lost too, for the same reasons. With `param` mapped like the values, the bound protects nothing: the key stays at 3,185 bytes or less. Every spec code is 46 bytes or less (`data/search-parameters-*.json`), so neither choice affects a real parameter.
        - **Had it been kept,** the refusal would have stayed where it is built (`write.rs:471-497`), answered 422 through a new variant, and kept the threshold for `param` at 128. The values' threshold could then have been 2,048, with G35's largest key of 4,337 bytes.
    - **Hashing at 2,048 bytes, as G35's limit was.** This lost: with `param` mapped at the same threshold, the largest key would be 6,257 bytes (64 + 34 + 3 × 2,048 + 15), and a single-row unit would go over the 4,864-byte batch bound (IW11). Two thresholds (`param` 128, values 2,048) would also work, but one rule is simpler, and it hashes only values that are rare anyway.
    - **Truncating long values to a prefix.** This lost: two different long values with the same prefix would match each other. That needs the residual comparison 0026 objected to, and the search would have to re-extract every candidate.
    - **A shorter or non-cryptographic hash** (64-bit FNV, or a truncated SHA-256). This lost: collisions become plausible across a large tenant. 71 bytes per long value is cheap.
    - **The marker `urn:x-helios:sha256:`, as `IMPLICIT_TOKEN_SYSTEM` is `urn:x-helios:implicit code system`.** This lost narrowly to `sha256:`, which is shorter. Either is one-to-one under the "starts with the marker" rule.
- **Consequences:**
    - **Easier:**
        - No conformant resource is refused for the length of an indexed value or of a custom SearchParameter's `code`. SQLite accepts them too. PostgreSQL has no rule against them either, but its btree indexes fail a write whose incompressible value is over about 2.7 KB, with a 500 (Context).
        - The largest index key and row shrink from about 4.3 KB to about 3.2 KB, so batches pack more rows.
        - One rule replaces two refusals, and the step 1 size check disappears.
    - **Harder:**
        - Each value must be mapped exactly once. The writer's step 1, the 5.4 check's fresh extraction and the query builder call `index_text`; nothing that reads stored keys does. A missed call writes or queries a long value raw, and a second call hashes a digest again; either way the search misses. Tests keep that honest:
            - per caller, a 1,025-byte value and a short value starting `sha256:`;
            - a stored hashed key through a sweeper replay and a repair, whose keys come back byte for byte unchanged;
            - a Step 6 query whose escaped value (`\,`) is over 1,024 bytes raw and at most 1,024 parsed, which finds its resource.
        - An operator reading an index table sees a digest for long values. `index_keys` holds the digest too, so the original value is only in the resource body.
        - If Cassandra ever serves `:above` or `:below` on uri, or `:contains`, a hashed value cannot be prefix-matched. A value over 1,024 bytes would then need a residual on the loaded resource. Nothing planned serves them (`coverage.md` section 6).
    - **To measure:** Step 5's fixture report ("Measure and record `index_keys` sizes per fixture resource", roadmap Step 5) also counts values over 1,024 bytes. None is expected.
    - **To revisit:** the 8 MiB payload bound's 400 (ADR-aacruzgon-0053) is a server limit too. It is left as it is here.
- **AI involvement:**
    - **Tool and model:** Claude Code, Claude Opus 5.5, as a worker session (`pr86-audit`): the independent audit of PR #86, a cross-check against the persistence README, this entry, and then the fixes from the independent audit of PR #88 (`pr88-audit.md`, findings B1, S1 to S4 and M1 to M8).
    - **What the AI proposed:**
        - In the audit report (`report-pr86-audit.md`, finding F2), hashing instead of refusing, "say 1,024 bytes", `sha256:` plus hex, and 422 for any refusal that stays.
        - In this draft:
            - the "starts with `sha256:`" rule;
            - retiring G38's refusal;
            - 1,024 bytes for all four components, from the batch bound.
        - After the audit of PR #88: the "applied exactly once" rule and its tests, the wire-byte comparison with IW11, the parsed-value rule for queries, and the rewording of the leftovers that still described the refusal.
    - **What the AI verified, and how:**
        - **FHIR:** R4, R4B and R5 `datatypes.html#string` and `http.html#create`, fetched.
        - **Code read at `b4b6d6ddd`:**
            - every `ValidationError` a backend can use for a business rule maps to 400 (`crates/rest/src/error.rs:1048-1066`). The 422 paths are `RestError::UnprocessableEntity` (`:591-595`), reached from `ValidationError::Patch`'s `PatchError::TestFailed` (`:1065`, `:1081-1083`), from bulk submit's parse and invalid-resource errors (`:922-925`) and from `SearchError::TooManyResults` (`:1123`). The first draft missed the patch and bulk-submit paths; the audit of PR #88 found them;
            - G38's check (`write.rs:474-497`) and its calls (`write.rs:1916`, `update.rs:112`, `upsert.rs:191`);
            - G35 is unbuilt;
            - Cassandra's served forms (`coverage.md` sections 2, 4 and 6).
        - **The key arithmetic:** from walkthrough 3.1's framing of 3 bytes per component. The wire sizes (3,299 and 3,291 bytes per row, about 6,590 per two-row unit) re-derived from CQL v5 `BATCH` framing after the audit of PR #88 gave them. Cassandra 5.0.9's `BatchStatement.java:333-335` read at tag `cassandra-5.0.9` on GitHub.
        - **The persistence README** (`crates/persistence/README.md`, read in full on `origin/main` `b4b6d6ddd` and upstream `c52ff36ad` in the audit's follow-up 4). It states no length limit, refusal or hashing of index values for any backend, so this entry needs no README edit.
        - **Reproduced on SQLite and PostgreSQL** (2026-11-17, after the audit of PR #88 questioned "PostgreSQL accepts any length"): `hfs` from `origin/main` `b4b6d6ddd`, built with `R4,sqlite,postgres`, on an in-memory SQLite and on a fresh `postgres:16` container (16.16, 8 KB pages, schema v55). Results as in Context. On PostgreSQL the three failing writes answered 500 and left no `resources` row. The audit cited `idx_search_token` (`schema.rs:5774-5776`); a later migration drops it, and `pg_indexes` on the fresh database lists only the two indexes Context names for these columns.
        - **Not run on Cassandra:** no Cassandra code was written, and no long value was stored on a node. The audit's experiments (read repair, Paxos reads, batch timeouts) concern ADR-sam-michaels-0006, not this rule.
    - **What Alan changed:** he took the recommendation to hash rather than refuse, or to answer 422 (Decision 1, 2026-11-17). He decided to retire G38's refusal after weighing it against keeping a 400 or a 422, because:
        - no other backend has a rule limiting a SearchParameter `code` (SQLite accepts any length; PostgreSQL has no check of its own, though its btree indexes fail a value over about 2.7 KB, as reproduced after the decision, see Context), and the persistence README states no length limit for any backend;
        - a refusal would contradict G35's own reasoning (400) or need a new upstream error variant (422);
        - the 65,535-byte key limit it protected is now bounded by the hash.
- **Edits with this entry** (CLAUDE.md, "Decision records"), in PR #88: its first commit, and the commits after its independent audit, which found leftovers that still described the refusal:
    - **`docs/Cassandra/cassandra-discussion.md`:**
        - design step 1: the "Size check", "Why it is needed", "Why each value is measured" and "What is not checked" bullets become the mapping (`:238-242`), with parsed query values and "applied exactly once";
        - the G38 bullet says the code is mapped, not refused (`:241`);
        - the resolved-gaps list's G38 line (`:406`).
    - **`docs/Cassandra/schema/walkthrough.md`:**
        - "Read this first"'s G38 description (`:11`);
        - section 1's `K(V)` row (`:37`);
        - section 2's blob size per row (`:169`): at most 3,079 bytes, from about 4 KB;
        - 3.1's step 1 row (`:336`) and its "Size check" and "SearchParameter `code` bound" paragraphs (`:343`, `:347`), with the clause that `main` still answers 400 until Step 5's extraction pull request;
        - the `create_or_update` turn's G38 check (`:442`);
        - G35 (`:860-865`) and G38's "Decided" bullet (`:984`).
        - **Left for the second pull request to merge:** "Index rows" (`:231`) still says "With the 2 KiB rule (G35) the largest single index row is about 4.3 KB". PR #86 rewrites the same paragraph and keeps that sentence, so editing it here would conflict. Whichever of PR #86 and PR #88 merges second changes it to the 1,024-byte mapping and 3,185 bytes.
    - **`docs/Cassandra/schema/coverage.md`:**
        - section 6's two size rows, moved under a new heading, "Formerly refused, now accepted" (`:206-213`), with `main`'s 400 until Step 5's extraction pull request;
        - section 7's G35 and G38 rows (`:246`, `:249`).
    - **`docs/Cassandra/schema/schema.cql`:** the size rule in the header comment (`:57-62`). Comment only; no table changes.
    - **`docs/Cassandra/schema/README.md`:** the "Statements that must never exist" row (`:43`), 0026's and 0027's rows (`:70`, `:71`), and the G38 bullet (`:107`).
    - **`docs/Cassandra/development-roadmap.md`:** Step 1's "Statements that must never exist" (`:171`); Step 4's tasks, handover and "What is left" (`:371`, `:385`, `:397`, `:410`), which keep `check_search_parameter_code` in `update_with_match` only for as long as it exists, since Step 5's extraction pull request removes it; and section 5's Step 6 test row (`:604`), which now expects a 2,000-byte SearchParameter `code` and a 3,000-byte `Identifier.value` to be accepted, where it expected 400 over 128 bytes.
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
        - `2026-11-06/0053-cas-size-bound.md:4` gains `; the 8 MiB bound stands, but the G35 and G38 refusals its 400 is compared with are replaced by hashing in ADR-aacruzgon-0079`, and its index cell `; the G35 and G38 refusals it cites hashed by [0079](2026-11-17/0079-hash-oversized-index-values.md)`. Its body (`:30`) stays as written.
        - `2026-11-06/0060-step3-scope.md:4` gains `; point 4's G38 bound is retired by ADR-aacruzgon-0079 (Step 5's extraction pull request removes it)`, and its index cell `; its G38 bound retired by [0079](2026-11-17/0079-hash-oversized-index-values.md)`. Its body (`:28`) stays as written.
    - **`ADR-aacruzgon.md`, the index:** this entry's row is added after 0078's.
    - **`development-roadmap.md`, Step 5's tasks (`:442`), in the first commit:** "Measure each row's `param` in the step 1 size check, as a backstop for the 128-byte bound …" becomes the mapping task, the removal of the 128-byte refusal, and a count of hashed values beside the `index_keys` sizes. PR #86 edits other lines of the same section; a trial merge with its branch (`git merge-tree`) is clean.
- **Links:**
    - this entry's pull request, PR #88, and its independent audit (`pr88-audit.md`);
    - ADR-aacruzgon-0026 (G35), 0027 (G38, G22), 0034, 0053, 0060;
    - ADR-sam-michaels-0006, IW6, IW9, IW11, SW11, RV1 and RV10, and its build order (PR #86);
    - Alan's review of PR #86, pullrequestreview-5941266315;
    - design step 1;
    - walkthrough 3.1, G35 and G38;
    - `coverage.md` sections 2, 4, 6 and 7;
    - [R4 datatypes.html#string](https://hl7.org/fhir/R4/datatypes.html#string), [R4 http.html#create](https://hl7.org/fhir/R4/http.html#create);
    - [PostgreSQL btree.html#BTREE-INTRO](https://www.postgresql.org/docs/current/btree.html#BTREE-INTRO).
