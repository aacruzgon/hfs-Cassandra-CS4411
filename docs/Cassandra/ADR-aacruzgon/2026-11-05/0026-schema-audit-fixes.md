# ADR-aacruzgon-0026: Schema audit fixes: compartment and FHIR-version residuals, a 2 KiB limit on indexed values, `write_id` on version rows, and intersected `_id` parameters

- **Date:** 2026-11-05
- **Status:** accepted
- **Context:**
    - **Why an audit.** Before the team records its sign-off on the schema (roadmap row 1.4), Alan asked for an independent review of `schema.cql`, the walkthrough, the coverage table and the design, against the code at `main` after the upstream sync (PR #28) and PR #32. A defect found now costs an edit; after Step 2 it costs a data migration.
    - **What the audit found that needed a decision.** Five gaps, each with workable alternatives:
        - **G33, compartment.** REST sets `SearchQuery.compartment` and relies on the backend to apply it:
            - for every non-Patient search under a SMART `patient/` scope (`crates/rest/src/patient_compartment.rs:183-201`, upstream #1618);
            - for compartment searches (`handlers/compartment.rs:142`, `331`);
            - for `$everything` (`handlers/everything/scope.rs:92`).

          No table indexes references, and the design neither served nor refused the field. A backend that ignored it would return other patients' resources.
        - **G34, FHIR version.** Upstream #2254 added `SearchQuery.fhir_version` (`types/search_params.rs:744-753`), set when the client names a `fhirVersion` MIME parameter (`handlers/search.rs:557`). Tag-checked searches can filter on `resources.fhir_version`, but `count` and an unfiltered `search_count` read only `resources_by_type`, which had no version.
        - **G35, oversized values.** Checked on a 5.0.9 node:
            - a 70,000-byte `code` in a partition key fails ("Key length of 70021 is longer than maximum of 65535");
            - so does a 70,000-byte clustering value;
            - so does a composite partition key whose parts sum to 80,021 bytes.

          FHIR strings may be up to 1 MB, and nothing in the shared extractor bounds an indexed value. The index batch would fail at step 5, after the compare-and-swap applied, and no retry, sweep or repair could finish the write.
        - **G36, unknown outcomes.** ADR-aacruzgon-0024 made "the serial read shows a version past ours" undecidable (504 or 503), because `resource_versions` had no `write_id` (G16 had dropped it when the sweeper stopped comparing it).
        - **G37, `_id`.** A `patient/` scope appends `_id=<patient>` to every Patient search (`patient_compartment.rs:172-181`), and the coverage table defined only one `_id` parameter.
    - **Why one new entry rather than an amendment to 0024.** This log is append-only (`ADR-aacruzgon.md`, "Rules"). G36 refines one rule of 0024 without reversing it, so 0024 keeps status `accepted`, and this entry records the change and references it.
- **Decision:**
    - **G33.** `query.compartment` is a residual filter on every candidate that passed the tag check:
        - re-extract the row with `SearchParameterExtractor::extract`, under its own `fhir_version` and the tenant's registry;
        - keep it only if one of its reference values for `compartment.params` equals `strip_reference_version(compartment.reference)` or starts with it followed by `/_history/`. This is SQLite's and MongoDB's rule (`backends/sqlite/search/query_builder.rs:863-896`, `backends/mongodb/search_impl.rs:847-862`).

      The parameters are the bundled CompartmentDefinition's (`helios_fhir::get_compartment_params`). Its R4 Patient entries match the [R4 Patient CompartmentDefinition](https://hl7.org/fhir/R4/compartmentdefinition-patient.html) for every type checked, for example "Observation: subject or performer".

      The rows read count against the same candidate cap as a date-only search. Past it, the search or count fails with `SearchError::TooManyResults`, which REST maps to 422 with issue `processing` (`crates/rest/src/error.rs:1123-1125`, `591-595`). The backend never returns a page or count that was only partly filtered. `search_count` and `_total` under a compartment run the same filter to the end. If the team decides G27 differently, the compartment cap follows.
    - **G34.** `resources_by_type` gains `fhir_version text`. X1 and the sweeper write it with the resource's own value, which is fixed at create.
        - Tag-checked searches compare `query.fhir_version` with `resources.fhir_version`.
        - `count` and an unfiltered `search_count` compare it with `resources_by_type.fhir_version` while they scan, in Rust, with no `ALLOW FILTERING` and no point reads.
    - **G35.** Before step 2, every `code`, `system` and `uri` value of the new index rows is measured in UTF-8 bytes. If any exceeds 2,048 bytes, the write is refused with `StorageError::Validation(ValidationError::InvalidResource { message, details })`, one `ValidationDetail` per offending element. REST answers 400 with OperationOutcome issue code `invalid` (`crates/rest/src/error.rs:1048`, `570-571`), and nothing is written.

      Each value is measured on its own, not the whole composite key:
        - the values are what the client controls;
        - every other component is bounded by the server: the tenant id at 64 bytes (`MAX_TENANT_ID_LEN`), the id at 64, and the resource type and parameter code from the registry;
        - so the largest key, a 2,048-byte system plus a 2,048-byte code, is about 4.3 KB. That is far below 65,535 bytes, fits one row in a 5 KiB batch, and gives the same answer in every tenant.
    - **G36.** `resource_versions` gains `write_id uuid`.
        - **Who writes it.** V1 and V2 write the attempt's own `write_id`. The sweeper's replay writes the one on the `resources` row. A V3 rewrite built from a read of `resources` (`delete`, `create_or_update`, `update_with_match`) carries that read's `write_id`. A V3 built from `update`'s `StoredResource`, which has none, leaves it unset.
        - **How it decides.** When the serial read after an unknown CAS outcome shows a version past ours, R3 on row V decides it. This refines ADR-aacruzgon-0024's table:
            - our `write_id` means applied and then superseded: success with version V, no index writes, and the intent row left for the sweeper;
            - another `write_id` is a definite miss;
            - a missing row or an unset `write_id` stays undecidable (504 or 503, as 0024 says). That is the case when the superseding version came from `update`.
    - **G37.** All `_id` parameters are intersected in Rust before any read, and only the intersection is point-read. `_id` is never checked against an index table.
- **Alternatives considered:**
    - **G33:**
        - Refuse `query.compartment` with 400. This lost: patient-scoped SMART apps and compartment searches would fail on every non-Patient type.
        - A reference index table. This lost: reference search is ✗ in this project, and it would be a fourth token-like table written on every update.
    - **G34:** No column, routing version-filtered counts through tag-checked reads. This lost: one point read per row for every `_count=0` or `_total` that names a version, where one text column costs a few bytes per row.
    - **G35:**
        - Skip the oversized row and log it. This lost: the resource would be silently unsearchable by that value.
        - Store a hash of long values. This lost: the key columns would change meaning, and every search would need a residual comparison.
        - Cassandra's own 65,535-byte limit. This lost: a single row could then break the 5 KiB batch rule, and the limit depends on the whole composite key, so it varies with the tenant id and table.
    - **G36:** Keep every superseded outcome undecidable. This lost: the column costs 16 bytes per version and turns a 504 into a definite answer whenever the next writer was a `delete`, `create_or_update` or `update_with_match`.
    - **Recording G36 as a dated amendment line in ADR-0024**, as PR #32's review was. This lost: the log is append-only, and this entry already records the rest of the audit.
- **Consequences:**
    - **Easier:**
        - patient-scoped and compartment searches work on a Cassandra primary, and can never leak;
        - version-filtered counts cost no extra reads;
        - no client value can produce an index row Cassandra refuses;
        - fewer unknown CAS outcomes reach the client as 504 or 503.
    - **Harder or costlier:**
        - a compartment search with no selective parameter reads the type's `resources_by_type` partition and re-extracts each row, so the cap's 422 will be reached on large tenants;
        - an `Identifier.value`, `Coding.code` or URI over 2,048 bytes is now a 400, stricter than the other backends, which accept it;
        - V3 on the `delete` and `create_or_update` paths must carry R1's `write_id`.
    - **To measure:**
        - the compartment residual's cost per page and how often the cap is reached (roadmap section 7);
        - whether any fixture or benchmark resource carries a value over 2 KiB.
    - **Tests:**
        - `fhir_version_filter_suite.rs` needs an R4+R5 test build that no CI job makes, and it also uses `_offset` and `_contained`. Its in-scope cases run by hand from a copy in `tests/cassandra/`. No CI job is added.
        - The roadmap's Step 6 gates now name only suites, or parts of suites, that can pass in scope; `total_mode_suite.rs` was added.
    - **CI:** the `Cassandra schema (single node)` job now asserts table names, key and clustering columns with their order, and the table options the design relies on, read from `system_schema`.
    - **Left open here, and decided in ADR-aacruzgon-0027:** whether the intent row keeps its unread `write_id` (G32), and G7, G20, G23, G27, C1, C4, C8 to C11, P5 and P6. That entry also records where the code did not fit the recommendations and what replaced them.
- **AI involvement:**
    - **Tools:** Claude Code (Opus 5.5). An orchestrating Claude Code session ran a Rex worker session in its own worktree, which used web-fetch subagents for the FHIR R4 and R4B search pages and the R4 Patient CompartmentDefinition.
    - **What the AI proposed:**
        - The worker audited the schema, the walkthrough, the coverage table, the design, ADRs 0008 to 0017, 0024 and 0025, the traits and the shared suites. Its report had 2 blocking, 4 should-fix and 12 minor findings and 6 open questions.
        - For each question it listed options. For the compartment filter: refuse, residual, or a reference table. For the version filter: a column or tag-checked reads. For oversized values: skip, reject or hash. For G36: add `write_id` to version rows.
        - The orchestrating Claude Code session recommended, for each question: the residual, with an error rather than a partial result at the cap and a count that is correct or rejected; the column; rejection with 400 at 2 KiB; and adding `write_id` now.
        - The worker then drafted the edits and this entry.
    - **What the human changed:** Alan accepted these recommendations. The orchestrating session confirmed the worker's reading that an unknown outcome stays undecidable only when the version that superseded ours came from `update`; Alan approved the ADR with it. Q5 (the intent row's `write_id`) and Q6 (the existing open points) were left to ADR-aacruzgon-0027.
    - **Verified:**
        - Code read at `main` (`15f2850d9`): `SearchQuery.fhir_version` and `compartment`; the REST code that sets them; the SQLite and MongoDB membership filters; the `ValidationError` and `SearchError` variants and their REST mapping; `MAX_TENANT_ID_LEN`; and every suite named in the roadmap's Step 6 gate.
        - On a fresh 5.0.9 node, the key-size limits and batch-size behaviour were measured as quoted above. The revised `schema.cql` was applied twice, the changed statements were run, and the CI assertions passed, then failed when a key or clustering order was altered. The node was removed afterwards.
        - Spec pages fetched: [R4 search.html](https://hl7.org/fhir/R4/search.html), [R4B search.html](https://hl7.org/fhir/R4B/search.html) and [R4 compartmentdefinition-patient.html](https://hl7.org/fhir/R4/compartmentdefinition-patient.html).
- **Links:**
    - **Files:** `docs/Cassandra/schema/schema.cql` (`resources_by_type.fhir_version`, `resource_versions.write_id`, the statement rules); `schema/walkthrough.md` (3.1 size check and unknown outcome, 5.4, 5.8, 5.9, 5.11, G32 to G37); `schema/coverage.md` (sections 2, 5, 6 and 7); `schema/README.md`; `cassandra-discussion.md` (scope note, FHIR versions, data model, write path steps 1 and 4, unknown outcome, sweeper, capabilities and rejection, open questions); `development-roadmap.md` (Step 1 checklist, cursor task, rows 6.4, 6a and 6b, Steps 6 and 6a to 6d, section 5); `spike-findings.md` (B1 note); `.github/workflows/fmt-clippy-cassandra.yml` (the schema job's assertions).
    - **Refines:** ADR-aacruzgon-0024 (the unknown-outcome rule). **Revises:** walkthrough G16 (from ADR-aacruzgon-0013).
    - **Pull request:** [PR #33](https://github.com/aacruzgon/hfs-Cassandra-CS4411/pull/33), from branch `docs/schema-audit-fixes`.
