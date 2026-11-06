# ADR-aacruzgon-0028: Sign-off on `schema.cql` as merged at `67f9e0532`

- **Date:** 2026-11-06
- **Status:** accepted
- **Context:**
    - **What the roadmap asks.** Step 1, item 1.4: every member records their sign-off on `schema.cql`, or their objection, in their own ADR log. Items 1.1 to 1.3 are marked done only after that (`schema/README.md`).
    - **Why a revision is named.** The team agreed the schema before the audit. PR #33 then changed it. The audit (ADR-aacruzgon-0026), the open items (ADR-aacruzgon-0027) and CodeRabbit's five reviews of the PR led to:
        - three new columns: `resources_by_type.fhir_version`, `resource_versions.write_id`, and `index_keys` on `resources` and `resource_versions`;
        - live rows that carry `deleted_at = 0` (G39);
        - the rewrite of row N moved before the CAS (G40);
        - the outage, replacement and repair rules (G7);
        - a gate on Step 6 for scoped `_has` and chains (C7, HeliosSoftware/hfs#2501).
      A sign-off has to say which version it covers, so this one names the commit.
- **Decision:** I sign off, with no objection, on `schema.cql` and the documents that define it, as merged to `main` in PR #33 at commit `67f9e05327f70b05e755c2fafaccd666e89cd45d`:
    - `schema.cql` (ten tables) and `keyspace-single-node.cql`;
    - `expected-schema.txt`: 92 rows of keys, clustering orders, column types and table options;
    - the walk-through (`walkthrough.md`) and the coverage table (`coverage.md`);
    - the decisions they carry: ADR-aacruzgon-0008 to 0017 and 0024 to 0027.

  Any later change to a table, key, column type or table option needs a new sign-off entry from every member.
- **Alternatives considered:**
    - **Treat the team's earlier agreement as the sign-off.** Rejected: that agreement was to the draft before PR #33, which added three columns and changed the write path. Signing that draft would not cover the schema Step 2 builds on.
    - **Wait until all five members agree, and record one shared entry.** Rejected: the roadmap and `CLAUDE.md` put each member's sign-off in their own log, and only the owner writes in a log. Each member records their own against the same commit.
    - **Sign off on `schema.cql` alone, without the walk-through and the coverage table.** Rejected: the schema's correctness depends on the write order, the CAS conditions and the residuals those two documents define. A schema signed without them could be built with a write path it was not designed for.
- **Consequences:**
    - Roadmap item 1.4 is in progress with one of five sign-offs. Items 1.1 to 1.3 have their evidence (PR #33) but stay in progress until all five are recorded. The README says the schema is agreed and lists who has signed.
    - The other four members each record their sign-off, or objection, in their own log (`ADR-anastasiabryz`, `ADR-diya-patel83`, `ADR-sam-michaels`, `ADR-sara-lahourpour`) against `67f9e0532`. An objection reopens Step 1 for the point it names.
    - Step 2 (the backend skeleton) builds against this revision. Changing a table, key or column type from here needs a new ADR and new sign-offs.
    - Step 6 carries the four scope rows as a gate (ADR-aacruzgon-0027, C7). This sign-off does not waive them.
- **AI involvement:** Claude Code (Opus 5.5) drafted this entry from the merged files and the roadmap's Step 1 rules. Alan confirmed each line before the commit. The draft checked:
    - that PR #33 merged as `67f9e0532`;
    - that the `Cassandra schema (single node)` check passes on that commit ("schema matches docs/Cassandra/schema/expected-schema.txt (92 rows)", run 42169555958);
    - that `schema.cql` creates ten tables;
    - which columns PR #33 added, from the decisions in ADR-aacruzgon-0026 and 0027;
    - that no other member's log has a sign-off entry yet.
- **Links:** PR #33 (merge commit `67f9e0532`); ADR-aacruzgon-0026, 0027; `docs/Cassandra/schema/`; roadmap Step 1, items 1.1 to 1.4.
