# ADR log: aacruzgon (Alan Cruz)

This is Alan Cruz's personal Architecture Decision Record log for the Cassandra backend. One directory per collaborator, `ADR-<github username>/`, so that every design decision made while working with an AI assistant is attributed to the person who drove the session and can be reviewed later.

**Layout.** This file holds the rules, the template and the index. Each entry is its own file in a folder named for the day the decision was worked on:

```text
ADR-aacruzgon/
  ADR-aacruzgon.md                  rules, template, index (this file)
  2026-10-13/
    0001-image-pin-and-paxos-v2.md
    0002-container-resource-limits.md
  YYYY-MM-DD/
    NNNN-<short-slug>.md
```

**Rules**

- Append-only. A decision that is later reversed gets a new entry that supersedes the old one; the old entry's status changes to `superseded by ADR-aacruzgon-NNNN`, and nothing is deleted.
- One entry per decision, one file per entry. Numbers are sequential across the whole log, not per folder, so `ADR-aacruzgon-NNNN` stays a unique reference. The folder is the date the decision was worked on; a later entry that supersedes an earlier one goes in its own date folder.
- Written by the human, in their own words, at the end of the session in which the decision was made. The AI assistant may draft it, but the human confirms every line before it is committed.
- Only the owner of this log edits it. Another collaborator who disagrees with a decision opens an entry in their own log that references this one.
- The design document (`cassandra-discussion.md`) and the roadmap (`development-roadmap.md`) are updated in the same commit when a decision changes them. The ADR is the record of *why*; those documents are the record of *what*.

**What counts as a decision worth an entry**

A choice between two or more workable designs; a change to a trait surface, table, key, column type or consistency level; a change to the write path, failure handling or the sweeper; a dependency or driver choice; a benchmark methodology choice; anything that deviates from `cassandra-discussion.md` or `development-roadmap.md`; anything the AI proposed that the human rejected or materially altered.

**Entry template** (copy into `YYYY-MM-DD/NNNN-<short-slug>.md`, fill every field, delete none, and add a row to the index below):

```markdown
# ADR-aacruzgon-NNNN: <short title>

- **Date:** YYYY-MM-DD
- **Status:** proposed | accepted | superseded by ADR-<username>-NNNN
- **Context:** what question came up, and what constraint made it a decision rather than a routine change.
- **Decision:** what was decided, in one or two sentences.
- **Alternatives considered:** each alternative and the reason it lost.
- **Consequences:** what becomes easier, what becomes harder, what must be measured or revisited.
- **AI involvement:** tool and model used; what the AI proposed; what the human changed; what was verified and how (code read, test run, spec page, benchmark).
- **Links:** commit, pull request or issue; the section of the design document or roadmap affected.
```

## Index

| ADR | Date | Title | Status |
| --- | --- | --- | --- |
| [0001](2026-10-13/0001-image-pin-and-paxos-v2.md) | 2026-10-13 | Cassandra image, configuration file and `paxos_variant: v2` | accepted |
| [0002](2026-10-13/0002-container-resource-limits.md) | 2026-10-13 | Resource limits for the single-node container | accepted |
| [0003](2026-10-13/0003-snitch-and-dc-name.md) | 2026-10-13 | Snitch, data-centre, rack and cluster names | accepted |
| [0004](2026-10-13/0004-postgres-version.md) | 2026-10-13 | PostgreSQL version for the benchmark baseline | accepted |
| [0005](2026-10-13/0005-postgres-synchronous-commit.md) | 2026-10-13 | `synchronous_commit` for the PostgreSQL baseline | accepted |
| [0006](2026-10-13/0006-postgres-server-settings.md) | 2026-10-13 | PostgreSQL server settings inside the shared envelope | accepted |
| [0007](2026-10-13/0007-non-standard-host-ports.md) | 2026-10-13 | Non-standard host ports for the benchmark databases | accepted |
| [0008](2026-10-13/0008-keyspace-outside-schema-file.md) | 2026-10-13 | Keep the keyspace out of `schema.cql` | accepted |
| [0009](2026-10-13/0009-gc-grace-seconds.md) | 2026-10-13 | `gc_grace_seconds` per table | accepted |
| [0010](2026-10-13/0010-compaction-default.md) | 2026-10-13 | Compaction stays at the node default for every table | accepted |
| [0011](2026-10-13/0011-created-at.md) | 2026-10-13 | Where `created_at` lives and what revive does to it | accepted |
| [0012](2026-10-14/0012-rewrite-previous-version-row.md) | 2026-10-14 | Every write also rewrites the previous version's history row | accepted |
| [0013](2026-10-14/0013-sweeper-replays-versions.md) | 2026-10-14 | The sweeper replays the intent's version, even if superseded | accepted |
| [0014](2026-10-14/0014-sweeper-startup-scan.md) | 2026-10-14 | The sweeper scans every live bucket when it starts | accepted |
| [0015](2026-10-14/0015-outbox-clustered-by-time.md) | 2026-10-14 | `pending_index_writes` is clustered by `enqueued_at` first | accepted |
| [0016](2026-10-14/0016-write-ts-from-last-updated.md) | 2026-10-14 | `write_ts` is derived from `last_updated` | accepted |
| [0017](2026-10-14/0017-repair-on-read-rule.md) | 2026-10-14 | Repair on read deletes only keys the current version does not produce | accepted |
| [0018](2026-10-14/0018-ci-on-self-hosted-runner.md) | 2026-10-14 | CI runs on the team's self-hosted runner | accepted |
| [0019](2026-10-14/0019-required-check-no-skip-ci.md) | 2026-10-14 | The R4B compile check is required on `main`, and commits no longer use `[skip ci]` | accepted |
| [0020](2026-10-14/0020-cassandra-test-harness-rules.md) | 2026-10-14 | Cassandra tests connect to a running node and isolate their data by tenant | accepted |
| [0021](2026-10-15/0021-item-0.5-required-checks.md) | 2026-10-15 | Item 0.5's four checks are required on `main`, the Cassandra schema job included | accepted |
| [0022](2026-10-15/0022-require-sha-pinned-actions.md) | 2026-10-15 | Every action is pinned to a commit SHA, and the repository requires it | accepted |
| [0023](2026-11-04/0023-light-heavy-runners-acruzdebian.md) | 2026-11-04 | CI splits into light and heavy jobs, and `acruzdebian` joins as a second runner for light jobs only | accepted |
| [0024](2026-11-05/0024-create-or-update-race-loop.md) | 2026-11-05 | `create_or_update` is a bounded CAS loop on every branch, with one intent row per attempt and a fixed rule for unknown outcomes | accepted |
| [0025](2026-11-05/0025-readhistory-stays-true.md) | 2026-11-05 | `readHistory` stays `true` on Cassandra; only the history interactions are omitted | accepted |
| [0026](2026-11-05/0026-schema-audit-fixes.md) | 2026-11-05 | Schema audit fixes: compartment and FHIR-version residuals, a 2 KiB limit on indexed values, `write_id` on version rows, and intersected `_id` parameters | accepted |
