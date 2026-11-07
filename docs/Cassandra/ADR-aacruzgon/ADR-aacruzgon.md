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
| [0027](2026-11-05/0027-step-1-open-items-decided.md) | 2026-11-05 | The open items are decided: `count`, date-only paging, stored index keys, the cap, the unread rows, `_sort`, the rejections, the sweeper and the schema's setup | accepted |
| [0028](2026-11-06/0028-schema-sign-off.md) | 2026-11-06 | Sign-off on `schema.cql` as merged at `67f9e0532` | accepted |
| [0029](2026-11-06/0029-cassandra-test-node.md) | 2026-11-06 | How the Cassandra test node is reached, started and shared | accepted |
| [0030](2026-11-06/0030-ci-cassandra-tests.md) | 2026-11-06 | The Cassandra job builds its tests before starting the node, and stays on the light runners | accepted |
| [0031](2026-11-06/0031-cassandra-config-names.md) | 2026-11-06 | `HFS_CASSANDRA_*` configuration names and defaults | accepted |
| [0032](2026-11-06/0032-cassandra-driver-settings.md) | 2026-11-06 | `cdrs-tokio` session settings: protocol v5, local-DC routing, no driver re-send of a CAS, and a client deadline | accepted |
| [0033](2026-11-06/0033-create-writes-resources-by-type.md) | 2026-11-06 | Step 2's `create` writes the `resources_by_type` row, and `count` is real at Milestone A | accepted |
| [0034](2026-11-06/0034-create-details.md) | 2026-11-06 | The choices inside Step 2's `create`: post-CAS retries, every attempt's `write_id`, id and code checks | accepted |
| [0035](2026-11-06/0035-write-timeout-simple.md) | 2026-11-06 | A CAS `WriteTimeout` with `write_type = SIMPLE` is confirmed by the serial read, not taken as applied | accepted |
| [0036](2026-11-06/0036-capability-instance-tests-skip.md) | 2026-11-06 | The capability contract's Cassandra instance tests skip without `HFS_TEST_CASSANDRA_URL` instead of starting the fallback container | accepted |
| [0037](2026-11-06/0037-declare-capabilities-as-they-land.md) | 2026-11-06 | Declare each Cassandra capability when its step lands, not at Step 2 | accepted |
| [0038](2026-11-06/0038-testable-reset-rotates-tenant.md) | 2026-11-06 | The Cassandra `TestableBackend` resets by rotating to fresh tenants | accepted |
| [0039](2026-11-06/0039-compile-shared-harness.md) | 2026-11-06 | `cassandra_tests` compiles the shared test harness as upstream wrote it, with one fix to `fixtures.rs` | accepted |
| [0040](2026-11-06/0040-cassandra-server-wiring.md) | 2026-11-06 | How the server selects and starts the Cassandra backend | accepted |
| [0041](2026-11-06/0041-stored-search-parameters-reload.md) | 2026-11-06 | Cassandra reloads every tenant's stored SearchParameters before it serves, listing the tenants from `resources_by_type`, and startup fails if the load fails | accepted |
| [0042](2026-11-06/0042-milestone-a-in-ci.md) | 2026-11-06 | Milestone A runs in the required Cassandra job, which stays on the light runners, and stops that job's own node | accepted |
| [0043](2026-11-06/0043-seeder-boot-time.md) | 2026-11-06 | The startup budget is 10 s of seeding per fresh tenant on the single node (provisional), and the conformance seed keeps the default `create_many` | accepted |
| [0044](2026-11-06/0044-upgrade-cdrs-tokio.md) | 2026-11-06 | Upgrade `cdrs-tokio` to 10.0.0, and drop the workarounds it makes unnecessary | accepted |
| [0045](2026-11-06/0045-ci-reports-and-boot-time.md) | 2026-11-06 | CI reports measured figures on every run, starting with the boot time; they never fail a job | accepted |
| [0046](2026-11-06/0046-more-ci-reports.md) | 2026-11-06 | More CI reports: latency, dependency freshness, advisories, sizes and step timings | accepted |
| [0047](2026-11-06/0047-coverage-report-in-ci.md) | 2026-11-06 | A Cassandra coverage report in CI, in its own workflow, not required and with no threshold | accepted |
| [0048](2026-11-06/0048-more-ci-reports-node-and-build.md) | 2026-11-06 | More CI reports: the R4B build, cargo timings, the node's start, its JVM and container, hfs's memory, and the stored-SearchParameter load | accepted |
| [0049](2026-11-06/0049-backport-upstream-2518.md) | 2026-11-06 | Carry upstream's versioned-`Location` fix (HeliosSoftware/hfs#2518) in the fork ahead of upstream, as `-x` cherry-picks | accepted |
| [0052](2026-11-06/0052-backport-upstream-2529.md) | 2026-11-06 | A delete with nothing to delete answers 200/204, through upstream's fix (HeliosSoftware/hfs#2529) carried as a `-x` cherry-pick with one hunk left out | accepted |
