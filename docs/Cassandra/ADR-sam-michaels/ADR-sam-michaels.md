# ADR log: sam-michaels (Samuel Michaels Rodriguez)

This is Samuel Michaels Rodriguez's personal Architecture Decision Record log for the Cassandra backend. One directory per collaborator, `ADR-<github username>/`, so that every design decision made while working with an AI assistant is attributed to the person who drove the session and can be reviewed later.

**Layout.** This file holds the rules, the template and the index. Each entry is its own file in a folder named for the day the decision was worked on:

```text
ADR-sam-michaels/
  ADR-sam-michaels.md                  rules, template, index (this file)
  YYYY-MM-DD/
    NNNN-<short-slug>.md
```

**Rules**

- Append-only. A decision that is later reversed gets a new entry that supersedes the old one; the old entry's status changes to `superseded by ADR-sam-michaels-NNNN`, and nothing is deleted.
- A later entry may supersede a single ID of a multi-decision entry by naming it, for example "supersedes ADR-sam-michaels-0006, IW15". In the same commit the earlier entry gains only an added, dated note naming that ID and the later entry (for example "*Update YYYY-MM-DD:* IW15 is superseded by ADR-sam-michaels-NNNN"), and its index row's status gains "partly superseded by NNNN (IW15)". The earlier entry's existing text, that ID's row included, is not edited, and the rest of that entry stands.
- One entry per decision, one file per entry. Numbers are sequential across the whole log, not per folder, so `ADR-sam-michaels-NNNN` stays a unique reference. The folder is the date the decision was worked on; a later entry that supersedes an earlier one goes in its own date folder.
- Written by the human, in their own words, at the end of the session in which the decision was made. The AI assistant may draft it, but the human confirms every line before it is committed.
- Only the owner of this log edits it. Another collaborator who disagrees with a decision opens an entry in their own log that references this one.
- The design document (`cassandra-discussion.md`) and the roadmap (`development-roadmap.md`) are updated in the same commit when a decision changes them. The ADR is the record of *why*; those documents are the record of *what*.

**What counts as a decision worth an entry**

A choice between two or more workable designs; a change to a trait surface, table, key, column type or consistency level; a change to the write path, failure handling or the sweeper; a dependency or driver choice; a benchmark methodology choice; anything that deviates from `cassandra-discussion.md` or `development-roadmap.md`; anything the AI proposed that the human rejected or materially altered.

**Entry template** (copy into `YYYY-MM-DD/NNNN-<short-slug>.md`, fill every field, delete none, and add a row to the index below):

```markdown
# ADR-sam-michaels-NNNN: <short title>

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
| [0001](2026-10-14/0001-self-hosted-runner-registration.md) | 2026-10-14 | Self-hosted runner registered on the 7900X rig, run by a dedicated non-admin user with rootless Docker | accepted; networking superseded by ADR-sam-michaels-0004 |
| [0002](2026-10-14/0002-persistent-target-dir-instead-of-rust-cache.md) | 2026-10-14 | CI keeps a bounded cargo target directory on the runner instead of using rust-cache | accepted |
| [0003](2026-10-14/0003-fmt-clippy-cassandra-jobs.md) | 2026-10-14 | Item 0.5's fmt, clippy and Cassandra jobs, clippy split in two under a memory cap, and the weekly Docker prune | accepted |
| [0004](2026-10-14/0004-rootless-docker-slirp4netns.md) | 2026-10-14 | gh-runner's rootless Docker uses slirp4netns with the builtin port driver, not pasta | accepted |
| [0005](2026-10-15/0005-upstream-canary.md) | 2026-10-15 | The upstream canary merges upstream nightly, runs 0.4's check, and keeps one "upstream drift" issue | accepted |
| [0006](2026-11-16/0006-step-5-design.md) | 2026-11-16 | Step 5's design: index rows by partition, the version row first, a quorum-first sweeper, bounded repair and a report-first CI check | accepted; amended on 2026-11-17 and 2026-11-18 after Alan's two reviews on #86; partly superseded by [0007](2026-11-17/0007-consistency-check-scan-modes.md) (RV13) |
| [0007](2026-11-17/0007-consistency-check-scan-modes.md) | 2026-11-17 | The consistency check's scan modes, and the extraction helper's seat in pull request 4 | accepted |
