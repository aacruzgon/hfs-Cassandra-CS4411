# ADR log: sara-lahourpour (Sara Lahourpour)

This is Sara Lahourpour's personal Architecture Decision Record log for the Cassandra backend. One directory per collaborator, `ADR-<github username>/`, so that every design decision made while working with an AI assistant is attributed to the person who drove the session and can be reviewed later.

**Layout.** This file holds the rules, the template and the index. Each entry is its own file in a folder named for the day the decision was worked on:

```text
ADR-sara-lahourpour/
  ADR-sara-lahourpour.md                  rules, template, index (this file)
  YYYY-MM-DD/
    NNNN-<short-slug>.md
```

**Rules**

- Append-only. A decision that is later reversed gets a new entry that supersedes the old one; the old entry's status changes to `superseded by ADR-sara-lahourpour-NNNN`, and nothing is deleted.
- One entry per decision, one file per entry. Numbers are sequential across the whole log, not per folder, so `ADR-sara-lahourpour-NNNN` stays a unique reference. The folder is the date the decision was worked on; a later entry that supersedes an earlier one goes in its own date folder.
- Written by the human, in their own words, at the end of the session in which the decision was made. The AI assistant may draft it, but the human confirms every line before it is committed.
- Only the owner of this log edits it. Another collaborator who disagrees with a decision opens an entry in their own log that references this one.
- The design document (`cassandra-discussion.md`) and the roadmap (`development-roadmap.md`) are updated in the same commit when a decision changes them. The ADR is the record of *why*; those documents are the record of *what*.

**What counts as a decision worth an entry**

A choice between two or more workable designs; a change to a trait surface, table, key, column type or consistency level; a change to the write path, failure handling or the sweeper; a dependency or driver choice; a benchmark methodology choice; anything that deviates from `cassandra-discussion.md` or `development-roadmap.md`; anything the AI proposed that the human rejected or materially altered.

**Entry template** (copy into `YYYY-MM-DD/NNNN-<short-slug>.md`, fill every field, delete none, and add a row to the index below):

```markdown
# ADR-sara-lahourpour-NNNN: <short title>

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
| [0001](2026-11-13/0001-race-checks-accept-504-at-rf-2.md) | 2026-11-13 | The race checks accept a 504 at RF 2 or more, read from the keyspace, and check the history instead | accepted |
