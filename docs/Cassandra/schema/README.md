# Cassandra schema: draft for the schema session

**Status: DRAFT. Nothing here is signed off.** This is the starting point for roadmap Step 1 (items 1.1 to 1.4).

**"The schema session"** in these files means the two team design sessions that roadmap Step 1 plans for week 1 (13 to 18 October), led by the owner of the shared files (**W**). The team decides there. Each member then records their sign-off, or objection, in their own ADR log (item 1.4). Rows 1.1 to 1.3 are marked done only after that.

Read [`spike-findings.md`](../spike-findings.md) first, then this page. The other files are reference.

| File | What it is | Roadmap item |
| --- | --- | --- |
| [`schema.cql`](schema.cql) | The ten tables, keys and table options, as the design document fixes them (revised by ADR-aacruzgon-0012 to 0017) | 1.1 |
| [`keyspace-single-node.cql`](keyspace-single-node.cql) | The keyspace for one local node, plus the `system_traces` setup step | 1.1 |
| [`walkthrough.md`](walkthrough.md) | Every in-scope operation walked through the tables in CQL, one Patient's worked example, and the gaps G1 to G31 | 1.2 |
| [`coverage.md`](coverage.md) | The query coverage table: parameter × form or prefix × table × expected result, plus every rejection | 1.3 |

## Apply it

```bash
docker compose -f docker/cassandra/docker-compose.yml up -d --wait
docker compose -f docker/cassandra/docker-compose.yml cp docs/Cassandra/schema/keyspace-single-node.cql cassandra:/tmp/
docker compose -f docker/cassandra/docker-compose.yml cp docs/Cassandra/schema/schema.cql cassandra:/tmp/
docker compose -f docker/cassandra/docker-compose.yml exec cassandra cqlsh -f /tmp/keyspace-single-node.cql
docker compose -f docker/cassandra/docker-compose.yml exec cassandra cqlsh -k helios -f /tmp/schema.cql
```

`schema.cql` does not name a keyspace. It is applied with `-k`, so the backend and the tests can use any keyspace name, for example one per test run. Every statement is `IF NOT EXISTS`, so applying it twice changes nothing.

## The checklist, as drafted

These are the items the roadmap's Step 1 checklist asks `schema.cql` to state. "As designed" means the draft copies the design document. "Accepted" means the draft chose something the design left open, and Alan accepted it (ADR-aacruzgon-0008 to 0011); the team reviews it at the session.

| Item | Draft | Why | |
| --- | --- | --- | --- |
| Keyspace | `helios`, `NetworkTopologyStrategy`, `'helios-dc1': 1` locally, `3` on the cluster. Kept in its own file. | The data-centre name matches `docker/cassandra`. A separate file means the replication factor and keyspace name are never baked into the table file. | Accepted |
| Common types | `id text`, `version_id int`, timestamps in milliseconds, `fhir_version text` as `as_mime_param()`, `write_ts bigint` in microseconds and always `last_updated × 1000`, `write_id uuid` | As the checklist says; the `write_ts` rule is ADR-0016 | As designed |
| `resources` columns | `data text`, `last_updated`, `created_at`, `deleted_at`, `fhir_version`, `write_ts`, `write_id`, `version_id` | `text` is the checklist's default; table compression (LZ4) is on anyway | As designed |
| `created_at` | Only on `resources`. Kept when a resource is revived. `vread` passes the version's own `last_updated`. | MongoDB keeps the original `created_at`; PostgreSQL stores none and passes `last_updated`. Both backends give a historical version its own `last_updated`. | Accepted |
| `resource_versions` columns | `data` (unset for a delete marker), `last_updated`, `deleted_at`, `fhir_version`, `write_ts` | As the checklist says. A delete marker has no content, as in PostgreSQL. | As designed |
| `resources_by_type` and the token, date and URI tables | Keys and clustering order exactly as the checklist; `param text` in every index partition key; `''` for "no system" | As designed. A node confirmed `start` and `end` work as column names; `token` is reserved and is not used. | As designed |
| `pending_index_writes` | Clustering `(enqueued_at, tenant_id, resource_type, id)`; `write_id` and `version_id`; `default_time_to_live = 86400` | The key order is ADR-0015 and `version_id` is ADR-0013 | As designed |
| `gc_grace_seconds` | 10,800 s (3 h) on the six index tables, `resources_by_type` **and `pending_index_writes`**; the 10-day default on `resources` and `resource_versions` | 3 h equals the node's `max_hint_window`, so a replica that was down for less than that still receives its hinted deletes. A resurrected row is harmless here: the tag check hides index rows, and a resurrected intent row makes the sweeper either delete it or replay writes that change nothing. `resources` keeps 10 days for a second reason: with the default `paxos_state_purging` (legacy), Paxos state for a table lives for `max(3 h, gc_grace_seconds)`. | Accepted (ADR-0009); the roadmap checklist now lists `pending_index_writes` too |
| Compaction | No table sets it: all use the node default, UCS with `T4` | See [Compaction](#compaction) | Accepted |
| Statements that must never exist | Listed at the top of `schema.cql`: a `DELETE` on `resources`, a bound `null`, `ALLOW FILTERING`, and any plain (non-LWT) write to `resources` | The last one is new. A plain write bypasses Paxos, so it would break the compare-and-swap. | As designed, plus one |
| `system_traces` | Set to RF 1 in `keyspace-single-node.cql`, a developer step; never run by the backend or on the cluster | Spike finding 1 | Accepted |

### Compaction

The Apache docs recommend UCS "for new workloads". They say tiered (`T`) settings suit write-heavy workloads and levelled (`L`) settings suit read-heavy ones "especially if bloom filters are not effective (e.g. with wide partitions)". The draft keeps `T4` everywhere until the measurements in roadmap section 7 say otherwise:

| Table | Workload | Draft | What would change it |
| --- | --- | --- | --- |
| `resources` | One row per partition, overwritten on every version, read by full key | `T4`: bloom filters work well for single-row point reads | Point-read p95 above budget |
| `resource_versions` | Append-only per partition | `T4` | Nothing expected |
| Six index tables, `resources_by_type` | Delete plus insert on every write; slice reads through tombstones | `T4` | Tombstones per read over 1,000 (the section 7 trigger): try `L4`, which merges overlapping SSTables sooner (an inference; the docs do not compare the two for tombstones) |
| `pending_index_writes` | Insert, then delete within seconds; 24 h TTL; read by time slice | `T4` | Tombstones per sweeper read above a few hundred. The docs recommend TimeWindowCompactionStrategy for TTL data, but these rows are deleted explicitly, long before they expire |

## Changes made to the design

The walk-through found places where the design document was wrong. These are now fixed in `cassandra-discussion.md`, in the roadmap and in `schema.cql`, each with an entry in `ADR-aacruzgon/`. Every member reviews them with the rest of the schema at the session.

| ADR | Problem found (walk-through gap) | Change | Schema change |
| --- | --- | --- | --- |
| 0012 | **A version's content could be lost for good (G2).** A crash between the CAS and the `resource_versions` write, followed by another update, overwrote that version in `resources` before it reached history. | Every update, delete and revive also rewrites the row for the version it replaced, from `current`, one microsecond below that version's `write_ts`, so an existing row always wins. | None |
| 0013 | **The sweeper left orphan index rows (G1).** "A later version's intent covers it" was wrong: the later version deletes only the keys of the version it read. | The intent row carries the `version_id` its CAS will create. If that version is current, the sweeper replays its writes at its own `write_ts`. If it has been superseded, the sweeper only deletes the previous version's keys. | `version_id int` on `pending_index_writes` |
| 0014 | **The sweeper only looked back one hour (G6),** while intents live 24 h. After an outage, a crashed create stayed unsearchable for good. | At startup the sweeper scans the 25 most recent buckets (a 24 h TTL can span 25), one short time slice per query. | None |
| 0015 | **Each sweeper read scanned every finished intent (G5). Measured:** 3,000 tombstones and a logged warning on one read. | `enqueued_at` leads the clustering key, and the sweeper reads only a time slice past a low-water mark. On a node that read scanned 0 tombstones. | Clustering `(enqueued_at, tenant_id, resource_type, id)` |
| 0016 | **The `write_ts` formula could not be computed (G4).** `update` receives a `StoredResource`, which has no `write_ts`. | `last_updated(N+1) = max(now_ms, last_updated(N) + 1 ms)` and `write_ts = last_updated × 1000`, so any version's `write_ts` can be recomputed. | None |
| 0017 | **Repair on read could delete a key the current version needs (G10),** because a delete wins a timestamp tie with the writer's insert. | Repair deletes a stale key only if the current version does not produce it; otherwise it upserts the key with the current tag. | None |
| 0024 | **Lost PUT races and unknown CAS outcomes (G13, G25, G26; upstream #2111).** The design retried `create_or_update` only for batch, mapped a missed CAS straight to 404 or 409, answered an unknown CAS outcome that was not ours with 503, and both deleted and rewrote one intent row across attempts. | `create_or_update` is one bounded loop over create, update and revive: a fresh read, `write_id` and intent row per attempt, at most 3 immediate retries after a definite miss, and 2 after a timed-out CAS the serial read proves did not apply (5 to 25 ms pause). An undecidable or timed-out serial read is 504. A plain delete that loses to an update retries once. A revive keeps the original `fhir_version`, and its literal `deleted_at = null` is the one allowed null. | Comments only (`write_id`, `pending_index_writes`, the null exception) |

Two corrections need no ADR because they fix a statement, not a decision:
- **The sweeper does compare clocks.** "No clocks are compared" was wrong: the grace period is a clock comparison. The design now states its assumption (G11).
- **A write is 404, never 410, when the resource was deleted in the gap** (G9). 410 is for `read` and `vread` only.

## Still proposed

| # | Problem (walk-through gap) | Proposal | Schema change |
| --- | --- | --- | --- |
| P5 | **Index rows nobody reads (G17, G18).** The extractor emits `_id` and `_lastUpdated` (served by `resources` and `resources_by_type`), display-only tokens with an empty code, and implicit-system rows in `token_system_index`. | Drop them before computing index keys | None |
| P6 | **Several Helios processes would apply the schema at once.** Roadmap Step 2 has `initialize` apply `schema.cql`. The Apache docs say nothing about concurrent schema changes; driver documentation advises waiting for schema agreement. | Apply the schema from one process (a setup command or the test harness). `initialize` only checks that the tables exist. | None |

## Decisions only the team can make

The full list is the "Decisions to record" group in [`walkthrough.md`](walkthrough.md#8-gaps-and-questions-for-the-session), plus the coverage-specific points in [`coverage.md`](coverage.md#7-open-points-for-the-session). The ones that change what `schema.cql` says, or what a test expects:

- **`count` (G7).** `count` reads `resources_by_type` without the tag check. So it counts writes in flight, orphans a crash left until the sweeper removes them, and, on the cluster, any row resurrected because `gc_grace_seconds` was lowered. Either accept that and document it, or tag-check `count`, which costs one point read per row.
- **`count(tenant, None)`** (all types) is called by the console metrics. No table can answer it without a list of resource types.
- **Too costly (G27).** The design returns `too-costly` for a date-only search over the candidate cap, but no search error produces that issue code. The nearest is `TooManyResults`, which gives 422. The one `too-costly` in Helios is a 500 for a transaction Bundle too large for MongoDB's cache (`crates/rest/src/handlers/batch.rs`).
- **`_profile`, `_source`, `_tag`, `_security`.** The extractor indexes them. Patient and Observation have no URI parameter of their own in R4, so `_profile` and `_source` are the only URI rows the coverage table has.

## What the Apache Cassandra 5.0 docs say

Read for this draft. Each line was checked against the 5.0 documentation source.

- **Compaction.** UCS "is a good choice for most workloads and is recommended for new workloads" and "is designed to be able to handle both immutable time-series data and workloads with lots of updates and deletes" ([overview](https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/compaction/overview.html)). Levelled settings are "preferable for read-heavy workloads, especially if bloom filters are not effective"; tiered settings are "preferable for write-heavy workloads" ([UCS](https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/compaction/ucs.html)). "Setting any `compaction` sub-options will erase *ALL* previous `compaction` options" ([DDL](https://cassandra.apache.org/doc/5.0/cassandra/developing/cql/ddl.html)).
- **Tombstones and repair.** A deleted row that comes back is a "zombie", and the grace period exists to prevent it ([tombstones](https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/compaction/tombstones.html)). "At a minimum, repair should be run often enough that the gc grace period never expires on unrepaired data" ([repair](https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/repair.html)). Hints are kept for `max_hint_window` (3 h) and "are best effort" ([hints](https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/hints.html)).
- **Batches.** The batchlog makes a logged batch all-or-nothing, but "operations are only isolated within a single partition", and a multi-partition batch has "a performance penalty". A logged batch to a single partition is converted to an unlogged one ([DML](https://cassandra.apache.org/doc/5.0/cassandra/developing/cql/dml.html)).
- **Lightweight transactions.** `IF` conditions cost "a non-negligible performance cost, because Paxos is used" ([DML](https://cassandra.apache.org/doc/5.0/cassandra/developing/cql/dml.html)). The yaml reference marks `paxos_variant: v2` as recommended: two round trips per write against four for v1 ([cassandra.yaml](https://cassandra.apache.org/doc/5.0/cassandra/managing/configuration/cass_yaml_file.html)). The 5.0 source warns that mixing `SERIAL` and `LOCAL_SERIAL` is "the most likely source of linearizability violations"; we use `LOCAL_SERIAL` only.
- **Where the docs are silent,** and this draft relies on our own node tests instead:
  - what a failed CAS returns;
  - that a bound `null` writes a tombstone;
  - how results from several partitions are ordered (they come back partition by partition, so the backend merges them);
  - lowering `gc_grace_seconds` below the repair interval;
  - outbox or queue tables;
  - concurrent schema changes.

## How this draft was checked

All on Cassandra 5.0.9 nodes from `docker/cassandra`, each on its own Compose project and port, torn down afterwards:

- **Applying it.** Both files apply to a fresh node with no errors, and applying them twice is silent. All ten tables have the intended `gc_grace_seconds`, TTL and default compaction.
- **The write path.** The statements it uses run against these tables:
  - the create and update compare-and-swaps, including the result of a failed one;
  - the revive compare-and-swap that clears `deleted_at`;
  - a delete marker with no data;
  - an intent row, with its 24 h TTL;
  - a logged index batch `USING TIMESTAMP`.
- **Searches:**
  - the `''` and implicit-system token partitions;
  - date slices on both tables;
  - keyset paging, alone and with a `_lastUpdated` lower bound. With an upper bound, the node rejects it, so the backend must send the tighter of the two upper bounds.
- **Date sentinels.** The open-range values `0001-01-01T00:00:00Z` and `9999-12-31T23:59:59.999Z` are stored exactly. cqlsh displays the second as `.998993`, which is only display rounding.
- **ADR-0015.** The tombstone measurement behind it, as described above.
- **The revised design's new statements.**
  - The sweeper's time-slice read (`enqueued_at > ? AND enqueued_at < minTimeuuid(?)`) returns only intents older than the cut-off.
  - A single-partition batch that writes the `resource_versions` rows for N+1 and N keeps each row's own `USING TIMESTAMP` (ADR-0012).
