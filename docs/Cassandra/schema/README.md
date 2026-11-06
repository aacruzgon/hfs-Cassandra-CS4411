# Cassandra schema: draft for the schema session

**Status: agreed by the team; sign-offs being recorded against `67f9e0532` (PR #33's merge), roadmap item 1.4.** Signed off so far: Alan (ADR-aacruzgon-0028). Each member records their sign-off, or objection, in their own ADR log against that commit; items 1.1 to 1.4 are done when all five are in. A later change to a table, key, column type or table option needs new sign-offs.

**"The schema session"** in these files means the two team design sessions that roadmap Step 1 plans for week 1 (13 to 18 October), led by the owner of the shared files (**W**). The team decides there. Each member then records their sign-off, or objection, in their own ADR log (item 1.4). Rows 1.1 to 1.3 are marked done only after that.

Read [`spike-findings.md`](../spike-findings.md) first, then this page. The other files are reference.

| File | What it is | Roadmap item |
| --- | --- | --- |
| [`schema.cql`](schema.cql) | The ten tables, keys and table options, as the design document fixes them (revised by ADR-aacruzgon-0012 to 0017 and 0026) | 1.1 |
| [`keyspace-single-node.cql`](keyspace-single-node.cql) | The keyspace for one local node, plus the `system_traces` setup step | 1.1 |
| [`walkthrough.md`](walkthrough.md) | Every in-scope operation walked through the tables in CQL, one Patient's worked example, and the gaps G1 to G37 | 1.2 |
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
| `resources` columns | `data text`, `last_updated`, `created_at`, `deleted_at`, `fhir_version`, `write_ts`, `write_id`, `version_id`, **`index_keys blob`** | `text` is the checklist's default; table compression (LZ4) is on anyway. `index_keys` is the current version's index rows, set by every compare-and-swap, so the next writer deletes exactly what that version wrote (walk-through G22, ADR-0027). `deleted_at` is always written: `0` means live, a delete writes its time, and readers treat `> 0` as deleted, so the table holds no tombstone (G39, ADR-0027) | As designed |
| `created_at` | Only on `resources`. Kept when a resource is revived. `vread` passes the version's own `last_updated`. | MongoDB keeps the original `created_at`; PostgreSQL stores none and passes `last_updated`. Both backends give a historical version its own `last_updated`. | Accepted |
| `resource_versions` columns | `data` (unset for a delete marker), `last_updated`, `deleted_at`, `fhir_version`, `write_ts`, **`write_id`**, **`index_keys blob`** | As the checklist says. A delete marker has no content, as in PostgreSQL. `write_id` decides an unknown CAS outcome that a later version superseded, whenever that version's rewrite of row `V` came from a read of `resources` (walkthrough G36). `index_keys` holds the index rows the version wrote, in the versioned encoding `schema.cql` documents (289 bytes for the worked example's 8 rows), so old keys are read back, not re-extracted with a registry that may have changed (walkthrough G22) | As designed; `write_id` added by ADR-0026, `index_keys` by ADR-0027 |
| `resources_by_type` and the token, date and URI tables | Keys and clustering order exactly as the checklist; **`fhir_version text` on `resources_by_type`**, so `count` can apply `SearchQuery.fhir_version` (G34); `param text` in every index partition key; `''` for "no system" | As designed. A node confirmed `start` and `end` work as column names; `token` is reserved and is not used. | As designed; `fhir_version` added by ADR-0026 |
| `pending_index_writes` | Clustering `(enqueued_at, tenant_id, resource_type, id)`; `write_id` and `version_id`; `default_time_to_live = 86400` | The key order is ADR-0015 and `version_id` is ADR-0013 | As designed |
| `gc_grace_seconds` | 10,800 s (3 h) on the six index tables, `resources_by_type` **and `pending_index_writes`**; the 10-day default on `resources` and `resource_versions` | 3 h equals the node's `max_hint_window`, so a replica that was down for less than that still receives its hinted deletes. A resurrected row is harmless here: the tag check hides index rows, and a resurrected intent row makes the sweeper either delete it or replay writes that change nothing. `resources` keeps 10 days for a second reason: with the default `paxos_state_purging` (legacy), Paxos state for a table lives for `max(3 h, gc_grace_seconds)`. | Accepted (ADR-0009); the roadmap checklist now lists `pending_index_writes` too |
| Compaction | No table sets it: all use the node default, UCS with `T4` | See [Compaction](#compaction) | Accepted |
| Statements that must never exist | Listed at the top of `schema.cql`: a `DELETE` on `resources`, a bound `null` or `null` literal, with no exception (ADR-aacruzgon-0027 replaced the revive's literal `SET deleted_at = null`, which ADR-aacruzgon-0024 allowed, with `deleted_at = 0`), `ALLOW FILTERING`, any plain (non-LWT) write to `resources`, and any index row with a `code`, `system` or `uri` value over 2,048 bytes | The last two are new. A plain write bypasses Paxos, so it would break the compare-and-swap. An oversized value fails Cassandra's key limit after the CAS has applied, so the write could never finish (walkthrough G35). | As designed, plus two (the 2 KiB rule is ADR-0026) |
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

The walk-through found places where the design document was wrong. These are now fixed in `cassandra-discussion.md`, in the roadmap and in `schema.cql`, each with an entry in `ADR-aacruzgon/`. Every member reviews them with the rest of the schema at the session. The rows marked Step 2 (ADR-aacruzgon-0032, 0034, 0035, 0041 and 0044) came from building the backend after the sign-off at `67f9e0532` (ADR-aacruzgon-0028): they change the walk-through's error handling or add a read path, not a table, key, column type or option.

| ADR | Problem found (walk-through gap) | Change | Schema change |
| --- | --- | --- | --- |
| 0012 | **A version's content could be lost for good (G2).** A crash between the CAS and the `resource_versions` write, followed by another update, overwrote that version in `resources` before it reached history. | Every update, delete and revive also rewrites the row for the version it replaced, from `current`, one microsecond below that version's `write_ts`, so an existing row always wins. | None |
| 0013 | **The sweeper left orphan index rows (G1).** "A later version's intent covers it" was wrong: the later version deletes only the keys of the version it read. | The intent row carries the `version_id` its CAS will create. If that version is current, the sweeper replays its writes at its own `write_ts`. If it has been superseded, the sweeper only deletes the previous version's keys. | `version_id int` on `pending_index_writes` |
| 0014 | **The sweeper only looked back one hour (G6),** while intents live 24 h. After an outage, a crashed create stayed unsearchable for good. | At startup the sweeper scans the 25 most recent buckets (a 24 h TTL can span 25), one short time slice per query. | None |
| 0015 | **Each sweeper read scanned every finished intent (G5). Measured:** 3,000 tombstones and a logged warning on one read. | `enqueued_at` leads the clustering key, and the sweeper reads only a time slice past a low-water mark. On a node that read scanned 0 tombstones. | Clustering `(enqueued_at, tenant_id, resource_type, id)` |
| 0016 | **The `write_ts` formula could not be computed (G4).** `update` receives a `StoredResource`, which has no `write_ts`. | `last_updated(N+1) = max(now_ms, last_updated(N) + 1 ms)` and `write_ts = last_updated × 1000`, so any version's `write_ts` can be recomputed. | None |
| 0017 | **Repair on read could delete a key the current version needs (G10),** because a delete wins a timestamp tie with the writer's insert. | Repair deletes a stale key only if the current version does not produce it; otherwise it upserts the key with the current tag. | None |
| 0024 | **Lost PUT races and unknown CAS outcomes (G13, G25, G26; upstream #2111).** The design retried `create_or_update` only for batch, mapped a missed CAS straight to 404 or 409, answered an unknown CAS outcome that was not ours with 503, and both deleted and rewrote one intent row across attempts. | `create_or_update` is one bounded loop over create, update and revive: a fresh read, `write_id` and intent row per attempt, at most 3 immediate retries after a definite miss (4 attempts in all), and 2 re-runs after a timed-out or failed CAS the serial read proves did not apply (5 to 25 ms pause), on every CAS, `create` included. An undecidable outcome is 504 after a timeout and 503 after a replica failure. A plain delete that loses to an update retries once. A revive keeps the original `fhir_version`. ADR-0024 also allowed the revive's literal `deleted_at = null` as the one null; **superseded** by ADR-aacruzgon-0027 (walkthrough G39): a revive writes `deleted_at = 0`, `resources` never writes a tombstone, and "never bind null" has no exception. | Comments only (`write_id`, `pending_index_writes`, and the null exception, which ADR-0027 removed) |
| 0026 | **The schema audit (2026-11-05)** found five gaps. The compartment filter was neither served nor refused (G33). `count` could not apply `SearchQuery.fhir_version` (G34). An index value over Cassandra's 64 KiB key limit made a committed write unindexable (G35). A superseded unknown CAS outcome was always undecidable (G36). A second `_id` parameter was not defined (G37). | The compartment filter is a residual on each loaded row, under the candidate cap, with `TooManyResults` (422) past it. A write with an indexed value over 2,048 bytes is refused with 400 before step 2. A superseded unknown outcome is decided by row `V`'s `write_id` where it is set. Every `_id` parameter is intersected. ADR-aacruzgon-0026 refines ADR-0024's unknown-outcome rule. | `fhir_version text` on `resources_by_type`; `write_id uuid` on `resource_versions` |
| 0027 | **Every open item (2026-11-05).** G7, G8, G12, G17, G18, G20, G22, G23, G27, G32, C1, C2, C4 to C11, P5 and P6 were open. Re-extracting a version's old keys with today's registry left orphans after a `SearchParameter` change (G22). | Decided as listed under "Decisions the team made". Each version row stores the index rows it wrote, and so does the current `resources` row, written by every compare-and-swap; deletes, the sweeper and repair read them back. Review of PR #33 added the `resources` copy, a 128-byte bound on a custom SearchParameter's `code` (G38), a second operations rule for the cluster, `deleted_at = 0` for a live row so `resources` writes no tombstone (G39), and the rewrite of row N before the compare-and-swap (G40). | `resource_versions.index_keys blob` |
| 0032 | **A CAS could still be re-sent by the driver (Step 2).** Its default retry policy re-sends any statement after `Unavailable` and `IsBootstrapping`, idempotent or not. | Every CAS also carries `FallthroughRetryPolicy`; walkthrough 3.1 says so. Changes error handling only, not a table, key, column type or option. | None |
| 0034 | **A late first attempt could answer a false 409 (Step 2).** After a dropped connection, attempt 1 can apply after R2 found no row; the walk-through compared the row with the current attempt only, and listed `Error::Timeout` as an unknown outcome although nothing was sent. | Walkthrough 3.1 matches the row's `write_id` against every attempt of the request, and maps `Error::Timeout` and a deadline before the CAS was sent directly. Changes error handling only, not a table, key, column type or option. | None |
| 0035 | **`WriteTimeout`/`SIMPLE` was taken as applied (Step 2),** but the timed-out commit can be another coordinator's proposal. | Walkthrough 3.1 makes `SIMPLE` a trigger confirmed by R2, never taken as applied. Changes error handling only, not a table, key, column type or option. | None |
| 0041 | **No statement listed the tenants (Step 2).** With no tenant registry, the stored-SearchParameter reload had no way to find every tenant. | Walkthrough section 2 adds R5, `SELECT DISTINCT tenant_id, resource_type FROM resources_by_type` (paged, `LOCAL_QUORUM`), and `coverage.md` lists it. Adds a read path, not a table, key, column type or option. | None |
| 0044 | **The walk-through named cdrs-tokio 8.1.9 (Step 2),** which could not decode `CAS_WRITE_UNKNOWN`. | Walkthrough 3.1 names cdrs-tokio 10, which decodes it as `ErrorType::CasWriteUnknown`; it is still an unknown outcome. Changes error handling only, not a table, key, column type or option. | None |

Two corrections need no ADR because they fix a statement, not a decision:
- **The sweeper does compare clocks.** "No clocks are compared" was wrong: the grace period is a clock comparison. The design now states its assumption (G11).
- **A write is 404, never 410, when the resource was deleted in the gap** (G9). 410 is for `read` and `vread` only.

## Proposals, decided

| # | Problem (walk-through gap) | Decision (ADR-aacruzgon-0027) | Schema change |
| --- | --- | --- | --- |
| P5 | **Index rows nobody reads (G17, G18).** The extractor emits `_id` and `_lastUpdated` (served by `resources` and `resources_by_type`), display-only tokens with an empty code, and implicit-system rows in `token_system_index`. | Drop them before computing index keys, and also the `''`-system rows in `token_system_index` and the empty URIs, which P5's first wording did not name. | None |
| P6 | **Several Helios processes would apply the schema at once.** The Apache docs say nothing about concurrent schema changes; driver documentation advises waiting for schema agreement. | Apply the schema from one process (a setup command or the test harness). `initialize` only checks that the tables exist; roadmap Step 2 says so. | None |

## Decisions the team made

ADR-aacruzgon-0027 records these. The walk-through's section 8 and `coverage.md` section 7 give each one's status.

- **`count` (G7).** `gc_grace_seconds` stays at 3 h on `resources_by_type`, with three operations rules on the cluster: a replica back from an outage shorter than `max_hint_window` (3 h), or after a spike of `Unavailable` errors, timeouts or dropped mutations, is repaired promptly, within 3 h of the outage's start; a replica down longer is replaced, never rejoined, even after repair; and the replacement node is repaired once it has bootstrapped, as Cassandra's replace-node procedure requires ([replacing a dead node](https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/topo_changes.html#replacing-a-dead-node)). They narrow resurrection, not rule it out: hints are best effort and expire at 3 h on these tables, so `count` on the cluster can be one too high per resurrected row, while search stays correct. Each resurrected or orphan row also counts against the candidate cap, so enough of them in one partition can make a page return 422; repair on read deletes the rows it reads. A filtered `search_count` uses tag checks and the candidate cap; an unfiltered `count` scans `resources_by_type`. `count(tenant, None)` loops over every enabled FHIR version's resource type names. The two counts may differ while a write is in flight, while a crashed write waits for the sweeper, and in the design's stated limitations.
- **Mass-update tombstones (G8).** A named benchmark case in roadmap Step 8.
- **Several sweepers (G12).** Accepted, with no lease row: this project runs one process, and the sweeper's writes are idempotent.
- **Date-only paging (G20).** Pages come in the date table's own order, with the table named in the cursor. `ge` is the single slice `end > s`. With several dates, one is sliced and the rest are residuals. `ne` and comma lists over both tables read `date_index`, then `date_index_by_end`.
- **Old keys after a registry change (G22).** Each version row stores the index rows it wrote (`index_keys`), and every compare-and-swap writes the current version's into `resources.index_keys`. Writers and repair read `K(N)` from `resources`, and the sweeper reads `K(V-1)` from row `V-1`; nothing is re-extracted. Only `update` needs an extra read. Adding a SearchParameter does not index existing resources until their next write.
- **`resources.deleted_at` (G39).** `0` means live, written on create and revive; a delete writes its time; every condition tests `deleted_at = 0` and every reader treats `> 0` as deleted. The revive's former literal-null tombstone could be purged while one replica still held the delete's value, and the revived resource then read as deleted; reproduced on a three-node 5.0.9 cluster. No column changes.
- **The rewrite of row N (G40).** Written before the intent row and the compare-and-swap, so row N exists whenever a later compare-and-swap applied. A paused writer followed by a crashed successor no longer leaves orphans, and two crashes in a row no longer lose a version. One more round trip on update, delete and revive.
- **A custom SearchParameter's `code` (G38).** Refused with 400 over 128 bytes when stored, and each index row's `param` is measured in the step 1 size check, so no key reaches Cassandra's limit.
- **The write that applied but was not indexed (G23).** It returns success, and the sweeper finishes it.
- **Too costly (G27).** The error is 422 `SearchError::TooManyResults`; no search produces `too-costly`. The cap counts distinct candidates point-read.
- **The intent row's `write_id` (G32).** It is kept as a diagnostic.
- **`_profile`, `_source`, `_tag`, `_security` (C1).** In scope.
- **`_id=sys|abc` and `_id=|abc` (C2).** Compared whole with the id, as SQLite and PostgreSQL do, so they match nothing.
- **Pipe form on ContactPoint and boolean (C4).** Matched as SQLite and PostgreSQL do.
- **The AND driver (C5).** A fixed order, no statistics: `_id`; `system|code` and `|code`; identity URIs (a canonical resource's own `url`); code-only tokens, `_profile`, `_source` and other URIs; `system|`; `_lastUpdated` only when nothing else can drive. Ties by query order.
- **`MaxPageSize` (C6).** REST's `HFS_MAX_PAGE_SIZE` (default 1000); no backend limit.
- **Composite part count and `_has` (C7).** The 400s the shared code returns today, pinned by Step 6 tests. `_has` is served through the shared resolver when its terminal parameter is token, date or URI; a broad terminal is a Step 8 benchmark case. The resolver misses versioned and absolute source references, an upstream defect filed as HeliosSoftware/hfs#2499. Under a scope, `_has` and chains are not confined in their hops (HeliosSoftware/hfs#2501, a security defect): four Step 6 rows are a gate, and if upstream has not fixed it by then the fork carries a REST patch that refuses them with 403 under a confined grant and checks each hop's type against the scopes. A forward chain on Cassandra answers 200 empty when its terminal matches nothing, and 400 otherwise.
- **Terminology modifiers (C8).** The design matches the REST code, including its fail-open on a failed expansion (upstream #2019, closed as not planned).
- **`_sort` (C9).** Accepted only when equal to the native order.
- **`_text`/`_content` (C10).** 501 by name in the backend, before the type check.
- **`_filter` (C11).** 501 by name in the Cassandra backend (`BackendError::UnsupportedCapability`); REST unchanged.

Nothing is open.

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
  - the revive compare-and-swap that clears `deleted_at` (since ADR-0027, by writing `0`);
  - a delete marker with no data;
  - an intent row, with its 24 h TTL;
  - a logged index batch `USING TIMESTAMP`.
- **Searches:**
  - the `''` and implicit-system token partitions;
  - date slices on both tables;
  - keyset paging, alone and with a `_lastUpdated` lower bound. With an upper bound, the node rejects it, so the backend must send the tighter of the two upper bounds.
- **Date sentinels.** The open-range values `0001-01-01T00:00:00Z` and `9999-12-31T23:59:59.999Z` are stored exactly. cqlsh displays the second as `.998993`, which is only display rounding.
- **ADR-0015.** The tombstone measurement behind it, as described above.
- **ADR-0026 (2026-11-05).** The revised file applies twice to a fresh 5.0.9 node. The changed statements (X1 with `fhir_version`, V1, V2 and V3 with `write_id`, R3, and the counting scan of `resources_by_type`) run against it. The CI job's `system_schema` assertions pass on it, and fail when a key or clustering order is altered.
- **ADR-0027's `resources.index_keys` (2026-11-05).** The revised file applies twice to a fresh 5.0.9 node, with ten tables. The CI assertion passes with 92 rows, and fails with a one-line diff when `resources.index_keys` is left out or typed `text`. C1 to C4 with `index_keys` apply, a missed CAS returns only its `IF` columns, and the paused-writer sequence (walk-through section 2) leaves no orphan.
- **ADR-0027's `deleted_at = 0` and V3 before the CAS (2026-11-06).**
  - **Three-node 5.0.9 cluster, RF 3, Paxos v2, `gc_grace_seconds = 0` standing in for 10 days.** A revive with one node down and its hint lost, then a purge on the other two and a restart without repair. The literal-null revive read back as deleted at `ALL`, read repair spread it to all three nodes, and the next update's CAS missed. The `deleted_at = 0` revive stayed live and its CAS applied.
  - **Single node.** The file applies twice, silently, with ten tables. The CI assertion passes with 92 rows (unchanged), and fails with a one-line diff when `resources.deleted_at` is typed `bigint`. C1 to C4 with `deleted_at = 0` apply, and a repeat delete misses, returning `deleted_at`. After a flush, `sstabledump` shows no tombstone for the sentinel path and one `deleted_at` cell tombstone for the literal null. The paused-writer sequence left a `resources_by_type` and a token orphan with V3 after the CAS, and none with V3 before it.
- **The revised design's new statements.**
  - The sweeper's time-slice read (`enqueued_at > ? AND enqueued_at < minTimeuuid(?)`) returns only intents older than the cut-off.
  - V3, the rewrite of row N at `ts(N) − 1`, is its own write before the intent row and the CAS; row N+1 is written after the CAS at `ts(N+1)`. Each carries its own `USING TIMESTAMP` (ADR-0012, ADR-0027 G40). Checked on a fresh 5.0.9 node (2026-11-06) with the paused-writer sequence: V3 for row 1 at `ts(1) − 1` before the CAS to version 2, and V3 for row 2 at `ts(2) − 1` before the CAS to version 3, then row 3 at `ts(3)`; the sweeper's R3 then read row 2 with its key set, and no orphan was left. The intent row (I1, I2) was not part of that run, and the rows' write times were not read back. The single-partition batch of rows N+1 and N that ADR-0012 described is no longer the write path.
