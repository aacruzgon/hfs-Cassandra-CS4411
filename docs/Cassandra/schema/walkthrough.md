# Paper walk-through of the schema

**DRAFT for the schema session (roadmap item 1.2).** Not reviewed by the team. Nothing here is decided. Where the walk-through had to pick something the design leaves open, it points to the gap list at the end (G1, G2, …).

This walks every in-scope operation through the ten tables, statement by statement. It follows the design in [`cassandra-discussion.md`](../cassandra-discussion.md) ("Proposed Cassandra data model" and "Consistency, failure handling and rejection"), the Step 1 checklist of [`development-roadmap.md`](../development-roadmap.md), and [`spike-findings.md`](../spike-findings.md). The tables are in [`schema.cql`](schema.cql); the directory overview is [`README.md`](README.md).

**This file holds the statements; [`coverage.md`](coverage.md) holds the expected results**: the per-parameter tables (token forms, date prefixes, `_lastUpdated`, `_id`, AND, cursor, count) and every rejection. Neither repeats the other.

Code facts were checked against the working tree on 2026-10-13 (`crates/persistence`, `crates/rest`). "Checked on a 5.0.9 node, 2026-10-13" marks live tests on a single Cassandra 5.0.9 node. Revised on 2026-10-14 for the design changes in ADR-aacruzgon-0012 to 0017.

> **Read this first.** The seven most serious gaps of the first draft (G1, G2, G4, G5, G6, G10, G11) are now resolved in the design, with G3, G9 and G16; section 8 says how. The most serious gaps still open:
>
> 1. **G7.** `count` has no tag check, so it counts writes in flight, crash orphans until the sweep, and rows resurrected on the cluster.
> 2. **G12.** Several Helios processes run several sweepers, each re-reading and replaying the same intents.
> 3. **G23.** What the client gets when the CAS applied but the index writes failed.
> 4. **G22.** Old keys are recomputed with today's search-parameter registry, so a registry change leaves rows behind.
> 5. **G27.** No search error produces `too-costly`; the nearest is 422.

## Contents

1. Conventions
2. Statement catalogue
3. Writes: create, update, delete, revive
4. Reads: read, vread, list_versions
5. Search
6. The sweeper
7. Worked example: one Patient, four versions
8. Gaps and questions for the session

## 1. Conventions

**Notation.**

| Symbol | Meaning |
| --- | --- |
| `T`, `P`, `id` | `tenant_id`, `resource_type`, resource id |
| `N` | the version the writer read; the new version is `N+1` |
| `V` | the version a sweeper or a repair is working on |
| `ts(V)` | `write_ts` of version `V`, a `bigint` in microseconds; always `lu(V) · 1000` |
| `lu(V)` | `last_updated` of version `V`, a `timestamp` in milliseconds: `lu(1) = now_ms`, `lu(N+1) = max(now_ms, lu(N) + 1 ms)` (ADR-aacruzgon-0016) |
| `w(V)` | `write_id` (uuid) of the request that wrote version `V` |
| `tu(V)` | `enqueued_at` (timeuuid) of the intent row for version `V` |
| `K(V)` | the index keys version `V` produces: one `resources_by_type` row, plus every token, date and URI row from `SearchParameterExtractor::extract` on `V`'s content. Empty when `V` is a delete marker. |
| `IMPLICIT` | the marker system for `code`-typed elements: `IMPLICIT_TOKEN_SYSTEM` = `"urn:x-helios:implicit code system"` (`search/converters.rs`) |
| `''` | the "no system" sentinel |

**Consistency.** Every statement sets its level explicitly (design, "Consistency levels").

| Statement kind | Consistency | Serial consistency |
| --- | --- | --- |
| Plain reads (`resources`, `resource_versions`, index tables, `pending_index_writes`) | `LOCAL_QUORUM` | — |
| Plain writes (intent row, version row, index batches, intent delete) | `LOCAL_QUORUM` | — |
| Compare-and-swap (CAS) on `resources` | `LOCAL_QUORUM` (commit) | `LOCAL_SERIAL` (Paxos) |
| Read that must settle an in-flight CAS (sweeper; unknown CAS outcome) | `LOCAL_SERIAL` | — |

The replication factor (RF) is the number of copies of each row: 1 on the single node, 3 on the cluster. With RF 1, `LOCAL_QUORUM` is one replica. `USING TIMESTAMP` is never put on a CAS: the node rejects it (spike A1).

**Assumptions.** Each is also a gap, so the session can overturn it.

- `_id` and `_lastUpdated` values from the extractor are **not** written to index tables; the design serves them from `resources` and `resources_by_type` (G17).
- Otherwise the design is followed as written, including rows no query reads (G18).
- `delete` leaves `data` in the `resources` row untouched (G29).

## 2. Statement catalogue

Each operation is a sequence of these statements. `?` is a bind marker.

### Intent rows (`pending_index_writes`)

```sql
-- I1: write the intent. Plain write, LOCAL_QUORUM. TTL comes from default_time_to_live (86400).
-- version_id is the version this request's CAS will create (1 for a create, else N+1).
INSERT INTO pending_index_writes
       (bucket, shard, enqueued_at, tenant_id, resource_type, id, write_id, version_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);

-- I2: delete the intent. Plain write, LOCAL_QUORUM. The request keeps I1's full key,
-- so a retried I1 rewrites the same row and I2 finds it after the hour rolls over.
DELETE FROM pending_index_writes
 WHERE bucket = ? AND shard = ? AND enqueued_at = ?
   AND tenant_id = ? AND resource_type = ? AND id = ?;

-- I3: sweeper read, one time slice of one partition. LOCAL_QUORUM.
-- Lower bound: the partition's low-water mark. Upper bound: now minus the grace period.
SELECT enqueued_at, tenant_id, resource_type, id, version_id
  FROM pending_index_writes
 WHERE bucket = ? AND shard = ?
   AND enqueued_at > ? AND enqueued_at < minTimeuuid(?)
 LIMIT ?;
```

Partition: one `(bucket, shard)`. `bucket` is the UTC hour of `enqueued_at` (`'2026-10-14T10'`), `shard` is `hash(tenant_id, id) mod 16`. `minTimeuuid` is for query bounds only, never an inserted value.

### Compare-and-swap on `resources`

All four use `LOCAL_SERIAL` for Paxos and `LOCAL_QUORUM` for the commit, on partition `(T, P, id)`. Absent columns are left unset, never bound to null. The one exception is C4's literal `deleted_at = null` (G13). `fhir_version` is the request's only in C1. C2 and C4 bind the version the resource was created with, from `current` or R1, as every other backend keeps it (ADR-aacruzgon-0024).

```sql
-- C1: create (POST, or create_or_update on a missing id). Version 1.
INSERT INTO resources
       (tenant_id, resource_type, id, version_id, data, last_updated,
        created_at, fhir_version, write_ts, write_id)
VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
IF NOT EXISTS;

-- C2: update of a live row, N -> N+1. fhir_version = current's (fixed at create).
UPDATE resources
   SET version_id = ?, data = ?, last_updated = ?, fhir_version = ?,
       write_ts = ?, write_id = ?
 WHERE tenant_id = ? AND resource_type = ? AND id = ?
    IF version_id = ? AND deleted_at = null;

-- C3: delete (plain and versioned), N -> N+1. Soft delete; never a CQL DELETE.
UPDATE resources
   SET version_id = ?, deleted_at = ?, last_updated = ?, write_ts = ?, write_id = ?
 WHERE tenant_id = ? AND resource_type = ? AND id = ?
    IF version_id = ? AND deleted_at = null;

-- C4: revive (create_or_update on a deleted row), N -> N+1.
-- deleted_at = null is a literal in the statement, not a bound null.
-- It writes one cell tombstone, on purpose (G13).
-- fhir_version = the original, from the row R1 read, never the request's (G13).
UPDATE resources
   SET version_id = ?, data = ?, last_updated = ?, fhir_version = ?,
       write_ts = ?, write_id = ?, deleted_at = null
 WHERE tenant_id = ? AND resource_type = ? AND id = ?
    IF version_id = ?;
```

**What a CAS returns when not applied** (spike A1; checked on a 5.0.9 node, 2026-10-13): `[applied] False` plus exactly the `IF`-clause columns with their current values. On a missing row, `[applied] False` only. `IF NOT EXISTS` returns the whole existing row.

**How it maps** (design step 3): no columns → `NotFound`, 404; `deleted_at` set → `NotFound`, 404; any other `version_id` → `VersionConflict`, 409. A write never returns 410; only `read` and `vread` do. After a definite miss the request runs I2.

### Version rows (`resource_versions`)

```sql
-- V1: content version. Plain write, LOCAL_QUORUM.
INSERT INTO resource_versions
       (tenant_id, resource_type, id, version_id, data, last_updated, fhir_version, write_ts)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
USING TIMESTAMP ?;                       -- ts(V)

-- V2: delete marker. data is left unset.
INSERT INTO resource_versions
       (tenant_id, resource_type, id, version_id, last_updated, deleted_at, fhir_version, write_ts)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
USING TIMESTAMP ?;                       -- ts(V)

-- V3: rewrite of row N on update, delete and revive (ADR-aacruzgon-0012).
-- V1, or V2 for a marker (deleted_at set, data unset), from what the writer holds,
-- USING TIMESTAMP ts(N) - 1.  One microsecond older than the original row.
```

Partition: `(T, P, id)` of `resource_versions`, a different table from `resources`. Every version row carries the resource's own `fhir_version`, the one it was created with (G13). V3 and the `N+1` row may share one single-partition batch, each with its own `USING TIMESTAMP`. If row `N` exists, the original wins every cell, even if the content re-serialises differently; if `N`'s request crashed before its step 4, V3 restores it.

### Index rows

Every index statement carries `USING TIMESTAMP ts(V)` of the version being written. One version's inserts and deletes go in logged batches, each under 5 KiB (`batch_size_warn_threshold`), sent concurrently at `LOCAL_QUORUM`. The batches are multi-partition, so they are size-checked (spike B1).

```sql
-- X1 resources_by_type
INSERT INTO resources_by_type (tenant_id, resource_type, last_updated, id, version_id)
VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?;
DELETE FROM resources_by_type USING TIMESTAMP ?
 WHERE tenant_id = ? AND resource_type = ? AND last_updated = ? AND id = ?;

-- X2 token_code_index
INSERT INTO token_code_index (tenant_id, resource_type, param, code, last_updated, id, system, version_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?;
DELETE FROM token_code_index USING TIMESTAMP ?
 WHERE tenant_id = ? AND resource_type = ? AND param = ? AND code = ?
   AND last_updated = ? AND id = ? AND system = ?;

-- X3 token_system_code_index
INSERT INTO token_system_code_index (tenant_id, resource_type, param, system, code, last_updated, id, version_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?;
DELETE FROM token_system_code_index USING TIMESTAMP ?
 WHERE tenant_id = ? AND resource_type = ? AND param = ? AND system = ? AND code = ?
   AND last_updated = ? AND id = ?;

-- X4 token_system_index
INSERT INTO token_system_index (tenant_id, resource_type, param, system, last_updated, id, code, version_id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?;
DELETE FROM token_system_index USING TIMESTAMP ?
 WHERE tenant_id = ? AND resource_type = ? AND param = ? AND system = ?
   AND last_updated = ? AND id = ? AND code = ?;

-- X5 date_index and X6 date_index_by_end (same partition key, different tables)
INSERT INTO date_index (tenant_id, resource_type, param, start, id, end, version_id)
VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?;
DELETE FROM date_index USING TIMESTAMP ?
 WHERE tenant_id = ? AND resource_type = ? AND param = ? AND start = ? AND id = ? AND end = ?;
INSERT INTO date_index_by_end (tenant_id, resource_type, param, end, id, start, version_id)
VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?;
DELETE FROM date_index_by_end USING TIMESTAMP ?
 WHERE tenant_id = ? AND resource_type = ? AND param = ? AND end = ? AND id = ? AND start = ?;

-- X7 uri_index
INSERT INTO uri_index (tenant_id, resource_type, param, uri, last_updated, id, version_id)
VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?;
DELETE FROM uri_index USING TIMESTAMP ?
 WHERE tenant_id = ? AND resource_type = ? AND param = ? AND uri = ? AND last_updated = ? AND id = ?;
```

```sql
-- How one version's index changes are sent: several of these, concurrently.
BEGIN BATCH            -- logged; LOCAL_QUORUM; serialized size < 5 KiB
  DELETE FROM ... USING TIMESTAMP ? WHERE ...;   -- keys in K(N) but not in K(N+1)
  INSERT INTO ... VALUES (...) USING TIMESTAMP ?; -- every key in K(N+1)
APPLY BATCH;
```

**Index rule for version `V`:** delete `K(V-1) \ K(V)`, insert all of `K(V)`, every statement at `ts(V)`. A key in both sets is only inserted, which rewrites its `version_id` tag. `last_updated` is in the clustering key of `resources_by_type` and every token and URI table, so only date keys survive an update unchanged.

**Partitions per version**, for `t` token, `d` date and `u` URI values: one `resources_by_type`; up to `3t` token; one `date_index` and one `date_index_by_end` per date parameter; up to `u` URI. An update touches the old ones and the new ones.

### Reads

```sql
-- R1: current row. LOCAL_QUORUM. Used by read, exists, _id, the tag check, and the pre-read of delete and create_or_update.
SELECT version_id, data, last_updated, created_at, deleted_at, fhir_version, write_ts, write_id
  FROM resources WHERE tenant_id = ? AND resource_type = ? AND id = ?;

-- R2: the same SELECT at LOCAL_SERIAL. Completes or cancels an in-flight Paxos proposal.
-- Used after an unknown CAS outcome, and by the sweeper for every intent.

-- R3: one version. LOCAL_QUORUM.
SELECT data, last_updated, deleted_at, fhir_version, write_ts
  FROM resource_versions
 WHERE tenant_id = ? AND resource_type = ? AND id = ? AND version_id = ?;

-- R4: all version numbers, oldest first. LOCAL_QUORUM.
SELECT version_id FROM resource_versions
 WHERE tenant_id = ? AND resource_type = ? AND id = ?
 ORDER BY version_id ASC;
```

## 3. Writes

Every write first calls `tenant.check_permission(Operation::…, resource_type)`; a refusal is a `TenantError` before any statement runs.

### 3.1 Write-path skeleton (design steps 1 to 6)

| Step | Statement | Partition | On failure |
| --- | --- | --- | --- |
| 1 | permission check; extract `K(N+1)`; compute `K(N)` from the old content | — | `TenantError` |
| 2 | I1, with `version_id = N+1` | `(bucket, shard)` | `Unavailable`: 503, nothing changed. `WriteTimeout`: an unused intent is swept, so harmless; 504 (`BackendError::Timeout`, G26) |
| 3 | C1 to C4 | `resources (T,P,id)` | not applied: I2, then the mapped error; unknown outcome below |
| 4 | V1 or V2 for `N+1` at `ts(N+1)`; on update, delete and revive also V3 for `N` at `ts(N) − 1` | `resource_versions (T,P,id)` | retry with the same timestamps |
| 5 | index batches, at `ts(N+1)` | index partitions | retry with the same timestamps; `WriteTimeout` type `BATCH` needs no retry (batchlog replays), `BATCH_LOG` does. Retries exhausted: G23 |
| 6 | I2 | `(bucket, shard)` | ignored: return success; the sweeper replays the same writes, which changes nothing, and deletes the intent |

**Timestamps** (ADR-aacruzgon-0016). `lu(1) = now_ms`; `lu(N+1) = max(now_ms, lu(N) + 1 ms)`; `ts(V) = lu(V) · 1000`. `lu(N)` comes from `current.last_modified()` on update and from R1 on delete and revive, so V3's timestamp is exact with no extra read. `w(N+1)` is a fresh uuid per CAS attempt (ADR-aacruzgon-0024), and `lu` and `ts` are computed from that attempt's R1.

**Unknown CAS outcome** (ADR-aacruzgon-0024, amended 2026-11-05).
- **Triggers:** `WriteTimeout` or `WriteFailure` with `write_type = CAS`, on any of C1 to C4, so for `create`, `create_or_update`, `update`, `delete` and `delete_versioned`.
    - With `paxos_variant: v2`, 5.0.9 throws `WriteFailure`/CAS when replicas fail, even in the commit phase after consensus (`Paxos.java:794-799`).
    - `CAS_WRITE_UNKNOWN` comes only from the legacy v1 path, and cdrs-tokio 8.1.9 cannot decode it; if it ever arrives, it is treated the same way.
    - A client-side timeout or a dropped connection after the CAS was sent is treated exactly like `WriteTimeout`/CAS. In cdrs-tokio that is `Error::General("Connection closed …")`, `Error::Io`, `Error::Timeout` (raised only while connecting), or the backend's own request deadline.
    - Every CAS is sent as non-idempotent, so neither the driver's retry policy nor speculative execution ever re-sends one.
- **What to do:** run R2. Contention alone can cause it, with nothing committed.

For an attempt that targeted version `V` (1 for C1):

| R2 shows | Meaning | Action |
| --- | --- | --- |
| `version_id = V`, our `write_id` | applied | continue at step 4; a create returns 201 |
| `version_id = N`, or no row for C1 | not applied, and never will be | run again as a new attempt after a random 5 to 25 ms pause, from a budget of 2 re-runs; a create keeps the same `id`; the attempt's I1 is left for the sweeper |
| `version_id = V`, another `write_id` | another writer won `V` | a definite miss: `AlreadyExists`, 409, from `create`; 409 from `update` and `delete_versioned`; the next attempt in `create_or_update` and a plain `delete` |
| `version_id > V` | undecidable: ours may have applied and been superseded | after `WriteTimeout`, a client-side timeout or a dropped connection: `BackendError::Timeout`, 504; after `WriteFailure`: `BackendError::Unavailable`, 503. No retry, which could write the body twice; I1 left for the sweeper |
| R2 times out, or fails | unknown | timed out: `BackendError::Timeout`, 504; failed (`ReadFailure`, `Unavailable`): `BackendError::Unavailable`, 503; I1 kept |

Every timeout is 504 and every unavailable node or replica failure 503, as `crates/rest/src/error.rs` maps them (G26). When the 2 re-runs are used up, the last attempt's error decides the same way: 504 after a `WriteTimeout`, 503 after a `WriteFailure`. `write_type = SIMPLE`: the CAS was decided and only the commit timed out, so treat it as applied.

### 3.2 Create (`ResourceStorage::create`, `POST /Patient`)

`Operation::Create`. The server assigns `id` and sets `meta.versionId = "1"`, `meta.lastUpdated = lu(1)`. Steps: I1 (`version_id` 1); C1; V1 for version 1 (no V3); inserts only (X1 at `(lu(1), id)` and every row of `K(1)`, tag 1), since `K(0)` is empty; I2.

| CAS result | Returned |
| --- | --- |
| applied | 201 Created, `ETag: W/"1"` |
| not applied (row returned): id collision, reachable only with a client-chosen id | `ResourceError::AlreadyExists`, 409 (inside `create_or_update` the loop restarts as an update) |
| unknown (`WriteTimeout` or `WriteFailure`, `write_type = CAS`) | R2 on the same `id`, then the table in 3.1: our `write_id` is 201; no row is a re-run with the same `id` and a fresh `w`, so a client never gets two resources from one `POST`; another `write_id` is `AlreadyExists`, 409 |

### 3.3 Update (`ResourceStorage::update(tenant, current, resource)`, `PUT /Patient/id`)

REST has already read the row, checked `If-Match` (412 on a mismatch, before storage is called), and passes `current`, version `N`. `Operation::Update`. `K(N)` is re-extracted from `current.content()` with `current.last_modified()` as `lu(N)`. Steps: I1; C2 with `IF version_id = N AND deleted_at = null`; V1 for `N+1`, and V3 rewrites `N` from `current`; delete `K(N) \ K(N+1)` and insert `K(N+1)` at `ts(N+1)`; I2.

| CAS result | Meaning | Returned |
| --- | --- | --- |
| applied | | 200 OK, `ETag: W/"N+1"` |
| `[applied] False` only | row never existed | `ResourceError::NotFound`, 404 |
| `deleted_at` set | soft-deleted after REST's read | `NotFound`, 404 (design step 3; the race suite also accepts `Gone`, G9) |
| `version_id ≠ N`, `deleted_at` null | lost the race | `ConcurrencyError::VersionConflict`, 409 |

`update_with_match(…, expected_version, …)` adds a pre-read R1: a different current version is `OptimisticLockFailure` (412) without a CAS; a CAS miss after that is 409.

### 3.4 Delete (`delete` and `delete_versioned`, `DELETE /Patient/id`)

The REST handler reads the row, then calls `delete_versioned(current.version_id())` with `If-Match` or `delete` without (`core/preconditions.rs`, `delete_under_precondition`). Neither passes the old content, so the backend pre-reads.

| # | Statement | Notes |
| --- | --- | --- |
| 0 | R1 | Gives `N`, `data`, `lu(N)`. Absent or `deleted_at` set: `NotFound`, 404, nothing written. `delete_versioned` with `N ≠ expected`: `VersionConflict`, 409, nothing written. |
| 1 | — | `Operation::Delete`. `K(N)` from `data`; `K(N+1)` empty; `deleted_at = lu(N+1)`. |
| 2–3 | I1; C3 with `IF version_id = N AND deleted_at = null` | |
| 4 | V2 (delete marker) for `N+1`; V3 rewrites `N` from R1's `data` | marker `data` unset |
| 5 | delete every key of `K(N)` at `ts(N+1)`, including `resources_by_type (lu(N), id)` | no inserts |
| 6 | I2 | |

| CAS result | `delete` | `delete_versioned` |
| --- | --- | --- |
| applied | 204 No Content | 204 |
| `[applied] False` only, or `deleted_at` set | `NotFound`, 404 | `NotFound`, 404 (a repeated `delete_versioned` must be `NotFound`, G9) |
| `version_id` moved, still live | retry once, at once, from step 0 with a new I1 (G25, ADR-aacruzgon-0024); a second miss: `VersionConflict`, 409; the new R1 finds it deleted: `NotFound`, 404 | `VersionConflict`, 409 (never retried) |

Helios answers 404, not the spec's 200/204, for a delete of an unknown or already-deleted id (`delete.rs`; G25).

### 3.5 Revive and `create_or_update` (`PUT` onto a deleted or unknown id; batch PUT)

The REST update handler maps `Gone` to "no current resource" and calls `create_or_update`, a bounded loop (ADR-aacruzgon-0024). It is reached by every unconditional `PUT` and every batch `PUT`. Each turn runs R1, takes a fresh `w`, and writes a new I1 (new `enqueued_at`) with that turn's version, never rewriting an earlier turn's row. Then:

| Branch | Row | CAS | Not applied |
| --- | --- | --- | --- |
| a | absent | C1 (version 1) | someone created it (`AlreadyExists`); I2, next turn |
| b | live | C2 with `IF version_id = N AND deleted_at = null` | `VersionConflict`, or `NotFound` if deleted in the gap; I2, next turn |
| c | deleted (revive) | C4 with `IF version_id = N`; `K(N)` is empty (delete marker) | `VersionConflict`; I2, next turn |

A definite miss starts the next turn at once, with no pause, for at most 3 retries, so 4 turns in all. An unknown outcome is settled first by the table in 3.1, from its own budget of 2 re-runs.

After the CAS applies, steps 4 to 6 run as for update: V1 for `N+1`; V3 for `N` (branch b a content row from R1; branch c a delete marker, `deleted_at` set and `data` unset, although `resources.data` still holds the old content); inserts of `K(N+1)` (branch b also deletes `K(N) \ K(N+1)`), I2. Returned: branch a 201 (`created = true`); branch b 200 (`created = false`); branch c 201 (`created = true`, PostgreSQL parity; R4 and R4B http.html#update require 201 for a resource "brought back to life"); a fourth definite miss is `VersionConflict`, 409 (that turn's I2 already ran). On revive, `fhir_version` is the original from R1 (G13, ADR-aacruzgon-0024) and `created_at` is kept (G14, ADR-aacruzgon-0011).

## 4. Reads

### 4.1 read (`GET /Patient/id`)

R1 at `LOCAL_QUORUM`, partition `resources (T,P,id)`. Absent: `Ok(None)`, 404. `deleted_at` set: `ResourceError::Gone`, 410 (404 if `HFS_RETURN_GONE` is false). Otherwise `StoredResource::from_storage(…)`, 200, `ETag: W/"version_id"`. `exists` is the same read and answers `false` for a deleted row (the trait doc requires MongoDB's behaviour).

### 4.2 vread (`GET /Patient/id/_history/vid`)

R3 at `LOCAL_QUORUM`, partition `resource_versions (T,P,id)`, one row. `vid` is parsed as an `int` first.

| Case | Backend returns | REST returns |
| --- | --- | --- |
| `vid` not a valid `int` (`"abc"`, `"1.0"`) | `Ok(None)` without a query | 404 |
| no such row | `Ok(None)` | 404 |
| content row | `StoredResource` with content | 200 |
| delete marker (`deleted_at` set, `data` unset) | `StoredResource` with `deleted_at` set and content `Value::Null`, as PostgreSQL | 410 when `HFS_RETURN_GONE` is true (the default), else 404 (`vread.rs`) |

`resource_versions` has no `created_at`, but `from_storage` needs one; PostgreSQL passes `last_updated` for both, which avoids reading `resources` too (G14).

### 4.3 list_versions

R4 at `LOCAL_QUORUM`, one partition. `ORDER BY version_id ASC` reverses the clustering order, which a single-partition query may do (or read `DESC` and reverse in Rust). Returns `["1", "2", …]`, numerically ordered past 9 because `version_id` is an `int`; an unknown id gives an empty list. The race suite requires "no gap, no duplicate"; V3 keeps that through one crash (G2).

## 5. Search

Expected results for every case are in [`coverage.md`](coverage.md). This section gives the statements.

### 5.1 What every search does

1. Validate (order and statuses in `coverage.md` section 6).
2. Stream candidates from one or more index partitions, in the shared order `(last_updated DESC, id DESC)` where the table has it.
3. Collapse rows with the same `(last_updated, id)`.
4. **Tag check:** R1 for the candidate. Keep it only if the row exists, `deleted_at` is unset and `resources.version_id` equals the index row's `version_id`. A mismatch hides the candidate and triggers repair on read (ADR-aacruzgon-0017): maintenance for the current version at its `ts`, from the row just loaded. A stale key the current version does not produce is deleted; one it produces is upserted with the current tag. Repair is logged as a write made from a read.
5. Apply residual filters.
6. Stop at `_count + 1` kept resources; the extra one decides whether `next` exists. `LIMIT` is a batch size, not the page size.

Index and R1 reads are `LOCAL_QUORUM`. A candidate whose index tag is `N` while `resources` is at `N+1` (a write between steps 3 and 5) is hidden: the design's in-flight window.

### 5.2 Token forms

Which form reads which partitions: [`coverage.md` section 2](coverage.md#2-token-forms).

```sql
-- T1 `code`: token_code_index, one partition (param, code).
SELECT last_updated, id, system, version_id FROM token_code_index
 WHERE tenant_id = ? AND resource_type = ? AND param = ? AND code = ?
   [AND (last_updated, id) < (?, ?)]
 LIMIT ?;

-- T2 `system|code` and `|code`: token_system_code_index. Run once per partition:
-- system = the named system (or '' for `|code`), and system = IMPLICIT.
SELECT last_updated, id, version_id FROM token_system_code_index
 WHERE tenant_id = ? AND resource_type = ? AND param = ? AND system = ? AND code = ?
   [AND (last_updated, id) < (?, ?)]
 LIMIT ?;

-- T3 `system|`: token_system_index, one partition (param, system).
SELECT last_updated, id, code, version_id FROM token_system_index
 WHERE tenant_id = ? AND resource_type = ? AND param = ? AND system = ?
   [AND (last_updated, id) < (?, ?)]
 LIMIT ?;
```

**Several partitions are one query each, merged in Rust.** Checked on a 5.0.9 node, 2026-10-13: `system IN ('', 'urn:x-helios:implicit code system') AND code = 'male' AND (last_updated, id) < (…) LIMIT n` is accepted, but rows come back partition by partition in partition-key order, not globally sorted, and `LIMIT` applies to the whole result. `ORDER BY` with that `IN` is rejected when paging ("Cannot page queries with both ORDER BY and a IN restriction on the partition key"). So T2's two partitions, and each value of a comma list, are separate queries whose sorted streams Rust merges, as the design's "merges two sorted streams" implies. An `IN` with `PER PARTITION LIMIT n` is the one-query alternative; Rust still merges.

### 5.3 URI

One partition `uri_index (param, uri)`. Exact and case-sensitive: a trailing `/` is a different partition. The reachable uri parameters are `_profile` and `_source` (scope: `coverage.md` C1).

```sql
SELECT last_updated, id, version_id FROM uri_index
 WHERE tenant_id = ? AND resource_type = ? AND param = ? AND uri = ?
   [AND (last_updated, id) < (?, ?)]
 LIMIT ?;
```

### 5.4 `_id`

`_id=a,b,c`: R1 for each id, concurrently (`read_batch`), one `resources (T,P,id)` partition per id. No index table and no tag check: the `resources` row is the truth. Drop absent and deleted rows, sort by `(last_updated DESC, id DESC)` in Rust, cut at the cursor. With other parameters, `_id` drives and each other token or URI parameter is a point read (5.8).

### 5.5 `_lastUpdated`

One partition `resources_by_type (T, P)`. Prefix-to-slice table: [`coverage.md` section 3](coverage.md#3-date-prefixes-r4-searchhtmlprefix); every upper bound there is strict (`<`).

```sql
-- L1: lower bound only (gt, sa, ge, and ne's upper half), with the cursor. Accepted on the node.
SELECT last_updated, id, version_id FROM resources_by_type
 WHERE tenant_id = ? AND resource_type = ?
   AND last_updated >= ?                -- the prefix's lower bound
   [AND (last_updated, id) < (?, ?)]    -- the cursor, from page 2 on
 LIMIT ?;

-- L2: an upper bound e (eq and ap, with their lower bound; lt, le, eb, and ne's lower half, alone).
-- Only one end bound is allowed (G19): send last_updated < e when e <= the cursor's
-- last_updated (or there is no cursor); otherwise send (last_updated, id) < (?, ?) instead.
SELECT last_updated, id, version_id FROM resources_by_type
 WHERE tenant_id = ? AND resource_type = ?
   [AND last_updated >= ?]              -- eq and ap only
   AND last_updated < ?
 LIMIT ?;
```

When `e <= cursor.last_updated`, every row below `e` is also below the cursor, so the bound alone is exact; otherwise the cursor alone is. `ne` reads `>= e` first, then `< s`; in `DESC` order that is a concatenation. `RangePredicate::matches` still runs on every row.

### 5.6 Unfiltered type search (`GET /Patient`)

One partition `resources_by_type (T, P)`. Each row gets the tag check; the read passes every tombstone between live rows (G8).

```sql
SELECT last_updated, id, version_id FROM resources_by_type
 WHERE tenant_id = ? AND resource_type = ?
   [AND (last_updated, id) < (?, ?)]
 LIMIT ?;
```

### 5.7 Date prefixes (date-only search)

One partition per table: `(T, P, param)`. The design's plan per prefix, with match conditions and residuals, is [`coverage.md` section 3](coverage.md#3-date-prefixes-r4-searchhtmlprefix). Two slice shapes cover all of them:

```sql
-- start slices on date_index (eq: both bounds; lt, le, ap: start < ?; sa: start >= ?)
SELECT start, id, end, version_id FROM date_index
 WHERE tenant_id = ? AND resource_type = ? AND param = ?
   AND start >= ? AND start < ?;

-- end slices on date_index_by_end (gt and ge's first half: end > ?; eb: end <= ?)
SELECT end, id, start, version_id FROM date_index_by_end
 WHERE tenant_id = ? AND resource_type = ? AND param = ?
   AND end > ?;
```

One clustering range per query: checked on a 5.0.9 node, 2026-10-13, `start > X AND end > Y` and `start > X AND id > 'a'` are both rejected. Hence two tables.

Grouped by cost:

- **Exact single slices** (`gt`, `lt`, `sa`, `eb`): every row in the slice matches.
- **Single slices with a residual** (`eq`, `le`, `ap`): some rows are read and dropped.
- **Unions** (`ne`, and `ge` as the design writes it): two slices, deduplicated by id. G20 has a single-slice option for `ge`.

All but `eq` are one-sided. Date tables have no `last_updated`, so the page order is known only after the R1 reads: the whole candidate set is read, tag-checked, sorted in Rust and cut at the cursor on every page, under the candidate cap (G20).

### 5.8 Several parameters (AND)

One token or URI partition drives in the shared order from the cursor. For each candidate that passes the tag check, every other token or URI parameter is a clustering point read using `resources.last_updated`:

```sql
SELECT version_id FROM token_system_code_index
 WHERE tenant_id = ? AND resource_type = ? AND param = ? AND system = ? AND code = ?
   AND last_updated = ? AND id = ?;
```

Dates and `_lastUpdated` are residual filters on the loaded `resources` row, as the design says ("Date predicates combined with any other parameter are residual filters"): re-extract the dates and apply `RangePredicate::matches`; compare `resources.last_updated` for `_lastUpdated`. A slice on the driving partition's `last_updated` is an optimisation option (G19).

### 5.9 `count`, `search_count`, `_count`

| Call | Statements | Partitions |
| --- | --- | --- |
| `count(tenant, Some(P))` | `SELECT id FROM resources_by_type WHERE tenant_id=? AND resource_type=?`, paged by the driver and counted in Rust (or `SELECT COUNT(*)`, which pages internally); no tag check | one, read whole, tombstones included |
| `count(tenant, None)` | one scan per known resource type (G7) | one per type |
| `search_count` with filters (`_count=0`, `_summary=count`) | the stream of 5.1 run to the end, with tag checks | as the search |
| `search_count` with no filters | `count`, or the stream of 5.6 (G7) | one |
| `_count=n` | page size `n`; read until `n + 1` resources pass | as the search |

### 5.10 A cursor page

The cursor is a plain `PageCursor`: `sort_values = [lu as RFC 3339 with milliseconds, <reserved bucket slot>]`, `resource_id = id`, encoded with `PageCursor::encode`. The page key is `(last_updated, id, bucket)`; `id` travels in `resource_id`, so the design's "third slot" for the bucket is the second element of `sort_values`.

Page 2 of `GET /Patient?gender=female&_count=20`:

1. `PageCursor::decode`; a malformed cursor is `SearchError::InvalidCursor`, 400.
2. T1 on `token_code_index (T, Patient, gender, female)` with `(last_updated, id) < (?, ?) LIMIT 50`.
3. Collapse, tag check, stop at 21 kept.
4. If the 21st exists, mint `next` from the 20th's `(last_updated, id)`. No `previous` on page 1.

The tuple relation compares by value on the `DESC, DESC` order, so it excludes every row of the cursor's own resource and places ties exactly (spike A3). A resource updated during paging moves ahead of the cursor and is not seen again (G21).

## 6. The sweeper

A background task in each Helios process (ADR-aacruzgon-0013 to 0015). It never compares `write_id`.

**What it reads.** Every poll (a few seconds), I3 on the 16 shards of the current and previous hour bucket, 32 partitions. Each `(bucket, shard)` keeps a low-water mark in memory, and an intent's bucket is always the hour of its own `enqueued_at`. I3 reads only after the mark and before `now − grace` (30 s default), so it skips finished intents' tombstones. After each successful poll the mark moves to the slice's upper bound, even if no rows came back; an intent that fails goes to an in-memory retry list instead of holding the mark. At startup the sweeper first walks the 25 most recent buckets (a 24 h TTL can span 25), 400 partitions, in short slices.

**For each intent, with `V = intent.version_id`:** R2 reads the `resources` row at `LOCAL_SERIAL`, which settles any in-flight Paxos proposal. Then one of four cases, each ending with I2:

| Case | Reads | Writes |
| --- | --- | --- |
| no row, or `version_id < V`: the CAS never applied | — | none |
| `V` current: a crashed or slow request | R3 for `V-1` (not for `V = 1`) | step 4 for `V`'s own row (never V3) and step 5, at `ts(V)`: V1, or V2 with `K(V)` empty if `deleted_at` is set; delete `K(V-1) \ K(V)`, insert `K(V)` |
| `V` superseded | R3 for `V` (for `ts(V)`) and `V-1` | delete all of `K(V-1)` at `ts(V)`; no inserts, since a later version already deleted `K(V)` and a late insert would outlive those tombstones (3 h) |
| `V` superseded, row `V` missing (two crashes in a row) | R3 for `V-1` | delete all of `K(V-1)` at `ts(V-1) + 1`, and log; `V` never reached step 5 |

`K(V-1)` is empty for version 1, a marker, or a missing row `V-1`, whose writer never reached step 5. Every write is older than any later version's, so it never removes a newer row, even after that row's tombstones are purged. A sweeper and a slow request may run the same writes. The grace period is the one clock comparison (G11).

## 7. Worked example: one Patient, four versions

**Setup.** Tenant `acme`, a Patient with one identifier, a gender and a birth date:

```json
{ "resourceType": "Patient",
  "identifier": [{ "system": "http://hospital.example/mrn", "value": "12345" }],
  "gender": "male",
  "birthDate": "1980-05-01" }
```

Extraction gives three parameters: `identifier` (token, system `http://hospital.example/mrn`, written `MRN`, code `12345`); `gender` (token on a `code` element, system `IMPLICIT`, code `male`); `birthdate` (date, `[1980-05-01T00:00:00.000Z, 1980-05-02T00:00:00.000Z)`, written `[B0, B1)`). The id is `p1` (really a UUID string), the intent shard is 7, and all four writes fall in bucket `'2026-10-14T10'`. `tu(V)` is each intent's `enqueued_at` timeuuid, from the writer's clock.

| Version | Operation | `ts(V)` (µs) | `lu(V)` | `write_id` |
| --- | --- | --- | --- | --- |
| 1 | create (`POST`) | 1791972000123000 | 2026-10-14 10:00:00.123 | w1 |
| 2 | update, gender `male` → `female` (`PUT`, `If-Match: W/"1"`) | 1791972300456000 | 10:05:00.456 | w2 |
| 3 | delete (`DELETE`, no `If-Match`) | 1791972600789000 | 10:10:00.789 | w3 |
| 4 | revive (`PUT` of the version 2 content) | 1791972900012000 | 10:15:00.012 | w4 |

`lu(1) = now_ms`; each later `lu(V) = max(now_ms, lu(V-1) + 1 ms)`, and `now` was always the larger; `ts(V) = lu(V) · 1000`.

### Version 1: create

I1 inserts `(tu(1), acme, Patient, p1)` with `w1` and `version_id 1` into `('2026-10-14T10', 7)`. C1 applies: `version_id 1`, `last_updated` and `created_at` `lu(1)`, `fhir_version '4.0'`, `write_ts ts(1)`, `write_id w1`, `data`. V1 writes version 1 at `ts(1)`. Then 9 index inserts, all `USING TIMESTAMP 1791972000123000`, tag 1; then I2.

| Table | Partition key (after `acme, Patient`) | Clustering key |
| --- | --- | --- |
| `resources_by_type` | — | `(lu(1), p1)` |
| `token_code_index` | `(identifier, 12345)` | `(lu(1), p1, MRN)` |
| `token_system_code_index` | `(identifier, MRN, 12345)` | `(lu(1), p1)` |
| `token_system_index` | `(identifier, MRN)` | `(lu(1), p1, 12345)` |
| `token_code_index` | `(gender, male)` | `(lu(1), p1, IMPLICIT)` |
| `token_system_code_index` | `(gender, IMPLICIT, male)` | `(lu(1), p1)` |
| `token_system_index` | `(gender, IMPLICIT)` | `(lu(1), p1, male)` (never read, G18) |
| `date_index` | `(birthdate)` | `(B0, p1, B1)` |
| `date_index_by_end` | `(birthdate)` | `(B1, p1, B0)` |

Nine partitions, nine rows; at an estimated 150 to 250 bytes per row, one batch under 5 KiB.

### Version 2: update (gender `male` → `female`)

`current` is version 1, so `K(1)` is the table above. I1 `(tu(2), acme, Patient, p1)`, `w2`, `version_id 2`. C2 `IF version_id = 1 AND deleted_at = null` applies: `version_id 2`, `lu(2)`, `ts(2)`, `w2`, new `data`. V1 writes version 2 at `ts(2)`; V3 rewrites version 1 from `current` at `ts(1) − 1`, so the original wins and nothing changes. Index changes, all `USING TIMESTAMP 1791972300456000`:

- **7 deletes:** every non-date row of `K(1)`.
- **9 inserts, tag 2:** `K(2)`, which is `K(1)` with `lu(2)` for `lu(1)` and `female` for `male`. The two date keys are unchanged, so they are only re-inserted (tag 1 → 2).

Then I2. Seven row tombstones, one in each of seven partitions. The identifier did not change, but its rows move, because `last_updated` is in the key.

**Between C2 and the index writes** (the in-flight window), `resources` is at version 2 while every index row has tag 1. `identifier=MRN|12345` reads `(lu(1), p1)` tag 1, R1 says 2, so the patient is hidden; `gender=female` finds nothing yet. Afterwards both work and `gender=male` is empty.

### Version 3: delete

REST read version 2 and, with no `If-Match`, called `delete`. R1 gives version 2, `data`, `lu(2)`. I1 `(tu(3), acme, Patient, p1)`, `w3`, `version_id 3`. C3 `IF version_id = 2 AND deleted_at = null` applies: `version_id 3`, `deleted_at` and `last_updated` `lu(3)`, `ts(3)`, `w3` (`data` kept, G29). V2 writes the marker, version 3, no `data`, at `ts(3)`; V3 rewrites version 2 from R1's `data` at `ts(2) − 1`. Then 9 deletes at `ts(3)` (`USING TIMESTAMP 1791972600789000`), every key of `K(2)`, including the two date rows; then I2.

Now `read` and vread 3 return 410; vread 2 returns version 2's content with 200; every search misses the patient.

### Version 4: revive

`PUT /Patient/p1` with the version 2 content. REST's read returned `Gone`, so it called `create_or_update`. R1: version 3, `deleted_at` set, so branch c. I1 `(tu(4), acme, Patient, p1)`, `w4`, `version_id 4`. C4 `IF version_id = 3` applies: `version_id 4`, `lu(4)`, `ts(4)`, `w4`, `data`, `deleted_at = null` (one cell tombstone). V1 writes version 4 at `ts(4)`; V3 rewrites the version 3 marker at `ts(3) − 1`, `deleted_at` set, `data` unset. `K(3)` is empty, so 9 inserts and no deletes, at `ts(4)` (`USING TIMESTAMP 1791972900012000`), tag 4: `K(2)` with `lu(4)` for `lu(2)`. Then I2. Returned: 201 (`created = true`), `ETag: W/"4"`.

The two date keys are the ones version 3 deleted. They come back because the insert at `ts(4)` is newer than the delete at `ts(3)`. With equal timestamps the delete would win, which is why `ts` must strictly increase per resource.

### Final state

| Table | Live rows for `p1` | Row tombstones left by `p1` |
| --- | --- | --- |
| `resources` | 1 row, version 4 | 1 cell tombstone (`deleted_at`) |
| `resource_versions` | versions 1, 2, 3 (marker), 4 | 0 |
| `pending_index_writes` `('2026-10-14T10', 7)` | 0 | 4 |
| `resources_by_type` | `(lu(4), p1)` tag 4 | 2 (`lu(1)`, `lu(2)`) |
| identifier, 3 token partitions | one row each at `lu(4)`, tag 4 | 2 each (`lu(1)`, `lu(2)`) |
| gender `male`: `token_code_index`, `token_system_code_index` | 0 | 1 each |
| gender `female`: `token_code_index`, `token_system_code_index` | one row each at `lu(4)`, tag 4 | 1 each |
| `token_system_index (gender, IMPLICIT)` | `(lu(4), p1, female)` tag 4 | 2 |
| `date_index`, `date_index_by_end` | one row each, tag 4 | the `ts(3)` delete, shadowed by the `ts(4)` insert until compaction |

`list_versions` returns `["1", "2", "3", "4"]`. Four writes left 16 index row tombstones (7 from the update, 9 from the delete; the two date ones are shadowed by version 4) and 4 intent tombstones, for one resource. The three V3 rewrites changed nothing and left no tombstones.

### What a crash would change

- **Killed after I1 of version 2** (no CAS): after 30 s the sweeper reads `resources` at `LOCAL_SERIAL`, sees version 1, below the intent's 2, and deletes the intent. Nothing else changes.
- **Killed after C2 of version 2** (no version row, no index writes): `read` returns version 2 at once, but vread 2 is 404 until the sweep (G2). Search hides the patient. The sweeper finds version 2 current, takes `K(2)` from `resources` and `K(1)` from row 1, writes row 2, deletes `K(1) \ K(2)`, inserts `K(2)` at `ts(2)`, and deletes the intent.
- **Same crash, but version 3 (the delete) lands before the sweeper runs.**
    - The delete pre-reads version 2's content. Its V3 restores row 2 at `ts(2) − 1`. It deletes `K(2)` at `ts(3)`; of those 9 keys only the two date keys exist (equal to `K(1)`'s, tag 1), so the date rows go.
    - Left behind: the 7 non-date rows of `K(1)`, tag 1: `resources_by_type (lu(1), p1)`, three identifier rows and three gender `male` rows. The tag check hides them; `count` is 1 for a deleted patient (G7).
    - The sweeper finds `resources` at 3, above the intent's 2: version 2 is superseded. It reads `ts(2)` from row 2 and `K(1)` from row 1, deletes all 9 keys of `K(1)` at `ts(2)` (the 7 orphans, and the two date keys version 3 already deleted), inserts nothing, then runs I2.
    - After the sweep: vread 2 is 200, `list_versions` is `["1", "2", "3"]`, `count` is 0. As in the crash-free run there are 16 index row tombstones (the two date keys carry both deletes, merged into one each); only the shadowed `K(2)` inserts are missing.

## 8. Gaps and questions for the session

G numbers are stable references; the list is grouped by severity, not sorted.

- **Resolved in the design** (kept short, so a reader sees why it changed): G1, G2, G3, G4, G5, G6, G9, G10, G11, G13, G16, G25, G26, G31.
- **Correctness bugs** (open): G7, G22.
- **Performance**: G8, G12, G17, G18, G20, G30.
- **Decisions to record**: G14, G15, G19, G21, G23, G24, G27, G28, G29.

### Resolved in the design

**G1. The sweeper left orphans when a later write superseded a crashed one.** *Resolved in the design (ADR-aacruzgon-0013).* The old sweeper dropped any intent whose `write_id` no longer matched, so the `K(N)` rows before the crash stayed. Now the intent carries `version_id` and the sweeper replays `V` whenever it exists (section 6). No `write_id` on `resource_versions` is needed: the replay is safe whoever made `V`.

**G2. A crash could lose a version's content for good.** *Resolved in the design (ADR-aacruzgon-0012).* A crash before the version row, then another update, overwrote `N+1` in `resources` before it reached history. Now every update, delete and revive rewrites row `N` (V3). V3 is at `ts(N) − 1`, so an existing row always wins. Left over, and now stated in the design: vread `N+1` is 404 until the sweep or the next write; two crashes in a row still lose a version's content.

**G3. Where the sweeper gets `K(V-1)`.** *Resolved in the design (ADR-aacruzgon-0013).* From row `V-1`, empty for version 1 or a marker. The row is there unless two crashes came in a row: then the sweeper deletes `K(V-1)` at `ts(V-1) + 1` (section 6). A registry change is G22.

**G4. `ts(N)` was not available on the update path.** *Resolved in the design (ADR-aacruzgon-0016).* `StoredResource` carries no `write_ts`. Now `lu(N+1) = max(now_ms, lu(N) + 1 ms)` and `ts = lu · 1000`, so any version's `ts` is recomputable, as V3 needs. Side effect: a burst of updates, or a clock stepping back, keeps `lu` ahead of the wall clock.

**G5. Tombstones in `pending_index_writes`.** *Resolved in the design (ADR-aacruzgon-0015).* With `tenant_id` before `enqueued_at`, every poll read the hour's tombstones (on a 5.0.9 node: "Read 0 live rows and 3000 tombstone cells"). Now `enqueued_at` leads and I3 reads past the mark (same test: 0 tombstone cells). Smaller buckets and `TimeWindowCompactionStrategy` stay options.

**G6. The sweeper polled 2 buckets, but intents live 24 h.** *Resolved in the design (ADR-aacruzgon-0014).* After an outage over an hour, a crashed create stayed unsearchable for good. Now the sweeper walks the 25 most recent buckets at startup. Past the TTL a crashed create stays unsearchable (readable by id) until next written, a limitation the design now states.

**G9. The CAS result mapping depends on the operation.** *Resolved in the design (step 3; a correction, no ADR).* `deleted_at` set is 410 for `read` and `vread` only; every write maps it to `NotFound`, 404, which the race suite allows (lines 92 to 93 and 436 to 443). `spike-findings.md` point 3 is corrected.

**G10. Repair on read could delete a key the in-flight writer needs.** *Resolved in the design (ADR-aacruzgon-0017).* A delete wins a timestamp tie, so deleting a stale date key at `ts(N+1)` beat the writer's insert. Now repair deletes only keys the current version does not produce and upserts the rest (5.1). Its read-latency cost is unmeasured.

**G11. The grace period is a clock comparison.** *Resolved in the design (documented).* The design said "no clocks are compared". It now states the assumption: clock skew plus intent-to-CAS latency stays under the grace period, which also guards the mark. A request that stalls longer and then crashes before its index writes is left to repair on read; a create or revive in that state stays unsearchable until next written, a stated limitation. So does a pause past `gc_grace_seconds` that resurrects rows `count` then includes. Deployment requirement: NTP, skew well under 30 s.

**G16. Columns on `resource_versions` and the intent row.** *Resolved in the design (ADR-aacruzgon-0013).* The intent row gained `version_id int`. `resource_versions` needs no `write_id`, since the sweeper no longer compares it, and no `created_at` (G14).

**G31. Sweeper edge cases found in the revision.** *Resolved in the design (ADR-aacruzgon-0013, 0015).* A late replay could resurrect `K(V)`: a superseded `V` now gets deletes only. A double crash: intent `V` deletes `K(V-1)` at `ts(V-1) + 1`, and a missing previous row means empty previous keys. A stuck intent pinned its mark, and an empty slice never moved it: now a retry list, and the mark moves to each poll's upper bound. Marks are per `(bucket, shard)`, with `bucket` from `enqueued_at`.

**G13. Revive details.** *Resolved in the design (ADR-aacruzgon-0024).*
- C4 clears `deleted_at` with a literal `deleted_at = null` in the statement. That is not a bound null: it writes exactly one cell tombstone, on purpose.
    - Checked on a 5.0.9 node, 2026-10-13: `UPDATE resources SET deleted_at = null, version_id = 4, … IF version_id = 3` applies, and `DELETE deleted_at FROM resources WHERE … IF version_id = 3` also works.
    - It is now the one stated exception to "never bind null" in `schema.cql`, in section 2 ("Compare-and-swap on `resources`") and in the roadmap's Step 1 checklist.
- A revive returns `created = true` (201) and keeps the **original** `fhir_version`, bound in C4 from R1. This matches every other backend:
    - `create_or_update` uses the request's version only to create;
    - `restore_deleted` and `update` keep the stored one (`sqlite/storage.rs:596` and 734, `postgres/storage.rs:1321` and 1429, `mongodb/storage.rs:2873` and 4981-4987).

**G25. Plain delete losing a race, and 404 for absent ids.** *Resolved in the design (ADR-aacruzgon-0024).*
- A `delete` without `If-Match` whose CAS misses because an update landed now retries once, at once, from the pre-read with a new I1, as MongoDB does (#1405).
- A second miss is `VersionConflict`, 409. The race suite still holds: "a resource is deleted once".
- `delete_versioned` never retries.
- A delete of an unknown or already-deleted id stays 404, matching Helios (`delete.rs`), not the spec's 200/204 (3.4).

**G26. 503 or 504.** *Resolved in the design (ADR-aacruzgon-0024).* The rule is the one `crates/rest/src/error.rs` already applies:
- a timeout (`WriteTimeout`, `ReadTimeout`, a timed-out serial read, or an outcome a timeout left undecidable) is `BackendError::Timeout`, 504;
- an unavailable node or connection, or a replica failure (`WriteFailure`, `ReadFailure`, or an outcome a `WriteFailure`/CAS left undecidable), is `BackendError::Unavailable`, 503 (amended 2026-11-05 after review on PR #32).

The failed intent write (3.1, step 2) and the unknown CAS outcome (3.1) now say so.

### Correctness bugs

**G7. `count` has no tag check, so it counts what `resources_by_type` holds, not what is live.** *Severity: high for the lasting errors; the in-flight ones are expected.*

| Kind | Case | Effect on `count` |
| --- | --- | --- |
| expected, in flight | create in flight or crashed after the CAS | one too few until the sweep |
| expected, in flight | delete in flight or crashed after the CAS | one too many until the sweep |
| expected, in flight | update split across batches | briefly 0 or 2 |
| expected, until the sweep | orphan `resources_by_type` row a crash left | one too many until the sweep; after TTL expiry, a stall past the grace, or a pause past `gc_grace_seconds` (G11), until repair reaches it |
| lasting | row resurrected after `gc_grace_seconds` (3 h on `resources_by_type` in `schema.cql`) | one too many, for good. Only with RF 3 and a replica outage longer than `max_hint_window` (3 h) that repair does not cover in time; impossible with RF 1 |

- So the design's "the tag check makes resurrected rows harmless" does not hold for `count`. Options: dedupe ids in memory during the scan (about 4 MB per 100,000 ids; fixes duplicates only); keep the default `gc_grace_seconds` on `resources_by_type`; or a tag check per row (too slow).
- `count(tenant, None)` (called by `/console` metrics, `console_metrics.rs`): no table enumerates a tenant's types. Loop over the registry's types (about 145 in R4), or add a per-tenant types table.
- A filtered `search_count` (`_count=0`): tag checks and the candidate cap? It must agree with the unfiltered path, or `GET /Patient?_count=0` and `GET /Patient?gender=male,female,other,unknown&_count=0` differ.

**G22. Old keys are recomputed with today's registry.** *Severity: low (orphans are hidden, but stay).* `K(N)` is re-extracted with the registry and extractor as they are now. If a SearchParameter or the extractor changed since version `N` was indexed, the deletes miss rows that were written. Option: store each version's key set (a compact `index_keys` blob on `resource_versions`), so deletes are exact and the sweeper and repair need no re-extraction. Cost: bytes per version; a schema change.

### Performance

**G8. Benchmark case: `count` and the last pages of an unfiltered search after a mass update.** *Severity: medium; a measurement to schedule, not a bug.* Updating every Patient once leaves a contiguous run of tombstones at the old end of `resources_by_type`, which a page reaching it reads in one command. 100,000 updated resources of one type give 100,000 tombstones, exactly the `tombstone_failure_threshold`; past it the read fails with `TombstoneOverwhelmingException`, a 5xx. That also needs `gc_grace_seconds` (3 h here) and compaction not to have purged them, which holds if the update phase and the reads run within 3 h. It is the design's own measure-first trigger (above 1,000 tombstones per read) firing by construction: make it a named benchmark case.

**G12. Several Helios processes means several sweepers.** *Severity: low (one process in this project).* Each polls all 32 partitions with its own marks, runs its own startup scan, and races the others on the same intents. The writes are idempotent, so the cost is duplicate I3 reads, Paxos reads and replays. Accept it, or take a lease row with an LWT.

**G17. `_id` and `_lastUpdated` from the extractor.** *Severity: medium.* `SearchParameterExtractor::extract` returns `_id` (token) and `_lastUpdated` (date) for every resource, and also `_profile`, `_source` (URI) and `_tag`, `_security` (token) (`search/extractor.rs`, comments around lines 1066 to 1072; tests around 2358 to 2401 assert they are indexed).

- If written, every resource gets three `_id` token rows no query reads, and a `_lastUpdated` row in both date tables whose key changes on every update: two more tombstones per update, in partitions holding every resource of the type.
- Proposed: drop `_id` and `_lastUpdated` before computing `K(V)`; `coverage.md` serves both without index rows. Scope of the other four: `coverage.md` C1.

**G18. Rows that no query reads.** *Severity: medium.*

- `token_system_index` rows for system `IMPLICIT`: `system|` never matches the marker. `(gender, IMPLICIT)` would hold every Patient, `(status, IMPLICIT)` every Observation.
- `token_system_index` rows for system `''`: no query form asks for "system is empty" with a pipe after it.
- Token rows with an empty code (`CodeableConcept.text` only, or an Identifier with a system and no value) belong in `token_system_index` only, never in the code-keyed tables.
- URI rows with an empty URI.
- Skipping these saves one row in three for every `code`-typed element: 8 rows per version in the worked example, not 9.

**G20. Date-only searches read their whole candidate set on every page.** *Severity: medium.*

- The page order is known only after R1 for every candidate, so page `k` repeats the full scan and all the point reads. Every prefix but `eq` is one-sided, so `birthdate=lt2000` on 100,000 patients hits the candidate cap.
- Option: page date-only results in the table's own order `(start, id)` or `(end, id)`, with the cursor in the reserved slot: native paging for every prefix except the unions `ne` and `ge`.
- Option for `ge`: the single slice `date_index_by_end end > s` with the full predicate as residual, instead of the design's union. Every `ge` match has `te > s`, so nothing is missed; it reads more rows when many ranges end inside `[s, e)`. (`le` is already the single slice `start < e`.)
- Undefined: is a search with two date parameters, a repeated date (`date=ge2020&date=lt2021`), or a date with `_lastUpdated` "date-only"? The design is silent. Proposed: slice on one date, residual for the rest.

**G30. Hot and huge partitions.** *Severity: medium; measure-first in the design.* `token_code_index (Patient, gender, female)` holds about half the patients; `token_system_index (identifier, MRN)` holds all of them if every patient has an MRN; with G18 unfixed, so does `token_system_index (gender, IMPLICIT)`. Each update of a patient leaves a tombstone in every one of these partitions.

### Decisions to record

**G14. `created_at`.** *Severity: low; proposed in ADR-aacruzgon-0011.* `resources` has it, `resource_versions` does not, and PostgreSQL stores none and passes `last_updated` as `created_at` everywhere. The ADR keeps it on `resources` only, written at version 1 and kept on revive (as MongoDB does), and has vread pass the version's own `last_updated`, which avoids a second read.

**G15. vread of a delete marker.** *Severity: low.* The backend returns a `StoredResource` with `deleted_at` set and content `Value::Null` (PostgreSQL parity); REST turns it into 410 (404 if `HFS_RETURN_GONE` is false). A `vid` that is not a valid `int` is `Ok(None)`, 404, with no query. Record both.

**G19. `_lastUpdated` next to the cursor.** *Severity: medium; resolved on the node, needs a line in the design.* Checked on a 5.0.9 node, 2026-10-13, on `resources_by_type`:

| Restriction | Result |
| --- | --- |
| `last_updated >= X AND (last_updated, id) < (…)` | accepted |
| `(last_updated) >= (X) AND (last_updated, id) < (…)` | accepted |
| `(last_updated, id) >= (X, '') AND (last_updated, id) < (…)` | accepted |
| `last_updated <= X AND (last_updated, id) < (…)` | rejected: "More than one restriction was found for the end bound on last_updated" |

- A lower bound and the cursor work together. An upper bound (`eq`'s upper half, `lt`, `le`, `eb`, `ap`'s upper half, `ne`'s lower half) and the cursor are both end bounds, and only one is allowed. Rule: Rust picks the tighter and sends only that one (5.5, L2); no stop condition is needed.
- Optimisation option, not the design: with another parameter `_lastUpdated` is a residual filter (5.8), but the token and URI tables also cluster on `last_updated`, so it could be a slice on the driving partition, under the same one-end-bound rule, reading fewer rows when the window is narrow. Record whether to take it.

**G21. Keyset paging on a changing key.** *Severity: low.* A resource updated while a client pages moves ahead of the cursor and is skipped; one on an earlier page that is updated is not seen twice. The spec leaves this to the server, but roadmap 6d's exit criterion ("exactly 100,000 distinct ids") holds only with no writes during the run. Say so in the criterion.

**G23. What the client gets when the CAS applied but the index writes failed.** *Severity: medium.* Retries are exhausted and the intent row is in place.

5xx is wrong for `POST`: the resource exists, and a client retry creates a second one (conditional create is out of scope). Success breaks the design's "any write the client saw succeed is searchable". Proposed: return success, rely on the sweeper, and narrow the claim to "searchable within poll interval plus grace when the index writes fail".

**G24. Retries and idempotency.** *Severity: medium.*

- Index batches and version rows are idempotent (fixed timestamps). The request now keeps I1's full key, so a retried intent rewrites the same row (design step 2).
- `bucket` is the UTC hour of `enqueued_at` itself, never a second clock reading (now in the design).
- The sweeper reads `shard` back from the row, so any function works, even random. If it is a hash, name one stable across processes and Rust releases (not `DefaultHasher`).
- `create_or_update` writes a new intent row for each turn, with its own `enqueued_at` and that turn's version, and a turn whose CAS misses deletes its own row (ADR-aacruzgon-0024). Rewriting one key with a new version, or deleting it and writing it again, would depend on coordinator timestamps.
- HTTP retries: a `POST` after a timeout creates a duplicate; a `PUT` with `If-Match: W/"N"` after success gets 412; a `DELETE` after success gets 404. The other backends do the same; the demo script and the benchmark harness must not count these as Cassandra errors.

**G27. `too-costly` has no search error.** *Severity: low.* `SearchError::TooManyResults`, the closest variant, maps to 422 `UnprocessableEntity` (`crates/rest/src/error.rs`). No search path produces issue code `too-costly`; its only producer is the batch handler's 500 for a transaction Bundle too large for MongoDB's cache (`handlers/batch.rs`). Accept 422, or add a variant for the upstream touch list. Also define what the cap counts: index rows read, or distinct candidates.

**G28. Batch `ifMatch` plus the `create_or_update` retry is last-writer-wins.** *Severity: low (upstream limitation).* REST checks a batch entry's `ifMatch` with a read, then calls `create_or_update` (`batch.rs` documents this window). Our loop retries on a newer version, so an entry conditioned on `W/"1"` can overwrite a version 2 it never saw. The router cannot call `update_with_match`, but our retry widens the window. Recorded in the design; the upstream `create_or_update` contract (#2111) makes the retry mandatory, and the bound is 3 retries, or 4 attempts (ADR-aacruzgon-0024).

**G29. Does a delete keep `data` in `resources`?** *Severity: low.* Keeping it costs nothing and writes no tombstone, and `read` never returns it; clearing it writes a cell tombstone. The walk-through keeps it. A revive must not read it as `K(N)` (the marker's key set is empty), and its V3 must leave `data` unset, as design step 4 now says.
