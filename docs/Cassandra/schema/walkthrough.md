# Paper walk-through of the schema

**DRAFT for the schema session (roadmap item 1.2).** Not reviewed by the team. Nothing here is decided. Where the walk-through had to pick something the design leaves open, it points to the gap list at the end (G1, G2, …).

This walks every in-scope operation through the ten tables, statement by statement. It follows the design in [`cassandra-discussion.md`](../cassandra-discussion.md) ("Proposed Cassandra data model" and "Consistency, failure handling and rejection"), the Step 1 checklist of [`development-roadmap.md`](../development-roadmap.md), and [`spike-findings.md`](../spike-findings.md). The tables are in [`schema.cql`](schema.cql); the directory overview is [`README.md`](README.md).

**This file holds the statements; [`coverage.md`](coverage.md) holds the expected results**: the per-parameter tables (token forms, date prefixes, `_lastUpdated`, `_id`, AND, cursor, count) and every rejection. Neither repeats the other.

Code facts were checked against the working tree on 2026-10-13 (`crates/persistence`, `crates/rest`). "Checked on a 5.0.9 node, 2026-10-13" marks live tests on a single Cassandra 5.0.9 node. Revised on 2026-10-14 for the design changes in ADR-aacruzgon-0012 to 0017, and on 2026-11-05 for ADR-aacruzgon-0024, 0026 and 0027. For ADR-0026 the code facts it relies on were re-checked on 2026-11-05 against `main` after the upstream sync (PR #28): `SearchQuery.fhir_version` and `SearchQuery.compartment`, the compartment membership rule of SQLite and MongoDB, the error variants, and every line reference in section 8. "Checked on a 5.0.9 node, 2026-11-05" marks the tests for ADR-0026.

> **Read this first.** No gap is open. The seven most serious gaps of the first draft (G1, G2, G4, G5, G6, G10, G11) are resolved in the design. So are G3, G9, G13, G16, G25, G26 and G31, the schema audit's G33 to G37 (ADR-aacruzgon-0026), and G7, G8, G12, G17, G18, G20, G22, G23, G27 and G32 (ADR-aacruzgon-0027). G14, G15, G19, G21, G24, G28, G29 and G30 were already settled by an earlier decision. Section 8 says how. G22 changed the schema: every version row now stores the index rows it wrote, as `index_keys`, and every compare-and-swap writes the same set into `resources.index_keys` (section 2, "Stored key sets"). G38 (a bound on a custom SearchParameter's `code`), G39 (`resources.deleted_at` without a tombstone) and G40 (V3 before the CAS) came from review on PR #33 (ADR-aacruzgon-0027).

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
| `K(V)` | the index rows version `V` wrote: one `resources_by_type` row, plus every token, date and URI row from `SearchParameterExtractor::extract` on `V`'s content when `V` was written, less the rows P5 drops (section 1, "Assumptions"). Written by the compare-and-swap that creates `V` into `resources.index_keys`, and by step 4 into row `V`'s `index_keys`; read back from there and never re-extracted (G22, section 2). Empty when `V` is a delete marker. No `code`, `system` or `uri` value in it is longer than 2,048 bytes, and no `param` longer than 128 (step 1, G35, G38). |
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

- Rows no query reads are dropped before the index keys are computed (P5, ADR-aacruzgon-0027, G17 and G18):
    - the `_id` token rows and `_lastUpdated` date rows from the extractor, which `resources` and `resources_by_type` serve;
    - display-only tokens, which have no system and an empty code (`CodeableConcept.text` alone, `IndexValue::token_display_only` in `search/converters.rs:189-197`);
    - `token_system_index` rows for the `IMPLICIT` marker, and for system `''` (G18);
    - `uri_index` rows whose URI is the empty string, which `convert_to_uri` writes for an empty value (`search/converters.rs:670-678`; G18).
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
-- write_id is not selected: the sweeper never reads it (G32).
SELECT enqueued_at, tenant_id, resource_type, id, version_id
  FROM pending_index_writes
 WHERE bucket = ? AND shard = ?
   AND enqueued_at > ? AND enqueued_at < minTimeuuid(?)
 LIMIT ?;
```

Partition: one `(bucket, shard)`. `bucket` is the UTC hour of `enqueued_at` (`'2026-10-14T10'`), `shard` is `hash(tenant_id, id) mod 16`. `minTimeuuid` is for query bounds only, never an inserted value.

### Compare-and-swap on `resources`

All four use `LOCAL_SERIAL` for Paxos and `LOCAL_QUORUM` for the commit, on partition `(T, P, id)`. Absent columns are left unset, never bound to null, with no exception. `resources.deleted_at` is always written: `0` (1970-01-01T00:00:00Z) means live, and a delete writes its own time, so `resources` never holds a tombstone (G39). `fhir_version` is the request's only in C1. C2 and C4 bind the version the resource was created with, from `current` or R1, as every other backend keeps it (ADR-aacruzgon-0024).

```sql
-- C1: create (POST, or create_or_update on a missing id). Version 1.
INSERT INTO resources
       (tenant_id, resource_type, id, version_id, data, last_updated,
        created_at, fhir_version, write_ts, write_id, index_keys, deleted_at)
VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 0)    -- index_keys = K(1); deleted_at 0 = live (G39)
IF NOT EXISTS;

-- C2: update of a live row, N -> N+1. fhir_version = current's (fixed at create).
UPDATE resources
   SET version_id = ?, data = ?, last_updated = ?, fhir_version = ?,
       write_ts = ?, write_id = ?, index_keys = ?     -- K(N+1)
 WHERE tenant_id = ? AND resource_type = ? AND id = ?
    IF version_id = ? AND deleted_at = 0;             -- 0 = live (G39)

-- C3: delete (plain and versioned), N -> N+1. Soft delete; never a CQL DELETE.
UPDATE resources
   SET version_id = ?, deleted_at = ?, last_updated = ?, write_ts = ?, write_id = ?,
       index_keys = 0x0100                             -- the empty set; deleted_at = lu(N+1)
 WHERE tenant_id = ? AND resource_type = ? AND id = ?
    IF version_id = ? AND deleted_at = 0;

-- C4: revive (create_or_update on a deleted row), N -> N+1.
-- deleted_at = 0 marks the row live again. It is an ordinary cell, newer than the
-- delete's, so the revive writes no tombstone (G39; replaces ADR-aacruzgon-0024's
-- literal deleted_at = null).
-- fhir_version = the original, from the row R1 read, never the request's (G13).
UPDATE resources
   SET version_id = ?, data = ?, last_updated = ?, fhir_version = ?,
       write_ts = ?, write_id = ?, index_keys = ?, deleted_at = 0   -- K(N+1); live
 WHERE tenant_id = ? AND resource_type = ? AND id = ?
    IF version_id = ?;
```

Every CAS writes `index_keys`, the key set of the version it creates (G22, below). It is in the same Paxos proposal as `data`, so it adds no statement.

**What a CAS returns when not applied** (spike A1; checked on a 5.0.9 node, 2026-10-13, and again with `index_keys` on 2026-11-05): `[applied] False` plus exactly the `IF`-clause columns with their current values, never `index_keys`. On a missing row, `[applied] False` only. `IF NOT EXISTS` returns the whole existing row.

**How it maps** (design step 3): no columns → `NotFound`, 404; `deleted_at > 0` → `NotFound`, 404; any other `version_id` → `VersionConflict`, 409. A write never returns 410; only `read` and `vread` do. After a definite miss the request runs I2.

### Version rows (`resource_versions`)

```sql
-- V1: content version. Plain write, LOCAL_QUORUM. write_id = w(V) (ADR-aacruzgon-0026).
-- index_keys = K(V), encoded as schema.cql says (ADR-aacruzgon-0027).
INSERT INTO resource_versions
       (tenant_id, resource_type, id, version_id, data, last_updated, fhir_version, write_ts, write_id, index_keys)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
USING TIMESTAMP ?;                       -- ts(V)

-- V2: delete marker. data is left unset; index_keys is the empty set, 0x01 0x00.
INSERT INTO resource_versions
       (tenant_id, resource_type, id, version_id, last_updated, deleted_at, fhir_version, write_ts, write_id, index_keys)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
USING TIMESTAMP ?;                       -- ts(V)

-- V3: rewrite of row N on update, delete and revive (ADR-aacruzgon-0012), written at
-- step 2, before I1 and the CAS (G40).
-- V1, or V2 for a marker (deleted_at > 0, data unset), from what the writer holds,
-- USING TIMESTAMP ts(N) - 1.  One microsecond older than the original row.
-- A content row leaves deleted_at unset: the live sentinel 0 is only for resources.
-- Built from R1 (delete, create_or_update, update_with_match): write_id = R1's write_id.
-- Built from update's StoredResource, which has no write_id: write_id left unset (G36).
-- index_keys: K(N), the index_keys of the resources row the writer read at
-- version N (R1; G22). Never left unset, and never re-extracted.
```

Partition: `(T, P, id)` of `resource_versions`, a different table from `resources`. Every version row carries the resource's own `fhir_version`, the one it was created with (G13), and the `write_id` of the CAS that created it, except a V3 built from `update`'s `StoredResource` (G36), and the `index_keys` of the version (G22, below). V3 is written at step 2, before the CAS (G40), so it no longer shares a batch with the `N+1` row. If row `N` exists, the original wins every cell, even if the content re-serialises differently; if `N`'s request crashed or paused before its step 4, V3 restores it. So whenever a CAS creating `N+1` has applied, row `N` exists, with the exact `K(N)`.

### Stored key sets (`index_keys`, G22)

Each version row stores `K(V)`, the index rows that version wrote, and the `resources` row holds the current version's copy, written by the CAS (ADR-aacruzgon-0027). Old keys are read back, not re-extracted, because the registry changes at runtime per tenant: a `SearchParameter` write invalidates the tenant's registry (`crates/persistence/src/backends/sqlite/storage.rs:520-529` in SQLite; the design gives Cassandra the same per-tenant `TenantSearchRegistries`). Re-extracting `K(N)` with today's registry would miss rows `N` wrote, and those orphans would cost point reads, count against the candidate cap, and never be cleaned.

- **Encoding.** Format 1, documented in `schema.cql` above `resource_versions`: a format byte `0x01`, a varint row count, then per row a table tag and the key columns the version row does not imply (`tenant_id`, `resource_type`, `id` and `last_updated` are the row's own). Strings are varint-length-prefixed UTF-8, timestamps 8-byte milliseconds. A later format gets a new first byte, and readers keep reading format 1.
- **Size.** The worked example's `K(1)`, 8 rows, is 289 bytes; `K(2)` is 293 (`female` for `male`); a marker's empty set is 2 bytes (computed from the encoding, section 7). The 2,048-byte rule bounds each value, so a blob grows by at most about 4 KB per row. The blob is in a single-partition write, which the batch thresholds do not check (`spike-findings.md`, B1).
- **Written** with the version, in statements that already run, so no extra write:
    - in `resources`, by every CAS: C1, C2 and C4 write `K(N+1)`, and C3 the empty set;
    - in `resource_versions`, in the same row and statement as the version: V1 and V2 at step 4, V3 at step 2, and the sweeper's replay.
- **Where `K(N)` comes from.** The `index_keys` of the `resources` row the writer read, R1 at `LOCAL_QUORUM`, before step 2. That read's `version_id` is the `N` the CAS is conditioned on. Deletes use it, and so does repair on read (5.1, step 4). The sweeper takes `K(V)` from its own R2 of `resources`, and `K(V-1)` from row `V-1`'s `index_keys`, since `resources` holds only the current version's (section 6).
- **Why the read is exact.** Only the CAS that creates version `N` writes `index_keys` at version `N`, and it writes `version_id` in the same mutation. So any read that returns `version_id = N` returns that CAS's `K(N)`. A CAS conditioned on `version_id = N` applies only if the row is still at `N`, so the keys it deletes are the ones `N`'s writer inserts. Nothing else writes `resources`: repair and the sweeper never do.
- **`update` reads `resources`, not row `N`** (R1, the one extra read). Its `StoredResource` has no blob, and row `N` of `resource_versions` may not exist yet: `N`'s writer may have crashed or paused between its CAS and step 4, which is exactly the race below. `resources` always holds `N`'s key set while `N` is current, because the CAS that made `N` current wrote it.
    - R1 shows `version_id = N`: use its `index_keys`.
    - R1 shows a higher version: the CAS could not apply. Return what C2's miss would give, without running it: `NotFound` (404) if `deleted_at > 0`, otherwise `VersionConflict` (409).
    - R1 shows a lower version: `N`'s CAS is still committing. Run R2 at `LOCAL_SERIAL`, which completes it, and use R2's row.
    - No row: `NotFound` (404).
- **Why the CAS carries it (CodeRabbit's review of PR #33).** With the key set only on `resource_versions`, a writer that found row `N` missing re-extracted `K(N)` under today's registry, and this sequence left orphans:
    1. Writer A extracts `K_A(N+1)` under registry R1, writes its intent, and its CAS applies `N+1`. A pauses before step 4.
    2. The tenant's registry changes to R2, through a `SearchParameter` write.
    3. Writer B updates `N+1` to `N+2`. Row `N+1` is missing, so B re-extracts `K_B(N+1)` under R2, deletes `K_B(N+1) \ K(N+2)` at `ts(N+2)` and inserts `K(N+2)`.
    4. A resumes and inserts `K_A(N+1)` at `ts(N+1)`. Keys in `K_A(N+1)` but not in `K_B(N+1)` or `K(N+2)` were never deleted: orphans tagged `N+1`. The sweeper did not help: it deletes `K(N)` for a superseded intent, never `K(N+1)`.

    Now B's R1 reads `resources` at `N+1`, whose `index_keys` is exactly `K_A(N+1)`, because A's CAS wrote it. B's CAS is conditioned on `version_id = N+1`, so B deletes `K_A(N+1) \ K(N+2)` at `ts(N+2)` and inserts `K(N+2)` at `ts(N+2)`. Each of A's late inserts at `ts(N+1)` is then either deleted by a newer tombstone or overwritten by B's newer insert, and A's late V1 for row `N+1` writes the same blob B's V3 wrote. No orphan is left. Checked on a 5.0.9 node, 2026-11-05: a key only in `K_A` stayed deleted after A's late insert, a key in both kept tag `N+2`, and row `N+1` held A's 289-byte blob. Carrying `K(N+1)` in A's intent row instead would not close it: B cannot find A's intent, whose key includes A's `enqueued_at`, and A deletes its intent when it resumes.
- **Cost.** The blob travels in every CAS's Paxos proposal and commit, and makes the `resources` row larger by the same amount: 289 bytes for the worked example's `K(1)`, and 2 for a delete's empty set (section 7). That example's `data` is about 140 bytes of JSON, without `meta`, so on that Patient the blob doubles what the CAS carries. A fixture resource's `data` is usually larger; the fixture sizes are measured in Step 8 (roadmap section 7). The CAS is already one partition and one statement, so its round trips do not change.
- **Why both copies are kept.** `resources` holds only the current version's set. The sweeper needs `K(V-1)` once `V` is current, from row `V-1`. Every version row keeps its own set, so a row V3 restores is complete and any version's keys can be checked against what it wrote (roadmap Step 5's exit criterion).
- **Versions written before the column.** None exist: no Cassandra data predates this schema, so every `resources` and version row has `index_keys`.
- **Extra reads and round trips per operation.** "Extra round trips" counts V3 at step 2 (G40). It used to share a single-partition batch with V1 at step 4; now it is its own sequential write before the CAS. The statement count is unchanged.

| Operation | Extra reads | Extra round trips | Why |
| --- | --- | --- | --- |
| `create` | 0 | 0 | `K(0)` is empty; no V3 |
| `update` | 1 (R1 of `resources`) | 1 (V3) | its `StoredResource` has no blob; `resources`, not row `N`, as above |
| `update_with_match` | 0 | 1 (V3) | its R1 already reads `resources` |
| `delete`, `delete_versioned` | 0 | 1 (V3) | the same |
| `create_or_update` | 0 | 1 (V3) per turn on a live row (b) or a delete marker (c); 0 on an absent id (a) | each turn's R1 already reads `resources`; a delete marker's set is empty |
| sweeper, `V` current | 0 | 0 | its R2 of `resources` already returns `K(V)`; R3 for `V-1` as before |
| sweeper, `V` superseded | 0 | 0 | it already reads rows `V` and `V-1` |
| repair on read | 0 | 0 | the tag check's R1 already returns the current version's `index_keys` |
| unknown CAS outcome | 0 | 0 | R2 already returns the row; R3 on row `V` runs only when `V` was superseded (3.1) |

A CAS that misses wastes its V3, which rewrites row `N` at an older timestamp and changes nothing.

- **Known limitation.** Adding a `SearchParameter` does not index existing resources. They are found by it only after their next write, and there is no reindex. Removing one leaves no orphans, since deletes use the stored rows.

### `deleted_at` and its readers (G39)

`resources.deleted_at` is always written. `0` (1970-01-01T00:00:00Z) means live, and C1 and C4 write it. A delete (C3) writes its `lu(N+1)`, which is never 0. A revive therefore overwrites the delete's cell with a newer one rather than deleting it, and `resources` holds no tombstone. A replica that missed any of these writes holds an older cell, and the newer one wins by timestamp, whatever the repair schedule. `resource_versions` is unchanged: a content row leaves `deleted_at` unset, and a marker sets it, which is never 0. A version row never changes state, so it never needs a tombstone either.

Every reader that decides "deleted" tests `deleted_at > 0`:

| Reader | Where | Deleted means |
| --- | --- | --- |
| `read` | 4.1, R1 | `ResourceError::Gone`, 410 (404 if `HFS_RETURN_GONE` is false) |
| `exists` | 4.1, R1 | `false` |
| `vread` | 4.2, R3 on `resource_versions` | a delete marker: `deleted_at` set, always `> 0`; 410 |
| `_id` search | 5.4, R1 | not returned |
| tag check, every search, filtered `search_count`, compartment residual | 5.1 step 4, R1 | candidate hidden |
| CAS miss mapping (C2, C3) | 2, "How it maps"; 3.3, 3.4 | `NotFound`, 404 |
| `update`'s R1, the pre-reads of `delete`, `update_with_match` | 2, "Stored key sets"; 3.3; 3.4 | `NotFound`, 404, nothing written |
| `create_or_update` branch choice | 3.5, R1 | branch c (revive) |
| sweeper, `V` current | 6, R2 | V is a marker: V2 for its row, and `K(V)` empty |
| `StoredResource::from_storage` | every R1 | `deleted_at` 0 is passed as `None` |

Unfiltered `count` reads `resources_by_type`, not `resources`, so it never tests `deleted_at`; a deleted resource has no `resources_by_type` row once its delete's index writes are done (5.9).

### Index rows

Every index statement carries `USING TIMESTAMP ts(V)` of the version being written. One version's inserts and deletes go in logged batches, each under 5 KiB (`batch_size_warn_threshold`), sent concurrently at `LOCAL_QUORUM`. The backend measures each batch itself, from every byte of its statements: the node's size check does not count partition-key bytes, which are most of an index row (checked on a 5.0.9 node, 2026-11-05: a 30-partition `uri_index` batch carrying about 120 KB of partition keys was accepted with no warning, while a `token_code_index` batch with 78 KiB of clustering values was refused as "Batch too large"). With the 2 KiB rule (G35) the largest single index row is about 4.3 KB, so every row fits in one batch.

```sql
-- X1 resources_by_type. fhir_version is the resource's, fixed at create (G34).
INSERT INTO resources_by_type (tenant_id, resource_type, last_updated, id, version_id, fhir_version)
VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ?;
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

**Index rule for version `V`:** delete `K(V-1) \ K(V)`, insert all of `K(V)`, every statement at `ts(V)`. A writer takes `K(V-1)` from the `index_keys` of the `resources` row it read at version `V-1`; the sweeper takes it from row `V-1` (G22). A key in both sets is only inserted, which rewrites its `version_id` tag. `last_updated` is in the clustering key of `resources_by_type` and every token and URI table, so only date keys survive an update unchanged.

**Partitions per version**, for `t` token, `d` date and `u` URI values: one `resources_by_type`; up to `3t` token; one `date_index` and one `date_index_by_end` per date parameter; up to `u` URI. An update touches the old ones and the new ones.

### Reads

```sql
-- R1: current row. LOCAL_QUORUM. Used by read, exists, _id, the tag check, the pre-read of
-- delete, update_with_match and create_or_update, and update's read of K(N) (G22).
-- index_keys is K(version_id): the writer's K(N), and repair's key set.
SELECT version_id, data, last_updated, created_at, deleted_at, fhir_version, write_ts, write_id, index_keys
  FROM resources WHERE tenant_id = ? AND resource_type = ? AND id = ?;

-- R2: the same SELECT at LOCAL_SERIAL. Completes or cancels an in-flight Paxos proposal.
-- Used after an unknown CAS outcome, and by the sweeper for every intent.

-- R3: one version. LOCAL_QUORUM. write_id decides a superseded unknown outcome (3.1);
-- index_keys gives the sweeper K(V-1) (G22).
SELECT data, last_updated, deleted_at, fhir_version, write_ts, write_id, index_keys
  FROM resource_versions
 WHERE tenant_id = ? AND resource_type = ? AND id = ? AND version_id = ?;

-- R4: all version numbers, oldest first. LOCAL_QUORUM.
SELECT version_id FROM resource_versions
 WHERE tenant_id = ? AND resource_type = ? AND id = ?
 ORDER BY version_id ASC;

-- R5: every partition key of resources_by_type, paged (5,000 rows), LOCAL_QUORUM. The one
-- read that crosses tenants: the stored-SearchParameter reload, at startup and on each TTL
-- refresh, keeps the SearchParameter rows client-side to list the tenants to reload.
-- A key-range read, valid without ALLOW FILTERING; it changes no table or key
-- (ADR-aacruzgon-0041).
SELECT DISTINCT tenant_id, resource_type FROM resources_by_type;
```

## 3. Writes

Every write first calls `tenant.check_permission(Operation::…, resource_type)`; a refusal is a `TenantError` before any statement runs.

### 3.1 Write-path skeleton (design steps 1 to 6)

| Step | Statement | Partition | On failure |
| --- | --- | --- | --- |
| 1 | permission check; for a SearchParameter, the `code` bound (G38, below); extract `K(N+1)`; `K(N)` is the `index_keys` of the `resources` row read at version `N`: the pre-read R1 of `delete`, `update_with_match` and `create_or_update`, or `update`'s own R1 (G22, section 2); size check of `K(N+1)` (below) | `resources (T,P,id)` for `update`'s R1 | `TenantError`; a value over 2,048 bytes, a `param` over 128, or a SearchParameter `code` over 128 is `ValidationError::InvalidResource`, 400; `update`'s R1 timed out is 504, failed is 503; nothing written |
| 2 | on update, delete and revive (`update`, `update_with_match`, `delete`, `delete_versioned`, and `create_or_update` on a live row or a delete marker): V3 for `N` at `ts(N) − 1`, from R1 or `current`, with R1's `K(N)` (G40); then I1, with `version_id = N+1` | `resource_versions (T,P,id)` for V3, then `(bucket, shard)` | V3 timed out: 504; failed: 503; nothing changed, since V3 only rewrites row `N` at an older timestamp. I1 `Unavailable`: 503, nothing changed. I1 `WriteTimeout`: an unused intent is swept, so harmless; 504 (`BackendError::Timeout`, G26) |
| 3 | C1 to C4 | `resources (T,P,id)` | not applied: I2, then the mapped error; unknown outcome below |
| 4 | V1 or V2 for `N+1` at `ts(N+1)` (V3 for `N` already ran at step 2) | `resource_versions (T,P,id)` | retry with the same timestamps |
| 5 | index batches, at `ts(N+1)` | index partitions | retry with the same timestamps; `WriteTimeout` type `BATCH` needs no retry (batchlog replays), `BATCH_LOG` does. Retries exhausted: return success, with I1 left for the sweeper, which makes the write searchable within one poll interval plus the grace period (G23, ADR-aacruzgon-0027) |
| 6 | I2 | `(bucket, shard)` | ignored: return success; the sweeper replays the same writes, which changes nothing, and deletes the intent |

**Size check** (ADR-aacruzgon-0026, G35; ADR-aacruzgon-0027, G38). Before step 2, every `code`, `system` and `uri` value in `K(N+1)` is measured in UTF-8 bytes, and so is every row's `param`. A value over 2,048 bytes, or a `param` over 128, refuses the whole write with `StorageError::Validation(ValidationError::InvalidResource { message, details })`, one `ValidationDetail` per offending element (its FHIRPath, the parameter and the byte length, severity `Error`). REST answers 400 with OperationOutcome issue code `invalid` (`crates/rest/src/error.rs:1048`, `570-571`). The values are measured one by one, not as a whole composite key: they are what the client controls, and the other components are bounded by the server (`tenant_id` at most 64 bytes, `MAX_TENANT_ID_LEN` in `tenant/id.rs:34`; `id` at most 64; `resource_type` at most 34, the longest type name in the spec data; `param` at most 128, by G38; timestamps fixed). So the largest partition key, `token_system_code_index`'s with a 2,048-byte `system` and `code` and a 128-byte `param`, is 64 + 34 + 128 + 2,048 + 2,048 bytes plus 3 bytes of framing per component, 4,337 bytes, far below Cassandra's 65,535-byte key limit, and the same resource is accepted or refused the same way in every tenant. `K(N)`, the old keys, is never checked: it was checked when `N` was written. The `param` check is a backstop: G38's bound on a stored SearchParameter's `code` means it should never fire, but a registry built from data stored before the bound, or from a configuration file, could still hold a longer code.

**SearchParameter `code` bound** (ADR-aacruzgon-0027, G38). On `create`, `update`, `update_with_match` and `create_or_update` of a `SearchParameter`, the same calls inside a batch Bundle included, a `code` longer than 128 bytes in UTF-8 is refused with `StorageError::Validation(ValidationError::InvalidResource { message, details })`, one `ValidationDetail` on `SearchParameter.code` with the byte length, before step 2, so nothing is written. REST answers 400, issue `invalid`, as for the size check. The FHIR `code` datatype has no length limit, so this is a stated server restriction (G38).

**Timestamps** (ADR-aacruzgon-0016). `lu(1) = now_ms`; `lu(N+1) = max(now_ms, lu(N) + 1 ms)`; `ts(V) = lu(V) · 1000`. `lu(N)` comes from `current.last_modified()` on update and from R1 on delete and revive, so V3's timestamp is exact with no extra read. `K(N)` comes from R1's `index_keys`; only `update` runs an R1 for it (G22). `w(N+1)` is a fresh uuid per CAS attempt (ADR-aacruzgon-0024), and `lu` and `ts` are computed from that attempt's R1.

**Unknown CAS outcome** (ADR-aacruzgon-0024, amended 2026-11-05).
- **Triggers:** `WriteTimeout` with `write_type = CAS` or `SIMPLE`, or `WriteFailure` with `write_type = CAS`, on any of C1 to C4, so for `create`, `create_or_update`, `update`, `delete` and `delete_versioned`.
    - With `paxos_variant: v2`, 5.0.9 throws `WriteFailure`/CAS when replicas fail, even in the commit phase after consensus (`Paxos.java:794-799`).
    - `CAS_WRITE_UNKNOWN` comes only from the legacy v1 path; cdrs-tokio 10 decodes it as `ErrorType::CasWriteUnknown` (8.1.9 could not, ADR-aacruzgon-0044), and if it ever arrives, it is treated the same way.
    - A client-side timeout or a dropped connection after the CAS was sent is treated exactly like `WriteTimeout`/CAS. In cdrs-tokio that is `Error::General("Connection closed …")`, `Error::Io`, or the backend's own request deadline once the CAS was sent. `Error::Timeout` (raised only while connecting) and a deadline that elapsed while preparing prove nothing was sent, and are mapped directly (ADR-aacruzgon-0034).
    - Every CAS is sent as non-idempotent and with `FallthroughRetryPolicy`, so neither the driver's retry policy nor speculative execution ever re-sends one (ADR-aacruzgon-0032).
- **What to do:** run R2. Contention alone can cause it, with nothing committed.

For an attempt that targeted version `V` (1 for C1):

| R2 shows | Meaning | Action |
| --- | --- | --- |
| `version_id = V`, our `write_id` (this or any earlier attempt of the request, ADR-aacruzgon-0034) | applied | continue at step 4; a create returns 201 |
| `version_id = N`, or no row for C1 | not applied, and never will be | run again as a new attempt after a random 5 to 25 ms pause, from a budget of 2 re-runs; a create keeps the same `id`; the attempt's I1 is left for the sweeper |
| `version_id = V`, another `write_id` (none of this request's attempts', ADR-aacruzgon-0034) | another writer won `V` | a definite miss: `AlreadyExists`, 409, from `create`; 409 from `update` and `delete_versioned`; the next attempt in `create_or_update` and a plain `delete` |
| `version_id > V` | ours may have applied and been superseded | R3 on row `V`, which now selects `write_id` (ADR-aacruzgon-0026, G36); see the next three rows |
| `version_id > V`; row `V` has our `write_id` | applied, then superseded | success with version `V`, as for "applied" (201 for a create or revive, 200 for an update, 204 for a delete); steps 4 and 5 are skipped, since a late insert of `K(V)` could outlive a later version's tombstones; I1 is left for the sweeper, whose superseded case deletes `K(V-1)` |
| `version_id > V`; row `V` has another `write_id` | another writer won `V` | a definite miss, as the third row |
| `version_id > V`; row `V`'s `write_id` unset | undecidable | after `WriteTimeout`, a client-side timeout or a dropped connection: `BackendError::Timeout`, 504; after `WriteFailure`: `BackendError::Unavailable`, 503. No retry, which could write the body twice; I1 left for the sweeper |
| R2 times out, or fails | unknown | timed out: `BackendError::Timeout`, 504; failed (`ReadFailure`, `Unavailable`): `BackendError::Unavailable`, 503; I1 kept |

Row `V`'s `write_id` is set by `V`'s own step 4 (V1 or V2), by the sweeper's replay of `V`, or by the next writer's V3 when that writer built it from R1: a `delete`, a `create_or_update` or an `update_with_match`. So the outcome is decided whenever the version that superseded ours came from one of those paths. It stays undecidable when it came from `update`, whose V3 is built from a `StoredResource` without a `write_id`. Row `V` always exists once a later CAS has applied, because that writer's V3 ran before its CAS (G40).

Every timeout is 504 and every unavailable node or replica failure 503, as `crates/rest/src/error.rs` maps them (G26). When the 2 re-runs are used up, the last attempt's error decides the same way: 504 after a `WriteTimeout`, 503 after a `WriteFailure`. `write_type = SIMPLE` is a trigger like `WriteTimeout`/CAS, confirmed by R2, never taken as applied (ADR-aacruzgon-0035).

### 3.2 Create (`ResourceStorage::create`, `POST /Patient`)

`Operation::Create`. The server assigns `id` and sets `meta.versionId = "1"`, `meta.lastUpdated = lu(1)`. Steps: I1 (`version_id` 1); C1; V1 for version 1 (no V3); inserts only (X1 at `(lu(1), id)` and every row of `K(1)`, tag 1), since `K(0)` is empty; I2.

| CAS result | Returned |
| --- | --- |
| applied | 201 Created, `ETag: W/"1"` |
| not applied (row returned): id collision, reachable only with a client-chosen id | `ResourceError::AlreadyExists`, 409 (inside `create_or_update` the loop restarts as an update) |
| unknown (`WriteTimeout` or `WriteFailure`, `write_type = CAS`) | R2 on the same `id`, then the table in 3.1: our `write_id` is 201; no row is a re-run with the same `id` and a fresh `w`, so a client never gets two resources from one `POST`; another `write_id` is `AlreadyExists`, 409 |

### 3.3 Update (`ResourceStorage::update(tenant, current, resource)`, `PUT /Patient/id`)

REST has already read the row, checked `If-Match` (412 on a mismatch, before storage is called), and passes `current`, version `N`. `Operation::Update`. `K(N)` is the `index_keys` of one R1 of `resources` (G22; section 2 says what to do when R1 shows another version). Steps: R1; V3 rewrites `N` from `current` with that `K(N)` (G40); I1; C2 with `IF version_id = N AND deleted_at = 0`, writing `K(N+1)`; V1 for `N+1` with `K(N+1)`; delete `K(N) \ K(N+1)` and insert `K(N+1)` at `ts(N+1)`; I2.

| CAS result | Meaning | Returned |
| --- | --- | --- |
| applied | | 200 OK, `ETag: W/"N+1"` |
| `[applied] False` only | row never existed | `ResourceError::NotFound`, 404 |
| `deleted_at > 0` | soft-deleted after REST's read | `NotFound`, 404 (design step 3; the race suite also accepts `Gone`, G9) |
| `version_id ≠ N`, `deleted_at` 0 | lost the race | `ConcurrencyError::VersionConflict`, 409 |

`update_with_match(…, expected_version, …)` adds a pre-read R1: a different current version is `OptimisticLockFailure` (412) without a CAS; a CAS miss after that is 409. Its V3, at step 2, is built from that R1, so it carries R1's `write_id`; plain `update`'s V3 leaves `write_id` unset (G36).

### 3.4 Delete (`delete` and `delete_versioned`, `DELETE /Patient/id`)

The REST handler reads the row, then calls `delete_versioned(current.version_id())` with `If-Match` or `delete` without (`core/preconditions.rs`, `delete_under_precondition`). Neither passes the old content, so the backend pre-reads.

| # | Statement | Notes |
| --- | --- | --- |
| 0 | R1 | R1 gives `N`, `data`, `lu(N)` and `K(N)`, its `index_keys` (G22). Absent or `deleted_at > 0`: `NotFound`, nothing written (REST answers 204 for `delete`, below). `delete_versioned` with `N ≠ expected`: `VersionConflict`, 409, nothing written. |
| 1 | — | `Operation::Delete`. `K(N)` from R1's `index_keys`; `K(N+1)` empty; `deleted_at = lu(N+1)`. |
| 2 | V3 rewrites `N` from R1's `data`, with `K(N)` (G40); then I1 | V3 before the CAS, so row `N` exists if C3 applies |
| 3 | C3 with `IF version_id = N AND deleted_at = 0` | C3 sets `deleted_at = lu(N+1)` and `index_keys` to the empty set |
| 4 | V2 (delete marker) for `N+1` | marker `data` unset, `index_keys` empty |
| 5 | delete every key of `K(N)` at `ts(N+1)`, including `resources_by_type (lu(N), id)` | no inserts |
| 6 | I2 | |

| CAS result | `delete` | `delete_versioned` |
| --- | --- | --- |
| applied | 204 No Content | 204 |
| `[applied] False` only, or `deleted_at > 0` | `NotFound`; REST answers 204 | `NotFound`, 404 (a repeated `delete_versioned` must be `NotFound`, G9) |
| `version_id` moved, still live | retry once, at once, from step 0 with a new I1 (G25, ADR-aacruzgon-0024); a second miss: `VersionConflict`, 409; the new R1 finds it deleted: `NotFound`, which REST answers 204 | `VersionConflict`, 409 (never retried) |

A delete of an unknown or already-deleted id is `NotFound` from the backend, as on SQLite, PostgreSQL and MongoDB. With no `If-Match`, the REST layer answers it with the spec's 204, or 200 with an informational OperationOutcome under `Prefer: return=OperationOutcome`, and writes nothing. That is HeliosSoftware/hfs#2529 (open upstream), which the fork carries (ADR-aacruzgon-0052); without it, Helios answers 404 (G25). Step 0's `NotFound` on a plain `delete` is answered the same way. With `If-Match`, REST answers 412 from its own read before storage is called, so `delete_versioned` meets `NotFound` only when another delete wins the race, and that stays 404.

### 3.5 Revive and `create_or_update` (`PUT` onto a deleted or unknown id; batch PUT)

The REST update handler maps `Gone` to "no current resource" and calls `create_or_update`, a bounded loop (ADR-aacruzgon-0024). It is reached by every unconditional `PUT` and every batch `PUT`. Each turn runs R1, whose `index_keys` is `K(N)` (G22), takes a fresh `w`, and writes a new I1 (new `enqueued_at`) with that turn's version, never rewriting an earlier turn's row. Then:

| Branch | Row | CAS | Not applied |
| --- | --- | --- | --- |
| a | absent | C1 (version 1) | someone created it (`AlreadyExists`); I2, next turn |
| b | live (`deleted_at = 0`) | C2 with `IF version_id = N AND deleted_at = 0` | `VersionConflict`, or `NotFound` if deleted in the gap; I2, next turn |
| c | deleted (`deleted_at > 0`; revive) | C4 with `IF version_id = N`, setting `deleted_at = 0`; `K(N)` is empty (delete marker) | `VersionConflict`; I2, next turn |

A definite miss starts the next turn at once, with no pause, for at most 3 retries, so 4 turns in all. An unknown outcome is settled first by the table in 3.1, from its own budget of 2 re-runs.

Branches b and c write V3 for `N` at step 2, before the CAS (G40): branch b a content row from R1; branch c a delete marker, `deleted_at` set and `data` unset, although `resources.data` still holds the old content. After the CAS applies, steps 4 to 6 run as for update: V1 for `N+1`; inserts of `K(N+1)` (branch b also deletes `K(N) \ K(N+1)`), I2. Returned: branch a 201 (`created = true`); branch b 200 (`created = false`); branch c 201 (`created = true`, PostgreSQL parity; R4 and R4B http.html#update require 201 for a resource "brought back to life"); a fourth definite miss is `VersionConflict`, 409 (that turn's I2 already ran). On revive, `fhir_version` is the original from R1 (G13, ADR-aacruzgon-0024) and `created_at` is kept (G14, ADR-aacruzgon-0011).

## 4. Reads

### 4.1 read (`GET /Patient/id`)

R1 at `LOCAL_QUORUM`, partition `resources (T,P,id)`. Absent: `Ok(None)`, 404. `deleted_at > 0`: `ResourceError::Gone`, 410 (404 if `HFS_RETURN_GONE` is false). Otherwise `StoredResource::from_storage(…)`, 200, `ETag: W/"version_id"`. `exists` is the same read and answers `false` for a deleted row (the trait doc requires MongoDB's behaviour).

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

R4 at `LOCAL_QUORUM`, one partition. `ORDER BY version_id ASC` reverses the clustering order, which a single-partition query may do (or read `DESC` and reverse in Rust). Returns `["1", "2", …]`, numerically ordered past 9 because `version_id` is an `int`; an unknown id gives an empty list. The race suite requires "no gap, no duplicate"; V3 before the CAS keeps that through any number of crashes (G2, G40).

## 5. Search

Expected results for every case are in [`coverage.md`](coverage.md). This section gives the statements.

### 5.1 What every search does

1. Validate (order and statuses in `coverage.md` section 6).
2. Stream candidates from one or more index partitions, in the shared order `(last_updated DESC, id DESC)` where the table has it.
3. Collapse rows with the same `(last_updated, id)`. A date-only search instead keeps each resource once, at its first matching row in its date table's ascending order (5.7).
4. **Tag check:** R1 for the candidate. Keep it only if the row exists, `deleted_at` is 0 (live; G39) and `resources.version_id` equals the index row's `version_id`. A mismatch hides the candidate and triggers repair on read (ADR-aacruzgon-0017): maintenance for the current version at its `ts`, with its key set from the `index_keys` of the R1 row just loaded (G22), so repair reads nothing more. A stale key the current version does not produce is deleted; one it produces is upserted with the current tag. Repair is logged as a write made from a read.
5. Apply residual filters: dates and `_lastUpdated` combined with another parameter (5.8); `query.fhir_version`, kept only if `resources.fhir_version` equals `as_mime_param()` of it (5.9, G34); `query.compartment` (5.11, G33).
6. Stop at `_count + 1` kept resources; the extra one decides whether `next` exists. `LIMIT` is a batch size, not the page size.
7. **Cap** (G27, ADR-aacruzgon-0027). The search counts the distinct candidates it point-reads (R1) for one page, or for one `search_count`. Past the configured cap it fails with `SearchError::TooManyResults`, which REST answers with 422, issue `processing` (`crates/rest/src/error.rs:1123-1125`, `591-595`). No search error produces `too-costly`; its only producer is the batch handler's 500 for a transaction too large for MongoDB's cache (`crates/rest/src/handlers/batch.rs:3745`). Stale rows count too: a candidate the tag check hides is still a candidate point-read, so enough resurrected or orphan rows ahead of the live matches can make a page fail before it fills (G7). Repair on read at step 4 deletes each one it reads, so a retry gets further.

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

**Several `_id` parameters are ANDed (G37).** Their id sets are intersected in Rust before any read, and only the intersection is point-read: `_id=a,b&_id=b` reads `b` only, and disjoint sets read nothing. `_id` is never checked against an index table. REST relies on this: under a `patient/` scope it appends `_id=<launch patient>` to every Patient search, "so `GET /Patient?_id=<other>` matches nothing rather than leaking" (`crates/rest/src/patient_compartment.rs:172-181`).

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

-- end slices on date_index_by_end (gt: end > e; ge: end > s; eb: end <= s)
SELECT end, id, start, version_id FROM date_index_by_end
 WHERE tenant_id = ? AND resource_type = ? AND param = ?
   AND end > ?;

-- page 2 on: the cursor replaces the slice's lower bound (it is always the tighter one,
-- and a clustering range takes one lower bound); the upper bound stays.
SELECT start, id, end, version_id FROM date_index
 WHERE tenant_id = ? AND resource_type = ? AND param = ?
   AND (start, id) > (?, ?) AND start < ?
 LIMIT ?;
```

One clustering range per query: checked on a 5.0.9 node, 2026-10-13, `start > X AND end > Y` and `start > X AND id > 'a'` are both rejected. Hence two tables.

Grouped by cost:

- **Exact single slices** (`gt`, `lt`, `sa`, `eb`): every row in the slice matches.
- **Single slices with a residual** (`eq`, `le`, `ap`, and `ge` as the single slice `end > s` on `date_index_by_end` with the full predicate as residual; every `ge` match has `te > s`, so nothing is missed): some rows are read and dropped.
- **Two slices read one after the other** (`ne`): `start < s` on `date_index`, then `end > e` on `date_index_by_end` (below).

**Paging in the table's order (ADR-aacruzgon-0027, G20).**
- A date-only search pages natively in its date table's own order: `(start ASC, id ASC)` on `date_index`, `(end ASC, id ASC)` on `date_index_by_end`. Paging is keyset, from the cursor, under the cap per page, with no Rust sort and no re-read of earlier pages.
- **Cursor.** `sort_values = [<start or end, RFC 3339 ms>, <reserved bucket slot>, "start" | "end"]`, with `resource_id = id`. The third element names the table, so a cursor minted by another order is `InvalidCursor` (400). The `(last_updated, id)` cursor of 5.10 keeps its two elements.
- **One resource, several rows.** A resource with several values of the parameter has a row per value. It is emitted only at its first matching row in slice order: after R1, its re-extracted values are checked, and a row is skipped when an earlier matching key of the same resource exists in the slice. So no page repeats a resource, and none is lost between pages.
- **Comma lists.** A comma list whose values all slice one table is merged in that table's order.
- **`ne`, and a comma list whose values slice both tables** (ADR-aacruzgon-0027). They read `date_index`'s slices first, to the end, then `date_index_by_end`'s, each in its own order. A resource is emitted only at its first matching row in that concatenated order: in the second table, a row is skipped when the resource's re-extracted values match a slice of the first. The cursor's third element names the table being read, `"start"` while in `date_index` and `"end"` after; a page that finishes the first table continues in the second from its start. Both tags are valid for these searches, and any other cursor is `InvalidCursor`.

**Several dates (G20).** A search whose only parameters are dates is still date-only when it has two or more date parameters, a repeated date, or a date with `_lastUpdated`. One is sliced and the rest are residuals on the loaded row:
- with `_lastUpdated` among them, `_lastUpdated` is sliced on `resources_by_type` (5.5), so the search pages in the default order;
- otherwise the first date parameter in the query's order is sliced, using its first occurrence if it is repeated.

**Sort.** When a date table is sliced, its order is the native one. `_sort=-_lastUpdated` is then not the native order, and is refused like any other `_sort` (C9, 400 `QueryParseError`). When `_lastUpdated` is sliced, the native order is the default one and `_sort=-_lastUpdated` is accepted.

### 5.8 Several parameters (AND)

One token or URI partition drives in the shared order from the cursor. **Which one (C5, ADR-aacruzgon-0027):** the first parameter in this fixed order, with no statistics, so the most specific key drives:

1. `_id` (point reads on `resources`, 5.4);
2. `system|code` and `|code` tokens;
3. identity URIs;
4. code-only tokens, `_profile`, `_source` and every other URI parameter: values many resources share (every US Core Patient carries the same profile);
5. `system|` tokens;
6. `_lastUpdated`, only when nothing else can drive (5.5).

Ties go by query order. Dates beside another parameter never drive (residuals, below), and other parameter types are refused before any read. The order is revisited only if the Step 8 benchmarks show it matters.

**Identity URI.** A registered `uri` parameter whose expression is the resource's own canonical `url`. In the R4 registry (`data/search-parameters-r4.json`) that is `url` on 28 types (ActivityDefinition, CapabilityStatement, ChargeItemDefinition, CodeSystem, CompartmentDefinition, ConceptMap, EffectEvidenceSynthesis, EventDefinition, Evidence, EvidenceVariable, ExampleScenario, GraphDefinition, ImplementationGuide, Library, Measure, MessageDefinition, OperationDefinition, PlanDefinition, Questionnaire, ResearchDefinition, ResearchElementDefinition, RiskEvidenceSynthesis, SearchParameter, StructureDefinition, StructureMap, TerminologyCapabilities, TestScript and ValueSet), and `system` on CodeSystem (`CodeSystem.url`). Three `url` parameters are not identities: `Device.url` (a network address), `Contract.url` (the basal contract definition) and Subscription's `url` (`Subscription.channel.endpoint`).

For each candidate that passes the tag check, every other token or URI parameter is a clustering point read using `resources.last_updated`:

```sql
SELECT version_id FROM token_system_code_index
 WHERE tenant_id = ? AND resource_type = ? AND param = ? AND system = ? AND code = ?
   AND last_updated = ? AND id = ?;
```

A non-driving parameter whose form reads two partitions takes one point read per partition, and a match in either counts: `system|code` reads the named system and `IMPLICIT`, `|code` reads `''` and `IMPLICIT`. A comma list takes one point read per value (and per partition of each), ORed. `code` and `system|` read a clustering prefix (`last_updated = ? AND id = ?` without the third column) of `token_code_index` or `token_system_index`, one partition each.

Dates and `_lastUpdated` are residual filters on the loaded `resources` row, as the design says ("Date predicates combined with any other parameter are residual filters"): re-extract the dates and apply `RangePredicate::matches`; compare `resources.last_updated` for `_lastUpdated`. A slice on the driving partition's `last_updated` is an optimisation option (G19).

### 5.9 `count`, `search_count`, `_count`

| Call | Statements | Partitions |
| --- | --- | --- |
| `count(tenant, Some(P))` | `SELECT id FROM resources_by_type WHERE tenant_id=? AND resource_type=?`, paged by the driver and counted in Rust (or `SELECT COUNT(*)`, which pages internally); no tag check | one, read whole, tombstones included |
| `count(tenant, None)` | the scan above for every resource type name of every enabled FHIR version: `FhirVersion::enabled_versions()` with `FhirResourceTypeProvider::get_resource_type_names`, as `ResourceTypeScope` does (`crates/persistence/src/search/type_qualifier.rs:98-118`), each name once; about 145 in R4 (G7) | one per type name |
| `search_count` with filters (`_count=0`, `_summary=count`, `_total=accurate` or `estimate`) | the stream of 5.1 run to the end, with tag checks, under the candidate cap (5.1, step 7); over the cap is `TooManyResults` (G7, ADR-aacruzgon-0027) | as the search |
| `search_count` with no filters | `count(tenant, Some(P))` (G7) | one |
| `search_count` with no filters and `query.fhir_version` set | `SELECT id, fhir_version FROM resources_by_type WHERE tenant_id=? AND resource_type=?`, counting only rows whose `fhir_version` equals `as_mime_param()` of the requested version, in Rust; no `ALLOW FILTERING`, no point reads (G34) | one, read whole |
| `search_count` with `query.compartment` set | the stream of 5.1 with the compartment residual (5.11) run to the end, under the cap; over the cap is `TooManyResults`, never a partial count | as the search |
| `_count=n` | page size `n`; read until `n + 1` resources pass | as the search |
| `_total=accurate` or `estimate` on a search | `SearchQuery::wants_total()` is true (`types/search_params.rs:844-849`): the page as above, plus one `search_count` of the same query for `Bundle.total`; `_total=none` or absent computes no total | as the search, twice |

**How far `count` and a filtered `search_count` may differ (G7, ADR-aacruzgon-0027).** `count` has no tag check, so it can differ from `search_count` with any filter in three cases:
- while a write is in flight, between its compare-and-swap and its index writes;
- while a crashed write waits for the sweeper: one poll interval plus the grace period, while the sweeper runs;
- in the design's stated limitations, where an orphan row stays counted until a search repairs it: an intent that expired during a sweeper outage longer than the 24 h TTL, a request stalled past the grace period, or one paused past `gc_grace_seconds` (design, "Clocks").

**`query.fhir_version` (G34).** Set when the client names a `fhirVersion` MIME parameter (`crates/rest/src/handlers/search.rs:557`, upstream #2254). Every tag-checked search applies it as a residual on `resources.fhir_version` (5.1, step 5). An unfiltered search with it set reads `resources_by_type` with `fhir_version` and drops other versions before the tag check, so it costs no extra reads. `None` matches every version.

### 5.10 A cursor page

The cursor is a plain `PageCursor`: `sort_values = [lu as RFC 3339 with milliseconds, <reserved bucket slot>]`, `resource_id = id`, encoded with `PageCursor::encode`. A date-only search's cursor adds a third element naming its table (5.7). The page key is `(last_updated, id, bucket)`; `id` travels in `resource_id`, so the design's "third slot" for the bucket is the second element of `sort_values`.

Page 2 of `GET /Patient?gender=female&_count=20`:

1. `PageCursor::decode`; a malformed cursor is `SearchError::InvalidCursor`, 400.
2. T1 on `token_code_index (T, Patient, gender, female)` with `(last_updated, id) < (?, ?) LIMIT 50`.
3. Collapse, tag check, stop at 21 kept.
4. If the 21st exists, mint `next` from the 20th's `(last_updated, id)`. No `previous` on page 1.

The tuple relation compares by value on the `DESC, DESC` order, so it excludes every row of the cursor's own resource and places ties exactly (spike A3). A resource updated during paging moves ahead of the cursor and is not seen again (G21).

### 5.11 Compartment filter (`query.compartment`, G33)

REST sets `query.compartment = Some(CompartmentMembership { params, reference })`:
- for every non-Patient search under a `patient/` scope (`crates/rest/src/patient_compartment.rs:183-201`);
- for a compartment search, `GET /Patient/123/Observation` (`handlers/compartment.rs:142`), and each type of `GET /Patient/123/*` (`compartment.rs:331`);
- for `$everything` (`handlers/everything/scope.rs:92`).

It is set on the base query only. The hops of a chain or `_has` are built by the shared resolver with a fresh `SearchQuery` and no compartment (`crates/persistence/src/search/chain_resolver.rs:570`, `603`, `684`, `728`), so under a `patient/` scope they select over the whole tenant, and only the path's resource type is checked against the token's scopes. A known upstream security defect ([HeliosSoftware/hfs#2501](https://github.com/HeliosSoftware/hfs/issues/2501), `coverage.md` section 6 and C7); four Step 6 rows gate Cassandra search on it (roadmap section 5).

`params` are the reference search parameters of the bundled CompartmentDefinition for that compartment and target type (`helios_fhir::get_compartment_params`; for R4 Patient and Observation, `subject` and `performer`, `crates/fhir/src/r4.rs:168626`), and `reference` is `Patient/123`.

No table indexes references, so membership is a residual filter on each candidate that passed the tag check (5.1, step 5). The rule is the one SQLite and MongoDB apply to their reference index rows (`backends/sqlite/search/query_builder.rs:863-896`, `backends/mongodb/search_impl.rs:847-862`):
- re-extract the row with `SearchParameterExtractor::extract`, under the resource's own `fhir_version` and the tenant's registry, and keep the `IndexValue::Reference` values of the parameters named in `params`;
- the candidate is a member if any of them equals `strip_reference_version(reference)` or starts with it followed by `/_history/`.

The other parameters drive as usual, in the shared order, so paging stays keyset. With no other parameter, the driver is `resources_by_type`.

**Cap.** The distinct candidates point-read for one page, or for one `search_count`, count against the search's candidate cap (5.1, step 7; G27). Past the cap the search fails with `SearchError::TooManyResults`. REST answers 422 with issue `processing` (`crates/rest/src/error.rs:1123-1125`, `591-595`). The search never returns a page or a count that was only partly filtered, and never ignores the filter. A page that fills before the cap is returned normally.

## 6. The sweeper

A background task in each Helios process (ADR-aacruzgon-0013 to 0015). It never compares `write_id`. This project runs one Helios process, so one sweeper; several would be safe, only costlier (G12).

**What it reads.** Every poll (a few seconds), I3 on the 16 shards of the current and previous hour bucket, 32 partitions. Each `(bucket, shard)` keeps a low-water mark in memory, and an intent's bucket is always the hour of its own `enqueued_at`. I3 reads only after the mark and before `now − grace` (30 s default), so it skips finished intents' tombstones. After each successful poll the mark moves to the slice's upper bound, even if no rows came back; an intent that fails goes to an in-memory retry list instead of holding the mark. At startup the sweeper first walks the 25 most recent buckets (a 24 h TTL can span 25), 400 partitions, in short slices.

**For each intent, with `V = intent.version_id`:** R2 reads the `resources` row at `LOCAL_SERIAL`, which settles any in-flight Paxos proposal. Then one of four cases, each ending with I2:

| Case | Reads | Writes |
| --- | --- | --- |
| no row, or `version_id < V`: the CAS never applied | — | none |
| `V` current: a crashed or slow request | R3 for `V-1` (not for `V = 1`) | `K(V)` is the `index_keys` R2 returned (G22). Step 4 for `V`'s own row (never V3), at `ts(V)`: V1, or V2 if `deleted_at > 0`, with `data`, `write_id` and `index_keys` from the `resources` row. It writes the same cells as `V`'s writer, so an existing row is unchanged. Then step 5 at `ts(V)`: delete `K(V-1) \ K(V)`, insert `K(V)` |
| `V` superseded | R3 for `V` (for `ts(V)`) and `V-1` | delete all of `K(V-1)` at `ts(V)`; no inserts, since a later version already deleted `K(V)` and a late insert would outlive those tombstones (3 h) |

`K(V-1)` is row `V-1`'s `index_keys` (G22), since `resources` holds only the current version's. Row `V-1` always exists for `V > 1`, and row `V` once `V` is superseded: each writer wrote the previous row (V3) before its CAS (G40). A missing row `V-1` therefore never stands for an empty set. It is empty for version 1, a marker, or a missing row `V-1`, whose writer never reached step 5. Every write is older than any later version's, so it never removes a newer row, even after that row's tombstones are purged. A sweeper and a slow request may run the same writes. The grace period is the one clock comparison (G11).

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

I1 inserts `(tu(1), acme, Patient, p1)` with `w1` and `version_id 1` into `('2026-10-14T10', 7)`. C1 applies: `version_id 1`, `last_updated` and `created_at` `lu(1)`, `fhir_version '4.0'`, `write_ts ts(1)`, `write_id w1`, `data`, and `index_keys` `K(1)`, the 8 rows below, which encode to 289 bytes (format 1, `schema.cql`). V1 writes version 1 at `ts(1)`, with `write_id w1` and the same `index_keys`. The `resources_by_type` row carries `fhir_version '4.0'`. Then 8 index inserts, all `USING TIMESTAMP 1791972000123000`, tag 1; then I2. The `token_system_index (gender, IMPLICIT)` row is not written (P5).

| Table | Partition key (after `acme, Patient`) | Clustering key |
| --- | --- | --- |
| `resources_by_type` | — | `(lu(1), p1)` |
| `token_code_index` | `(identifier, 12345)` | `(lu(1), p1, MRN)` |
| `token_system_code_index` | `(identifier, MRN, 12345)` | `(lu(1), p1)` |
| `token_system_index` | `(identifier, MRN)` | `(lu(1), p1, 12345)` |
| `token_code_index` | `(gender, male)` | `(lu(1), p1, IMPLICIT)` |
| `token_system_code_index` | `(gender, IMPLICIT, male)` | `(lu(1), p1)` |
| `date_index` | `(birthdate)` | `(B0, p1, B1)` |
| `date_index_by_end` | `(birthdate)` | `(B1, p1, B0)` |

Eight partitions, eight rows; at an estimated 150 to 250 bytes per row, one batch under 5 KiB.

### Version 2: update (gender `male` → `female`)

`current` is version 1. R1 reads `resources` at version 1, whose `index_keys` is `K(1)`, the table above (G22). V3 rewrites version 1 from `current` at `ts(1) − 1`, with the `K(1)` R1 returned, so the original wins and nothing changes. I1 `(tu(2), acme, Patient, p1)`, `w2`, `version_id 2`. C2 `IF version_id = 1 AND deleted_at = 0` applies: `version_id 2`, `lu(2)`, `ts(2)`, `w2`, new `data`, `index_keys` `K(2)`, 293 bytes. V1 writes version 2 at `ts(2)`, with the same `K(2)`. Index changes, all `USING TIMESTAMP 1791972300456000`:

- **6 deletes:** every non-date row of `K(1)`.
- **8 inserts, tag 2:** `K(2)`, which is `K(1)` with `lu(2)` for `lu(1)` and `female` for `male`. The two date keys are unchanged, so they are only re-inserted (tag 1 → 2).

Then I2. Six row tombstones, one in each of six partitions. The identifier did not change, but its rows move, because `last_updated` is in the key.

**Between C2 and the index writes** (the in-flight window), `resources` is at version 2 while every index row has tag 1. `identifier=MRN|12345` reads `(lu(1), p1)` tag 1, R1 says 2, so the patient is hidden; `gender=female` finds nothing yet. Afterwards both work and `gender=male` is empty.

### Version 3: delete

REST read version 2 and, with no `If-Match`, called `delete`. R1 gives version 2, `data`, `lu(2)` and `K(2)`. V3 rewrites version 2 from R1's `data` at `ts(2) − 1`. I1 `(tu(3), acme, Patient, p1)`, `w3`, `version_id 3`. C3 `IF version_id = 2 AND deleted_at = 0` applies: `version_id 3`, `deleted_at` and `last_updated` `lu(3)`, `ts(3)`, `w3`, `index_keys` the empty set (`data` kept, G29). V2 writes the marker, version 3, no `data`, with the empty key set (2 bytes), at `ts(3)`. Then 8 deletes at `ts(3)` (`USING TIMESTAMP 1791972600789000`), every key of `K(2)`, including the two date rows; then I2.

Now `read` and vread 3 return 410; vread 2 returns version 2's content with 200; every search misses the patient.

### Version 4: revive

`PUT /Patient/p1` with the version 2 content. REST's read returned `Gone`, so it called `create_or_update`. R1: version 3, `deleted_at > 0`, so branch c, and R1's `index_keys`, `K(3)`, is empty. V3 rewrites the version 3 marker at `ts(3) − 1`, `deleted_at` set, `data` unset. I1 `(tu(4), acme, Patient, p1)`, `w4`, `version_id 4`. C4 `IF version_id = 3` applies: `version_id 4`, `lu(4)`, `ts(4)`, `w4`, `data`, `index_keys` `K(4)`, `deleted_at = 0` (an ordinary cell, newer than the delete's; no tombstone, G39). V1 writes version 4 at `ts(4)`, with the same `K(4)` (293 bytes: `last_updated` is implied by the row, so `K(4)` encodes as `K(2)` does). `K(3)` is empty, so 8 inserts and no deletes, at `ts(4)` (`USING TIMESTAMP 1791972900012000`), tag 4: `K(2)` with `lu(4)` for `lu(2)`. Then I2. Returned: 201 (`created = true`), `ETag: W/"4"`.

The two date keys are the ones version 3 deleted. They come back because the insert at `ts(4)` is newer than the delete at `ts(3)`. With equal timestamps the delete would win, which is why `ts` must strictly increase per resource.

### Final state

| Table | Live rows for `p1` | Row tombstones left by `p1` |
| --- | --- | --- |
| `resources` | 1 row, version 4, with the 293-byte `K(4)` as `index_keys`, `deleted_at` 0 | 0 |
| `resource_versions` | versions 1, 2, 3 (marker), 4, with 289 + 293 + 2 + 293 = 877 bytes of `index_keys` | 0 |
| `pending_index_writes` `('2026-10-14T10', 7)` | 0 | 4 |
| `resources_by_type` | `(lu(4), p1)` tag 4 | 2 (`lu(1)`, `lu(2)`) |
| identifier, 3 token partitions | one row each at `lu(4)`, tag 4 | 2 each (`lu(1)`, `lu(2)`) |
| gender `male`: `token_code_index`, `token_system_code_index` | 0 | 1 each |
| gender `female`: `token_code_index`, `token_system_code_index` | one row each at `lu(4)`, tag 4 | 1 each |
| `date_index`, `date_index_by_end` | one row each, tag 4 | the `ts(3)` delete, shadowed by the `ts(4)` insert until compaction |

`list_versions` returns `["1", "2", "3", "4"]`. Four writes left 14 index row tombstones (6 from the update, 8 from the delete; the two date ones are shadowed by version 4) and 4 intent tombstones, for one resource. The three V3 rewrites changed nothing and left no tombstones.

### What a crash would change

- **Killed after I1 of version 2** (no CAS): after 30 s the sweeper reads `resources` at `LOCAL_SERIAL`, sees version 1, below the intent's 2, and deletes the intent. Nothing else changes.
- **Killed after C2 of version 2** (no version row, no index writes): `read` returns version 2 at once, but vread 2 is 404 until the sweep (G2). Search hides the patient. The sweeper finds version 2 current, and its R2 returns `K(2)`, which C2 wrote; `K(1)` is row 1's `index_keys`. It writes row 2 from the `resources` row, with that `K(2)` as `index_keys`, deletes `K(1) \ K(2)`, inserts `K(2)` at `ts(2)`, and deletes the intent.
- **Same crash, but version 3 (the delete) lands before the sweeper runs.**
    - The delete's R1 gives version 2's content and `K(2)`, which C2 wrote (G22). Its V3, before C3, restores row 2 at `ts(2) − 1`, with that `K(2)` as `index_keys`. It deletes `K(2)` at `ts(3)`; of those 8 keys only the two date keys exist (equal to `K(1)`'s, tag 1), so the date rows go.
    - Left behind: the 6 non-date rows of `K(1)`, tag 1: `resources_by_type (lu(1), p1)`, three identifier rows and two gender `male` rows. The tag check hides them; `count` is 1 for a deleted patient (G7).
    - The sweeper finds `resources` at 3, above the intent's 2: version 2 is superseded. It reads `ts(2)` from row 2 and `K(1)` from row 1's `index_keys`, deletes all 8 keys of `K(1)` at `ts(2)` (the 6 orphans, and the two date keys version 3 already deleted), inserts nothing, then runs I2.
    - After the sweep: vread 2 is 200, `list_versions` is `["1", "2", "3"]`, `count` is 0. As in the crash-free run there are 14 index row tombstones (the two date keys carry both deletes, merged into one each); only the shadowed `K(2)` inserts are missing.

## 8. Gaps and questions for the session

G numbers are stable references; the list is grouped by severity, not sorted.

- **Resolved in the design:** G1, G2, G3, G4, G5, G6, G9, G10, G11, G13, G16, G25, G26, G31; from the schema audit G33 to G37 (ADR-aacruzgon-0026); and G17, G23, G27, G32 (ADR-aacruzgon-0027).
- **Resolved by an earlier decision**, now marked as such: G14, G15, G19, G21, G24, G28, G29, G30.
- **Resolved last** (ADR-aacruzgon-0027, the items still open after PR #33's first commit): G7, G8, G12, G18, G20, G22.
- **Raised in review of PR #33, resolved (ADR-aacruzgon-0027):** G38, G39, G40.
- **Open:** none.

### Resolved in the design

**G1. The sweeper left orphans when a later write superseded a crashed one.** *Resolved in the design (ADR-aacruzgon-0013).* The old sweeper dropped any intent whose `write_id` no longer matched, so the `K(N)` rows before the crash stayed. Now the intent carries `version_id` and the sweeper replays `V` whenever it exists (section 6). No `write_id` on `resource_versions` is needed: the replay is safe whoever made `V`.

**G2. A crash could lose a version's content for good.** *Resolved in the design (ADR-aacruzgon-0012).* A crash before the version row, then another update, overwrote `N+1` in `resources` before it reached history. Now every update, delete and revive rewrites row `N` (V3). V3 is at `ts(N) − 1`, so an existing row always wins. Left over, and stated in the design: vread `N+1` is 404 until the sweep or the next write. "Two crashes in a row still lose a version's content" was also left over, until G40 moved V3 before the CAS: the next writer now restores row `N+1` before it can overwrite `resources`.

**G3. Where the sweeper gets `K(V-1)`.** *Resolved in the design (ADR-aacruzgon-0013).* From row `V-1`, empty for version 1 or a marker. The row was thought to be missing only after two crashes in a row; G40 found that a paused writer breaks that assumption, and moved V3 before the CAS, so row `V-1` now always exists for `V > 1` (section 6). Since G22 the keys are row `V-1`'s stored `index_keys`, so a registry change no longer alters them.

**G4. `ts(N)` was not available on the update path.** *Resolved in the design (ADR-aacruzgon-0016).* `StoredResource` carries no `write_ts`. Now `lu(N+1) = max(now_ms, lu(N) + 1 ms)` and `ts = lu · 1000`, so any version's `ts` is recomputable, as V3 needs. Side effect: a burst of updates, or a clock stepping back, keeps `lu` ahead of the wall clock.

**G5. Tombstones in `pending_index_writes`.** *Resolved in the design (ADR-aacruzgon-0015).* With `tenant_id` before `enqueued_at`, every poll read the hour's tombstones (on a 5.0.9 node: "Read 0 live rows and 3000 tombstone cells"). Now `enqueued_at` leads and I3 reads past the mark (same test: 0 tombstone cells). Smaller buckets and `TimeWindowCompactionStrategy` stay options.

**G6. The sweeper polled 2 buckets, but intents live 24 h.** *Resolved in the design (ADR-aacruzgon-0014).* After an outage over an hour, a crashed create stayed unsearchable for good. Now the sweeper walks the 25 most recent buckets at startup. Past the TTL a crashed create stays unsearchable (readable by id) until next written, a limitation the design now states.

**G9. The CAS result mapping depends on the operation.** *Resolved in the design (step 3; a correction, no ADR).* `deleted_at` set (`> 0` since G39) is 410 for `read` and `vread` only; every write maps it to `NotFound`, 404, which the race suite allows (`versioned_write_race_suite.rs:94-95`, and the deleted-row arms at 262, 369 and 541). For a plain `delete`, REST answers that `NotFound` with 204, the spec's no-op (3.4, ADR-aacruzgon-0052). `spike-findings.md` point 3 is corrected.

**G10. Repair on read could delete a key the in-flight writer needs.** *Resolved in the design (ADR-aacruzgon-0017).* A delete wins a timestamp tie, so deleting a stale date key at `ts(N+1)` beat the writer's insert. Now repair deletes only keys the current version does not produce and upserts the rest (5.1). Its read-latency cost is unmeasured.

**G11. The grace period is a clock comparison.** *Resolved in the design (documented).* The design said "no clocks are compared". It now states the assumption: clock skew plus intent-to-CAS latency stays under the grace period, which also guards the mark. A request that stalls longer and then crashes before its index writes is left to repair on read; a create or revive in that state stays unsearchable until next written, a stated limitation. So does a pause past `gc_grace_seconds` that resurrects rows `count` then includes. Deployment requirement: NTP, skew well under 30 s.

**G16. Columns on `resource_versions` and the intent row.** *Resolved in the design (ADR-aacruzgon-0013); revised by G36.* The intent row gained `version_id int`. `resource_versions` needs no `created_at` (G14). It was first left without a `write_id`, since the sweeper no longer compares it; ADR-aacruzgon-0026 added one for the unknown-outcome rule (G36).

**G31. Sweeper edge cases found in the revision.** *Resolved in the design (ADR-aacruzgon-0013, 0015).* A late replay could resurrect `K(V)`: a superseded `V` now gets deletes only. A double crash: intent `V` deleted `K(V-1)` at `ts(V-1) + 1`, and a missing previous row meant empty previous keys; G40 replaces both rules, since neither row can be missing now. A stuck intent pinned its mark, and an empty slice never moved it: now a retry list, and the mark moves to each poll's upper bound. Marks are per `(bucket, shard)`, with `bucket` from `enqueued_at`.

**G13. Revive details.** *Resolved in the design (ADR-aacruzgon-0024).*
- C4 cleared `deleted_at` with a literal `deleted_at = null` in the statement. That was not a bound null: it wrote exactly one cell tombstone, on purpose. G39 replaced it with `deleted_at = 0`, because that tombstone could be purged on replicas while one replica still held the delete's value.
    - History, before G39. Checked on a 5.0.9 node, 2026-10-13: `UPDATE resources SET deleted_at = null, version_id = 4, … IF version_id = 3` applies, and `DELETE deleted_at FROM resources WHERE … IF version_id = 3` also works.
    - It is now the one stated exception to "never bind null" in `schema.cql`, in section 2 ("Compare-and-swap on `resources`") and in the roadmap's Step 1 checklist.
- A revive returns `created = true` (201) and keeps the **original** `fhir_version`, bound in C4 from R1. This matches every other backend:
    - `create_or_update` uses the request's version only to create;
    - `restore_deleted` and `update` keep the stored one (`sqlite/storage.rs:596` and 734, `postgres/storage.rs:1321` and 1429, `mongodb/storage.rs:2873` and 4981-4987).

**G25. Plain delete losing a race, and 404 for absent ids.** *Resolved in the design (ADR-aacruzgon-0024).*
- A `delete` without `If-Match` whose CAS misses because an update landed now retries once, at once, from the pre-read with a new I1, as MongoDB does (#1405).
- A second miss is `VersionConflict`, 409. The race suite still holds: "a resource is deleted once".
- `delete_versioned` never retries.
- A delete of an unknown or already-deleted id is `NotFound` from the backend, and REST answers it with the spec's 204 (3.4). It was 404, matching Helios; it is 204 with HeliosSoftware/hfs#2529 (open upstream), which the fork carries (ADR-aacruzgon-0052).

**G26. 503 or 504.** *Resolved in the design (ADR-aacruzgon-0024).* The rule is the one `crates/rest/src/error.rs` already applies:
- a timeout (`WriteTimeout`, `ReadTimeout`, a timed-out serial read, or an outcome a timeout left undecidable) is `BackendError::Timeout`, 504;
- an unavailable node or connection, or a replica failure (`WriteFailure`, `ReadFailure`, or an outcome a `WriteFailure`/CAS left undecidable), is `BackendError::Unavailable`, 503 (amended 2026-11-05 after review on PR #32).

The failed intent write (3.1, step 2) and the unknown CAS outcome (3.1) now say so.

**G33. The compartment filter was neither served nor refused.** *Resolved in the design (ADR-aacruzgon-0026).* Under a `patient/` scope, and for compartment searches and `$everything`, REST sets `query.compartment` and relies on the backend to apply it. No table indexes references, and ignoring the field would have returned other patients' resources. It is now a residual filter on every loaded row, with SQLite's and MongoDB's membership rule and the search's candidate cap, and `TooManyResults` (422) past the cap. A count under a compartment runs the same filter (5.9, 5.11).

**G34. `query.fhir_version` could not be applied by `count`.** *Resolved in the design (ADR-aacruzgon-0026).* Upstream #2254 added `SearchQuery.fhir_version`. Tag-checked searches apply it to `resources.fhir_version`. The `count` path reads no `resources` rows, so `resources_by_type` now carries `fhir_version`, written by X1 and the sweeper, and an unfiltered `search_count` drops other versions while it scans (5.9).

**G35. An index value over Cassandra's key limit made a committed write unindexable.** *Resolved in the design (ADR-aacruzgon-0026).* Checked on a 5.0.9 node, 2026-11-05:
- a 70,000-byte `code` in a partition key fails with "Key length of 70021 is longer than maximum of 65535";
- so does a 70,000-byte `system` in a clustering key ("Key length of 70010");
- so does a composite partition key of a 40,000-byte `system` plus a 40,000-byte `code` (80,021).

The CAS had already applied when step 5 failed, so no retry, sweep or repair could ever finish the write. Now step 1 refuses any `code`, `system` or `uri` value over 2,048 UTF-8 bytes with `ValidationError::InvalidResource`, 400, before the intent row (3.1, "Size check").

**G36. A superseded unknown outcome was always undecidable.** *Resolved in the design (ADR-aacruzgon-0026, refining ADR-aacruzgon-0024).*
- `resource_versions` gains `write_id`, written by V1 and V2 and by any V3 built from R1.
- When the serial read finds a version past ours, R3 on row `V` decides it: our `write_id` is success with no index writes; another is a definite miss.
- It stays undecidable (504 or 503) when row `V` has no `write_id` (row `V` can no longer be missing once superseded, G40), which is the case when `update`, whose `StoredResource` has no `write_id`, superseded it (3.1).

**G37. Two `_id` parameters must intersect.** *Resolved in the design (ADR-aacruzgon-0026).* A `patient/` scope appends `_id=<patient>` to every Patient search. If it had been checked against an index table, P5 would have made such searches empty; if only the first `_id` had been used, they would have leaked other patients. All `_id` parameters are now intersected before any read (5.4).

**G17. `_id` and `_lastUpdated` from the extractor.** *Resolved in the design (P5, ADR-aacruzgon-0027).* The `_id` and `_lastUpdated` rows are dropped before `K(V)` is computed. `_profile`, `_source`, `_tag` and `_security` are in scope (C1) and stay. `SearchParameterExtractor::extract` returns `_id` (token) and `_lastUpdated` (date) for every resource, and also `_profile`, `_source` (URI) and `_tag`, `_security` (token) (`search/extractor.rs`, the comment at line 1257; the tests at 2507 to 2521 assert `_source` and `_profile` are indexed).

- If written, every resource gets three `_id` token rows no query reads, and a `_lastUpdated` row in both date tables whose key changes on every update: two more tombstones per update, in partitions holding every resource of the type.
- Decided: drop `_id` and `_lastUpdated` before computing `K(V)`; `coverage.md` serves both without index rows. The other four are in scope (`coverage.md` C1). This is safe only because `_id` is never checked against an index table, a second `_id` parameter included (G37).

**G23. What the client gets when the CAS applied but the index writes failed.** *Resolved in the design (ADR-aacruzgon-0027).* Decided: return success and rely on the sweeper; the design's claim is narrowed to "searchable within one poll interval plus the grace period when the index writes fail". Retries are exhausted and the intent row is in place.

5xx is wrong for `POST`: the resource exists, and a client retry creates a second one (conditional create is out of scope). Success breaks the design's "any write the client saw succeed is searchable". Proposed: return success, rely on the sweeper, and narrow the claim to "searchable within poll interval plus grace when the index writes fail".

**G27. `too-costly` has no search error.** *Resolved in the design (ADR-aacruzgon-0027).* Decided: the cap's error is 422 `SearchError::TooManyResults`, issue `processing`; no new variant, and `too-costly` is not produced by any search. The cap counts distinct candidates point-read, per page or per `search_count` (5.1, step 7). The compartment cap follows the same rule. `SearchError::TooManyResults`, the closest variant, maps to 422 `UnprocessableEntity` (`crates/rest/src/error.rs`). No search path produces issue code `too-costly`; its only producer is the batch handler's 500 for a transaction Bundle too large for MongoDB's cache (`handlers/batch.rs`). Accept 422, or add a variant for the upstream touch list. Also define what the cap counts: index rows read, or distinct candidates.

**G32. Does the intent row need `write_id`?** *Resolved in the design (ADR-aacruzgon-0027).* Decided: keep it as a diagnostic, so an operator can match a stuck intent to a request's log; the sweeper still never reads it. I1 writes it and the table has the column, but I3 does not select it and the sweeper never compares it (section 6). Keep it as a diagnostic (an operator can match a stuck intent to a request's log), or drop the column and the bind from I1. Dropping it later is a schema change, though one with no data to migrate, since intent rows live 24 h.

### Resolved by an earlier decision

**G14. `created_at`.** *Resolved (ADR-aacruzgon-0011, accepted).* `resources` has it, `resource_versions` does not, and PostgreSQL stores none and passes `last_updated` as `created_at` everywhere. The ADR keeps it on `resources` only, written at version 1 and kept on revive (as MongoDB does), and has vread pass the version's own `last_updated`, which avoids a second read.

**G15. vread of a delete marker.** *Resolved (design, "Why `readHistory` stays `true`": a delete marker answers 410; ADR-aacruzgon-0025).* The backend returns a `StoredResource` with `deleted_at` set and content `Value::Null` (PostgreSQL parity); REST turns it into 410 (404 if `HFS_RETURN_GONE` is false). A `vid` that is not a valid `int` is `Ok(None)`, 404, with no query.

**G19. `_lastUpdated` next to the cursor.** *Resolved (checked on the node; the design states the rule under "Search", ADR-aacruzgon-0027 records it).* Checked on a 5.0.9 node, 2026-10-13, on `resources_by_type`:

| Restriction | Result |
| --- | --- |
| `last_updated >= X AND (last_updated, id) < (…)` | accepted |
| `(last_updated) >= (X) AND (last_updated, id) < (…)` | accepted |
| `(last_updated, id) >= (X, '') AND (last_updated, id) < (…)` | accepted |
| `last_updated <= X AND (last_updated, id) < (…)` | rejected: "More than one restriction was found for the end bound on last_updated" |

- A lower bound and the cursor work together. An upper bound (`eq`'s upper half, `lt`, `le`, `eb`, `ap`'s upper half, `ne`'s lower half) and the cursor are both end bounds, and only one is allowed. Rule: Rust picks the tighter and sends only that one (5.5, L2); no stop condition is needed.
- Optimisation option, not the design: with another parameter `_lastUpdated` is a residual filter (5.8), but the token and URI tables also cluster on `last_updated`, so it could be a slice on the driving partition, under the same one-end-bound rule, reading fewer rows when the window is narrow. Record whether to take it.

**G21. Keyset paging on a changing key.** *Resolved (roadmap 6d's exit criterion says "with no writes during the run" and cites this gap).* A resource updated while a client pages moves ahead of the cursor and is skipped; one on an earlier page that is updated is not seen twice. The spec leaves this to the server, but roadmap 6d's exit criterion ("exactly 100,000 distinct ids") holds only with no writes during the run.

**G24. Retries and idempotency.** *Resolved (design step 2 and ADR-aacruzgon-0024).*

- Index batches and version rows are idempotent (fixed timestamps). The request now keeps I1's full key, so a retried intent rewrites the same row (design step 2).
- `bucket` is the UTC hour of `enqueued_at` itself, never a second clock reading (now in the design).
- The sweeper reads `shard` back from the row, so any function works, even random. If it is a hash, name one stable across processes and Rust releases (not `DefaultHasher`).
- `create_or_update` writes a new intent row for each turn, with its own `enqueued_at` and that turn's version, and a turn whose CAS misses deletes its own row (ADR-aacruzgon-0024). Rewriting one key with a new version, or deleting it and writing it again, would depend on coordinator timestamps.
- HTTP retries: a `POST` after a timeout creates a duplicate; a `PUT` with `If-Match: W/"N"` after success gets 412; a `DELETE` after success gets 404. The other backends do the same; the demo script and the benchmark harness must not count these as Cassandra errors.

**G28. Batch `ifMatch` plus the `create_or_update` retry is last-writer-wins.** *Resolved (ADR-aacruzgon-0024; an upstream limitation, recorded in the design).* REST checks a batch entry's `ifMatch` with a read, then calls `create_or_update` (`batch.rs` documents this window). Our loop retries on a newer version, so an entry conditioned on `W/"1"` can overwrite a version 2 it never saw. The router cannot call `update_with_match`, but our retry widens the window. Recorded in the design; the upstream `create_or_update` contract (#2111) makes the retry mandatory, and the bound is 3 retries, or 4 attempts (ADR-aacruzgon-0024).

**G29. Does a delete keep `data` in `resources`?** *Resolved (design step 4: "`resources.data` still holds the old content" after a delete).* Keeping it costs nothing and writes no tombstone, and `read` never returns it; clearing it writes a cell tombstone. The walk-through keeps it. A revive must not read it as `K(N)` (the marker's key set is empty), and its V3 must leave `data` unset, as design step 4 now says.

**G30. Hot and huge partitions.** *Resolved (design, "measure partition growth first, bucket on a trigger"; roadmap section 7).* `token_code_index (Patient, gender, female)` holds about half the patients; `token_system_index (identifier, MRN)` holds all of them if every patient has an MRN; `token_system_index (gender, IMPLICIT)` is no longer written (P5). Each update of a patient leaves a tombstone in every one of these partitions.

### Resolved last (ADR-aacruzgon-0027)

**G7. `count` has no tag check, so it counts what `resources_by_type` holds, not what is live.** *Resolved in the design (ADR-aacruzgon-0027).*

| Kind | Case | Effect on `count` |
| --- | --- | --- |
| expected, in flight | create in flight or crashed after the CAS | one too few until the sweep |
| expected, in flight | delete in flight or crashed after the CAS | one too many until the sweep |
| expected, in flight | update split across batches | briefly 0 or 2 |
| expected, until the sweep | orphan `resources_by_type` row a crash left | one too many until the sweep; after TTL expiry, a stall past the grace, or a pause past `gc_grace_seconds` (G11), until repair reaches it |
| lasting | row resurrected after `gc_grace_seconds` (3 h on `resources_by_type` in `schema.cql`) | one too many, until an unfiltered type search pages over it and repair on read deletes it. Only with RF 3, when a replica missed a delete (an outage of any length, a dropped mutation, or a lost hint) and repair did not reach it before the tombstone was purged; impossible with RF 1 |

- So the design's "the tag check makes resurrected rows harmless" does not hold for `count`. Options: dedupe ids in memory during the scan (about 4 MB per 100,000 ids; fixes duplicates only); keep the default `gc_grace_seconds` on `resources_by_type`; or a tag check per row (too slow).
- `count(tenant, None)` (called by `/console` metrics, `console_metrics.rs`): no table enumerates a tenant's types. Loop over the registry's types (about 145 in R4), or add a per-tenant types table.
- A filtered `search_count` (`_count=0`): tag checks and the candidate cap? It must agree with the unfiltered path, or `GET /Patient?_count=0` and `GET /Patient?gender=male,female,other,unknown&_count=0` differ.

- **Decided (ADR-aacruzgon-0027):**
    - `gc_grace_seconds` stays at 3 h on `resources_by_type` (ADR-aacruzgon-0009), with three operations rules on the cluster: a replica back from an outage shorter than `max_hint_window` (3 h), or after a spike of `Unavailable` errors, timeouts or dropped mutations, is repaired promptly, within 3 h of the outage's start; a replica down longer than `max_hint_window` is replaced (`replace_address_first_boot`), never rejoined, even after repair; and the replacement node is repaired once it has bootstrapped, since Cassandra's replace procedure says "you **MUST** run repair to make the replaced node consistent again" when "The node is down for longer than `max_hint_window` before being replaced" ([replacing a dead node](https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/topo_changes.html#replacing-a-dead-node)). The rules narrow the lasting row of the table above; they do not remove it.
    - **Why it is only narrowed (CodeRabbit's review of PR #33).** Hints are best effort: "Hints are best effort, however, and do not guarantee eventual consistency like anti-entropy repair does" ([hints](https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/hints.html)). A hint also expires at the smallest `gc_grace_seconds` of its mutation's tables (`Hint.java:56-57`, `131-158` in `cassandra-5.0.9`), so a hint for `resources_by_type` lives 3 h, the same as `max_hint_window`. A hint not delivered in time, one never written (disabled, past the window, or over the per-host limit, `StorageProxy.java:2428-2462`), or one lost with a coordinator before its 10 s flush can each leave a replica holding a row whose tombstone the others purge after 3 h. Cassandra's own rule is "repair should be run often enough that the gc grace period never expires on unrepaired data" ([repair](https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/repair.html)), which a 3 h grace makes near-continuous.
    - **What is at risk.** Only `count`, `_count=0` without filters, and `/console` metrics, on the three-node cluster: one too many per resurrected `resources_by_type` row. Search is never wrong: a resurrected row has an old `version_id`, so the tag check hides it (5.1, step 4). The deliverable runs at RF 1, where no replica can miss a write.
    - **Search can fail, not only count.** Each resurrected index or `resources_by_type` row is a candidate that a search point-reads and the tag check hides, so it counts against the candidate cap (5.1, step 7). If enough of them lie ahead of the live matches in one partition, that page returns 422 `TooManyResults` before it fills. It never returns a wrong result. Repair on read deletes each stale row the search reads, at step 4, before the cap is checked at step 7, so a retry gets further. The same holds for the orphans of the design's stated limitations. Reaching the cap needs that many missed deletes in one partition, all purged before repair, so we accept it with the `count` exposure.
    - **Rejected:** `only_purge_repaired_tombstones` on `resources_by_type`, which purges a tombstone only after repair (it defaults to false, `CompactionStrategyManager.java:1239-1242`): without regular repair, tombstones are never purged, which is worse than G8, and setting it in `schema.cql` erases the inherited UCS sub-options, as the README warns (checked on a 5.0.9 node, 2026-11-05). A higher `gc_grace_seconds`, back towards the 10-day default: tombstones would stay for days, the G8 cost. Either is the fix if the cluster ever becomes a deployment target.
    - A filtered `search_count` uses tag checks and the candidate cap; an unfiltered `count` scans `resources_by_type`.
    - `count(tenant, None)` loops over the resource type names of every enabled FHIR version (`FhirVersion::enabled_versions()` with `FhirResourceTypeProvider::get_resource_type_names`, as `ResourceTypeScope` does, `search/type_qualifier.rs:98-118`), each name once. Not over the registry's `resource_types()`, which the first recommendation named: it returns the bases of registered parameters (`crates/fhir/src/search/registry.rs:538-541`), which in R4 include the abstract `Resource` and `DomainResource` and miss every type with no parameter of its own (`Binary`, `Parameters`, `OperationOutcome`, from `data/search-parameters-r4.json`), so it would undercount.
    - `count` and a filtered `search_count` may differ in three cases: while a write is in flight; while a crashed write waits for the sweeper; and in the design's stated limitations (an intent that expired during a sweeper outage over 24 h, a request stalled past the grace period, or one paused past `gc_grace_seconds`), where an orphan row stays counted until a search repairs it (5.9). "Only while writes are in flight", the first wording, was not accurate.

**G18. Rows that no query reads.** *Resolved in the design (P5, ADR-aacruzgon-0027).*

- `token_system_index` rows for system `IMPLICIT`: `system|` never matches the marker. `(gender, IMPLICIT)` would hold every Patient, `(status, IMPLICIT)` every Observation.
- `token_system_index` rows for system `''`: no query form asks for "system is empty" with a pipe after it.
- Token rows with an empty code are display-only: the converter emits an empty code only for `CodeableConcept.text` with no coding, with no system (`IndexValue::token_display_only`, `search/converters.rs:189-197`). It skips a Coding without a code and an Identifier without a value (`converters.rs:444-445`, `502`). They are dropped.
- URI rows with an empty URI.
- Decided: the `IMPLICIT` and `''` rows in `token_system_index`, the display-only tokens, the empty URIs and the extractor's `_id` and `_lastUpdated` rows are dropped before `K(V)` is computed. Dropping the `IMPLICIT` rows saves one row in three for every `code`-typed element (8 rows per version in the worked example, not 9). The converter writes an empty URI for an empty string (`convert_to_uri`, `search/converters.rs:670-678`), which FHIR does not allow but a client can send; no query can match it. The `''` rows and the empty URIs were added after P5's first wording, which did not name them.

**G20. Date-only searches read their whole candidate set on every page.** *Resolved in the design (ADR-aacruzgon-0027).*

- The page order is known only after R1 for every candidate, so page `k` repeats the full scan and all the point reads. Every prefix but `eq` is one-sided, so `birthdate=lt2000` on 100,000 patients hits the candidate cap.
- Option: page date-only results in the table's own order `(start, id)` or `(end, id)`, with the cursor in the reserved slot: native paging for every prefix except the unions `ne` and `ge`.
- Option for `ge`: the single slice `date_index_by_end end > s` with the full predicate as residual, instead of the design's union. Every `ge` match has `te > s`, so nothing is missed; it reads more rows when many ranges end inside `[s, e)`. (`le` is already the single slice `start < e`.)
- Undefined: is a search with two date parameters, a repeated date (`date=ge2020&date=lt2021`), or a date with `_lastUpdated` "date-only"? The design is silent. Proposed: slice on one date, residual for the rest.

- **Decided (ADR-aacruzgon-0027):**
    - date-only searches page natively in the date table's own order, with the table named in the cursor (5.7);
    - `ge` is the single slice `end > s` with the full predicate as residual;
    - with several dates, `_lastUpdated` is sliced if present, otherwise the first date parameter in query order.
    - `ne`, and a comma list whose values slice both tables, read `date_index`'s slices first, then `date_index_by_end`'s, each in its own order. A resource is emitted only at its first matching row in that concatenated order, and the cursor's third element names the table being read (5.7). The first recommendation, "the table's own order", does not apply to them: they read two tables with no shared order.

**G8. Benchmark case: `count` and the last pages of an unfiltered search after a mass update.** *Resolved: a named benchmark case in roadmap Step 8 (ADR-aacruzgon-0027).* Updating every Patient once leaves a contiguous run of tombstones at the old end of `resources_by_type`, which a page reaching it reads in one command. 100,000 updated resources of one type give 100,000 tombstones, exactly the `tombstone_failure_threshold`; past it the read fails with `TombstoneOverwhelmingException`, a 5xx. That also needs `gc_grace_seconds` (3 h here) and compaction not to have purged them, which holds if the update phase and the reads run within 3 h. It is the design's own measure-first trigger (above 1,000 tombstones per read) firing by construction. Decided: roadmap Step 8 names it as a benchmark case, "mass update": update every resource of one type once, then run `count` and page an unfiltered search to its last page within 3 h, and record tombstones scanned per read against `tombstone_warn_threshold` (1,000) and `tombstone_failure_threshold` (100,000).

**G12. Several Helios processes means several sweepers.** *Resolved: accepted, no lease row (ADR-aacruzgon-0027).* Each polls all 32 partitions with its own marks, runs its own startup scan, and races the others on the same intents. The writes are idempotent, so the cost is duplicate I3 reads, Paxos reads and replays. Decided: accept it. This project runs one Helios process. Every sweeper write carries the version's fixed timestamp, so two sweepers, or a sweeper and a slow request, write the same cells and change nothing; extra sweepers only add duplicate reads. A lease row would add an LWT per poll, and a lease that outlives a crashed holder would stall every intent until it expired.

**G22. Old keys are recomputed with today's registry.** *Resolved in the design: each version stores its key set (ADR-aacruzgon-0027).* `K(N)` was re-extracted with the registry and extractor as they are now. If a SearchParameter or the extractor changed since version `N` was indexed, the deletes missed rows that were written. The registry does change at runtime, per tenant: a `SearchParameter` write invalidates the tenant's registry (`crates/persistence/src/backends/sqlite/storage.rs:520-529`). The orphans would waste point reads, count against the candidate cap, and never be cleaned.

- Decided: `resource_versions.index_keys blob` holds `K(V)`, written in the same row as the version (V1, V2, V3 and the sweeper's replay), and `resources.index_keys blob` holds the current version's, written by every CAS. Neither adds a statement. Writers and repair take `K(N)` from `resources`; the sweeper takes `K(V)` from `resources` and `K(V-1)` from row `V-1`. Nothing is re-extracted. The encoding, the sizes, the race the `resources` copy closes, the extra reads per operation and the limitation on new SearchParameters are in section 2, "Stored key sets".
- The `resources` copy was added after CodeRabbit's review of PR #33. The first decision stored the set on `resource_versions` only and re-extracted `K(N)` when row `N` was missing, which left orphans when a writer paused after its CAS while the registry changed (section 2). It also had one extra read on five operations; now only `update` has one.
- This reverses the earlier proposal to accept the orphans. It was reversed after Alan asked "why would we leave orphan rows?".

### Raised in review of PR #33 (ADR-aacruzgon-0027)

**G38. A custom SearchParameter's `code` becomes `param` in every index key, and nothing bounds it.** *Resolved: a 128-byte bound when a SearchParameter is stored, and `param` in the step 1 size check (ADR-aacruzgon-0027).* CodeRabbit's review of PR #33 found it.
- **Where it is unbounded.** [R4 `SearchParameter.code`](https://hl7.org/fhir/R4/searchparameter-definitions.html#SearchParameter.code) is a `code`, 1..1, with no invariant; it says only "For maximum compatibility, use only lowercase ASCII characters". The [`code` datatype](https://hl7.org/fhir/R4/datatypes.html#code) has no length limit, and its [string](https://hl7.org/fhir/R4/datatypes.html#string) base only "SHALL NOT exceed 1MB". R5 is the same. The loader checks characters, not length (`crates/fhir/src/search/loader.rs:31-36`, `512-520`).
- **Measured on a 5.0.9 node.** On 2026-11-05, with a 64-byte tenant, resource type `MedicinalProductUndesirableEffect` (33 bytes) and a 2,048-byte `system` and `code` in `token_system_code_index`: a `param` of 61,327 bytes was accepted, and one of 61,328 failed with "Key length of 65536 is longer than maximum of 65535". Such a write would commit its CAS and then fail step 5 on every retry. The 5 KiB batch argument (3.1) already fails once `param` passes about 1 KB.
- **Why 128.** The longest code in the spec data is 33 bytes in R4 and R4B, 35 in R5 and 46 in R6 (`data/search-parameters-*.json`). 128 leaves almost three times that, and keeps the largest key at 4,337 bytes.
- **Decided:** the Cassandra backend refuses to store a SearchParameter whose `code` is over 128 UTF-8 bytes, with 400 (3.1, "SearchParameter `code` bound"), and the step 1 size check also measures `param`, as a backstop. This is a stated server restriction; FHIR sets no limit. Conformance seeding goes through `create_many` (`crates/persistence/src/search/seeder.rs:110`), and every spec code passes.

**G39. A missed revive brought `resources.deleted_at` back.** *Resolved: `deleted_at = 0` means live, and `resources` writes no tombstone (ADR-aacruzgon-0027).* CodeRabbit's third review of PR #33 found it.
- **The fault.** C4 cleared `deleted_at` with a literal null, the one cell tombstone in `resources`. `resources` keeps the 10-day default `gc_grace_seconds` (`expected-schema.txt`). A replica that missed the revive and its hint kept the delete's value. Once the other replicas had purged the tombstone, any read that included that replica reconciled to the old value: the zombie the 5.0 [tombstones](https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/compaction/tombstones.html) page describes. Then:
    - `read` answered 410;
    - search hid the resource;
    - `update` missed its CAS and answered 404;
    - read repair copied the value to the other replicas.
- **Why Paxos v2 does not prevent it.** In `cassandra-5.0.9`:
    - a commit is hinted like any write (`PaxosCommit.java:219-231`, `allowHints = true` at `Paxos.java:764`), and hints are best effort;
    - a prepare refreshes stale promisers with the latest commit only (`PaxosPrepare.java:80-104`, `852-857`; `PaxosPrepareRefresh.java:177-184`), and a later C2 does not touch `deleted_at`;
    - once the tombstone is purged, read repair and repair both treat the stale value as the newest.
- **Reproduced on a three-node 5.0.9 cluster** (2026-11-06; RF 3, Paxos v2; `gc_grace_seconds = 0` standing in for the 10 days, which also gives a hint a TTL of 0, standing in for a lost one). Steps: create and delete with all nodes up; stop one node; revive, then update to version 4; flush and compact the other two; restart the stopped node; no repair.

    | Read after the restart | Literal-null revive | `deleted_at = 0` revive |
    | --- | --- | --- |
    | `CONSISTENCY ALL` | version 4, revived data, `deleted_at` = the delete's time | version 4, `deleted_at` 1970-01-01 |
    | `ONE` on each node afterwards | the delete's time on all three | 1970-01-01 on all three |
    | next CAS, `IF version_id = 4 AND deleted_at = null` / `= 0` | `[applied] False`, so 404 | `[applied] True` |

- **Decided.** C1 and C4 write `deleted_at = 0`, a delete writes its own time, and C2 and C3 test `deleted_at = 0`. Every reader tests `deleted_at > 0` (section 2, "`deleted_at` and its readers"). `resources` and `resource_versions` now hold no tombstone, so their grace period no longer affects correctness. No column or type changes, and `expected-schema.txt` is unchanged. Repair within the 10-day grace period stays routine practice on the cluster ([repair](https://cassandra.apache.org/doc/5.0/cassandra/managing/operating/repair.html): "At a minimum, repair should be run often enough that the gc grace period never expires on unrepaired data"), but correctness no longer depends on it. It supersedes ADR-aacruzgon-0024's literal-null rule (G13).

**G40. A missing row `V-1` was read as an empty key set.** *Resolved: V3 is written before the CAS (ADR-aacruzgon-0027).* CodeRabbit's third review of PR #33 found it. It predates `resources.index_keys`: ADR-aacruzgon-0013's sweeper took a missing row `V-1` to mean "its writer never reached step 5".
- **The sequence, before the fix.**
    1. Writer `N`'s CAS applies; `N` pauses before step 4.
    2. Writer `N+1` reads `K(N)` from `resources`; its CAS applies and overwrites `index_keys`; it crashes before step 4. `K(N)` is now held only in the paused writer's memory.
    3. The sweeper takes `N+1`'s intent: `N+1` is current, row `N` is missing, so `K(N)` was taken as empty. It inserts `K(N+1)`, deletes nothing, and deletes the intent.
    4. Writer `N` resumes and inserts `K(N)`. Every key of `K(N) \ K(N+1)` is an orphan tagged `N`. That always includes `resources_by_type (lu(N), id)`, so `count` was one too high even on a single node.
- **Decided.** V3, the rewrite of row `N`, moves from step 4 to step 2, before the intent row and the CAS (3.1). Once any CAS creating `N+1` has applied, row `N` exists with the exact `K(N)`. The sweeper's `K(V-1)` therefore always comes from a row that exists (section 6), and in step 3 above it deletes `K(N) \ K(N+1)` at `ts(N+1)`; writer `N`'s late inserts at `ts(N)` lose. It also removes the "two crashes in a row" limitation (G2) and the sweeper's missing-row branch (G3, G31).
- **Cost.** One more sequential round trip on `update`, `update_with_match`, `delete`, `delete_versioned` and a `create_or_update` turn on a live row or a delete marker, since V3 no longer shares a batch with V1. A CAS that misses wastes its V3, which changes nothing. No schema change (section 2, "Extra reads and round trips").
- **Checked on a fresh 5.0.9 node** (2026-11-06), with the statements in section 2: see ADR-aacruzgon-0027.
