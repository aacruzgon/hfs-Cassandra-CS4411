# Cassandra spike: what the node actually does

Roadmap item 0.10. **Read this before the schema session (Step 1).** Every experiment ran on the project's own single node (`docker/cassandra`: Cassandra 5.0.9, `cassandra_latest.yaml`, Paxos v2, 1G heap, DC `helios-dc1`, RF 1) against a two-table toy schema: the design's `resources` and `resources_by_type`. Full transcripts and traces are in [`spike/`](spike/): `a/` holds A1–A4, `b/` holds B1–B5 and `c/` holds the LWT kill test. Each finding below says whether the claim in `cassandra-discussion.md` held.

## Summary

| # | Question | Result | Design claim |
| --- | --- | --- | --- |
| A1 | What does a compare-and-swap (CAS) return when it fails? | The current values of the condition columns; nothing at all for a missing row | Held |
| A1 | Does a CAS whose only condition is `deleted_at = null` create a row? | **Yes**, a ghost row | Held: never write one |
| A1 | `USING TIMESTAMP` on a lightweight transaction (LWT)? | Rejected | Held: `write_ts` stays a column |
| A2 | Slice vs full-partition read | Cost follows rows read, not partition size | Held |
| A3 | Keyset paging on `(last_updated DESC, id DESC)` | Exact, including ties across page boundaries | Held |
| A4 | `''` as "no system" in a partition key | Works in a composite key; rejected as a whole key | Held, with a limit |
| B1 | Batch size thresholds | Warn at 5 KiB, fail at 50 KiB, logged or unlogged; **single-partition batches are exempt** | Held, plus a detail |
| B2 | Tombstones | Read through and counted; the 1,000 warning fires | Held |
| B3 | Fixed write timestamps; binding `null` | A later delete beats an earlier insert; `null` writes a cell tombstone | Held |
| B4 | `kill -9` mid-batch | No half-applied batch, but **about 10 s of acknowledged writes lost** | Held: expected at RF 1 |
| C | Do acknowledged LWT writes survive `kill -9`? | **No**: about 3.5–4.7 s lost, a clean prefix cut | Settles ADR-0005's open question |
| — | Tracing on one node | **Fails until `system_traces` is set to RF 1** | Not in the design |

## Lightweight transactions (A1)

The write path tells "gone", "not there" and "lost the race" apart from the result of a failed CAS, without a second read. That works:

```text
UPDATE … IF version_id = 1 AND deleted_at = null      -- row is already at version 2
 [applied] | version_id | deleted_at
     False |          2 |       null                      -> 409

UPDATE … IF version_id = 3 AND deleted_at = null      -- row was soft-deleted at version 3
     False |          3 | 2026-10-14 02:54:22.511000+0000 -> deleted (404 for a write, see point 3)

UPDATE … WHERE id = 'missing1' IF version_id = 1
     False                                                -> 404 (no columns at all)
```

**The ghost row is real.** `UPDATE … SET version_id = 2 WHERE id = 'missing2' IF deleted_at = null` returned `[applied] True` and created a row. So `create` must stay `INSERT … IF NOT EXISTS`, and revive must keep `IF version_id = N`.

`USING TIMESTAMP` on an LWT fails with `Cannot provide custom timestamp for conditional updates`, which is why the design stores `write_ts` as a column and applies it only to the index writes. A read at `LOCAL_SERIAL` works.

A traced LWT shows Paxos v2's shape: a prepare round that carries the condition read (`Preparing … with read`, `Promising read/write ballot`), then propose/accept, then commit. That took about 5 ms on one node, with no network involved.

## Reads and paging (A2, A3, A4)

5,000 rows in one `resources_by_type` partition, one SSTable:

```text
(last_updated, id) < (…) LIMIT 20    Read 20 live rows and 0 tombstone cells     9.5 ms
SELECT count(*) …                    50 internal pages of 100 rows, 5000 rows   50.9 ms
```

Read cost follows the rows read, so `count` and the date scans really are O(rows), as the design says.

Keyset paging with `(last_updated, id) < (?, ?)` on the `DESC, DESC` order excludes the cursor row and places tied rows correctly, even when a tie is split across a page boundary. Five pages of 10 matched one `LIMIT 50` exactly.

`''` works as one component of a composite partition key, and stays distinct from a real system. As a *whole* partition key it is rejected with `Key may not be empty`, so the sentinel is only safe where the key has other components. It does in every table the design uses it in.

## Batches (B1)

| Batch | Size | Result |
| --- | --- | --- |
| Logged, 6 partitions | 6.4 KiB | Succeeds with `Batch for [spike.resources] is of size 6564, exceeding specified threshold of 5120` |
| Logged, 60 partitions | 64 KiB | `InvalidRequest … Batch too large` |
| Unlogged, 60 partitions | 64 KiB | `Batch too large` |
| Logged, **1 partition**, 60 rows | 64 KiB | **Succeeds, no warning** |

The index batches are multi-partition, so the design's rule stands: keep each under 5 KiB, and never raise the threshold. A batch to a single partition is not size-checked at all. **Added 2026-11-05 (schema audit, ADR-aacruzgon-0026):** these batches carried their bytes in a regular `data` column. The check does not count partition-key bytes. On a 5.0.9 node, a 30-partition logged batch to `uri_index` with about 120 KB of partition keys was accepted with no warning, while a `token_code_index` batch with 78 KiB of clustering values was refused. Index rows are mostly key bytes, so the backend sizes its batches itself; the thresholds do not catch an oversized index batch.

## Tombstones and timestamps (B2, B3)

After deleting 300 of 500 rows: `Read 200 live rows and 300 tombstone cells`. One page of the paged read returned **0 live rows and 300 tombstone cells**, which is a client waiting on rows that are no longer there. At 1,200 deleted rows the server logs:

```text
WARN … Read 300 live rows and 1200 tombstone cells for query SELECT * FROM spike.resources_by_type … (see tombstone_warn_threshold)
```

That is the exact signal the roadmap's measure-first trigger uses, and the trace shows it per query.

Fixed timestamps behave as the write path needs. `DELETE … USING TIMESTAMP 300` followed by `INSERT … USING TIMESTAMP 200` leaves the row absent. An update at a lower timestamp than the current one loses.

Binding an explicit `null` writes a cell tombstone, shown by `sstabledump` as `{ "name" : "b", "deletion_info" : { … } }`; omitting the column writes nothing. So "leave absent columns unset" is a real rule.

## Crash behaviour (B4, C)

The client sent logged batches of 3 partitions each and recorded every acknowledgement. Then the node was killed with `SIGKILL` and restarted:

```text
CommitLog.java:210 - Log replay complete, 18402 replayed mutations in 605 ms
last acknowledged batch: 5481    batches present after restart: 1..3651, contiguous, none partial
```

**1,830 acknowledged batches, about 10 s of writes, were lost.** That is `commitlog_sync: periodic` at 10 s doing what it says. Batches stayed atomic, and the survivors are a clean prefix. The batchlog entries were lost together with their data, so the replay path itself had nothing to redo.

**Lightweight transactions get no special treatment (C).** A client sent conditional inserts (`IF NOT EXISTS`), plus a conditional update every 10th write, recording each acknowledgement with a timestamp. The node was then killed. This was run twice:

| | Acknowledged | Present after restart | Lost |
| --- | --- | --- | --- |
| Run 1, inserts | 1..2941 | 1..2628, no gaps | 313 (about 3.5 s) |
| Run 1, updates | 294 | 262 | 32 |
| Run 2, inserts | 1..2978 | 1..2525, no gaps | 453 (about 4.7 s) |
| Run 2, updates | 297 | 252 | 45 |

A Paxos commit does not force a commit-log sync. `system.paxos` is flushed and replayed like any other table, so an acknowledged LWT is exactly as durable as a plain write under `commitlog_sync: periodic`. The loss stayed under 10 s here because each kill landed partway through a sync period. **This settles the open question in ADR-aacruzgon-0005**: the durability table there is right for our write path, which is entirely LWTs.

## For the schema session

1. **Tracing needs `system_traces` at RF 1 on a single node.** It defaults to RF 2, so fetching a trace at `LOCAL_QUORUM` fails with `Cannot achieve consistency level LOCAL_QUORUM … alive_replicas 1`. Both spike halves had to run `ALTER KEYSPACE system_traces WITH replication = {'class': 'NetworkTopologyStrategy', 'helios-dc1': 1}`. Decide where that belongs: a single-node setup step, not `schema.cql`, since the backend should not alter system keyspaces.
2. **Keep the three CAS forms** the design already names. Create is `IF NOT EXISTS`; update and delete are `IF version_id = N AND deleted_at = null`; revive is `IF version_id = N`. Never a null-only condition.
3. **Map the CAS result without a second read:** no columns means 404; `deleted_at` set means the resource was deleted; anything else is 409. *Correction, found in the schema walk-through:* for a write, a deleted resource is also 404, as the design says (`ResourceError::NotFound`). The race suite accepts `NotFound` or `Gone` from a writer that lost to a delete, and requires `NotFound` from a repeated `delete_versioned`. 410 is for `read` and `vread` only.
4. **Keep index batches under 5 KiB**; they are multi-partition. A single-partition batch would not be checked, so nothing should rely on the threshold for those.
5. **At RF 1, a node crash loses up to about 10 s of acknowledged writes.** B4 measured this on one table. Because all tables share one commit log, replayed in order, we expect the loss to be a time-ordered suffix across every table. That is an inference, not measured. If it holds, the sweeper is safe: an intent row is written before its CAS, so a surviving CAS implies a surviving intent row.
    - The design's failure handling covers a Helios crash.
    - The failure tests (roadmap section 6) should also kill the node, and expect this loss at RF 1. This is noted under the section 6 table as a point to settle when those tests are written.
6. **`count` and date scans read every row and every tombstone in the partition.** Tombstones per read is the number to watch.
