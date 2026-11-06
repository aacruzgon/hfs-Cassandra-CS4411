# Single-node Cassandra

A single Apache Cassandra 5.0 node for the Cassandra persistence backend: local
development, the Cassandra test job, and the benchmarks in
`docs/Cassandra/development-roadmap.md` (item 0.1). The three-node cluster
(item 0.3) and the PostgreSQL benchmark container (item 0.2) are separate
files with the same CPU and memory limits.

## Run

```bash
# 16 and 18 GB laptops (1G heap)
docker compose -f docker/cassandra/docker-compose.yml up -d --wait

# 32 GB machines, and every benchmark that goes in the report (2G heap)
docker compose -f docker/cassandra/docker-compose.yml \
               -f docker/cassandra/docker-compose.heap2g.yml up -d --wait
```

`--wait` returns once CQL answers on `127.0.0.1:59042`. A first boot takes
about 59 seconds on an Apple-silicon laptop and a restart about 11. Data lives
in the `hfs-cassandra_cassandra-data` volume and survives `down`; use
`down -v` to start from an empty node.

```bash
docker compose -f docker/cassandra/docker-compose.yml exec cassandra cqlsh
```

Point clients at `127.0.0.1:59042`, not `localhost`: the port is published on
IPv4 loopback only, and `localhost` can resolve to `::1` first. The host port
is 59042, not Cassandra's usual 9042, so a Cassandra already running on the
host cannot silently answer instead; set `CASSANDRA_HOST_PORT` to change it.
Inside the container, and for other containers, the port is still 9042.

The node advertises its container IP as `rpc_address` in `system.local`.
`cdrs-tokio` 8.1.9, the backend's driver, never uses that address for the node
it connected through: it keeps the contact point (`build_node_broadcast_rpc_address`
in its `cluster_metadata_manager.rs`), and one node has no peers to learn. So
`127.0.0.1:59042` works as is, and `CASSANDRA_BROADCAST_RPC_ADDRESS` is not
needed. `tests/cassandra_tests.rs` checks that the driver addresses the node by
its contact point.

**A restart of the node can leave an open `cdrs-tokio` 8.1.9 session failing
for as long as its control connection stays up.** Queries then fail with "No
nodes available in query plan!". Setting `CASSANDRA_BROADCAST_RPC_ADDRESS:
127.0.0.1` does not change this.

The driver's control connection does one of two things when it reconnects:

- **Its query plan is empty** (the node is down or ignored): it rebuilds the
  metadata from its contact points, which start `Up`, and the session works
  again (`control_connection.rs`, `full_refresh`).
- **The plan still holds the node:** it updates the node in place.
  `Node::clone_with_node_info` (`topology/node.rs`) compares the node's gossip
  address (`<listen address>:7000`) with its contact point, finds them
  different, and marks the node `Unknown`, which drops it from every plan.

With one node no peer event sets it back. So the session stays broken until
something drops the control connection again, which takes the first path, or
until the client opens a new session.

Checked on 2026-11-06 with the Compose file on host port 59242. A `docker
compose restart` at 8 s gave failures until the session recovered at 36 s (the
first path). Failures came back at 40 s (the second path) and lasted to the end
of a 180 s run, with and without the variable; the control connection stayed up
throughout. That a later drop restores the session comes from the driver
source, not from this run. After restarting Cassandra, restart anything that
holds a session to it, Helios included, rather than wait for that.

## Recorded configuration

| Setting | Value | Why |
| --- | --- | --- |
| Image | `cassandra:5.0.9-trixie@sha256:618a493b…2889c` | Cassandra 5.0.9, OpenJDK 17.0.20.1 (2026-08-18), Debian 13 trixie; pushed 2026-10-06 for amd64 and arm64. Trixie because the Docker official-images library lists no 5.0 bookworm tag any more, so a bookworm build gets no further rebuilds; `5.0`, `5` and `latest` point to trixie. Pinned by digest so the image does not change under us. |
| Config file | `cassandra_latest.yaml`, as shipped | `entrypoint.sh` copies it over `cassandra.yaml` on every boot. Copying, rather than `-Dcassandra.config`, is needed because the image's entrypoint writes the cluster name, snitch and DC into `cassandra.yaml` only. See "Why cassandra_latest.yaml" below. |
| Changed by `cassandra_latest.yaml` | `paxos_variant` v2; trie memtables with `memtable_allocation_type: offheap_objects`; SSTable `selected_format: bti`; `default_compaction` `UnifiedCompactionStrategy` (T4); `concurrent_compactors` 8; `key_cache_size` 0MiB; `column_index_size` 4KiB; `commitlog_disk_access_mode: auto`; `trickle_fsync: true`; `uuid_sstable_identifiers_enabled: true`; `stream_entire_sstables: true` (explicit; also the default when unset); `default_secondary_index: sai`; `batchlog_endpoint_strategy: dynamic_remote`; `storage_compatibility_mode: NONE` | Versus the stock `cassandra.yaml` of the same image. `NONE` means 5.0-only on-disk formats that 4.x cannot read. `concurrent_compactors` 8 exceeds the 4-CPU limit; it is kept as shipped on purpose, because changing it is unmeasured. |
| `paxos_variant` | `v2` | Two round trips per LWT write instead of four. Already set by `cassandra_latest.yaml` (the stock file leaves v1); `entrypoint.sh` still calls `set_yaml paxos_variant v2`, which fails the boot if the key is ever missing or changed. |
| `paxos_state_purging` | `legacy` (default, in neither file) | Safe with `LOCAL_QUORUM` commit consistency. Never switch to `repaired` and back. |
| Snitch, DC, rack | `GossipingPropertyFileSnitch`, `helios-dc1`, `helios-rack1`; cluster `helios` | The same as the three-node cluster, so one `NetworkTopologyStrategy` map, `{'helios-dc1': N}`, works everywhere. Cassandra refuses to start a node whose DC, rack or cluster name differs from what its data recorded, so changing any of them means `down -v`. |
| Heap | `MAX_HEAP_SIZE` 1G, or 2G with the override | G1 sets `-Xms` equal to `-Xmx`. `HEAP_NEWSIZE` stays unset, because it is ignored under G1. |
| Direct memory | 512M, or 1G with the override | Half the heap. Otherwise `cassandra-env.sh` sizes it from the host's RAM. |
| GC threads | 4 parallel, 4 concurrent | Otherwise `cassandra-env.sh` uses the host's core count, not the CPU limit |
| Host port | `127.0.0.1:59042` (`CASSANDRA_HOST_PORT` overrides) | A process already listening on 9042 on the host would answer instead of the container without any error. See ADR-aacruzgon-0007. |
| CPU limit | 4 | Same in both profiles and for PostgreSQL |
| Memory limit | 3 GiB, or 5 GiB with the override; swap disabled | Indicative, Apple-silicon 18 GB laptop under OrbStack: idle 1.61 GiB (1G heap) and 2.65 GiB (2G heap); peak under a 32-thread write load 1.91 GiB of 3 GiB and 3.31 GiB of 5 GiB; no OOM kill. With the stock yaml a 1.5 GiB limit was OOM-killed at boot. |
| Unchanged | `commitlog_sync: periodic` (10000ms), `batch_size_warn_threshold` 5KiB, `batch_size_fail_threshold` 50KiB, tombstone warn 1000 / fail 100000, `num_tokens` 16 | The same in both files. Recorded so that a benchmark report can state them |

OrbStack or Docker Desktop must give its VM more memory than the container
limit.

## Why cassandra_latest.yaml

Its header describes it as the configuration "for new users of Cassandra who
want to get the most out of their cluster, and for users evaluating the
technology", and says to copy it over `cassandra.yaml`, which is what
`entrypoint.sh` does. The 5.0 `NEWS.txt` adds that it is "only recommended for
production use after careful evaluation"; this project is that evaluation. On an indicative
laptop comparison (same image, heap, limits and Paxos v2; `cassandra-stress`,
32 threads, 3 x 60 s runs) it gave about +25% plain-write and +67% read
throughput over the stock file, with the ranges not overlapping. LWT
throughput differences were within run-to-run spread, so the item-0.6
benchmark harness must measure LWT at steady state. Memory peaks stay within
both limits. Details and the decision:
[ADR-aacruzgon-0001](../../docs/Cassandra/ADR-aacruzgon/2026-10-13/0001-image-pin-and-paxos-v2.md).

## Check that the settings are in effect

```bash
docker compose -f docker/cassandra/docker-compose.yml exec cassandra cqlsh -e "
  SELECT name, value FROM system_views.settings
  WHERE name IN ('paxos_variant', 'paxos_state_purging', 'endpoint_snitch',
                 'batch_size_warn_threshold', 'batch_size_fail_threshold',
                 'commitlog_sync', 'commitlog_sync_period');
  SELECT release_version, data_center, rack FROM system.local;"
```

Expected: `paxos_variant` `v2`, `paxos_state_purging` `legacy`, `5.0.9`,
`helios-dc1`, `helios-rack1`. Add `'memtable_allocation_type',
'storage_compatibility_mode'` to the list to confirm `cassandra_latest.yaml` is
in use (`offheap_objects`, `NONE`). There is no `nodetool` command for the
Paxos variant; the `system_views.settings` virtual table is the source of
truth.
