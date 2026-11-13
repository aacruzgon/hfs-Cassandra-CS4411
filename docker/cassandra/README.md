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
`cdrs-tokio` 10.0.0, the backend's driver, never uses that address for the node
it connected through: it keeps the contact point (`build_node_broadcast_rpc_address`
in its `cluster/cluster_metadata_manager.rs`), and one node has no peers to
learn. So `127.0.0.1:59042` works as is, and `CASSANDRA_BROADCAST_RPC_ADDRESS`
is not needed. `tests/cassandra_tests.rs` checks that the driver addresses the
node by its contact point.

**A restart of the node needs no restart of Helios.** An open session fails
while the node is down and recovers by itself once it is back. Since
`cdrs-tokio` 9.1.0, a control-connection reconnect keeps the known node and
marks it up (`Node::clone_with_node_info`, `cluster/topology/node.rs:376-413`
in 10.0.0), and its pool reconnects. `cdrs-tokio` 8.1.9, used before
2026-11-06, compared the node's gossip address with its contact point
there, marked it `Unknown`, and left the session failing with "No nodes
available in query plan!" until something reopened it.

Checked on 2026-11-06 against a node of this Compose file on host port 59442
(`crates/persistence` `backend.rs`, `restart_tests`). In 10 `docker stop` /
`docker start` runs, health checks passed again 11 to 30 s after the start,
then stayed healthy for 120 s each.

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

# Three-node cluster

Three Cassandra nodes on one host, for the failure tests and anything else
that needs RF 3 (`docs/Cassandra/development-roadmap.md`, item 0.3 and section
6). It is its own Compose project, `hfs-cassandra-cluster`, with its own
containers, network and volumes, so its Compose commands never act on the
single node above or the CI projects. It does share the memory of Docker's VM
with them.

## Run

**Check the memory first.** The default limits add up to 7.5 GiB, and the three
nodes use about 4 GiB even at idle (1.31 to 1.35 GiB each). That comes on top
of every other container in the VM, and a boot that does not fit can take one
of them down: on 2026-11-09, on an 8.8 GiB OrbStack VM with about 3.7 GiB free,
the single node above was OOM-killed while this cluster booted beside it. Add
up the limits of what is running (`docker stats --no-stream`) before you start.
If 7.5 GiB more does not fit in the VM, wait, or stop your own single node
first.

```bash
# Functional checks on 16 and 18 GB laptops (768M heap, 7.5 GiB in all)
docker compose -f docker/cassandra/docker-compose.cluster.yml up -d --wait

# 32 GB machines, and every benchmark that goes in the report (2G heap, 15 GiB in all)
docker compose -f docker/cassandra/docker-compose.cluster.yml \
               -f docker/cassandra/docker-compose.cluster.heap2g.yml up -d --wait
```

Compose starts `cassandra-1`, waits until it answers CQL, then `cassandra-2`,
then `cassandra-3`; a first boot takes 3.5 to 4 minutes. Check the ring, then
create the keyspace at RF 3 and apply the tables:

```bash
c() { docker compose -f docker/cassandra/docker-compose.cluster.yml "$@"; }
c exec cassandra-1 nodetool status                 # three UN, racks 1 to 3
c cp docker/cassandra/keyspace-three-node.cql cassandra-1:/tmp/
c cp docs/Cassandra/schema/schema.cql cassandra-1:/tmp/
c exec cassandra-1 cqlsh -f /tmp/keyspace-three-node.cql
c exec cassandra-1 cqlsh -k helios -f /tmp/schema.cql
c exec cassandra-1 nodetool status helios          # Owns 100.0% on each node
```

Do not apply `keyspace-single-node.cql` here: it creates the keyspace at RF 1,
and its `system_traces` step is for one node only.

Every `docker compose` command for this cluster must name this file, so that
it acts on this project alone. `c down` keeps the data; `c down -v` removes
this project's three volumes and nothing else.

Clients on the host use all three published ports as contact points:

```bash
HFS_CASSANDRA_CONTACT_POINTS=127.0.0.1:59342,127.0.0.1:59343,127.0.0.1:59344
```

`cdrs-tokio` finds the other nodes from whichever it connects to, and addresses
them by their container addresses (`172.29.59.11` to `.13`). OrbStack and
Docker on Linux route those from the host. Docker Desktop for Mac and Windows
does not, so a client on the host there reaches only the node it connected
through; run it in a container on the cluster's network instead.

## Recorded configuration

| Setting | Value | Why |
| --- | --- | --- |
| Image, entrypoint | The same as the single node's | The same `cassandra_latest.yaml`, `paxos_variant: v2` asserted at boot, and every other setting in the table above. |
| Topology | One DC `helios-dc1`; racks `helios-rack1`, `helios-rack2`, `helios-rack3`; cluster `helios`; `GossipingPropertyFileSnitch` | One rack per node, so `NetworkTopologyStrategy` puts one replica in each and `batchlog_endpoint_strategy: dynamic_remote` has a remote rack to choose (ADR-aacruzgon-0003 left this open). A rack cannot be changed once the node has data, so changing one means `down -v`. |
| Seeds | `cassandra-1`, `cassandra-2` | One node stopped still leaves a seed. A seed never bootstraps, so replacing one takes an extra step ("Replacing a node" below). |
| Start order | One at a time, each waiting for the previous node's CQL healthcheck | A node answers CQL only after it has joined the ring. |
| Replication | `'helios-dc1': 3`, in `keyspace-three-node.cql` | The signed-off `schema.cql` has no replication setting; the keyspace is created first and is kept out of that file on purpose (ADR-aacruzgon-0008). |
| Heap | `MAX_HEAP_SIZE` 768M, or 2G with the override | |
| Direct memory | 384M, or 1G with the override | Half the heap, as on the single node. |
| GC threads | 2 parallel and 2 concurrent, or 4 with the override | Matches the CPU limit. |
| CPU limit | 2 per node (6 in all), or 4 per node with the override | |
| Memory limit | 2.5 GiB per node (7.5 GiB in all), or 5 GiB per node with the override; swap disabled | Indicative, Apple-silicon laptop under OrbStack, 768M heap, a 2 GiB limit for the check: 1.31 GiB per node at idle; `memory.peak` 1.47 GiB over the failure procedures and the backend tests; no OOM kill. The 2.5 GiB default then booted from clean and peaked at 1.42 GiB. No sustained load was measured, and the 2G override has not been booted. |
| Host ports | `127.0.0.1:59342`, `59343`, `59344` (`CASSANDRA_CLUSTER_HOST_PORT_1` to `_3` override) | Not 59042 (developers), 59142 (the required CI job) or 59242 (coverage and FHIR versions). |
| Network | Fixed addresses `172.29.59.11` to `.13` (`CASSANDRA_CLUSTER_SUBNET_PREFIX` overrides `172.29.59`) | A node keeps its address over a restart and a reconnect, and the seed list can name addresses. |
| `stop_grace_period` | 60 s | Long enough for a drain on `SIGTERM`. |

## Backend tests against the cluster

```bash
HFS_TEST_CASSANDRA_URL=127.0.0.1:59342 \
  cargo test -p helios-persistence --features cassandra --test cassandra_tests
```

These tests were written for one node, and on three a few fail for reasons
that are expected today:

- `node_runs_the_docker_cassandra_configuration` always fails. It fails at the
  rack assertion (`cassandra_tests.rs:185`) if the query reaches another node,
  and otherwise at `one node, no peers` (`:214`), because on the cluster the
  driver sees three nodes.
The concurrent-write tests, those in `step3_suites`
(`versioned_write_race_suite.rs`) and
`create_read::cassandra_concurrent_creates_of_one_id_make_exactly_one_resource`,
can get a backend `Timeout` (504) from a `LOCAL_SERIAL` read: `CAS operation
timed out: received 0 of 2 required responses`. Cassandra 5.0.9 raises it when
a competing proposal supersedes the read's Paxos round, and it can reach the
writer that won. On RF 3 this is an accepted answer (issue #77,
ADR-aacruzgon-0072 decision 4), so these tests read the keyspace's replication
factor and, at RF 2 or more, accept the 504 and check the invariants instead:
no history gap, one winner per version, no lost write. Each prints its mode,
`[race policy case=race:<test> rf=3 accept_504=true]`. A 504'd write that
applied can leave its version out of the history and `count` one off until the
sweeper replays its intent (Step 5), so the history may lack its newest version
only when the `resources` row's `write_id` matches a kept intent row for it
(ADR-sara-lahourpour-0001). At RF 1 the 504 still fails the test.

### Docker Desktop for Windows and Mac

From the host, every test fails after 60 s (the harness's `READY_TIMEOUT`):
`SELECT table_name FROM system_schema.tables …: no answer within 60s`. The
driver learns the other nodes' `172.29.59.x` addresses from
`system.peers_v2`, and Docker Desktop does not route those from the host
(including WSL 2). Build the test binary on the host and run it in a container
on the cluster's network instead. From the repository root, on Ubuntu under
WSL 2, with an image of the same Ubuntu release as the distro, so that its
glibc matches the binary's (`ubuntu:26.04` here):

```bash
bin=$(cargo test -p helios-persistence --features cassandra --test cassandra_tests --no-run 2>&1 \
      | sed -n 's/^ *Executable .* (\(.*\))$/\1/p')
docker run --rm --network hfs-cassandra-cluster_cluster --user "$(id -u):$(id -g)" \
  -v /usr/lib/x86_64-linux-gnu/libssl.so.3:/usr/lib/x86_64-linux-gnu/libssl.so.3:ro \
  -v /usr/lib/x86_64-linux-gnu/libcrypto.so.3:/usr/lib/x86_64-linux-gnu/libcrypto.so.3:ro \
  -v "$PWD":"$PWD":ro -v "$bin":/t/cassandra_tests:ro -w "$PWD/crates/persistence" \
  -e HFS_TEST_CASSANDRA_URL=172.29.59.11:9042 ubuntu:26.04 /t/cassandra_tests
```

The repository is mounted at the same path because some tests read files by
the path compiled into the binary. The two OpenSSL libraries come from the
host, so the image needs nothing installed. On 2026-11-12 and 13 (Windows,
Docker Desktop with WSL 2, the default limits), the tests passed this way
except `node_runs_the_docker_cassandra_configuration`; one of five runs also
had four ordinary `LOCAL_QUORUM` timeouts (`received only 1 responses`) in
non-race tests, while the host was compiling.

## Failure procedures

Run these only on this project. The commands use the `c` function from "Run".
Container names are `hfs-cassandra-cluster-cassandra-N-1`.

**One node down, for less than `max_hint_window` (3 h).** `LOCAL_QUORUM` reads
and writes and LWTs at `LOCAL_SERIAL` carry on, from either of the other two
nodes.

```bash
c stop cassandra-3
c exec cassandra-1 nodetool status                 # DN for the stopped node
c start cassandra-3
# c start returns before the node listens; nodetool is refused for about 20 s
until [ "$(docker inspect --format '{{.State.Health.Status}}' hfs-cassandra-cluster-cassandra-3-1)" = healthy ]; do sleep 2; done
c exec cassandra-3 nodetool status                 # back to UN
c exec cassandra-3 nodetool repair                 # at once, within 3 h of the stop
```

The operations rules are in section 6 of the roadmap: a node down longer than
3 h is replaced (`replace_address_first_boot`), never restarted, and the
replacement is repaired once it has bootstrapped.

**Replacing a node.** Use `cassandra-3` when the test lets you choose. It is the
only node that is not a seed, so it needs no extra step. Put the flag in an
override file outside the repository, here `/tmp/replace.yml`, and keep the GC
settings of the profile in use:

```yaml
services:
  cassandra-3:
    environment:
      JVM_EXTRA_OPTS: -XX:ParallelGCThreads=2 -XX:ConcGCThreads=2 -Dcassandra.replace_address_first_boot=172.29.59.13
```

```bash
c stop cassandra-3
docker rm -f hfs-cassandra-cluster-cassandra-3-1
docker volume rm hfs-cassandra-cluster_cassandra-3-data
c exec cassandra-1 nodetool status                 # DN for the replaced node
c -f /tmp/replace.yml up -d --no-deps --wait cassandra-3
c exec cassandra-3 nodetool repair
```

On a cluster started with the 2G override, `c` names only the default file, so
this `up` would bring the node back at 2 CPUs, a 768M heap and 2.5 GiB. Name the
override before the replace file. The replace file sets all of `JVM_EXTRA_OPTS`,
so also set both GC options in it to 4, or the node runs 2 GC threads on 4 CPUs:

```bash
c -f docker/cassandra/docker-compose.cluster.heap2g.yml -f /tmp/replace.yml up -d --no-deps --wait cassandra-3
# The seed route's recreate step, below, has no replace file but needs the override:
c -f docker/cassandra/docker-compose.cluster.heap2g.yml up -d --no-deps --wait cassandra-2
```

`cassandra-1` and `cassandra-2` are seeds, and a seed never bootstraps. With the
same steps, a replacement seed exits with code 3 (`Replacing a node without
bootstrapping risks invalidating consistency guarantees ...`). To replace one,
take it off its own seed list for that first boot by also setting
`CASSANDRA_SEEDS` in the override to the other seed alone, for `cassandra-2`
`172.29.59.11`, with that seed up. The node then bootstraps. Afterwards, recreate
it from the normal file (`c up -d --no-deps --wait cassandra-2`), where the
replace flag is gone and the node, already bootstrapped, keeps its data.

This seed route was run on 2026-11-09, once on each seed, at a 512M heap, 256M
of direct memory and 1700 MiB per node, with 300 rows at RF 3. `cassandra-2`
was healthy in 65 s, and `cassandra-1` in 76 s on the volume a refused first
attempt had left. Each held the rows before any repair, and kept them when
recreated from the normal file and repaired.

Do not use `-Dcassandra.allow_unsafe_replace=true`, which the error message
offers. It lets the seed join on the dead node's tokens with no data and no
streaming, so it answers reads for ranges it does not hold until a repair ends.
Section 6 requires a replacement that bootstraps from the others.

**Coordinator isolated.** `docker network disconnect` takes the node off the
cluster network, and off its published host port too, so run the client inside
the container.

```bash
NODE=hfs-cassandra-cluster-cassandra-1-1
docker network disconnect hfs-cassandra-cluster_cluster $NODE
docker exec $NODE cqlsh -e "CONSISTENCY LOCAL_QUORUM; INSERT INTO ..."   # fails
docker network connect --ip 172.29.59.11 hfs-cassandra-cluster_cluster $NODE
docker exec $NODE nodetool status                   # all UN within seconds
docker exec $NODE nodetool repair
```

Pass `--ip`, or the node comes back with another address. On the isolated node
a statement first fails with **`WriteTimeout`** (code 1100), for as long as the
failure detector takes to convict its peers, and only then with
**`Unavailable`** (code 1000):

| Statement on the isolated node | `WriteTimeout` | Then `Unavailable` |
| --- | --- | --- |
| Plain write at `LOCAL_QUORUM` | 2 s each (`write_request_timeout`), for about 20 s | At once |
| LWT at `LOCAL_SERIAL` | 1 s each (`cas_contention_timeout`), for about 18 s | At once |

A plain write that timed out can still take effect: it was applied on the
isolated node's own replica and came back after the reconnect. An LWT that timed
out has an unknown outcome too; the two checked after the reconnect had not
applied. The other two nodes keep serving throughout.
