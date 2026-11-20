#!/usr/bin/env bash
# The three-node job's pause scenarios: raw CQL from cqlsh on node 1, with one
# or two replicas paused (docker pause), at LOCAL_QUORUM on the RF 3 keyspace.
# No HFS code runs. Modelled on the PR #90 review's 3n-run.sh and 3n-cqlsh.sh
# (2026-11-19, acruzdebian), with the writer's batch replaced by the same
# statements in CQL.
#
#   pause-scenarios.sh OUT_DIR
#
# Scenarios (each with its own tenant, so none reads another's rows):
#   s1   one-partition logged batch (insert v2, delete v1 in (tenant, Patient),
#        the shape of an update's index delta), node 2 paused, sent at once
#   s2   the same batch, nodes 2 and 3 paused, sent at once (still UN on node 1)
#   s3   the same batch, nodes 2 and 3 paused and marked DN on node 1
#   s4a  two-partition logged batch (Patient and Observation), node 2 paused,
#        sent at once (still UN)
#   s4b  the same, node 2 paused and marked DN
# What each should give, from the review's runs: s1 success; s2 WriteTimeout
# SIMPLE, received 1, block_for 2, applied on the coordinator; s3 Unavailable,
# required 2, alive 1, not applied; s4a WriteTimeout BATCH_LOG, received 1,
# applied later by a batchlog replay; s4b success. These are recorded, not
# asserted: a scenario that differs is a finding for the report.
#
# nodetool runs with a 128m heap (its own default): docker exec inherits the
# node's MAX_HEAP_SIZE=512M, and every nodetool call is a JVM in the node's
# 1700 MiB cgroup.
#
# Node 1 is always the coordinator (cqlsh inside it, contact point itself),
# so a paused node is never the coordinator. Rows are read back at ALL after
# every node is UN again and 25 s have passed (batchlog replay runs every
# 10 s; hints are delivered on UN).
#
# Writes OUT_DIR/<id>.out (cqlsh's output), OUT_DIR/<id>.meta (key=value),
# OUT_DIR/<id>.read (the read at ALL), OUT_DIR/timeline.log and
# OUT_DIR/status-*.txt (nodetool status at each step). Always unpauses every
# node on exit, also when it fails or is cancelled. Exits 0 unless the
# cluster could not be brought back to three UN nodes.
set -uo pipefail

out=${1:?usage: pause-scenarios.sh OUT_DIR}
: "${THREE_NODE_PROJECT:?}" "${CASSANDRA_CLUSTER_SUBNET_PREFIX:?}"
mkdir -p "$out"
N1=$THREE_NODE_PROJECT-cassandra-1-1
N2=$THREE_NODE_PROJECT-cassandra-2-1
N3=$THREE_NODE_PROJECT-cassandra-3-1
IP2=$CASSANDRA_CLUSTER_SUBNET_PREFIX.12
IP3=$CASSANDRA_CLUSTER_SUBNET_PREFIX.13
run_id=${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-1}

unpause_all() { docker unpause "$N2" "$N3" >/dev/null 2>&1 || true; docker unpause "$N2" >/dev/null 2>&1 || true; docker unpause "$N3" >/dev/null 2>&1 || true; }
trap unpause_all EXIT

ms() { date +%s%3N; }
ts() { date -u +%T.%3N; }
log() { echo " $(ts) $*" | tee -a "$out/timeline.log"; }
status() { docker exec -e MAX_HEAP_SIZE=128m "$N1" nodetool status 2>/dev/null | grep -E '^(UN|DN|UJ|DJ|UL|DL|UM|DM)' | awk '{print $1, $2}'; }
snapshot() { status > "$out/status-$1.txt"; sed 's/^/   /' "$out/status-$1.txt" | tee -a "$out/timeline.log"; }
# wait_down IP: until node 1 reports IP as DN; prints the seconds it took.
wait_down() {
  local start; start=$(ms)
  for _ in $(seq 1 90); do
    if status | grep -q "^DN $1\$"; then echo $(( ($(ms) - start) / 1000 )); return 0; fi
    sleep 2
  done
  echo "timeout"; return 1
}
wait_up() {
  for _ in $(seq 1 90); do
    [ "$(status | grep -c '^UN ')" = 3 ] && return 0
    sleep 2
  done
  return 1
}
cql() { docker exec -i "$N1" cqlsh --request-timeout 30 -k helios 2>&1; }

# The one-partition batch: the update delta's two statements on (tenant,
# Patient), at one write timestamp, as the writer sends it (#90).
one_partition() { # tenant
  echo "CONSISTENCY LOCAL_QUORUM;
BEGIN BATCH USING TIMESTAMP 2000001
INSERT INTO resources_by_type (tenant_id, resource_type, last_updated, id, version_id, fhir_version) VALUES ('$1', 'Patient', 2000, 'p', 2, '4.0');
DELETE FROM resources_by_type WHERE tenant_id = '$1' AND resource_type = 'Patient' AND last_updated = 1000 AND id = 'p';
APPLY BATCH;"
}
two_partitions() { # tenant
  echo "CONSISTENCY LOCAL_QUORUM;
BEGIN BATCH USING TIMESTAMP 2000001
INSERT INTO resources_by_type (tenant_id, resource_type, last_updated, id, version_id, fhir_version) VALUES ('$1', 'Patient', 2000, 'p', 1, '4.0');
INSERT INTO resources_by_type (tenant_id, resource_type, last_updated, id, version_id, fhir_version) VALUES ('$1', 'Observation', 2000, 'o', 1, '4.0');
APPLY BATCH;"
}
seed() { # tenant: row v1 at last_updated 1000, all nodes up
  echo "CONSISTENCY LOCAL_QUORUM;
INSERT INTO resources_by_type (tenant_id, resource_type, last_updated, id, version_id, fhir_version) VALUES ('$1', 'Patient', 1000, 'p', 1, '4.0') USING TIMESTAMP 1000001;" | cql > "$out/seed-$1.out"
}

tenant_of() { echo "ci-3n-$run_id-$1"; }

# send ID KIND PAUSED STATE DESCRIPTION EXPECTED: one batch from node 1.
send() {
  local id=$1 kind=$2 paused=$3 state=$4 desc=$5 expected=$6
  local tenant start took
  tenant=$(tenant_of "$id")
  start=$(ms)
  if [ "$kind" = one ]; then one_partition "$tenant"; else two_partitions "$tenant"; fi | cql > "$out/$id.out"
  took=$(( $(ms) - start ))
  {
    echo "id=$id"; echo "kind=$kind"; echo "paused=$paused"; echo "state=$state"
    echo "description=$desc"; echo "expected=$expected"; echo "took_ms=$took"; echo "tenant=$tenant"
  } > "$out/$id.meta"
  log "$id ($desc): $(grep -v '^Consistency level' "$out/$id.out" | grep -v '^$' | head -1 | cut -c1-200) [${took} ms]"
}

read_all() { # ID: the scenario's rows at ALL
  local tenant; tenant=$(tenant_of "$1")
  docker exec "$N1" cqlsh --request-timeout 30 -k helios -e "CONSISTENCY ALL; SELECT JSON resource_type, toUnixTimestamp(last_updated) AS lu, id, version_id, WRITETIME(version_id) AS w FROM resources_by_type WHERE tenant_id = '$tenant' AND resource_type IN ('Patient', 'Observation');" \
    > "$out/$1.read" 2>&1
}

echo "=== pause scenarios, run $run_id, $(ts) UTC, write_request_timeout $(docker exec -e MAX_HEAP_SIZE=128m "$N1" nodetool gettimeout write 2>/dev/null | tail -1)" | tee "$out/timeline.log"
wait_up || { log "the cluster is not three UN nodes; no scenario run"; snapshot start; exit 1; }
snapshot start
# The one-partition scenarios' row v1, written while every node is up.
for id in s1 s2 s3; do seed "$(tenant_of "$id")"; done
log "seeded v1 for s1, s2 and s3"

# Window 1: node 2 paused.
docker pause "$N2" >/dev/null; log "paused node 2 ($IP2)"; snapshot w1-paused
send s1 one "node 2" "UN (at once)" "one-partition batch, one replica paused" "success"
send s4a two "node 2" "UN (at once)" "two-partition batch, one replica paused" "WriteTimeout BATCH_LOG, received 1, block_for 2; applied later"
d=$(wait_down "$IP2"); log "node 1 marked node 2 DN after ${d} s more"; echo "w1_down_s=$d" >> "$out/windows.meta"; snapshot w1-down
send s4b two "node 2" "DN" "two-partition batch, one replica marked down" "success"
docker unpause "$N2" >/dev/null
if wait_up; then log "unpaused node 2, 3 UN"; else log "node 2 did not come back UN"; fi
snapshot w1-up

# Window 2: nodes 2 and 3 paused.
docker pause "$N2" "$N3" >/dev/null; log "paused nodes 2 and 3 ($IP2, $IP3)"; snapshot w2-paused
send s2 one "nodes 2 and 3" "UN (at once)" "one-partition batch, two replicas paused" "WriteTimeout SIMPLE, received 1, block_for 2; applied on the coordinator"
d2=$(wait_down "$IP2"); d3=$(wait_down "$IP3"); log "node 1 marked nodes 2 and 3 DN (${d2} s, then ${d3} s more)"
echo "w2_down_s=$d2+$d3" >> "$out/windows.meta"; snapshot w2-down
send s3 one "nodes 2 and 3" "DN" "one-partition batch, two replicas marked down" "Unavailable, required 2, alive 1; not applied"
docker unpause "$N2" "$N3" >/dev/null
ok=0; wait_up && ok=1
if [ $ok = 1 ]; then log "unpaused nodes 2 and 3, 3 UN"; else log "nodes 2 and 3 did not come back UN"; fi
snapshot w2-up

sleep 25
log "25 s later (batchlog replay every 10 s, hints): reading every scenario's rows at ALL"
for id in s1 s2 s3 s4a s4b; do read_all "$id"; done
snapshot end
echo "=== end $(ts)" | tee -a "$out/timeline.log"
[ $ok = 1 ]
