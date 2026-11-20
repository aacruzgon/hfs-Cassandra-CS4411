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
# Each scenario's state is checked, not assumed. A batch sent "at once"
# counts only if node 1 still reports the paused nodes UN in the status taken
# right after it: a paused node cannot gossip, so it goes from UN to DN and
# not back, and UN after the batch means UN while it was sent. Otherwise its
# .meta says invalid=. A "DN" batch is sent only once node 1 reports the
# paused nodes DN; if that is not seen within the wait (or nodetool fails),
# the batch is not sent and its .meta says not_run=. The CI runs so far sent
# s4a about 5 s after the pause and saw node 2 DN about 18 s after it. So is
# a scenario whose v1 row was not seeded (s1 to s3), or whose window's
# docker pause failed.
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

# scenario VERB ID [REASON]: VERB (send or not_sent) with scenario ID's
# fixed fields.
scenario() {
  local verb=$1 id=$2; shift 2
  case $id in
    s1) set -- one "node 2" "UN (at once)" "one-partition batch, one replica paused" "success" "$@" ;;
    s4a) set -- two "node 2" "UN (at once)" "two-partition batch, one replica paused" "WriteTimeout BATCH_LOG, received 1, block_for 2; applied later" "$@" ;;
    s4b) set -- two "node 2" "DN" "two-partition batch, one replica marked down" "success" "$@" ;;
    s2) set -- one "nodes 2 and 3" "UN (at once)" "one-partition batch, two replicas paused" "WriteTimeout SIMPLE, received 1, block_for 2; applied on the coordinator" "$@" ;;
    s3) set -- one "nodes 2 and 3" "DN" "one-partition batch, two replicas marked down" "Unavailable, required 2, alive 1; not applied" "$@" ;;
  esac
  "$verb" "$id" "$@"
}

# run_scenario ID: send ID's batch, unless its v1 row was not seeded.
run_scenario() {
  case " $unseeded " in
    *" $1 "*) scenario not_sent "$1" "its v1 row was not seeded (seed-$(tenant_of "$1").out)" ;;
    *) scenario send "$1" ;;
  esac
}

# seeded ID: cqlsh ran the seed (its consistency line) and printed no error;
# the caller also checks that it exited 0.
seeded() {
  local f; f="$out/seed-$(tenant_of "$1").out"
  grep -q '^Consistency level set' "$f" && ! grep -qE 'Error|Exception|NoHostAvailable' "$f"
}

# not_sent ID KIND PAUSED STATE DESCRIPTION EXPECTED REASON: the scenario's
# .meta, without sending its batch.
not_sent() {
  {
    echo "id=$1"; echo "kind=$2"; echo "paused=$3"; echo "state=$4"
    echo "description=$5"; echo "expected=$6"; echo "not_run=$7"; echo "tenant=$(tenant_of "$1")"
  } > "$out/$1.meta"
  log "$1 ($5): not sent: $7"
}

# confirm_un SNAPSHOT "IP..." ID...: after the batches sent "at once", node 1
# must still report every paused IP UN in status-SNAPSHOT.txt; otherwise each
# ID is invalid.
confirm_un() {
  local snap=$1 ips=$2 seen="" ok=1 ip s id; shift 2
  for ip in $ips; do
    s=$(awk -v ip="$ip" '$2 == ip {print $1}' "$out/status-$snap.txt")
    [ "$s" = UN ] || ok=0
    seen="${seen:+$seen, }$ip ${s:-not listed}"
  done
  for id in "$@"; do
    echo "seen_after=$seen" >> "$out/$id.meta"
    if [ "$ok" = 0 ]; then
      echo "invalid=node 1 reported $seen right after the batch, so it is not known to have been sent while node 1 saw them UN" >> "$out/$id.meta"
      log "$id: invalid, node 1 reported $seen right after it"
    fi
  done
}

read_all() { # ID: the scenario's rows at ALL
  local tenant; tenant=$(tenant_of "$1")
  docker exec "$N1" cqlsh --request-timeout 30 -k helios -e "CONSISTENCY ALL; SELECT JSON resource_type, toUnixTimestamp(last_updated) AS lu, id, version_id, WRITETIME(version_id) AS w FROM resources_by_type WHERE tenant_id = '$tenant' AND resource_type IN ('Patient', 'Observation');" \
    > "$out/$1.read" 2>&1
}

echo "=== pause scenarios, run $run_id, $(ts) UTC, write_request_timeout $(docker exec -e MAX_HEAP_SIZE=128m "$N1" nodetool gettimeout write 2>/dev/null | tail -1)" | tee "$out/timeline.log"
wait_up || { log "the cluster is not three UN nodes; no scenario run"; snapshot start; exit 1; }
snapshot start
# The one-partition scenarios' row v1, written while every node is up. A
# scenario whose seed failed is not sent (run_scenario).
unseeded=""
for id in s1 s2 s3; do
  seed "$(tenant_of "$id")"; status=$?
  if [ "$status" != 0 ] || ! seeded "$id"; then
    unseeded="$unseeded $id"
    log "the seed of $id failed (exit $status): $(head -c 200 "$out/seed-$(tenant_of "$id").out")"
  fi
done
log "seeded v1 for s1, s2 and s3${unseeded:+, except$unseeded}"

# Window 1: node 2 paused. The status snapshot comes after the two batches
# sent at once, so it does not delay them, and confirms their state.
if docker pause "$N2" >/dev/null; then
  log "paused node 2 ($IP2)"
  run_scenario s1
  run_scenario s4a
  snapshot w1-sent; confirm_un w1-sent "$IP2" s1 s4a
  if d=$(wait_down "$IP2"); then
    log "node 1 marked node 2 DN after ${d} s more"; snapshot w1-down
    run_scenario s4b
  else
    log "node 1 did not report node 2 DN within the wait"; snapshot w1-down
    scenario not_sent s4b "node 1 did not report node 2 DN within the wait (or nodetool status failed)"
  fi
  echo "w1_down_s=$d" >> "$out/windows.meta"
  docker unpause "$N2" >/dev/null
else
  log "docker pause of node 2 failed"
  for id in s1 s4a s4b; do scenario not_sent "$id" "docker pause of node 2 failed"; done
  echo "w1_down_s=not paused" >> "$out/windows.meta"
fi
if wait_up; then log "node 2 back, 3 UN"; else log "node 2 did not come back UN"; fi
snapshot w1-up

# Window 2: nodes 2 and 3 paused. A pause that failed for one node is undone.
if docker pause "$N2" "$N3" >/dev/null; then
  log "paused nodes 2 and 3 ($IP2, $IP3)"
  run_scenario s2
  snapshot w2-sent; confirm_un w2-sent "$IP2 $IP3" s2
  d3=skipped
  if d2=$(wait_down "$IP2") && d3=$(wait_down "$IP3"); then
    log "node 1 marked nodes 2 and 3 DN (${d2} s, then ${d3} s more)"; snapshot w2-down
    run_scenario s3
  else
    log "node 1 did not report nodes 2 and 3 DN within the wait"; snapshot w2-down
    scenario not_sent s3 "node 1 did not report nodes 2 and 3 DN within the wait (or nodetool status failed)"
  fi
  # "13+16", "timeout" (node 2 never DN, node 3 not waited for) or "13+timeout".
  if [ "$d3" = skipped ]; then echo "w2_down_s=$d2" >> "$out/windows.meta"; else echo "w2_down_s=$d2+$d3" >> "$out/windows.meta"; fi
else
  unpause_all
  log "docker pause of nodes 2 and 3 failed"
  for id in s2 s3; do scenario not_sent "$id" "docker pause of nodes 2 and 3 failed"; done
  echo "w2_down_s=not paused" >> "$out/windows.meta"
fi
unpause_all
ok=0; wait_up && ok=1
if [ $ok = 1 ]; then log "unpaused nodes 2 and 3, 3 UN"; else log "nodes 2 and 3 did not come back UN"; fi
snapshot w2-up

sleep 25
log "25 s later (batchlog replay every 10 s, hints): reading every scenario's rows at ALL"
for id in s1 s2 s3 s4a s4b; do read_all "$id"; done
snapshot end
echo "=== end $(ts)" | tee -a "$out/timeline.log"
[ $ok = 1 ]
