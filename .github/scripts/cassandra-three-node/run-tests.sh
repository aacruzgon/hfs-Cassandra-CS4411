#!/usr/bin/env bash
# Runs one already-built test binary against the three-node cluster, from a
# container on the cluster's own network (.github/workflows/cassandra-three-node.yml).
#
#   run-tests.sh LABEL SUITE BINARY [LIBTEST ARGS...]
#
# Why a container: the runners' Docker is rootless with slirp4netns
# (ADR-sam-michaels-0004, ADR-aacruzgon-0023), so the nodes' addresses on the
# cluster network are not reachable from the runner itself, only their
# published ports are. The driver learns every node's address from
# system.peers_v2 and routes requests to all three (session.rs,
# TopologyAwareLoadBalancingStrategy with LocalDcOnly), so a client on the
# runner would reach node 1 only. On the cluster network it reaches all three,
# as docker/cassandra/README.md describes for Docker Desktop.
#
# The container is the cluster's own pinned Cassandra image (Debian 13, glibc
# 2.41, libssl3), which the runner already has. The checkout is mounted
# read-only and the target directory read-write at their own paths, so the
# paths compiled into the binary (env!("CARGO_MANIFEST_DIR")) still resolve.
#
# Writes, under $THREE_NODE_OUT:
#   logs/LABEL.log          the libtest output, after a cargo-style "Running" line
#                           so ci-report.py's test parser reads it
#   run/LABEL.json          exit status, wall time, the container's memory.peak
#                           and OOM kills, the timeout; or "skipped": "deadline"
#                           for a run not started (below)
#   run/LABEL/coordinator-before.tsv, coordinator-after.tsv
#                           every node's ClientRequest timeouts, unavailables
#                           and failures just before and just after the run
#                           (coordinator-metrics.sh), so report.py can count
#                           the 504s and 503s the nodes answered during it,
#                           including those a test accepted and passed on
#
# Exits with the test binary's status: 124 when the timeout stopped it, 137
# when it was killed (the timeout's SIGKILL, 30 s after its SIGINT, or the
# kernel's OOM killer; run/LABEL.json's oom_kills tells which). The workflow
# step is continue-on-error: the job reports failures, it does not fail on
# them.
#
# THREE_NODE_DEADLINE (epoch seconds, optional): the step's own deadline. The
# timeout is cut so the container ends a minute before it, less the after-
# reading's worst case (90 s), and a run with under two minutes left is not
# started (exit 125; its log and run file say so). So the step's
# timeout-minutes never has to kill a docker client, or a nodetool reading,
# and leave a container running or the run file unwritten.
set -uo pipefail

label=$1 suite=$2 binary=$3
shift 3

: "${THREE_NODE_OUT:?}" "${THREE_NODE_NETWORK:?}" "${THREE_NODE_IMAGE:?}" "${THREE_NODE_CONTACT:?}"
: "${GITHUB_WORKSPACE:?}" "${CARGO_TARGET_DIR:?}"
timeout_min=${THREE_NODE_TEST_TIMEOUT_MIN:-35}
timeout_s=$(( timeout_min * 60 ))
memory=${THREE_NODE_TEST_MEMORY:-6g}
name="hfs-cassandra-3n-tests-$label"

mkdir -p "$THREE_NODE_OUT/logs" "$THREE_NODE_OUT/run" "$THREE_NODE_OUT/run/$label"
log="$THREE_NODE_OUT/logs/$label.log"
run_dir="$THREE_NODE_OUT/run/$label"

# The nodes' counters before the run, ahead of the deadline check so the
# time they take (three nodetool JVMs per node) is not the run's.
here=$(cd "$(dirname "$0")" && pwd)
"$here/coordinator-metrics.sh" "$run_dir/coordinator-before.tsv"

# A repeat not started still gets its log (the "Running" line, so both
# reports list it) and a run file saying why, so report.py shows it as not
# run instead of leaving it out.
# The after-reading's worst case (coordinator-metrics.sh: 3 nodes x 3 calls x
# 10 s) is reserved too, so a hung nodetool after the run still ends before
# the step's own timeout.
readings_s=90
if [ -n "${THREE_NODE_DEADLINE:-}" ]; then
  left=$(( THREE_NODE_DEADLINE - $(date +%s) - 60 - readings_s ))
  if [ "$left" -lt 120 ]; then
    echo "::warning::$label not run: ${left} s left before the step's deadline"
    { echo "     Running $suite ($binary)"
      echo "not run (run-tests.sh): ${left} s left before the step's deadline"; } > "$log"
    python3 -c 'import json, sys; l, s, b, left = sys.argv[2:]; json.dump({"label": l, "suite": s, "binary": b,
"skipped": "deadline", "left_s": int(left)}, open(sys.argv[1], "w"), indent=2)' \
      "$THREE_NODE_OUT/run/$label.json" "$label" "$suite" "$binary" "$left"
    exit 125
  fi
  [ "$left" -lt "$timeout_s" ] && timeout_s=$left
fi

# A container of an earlier, killed run.
docker rm -f "$name" >/dev/null 2>&1 || true

# On a signal sent to this script itself (SIGINT, SIGTERM, SIGHUP), remove
# the container: docker run's client does not stop it (its PID 1 is sh, which
# ignores the forwarded SIGINT). docker run runs in the background and the
# script waits for it, so the trap runs as soon as the signal arrives.
# A GitHub cancel does not reach this script: the runner signals only the
# step's shell (actions/runner, ProcessInvoker), which waits for its
# foreground child. What covers a cancel is the workflow's always() step
# "Stop the cluster", which removes every hfs-cassandra-3n-tests-* container,
# and what prevents a step timeout is THREE_NODE_DEADLINE, above.
# shellcheck disable=SC2329  # called from the traps below
remove_container() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap 'remove_container; exit 130' INT
trap 'remove_container; exit 143' TERM HUP

echo "     Running $suite ($binary)" > "$log"
echo "Running $binary $* in $name on $THREE_NODE_NETWORK, HFS_TEST_CASSANDRA_URL=$THREE_NODE_CONTACT, timeout ${timeout_s} s, memory $memory"
started=$(date +%s)
# Inside: the binary under coreutils timeout (SIGINT first, SIGKILL 30 s
# later), then the container's own cgroup figures, read after the tests and
# before the container exits. The cgroup is the container's (cgroup v2, a
# private cgroup namespace by default), so memory.peak is the tests' peak.
{ docker run --rm --name "$name" \
  --network "$THREE_NODE_NETWORK" \
  --memory "$memory" --memory-swap "$memory" \
  -v "$GITHUB_WORKSPACE:$GITHUB_WORKSPACE:ro" \
  -v "$CARGO_TARGET_DIR:$CARGO_TARGET_DIR" \
  -v "$run_dir:/out" \
  -w "$GITHUB_WORKSPACE/crates/persistence" \
  -e HFS_TEST_CASSANDRA_URL="$THREE_NODE_CONTACT" \
  -e CARGO_MANIFEST_DIR="$GITHUB_WORKSPACE/crates/persistence" \
  -e CARGO_PKG_NAME=helios-persistence \
  -e CARGO_TARGET_TMPDIR="$CARGO_TARGET_DIR/tmp" \
  -e RUST_BACKTRACE=0 \
  -e TIMEOUT_S="$timeout_s" \
  --entrypoint sh "$THREE_NODE_IMAGE" -c '
    ldd "$1" | grep -E "not found" && echo "missing shared libraries for $1"
    timeout -s INT -k 30s "${TIMEOUT_S}s" "$@"
    status=$?
    echo "$status" > /out/status
    cat /sys/fs/cgroup/memory.peak > /out/memory.peak 2>/dev/null
    cat /sys/fs/cgroup/memory.events > /out/memory.events 2>/dev/null
    exit "$status"
  ' sh "$binary" "$@"; echo "$?" > "$run_dir/docker_status"; } 2>&1 | tee -a "$log" &
wait "$!"
docker_status=$(cat "$run_dir/docker_status" 2>/dev/null || echo "")
wall=$(( $(date +%s) - started ))

status=$(cat "$run_dir/status" 2>/dev/null || echo "$docker_status")
peak=$(cat "$run_dir/memory.peak" 2>/dev/null || echo "")
oom=$(awk '$1 == "oom_kill" {print $2}' "$run_dir/memory.events" 2>/dev/null || echo "")
python3 - "$THREE_NODE_OUT/run/$label.json" "$label" "$suite" "$binary" "$status" "$docker_status" \
  "$wall" "$timeout_s" "$memory" "$peak" "$oom" "$@" <<'EOF'
import json, sys
out, label, suite, binary, status, docker_status, wall, timeout_s, memory, peak, oom, *args = sys.argv[1:]
num = lambda s: int(s) if s.strip().lstrip("-").isdigit() else None
json.dump({
    "label": label, "suite": suite, "binary": binary, "args": args,
    "exit_status": num(status), "docker_status": num(docker_status), "wall_s": num(wall),
    # coreutils timeout exits 124 when it stopped the binary with SIGINT. 137
    # is a SIGKILL: the timeout's, 30 s after an ignored SIGINT, or the OOM
    # killer's (oom_kills > 0). Only 124 is reported as a timeout.
    "timeout_s": num(timeout_s), "timed_out": status.strip() == "124",
    "killed": status.strip() == "137",
    "memory_limit": memory, "memory_peak_bytes": num(peak), "oom_kills": num(oom),
}, open(out, "w"), indent=2)
EOF
echo "$label: exit $status after ${wall} s; container memory.peak ${peak:-unknown} bytes; OOM kills ${oom:-unknown}"
# After the run file, so a slow reading cannot cost it.
"$here/coordinator-metrics.sh" "$run_dir/coordinator-after.tsv"
exit "$status"
