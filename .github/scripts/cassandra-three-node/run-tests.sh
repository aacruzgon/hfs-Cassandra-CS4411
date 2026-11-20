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
#                           and OOM kills, the timeout
#
# Exits with the test binary's status (124 when the timeout stopped it). The
# workflow step is continue-on-error: the job reports failures, it does not
# fail on them.
set -uo pipefail

label=$1 suite=$2 binary=$3
shift 3

: "${THREE_NODE_OUT:?}" "${THREE_NODE_NETWORK:?}" "${THREE_NODE_IMAGE:?}" "${THREE_NODE_CONTACT:?}"
: "${GITHUB_WORKSPACE:?}" "${CARGO_TARGET_DIR:?}"
timeout_min=${THREE_NODE_TEST_TIMEOUT_MIN:-35}
memory=${THREE_NODE_TEST_MEMORY:-6g}
name="hfs-cassandra-3n-tests-$label"

mkdir -p "$THREE_NODE_OUT/logs" "$THREE_NODE_OUT/run" "$THREE_NODE_OUT/run/$label"
log="$THREE_NODE_OUT/logs/$label.log"
run_dir="$THREE_NODE_OUT/run/$label"

# A container of an earlier, killed run.
docker rm -f "$name" >/dev/null 2>&1 || true

echo "     Running $suite ($binary)" > "$log"
echo "Running $binary $* in $name on $THREE_NODE_NETWORK, HFS_TEST_CASSANDRA_URL=$THREE_NODE_CONTACT, timeout ${timeout_min} min, memory $memory"
started=$(date +%s)
# Inside: the binary under coreutils timeout (SIGINT first, SIGKILL 30 s
# later), then the container's own cgroup figures, read after the tests and
# before the container exits. The cgroup is the container's (cgroup v2, a
# private cgroup namespace by default), so memory.peak is the tests' peak.
docker run --rm --name "$name" \
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
  -e TIMEOUT_MIN="$timeout_min" \
  --entrypoint sh "$THREE_NODE_IMAGE" -c '
    ldd "$1" | grep -E "not found" && echo "missing shared libraries for $1"
    timeout -s INT -k 30s "${TIMEOUT_MIN}m" "$@"
    status=$?
    echo "$status" > /out/status
    cat /sys/fs/cgroup/memory.peak > /out/memory.peak 2>/dev/null
    cat /sys/fs/cgroup/memory.events > /out/memory.events 2>/dev/null
    exit "$status"
  ' sh "$binary" "$@" 2>&1 | tee -a "$log"
docker_status=${PIPESTATUS[0]}
wall=$(( $(date +%s) - started ))

status=$(cat "$run_dir/status" 2>/dev/null || echo "$docker_status")
peak=$(cat "$run_dir/memory.peak" 2>/dev/null || echo "")
oom=$(awk '$1 == "oom_kill" {print $2}' "$run_dir/memory.events" 2>/dev/null || echo "")
python3 - "$THREE_NODE_OUT/run/$label.json" "$label" "$suite" "$binary" "$status" "$docker_status" \
  "$wall" "$timeout_min" "$memory" "$peak" "$oom" "$@" <<'EOF'
import json, sys
out, label, suite, binary, status, docker_status, wall, timeout_min, memory, peak, oom, *args = sys.argv[1:]
num = lambda s: int(s) if s.strip().lstrip("-").isdigit() else None
json.dump({
    "label": label, "suite": suite, "binary": binary, "args": args,
    "exit_status": num(status), "docker_status": num(docker_status), "wall_s": num(wall),
    # coreutils timeout: 124 after SIGINT, 137 when it had to SIGKILL.
    "timeout_min": num(timeout_min), "timed_out": status.strip() in ("124", "137"),
    "memory_limit": memory, "memory_peak_bytes": num(peak), "oom_kills": num(oom),
}, open(out, "w"), indent=2)
EOF
echo "$label: exit $status after ${wall} s; container memory.peak ${peak:-unknown} bytes; OOM kills ${oom:-unknown}"
exit "$status"
