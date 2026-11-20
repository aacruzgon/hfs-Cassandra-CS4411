#!/usr/bin/env bash
# Samples, every INTERVAL seconds until killed, the memory of this job's
# containers (the three nodes and the test container; docker stats, which
# reads each container's cgroup) and the host's MemAvailable. The containers
# run in gh-runner's rootless Docker, in its user manager, outside the runner
# service's cgroup (ADR-aacruzgon-0023), so ci-report.py's cgroup sampler
# does not see them.
#
#   sample-memory.sh OUT_FILE PREFIX [INTERVAL]
#
# One line per container per sample: epoch<TAB>name<TAB>usage (docker's
# "1.2GiB / 1.66GiB"), and one per sample: epoch<TAB>host<TAB>MemAvailable kB.
set -u
out=$1 prefix=$2 interval=${3:-5}
while :; do
  now=$(date +%s)
  docker stats --no-stream --format '{{.Name}}\t{{.MemUsage}}' 2>/dev/null \
    | awk -v now="$now" -v p="$prefix" -F '\t' 'index($1, p) == 1 {print now "\t" $1 "\t" $2}' >> "$out"
  awk -v now="$now" '$1 == "MemAvailable:" {print now "\thost\t" $2}' /proc/meminfo >> "$out"
  sleep "$interval"
done
