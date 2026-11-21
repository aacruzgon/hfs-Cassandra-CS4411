#!/usr/bin/env bash
# Every node's coordinator request counters, for the three-node job's count
# of the 504s and 503s the nodes answered (run-tests.sh calls it before and
# after each run of a test binary; report.py takes the difference).
#
#   coordinator-metrics.sh OUT
#
# Reads, from each running container named $THREE_NODE_PROJECT-cassandra-*,
# the JMX meters org.apache.cassandra.metrics:type=ClientRequest,scope=*,
# name=Timeouts, Unavailables and Failures (their Count), through the image's
# own nodetool sjk. Cassandra 5.0.9 has no virtual table for them
# (system_views has latencies only, and system_metrics is not in 5.0). One
# sjk call per name: a pattern over every ClientRequest bean stops at the
# first bean without a Count (ViewPendingMutations). nodetool runs with a
# 128m heap, as everywhere in this job (pause-scenarios.sh says why).
#
# Writes OUT as tab-separated lines: container, scope (CASRead, CASWrite,
# Read, Write, and the per-consistency ones such as Read-QUORUM), name,
# count. A call that fails or prints something else writes one line with the
# scope "-" and the count "error", and one that outlasts its timeout the
# count "timeout", so report.py can say what it could not read. Always
# exits 0.
#
# Each call is bounded by THREE_NODE_METRICS_CALL_TIMEOUT seconds (default
# 8; one call measured 0.77 s, three per node 1.47 s), killed 2 s after
# that, so a hung nodetool costs at most 9 x 10 s = 90 s for three nodes,
# which run-tests.sh reserves before the step's deadline.
set -uo pipefail

out=${1:?usage: coordinator-metrics.sh OUT}
: "${THREE_NODE_PROJECT:?}"
: > "$out"
call_timeout=${THREE_NODE_METRICS_CALL_TIMEOUT:-8}
for c in $(docker ps --format '{{.Names}}' --filter "name=^${THREE_NODE_PROJECT}-cassandra-" | sort); do
  for name in Timeouts Unavailables Failures; do
    raw=$(timeout -k 2 "$call_timeout" docker exec -e MAX_HEAP_SIZE=128m "$c" nodetool sjk mx -all -mg \
      -b "org.apache.cassandra.metrics:type=ClientRequest,scope=*,name=$name" -f Count 2>&1)
    status=$?
    if [ "$status" = 124 ] || [ "$status" = 137 ]; then
      printf '%s\t-\t%s\ttimeout\n' "$c" "$name" >> "$out"
      continue
    fi
    # Pairs of lines: the bean's name, then its Count.
    if [ "$status" != 0 ] || ! parsed=$(awk -v c="$c" -v n="$name" '
        NR % 2 == 1 {
          if ($0 !~ /^org\.apache\.cassandra\.metrics:type=ClientRequest,scope=[^,]+,name=/) exit 1
          sub(/^org\.apache\.cassandra\.metrics:type=ClientRequest,scope=/, ""); sub(/,name=.*/, "")
          scope = $0; next
        }
        { if ($0 !~ /^[0-9]+$/) exit 1; print c "\t" scope "\t" n "\t" $0 }
        END { if (NR % 2 == 1) exit 1 }' <<< "$raw"); then
      printf '%s\t-\t%s\terror\n' "$c" "$name" >> "$out"
      continue
    fi
    [ -n "$parsed" ] && printf '%s\n' "$parsed" >> "$out"
  done
done
exit 0
