"""Sweeper measurements on the dev node, keyspace item5_sw ONLY.

Runs inside the Cassandra container with the bundled driver (the one cqlsh uses).
Usage: python3 exp.py <experiment> [args]
"""
import glob
import random
import statistics
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

_zip = glob.glob("/opt/cassandra/lib/cassandra-driver-internal-only-*.zip")[0]
sys.path.insert(0, _zip + "/cassandra-driver-3.29.0")
for _z in glob.glob("/opt/cassandra/lib/geomet-*.zip") + glob.glob("/opt/cassandra/lib/pure_sasl-*.zip") + glob.glob("/opt/cassandra/lib/pyasyncore-*.zip"):
    sys.path.insert(0, _z)

from cassandra import ConsistencyLevel  # noqa: E402
from cassandra.cluster import Cluster  # noqa: E402
from cassandra.concurrent import execute_concurrent_with_args  # noqa: E402
from cassandra.query import SimpleStatement  # noqa: E402
from cassandra.util import uuid_from_time  # noqa: E402

KS = "item5_sw"
assert KS == "item5_sw"


def connect():
    cluster = Cluster(["127.0.0.1"], port=9042)
    session = cluster.connect(KS)
    session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
    session.default_timeout = 30
    return cluster, session


def tu(at: datetime) -> uuid.UUID:
    """A v1 timeuuid for `at`, random clock sequence and multicast node (as write.rs)."""
    node = random.getrandbits(48) | (1 << 40)
    return uuid_from_time(at.timestamp(), node=node, clock_seq=random.getrandbits(14))


def bucket_of(at: datetime) -> str:
    return at.strftime("%Y-%m-%dT%H")


def trace_summary(rs):
    trace = rs.get_query_trace(max_wait_sec=10)
    lines = [e.description for e in trace.events if "tombstone" in e.description or "live rows" in e.description]
    return trace.duration.total_seconds() * 1000 if trace.duration else None, lines


INSERT = ("INSERT INTO pending_index_writes (bucket, shard, enqueued_at, tenant_id, resource_type, id, "
          "write_id, version_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
DELETE = ("DELETE FROM pending_index_writes WHERE bucket = ? AND shard = ? AND enqueued_at = ? "
          "AND tenant_id = ? AND resource_type = ? AND id = ?")
I3 = ("SELECT enqueued_at, tenant_id, resource_type, id, version_id FROM pending_index_writes "
      "WHERE bucket = ? AND shard = ? AND enqueued_at >= minTimeuuid(?) AND enqueued_at < minTimeuuid(?) LIMIT ?")
I3_WHOLE = ("SELECT enqueued_at, tenant_id, resource_type, id, version_id FROM pending_index_writes "
            "WHERE bucket = ? AND shard = ?")


def exp_tombstones(session):
    """3,000 finished intents over one hour in one partition, plus 3 live ones; trace reads."""
    ins, dele = session.prepare(INSERT), session.prepare(DELETE)
    i3, whole = session.prepare(I3), session.prepare(I3_WHOLE)
    hour = datetime(2026, 11, 12, 5, 0, 0, tzinfo=timezone.utc)
    bucket, shard = bucket_of(hour), 3
    rows = []
    for k in range(3000):
        at = hour + timedelta(seconds=k * 1.2)  # 50 per minute
        rows.append((bucket, shard, tu(at), "test-sw", "Patient", f"p{k}", uuid.uuid4(), 2))
    execute_concurrent_with_args(session, ins, rows, concurrency=64)
    execute_concurrent_with_args(session, dele, [r[:6] for r in rows], concurrency=64)
    live = [(bucket, shard, tu(hour + timedelta(minutes=m, seconds=30)), "test-sw", "Patient", f"live{m}", uuid.uuid4(), 1)
            for m in (10, 30, 59)]
    execute_concurrent_with_args(session, ins, live, concurrency=8)
    out = {}
    rs = session.execute(whole, (bucket, shard), trace=True)
    out["whole partition"] = (len(list(rs)),) + trace_summary(rs)
    t = hour + timedelta(minutes=10)
    rs = session.execute(i3, (bucket, shard, t, t + timedelta(minutes=1), 1000), trace=True)
    out["one-minute slice 05:10-05:11"] = (len(list(rs)),) + trace_summary(rs)
    t = hour + timedelta(minutes=30)
    rs = session.execute(i3, (bucket, shard, t, t + timedelta(seconds=5), 1000), trace=True)
    out["5 s slice 05:30:00-05:30:05"] = (len(list(rs)),) + trace_summary(rs)
    t = hour + timedelta(hours=1)
    rs = session.execute(i3, (bucket, shard, t, t + timedelta(minutes=1), 1000), trace=True)
    out["slice past the hour"] = (len(list(rs)),) + trace_summary(rs)
    rs = session.execute(i3, (bucket, shard, hour, hour + timedelta(hours=1), 1000), trace=True)
    out["whole hour as one slice"] = (len(list(rs)),) + trace_summary(rs)
    for name, (n, ms, lines) in out.items():
        print(f"{name}: rows={n} trace_ms={ms:.2f} :: {' | '.join(lines)}")


def exp_bounds(session):
    """Half-open [t, t2) with minTimeuuid on both ends: no row lost or read twice at the edge."""
    ins, i3 = session.prepare(INSERT), session.prepare(I3)
    base = datetime(2026, 11, 12, 6, 0, 0, tzinfo=timezone.utc)
    bucket, shard = bucket_of(base), 4
    edge = base + timedelta(minutes=1)
    probes = {
        "edge-1us": edge - timedelta(microseconds=1),
        "edge+0": edge,
        "edge+100ns-ish": edge + timedelta(microseconds=0.1),
        "edge+999us": edge + timedelta(microseconds=999),
        "edge+1ms": edge + timedelta(milliseconds=1),
    }
    for name, at in probes.items():
        session.execute(ins, (bucket, shard, tu(at), "test-sw", "Patient", name, uuid.uuid4(), 1))
    first = sorted(r.id for r in session.execute(i3, (bucket, shard, base, edge, 100)))
    second = sorted(r.id for r in session.execute(i3, (bucket, shard, edge, edge + timedelta(minutes=1), 100)))
    print("slice [base, edge):", first)
    print("slice [edge, edge+1m):", second)
    print("overlap:", set(first) & set(second), "missing:", set(probes) - set(first) - set(second))


def exp_ttl(session):
    """Expired intents (never deleted): tombstones counted per row."""
    ins = session.prepare(INSERT + " USING TTL 2")
    i3 = session.prepare(I3)
    base = datetime(2026, 11, 12, 7, 0, 0, tzinfo=timezone.utc)
    bucket, shard = bucket_of(base), 5
    rows = [(bucket, shard, tu(base + timedelta(seconds=k)), "test-sw", "Patient", f"e{k}", uuid.uuid4(), 1) for k in range(100)]
    execute_concurrent_with_args(session, ins, rows, concurrency=16)
    time.sleep(4)
    rs = session.execute(i3, (bucket, shard, base, base + timedelta(minutes=5), 1000), trace=True)
    n = len(list(rs))
    ms, lines = trace_summary(rs)
    print(f"100 expired rows: live={n} trace_ms={ms:.2f} :: {' | '.join(lines)}")


def timed(fn, n):
    t0 = time.perf_counter()
    fn()
    return (time.perf_counter() - t0) * 1000 / n


def exp_latency(session):
    """Empty-slice I3 reads, the startup scan, and SERIAL vs LOCAL_QUORUM point reads."""
    i3 = session.prepare(I3)
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    # Startup scan shape: 24 h x 60 one-minute slices x 16 shards on buckets with no rows.
    args = []
    for m in range(24 * 60):
        lo = now - timedelta(hours=24) + timedelta(minutes=m)
        for s in range(16):
            args.append((bucket_of(lo), s, lo, lo + timedelta(minutes=1), 1000))
    seq = args[:2000]

    def run_seq():
        for a in seq:
            session.execute(i3, a)
    print(f"empty I3 slice, sequential: {timed(run_seq, len(seq)):.3f} ms/query over {len(seq)}")
    for conc in (16, 64):
        t0 = time.perf_counter()
        execute_concurrent_with_args(session, i3, args, concurrency=conc)
        total = time.perf_counter() - t0
        print(f"startup scan, {len(args)} one-minute slices, concurrency {conc}: {total:.1f} s "
              f"({total * 1000 / len(args):.3f} ms/query effective)")
    # Point reads of resources: LOCAL_QUORUM vs LOCAL_SERIAL (one row present, one absent).
    session.execute("INSERT INTO resources (tenant_id, resource_type, id, version_id, data, last_updated, "
                    "created_at, fhir_version, write_ts, write_id, index_keys, deleted_at) VALUES "
                    "('test-sw', 'Patient', 'lat1', 1, '{}', toTimestamp(now()), toTimestamp(now()), '4.0', 0, uuid(), 0x0100, 0) IF NOT EXISTS")
    sel = session.prepare("SELECT version_id, data, last_updated, created_at, deleted_at, fhir_version, write_ts, "
                          "write_id, index_keys FROM resources WHERE tenant_id = ? AND resource_type = ? AND id = ?")
    for cl_name, cl in (("LOCAL_QUORUM", ConsistencyLevel.LOCAL_QUORUM), ("LOCAL_SERIAL", ConsistencyLevel.LOCAL_SERIAL)):
        sel.consistency_level = cl
        for rid in ("lat1", "absent"):
            samples = []
            for _ in range(500):
                t0 = time.perf_counter()
                session.execute(sel, ("test-sw", "Patient", rid))
                samples.append((time.perf_counter() - t0) * 1000)
            samples.sort()
            print(f"R1/R2 {cl_name} row={rid}: median {statistics.median(samples):.3f} ms, "
                  f"p95 {samples[int(0.95 * len(samples))]:.3f} ms")
    sel.consistency_level = ConsistencyLevel.LOCAL_SERIAL
    rs = session.execute(sel, ("test-sw", "Patient", "lat1"), trace=True)
    trace = rs.get_query_trace(max_wait_sec=10)
    print("serial read trace:", trace.duration, "|", " | ".join(e.description for e in trace.events)[:1500])


def exp_load(session, seconds: int, rate: int, poll_s: float, grace_s: float):
    """Writers insert then delete intents at `rate`/s; a sweeper polls 16 shards each `poll_s`."""
    ins, dele, i3 = session.prepare(INSERT), session.prepare(DELETE), session.prepare(I3)
    start = time.time()
    marks = {}
    polls = slices = live_seen = 0
    max_rows = 0
    next_poll = start + poll_s
    written = 0
    while time.time() - start < seconds:
        tick = time.time()
        batch = []
        for _ in range(rate // 10):
            at = datetime.now(timezone.utc)
            sid = random.getrandbits(32)
            key = (bucket_of(at), sid % 16, tu(at), "test-sw-load", "Patient", f"p{sid}")
            batch.append(key)
        execute_concurrent_with_args(session, ins, [k + (uuid.uuid4(), 2) for k in batch], concurrency=32)
        execute_concurrent_with_args(session, dele, batch, concurrency=32)
        written += len(batch)
        if time.time() >= next_poll:
            hi = datetime.now(timezone.utc) - timedelta(seconds=grace_s)
            hi = hi.replace(microsecond=(hi.microsecond // 1000) * 1000)
            for s in range(16):
                for b in {bucket_of(hi), bucket_of(hi - timedelta(hours=1))}:
                    lo = marks.get((b, s), datetime.strptime(b, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc))
                    lo = max(lo, hi - timedelta(seconds=grace_s + poll_s * 2)) if (b, s) not in marks else lo
                    bucket_end = datetime.strptime(b, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc) + timedelta(hours=1)
                    top = min(hi, bucket_end)
                    if lo < top:
                        rs = list(session.execute(i3, (b, s, lo, top, 1000)))
                        live_seen += len(rs)
                        max_rows = max(max_rows, len(rs))
                        slices += 1
                        marks[(b, s)] = top
            polls += 1
            next_poll += poll_s
        time.sleep(max(0.0, 0.1 - (time.time() - tick)))
    print(f"load: {written} intents in {seconds} s ({written / seconds:.0f}/s), {polls} polls, "
          f"{slices} slices, live rows seen {live_seen}")


def main():
    cluster, session = connect()
    try:
        exp = sys.argv[1]
        if exp == "tombstones":
            exp_tombstones(session)
        elif exp == "bounds":
            exp_bounds(session)
        elif exp == "ttl":
            exp_ttl(session)
        elif exp == "latency":
            exp_latency(session)
        elif exp == "load":
            exp_load(session, int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4]), float(sys.argv[5]))
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
