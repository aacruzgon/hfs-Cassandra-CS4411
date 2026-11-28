#!/usr/bin/env python3
"""The benchmark harness of roadmap row 0.6. See README.md next to this file.

    python3 docs/Cassandra/benchmark/bench.py fetch-k6 --dest /some/dir
    python3 docs/Cassandra/benchmark/bench.py calibrate ...
    python3 docs/Cassandra/benchmark/bench.py run ...
    python3 docs/Cassandra/benchmark/bench.py summarize OUT

It drives k6 (load.js) against one HFS process and one database container
from docker/cassandra or docker/postgres, unchanged. For every run it resets
the database, starts HFS, loads the dataset, settles the database, warms up,
measures, and records what it measured and what it ran on.

It needs Python 3.9 or later, Docker with Compose, and a release build of hfs
with the backend under test compiled in. It uses no third-party package.

Exit status: 0 when every run was measured or recorded as refused by the
server, 1 when any run failed, 2 on a usage or setup error.
"""

import argparse
import collections
import contextlib
import csv
import fcntl
import gzip
import hashlib
import json
import math
import os
import platform
import re
import shlex
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.error
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]

K6_VERSION = "2.3.0"
K6_ASSETS = {
    ("Darwin", "arm64"): (
        "k6-v2.3.0-macos-arm64.zip",
        "b2417a3038edc5fe81dc178a889237724b595c5c9cfed875822008e46e862c7d",
    ),
    ("Darwin", "x86_64"): (
        "k6-v2.3.0-macos-amd64.zip",
        "c83bb16f54f0676afa4ea235fd757f3892d307dbd1bd201450de83af31986654",
    ),
    ("Linux", "x86_64"): (
        "k6-v2.3.0-linux-amd64.tar.gz",
        "39c3117b6af817592dcd0ce4242105c0a7af10948c2a425306f0be8f7a8a8ab1",
    ),
    ("Linux", "aarch64"): (
        "k6-v2.3.0-linux-arm64.tar.gz",
        "5ca3433e8201da72a284aaa241a1bb5fb47f4abb4e384d39410ddd8062f49b90",
    ),
}

OPS = ("create", "read", "update", "token", "date", "uri")
EXTRA_OPS = ("hotkey",)
SEARCH_OPS = ("token", "date", "uri")

SNAPSHOT_MARKER = ".hfsbench-snapshot-complete"
# The index tables every Observation of the dataset writes to (code, effective date, profile).
RW6_INDEX_TABLES = ("token_code_index", "date_index", "uri_index")
# The tables nodetool tablehistograms reads after each Cassandra run: those the
# operations write (resources, versions, intents) and those the searches will read.
HISTOGRAM_TABLES = ("resources", "resource_versions", "pending_index_writes", "token_system_code_index",
                    "date_index", "uri_index")

# Refused outright: these are the shared development containers.
FORBIDDEN_PROJECTS = {"hfs-cassandra", "hfs-postgres"}
FORBIDDEN_PORTS = {59042, 55432}

CALIBRATION_FRACTION = 0.8

# Above this share of the schedule dropped, a run's latency is reported as saturated.
# Healthy runs dropped 0.1 to 0.9% (start-up); one that couldn't hold its rate dropped 6 to 56%.
# At 1%, at most a fifth of the 5% tail behind p95 can be missing.
SATURATED_DROP_PCT = 1.0

# A host is quiet when other processes use at most this much CPU in total.
# Measured on the idle Mac; see README "Host load". The load average is
# recorded but not used: it still holds the harness's own previous run.
QUIET_FOREIGN_CPU_PCT = 200.0
HOST_PROCESS_MIN_PCT = 5.0
# The process that is the Docker VM, on hosts where containers run inside one.
DOCKER_VM_PROCESSES = (
    "OrbStack Helper", "com.docker.virtualization", "com.docker.krun", "com.docker.hyperkit",
    "com.apple.Virtualization.VirtualMachine", "qemu-system-", "limactl", "vmmem",
)
# A CI runner's job: GitHub's runner starts one Runner.Worker per job. While one
# runs the host is busy, whatever its CPU at that moment (acruzdebian is a runner).
CI_JOB_PROCESSES = ("Runner.Worker",)
# On Linux, CPU is the change in /proc over this many seconds (see cpu_between).
LINUX_CPU_INTERVAL_SECONDS = 2.0
# How often the host is read during a measured window (HostWatch).
HOST_WATCH_SECONDS = 10.0
HOST_WATCH_FIRST_SECONDS = 5.0
# A pause between the readings before the window and k6's start.
WINDOW_SETTLE_SECONDS = 3.0
BATCH_WORKERS = 4
SAMPLE_SECONDS = 2.0
SETTLE_TIMEOUT_SECONDS = 1800
# Beyond a run's measured seconds: k6's graceful stop, the reduction of its CSV, the collection.
RUN_OVERHEAD_SECONDS = 30
# Before the first run of a no-reset cell: the restore, the boot, a settle.
NO_RESET_START_SECONDS = 180
# A gap this large between the wall clock and the monotonic clock over a measurement means
# the host slept during it (a closed lid). Clock adjustments are far smaller than this.
HOST_SLEEP_TOLERANCE_SECONDS = 2.0


class Failure(Exception):
    """A run could not be completed; the message says why."""


class BudgetExhausted(Exception):
    """The overnight time budget ran out while waiting; not a failure of the run."""


# Where overnight also keeps its log, so a closed terminal doesn't lose it.
LOG_SINK = None


def log(message: str) -> None:
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
    try:
        print(line, flush=True)
    except OSError:  # The terminal has gone away.
        pass
    if LOG_SINK is not None:
        LOG_SINK.write(line + "\n")
        LOG_SINK.flush()


def detach_output() -> None:
    """Point stdout and stderr at nothing, once the terminal behind them has closed."""
    null = os.open(os.devnull, os.O_WRONLY)
    os.dup2(null, 1)
    os.dup2(null, 2)
    os.close(null)


def stop_process(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)


def run_cmd(
    command: Sequence[str],
    check: bool = True,
    env: Optional[Dict[str, str]] = None,
    stdin_path: Optional[Path] = None,
    timeout: Optional[float] = None,
    own_session: bool = False,
) -> subprocess.CompletedProcess:
    """Run a command and capture its output.

    own_session puts it outside the terminal's process group, so a Ctrl-C
    pressed again during the cleanup can't cut that cleanup short.
    """
    stdin = stdin_path.open("rb") if stdin_path else subprocess.DEVNULL
    try:
        result = subprocess.run(
            list(command),
            stdin=stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env,
            timeout=timeout,
            start_new_session=own_session,
        )
    finally:
        if stdin_path:
            stdin.close()
    if check and result.returncode != 0:
        raise Failure(
            f"{' '.join(command[:6])} exited {result.returncode}: "
            f"{(result.stderr or result.stdout)[-800:]}"
        )
    return result


def clocks() -> Tuple[float, float]:
    """The wall clock, which runs on while the host sleeps, and the monotonic clock, which stops.

    time.monotonic() is mach_absolute_time() on macOS and CLOCK_MONOTONIC on
    Linux; neither counts a sleep or a suspend.
    """
    return time.time(), time.monotonic()


def seconds_asleep(before: Tuple[float, float], after: Tuple[float, float]) -> float:
    """How long the host slept between two readings of clocks()."""
    return max(0.0, (after[0] - before[0]) - (after[1] - before[1]))


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


# --- analysis, independent of Docker and k6 so it can be tested --------------


def percentile(ordered: Sequence[float], fraction: float) -> float:
    """Nearest rank: the smallest value with at least `fraction` of values at or below it."""
    if not ordered:
        return float("nan")
    rank = max(1, math.ceil(fraction * len(ordered)))
    return ordered[rank - 1]


def status_class(status: str) -> str:
    """The error accounting of roadmap Step 0: 409 and 412 apart from 5xx."""
    if status == "409":
        return "s409"
    if status == "412":
        return "s412"
    if status == "501":
        return "s501"
    if len(status) == 3 and status.isdigit():
        leading = status[0]
        if leading == "2":
            return "s2xx"
        if leading == "4":
            return "s4xx_other"
        if leading == "5":
            return "s5xx_other"
    return "s_other"


CLASSES = ("s2xx", "s409", "s412", "s4xx_other", "s501", "s5xx_other", "s_other")

BIN_FIELDS = (
    ["bin_start_s", "completed", "completed_rps", "ok_rps", "p50_ms", "p95_ms", "p99_ms"]
    + list(CLASSES)
    + ["dropped_iterations"]
)


def open_text(path: Path, mode: str):
    if path.suffix == ".gz":
        return gzip.open(path, mode + "t", encoding="utf-8", newline="")
    return path.open(mode, encoding="utf-8", newline="")


# Tail percentiles need enough requests beyond them to mean anything: fewer
# than this many and the value is kept but flagged unreliable.
TAIL_MIN_BEYOND = 10
PERCENTILES = (("p50", 0.50), ("p90", 0.90), ("p95", 0.95), ("p99", 0.99), ("p999", 0.999), ("p9999", 0.9999))
# The 95 % interval of p95 and p99: a bootstrap (fixed seed) up to this many requests,
# above it the distribution-free order-statistic interval, which a bootstrap of that
# size converges to and which costs nothing (pure Python resampling of millions of
# requests would take minutes per run).
BOOTSTRAP_RESAMPLES = 200
BOOTSTRAP_SEED = 4411
BOOTSTRAP_MAX_N = 20000
# The latency histogram: 10 buckets per decade from 0.1 ms to 10^4.8 ms (63 s), plus
# one below and one above.
HISTOGRAM_PER_DECADE = 10
HISTOGRAM_LOW_EXP = -10   # 10^(-10/10) = 0.1 ms
HISTOGRAM_HIGH_EXP = 48   # 10^(48/10) = 63,096 ms
K6_PHASES = ("blocked", "connecting", "sending", "waiting", "receiving")
STATUS_LATENCY_CLASSES = ("s2xx", "s409", "s412", "s4xx_other", "s5xx_other")


def reliable(n: int, fraction: float) -> bool:
    """At least TAIL_MIN_BEYOND requests lie beyond this percentile: n >= 10,000 for p99.9."""
    return n * (1.0 - fraction) >= TAIL_MIN_BEYOND - 1e-9


def latency_stats(ordered: Sequence[float], suffix: str = "") -> dict:
    """Every percentile, min, max, mean and standard deviation of sorted latencies, in ms.

    suffix "" names the figures over every response (p50_ms), "_ok" over 2xx (p50_ok_ms).
    """
    n = len(ordered)
    stats: dict = {f"n{suffix}": n}
    for name, fraction in PERCENTILES:
        stats[f"{name}{suffix}_ms"] = percentile(ordered, fraction)
    for name, fraction in (("p999", 0.999), ("p9999", 0.9999)):
        stats[f"{name}{suffix}_reliable"] = reliable(n, fraction)
    stats[f"min{suffix}_ms"] = ordered[0] if n else float("nan")
    stats[f"max{suffix}_ms"] = ordered[-1] if n else float("nan")
    stats[f"mean{suffix}_ms"] = statistics.fmean(ordered) if n else float("nan")
    stats[f"stddev{suffix}_ms"] = statistics.pstdev(ordered) if n > 1 else float("nan")
    return stats


def percentile_intervals(ordered: Sequence[float], fractions: Sequence[float]) -> Tuple[Dict[float, Tuple[float, float]], str]:
    """A 95 % interval for each nearest-rank percentile of sorted values, and how they were made.

    Up to BOOTSTRAP_MAX_N values: a bootstrap with a fixed seed, every percentile
    read from the same resamples. Above it: the order-statistic interval.
    """
    n = len(ordered)
    if n < 2:
        return {f: (float("nan"), float("nan")) for f in fractions}, "none"
    if n <= BOOTSTRAP_MAX_N:
        import random
        rng = random.Random(BOOTSTRAP_SEED)
        estimates: Dict[float, List[float]] = {f: [] for f in fractions}
        for _ in range(BOOTSTRAP_RESAMPLES):
            resample = sorted(rng.choices(ordered, k=n))
            for f in fractions:
                estimates[f].append(percentile(resample, f))
        low_at, high_at = int(0.025 * BOOTSTRAP_RESAMPLES), int(0.975 * BOOTSTRAP_RESAMPLES) - 1
        return ({f: (sorted(v)[low_at], sorted(v)[high_at]) for f, v in estimates.items()},
                f"bootstrap {BOOTSTRAP_RESAMPLES} resamples, seed {BOOTSTRAP_SEED}")
    result = {}
    for f in fractions:
        centre = n * f
        spread = 1.96 * math.sqrt(n * f * (1.0 - f))
        result[f] = (ordered[max(1, math.floor(centre - spread)) - 1], ordered[min(n, math.ceil(centre + spread)) - 1])
    return result, "order statistic (binomial, normal approximation)"


def intervals(ordered: Sequence[float], suffix: str = "") -> dict:
    bounds, method = percentile_intervals(ordered, (0.95, 0.99))
    result: dict = {f"ci{suffix}_method": method}
    for name, fraction in (("p95", 0.95), ("p99", 0.99)):
        result[f"{name}{suffix}_ci_low_ms"], result[f"{name}{suffix}_ci_high_ms"] = bounds[fraction]
    return result


def histogram_bucket(value_ms: float) -> int:
    """The bucket of a latency: -1 below 0.1 ms, then one per tenth of a decade, the last one open."""
    if value_ms < 10 ** (HISTOGRAM_LOW_EXP / HISTOGRAM_PER_DECADE):
        return -1
    index = math.floor(HISTOGRAM_PER_DECADE * math.log10(value_ms)) - HISTOGRAM_LOW_EXP
    return min(index, HISTOGRAM_HIGH_EXP - HISTOGRAM_LOW_EXP)


def write_histogram(rows: Sequence[Tuple[float, float, str]], out: Path) -> None:
    """histogram.csv: log-spaced latency buckets, counted per status class."""
    counts: Dict[int, collections.Counter] = collections.defaultdict(collections.Counter)
    for _, value, status in rows:
        counts[histogram_bucket(value)][status_class(status)] += 1
    top = HISTOGRAM_HIGH_EXP - HISTOGRAM_LOW_EXP
    with out.open("w", encoding="utf-8", newline="") as sink:
        writer = csv.writer(sink)
        writer.writerow(["lower_ms", "upper_ms", "total"] + list(CLASSES))
        for index in range(-1, top + 1):
            lower = 0.0 if index < 0 else 10 ** ((index + HISTOGRAM_LOW_EXP) / HISTOGRAM_PER_DECADE)
            upper = (10 ** (HISTOGRAM_LOW_EXP / HISTOGRAM_PER_DECADE) if index < 0
                     else "" if index == top else 10 ** ((index + 1 + HISTOGRAM_LOW_EXP) / HISTOGRAM_PER_DECADE))
            bucket = counts.get(index, collections.Counter())
            writer.writerow([f"{lower:.4g}", upper if upper == "" else f"{upper:.4g}", sum(bucket.values())]
                            + [bucket[name] for name in CLASSES])


def analyse(
    k6_csv: Path,
    requests_out: Path,
    bins_out: Path,
    duration_s: float,
    bin_seconds: int,
    histogram_out: Optional[Path] = None,
) -> dict:
    """Reduce k6's CSV to per-request rows and time bins, and return the run's metrics."""
    import array
    rows: List[Tuple[float, float, str]] = []  # end time ms, duration ms, status
    dropped: List[Tuple[float, float]] = []
    phases = {phase: array.array("d") for phase in K6_PHASES}
    data = {"data_sent": 0.0, "data_received": 0.0}
    with open_text(k6_csv, "r") as source, open_text(requests_out, "w") as sink:
        writer = csv.writer(sink)
        writer.writerow(["end_ms", "op", "status", "duration_ms", "error_code"])
        for row in csv.DictReader(source):
            name = row["metric_name"]
            if name == "http_req_duration":
                end = float(row["timestamp"])
                value = float(row["metric_value"])
                status = row.get("status") or "0"
                rows.append((end, value, status))
                writer.writerow(
                    [int(end), row.get("name", ""), status, f"{value:.3f}", row.get("error_code", "")]
                )
            elif name == "dropped_iterations":
                dropped.append((float(row["timestamp"]), float(row["metric_value"])))
            elif name.startswith("http_req_") and name[9:] in phases:
                phases[name[9:]].append(float(row["metric_value"]))
            elif name in data:
                data[name] += float(row["metric_value"])

    metrics: dict = {"completed": len(rows)}
    statuses: collections.Counter = collections.Counter(r[2] for r in rows)
    classes: collections.Counter = collections.Counter()
    for status, count in statuses.items():
        classes[status_class(status)] += count
    for name in CLASSES:
        metrics[name] = classes[name]
    metrics["statuses"] = ";".join(f"{k}:{statuses[k]}" for k in sorted(statuses))
    metrics["dropped_iterations"] = int(sum(v for _, v in dropped))
    metrics["throughput_rps"] = len(rows) / duration_s
    metrics["ok_rps"] = classes["s2xx"] / duration_s
    ordered = sorted(r[1] for r in rows)
    metrics.update(latency_stats(ordered))
    # The same, over 2xx only: a fast 503 or 409 would otherwise pull the figures down.
    ok_only = sorted(r[1] for r in rows if status_class(r[2]) == "s2xx")
    metrics.update(latency_stats(ok_only, "_ok"))
    metrics.update(intervals(ordered))
    metrics.update(intervals(ok_only, "_ok"))
    # A slow 409 shows apart from a slow 201.
    for name in STATUS_LATENCY_CLASSES:
        members = sorted(r[1] for r in rows if status_class(r[2]) == name)
        metrics[f"{name}_p50_ms"] = percentile(members, 0.50) if members else None
        metrics[f"{name}_p99_ms"] = percentile(members, 0.99) if members else None
        metrics[f"{name}_max_ms"] = members[-1] if members else None
    # Where the time went, as k6 saw it: queueing for a connection, connecting, sending, waiting, receiving.
    for phase, values in phases.items():
        ordered_phase = sorted(values)
        metrics[f"k6_{phase}_p50_ms"] = percentile(ordered_phase, 0.50) if ordered_phase else None
        metrics[f"k6_{phase}_p99_ms"] = percentile(ordered_phase, 0.99) if ordered_phase else None
    metrics["k6_data_sent_bytes"] = int(data["data_sent"])
    metrics["k6_data_received_bytes"] = int(data["data_received"])
    metrics.update(saturation(len(rows), metrics["dropped_iterations"]))
    if histogram_out is not None:
        write_histogram(rows, histogram_out)

    # Bins are counted from the first event, so bin 0 is the start of the run
    # and a slow start or a decline shows in the sequence.
    first = min([r[0] for r in rows] + [d[0] for d in dropped], default=0.0)
    width_ms = bin_seconds * 1000.0
    per_bin: Dict[int, List[Tuple[float, float, str]]] = collections.defaultdict(list)
    dropped_per_bin: collections.Counter = collections.Counter()
    for row in rows:
        per_bin[int((row[0] - first) // width_ms)].append(row)
    for moment, value in dropped:
        dropped_per_bin[int((moment - first) // width_ms)] += int(value)

    ok_per_bin: List[float] = []
    with bins_out.open("w", encoding="utf-8", newline="") as sink:
        writer = csv.writer(sink)
        writer.writerow(BIN_FIELDS)
        # Every bin up to the last event, empty ones included: a stall is a
        # row of zeros, not a gap the steady-state figure would close up.
        last = max(list(per_bin) + list(dropped_per_bin), default=-1)
        for index in range(last + 1):
            members = per_bin.get(index, [])
            latencies = sorted(r[1] for r in members)
            counts: collections.Counter = collections.Counter(status_class(r[2]) for r in members)
            ok = counts["s2xx"] / bin_seconds
            ok_per_bin.append(ok)
            writer.writerow(
                [index * bin_seconds, len(members), f"{len(members) / bin_seconds:.1f}", f"{ok:.1f}"]
                + [f"{percentile(latencies, q):.3f}" if latencies else "" for q in (0.50, 0.95, 0.99)]
                + [counts[name] for name in CLASSES]
                + [dropped_per_bin.get(index, 0)]
            )
    metrics["steady_change_pct"] = steady_change_pct(ok_per_bin)
    return metrics


def saturation(completed: int, dropped: int) -> dict:
    """How much of the fixed schedule was served, and whether its latency can be read as latency.

    A dropped iteration never got a virtual user, so its latency is in no
    percentile: the more a backend sheds, the better what is left looks.
    """
    offered = completed + dropped
    drop_pct = 100.0 * dropped / offered if offered else 0.0
    saturated = drop_pct > SATURATED_DROP_PCT
    return {"offered": offered, "drop_pct": drop_pct, "saturated": saturated,
            "latency_reading": "saturated" if saturated else "normal"}


def steady_change_pct(bin_rps: Sequence[float]) -> Optional[float]:
    """Change in successful throughput between the first and last third of the full bins, in percent.

    The first and last bins are partial and are left out. Negative means the
    throughput fell during the run, as in ADR-aacruzgon-0001. Needs at least 3
    full bins.
    """
    full = list(bin_rps[1:-1])
    if len(full) < 3:
        return None
    third = max(1, len(full) // 3)
    early = statistics.fmean(full[:third])
    late = statistics.fmean(full[-third:])
    if early == 0:
        return None
    return (late - early) / early * 100.0


def calibrated_rate(capacity_rps: float) -> int:
    return max(1, math.floor(capacity_rps * CALIBRATION_FRACTION))


def parse_size(text: str) -> float:
    match = re.fullmatch(r"\s*([0-9.]+)\s*([KMGT]?i?B)\s*", text)
    if match is None:
        raise ValueError(text)
    factors = {
        "B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4,
        "kB": 1000, "KB": 1000, "MB": 1000**2, "GB": 1000**3, "TB": 1000**4,
    }
    return float(match.group(1)) * factors[match.group(2)]


def docker_limits_gib(docker_stats_lines: Iterable[str]) -> float:
    """Sum the memory limits from `docker stats --format '{{.Name}}\\t{{.MemUsage}}...'`."""
    total = 0.0
    for line in docker_stats_lines:
        fields = line.split("\t")
        if len(fields) < 2 or "/" not in fields[1]:
            continue
        try:
            total += parse_size(fields[1].split("/", 1)[1])
        except ValueError:  # `-- / --`: a container that is starting has no figures yet.
            continue
    return total / 1024**3


def other_containers_cpu(docker_stats_lines: Iterable[str], project: str) -> List[dict]:
    """The CPU of the containers that aren't this harness's, from `docker stats`."""
    others = []
    for line in docker_stats_lines:
        fields = line.split("\t")
        if len(fields) < 3 or fields[0].startswith(f"{project}-"):
            continue
        try:
            cpu = float(fields[2].rstrip("%"))
        except ValueError:
            continue
        if cpu >= HOST_PROCESS_MIN_PCT:
            others.append({"pid": None, "cpu_pct": cpu, "command": f"container {fields[0]}"})
    return others


# --- the host -------------------------------------------------------------------


def parse_ps(text: str) -> Dict[int, Tuple[int, float, str]]:
    processes: Dict[int, Tuple[int, float, str]] = {}
    for line in text.splitlines():
        fields = line.split(None, 3)
        if len(fields) == 4:
            processes[int(fields[0])] = (int(fields[1]), float(fields[2]), fields[3].strip())
    return processes


def is_docker_vm(command: str) -> bool:
    return any(name in command for name in DOCKER_VM_PROCESSES)


def foreign_processes(processes: Dict[int, Tuple[int, float, str]], root_pid: int,
                      own_roots: Iterable[int] = ()) -> List[dict]:
    """Processes above the reporting threshold, without the harness's own and the Docker VM.

    On macOS and Windows the database runs inside the VM, so the VM's process
    is the harness's own load. Other containers are counted from `docker stats`.
    """
    own = {root_pid, *own_roots}
    grew = True
    while grew:
        grew = False
        for pid, (parent, _, _) in processes.items():
            if parent in own and pid not in own:
                own.add(pid)
                grew = True
    return sorted(
        ({"pid": pid, "cpu_pct": cpu, "command": command[-60:]}
         for pid, (_, cpu, command) in processes.items()
         if pid not in own and cpu >= HOST_PROCESS_MIN_PCT and not is_docker_vm(command)),
        key=lambda item: -item["cpu_pct"],
    )


def host_is_quiet(record: dict) -> bool:
    return record["foreign_cpu_pct"] <= QUIET_FOREIGN_CPU_PCT and not record.get("ci_jobs")


def is_ci_job(command: str) -> bool:
    return any(name in command for name in CI_JOB_PROCESSES)


def parse_proc_stat(text: str) -> Optional[Tuple[int, str, int]]:
    """(ppid, comm, utime + stime in clock ticks) from one /proc/<pid>/stat, or None."""
    left, right = text.find("("), text.rfind(")")
    if left < 0 or right < left:
        return None
    fields = text[right + 2:].split()
    try:
        return int(fields[1]), text[left + 1:right], int(fields[11]) + int(fields[12])
    except (IndexError, ValueError):
        return None


def read_proc(root: Path = Path("/proc"), cmdlines: Optional[Iterable[int]] = None) -> Dict[int, Tuple[int, str, int, int, str]]:
    """pid -> (ppid, comm, CPU ticks, uid, cmdline) for every process /proc shows.

    One read of `stat` per process; `cmdline` only for the pids named (all when
    None), since reading it for every process doubles the cost of a reading.
    """
    wanted = None if cmdlines is None else set(cmdlines)
    processes: Dict[int, Tuple[int, str, int, int, str]] = {}
    for entry in root.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            parsed = parse_proc_stat((entry / "stat").read_text(encoding="utf-8", errors="replace"))
            uid = entry.stat().st_uid
            cmdline = ""
            if wanted is None or int(entry.name) in wanted:
                cmdline = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "replace").strip()
        except OSError:  # It exited between the listing and the read.
            continue
        if parsed is not None:
            processes[int(entry.name)] = (parsed[0], parsed[1], parsed[2], uid, cmdline)
    return processes


def read_cmdline(pid: int) -> str:
    try:
        return Path(f"/proc/{pid}/cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "replace").strip()
    except OSError:
        return ""


def needs_cmdline(comm: str) -> bool:
    """The processes whose command line decides whose they are (docker_plumbing) or that are CI jobs."""
    return comm in ("docker-proxy", "dockerd", "containerd") or comm.startswith("containerd-shim") or is_ci_job(comm)


def cpu_between(before: Dict[int, Tuple[int, str, int, int, str]], after: Dict[int, Tuple[int, str, int, int, str]],
                seconds: float, ticks_per_second: float) -> Dict[int, Tuple[int, float, str]]:
    """Each process's CPU over the interval, in percent of one core, as parse_ps gives it.

    `ps` on Linux reports a process's CPU over its whole life, so a long-lived
    process that has just become busy (a CI runner's daemon, an IDE) barely
    shows there. Two readings of /proc a few seconds apart show it.
    """
    result: Dict[int, Tuple[int, float, str]] = {}
    for pid, (ppid, comm, ticks, _, cmdline) in after.items():
        earlier = before.get(pid)
        used = ticks - earlier[2] if earlier is not None else 0
        result[pid] = (ppid, 100.0 * used / ticks_per_second / seconds if seconds > 0 else 0.0, cmdline or comm)
    return result


def docker_plumbing(processes: Dict[int, Tuple[int, str, int, int, str]], port: int, container: str) -> List[int]:
    """The system Docker's processes that serve this harness's container, on Linux.

    The root-owned dockerd and containerd answer the harness's own `docker exec`
    and `docker stats`; docker-proxy carries HFS's traffic to the database's host
    port; the container's shim is the parent of the database. A rootless Docker
    (a CI runner's) runs as its own user and stays foreign.
    """
    own = []
    for pid, (_, comm, _, uid, cmdline) in processes.items():
        if uid == 0 and comm in ("dockerd", "containerd"):
            own.append(pid)
        elif uid == 0 and comm == "docker-proxy" and f"-host-port {port}" in cmdline:
            own.append(pid)
        elif uid == 0 and comm.startswith("containerd-shim") and container and container[:12] in cmdline:
            own.append(pid)
    return own


def own_database_pid(backend: "Backend") -> List[int]:
    """On Linux the database's processes are the host's, and need naming as the harness's own."""
    if platform.system() != "Linux":
        return []
    try:
        container = backend.container_id()
        pid = run_cmd(["docker", "inspect", "--format", "{{.State.Pid}}", container], check=False).stdout.strip()
    except (Failure, OSError, subprocess.SubprocessError):
        return []
    return [int(pid)] if pid.isdigit() and int(pid) > 0 else []


def host_load(backend: "Backend", with_docker: bool = True) -> dict:
    """The host's load, as the kernel sees it, without the harness's own processes.

    On Linux, CPU is measured over LINUX_CPU_INTERVAL_SECONDS from /proc; on
    macOS `ps` already gives recent CPU. A CI runner's job (Runner.Worker) makes
    the host busy whatever its CPU, since it can start a build at any moment.
    """
    load_1m, load_5m, load_15m = os.getloadavg()
    ci_jobs: List[str] = []
    if platform.system() == "Linux":
        before = read_proc(cmdlines=())
        time.sleep(LINUX_CPU_INTERVAL_SECONDS)
        light = read_proc(cmdlines=())
        ticks = os.sysconf("SC_CLK_TCK")
        busy = cpu_between(before, light, LINUX_CPU_INTERVAL_SECONDS, ticks)
        # Command lines only where they matter: docker's and the CI's processes, and those above the threshold.
        named = [pid for pid, (_, comm, _, _, _) in light.items()
                 if needs_cmdline(comm) or busy.get(pid, (0, 0.0, ""))[1] >= HOST_PROCESS_MIN_PCT]
        after = dict(light)
        for pid in named:
            after[pid] = light[pid][:4] + (read_cmdline(pid) or light[pid][1],)
        processes = cpu_between(before, after, LINUX_CPU_INTERVAL_SECONDS, ticks)
        try:
            container = backend.container_id()
        except (Failure, OSError, subprocess.SubprocessError):
            container = ""
        own = own_database_pid(backend) + docker_plumbing(after, backend.port, container)
        ci_jobs = sorted({f"{pid} {cmdline[:80]}" for pid, (_, comm, _, _, cmdline) in after.items()
                          if is_ci_job(comm) or is_ci_job(cmdline)})
    else:
        processes = parse_ps(run_cmd(["ps", "-A", "-o", "pid=,ppid=,pcpu=,comm="], check=False).stdout)
        own = []
        ci_jobs = sorted({f"{pid} {command[-80:]}" for pid, (_, _, command) in processes.items() if is_ci_job(command)})
    foreign = foreign_processes(processes, os.getpid(), own)
    stats = None
    if with_docker:
        stats = run_cmd(["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"], check=False)
        if stats.returncode == 0 and platform.system() != "Linux":
            # Inside the VM, which `ps` no longer counts. On Linux the processes are the host's.
            foreign = sorted(foreign + other_containers_cpu(stats.stdout.splitlines(), backend.project),
                             key=lambda item: -item["cpu_pct"])
    return {
        "load_1m": load_1m, "load_5m": load_5m, "load_15m": load_15m,
        "cpus": os.cpu_count(),
        "foreign_cpu_pct": sum(item["cpu_pct"] for item in foreign),
        "processes_above_5pct": foreign,
        "ci_jobs": ci_jobs,
        "docker_limits_gib": docker_limits_gib(stats.stdout.splitlines()) if stats is not None and stats.returncode == 0 else None,
    }


def proc_ticks(pid: int, children: bool = True) -> Optional[int]:
    """A process's CPU ticks so far, with those of the children it has reaped (cutime, cstime)."""
    try:
        text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    fields = text[text.rfind(")") + 2:].split()
    try:
        return sum(int(fields[i]) for i in ((11, 12, 13, 14) if children else (11, 12)))
    except (IndexError, ValueError):
        return None


def host_busy_ticks(text: str) -> Optional[int]:
    """Every CPU's non-idle ticks since boot, from /proc/stat's first line (idle and iowait left out)."""
    for line in text.splitlines():
        fields = line.split()
        if fields and fields[0] == "cpu":
            values = [int(v) for v in fields[1:]]
            # user nice system idle iowait irq softirq steal (guest time is inside user).
            return sum(values[:3]) + sum(values[5:8])
    return None


def own_processes(processes: Dict[int, Tuple[int, str, int, int, str]], roots: Iterable[int]) -> List[int]:
    """The roots and every descendant of them in one reading of /proc."""
    own = set(roots)
    grew = True
    while grew:
        grew = False
        for pid, entry in processes.items():
            if entry[0] in own and pid not in own:
                own.add(pid)
                grew = True
    return sorted(own)


class HostWatch(threading.Thread):
    """The host's load during the measured window, so a job that starts mid-run is seen.

    The reading before the run (wait_for_quiet_host) can't see what starts
    after it: a CI job, a build, another worker's container.

    On Linux it does nothing heavy inside the window. prepare() reads /proc once,
    before k6 starts, to find the harness's own processes (its tree, the database
    container's, and the system Docker's plumbing for it). Each reading then takes
    /proc/stat, those processes' ticks, and the comm of any process that is new
    since the last reading: foreign CPU is the host's non-idle CPU less the
    harness's own, and a new Runner.Worker is a CI job. Only a reading that finds
    the host busy scans every process, to name them. The first version scanned
    every process every 10 s and lifted the run's tail (bench-checkpoint-prep's
    overhead control). On macOS each reading is host_load (ps).
    """

    def __init__(self, backend: "Backend", every_seconds: float = HOST_WATCH_SECONDS):
        super().__init__(daemon=True)
        self.backend = backend
        self.every = every_seconds
        self.halt = threading.Event()
        self.samples: List[dict] = []
        self.linux = platform.system() == "Linux"
        self.own: List[int] = []
        self.seen: set = set()
        self.ci_jobs: Dict[int, str] = {}

    def prepare(self, extra_own: Iterable[int] = ()) -> None:
        """Before the window: which processes are the harness's own, and which exist already."""
        if not self.linux:
            return
        processes = read_proc(cmdlines=[])
        named = {pid: read_cmdline(pid) for pid, entry in processes.items() if needs_cmdline(entry[1])}
        processes = {pid: (entry[:4] + (named.get(pid, ""),)) for pid, entry in processes.items()}
        try:
            container = self.backend.container_id()
        except (Failure, OSError, subprocess.SubprocessError):
            container = ""
        roots = [os.getpid(), *extra_own, *own_database_pid(self.backend),
                 *docker_plumbing(processes, self.backend.port, container)]
        self.own = own_processes(processes, roots)
        self.seen = set(processes)
        self.ci_jobs = {pid: entry[4] or entry[1] for pid, entry in processes.items() if is_ci_job(entry[1])}

    def linux_reading(self, last: Optional[Tuple[float, int, int]]) -> Tuple[dict, Tuple[float, int, int]]:
        now = time.monotonic()
        busy = host_busy_ticks(Path("/proc/stat").read_text(encoding="utf-8")) or 0
        own = sum(t for t in (proc_ticks(pid) for pid in self.own) if t is not None)
        current = set(int(name) for name in os.listdir("/proc") if name.isdigit())
        for pid in current - self.seen:
            try:
                comm = Path(f"/proc/{pid}/comm").read_text(encoding="utf-8").strip()
            except OSError:
                continue
            if is_ci_job(comm):
                self.ci_jobs[pid] = comm
        self.seen = current
        # A job counts while its Runner.Worker runs; one that ended between readings was seen when new.
        jobs = sorted(f"{pid} {command[:80]}" for pid, command in self.ci_jobs.items())
        self.ci_jobs = {pid: command for pid, command in self.ci_jobs.items() if pid in current}
        record: dict = {"ci_jobs": jobs, "processes_above_5pct": []}
        if last is None:
            record["foreign_cpu_pct"] = 0.0
            record["baseline"] = True
        else:
            seconds = now - last[0]
            foreign_ticks = (busy - last[1]) - (own - last[2])
            record["foreign_cpu_pct"] = max(0.0, 100.0 * foreign_ticks / os.sysconf("SC_CLK_TCK") / seconds) if seconds > 0 else 0.0
            if not host_is_quiet(record):
                # Busy already: name what is busy, as the reading before a run does.
                record["processes_above_5pct"] = host_load(self.backend, with_docker=False)["processes_above_5pct"]
        return record, (now, busy, own)

    def run(self) -> None:
        # Not during k6's start-up, when every VU starts at once.
        self.halt.wait(HOST_WATCH_FIRST_SECONDS)
        last = None
        while not self.halt.is_set():
            try:
                if self.linux:
                    record, last = self.linux_reading(last)
                else:
                    record = host_load(self.backend, with_docker=True)
            except Exception as error:  # A failed reading must not end the run.
                record = {"error": str(error)[:200], "foreign_cpu_pct": 0.0, "ci_jobs": [], "processes_above_5pct": []}
            record["t_s"] = round(time.monotonic(), 1)
            if not record.get("baseline"):
                self.samples.append(record)
            self.halt.wait(self.every)

    def stop(self) -> None:
        self.halt.set()
        self.join(timeout=30)

    def summary(self) -> dict:
        return host_watch_summary(self.samples)


def host_watch_summary(samples: Sequence[dict]) -> dict:
    """What the samples taken during a run say: its peak foreign CPU and whether it was ever busy."""
    readings = [s for s in samples if "error" not in s]
    busy = [s for s in readings if not host_is_quiet(s)]
    peak = max(readings, key=lambda s: s["foreign_cpu_pct"], default=None)
    jobs = sorted({job for s in readings for job in s.get("ci_jobs", [])})
    return {
        "host_watch_samples": len(readings),
        "host_watch_failed_samples": len(samples) - len(readings),
        "host_busy_during": bool(busy),
        "host_busy_during_samples": len(busy),
        "host_foreign_cpu_max_pct": peak["foreign_cpu_pct"] if peak else None,
        "host_peak_processes": peak["processes_above_5pct"][:8] if peak else [],
        "host_ci_jobs_during": jobs,
    }


def wait_for_quiet_host(cap_seconds: float, backend: "Backend") -> dict:
    """Wait, up to the cap, for a quiet host; return the last reading, marked busy if it never came."""
    started = time.monotonic()
    warned = False
    while True:
        record = host_load(backend)
        quiet = host_is_quiet(record)
        waited = time.monotonic() - started
        if quiet or waited >= cap_seconds:
            record.update(busy=not quiet, waited_s=round(waited, 1))
            if not quiet:
                log(f"warning: busy host, recorded: load {record['load_1m']:.1f}, foreign CPU {record['foreign_cpu_pct']:.0f}%"
                    + (f", CI jobs {record['ci_jobs']}" if record.get("ci_jobs") else ""))
            return record
        if not warned:
            log(f"host not quiet (load {record['load_1m']:.1f}, foreign CPU {record['foreign_cpu_pct']:.0f}%"
                + (f", CI jobs {record['ci_jobs']}" if record.get("ci_jobs") else "")
                + f"); waiting up to {cap_seconds:.0f} s")
            warned = True
        time.sleep(10)


# --- the database containers ---------------------------------------------------


TCP_STATES = {"01": "ESTABLISHED", "02": "SYN_SENT", "03": "SYN_RECV", "04": "FIN_WAIT1", "05": "FIN_WAIT2",
              "06": "TIME_WAIT", "07": "CLOSE", "08": "CLOSE_WAIT", "09": "LAST_ACK", "0A": "LISTEN", "0B": "CLOSING"}
# TIME-WAIT lasts 60 s on Linux (TCP_TIMEWAIT_LEN); the wait covers it with a margin.
PORT_WAIT_SECONDS = 90


def sockets_on_port(port: int, tables: Sequence[str]) -> List[str]:
    """The states of the sockets whose local port is `port`, from /proc/net/tcp's format."""
    states = []
    for text in tables:
        for line in text.splitlines()[1:]:
            fields = line.split()
            if len(fields) < 4 or ":" not in fields[1]:
                continue
            try:
                local_port = int(fields[1].rsplit(":", 1)[1], 16)
            except ValueError:
                continue
            if local_port == port:
                states.append(TCP_STATES.get(fields[3].upper(), fields[3]))
    return states


def held_by_client_sockets(port: int) -> List[str]:
    """On Linux, the sockets that would refuse a listener on `port`; elsewhere, none.

    The harness's database ports (59542, 55632) are inside Linux's ephemeral range
    (32768-60999), so any client socket on the machine can be given one, and in
    TIME-WAIT it refuses a listener for about 60 s, SO_REUSEADDR or not
    (ADR-aacruzgon-0082, 0086, measured on acruzdebian). Each reset binds the port
    again, so the harness waits for it to be free.
    """
    if platform.system() != "Linux":
        return []
    tables = []
    for name in ("/proc/net/tcp", "/proc/net/tcp6"):
        try:
            tables.append(Path(name).read_text(encoding="utf-8"))
        except OSError:
            continue
    return sockets_on_port(port, tables)


def wait_for_port(port: int, cap_seconds: float = PORT_WAIT_SECONDS) -> float:
    """Wait until no socket holds the port, up to the cap; return the seconds waited."""
    started = time.monotonic()
    held = held_by_client_sockets(port)
    if held:
        log(f"port {port} is held by {len(held)} socket(s) ({', '.join(sorted(set(held)))}); waiting up to {cap_seconds:.0f} s")
    while held:
        if time.monotonic() - started > cap_seconds:
            raise Failure(f"port {port} still held after {cap_seconds:.0f} s by sockets in {sorted(set(held))}")
        time.sleep(2)
        held = held_by_client_sockets(port)
    return time.monotonic() - started


class Backend:
    """One database under its own Compose project name."""

    name = ""
    service = ""
    compose_dir = ""
    volume_key = ""
    port_var = ""
    default_port = 0

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.profile = args.profile
        self.baseline = getattr(args, "baseline", "on")
        self.project = args.project or f"hfsbench-{self.name}"
        self.port = args.db_port or self.default_port
        self.port_wait_s = 0.0
        if self.project in FORBIDDEN_PROJECTS:
            raise Failure(f"refusing the shared Compose project {self.project}")
        if self.port in FORBIDDEN_PORTS:
            raise Failure(f"refusing the shared development port {self.port}")

    def files(self) -> List[Path]:
        return []

    @property
    def limit_gib(self) -> int:
        return 5 if self.profile == "5g" else 3

    @property
    def label(self) -> str:
        return self.name

    def compose_command(self) -> List[str]:
        command = ["docker", "compose", "-p", self.project]
        for path in self.files():
            command += ["-f", str(path)]
        return command

    def compose(self, *arguments: str, check: bool = True, timeout: float = 900,
                own_session: bool = False) -> subprocess.CompletedProcess:
        env = dict(os.environ)
        env[self.port_var] = str(self.port)
        return run_cmd(self.compose_command() + list(arguments), check=check, env=env, timeout=timeout, own_session=own_session)

    def exec_(self, *arguments: str, check: bool = True, timeout: float = 600) -> subprocess.CompletedProcess:
        return self.compose("exec", "-T", self.service, *arguments, check=check, timeout=timeout)

    def container_id(self) -> str:
        return self.compose("ps", "-q", self.service).stdout.strip()

    def reset(self, before_start=None) -> None:
        self.compose("down", "-v", "--remove-orphans", timeout=300)
        self._remove_data_volume()
        self._wait_for_budget()
        if before_start:
            before_start()
        self.up()
        self.prepare()

    def up(self) -> None:
        self.port_wait_s = wait_for_port(self.port)
        self.compose("up", "-d", "--wait", "--remove-orphans")

    def stop(self) -> None:
        # The cleanup after a stop, too: out of the terminal's reach (see run_cmd).
        self.compose("down", "-v", "--remove-orphans", check=False, timeout=300, own_session=True)
        self._remove_data_volume()

    def _remove_data_volume(self) -> None:
        # `down -v` skips a volume no container uses: one a restore created before a stop.
        run_cmd(["docker", "volume", "rm", "-f", self.data_volume], check=False, own_session=True)

    # Snapshots: a copy of the data volume taken with the database stopped, so
    # a restore is the same on both backends and needs no database command.
    @property
    def data_volume(self) -> str:
        return f"{self.project}_{self.volume_key}"

    def schema_files(self) -> List[Path]:
        """The files that make the database's schema before HFS writes to it."""
        return []

    def schema_sha256(self) -> str:
        digest = hashlib.sha256()
        for path in self.schema_files():
            digest.update(path.name.encode())
            digest.update(sha256_file(path).encode())
        return digest.hexdigest()

    def snapshot_volume(self, dataset: "Dataset", hfs_commit: str) -> str:
        # The backend is named: `--project` gives both backends one project. So
        # are the hfs commit that loaded it and the schema it was loaded into: a
        # snapshot from an older write path is never restored (RW6, ADR-sam-michaels-0006).
        return (f"{self.project}_snapshot-{self.name}-{dataset.size}-{dataset.sha256[:12]}-{hfs_commit[:12]}"
                f"-{self.schema_sha256()[:8]}")

    def snapshot_provenance(self, volume: str) -> Optional[dict]:
        """What the snapshot's marker says about how it was built, or None for an older marker."""
        probe = ["docker", "run", "--rm", "--network", "none", "--entrypoint", "cat", "-v", f"{volume}:/s:ro",
                 self._image(), f"/s/{SNAPSHOT_MARKER}"]
        result = run_cmd(probe, check=False, timeout=120)
        try:
            return json.loads(result.stdout) if result.returncode == 0 and result.stdout.strip() else None
        except ValueError:
            return None

    def verify_loaded(self) -> dict:
        """Checks on a freshly loaded database before its snapshot is kept."""
        return {}

    def has_snapshot(self, volume: str) -> bool:
        """True only for a finished copy: the marker is written after the copy succeeds."""
        if run_cmd(["docker", "volume", "inspect", volume], check=False).returncode != 0:
            return False
        probe = ["docker", "run", "--rm", "--network", "none", "--entrypoint", "sh", "-v", f"{volume}:/s:ro",
                 self._image(), "-c", f"test -f /s/{SNAPSHOT_MARKER}"]
        return run_cmd(probe, check=False, timeout=120).returncode == 0

    def _image(self) -> str:
        return self.compose("config", "--images").stdout.split()[0]

    def _copy_volume(self, source: str, destination: str, finish: str) -> None:
        name = f"{self.project}-volume-copy"
        if run_cmd(["docker", "volume", "inspect", source], check=False).returncode != 0:
            # `docker run -v` would create it empty, and a restore would then copy nothing.
            raise Failure(f"the volume {source} is gone (or Docker didn't answer), so there is nothing to copy")
        try:
            run_cmd(
                ["docker", "run", "--rm", "--name", name, "--network", "none", "--entrypoint", "sh",
                 "-v", f"{source}:/from:ro", "-v", f"{destination}:/to", self._image(),
                 "-c", f"find /to -mindepth 1 -delete && cp -a /from/. /to/ && {finish}"],
                timeout=1800,
            )
        except BaseException:
            # Killing `docker run` leaves its container copying, and holding both volumes.
            run_cmd(["docker", "rm", "-f", name], check=False, own_session=True)
            raise

    def quiesce(self) -> None:
        self.compose("stop", "-t", "120", timeout=300)

    def save_snapshot(self, volume: str, provenance: Optional[dict] = None) -> None:
        self.quiesce()
        run_cmd(["docker", "volume", "create", "--label", f"hfsbench.project={self.project}", volume])
        # The marker is written last, so its presence still means a finished copy;
        # it holds how the snapshot was made, which every run restoring it records.
        text = shlex.quote(json.dumps(provenance or {}, sort_keys=True))
        self._copy_volume(self.data_volume, volume, f"printf '%s' {text} > /to/{SNAPSHOT_MARKER}")

    def restore_snapshot(self, volume: str, before_start=None) -> None:
        self.quiesce()
        self._wait_for_budget()
        if before_start:
            before_start()
        self._copy_volume(volume, self.data_volume, f"rm -f /to/{SNAPSHOT_MARKER}")
        self.up()

    def drop_snapshots(self, volumes: Optional[Sequence[str]] = None) -> None:
        """Remove the named snapshots, or every snapshot of this project."""
        if volumes is None:
            listed = run_cmd(["docker", "volume", "ls", "-q", "--filter", f"label=hfsbench.project={self.project}"], check=False)
            volumes = listed.stdout.split()
        for volume in volumes:
            run_cmd(["docker", "volume", "rm", "-f", volume], check=False, own_session=True)

    def _wait_for_budget(self) -> None:
        budget = self.args.docker_budget_gib
        if not budget:
            return
        while True:
            lines = run_cmd(
                ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}"]
            ).stdout.splitlines()
            used = docker_limits_gib(lines)
            if used + self.limit_gib <= budget:
                return
            deadline = getattr(self.args, "deadline", None)
            wait = 180
            if deadline is not None:
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise BudgetExhausted("the Docker budget stayed full until the time budget ran out")
                wait = min(wait, max(1, remaining))
            log(f"docker budget: {used:.1f} + {self.limit_gib} GiB exceeds {budget}; waiting {wait:.0f} s")
            time.sleep(wait)

    def prepare(self) -> None:
        """Whatever the database needs before HFS starts."""

    def hfs_log_checks(self) -> Sequence[str]:
        """Lines hfs's log must hold once it is ready, or the run is not of the write path it names."""
        return ()

    def hfs_env(self) -> Dict[str, str]:
        raise NotImplementedError

    def settle(self, flush: bool) -> dict:
        raise NotImplementedError

    def settings(self) -> dict:
        raise NotImplementedError

    def collect(self, out_dir: Path) -> None:
        raise NotImplementedError

    def before_measure(self, out_dir: Path) -> None:
        """Just before the measured window: reset what can be reset, and read what can't for comparison."""

    # Read the database's counters before the warm-up (early_db_counters).
    db_counters_before_warmup = False

    def window_counters(self) -> dict:
        """The database's own counters (since its start) that the window is the difference of."""
        return {}

    def extra_collect(self, out_dir: Path) -> None:
        """Further readings after the window, kept as files (`--extra-metrics on`)."""


class Cassandra(Backend):
    name = "cassandra"
    service = "cassandra"
    volume_key = "cassandra-data"
    port_var = "CASSANDRA_HOST_PORT"
    default_port = 59542

    def files(self) -> List[Path]:
        base = REPO / "docker" / "cassandra"
        paths = [base / "docker-compose.yml"]
        if self.profile == "5g":
            paths.append(base / "docker-compose.heap2g.yml")
        return paths

    def schema_files(self) -> List[Path]:
        schema = REPO / "docs" / "Cassandra" / "schema"
        return [schema / "keyspace-single-node.cql", schema / "schema.cql"]

    def prepare(self) -> None:
        # A fresh node (`down -v` came first), so `helios` is created here, empty:
        # no keyspace written before Step 5.1 is ever measured (RW6).
        # schema.cql names no keyspace: it is applied inside the one created first.
        keyspace, schema = self.schema_files()
        for path, arguments in ((keyspace, []), (schema, ["-k", "helios"])):
            command = ["docker", "exec", "-i", self.container_id(), "cqlsh", *arguments]
            run_cmd(command, stdin_path=path, timeout=300)

    def verify_loaded(self) -> dict:
        # Step 5's write path writes index rows with every resource. A load whose
        # index tables are empty went through some other write path, and search
        # would miss every resource in it (RW6): refuse to keep it.
        found = {}
        for table in RW6_INDEX_TABLES:
            text = self.exec_("cqlsh", "-e", f"SELECT id FROM helios.{table} LIMIT 1;").stdout
            found[table] = bool(re.search(r"\((1) rows?\)", text))
        empty = [table for table, present in found.items() if not present]
        if empty:
            raise Failure(f"after the load, {', '.join(empty)} hold no rows: the index writes of Step 5 did not run")
        return {"index_tables_with_rows": found}

    def hfs_env(self) -> Dict[str, str]:
        # Every setting of the write path is named, at today's defaults, so a run
        # records what it ran with and an exported variable can't change it.
        # The sweeper runs with the server, as in production (ADR-sam-michaels-0006,
        # SW1). The request timeout must exceed the connect timeout or hfs refuses
        # to start (ADR-aacruzgon-0100).
        return {
            "HFS_STORAGE_BACKEND": "cassandra",
            "HFS_CASSANDRA_CONTACT_POINTS": f"127.0.0.1:{self.port}",
            "HFS_CASSANDRA_KEYSPACE": "helios",
            "HFS_CASSANDRA_LOCAL_DC": "helios-dc1",
            "HFS_CASSANDRA_CONNECT_TIMEOUT_MS": "5000",
            "HFS_CASSANDRA_REQUEST_TIMEOUT_MS": "10000",
            "HFS_CASSANDRA_SWEEPER_ENABLED": "true",
        }

    def hfs_log_checks(self) -> Sequence[str]:
        return (CASSANDRA_SWEEPER_STARTED,) if self.hfs_env().get("HFS_CASSANDRA_SWEEPER_ENABLED") == "true" else ()

    def settle(self, flush: bool) -> dict:
        if flush:
            self.exec_("nodetool", "flush", timeout=SETTLE_TIMEOUT_SECONDS)
        started = time.monotonic()
        while True:
            text = self.exec_("nodetool", "compactionstats").stdout
            match = re.search(r"pending tasks\D+(\d+)", text)
            pending = int(match.group(1)) if match else -1
            if pending == 0:
                return {"pending_compactions": 0, "seconds_waited": time.monotonic() - started}
            if time.monotonic() - started > SETTLE_TIMEOUT_SECONDS:
                raise Failure(f"pending compactions still {pending} after {SETTLE_TIMEOUT_SECONDS} s")
            time.sleep(2)

    def quiesce(self) -> None:
        # Drained, so the copy holds no commit log to replay. A node that is
        # already stopped (the second call in a restore) has nothing to drain.
        self.exec_("nodetool", "drain", check=False, timeout=300)
        super().quiesce()

    def state_digest(self) -> dict:
        names = self.exec_(
            "cqlsh", "-e", "SELECT table_name FROM system_schema.tables WHERE keyspace_name='helios';"
        ).stdout
        tables = [line.strip() for line in names.splitlines()[3:] if line.strip() and not line.strip().startswith("(")]
        digest: dict = {"tables": {}}
        for table in tables:
            exported = self.exec_(
                "cqlsh", "--request-timeout=900", "-e", f"COPY helios.{table} TO STDOUT WITH HEADER=false;",
                timeout=1800,
            ).stdout
            rows = sorted(line for line in exported.splitlines() if line and not line.startswith(("Using ", "Starting ", "Processed", "Rate ")))
            digest["tables"][table] = {
                "rows": len(rows),
                "sha256": hashlib.sha256("\n".join(rows).encode("utf-8")).hexdigest(),
            }
        return digest

    def settings(self) -> dict:
        wanted = (
            "paxos_variant", "paxos_state_purging", "commitlog_sync", "commitlog_sync_period",
            "commitlog_disk_access_mode", "memtable_allocation_type", "trickle_fsync",
            "cas_contention_timeout", "write_request_timeout", "read_request_timeout",
            "storage_compatibility_mode", "concurrent_compactors",
        )
        names = ",".join(f"'{name}'" for name in wanted)
        query = f"SELECT name, value FROM system_views.settings WHERE name IN ({names});"
        return {
            "config": self.exec_("cqlsh", "-e", query).stdout,
            "release": self.exec_("nodetool", "version").stdout.strip(),
            "info": self.exec_("nodetool", "info").stdout,
            "status": self.exec_("nodetool", "status").stdout,
        }

    def collect(self, out_dir: Path) -> None:
        for name, command in (
            ("proxyhistograms", ["nodetool", "proxyhistograms"]),
            ("tablestats", ["nodetool", "tablestats", "helios"]),
            ("compactionstats", ["nodetool", "compactionstats"]),
            ("gcstats", ["nodetool", "gcstats"]),
        ):
            result = self.exec_(*command, check=False)
            (out_dir / f"{name}.txt").write_text(result.stdout + result.stderr, encoding="utf-8")

    db_counters_before_warmup = True

    def window_counters(self) -> dict:
        return parse_tpstats(self.exec_("nodetool", "tpstats").stdout)

    def extra_collect(self, out_dir: Path) -> None:
        # tablehistograms are recent-decaying histograms over the node's life, so
        # they include the warm-up; compactionhistory lists every compaction since start.
        for name, command in (("tpstats", ["nodetool", "tpstats"]),
                              ("compactionhistory", ["nodetool", "compactionhistory"])):
            result = self.exec_(*command, check=False)
            (out_dir / f"{name}.txt").write_text(result.stdout + result.stderr, encoding="utf-8")
        # One table per call in 5.0 ("requires <keyspace> <table> or <keyspace.table>"),
        # all in one exec: the tables the operations read and write.
        script = "; ".join(f"echo '== helios.{table}'; nodetool tablehistograms helios.{table}"
                           for table in HISTOGRAM_TABLES)
        result = self.exec_("sh", "-c", script, check=False)
        (out_dir / "tablehistograms.txt").write_text(result.stdout + result.stderr, encoding="utf-8")

    def before_measure(self, out_dir: Path) -> None:
        # Only gcstats covers the window after this: it restarts on being read.
        # tablestats counts from the node's start, so the window is the reading
        # after the run less this one; proxyhistograms' percentiles can't be
        # subtracted, so the reading after the run still includes the warm-up.
        for name, command in (
            ("proxyhistograms", ["nodetool", "proxyhistograms"]),
            ("tablestats", ["nodetool", "tablestats", "helios"]),
            ("gcstats", ["nodetool", "gcstats"]),
        ):
            result = self.exec_(*command, check=False)
            (out_dir / f"{name}-before.txt").write_text(result.stdout + result.stderr, encoding="utf-8")


class Postgres(Backend):
    name = "postgres"
    service = "postgres"
    volume_key = "postgres-data"
    port_var = "PG_HOST_PORT"
    default_port = 55632

    def files(self) -> List[Path]:
        base = REPO / "docker" / "postgres"
        paths = [base / "docker-compose.yml"]
        if self.profile == "5g":
            paths.append(base / "docker-compose.mem5g.yml")
        if self.baseline == "off":
            paths.append(base / "docker-compose.async.yml")
        return paths

    @property
    def label(self) -> str:
        return f"postgres-sync-{self.baseline}"

    def hfs_env(self) -> Dict[str, str]:
        return {
            "HFS_STORAGE_BACKEND": "postgres",
            "HFS_DATABASE_URL": f"postgresql://helios:helios@127.0.0.1:{self.port}/helios",
            "HFS_PG_MAX_CONNECTIONS": "32",
        }

    def psql(self, sql: str) -> str:
        return self.exec_("psql", "-U", "helios", "-d", "helios", "-At", "-c", sql).stdout

    def state_digest(self) -> dict:
        tables = self.psql(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY 1;"
        ).split()
        digest: dict = {"tables": {}}
        for table in tables:
            row = self.psql(
                f"SELECT count(*), md5(coalesce(string_agg(h, '' ORDER BY h), '')) FROM (SELECT md5(t::text) AS h FROM {table} t) s;"
            ).strip()
            rows, checksum = row.split("|")
            digest["tables"][table] = {"rows": int(rows), "md5": checksum}
        return digest

    def settle(self, flush: bool) -> dict:
        # PostgreSQL's counterpart of a flush and of compaction reaching zero.
        started = time.monotonic()
        if flush:
            self.psql("CHECKPOINT;")
            self.psql("VACUUM (ANALYZE);")
        return {"checkpoint_vacuum": flush, "seconds_waited": time.monotonic() - started}

    def settings(self) -> dict:
        names = (
            "server_version", "synchronous_commit", "fsync", "wal_sync_method", "shared_buffers",
            "effective_cache_size", "maintenance_work_mem", "max_connections", "work_mem",
        )
        quoted = ",".join(f"'{name}'" for name in names)
        return {
            "pg_settings": self.psql(
                f"SELECT name, current_setting(name) FROM pg_settings WHERE name IN ({quoted}) ORDER BY name;"
            )
        }

    def window_counters(self) -> dict:
        query = (
            "SELECT json_build_object("
            "'db', (SELECT row_to_json(d) FROM (SELECT xact_commit, xact_rollback, blks_read, blks_hit, tup_inserted,"
            " tup_updated, tup_deleted, conflicts, temp_files, temp_bytes, deadlocks FROM pg_stat_database"
            " WHERE datname = 'helios') d),"
            "'wal', (SELECT row_to_json(w) FROM (SELECT wal_records, wal_fpi, wal_bytes FROM pg_stat_wal) w),"
            "'checkpointer', (SELECT row_to_json(c) FROM (SELECT num_timed, num_requested, num_done, buffers_written"
            " FROM pg_stat_checkpointer) c),"
            "'bgwriter', (SELECT row_to_json(b) FROM (SELECT buffers_clean, maxwritten_clean, buffers_alloc"
            " FROM pg_stat_bgwriter) b),"
            "'tables', (SELECT row_to_json(t) FROM (SELECT sum(n_tup_ins) AS n_tup_ins, sum(n_tup_upd) AS n_tup_upd,"
            " sum(n_dead_tup) AS n_dead_tup, sum(autovacuum_count) AS autovacuum_count,"
            " sum(autoanalyze_count) AS autoanalyze_count FROM pg_stat_user_tables) t),"
            "'lock_waits', (SELECT count(*) FROM pg_locks WHERE NOT granted));"
        )
        return json.loads(self.psql(query))

    def extra_collect(self, out_dir: Path) -> None:
        queries = {
            "pg_stat_user_tables": "SELECT relname, seq_scan, idx_scan, n_tup_ins, n_tup_upd, n_tup_hot_upd, n_live_tup, n_dead_tup, autovacuum_count, autoanalyze_count FROM pg_stat_user_tables ORDER BY n_tup_ins + n_tup_upd DESC;",
            "pg_stat_wal": "SELECT * FROM pg_stat_wal;",
            "pg_stat_bgwriter": "SELECT * FROM pg_stat_bgwriter;",
            "pg_stat_checkpointer": "SELECT * FROM pg_stat_checkpointer;",
            "pg_locks_waiting": "SELECT locktype, mode, count(*) FROM pg_locks WHERE NOT granted GROUP BY 1, 2;",
        }
        for name, sql in queries.items():
            result = self.exec_("psql", "-U", "helios", "-d", "helios", "-c", sql, check=False)
            (out_dir / f"{name}.txt").write_text(result.stdout + result.stderr, encoding="utf-8")

    def before_measure(self, out_dir: Path) -> None:
        # The statements of the load and the warm-up go; the database's own
        # counters are kept, for autovacuum's sake, and read before and after.
        reset = self.exec_("psql", "-U", "helios", "-d", "helios", "-At", "-c", "SELECT pg_stat_statements_reset();", check=False)
        if reset.returncode != 0:
            log(f"warning: pg_stat_statements_reset failed: {reset.stderr[-200:]}")
        result = self.exec_("psql", "-U", "helios", "-d", "helios", "-c", "SELECT * FROM pg_stat_database WHERE datname = 'helios';", check=False)
        (out_dir / "pg_stat_database-before.txt").write_text(result.stdout + result.stderr, encoding="utf-8")

    def collect(self, out_dir: Path) -> None:
        queries = {
            "pg_stat_statements": "SELECT calls, round(total_exec_time::numeric, 1), round(mean_exec_time::numeric, 3), left(query, 120) FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 25;",
            "pg_stat_database": "SELECT * FROM pg_stat_database WHERE datname = 'helios';",
        }
        for name, sql in queries.items():
            result = self.exec_("psql", "-U", "helios", "-d", "helios", "-c", sql, check=False)
            (out_dir / f"{name}.txt").write_text(result.stdout + result.stderr, encoding="utf-8")


# --- HFS ------------------------------------------------------------------------


CASSANDRA_SWEEPER_STARTED = "Cassandra index sweeper started"


def hfs_environment(port: int, backend_env: Dict[str, str]) -> Dict[str, str]:
    """What hfs runs with: the harness's settings only, never an HFS_* exported in the shell."""
    settings = dict(backend_env)
    settings.update(
        {
            "HFS_SERVER_HOST": "127.0.0.1",
            "HFS_SERVER_PORT": str(port),
            "HFS_LOG_LEVEL": "warn",
            # warn, except hfs's own startup lines: they say which background tasks started.
            "RUST_LOG": "warn,hfs=info",
            "HFS_BASE_URL": f"http://127.0.0.1:{port}",
            # The spec bundles, without which HFS seeds no SearchParameters.
            "HFS_DATA_DIR": str(REPO / "data"),
        }
    )
    return settings


def hfs_process_env(port: int, backend_env: Dict[str, str], inherited: Dict[str, str]) -> Dict[str, str]:
    """The process environment: the shell's, less any HFS_*, RUST_LOG or OTEL_* in it, plus hfs_environment."""
    env = {key: value for key, value in inherited.items() if not key.startswith(("HFS_", "RUST_LOG", "OTEL_", "HELIOS_"))}
    env.update(hfs_environment(port, backend_env))
    return env


class Hfs:
    def __init__(self, binary: Path, port: int, log_path: Path, backend_env: Dict[str, str],
                 log_checks: Sequence[str] = ()):
        env = hfs_process_env(port, backend_env, os.environ)
        self.log_checks = tuple(log_checks)
        with socket.socket() as probe:
            probe.settimeout(1)
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                raise Failure(f"something is already listening on port {port}; load would go to it")
        self.base = f"http://127.0.0.1:{port}"
        self.log_path = log_path
        self.log_file = log_path.open("w", encoding="utf-8")
        self.process = subprocess.Popen([str(binary)], env=env, stdout=self.log_file, stderr=subprocess.STDOUT)
        CLEANUP["processes"][self.process.pid] = "hfs"
        try:
            self._wait_ready()
        except BaseException:  # A signal too: nothing else knows about this process yet.
            self.stop()
            raise

    def _wait_ready(self, timeout: float = 300) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise Failure(f"hfs exited {self.process.returncode} at boot: {self.tail()}")
            try:
                with urllib.request.urlopen(f"{self.base}/metadata", timeout=10) as response:
                    if response.status == 200 and self.process.poll() is None:
                        self._check_seeded()
                        return
            except (urllib.error.URLError, OSError):
                pass
            time.sleep(0.5)
        raise Failure(f"hfs not ready after {timeout} s: {self.tail()}")

    def _check_seeded(self) -> None:
        # Without the spec bundles HFS answers searches without indexing
        # anything, which would invalidate every number silently.
        log_text = self.log_path.read_text(encoding="utf-8")
        for phrase in ("Could not load spec SearchParameters", "no spec bundle loaded"):
            if phrase in log_text:
                raise Failure(f"hfs started without its spec bundles ({phrase}): check HFS_DATA_DIR")
        for phrase in self.log_checks:
            if phrase not in log_text:
                raise Failure(f"hfs is ready but its log lacks {phrase!r}: not the write path this run names")

    def tail(self) -> str:
        try:
            return self.log_path.read_text(encoding="utf-8")[-600:]
        except OSError:
            return ""

    def stop(self) -> None:
        stop_process(self.process)
        CLEANUP["processes"].pop(self.process.pid, None)
        self.log_file.close()


def fetch(url: str, method: str = "GET", body: Optional[bytes] = None, timeout: float = 60) -> Tuple[int, str, Optional[dict]]:
    """One FHIR request: the status, the start of the body, and the parsed body if it is JSON."""
    headers = {"Accept": "application/fhir+json", "Content-Type": "application/fhir+json"}
    request = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            status = response.status
    except urllib.error.HTTPError as error:
        raw = error.read()
        status = error.code
    try:
        parsed = json.loads(raw) if raw else None
    except ValueError:
        parsed = None
    return status, raw[:600].decode("utf-8", "replace"), parsed


def search_once(url: str, timeout: float = 60) -> Tuple[int, str, Optional[int]]:
    """GET a search; return its status, the start of its body, and how many entries it matched."""
    request = urllib.request.Request(url, headers={"Accept": "application/fhir+json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read()
            return response.status, body[:600].decode("utf-8", "replace"), len(json.loads(body).get("entry", []))
    except urllib.error.HTTPError as error:
        return error.code, error.read(600).decode("utf-8", "replace"), None


# --- datasets --------------------------------------------------------------------


def next_month(month: str) -> str:
    year, number = (int(part) for part in month.split("-"))
    return f"{year + 1:04d}-01" if number == 12 else f"{year:04d}-{number + 1:02d}"


def build_search_params(dataset: Path) -> Tuple[List[str], Dict[str, List[str]]]:
    """Read one dataset and return its ids and the fixed query set for each search.

    The queries are chosen deterministically from the dataset's own values
    (most frequent first, ties by value), so both backends and every machine
    run the same list. Only the first page of each is fetched.
    """
    ids: List[str] = []
    codes: collections.Counter = collections.Counter()
    months: collections.Counter = collections.Counter()
    profiles: collections.Counter = collections.Counter()
    with dataset.open(encoding="utf-8") as stream:
        for line in stream:
            resource = json.loads(line)
            kind = resource["resourceType"]
            ids.append(f"{kind}/{resource['id']}")
            if kind != "Observation":
                continue
            coding = resource["code"]["coding"][0]
            codes[f"{coding['system']}|{coding['code']}"] += 1
            when = resource.get("effectiveDateTime") or resource.get("issued")
            if when:
                months[when[:7]] += 1
            for url in resource.get("meta", {}).get("profile", []):
                profiles[url] += 1

    def top(counter: collections.Counter, limit: int) -> List[str]:
        return [k for k, _ in sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))[:limit]]

    def encode(value: str) -> str:
        return urllib.request.quote(value, safe="")

    return ids, {
        "token": [f"Observation?code={encode(code)}&_count=20" for code in top(codes, 20)],
        "date": [
            f"Observation?date=ge{m}-01&date=lt{next_month(m)}-01&_count=20" for m in top(months, 12)
        ],
        "uri": [f"Observation?_profile={encode(url)}&_count=20" for url in top(profiles, 20)],
    }


def published_sha256(sums: Path, name: str) -> Optional[str]:
    """The checksum SHA256SUMS records for one file name, or None."""
    if not sums.is_file():
        return None
    for line in sums.read_text(encoding="utf-8").splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[1].lstrip("*") == name:
            return fields[0].lower()
    return None


def verify_dataset(path: Path, sums: Path) -> str:
    """Return the file's SHA-256 if it is exactly the published file; refuse otherwise.

    Hashing only. Whether a machine can regenerate the published bytes is
    `generate_datasets.py --verify`, a separate step.
    """
    actual = sha256_file(path) if path.is_file() else None
    expected = published_sha256(sums, path.name)
    if expected is None or actual != expected:
        if expected is not None:
            expected_text = expected
        elif sums.is_file():
            expected_text = f"none ({sums} has no line for {path.name})"
        else:
            expected_text = f"none ({sums} does not exist; see --sha256sums)"
        raise Failure(
            f"refusing dataset {path}: expected sha256 {expected_text}, "
            f"actual {actual or 'none (the file is missing)'}"
        )
    return actual


class Dataset:
    """The dataset of one size and the files derived from it."""

    def __init__(self, size: int, directory: Path, prepared: Path, batches_tool: Path,
                 sums: Path, verified: bool = True):
        self.size = size
        self.path = directory / f"patient-observation-{size}.ndjson"
        self.sums = sums
        self.verified = verified
        self.sha256 = self._current_sha256()
        # Named by hash, so files derived from other bytes are never reused.
        self.root = prepared / f"{size}-{self.sha256[:12]}"
        self.ids_file = self.root / "ids.txt"
        self.search_file = self.root / "search.json"
        self.batches = self.root / "batches"
        self.manifest = self.root / "derived.sha256"
        self.tool_sha256 = sha256_file(batches_tool)
        if not self._derived_intact():
            if self.root.exists():
                log(f"rebuilding {self.root}: it is partial, or not what this dataset and tool produce")
                shutil.rmtree(self.root)
            self.root.mkdir(parents=True)
            ids, queries = build_search_params(self.path)
            self.ids_file.write_text("\n".join(ids) + "\n", encoding="utf-8")
            self.search_file.write_text(json.dumps(queries, indent=1), encoding="utf-8")
            run_cmd([sys.executable, str(batches_tool), str(self.path), str(self.batches)])
            self.manifest.write_text(self._derived_digest() + "\n", encoding="utf-8")

    def _derived_digest(self) -> str:
        digest = hashlib.sha256(self.tool_sha256.encode())
        for path in [self.ids_file, self.search_file, *sorted(self.batches.glob("batch-*.json"))]:
            digest.update(path.name.encode())
            digest.update(sha256_file(path).encode())
        return digest.hexdigest()

    def _derived_intact(self) -> bool:
        if not (self.manifest.is_file() and self.ids_file.is_file() and self.search_file.is_file()):
            return False
        return self.manifest.read_text(encoding="utf-8").strip() == self._derived_digest()

    def _current_sha256(self) -> str:
        if self.verified:
            return verify_dataset(self.path, self.sums)
        if not self.path.is_file():
            raise Failure(f"no dataset at {self.path}")
        return sha256_file(self.path)

    def check_before_load(self) -> None:
        """Hash the bytes about to be loaded, so every run names exactly its data."""
        current = self._current_sha256()
        if current != self.sha256:
            raise Failure(f"refusing dataset {self.path}: expected sha256 {self.sha256}, actual {current}")
        if not self._derived_intact():
            raise Failure(f"refusing the files derived from {self.path}: {self.root} changed after it was built")


def post_batch(base: str, path: Path) -> collections.Counter:
    request = urllib.request.Request(
        f"{base}/",
        data=path.read_bytes(),
        headers={"Content-Type": "application/fhir+json", "Accept": "application/fhir+json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            bundle = json.loads(response.read())
    except urllib.error.HTTPError as error:
        raise Failure(f"{path.name}: HTTP {error.code}: {error.read(400)!r}")
    # A batch answers 200 even when entries fail (docs/Cassandra/datasets/README.md).
    return collections.Counter(entry["response"]["status"][:3] for entry in bundle["entry"])


def load_dataset(base: str, dataset: Dataset) -> dict:
    files = sorted(dataset.batches.glob("batch-*.json"))
    started = time.monotonic()
    totals: collections.Counter = collections.Counter()
    with ThreadPoolExecutor(max_workers=BATCH_WORKERS) as pool:
        try:
            for counts in pool.map(lambda path: post_batch(base, path), files):
                totals.update(counts)
        except BaseException:  # A signal: don't send the batches still queued.
            pool.shutdown(wait=False, cancel_futures=True)
            raise
    failed = sum(v for k, v in totals.items() if not k.startswith("2"))
    loaded = sum(totals.values())
    seconds = time.monotonic() - started
    if failed or loaded != dataset.size:
        raise Failure(f"load: {loaded} entries of {dataset.size}, statuses {dict(totals)}")
    return {"seconds": seconds, "bundles": len(files), "statuses": dict(totals)}


# --- what the run ran on ------------------------------------------------------


def git_value(*arguments: str) -> str:
    result = run_cmd(["git", "-C", str(REPO), *arguments], check=False)
    return result.stdout.strip()


def machine_record() -> dict:
    system = platform.system()
    record: dict = {
        "system": system,
        "release": platform.release(),
        "machine": platform.machine(),
        "hostname": platform.node(),
        "cpus": os.cpu_count(),
    }
    if system == "Darwin":
        record["cpu_model"] = run_cmd(["sysctl", "-n", "machdep.cpu.brand_string"], check=False).stdout.strip()
        record["ram_bytes"] = int(run_cmd(["sysctl", "-n", "hw.memsize"], check=False).stdout.strip() or 0)
        record["os"] = " ".join(run_cmd(["sw_vers"], check=False).stdout.split())
    else:
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("model name"):
                record["cpu_model"] = line.split(":", 1)[1].strip()
                break
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                record["ram_bytes"] = int(line.split()[1]) * 1024
                break
        record["os"] = Path("/etc/os-release").read_text(encoding="utf-8").split("\n", 1)[0]
        record["kernel"] = platform.version()
        record["cpu_governor"] = read_first(Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"))
    record["disk"] = disk_record()
    return record


def read_first(path: Path) -> Optional[str]:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def disk_record() -> dict:
    """Where the databases' volumes live: Docker's root, its file system, and on Linux the device behind it."""
    root = run_cmd(["docker", "info", "--format", "{{.DockerRootDir}}"], check=False).stdout.strip()
    record: dict = {"docker_root": root or None}
    if root and platform.system() == "Linux":
        df = run_cmd(["df", "--output=source,fstype,size,avail", root], check=False).stdout.split("\n")
        if len(df) > 1 and df[1].split():
            source, fstype, size, avail = (df[1].split() + ["", "", "", ""])[:4]
            record.update(source=source, fstype=fstype, size_kib=size, avail_kib=avail)
            device = Path(source).name
            # nvme0n1p6 -> nvme0n1, sda2 -> sda
            base = re.sub(r"p\d+$", "", device) if device.startswith("nvme") else re.sub(r"\d+$", "", device)
            record["model"] = read_first(Path(f"/sys/block/{base}/device/model"))
            record["rotational"] = read_first(Path(f"/sys/block/{base}/queue/rotational"))
    return record


def environment_record(args: argparse.Namespace, backend: Backend) -> dict:
    docker = run_cmd(
        ["docker", "info", "--format", "{{.ServerVersion}}|{{.OperatingSystem}}|{{.NCPU}}|{{.MemTotal}}"],
        check=False,
    ).stdout.strip()
    k6 = run_cmd([str(args.k6), "version"], check=False).stdout.strip()
    profile = args.build_profile or ("release" if "release" in str(args.hfs_bin) else "unknown")
    version = run_cmd([str(args.hfs_bin), "--version"], check=False).stdout.strip()
    # The binary's own commit, since the checkout may have moved after the build.
    built = re.search(r"\(git ([0-9a-f]+)\)", version)
    return {
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "hfs_commit": built.group(1) if built else git_value("rev-parse", "HEAD"),
        "harness_commit": git_value("rev-parse", "HEAD"),
        "hfs_tree_dirty": bool(git_value("status", "--porcelain", "--", "crates")),
        "hfs_build_profile": profile,
        "hfs_binary": str(args.hfs_bin),
        "hfs_version": version,
        "k6": k6,
        "machine": machine_record(),
        "docker": docker,
        "backend": backend.label,
        "profile": backend.profile,
        "compose_files": [str(p.relative_to(REPO)) for p in backend.files()],
        "compose_sha256": {str(p.relative_to(REPO)): sha256_file(p) for p in backend.files()},
        "schema_sha256": backend.schema_sha256() if backend.schema_files() else None,
        "hfs_environment": hfs_environment(args.hfs_port, backend.hfs_env()),
        "db_port": backend.port,
        "max_vus_factor": getattr(args, "max_vus_factor", 1),
        "project": backend.project,
        "operating_note": args.note or "",
    }


def container_record(container: str) -> dict:
    fields = run_cmd(
        [
            "docker", "inspect", "--format",
            "{{.Image}}|{{.Config.Image}}|{{.HostConfig.Memory}}|{{.HostConfig.MemorySwap}}|{{.HostConfig.NanoCpus}}|{{.State.OOMKilled}}",
            container,
        ]
    ).stdout.strip().split("|")
    return {
        "image_id": fields[0],
        "image": fields[1],
        "mem_limit_bytes": int(fields[2]),
        "memswap_limit_bytes": int(fields[3]),
        "nano_cpus": int(fields[4]),
        "oom_killed": fields[5] == "true",
    }


# --- memory sampling ------------------------------------------------------------


class MemorySampler(threading.Thread):
    """Sample one container's cgroup and two processes' RSS while a run is measured.

    memory.peak alone is not enough: it counts page cache and reads the limit
    on any database that has read more than the limit (ADR-aacruzgon-0002's
    check; the bench study measured it on PostgreSQL). `used_bytes` is what
    `docker stats` reports, memory.current less inactive_file.
    """

    FIELDS = ["t_s", "current_bytes", "used_bytes", "anon_bytes", "file_bytes", "hfs_rss_kib", "k6_rss_kib"]

    def __init__(self, container: str, pids: Dict[str, int], out_path: Path):
        super().__init__(daemon=True)
        self.container = container
        self.pids = pids
        self.out_path = out_path
        self.halt = threading.Event()
        self.missed = 0
        self.peaks = {"used_bytes": 0, "anon_bytes": 0, "current_bytes": 0, "hfs_rss_kib": 0, "k6_rss_kib": 0}
        # The container's CPU and I/O counters at the first and the last good sample: the
        # window's cgroup figures come from the sampler's own reads, so no extra
        # `docker exec` touches the container at the window's edge (overhead control).
        self.cgroup_first: Optional[Dict[str, int]] = None
        self.cgroup_last: Optional[Dict[str, int]] = None
        self.cgroup_first_t: Optional[float] = None
        self.cgroup_last_t: Optional[float] = None

    def sample_container(self) -> Dict[str, int]:
        script = ("cat /sys/fs/cgroup/memory.current; cat /sys/fs/cgroup/memory.stat; echo '==cpu'; "
                  "cat /sys/fs/cgroup/cpu.stat; echo '==io'; cat /sys/fs/cgroup/io.stat")
        try:
            result = run_cmd(["docker", "exec", self.container, "sh", "-c", script], check=False, timeout=20)
        except (subprocess.SubprocessError, OSError):
            self.missed += 1
            return {}
        if result.returncode != 0:
            self.missed += 1
            return {}
        memory, _, rest = result.stdout.partition("==cpu")
        cpu, _, io = rest.partition("==io")
        lines = memory.splitlines()
        try:
            current = int(lines[0])
            stat = dict(line.split() for line in lines[1:] if line.strip())
            counters = dict(parse_cpu_stat(cpu), **parse_io_stat(io))
            if "usage_usec" in counters:
                now = time.monotonic()
                if self.cgroup_first is None:
                    self.cgroup_first, self.cgroup_first_t = counters, now
                self.cgroup_last, self.cgroup_last_t = counters, now
            return {
                "current_bytes": current,
                "used_bytes": current - int(stat.get("inactive_file", 0)),
                "anon_bytes": int(stat.get("anon", 0)),
                "file_bytes": int(stat.get("file", 0)),
            }
        except (IndexError, ValueError):
            # An answer cut short (a Ctrl-C reaches `docker exec` too) is a missed
            # sample, not the end of the sampling thread.
            self.missed += 1
            return {}

    def rss(self, pid: int) -> int:
        result = run_cmd(["ps", "-o", "rss=", "-p", str(pid)], check=False)
        text = result.stdout.strip()
        return int(text) if text.isdigit() else 0

    def run(self) -> None:
        started = time.monotonic()
        with self.out_path.open("w", encoding="utf-8", newline="") as sink:
            writer = csv.writer(sink)
            writer.writerow(self.FIELDS)
            while not self.halt.is_set():
                row: Dict[str, object] = dict(self.sample_container())
                row["t_s"] = f"{time.monotonic() - started:.1f}"
                row["hfs_rss_kib"] = self.rss(self.pids["hfs"])
                row["k6_rss_kib"] = self.rss(self.pids["k6"])
                writer.writerow([row.get(field, "") for field in self.FIELDS])
                sink.flush()
                for key in self.peaks:
                    self.peaks[key] = max(self.peaks[key], int(row.get(key, 0) or 0))
                self.halt.wait(SAMPLE_SECONDS)

    def stop(self) -> None:
        self.halt.set()
        self.join(timeout=30)

    def memory_peak_file(self) -> Optional[int]:
        result = run_cmd(["docker", "exec", self.container, "cat", "/sys/fs/cgroup/memory.peak"], check=False)
        text = result.stdout.strip()
        return int(text) if text.isdigit() else None


# --- counters read before and after the measured window -------------------------
# Everything here is report-only: a reading that fails is left out, and no
# figure changes a run's status. `--extra-metrics off` skips all of it, and
# the host watch, for the overhead control.


def parse_cpu_stat(text: str) -> Dict[str, int]:
    """cgroup v2 cpu.stat: usage_usec, user_usec, system_usec, nr_periods, nr_throttled, throttled_usec."""
    values = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) == 2 and fields[1].lstrip("-").isdigit():
            values[fields[0]] = int(fields[1])
    return values


def parse_io_stat(text: str) -> Dict[str, int]:
    """cgroup v2 io.stat, summed over devices: rbytes, wbytes, rios, wios."""
    totals = collections.Counter()
    for line in text.splitlines():
        for field in line.split()[1:]:
            key, _, value = field.partition("=")
            if key in ("rbytes", "wbytes", "rios", "wios") and value.isdigit():
                totals[key] += int(value)
    return dict(totals)


def parse_tpstats(text: str) -> Dict[str, int]:
    """nodetool tpstats: each pool's counters and each message type's dropped count, since the node started."""
    values: Dict[str, int] = {}
    section = None
    for line in text.splitlines():
        fields = line.split()
        if not fields:
            continue
        if fields[0] == "Pool" and "Name" in fields:
            section = "pool"
            continue
        if fields[0] == "Message" and len(fields) > 1 and fields[1] == "type":
            section = "dropped"
            continue
        if section == "pool" and len(fields) >= 6 and all(f.lstrip("-").isdigit() for f in fields[-5:]):
            for key, value in zip(("active", "pending", "completed", "blocked", "all_time_blocked"), fields[-5:]):
                values[f"pool.{fields[0]}.{key}"] = int(value)
        elif section == "dropped" and len(fields) >= 2 and fields[1].isdigit() and fields[0].isupper():
            values[f"dropped.{fields[0]}"] = int(fields[1])
    return values


def flatten(prefix: str, value, into: Dict[str, float]) -> None:
    if isinstance(value, dict):
        for key, inner in value.items():
            flatten(f"{prefix}.{key}" if prefix else str(key), inner, into)
    elif isinstance(value, bool):
        return
    elif isinstance(value, (int, float)):
        into[prefix] = value
    elif isinstance(value, str):
        try:
            into[prefix] = float(value)
        except ValueError:
            pass


def process_cpu_seconds(pid: int) -> Optional[float]:
    """A process's CPU time so far (user and system), or None."""
    if platform.system() == "Linux":
        try:
            parsed = parse_proc_stat(Path(f"/proc/{pid}/stat").read_text(encoding="utf-8", errors="replace"))
        except OSError:
            return None
        return parsed[2] / os.sysconf("SC_CLK_TCK") if parsed else None
    text = run_cmd(["ps", "-o", "cputime=", "-p", str(pid)], check=False).stdout.strip()
    try:
        seconds = 0.0
        for part in text.replace("-", ":").split(":"):
            seconds = seconds * 60 + float(part)
        return seconds if text else None
    except ValueError:
        return None


def host_counters() -> Dict[str, float]:
    """Linux: context switches and the CPU's steal and iowait since boot, and the cores' mean frequency now."""
    values: Dict[str, float] = {}
    if platform.system() != "Linux":
        return values
    try:
        for line in Path("/proc/stat").read_text(encoding="utf-8").splitlines():
            fields = line.split()
            if fields[0] == "ctxt":
                values["ctxt"] = int(fields[1])
            elif fields[0] == "cpu" and len(fields) >= 9:
                ticks = os.sysconf("SC_CLK_TCK")
                values["iowait_s"] = int(fields[5]) / ticks
                values["steal_s"] = int(fields[8]) / ticks
    except OSError:
        pass
    frequencies = []
    for path in Path("/sys/devices/system/cpu").glob("cpu[0-9]*/cpufreq/scaling_cur_freq"):
        text = read_first(path)
        if text and text.isdigit():
            frequencies.append(int(text) / 1000.0)
    if frequencies:
        values["cpu_mhz_mean"] = statistics.fmean(frequencies)
    return values


def read_window(backend: "Backend", hfs: "Hfs", container: str, db: Optional[dict] = None) -> Dict[str, float]:
    """One reading of every counter the window is measured by (taken before and after it).

    db, when given, is the database's own reading taken earlier (early_db_counters).
    """
    values: Dict[str, float] = {}
    # The container's cgroup counters are not read here: the memory sampler reads them
    # during the window (MemorySampler.cgroup_first, cgroup_last).
    readings = (
        ("db", (lambda: db) if db is not None else backend.window_counters),
        ("host", host_counters),
        ("hfs", lambda: {"cpu_s": process_cpu_seconds(hfs.process.pid)}),
    )
    for prefix, read in readings:
        try:
            flatten(prefix, read(), values)
        except (Failure, OSError, subprocess.SubprocessError, ValueError, KeyError) as error:
            values[f"{prefix}.error"] = 1
            log(f"warning: the {prefix} reading for the window failed: {str(error)[:200]}")
    return values


def window_delta(before: Dict[str, float], after: Dict[str, float]) -> Dict[str, float]:
    """after less before, for every counter in both; gauges (frequency, pending) are kept as read after."""
    delta = {}
    for key, value in after.items():
        if key in before and not key.endswith((".error",)):
            delta[key] = value - before[key]
    return delta


# The columns of runs.csv taken from the window's readings: (column, key, scale, kind).
# kind "delta" is after less before; "after" is the reading after the window.
WINDOW_COLUMNS = (
    ("db_cpu_s", "cgroup.usage_usec", 1e-6, "delta"),
    ("db_nr_throttled", "cgroup.nr_throttled", 1, "delta"),
    ("db_throttled_s", "cgroup.throttled_usec", 1e-6, "delta"),
    ("db_io_read_mib", "cgroup.rbytes", 1 / 1024**2, "delta"),
    ("db_io_write_mib", "cgroup.wbytes", 1 / 1024**2, "delta"),
    ("db_io_read_ops", "cgroup.rios", 1, "delta"),
    ("db_io_write_ops", "cgroup.wios", 1, "delta"),
    ("hfs_cpu_s", "hfs.cpu_s", 1, "delta"),
    ("host_ctxt_switches", "host.ctxt", 1, "delta"),
    ("host_steal_s", "host.steal_s", 1, "delta"),
    ("host_iowait_s", "host.iowait_s", 1, "delta"),
    ("host_cpu_mhz_mean", "host.cpu_mhz_mean", 1, "after"),
    ("pg_xact_commit", "db.db.xact_commit", 1, "delta"),
    ("pg_xact_rollback", "db.db.xact_rollback", 1, "delta"),
    ("pg_blks_read", "db.db.blks_read", 1, "delta"),
    ("pg_blks_hit", "db.db.blks_hit", 1, "delta"),
    ("pg_deadlocks", "db.db.deadlocks", 1, "delta"),
    ("pg_temp_files", "db.db.temp_files", 1, "delta"),
    ("pg_conflicts", "db.db.conflicts", 1, "delta"),
    ("pg_wal_records", "db.wal.wal_records", 1, "delta"),
    ("pg_wal_mib", "db.wal.wal_bytes", 1 / 1024**2, "delta"),
    ("pg_checkpoints", "db.checkpointer.num_done", 1, "delta"),
    ("pg_autovacuum_runs", "db.tables.autovacuum_count", 1, "delta"),
    ("pg_dead_tuples_after", "db.tables.n_dead_tup", 1, "after"),
    ("pg_lock_waits_after", "db.lock_waits", 1, "after"),
)


def window_columns(window: dict) -> dict:
    """The runs.csv figures from a window record; Cassandra's dropped messages are summed over types."""
    if not window:
        return {}
    before, after, delta = window.get("before", {}), window.get("after", {}), window.get("delta", {})
    row = {}
    for column, key, scale, kind in WINDOW_COLUMNS:
        source = delta if kind == "delta" else after
        if key in source:
            row[column] = source[key] * scale
    dropped = [value for key, value in delta.items() if key.startswith("db.dropped.")]
    if dropped or any(key.startswith("db.pool.") for key in after):
        row["cassandra_dropped_messages"] = sum(dropped)
        row["cassandra_pending_after"] = sum(v for k, v in after.items() if k.startswith("db.pool.") and k.endswith(".pending"))
        row["cassandra_blocked"] = sum(v for k, v in delta.items() if k.startswith("db.pool.") and k.endswith(".all_time_blocked"))
    return row


def scrape_metrics(hfs: "Hfs", path: Path) -> None:
    """hfs's Prometheus /metrics (helios-observability), saved as served; nothing if it isn't."""
    try:
        with urllib.request.urlopen(f"{hfs.base}/metrics", timeout=10) as response:
            path.write_bytes(response.read())
    except (urllib.error.URLError, OSError) as error:
        path.write_text(f"# not scraped: {error}\n", encoding="utf-8")


# --- k6 and one measured run ---------------------------------------------------


def k6_command(args: argparse.Namespace, op: str, base: str, vus: int, mode: str, rate: int,
               seconds: int, dataset: Dataset, csv_out: Optional[Path]) -> List[str]:
    command = [str(args.k6), "run", "--quiet", "--no-usage-report"]
    if csv_out is not None:
        command += ["--out", f"csv={csv_out}"]
    command += ["--system-tags", "status,name,error_code"]
    environment = {
        "OP": op, "BASE_URL": base, "VUS": vus, "MAX_VUS": max_vus(args, vus), "MODE": mode, "RATE": rate,
        "DURATION": f"{seconds}s", "IDS_FILE": dataset.ids_file,
        "BODIES_FILE": dataset.path, "SEARCH_FILE": dataset.search_file,
    }
    for key, value in environment.items():
        command += ["--env", f"{key}={value}"]
    return command + [str(HERE / "load.js")]


def max_vus(args: argparse.Namespace, vus: int) -> int:
    """k6's maxVUs on an open-loop run: the concurrency, times --max-vus-factor (1 unless chosen)."""
    return vus * max(1, int(getattr(args, "max_vus_factor", 1) or 1))


def k6_environment() -> Dict[str, str]:
    env = dict(os.environ)
    env["K6_CSV_TIME_FORMAT"] = "unix_milli"
    return env


def early_db_counters(args: argparse.Namespace, backend: "Backend") -> Optional[dict]:
    """The database counters a backend must read before the warm-up rather than just before the window.

    Cassandra's `nodetool tpstats` just before the window lifted the run's start-up
    max by 2 ms in every pair of bench-checkpoint-prep's overhead control, and
    taking it out alone removed that; read earlier, its delta covers the warm-up
    and the window. None when nothing needs it.
    """
    if not extra_metrics(args) or not backend.db_counters_before_warmup or "window" in skipped_collectors(args):
        return None
    try:
        return backend.window_counters()
    except (Failure, OSError, subprocess.SubprocessError, ValueError) as error:
        log(f"warning: the database's counters before the warm-up failed: {str(error)[:200]}")
        return {}


def measure(args: argparse.Namespace, backend: Backend, hfs: Hfs, op: str, vus: int, mode: str,
            rate: int, seconds: int, dataset: Dataset, run_dir: Path, early_db: Optional[dict] = None) -> dict:
    container = backend.container_id()
    k6_csv = run_dir / "k6.csv.gz"
    extra = extra_metrics(args)
    watch = HostWatch(backend)
    skip = skipped_collectors(args)
    window_before: Dict[str, float] = {}
    if extra:
        # Read before k6 starts, so the readings themselves stay out of the window,
        # then let the node settle before the first request.
        if "metrics-scrape" not in skip:
            scrape_metrics(hfs, run_dir / "metrics-before.txt")
        if "window" not in skip:
            window_before = read_window(backend, hfs, container, early_db)
        if "host-watch" not in skip:
            watch.prepare([hfs.process.pid])
        time.sleep(WINDOW_SETTLE_SECONDS)
    started = clocks()
    with (run_dir / "k6.log").open("w", encoding="utf-8") as output:
        process = subprocess.Popen(
            k6_command(args, op, hfs.base, vus, mode, rate, seconds, dataset, k6_csv),
            env=k6_environment(), stdout=output, stderr=subprocess.STDOUT,
        )
        CLEANUP["processes"][process.pid] = "k6"
        sampler = MemorySampler(container, {"hfs": hfs.process.pid, "k6": process.pid}, run_dir / "memory.csv")
        sampler.start()
        watching = extra and "host-watch" not in skip
        if watching:
            watch.own.append(process.pid)  # k6, started after prepare()
            watch.start()
        try:
            code = process.wait(timeout=seconds + 900)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=30)
            raise Failure(f"k6 still running {seconds + 900} s after its start; killed")
        except BaseException:  # A signal: k6 must not outlive the harness.
            stop_process(process)
            k6_csv.unlink(missing_ok=True)
            raise
        finally:
            CLEANUP["processes"].pop(process.pid, None)
            sampler.stop()
            if watching:
                watch.stop()
    ended = clocks()
    window: dict = {}
    if extra:
        if watching:
            (run_dir / "host-watch.json").write_text(json.dumps(watch.samples, indent=1), encoding="utf-8")
        window_after = read_window(backend, hfs, container) if "window" not in skip else {}
        if "metrics-scrape" not in skip:
            scrape_metrics(hfs, run_dir / "metrics-after.txt")
        cgroup_covers = None
        if sampler.cgroup_first is not None and sampler.cgroup_last is not None and "window" not in skip:
            flatten("cgroup", sampler.cgroup_first, window_before)
            flatten("cgroup", sampler.cgroup_last, window_after)
            cgroup_covers = (f"{sampler.cgroup_last_t - sampler.cgroup_first_t:.1f} s, from the memory sampler's "
                             f"first good sample (at k6's start) to its last (at most 2 s before k6 exits)")
        window = {"before": window_before, "after": window_after, "delta": window_delta(window_before, window_after),
                  "covers": "the measured window and k6's graceful stop; the readings are taken just before k6 "
                            "starts and just after it exits",
                  "db_covers": ("the warm-up, the window and k6's graceful stop (read before the warm-up)"
                                if early_db is not None else "as covers"),
                  "cgroup_covers": cgroup_covers}
    if code != 0:
        raise Failure(f"k6 exited {code}; see {run_dir / 'k6.log'}")
    # 3. A run during which HFS died is not a measurement of HFS.
    if hfs.process.poll() is not None:
        raise Failure(f"hfs exited {hfs.process.returncode} during the measurement: {hfs.tail()}")
    metrics = analyse(k6_csv, run_dir / "requests.csv.gz", run_dir / "bins.csv", seconds, args.bin_seconds,
                      run_dir / "histogram.csv")
    k6_csv.unlink()
    record = container_record(container)
    peak_file = sampler.memory_peak_file()
    metrics.update(
        {
            "mem_peak_used_mib": sampler.peaks["used_bytes"] / 1024**2,
            "mem_peak_anon_mib": sampler.peaks["anon_bytes"] / 1024**2,
            "mem_peak_current_mib": sampler.peaks["current_bytes"] / 1024**2,
            "memory_peak_file_mib": peak_file / 1024**2 if peak_file is not None else None,
            "mem_limit_mib": record["mem_limit_bytes"] / 1024**2,
            "hfs_peak_rss_mib": sampler.peaks["hfs_rss_kib"] / 1024,
            "k6_peak_rss_mib": sampler.peaks["k6_rss_kib"] / 1024,
            "oom_killed": record["oom_killed"],
            "memory_samples_missed": sampler.missed,
            "container": record,
            # A sleep stops k6 and the database with it: its bins hold a gap, and its
            # throughput and percentiles describe neither the rate nor the backend.
            "measure_wall_s": round(ended[0] - started[0], 3),
            "host_slept_s": round(seconds_asleep(started, ended), 3),
        }
    )
    if watching:
        metrics.update(watch.summary())
    metrics["skipped_collectors"] = ",".join(sorted(skip))
    metrics["window"] = window
    metrics.update(window_columns(window))
    metrics["extra_metrics"] = extra
    return metrics


COLLECTORS = ("metrics-scrape", "window", "host-watch")


def skipped_collectors(args: argparse.Namespace) -> set:
    """The run-time collectors `--skip-collectors` turns off, one at a time, to attribute an overhead."""
    names = {name.strip() for name in (getattr(args, "skip_collectors", "") or "").split(",") if name.strip()}
    unknown = names - set(COLLECTORS)
    if unknown:
        raise Failure(f"unknown collector {sorted(unknown)}; expected any of {COLLECTORS}")
    return names


def extra_metrics(args: argparse.Namespace) -> bool:
    return getattr(args, "extra_metrics", "on") != "off"


def run_status(metrics: dict) -> str:
    """ok, or why the run is not a measurement: the host slept, or nothing succeeded."""
    if metrics.get("host_slept_s", 0.0) > HOST_SLEEP_TOLERANCE_SECONDS:
        return "host_slept"
    # Nothing succeeded: the setup is broken, not merely slow.
    return "ok" if metrics["s2xx"] > 0 else "no_success"


def warm_up(args: argparse.Namespace, hfs: Hfs, op: str, vus: int, mode: str, rate: int,
            dataset: Dataset, run_dir: Path) -> None:
    if args.warmup <= 0:
        return
    command = k6_command(args, op, hfs.base, vus, mode, rate, args.warmup, dataset, None)
    with (run_dir / "k6-warmup.log").open("w", encoding="utf-8") as output:
        code = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT,
                              timeout=args.warmup + 600).returncode
    if code != 0:
        raise Failure(f"warm-up k6 exited {code}; see {run_dir / 'k6-warmup.log'}")


# --- the plan --------------------------------------------------------------------


def cell_id(size: int, op: str, vus: int) -> str:
    return f"{size}/{op}/c{vus}"


def load_rates(path: Path) -> Dict[str, dict]:
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8")).get("rates", {})


def pick_rate(args: argparse.Namespace, rates: Dict[str, dict], size: int, op: str, vus: int) -> int:
    if args.rate:
        return args.rate
    if args.mode == "closed":
        return 0
    entry = rates.get(cell_id(size, op, vus))
    if entry is None:
        raise Failure(f"no calibrated rate for {cell_id(size, op, vus)} in {args.rates}; run calibrate")
    return int(entry["rate_rps"])


def write_run(run_dir: Path, record: dict) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "run.json").write_text(json.dumps(record, indent=1, sort_keys=True), encoding="utf-8")


def search_probe(hfs: Hfs, op: str, dataset: Dataset) -> Tuple[int, str, Optional[int]]:
    queries = json.loads(dataset.search_file.read_text(encoding="utf-8"))[op]
    return search_once(f"{hfs.base}/{queries[0]}")


def timing_key(args: argparse.Namespace, backend: Backend, dataset: "Dataset") -> Tuple:
    """Runs of one key take alike: a short open-loop run says nothing about a long no-reset one."""
    return (backend.label, dataset.size, args.mode, bool(args.no_reset), args.duration, args.warmup)


def seconds_needed(args: argparse.Namespace, backend: Backend, dataset: "Dataset", runs_left: int, fresh: bool) -> float:
    """How long the next run needs. A fresh no-reset cell has to fit every run it has left."""
    per_run = args.duration + RUN_OVERHEAD_SECONDS
    if args.no_reset:
        return runs_left * per_run + (args.warmup + NO_RESET_START_SECONDS if fresh else 0)
    # Until a run of this kind has been timed, assume the slow case: a load.
    guess = args.warmup + per_run + (600 if dataset.size >= 100000 else 180)
    return args.run_seconds.get(timing_key(args, backend, dataset), guess)


def out_of_budget(args: argparse.Namespace, backend: Backend, dataset: "Dataset", runs_left: int = 1,
                  fresh: bool = True) -> Optional[str]:
    """Why the next run must not start before the overnight deadline, or None if it fits."""
    deadline = getattr(args, "deadline", None)
    if deadline is None:
        return None
    needed = seconds_needed(args, backend, dataset, runs_left, fresh)
    left = deadline - time.time()
    if needed > left:
        return f"it needs about {needed:.0f} s and {max(left, 0):.0f} s are left before the deadline"
    return None


def note_run_seconds(args: argparse.Namespace, backend: Backend, dataset: "Dataset", seconds: float) -> None:
    """Keep the longest run of its kind: the next run of that kind may be as slow."""
    if getattr(args, "deadline", None) is not None:
        key = timing_key(args, backend, dataset)
        args.run_seconds[key] = max(args.run_seconds.get(key, 0.0), seconds)


def host_fields(host: Optional[dict]) -> dict:
    """The reading before the run. host_busy also takes the readings during it (busy_overall)."""
    if not host:
        return {}
    return {
        "host": host, "host_busy": host["busy"], "host_busy_before": host["busy"], "host_load_1m": host["load_1m"],
        "host_foreign_cpu_pct": host["foreign_cpu_pct"], "host_docker_limits_gib": host["docker_limits_gib"],
        "host_ci_jobs_before": host.get("ci_jobs", []),
    }


def busy_overall(record: dict) -> bool:
    """A run is busy if the host was busy before it or at any reading during it."""
    return bool(record.get("host_busy_before") or record.get("host_busy_during"))


def build_snapshot(args: argparse.Namespace, backend: Backend, dataset: "Dataset", volume: str, run_dir: Path) -> dict:
    """Load the dataset through HFS once, settle it, and keep a copy of the volume."""
    log(f"{backend.label} {dataset.size}: building the snapshot")
    started = time.monotonic()
    backend.reset()
    hfs = Hfs(args.hfs_bin, args.hfs_port, run_dir / "hfs-snapshot-build.log", backend.hfs_env(), backend.hfs_log_checks())
    try:
        load = load_dataset(hfs.base, dataset)
        settle = backend.settle(flush=True)
        checks = backend.verify_loaded()
    finally:
        hfs.stop()
    provenance = {
        "volume": volume, "built_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "built_by_label": args.label or "", "built_by_backend": backend.label, "hfs_commit": hfs_commit_of(args),
        "schema_sha256": backend.schema_sha256(), "dataset_sha256": dataset.sha256, "fresh_keyspace": True,
        "checks": checks,
    }
    backend.save_snapshot(volume, provenance)
    return {"seconds": time.monotonic() - started, "load": load, "settle": settle, "provenance": provenance}


def hfs_commit_of(args: argparse.Namespace) -> str:
    return getattr(args, "hfs_commit", "") or ""


def check_restored(hfs: Hfs, dataset: "Dataset") -> None:
    """One read of the dataset, since a restore has no load to check entry by entry."""
    first = dataset.ids_file.read_text(encoding="utf-8").split("\n", 1)[0]
    status, _, _ = fetch(f"{hfs.base}/{first}")
    if status != 200:
        raise Failure(f"the restored database answered {status} for {first}")


def run_cell(args: argparse.Namespace, backend: Backend, dataset: Dataset, op: str, vus: int,
             rate: int, out_root: Path, environment: dict) -> bool:
    """Run every run of one (size, operation, concurrency). Returns False if any failed."""
    cell_dir = out_root / backend.label / str(dataset.size) / op / f"c{vus}"
    base_record = {
        "label": args.label or "", "backend": backend.label, "profile": backend.profile,
        "size": dataset.size, "op": op, "concurrency": vus, "mode": args.mode,
        "target_rate": rate, "duration_s": args.duration, "warmup_s": args.warmup,
        "max_vus": max_vus(args, vus) if args.mode == "open" else vus,
        "dataset_file": dataset.path.name, "dataset_sha256": dataset.sha256,
        "dataset_verified": dataset.verified, "reset": args.reset, "synchronous_commit": backend.baseline if backend.name == "postgres" else None,
        "hfs_commit": environment["hfs_commit"], "hfs_build_profile": environment["hfs_build_profile"],
    }

    def already_done(number: int) -> bool:
        path = cell_dir / f"run{number}" / "run.json"
        if not args.resume or not path.is_file():
            return False
        recorded = json.loads(path.read_text(encoding="utf-8"))
        return recorded.get("status") in ("ok", "not_implemented", "refused") and recorded.get("dataset_sha256") == dataset.sha256

    def cell_refused() -> bool:
        # Recorded by whichever run probed first: an interleaved night may reach run 2 first.
        for path in sorted(cell_dir.glob("run*/run.json")) if args.resume else []:
            if json.loads(path.read_text(encoding="utf-8")).get("status") in ("not_implemented", "refused"):
                return True
        return False

    # An interleaved night runs one repetition of a cell at a time (overnight's --order).
    only = getattr(args, "only_run", None)
    numbers = [only] if only else list(range(1, args.runs + 1))
    if cell_refused() or all(already_done(n) for n in numbers):
        log(f"skip {cell_id(dataset.size, op, vus)}: already recorded")
        return True

    if args.no_reset and args.resume and cell_dir.exists():
        # One data set across its runs: a partial cell can only start over.
        shutil.rmtree(cell_dir)

    hfs: Optional[Hfs] = None
    ok = True

    def bring_up(run_dir: Path) -> dict:
        nonlocal hfs
        if hfs is not None:
            hfs.stop()
            hfs = None
        timing: dict = {}

        def before_start() -> None:
            timing["host"] = wait_for_quiet_host(args.host_wait_cap, backend)

        started = time.monotonic()
        if args.reset == "snapshot":
            volume = backend.snapshot_volume(dataset, environment["hfs_commit"])
            if not backend.has_snapshot(volume):
                timing["snapshot_build"] = build_snapshot(args, backend, dataset, volume, run_dir)
                args.built_snapshots.append(volume)
                started = time.monotonic()
            # Which load this run measures: its marker, written when it was built.
            timing["snapshot"] = backend.snapshot_provenance(volume) or {"volume": volume, "provenance": "unrecorded"}
            backend.restore_snapshot(volume, before_start)
        else:
            backend.reset(before_start)
            timing["snapshot"] = {"volume": None, "fresh_keyspace": True, "reloaded": True}
        timing["port_wait_s"] = round(backend.port_wait_s, 1)
        hfs = Hfs(args.hfs_bin, args.hfs_port, run_dir / "hfs.log", backend.hfs_env(), backend.hfs_log_checks())
        timing["reset_and_boot_s"] = time.monotonic() - started
        return timing

    try:
        for number in numbers:
            run_dir = cell_dir / f"run{number}"
            run_dir.mkdir(parents=True, exist_ok=True)
            record = dict(base_record, run=number, status="failed")
            if already_done(number):
                continue
            fresh = number == numbers[0] or not args.no_reset
            runs_left = sum(1 for later in numbers if later >= number and not already_done(later))
            refusal = out_of_budget(args, backend, dataset, runs_left, fresh)
            if refusal:
                args.budget_stopped = True
                args.budget_stop_reason = f"{backend.label} {cell_id(dataset.size, op, vus)} run {number} was not started: {refusal}"
                log(f"stopping: {args.budget_stop_reason}")
                break
            run_started = time.monotonic()
            try:
                timing: dict = {}
                early_db: Optional[dict] = None
                if fresh:
                    dataset.check_before_load()
                    log(f"{backend.label} {cell_id(dataset.size, op, vus)} run {number}: reset")
                    timing.update(bring_up(run_dir))
                    if op in SEARCH_OPS:
                        code, body, _ = search_probe(hfs, op, dataset)
                        if code >= 500 and code != 501:
                            raise Failure(f"search probe answered {code}: {body}")
                        if not 200 <= code < 300:
                            status = "not_implemented" if code == 501 else "refused"
                            log(f"{backend.label} {op}: the server answered {code}; recorded, no load run")
                            record.update(status=status, probe_status=code, probe_body=body, completed=0, statuses=f"{code}:1")
                            write_run(run_dir, record)
                            break
                    if args.reset == "snapshot":
                        # The snapshot was taken flushed and settled; its statistics are in it.
                        check_restored(hfs, dataset)
                        timing["settle_after_load"] = backend.settle(flush=False)
                    else:
                        timing["load"] = load_dataset(hfs.base, dataset)
                        timing["settle_after_load"] = backend.settle(flush=True)
                        timing["load_checks"] = backend.verify_loaded()
                    if op in SEARCH_OPS:
                        # After the settle, once PostgreSQL has statistics. A query
                        # that matches nothing would measure an empty answer.
                        code, _, entries = search_probe(hfs, op, dataset)
                        timing["first_query_status"] = code
                        timing["first_query_entries"] = entries
                        if not entries:
                            log(f"warning: {op}'s first query answered {code} with {entries} entries")
                    early_db = early_db_counters(args, backend)
                    warm_up(args, hfs, op, vus, args.mode, rate, dataset, run_dir)
                    timing["settle_after_warmup"] = backend.settle(flush=False)
                else:
                    early_db = early_db_counters(args, backend)
                    timing["host"] = wait_for_quiet_host(0, backend)
                record.update(host_fields(timing.get("host")))
                backend_settings = backend.settings()
                (run_dir / "db-settings.json").write_text(json.dumps(backend_settings, indent=1), encoding="utf-8")
                backend.before_measure(run_dir)
                # When, on the wall clock: legs of one cell measured far apart may meet a different host.
                record["measure_started_utc"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
                log(f"{backend.label} {cell_id(dataset.size, op, vus)} run {number}: measure ({args.mode}, rate {rate}, {args.duration} s)")
                metrics = measure(args, backend, hfs, op, vus, args.mode, rate, args.duration, dataset, run_dir,
                                  early_db=early_db)
                backend.collect(run_dir)
                if extra_metrics(args):
                    backend.extra_collect(run_dir)
                environment["container"] = metrics["container"]
                status = run_status(metrics)
                ok = ok and status == "ok"
                record.update(metrics, status=status, timing=timing, no_reset=bool(args.no_reset))
                record["host_busy"] = busy_overall(record)
                if record.get("host_busy_during"):
                    log(f"warning: the host was busy during {cell_id(dataset.size, op, vus)} run {number}: "
                        f"foreign CPU up to {metrics['host_foreign_cpu_max_pct']:.0f}%"
                        + (f", CI jobs {metrics['host_ci_jobs_during']}" if metrics["host_ci_jobs_during"] else "")
                        + "; recorded as host_busy")
                if status == "host_slept":
                    record["failure"] = (f"the host slept {metrics['host_slept_s']:.0f} s during the measurement "
                                         f"(it took {metrics['measure_wall_s']:.0f} s for {args.duration} s); not a measurement")
                    log(f"HOST SLEPT {metrics['host_slept_s']:.0f} s during {cell_id(dataset.size, op, vus)} run {number}: "
                        "recorded as host_slept, not ok; --resume measures it again")
                log(
                    f"  completed {metrics['completed']} ok_rps {metrics['ok_rps']:.1f} "
                    f"p50 {metrics['p50_ms']:.2f} p95 {metrics['p95_ms']:.2f} p99 {metrics['p99_ms']:.2f} "
                    f"max {metrics['max_ms']:.2f} dropped {metrics['dropped_iterations']}"
                    + (f"; SATURATED, {metrics['drop_pct']:.1f}% of the schedule dropped: these latencies are only of the requests that got a VU"
                       if metrics["saturated"] else "")
                )
            except BudgetExhausted as exhausted:
                args.budget_stopped = True
                args.budget_stop_reason = f"{backend.label} {cell_id(dataset.size, op, vus)} run {number}: {exhausted}"
                log(f"stopping before {cell_id(dataset.size, op, vus)} run {number}: {exhausted}")
                # Nothing was measured, so nothing is recorded and --resume runs it.
                if not any(run_dir.iterdir()):
                    run_dir.rmdir()
                break
            except (Failure, subprocess.SubprocessError, OSError, ValueError, KeyError) as failure:
                record["failure"] = str(failure)
                ok = False
                log(f"FAILED {cell_id(dataset.size, op, vus)} run {number}: {failure}")
            write_run(run_dir, record)
            if record["status"] == "ok":
                # A snapshot is built once; later runs of this kind don't pay for it again.
                built = timing.get("snapshot_build", {}).get("seconds", 0.0)
                note_run_seconds(args, backend, dataset, time.monotonic() - run_started - built)
            if not ok and args.no_reset:
                break
    finally:
        # On a signal too: an HFS left behind would hold the port against --resume.
        with cleanup_shield():
            if hfs is not None:
                hfs.stop()
    (cell_dir / "environment.json").write_text(json.dumps(environment, indent=1, sort_keys=True), encoding="utf-8")
    return ok


def port_holder(port: int) -> Optional[str]:
    """What listens on a local port, or None if nothing does."""
    with socket.socket() as probe:
        probe.settimeout(1)
        if probe.connect_ex(("127.0.0.1", port)) != 0:
            return None
    try:
        pids = run_cmd(["lsof", "-nP", "-t", f"-iTCP:{port}", "-sTCP:LISTEN"], check=False).stdout.split()
    except OSError:
        return "an unknown process (lsof is not installed)"
    if not pids:
        return "an unknown process"
    command = run_cmd(["ps", "-o", "comm=", "-p", pids[0]], check=False).stdout.strip()
    return f"process {pids[0]} ({command})"


def require_tool(path: Path, what: str) -> None:
    if shutil.which(str(path)) is None:
        raise Failure(f"no {what} at {path}")


def require_free_port(port: int) -> None:
    holder = port_holder(port)
    if holder:
        raise Failure(f"port {port} is held by {holder}, so HFS can't start: stop it, "
                      "perhaps an HFS left by an interrupted run, or choose another --hfs-port")


def run_plan(args: argparse.Namespace) -> Tuple[Path, bool]:
    if args.backend == "postgres" and args.baseline not in ("on", "off"):
        raise Failure("--baseline is on or off")
    backend: Backend = Cassandra(args) if args.backend == "cassandra" else Postgres(args)
    require_free_port(args.hfs_port)
    require_tool(args.k6, "k6 (see fetch-k6 and --k6)")
    require_tool(args.hfs_bin, "hfs binary (see --hfs-bin)")
    out_root = args.out.resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    prepared = out_root / "prepared"
    rates = load_rates(args.rates) if args.rates else {}
    ops = expand_ops(args.ops)
    args.built_snapshots = []
    # Every rate is looked up before anything starts, so a gap can't stop a long matrix halfway.
    planned = {(size, op, vus): pick_rate(args, rates, size, op, vus)
               for size in args.sizes for op in ops for vus in args.concurrency}
    # Every dataset is checked before anything starts, so a bad file can't stop a long matrix halfway.
    if args.unverified_dataset:
        if backend.profile != "3g":
            raise Failure("--unverified-dataset is for development only: it requires --profile 3g, which is never reported")
        log("WARNING: --unverified-dataset: these runs are marked unverified and never go in the report")
    datasets = {
        size: Dataset(size, args.dataset_dir.resolve(), prepared, args.batches_tool,
                      args.sha256sums.resolve(), verified=not args.unverified_dataset)
        for size in args.sizes
    }
    environment = environment_record(args, backend)
    args.hfs_commit = environment["hfs_commit"]
    if f"v{K6_VERSION}" not in environment["k6"]:
        raise Failure(f"k6 is not the pinned v{K6_VERSION}: {environment['k6']}; see fetch-k6")
    environment_path = out_root / f"environment-{backend.label}.json"
    all_ok = True
    CLEANUP["plans"][id(args)] = (backend, args, out_root, [])
    try:
        for size in args.sizes:
            dataset = datasets[size]
            for op in ops:
                for vus in args.concurrency:
                    if getattr(args, "budget_stopped", False):
                        break
                    if not run_cell(args, backend, dataset, op, vus, planned[(size, op, vus)], out_root, environment):
                        all_ok = False
    finally:
        with cleanup_shield():
            backend.stop()
            if args.reset == "snapshot" and not getattr(args, "keep_snapshots", False):
                # Only what this command built: an overnight's snapshots outlive a `run` beside them.
                backend.drop_snapshots(args.built_snapshots)
            environment_path.write_text(json.dumps(environment, indent=1, sort_keys=True), encoding="utf-8")
            summarize(out_root)
            CLEANUP["plans"].pop(id(args), None)
    return out_root, all_ok


# --- summaries ------------------------------------------------------------------


RUN_FIELDS = [
    "label", "backend", "profile", "size", "op", "concurrency", "mode", "target_rate", "run",
    "status", "latency_reading", "duration_s", "completed", "offered", "drop_pct", "throughput_rps", "ok_rps",
    "p50_ms", "p95_ms", "p99_ms", "p50_ok_ms", "p95_ok_ms", "p99_ok_ms",
    "max_ms", "s2xx", "s409", "s412", "s4xx_other", "s501", "s5xx_other", "s_other", "statuses",
    "dropped_iterations", "saturated", "steady_change_pct", "reset", "mem_peak_used_mib", "mem_peak_anon_mib",
    "memory_peak_file_mib", "mem_limit_mib", "hfs_peak_rss_mib", "k6_peak_rss_mib", "oom_killed",
    "synchronous_commit", "hfs_commit", "hfs_build_profile", "dataset_file", "dataset_sha256",
    "dataset_verified", "memory_samples_missed", "host_busy", "host_load_1m", "host_foreign_cpu_pct",
    "host_docker_limits_gib", "host_slept_s", "measure_wall_s", "failure",
] + [
    # Added on 2026-11-28 (bench-checkpoint-prep), after every earlier column so readers by position still work.
    "max_vus", "measure_started_utc", "extra_metrics", "skipped_collectors",
    "host_busy_before", "host_busy_during", "host_foreign_cpu_max_pct", "host_ci_jobs_during",
    # Latency, every response then 2xx only (ms); n is the request count each is over.
    "n", "min_ms", "mean_ms", "stddev_ms", "p90_ms", "p999_ms", "p999_reliable", "p9999_ms", "p9999_reliable",
    "n_ok", "min_ok_ms", "mean_ok_ms", "stddev_ok_ms", "max_ok_ms", "p90_ok_ms", "p999_ok_ms", "p999_ok_reliable",
    "p9999_ok_ms", "p9999_ok_reliable",
    "p95_ci_low_ms", "p95_ci_high_ms", "p99_ci_low_ms", "p99_ci_high_ms", "ci_method",
    "p95_ok_ci_low_ms", "p95_ok_ci_high_ms", "p99_ok_ci_low_ms", "p99_ok_ci_high_ms", "ci_ok_method",
] + [f"{name}_{stat}_ms" for name in STATUS_LATENCY_CLASSES for stat in ("p50", "p99", "max")] + [
    f"k6_{phase}_{stat}_ms" for phase in K6_PHASES for stat in ("p50", "p99")] + [
    "k6_data_sent_bytes", "k6_data_received_bytes",
] + [column for column, *_ in WINDOW_COLUMNS] + [
    "cassandra_dropped_messages", "cassandra_pending_after", "cassandra_blocked",
]

SUMMARY_KEY = [
    "label", "backend", "profile", "size", "op", "concurrency", "max_vus", "mode", "target_rate", "synchronous_commit",
    "dataset_file", "dataset_sha256", "dataset_verified", "reset",
]

# Every percentile and max, over the runs of a cell: mean, min, max and coefficient of variation.
SPREAD_METRICS = ("p50_ms", "p90_ms", "p95_ms", "p99_ms", "p999_ms", "p9999_ms", "max_ms",
                  "p50_ok_ms", "p90_ok_ms", "p95_ok_ms", "p99_ok_ms", "p999_ok_ms", "p9999_ok_ms", "max_ok_ms")

SUMMARY_FIELDS = [field for field in SUMMARY_KEY if field not in ("reset", "max_vus")] + [
    "cell_status", "latency_reading", "saturated_runs", "runs_ok", "runs_recorded", "throughput_rps_mean", "throughput_rps_min",
    "throughput_rps_max", "throughput_cv_pct", "ok_rps_mean", "drop_pct_mean", "p50_ms_mean", "p50_cv_pct",
    "p95_ms_mean", "p95_cv_pct", "p99_ms_mean", "p50_ok_ms_mean", "p95_ok_ms_mean", "p99_ok_ms_mean", "s2xx", "s409", "s412", "s4xx_other", "s501",
    "s5xx_other", "s_other", "dropped_iterations", "steady_change_pct_mean", "reset", "mem_peak_used_mib",
    "mem_peak_anon_mib", "memory_peak_file_mib", "mem_limit_mib", "hfs_peak_rss_mib",
    "k6_peak_rss_mib", "oom_killed", "host_busy_runs", "host_slept_runs", "memory_samples_missed",
] + [
    # Added on 2026-11-28, after every earlier column.
    "max_vus", "host_busy_during_runs",
] + [f"{metric}_{stat}" for metric in SPREAD_METRICS for stat in ("mean", "min", "max", "cv_pct")
     if f"{metric}_{stat}" not in ("p50_ms_mean", "p95_ms_mean", "p99_ms_mean", "p50_ok_ms_mean", "p95_ok_ms_mean", "p99_ok_ms_mean")] + [
    "db_cpu_s_mean", "hfs_cpu_s_mean", "db_throttled_s_mean", "db_io_write_mib_mean", "cassandra_dropped_messages_total",
    "pg_wal_mib_mean", "pg_deadlocks_total",
]


def fmt(value: object) -> str:
    if isinstance(value, float):
        return "" if math.isnan(value) else f"{value:.3f}"
    return "" if value is None else str(value)


def cv_pct(values: Sequence[float]) -> Optional[float]:
    if len(values) < 2 or statistics.fmean(values) == 0:
        return None
    return statistics.stdev(values) / statistics.fmean(values) * 100.0


def summarize(out_root: Path) -> None:
    records: List[dict] = []
    for path in sorted(out_root.rglob("run.json")):
        records.append(json.loads(path.read_text(encoding="utf-8")))
    records.sort(key=lambda r: (r.get("backend", ""), r.get("size", 0), r.get("op", ""), r.get("concurrency", 0), r.get("run", 0)))

    with (out_root / "runs.csv").open("w", encoding="utf-8", newline="") as sink:
        writer = csv.writer(sink)
        writer.writerow(RUN_FIELDS)
        for record in records:
            writer.writerow([fmt(record.get(field)) for field in RUN_FIELDS])

    groups: Dict[Tuple, List[dict]] = collections.defaultdict(list)
    for record in records:
        groups[tuple(record.get(field) for field in SUMMARY_KEY)].append(record)
    with (out_root / "summary.csv").open("w", encoding="utf-8", newline="") as sink:
        writer = csv.writer(sink)
        writer.writerow(SUMMARY_FIELDS)
        for key, members in groups.items():
            ok = [m for m in members if m.get("status") == "ok"]
            statuses = sorted({m.get("status", "") for m in members})

            def column(name: str) -> List[float]:
                return [m[name] for m in ok if isinstance(m.get(name), (int, float))]

            def mean(name: str) -> Optional[float]:
                values = column(name)
                return statistics.fmean(values) if values else None

            def largest(name: str) -> Optional[float]:
                values = column(name)
                return max(values) if values else None

            def total(name: str) -> int:
                return int(sum(column(name)))

            throughput = column("throughput_rps")
            row = {field: value for field, value in zip(SUMMARY_KEY, key)}
            row.update(
                cell_status=",".join(statuses),
                latency_reading="saturated" if any(m.get("saturated") for m in ok) else "normal" if ok else "",
                saturated_runs=sum(1 for m in ok if m.get("saturated")),
                drop_pct_mean=mean("drop_pct"),
                p50_ok_ms_mean=mean("p50_ok_ms"),
                p95_ok_ms_mean=mean("p95_ok_ms"),
                p99_ok_ms_mean=mean("p99_ok_ms"),
                runs_ok=len(ok),
                runs_recorded=len(members),
                throughput_rps_mean=mean("throughput_rps"),
                throughput_rps_min=min(throughput) if throughput else None,
                throughput_rps_max=max(throughput) if throughput else None,
                throughput_cv_pct=cv_pct(throughput),
                ok_rps_mean=mean("ok_rps"),
                p50_ms_mean=mean("p50_ms"),
                p50_cv_pct=cv_pct(column("p50_ms")),
                p95_ms_mean=mean("p95_ms"),
                p95_cv_pct=cv_pct(column("p95_ms")),
                p99_ms_mean=mean("p99_ms"),
                s2xx=total("s2xx"), s409=total("s409"), s412=total("s412"),
                s4xx_other=total("s4xx_other"), s501=total("s501"),
                s5xx_other=total("s5xx_other"), s_other=total("s_other"),
                dropped_iterations=total("dropped_iterations"),
                steady_change_pct_mean=mean("steady_change_pct"),
                mem_peak_used_mib=largest("mem_peak_used_mib"),
                mem_peak_anon_mib=largest("mem_peak_anon_mib"),
                memory_peak_file_mib=largest("memory_peak_file_mib"),
                mem_limit_mib=largest("mem_limit_mib"),
                hfs_peak_rss_mib=largest("hfs_peak_rss_mib"),
                k6_peak_rss_mib=largest("k6_peak_rss_mib"),
                oom_killed=any(m.get("oom_killed") for m in ok),
                host_busy_runs=sum(1 for m in members if m.get("host_busy")),
                host_slept_runs=sum(1 for m in members if m.get("status") == "host_slept"),
                # Over the runs the peaks come from: a missed sample is a gap in them.
                memory_samples_missed=total("memory_samples_missed"),
                max_vus=key[SUMMARY_KEY.index("max_vus")],
                host_busy_during_runs=sum(1 for m in members if m.get("host_busy_during")),
                db_cpu_s_mean=mean("db_cpu_s"), hfs_cpu_s_mean=mean("hfs_cpu_s"),
                db_throttled_s_mean=mean("db_throttled_s"), db_io_write_mib_mean=mean("db_io_write_mib"),
                cassandra_dropped_messages_total=total("cassandra_dropped_messages") if column("cassandra_dropped_messages") else None,
                pg_wal_mib_mean=mean("pg_wal_mib"),
                pg_deadlocks_total=total("pg_deadlocks") if column("pg_deadlocks") else None,
            )
            for metric in SPREAD_METRICS:
                values = column(metric)
                row.setdefault(f"{metric}_mean", statistics.fmean(values) if values else None)
                row[f"{metric}_min"] = min(values) if values else None
                row[f"{metric}_max"] = max(values) if values else None
                row[f"{metric}_cv_pct"] = cv_pct(values)
            writer.writerow([fmt(row.get(field)) for field in SUMMARY_FIELDS])
    log(f"wrote {out_root / 'runs.csv'} and {out_root / 'summary.csv'}")


# --- commands ------------------------------------------------------------------


def expand_ops(text: str) -> List[str]:
    if text == "all":
        return list(OPS)
    ops = [part.strip() for part in text.split(",") if part.strip()]
    for op in ops:
        if op not in OPS + EXTRA_OPS:
            raise Failure(f"unknown operation {op}; expected any of {OPS + EXTRA_OPS}")
    return ops


def int_list(text: str) -> List[int]:
    return [int(part) for part in text.split(",") if part.strip()]


def add_run_arguments(parser: argparse.ArgumentParser, out_required: bool = True) -> None:
    parser.add_argument("--dataset-dir", type=Path, required=True, help="holds patient-observation-<size>.ndjson")
    parser.add_argument("--hfs-bin", type=Path, default=REPO / "target" / "release" / "hfs")
    parser.add_argument("--k6", type=Path, default=Path("k6"), help="the pinned k6, see fetch-k6")
    parser.add_argument("--out", type=Path, required=out_required, help="directory for results; created if missing")
    parser.add_argument("--sizes", type=int_list, default=[10000, 100000])
    parser.add_argument("--ops", default="all", help="comma list of %s, or all" % (",".join(OPS + EXTRA_OPS),))
    parser.add_argument("--concurrency", type=int_list, default=[1, 8, 32])
    parser.add_argument("--duration", type=int, default=120, help="seconds measured (default: %(default)s)")
    parser.add_argument("--warmup", type=int, default=60, help="seconds of warm-up (default: %(default)s)")
    parser.add_argument("--profile", choices=("5g", "3g"), default="5g", help="5g is the report profile (ADR-aacruzgon-0002)")
    parser.add_argument("--project", help="Compose project name (default hfsbench-<backend>)")
    parser.add_argument("--db-port", type=int, help="host port of the database")
    parser.add_argument("--hfs-port", type=int, default=8180)
    parser.add_argument("--batches-tool", type=Path, default=HERE.parent / "datasets" / "ndjson_to_batches.py")
    parser.add_argument("--sha256sums", type=Path, default=HERE.parent / "datasets" / "SHA256SUMS",
                        help="the published checksums every dataset must match (default: %(default)s)")
    parser.add_argument("--reset", choices=("snapshot", "reload"), default="snapshot",
                        help="snapshot restores a copy of the loaded, settled volume before each run; "
                             "reload loads through HFS every run (default: %(default)s)")
    parser.add_argument("--host-wait-cap", type=float, default=120, help="seconds to wait for a quiet host before each run, then record it busy (default: %(default)s)")
    parser.add_argument("--bin-seconds", type=int, default=10, help="width of the time bins (default: %(default)s)")
    parser.add_argument("--build-profile", help="recorded HFS build profile (default: from the binary's path)")
    parser.add_argument("--docker-budget-gib", type=float, help="wait until running limits plus this leg's fit (for shared machines)")
    parser.add_argument("--note", help="free text recorded with the environment")
    parser.add_argument("--label", help="recorded with each run")
    parser.add_argument("--extra-metrics", choices=("on", "off"), default="on",
                        help="the host watch and the readings before and after each window (report-only); "
                             "off is for the overhead control (default: %(default)s)")
    parser.add_argument("--skip-collectors", default="",
                        help="with --extra-metrics on, turn off some of: %s (diagnostic, for the overhead control)" % ",".join(COLLECTORS))
    parser.add_argument("--max-vus-factor", type=int, default=1,
                        help="open loop: k6's maxVUs is the concurrency times this; 1 is row 0.6's rule "
                             "(an open question for Alan, see README \"Saturated runs\") (default: %(default)s)")


# --- a stop the terminal cuts short ----------------------------------------------
# A terminal can kill the harness before its cleanup is done: Rex kills a closed
# pane's process group about 0.85 s after the SIGHUP, and Cassandra's `compose down`
# alone takes about 5 s. So the first signal also starts a reaper in a session of its
# own, which that kill doesn't reach. It waits for the harness to exit and, only if the
# harness didn't get to the end of its cleanup, does the rest (cmd_reap).
CLEANUP: dict = {"plans": {}, "processes": {}, "overnight": None}
REAPER: Dict[str, Optional[Path]] = {"plan": None}


def cleanup_plan(reason: str) -> dict:
    """What the cleanup would still have to do, from what is running now."""
    backends = []
    for backend, args, out_root, volumes in CLEANUP["plans"].values():
        snapshots = list(volumes)
        if getattr(args, "reset", None) == "snapshot" and not getattr(args, "keep_snapshots", False):
            snapshots += list(getattr(args, "built_snapshots", []))
        backends.append({
            "compose": backend.compose_command(), "env": {backend.port_var: str(backend.port)},
            "containers": [f"{backend.project}-volume-copy"], "volumes": [backend.data_volume],
            "snapshots": snapshots, "out": str(out_root) if out_root else None,
        })
    processes = [{"pid": pid, "name": name} for pid, name in CLEANUP["processes"].items()]
    return {"reason": reason, "backends": backends, "processes": processes, "overnight": CLEANUP["overnight"]}


def start_reaper(reason: str) -> None:
    """Start the reaper for this stop, if there is anything to clean up. Never fails the stop."""
    if not (CLEANUP["plans"] or CLEANUP["processes"] or CLEANUP["overnight"]):
        return
    try:
        base = Path(tempfile.gettempdir()) / f"hfsbench-cleanup-{os.getpid()}"
        plan_path = base.with_suffix(".json")
        plan_path.write_text(json.dumps(cleanup_plan(reason)), encoding="utf-8")
        with base.with_suffix(".log").open("a", encoding="utf-8") as sink:
            subprocess.Popen([sys.executable, str(Path(__file__).resolve()), "reap", "--pid", str(os.getpid()),
                              "--plan", str(plan_path)], stdin=subprocess.DEVNULL, stdout=sink,
                             stderr=subprocess.STDOUT, start_new_session=True, close_fds=True)
        REAPER["plan"] = plan_path
    except Exception:  # The harness's own cleanup still runs.
        pass


def cleanup_finished() -> None:
    """Tell the reaper that the harness's own cleanup got to the end."""
    if REAPER["plan"] is not None:
        REAPER["plan"].with_suffix(".done").write_text("", encoding="utf-8")


def process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    state = run_cmd(["ps", "-o", "stat=", "-p", str(pid)], check=False).stdout.strip()
    return bool(state) and not state.startswith("Z")


def stop_leftover(pid: int, name: str) -> None:
    """Stop a process the harness started, if it is still that process."""
    command = run_cmd(["ps", "-o", "comm=", "-p", str(pid)], check=False).stdout.strip()
    if not command or Path(command).name != name:
        return
    os.kill(pid, signal.SIGTERM)
    for _ in range(60):
        if not process_alive(pid):
            return
        time.sleep(0.5)
    os.kill(pid, signal.SIGKILL)


def finish_overnight(overnight: dict, reason: str, ended: Optional[float] = None) -> None:
    """The state and the morning summary, as the harness would have written them on a stop.

    ended is time.monotonic() when the harness was found gone. That clock is the
    system's (mach_absolute_time on macOS, CLOCK_MONOTONIC on Linux), so it compares
    with the part_started the harness read, and it is awake time, as Took (awake) is.
    """
    state_path = Path(overnight["state_path"])
    state = json.loads(state_path.read_text(encoding="utf-8"))
    # "running" until the harness's own last save, which records the stop and the part's
    # time itself: after that, the part is the harness's and must not be counted twice.
    if overnight.get("part") and state.get("stopped_by") == "running":
        entry = state["parts"].setdefault(overnight["part"], {"seconds": 0.0})
        if entry.get("status") != "complete":
            entry["status"] = f"interrupted by {reason}"
            if ended is not None and overnight.get("part_started") is not None:
                entry["seconds"] = entry.get("seconds", 0.0) + max(0.0, ended - overnight["part_started"])
            # As cmd_overnight does when the part starts (it drops it in memory only, so the
            # state on disk can still hold an earlier night's error for this part).
            entry.pop("error", None)
    state["stopped_by"] = (f"{reason}; the harness was killed before its cleanup was done, "
                           "and a separate process finished it")
    scratch = state_path.with_suffix(".tmp")
    scratch.write_text(json.dumps(state, indent=1, sort_keys=True), encoding="utf-8")
    scratch.replace(state_path)
    write_morning_summary(Path(overnight["out_root"]), state, overnight["parts"], overnight["resume_command"])


def cmd_reap(args: argparse.Namespace) -> int:
    """Wait for a stopped harness to exit; finish its cleanup if it was killed first."""
    global LOG_SINK
    done = args.plan.with_suffix(".done")
    # One reaper per stop: a second one waits here until the first is done, then finds its
    # plan gone and leaves. The lock also tells a starting harness that a reaper is at work.
    try:
        lock = args.plan.open("rb")
    except FileNotFoundError:
        return 0
    fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
    if not args.plan.exists():
        lock.close()
        return 0
    try:
        # A harness that got to the end of its cleanup has written `.done`: nothing to wait for.
        while not done.exists() and process_alive(args.pid):
            time.sleep(0.5)
        ended = time.monotonic()
        try:
            if done.exists():
                # Nothing to do and nothing to say: its log would be empty.
                args.plan.with_suffix(".log").unlink(missing_ok=True)
                return 0
            plan = json.loads(args.plan.read_text(encoding="utf-8"))
            overnight = plan.get("overnight")
            if overnight:
                LOG_SINK = (Path(overnight["out_root"]) / "overnight.log").open("a", encoding="utf-8")
            log(f"reaper: the harness (pid {args.pid}) was killed after a {plan['reason']} before its cleanup was done; finishing it")
            for entry in plan["processes"]:
                stop_leftover(entry["pid"], entry["name"])
            for backend in plan["backends"]:
                env = dict(os.environ)
                env.update(backend["env"])
                commands = [backend["compose"] + ["down", "-v", "--remove-orphans"]]
                commands += [["docker", "rm", "-f", name] for name in backend["containers"]]
                commands += [["docker", "volume", "rm", "-f", name] for name in backend["volumes"] + backend["snapshots"]]
                for command in commands:
                    try:
                        run_cmd(command, check=False, env=env, timeout=300)
                    except (subprocess.SubprocessError, OSError) as failure:
                        log(f"reaper: {' '.join(command[:4])}: {failure}")
                if backend["out"]:
                    summarize(Path(backend["out"]))
            if overnight:
                finish_overnight(overnight, plan["reason"], ended)
            log("reaper: the cleanup is done")
        finally:
            args.plan.unlink(missing_ok=True)
            done.unlink(missing_ok=True)
        return 0
    finally:
        lock.close()  # after the plan is gone: a second reaper then finds nothing to do


def plan_overlaps(plan: dict, projects: Iterable[str], outs: Iterable[Path]) -> bool:
    """Whether a stop's cleanup would touch what a starting command uses: its Compose projects or its results.

    A cleanup takes down only the projects its plan names and writes only into its
    own output folders, so another project's stop (another worker's, on the same
    machine and temporary directory) can't harm this start (pr76-delta4, m2).
    """
    mine = set(projects)
    for backend in plan.get("backends", []):
        compose = backend.get("compose", [])
        if "-p" in compose and compose.index("-p") + 1 < len(compose) and compose[compose.index("-p") + 1] in mine:
            return True
    wanted = {Path(out).expanduser().resolve() for out in outs if out}
    named = [backend.get("out") for backend in plan.get("backends", [])]
    named.append((plan.get("overnight") or {}).get("out_root"))
    for out in named:
        if out:
            path = Path(out).resolve()
            if any(path == w or w in path.parents or path in w.parents for w in wanted):
                return True
    return False


def reaper_at_work(projects: Optional[Iterable[str]] = None, outs: Iterable[Path] = ()) -> Optional[Path]:
    """The log of a stop still being cleaned up that this start would collide with, or None.

    The plan's lock is held from the stop until its cleanup is over, by the stopped
    harness's reaper. A plan nobody holds is a leftover (a reaper killed with -9) and
    is passed over. Given `projects`, a plan that names none of them and none of
    `outs` is passed over too; without them, any plan refuses.
    """
    projects = list(projects) if projects is not None else None
    outs = list(outs)
    for plan in sorted(Path(tempfile.gettempdir()).glob("hfsbench-cleanup-*.json")):
        try:
            handle = plan.open("rb")
        except OSError:
            continue
        with handle:
            if plan.with_suffix(".done").exists():
                continue  # that harness finished its own cleanup; its reaper is only leaving
            if projects is not None:
                try:
                    content = json.loads(handle.read().decode("utf-8"))
                except ValueError:
                    content = None  # unreadable: assume it is ours
                if content is not None and not plan_overlaps(content, projects, outs):
                    continue
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                return plan.with_suffix(".log")
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    return None


def require_no_reaper(projects: Optional[Iterable[str]] = None, outs: Iterable[Path] = ()) -> None:
    """Refuse to start while a stop of the same projects or results is still being cleaned up.

    That cleanup would take this run's database down.
    """
    log_path = reaper_at_work(projects, outs)
    if log_path is not None:
        # The log stays empty while the stopped harness does the cleanup itself, and
        # is removed when it finishes (pr76-delta4, m1).
        raise Failure(f"a stopped run of the same Compose project or results is still being cleaned up "
                      f"(its log is {log_path}): start again in a few seconds, once that log says the cleanup "
                      "is done or the log is gone")


def default_project(args: argparse.Namespace, backend: str) -> str:
    return getattr(args, "project", None) or f"hfsbench-{backend}"


def overnight_projects(args: argparse.Namespace) -> List[str]:
    return [default_project(args, "cassandra"), default_project(args, "postgres")]


# A stop that arrives while a cleanup is under way waits for it (cleanup_shield).
SHIELD = {"depth": 0, "pending": False}


@contextlib.contextmanager
def cleanup_shield(raise_after: bool = True):
    """Run a cleanup that a first signal must not cut short; the stop takes effect after it.

    A signal raises KeyboardInterrupt wherever the harness is, and when that is
    inside a cleanup command, subprocess.run kills the command: a `compose down`
    killed that way leaves the container and its volume. Inside this block the
    signal is only noted, and KeyboardInterrupt is raised when the block ends
    (raise_after False leaves that to the caller, who checks the stop itself).
    """
    SHIELD["depth"] += 1
    try:
        yield
    finally:
        SHIELD["depth"] -= 1
    if SHIELD["depth"] == 0 and SHIELD["pending"]:
        SHIELD["pending"] = False
        if raise_after:
            raise KeyboardInterrupt


def stop_on_signals() -> List[signal.Signals]:
    """Turn SIGINT, SIGTERM and SIGHUP into KeyboardInterrupt; return the list that names the signal.

    So a stop runs the same cleanup as a finish: every `finally` stops HFS and
    k6 and takes the database down (a backgrounded process otherwise ignores
    SIGINT, and SIGTERM and SIGHUP would kill it with no cleanup at all). A
    closed terminal is a SIGHUP; `nohup` has already asked for that to be
    ignored, so it stays so. A second signal during the cleanup is ignored, and
    a first one that arrives inside a cleanup_shield waits for it to finish.
    """
    stopping: List[signal.Signals] = []
    guard = {"taken": False}

    def on_signal(number: int, _frame) -> None:
        # Tested and set before any call: CPython runs a pending handler at a call, and a
        # closed Rex pane sends two SIGHUPs about 1 ms apart, so a second one could re-enter
        # here before the first was recorded and start a second reaper. cmd_reap's lock
        # covers what is left of that window.
        if guard["taken"]:
            return  # The cleanup is under way; let it finish.
        guard["taken"] = True
        stopping.append(signal.Signals(number))
        if number == signal.SIGHUP:
            detach_output()
        start_reaper(signal.Signals(number).name)
        if SHIELD["depth"]:
            SHIELD["pending"] = True
            return
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    if signal.getsignal(signal.SIGHUP) != signal.SIG_IGN:
        signal.signal(signal.SIGHUP, on_signal)
    return stopping


def stops_cleanly(command):
    """A command whose `finally` blocks clean up, run so that a signal reaches them."""
    def run(args: argparse.Namespace) -> int:
        stopping = stop_on_signals()
        try:
            return command(args)
        except KeyboardInterrupt:
            log(f"stopped by {stopping[0].name if stopping else 'an interrupt'}; the cleanup has run")
            return 128 + int(stopping[0]) if stopping else 130
    run.__name__ = command.__name__
    run.__doc__ = command.__doc__
    return run


@stops_cleanly
def cmd_run(args: argparse.Namespace) -> int:
    require_no_reaper([default_project(args, args.backend)], [args.out])
    args.rates = args.rates if args.rates else HERE / "rates.json"
    if args.resume and args.no_reset:
        raise Failure("--resume cannot continue a --no-reset cell")
    _, ok = run_plan(args)
    return 0 if ok else 1


@stops_cleanly
def cmd_calibrate(args: argparse.Namespace) -> int:
    require_no_reaper([default_project(args, "postgres")], [args.out])
    args.resume = False
    args.label = "calibration"
    return run_calibration(args)


def run_calibration(args: argparse.Namespace) -> int:
    args.backend = "postgres"
    args.baseline = "on"
    args.mode = "closed"
    args.rate = 0
    args.rates = None
    args.no_reset = False
    args.runs = 1
    args.unverified_dataset = False
    out_root, ok = run_plan(args)
    rows = list(csv.DictReader((out_root / "runs.csv").open(encoding="utf-8", newline="")))
    environment = json.loads((out_root / "environment-postgres-sync-on.json").read_text(encoding="utf-8"))
    # A partial calibration adds to the file; each entry says when and where it was measured.
    rates = load_rates(args.rates_out)
    for row in rows:
        if row["label"] != args.label:
            continue
        if row["status"] != "ok" or float(row["ok_rps"]) <= 0:
            ok = False
            continue
        capacity = float(row["ok_rps"])
        rates[cell_id(int(row["size"]), row["op"], int(row["concurrency"]))] = {
            "capacity_rps": round(capacity, 1),
            "rate_rps": calibrated_rate(capacity),
            "calibrated_at_utc": environment["recorded_at_utc"],
            "machine": environment["machine"].get("hostname"),
            "dataset_sha256": row["dataset_sha256"],
        }
    document = {
        "note": "Closed-loop capacity on PostgreSQL with synchronous_commit on; rate_rps is 80% of it. Fixed once; Cassandra and PostgreSQL off run the same schedule.",
        "calibrated_at_utc": environment["recorded_at_utc"],
        "hfs_commit": environment["hfs_commit"],
        "hfs_build_profile": environment["hfs_build_profile"],
        "machine": environment["machine"],
        "k6": environment["k6"],
        "profile": environment["profile"],
        "duration_s": args.duration,
        "warmup_s": args.warmup,
        "run_note": args.note or "",
        "rates": dict(sorted(rates.items())),
    }
    args.rates_out.write_text(json.dumps(document, indent=1, sort_keys=False) + "\n", encoding="utf-8")
    log(f"wrote {args.rates_out}: {len(rates)} cells")
    return 0 if ok else 1


PURPOSES = {
    "laptop-dry-run": (
        "LAPTOP DRY RUN, NOT FOR THE REPORT. These are numbers from a laptop, for timings and for finding "
        "bugs. ADR-aacruzgon-0002: laptop numbers never go in the report."
    ),
    "checkpoint": (
        "CHECKPOINT RUN, NOT THE STEP 8 REPORT RUN. A comparison of the write path at one moment of the project, "
        "on one machine; the report's numbers come from Step 8's own run (roadmap row 8.4)."
    ),
}
LAPTOP_BANNER = PURPOSES["laptop-dry-run"]
OVERNIGHT_LABEL = "laptop-dry-run"
CALIBRATION_SECONDS = 60  # What `calibrate` measures per cell by default.
BUDGET_MARGIN_SECONDS = 120


def default_purpose() -> str:
    return "laptop-dry-run" if platform.system() == "Darwin" else "checkpoint"


def overnight_purpose(args: argparse.Namespace) -> str:
    return getattr(args, "purpose", None) or default_purpose()


def overnight_banner(args: argparse.Namespace) -> str:
    return PURPOSES[overnight_purpose(args)]


def leg_label(leg: dict) -> str:
    return "cassandra" if leg["backend"] == "cassandra" else f"postgres-sync-{leg.get('baseline', 'on')}"


def matrix_legs(ops: Sequence[str]) -> List[dict]:
    """The legs of the matrix in their first order: PostgreSQL on, Cassandra, PostgreSQL off (writes only)."""
    write_ops = [op for op in ops if op in ("create", "update")]
    legs = [{"backend": "postgres", "baseline": "on", "ops": list(ops)},
            {"backend": "cassandra", "ops": list(ops)}]
    if write_ops:
        legs.append({"backend": "postgres", "baseline": "off", "ops": write_ops})
    return legs


def overnight_parts(args: argparse.Namespace) -> List[dict]:
    """The parts in the order they run.

    `--order interleaved` (the default): one part per repetition, and inside it
    each cell's legs back to back, their order rotated from one repetition to
    the next. A leg is never measured hours after the others it is compared
    with, and each leg's three runs are spread across the night, so a host that
    changes speed shows as spread between a leg's runs, not as a difference
    between legs (pg-off-oddity, 2026-11-11). `--order blocks` keeps the old
    order: each leg's whole matrix in turn.
    """
    purpose = overnight_purpose(args)
    ops = expand_ops(args.ops)
    write_ops = [op for op in ops if op in ("create", "update")]
    conc = args.concurrency
    sizes = args.sizes
    parts: List[dict] = [{"name": "calibration", "kind": "calibrate", "backend": "postgres", "sizes": sizes,
                          "ops": ops, "concurrency": conc, "runs": 1, "out": "calibration",
                          "duration": min(args.duration, CALIBRATION_SECONDS),
                          "label": f"{purpose}-calibration"}]
    if getattr(args, "order", "interleaved") == "interleaved":
        for number in range(1, args.runs + 1):
            parts.append({"name": f"matrix-run{number}", "kind": "interleaved", "legs": matrix_legs(ops),
                          "sizes": sizes, "ops": ops, "concurrency": conc, "runs": args.runs, "run": number,
                          "out": "matrix", "label": purpose})
    else:
        for size in sizes:
            parts.append({"name": f"postgres-on-{size}", "kind": "run", "backend": "postgres", "baseline": "on",
                          "sizes": [size], "ops": ops, "concurrency": conc, "runs": args.runs, "out": "matrix",
                          "label": purpose})
        for size in sizes:
            parts.append({"name": f"cassandra-{size}", "kind": "run", "backend": "cassandra",
                          "sizes": [size], "ops": ops, "concurrency": conc, "runs": args.runs, "out": "matrix",
                          "label": purpose})
        if write_ops:
            parts.append({"name": "postgres-off", "kind": "run", "backend": "postgres", "baseline": "off",
                          "sizes": sizes, "ops": write_ops, "concurrency": conc, "runs": args.runs, "out": "matrix",
                          "label": purpose})
    for backend in ("cassandra", "postgres"):
        parts.append({"name": f"lwt-steady-{backend}", "kind": "run", "backend": backend, "baseline": "on",
                      "sizes": sizes[:1], "ops": ["create"], "concurrency": [max(conc)], "runs": 3, "out": "lwt-steady",
                      "mode": "closed", "duration": args.lwt_duration, "no_reset": True,
                      "label": f"{purpose}-lwt-steady"})
    return parts


def interleaved_schedule(part: dict) -> List[Tuple[int, str, int, dict]]:
    """(size, op, concurrency, leg) in the order an interleaved part runs them.

    The legs of one cell run back to back. Their order turns by one place each
    repetition, so each leg goes first, second and third once in three runs and
    no leg always meets the host straight after the same other one.
    """
    number = part["run"]
    schedule = []
    for size in part["sizes"]:
        for op in part["ops"]:
            legs = [leg for leg in part["legs"] if op in leg["ops"]]
            if legs:
                turn = (number - 1) % len(legs)
                legs = legs[turn:] + legs[:turn]
            for vus in part["concurrency"]:
                for leg in legs:
                    schedule.append((size, op, vus, leg))
    return schedule


def overnight_preflight(args: argparse.Namespace, parts: List[dict]) -> None:
    require_no_reaper(overnight_projects(args), [args.out])
    require_free_port(args.hfs_port)
    for size in sorted({size for part in parts for size in part["sizes"]}):
        verify_dataset(args.dataset_dir.resolve() / f"patient-observation-{size}.ndjson", args.sha256sums.resolve())
    log("preflight: datasets match the published checksums")
    if not args.batches_tool.is_file():
        raise Failure(f"no batches tool at {args.batches_tool}; see --batches-tool")
    if not args.hfs_bin.is_file() or not os.access(args.hfs_bin, os.X_OK):
        raise Failure(f"no hfs binary at {args.hfs_bin}: build it with cargo build --release -p helios-hfs --features cassandra,postgres")
    if "release" not in str(args.hfs_bin) and args.build_profile != "release":
        raise Failure(f"{args.hfs_bin} does not look like a release build; pass --build-profile release if it is one")
    version = run_cmd([str(args.hfs_bin), "--version"], check=False)
    if version.returncode != 0:
        raise Failure(f"{args.hfs_bin} --version failed: {version.stderr[-300:]}")
    log(f"preflight: {version.stdout.strip()}, release build")
    require_tool(args.k6, "k6 (see fetch-k6 and --k6)")
    k6 = run_cmd([str(args.k6), "version"], check=False)
    if f"v{K6_VERSION}" not in k6.stdout:
        raise Failure(f"k6 is not the pinned v{K6_VERSION}: {k6.stdout.strip() or k6.stderr.strip()}; see fetch-k6")
    log(f"preflight: {k6.stdout.strip()}")
    lines = run_cmd(["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}"]).stdout.splitlines()
    others = [line for line in lines if not line.startswith("hfsbench-")]
    used = docker_limits_gib(others)
    leg_gib = 5 if args.profile == "5g" else 3
    # Legs run one at a time.
    if args.docker_budget_gib and used + leg_gib > args.docker_budget_gib:
        raise Failure(f"docker budget: {used:.1f} GiB of limits are running, plus {leg_gib} for a leg is over {args.docker_budget_gib}; running: {others}")
    log(f"preflight: docker limits {used:.1f} GiB running ({len(others)} containers) + {leg_gib} per leg, "
        f"budget {args.docker_budget_gib or 'none'}; profile {args.profile}")
    free = shutil.disk_usage(args.out).free / 1024**3
    if free < 20:
        log(f"warning: only {free:.0f} GiB free for results and Docker volumes")
    host = host_load(Postgres(args))
    if not host_is_quiet(host):
        log(f"warning: the host is busy at the start: foreign CPU {host['foreign_cpu_pct']:.0f}%"
            + (f", CI jobs {host['ci_jobs']}" if host.get("ci_jobs") else "") + "; every run records its own reading")


def part_legs(part: dict) -> List[dict]:
    return part.get("legs") or [{"backend": part.get("backend", "postgres"), "baseline": part.get("baseline", "on"),
                                 "ops": part["ops"]}]


DECLINED = ("not_implemented", "refused")


def part_records(out_root: Path, part: dict) -> List[dict]:
    """The run records that belong to a part: its label, sizes, legs and, for one repetition, its run."""
    label = part.get("label", OVERNIGHT_LABEL)
    legs = {leg_label(leg): set(leg["ops"]) for leg in part_legs(part)}
    only = part.get("run") if part.get("kind") == "interleaved" else None
    records = []
    directory = out_root / part["out"]
    for path in sorted(directory.rglob("run.json")) if directory.is_dir() else []:
        record = json.loads(path.read_text(encoding="utf-8"))
        if record.get("label") != label or record.get("size") not in part["sizes"]:
            continue
        if record.get("op") not in legs.get(record.get("backend"), set()):
            continue
        # A declined cell is recorded once, by whichever run probed it, and is done for every repetition.
        if only is not None and record.get("run") != only and record.get("status") not in DECLINED:
            continue
        records.append(record)
    return records


def part_counts(records: List[dict], part: dict) -> dict:
    declined = {(r.get("backend"), r["size"], r["op"], r["concurrency"]) for r in records if r.get("status") in DECLINED}
    ok = [r for r in records if r.get("status") == "ok"]
    runs = 1 if part.get("kind") == "interleaved" else part["runs"]
    # A cell the server declined is complete after its one probe.
    done = len(ok) + len(declined) * runs
    total = sum(len(part["sizes"]) * len(leg["ops"]) * len(part["concurrency"]) for leg in part_legs(part)) * runs
    return {
        "ok": len(ok), "declined_cells": len(declined),
        "failed": sum(1 for r in records if r.get("status") in ("failed", "no_success")),
        "busy_host": sum(1 for r in records if r.get("host_busy")),
        "host_slept": sum(1 for r in records if r.get("status") == "host_slept"),
        "saturated": sum(1 for r in ok if r.get("saturated")),
        "missed_samples": sum(int(r.get("memory_samples_missed") or 0) for r in ok),
        "left": max(0, total - done), "total": total,
    }


def parse_utc(text: Optional[str]) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(text) if text else None
    except ValueError:
        return None


COMPARISON_FIELDS = ["size", "op", "concurrency", "run", "legs", "first_started_utc", "span_min",
                     "host_busy_differs", "foreign_cpu_max_pct_spread"]


def leg_comparisons(records: Sequence[dict]) -> List[dict]:
    """For each cell and repetition measured on more than one leg: how far apart the legs ran, and on what host.

    A difference between two legs is the backends' only if they met the same
    host. span_min is the time between the first and last leg's start;
    host_busy_differs says one leg's run met a busy host and another's didn't.
    """
    groups: Dict[Tuple, List[dict]] = collections.defaultdict(list)
    for record in records:
        if record.get("status") == "ok" and record.get("measure_started_utc"):
            groups[(record["size"], record["op"], record["concurrency"], record.get("run"))].append(record)
    rows = []
    for (size, op, vus, number), members in sorted(groups.items(), key=lambda kv: tuple(str(k) for k in kv[0])):
        if len({m["backend"] for m in members}) < 2:
            continue
        starts = [parse_utc(m["measure_started_utc"]) for m in members]
        starts = [t for t in starts if t is not None]
        cpu = [m.get("host_foreign_cpu_max_pct") for m in members if isinstance(m.get("host_foreign_cpu_max_pct"), (int, float))]
        rows.append({
            "size": size, "op": op, "concurrency": vus, "run": number,
            "legs": ";".join(sorted(m["backend"] for m in members)),
            "first_started_utc": min(starts).isoformat(timespec="seconds") if starts else "",
            "span_min": round((max(starts) - min(starts)).total_seconds() / 60, 1) if starts else None,
            "host_busy_differs": len({bool(m.get("host_busy")) for m in members}) > 1,
            "foreign_cpu_max_pct_spread": round(max(cpu) - min(cpu), 1) if len(cpu) == len(members) else None,
        })
    return rows


def write_comparisons(directory: Path) -> List[dict]:
    """comparisons.csv beside runs.csv: one row per cell and repetition run on several legs."""
    records = [json.loads(path.read_text(encoding="utf-8")) for path in sorted(directory.rglob("run.json"))] if directory.is_dir() else []
    rows = leg_comparisons(records)
    if directory.is_dir():
        with (directory / "comparisons.csv").open("w", encoding="utf-8", newline="") as sink:
            writer = csv.writer(sink)
            writer.writerow(COMPARISON_FIELDS)
            for row in rows:
                writer.writerow([fmt(row.get(field)) for field in COMPARISON_FIELDS])
    return rows


def drift_lines(rows: Sequence[dict]) -> List[str]:
    """The morning summary's section on host drift between the legs it compares."""
    if not rows:
        return ["No cell has been measured on two legs yet."]
    spans = [row["span_min"] for row in rows if row["span_min"] is not None]
    differs = [row for row in rows if row["host_busy_differs"]]
    lines = [f"{len(rows)} cell repetitions were measured on two or more legs. The legs of one cell started "
             f"at most {max(spans):.0f} min apart (median {statistics.median(spans):.0f} min)." if spans else
             f"{len(rows)} cell repetitions were measured on two or more legs.",
             "",
             f"**{len(differs)} of them met a busy host on one leg and a quiet one on another**: compare those legs with care."
             if differs else "None met a busy host on one leg and a quiet one on another."]
    for row in differs[:20]:
        lines.append(f"- `{row['size']}/{row['op']}/c{row['concurrency']}` run {row['run']}: {row['legs']}")
    if len(differs) > 20:
        lines.append(f"- and {len(differs) - 20} more, in `comparisons.csv`")
    lines += ["", "Each leg's own runs are spread across the night, so `throughput_cv_pct` and `p95_cv_pct` in "
              "`summary.csv` show how much the host changed under one leg. `comparisons.csv` has every row."]
    return lines


def latency_lines(directory: Path) -> List[str]:
    """The morning summary's table of each measured cell: throughput, drops and the 2xx latency, means over its runs."""
    path = directory / "summary.csv"
    if not path.is_file():
        return ["No cell has been measured yet."]
    with path.open(encoding="utf-8", newline="") as source:
        rows = [row for row in csv.DictReader(source) if row.get("runs_ok") not in ("", "0", None)]
    if not rows:
        return ["No cell has been measured yet."]

    def number(row: dict, name: str, digits: int = 2) -> str:
        value = row.get(name, "")
        try:
            return f"{float(value):.{digits}f}"
        except ValueError:
            return ""

    lines = ["Means over each cell's ok runs, 2xx responses only, in ms. `summary.csv` has every figure.", "",
             "| Leg | Size | Op | c | max VUs | Runs ok | ok/s | Dropped % | p50 | p95 | p99 | max | Saturated runs |",
             "| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    rows.sort(key=lambda r: (int(r["size"] or 0), r["op"], int(r["concurrency"] or 0), r["backend"]))
    for row in rows:
        lines.append(f"| {row['backend']} | {row['size']} | {row['op']} | {row['concurrency']} | {row.get('max_vus', '')} | "
                     f"{row['runs_ok']} | {number(row, 'ok_rps_mean', 1)} | {number(row, 'drop_pct_mean', 1)} | "
                     f"{number(row, 'p50_ok_ms_mean')} | {number(row, 'p95_ok_ms_mean')} | {number(row, 'p99_ok_ms_mean')} | "
                     f"{number(row, 'max_ok_ms_mean')} | {row.get('saturated_runs', '')} |")
    return lines


def write_morning_summary(out_root: Path, state: dict, parts: List[dict], resume_command: str,
                          banner: str = LAPTOP_BANNER, title: str = "Overnight dry run") -> None:
    finished = datetime.now(timezone.utc)
    budget_end = datetime.fromisoformat(state["budget_end_utc"])
    against_budget = round((finished - budget_end).total_seconds() / 60)
    banner = state.get("banner", banner)
    title = state.get("title", title)
    lines = [f"# {title}, {out_root.name}", "", f"**{banner}**", "",
             f"Started {state['started_utc']}, finished {finished.isoformat(timespec='seconds')}. "
             f"Budget {state['hours']} h, ending {state['budget_end_utc']}: "
             + (f"finished {against_budget} min after the end of the budget." if against_budget > 0
                else f"finished {-against_budget} min before the end of the budget."),
             "", f"Stopped by: {state['stopped_by']}.", "",
             "| Part | Status | Took (awake) | Runs ok | Declined cells | Failed | Busy host | Host slept | Saturated | Missed memory samples | Left |",
             "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    failures: List[str] = []
    left_any = False
    for part in parts:
        entry = state["parts"].get(part["name"], {})
        records = part_records(out_root, part)
        counts = part_counts(records, part)
        left_any = left_any or counts["left"] > 0 or entry.get("status") != "complete"
        minutes = entry.get("seconds", 0) / 60
        lines.append(f"| {part['name']} | {entry.get('status', 'not started')} | {minutes:.0f} min | {counts['ok']} | "
                     f"{counts['declined_cells']} | {counts['failed']} | {counts['busy_host']} | {counts['host_slept']} | {counts['saturated']} | {counts['missed_samples']} | {counts['left']} of {counts['total']} |")
        if entry.get("error"):
            failures.append(f"- `{part['name']}`: {entry['error']}")
        failures += [f"- `{r['backend']} {r['size']}/{r['op']}/c{r['concurrency']} run {r['run']}`: {r.get('status')}: {str(r.get('failure', ''))[:300]}"
                     for r in records if r.get("status") in ("failed", "no_success", "host_slept")]
    lines += ["", "## Failed or refused", ""] + (failures or ["None."])
    lines += ["", "## Host drift between legs", ""] + drift_lines(write_comparisons(out_root / "matrix"))
    lines += ["", "## Latency by cell", ""] + latency_lines(out_root / "matrix")
    lines += ["", "## What is left", ""]
    lines += [f"Continue, from the directory this run was started in, with:\n\n```bash\n{resume_command}\n```"] if left_any else ["Nothing: every part is complete."]
    lines += ["", f"Results are under `{out_root}`: `matrix/`, `calibration/` and `lwt-steady/`, each with `runs.csv` and `summary.csv`, and `matrix/comparisons.csv`."]
    (out_root / "morning-summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    # The markers a watcher waits on: FINISHED once the night has ended, however
    # it ended; COMPLETE only if nothing is left to resume.
    (out_root / "FINISHED").write_text(f"{finished.isoformat(timespec='seconds')} {state['stopped_by']}\n", encoding="utf-8")
    if not left_any:
        (out_root / "COMPLETE").write_text(finished.isoformat(timespec="seconds") + "\n", encoding="utf-8")



def resume_arguments(argv: Sequence[str]) -> List[str]:
    """The options a run was started with, without those the resume command sets itself."""
    kept: List[str] = []
    skip_next = False
    for word in argv:
        if skip_next:
            skip_next = False
        elif word in ("--hours", "--out"):
            skip_next = True
        elif word != "--resume" and not word.startswith(("--hours=", "--out=")):
            kept.append(word)
    return kept


def cmd_overnight(args: argparse.Namespace) -> int:
    global LOG_SINK
    # It catches the stop itself, to save the state and write the morning summary.
    stopping = stop_on_signals()
    # The 3 GiB profile is the only one an 8.8 GiB Docker VM fits; elsewhere the report's 5 GiB (ADR-aacruzgon-0002).
    if not args.profile:
        args.profile = "3g" if platform.system() == "Darwin" else "5g"
    if args.docker_budget_gib is None and platform.system() == "Darwin":
        args.docker_budget_gib = 8.3  # The Mac's shared budget (step3/docker-budget.md).
    args.purpose = overnight_purpose(args)
    banner = overnight_banner(args)
    if args.resume and args.out is None:
        raise Failure("--resume needs --out, the directory named in the morning summary")
    out_root = (args.out or Path.home() / "hfs-bench" / datetime.now().strftime("%Y-%m-%d")).expanduser().resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    args.out = out_root
    state_path = out_root / "overnight-state.json"
    budget_stop_reason: Optional[str] = None
    if state_path.exists() and not args.resume:
        raise Failure(f"{out_root} already holds an overnight run: pass --resume to continue it, or another --out")
    parts = overnight_parts(args)
    overnight_preflight(args, parts)
    (out_root / "NOT-FOR-REPORT.txt").write_text(banner + "\n", encoding="utf-8")
    for marker in ("FINISHED", "COMPLETE"):
        (out_root / marker).unlink(missing_ok=True)
    LOG_SINK = (out_root / "overnight.log").open("a", encoding="utf-8")
    log(f"overnight {args.purpose}: order {args.order}, profile {args.profile}, runs {args.runs}, "
        f"duration {args.duration} s, warm-up {args.warmup} s, max-vus-factor {args.max_vus_factor}")
    if platform.system() == "Darwin":
        # Held until this process exits, so one process is all there is to stop.
        subprocess.Popen(["caffeinate", "-dimsu", "-w", str(os.getpid())],
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    state = json.loads(state_path.read_text(encoding="utf-8")) if args.resume and state_path.exists() else {"parts": {}}
    started_at = datetime.now(timezone.utc)
    state.update(started_utc=started_at.isoformat(timespec="seconds"), hours=args.hours, stopped_by="running",
                 budget_end_utc=(started_at + timedelta(hours=args.hours)).isoformat(timespec="seconds"),
                 banner=banner, title="Overnight dry run" if args.purpose == "laptop-dry-run" else "Overnight checkpoint run",
                 purpose=args.purpose, order=args.order, profile=args.profile)
    # On the wall clock: a sleeping Mac must not push the end of the night out.
    deadline = time.time() + args.hours * 3600 - BUDGET_MARGIN_SECONDS
    run_seconds: Dict[Tuple[str, int], float] = {}
    rates_path = out_root / f"rates-{args.purpose}.json"
    resume_command = shlex.join(
        ["python3", str(Path(__file__).resolve()), "overnight", "--resume", "--out", str(out_root),
         "--hours", f"{args.hours:g}", *resume_arguments(sys.argv[2:])])

    def save_state() -> None:
        # Whole or not at all: a signal can arrive during the write.
        scratch = state_path.with_suffix(".tmp")
        scratch.write_text(json.dumps(state, indent=1, sort_keys=True), encoding="utf-8")
        scratch.replace(state_path)

    # What a reaper needs to finish the state and the summary (start_reaper).
    save_state()
    CLEANUP["overnight"] = {"out_root": str(out_root), "state_path": str(state_path), "parts": parts,
                            "resume_command": resume_command, "part": None, "part_started": None}
    any_failed = False
    budget_stopped = False
    calibration_incomplete = False
    stop_reason: Optional[str] = None
    try:
        for part in parts:
            entry = state["parts"].setdefault(part["name"], {"seconds": 0.0})
            if entry.get("status") == "complete":
                log(f"part {part['name']}: already complete")
                continue
            if budget_stopped:
                break
            log(f"part {part['name']}: starting")
            CLEANUP["overnight"]["part"] = part["name"]
            CLEANUP["overnight"]["part_started"] = None
            # An error from an earlier night is about that attempt; this one records its own.
            entry.pop("error", None)
            def plan_for(backend: str, baseline: str, sizes: List[int], ops: Sequence[str],
                         concurrency: List[int], only_run: Optional[int] = None) -> argparse.Namespace:
                plan = argparse.Namespace(**vars(args))
                plan.backend = backend
                plan.baseline = baseline
                plan.out = out_root / part["out"]
                plan.sizes = sizes
                plan.ops = ",".join(ops)
                plan.concurrency = concurrency
                plan.runs = part["runs"]
                plan.only_run = only_run
                plan.mode = part.get("mode", "open")
                plan.no_reset = part.get("no_reset", False)
                plan.duration = part.get("duration", args.duration)
                plan.label = part.get("label", OVERNIGHT_LABEL)
                plan.note = f"{banner} {args.note or ''}".strip()
                plan.rate = None
                plan.rates = None if plan.mode == "closed" else rates_path
                plan.resume = True
                plan.unverified_dataset = False
                plan.keep_snapshots = True
                plan.deadline = deadline
                plan.run_seconds = run_seconds
                plan.budget_stopped = False
                plan.budget_stop_reason = None
                return plan

            # Awake time: the monotonic clock stops while the host sleeps (Took (awake) in the summary).
            started = time.monotonic()
            CLEANUP["overnight"]["part_started"] = started  # so a reaper can add this part's time
            plan = plan_for(part.get("backend", "postgres"), part.get("baseline", "on"), part["sizes"], part["ops"],
                            part["concurrency"])
            try:
                if part["kind"] == "calibrate":
                    plan.rates_out = rates_path
                    code = run_calibration(plan)
                    ok = code == 0
                elif part["kind"] == "interleaved":
                    ok = True
                    for size, op, vus, leg in interleaved_schedule(part):
                        plan = plan_for(leg["backend"], leg.get("baseline", "on"), [size], [op], [vus], part["run"])
                        _, leg_ok = run_plan(plan)
                        ok = ok and leg_ok
                        if plan.budget_stopped:
                            break
                else:
                    _, ok = run_plan(plan)
            except Failure as failure:
                ok = False
                entry["error"] = str(failure)
                log(f"part {part['name']}: {failure}")
            except KeyboardInterrupt:
                entry["seconds"] = entry.get("seconds", 0.0) + time.monotonic() - started
                entry["status"] = f"interrupted by {stopping[0].name}" if stopping else "interrupted"
                raise
            entry["seconds"] = entry.get("seconds", 0.0) + time.monotonic() - started
            # A run that failed or was measured across a sleep fails the night, budget or not.
            any_failed = any_failed or not ok
            if plan.budget_stopped:
                entry["status"] = "stopped at the budget"
                budget_stopped = True
                budget_stop_reason = plan.budget_stop_reason
            else:
                entry["status"] = "complete" if ok else "complete with failures"
            save_state()
            # This part's result and time are in the saved state: a stop now interrupts no part.
            CLEANUP["overnight"].update(part=None, part_started=None)
            if part["kind"] == "calibrate" and entry["status"] != "complete":
                log("calibration is not complete, so the parts that need its rates can't run yet")
                calibration_incomplete = True
                break
    except KeyboardInterrupt:
        stop_reason = stopping[0].name if stopping else "an interrupt"
        log(f"stopped by {stop_reason}; the cleanup has run")
    finally:
        # A stop that arrives now is checked below, through `stopping`.
        with cleanup_shield(raise_after=False):
            if stopping and stop_reason is None:
                stop_reason = stopping[0].name
            state["stopped_by"] = stop_reason or (
                f"the time budget: {budget_stop_reason}" if budget_stopped
                else "an incomplete calibration" if calibration_incomplete else "finishing every part")
            save_state()
            write_morning_summary(out_root, state, parts, resume_command)
            log(f"wrote {out_root / 'morning-summary.md'}")
    if stopping:
        return 128 + int(stopping[0])
    if all(state["parts"].get(part["name"], {}).get("status") == "complete" for part in parts):
        for backend in (Cassandra(args), Postgres(args)):
            backend.drop_snapshots()
    return 1 if any_failed else 0


def dirty_database(base: str, dataset: "Dataset") -> dict:
    """What a run leaves behind: new resources and a new version of a loaded one."""
    markers = []
    for number in range(200):
        status, _, parsed = fetch(f"{base}/Patient", "POST", json.dumps(
            {"resourceType": "Patient", "active": True, "birthDate": f"1900-01-{number % 28 + 1:02d}"}).encode())
        if status != 201 or not parsed:
            raise Failure(f"marker POST answered {status}")
        markers.append(f"Patient/{parsed['id']}")
    line = dataset.path.read_text(encoding="utf-8").split("\n", 1)[0]
    resource = json.loads(line)
    target = f"{resource['resourceType']}/{resource['id']}"
    resource["text"] = {"status": "generated", "div": '<div xmlns="http://www.w3.org/1999/xhtml">dirty</div>'}
    status, _, parsed = fetch(f"{base}/{target}", "PUT", json.dumps(resource).encode())
    if status not in (200, 201) or not parsed:
        raise Failure(f"update PUT answered {status}")
    return {"markers": markers, "target": target, "dirty_version": parsed["meta"]["versionId"]}


def check_clean(base: str, dirt: dict) -> dict:
    """That nothing the previous run wrote is still there."""
    survivors = [m for m in dirt["markers"] if fetch(f"{base}/{m}")[0] != 404]
    status, _, parsed = fetch(f"{base}/{dirt['target']}")
    version = (parsed or {}).get("meta", {}).get("versionId")
    return {
        "marker_survivors": len(survivors), "markers_checked": len(dirt["markers"]),
        "target_status": status, "target_version": version, "dirty_version": dirt["dirty_version"],
        "clean": not survivors and status == 200 and version != dirt["dirty_version"] and "dirty" not in json.dumps(parsed),
    }


@stops_cleanly
def cmd_snapshot_check(args: argparse.Namespace) -> int:
    """Measure a reset by reloading against a snapshot restore, and that a restore is exact."""
    require_no_reaper([default_project(args, args.backend)], [args.out])
    args.reset = "snapshot"
    backend: Backend = Cassandra(args) if args.backend == "cassandra" else Postgres(args)
    out_root = args.out.resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    dataset = Dataset(args.size, args.dataset_dir.resolve(), out_root / "prepared", args.batches_tool, args.sha256sums.resolve())
    volume = f"{backend.project}_snapshot-check"
    result: dict = {"backend": backend.label, "size": args.size, "profile": backend.profile, "restores": []}
    hfs: Optional[Hfs] = None
    log_path = out_root / f"hfs-snapshot-check-{backend.name}.log"

    def start() -> Hfs:
        return Hfs(args.hfs_bin, args.hfs_port, log_path, backend.hfs_env(), backend.hfs_log_checks())

    all_clean = True
    CLEANUP["plans"][id(args)] = (backend, args, None, [volume])
    try:
        log(f"{backend.label} {args.size}: reset by reloading")
        started = time.monotonic()
        backend.reset()
        boot = time.monotonic()
        hfs = start()
        load = load_dataset(hfs.base, dataset)
        settle = backend.settle(flush=True)
        result["reload"] = {
            "reset_and_up_s": boot - started, "total_to_settled_s": time.monotonic() - started,
            "load_s": load["seconds"], "settle": settle,
        }
        reference = backend.state_digest()
        result["reference_rows"] = {name: entry["rows"] for name, entry in reference["tables"].items()}
        hfs.stop()
        hfs = None
        started = time.monotonic()
        backend.save_snapshot(volume)
        result["save_s"] = time.monotonic() - started
        size = run_cmd(["docker", "run", "--rm", "-v", f"{volume}:/s:ro", "--entrypoint", "du", backend._image(), "-sb", "/s"], check=False)
        result["snapshot_bytes"] = int(size.stdout.split()[0]) if size.stdout.split() else None

        for number in range(1, args.restores + 1):
            log(f"{backend.label} {args.size}: restore {number}")
            started = time.monotonic()
            backend.restore_snapshot(volume)
            copied = time.monotonic()
            hfs = start()
            check_restored(hfs, dataset)
            settle = backend.settle(flush=False)
            ready = time.monotonic()
            digest = backend.state_digest()
            identical = digest == reference
            dirt = dirty_database(hfs.base, dataset)
            hfs.stop()
            hfs = None
            backend.restore_snapshot(volume)
            hfs = start()
            clean = check_clean(hfs.base, dirt)
            digest_after_dirty = backend.state_digest()
            hfs.stop()
            hfs = None
            result["restores"].append(
                {
                    "number": number, "restore_and_up_s": copied - started,
                    "total_to_settled_s": ready - started, "identical_to_reference": identical,
                    "differs": [name for name, entry in digest["tables"].items() if reference["tables"].get(name) != entry],
                    "identical_after_dirty_restore": digest_after_dirty == reference,
                    "no_previous_data": clean,
                }
            )
            all_clean = all_clean and identical and clean["clean"] and digest_after_dirty == reference
            log(f"  restore {number}: {ready - started:.1f} s, identical {identical}, clean {clean['clean']}")
    finally:
        with cleanup_shield():
            if hfs is not None:
                hfs.stop()
            (out_root / f"snapshot-check-{backend.name}-{args.size}.json").write_text(json.dumps(result, indent=1), encoding="utf-8")
            backend.stop()
            run_cmd(["docker", "volume", "rm", "-f", volume], check=False, own_session=True)
            CLEANUP["plans"].pop(id(args), None)
    return 0 if all_clean else 1


def cmd_summarize(args: argparse.Namespace) -> int:
    summarize(args.out.resolve())
    return 0


def cmd_fetch_k6(args: argparse.Namespace) -> int:
    key = (platform.system(), platform.machine())
    if key not in K6_ASSETS:
        raise Failure(f"no pinned k6 for {key}")
    name, expected = K6_ASSETS[key]
    stem = name.rsplit(".zip", 1)[0].rsplit(".tar.gz", 1)[0]
    destination = args.dest.resolve() / stem
    binary = destination / "k6"
    if binary.is_file():
        log(f"already present: {binary}")
        print(binary)
        return 0
    url = f"https://github.com/grafana/k6/releases/download/v{K6_VERSION}/{name}"
    with tempfile.TemporaryDirectory() as scratch:
        archive = Path(scratch) / name
        log(f"downloading {url}")
        with urllib.request.urlopen(url, timeout=300) as response, archive.open("wb") as sink:
            shutil.copyfileobj(response, sink)
        actual = sha256_file(archive)
        if actual != expected:
            raise Failure(f"{name}: sha256 {actual}, expected {expected}")
        log(f"sha256 verified: {actual}")
        if name.endswith(".zip"):
            with zipfile.ZipFile(archive) as bundle:
                bundle.extractall(destination.parent)
        else:
            with tarfile.open(archive) as bundle:
                # The checksum is verified; the data filter also refuses odd members (Python 3.12+, backported).
                if hasattr(tarfile, "data_filter"):
                    bundle.extractall(destination.parent, filter="data")
                else:
                    bundle.extractall(destination.parent)
    binary.chmod(0o755)
    print(binary)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)

    run_parser = commands.add_parser("run", help="measure runs on one backend")
    add_run_arguments(run_parser)
    run_parser.add_argument("--backend", choices=("cassandra", "postgres"), required=True)
    run_parser.add_argument("--baseline", default="on", help="PostgreSQL synchronous_commit: on or off")
    run_parser.add_argument("--runs", type=int, default=3)
    run_parser.add_argument("--mode", choices=("open", "closed"), default="open")
    run_parser.add_argument("--rate", type=int, help="fixed arrival rate for every cell; overrides the rates file (development)")
    run_parser.add_argument("--rates", type=Path, help="the calibrated rates (default: rates.json here)")
    run_parser.add_argument("--no-reset", action="store_true", help="run all three runs on one data set (the LWT steady-state case)")
    run_parser.add_argument("--resume", action="store_true", help="skip runs already recorded in --out")
    run_parser.add_argument("--unverified-dataset", action="store_true",
                            help="development only, with --profile 3g: run a file that is not the published one; its runs are marked dataset_verified False")
    run_parser.set_defaults(handler=cmd_run)

    calibrate_parser = commands.add_parser("calibrate", help="measure PostgreSQL capacity closed-loop and write the 80% rates")
    add_run_arguments(calibrate_parser)
    calibrate_parser.add_argument("--rates-out", type=Path, default=HERE / "rates.json")
    calibrate_parser.set_defaults(handler=cmd_calibrate, duration=CALIBRATION_SECONDS)

    check_parser = commands.add_parser("snapshot-check", help="measure reload against snapshot restore, and that a restore is exact")
    add_run_arguments(check_parser)
    check_parser.add_argument("--backend", choices=("cassandra", "postgres"), required=True)
    check_parser.add_argument("--baseline", default="on")
    check_parser.add_argument("--size", type=int, default=100000)
    check_parser.add_argument("--restores", type=int, default=3)
    check_parser.set_defaults(handler=cmd_snapshot_check)

    overnight_parser = commands.add_parser("overnight", help="an unattended laptop dry run within a time budget; see README")
    add_run_arguments(overnight_parser, out_required=False)
    overnight_parser.add_argument("--hours", type=float, required=True, help="time budget in hours")
    overnight_parser.add_argument("--runs", type=int, default=3)
    overnight_parser.add_argument("--resume", action="store_true", help="continue the overnight run in --out")
    overnight_parser.add_argument("--lwt-duration", type=int, default=600, help="seconds of each LWT steady-state run (default: %(default)s)")
    overnight_parser.add_argument("--order", choices=("interleaved", "blocks"), default="interleaved",
                                  help="interleaved: each cell's legs back to back, one repetition at a time; "
                                       "blocks: each leg's whole matrix in turn (default: %(default)s)")
    overnight_parser.add_argument("--purpose", choices=sorted(PURPOSES),
                                  help="what the night is, written on every run and the summary "
                                       "(default: laptop-dry-run on macOS, checkpoint elsewhere)")
    # --profile: 3g on macOS, 5g elsewhere; --docker-budget-gib: 8.3 on macOS, none elsewhere.
    overnight_parser.set_defaults(handler=cmd_overnight, docker_budget_gib=None, profile=None,
                                  backend="postgres", baseline="on")

    summarize_parser = commands.add_parser("summarize", help="rewrite runs.csv and summary.csv from the run records")
    summarize_parser.add_argument("out", type=Path)
    summarize_parser.set_defaults(handler=cmd_summarize)

    reap_parser = commands.add_parser("reap", help="internal: finish the cleanup of a harness killed during its stop")
    reap_parser.add_argument("--pid", type=int, required=True)
    reap_parser.add_argument("--plan", type=Path, required=True)
    reap_parser.set_defaults(handler=cmd_reap)

    fetch_parser = commands.add_parser("fetch-k6", help="download the pinned k6 and verify its checksum")
    fetch_parser.add_argument("--dest", type=Path, required=True)
    fetch_parser.set_defaults(handler=cmd_fetch_k6)

    return parser


def main(argv: List[str]) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except Failure as failure:
        print(f"error: {failure}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130
    finally:
        cleanup_finished()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
