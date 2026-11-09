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
import csv
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
BATCH_WORKERS = 4
SAMPLE_SECONDS = 2.0
SETTLE_TIMEOUT_SECONDS = 1800
# Beyond a run's measured seconds: k6's graceful stop, the reduction of its CSV, the collection.
RUN_OVERHEAD_SECONDS = 30
# Before the first run of a no-reset cell: the restore, the boot, a settle.
NO_RESET_START_SECONDS = 180


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
) -> subprocess.CompletedProcess:
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


def analyse(
    k6_csv: Path,
    requests_out: Path,
    bins_out: Path,
    duration_s: float,
    bin_seconds: int,
) -> dict:
    """Reduce k6's CSV to per-request rows and time bins, and return the run's metrics."""
    rows: List[Tuple[float, float, str]] = []  # end time ms, duration ms, status
    dropped: List[Tuple[float, float]] = []
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
    for label, fraction in (("p50_ms", 0.50), ("p95_ms", 0.95), ("p99_ms", 0.99)):
        metrics[label] = percentile(ordered, fraction)
    metrics["max_ms"] = ordered[-1] if ordered else float("nan")
    # The same, over 2xx only: a fast 503 or 409 would otherwise pull the figures down.
    ok_only = sorted(r[1] for r in rows if status_class(r[2]) == "s2xx")
    for label, fraction in (("p50_ok_ms", 0.50), ("p95_ok_ms", 0.95), ("p99_ok_ms", 0.99)):
        metrics[label] = percentile(ok_only, fraction)
    metrics.update(saturation(len(rows), metrics["dropped_iterations"]))

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
    return record["foreign_cpu_pct"] <= QUIET_FOREIGN_CPU_PCT


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


def host_load(backend: "Backend") -> dict:
    """The host's load, as `ps` and the kernel see it, without the harness's own processes."""
    load_1m, load_5m, load_15m = os.getloadavg()
    processes = parse_ps(run_cmd(["ps", "-A", "-o", "pid=,ppid=,pcpu=,comm="], check=False).stdout)
    foreign = foreign_processes(processes, os.getpid(), own_database_pid(backend))
    stats = run_cmd(["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"], check=False)
    if stats.returncode == 0 and platform.system() != "Linux":
        # Inside the VM, which `ps` no longer counts. On Linux `ps` already sees them.
        foreign = sorted(foreign + other_containers_cpu(stats.stdout.splitlines(), backend.project),
                         key=lambda item: -item["cpu_pct"])
    return {
        "load_1m": load_1m, "load_5m": load_5m, "load_15m": load_15m,
        "cpus": os.cpu_count(),
        "foreign_cpu_pct": sum(item["cpu_pct"] for item in foreign),
        "processes_above_5pct": foreign,
        "docker_limits_gib": docker_limits_gib(stats.stdout.splitlines()) if stats.returncode == 0 else None,
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
                log(f"warning: busy host, recorded: load {record['load_1m']:.1f}, foreign CPU {record['foreign_cpu_pct']:.0f}%")
            return record
        if not warned:
            log(f"host not quiet (load {record['load_1m']:.1f}, foreign CPU {record['foreign_cpu_pct']:.0f}%); waiting up to {cap_seconds:.0f} s")
            warned = True
        time.sleep(10)


# --- the database containers ---------------------------------------------------


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

    def compose(self, *arguments: str, check: bool = True, timeout: float = 900) -> subprocess.CompletedProcess:
        command = ["docker", "compose", "-p", self.project]
        for path in self.files():
            command += ["-f", str(path)]
        env = dict(os.environ)
        env[self.port_var] = str(self.port)
        return run_cmd(command + list(arguments), check=check, env=env, timeout=timeout)

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
        self.compose("up", "-d", "--wait", "--remove-orphans")
        self.prepare()

    def stop(self) -> None:
        self.compose("down", "-v", "--remove-orphans", check=False, timeout=300)
        self._remove_data_volume()

    def _remove_data_volume(self) -> None:
        # `down -v` skips a volume no container uses: one a restore created before a stop.
        run_cmd(["docker", "volume", "rm", "-f", self.data_volume], check=False)

    # Snapshots: a copy of the data volume taken with the database stopped, so
    # a restore is the same on both backends and needs no database command.
    @property
    def data_volume(self) -> str:
        return f"{self.project}_{self.volume_key}"

    def snapshot_volume(self, dataset: "Dataset", hfs_commit: str) -> str:
        # The backend is named: `--project` gives both backends one project.
        return f"{self.project}_snapshot-{self.name}-{dataset.size}-{dataset.sha256[:12]}-{hfs_commit[:12]}"

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
        try:
            run_cmd(
                ["docker", "run", "--rm", "--name", name, "--network", "none", "--entrypoint", "sh",
                 "-v", f"{source}:/from:ro", "-v", f"{destination}:/to", self._image(),
                 "-c", f"find /to -mindepth 1 -delete && cp -a /from/. /to/ && {finish}"],
                timeout=1800,
            )
        except BaseException:
            # Killing `docker run` leaves its container copying, and holding both volumes.
            run_cmd(["docker", "rm", "-f", name], check=False)
            raise

    def quiesce(self) -> None:
        self.compose("stop", "-t", "120", timeout=300)

    def save_snapshot(self, volume: str) -> None:
        self.quiesce()
        run_cmd(["docker", "volume", "create", "--label", f"hfsbench.project={self.project}", volume])
        self._copy_volume(self.data_volume, volume, f"touch /to/{SNAPSHOT_MARKER}")

    def restore_snapshot(self, volume: str, before_start=None) -> None:
        self.quiesce()
        self._wait_for_budget()
        if before_start:
            before_start()
        self._copy_volume(volume, self.data_volume, f"rm -f /to/{SNAPSHOT_MARKER}")
        self.compose("up", "-d", "--wait", "--remove-orphans")

    def drop_snapshots(self, volumes: Optional[Sequence[str]] = None) -> None:
        """Remove the named snapshots, or every snapshot of this project."""
        if volumes is None:
            listed = run_cmd(["docker", "volume", "ls", "-q", "--filter", f"label=hfsbench.project={self.project}"], check=False)
            volumes = listed.stdout.split()
        for volume in volumes:
            run_cmd(["docker", "volume", "rm", "-f", volume], check=False)

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
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BudgetExhausted("the Docker budget stayed full until the time budget ran out")
                wait = min(wait, max(1, remaining))
            log(f"docker budget: {used:.1f} + {self.limit_gib} GiB exceeds {budget}; waiting {wait:.0f} s")
            time.sleep(wait)

    def prepare(self) -> None:
        """Whatever the database needs before HFS starts."""

    def hfs_env(self) -> Dict[str, str]:
        raise NotImplementedError

    def settle(self, flush: bool) -> dict:
        raise NotImplementedError

    def settings(self) -> dict:
        raise NotImplementedError

    def collect(self, out_dir: Path) -> None:
        raise NotImplementedError

    def before_measure(self, out_dir: Path) -> None:
        """Mark the start of the measured window, so collect() reads the window and not the load."""


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

    def prepare(self) -> None:
        schema = REPO / "docs" / "Cassandra" / "schema"
        # schema.cql names no keyspace: it is applied inside the one created first.
        for name, arguments in (("keyspace-single-node.cql", []), ("schema.cql", ["-k", "helios"])):
            command = ["docker", "exec", "-i", self.container_id(), "cqlsh", *arguments]
            run_cmd(command, stdin_path=schema / name, timeout=300)

    def hfs_env(self) -> Dict[str, str]:
        return {
            "HFS_STORAGE_BACKEND": "cassandra",
            "HFS_CASSANDRA_CONTACT_POINTS": f"127.0.0.1:{self.port}",
            "HFS_CASSANDRA_KEYSPACE": "helios",
            "HFS_CASSANDRA_LOCAL_DC": "helios-dc1",
        }

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

    def before_measure(self, out_dir: Path) -> None:
        # nodetool's counters run from the node's start: this reading is what to
        # take away from the one after the run. gcstats restarts on being read.
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


class Hfs:
    def __init__(self, binary: Path, port: int, log_path: Path, backend_env: Dict[str, str]):
        env = dict(os.environ)
        env.update(backend_env)
        env.update(
            {
                "HFS_SERVER_HOST": "127.0.0.1",
                "HFS_SERVER_PORT": str(port),
                "HFS_LOG_LEVEL": "warn",
                "HFS_BASE_URL": f"http://127.0.0.1:{port}",
                # The spec bundles, without which HFS seeds no SearchParameters.
                "HFS_DATA_DIR": str(REPO / "data"),
            }
        )
        with socket.socket() as probe:
            probe.settimeout(1)
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                raise Failure(f"something is already listening on port {port}; load would go to it")
        self.base = f"http://127.0.0.1:{port}"
        self.log_path = log_path
        self.log_file = log_path.open("w", encoding="utf-8")
        self.process = subprocess.Popen([str(binary)], env=env, stdout=self.log_file, stderr=subprocess.STDOUT)
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

    def tail(self) -> str:
        try:
            return self.log_path.read_text(encoding="utf-8")[-600:]
        except OSError:
            return ""

    def stop(self) -> None:
        stop_process(self.process)
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

    def sample_container(self) -> Dict[str, int]:
        script = "cat /sys/fs/cgroup/memory.current; cat /sys/fs/cgroup/memory.stat"
        try:
            result = run_cmd(["docker", "exec", self.container, "sh", "-c", script], check=False, timeout=20)
        except (subprocess.SubprocessError, OSError):
            self.missed += 1
            return {}
        if result.returncode != 0:
            self.missed += 1
            return {}
        lines = result.stdout.splitlines()
        current = int(lines[0])
        stat = dict(line.split() for line in lines[1:])
        return {
            "current_bytes": current,
            "used_bytes": current - int(stat.get("inactive_file", 0)),
            "anon_bytes": int(stat.get("anon", 0)),
            "file_bytes": int(stat.get("file", 0)),
        }

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


# --- k6 and one measured run ---------------------------------------------------


def k6_command(args: argparse.Namespace, op: str, base: str, vus: int, mode: str, rate: int,
               seconds: int, dataset: Dataset, csv_out: Optional[Path]) -> List[str]:
    command = [str(args.k6), "run", "--quiet", "--no-usage-report"]
    if csv_out is not None:
        command += ["--out", f"csv={csv_out}"]
    command += ["--system-tags", "status,name,error_code"]
    environment = {
        "OP": op, "BASE_URL": base, "VUS": vus, "MODE": mode, "RATE": rate,
        "DURATION": f"{seconds}s", "IDS_FILE": dataset.ids_file,
        "BODIES_FILE": dataset.path, "SEARCH_FILE": dataset.search_file,
    }
    for key, value in environment.items():
        command += ["--env", f"{key}={value}"]
    return command + [str(HERE / "load.js")]


def k6_environment() -> Dict[str, str]:
    env = dict(os.environ)
    env["K6_CSV_TIME_FORMAT"] = "unix_milli"
    return env


def measure(args: argparse.Namespace, backend: Backend, hfs: Hfs, op: str, vus: int, mode: str,
            rate: int, seconds: int, dataset: Dataset, run_dir: Path) -> dict:
    container = backend.container_id()
    k6_csv = run_dir / "k6.csv.gz"
    with (run_dir / "k6.log").open("w", encoding="utf-8") as output:
        process = subprocess.Popen(
            k6_command(args, op, hfs.base, vus, mode, rate, seconds, dataset, k6_csv),
            env=k6_environment(), stdout=output, stderr=subprocess.STDOUT,
        )
        sampler = MemorySampler(container, {"hfs": hfs.process.pid, "k6": process.pid}, run_dir / "memory.csv")
        sampler.start()
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
            sampler.stop()
    if code != 0:
        raise Failure(f"k6 exited {code}; see {run_dir / 'k6.log'}")
    # 3. A run during which HFS died is not a measurement of HFS.
    if hfs.process.poll() is not None:
        raise Failure(f"hfs exited {hfs.process.returncode} during the measurement: {hfs.tail()}")
    metrics = analyse(k6_csv, run_dir / "requests.csv.gz", run_dir / "bins.csv", seconds, args.bin_seconds)
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
        }
    )
    return metrics


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
    left = deadline - time.monotonic()
    if needed > left:
        return f"it needs about {needed:.0f} s and {max(left, 0):.0f} s are left before the deadline"
    return None


def note_run_seconds(args: argparse.Namespace, backend: Backend, dataset: "Dataset", seconds: float) -> None:
    """Keep the longest run of its kind: the next run of that kind may be as slow."""
    if getattr(args, "deadline", None) is not None:
        key = timing_key(args, backend, dataset)
        args.run_seconds[key] = max(args.run_seconds.get(key, 0.0), seconds)


def host_fields(host: Optional[dict]) -> dict:
    if not host:
        return {}
    return {
        "host": host, "host_busy": host["busy"], "host_load_1m": host["load_1m"],
        "host_foreign_cpu_pct": host["foreign_cpu_pct"], "host_docker_limits_gib": host["docker_limits_gib"],
    }


def build_snapshot(args: argparse.Namespace, backend: Backend, dataset: "Dataset", volume: str, run_dir: Path) -> dict:
    """Load the dataset through HFS once, settle it, and keep a copy of the volume."""
    log(f"{backend.label} {dataset.size}: building the snapshot")
    started = time.monotonic()
    backend.reset()
    hfs = Hfs(args.hfs_bin, args.hfs_port, run_dir / "hfs-snapshot-build.log", backend.hfs_env())
    try:
        load = load_dataset(hfs.base, dataset)
        settle = backend.settle(flush=True)
    finally:
        hfs.stop()
    backend.save_snapshot(volume)
    return {"seconds": time.monotonic() - started, "load": load, "settle": settle}


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
        path = cell_dir / "run1" / "run.json"
        return args.resume and path.is_file() and json.loads(path.read_text(encoding="utf-8")).get("status") in ("not_implemented", "refused")

    if cell_refused() or all(already_done(n) for n in range(1, args.runs + 1)):
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
            backend.restore_snapshot(volume, before_start)
        else:
            backend.reset(before_start)
        hfs = Hfs(args.hfs_bin, args.hfs_port, run_dir / "hfs.log", backend.hfs_env())
        timing["reset_and_boot_s"] = time.monotonic() - started
        return timing

    try:
        for number in range(1, args.runs + 1):
            run_dir = cell_dir / f"run{number}"
            run_dir.mkdir(parents=True, exist_ok=True)
            record = dict(base_record, run=number, status="failed")
            if already_done(number):
                continue
            fresh = number == 1 or not args.no_reset
            runs_left = sum(1 for later in range(number, args.runs + 1) if not already_done(later))
            refusal = out_of_budget(args, backend, dataset, runs_left, fresh)
            if refusal:
                args.budget_stopped = True
                args.budget_stop_reason = f"{backend.label} {cell_id(dataset.size, op, vus)} run {number} was not started: {refusal}"
                log(f"stopping: {args.budget_stop_reason}")
                break
            run_started = time.monotonic()
            try:
                timing: dict = {}
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
                    if op in SEARCH_OPS:
                        # After the settle, once PostgreSQL has statistics. A query
                        # that matches nothing would measure an empty answer.
                        code, _, entries = search_probe(hfs, op, dataset)
                        timing["first_query_status"] = code
                        timing["first_query_entries"] = entries
                        if not entries:
                            log(f"warning: {op}'s first query answered {code} with {entries} entries")
                    warm_up(args, hfs, op, vus, args.mode, rate, dataset, run_dir)
                    timing["settle_after_warmup"] = backend.settle(flush=False)
                else:
                    timing["host"] = wait_for_quiet_host(0, backend)
                record.update(host_fields(timing.get("host")))
                backend_settings = backend.settings()
                (run_dir / "db-settings.json").write_text(json.dumps(backend_settings, indent=1), encoding="utf-8")
                backend.before_measure(run_dir)
                log(f"{backend.label} {cell_id(dataset.size, op, vus)} run {number}: measure ({args.mode}, rate {rate}, {args.duration} s)")
                metrics = measure(args, backend, hfs, op, vus, args.mode, rate, args.duration, dataset, run_dir)
                backend.collect(run_dir)
                environment["container"] = metrics["container"]
                # Nothing succeeded: the setup is broken, not merely slow.
                measured = metrics["s2xx"] > 0
                ok = ok and measured
                record.update(metrics, status="ok" if measured else "no_success", timing=timing, no_reset=bool(args.no_reset))
                log(
                    f"  completed {metrics['completed']} ok_rps {metrics['ok_rps']:.1f} "
                    f"p50 {metrics['p50_ms']:.2f} p95 {metrics['p95_ms']:.2f} dropped {metrics['dropped_iterations']}"
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
    if f"v{K6_VERSION}" not in environment["k6"]:
        raise Failure(f"k6 is not the pinned v{K6_VERSION}: {environment['k6']}; see fetch-k6")
    environment_path = out_root / f"environment-{backend.label}.json"
    all_ok = True
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
        backend.stop()
        if args.reset == "snapshot" and not getattr(args, "keep_snapshots", False):
            # Only what this command built: an overnight's snapshots outlive a `run` beside them.
            backend.drop_snapshots(args.built_snapshots)
        environment_path.write_text(json.dumps(environment, indent=1, sort_keys=True), encoding="utf-8")
        summarize(out_root)
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
    "host_docker_limits_gib", "failure",
]

SUMMARY_KEY = [
    "label", "backend", "profile", "size", "op", "concurrency", "mode", "target_rate", "synchronous_commit",
    "dataset_file", "dataset_sha256", "dataset_verified", "reset",
]

SUMMARY_FIELDS = [field for field in SUMMARY_KEY if field != "reset"] + [
    "cell_status", "latency_reading", "saturated_runs", "runs_ok", "runs_recorded", "throughput_rps_mean", "throughput_rps_min",
    "throughput_rps_max", "throughput_cv_pct", "ok_rps_mean", "drop_pct_mean", "p50_ms_mean", "p50_cv_pct",
    "p95_ms_mean", "p95_cv_pct", "p99_ms_mean", "p50_ok_ms_mean", "p95_ok_ms_mean", "p99_ok_ms_mean", "s2xx", "s409", "s412", "s4xx_other", "s501",
    "s5xx_other", "s_other", "dropped_iterations", "steady_change_pct_mean", "reset", "mem_peak_used_mib",
    "mem_peak_anon_mib", "memory_peak_file_mib", "mem_limit_mib", "hfs_peak_rss_mib",
    "k6_peak_rss_mib", "oom_killed", "host_busy_runs",
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
            )
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


def cmd_run(args: argparse.Namespace) -> int:
    args.rates = args.rates if args.rates else HERE / "rates.json"
    if args.resume and args.no_reset:
        raise Failure("--resume cannot continue a --no-reset cell")
    _, ok = run_plan(args)
    return 0 if ok else 1


def cmd_calibrate(args: argparse.Namespace) -> int:
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


LAPTOP_BANNER = (
    "LAPTOP DRY RUN, NOT FOR THE REPORT. These are numbers from a laptop, for timings and for finding "
    "bugs. ADR-aacruzgon-0002: laptop numbers never go in the report."
)
OVERNIGHT_LABEL = "laptop-dry-run"
CALIBRATION_SECONDS = 60  # What `calibrate` measures per cell by default.
BUDGET_MARGIN_SECONDS = 120


def overnight_parts(args: argparse.Namespace) -> List[dict]:
    """The parts in the order they run. A part is a list of `run` plans sharing one purpose."""
    ops = expand_ops(args.ops)
    write_ops = [op for op in ops if op in ("create", "update")]
    conc = args.concurrency
    sizes = args.sizes
    parts: List[dict] = [{"name": "calibration", "kind": "calibrate", "backend": "postgres", "sizes": sizes,
                          "ops": ops, "concurrency": conc, "runs": 1, "out": "calibration",
                          "duration": min(args.duration, CALIBRATION_SECONDS),
                          "label": f"{OVERNIGHT_LABEL}-calibration"}]
    for size in sizes:
        parts.append({"name": f"postgres-on-{size}", "kind": "run", "backend": "postgres", "baseline": "on",
                      "sizes": [size], "ops": ops, "concurrency": conc, "runs": args.runs, "out": "matrix"})
    for size in sizes:
        parts.append({"name": f"cassandra-{size}", "kind": "run", "backend": "cassandra",
                      "sizes": [size], "ops": ops, "concurrency": conc, "runs": args.runs, "out": "matrix"})
    if write_ops:
        parts.append({"name": "postgres-off", "kind": "run", "backend": "postgres", "baseline": "off",
                      "sizes": sizes, "ops": write_ops, "concurrency": conc, "runs": args.runs, "out": "matrix"})
    for backend in ("cassandra", "postgres"):
        parts.append({"name": f"lwt-steady-{backend}", "kind": "run", "backend": backend, "baseline": "on",
                      "sizes": sizes[:1], "ops": ["create"], "concurrency": [max(conc)], "runs": 3, "out": "lwt-steady",
                      "mode": "closed", "duration": args.lwt_duration, "no_reset": True,
                      "label": f"{OVERNIGHT_LABEL}-lwt-steady"})
    return parts


def overnight_preflight(args: argparse.Namespace, parts: List[dict]) -> None:
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
    # 3 GiB per leg: legs run one at a time.
    if used + 3 > args.docker_budget_gib:
        raise Failure(f"docker budget: {used:.1f} GiB of limits are running, plus 3 for a leg is over {args.docker_budget_gib}; running: {others}")
    log(f"preflight: docker limits {used:.1f} GiB running ({len(others)} containers) + 3 per leg, budget {args.docker_budget_gib}")
    free = shutil.disk_usage(args.out).free / 1024**3
    if free < 20:
        log(f"warning: only {free:.0f} GiB free for results and Docker volumes")


def part_records(out_root: Path, part: dict) -> List[dict]:
    label = part.get("label", OVERNIGHT_LABEL)
    records = []
    for path in sorted((out_root / part["out"]).rglob("run.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        if record.get("label") != label or record.get("size") not in part["sizes"] or record.get("op") not in part["ops"]:
            continue
        expected_backend = part["backend"] if part["backend"] == "cassandra" else f"postgres-sync-{part.get('baseline', 'on')}"
        if record.get("backend") == expected_backend:
            records.append(record)
    return records


def part_counts(records: List[dict], part: dict) -> dict:
    declined = {(r["size"], r["op"], r["concurrency"]) for r in records if r.get("status") in ("not_implemented", "refused")}
    ok = [r for r in records if r.get("status") == "ok"]
    # A cell the server declined is complete after its one probe.
    done = len(ok) + len(declined) * part["runs"]
    total = len(part["sizes"]) * len(part["ops"]) * len(part["concurrency"]) * part["runs"]
    return {
        "ok": len(ok), "declined_cells": len(declined),
        "failed": sum(1 for r in records if r.get("status") in ("failed", "no_success")),
        "busy_host": sum(1 for r in records if r.get("host_busy")),
        "saturated": sum(1 for r in ok if r.get("saturated")),
        "left": max(0, total - done), "total": total,
    }


def write_morning_summary(out_root: Path, state: dict, parts: List[dict], resume_command: str) -> None:
    finished = datetime.now(timezone.utc)
    budget_end = datetime.fromisoformat(state["budget_end_utc"])
    against_budget = round((finished - budget_end).total_seconds() / 60)
    lines = [f"# Overnight dry run, {out_root.name}", "", f"**{LAPTOP_BANNER}**", "",
             f"Started {state['started_utc']}, finished {finished.isoformat(timespec='seconds')}. "
             f"Budget {state['hours']} h, ending {state['budget_end_utc']}: "
             + (f"finished {against_budget} min after the end of the budget." if against_budget > 0
                else f"finished {-against_budget} min before the end of the budget."),
             "", f"Stopped by: {state['stopped_by']}.", "",
             "| Part | Status | Took | Runs ok | Declined cells | Failed | Busy host | Saturated | Left |",
             "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    failures: List[str] = []
    left_any = False
    for part in parts:
        entry = state["parts"].get(part["name"], {})
        records = part_records(out_root, part)
        counts = part_counts(records, part)
        left_any = left_any or counts["left"] > 0 or entry.get("status") != "complete"
        minutes = entry.get("seconds", 0) / 60
        lines.append(f"| {part['name']} | {entry.get('status', 'not started')} | {minutes:.0f} min | {counts['ok']} | "
                     f"{counts['declined_cells']} | {counts['failed']} | {counts['busy_host']} | {counts['saturated']} | {counts['left']} of {counts['total']} |")
        if entry.get("error"):
            failures.append(f"- `{part['name']}`: {entry['error']}")
        failures += [f"- `{r['backend']} {r['size']}/{r['op']}/c{r['concurrency']} run {r['run']}`: {r.get('status')}: {str(r.get('failure', ''))[:300]}"
                     for r in records if r.get("status") in ("failed", "no_success")]
    lines += ["", "## Failed or refused", ""] + (failures or ["None."])
    lines += ["", "## What is left", ""]
    lines += [f"Continue, from the directory this run was started in, with:\n\n```bash\n{resume_command}\n```"] if left_any else ["Nothing: every part is complete."]
    lines += ["", f"Results are under `{out_root}`: `matrix/`, `calibration/` and `lwt-steady/`, each with `runs.csv` and `summary.csv`."]
    (out_root / "morning-summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


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
    # Each raises KeyboardInterrupt, so a stop runs the same cleanup as a finish
    # (a backgrounded process otherwise ignores SIGINT). A closed terminal is a
    # SIGHUP; `nohup` has already asked for that to be ignored, so it stays so.
    stopping: List[signal.Signals] = []

    def on_signal(number: int, _frame) -> None:
        if stopping:
            return  # The cleanup is under way; let it finish.
        stopping.append(signal.Signals(number))
        if number == signal.SIGHUP:
            detach_output()
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    if signal.getsignal(signal.SIGHUP) != signal.SIG_IGN:
        signal.signal(signal.SIGHUP, on_signal)
    args.profile = "3g"
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
    (out_root / "NOT-FOR-REPORT.txt").write_text(LAPTOP_BANNER + "\n", encoding="utf-8")
    LOG_SINK = (out_root / "overnight.log").open("a", encoding="utf-8")
    if platform.system() == "Darwin":
        # Held until this process exits, so one process is all there is to stop.
        subprocess.Popen(["caffeinate", "-dimsu", "-w", str(os.getpid())],
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    state = json.loads(state_path.read_text(encoding="utf-8")) if args.resume and state_path.exists() else {"parts": {}}
    started_at = datetime.now(timezone.utc)
    state.update(started_utc=started_at.isoformat(timespec="seconds"), hours=args.hours, stopped_by="running",
                 budget_end_utc=(started_at + timedelta(hours=args.hours)).isoformat(timespec="seconds"))
    deadline = time.monotonic() + args.hours * 3600 - BUDGET_MARGIN_SECONDS
    run_seconds: Dict[Tuple[str, int], float] = {}
    rates_path = out_root / "rates-laptop-dry-run.json"
    resume_command = shlex.join(
        ["python3", str(Path(__file__).resolve()), "overnight", "--resume", "--out", str(out_root), "--hours", "8",
         *resume_arguments(sys.argv[2:])])

    def save_state() -> None:
        # Whole or not at all: a signal can arrive during the write.
        scratch = state_path.with_suffix(".tmp")
        scratch.write_text(json.dumps(state, indent=1, sort_keys=True), encoding="utf-8")
        scratch.replace(state_path)

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
            plan = argparse.Namespace(**vars(args))
            plan.backend = part["backend"]
            plan.baseline = part.get("baseline", "on")
            plan.out = out_root / part["out"]
            plan.sizes = part["sizes"]
            plan.ops = ",".join(part["ops"])
            plan.concurrency = part["concurrency"]
            plan.runs = part["runs"]
            plan.mode = part.get("mode", "open")
            plan.no_reset = part.get("no_reset", False)
            plan.duration = part.get("duration", args.duration)
            plan.label = part.get("label", OVERNIGHT_LABEL)
            plan.note = f"{LAPTOP_BANNER} {args.note or ''}".strip()
            plan.rate = None
            plan.rates = rates_path
            plan.resume = True
            plan.unverified_dataset = False
            plan.keep_snapshots = True
            plan.deadline = deadline
            plan.run_seconds = run_seconds
            plan.budget_stopped = False
            plan.budget_stop_reason = None
            started = time.monotonic()
            try:
                if part["kind"] == "calibrate":
                    plan.rates_out = rates_path
                    code = run_calibration(plan)
                    ok = code == 0
                else:
                    if plan.mode == "closed":
                        plan.rates = None
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
            if plan.budget_stopped:
                entry["status"] = "stopped at the budget"
                budget_stopped = True
                budget_stop_reason = plan.budget_stop_reason
            else:
                entry["status"] = "complete" if ok else "complete with failures"
                any_failed = any_failed or not ok
            save_state()
            if part["kind"] == "calibrate" and entry["status"] != "complete":
                log("calibration is not complete, so the parts that need its rates can't run yet")
                calibration_incomplete = True
                break
    except KeyboardInterrupt:
        stop_reason = stopping[0].name if stopping else "an interrupt"
        log(f"stopped by {stop_reason}; the cleanup has run")
    finally:
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


def cmd_snapshot_check(args: argparse.Namespace) -> int:
    """Measure a reset by reloading against a snapshot restore, and that a restore is exact."""
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
        return Hfs(args.hfs_bin, args.hfs_port, log_path, backend.hfs_env())

    all_clean = True
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
        if hfs is not None:
            hfs.stop()
        (out_root / f"snapshot-check-{backend.name}-{args.size}.json").write_text(json.dumps(result, indent=1), encoding="utf-8")
        backend.stop()
        run_cmd(["docker", "volume", "rm", "-f", volume], check=False)
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
    overnight_parser.set_defaults(handler=cmd_overnight, docker_budget_gib=8.3, backend="postgres", baseline="on")

    summarize_parser = commands.add_parser("summarize", help="rewrite runs.csv and summary.csv from the run records")
    summarize_parser.add_argument("out", type=Path)
    summarize_parser.set_defaults(handler=cmd_summarize)

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


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
