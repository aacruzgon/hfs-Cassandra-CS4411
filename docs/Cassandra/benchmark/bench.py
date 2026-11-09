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
import shutil
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
from datetime import datetime, timezone
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

# Refused outright: these are the shared development containers.
FORBIDDEN_PROJECTS = {"hfs-cassandra", "hfs-postgres"}
FORBIDDEN_PORTS = {59042, 55432}

CALIBRATION_FRACTION = 0.8
BATCH_WORKERS = 4
SAMPLE_SECONDS = 2.0
SETTLE_TIMEOUT_SECONDS = 1800


class Failure(Exception):
    """A run could not be completed; the message says why."""


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


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
    """Sum the memory limits from `docker stats --format '{{.Name}}\\t{{.MemUsage}}'`."""
    total = 0.0
    for line in docker_stats_lines:
        if "\t" not in line or "/" not in line:
            continue
        limit = line.split("\t", 1)[1].split("/", 1)[1]
        total += parse_size(limit)
    return total / 1024**3


# --- the database containers ---------------------------------------------------


class Backend:
    """One database under its own Compose project name."""

    name = ""
    service = ""
    compose_dir = ""
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

    def reset(self) -> None:
        self.compose("down", "-v", "--remove-orphans", timeout=300)
        self._wait_for_budget()
        self.compose("up", "-d", "--wait", "--remove-orphans")
        self.prepare()

    def stop(self) -> None:
        self.compose("down", "-v", "--remove-orphans", check=False, timeout=300)

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
            log(f"docker budget: {used:.1f} + {self.limit_gib} GiB exceeds {budget}; waiting 180 s")
            time.sleep(180)

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


class Cassandra(Backend):
    name = "cassandra"
    service = "cassandra"
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


class Postgres(Backend):
    name = "postgres"
    service = "postgres"
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
        except Failure:
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
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        self.log_file.close()


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


class Dataset:
    """The dataset of one size and the files derived from it."""

    def __init__(self, size: int, directory: Path, prepared: Path, batches_tool: Path):
        self.size = size
        self.path = directory / f"patient-observation-{size}.ndjson"
        if not self.path.is_file():
            raise Failure(f"no dataset at {self.path}; see docs/Cassandra/datasets/README.md")
        self.root = prepared / str(size)
        self.ids_file = self.root / "ids.txt"
        self.search_file = self.root / "search.json"
        self.batches = self.root / "batches"
        self.sha256 = sha256_file(self.path)
        if not self.ids_file.is_file():
            self.root.mkdir(parents=True, exist_ok=True)
            ids, queries = build_search_params(self.path)
            self.ids_file.write_text("\n".join(ids) + "\n", encoding="utf-8")
            self.search_file.write_text(json.dumps(queries, indent=1), encoding="utf-8")
        if not self.batches.is_dir():
            run_cmd([sys.executable, str(batches_tool), str(self.path), str(self.batches)])


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
        for counts in pool.map(lambda path: post_batch(base, path), files):
            totals.update(counts)
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


def run_cell(args: argparse.Namespace, backend: Backend, dataset: Dataset, op: str, vus: int,
             rate: int, out_root: Path, environment: dict) -> bool:
    """Run every run of one (size, operation, concurrency). Returns False if any failed."""
    cell_dir = out_root / backend.label / str(dataset.size) / op / f"c{vus}"
    base_record = {
        "label": args.label or "", "backend": backend.label, "profile": backend.profile,
        "size": dataset.size, "op": op, "concurrency": vus, "mode": args.mode,
        "target_rate": rate, "duration_s": args.duration, "warmup_s": args.warmup,
        "dataset_sha256": dataset.sha256, "synchronous_commit": backend.baseline if backend.name == "postgres" else None,
        "hfs_commit": environment["hfs_commit"], "hfs_build_profile": environment["hfs_build_profile"],
    }

    def already_done(number: int) -> bool:
        path = cell_dir / f"run{number}" / "run.json"
        if not args.resume or not path.is_file():
            return False
        return json.loads(path.read_text(encoding="utf-8")).get("status") in ("ok", "not_implemented", "refused")

    def cell_refused() -> bool:
        path = cell_dir / "run1" / "run.json"
        return args.resume and path.is_file() and json.loads(path.read_text(encoding="utf-8")).get("status") in ("not_implemented", "refused")

    if cell_refused() or all(already_done(n) for n in range(1, args.runs + 1)):
        log(f"skip {cell_id(dataset.size, op, vus)}: already recorded")
        return True

    hfs: Optional[Hfs] = None
    ok = True

    def bring_up(run_dir: Path) -> dict:
        nonlocal hfs
        if hfs is not None:
            hfs.stop()
            hfs = None
        started = time.monotonic()
        backend.reset()
        hfs = Hfs(args.hfs_bin, args.hfs_port, run_dir / "hfs.log", backend.hfs_env())
        return {"reset_and_boot_s": time.monotonic() - started}

    for number in range(1, args.runs + 1):
        run_dir = cell_dir / f"run{number}"
        run_dir.mkdir(parents=True, exist_ok=True)
        record = dict(base_record, run=number, status="failed")
        if already_done(number):
            continue
        fresh = number == 1 or not args.no_reset
        try:
            timing: dict = {}
            if fresh:
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
            backend_settings = backend.settings()
            (run_dir / "db-settings.json").write_text(json.dumps(backend_settings, indent=1), encoding="utf-8")
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
            )
        except (Failure, subprocess.SubprocessError, OSError, ValueError, KeyError) as failure:
            record["failure"] = str(failure)
            ok = False
            log(f"FAILED {cell_id(dataset.size, op, vus)} run {number}: {failure}")
        write_run(run_dir, record)
        if not ok and args.no_reset:
            break
    if hfs is not None:
        hfs.stop()
    (cell_dir / "environment.json").write_text(json.dumps(environment, indent=1, sort_keys=True), encoding="utf-8")
    return ok


def run_plan(args: argparse.Namespace) -> Tuple[Path, bool]:
    if args.backend == "postgres" and args.baseline not in ("on", "off"):
        raise Failure("--baseline is on or off")
    backend: Backend = Cassandra(args) if args.backend == "cassandra" else Postgres(args)
    out_root = args.out.resolve()
    out_root.mkdir(parents=True, exist_ok=True)
    prepared = out_root / "prepared"
    rates = load_rates(args.rates) if args.rates else {}
    ops = expand_ops(args.ops)
    # Every rate is looked up before anything starts, so a gap can't stop a long matrix halfway.
    planned = {(size, op, vus): pick_rate(args, rates, size, op, vus)
               for size in args.sizes for op in ops for vus in args.concurrency}
    environment = environment_record(args, backend)
    if f"v{K6_VERSION}" not in environment["k6"]:
        raise Failure(f"k6 is not the pinned v{K6_VERSION}: {environment['k6']}; see fetch-k6")
    environment_path = out_root / f"environment-{backend.label}.json"
    all_ok = True
    try:
        for size in args.sizes:
            dataset = Dataset(size, args.dataset_dir.resolve(), prepared, args.batches_tool)
            for op in ops:
                for vus in args.concurrency:
                    if not run_cell(args, backend, dataset, op, vus, planned[(size, op, vus)], out_root, environment):
                        all_ok = False
    finally:
        backend.stop()
        environment_path.write_text(json.dumps(environment, indent=1, sort_keys=True), encoding="utf-8")
        summarize(out_root)
    return out_root, all_ok


# --- summaries ------------------------------------------------------------------


RUN_FIELDS = [
    "label", "backend", "profile", "size", "op", "concurrency", "mode", "target_rate", "run",
    "status", "duration_s", "completed", "throughput_rps", "ok_rps", "p50_ms", "p95_ms", "p99_ms",
    "max_ms", "s2xx", "s409", "s412", "s4xx_other", "s501", "s5xx_other", "s_other", "statuses",
    "dropped_iterations", "steady_change_pct", "mem_peak_used_mib", "mem_peak_anon_mib",
    "memory_peak_file_mib", "mem_limit_mib", "hfs_peak_rss_mib", "k6_peak_rss_mib", "oom_killed",
    "synchronous_commit", "hfs_commit", "hfs_build_profile", "dataset_sha256", "memory_samples_missed", "failure",
]

SUMMARY_KEY = ["label", "backend", "profile", "size", "op", "concurrency", "mode", "target_rate", "synchronous_commit"]

SUMMARY_FIELDS = SUMMARY_KEY + [
    "cell_status", "runs_ok", "runs_recorded", "throughput_rps_mean", "throughput_rps_min",
    "throughput_rps_max", "throughput_cv_pct", "ok_rps_mean", "p50_ms_mean", "p50_cv_pct",
    "p95_ms_mean", "p95_cv_pct", "p99_ms_mean", "s2xx", "s409", "s412", "s4xx_other", "s501",
    "s5xx_other", "s_other", "dropped_iterations", "steady_change_pct_mean", "mem_peak_used_mib",
    "mem_peak_anon_mib", "memory_peak_file_mib", "mem_limit_mib", "hfs_peak_rss_mib",
    "k6_peak_rss_mib", "oom_killed",
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


def add_run_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--dataset-dir", type=Path, required=True, help="holds patient-observation-<size>.ndjson")
    parser.add_argument("--hfs-bin", type=Path, default=REPO / "target" / "release" / "hfs")
    parser.add_argument("--k6", type=Path, default=Path("k6"), help="the pinned k6, see fetch-k6")
    parser.add_argument("--out", type=Path, required=True, help="directory for results; created if missing")
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
    args.backend = "postgres"
    args.baseline = "on"
    args.mode = "closed"
    args.rate = 0
    args.rates = None
    args.no_reset = False
    args.resume = False
    args.runs = 1
    args.label = "calibration"
    out_root, ok = run_plan(args)
    rows = list(csv.DictReader((out_root / "runs.csv").open(encoding="utf-8", newline="")))
    environment = json.loads((out_root / "environment-postgres-sync-on.json").read_text(encoding="utf-8"))
    # A partial calibration adds to the file; each entry says when and where it was measured.
    rates = load_rates(args.rates_out)
    for row in rows:
        if row["label"] != "calibration":
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
        "rates": dict(sorted(rates.items())),
    }
    args.rates_out.write_text(json.dumps(document, indent=1, sort_keys=False) + "\n", encoding="utf-8")
    log(f"wrote {args.rates_out}: {len(rates)} cells")
    return 0 if ok else 1


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


def main(argv: List[str]) -> int:
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
    run_parser.set_defaults(handler=cmd_run)

    calibrate_parser = commands.add_parser("calibrate", help="measure PostgreSQL capacity closed-loop and write the 80% rates")
    add_run_arguments(calibrate_parser)
    calibrate_parser.add_argument("--rates-out", type=Path, default=HERE / "rates.json")
    calibrate_parser.set_defaults(handler=cmd_calibrate, duration=60)

    summarize_parser = commands.add_parser("summarize", help="rewrite runs.csv and summary.csv from the run records")
    summarize_parser.add_argument("out", type=Path)
    summarize_parser.set_defaults(handler=cmd_summarize)

    fetch_parser = commands.add_parser("fetch-k6", help="download the pinned k6 and verify its checksum")
    fetch_parser.add_argument("--dest", type=Path, required=True)
    fetch_parser.set_defaults(handler=cmd_fetch_k6)

    args = parser.parse_args(argv)
    try:
        return args.handler(args)
    except Failure as failure:
        print(f"error: {failure}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
