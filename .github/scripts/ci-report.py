#!/usr/bin/env python3
"""CI reports: measured figures from a job, as a step-summary section and a JSON file.

Every report is one section. `add` and the report commands below each:

- append `## <title>` and the section's Markdown to `$GITHUB_STEP_SUMMARY`;
- write `<id>.md` and `<id>.json` to the job's report directory
  (`$CI_REPORT_DIR`, default `$RUNNER_TEMP/ci-report`), the JSON as
  `{"id", "title", "context", "data"}`;
- turn each string in the data's top-level `"warnings"` list into a
  `::warning::` annotation.

The job's last step uploads that directory as the artifact `ci-report-<job>`.
`context` is the same in every file of a job: the commit, the runner, the
build profile (`$CI_REPORT_PROFILE`), the CPUs, the toolchain, and the
Cassandra version once `context --cassandra-container` has recorded it.

Reports never fail a job for a figure. `build` exits with its command's
status, and the other commands exit 0 even when there is nothing to read,
and say so in the section. Only the Python standard library is used.

Commands:

  context [--cassandra-container NAME]
  add ID TITLE --markdown FILE [--json FILE]
  build ID TITLE -- COMMAND...      run COMMAND, sampling the job's cgroup
  tests ID TITLE LOG...             parse libtest output (`-- --show-output`)
  readiness ID TITLE LOG            the readiness test's `[key=ms]` pairs
  node-stats ID TITLE CONTAINER     nodetool and system_views on a node

A later step adds a section with `add`, or with a new command here.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shlex
import subprocess
import sys
import threading
import time
from pathlib import Path

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
MIB = 1024 * 1024


# --- the mechanism ---------------------------------------------------------


def report_dir() -> Path:
    default = Path(os.environ.get("RUNNER_TEMP", ".")) / "ci-report"
    path = Path(os.environ.get("CI_REPORT_DIR") or default)
    path.mkdir(parents=True, exist_ok=True)
    return path


def run(args: list[str], timeout: float = 60) -> str | None:
    """A command's stdout, or None if it cannot run, times out or fails."""
    try:
        done = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return done.stdout if done.returncode == 0 else None


def read(path: Path) -> str | None:
    try:
        return path.read_text().strip()
    except OSError:
        return None


def cgroup() -> Path | None:
    """The cgroup v2 directory this process runs in: the runner service's."""
    for line in (read(Path("/proc/self/cgroup")) or "").splitlines():
        if line.startswith("0::"):
            return Path("/sys/fs/cgroup" + line[3:])
    return None


def context() -> dict:
    """The job's context, computed once and kept in context.json."""
    path = report_dir() / "context.json"
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        pass
    group = cgroup()
    try:
        affinity = len(os.sched_getaffinity(0))
    except AttributeError:
        affinity = None
    ctx = {
        "commit": os.environ.get("GITHUB_SHA"),
        "ref": os.environ.get("GITHUB_REF_NAME"),
        "workflow": os.environ.get("GITHUB_WORKFLOW"),
        "job": os.environ.get("GITHUB_JOB"),
        "run_id": os.environ.get("GITHUB_RUN_ID"),
        "run_attempt": os.environ.get("GITHUB_RUN_ATTEMPT"),
        "runner": os.environ.get("RUNNER_NAME") or platform.node(),
        "build_profile": os.environ.get("CI_REPORT_PROFILE"),
        "cpus": os.cpu_count(),
        "cpus_usable": affinity,
        "cgroup_cpu_max": read(group / "cpu.max") if group else None,
        "cgroup_memory_max": read(group / "memory.max") if group else None,
        "rustc": (run(["rustc", "-V"]) or "").strip() or None,
        "cargo": (run(["cargo", "-V"]) or "").strip() or None,
        "cassandra_version": None,
    }
    path.write_text(json.dumps(ctx, indent=2))
    return ctx


def save_context(ctx: dict) -> None:
    (report_dir() / "context.json").write_text(json.dumps(ctx, indent=2))


def warn(message: str) -> None:
    # A newline would end the workflow command early.
    print(f"::warning title=CI report::{message.replace(chr(10), ' ')}", flush=True)


def add(report_id: str, title: str, markdown: str, data: dict) -> None:
    directory = report_dir()
    (directory / f"{report_id}.md").write_text(f"## {title}\n\n{markdown.rstrip()}\n")
    document = {"id": report_id, "title": title, "context": context(), "data": data}
    (directory / f"{report_id}.json").write_text(json.dumps(document, indent=2))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as out:
            out.write(f"## {title}\n\n{markdown.rstrip()}\n\n")
    else:
        print(f"## {title}\n\n{markdown.rstrip()}\n")
    for message in data.get("warnings", []):
        warn(message)


def table(header: list[str], rows: list[list]) -> str:
    def cell(value) -> str:
        return "–" if value is None else str(value).replace("|", "\\|")

    lines = ["| " + " | ".join(header) + " |", "|" + " --- |" * len(header)]
    lines += ["| " + " | ".join(cell(v) for v in row) + " |" for row in rows]
    return "\n".join(lines) + "\n"


# --- context ---------------------------------------------------------------


def cmd_context(args) -> int:
    ctx = context()
    if args.cassandra_container:
        out = run(["docker", "exec", args.cassandra_container, "nodetool", "version"])
        match = re.search(r"ReleaseVersion:\s*(\S+)", out or "")
        ctx["cassandra_version"] = match.group(1) if match else None
        ctx["cassandra_container"] = args.cassandra_container
        save_context(ctx)
    rows = [[key, f"`{value}`" if value is not None else None] for key, value in ctx.items()]
    add("context", "Context", table(["Key", "Value"], rows), {})
    return 0


# --- add -------------------------------------------------------------------


def cmd_add(args) -> int:
    markdown = Path(args.markdown).read_text() if Path(args.markdown).exists() else None
    data = {}
    if args.json:
        try:
            data = json.loads(Path(args.json).read_text())
        except (OSError, ValueError) as error:
            data = {"warnings": [f"{args.title}: {args.json} could not be read ({error})"]}
    if markdown is None:
        markdown = f"Not produced: {args.markdown} does not exist."
        data.setdefault("warnings", []).append(f"{args.title}: {args.markdown} was not produced")
    add(args.id, args.title, markdown, data)
    return 0


# --- build: wall time and memory -------------------------------------------


def memory_stat(group: Path, key: str) -> int | None:
    for line in (read(group / "memory.stat") or "").splitlines():
        name, _, value = line.partition(" ")
        if name == key:
            return int(value)
    return None


def oom_kills(group: Path) -> int:
    for line in (read(group / "memory.events") or "").splitlines():
        name, _, value = line.partition(" ")
        if name == "oom_kill":
            return int(value)
    return 0


def cmd_build(args) -> int:
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    group = cgroup()
    sampled = bool(group and (group / "memory.current").exists() and (group / "memory.stat").exists())
    peaks = {"current": 0, "anon": 0}
    stop = threading.Event()

    def sample() -> None:
        # Every second, as the step's shell sampler did before: memory.current
        # (page cache included, so it reaches the cap whenever the cache fills
        # it) and memory.stat's anon (what the kernel cannot reclaim, the
        # figure that must stay under the cap; ADR-aacruzgon-0023).
        while not stop.is_set():
            current = int(read(group / "memory.current") or 0)
            anon = memory_stat(group, "anon") or 0
            peaks["current"] = max(peaks["current"], current)
            peaks["anon"] = max(peaks["anon"], anon)
            stop.wait(1)

    oom_before = oom_kills(group) if sampled else 0
    sampler = threading.Thread(target=sample, daemon=True) if sampled else None
    if sampler:
        sampler.start()
    started = time.monotonic()
    try:
        status = subprocess.call(command)
    except OSError as error:
        print(f"{command[0]}: {error}", file=sys.stderr)
        status = 127
    wall = time.monotonic() - started
    stop.set()
    if sampler:
        sampler.join()
    try:
        report_build(args, command, status, wall, group, sampled, peaks, oom_kills(group) - oom_before if sampled else None)
    except Exception as error:  # noqa: BLE001
        warn(f"{args.title}: the build report failed: {error!r}")
    return status


def report_build(args, command, status, wall, group, sampled, peaks, oom) -> None:
    memory_max = read(group / "memory.max") if group else None
    runner = context()["runner"]
    print(
        f"Build ran {wall:.0f} s on {runner} ({group}). "
        f"Peak memory.current: {peaks['current'] // MIB} MiB; peak anon: {peaks['anon'] // MIB} MiB; "
        f"memory.max: {memory_max}; OOM kills during the build: {oom}",
        flush=True,
    )

    data = {
        "command": shlex.join(command),
        "exit_status": status,
        "wall_s": round(wall, 1),
        "sampled": sampled,
        "cgroup": str(group) if group else None,
        "peak_memory_current_mib": peaks["current"] // MIB if sampled else None,
        "peak_anon_mib": peaks["anon"] // MIB if sampled else None,
        "memory_max": memory_max,
        "oom_kills": oom,
        "cargo_build_jobs": os.environ.get("CARGO_BUILD_JOBS"),
        "warnings": [],
    }
    if oom:
        data["warnings"].append(f"{args.title}: {oom} OOM kill(s) in the runner's cgroup during the build")
    if not sampled:
        data["warnings"].append(f"{args.title}: no cgroup v2 memory files, so memory was not sampled")
    cap_mib = int(memory_max) // MIB if memory_max and memory_max.isdigit() else None
    margin = f"{(cap_mib - peaks['anon'] // MIB):,} MiB" if cap_mib and sampled else "–"
    md = table(
        ["Command", "Result", "Wall time", "Peak anon", "Peak memory.current", "Cap (memory.max)", "Anon margin", "OOM kills"],
        [[
            f"`{data['command']}`",
            "ok" if status == 0 else f"exit {status}",
            f"{wall:.0f} s",
            f"{data['peak_anon_mib']:,} MiB" if sampled else None,
            f"{data['peak_memory_current_mib']:,} MiB" if sampled else None,
            f"{cap_mib:,} MiB" if cap_mib else memory_max,
            margin,
            oom,
        ]],
    )
    md += (
        "\nSampled every second on the runner service's cgroup. `anon` is the memory the kernel "
        "cannot reclaim and must fit the cap; `memory.current` includes page cache, which fills "
        "the cgroup up to the cap (ADR-aacruzgon-0023).\n"
    )
    add(args.id, args.title, md, data)


# --- tests: libtest output -------------------------------------------------

RUNNING = re.compile(r"^\s*Running (\S+(?: \S+)?) \((.+)\)\s*$")
RESULT = re.compile(
    r"^test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; "
    r"(\d+) filtered out; finished in ([\d.]+)s"
)
TEST_LINE = re.compile(r"^test (\S+) \.\.\. (\w+)")
CAPTURED = re.compile(r"^---- (\S+) std(?:out|err) ----$")
SKIP = re.compile(r"^\s*skipping\b", re.IGNORECASE)


def parse_tests(label: str, text: str) -> tuple[list[dict], list[dict]]:
    """Suites and skips in one log. A skip is a line starting "Skipping", in a
    test's captured output (`--show-output`) or, with `--nocapture`, anywhere
    in a suite: then it is named after the suite's only test, if it ran one."""
    suites, skips = [], []
    suite = None
    in_test = None
    for raw in text.splitlines():
        line = ANSI.sub("", raw).rstrip()
        if match := RUNNING.match(line):
            suite = {"step": label, "suite": match.group(1), "binary": match.group(2), "tests": [], "loose_skips": []}
            suites.append(suite)
            in_test = None
            continue
        if suite is None:
            continue
        if match := TEST_LINE.match(line):
            suite["tests"].append({"name": match.group(1), "result": match.group(2)})
            continue
        if match := CAPTURED.match(line):
            in_test = match.group(1)
            continue
        if line in ("successes:", "failures:"):
            in_test = None
            continue
        if match := RESULT.match(line):
            suite.update(
                result=match.group(1), passed=int(match.group(2)), failed=int(match.group(3)),
                ignored=int(match.group(4)), filtered_out=int(match.group(6)),
                seconds=float(match.group(7)),
            )
            in_test = None
            continue
        if SKIP.match(line):
            if in_test:
                skips.append({"step": label, "suite": suite["suite"], "test": in_test, "message": line.strip()})
            else:
                suite["loose_skips"].append(line.strip())
    for suite in suites:
        ran = [t["name"] for t in suite["tests"] if t["result"] == "ok"]
        for message in suite.pop("loose_skips"):
            name = ran[0] if len(ran) == 1 else None
            skips.append({"step": label, "suite": suite["suite"], "test": name, "message": message})
    return suites, skips


def cmd_tests(args) -> int:
    suites, skips, missing = [], [], []
    for log in args.logs:
        path = Path(log)
        if not path.exists():
            missing.append(path.stem)
            continue
        found, skipped = parse_tests(path.stem, path.read_text(errors="replace"))
        suites += found
        skips += skipped
    warnings = []
    if skips:
        names = ", ".join(s["test"] or f"(a test in {s['suite']})" for s in skips)
        warnings.append(
            f"{len(skips)} Cassandra live test(s) skipped in a job that has a node: {names}"
        )
    for name in missing:
        warnings.append(f"Test report: no log for the step {name!r} (it did not run, or failed before testing)")
    for suite in suites:
        if "result" not in suite:
            warnings.append(f"Test report: {suite['step']} {suite['suite']} printed no `test result` line")

    rows = []
    for suite in suites:
        failed = [t["name"] for t in suite["tests"] if t["result"] == "FAILED"]
        ignored = [t["name"] for t in suite["tests"] if t["result"] == "ignored"]
        rows.append([
            suite["step"], f"`{suite['suite']}`", suite.get("result", "no result line"),
            suite.get("passed"), suite.get("failed"), suite.get("ignored"), suite.get("filtered_out"),
            f"{suite['seconds']:.2f} s" if "seconds" in suite else None,
            ", ".join(f"`{n}`" for n in failed + ignored) or None,
        ])
    md = table(["Step", "Suite", "Result", "Passed", "Failed", "Ignored", "Filtered out", "Time", "Failed or ignored"], rows)
    if skips:
        md += "\n**Skipped live tests** (a skip passes in libtest's count, so it is listed here):\n\n"
        md += table(["Step", "Suite", "Test", "Message"], [[s["step"], f"`{s['suite']}`", f"`{s['test']}`" if s["test"] else "?", s["message"]] for s in skips])
    else:
        md += "\nNo live test printed a skip.\n"
    md += (
        "\nPer-suite time is libtest's own `finished in`; stable libtest prints no per-test time. "
        "Skips are read from each passing test's captured output (`--show-output`).\n"
    )
    if missing:
        md += "\nNo log for: " + ", ".join(f"`{m}`" for m in missing) + ".\n"
    add(args.id, args.title, md, {"suites": suites, "skipped": skips, "warnings": warnings})
    return 0


# --- readiness -------------------------------------------------------------

READINESS_KEYS = ["stop_ms", "stopped_to_503_ms", "start_ms", "started_to_200_ms"]


def cmd_readiness(args) -> int:
    path = Path(args.log)
    text = ANSI.sub("", path.read_text(errors="replace")) if path.exists() else ""
    figures = {}
    for key in READINESS_KEYS:
        match = re.search(rf"\b{key}=(\d+)\b", text)
        figures[key] = int(match.group(1)) if match else None
    result = re.search(r"^test result: (\w+)\.", text, re.MULTILINE)
    warnings = []
    if not path.exists():
        warnings.append("Readiness report: the readiness step left no log")
    elif None in figures.values():
        warnings.append("Readiness report: the readiness test printed no figures for " + ", ".join(k for k, v in figures.items() if v is None))
    seconds = lambda ms: None if ms is None else f"{ms / 1000:.2f} s"
    md = table(
        ["`docker stop` took", "Stop returned → `/_readiness` 503", "`docker start` took", "Start returned → `/_readiness` 200", "Test"],
        [[seconds(figures["stop_ms"]), seconds(figures["stopped_to_503_ms"]), seconds(figures["start_ms"]),
          seconds(figures["started_to_200_ms"]), result.group(1) if result else "no result line"]],
    )
    md += "\nFrom the readiness test's own `[key=ms]` line, on this job's node (`docker stop --time 30`, then `docker start`). `/_readiness` is polled every 0.5 s.\n"
    add(args.id, args.title, md, {**figures, "test_result": result.group(1) if result else None, "warnings": warnings})
    return 0


# --- node statistics -------------------------------------------------------

TABLESTATS_KEYS = {
    "Number of partitions (estimate)": "partitions_estimate",
    "SSTable count": "sstables",
    "Space used (live)": "space_live_bytes",
    "Memtable cell count": "memtable_cells",
    "Local read count": "local_reads",
    "Local write count": "local_writes",
    "Average tombstones per slice (last five minutes)": "avg_tombstones_per_slice_5m",
    "Maximum tombstones per slice (last five minutes)": "max_tombstones_per_slice_5m",
    "Droppable tombstone ratio": "droppable_tombstone_ratio",
}

VIRTUAL_TABLES = ["local_read_latency", "local_write_latency", "tombstones_per_read", "rows_per_read", "max_partition_size"]


def number(text: str):
    try:
        value = float(text)
    except ValueError:
        return text
    return int(value) if value.is_integer() else value


def tablestats(out: str) -> dict:
    tables, current = {}, None
    for line in out.splitlines():
        key, _, value = line.strip().partition(": ")
        if key == "Table":
            current = tables.setdefault(value, {})
        elif current is not None and key in TABLESTATS_KEYS:
            current[TABLESTATS_KEYS[key]] = number(value.split()[0]) if value else None
    return tables


def virtual_tables(out: str) -> dict:
    """One `cqlsh -e` run of `SELECT JSON` per table, in VIRTUAL_TABLES order:
    each result starts with its own `[json]` header."""
    results, index = {}, -1
    for line in out.splitlines():
        line = line.strip()
        if line == "[json]":
            index += 1
        elif line.startswith("{") and 0 <= index < len(VIRTUAL_TABLES):
            row = json.loads(line)
            results.setdefault(VIRTUAL_TABLES[index], {})[row.pop("table_name", "?")] = row
    return results


def tpstats(out: str) -> tuple[list[dict], list[dict]]:
    pools, dropped, section = [], [], None
    for line in out.splitlines():
        cells = line.split()
        if not cells:
            continue
        if cells[0] == "Pool":
            section = "pools"
        elif cells[0] == "Message" and len(cells) > 1 and cells[1] == "type":
            section = "dropped"
        elif section == "pools" and len(cells) >= 6 and cells[1].isdigit():
            pools.append(dict(zip(["pool", "active", "pending", "completed", "blocked", "all_time_blocked"], [cells[0]] + [int(c) for c in cells[1:6]])))
        elif section == "dropped" and len(cells) >= 2 and cells[1].isdigit():
            dropped.append({"message_type": cells[0], "dropped": int(cells[1])})
    return pools, dropped


def proxy_cas(out: str) -> list[dict]:
    """The CAS read and write columns of `nodetool proxyhistograms` (micros)."""
    rows = []
    for line in out.splitlines():
        cells = line.split()
        if len(cells) == 7 and (cells[0].endswith("%") or cells[0] in ("Min", "Max")):
            rows.append({"percentile": cells[0], "cas_read_us": number(cells[4]), "cas_write_us": number(cells[5])})
    return rows


def cas_counts(out: str) -> dict:
    """`nodetool sjk mx -mg -all` output: a bean name line, then its Count."""
    counts, bean = {}, None
    for line in out.splitlines():
        line = line.strip()
        if line.startswith("org.apache.cassandra.metrics:"):
            bean = dict(part.split("=", 1) for part in line.split(":", 1)[1].split(","))
        elif bean and re.fullmatch(r"-?\d+", line):
            counts[f"{bean.get('scope')}.{bean.get('name')}"] = int(line)
            bean = None
    return counts


def cmd_node_stats(args) -> int:
    container = args.container
    started = time.monotonic()
    nodetool = lambda *a: run(["docker", "exec", container, "nodetool", *a], timeout=120)
    raw = {
        "tablestats": nodetool("tablestats", args.keyspace),
        "tpstats": nodetool("tpstats"),
        "proxyhistograms": nodetool("proxyhistograms"),
        "cas": nodetool("sjk", "mx", "-mg", "-all", "-f", "Count", "-b", "org.apache.cassandra.metrics:type=ClientRequest,scope=CAS*,name=*"),
        "system_views": run(["docker", "exec", container, "cqlsh", "-e", " ".join(
            f"SELECT JSON * FROM system_views.{t} WHERE keyspace_name = '{args.keyspace}';" for t in VIRTUAL_TABLES
        )], timeout=120),
    }
    for name, out in raw.items():
        (report_dir() / f"{args.id}-{name}.txt").write_text(out or "")
    warnings = [f"Node statistics: {name} could not be read from {container}" for name, out in raw.items() if out is None]
    tables = tablestats(raw["tablestats"] or "")
    views = virtual_tables(raw["system_views"] or "")
    pools, dropped = tpstats(raw["tpstats"] or "")
    cas = cas_counts(raw["cas"] or "")
    histogram = proxy_cas(raw["proxyhistograms"] or "")
    took = time.monotonic() - started

    def view(name: str, table_name: str, key: str):
        return views.get(name, {}).get(table_name, {}).get(key)

    rows = []
    for name in sorted(set(tables) | set(views.get("local_read_latency", {}))):
        stats = tables.get(name, {})
        rows.append([
            f"`{name}`", stats.get("partitions_estimate"), stats.get("sstables"), stats.get("memtable_cells"),
            view("local_read_latency", name, "count"), view("local_read_latency", name, "p99th_ms"),
            view("local_write_latency", name, "count"), view("local_write_latency", name, "p99th_ms"),
            view("tombstones_per_read", name, "count"), view("tombstones_per_read", name, "max"),
            view("tombstones_per_read", name, "p99th"), stats.get("droppable_tombstone_ratio"),
        ])
    md = f"Snapshot of `{container}`, keyspace `{args.keyspace}`, after the tests and before the readiness test restarts the node (a restart resets these metrics). Read in {took:.1f} s.\n\n"
    md += table(["Table", "Partitions (est.)", "SSTables", "Memtable cells", "Local reads", "Read p99 (ms)",
                 "Local writes", "Write p99 (ms)", "Reads with tombstone counts", "Tombstones per read (max)",
                 "Tombstones per read (p99)", "Droppable tombstone ratio"], rows)
    md += "\n**Lightweight transactions** (`ClientRequest` CASWrite/CASRead counts):\n\n"
    cas_rows = [[key, value] for key, value in sorted(cas.items()) if value or key.endswith(".Latency")]
    md += table(["Metric", "Count"], cas_rows) if cas_rows else "Not read.\n"
    if histogram:
        md += "\nCAS latency (`nodetool proxyhistograms`, µs):\n\n"
        md += table(["Percentile", "CAS read", "CAS write"], [[r["percentile"], r["cas_read_us"], r["cas_write_us"]] for r in histogram])
    busy = [p for p in pools if p["completed"] or p["pending"] or p["blocked"] or p["all_time_blocked"]]
    md += "\n**Thread pools** (`nodetool tpstats`, pools with any work):\n\n"
    md += table(["Pool", "Active", "Pending", "Completed", "Blocked", "All-time blocked"],
                [[p["pool"], p["active"], p["pending"], p["completed"], p["blocked"], p["all_time_blocked"]] for p in busy])
    dropped_any = [d for d in dropped if d["dropped"]]
    md += "\nDropped messages: " + (", ".join(f"{d['message_type']} {d['dropped']}" for d in dropped_any) or "none") + ".\n"
    md += "\nRaw outputs are in the artifact, next to this report.\n"
    if dropped_any:
        warnings.append("Node statistics: the node dropped messages: " + ", ".join(d["message_type"] for d in dropped_any))
    data = {"container": container, "keyspace": args.keyspace, "seconds": round(took, 1), "tables": tables,
            "system_views": views, "cas_counts": cas, "cas_latency_us": histogram, "thread_pools": pools,
            "dropped_messages": dropped, "warnings": warnings}
    add(args.id, args.title, md, data)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="cmd", required=True)
    c = commands.add_parser("context")
    c.add_argument("--cassandra-container")
    c.set_defaults(func=cmd_context)
    for name, func in [("add", cmd_add), ("build", cmd_build), ("tests", cmd_tests), ("readiness", cmd_readiness), ("node-stats", cmd_node_stats)]:
        c = commands.add_parser(name)
        c.add_argument("id")
        c.add_argument("title")
        c.set_defaults(func=func)
        if name == "add":
            c.add_argument("--markdown", required=True)
            c.add_argument("--json")
        elif name == "build":
            c.add_argument("command", nargs=argparse.REMAINDER)
        elif name == "tests":
            c.add_argument("logs", nargs="+")
        elif name == "readiness":
            c.add_argument("log")
        elif name == "node-stats":
            c.add_argument("container")
            c.add_argument("--keyspace", default="helios")
    args = parser.parse_args()
    if args.cmd != "build":
        # A report must never fail the job: an unexpected error is a warning.
        try:
            return args.func(args)
        except Exception as error:  # noqa: BLE001
            warn(f"{args.cmd} {getattr(args, 'title', '')}: the report failed: {error!r}")
            return 0
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
