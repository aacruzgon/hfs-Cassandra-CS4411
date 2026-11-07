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
  build ID TITLE -- COMMAND...      run COMMAND, sampling the job's cgroup; with
                                    cargo's --timings, keep its HTML and top units
  timed GROUP LABEL -- COMMAND...   run COMMAND and record its wall time in GROUP
  node-start ID TITLE CONTAINER GROUP  the node's boot to CQL, and GROUP's times
  tests ID TITLE LOG...             parse libtest output (`-- --show-output`)
  readiness ID TITLE LOG            the readiness test's `[key=ms]` pairs
  node-stats ID TITLE CONTAINER     nodetool and system_views on a node
  cql-audit ID TITLE FILE [--check] the node's CQL audit log (auditlogviewer
                                    output); --check fails on a CQL DELETE
                                    against resources or resource_versions
  cas ID TITLE LOG...               the `[cas case=… key=value …]` lines
  intents ID TITLE CONTAINER        intent rows left in pending_index_writes
  spec ID TITLE LOG                 the `[spec row=… expected=… got=…]` lines

A later step adds a section with `add`, or with a new command here.

`cql-audit --check` is the one command that is a check rather than a report:
it exits 1 when the audit log shows a CQL DELETE against `resources` or
`resource_versions`, or when it shows nothing, which would prove nothing
(roadmap Step 3's exit criterion; Step 3 plan D6).
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
        # On a pull request GITHUB_SHA is the merge commit; CI_REPORT_COMMIT
        # (the workflow sets it to the pull request's head) is what was pushed.
        "commit": os.environ.get("CI_REPORT_COMMIT") or os.environ.get("GITHUB_SHA"),
        "github_sha": os.environ.get("GITHUB_SHA"),
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
    # allow_nan=False: NaN is not JSON, and strict parsers reject it.
    (directory / f"{report_id}.json").write_text(json.dumps(document, indent=2, allow_nan=False))
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
    started_wall = time.time()
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
        timings = keep_timings(args.id, started_wall) if "--timings" in command else None
        report_build(args, command, status, wall, group, sampled, peaks, oom_kills(group) - oom_before if sampled else None, timings)
    except Exception as error:  # noqa: BLE001
        warn(f"{args.title}: the build report failed: {error!r}")
    return status


def keep_timings(report_id: str, since: float) -> dict:
    """cargo --timings writes target/cargo-timings/cargo-timing-<time>.html and
    a copy, cargo-timing.html. Keep this build's copy in the report, read its
    unit data, and delete the older timestamped files: the target dir is
    persistent, so they would otherwise pile up run after run."""
    target = Path(os.environ.get("CARGO_TARGET_DIR") or "target")
    directory = target / "cargo-timings"
    html = directory / "cargo-timing.html"
    try:
        fresh = html.stat().st_mtime >= since - 1
    except OSError:
        return {"html": None, "note": f"{html} was not written"}
    for old in directory.glob("cargo-timing-*.html"):
        try:
            if old.stat().st_mtime < since - 1:
                old.unlink()
        except OSError:
            pass
    if not fresh:
        return {"html": None, "note": f"{html} is older than this build"}
    text = html.read_text(errors="replace")
    (report_dir() / f"{report_id}-timings.html").write_text(text)
    match = re.search(r"const UNIT_DATA = (\[.*?\]);\s*$", text, re.MULTILINE | re.DOTALL)
    units = []
    if match:
        try:
            units = json.loads(match.group(1))
        except ValueError:
            units = []
    rows = sorted(
        # cargo names every unit that is not a build script "todo".
        ({"unit": " ".join(f"{u.get('name')} {u.get('version', '')} {u.get('target', '')}".split()),
          "mode": "compile" if u.get("mode") == "todo" else u.get("mode"),
          "seconds": round(float(u.get("duration") or 0), 2)} for u in units),
        key=lambda u: -u["seconds"],
    )
    total = re.search(r"<td>Total time:</td><td>([^<]+)</td>", text)
    fresh_units = re.search(r"<td>Fresh units:</td><td>([^<]+)</td>", text)
    return {
        "html": f"{report_id}-timings.html",
        "units": len(units),
        "total_time": total.group(1) if total else None,
        "fresh_units": fresh_units.group(1) if fresh_units else None,
        "slowest": rows[:10],
    }


def report_build(args, command, status, wall, group, sampled, peaks, oom, timings=None) -> None:
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
        "timings": timings,
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
    # Cold or warm: L10's "Target directory size" section says whether the
    # reset step reset the directory.
    if timings and timings.get("html"):
        md += (f"\n`cargo --timings`: {timings['units']} units compiled, {timings.get('fresh_units') or '?'} already up to "
               f"date, total {timings.get('total_time') or '?'}; the full chart is `{timings['html']}` in the artifact.")
        if timings["slowest"]:
            md += " Slowest units:\n\n" + table(["Unit", "Mode", "Time"], [[f"`{u['unit']}`", u["mode"], f"{u['seconds']:.1f} s"] for u in timings["slowest"]])
        else:
            md += "\n"
    elif timings:
        md += f"\n`cargo --timings`: {timings['note']}.\n"
    add(args.id, args.title, md, data)


# --- timed commands and the node's start ---------------------------------


def cmd_timed(args) -> int:
    """Runs COMMAND, appends {"label", "seconds", "exit_status"} to
    inputs/<group>.jsonl, and exits with COMMAND's status."""
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    started = time.monotonic()
    try:
        status = subprocess.call(command)
    except OSError as error:
        print(f"{command[0]}: {error}", file=sys.stderr)
        status = 127
    seconds = time.monotonic() - started
    try:
        inputs = report_dir() / "inputs"
        inputs.mkdir(exist_ok=True)
        with open(inputs / f"{args.group}.jsonl", "a", encoding="utf-8") as out:
            out.write(json.dumps({"label": args.label, "seconds": round(seconds, 2), "exit_status": status}) + "\n")
        print(f"{args.label}: {seconds:.1f} s", flush=True)
    except Exception as error:  # noqa: BLE001
        warn(f"timed {args.label}: could not record the time: {error!r}")
    return status


CQL_LISTENING = "Starting listening for CQL clients"


def iso(text: str):
    """A docker timestamp (RFC 3339, nanoseconds) as seconds since the epoch."""
    from datetime import datetime

    match = re.match(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)", text.strip())
    if not match:
        return None
    zone = "+00:00" if match.group(3) == "Z" else match.group(3)
    base = datetime.fromisoformat(match.group(1) + zone).timestamp()
    return base + float("0." + match.group(2)) if match.group(2) else base


def cmd_node_start(args) -> int:
    rows = []
    path = report_dir() / "inputs" / f"{args.group}.jsonl"
    for line in (read(path) or "").splitlines():
        try:
            rows.append(json.loads(line))
        except ValueError:
            pass
    # From the container's own clock: its start to the native transport's
    # line, which is when CQL could first answer, without the healthcheck's
    # polling interval.
    started_text = (run(["docker", "inspect", "-f", "{{.State.StartedAt}}", args.container]) or "").strip()
    started = iso(started_text)
    logs = None
    if started_text:
        # Only the log since the container's last start, so a restarted
        # container's earlier boot is not read.
        try:
            logs = subprocess.run(["docker", "logs", "--timestamps", "--since", started_text, args.container],
                                  capture_output=True, text=True, timeout=60)
        except (OSError, subprocess.TimeoutExpired):
            logs = None
    listening = None
    for line in ((logs.stdout + logs.stderr) if logs else "").splitlines():
        if CQL_LISTENING in line:
            listening = iso(line.split(" ", 1)[0])
            break
    to_cql = round(listening - started, 2) if started and listening else None
    warnings = []
    if to_cql is None:
        warnings.append(f"Node start: no {CQL_LISTENING!r} line, or no start time, for {args.container}")
    if not rows:
        warnings.append(f"Node start: no timed steps in {path.name}")
    md = table(["Step", "Wall time", "Result"], [[r["label"], f"{r['seconds']:.1f} s", "ok" if r["exit_status"] == 0 else f"exit {r['exit_status']}"] for r in rows])
    md += (f"\nContainer start → \"{CQL_LISTENING}\" (from the container's own log timestamps): "
           f"{'%.1f s' % to_cql if to_cql is not None else 'not read'}. The healthcheck then needs `cqlsh` to answer, "
           "polled every 10 s, so the start step's wall time is longer.\n")
    add(args.id, args.title, md, {"container": args.container, "container_start_to_cql_s": to_cql, "steps": rows, "warnings": warnings})
    return 0


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
    total = lambda a, b: None if figures[a] is None or figures[b] is None else figures[a] + figures[b]
    md = table(
        ["Stop issued → `/_readiness` 503 (upper bound)", "`docker stop` took", "Start issued → `/_readiness` 200", "`docker start` took", "Test"],
        [[seconds(total("stop_ms", "stopped_to_503_ms")), seconds(figures["stop_ms"]),
          seconds(total("start_ms", "started_to_200_ms")), seconds(figures["start_ms"]),
          result.group(1) if result else "no result line"]],
    )
    md += (
        "\nFrom the readiness test's own `[key=ms]` lines, on this job's node (`docker stop --time 30`, then "
        "`docker start`). `/_readiness` is polled every 0.5 s, from the moment each command returns. The node stops "
        "serving during `docker stop`, so the 503 often comes before the stop returns: the stop's figure is an upper "
        "bound, and the time from the command's return alone is usually 0.\n"
    )
    add(args.id, args.title, md, {**figures, "stop_issued_to_503_upper_bound_ms": total("stop_ms", "stopped_to_503_ms"),
                                  "start_issued_to_200_ms": total("start_ms", "started_to_200_ms"), "test_result": result.group(1) if result else None, "warnings": warnings})
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

# The design's trigger for time-bucketing a partition: tombstones scanned per
# read above the warn threshold (cassandra-discussion.md, "Decision: measure
# partition growth first, bucket on a trigger").
TOMBSTONE_TRIGGER = 1000

VIRTUAL_TABLES = ["local_read_latency", "local_write_latency", "tombstones_per_read", "rows_per_read", "max_partition_size"]


def number(text: str):
    """A number, or None for nodetool's NaN (no samples yet), which JSON has no
    value for."""
    try:
        value = float(text)
    except ValueError:
        return text
    if value != value or value in (float("inf"), float("-inf")):
        return None
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


def node_info(out: str) -> dict:
    info = {}
    for line in out.splitlines():
        key, _, value = line.partition(":")
        key, value = key.strip(), value.strip()
        if key == "Heap Memory (MB)":
            used, _, total = value.partition("/")
            info["heap_used_mb"], info["heap_total_mb"] = number(used.strip()), number(total.strip())
        elif key == "Off Heap Memory (MB)":
            info["off_heap_mb"] = number(value)
        elif key == "Uptime (seconds)":
            info["uptime_s"] = number(value)
        elif key == "Load":
            info["load"] = value
    return info


def gc_stats(out: str) -> dict:
    """`nodetool gcstats`: a header line, then one line of values."""
    lines = [line for line in out.splitlines() if line.strip()]
    for index, line in enumerate(lines[:-1]):
        if "Interval" in line and "GC" in line:
            values = lines[index + 1].split()
            names = ["interval_ms", "max_gc_elapsed_ms", "total_gc_elapsed_ms", "stdev_gc_elapsed_ms",
                     "gc_reclaimed_mb", "collections", "direct_memory_bytes"]
            return {name: number(value) for name, value in zip(names, values)}
    return {}


def compaction_stats(out: str) -> dict:
    # 5.0 prints "pending tasks  0" (4.x: "pending tasks: 0").
    stats = {}
    for key, name in [("pending tasks", "pending"), ("compactions completed", "completed"), ("compactions aborted", "aborted")]:
        found = re.search(rf"^{key}:?\s+(\d+)\s*$", out, re.MULTILINE)
        if found:
            stats[name] = int(found.group(1))
    match = "pending" in stats
    # Each active compaction is a row under the header that starts with "id".
    rows, header = 0, False
    for line in out.splitlines():
        cells = line.split()
        if cells[:1] == ["id"]:
            header = True
        elif header and cells and not line.strip().startswith("active compaction remaining time"):
            rows += 1
    if match:
        stats["active"] = rows
    return stats


def container_memory(out: str) -> dict:
    memory = {}
    for line in out.splitlines():
        key, _, value = line.strip().partition(" ")
        value = value.strip()
        memory[key] = int(value) if value.isdigit() else (None if value in ("", "max") else value)
    return memory


def cmd_node_stats(args) -> int:
    container = args.container
    started = time.monotonic()
    nodetool = lambda *a: run(["docker", "exec", container, "nodetool", *a], timeout=120)
    raw = {
        # First: every nodetool call below is a JVM in the container's cgroup
        # (with the compose file's MAX_HEAP_SIZE), and would raise its peak.
        "container_memory": run(["docker", "exec", container, "sh", "-c",
                                 "cd /sys/fs/cgroup && for f in memory.peak memory.current memory.max; do echo \"$f $(cat $f 2>/dev/null)\"; done; "
                                 "grep -E '^(anon|file) ' memory.stat; grep -E '^(oom|oom_kill) ' memory.events; true"]),
        "tablestats": nodetool("tablestats", args.keyspace),
        "tpstats": nodetool("tpstats"),
        "proxyhistograms": nodetool("proxyhistograms"),
        "cas": nodetool("sjk", "mx", "-mg", "-all", "-f", "Count", "-b", "org.apache.cassandra.metrics:type=ClientRequest,scope=CAS*,name=*"),
        "info": nodetool("info"),
        "gcstats": nodetool("gcstats"),
        "compactionstats": nodetool("compactionstats"),
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
    jvm = node_info(raw["info"] or "")
    gc = gc_stats(raw["gcstats"] or "")
    compaction = compaction_stats(raw["compactionstats"] or "")
    memory = container_memory(raw["container_memory"] or "")
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
                 "Local writes", "Write p99 (ms)", "Reads (tombstone histogram)", "Tombstones per read (max)",
                 "Tombstones per read (p99)", "Droppable tombstone ratio"], rows)
    md += "\n**Lightweight transactions** (`ClientRequest` CASWrite/CASRead, since the node started):\n\n"
    cas_rows = []
    for scope in ("CASWrite", "CASRead"):
        requests = cas.get(f"{scope}.Latency")
        total_us = cas.get(f"{scope}.TotalLatency")
        if requests is None:
            continue
        mean = f"{total_us / requests / 1000:.2f} ms" if requests and total_us is not None else None
        others = ", ".join(f"{name.split('.', 1)[1]} {value}" for name, value in sorted(cas.items())
                           if name.startswith(scope + ".") and value and not name.endswith(("Latency", "TotalLatency")))
        cas_rows.append([scope, requests, mean, others or "none"])
    md += table(["Scope", "Requests", "Mean latency", "Other non-zero counts"], cas_rows) if cas_rows else "Not read.\n"
    if histogram:
        md += "\nCAS latency (`nodetool proxyhistograms`, µs):\n\n"
        md += table(["Percentile", "CAS read", "CAS write"], [[r["percentile"], r["cas_read_us"], r["cas_write_us"]] for r in histogram])
    busy = [p for p in pools if p["completed"] or p["pending"] or p["blocked"] or p["all_time_blocked"]]
    md += "\n**Thread pools** (`nodetool tpstats`, pools with any work):\n\n"
    md += table(["Pool", "Active", "Pending", "Completed", "Blocked", "All-time blocked"],
                [[p["pool"], p["active"], p["pending"], p["completed"], p["blocked"], p["all_time_blocked"]] for p in busy])
    md += "\n**JVM and container** (`nodetool info`, `gcstats` since the last call, `compactionstats`; the container's cgroup):\n\n"
    mib = lambda b: None if b is None else f"{b / MIB:,.0f} MiB"
    md += table(["Figure", "Value"], [
        ["Heap used / total", f"{jvm.get('heap_used_mb')} / {jvm.get('heap_total_mb')} MB" if jvm.get("heap_used_mb") is not None else None],
        ["Off heap", f"{jvm.get('off_heap_mb')} MB" if jvm.get("off_heap_mb") is not None else None],
        ["Uptime", f"{jvm.get('uptime_s')} s" if jvm.get("uptime_s") is not None else None],
        ["GC collections / total pause / max pause", f"{gc.get('collections')} / {gc.get('total_gc_elapsed_ms')} ms / {gc.get('max_gc_elapsed_ms')} ms" if gc else None],
        ["Compactions pending / active / completed / aborted", f"{compaction.get('pending')} / {compaction.get('active')} / {compaction.get('completed')} / {compaction.get('aborted')}" if compaction else None],
        ["Container memory peak / limit", f"{mib(memory.get('memory.peak'))} / {mib(memory.get('memory.max'))}" if memory.get("memory.peak") is not None else None],
        ["Container memory now (anon, page cache)", f"{mib(memory.get('memory.current'))} ({mib(memory.get('anon'))}, {mib(memory.get('file'))})" if memory.get("memory.current") is not None else None],
        ["Container OOM kills", memory.get("oom_kill")],
    ])
    if memory.get("memory.peak") is not None and memory.get("memory.max"):
        share = memory["memory.peak"] / memory["memory.max"]
        md += (f"\nThe peak is {share:.0%} of the limit (ADR-aacruzgon-0002), page cache included; it covers the node's life up to "
               "this snapshot, read before this step's own `nodetool` calls. Anything run in the container counts against the "
               "same limit: the healthcheck's `cqlsh` and every `docker exec`, `nodetool` included (a JVM each).\n")
        if share > 0.9:
            warnings.append(f"Node statistics: the container's memory peak is {share:.0%} of its limit")
    if memory.get("oom_kill"):
        warnings.append(f"Node statistics: {memory['oom_kill']} OOM kill(s) in the Cassandra container")
    dropped_any = [d for d in dropped if d["dropped"]]
    md += "\nDropped messages: " + (", ".join(f"{d['message_type']} {d['dropped']}" for d in dropped_any) or "none") + ".\n"
    md += (
        "\nLatency and tombstone columns are `system_views` histograms (recent, decaying); "
        f"a table over {TOMBSTONE_TRIGGER} tombstones per read, the design's bucketing trigger, is a warning. "
        "Raw outputs are in the artifact, next to this report.\n"
    )
    for name, row in views.get("tombstones_per_read", {}).items():
        if (row.get("max") or 0) > TOMBSTONE_TRIGGER:
            warnings.append(f"Node statistics: a read of {name} scanned {row['max']:.0f} tombstones, over the design's bucketing trigger of {TOMBSTONE_TRIGGER}")
    if dropped_any:
        warnings.append("Node statistics: the node dropped messages: " + ", ".join(d["message_type"] for d in dropped_any))
    crud_tombstones, crud_md, crud_warnings = crud_tombstone_rows(views.get("tombstones_per_read", {}))
    md += crud_md
    warnings += crud_warnings
    data = {"container": container, "keyspace": args.keyspace, "seconds": round(took, 1), "tables": tables,
            "crud_tombstones": crud_tombstones,
            "system_views": views, "cas_counts": cas, "cas_latency_us": histogram, "thread_pools": pools,
            "jvm": jvm, "gc": gc, "compaction": compaction, "container_memory": memory,
            "dropped_messages": dropped, "warnings": warnings}
    add(args.id, args.title, md, data)
    return 0


# The soft-delete tables never hold a tombstone (schema.cql, G39; ADR-aacruzgon-0027),
# so a read of either that scans one means a CQL DELETE or a bound null reached
# it. resources_by_type moves on every update and loses its row on every delete
# (the X1 delta), so it is expected to show tombstones from Step 3 on.
# The histogram's lowest bucket is 1: a read that scanned no tombstone reports
# 1 (checked on a 5.0.9 node: a table never deleted from showed max 1.0, and
# two deleted rows showed 2.0). So only a figure above 1 shows a tombstone.
TOMBSTONE_FLOOR = 1
CRUD_TOMBSTONE_TABLES = [
    ("resources", "1 or less: soft delete writes cells, never a tombstone (G39)"),
    ("resource_versions", "1 or less: history rows are never deleted (G39)"),
    ("resources_by_type", "above 1 from Step 3: update and delete remove the X1 row"),
]


def crud_tombstone_rows(tombstones: dict) -> tuple[list[dict], str, list[str]]:
    """R6: `tombstones_per_read` for the three tables Step 3 writes, with what
    each should show, and a warning when a soft-delete table read one."""
    rows, warnings = [], []
    for name, expected in CRUD_TOMBSTONE_TABLES:
        row = tombstones.get(name, {})
        entry = {"table": name, "reads": row.get("count"), "max": row.get("max"), "p99": row.get("p99th"), "expected": expected}
        rows.append(entry)
        if name != "resources_by_type" and (row.get("max") or 0) > TOMBSTONE_FLOOR:
            warnings.append(f"Node statistics: a read of {name} scanned up to {row['max']:.0f} tombstones; it should never hold one (G39)")
    md = ("\n**Tombstones on the tables Step 3 writes** (`system_views.tombstones_per_read`, a recent, decaying "
          "histogram; it shows tombstones that reads scanned, not every one written. Its lowest bucket is 1, so 1 also means "
          "none: only a figure above 1 shows a tombstone):\n\n")
    md += table(["Table", "Reads", "Tombstones per read (max)", "(p99)", "Expected"],
                [[f"`{r['table']}`", r["reads"], r["max"], r["p99"], r["expected"]] for r in rows])
    return rows, md, warnings


# --- the CQL audit log (Step 3 plan D6 and report R7) -----------------------

# The tables that only ever take a soft delete (schema.cql "never a CQL DELETE
# against resources"; G39). Compared exactly: Step 3 legitimately deletes from
# resources_by_type (X1) and pending_index_writes (I2).
PROTECTED_TABLES = ("resources", "resource_versions")

# A DELETE statement against a protected table, anywhere in an entry's CQL
# text, keyspace-qualified or quoted or not. The name must end there, so
# resources_by_type never matches. It catches what the type and scope alone
# miss (AuditLogManager.java, 5.0.9): a CQL-text batch, logged as one
# scope-less BATCH entry holding every child; and a failed statement, whose
# type becomes REQUEST_FAILURE and whose scope is dropped when it was not
# prepared. It is not tied to the start of a statement, so a string literal or
# comment naming such a DELETE would also match: a false failure, never a
# missed one.
DELETE_TEXT = re.compile(
    r'\bDELETE\b[^;]*?\bFROM\s+(?:"?\w+"?\s*\.\s*)?"?(resources|resource_versions)"?(?![\w"])',
    re.IGNORECASE | re.DOTALL,
)
# Things the schema says must never exist (schema/README.md), that the audit
# text can show: a plain (non-LWT) write to resources, and ALLOW FILTERING.
# Bound values are not logged, so a bound null cannot be seen here.
NOT_LWT = re.compile(r"\bIF\b", re.IGNORECASE)
# Statements that remove a whole table's data: a violation too, by scope.
TABLE_WIPES = ("TRUNCATE", "DROP_TABLE")
ALLOW_FILTERING = re.compile(r"\bALLOW\s+FILTERING\b", re.IGNORECASE)
# A failed native-protocol batch logs one REQUEST_FAILURE entry, "BATCH of N
# statements at consistency X; <error>", with no keyspace from its statements,
# no scope and no child text (AuditLogManager.batchFailure, 5.0.9). So a DELETE
# inside it cannot be ruled out, and it may have applied: a logged batch that
# timed out is still replayed from the batchlog (verified on 5.0.9: 400
# DELETEs on resources, applied about a minute after the WriteTimeout). Under
# --check it fails the check as unverifiable. The backend sends no native
# batch, so the backend never trips it.
FAILED_BATCH = re.compile(r"^BATCH of \d+ statements\b")


def open_text(path: Path) -> str:
    if path.suffix == ".gz":
        import gzip

        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as source:
            return source.read()
    return path.read_text(errors="replace")


def audit_records(text: str) -> list[dict]:
    """`auditlogviewer` output: each record is a `Type: <type>` line, then
    `LogMessage: <entry>` (tools/AuditLogViewer.java). An entry's CQL text may
    span lines, so a record runs to the next `Type: ` line."""
    messages, current = [], None
    for line in text.splitlines():
        if line.startswith("Type: "):
            if current is not None:
                messages.append(current)
            current = None
        elif line.startswith("LogMessage: "):
            if current is not None:
                messages.append(current)
            current = line[len("LogMessage: "):]
        elif current is not None:
            current += "\n" + line
    if current is not None:
        messages.append(current)
    return [audit_entry(message) for message in messages]


def audit_entry(message: str) -> dict:
    """One entry, `user:…|host:…|source:…|port:…|timestamp:…|type:…|category:…
    [|batch:…][|ks:…][|scope:…][|operation:<CQL>]` (AuditLogEntry.getLogString).
    `operation` is last and is free text, so the fields are read only from the
    part before it."""
    head, found, operation = message.partition("|operation:")
    fields = {}
    for part in head.split("|"):
        key, _, value = part.partition(":")
        fields.setdefault(key, value)
    return {
        "type": fields.get("type"),
        "category": fields.get("category"),
        "ks": fields.get("ks"),
        "scope": fields.get("scope"),
        "batch": fields.get("batch"),
        "operation": operation if found else None,
    }


def in_keyspace(entry: dict, keyspace: str) -> bool:
    """An entry from `keyspace`, or one with no keyspace: a failed batch, or a
    scope-less entry from a session with no current keyspace, may be ours."""
    return entry["ks"] in (None, "", keyspace)


def audit_unverifiable(entry: dict, keyspace: str) -> str | None:
    """Why an entry hides statements the check cannot see, or None."""
    if (in_keyspace(entry, keyspace) and entry["type"] == "REQUEST_FAILURE"
            and FAILED_BATCH.match(entry["operation"] or "")):
        return "a failed native-protocol batch, whose statements the audit log does not record"
    return None


def audit_violation(entry: dict, keyspace: str) -> str | None:
    """Why an entry is a CQL DELETE against a protected table, or None."""
    if not in_keyspace(entry, keyspace):
        return None
    if entry["type"] == "DELETE" and entry["scope"] in PROTECTED_TABLES:
        return f"type:DELETE with scope:{entry['scope']}"
    if entry["type"] in TABLE_WIPES and entry["scope"] in PROTECTED_TABLES:
        return f"type:{entry['type']} with scope:{entry['scope']}"
    match = DELETE_TEXT.search(entry["operation"] or "")
    if match:
        return f"a DELETE FROM {match.group(1)} in the CQL text of a {entry['type']} entry"
    return None


def cmd_cql_audit(args) -> int:
    path = Path(args.file)
    started = time.monotonic()
    text = open_text(path) if path.exists() else None
    entries = audit_records(text or "")
    took = time.monotonic() - started
    ours = [e for e in entries if e["ks"] == args.keyspace]
    dml = [e for e in ours if e["category"] == "DML"]
    violations, unverifiable = [], []
    for entry in entries:
        for found, reason in ((violations, audit_violation(entry, args.keyspace)),
                              (unverifiable, audit_unverifiable(entry, args.keyspace))):
            if reason:
                found.append({"reason": reason, "type": entry["type"], "scope": entry["scope"],
                              "operation": " ".join((entry["operation"] or "").split())[:500]})
    counts = {}
    for entry in ours:
        key = (entry["scope"] or "(none)", entry["type"] or "?")
        counts[key] = counts.get(key, 0) + 1
    plain_writes = [e for e in ours if e["scope"] == "resources" and e["type"] == "UPDATE"
                    and not NOT_LWT.search(e["operation"] or "")]
    filtering = [e for e in ours if ALLOW_FILTERING.search(e["operation"] or "")]
    prepares = sum(1 for e in ours if e["type"] == "PREPARE_STATEMENT")
    failures = [e for e in entries if in_keyspace(e, args.keyspace) and e["category"] == "ERROR"]

    problems, warnings = [], []
    if text is None:
        problems.append(f"no audit log: {path} does not exist (the audit-log dump did not run)")
    elif not dml:
        problems.append(f"the audit log has no DML entry for keyspace {args.keyspace}, so it proves nothing "
                        "(was `nodetool enableauditlog` run before the tests?)")
    for v in violations:
        problems.append(f"CQL DELETE against a soft-delete table: {v['reason']}: {v['operation'][:200]}")
    for u in unverifiable:
        problems.append(f"cannot rule out a CQL DELETE: {u['reason']}: {u['operation'][:200]}")
    if plain_writes:
        warnings.append(f"CQL audit: {len(plain_writes)} plain (non-IF) write(s) to resources; every write there must be a CAS (schema/README.md)")
    if filtering:
        warnings.append(f"CQL audit: {len(filtering)} prepared or DML statement(s) with ALLOW FILTERING (schema/README.md: never)")
    if failures:
        warnings.append(f"CQL audit: {len(failures)} failed statement(s) (REQUEST_FAILURE) in keyspace {args.keyspace} or with none")
    if not args.check:
        warnings += problems

    verdict = ("**Check: failed.** " + " ".join(problems)) if problems else (
        f"**Check: passed.** No CQL `DELETE` against `{PROTECTED_TABLES[0]}` or `{PROTECTED_TABLES[1]}` "
        f"in {len(entries):,} entries ({len(dml):,} DML in `{args.keyspace}`).")
    md = verdict + "\n\n"
    md += (f"From the node's audit log (`nodetool enableauditlog`, BinAuditLogger, categories DML, ERROR, PREPARE and DDL, "
           "every keyspace but the system ones, so an entry with no keyspace is kept), read with `auditlogviewer` after "
           "the tests. A violation is an entry with "
           "`type:DELETE` and a scope of exactly `resources` or `resource_versions`, or any entry whose CQL text holds "
           "a `DELETE … FROM` either table: a CQL-text batch logs one scope-less `BATCH` entry, and a failed statement "
           "a `REQUEST_FAILURE`. A `TRUNCATE` or `DROP TABLE` of either table is one too. A failed native-protocol batch "
           "fails the check as well: its entry holds no statement text, and a logged batch that timed out may still "
           "apply. `resources_by_type` (X1) and "
           f"`pending_index_writes` (I2) are deleted from legitimately. Parsed in {took:.1f} s.\n\n")
    md += "**DML by table and type** (R7; `UPDATE` is an INSERT or an UPDATE; the driver may re-send an idempotent statement, so a count can exceed one per operation):\n\n"
    tables = sorted({scope for scope, _ in counts})
    types = sorted({kind for _, kind in counts})
    md += table(["Table"] + [f"`{t}`" for t in types] + ["Total"],
                [[f"`{s}`"] + [counts.get((s, t)) for t in types] + [sum(counts.get((s, t), 0) for t in types)] for s in tables])
    md += (f"\nPrepared statements: {prepares}. Failed statements: {len(failures)}, of which failed native batches: "
           f"{len(unverifiable)}. Plain writes to `resources`: "
           f"{len(plain_writes)}. `ALLOW FILTERING` (prepared or DML statements; plain queries are not logged): {len(filtering)}.\n")
    if violations:
        md += "\n**Violations:**\n\n" + table(["Reason", "CQL"], [[v["reason"], f"`{v['operation'][:300]}`"] for v in violations[:20]])
    if unverifiable:
        md += "\n**Unverifiable:**\n\n" + table(["Reason", "Entry"], [[u["reason"], f"`{u['operation'][:300]}`"] for u in unverifiable[:20]])
    data = {
        "file": str(path), "entries": len(entries), "keyspace_entries": len(ours), "dml_entries": len(dml),
        "by_table_and_type": [{"table": s, "type": t, "count": n} for (s, t), n in sorted(counts.items())],
        "prepares": prepares, "failures": len(failures), "plain_writes_to_resources": len(plain_writes),
        "allow_filtering": len(filtering), "violations": violations, "unverifiable": unverifiable,
        "check": bool(args.check),
        "check_passed": not problems, "parse_seconds": round(took, 2), "warnings": warnings,
    }
    add(args.id, args.title, md, data)
    if args.check and problems:
        for problem in problems[:10]:
            print(f"::error title=CQL audit (Step 3 exit criterion)::{problem.replace(chr(10), ' ')}", flush=True)
        return 1
    return 0


# --- key=value lines: CAS counters (R2, R3, R8) and HL7 rows (R10) ----------

# One `key=value` pair: the value is a double-quoted string with JSON
# escapes (Rust's `{:?}` of a plain string), or a bare token, which may hold
# quoted runs, so a weak ETag `W/"1"` is one value, not `W/`.
QUOTED = r'"(?:[^"\\]|\\.)*"'
PAIR = re.compile(r'([A-Za-z_][\w.-]*)=((?:' + QUOTED + r'|[^\s\]"])+)')


def tagged_lines(text: str, tag: str) -> list[dict]:
    """Every `[<tag> key=value …]` in a log, in order, as a dict of strings."""
    pattern = re.compile(r"\[" + re.escape(tag) + r'\s+((?:"(?:[^"\\]|\\.)*"|[^\]"])*)\]')
    found = []
    for raw in text.splitlines():
        for match in pattern.finditer(ANSI.sub("", raw)):
            pairs = {}
            for key, value in PAIR.findall(match.group(1)):
                if re.fullmatch(QUOTED, value):
                    try:
                        value = json.loads(value)
                    except ValueError:
                        value = value[1:-1]
                pairs[key] = value
            found.append(pairs)
    return found


# CAS counters that should be 0 on a healthy single node: a write left
# undecidable, a retry budget run out, a post-CAS write that ran out of its
# tries (G23, R8). Matched by name, so the adapter's names decide.
CAS_WARN_KEYS = re.compile(r"undecidable|exhausted|post_cas")

# Logs whose CAS lines come from the engine's own tests, which drive those
# outcomes on purpose through the fault seam (an undecidable write, a budget
# run out): tabulated, never warned on, so a real one elsewhere stands out.
CAS_FAULT_LOGS = {"persistence-lib-tests"}

# Cases a full run prints: the race wrappers' (`cassandra_tests.rs`,
# `step3_suites`) and the latency report's. A run with none of one kind has
# lost its lines (a dropped --show-output, a renamed case), so it warns.
CAS_EXPECTED_CASES = ("race:", "latency-report:")


def cmd_cas(args) -> int:
    rows, missing = [], []
    for log in args.logs:
        path = Path(log)
        if not path.exists():
            missing.append(path.name)
            continue
        for pairs in tagged_lines(path.read_text(errors="replace"), "cas"):
            pairs.setdefault("case", "?")
            rows.append({"log": path.stem, **{k: number(v) if k != "case" else v for k, v in pairs.items()}})
    keys = []
    for row in rows:
        keys += [k for k in row if k not in ("log", "case") and k not in keys]
    warnings = [f"CAS counters: no log {m}" for m in missing]
    for prefix in CAS_EXPECTED_CASES:
        if not any(str(r["case"]).startswith(prefix) for r in rows):
            warnings.append(f"CAS counters: no `{prefix}…` case in the logs; its lines were not printed")
    for row in rows:
        for key in keys:
            value = row.get(key)
            if row["log"] in CAS_FAULT_LOGS:
                continue
            if CAS_WARN_KEYS.search(key) and isinstance(value, (int, float)) and value > 0:
                warnings.append(f"CAS counters: {row['case']} has {key}={value}; expected 0 on a healthy node")
        if isinstance(row.get("attempts"), (int, float)) and isinstance(row.get("applied"), (int, float)) and row["applied"]:
            row["attempts_per_applied"] = round(row["attempts"] / row["applied"], 3)
    if rows and any("attempts_per_applied" in r for r in rows):
        keys.append("attempts_per_applied")
    if rows:
        md = table(["Log", "Case"] + [f"`{k}`" for k in keys], [[r["log"], f"`{r['case']}`"] + [r.get(k) for k in keys] for r in rows])
        md += ("\nOne row per `[cas case=… key=value …]` line: the backend's CAS outcome counters "
               "(`CassandraBackend::cas_stats()`, L1) over what that case did. Rows are per backend instance, "
               "so they are not summed. `attempts_per_applied` is the contention figure (R3). Rows from "
               + ", ".join(f"`{n}`" for n in sorted(CAS_FAULT_LOGS))
               + " are the engine's own fault-seam tests, which provoke undecidable writes and exhausted "
               "budgets on purpose, so they are never warned on.\n")
    else:
        md = ("No `[cas case=…]` line in the logs: no test printed the backend's `cas_stats()`, "
              "or no test printed them.\n")
    if missing:
        md += "\nNo log for: " + ", ".join(f"`{m}`" for m in missing) + ".\n"
    add(args.id, args.title, md, {"rows": rows, "keys": keys, "missing_logs": missing, "warnings": warnings})
    return 0


def cmd_spec(args) -> int:
    path = Path(args.log)
    lines = tagged_lines(path.read_text(errors="replace"), "spec") if path.exists() else []
    rows = [{"row": p.get("row"), "expected": p.get("expected"), "got": p.get("got"),
             "detail": " ".join(f"{k}={v}" for k, v in p.items() if k not in ("row", "expected", "got")) or None}
            for p in lines]
    mismatches = [r for r in rows if r["expected"] != r["got"]]
    warnings = []
    if not path.exists():
        warnings.append(f"HL7 rows: no log {path.name} (the Step 3 REST test did not run)")
    elif not rows:
        warnings.append(f"HL7 rows: {path.name} has no `[spec row=… expected=… got=…]` line")
    for r in mismatches:
        warnings.append(f"HL7 rows: {r['row']} expected {r['expected']}, got {r['got']}")
    md = table(["Matrix row", "Expected", "Observed", "Match", "Detail"],
               [[f"`{r['row']}`", r["expected"], r["got"], "yes" if r["expected"] == r["got"] else "**no**", r["detail"]] for r in rows]) if rows else ""
    md += (f"\n{len(rows)} check(s) observed through `hfs` on Cassandra, {len(mismatches)} mismatch(es). Each row is one "
           "`[spec row=… expected=… got=…]` line the test printed; the row ids are the Step 3 HL7 requirements matrix's.\n")
    add(args.id, args.title, md, {"rows": rows, "mismatches": len(mismatches), "warnings": warnings})
    return 0


# --- intent rows left behind (R4) -------------------------------------------

INTENT_SHARDS = 16  # schema.cql, pending_index_writes; write.rs INTENT_SHARDS
TEST_TENANT = re.compile(r"^test-(.*)-[0-9a-f]{32}$")


def cql_text(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def json_rows(out: str) -> list[dict]:
    rows = []
    for line in (out or "").splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass
    return rows


def intent_buckets(now, hours: int) -> list[str]:
    from datetime import timedelta

    return [(now - timedelta(hours=h)).strftime("%Y-%m-%dT%H") for h in range(hours)]


def classify_intent(intent: dict, current: dict | None, read: bool = True) -> str:
    if not read:
        return "not read (over --limit)"
    if current is None:
        return "no resource row"
    if intent.get("version_id") is not None and current.get("version_id") is not None and intent["version_id"] <= current["version_id"]:
        return "at or below the current version"
    return "above the current version"


def cmd_intents(args) -> int:
    from datetime import datetime, timezone

    started = time.monotonic()
    buckets = intent_buckets(datetime.now(timezone.utc), args.hours)
    cqlsh = lambda q: run(["docker", "exec", args.container, "cqlsh", "-e", q], timeout=120)
    out = cqlsh(f"SELECT JSON bucket, shard, tenant_id, resource_type, id, version_id FROM {args.keyspace}.pending_index_writes "
                f"WHERE bucket IN ({', '.join(cql_text(b) for b in buckets)}) AND shard IN ({', '.join(str(s) for s in range(INTENT_SHARDS))});")
    warnings = []
    if out is None:
        warnings.append(f"Intent rows: pending_index_writes could not be read from {args.container}")
    intents = [r for r in json_rows(out) if str(r.get("tenant_id", "")).startswith(args.tenant_prefix)]
    keys = sorted({(r["tenant_id"], r["resource_type"], r["id"]) for r in intents})[: args.limit]
    looked_up = set(keys)
    current = {}
    if keys:
        reads = " ".join(f"SELECT JSON tenant_id, resource_type, id, version_id, deleted_at FROM {args.keyspace}.resources "
                         f"WHERE tenant_id = {cql_text(t)} AND resource_type = {cql_text(rt)} AND id = {cql_text(i)};" for t, rt, i in keys)
        for row in json_rows(cqlsh(reads)):
            current[(row.get("tenant_id"), row.get("resource_type"), row.get("id"))] = row
    by_label, by_class = {}, {}
    for intent in intents:
        match = TEST_TENANT.match(intent["tenant_id"])
        label = match.group(1) if match else intent["tenant_id"]
        key = (intent["tenant_id"], intent["resource_type"], intent["id"])
        kind = classify_intent(intent, current.get(key), key in looked_up)
        intent["class"] = kind
        by_label.setdefault(label, {}).setdefault(kind, 0)
        by_label[label][kind] += 1
        by_class[kind] = by_class.get(kind, 0) + 1
    took = time.monotonic() - started
    md = (f"**{len(intents)} intent row(s)** left in `pending_index_writes` for `{args.tenant_prefix}` tenants, in the buckets "
          f"{', '.join(f'`{b}`' for b in buckets)} (all {INTENT_SHARDS} shards). Read in {took:.1f} s.\n\n")
    if intents:
        kinds = sorted(by_class)
        md += table(["Test (tenant label)"] + kinds + ["Total"],
                    [[f"`{label}`"] + [counts.get(k) for k in kinds] + [sum(counts.values())] for label, counts in sorted(by_label.items())])
    md += ("\nA finished write deletes its intent (I2), and so does a definite miss. One is left on purpose after an "
           "unknown outcome, an undecidable write, a failed serial read, or a post-CAS write that ran out of its tries "
           "(cassandra-discussion.md, the failure table; ADR-aacruzgon-0034), for the sweeper (Step 5). \"Above the current "
           "version\" is an attempt that never applied; \"at or below\" one that applied and left its intent. Report only.\n")
    add(args.id, args.title, md, {"buckets": buckets, "tenant_prefix": args.tenant_prefix, "intents": len(intents),
                                  "by_class": by_class, "by_test": by_label, "rows": intents[:500],
                                  "seconds": round(took, 1), "warnings": warnings})
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="cmd", required=True)
    c = commands.add_parser("context")
    c.add_argument("--cassandra-container")
    c.set_defaults(func=cmd_context)
    c = commands.add_parser("timed")
    c.add_argument("group")
    c.add_argument("label")
    c.add_argument("command", nargs=argparse.REMAINDER)
    c.set_defaults(func=cmd_timed)
    for name, func in [("add", cmd_add), ("build", cmd_build), ("tests", cmd_tests), ("readiness", cmd_readiness), ("node-stats", cmd_node_stats), ("node-start", cmd_node_start),
                       ("cql-audit", cmd_cql_audit), ("cas", cmd_cas), ("intents", cmd_intents), ("spec", cmd_spec)]:
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
        elif name == "node-start":
            c.add_argument("container")
            c.add_argument("group")
        elif name == "cql-audit":
            c.add_argument("file")
            c.add_argument("--keyspace", default="helios")
            c.add_argument("--check", action="store_true",
                           help="exit 1 on a CQL DELETE against resources or resource_versions, or an empty log")
        elif name == "cas":
            c.add_argument("logs", nargs="+")
        elif name == "intents":
            c.add_argument("container")
            c.add_argument("--keyspace", default="helios")
            c.add_argument("--tenant-prefix", default="test-")
            # The job runs well under 40 minutes, so this hour and the two
            # before it hold every intent it wrote.
            c.add_argument("--hours", type=int, default=3)
            c.add_argument("--limit", type=int, default=500, help="most resources whose current version is read")
        elif name == "spec":
            c.add_argument("log")
    args = parser.parse_args()
    if args.cmd == "cql-audit" and args.check:
        # A check, not a report: an error must not pass as "no DELETE found".
        try:
            return args.func(args)
        except Exception as error:  # noqa: BLE001
            print(f"::error title=CQL audit (Step 3 exit criterion)::the check could not run: {error!r}", flush=True)
            return 1
    if args.cmd not in ("build", "timed"):
        # A report must never fail the job: an unexpected error is a warning.
        try:
            return args.func(args)
        except Exception as error:  # noqa: BLE001
            warn(f"{args.cmd} {getattr(args, 'title', '')}: the report failed: {error!r}")
            return 0
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
