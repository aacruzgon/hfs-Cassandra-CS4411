#!/usr/bin/env python3
"""Report sections of the three-node job (.github/workflows/cassandra-three-node.yml).

Each command writes a Markdown file and a JSON file, which the workflow adds
to the job summary and the artifact with `ci-report.py add`. Report only: a
command never fails the job; what it cannot read, it says in the section.
Only the Python standard library is used.

  tests OUT_DIR MD JSON       every failing test of the test steps, with the
                              first line of its failure and whether it is a
                              504 of issue #77, from OUT_DIR/logs/*.log and
                              OUT_DIR/run/*.json (run-tests.sh)
  pause DIR MD JSON           the pause scenarios (pause-scenarios.sh)
  memory OUT_DIR MD JSON      the memory figures: the sampler's file, each
                              test container's and each node's memory.peak
"""

from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
MIB = 1024 * 1024

RUNNING = re.compile(r"^\s*Running (\S+(?: \S+)?) \((.+)\)\s*$")
RESULT = re.compile(
    r"^test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; "
    r"(\d+) filtered out; finished in ([\d.]+)s"
)
TEST_LINE = re.compile(r"^test (\S+) \.\.\. (\w+)")
BLOCK = re.compile(r"^---- (\S+) std(?:out|err) ----$")
PANIC_NEW = re.compile(r"^thread '([^']*)' (?:\(\d+\) )?panicked at (\S+?):?$")
PANIC_OLD = re.compile(r"^thread '([^']*)' panicked at '(.*)', (\S+)$")

# Issue #77: on RF 3 a LOCAL_SERIAL read superseded by a competing Paxos round
# answers BackendError::Timeout (504), "CAS operation timed out: received 0 of
# 2 required responses" (ADR-aacruzgon-0072 decision 4). Any other Timeout is
# a 504 too, but not that one.
TIMEOUT = re.compile(r"Timeout \{|operation timed out|\b504\b")
CAS_SIGNATURE = re.compile(r"CAS operation timed out|received \d+ of \d+ required responses|serial read")
UNAVAILABLE = re.compile(r"Unavailable \{|backend unavailable|\b503\b")
# docker/cassandra/README.md, "Backend tests against the cluster": always
# fails on three nodes (rack assertion, or "one node, no peers").
EXPECTED_ON_CLUSTER = {"node_runs_the_docker_cassandra_configuration"}

CLASSES = [
    ("504-77", "504, issue #77 (serial read or CAS timed out)"),
    ("504-other", "504, another timeout"),
    ("503", "503 (Unavailable)"),
    ("expected", "expected on three nodes (README)"),
    ("other", "other"),
]


def esc(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def table(header: list[str], rows: list[list]) -> str:
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for row in rows:
        lines.append("| " + " | ".join("–" if c is None else esc(str(c)) for c in row) + " |")
    return "\n".join(lines) + "\n"


def write(md_path: str, json_path: str, md: str, data: dict) -> None:
    Path(md_path).write_text(md)
    Path(json_path).write_text(json.dumps(data, indent=2))


def classify(name: str, text: str) -> str:
    short = name.rsplit("::", 1)[-1]
    if short in EXPECTED_ON_CLUSTER:
        return "expected"
    if TIMEOUT.search(text):
        return "504-77" if CAS_SIGNATURE.search(text) else "504-other"
    if UNAVAILABLE.search(text):
        return "503"
    return "other"


def first_failure_line(lines: list[str]) -> tuple[str | None, str | None]:
    """The first line of the panic message, and where it panicked."""
    for i, line in enumerate(lines):
        if m := PANIC_OLD.match(line):
            return m.group(2), m.group(3)
        if m := PANIC_NEW.match(line):
            for following in lines[i + 1:]:
                if following.strip():
                    return following.strip(), m.group(2)
            return None, m.group(2)
    for line in lines:
        if line.strip():
            return line.strip(), None
    return None, None


def parse_log(path: Path) -> dict:
    text = ANSI.sub("", path.read_text(errors="replace"))
    lines = [l.rstrip() for l in text.splitlines()]
    suite = {"label": path.stem, "suite": None, "tests": {}, "result": None, "blocks": {}}
    current = None
    in_failures = False
    for line in lines:
        if m := RUNNING.match(line):
            suite["suite"] = m.group(1)
            continue
        if m := TEST_LINE.match(line):
            suite["tests"][m.group(1)] = m.group(2)
            continue
        if line == "failures:":
            in_failures = True
            current = None
            continue
        if in_failures and (m := BLOCK.match(line)):
            current = m.group(1)
            suite["blocks"].setdefault(current, [])
            continue
        if m := RESULT.match(line):
            suite["result"] = {
                "result": m.group(1), "passed": int(m.group(2)), "failed": int(m.group(3)),
                "ignored": int(m.group(4)), "filtered_out": int(m.group(6)), "seconds": float(m.group(7)),
            }
            current = None
            continue
        if current is not None:
            suite["blocks"][current].append(line)
    failures = []
    for name, result in suite["tests"].items():
        if result != "FAILED":
            continue
        block = suite["blocks"].get(name, [])
        first, where = first_failure_line(block)
        failures.append({
            "suite": suite["suite"] or suite["label"], "test": name,
            "class": classify(name, "\n".join(block)),
            "first_line": first, "location": where,
        })
    suite["failures"] = failures
    suite["ignored"] = sorted(n for n, r in suite["tests"].items() if r == "ignored")
    del suite["blocks"]
    return suite


def cmd_tests(out_dir: str, md_path: str, json_path: str) -> None:
    out = Path(out_dir)
    suites = []
    for log in sorted((out / "logs").glob("*.log")):
        suite = parse_log(log)
        run = out / "run" / f"{log.stem}.json"
        suite["run"] = json.loads(run.read_text()) if run.exists() else None
        suites.append(suite)
    rows, failures, warnings = [], [], []
    for s in suites:
        r = s["result"] or {}
        run = s["run"] or {}
        peak = run.get("memory_peak_bytes")
        if not s["result"]:
            warnings.append(f"Three-node tests: {s['label']} printed no `test result` line"
                            + (" (stopped by the timeout)" if run.get("timed_out") else ""))
        rows.append([
            s["label"], f"`{s['suite']}`" if s["suite"] else None,
            r.get("passed"), r.get("failed"), r.get("ignored"), r.get("filtered_out"),
            f"{r['seconds']:.1f} s" if "seconds" in r else None,
            run.get("exit_status"), "yes" if run.get("timed_out") else "no",
            f"{run['wall_s']} s" if run.get("wall_s") is not None else None,
            f"{peak // MIB:,} MiB" if peak else None, run.get("oom_kills"),
        ])
        failures += s["failures"]
    counts = {key: sum(1 for f in failures if f["class"] == key) for key, _ in CLASSES}
    md = table(["Step", "Suite", "Passed", "Failed", "Ignored", "Filtered out", "libtest time",
                "Exit", "Timed out", "Wall time", "Test container memory.peak", "OOM kills"], rows)
    md += "\n**Failures by cause:** " + "; ".join(f"{label}: {counts[key]}" for key, label in CLASSES) + ".\n"
    if failures:
        md += "\n" + table(
            ["Suite", "Test", "Cause", "First line of the failure", "Where"],
            [[f"`{f['suite']}`", f"`{f['test']}`", dict(CLASSES)[f["class"]],
              (f["first_line"] or "")[:300], f"`{f['location']}`" if f["location"] else None] for f in failures],
        )
    else:
        md += "\nNo test failed.\n"
    md += (
        "\nA failure is a 504 of issue #77 when its output has a backend `Timeout` together with the serial "
        "read's or CAS's text (`CAS operation timed out`, `received N of M required responses`). "
        "`node_runs_the_docker_cassandra_configuration` always fails on three nodes "
        "(docker/cassandra/README.md, \"Backend tests against the cluster\"). The job never fails on a test; "
        "this table is the result.\n"
    )
    write(md_path, json_path, md, {"suites": suites, "failures_by_class": counts, "warnings": warnings})


# --- pause scenarios ---------------------------------------------------------

# What the PR #90 review measured on acruzdebian (2026-11-19), against which
# each scenario is compared. applied: "yes", "no".
EXPECTED = {
    "s1": {"outcome": "success", "write_type": None, "applied": "yes"},
    "s2": {"outcome": "WriteTimeout", "write_type": "SIMPLE", "applied": "yes"},
    "s3": {"outcome": "Unavailable", "write_type": None, "applied": "no"},
    "s4a": {"outcome": "WriteTimeout", "write_type": "BATCH_LOG", "applied": "yes"},
    "s4b": {"outcome": "success", "write_type": None, "applied": "yes"},
}


def parse_outcome(text: str) -> dict:
    errors = [l for l in text.splitlines() if re.search(r"Error|Exception|NoHostAvailable", l)]
    if not errors:
        return {"outcome": "success"}
    line = errors[0]
    result = {"error": line.strip()[:400]}
    info = {}
    if m := re.search(r"info=(\{[^{}]*\})", line.replace("\\'", "'")):
        try:
            info = ast.literal_eval(m.group(1))
        except (ValueError, SyntaxError):
            info = {}
    for key in ("consistency", "required_responses", "received_responses", "write_type",
                "required_replicas", "alive_replicas"):
        if key in info:
            result[key] = info[key]
    if "WriteTimeout" in line:
        result["outcome"] = "WriteTimeout"
    elif "Unavailable" in line:
        result["outcome"] = "Unavailable"
    elif "WriteFailure" in line:
        result["outcome"] = "WriteFailure"
    else:
        result["outcome"] = "other error"
    return result


def parse_rows(text: str) -> list[dict] | None:
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass
    if not rows and not re.search(r"\(0 rows\)", text):
        return None
    return rows


def applied(kind: str, rows: list[dict] | None) -> str:
    if rows is None:
        return "unread"
    if kind == "one":
        versions = sorted(r.get("version_id") for r in rows)
        if versions == [2]:
            return "yes"
        if versions == [1]:
            return "no"
        return f"rows {versions}"
    types = sorted(r.get("resource_type") for r in rows)
    if types == ["Observation", "Patient"]:
        return "yes"
    if not types:
        return "no"
    return f"partly ({', '.join(types)})"


def meta(path: Path) -> dict:
    values = {}
    for line in path.read_text().splitlines():
        if "=" in line:
            key, value = line.split("=", 1)
            values[key] = value
    return values


def cmd_pause(directory: str, md_path: str, json_path: str) -> None:
    d = Path(directory)
    scenarios, rows, warnings = [], [], []
    if not d.exists():
        write(md_path, json_path, "The pause scenarios did not run.\n",
              {"scenarios": [], "warnings": ["Pause scenarios: no output directory"]})
        return
    for sid in EXPECTED:
        mpath = d / f"{sid}.meta"
        if not mpath.exists():
            warnings.append(f"Pause scenarios: {sid} did not run")
            rows.append([sid, None, None, None, "did not run", None, None, None, None, None])
            continue
        m = meta(mpath)
        outcome = parse_outcome((d / f"{sid}.out").read_text(errors="replace") if (d / f"{sid}.out").exists() else "")
        read = d / f"{sid}.read"
        rows_read = parse_rows(read.read_text(errors="replace")) if read.exists() else None
        got_applied = applied(m.get("kind", ""), rows_read)
        expected = EXPECTED[sid]
        same = (outcome["outcome"] == expected["outcome"]
                and (expected["write_type"] is None or outcome.get("write_type") == expected["write_type"])
                and got_applied == expected["applied"])
        if not same:
            warnings.append(f"Pause scenario {sid} differs from the PR #90 review's run: "
                            f"{outcome['outcome']} {outcome.get('write_type') or ''}, applied {got_applied}")
        detail = ", ".join(f"{k} {outcome[k]}" for k in ("write_type", "consistency", "received_responses",
                                                          "required_responses", "alive_replicas", "required_replicas")
                           if k in outcome)
        scenarios.append({**m, **outcome, "applied": got_applied, "rows": rows_read,
                          "expected": expected, "as_expected": same})
        rows.append([sid, m.get("description"), m.get("paused"), m.get("state"), outcome["outcome"], detail or None,
                     f"{m.get('took_ms')} ms", got_applied,
                     f"{expected['outcome']}{' ' + expected['write_type'] if expected['write_type'] else ''}, applied {expected['applied']}",
                     "yes" if same else "**no**"])
    windows = meta(d / "windows.meta") if (d / "windows.meta").exists() else {}
    md = table(["Scenario", "Batch", "Paused", "Node 1 saw them", "Answer", "Detail", "Time",
                "Applied (read at ALL)", "PR #90 review", "Same"], rows)
    md += (
        f"\nTime until node 1 marked a paused node DN: {windows.get('w1_down_s', '?')} s with node 2 paused "
        f"(after s1 and s4a), {windows.get('w2_down_s', '?')} s with nodes 2 and 3 (after s2).\n"
        "\nRaw CQL from `cqlsh` on node 1 (the coordinator), `LOCAL_QUORUM`, keyspace RF 3, no HFS code. "
        "One-partition: an update's index delta (insert v2, delete v1 of one `(tenant, Patient)` row). "
        "Two-partition: Patient and Observation rows in one LOGGED batch. \"Applied\" is read at `ALL` once every "
        "node is UN again and 25 s have passed (batchlog replay runs every 10 s). `cqlsh`'s Python driver re-sends "
        "a `BATCH_LOG` timeout once, so s4a's time is about two node timeouts. Recorded, not asserted.\n"
    )
    write(md_path, json_path, md, {"scenarios": scenarios, "windows": windows, "warnings": warnings})


# --- memory ------------------------------------------------------------------

UNITS = {"B": 1, "KiB": 1024, "MiB": MIB, "GiB": 1024 * MIB, "kB": 1000, "MB": 1000 ** 2, "GB": 1000 ** 3}


def to_bytes(text: str) -> int | None:
    m = re.match(r"^\s*([\d.]+)\s*([A-Za-z]+)\s*$", text)
    if not m or m.group(2) not in UNITS:
        return None
    return int(float(m.group(1)) * UNITS[m.group(2)])


def cmd_memory(out_dir: str, md_path: str, json_path: str) -> None:
    out = Path(out_dir)
    samples_path = out / "memory-samples.tsv"
    per_container: dict[str, int] = {}
    per_sample: dict[str, int] = {}
    host_min = None
    count = 0
    if samples_path.exists():
        for line in samples_path.read_text(errors="replace").splitlines():
            parts = line.split("\t")
            if len(parts) != 3:
                continue
            when, name, value = parts
            if name == "host":
                kb = int(value) if value.isdigit() else None
                if kb is not None:
                    host_min = kb if host_min is None else min(host_min, kb)
                    count += 1
                continue
            used = to_bytes(value.split("/")[0])
            if used is None:
                continue
            per_container[name] = max(per_container.get(name, 0), used)
            per_sample[when] = per_sample.get(when, 0) + used
    rows = [[f"`{n}`", f"{b // MIB:,} MiB"] for n, b in sorted(per_container.items())]
    peaks = []
    for f in sorted((out / "nodes").glob("*.cgroup")) if (out / "nodes").exists() else []:
        values = dict(l.split(" ", 1) for l in f.read_text().splitlines() if " " in l)
        peak = values.get("memory.peak", "").strip()
        state = out / "nodes" / f"{f.stem}.state"
        peaks.append({"node": f.stem, "state": state.read_text().strip() if state.exists() else None,
                      "memory_peak_bytes": int(peak) if peak.isdigit() else None,
                      "memory_max": values.get("memory.max", "").strip() or None,
                      "oom_kill": values.get("oom_kill", "").strip() or None})
    for f in sorted((out / "run").glob("*.json")) if (out / "run").exists() else []:
        run = json.loads(f.read_text())
        peaks.append({"node": f"tests: {run.get('label')}", "state": f"exit {run.get('exit_status')}",
                      "memory_peak_bytes": run.get("memory_peak_bytes"),
                      "memory_max": run.get("memory_limit"), "oom_kill": run.get("oom_kills")})
    total = max(per_sample.values()) if per_sample else None
    md = "**Each container's own cgroup** (`memory.peak` over its life; the nodes' include boot, schema, tests and the pause scenarios):\n\n"
    md += table(["Container", "State at the end", "memory.peak", "Limit", "OOM kills"],
                [[p["node"], p["state"], f"{p['memory_peak_bytes'] // MIB:,} MiB" if p["memory_peak_bytes"] else None,
                  p["memory_max"], p["oom_kill"]] for p in peaks])
    md += (f"\n**Sampled every 5 s with `docker stats`** ({count} samples): the job's containers together peaked at "
           f"{total // MIB:,} MiB" if total else "\n**Sampled with `docker stats`:** no sample was read")
    md += (f"; the host's `MemAvailable` was at least {host_min // 1024:,} MiB.\n" if host_min is not None else ".\n")
    if rows:
        md += "\n" + table(["Container", "Highest sample"], rows)
    md += ("\nThe containers run in gh-runner's rootless Docker, in its user manager, outside the runner service's "
           "cgroup (ADR-aacruzgon-0023), so the build's figures (above) and these are separate. The cluster's limits "
           "are 3 × 1700 MiB, and each test container's 6 GiB.\n")
    write(md_path, json_path, md, {"containers": peaks, "sampled_peak_by_container": per_container,
                                   "sampled_peak_total_bytes": total, "host_min_available_kb": host_min,
                                   "samples": count, "warnings": []})


def main() -> int:
    if len(sys.argv) != 5 or sys.argv[1] not in ("tests", "pause", "memory"):
        print(__doc__, file=sys.stderr)
        return 0
    command, *args = sys.argv[1:]
    try:
        {"tests": cmd_tests, "pause": cmd_pause, "memory": cmd_memory}[command](*args)
    except Exception as error:  # noqa: BLE001  report only
        print(f"::warning::three-node report {command} failed: {error!r}")
        Path(args[1]).write_text(f"The report could not be built: `{error!r}`.\n")
        Path(args[2]).write_text(json.dumps({"warnings": [f"three-node report {command} failed: {error!r}"]}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
