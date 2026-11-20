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
RUNNING_N = re.compile(r"^running (\d+) tests?$")
TEST_LINE = re.compile(r"^test (\S+) \.\.\. (\w+)")
# libtest's warning for a test still running after 60 s. With the timeout
# stopping the binary, it is the only line a hung test leaves.
STILL_RUNNING = re.compile(r"^test (\S+) has been running for over \d+ seconds$")
# The write engine's counters, one line per race test that prints them
# (cassandra_tests.rs, "[cas case=race:<test> key=value …]"; ci-report.py cas).
# serial_read_failures counts serial reads that failed or timed out; a timed
# out one is issue #77's 504. Since PR #83 a race test accepts that 504 at RF
# 2 or more and passes, so only this counter shows it; a test that panics
# before it prints its line is not in it. Only race: cases count: the --lib
# node tests' cases (c4-r2-timeout, r2-503-…) fail a serial read on purpose.
CAS_LINE = re.compile(r"\[cas case=(race:\S+) ([^\]]*)\]")
SERIAL_READ_FAILURES = re.compile(r"(?:^| )serial_read_failures=(\d+)(?= |$)")
BLOCK = re.compile(r"^---- (\S+) std(?:out|err) ----$")
PANIC_NEW = re.compile(r"^thread '([^']*)' (?:\(\d+\) )?panicked at (\S+?):?$")
PANIC_OLD = re.compile(r"^thread '([^']*)' panicked at '(.*)', (\S+)$")

# A failure is classified from its panic's own message, the first line after
# "panicked at", and only when that line's subject is a backend error: the
# line ends with the error, right after "got " or ": " (an assertion's
# `got {outcome:?}`, an unwrap's "on an `Err` value: "), bare or inside
# `Err(…)` or `Failed(…)` (a test that prints the result it did not expect,
# "a repeated delete_versioned is NotFound: {other:?}"). A line that goes on
# after the error ("… instead of a refusal") is not about it. An invariant panic
# that lists every writer's outcome ("nothing is live: [Updated(…),
# Failed(Backend(Timeout { … }))]", PR #83) is about the invariant, not the
# 504, so it is "other", with a note.
SUBJECT_ERROR = re.compile(
    r"(?:: |got )(?:Err\()?(?:Failed\()?(?:Backend\()?(?P<kind>Timeout|Unavailable) \{ backend_name: \"[^\"]*\", "
    r"message: \"(?P<message>(?:[^\"\\]|\\.)*)\" \}\)?\)?\)?$"
)
# Issue #77: on RF 3 a LOCAL_SERIAL read superseded by a competing Paxos round
# answers BackendError::Timeout (504), "serial read: CAS operation timed out:
# received 0 of 2 required responses" (ADR-aacruzgon-0072 decision 4). The
# backend prefixes the operation (errors.rs, "{operation}: {message}"), so a
# CAS write's timeout ("create: CAS operation timed out: …") is another 504.
# Since PR #83 (merged 2026-11-20) the race checks accept a 504 at RF 2 or
# more, so on main a test failing on one is new: the class reports it.
ISSUE_77 = re.compile(r"^serial read: CAS operation timed out")
ANY_504 = re.compile(r"Timeout \{")
ANY_77 = re.compile(r"serial read: CAS operation timed out")
# docker/cassandra/README.md, "Backend tests against the cluster": on three
# nodes this test fails at the rack assertion (when system.local is answered
# by node 2 or 3) or at "one node, no peers". Only those two are expected; it
# also checks the release, the data centre and paxos_variant.
EXPECTED_ON_CLUSTER = "node_runs_the_docker_cassandra_configuration"
EXPECTED_PEERS = re.compile(r"^assertion `left == right` failed: one node, no peers: ")
EXPECTED_RACK_LEFT = re.compile(r'^\s*left: "helios-rack[0-9]+"$')
EXPECTED_RACK_RIGHT = re.compile(r'^\s*right: "helios-rack1"$')

CLASSES = [
    ("504-77", "504, issue #77 (serial read timed out)"),
    ("504-other", "504, another timeout"),
    ("503", "503 (Unavailable)"),
    ("expected", "expected on three nodes (README)"),
    ("timed-out", "timed out (still running when the timeout stopped the run)"),
    ("killed", "killed (still running when the binary was killed)"),
    ("unknown", "cause unknown (the run was stopped before libtest printed it)"),
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


def classify(name: str, message: list[str], text: str) -> tuple[str, str | None]:
    """The class of one failure, from its panic message's lines (`message`,
    the first one first), and a note. `text` is the test's whole output, used
    only for the note."""
    first = message[0] if message else ""
    # First, for every test, the configuration test included: a 503 or 504
    # keeps its class wherever it happens.
    if m := SUBJECT_ERROR.search(first):
        if m.group("kind") == "Unavailable":
            return "503", None
        if ISSUE_77.match(m.group("message")):
            return "504-77", None
        return "504-other", None
    if name.rsplit("::", 1)[-1] == EXPECTED_ON_CLUSTER:
        rack = (first == "assertion `left == right` failed" and len(message) >= 3
                and EXPECTED_RACK_LEFT.match(message[1]) and EXPECTED_RACK_RIGHT.match(message[2]))
        if rack or EXPECTED_PEERS.match(first):
            return "expected", None
        return "other", "not the rack or peers assertion the README expects"
    if ANY_77.search(text):
        return "other", "a #77 504 in its message, not as its subject"
    if ANY_504.search(text):
        return "other", "a 504 in its message, not as its subject"
    return "other", None


def panic_message(lines: list[str], start: int) -> list[str]:
    """The panic message's lines after a "panicked at" header at `start`: up to
    a blank line, the backtrace note, or the next libtest line."""
    message = []
    for line in lines[start + 1:]:
        if not line.strip():
            if message:
                break
            continue
        if (line.startswith("note: run with `RUST_BACKTRACE") or PANIC_NEW.match(line)
                or TEST_LINE.match(line) or STILL_RUNNING.match(line) or BLOCK.match(line)):
            break
        message.append(line.rstrip())
    return message


def shown(message: list[str]) -> str:
    """The message's first line; for a bare `assert_eq!`, with its left and
    right, which say which assertion failed."""
    first = message[0]
    if first == "assertion `left == right` failed" and len(message) >= 3:
        return f"{first} ({message[1].strip()}, {message[2].strip()})"
    return first


def panics(lines: list[str]) -> list[dict]:
    """Every panic in `lines`: the thread (libtest names it after the test),
    the location, and the message's lines."""
    found = []
    for i, line in enumerate(lines):
        if m := PANIC_OLD.match(line):
            found.append({"thread": m.group(1), "location": m.group(3), "message": [m.group(2)]})
        elif m := PANIC_NEW.match(line):
            found.append({"thread": m.group(1), "location": m.group(2), "message": panic_message(lines, i)})
    return found


def first_failure_line(lines: list[str]) -> tuple[str | None, str | None]:
    """The first line of the panic message, and where it panicked."""
    for p in panics(lines):
        return (p["message"][0] if p["message"] else None), p["location"]
    for line in lines:
        if line.strip():
            return line.strip(), None
    return None, None


def parse_log(path: Path, stopped: str | None = None) -> dict:
    """One run's log. Reads the captured blocks after "failures:" (capture
    mode) and every panic in the log (`--nocapture`, where libtest prints a
    panic as it happens, under the test's thread name). `stopped`: how the
    run was stopped, "timeout" or "killed" (run-tests.sh's JSON), or None;
    a test still running then is reported as timed out or killed."""
    text = ANSI.sub("", path.read_text(errors="replace"))
    lines = [l.rstrip() for l in text.splitlines()]
    suite = {"label": path.stem, "suite": None, "tests": {}, "result": None, "blocks": {},
             "announced": None, "still_running": []}
    current = None
    in_failures = False
    for line in lines:
        if m := RUNNING.match(line):
            suite["suite"] = m.group(1)
            continue
        if m := RUNNING_N.match(line):
            suite["announced"] = int(m.group(1))
            continue
        if m := STILL_RUNNING.match(line):
            if m.group(1) not in suite["still_running"]:
                suite["still_running"].append(m.group(1))
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
    inline: dict[str, dict] = {}
    for p in panics(lines):
        inline.setdefault(p["thread"], p)
    failures = []
    for name, result in suite["tests"].items():
        if result != "FAILED":
            continue
        block = suite["blocks"].get(name)
        source = panics(block)[0] if block and panics(block) else inline.get(name)
        message = source["message"] if source else []
        if source is None and suite["result"] is None:
            cls, note = "unknown", None
        else:
            cls, note = classify(name, message, "\n".join(block) if block else "\n".join(message))
        failures.append({
            "label": suite["label"], "suite": suite["suite"] or suite["label"], "test": name,
            "class": cls, "note": note,
            "first_line": shown(message) if message else (first_failure_line(block)[0] if block else None),
            "location": source["location"] if source else None,
        })
    if stopped or suite["result"] is None:
        cls, when = (("killed", "when the binary was killed") if stopped == "killed"
                     else ("timed-out", "when the timeout stopped the run") if stopped == "timeout"
                     else ("unknown", "when the run ended without a result"))
        for name in suite["still_running"]:
            if name not in suite["tests"]:
                failures.append({
                    "label": suite["label"], "suite": suite["suite"] or suite["label"], "test": name,
                    "class": cls, "note": None,
                    "first_line": f"still running {when} (libtest: running for over 60 seconds)",
                    "location": None,
                })
    reported = len(suite["tests"]) + sum(1 for n in suite["still_running"] if n not in suite["tests"])
    suite["unreported"] = (max(suite["announced"] - reported, 0)
                           if suite["result"] is None and suite["announced"] is not None else 0)
    suite["failures"] = failures
    suite["ignored"] = sorted(n for n, r in suite["tests"].items() if r == "ignored")
    suite["cas_lines"] = 0
    suite["serial_read_failures"] = 0
    for m in CAS_LINE.finditer(text):
        suite["cas_lines"] += 1
        if n := SERIAL_READ_FAILURES.search(m.group(2)):
            suite["serial_read_failures"] += int(n.group(1))
    del suite["blocks"]
    return suite


def ended(run: dict) -> str | None:
    """How the run ended, from run-tests.sh's JSON: an exit, the timeout
    (coreutils timeout's 124), or a kill (137: the timeout's SIGKILL 30 s
    after its SIGINT, or the kernel's OOM killer, which oom_kills tells),
    or not started at all (too close to the step's deadline)."""
    if run and run.get("skipped"):
        return f"not run ({run['skipped']})"
    if not run or run.get("exit_status") is None:
        return None
    if run.get("timed_out"):
        return "timeout"
    if run.get("killed"):
        return "killed (OOM kill)" if (run.get("oom_kills") or 0) > 0 else "killed"
    return "exit"


def cmd_tests(out_dir: str, md_path: str, json_path: str) -> None:
    out = Path(out_dir)
    suites = []
    for log in sorted((out / "logs").glob("*.log")):
        run_file = out / "run" / f"{log.stem}.json"
        run = json.loads(run_file.read_text()) if run_file.exists() else None
        how = "timeout" if run and run.get("timed_out") else "killed" if run and run.get("killed") else None
        suite = parse_log(log, stopped=how)
        suite["run"] = run
        suites.append(suite)
    rows, failures, warnings = [], [], []
    for s in suites:
        r = s["result"] or {}
        run = s["run"] or {}
        peak = run.get("memory_peak_bytes")
        how = ended(run)
        if run.get("skipped"):
            warnings.append(f"Three-node tests: {s['label']} not run ({run['skipped']}: "
                            f"{run.get('left_s')} s left before the step's deadline)")
        elif not s["result"]:
            warnings.append(f"Three-node tests: {s['label']} printed no `test result` line"
                            + (f" (ended: {how})" if how and how != "exit" else "")
                            + (f"; {s['unreported']} test(s) never reported a result" if s["unreported"] else ""))
        rows.append([
            s["label"], f"`{s['suite']}`" if s["suite"] else None,
            r.get("passed"), r.get("failed"), r.get("ignored"), r.get("filtered_out"),
            f"{r['seconds']:.1f} s" if "seconds" in r else None,
            run.get("exit_status"), how,
            f"{run['wall_s']} s" if run.get("wall_s") is not None else None,
            f"{peak // MIB:,} MiB" if peak else None, run.get("oom_kills"),
        ])
        failures += s["failures"]
    counts = {key: sum(1 for f in failures if f["class"] == key) for key, _ in CLASSES}
    # Repeats: run-tests.sh labels are <step>-r<N>. Per step, how many runs,
    # and per test, in how many of them it failed.
    def step_of(label: str) -> str:
        return re.sub(r"-r\d+$", "", label)
    # A repeat that was not started is not a run.
    ran = [s_ for s_ in suites if not (s_["run"] or {}).get("skipped")]
    runs: dict[str, int] = {}
    for s_ in ran:
        runs[step_of(s_["label"])] = runs.get(step_of(s_["label"]), 0) + 1
    by_test: dict[tuple, dict] = {}
    for f in failures:
        key = (step_of(f["label"]), f["test"])
        entry = by_test.setdefault(key, {"step": key[0], "suite": f["suite"], "test": f["test"], "runs_failed": 0,
                                         "classes": [], "notes": [], "first_line": f["first_line"],
                                         "location": f["location"]})
        entry["runs_failed"] += 1
        if f["class"] not in entry["classes"]:
            entry["classes"].append(f["class"])
        if f.get("note") and f["note"] not in entry["notes"]:
            entry["notes"].append(f["note"])
        if not entry["first_line"] and f["first_line"]:
            entry["first_line"], entry["location"] = f["first_line"], f["location"]
    runs_with_77 = {}
    for s_ in suites:
        if any(f["class"] == "504-77" for f in s_["failures"]):
            runs_with_77[step_of(s_["label"])] = runs_with_77.get(step_of(s_["label"]), 0) + 1
    md = table(["Step", "Suite", "Passed", "Failed", "Ignored", "Filtered out", "libtest time",
                "Exit", "Ended", "Wall time", "Test container memory.peak", "OOM kills"], rows)
    md += ("\n**Failures by cause** (one per failing test per run): "
           + "; ".join(f"{label}: {counts[key]}" for key, label in CLASSES) + ".\n")
    md += ("\n**Runs with a test failing on a #77 504:** "
           + "; ".join(f"`{step}` {runs_with_77.get(step, 0)} of {n}" for step, n in sorted(runs.items())) + ".\n")
    serial: dict[str, list[int]] = {}
    for s_ in ran:
        if s_["cas_lines"]:
            serial.setdefault(step_of(s_["label"]), []).append(s_["serial_read_failures"])
    if serial:
        md += ("\n**Failed serial reads in the race tests** (`serial_read_failures` in their `[cas case=race:…]` "
               "lines, per run): "
               + "; ".join(f"`{step}` {', '.join(map(str, ns))} ({sum(ns)} in {len(ns)} runs)"
                           for step, ns in sorted(serial.items()))
               + ". A serial read that timed out is issue #77's 504 (one that failed is a 503). Since PR #83 a "
                 "race test accepts the 504 at RF 2 or more and passes, so a 504 a passing test accepted shows only "
                 "here; a test that panics before it prints its `[cas]` line is not counted.\n")
    if by_test:
        md += "\n**Every failing test, over all runs:**\n\n" + table(
            ["Step", "Test", "Failed in", "Cause", "First line of the failure (first run it failed in)", "Where"],
            [[e["step"], f"`{e['test']}`", f"{e['runs_failed']} of {runs.get(e['step'], '?')} runs",
              ", ".join(dict(CLASSES)[c] for c in e["classes"]) + "".join(f" ({n})" for n in e["notes"]),
              (e["first_line"] or "")[:300], f"`{e['location']}`" if e["location"] else None]
             for e in sorted(by_test.values(), key=lambda e: (e["step"], -e["runs_failed"], e["test"]))],
        )
        md += "\n<details><summary>Each failure in each run</summary>\n\n" + table(
            ["Run", "Test", "Cause", "First line of the failure"],
            [[f["label"], f"`{f['test']}`", dict(CLASSES)[f["class"]] + (f" ({f['note']})" if f.get("note") else ""),
              (f["first_line"] or "")[:300]] for f in failures],
        ) + "\n</details>\n"
    elif ran:
        md += "\nNo test failed.\n"
    else:
        md += ("\n**No test ran:** no repeat of either step left a log (`logs/*.log`), or every repeat was "
               "not started. The tests were skipped (the cluster or the schema step failed) or their logs "
               "are missing; this is not a pass.\n")
        warnings.append("Three-node tests: no test ran (no log, or no repeat started), so no failure count")
    unreported = [(s_["label"], s_["unreported"]) for s_ in suites if s_["unreported"]]
    if unreported:
        md += ("\n**Tests that never reported a result** (the run was stopped; libtest names a test only "
               "once it has run for 60 s): " + "; ".join(f"{label}: {n}" for label, n in unreported) + ".\n")
    md += (
        "\nA failure's cause is read from its panic's first line, and only when that line's subject is the "
        "backend error (`got Failed(Backend(Timeout { … }))`, or an unwrap's `` `Err` value: ``). It is a 504 "
        "of issue #77 when that error is a `Timeout` whose message starts `serial read: CAS operation timed "
        "out`; a CAS write's timeout (`create: CAS operation timed out`) is another 504. A panic about "
        "something else that lists outcomes including a 504 is \"other\", with a note. "
        "`node_runs_the_docker_cassandra_configuration` is expected only at its rack assertion or at "
        "`one node, no peers` (docker/cassandra/README.md, \"Backend tests against the cluster\"). "
        "The job never fails on a test; this table is the result.\n"
    )
    write(md_path, json_path, md, {"suites": suites, "runs": runs, "failures_by_class": counts,
                                   "runs_with_77": runs_with_77, "serial_read_failures": serial,
                                   "by_test": list(by_test.values()),
                                   "warnings": warnings})


# --- pause scenarios ---------------------------------------------------------

# What the PR #90 review measured on acruzdebian (2026-11-19), against which
# each scenario is compared. applied: "yes", "no".
EXPECTED = {
    "s1": {"outcome": "success", "write_type": None, "applied": "yes", "counts": {}},
    "s2": {"outcome": "WriteTimeout", "write_type": "SIMPLE", "applied": "yes",
           "counts": {"received_responses": 1, "required_responses": 2}},
    "s3": {"outcome": "Unavailable", "write_type": None, "applied": "no",
           "counts": {"alive_replicas": 1, "required_replicas": 2}},
    "s4a": {"outcome": "WriteTimeout", "write_type": "BATCH_LOG", "applied": "yes",
            "counts": {"received_responses": 1, "required_responses": 2}},
    "s4b": {"outcome": "success", "write_type": None, "applied": "yes", "counts": {}},
}


def parse_outcome(text: str | None) -> dict:
    """cqlsh's answer to one batch. Success needs evidence that cqlsh ran it:
    no error line, and the "Consistency level set" line it prints first.
    No output file (None), or output without either, is unknown."""
    if text is None:
        return {"outcome": "unknown", "error": "no cqlsh output file"}
    errors = [l for l in text.splitlines() if re.search(r"Error|Exception|NoHostAvailable", l)]
    if not errors:
        if re.search(r"^Consistency level set to ", text, re.MULTILINE):
            return {"outcome": "success"}
        return {"outcome": "unknown", "error": "cqlsh printed neither an error nor its consistency line"}
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
    """The rows of a `SELECT JSON` read, or None when the read is missing
    or incomplete: a row that is not whole JSON, or no `(N rows)` line whose
    N is the number of rows read."""
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                rows.append(json.loads(line))
            except ValueError:
                return None
    count = re.search(r"^\((\d+) rows\)$", text, re.MULTILINE)
    if not count or int(count.group(1)) != len(rows):
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


def seconds(value: str) -> str:
    """windows.meta's value with " s" where it is a number of seconds
    ("13", "16+0"), as written otherwise ("timeout", "not paused")."""
    return f"{value} s" if re.fullmatch(r"\d+(\+\d+)?", value) else value


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
        expected = EXPECTED[sid]
        expected_text = (f"{expected['outcome']}{' ' + expected['write_type'] if expected['write_type'] else ''}"
                         + "".join(f", {k} {v}" for k, v in expected["counts"].items())
                         + f", applied {expected['applied']}")
        # pause-scenarios.sh did not send the batch: the state it needs
        # (node 1 marking the paused nodes DN) was never seen.
        if m.get("not_run"):
            warnings.append(f"Pause scenario {sid} not run: {m['not_run']}")
            scenarios.append({**m, "outcome": "not run", "expected": expected, "as_expected": None})
            rows.append([sid, m.get("description"), m.get("paused"), m.get("state"), "not run", m["not_run"],
                         None, None, expected_text, "–"])
            continue
        out_file = d / f"{sid}.out"
        outcome = parse_outcome(out_file.read_text(errors="replace") if out_file.exists() else None)
        if outcome["outcome"] == "unknown":
            warnings.append(f"Pause scenario {sid}: its answer is unknown ({outcome['error']})")
        read = d / f"{sid}.read"
        rows_read = parse_rows(read.read_text(errors="replace")) if read.exists() else None
        got_applied = applied(m.get("kind", ""), rows_read)
        same = (outcome["outcome"] == expected["outcome"]
                and (expected["write_type"] is None or outcome.get("write_type") == expected["write_type"])
                and all(outcome.get(k) == v for k, v in expected["counts"].items())
                and got_applied == expected["applied"])
        # Not comparable: the batch's state was not established (node 1 no
        # longer saw the paused nodes UN right after it), or its answer is
        # unknown.
        verdict = "yes" if same else "**no**"
        if m.get("invalid"):
            same, verdict = None, "invalid"
            warnings.append(f"Pause scenario {sid} is invalid: {m['invalid']}")
        elif outcome["outcome"] == "unknown":
            same, verdict = None, "unknown"
        elif got_applied == "unread":
            same, verdict = None, "unread"
            warnings.append(f"Pause scenario {sid}: its read at ALL is missing or incomplete")
        elif not same:
            warnings.append(f"Pause scenario {sid} differs from the PR #90 review's run: "
                            f"{outcome['outcome']} {outcome.get('write_type') or ''}"
                            + "".join(f", {k} {outcome.get(k)}" for k in expected["counts"])
                            + f", applied {got_applied}")
        detail = ", ".join(f"{k} {outcome[k]}" for k in ("write_type", "consistency", "received_responses",
                                                          "required_responses", "alive_replicas", "required_replicas")
                           if k in outcome)
        scenarios.append({**m, **outcome, "applied": got_applied, "rows": rows_read,
                          "expected": expected, "as_expected": same})
        state = m.get("state")
        if m.get("seen_after"):
            state = f"{state}; {m['seen_after']} right after"
        rows.append([sid, m.get("description"), m.get("paused"), state, outcome["outcome"], detail or None,
                     f"{m.get('took_ms')} ms", got_applied, expected_text, verdict])
    windows = meta(d / "windows.meta") if (d / "windows.meta").exists() else {}
    md = table(["Scenario", "Batch", "Paused", "Node 1 saw them", "Answer", "Detail", "Time",
                "Applied (read at ALL)", "PR #90 review", "Same"], rows)
    md += (
        f"\nTime until node 1 marked a paused node DN: {seconds(windows.get('w1_down_s', '?'))} with node 2 paused "
        f"(after s1 and s4a), {seconds(windows.get('w2_down_s', '?'))} with nodes 2 and 3 (after s2); "
        "\"timeout\" is not within the script's wait, and the DN scenario of that window is then not run; "
        "\"not paused\" is a window whose `docker pause` failed, and none of its scenarios ran.\n"
        "\nRaw CQL from `cqlsh` on node 1 (the coordinator), `LOCAL_QUORUM`, keyspace RF 3, no HFS code. "
        "One-partition: an update's index delta (insert v2, delete v1 of one `(tenant, Patient)` row). "
        "Two-partition: Patient and Observation rows in one LOGGED batch. \"Applied\" is read at `ALL` once every "
        "node is UN again and 25 s have passed (batchlog replay runs every 10 s). `cqlsh`'s Python driver re-sends "
        "a `BATCH_LOG` timeout once, so s4a's time is about two node timeouts. A batch sent \"at once\" counts "
        "only if node 1 still reports the paused nodes UN right after it (a paused node goes from UN to DN, not "
        "back), and is \"invalid\" otherwise. Recorded, not asserted.\n"
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
        limit = values.get("memory.max", "").strip()
        peaks.append({"node": f.stem, "state": state.read_text().strip() if state.exists() else None,
                      "memory_peak_bytes": int(peak) if peak.isdigit() else None,
                      "memory_max": f"{int(limit) // MIB:,} MiB" if limit.isdigit() else (limit or None),
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
        # Each fallback on its own: a destination that cannot be written
        # (the error above may be just that) does not stop the other, and
        # the report still exits 0.
        for path, text in ((args[1], f"The report could not be built: `{error!r}`.\n"),
                           (args[2], json.dumps({"warnings": [f"three-node report {command} failed: {error!r}"]}))):
            try:
                Path(path).write_text(text)
            except OSError as write_error:
                print(f"::warning::three-node report {command}: could not write {path}: {write_error!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
