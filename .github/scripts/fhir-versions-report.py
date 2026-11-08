#!/usr/bin/env python3
"""Cassandra tests per FHIR version: one row per version and suite (report only).

The `all-versions` job of .github/workflows/fhir-versions.yml builds the
Cassandra test binaries once with every FHIR version on, then runs them once
per version with HFS_TEST_FHIR_VERSION set, and keeps each run's output in
`<logs-dir>/fhir-<version>-<suite>.log`, ending with a line `exit=<status>`.
The suites are:

  lib    helios-persistence `--lib -- backends::cassandra`
  tests  helios-persistence `--test cassandra_tests --test backend_capability_contract`
  rest   helios-hfs `--test cassandra_milestone_a`, the Milestone A round trip
         and the Step 3 CRUD test (`--exact`, so exactly 2 tests)

For each log this reads:

- every libtest `test result:` line, summing passed, failed and ignored over
  the binaries one cargo command ran;
- the selector line the persistence tests print once per test binary,
  `fhir-versions: HFS_TEST_FHIR_VERSION=<value> -> <Variant> (<version>)`.
  It is expected in every binary that ran a test and must name the value
  the step set and that value's variant: a missing or different one means
  the selector silently did not apply, and the run tested the build's
  default version instead. The hfs tests print it with a tail, ", passed to
  hfs as HFS_DEFAULT_FHIR_VERSION=<Variant>", and assert that hfs's startup
  line names that version. The one exception is backend_capability_contract,
  which writes no resource and never asks for a version (VERSION_NEUTRAL);
- the exit status from the last `exit=<n>` (or `rc=<n>`) line.

Usage: fhir-versions-report.py <logs-dir> <out-dir> [--versions R4,R4B,R5,R6]

Without --versions: R4, R4B, R5 and R6, plus any other version a log in
<logs-dir> is named for. Writes <out-dir>/fhir-versions.md and .json and
prints the Markdown; the JSON's "warnings" list becomes ::warning::
annotations through `ci-report.py add`. Exits 0 whatever the logs say: this
writes the report, and the job's next step fails it unless the JSON's "all_ok"
is true. Standard library only; runs with `python3 -I`. Tests:
.github/scripts/test_fhir_versions_report.py.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
RESULT = re.compile(r"^test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;")
# libtest's first line for each binary, with or without cargo around it.
BINARY_START = re.compile(r"^running (\d+) tests?$")
# cargo's line before each binary, which names it.
CARGO_RUNNING = re.compile(r"^\s*Running (\S+(?: \S+)?) \((.+)\)\s*$")
SELECTOR = re.compile(r"^fhir-versions: HFS_TEST_FHIR_VERSION=(\S*) -> (\S+) \(([^)]*)\)(?:,.*)?\s*$")
# Test binaries that never ask for a version: they print no selector line.
VERSION_NEUTRAL = ("backend_capability_contract",)
EXIT = re.compile(r"^(?:exit|rc)=(-?\d+)\s*$")
LOG_NAME = re.compile(r"^fhir-(.+)-(lib|tests|rest)\.log$")

DEFAULT_VERSIONS = ["R4", "R4B", "R5", "R6"]
# FhirVersion's variant for each value the step may set: the variant names
# themselves and their version numbers (FhirVersion::from_storage forms).
VARIANT = {
    "R4": "R4", "4.0": "R4", "R4B": "R4B", "4.3": "R4B",
    "R5": "R5", "5.0": "R5", "R6": "R6", "6.0": "R6",
}

SUITES = [
    {"id": "lib", "label": "persistence --lib backends::cassandra", "selector": True, "tests": None},
    {"id": "tests", "label": "persistence cassandra_tests + backend_capability_contract",
     "selector": True, "tests": None},
    {"id": "rest", "label": "hfs cassandra_milestone_a (round trip + Step 3 CRUD)",
     "selector": True, "tests": 2},
]


def parse_log(text: str) -> dict:
    """The binaries, results, selector lines and exit status in one log."""
    binaries: list[dict] = []
    name = None
    exit_status = None
    for raw in text.splitlines():
        line = ANSI.sub("", raw).rstrip()
        if match := CARGO_RUNNING.match(line):
            name = match.group(1)
            continue
        if match := BINARY_START.match(line):
            binaries.append({"name": name, "planned": int(match.group(1)), "result": None, "selectors": []})
            name = None
            continue
        if match := EXIT.match(line):
            exit_status = int(match.group(1))
            continue
        if match := SELECTOR.match(line):
            selector = {"value": match.group(1), "variant": match.group(2), "version": match.group(3)}
            if not binaries:
                binaries.append({"name": None, "planned": None, "result": None, "selectors": []})
            binaries[-1]["selectors"].append(selector)
            continue
        if match := RESULT.match(line):
            if not binaries or binaries[-1]["result"] is not None:
                binaries.append({"name": name, "planned": None, "result": None, "selectors": []})
            binaries[-1]["result"] = {
                "outcome": match.group(1), "passed": int(match.group(2)),
                "failed": int(match.group(3)), "ignored": int(match.group(4)),
            }
    return {"binaries": binaries, "exit": exit_status}


def check_selector(binaries: list[dict], version: str, expected: bool) -> tuple[str, dict | None, list[str]]:
    """The selector cell, the first selector line, and the problems found."""
    problems = []
    lines = [s for b in binaries for s in b["selectors"]]
    first = lines[0] if lines else None
    want_variant = VARIANT.get(version)
    for s in lines:
        if s["value"] != version or (want_variant and s["variant"] != want_variant):
            problems.append(
                f"selector names HFS_TEST_FHIR_VERSION={s['value']} -> {s['variant']}, "
                f"not {version}" + (f" -> {want_variant}" if want_variant else "")
            )
            break
    if expected:
        ran = [b for b in binaries if b["result"] and (b["result"]["passed"] + b["result"]["failed"]) > 0]
        silent = [b for b in ran if not b["selectors"]
                  and not any(n in (b["name"] or "") for n in VERSION_NEUTRAL)]
        if ran and len(silent) == len(ran):
            problems.append("no selector line: the version selector may not have applied")
        elif silent:
            names = ", ".join(b["name"] or "?" for b in silent)
            problems.append(f"no selector line in {len(silent)} of {len(ran)} binaries ({names})")
    if problems:
        cell = "NOT OK"
    elif first:
        cell = f"ok ({first['variant']})"
    else:
        cell = "not expected (hfs)" if not expected else "–"
    return cell, first, problems


def report_cell(path: Path, version: str, suite: dict) -> dict:
    row = {"version": version, "suite": suite["id"], "log": path.name, "passed": None, "failed": None,
           "ignored": None, "exit": None, "binaries": 0, "selector": None, "selector_line": None,
           "selector_expected": suite["selector"], "problems": []}
    try:
        text = path.read_text(errors="replace")
    except OSError:
        row["problems"].append("log missing")
        row["selector"] = "–"
        row["ok"] = False
        return row
    parsed = parse_log(text)
    results = [b["result"] for b in parsed["binaries"] if b["result"]]
    row["binaries"] = len(results)
    row["exit"] = parsed["exit"]
    if results:
        for key in ("passed", "failed", "ignored"):
            row[key] = sum(r[key] for r in results)
    else:
        row["problems"].append("no `test result` line")
    unfinished = [b for b in parsed["binaries"] if b["planned"] is not None and b["result"] is None]
    if unfinished:
        row["problems"].append(f"{len(unfinished)} binary(ies) started and printed no `test result` line")
    if parsed["exit"] is None:
        row["problems"].append("no `exit=` line: the run was cut off")
    elif parsed["exit"] != 0:
        row["problems"].append(f"exit status {parsed['exit']}")
    if row["failed"]:
        row["problems"].append(f"{row['failed']} failed")
    if results and row["passed"] == 0 and not row["failed"]:
        row["problems"].append("no test passed: the filter matched nothing")
    ran = (row["passed"] or 0) + (row["failed"] or 0) + (row["ignored"] or 0)
    if results and suite["tests"] is not None and ran != suite["tests"]:
        row["problems"].append(f"{ran} tests ran, expected exactly {suite['tests']}")
    cell, first, problems = check_selector(parsed["binaries"], version, suite["selector"])
    row["selector"], row["selector_line"] = cell, first
    row["problems"] += problems
    row["ok"] = not row["problems"]
    return row


def versions_in(logs: Path, requested: list[str] | None) -> list[str]:
    if requested:
        return requested
    found = []
    for path in sorted(logs.glob("fhir-*.log")):
        if (match := LOG_NAME.match(path.name)) and match.group(1) not in DEFAULT_VERSIONS + found:
            found.append(match.group(1))
    return DEFAULT_VERSIONS + found


def cell(value) -> str:
    return "–" if value is None else str(value).replace("|", "\\|")


def build(logs: Path, versions: list[str]) -> tuple[str, dict]:
    rows = [report_cell(logs / f"fhir-{v}-{s['id']}.log", v, s) for v in versions for s in SUITES]
    warnings = [f"FHIR versions: {r['version']} {r['suite']}: {p}" for r in rows for p in r["problems"]]
    labels = {s["id"]: s["label"] for s in SUITES}
    lines = [
        "| Version | Suite | Passed | Failed | Ignored | Exit | Selector line | Result |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for r in rows:
        result = "ok" if r["ok"] else "**FAILED**: " + "; ".join(r["problems"])
        lines.append("| " + " | ".join(cell(v) for v in [
            r["version"], f"`{r['suite']}`", r["passed"], r["failed"], r["ignored"], r["exit"],
            r["selector"], result]) + " |")
    ok = sum(r["ok"] for r in rows)
    markdown = "\n".join([
        f"{ok} of {len(rows)} runs ok, over {len(versions)} FHIR version{"s" if len(versions) != 1 else ""} ({', '.join(versions)}), "
        "one build with every version on, one Cassandra node.",
        "",
        *lines,
        "",
        "Suites: " + "; ".join(f"`{k}` {v}" for k, v in labels.items()) + ".",
        "",
        "Selector line: `fhir-versions: HFS_TEST_FHIR_VERSION=<value> -> <Variant> (<version>)`, which each "
        "test binary prints once (the hfs tests add the `HFS_DEFAULT_FHIR_VERSION` they pass to hfs, and "
        "assert hfs's startup line names it). It is required in every binary that ran a test, except "
        "`backend_capability_contract`, which writes no resource and never asks for a version, and it must "
        "name the version the step set: without it the run may have tested the build's default version.",
    ])
    data = {"versions": versions, "suites": SUITES, "rows": rows, "ok": ok, "runs": len(rows),
            "all_ok": ok == len(rows), "warnings": warnings}
    return markdown + "\n", data


def main(argv: list[str]) -> int:
    args, versions = list(argv), None
    if "--versions" in args:
        at = args.index("--versions")
        if at + 1 >= len(args):
            print("--versions needs a comma-separated list", file=sys.stderr)
            return 2
        versions = [v for v in args[at + 1].split(",") if v]
        del args[at:at + 2]
    if len(args) != 2:
        print(__doc__.split("\n\n")[0], file=sys.stderr)
        print("usage: fhir-versions-report.py <logs-dir> <out-dir> [--versions R4,R4B,R5,R6]", file=sys.stderr)
        return 2
    logs, out = Path(args[0]), Path(args[1])
    markdown, data = build(logs, versions_in(logs, versions))
    out.mkdir(parents=True, exist_ok=True)
    (out / "fhir-versions.md").write_text(markdown)
    (out / "fhir-versions.json").write_text(json.dumps(data, indent=2, allow_nan=False))
    print(markdown)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
