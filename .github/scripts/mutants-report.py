#!/usr/bin/env python3
"""Mutation testing of the Cassandra backend: what cargo-mutants caught and missed (report only).

The `mutants` job of .github/workflows/cassandra-mutants.yml runs
cargo-mutants on the lines a pull request changes under
crates/persistence/src/backends/cassandra/ (ADR-aacruzgon-0082). This reads
the run's output directory, `mutants.out/`:

- `outcomes.json`: one outcome per scenario, the baseline first, each with
  its summary (`Success` for the baseline; `CaughtMutant`, `MissedMutant`,
  `Unviable` or `Timeout` for a mutant; `Failure` for a baseline that failed)
  and the duration of its build and test phases; and the run's totals;
- `mutants.json`: every mutant that was generated, which is written before
  testing starts, so a run that was cut short still says how many there were.

It writes `<out-dir>/mutants.md` and `<out-dir>/mutants.json`: the counts,
the mutation score, a table of every missed mutant (where it is, in which
function, and what was replaced), the timed-out ones, the counts per file and
the per-mutant build and test times. A missed mutant is a change to the code
that no test noticed; it is a lead for a reviewer, not a verdict.

Usage: mutants-report.py <mutants.out dir> <out-dir> [--exit-status N]
                         [--diff FILE] [--title-note TEXT]

--exit-status is cargo-mutants' own exit status, which says how the run
ended (book/src/exit-codes.md, cargo-mutants 27.1.0). --diff is the diff the
run was given with --in-diff, counted in the report. The JSON's "warnings"
list becomes ::warning:: annotations through `ci-report.py add`. Exits 0
whatever the run found: the job is report-only. Standard library only; runs
with `python3 -I`. Tests: .github/scripts/test_mutants_report.py.
"""

from __future__ import annotations

import json
import re
import statistics
import sys
from pathlib import Path

# cargo-mutants' exit statuses (book/src/exit-codes.md, 27.1.0).
EXIT_MEANING = {
    0: "every viable mutant was caught",
    1: "usage error",
    2: "some mutants were missed",
    3: "some mutants timed out",
    4: "the baseline tests failed in the unmutated tree, so no mutant was tested",
    5: "the new side of the --in-diff diff does not match the tree",
    6: "the --in-diff diff is not a valid diff",
    70: "internal error in cargo-mutants",
}
# Statuses that mean the run itself worked, whatever it found.
RAN = (0, 2, 3)
OUTCOMES = {
    "CaughtMutant": "caught",
    "MissedMutant": "missed",
    "Unviable": "unviable",
    "Timeout": "timeout",
}
# "<file>:<line>:<col>: <what>", the mutant names cargo-mutants prints.
NAME = re.compile(r"^(?P<file>[^:]+):(?P<line>\d+):(?P<col>\d+): (?P<what>.*)$")


def cell(text: object) -> str:
    """A Markdown table cell: pipes escaped (mutants of `||` and `|` are common), no newlines."""
    return str(text).replace("|", "\\|").replace("\n", " ")


def table(headers: list[str], rows: list[list[object]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    out += ["| " + " | ".join(cell(c) for c in row) + " |" for row in rows]
    return "\n".join(out) + "\n"


def mutant_of(outcome: dict) -> dict | None:
    scenario = outcome.get("scenario")
    if isinstance(scenario, dict) and isinstance(scenario.get("Mutant"), dict):
        return scenario["Mutant"]
    return None


def describe(mutant: dict) -> dict:
    """Where a mutant is and what it does, from its JSON (name, file, span, function, replacement)."""
    name = mutant.get("name") or ""
    match = NAME.match(name)
    line = (mutant.get("span") or {}).get("start", {}).get("line")
    if line is None and match:
        line = int(match.group("line"))
    function = (mutant.get("function") or {}).get("function_name")
    return {
        "name": name,
        "file": mutant.get("file") or (match.group("file") if match else None),
        "line": line,
        "function": function,
        "mutation": match.group("what") if match else name,
        "replacement": mutant.get("replacement"),
        "genre": mutant.get("genre"),
    }


def phase_seconds(outcome: dict, phase: str) -> float | None:
    for result in outcome.get("phase_results") or []:
        if result.get("phase") == phase and isinstance(result.get("duration"), (int, float)):
            return float(result["duration"])
    return None


def spread(values: list[float]) -> dict | None:
    if not values:
        return None
    return {"n": len(values), "median_s": round(statistics.median(values), 1), "max_s": round(max(values), 1),
            "total_s": round(sum(values), 1)}


def diff_stats(path: Path) -> dict | None:
    try:
        text = path.read_text(errors="replace")
    except OSError:
        return None
    files = [line[6:] for line in text.splitlines() if line.startswith("+++ b/")]
    added = sum(1 for line in text.splitlines() if line.startswith("+") and not line.startswith("+++"))
    removed = sum(1 for line in text.splitlines() if line.startswith("-") and not line.startswith("---"))
    return {"files": files, "added_lines": added, "removed_lines": removed}


def build(out_dir: Path, exit_status: int | None, diff: Path | None) -> dict:
    warnings: list[str] = []
    data: dict = {"exit_status": exit_status, "exit_meaning": EXIT_MEANING.get(exit_status) if exit_status is not None else None,
                  "warnings": warnings}
    if diff is not None:
        data["diff"] = diff_stats(diff)
    try:
        generated = json.loads((out_dir / "mutants.json").read_text())
        data["generated"] = len(generated) if isinstance(generated, list) else None
    except (OSError, ValueError):
        data["generated"] = None
    try:
        outcomes = json.loads((out_dir / "outcomes.json").read_text())
    except (OSError, ValueError) as error:
        data["outcomes_read"] = False
        if data["generated"] == 0 or (diff is not None and data.get("diff") and not data["diff"]["files"]):
            data["note"] = "no mutants: the diff changes no Rust code under the backend"
        elif exit_status in (None, 0) and data["generated"] is None:
            data["note"] = "cargo-mutants wrote no outcomes.json"
            warnings.append(f"Mutation testing: no outcomes.json in {out_dir} ({error.__class__.__name__})")
        else:
            warnings.append(f"Mutation testing: no outcomes.json in {out_dir} ({error.__class__.__name__})")
        if exit_status is not None and exit_status not in RAN:
            warnings.append(f"Mutation testing: cargo-mutants exited {exit_status} ({data['exit_meaning'] or 'unknown status'})")
        return data
    data["outcomes_read"] = True
    data["cargo_mutants_version"] = outcomes.get("cargo_mutants_version")
    data["start_time"] = outcomes.get("start_time")
    data["end_time"] = outcomes.get("end_time")
    counts = {key: 0 for key in ("caught", "missed", "unviable", "timeout")}
    rows: dict[str, list[dict]] = {key: [] for key in counts}
    per_file: dict[str, dict[str, int]] = {}
    builds: list[float] = []
    tests: list[float] = []
    baseline = None
    for outcome in outcomes.get("outcomes") or []:
        summary = outcome.get("summary")
        mutant = mutant_of(outcome)
        if mutant is None:
            baseline = {
                "summary": summary,
                "build_s": phase_seconds(outcome, "Build"),
                "test_s": phase_seconds(outcome, "Test"),
                "log": outcome.get("log_path"),
            }
            continue
        kind = OUTCOMES.get(summary)
        if kind is None:
            warnings.append(f"Mutation testing: unknown outcome {summary!r} for {mutant.get('name')}")
            continue
        counts[kind] += 1
        info = describe(mutant)
        info["diff"] = outcome.get("diff_path")
        info["log"] = outcome.get("log_path")
        rows[kind].append(info)
        per_file.setdefault(info["file"] or "?", {key: 0 for key in counts})[kind] += 1
        b, t = phase_seconds(outcome, "Build"), phase_seconds(outcome, "Test")
        if b is not None:
            builds.append(b)
        if t is not None:
            tests.append(t)
    tested = counts["caught"] + counts["missed"] + counts["timeout"]
    data.update({
        "counts": counts,
        "tested": sum(counts.values()),
        "total_mutants": outcomes.get("total_mutants"),
        # The usual mutation score: caught over the viable mutants. A timeout
        # counts as not caught, as cargo-mutants' exit status 3 treats it.
        "score": round(counts["caught"] / tested, 3) if tested else None,
        "missed": sorted(rows["missed"], key=lambda r: (r["file"] or "", r["line"] or 0, r["name"])),
        "timeout": sorted(rows["timeout"], key=lambda r: (r["file"] or "", r["line"] or 0, r["name"])),
        "unviable": sorted(rows["unviable"], key=lambda r: (r["file"] or "", r["line"] or 0, r["name"])),
        "per_file": per_file,
        "baseline": baseline,
        "build_times": spread(builds),
        "test_times": spread(tests),
    })
    if baseline and baseline["summary"] != "Success":
        warnings.append(f"Mutation testing: the baseline (unmutated tree) did not pass ({baseline['summary']}), so no mutant was tested")
    if exit_status is not None and exit_status not in RAN and exit_status != 4:
        warnings.append(f"Mutation testing: cargo-mutants exited {exit_status} ({data['exit_meaning'] or 'unknown status'})")
    generated = data.get("generated")
    if generated is not None and data["tested"] < generated:
        data["untested"] = generated - data["tested"]
        warnings.append(f"Mutation testing: {data['untested']} of {generated} mutants were not tested (the run stopped early)")
    return data


def markdown(data: dict, title_note: str | None = None) -> str:
    md = ""
    if title_note:
        md += title_note.strip() + "\n\n"
    if data.get("diff") is not None:
        d = data["diff"]
        md += (f"Diff: {len(d['files'])} file(s) under the backend, +{d['added_lines']} / -{d['removed_lines']} lines.\n\n")
    if not data.get("outcomes_read"):
        md += (data.get("note") or "No outcomes were written.") + "\n"
        if data.get("exit_status") is not None:
            md += f"\ncargo-mutants exited {data['exit_status']} ({data.get('exit_meaning') or 'unknown status'}).\n"
        return md
    c = data["counts"]
    score = f"{data['score']:.1%}" if data.get("score") is not None else "–"
    md += table(
        ["Mutants", "Caught", "Missed", "Unviable", "Timed out", "Score (caught / viable)"],
        [[data.get("generated") if data.get("generated") is not None else data["tested"], c["caught"], c["missed"],
          c["unviable"], c["timeout"], score]],
    )
    md += (f"\ncargo-mutants {data.get('cargo_mutants_version') or '?'}, exit {data.get('exit_status')}"
           f" ({data.get('exit_meaning') or 'unknown status'}). Report only: the job passes whatever survives. "
           "A missed mutant is a change no test noticed, a lead for a reviewer and not a verdict; "
           "an unviable one did not compile. The diffs and logs are in `mutants.out/` in the artifact.\n")
    if data.get("missed"):
        md += "\n### Missed mutants\n\n" + table(
            ["Where", "Function", "Mutation", "Diff"],
            [[f"`{r['file']}:{r['line']}`", f"`{r['function']}`" if r["function"] else "–", f"`{r['mutation']}`",
              f"`{r['diff']}`" if r.get("diff") else "–"] for r in data["missed"]],
        )
    if data.get("timeout"):
        md += "\n### Timed-out mutants\n\n" + table(
            ["Where", "Function", "Mutation"],
            [[f"`{r['file']}:{r['line']}`", f"`{r['function']}`" if r["function"] else "–", f"`{r['mutation']}`"]
             for r in data["timeout"]],
        )
    if data.get("per_file"):
        md += "\n### Per file\n\n" + table(
            ["File", "Caught", "Missed", "Unviable", "Timed out"],
            [[f"`{name}`", v["caught"], v["missed"], v["unviable"], v["timeout"]]
             for name, v in sorted(data["per_file"].items())],
        )
    rows = []
    base = data.get("baseline")
    if base:
        rows.append(["Baseline (unmutated)", fmt_s(base.get("build_s")), fmt_s(base.get("test_s")), "–", base.get("summary")])
    for label, key in (("Per mutant: build", "build_times"), ("Per mutant: test", "test_times")):
        s = data.get(key)
        if s:
            rows.append([label, f"median {s['median_s']} s", f"max {s['max_s']} s", f"total {s['total_s']} s", f"{s['n']} runs"])
    if rows:
        md += "\n### Times\n\n" + table(["Phase", "", "", "", ""], rows)
    return md


def fmt_s(value: float | None) -> str:
    return f"{value:.1f} s" if value is not None else "–"


def main(argv: list[str]) -> int:
    args = list(argv)
    exit_status = diff = note = None
    if "--exit-status" in args:
        i = args.index("--exit-status")
        try:
            exit_status = int(args[i + 1])
        except (IndexError, ValueError):
            exit_status = None
        del args[i:i + 2]
    if "--diff" in args:
        i = args.index("--diff")
        diff = Path(args[i + 1]) if i + 1 < len(args) else None
        del args[i:i + 2]
    if "--title-note" in args:
        i = args.index("--title-note")
        note = args[i + 1] if i + 1 < len(args) else None
        del args[i:i + 2]
    if len(args) != 2:
        print(__doc__, file=sys.stderr)
        return 0
    source, out = Path(args[0]), Path(args[1])
    out.mkdir(parents=True, exist_ok=True)
    data = build(source, exit_status, diff)
    md = markdown(data, note)
    (out / "mutants.md").write_text(md)
    (out / "mutants.json").write_text(json.dumps(data, indent=2) + "\n")
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
