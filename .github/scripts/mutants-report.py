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

For each unviable mutant it reads the first rustc error in the mutant's log
(`mutants.out/log/`) and groups the mutants by why they did not compile: a
`Result` type alias cargo-mutants does not see through, a const evaluation
that rejected the mutant at compile time (a compile-time assertion, as good
as caught), `Default::default()` for a type with no `Default`, a guessed
constructor the type does not have, or the rustc error itself. When at least
TESTED_LITTLE_SHARE of the mutants did not compile for a reason other than a
const evaluation, so they tested nothing, the report says so at the top and
warns, with the main reasons (ADR-aacruzgon-0084).

`--alias NAME=ERROR`, given once per alias the job rewrote with
mutants-result-alias.py, spells the rewritten return types back as the
source does (`::core::result::Result<T, ERROR>` as `NAME<T>`) in the
mutants' names.

Usage: mutants-report.py <mutants.out dir> <out-dir> [--exit-status N]
                         [--diff FILE] [--title-note TEXT]
                         [--alias NAME=ERROR]...

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
# At least this share of a run's mutants did not compile for a reason other
# than a const evaluation, so they tested nothing: the report warns that the
# run tested little. On #102 (run 44801901198) the share was 29 of 30, 97 %,
# all but one a StorageResult alias mutant; on #90 (run 44714518327) it was
# 22 of 153, 14 % (54 unviable, 32 of them rejected by const evaluation).
# Half is far from both: below it most mutants were tested.
TESTED_LITTLE_SHARE = 0.5
# The return type mutants-result-alias.py writes for an alias.
REWRITTEN_RESULT = "::core::result::Result<"
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
# rustc's own error lines, `error[E0599]: ...` or `error: ...`; the summary
# lines that follow the real errors are not reasons.
RUSTC_ERROR = re.compile(r"^error(?:\[(?P<code>E\d{4})\])?: (?P<message>.*)$", re.M)
NOT_A_REASON = re.compile(r"^(could not compile|aborting due to|Some errors have detailed|For more information)")
# A constructor cargo-mutants guesses for a generic type it does not know.
GUESSED_CONSTRUCTOR = re.compile(r"\b(?P<type>[A-Z][A-Za-z0-9_]*)::(?:new|from|from_iter)\b")


def cell(text: object) -> str:
    """A Markdown table cell: pipes escaped (mutants of `||` and `|` are common), no newlines."""
    return str(text).replace("|", "\\|").replace("\n", " ")


def table(headers: list[str], rows: list[list[object]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    out += ["| " + " | ".join(cell(c) for c in row) + " |" for row in rows]
    return "\n".join(out) + "\n"


def json_kind(value: object) -> str:
    """What a decoded JSON value is, in JSON's own words."""
    return {dict: "object", list: "array", str: "string", bool: "boolean", int: "number",
            float: "number", type(None): "null"}.get(type(value), type(value).__name__)


def unalias(text: str, aliases: dict[str, str]) -> str:
    """`::core::result::Result<T, ERROR>`, as mutants-result-alias.py writes an alias, spelled `NAME<T>` again."""
    if not aliases or REWRITTEN_RESULT not in text:
        return text
    by_error = {error: name for name, error in aliases.items()}
    out, i = "", 0
    while (start := text.find(REWRITTEN_RESULT, i)) >= 0:
        open_at = start + len(REWRITTEN_RESULT) - 1
        depth, close_at = 0, -1
        for k in range(open_at, len(text)):
            if text[k] == "<":
                depth += 1
            elif text[k] == ">" and text[k - 1] not in "-=":
                depth -= 1
                if depth == 0:
                    close_at = k
                    break
        inner = text[open_at + 1:close_at] if close_at >= 0 else ""
        head, sep, error = inner.rpartition(",")
        name = by_error.get(error.strip()) if sep else None
        if name is None:
            out += text[i:open_at + 1]
            i = open_at + 1
            continue
        out += text[i:start] + f"{name}<{head.rstrip()}>"
        i = close_at + 1
    return out + text[i:]


def first_rustc_error(log: str) -> tuple[str | None, str] | None:
    """The first rustc error in a mutant's log, as (code, message), ANSI colours removed."""
    for match in RUSTC_ERROR.finditer(ANSI.sub("", log)):
        message = match.group("message").strip()
        if not NOT_A_REASON.match(message):
            return match.group("code"), message
    return None


def unviable_reason(replacement: str | None, error: tuple[str | None, str] | None) -> tuple[str, bool]:
    """Why an unviable mutant did not compile, and whether a const evaluation rejected it (as good as caught)."""
    code, message = error if error else (None, "")
    guessed = GUESSED_CONSTRUCTOR.search(replacement or "")
    if guessed and guessed.group("type").endswith("Result"):
        return (f"`{guessed.group('type')}::new`, `::from` or `::from_iter`: a `Result` type alias "
                "cargo-mutants does not see through, so it never tries `Ok(…)`"), False
    if code == "E0080" or message.startswith("constant evaluation is taking a long time"):
        return ("rejected at compile time by a const evaluation (a compile-time assertion panicked, or the "
                "mutated const fn overflowed, went out of bounds or ran too long): as good as caught"), True
    if code == "E0277" and "Default" in message and "Default::default()" in (replacement or ""):
        return "`Default::default()` for a type with no `Default` impl", False
    # E0599 for a missing `new`, E0277, E0271 or E0283 for a `From` or `FromIterator` it does not implement.
    if code in ("E0599", "E0277", "E0271", "E0283") and guessed:
        return "a guessed constructor the type does not have (`T::new`, `::from` or `::from_iter`)", False
    if error is None:
        return "no rustc error found in the mutant's log", False
    return f"rustc {code or 'error'}: " + re.sub(r"`[^`]*`", "`…`", message)[:120], False


def mutant_of(outcome: dict) -> dict | None:
    scenario = outcome.get("scenario")
    if isinstance(scenario, dict) and isinstance(scenario.get("Mutant"), dict):
        return scenario["Mutant"]
    return None


def describe(mutant: dict, aliases: dict[str, str] | None = None) -> dict:
    """Where a mutant is and what it does, from its JSON (name, file, span, function, replacement)."""
    name = unalias(mutant.get("name") or "", aliases or {})
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


def build(out_dir: Path, exit_status: int | None, diff: Path | None,
          aliases: dict[str, str] | None = None) -> dict:
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
    # Valid JSON is not yet an outcomes document: `[]` or `null` would fail
    # at the first field, and the job would get no report at all.
    # cargo-mutants always writes the outcomes array, so `{}` or
    # `{"outcomes": null}` is a damaged file, not a run with nothing in it.
    if not isinstance(outcomes, dict) or not isinstance(outcomes.get("outcomes"), list):
        data["outcomes_read"] = False
        data["note"] = f"outcomes.json is not an outcomes document (a JSON {json_kind(outcomes)}), so no outcome was read"
        warnings.append(f"Mutation testing: {out_dir / 'outcomes.json'} is not an outcomes document "
                        f"(a JSON {json_kind(outcomes)}); no outcome was read")
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
        if not isinstance(outcome, dict):
            warnings.append(f"Mutation testing: an outcome that is not an object (a JSON {json_kind(outcome)}) was skipped")
            continue
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
        info = describe(mutant, aliases)
        info["diff"] = outcome.get("diff_path")
        info["log"] = outcome.get("log_path")
        if kind == "unviable":
            try:
                log = (out_dir / info["log"]).read_text(errors="replace") if info["log"] else ""
            except OSError:
                log = ""
            error = first_rustc_error(log)
            info["rustc_error"] = None if error is None else {"code": error[0], "message": error[1]}
            info["reason"], info["const_eval"] = unviable_reason(info["replacement"], error)
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
    reasons: dict[str, dict] = {}
    for row in data["unviable"]:
        entry = reasons.setdefault(row["reason"], {"reason": row["reason"], "count": 0,
                                                   "const_eval": row["const_eval"], "example": row["name"]})
        entry["count"] += 1
    data["unviable_reasons"] = sorted(reasons.values(), key=lambda r: (-r["count"], r["reason"]))
    total = data.get("generated") or data["tested"]
    untestable = sum(r["count"] for r in data["unviable_reasons"] if not r["const_eval"])
    data["untestable"] = untestable
    data["untestable_share"] = round(untestable / total, 3) if total else None
    data["tested_little"] = bool(total) and untestable / total >= TESTED_LITTLE_SHARE
    if data["tested_little"]:
        main_reasons = "; ".join(f"{r['count']}: {r['reason']}" for r in data["unviable_reasons"]
                                 if not r["const_eval"])
        warnings.append(f"Mutation testing tested little: {untestable} of {total} mutants "
                        f"({untestable / total:.0%}) did not compile, so they tested nothing. {main_reasons}")
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
    if data.get("tested_little"):
        total = data.get("generated") or data["tested"]
        md += (f"> **Warning: this run tested little.** {data['untestable']} of {total} mutants "
               f"({data['untestable_share']:.0%}) did not compile for a reason other than a compile-time "
               "check, so they tested nothing, and the score below rests on the few that did. "
               "The reasons are under \"Why the unviable mutants did not compile\".\n\n")
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
    if data.get("unviable_reasons"):
        md += "\n### Why the unviable mutants did not compile\n\n" + table(
            ["Reason (the first rustc error in each mutant's log)", "Mutants", "Example"],
            [[r["reason"], r["count"], f"`{r['example']}`"] for r in data["unviable_reasons"]],
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
    aliases: dict[str, str] = {}
    while "--alias" in args:
        i = args.index("--alias")
        name, _, error = (args[i + 1] if i + 1 < len(args) else "").partition("=")
        if name and error:
            aliases[name] = error
        del args[i:i + 2]
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
    data = build(source, exit_status, diff, aliases)
    md = markdown(data, note)
    (out / "mutants.md").write_text(md)
    (out / "mutants.json").write_text(json.dumps(data, indent=2) + "\n")
    print(md)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
