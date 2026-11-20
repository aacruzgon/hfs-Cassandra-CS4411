#!/usr/bin/env python3
"""Flakiness of the Cassandra tests a pull request adds or changes (report only).

The `flaky` job of .github/workflows/cassandra-flaky.yml re-runs each test a
pull request adds or changes in the Cassandra backend, one test per run with
`--exact`, N times, and reports how many runs passed (ADR-aacruzgon-0082).
Commands:

  find --diff FILE --root DIR [--out FILE]
      The tests the diff adds or changes, as a JSON list. FILE is
      `git diff -U0 base...head` (any context width works); DIR is the
      checkout of the head. A test is every function whose attributes include
      one whose path ends in `test` (`#[test]`, `#[tokio::test]`,
      `#[tokio::test(flavor = …)]`), as cargo-mutants and libtest's users
      write them; `#[cfg(test)]` is not one. A test is changed when a line
      the diff adds or removes falls anywhere from its first attribute to its
      closing brace: an attribute on the line above, the signature, a rename,
      the body. Strings, chars and comments are blanked before anything is
      matched, so a test's source quoted in a string literal (as the
      backend's source scans do) is not a test, and a brace in a string does
      not end one. Scope: helios-persistence's --lib tests under
      src/backends/cassandra/, named by their module path
      (backends::cassandra::write::tests::…), and the integration tests in
      tests/cassandra/ and tests/cassandra_tests.rs, named by the module the
      test crate's `#[path = …] mod …;` gives each file.

  bins --cargo-json FILE --out FILE
      The test executables from `cargo test --no-run --message-format=json`:
      {"lib": path, "cassandra_tests": path, …}.

  run --tests FILE --bins FILE --runs N --logs DIR [--timeout S] [--budget-minutes M]
      Runs every test N times, each run its own process:
      `<binary> --exact <name> --show-output --test-threads 1`. Writes
      DIR/results.json and one log per test. Past M minutes no new run
      starts; the report counts the runs not reached.

  report --results FILE --out DIR
      DIR/flaky.md and DIR/flaky.json: per test, the runs, passes, failures,
      timeouts and the first failure's message. A run that reports no test
      (its name matched nothing) is "not run", and a test that printed
      "skipping:" passed without testing anything; both are warnings.

Every command exits 0 whatever it finds: the job is report-only. Standard
library only; runs with `python3 -I`. Tests: .github/scripts/test_flaky_tests.py.
"""

from __future__ import annotations

import bisect
import json
import re
import statistics
import subprocess
import sys
import time
from pathlib import Path

LIB_ROOT = "crates/persistence/src/"
LIB_SCOPE = "crates/persistence/src/backends/cassandra/"
TESTS_ROOT = "crates/persistence/tests/"
TESTS_SCOPE = ("crates/persistence/tests/cassandra/", "crates/persistence/tests/cassandra_tests.rs")

# An attribute whose path ends in `test`: `#[test]`, `#[tokio::test]`,
# `#[tokio::test(flavor = "multi_thread")]`. Matched on blanked source, where
# string contents are spaces, so the arguments' strings cannot confuse it.
TEST_ATTR = re.compile(r"#\s*\[\s*(?:[A-Za-z_]\w*\s*::\s*)*test\s*(?:\(|\])")
FN = re.compile(r"\bfn\s+([A-Za-z_]\w*)")
INLINE_MOD = re.compile(r"\bmod\s+([A-Za-z_]\w*)\s*\{")
HUNK = re.compile(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@")
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
RESULT = re.compile(r"^test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;")


# --- reading Rust source ----------------------------------------------------


def blank(source: str) -> str:
    """The source with every comment, string, byte string, raw string and char
    literal replaced by spaces, newlines kept, so offsets and line numbers
    stay the same and only code is left to match."""
    out = list(source)
    i, n = 0, len(source)

    def wipe(start: int, end: int) -> None:
        for k in range(start, min(end, n)):
            if out[k] != "\n":
                out[k] = " "

    while i < n:
        c = source[i]
        nxt = source[i + 1] if i + 1 < n else ""
        if c == "/" and nxt == "/":
            end = source.find("\n", i)
            end = n if end < 0 else end
            wipe(i, end)
            i = end
        elif c == "/" and nxt == "*":
            depth, j = 1, i + 2
            while j < n and depth:
                if source.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif source.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            wipe(i, j)
            i = j
        elif (c in "rb" and (i == 0 or not (source[i - 1].isalnum() or source[i - 1] == "_"))
              and re.match(r"(?:br|r)#*\"", source[i:i + 260])):
            m = re.match(r"(?:br|r)(#*)\"", source[i:])
            hashes = m.group(1)
            close = "\"" + hashes
            j = source.find(close, i + m.end())
            j = n if j < 0 else j + len(close)
            wipe(i, j)
            i = j
        elif c == "\"" or (c == "b" and nxt == "\"" and (i == 0 or not (source[i - 1].isalnum() or source[i - 1] == "_"))):
            j = i + (2 if c == "b" else 1)
            while j < n and source[j] != "\"":
                j += 2 if source[j] == "\\" else 1
            wipe(i, j + 1)
            i = j + 1
        elif c == "'":
            # A char literal ('x', '\n', '\u{1F600}', '{') or a lifetime ('a).
            m = re.match(r"'(?:\\(?:u\{[0-9A-Fa-f_]+\}|x[0-9A-Fa-f]{2}|.)|[^\\'\n])'", source[i:i + 14])
            if m:
                wipe(i, i + m.end())
                i += m.end()
            else:
                i += 1
        else:
            i += 1
    return "".join(out)


def match_brace(code: str, open_at: int) -> int:
    """The offset of the brace that closes the one at open_at (blanked code)."""
    depth = 0
    for k in range(open_at, len(code)):
        if code[k] == "{":
            depth += 1
        elif code[k] == "}":
            depth -= 1
            if depth == 0:
                return k
    return len(code) - 1


class Lines:
    def __init__(self, text: str):
        self.starts = [0] + [m.end() for m in re.finditer("\n", text)]

    def of(self, offset: int) -> int:
        return bisect.bisect_right(self.starts, offset)


def inline_mods(code: str) -> list[tuple[int, int, str]]:
    return [(m.end() - 1, match_brace(code, m.end() - 1), m.group(1)) for m in INLINE_MOD.finditer(code)]


def tests_in(source: str) -> list[dict]:
    """Every test function in one file: its name, the inline modules it is
    in, and the lines from its first attribute to its closing brace."""
    code = blank(source)
    lines = Lines(source)
    mods = inline_mods(code)
    found, seen_fn = [], set()
    for attr in TEST_ATTR.finditer(code):
        fn = FN.search(code, attr.end())
        if not fn or fn.start() in seen_fn:
            continue
        # Nothing but attributes and modifiers between the attribute and fn.
        between = code[attr.end():fn.start()]
        if re.search(r"[;{}]", re.sub(r"#\s*\[[^\]]*\]", "", between)):
            continue
        seen_fn.add(fn.start())
        body = code.find("{", fn.end())
        if body < 0:
            continue
        end = match_brace(code, body)
        # The attribute block: the test attribute and every attribute line
        # right above it (#[ignore], #[should_panic], #[cfg_attr(…)]).
        start_line = lines.of(attr.start())
        source_lines = code.split("\n")
        while start_line > 1 and source_lines[start_line - 2].strip().startswith("#["):
            start_line -= 1
        path = [name for (o, c, name) in mods if o < fn.start() < c]
        found.append({
            "fn": fn.group(1),
            "modules": path,
            "start": start_line,
            "fn_line": lines.of(fn.start()),
            "end": lines.of(end),
        })
    return found


# --- the diff ---------------------------------------------------------------


def changed_lines(diff: str) -> dict[str, dict]:
    """Per new-side file: the lines the diff adds, and the points where it
    removes lines (a removal between new lines c and c+1 is recorded as c)."""
    files: dict[str, dict] = {}
    current = None
    new_line = 0
    in_hunk = False
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            current, in_hunk = None, False
            continue
        if not in_hunk and line.startswith("+++ "):
            name = line[4:].strip()
            if name == "/dev/null":
                current = None
            else:
                name = name[2:] if name.startswith("b/") else name
                current = files.setdefault(name, {"added": set(), "removed_at": set()})
            continue
        m = HUNK.match(line)
        if m:
            in_hunk = True
            new_line = int(m.group(3))
            # A hunk that only removes names the line before the gap.
            if m.group(4) == "0":
                new_line += 1
            continue
        if current is None or not in_hunk:
            continue
        if line.startswith("+"):
            current["added"].add(new_line)
            new_line += 1
        elif line.startswith("-"):
            current["removed_at"].add(new_line - 1)
        elif line.startswith(" ") or line == "":
            new_line += 1
    return files


def touched(test: dict, change: dict) -> str | None:
    span = range(test["start"], test["end"] + 1)
    added = [n for n in span if n in change["added"]]
    # A removal just above the first line (an attribute taken away) counts too.
    removed = [c for c in change["removed_at"] if test["start"] - 1 <= c < test["end"]]
    if not added and not removed:
        return None
    return "added" if len(added) == len(span) and not removed else "changed"


# --- where each file's tests live ---------------------------------------------


def lib_module(path: str) -> list[str] | None:
    if not path.startswith(LIB_ROOT) or not path.endswith(".rs"):
        return None
    parts = path[len(LIB_ROOT):-3].split("/")
    if parts[-1] in ("mod", "lib"):
        parts = parts[:-1]
    return parts


def test_crate_modules(root: Path) -> dict[str, list[tuple[str, list[str]]]]:
    """For each file under tests/, the test binaries that include it and the
    module path each gives it, from every root tests/*.rs: `#[path = "…"] mod
    x;`, plain `mod x;` (tests/x.rs or tests/x/mod.rs), inside inline modules
    too."""
    found: dict[str, list[tuple[str, list[str]]]] = {}
    tests_dir = root / TESTS_ROOT
    for crate in sorted(tests_dir.glob("*.rs")):
        target = crate.stem
        found.setdefault(f"{TESTS_ROOT}{crate.name}", []).append((target, []))
        source = crate.read_text(errors="replace")
        code = blank(source)
        mods = inline_mods(code)
        for m in re.finditer(r"(?:#\s*\[\s*path\s*=\s*\"([^\"]+)\"\s*\]\s*)?(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z_]\w*)\s*;", source):
            if code[m.start()] == " " and code[m.end() - 1] == " ":
                continue  # in a comment or a string
            outer = [name for (o, c, name) in mods if o < m.start() < c]
            if m.group(1):
                rel = m.group(1)
            else:
                rel = "/".join(outer + [m.group(2)]) + ".rs"
                if not (tests_dir / rel).exists():
                    rel = "/".join(outer + [m.group(2), "mod.rs"])
            found.setdefault(f"{TESTS_ROOT}{rel}", []).append((target, outer + [m.group(2)]))
    return found


def find(diff_text: str, root: Path) -> dict:
    changes = changed_lines(diff_text)
    crate_mods = None
    tests, warnings, files = [], [], []
    for path, change in sorted(changes.items()):
        if not path.endswith(".rs"):
            continue
        in_lib = path.startswith(LIB_SCOPE)
        in_tests = path.startswith(TESTS_SCOPE)
        if not (in_lib or in_tests):
            continue
        try:
            source = (root / path).read_text(errors="replace")
        except OSError as error:
            warnings.append(f"{path}: not readable in the head ({error.__class__.__name__})")
            continue
        if in_lib:
            places = [("lib", lib_module(path) or [])]
        else:
            if crate_mods is None:
                crate_mods = test_crate_modules(root)
            places = crate_mods.get(path) or []
            if not places:
                warnings.append(f"{path}: no test crate under {TESTS_ROOT} includes it, so its tests cannot be named")
                continue
        file_tests = tests_in(source)
        files.append({"file": path, "tests": len(file_tests)})
        for test in file_tests:
            kind = touched(test, change)
            if not kind:
                continue
            for target, prefix in places:
                tests.append({
                    "target": target,
                    "name": "::".join(prefix + test["modules"] + [test["fn"]]),
                    "file": path,
                    "line": test["fn_line"],
                    "change": kind,
                })
    tests.sort(key=lambda t: (t["target"], t["name"]))
    return {"tests": tests, "files": files, "warnings": warnings}


# --- running them -------------------------------------------------------------


def bins(cargo_json: str) -> dict[str, str]:
    """{"lib": executable, "<test target>": executable} from cargo's JSON messages."""
    found: dict[str, str] = {}
    for line in cargo_json.splitlines():
        try:
            msg = json.loads(line)
        except ValueError:
            continue
        if msg.get("reason") != "compiler-artifact" or not msg.get("executable"):
            continue
        target = msg.get("target") or {}
        kinds = target.get("kind") or []
        if not (msg.get("profile") or {}).get("test"):
            continue
        if "lib" in kinds or "rlib" in kinds:
            found["lib"] = msg["executable"]
        elif "test" in kinds:
            found[target.get("name")] = msg["executable"]
    return found


def classify(output: str, status: int, timed_out: bool) -> dict:
    text = ANSI.sub("", output)
    result = None
    for line in text.splitlines():
        m = RESULT.match(line.strip())
        if m:
            result = {"passed": int(m.group(2)), "failed": int(m.group(3)), "ignored": int(m.group(4))}
    if timed_out:
        outcome = "timeout"
    elif result is None:
        outcome = "error"
    elif result["failed"]:
        outcome = "fail"
    elif result["passed"] == 1:
        outcome = "pass"
    elif result["ignored"]:
        outcome = "ignored"
    else:
        outcome = "not run"
    if outcome == "pass" and status != 0:
        outcome = "error"
    return {"outcome": outcome, "status": status, "skipped": outcome == "pass" and "skipping:" in text}


def failure_message(output: str) -> str:
    """The first failure's message: the panic line and what follows it, from libtest's failures section."""
    text = ANSI.sub("", output)
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if "panicked at" in line:
            tail = [line.strip()] + [x.strip() for x in lines[i + 1:i + 6] if x.strip() and not x.startswith("note:")]
            return " ".join(tail)[:400]
    for i, line in enumerate(lines):
        if line.startswith("---- ") and line.endswith(" ----"):
            return " ".join(x.strip() for x in lines[i + 1:i + 6] if x.strip())[:400]
    tail = [x.strip() for x in lines if x.strip()][-3:]
    return " ".join(tail)[:400]


def run(tests: list[dict], binaries: dict[str, str], runs: int, logs: Path, timeout: float,
        budget_s: float | None = None) -> dict:
    """Each test `runs` times. Past budget_s no new run starts, and the runs
    not reached are counted in the result rather than run."""
    logs.mkdir(parents=True, exist_ok=True)
    results = []
    began = time.monotonic()
    stopped = False
    for index, test in enumerate(tests):
        binary = binaries.get(test["target"])
        entry = {**test, "runs": [], "log": None, "not_reached": 0}
        if not binary:
            entry["error"] = f"no test binary for target {test['target']!r}"
            results.append(entry)
            continue
        log_name = f"{index:03d}-{test['target']}-{test['name'].replace('::', '.')}.log"
        entry["log"] = log_name
        with open(logs / log_name, "w") as log:
            for attempt in range(1, runs + 1):
                if budget_s is not None and time.monotonic() - began > budget_s:
                    stopped = True
                    entry["not_reached"] = runs - attempt + 1
                    break
                started = time.monotonic()
                timed_out = False
                try:
                    done = subprocess.run([binary, "--exact", test["name"], "--show-output", "--test-threads", "1"],
                                          capture_output=True, text=True, timeout=timeout)
                    output, status = done.stdout + done.stderr, done.returncode
                except subprocess.TimeoutExpired as error:
                    output = (error.stdout or b"").decode(errors="replace") if isinstance(error.stdout, bytes) else (error.stdout or "")
                    output += (error.stderr or b"").decode(errors="replace") if isinstance(error.stderr, bytes) else (error.stderr or "")
                    status, timed_out = 124, True
                seconds = round(time.monotonic() - started, 2)
                verdict = classify(output, status, timed_out)
                verdict["seconds"] = seconds
                if verdict["outcome"] not in ("pass", "ignored"):
                    verdict["message"] = failure_message(output) if not timed_out else f"timed out after {timeout:.0f} s"
                entry["runs"].append(verdict)
                log.write(f"===== run {attempt} of {runs}: {verdict['outcome']} (exit {status}, {seconds} s)\n{output}\n")
                print(f"{test['target']} {test['name']} run {attempt}/{runs}: {verdict['outcome']} ({seconds} s)", flush=True)
        results.append(entry)
    return {"runs_per_test": runs, "timeout_s": timeout, "budget_s": budget_s, "stopped_at_budget": stopped,
            "results": results}


# --- the report -----------------------------------------------------------------


def cell(text: object) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def table(headers: list[str], rows: list[list[object]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join("---" for _ in headers) + " |"]
    out += ["| " + " | ".join(cell(c) for c in row) + " |" for row in rows]
    return "\n".join(out) + "\n"


def report(results: dict, found: dict | None = None) -> tuple[str, dict]:
    rows, data_rows, warnings = [], [], list((found or {}).get("warnings") or [])
    for entry in results.get("results") or []:
        runs = entry.get("runs") or []
        count = {k: sum(1 for r in runs if r["outcome"] == k) for k in ("pass", "fail", "timeout", "error", "ignored", "not run")}
        skipped = sum(1 for r in runs if r.get("skipped"))
        first = next((r for r in runs if r["outcome"] not in ("pass", "ignored")), None)
        seconds = [r["seconds"] for r in runs if "seconds" in r]
        unstable = count["fail"] + count["timeout"] + count["error"]
        row = {
            "target": entry["target"], "name": entry["name"], "file": entry.get("file"), "line": entry.get("line"),
            "change": entry.get("change"), "runs": len(runs), "passes": count["pass"], "failures": count["fail"],
            "timeouts": count["timeout"], "errors": count["error"], "ignored": count["ignored"],
            "not_run": count["not run"], "skipped": skipped,
            "first_failure": (first or {}).get("message") if first else entry.get("error"),
            "median_s": round(statistics.median(seconds), 2) if seconds else None,
            "max_s": round(max(seconds), 2) if seconds else None,
            "log": entry.get("log"),
            "not_reached": entry.get("not_reached") or 0,
            "flaky": 0 < unstable < len(runs),
            "always_fails": bool(runs) and unstable == len(runs),
        }
        data_rows.append(row)
        if entry.get("error"):
            warnings.append(f"{entry['name']}: {entry['error']}")
        if row["not_run"]:
            warnings.append(f"{entry['name']}: matched no test in {entry['target']} on {row['not_run']} run(s); its name may be wrong")
        if row["flaky"]:
            warnings.append(f"Flaky: {entry['name']} failed {unstable} of {len(runs)} runs")
        if entry.get("not_reached"):
            warnings.append(f"{entry['name']}: {entry['not_reached']} of its runs were not reached within the time budget")
        if row["always_fails"]:
            warnings.append(f"{entry['name']} failed every one of its {len(runs)} runs")
        status = ("flaky" if row["flaky"] else "fails every run" if row["always_fails"] else
                  "ignored" if row["ignored"] == len(runs) and runs else
                  "not run (its name matched no test)" if count["not run"] == len(runs) and runs else
                  "skipped (no node?)" if skipped == len(runs) and runs else "stable" if runs else "not run")
        rows.append([
            f"`{entry['name']}`", entry["target"], entry.get("change") or "–", len(runs), count["pass"],
            count["fail"] + count["error"], count["timeout"], status,
            f"{row['median_s']} / {row['max_s']} s" if seconds else "–",
            row["first_failure"] or "–",
        ])
    total = len(data_rows)
    flaky = sum(1 for r in data_rows if r["flaky"])
    failing = sum(1 for r in data_rows if r["always_fails"])
    md = ""
    expected = len((found or {}).get("tests") or [])
    if total == 0 and expected:
        md += (f"{expected} test(s) were found in the diff, but none was run: the build or the node "
               "did not come up. See the job's log.\n")
        warnings.append(f"Flaky tests: {expected} test(s) found, none run")
    elif total == 0:
        md += "No test added or changed under the backend's lib or tests/cassandra/, so nothing was re-run.\n"
    else:
        md += (f"{total} test(s) re-run {results.get('runs_per_test')} times each, one test per process "
               f"(`--exact`), {results.get('timeout_s'):.0f} s timeout per run: {flaky} flaky, {failing} failing every run. "
               "Report only: the job passes whatever this finds. A test that printed `skipping:` passed without "
               "testing anything.\n\n")
        if results.get("stopped_at_budget"):
            md += (f"The run stopped at its {results.get('budget_s', 0) / 60:.0f}-minute budget; "
                   "the tests with fewer runs than the rest were not finished.\n\n")
        md += table(["Test", "Binary", "Change", "Runs", "Passes", "Failures", "Timeouts", "Status",
                     "Median / max time", "First failure"], rows)
    if found and found.get("files"):
        md += "\nFiles read: " + ", ".join(f"`{f['file']}` ({f['tests']} tests)" for f in found["files"]) + ".\n"
    data = {"runs_per_test": results.get("runs_per_test"), "tests": data_rows, "flaky": flaky,
            "always_fail": failing, "warnings": warnings}
    return md, data


# --- the command line -------------------------------------------------------------


def option(args: list[str], name: str, default: str | None = None) -> str | None:
    if name in args:
        i = args.index(name)
        if i + 1 < len(args):
            return args[i + 1]
    return default


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__, file=sys.stderr)
        return 0
    cmd, args = argv[0], argv[1:]
    if cmd == "find":
        diff_path, root = option(args, "--diff"), Path(option(args, "--root", "."))
        try:
            diff_text = Path(diff_path).read_text(errors="replace") if diff_path else ""
        except OSError:
            diff_text = ""
        result = find(diff_text, root)
        text = json.dumps(result, indent=2) + "\n"
        out = option(args, "--out")
        if out:
            Path(out).write_text(text)
        for test in result["tests"]:
            print(f"{test['change']:>7}  {test['target']}  {test['name']}  ({test['file']}:{test['line']})")
        for warning in result["warnings"]:
            print(f"warning: {warning}")
        return 0
    if cmd == "bins":
        text = Path(option(args, "--cargo-json")).read_text(errors="replace")
        found = bins(text)
        Path(option(args, "--out")).write_text(json.dumps(found, indent=2) + "\n")
        print(json.dumps(found, indent=2))
        return 0
    if cmd == "run":
        tests = json.loads(Path(option(args, "--tests")).read_text())["tests"]
        binaries = json.loads(Path(option(args, "--bins")).read_text())
        logs = Path(option(args, "--logs"))
        budget = option(args, "--budget-minutes")
        result = run(tests, binaries, int(option(args, "--runs", "20")), logs, float(option(args, "--timeout", "300")),
                     float(budget) * 60 if budget else None)
        (logs / "results.json").write_text(json.dumps(result, indent=2) + "\n")
        return 0
    if cmd == "report":
        try:
            results = json.loads(Path(option(args, "--results")).read_text())
        except (OSError, ValueError, TypeError):
            results = {"results": []}
        found = None
        if option(args, "--found"):
            try:
                found = json.loads(Path(option(args, "--found")).read_text())
            except (OSError, ValueError):
                found = None
        md, data = report(results, found)
        out = Path(option(args, "--out", "."))
        out.mkdir(parents=True, exist_ok=True)
        (out / "flaky.md").write_text(md)
        (out / "flaky.json").write_text(json.dumps(data, indent=2) + "\n")
        print(md)
        return 0
    print(__doc__, file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
