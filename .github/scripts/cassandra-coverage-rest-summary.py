#!/usr/bin/env python3
"""Summarise the coverage of crates/rest/src that the Cassandra tests reach.

Used by .github/workflows/cassandra-coverage.yml; also runs locally. Standard
library only, so it needs nothing installed on the runner.

    cassandra-coverage-rest-summary.py EXPORT_JSON WITHOUT_STEP3_JSON OUT_MARKDOWN OUT_JSON [SOURCE_ROOT]

EXPORT_JSON is a `cargo llvm-cov report --json` export of the job's
profiles, limited by its ignore regex to crates/rest/src. WITHOUT_STEP3_JSON
is the same export with the profiles of the "Step 3 CRUD through hfs" step
set aside, or `-` when there is none (the step did not run, or that export
failed); the section then says so. SOURCE_ROOT is the checkout (default: the
current directory), read to name functions.

The section is separate from the backend's (cassandra-coverage-summary.py),
whose scope and figures it does not touch. It reports:

- lines, regions and functions of crates/rest/src, covered and total, with
  every test and without step 3's profiles;
- the files whose line coverage step 3 changes;
- named REST paths that Step 3's REST test exercises (WATCH), with how often
  they ran with and without step 3's profiles;
- how many functions only step 3 reaches, and the first of them.

A function is the `fn` item whose span holds the first line of its code in
the source: closures and async bodies fold into their item, and every
instantiation of a generic one counts as one function, covered if any of
them ran. Its count is the sum of theirs. Two items of one name in one file
(the `fn from` of each `From` impl) stay two functions. An item spans from
its `fn` line to its closing `}` alone at the item's own indentation, as
rustfmt writes it, or to the end of its line for a one-line body. Code
outside every item, such as a static's initializer closure, counts in lines
and regions but not as a function. llvm-cov's own function figures count
each instantiation; the totals here keep llvm-cov's lines and regions and
use the per-item function count. Nothing here fails on a low figure: the
report has no threshold.
"""

import json
import os
import re
import sys

REST_DIR = "crates/rest/src/"

# Named REST paths that Step 3's REST test reaches: (label, file under
# crates/rest/src/, function). A function missing from the export shows as
# missing rather than as zero.
WATCH = [
    ("Batch entry `ifMatch` check", "handlers/batch.rs", "check_entry_if_match"),
    ("Batch entry", "handlers/batch.rs", "process_batch_entry"),
    ("PATCH", "handlers/patch.rs", "patch_handler"),
    ("PUT (update, revive)", "handlers/update.rs", "update_handler"),
    ("DELETE", "handlers/delete.rs", "delete_handler"),
    ("HEAD", "handlers/read.rs", "head_read_handler"),
]

# How many of the functions only step 3 reaches are listed by name.
LIST_LIMIT = 25

FN_ITEM = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:const\s+)?(?:async\s+)?(?:unsafe\s+)?(?:extern\s+\"[^\"]*\"\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)")


def repo_path(filename):
    """The path relative to crates/rest/src/, or None if outside it."""
    filename = filename.replace("\\", "/")
    at = filename.find(REST_DIR)
    return None if at < 0 else filename[at + len(REST_DIR):]


def item_end(lines, start):
    """The last line (1-based) of the `fn` item starting at `start`.

    A declaration without a body ends at its `;`; a one-line body at the line
    its braces close on; any other body at its closing `}` alone at the
    item's own indentation (rustfmt). Without one, the file's end."""
    indent = lines[start - 1][: len(lines[start - 1]) - len(lines[start - 1].lstrip())]
    opened = False
    for number in range(start, len(lines) + 1):
        text = lines[number - 1].rstrip()
        if not opened:
            if "{" in text:
                opened = True
                body = text[text.index("{"):]
                if body.endswith("}") and body.count("{") == body.count("}"):
                    return number
            elif text.endswith(";"):
                return number
        elif text == indent + "}":
            return number
    return len(lines)


class Sources:
    """The `fn` items of each crates/rest/src file, read once: (start, end, name)."""

    def __init__(self, root):
        self.root, self.items = root, {}

    def spans(self, rel):
        if rel not in self.items:
            items = []
            try:
                with open(os.path.join(self.root, REST_DIR, rel), encoding="utf-8") as fh:
                    lines = fh.read().split("\n")
            except OSError:
                lines = []
            for number, text in enumerate(lines, 1):
                m = FN_ITEM.match(text)
                if m:
                    items.append((number, item_end(lines, number), m.group(1)))
            self.items[rel] = items
        return self.items[rel]

    def item(self, rel, line):
        """(start line, name) of the innermost `fn` item whose span holds
        `line` (1-based), or None when no item does."""
        found = None
        for start, end, name in self.spans(rel):
            if start > line:
                break
            if end >= line:
                found = (start, name)
        return found

    def enclosing(self, rel, line):
        """The name of the `fn` item that holds `line`, or None."""
        found = self.item(rel, line)
        return found and found[1]


def first_code_line(fn):
    lines = [r[0] for r in fn.get("regions", []) if (r[7] if len(r) > 7 else 0) == 0]
    return min(lines) if lines else None


def load(path, sources):
    """Per-file summaries and per-function counts of an export."""
    with open(path, encoding="utf-8") as fh:
        export = json.load(fh)
    files, functions, names = {}, {}, {}
    for data in export.get("data", []):
        for entry in data.get("files", []):
            rel = repo_path(entry.get("filename", ""))
            if rel is not None:
                files[rel] = entry.get("summary", {})
        for fn in data.get("functions", []):
            paths = [repo_path(f) for f in fn.get("filenames", [])]
            line = first_code_line(fn)
            if not paths or paths[0] is None or line is None:
                continue
            found = sources.item(paths[0], line)
            if found is None:
                continue
            key = (paths[0], found[0])
            names[key] = found[1]
            functions[key] = functions.get(key, 0) + fn.get("count", 0)
    return {"files": files, "functions": functions, "names": names}


def calls_named(loaded, file, name):
    """The summed count of the items called `name` in `file`, or None if none is in the export."""
    counts = [c for k, c in loaded["functions"].items() if k[0] == file and loaded["names"][k] == name]
    return sum(counts) if counts else None


def pct(covered, total):
    return None if total == 0 else round(100.0 * covered / total, 2)


def fmt_cell(covered, total):
    p = pct(covered, total)
    return "–" if p is None else f"{p:.1f}% ({covered}/{total})"


def totals(loaded):
    out = {}
    for kind in ("lines", "regions"):
        covered = sum(s.get(kind, {}).get("covered", 0) for s in loaded["files"].values())
        count = sum(s.get(kind, {}).get("count", 0) for s in loaded["files"].values())
        out[kind] = {"covered": covered, "count": count, "percent": pct(covered, count)}
    fns = loaded["functions"]
    covered = sum(1 for c in fns.values() if c > 0)
    out["functions"] = {"covered": covered, "count": len(fns), "percent": pct(covered, len(fns))}
    return out


def summarise(with_all, without):
    report = {"total": totals(with_all), "without_step3": None, "files_step3_changes": [],
              "watch": [], "only_step3": None}
    if without is not None:
        report["without_step3"] = totals(without)
        changes = []
        for name, summary in sorted(with_all["files"].items()):
            after = summary.get("lines", {}).get("covered", 0)
            before = without["files"].get(name, {}).get("lines", {}).get("covered", 0)
            if after != before:
                changes.append({"file": name, "lines_without_step3": before, "lines": after,
                                "lines_count": summary.get("lines", {}).get("count", 0)})
        changes.sort(key=lambda r: (-(r["lines"] - r["lines_without_step3"]), r["file"]))
        report["files_step3_changes"] = changes
        only = sorted(k for k, c in with_all["functions"].items() if c > 0 and without["functions"].get(k, 0) == 0)
        report["only_step3"] = {"count": len(only), "functions": [
            {"file": f, "function": with_all["names"][(f, line)], "line": line,
             "calls": with_all["functions"][(f, line)]} for f, line in only]}
    for label, file, name in WATCH:
        calls = calls_named(with_all, file, name)
        present = calls is not None
        report["watch"].append({
            "label": label, "file": file, "function": name, "present": present,
            "calls": calls,
            "calls_without_step3": (calls_named(without, file, name) or 0) if (present and without is not None) else None,
        })
    return report


def markdown(report, run):
    t, b = report["total"], report["without_step3"]
    row = lambda x: " | ".join(fmt_cell(x[k]["covered"], x[k]["count"]) for k in ("lines", "regions", "functions"))
    out = [
        "Coverage of `crates/rest/src` from the same profiles as the backend's section, as "
        f"`cargo llvm-cov` counts it ({run.get('cargo_llvm_cov') or 'cargo-llvm-cov'}). REST runs in the "
        "`hfs` processes the Milestone A tests start: the round trip, Step 3 CRUD through REST and "
        "readiness. A section of its own: the backend's scope and figures do not change. No threshold.",
        "",
        "| `crates/rest/src` | Lines | Regions | Functions |",
        "| --- | --- | --- | --- |",
        f"| Every test | {row(t)} |",
        (f"| Without step 3's profiles | {row(b)} |" if b is not None
         else "| Without step 3's profiles | not available: the step did not run, or that export failed | – | – |"),
        "",
        "A function is the `fn` item its code is in: closures, async bodies and generic instances fold into it, "
        "and its count sums their entry counts (an async body counts once per poll). Two items of one name in one "
        "file count as two; code outside every `fn` item, such as a static's initializer, counts in lines and "
        "regions only.",
        "",
        "| REST path | Function | Count | Without step 3 |",
        "| --- | --- | --- | --- |",
    ]
    for w in report["watch"]:
        where = f"`{w['file']}`: `{w['function']}`"
        if not w["present"]:
            out.append(f"| {w['label']} | {where} | not in the export | – |")
        else:
            without = "–" if w["calls_without_step3"] is None else str(w["calls_without_step3"])
            out.append(f"| {w['label']} | {where} | {w['calls']} | {without} |")
    out.append("")
    if b is not None:
        if report["files_step3_changes"]:
            out += ["Files whose line coverage step 3 changes:", "",
                    "| File | Lines without step 3 | Lines | Of |", "| --- | --- | --- | --- |"]
            out += [f"| `{c['file']}` | {c['lines_without_step3']} | {c['lines']} | {c['lines_count']} |"
                    for c in report["files_step3_changes"]]
            out.append("")
        only = report["only_step3"]
        line = f"Functions only step 3 reaches: **{only['count']}**"
        if only["functions"]:
            listed = only["functions"][:LIST_LIMIT]
            line += ": " + ", ".join(f"`{f['file']}:{f['line']}`: `{f['function']}` ({f['calls']})" for f in listed)
            line += ", …" if only["count"] > LIST_LIMIT else ""
        out += [line + ".", ""]
    return "\n".join(out)


def main(argv):
    if len(argv) not in (5, 6):
        sys.exit(__doc__)
    export_path, without_path, md_path, json_path = argv[1:5]
    sources = Sources(argv[5] if len(argv) == 6 else ".")
    with_all = load(export_path, sources)
    without = None
    if without_path != "-":
        try:
            without = load(without_path, sources)
        except (OSError, ValueError):
            without = None
    run = {"cargo_llvm_cov": os.environ.get("COVERAGE_LLVM_COV_VERSION") or None}
    report = summarise(with_all, without)
    report["run"] = run
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
        fh.write("\n")
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write(markdown(report, run))


if __name__ == "__main__":
    main(sys.argv)
