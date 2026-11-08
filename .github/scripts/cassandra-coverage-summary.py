#!/usr/bin/env python3
"""Summarise a `cargo llvm-cov report --json` export for the Cassandra backend.

Used by .github/workflows/cassandra-coverage.yml; also runs locally. Standard
library only, so it needs nothing installed on the runner.

    cassandra-coverage-summary.py EXPORT_JSON OUT_MARKDOWN OUT_JSON

It reports, for every file under crates/persistence/src/backends/cassandra/
and for crates/hfs/src/main.rs: lines, regions and functions, covered and
total, as llvm-cov counts them. The backend's total sums the backend's files.
main.rs is mostly not Cassandra code, so it also gets a row of its own for
the Cassandra startup path: the regions and functions of every function in
main.rs whose symbol mentions `cassandra`, that is start_cassandra and its
helpers with their closures and async bodies, and the CassandraBackend
instances of the generic startup functions (provisioned_tenants,
seed_conformance_resources). A function counts once per symbol, as llvm-cov
counts them.

The workflow hands both outputs to `.github/scripts/ci-report.py add`, which
adds the section title, the job's context (commit, runner, toolchain,
Cassandra version, ...) and the step summary; the build's time and memory
come from `ci-report.py build`. This script adds only what is particular to
coverage, from the environment variables in RUN_KEYS (null when unset).
Nothing here fails on a low figure: the report has no threshold.
"""

import json
import os
import re
import sys

BACKEND_DIR = "crates/persistence/src/backends/cassandra/"
MAIN_RS = "crates/hfs/src/main.rs"

# Environment variable -> key in the JSON's "run".
RUN_KEYS = {
    "COVERAGE_LLVM_COV_VERSION": "cargo_llvm_cov",
    "COVERAGE_MEASURED": "measured",
    "COVERAGE_MILESTONE_PROFILES": "milestone_a_profile_files",
}


def repo_path(filename):
    """The path relative to the repository, or None if outside the scope."""
    filename = filename.replace("\\", "/")
    for marker in (BACKEND_DIR, MAIN_RS):
        at = filename.find(marker)
        if at >= 0:
            return filename[at:]
    return None


def counts(summary, kind):
    entry = summary.get(kind, {})
    return entry.get("covered", 0), entry.get("count", 0)


def pct(covered, total):
    return None if total == 0 else round(100.0 * covered / total, 2)


def fmt_cell(covered, total):
    p = pct(covered, total)
    return "–" if p is None else f"{p:.1f}% ({covered}/{total})"


def readable(name):
    """A short readable name for a main.rs function symbol, for the report.

    rustc emits v0 symbols (`_RNCNvCs…_3hfs15start_cassandra0B3_`) and older
    toolchains legacy ones (`_ZN3hfs15start_cassandra…17h…E`). This is not a
    demangler: it keeps the function's own name, marks a generic instance and
    counts the closures (async bodies) around it, which is all the report
    needs. A name it cannot read is returned as is.
    """
    # The crate root: v0 `C[s<base-62>_]3hfs`, legacy `_ZN3hfs`.
    at = re.search(r"(?:C(?:s[0-9A-Za-z]*_)?|_ZN)3hfs(\d+)", name)
    if not at:
        return name
    start = at.end()
    ident = name[start : start + int(at.group(1))]
    if name.startswith("_R"):
        prefix = re.match(r"_R((?:NC|I)*)", name).group(1)
        closures, generic = prefix.count("NC"), "I" in prefix
    else:
        closures = name.count("closure")
        generic = False
    text = f"hfs::{ident}"
    if generic:
        text += "<CassandraBackend>" if "CassandraBackend" in name else "<…>"
    return text + "::{closure}" * closures


def cassandra_path_in_main(functions):
    """Regions and functions of main.rs's functions whose name has `cassandra`.

    A function is listed once per instantiation; a region is counted once per
    span, covered if any instantiation ran it. Only code regions (kind 0).
    """
    regions, hit, first_line, recorded = {}, {}, {}, False
    for fn in functions:
        name = fn.get("name", "")
        files = [repo_path(f) for f in fn.get("filenames", [])]
        if "cassandra" not in name or not files or files[0] != MAIN_RS:
            continue
        hit[name] = hit.get(name, False) or fn.get("count", 0) > 0
        if "start_cassandra" in name and fn.get("count", 0) > 0:
            recorded = True
        code = [r[0] for r in fn.get("regions", []) if (r[7] if len(r) > 7 else 0) == 0]
        if code:
            first_line[name] = min(code + [first_line.get(name, code[0])])
        for region in fn.get("regions", []):
            line_start, col_start, line_end, col_end, count, file_id = region[:6]
            kind = region[7] if len(region) > 7 else 0
            if kind != 0 or files[file_id] != MAIN_RS:
                continue
            span = (line_start, col_start, line_end, col_end)
            regions[span] = regions.get(span, 0) or count
    return {
        "regions": {"covered": sum(1 for c in regions.values() if c > 0), "count": len(regions)},
        "functions": {"covered": sum(1 for v in hit.values() if v), "count": len(hit)},
        "hfs_coverage_recorded": recorded,
        "symbols": sorted(
            ({"symbol": k, "name": readable(k), "line": first_line.get(k), "ran": v} for k, v in hit.items()),
            key=lambda f: (f["line"] or 0, f["name"]),
        ),
    }


def main(argv):
    if len(argv) != 4:
        sys.exit(__doc__)
    export_path, md_path, json_path = argv[1:]
    with open(export_path, encoding="utf-8") as fh:
        export = json.load(fh)

    rows, totals = [], {"lines": [0, 0], "regions": [0, 0], "functions": [0, 0]}
    main_rs_functions = []
    for data in export.get("data", []):
        for entry in data.get("files", []):
            path = repo_path(entry.get("filename", ""))
            if path is None:
                continue
            summary = entry.get("summary", {})
            row = {"file": path}
            for kind in totals:
                covered, total = counts(summary, kind)
                row[kind] = {"covered": covered, "count": total, "percent": pct(covered, total)}
                if path.startswith(BACKEND_DIR):
                    totals[kind][0] += covered
                    totals[kind][1] += total
            rows.append(row)
        main_rs_functions.extend(data.get("functions", []))
    rows.sort(key=lambda r: (r["file"] == MAIN_RS, r["file"]))

    backend_total = {
        kind: {"covered": c, "count": t, "percent": pct(c, t)} for kind, (c, t) in totals.items()
    }
    path_in_main = cassandra_path_in_main(main_rs_functions)
    for kind in ("regions", "functions"):
        entry = path_in_main[kind]
        entry["percent"] = pct(entry["covered"], entry["count"])

    run = {key: os.environ.get(var) or None for var, key in RUN_KEYS.items()}
    report = {
        "run": run,
        "backend_total": backend_total,
        "files": rows,
        "hfs_main_cassandra_path": path_in_main,
    }
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
        fh.write("\n")

    out = [
        "Line, region and function coverage as `cargo llvm-cov` counts it "
        f"({run['cargo_llvm_cov'] or 'cargo-llvm-cov'}), debug build, single node, "
        f"no threshold. Measured: {run['measured'] or 'see the workflow'}."
    ]
    out.append("")
    out.append("| File | Lines | Regions | Functions |")
    out.append("| --- | --- | --- | --- |")
    for row in rows:
        cells = [fmt_cell(row[k]["covered"], row[k]["count"]) for k in ("lines", "regions", "functions")]
        name = row["file"].removeprefix(BACKEND_DIR) if row["file"].startswith(BACKEND_DIR) else row["file"]
        out.append(f"| `{name}` | " + " | ".join(cells) + " |")
    cells = [fmt_cell(*totals[k]) for k in ("lines", "regions", "functions")]
    out.append(f"| **Backend total** (`{BACKEND_DIR}`) | " + " | ".join(f"**{c}**" for c in cells) + " |")
    pr, pf = path_in_main["regions"], path_in_main["functions"]
    out.append(
        f"| `main.rs`, Cassandra path only | – | {fmt_cell(pr['covered'], pr['count'])} "
        f"| {fmt_cell(pf['covered'], pf['count'])} |"
    )
    out.append("")
    out.append(
        "Coverage of the `hfs` processes the Milestone A tests start (`start_cassandra` ran): "
        + ("**recorded**." if path_in_main["hfs_coverage_recorded"] else "**not recorded**: the `main.rs` figures are not a measurement.")
    )
    if run.get("milestone_a_profile_files") is not None:
        out[-1] += (f" Profile files the three Milestone A steps added: {run['milestone_a_profile_files']}"
                    " (one per test binary run plus one per `hfs` started).")
    out.append("")
    not_run = [f for f in path_in_main["symbols"] if not f["ran"]]
    if not_run:
        out.append("Cassandra-path functions in `main.rs` that never ran: "
                   + ", ".join(f"`{f['name']}` (line {f['line']})" for f in not_run) + ".")
        out.append("")
    with open(md_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(out))


if __name__ == "__main__":
    main(sys.argv)
