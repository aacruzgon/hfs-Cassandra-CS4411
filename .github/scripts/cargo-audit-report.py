#!/usr/bin/env python3
"""RustSec advisory report for Cargo.lock (report only; never fails on a
finding).

Runs `cargo audit --json` (the binary in $CARGO_AUDIT, else `cargo-audit` on
PATH) and lists every advisory it reports: vulnerabilities, and the
unmaintained, unsound and notice warnings. cargo-audit reads its config from
`.cargo/audit.toml`, not from the repository root's `audit.toml`, so that
file's `ignore` list does not apply here; instead each advisory it names is
marked "acknowledged". The JSON's top-level "warnings" list has one entry
naming every vulnerability `audit.toml` does not acknowledge; `ci-report.py
add` turns it into a ::warning:: annotation.

Usage: cargo-audit-report.py <out-dir>
Writes <out-dir>/cargo-audit.json and .md and prints the Markdown. Exits
non-zero only when cargo-audit cannot run or its output cannot be read.
"""

import json
import os
import re
import subprocess
import sys
import time


def acknowledged_ids(path="audit.toml"):
    """The RUSTSEC ids in audit.toml's [advisories] ignore list."""
    try:
        text = open(path).read()
    except OSError:
        return set()
    match = re.search(r"ignore\s*=\s*\[(.*?)\]", text, re.S)
    if not match:
        return set()
    body = "\n".join(line.split("#", 1)[0] for line in match.group(1).splitlines())
    return set(re.findall(r'"(RUSTSEC-\d{4}-\d{4})"', body))


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: cargo-audit-report.py <out-dir>")
    out_dir = sys.argv[1]
    os.makedirs(out_dir, exist_ok=True)
    binary = os.environ.get("CARGO_AUDIT") or "cargo-audit"
    started = time.monotonic()
    # `cargo-audit audit` is what `cargo audit` runs. Exit status 1 means
    # "found vulnerabilities", which is a figure here, not a failure.
    result = subprocess.run([binary, "audit", "--json"], capture_output=True, text=True)
    elapsed = round(time.monotonic() - started, 1)
    # The raw output, kept with the report so it can be re-read later.
    with open(os.path.join(out_dir, "cargo-audit-raw.json"), "w") as f:
        f.write(result.stdout)
    try:
        data = json.loads(result.stdout)
    except ValueError:
        sys.stderr.write(result.stderr)
        sys.exit(f"cargo-audit gave no JSON (exit {result.returncode})")
    version = subprocess.run([binary, "--version"], capture_output=True, text=True).stdout.strip()

    acknowledged = acknowledged_ids()
    entries = []
    seen = set()
    for item in data["vulnerabilities"]["list"]:
        entries.append(("vulnerability", item))
    for kind, items in sorted(data.get("warnings", {}).items()):
        for item in items:
            entries.append((kind, item))
    rows = []
    for kind, item in entries:
        advisory = item.get("advisory") or {}
        package = item["package"]
        key = (advisory.get("id"), package["name"], package["version"])
        if key in seen:
            continue  # the same advisory reached through two paths
        seen.add(key)
        rows.append(
            {
                "kind": kind,
                "id": advisory.get("id"),
                "package": package["name"],
                "version": package["version"],
                "title": advisory.get("title", ""),
                "patched": (item.get("versions") or {}).get("patched", []),
                "acknowledged_in_audit_toml": advisory.get("id") in acknowledged,
            }
        )

    counts = {}
    for r in rows:
        counts[r["kind"]] = counts.get(r["kind"], 0) + 1
    new_vulns = [r for r in rows if r["kind"] == "vulnerability" and not r["acknowledged_in_audit_toml"]]
    report = {
        "report": "cargo-audit",
        "cargo_audit": version,
        "advisory_db": data.get("database", {}),
        "lockfile_dependencies": data.get("lockfile", {}).get("dependency-count"),
        "elapsed_s": elapsed,
        "counts": counts,
        "unacknowledged_vulnerabilities": len(new_vulns),
        # One annotation for all of them, so a run page stays readable.
        "warnings": [
            f"cargo audit: {len(new_vulns)} vulnerabilities not acknowledged in audit.toml: "
            + "; ".join(f"{r['id']} in {r['package']} {r['version']}" for r in new_vulns)
        ]
        if new_vulns
        else [],
        "advisories": rows,
    }

    db = data.get("database", {})
    lines = [
        f"`{version}`, advisory database of {str(db.get('last-updated', '?'))[:10]} "
        f"({db.get('advisory-count', '?')} advisories), "
        f"{data.get('lockfile', {}).get('dependency-count', '?')} locked crates, {elapsed} s. "
        + (", ".join(f"{n} {k}" for k, n in sorted(counts.items())) or "No advisories")
        + f"; {len(new_vulns)} vulnerabilities not acknowledged in `audit.toml`.",
        "",
        "| Kind | Advisory | Crate | Locked | Patched | Acknowledged in `audit.toml` | Title |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    order = {"vulnerability": 0, "unsound": 1, "unmaintained": 2}
    for r in sorted(rows, key=lambda r: (order.get(r["kind"], 3), r["acknowledged_in_audit_toml"], r["id"] or "")):
        lines.append(
            f"| {r['kind']} | {r['id']} | `{r['package']}` | {r['version']} | "
            f"{', '.join(r['patched']) or 'none'} | {'yes' if r['acknowledged_in_audit_toml'] else 'no'} | "
            f"{r['title'].replace('|', '/')} |"
        )
    md = "\n".join(lines) + "\n"

    with open(os.path.join(out_dir, "cargo-audit.json"), "w") as f:
        json.dump(report, f, indent=2)
        f.write("\n")
    with open(os.path.join(out_dir, "cargo-audit.md"), "w") as f:
        f.write(md)
    print(md)


if __name__ == "__main__":
    main()
