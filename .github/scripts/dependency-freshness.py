#!/usr/bin/env python3
"""Dependency freshness report: the locked version of each chosen crate against
the newest release on crates.io (report only; it never fails on a figure).

The CI half of the rule "check a dependency's newer releases before working
around its bug" (roadmap Step 2, ADR draft "more CI reports"). The crates are:
  - every registry dependency `helios-hfs` declares (normal and dev);
  - every dependency `helios-persistence`'s `cassandra` feature enables;
  - EXTRA below: crates the Cassandra backend's behaviour depends on that are
    not direct dependencies of either, or that matter enough to always list.

The locked version comes from `cargo metadata --locked --all-features`, the
exact version the workspace resolves for that dependency of that package.

crates.io etiquette (https://crates.io/data-access, "Crawlers"): one request
at a time, at most one per second, with a User-Agent that names the client
and how to reach its owner.

Usage: dependency-freshness.py <out-dir>
Writes <out-dir>/dependency-freshness.json and .md and prints the Markdown.
The JSON's top-level "warnings" list holds at most two entries, one naming
every crate a major (semver-incompatible) version behind and one naming every
yanked locked version; `ci-report.py add` turns each into a ::warning::.
Exits non-zero only when the measurement itself cannot run (no cargo
metadata); an unreachable crates.io is reported, not failed.
"""

import http.client
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

USER_AGENT = (
    "hfs-Cassandra-CS4411-ci-dependency-freshness "
    "(+https://github.com/aacruzgon/hfs-Cassandra-CS4411)"
)
EXTRA = ["cassandra-protocol", "cdrs-tokio", "tokio", "uuid", "testcontainers"]
# Whose transitive dependency each EXTRA crate may be, when it is not a
# direct dependency of helios-hfs or helios-persistence.
EXTRA_PARENTS = ["helios-persistence", "helios-hfs", "cdrs-tokio"]
REQUEST_INTERVAL_S = 1.0


def parse(version):
    """(major, minor, patch, is_prerelease) of a semver string."""
    core, _, pre = version.partition("+")[0].partition("-")
    parts = [int(p) for p in core.split(".")[:3]]
    while len(parts) < 3:
        parts.append(0)
    return parts[0], parts[1], parts[2], bool(pre)


def compat_key(version):
    """Cargo's caret compatibility class: 1.x.y -> (1,), 0.3.y -> (0, 3),
    0.0.4 -> (0, 0, 4)."""
    major, minor, patch, _ = parse(version)
    if major > 0:
        return (major,)
    if minor > 0:
        return (0, minor)
    return (0, 0, patch)


def metadata():
    out = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--locked", "--all-features"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return json.loads(out)


def chosen_crates(meta):
    """{crate name: (locked version, [why listed])}."""
    packages = {p["id"]: p for p in meta["packages"]}
    by_name = {}
    for p in meta["packages"]:
        by_name.setdefault(p["name"], []).append(p)
    nodes = {n["id"]: n for n in meta["resolve"]["nodes"]}

    def resolved_deps(pkg_name):
        """{dependency crate name: version} as resolved for pkg_name."""
        result = {}
        for pkg in by_name.get(pkg_name, []):
            node = nodes.get(pkg["id"])
            if not node:
                continue
            for dep in node["deps"]:
                dep_pkg = packages[dep["pkg"]]
                result[dep_pkg["name"]] = dep_pkg["version"]
        return result

    def registry_package(name):
        return all(
            p.get("source") and p["source"].startswith("registry+")
            for p in by_name.get(name, [])
        ) and name in by_name

    chosen = {}

    def add(name, version, why):
        entry = chosen.setdefault(name, [version, []])
        if why not in entry[1]:
            entry[1].append(why)

    hfs = by_name["helios-hfs"][0]
    hfs_resolved = resolved_deps("helios-hfs")
    for dep in hfs["dependencies"]:
        name = dep["name"]
        if dep.get("source", "") and dep["source"].startswith("registry+") and name in hfs_resolved:
            kind = dep.get("kind") or "normal"
            add(name, hfs_resolved[name], f"helios-hfs ({kind})")

    persistence = by_name["helios-persistence"][0]
    p_resolved = resolved_deps("helios-persistence")
    for entry in persistence["features"].get("cassandra", []):
        if entry.startswith("dep:"):
            name = entry[4:]
            if name in p_resolved:
                add(name, p_resolved[name], "helios-persistence `cassandra` feature")

    for name in EXTRA:
        if name in chosen:
            continue
        for parent in EXTRA_PARENTS:
            resolved = resolved_deps(parent)
            if name in resolved and registry_package(name):
                add(name, resolved[name], f"listed always (via {parent})")
                break
    return {k: (v[0], v[1]) for k, v in sorted(chosen.items())}


def crates_io(name):
    request = urllib.request.Request(
        f"https://crates.io/api/v1/crates/{name}",
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)


def main():
    if len(sys.argv) != 2:
        sys.exit("usage: dependency-freshness.py <out-dir>")
    out_dir = sys.argv[1]
    os.makedirs(out_dir, exist_ok=True)
    started = time.monotonic()

    crates = chosen_crates(metadata())
    rows = []
    warnings = []  # one entry per crate a major version behind
    yanked_locks = []
    last_request = 0.0
    for name, (locked, why) in crates.items():
        wait = REQUEST_INTERVAL_S - (time.monotonic() - last_request)
        if wait > 0:
            time.sleep(wait)
        last_request = time.monotonic()
        row = {"crate": name, "locked": locked, "listed_because": why}
        # OSError covers URLError, socket.timeout (not a TimeoutError before
        # Python 3.10) and a reset connection; HTTPException an incomplete
        # read; KeyError and TypeError a response without the fields read.
        try:
            data = crates_io(name)
            crate = data["crate"]
            latest = crate.get("max_stable_version") or crate["max_version"]
            dates = {v["num"]: v["created_at"][:10] for v in data["versions"]}
            yanked = {v["num"] for v in data["versions"] if v.get("yanked")}
        except (OSError, ValueError, KeyError, TypeError, http.client.HTTPException) as e:
            row["error"] = f"crates.io: {type(e).__name__}: {e}"
            rows.append(row)
            continue
        if parse(latest)[:3] <= parse(locked)[:3]:
            gap = "current"
        elif compat_key(latest) == compat_key(locked):
            gap = "compatible"
        else:
            gap = "major"
            warnings.append(f"{name} {locked} → {latest} ({dates.get(latest, '?')})")
        row.update(
            {
                "latest": latest,
                "gap": gap,
                "locked_released": dates.get(locked),
                "latest_released": dates.get(latest),
                "locked_yanked": locked in yanked,
            }
        )
        if locked in yanked:
            yanked_locks.append(f"{name} {locked}")
        rows.append(row)

    elapsed = round(time.monotonic() - started, 1)
    # One annotation per kind, not one per crate, so a run page stays readable.
    summary_warnings = []
    if warnings:
        summary_warnings.append(
            f"{len(warnings)} dependencies a major (semver-incompatible) version behind "
            f"crates.io: {'; '.join(warnings)}. Check their changelogs before working "
            "around a bug in one."
        )
    if yanked_locks:
        summary_warnings.append(f"Cargo.lock holds versions crates.io has yanked: {'; '.join(yanked_locks)}")
    counts = {g: sum(1 for r in rows if r.get("gap") == g) for g in ("current", "compatible", "major")}
    counts["unreachable"] = sum(1 for r in rows if "error" in r)
    report = {
        "report": "dependency-freshness",
        "source": "cargo metadata --locked --all-features; https://crates.io/api/v1/crates/<name>",
        "checked_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "elapsed_s": elapsed,
        "counts": counts,
        "warnings": summary_warnings,
        "crates": rows,
    }

    order = {"major": 0, "compatible": 1, "current": 2}
    lines = [
        f"{len(rows)} crates: {counts['major']} a major (semver-incompatible) version behind, "
        f"{counts['compatible']} behind within their semver range (`cargo update` reaches them), "
        f"{counts['current']} current"
        + (f", {counts['unreachable']} not reached" if counts["unreachable"] else "")
        + f". Checked against crates.io in {elapsed} s.",
        "",
        "| Crate | Locked | Released | Latest | Released | Gap | Listed because |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for r in sorted(rows, key=lambda r: (order.get(r.get("gap"), -1), r["crate"])):
        if "error" in r:
            lines.append(f"| `{r['crate']}` | {r['locked']} | | | | {r['error']} | {'; '.join(r['listed_because'])} |")
            continue
        gap = {"major": "**major**", "compatible": "compatible", "current": "current"}[r["gap"]]
        if r["locked_yanked"]:
            gap += ", locked version yanked"
        lines.append(
            f"| `{r['crate']}` | {r['locked']} | {r['locked_released'] or '?'} | {r['latest']} | "
            f"{r['latest_released'] or '?'} | {gap} | {'; '.join(r['listed_because'])} |"
        )
    md = "\n".join(lines) + "\n"

    with open(os.path.join(out_dir, "dependency-freshness.json"), "w") as f:
        json.dump(report, f, indent=2)
        f.write("\n")
    with open(os.path.join(out_dir, "dependency-freshness.md"), "w") as f:
        f.write(md)
    print(md)


if __name__ == "__main__":
    main()
