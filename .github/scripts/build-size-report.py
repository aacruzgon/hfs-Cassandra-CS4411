#!/usr/bin/env python3
"""Artifact and cache size (report only; never fails the job).

- The persistent target directory: its size when the job began (before the
  "Reset the target dir if it is stale or over its cap" step), whether that
  step reset it and why, and its size now. The reset step records the first
  three in TARGET_DIR_BEFORE_BYTES, TARGET_DIR_RESET and
  TARGET_DIR_RESET_REASON; CARGO_TARGET_DIR is the directory.
- Optionally, the binaries the job built: each argument after the out-dir is a
  file, or `@<file>` naming a file of cargo's `--message-format=json` lines,
  whose test executables are taken.

Usage: build-size-report.py <out-dir> [<binary> | @<cargo-json>]...
Writes <out-dir>/build-size.json and .md and prints the Markdown. The JSON's
"warnings" list (a ::warning:: through `ci-report.py add`) says when the
directory ends the job over its cap, so the next run will build cold.
"""

import json
import os
import subprocess
import sys

GIB = 1024 ** 3
MIB = 1024 ** 2


def disk_usage(path):
    """Bytes `du` counts (disk usage, as the reset step's cap does)."""
    try:
        out = subprocess.run(
            ["du", "-s", "--block-size=1", path], capture_output=True, text=True, check=True
        ).stdout
        return int(out.split()[0])
    except (subprocess.CalledProcessError, ValueError, IndexError, FileNotFoundError):
        return None


def human(n):
    if n is None:
        return "unknown"
    return f"{n / GIB:.2f} GiB" if n >= GIB else f"{n / MIB:.1f} MiB"


def binaries(args):
    found = []
    for arg in args:
        if arg.startswith("@"):
            try:
                lines = open(arg[1:]).read().splitlines()
            except OSError:
                continue
            for line in lines:
                try:
                    msg = json.loads(line)
                except ValueError:
                    continue
                exe = msg.get("executable") if msg.get("reason") == "compiler-artifact" else None
                if exe:
                    found.append((msg["target"]["name"], exe))
        else:
            found.append((os.path.basename(arg), arg))
    seen, result = set(), []
    for name, path in found:
        if path in seen:
            continue
        seen.add(path)
        size = os.path.getsize(path) if os.path.exists(path) else None
        result.append({"name": name, "path": path, "bytes": size})
    return result


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: build-size-report.py <out-dir> [<binary> | @<cargo-json>]...")
    out_dir = sys.argv[1]
    env = os.environ
    target = env.get("CARGO_TARGET_DIR")
    before = env.get("TARGET_DIR_BEFORE_BYTES")
    before = int(before) if before and before.isdigit() else None
    after = disk_usage(target) if target else None
    bins = binaries(sys.argv[2:])
    cap = env.get("TARGET_DIR_CAP_GB")
    warnings = []
    if after is not None and cap and cap.isdigit() and after > int(cap) * GIB:
        warnings.append(
            f"target directory {target} is {human(after)}, over its {cap} GB cap: "
            "the next run on this runner resets it and builds cold"
        )
    report = {
        "report": "build-size",
        "target_dir": target,
        "target_dir_before_bytes": before,
        "target_dir_after_bytes": after,
        "target_dir_reset": env.get("TARGET_DIR_RESET"),
        "target_dir_reset_reason": env.get("TARGET_DIR_RESET_REASON") or None,
        "target_dir_added_bytes": (
            after - (0 if env.get("TARGET_DIR_RESET") == "yes" else before)
            if after is not None and (before is not None or env.get("TARGET_DIR_RESET") == "yes")
            else None
        ),
        "target_dir_cap_gb": env.get("TARGET_DIR_CAP_GB"),
        "binaries": bins,
        "warnings": warnings,
    }

    reset = env.get("TARGET_DIR_RESET", "unknown")
    reason = env.get("TARGET_DIR_RESET_REASON")
    # After a reset the job built into an empty directory, so the job's own
    # growth is measured from zero, not from the size the reset removed.
    baseline = 0 if reset == "yes" else before
    if baseline is not None and after is not None:
        grew = after - baseline
        delta = f"; this job {'added' if grew >= 0 else 'removed'} {human(abs(grew))}"
    else:
        delta = ""
    lines = [
        f"Target directory `{target}` (cap {env.get('TARGET_DIR_CAP_GB', '?')} GB): "
        f"{human(before)} when the job began, {human(after)} now{delta}. "
        f"Reset this run: **{reset}**" + (f" ({reason}; the job built from empty)" if reset == "yes" else "") + ".",
    ]
    if bins:
        lines += ["", "| Binary | Size | Path |", "| --- | ---: | --- |"]
        for b in bins:
            lines.append(f"| `{b['name']}` | {human(b['bytes'])} | `{b['path']}` |")
    md = "\n".join(lines) + "\n"

    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "build-size.json"), "w") as f:
        json.dump(report, f, indent=2)
        f.write("\n")
    with open(os.path.join(out_dir, "build-size.md"), "w") as f:
        f.write(md)
    print(md)


if __name__ == "__main__":
    main()
