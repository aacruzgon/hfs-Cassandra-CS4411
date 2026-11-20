#!/usr/bin/env python3
"""Let cargo-mutants see through a `Result` type alias (report only).

cargo-mutants 27.1.0 knows a function returns a `Result` only when the last
segment of its return type is the identifier `Result` (`path_ends_with(path,
"Result")` in src/fnvalue.rs). Then it replaces the body with `Ok(...)` of each
value it can make for the `Ok` type. The backend's functions mostly return
`StorageResult<T>`, an alias for `Result<T, StorageError>`
(crates/persistence/src/error.rs), which the tool reads as an unknown generic
type: it tries `StorageResult::new()`, `::new(v)`, `::from(v)` and
`::from_iter([v])`, and rustc rejects every one (E0599, E0271, E0277, E0283),
so those mutants are unviable and test nothing (ADR-aacruzgon-0084).

This script runs in the mutation job's own checkout, never in a commit. For
each alias given as `--alias NAME=ERROR`, it rewrites every return type
`-> NAME<T>` (or `-> some::path::NAME<T>`) in the .rs files under the given
directories into `-> ::core::result::Result<T, ERROR>`, the same type spelled
so that cargo-mutants recognises it. Comments, strings and char literals are
left alone, and the rewrite never adds or removes a line, so every file:line
in the report is still the line the pull request shows.

cargo-mutants checks that the new side of an `--in-diff` diff matches the
tree (in_diff.rs, `check_diff_new_text_matches`), so with `--diff FILE` the
script also replaces each new-side line of that diff (context and added
lines) with the rewritten file's line of the same number. The removed lines
and the hunk headers stay as they were.

Usage: mutants-result-alias.py --alias NAME=ERROR [--alias ...] [--diff FILE]
                               [--summary FILE] <dir>...

Prints what it rewrote; `--summary` writes the same as JSON. Exits 1 if it
could not rewrite a file it had started on (an unbalanced `<`), having
changed nothing, so the job can carry on with the tree as it was. Standard
library only; runs with `python3 -I`. Tests: test_mutants_report.py.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

RESULT = "::core::result::Result"
IDENT = r"[A-Za-z_][A-Za-z0-9_]*"


class Unbalanced(ValueError):
    """A `NAME<` with no matching `>` in the code that follows it."""


def code_mask(text: str) -> list[bool]:
    """For each character, whether it is code: not in a comment, a string or a char literal."""
    mask = [True] * len(text)
    i, n = 0, len(text)

    def blank(start: int, end: int) -> None:
        for k in range(start, min(end, n)):
            mask[k] = False

    while i < n:
        c = text[i]
        if text.startswith("//", i):
            end = text.find("\n", i)
            end = n if end < 0 else end
            blank(i, end)
            i = end
        elif text.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if text.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif text.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            blank(i, j)
            i = j
        elif (raw := re.match(r'(?:b|c)?r(#*)"', text[i:i + 300])) and (i == 0 or not re.match(r"\w", text[i - 1])):
            close = '"' + raw.group(1)
            end = text.find(close, i + raw.end())
            end = n if end < 0 else end + len(close)
            blank(i, end)
            i = end
        elif c == '"':
            j = i + 1
            while j < n and text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            blank(i, j + 1)
            i = j + 1
        elif c == "'":
            # A char literal ('x', '\n', '\u{1F600}', '\''), not a lifetime ('a).
            lit = re.match(r"'(?:\\(?:u\{[0-9A-Fa-f]{1,6}\}|x[0-9A-Fa-f]{2}|.)|[^\\'\n])'", text[i:i + 12])
            if lit:
                blank(i, i + lit.end())
                i += lit.end()
            else:
                i += 1
        else:
            i += 1
    return mask


def closing_angle(text: str, mask: list[bool], open_at: int) -> int:
    """The index of the `>` that closes the `<` at open_at, counting only code; `->` and `=>` are not brackets."""
    depth = 0
    for k in range(open_at, len(text)):
        if not mask[k]:
            continue
        ch = text[k]
        if ch == "<":
            depth += 1
        elif ch == ">" and text[k - 1] not in "-=":
            depth -= 1
            if depth == 0:
                return k
        elif ch in ";{" and depth:
            break
    raise Unbalanced(f"no closing > for the < at offset {open_at}")


def rewrite_source(text: str, aliases: dict[str, str]) -> tuple[str, int]:
    """The text with each `-> ALIAS<T>` return type spelled `-> ::core::result::Result<T, ERROR>`, and how many."""
    names = "|".join(re.escape(a) for a in sorted(aliases, key=len, reverse=True))
    pattern = re.compile(rf"->\s*(?P<path>(?:(?:{IDENT})?::)?(?:{IDENT}::)*(?P<alias>{names}))\s*<")
    mask = code_mask(text)
    found = [m for m in pattern.finditer(text) if mask[m.start()]]
    # Every edit is found on the original text, then applied from the last
    # offset to the first, so each offset still holds, nested aliases too.
    edits: list[tuple[int, int, str]] = []
    for m in found:
        open_at = m.end() - 1
        close_at = closing_angle(text, mask, open_at)
        # After a trailing comma (rustfmt's multi-line style) the error type needs none of its own.
        sep = " " if text[open_at + 1:close_at].rstrip().endswith(",") else ", "
        edits.append((m.start("path"), m.end("path"), RESULT))
        edits.append((close_at, close_at, sep + aliases[m.group("alias")]))
    for start, end, new in sorted(edits, reverse=True):
        text = text[:start] + new + text[end:]
    return text, len(found)


def realign_diff(diff_text: str, files: dict[str, list[str]]) -> str:
    """The diff with each new-side line of a file in `files` replaced by that file's line of the same number."""
    out: list[str] = []
    lines: list[str] | None = None
    new_no = old_left = new_left = 0
    for raw in diff_text.splitlines(keepends=True):
        body = raw.rstrip("\n")
        end = raw[len(body):]
        if old_left or new_left:
            # Inside a hunk, counted from its header, so an added line that
            # starts with "++ " is never read as a file header.
            tag = raw[:1]
            if tag in (" ", "+"):
                if lines is not None and 0 < new_no <= len(lines):
                    body = tag + lines[new_no - 1]
                new_no += 1
                new_left -= 1
                old_left -= tag == " "
            elif tag == "-":
                old_left -= 1
            out.append(body + end)
            continue
        if raw.startswith("+++ "):
            path = body[4:].split("\t")[0]
            lines = files.get(path[2:] if path.startswith("b/") else path)
        elif hunk := re.match(r"^@@ -\d+(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", body):
            old_left = int(hunk.group(1) or 1)
            new_no = int(hunk.group(2))
            new_left = int(hunk.group(3) or 1)
        out.append(raw)
    return "".join(out)


def run(dirs: list[Path], aliases: dict[str, str], diff: Path | None, root: Path) -> dict:
    rewritten: dict[str, str] = {}
    counts: dict[str, int] = {}
    for d in dirs:
        for path in sorted(d.rglob("*.rs")):
            text = path.read_text()
            new, count = rewrite_source(text, aliases)
            if count:
                if new.count("\n") != text.count("\n"):
                    raise Unbalanced(f"{path}: the rewrite changed the number of lines")
                rel = path.resolve().relative_to(root.resolve()).as_posix()
                rewritten[rel] = new
                counts[rel] = count
    # Every file is rewritten in memory first, so a failure changes nothing.
    diff_text = diff.read_text(errors="surrogateescape") if diff is not None else None
    for rel, new in rewritten.items():
        (root / rel).write_text(new)
    if diff is not None:
        diff.write_text(realign_diff(diff_text, {rel: new.split("\n") for rel, new in rewritten.items()}),
                        errors="surrogateescape")
    return {"aliases": aliases, "return_types": sum(counts.values()), "files": counts}


def main(argv: list[str]) -> int:
    args = list(argv)
    aliases: dict[str, str] = {}
    diff = summary = None
    while "--alias" in args:
        i = args.index("--alias")
        name, _, error = (args[i + 1] if i + 1 < len(args) else "").partition("=")
        if not re.fullmatch(IDENT, name) or not error:
            print(f"--alias takes NAME=ERROR, got {args[i + 1:i + 2]}", file=sys.stderr)
            return 2
        aliases[name] = error
        del args[i:i + 2]
    for flag in ("--diff", "--summary"):
        if flag in args:
            i = args.index(flag)
            value = Path(args[i + 1]) if i + 1 < len(args) else None
            del args[i:i + 2]
            if flag == "--diff":
                diff = value
            else:
                summary = value
    if not aliases or not args:
        print(__doc__, file=sys.stderr)
        return 2
    try:
        data = run([Path(a) for a in args], aliases, diff, Path.cwd())
    except (Unbalanced, OSError, ValueError) as error:
        print(f"mutants-result-alias: nothing rewritten: {error}", file=sys.stderr)
        return 1
    for rel, count in sorted(data["files"].items()):
        print(f"{rel}: {count} return type(s)")
    print(f"{data['return_types']} return type(s) in {len(data['files'])} file(s) now read as Result")
    if summary is not None:
        summary.parent.mkdir(parents=True, exist_ok=True)
        summary.write_text(json.dumps(data, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
