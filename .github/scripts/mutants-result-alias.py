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
and the hunk headers stay as they were. The diff is split on "\n" only, and
files and diff are read and written with their newlines as they are, so a
form feed or a CRLF line survives. Realigning would make a stale diff look
current (a dispatch's in_diff range whose new side is not the checked-out
head), so first each new-side line of every file to be rewritten is checked
against that file as it is, and a mismatch stops the script before it
writes anything (exit 3): cargo-mutants then refuses the stale diff itself.

Usage: mutants-result-alias.py --alias NAME=ERROR [--alias ...] [--diff FILE]
                               [--summary FILE] <dir>...

Prints what it rewrote; `--summary` writes the same as JSON. Exits 1 if the
rewrite failed, 3 if the diff does not match the tree, 2 on a usage error. Every file is rewritten in memory first, so an unbalanced
`<` changes nothing; a write that fails part-way (a read-only file) can leave
some files rewritten, which the job's step undoes with `git checkout`, so the
job carries on with the tree as it was. Standard library only; runs with
`python3 -I`. Tests: test_mutants_report.py.
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


class StaleDiff(ValueError):
    """A new-side line of the diff that is not the tree's line of the same number."""


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
    """The index of the `>` that closes the `<` at open_at, counting only code.

    `->` and `=>` are not brackets. Inside `()`, `[]` or a const-generic
    `{}` (one that opens right after `<` or `,`) nothing is counted, so
    `[u8; 4]` and `Foo<{ N }>` are types; a `;` or another `{` outside
    them means the `<` never closed (the function's body or the next item).
    """
    depth = nest = 0
    prev = ""
    for k in range(open_at, len(text)):
        if not mask[k]:
            continue
        ch = text[k]
        if ch in "([":
            nest += 1
        elif ch in ")]}":
            nest = max(nest - 1, 0)
        elif ch == "{":
            if not nest and prev not in "<,":
                break
            nest += 1
        elif not nest:
            if ch == "<":
                depth += 1
            elif ch == ">" and text[k - 1] not in "-=":
                depth -= 1
                if depth == 0:
                    return k
            elif ch == ";":
                break
        if not ch.isspace():
            prev = ch
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
        # The last character of code before the `>`, so a comment after a
        # trailing comma, or one that ends with a comma, is not misread.
        last = next((text[k] for k in range(close_at - 1, open_at, -1) if mask[k] and not text[k].isspace()), "")
        sep = " " if last == "," else ", "
        edits.append((m.start("path"), m.end("path"), RESULT))
        edits.append((close_at, close_at, sep + aliases[m.group("alias")]))
    for start, end, new in sorted(edits, reverse=True):
        text = text[:start] + new + text[end:]
    return text, len(found)


def realign_diff(diff_text: str, files: dict[str, list[str]],
                 originals: dict[str, list[str]] | None = None) -> str:
    """The diff with each new-side line of a file in `files` replaced by that file's line of the same number.

    With `originals` (the same files before the rewrite), each such line must
    first be the original file's line of that number, or StaleDiff is raised.
    """
    out: list[str] = []
    lines: list[str] | None = None
    before: list[str] | None = None
    path = ""
    new_no = old_left = new_left = 0
    # On "\n" only: str.splitlines would also split a line at a form feed,
    # \x1c-\x1e, \x85, U+2028 or U+2029.
    for raw in (line for line in re.split(r"(?<=\n)", diff_text) if line):
        body = raw.rstrip("\n")
        end = raw[len(body):]
        if old_left or new_left:
            # Inside a hunk, counted from its header, so an added line that
            # starts with "++ " is never read as a file header.
            tag = raw[:1]
            if tag in (" ", "+"):
                if lines is not None and before is not None:
                    was = before[new_no - 1] if 0 < new_no <= len(before) else ""
                    if body[1:] != was:
                        raise StaleDiff(f"{path} line {new_no}: the diff has {body[1:]!r}, the tree has {was!r}")
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
            path = path[2:] if path.startswith("b/") else path
            lines = files.get(path)
            before = (originals or {}).get(path) if originals is not None else None
        elif hunk := re.match(r"^@@ -\d+(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", body):
            old_left = int(hunk.group(1) or 1)
            new_no = int(hunk.group(2))
            new_left = int(hunk.group(3) or 1)
        out.append(raw)
    return "".join(out)


def read(path: Path, errors: str = "strict") -> str:
    """The file's text with its newlines as they are (no CRLF translation)."""
    with open(path, encoding="utf-8", errors=errors, newline="") as f:
        return f.read()


def write(path: Path, text: str, errors: str = "strict") -> None:
    with open(path, "w", encoding="utf-8", errors=errors, newline="") as f:
        f.write(text)


def run(dirs: list[Path], aliases: dict[str, str], diff: Path | None, root: Path) -> dict:
    rewritten: dict[str, str] = {}
    originals: dict[str, str] = {}
    counts: dict[str, int] = {}
    for d in dirs:
        for path in sorted(d.rglob("*.rs")):
            text = read(path)
            new, count = rewrite_source(text, aliases)
            if count:
                if new.count("\n") != text.count("\n"):
                    raise Unbalanced(f"{path}: the rewrite changed the number of lines")
                rel = path.resolve().relative_to(root.resolve()).as_posix()
                originals[rel] = text
                rewritten[rel] = new
                counts[rel] = count
    # Every file is rewritten in memory first, so a file that cannot be
    # rewritten changes nothing; a failed write is undone by the job's step.
    # The diff is checked against the tree and realigned before any file is written.
    realigned = None
    if diff is not None:
        realigned = realign_diff(read(diff, "surrogateescape"),
                                 {rel: new.split("\n") for rel, new in rewritten.items()},
                                 {rel: old.split("\n") for rel, old in originals.items()})
    for rel, new in rewritten.items():
        write(root / rel, new)
    if realigned is not None:
        write(diff, realigned, "surrogateescape")
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
    except StaleDiff as error:
        print(f"mutants-result-alias: nothing rewritten, the diff does not match the tree: {error}", file=sys.stderr)
        return 3
    except (Unbalanced, OSError, ValueError) as error:
        print(f"mutants-result-alias: rewrite failed; the job restores the tree: {error}", file=sys.stderr)
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
