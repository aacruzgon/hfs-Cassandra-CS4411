#!/usr/bin/env python3
"""Turn one dataset NDJSON into FHIR batch Bundles of PUT entries.

    python3 docs/Cassandra/datasets/ndjson_to_batches.py \
        patient-observation-10000.ndjson /tmp/batches

This is the load phase of roadmap Step 0. Each Bundle is a `batch` whose
entries are `PUT <type>/<id>`, so loading twice overwrites instead of
duplicating, and the ids in the dataset survive. It writes files and talks to
nothing. It needs Python 3.9 or later and nothing else.

A batch answers 200 even when entries fail, so whatever posts these Bundles
has to read `entry[].response.status` in the response. See README.md.

Exit status: 0 on success, 2 when the input is unusable.
"""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import List

# README.md gives the measurements behind this number. A Bundle has no entry
# limit in the server, but `HFS_REQUEST_TIMEOUT` (30 s) covers a whole Bundle.
DEFAULT_ENTRIES = 500

# Well under `HFS_MAX_BODY_SIZE` (128 MiB), which also holds a body in memory.
DEFAULT_MAX_BYTES = 32 * 1024 * 1024

BUNDLE_START = '{"entry":['
BUNDLE_END = '],"resourceType":"Bundle","type":"batch"}\n'
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


class Failure(Exception):
    """The input cannot be converted; the message says why."""


def entry_for(line: str, number: int) -> str:
    try:
        resource = json.loads(line)
        kind, resource_id = resource["resourceType"], resource["id"]
    except (ValueError, KeyError, TypeError) as error:
        raise Failure(f"line {number}: not a FHIR resource: {error}")
    if UUID.fullmatch(resource_id) is None:
        raise Failure(f"line {number}: id {resource_id} is not a UUID")
    # R4 Bundle.entry.fullUrl "SHALL have a value" except on a POST, and
    # urn:uuid is absolute. The line goes in as it is, so a decimal such as
    # 704.20 keeps its text.
    request = json.dumps({"method": "PUT", "url": f"{kind}/{resource_id}"})
    return (
        f'{{"fullUrl":"urn:uuid:{resource_id}","request":{request},"resource":'
        + line.rstrip("\n")
        + "}"
    )


def write_bundle(out_dir: Path, sequence: int, entries: List[str]) -> None:
    body = BUNDLE_START + ",".join(entries) + BUNDLE_END
    (out_dir / f"batch-{sequence:06d}.json").write_text(body, encoding="utf-8")


def convert(source: Path, out_dir: Path, max_entries: int, max_bytes: int) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    if any(out_dir.iterdir()):
        raise Failure(f"{out_dir} is not empty; old Bundles would be loaded too")
    # The text around the entries. Each entry below counts one comma, which
    # is one more than a Bundle has, hence the - 1.
    envelope = len(BUNDLE_START) + len(BUNDLE_END) - 1
    sequence, total = 0, 0
    entries: List[str] = []
    size = envelope
    with source.open(encoding="utf-8", newline="\n") as stream:
        for number, line in enumerate(stream, start=1):
            entry = entry_for(line, number)
            cost = len(entry.encode("utf-8")) + 1
            if cost + envelope > max_bytes:
                raise Failure(f"line {number}: one entry is over {max_bytes} bytes")
            if entries and (len(entries) == max_entries or size + cost > max_bytes):
                sequence += 1
                write_bundle(out_dir, sequence, entries)
                entries, size = [], envelope
            entries.append(entry)
            size += cost
            total += 1
    if entries:
        sequence += 1
        write_bundle(out_dir, sequence, entries)
    if total == 0:
        raise Failure(f"{source} holds no resources")
    return {"bundles": sequence, "entries": total}


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Turn dataset NDJSON into batch Bundles of PUT entries."
    )
    parser.add_argument("source", type=Path, help="the dataset NDJSON")
    parser.add_argument("out_dir", type=Path, help="an empty or new directory")
    parser.add_argument(
        "--entries",
        type=int,
        default=DEFAULT_ENTRIES,
        help="most entries per Bundle (default: %(default)s)",
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=DEFAULT_MAX_BYTES,
        help="most bytes per Bundle (default: %(default)s)",
    )
    args = parser.parse_args(argv)
    if args.entries < 1 or args.max_bytes < 1:
        parser.error("--entries and --max-bytes must be at least 1")
    try:
        result = convert(args.source, args.out_dir, args.entries, args.max_bytes)
    except (Failure, OSError) as failure:
        print(f"error: {failure}", file=sys.stderr)
        return 2
    print(f"{result['entries']} entries in {result['bundles']} Bundles in {args.out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
