#!/usr/bin/env python3
"""Generate the benchmark datasets of roadmap row 0.7, or check they regenerate.

    python3 docs/Cassandra/datasets/generate_datasets.py --out-dir /some/big/disk/d
    python3 docs/Cassandra/datasets/generate_datasets.py --verify

One Synthea run with every input fixed, then Patient and Observation only, as
sorted NDJSON, cut to exactly 10,000 and 100,000 resources. See README.md next
to this file. It needs Python 3.9 or later, Java 17 or later, and nothing else.

The inputs below are the whole recipe. Three of them are easy to miss, and
each one changes the data if it is left out:

  -e        Without it Synthea simulates up to the current time, so the same
            command run on a later date produces different patients.
  user.timezone
            Synthea renders dates in the JVM's zone, so `effectiveDateTime`
            and even `birthDate` change with the machine's time zone.
  the jar   `master-branch-latest` moves; v4.0.0's digest does not.

Whole Bundles are not reproducible: resource types other than Patient and
Observation differ between runs, even at the same thread count. That is why
the checksums cover the filtered NDJSON and not Synthea's output.

Exit status: 0 on success, 1 when --verify finds a difference, 2 when the run
could not be done (no Java, a jar that fails its checksum, Synthea failing).
"""

import argparse
import hashlib
import http.client
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from collections import Counter
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Set, Tuple

RELEASE_TAG = "v4.0.0"
JAR_NAME = "synthea-with-dependencies.jar"
JAR_URL = (
    "https://github.com/synthetichealth/synthea/releases/download/"
    f"{RELEASE_TAG}/{JAR_NAME}"
)
JAR_SHA256 = "ed43c20ad40ba5c3bc724503a5af032715fe3c491620b766148e7c2361e6ecc1"

SEED = 20261108
CLINICIAN_SEED = 20261108
REFERENCE_DATE = "20260101"
END_DATE = "20260101"
POPULATION = 400
STATE = "Massachusetts"
THREADS = 4

# Peak resident memory at 400 people is about 2.1 GB with the JVM's default
# heap. The heap has no effect on the data, so a lower one is safe to try.
MAX_HEAP = "3g"

JVM_PROPERTIES = (
    "-Duser.timezone=UTC",
    "-Duser.language=en",
    "-Duser.country=US",
    "-Dfile.encoding=UTF-8",
)

# Each is Synthea's default today, except the two hospital and practitioner
# lines, which skip files the filter never reads. They are stated so the
# manifest records them and a change of default cannot move the data.
SYNTHEA_CONFIG = {
    "exporter.years_of_history": "10",
    "exporter.fhir.export": "true",
    "exporter.fhir.transaction_bundle": "true",
    "exporter.fhir.bulk_data": "false",
    "exporter.fhir.use_us_core_ig": "true",
    "exporter.fhir.us_core_version": "6.1.0",
    "exporter.hospital.fhir.export": "false",
    "exporter.practitioner.fhir.export": "false",
    "exporter.metadata.export": "true",
}

SIZES = (10_000, 100_000)
KEPT_TYPES = ("Patient", "Observation")

# Synthea names a patient's Bundle `<names>_<Patient.id>.json`.
PATIENT_FILE = re.compile(
    r"_([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.json$"
)
URN_UUID = "urn:uuid:"
DOWNLOAD_ATTEMPTS = 6
HERE = Path(__file__).resolve().parent
COMMITTED_MANIFEST = HERE / "manifest.json"
# Fields that describe one run and are not compared by --verify.
UNCOMPARED_SETTINGS = ("threads",)


class Failure(Exception):
    """The run could not be done; the message says why."""


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def download(url: str, destination: Path) -> None:
    """Fetch url to destination, retrying the transient failures GitHub gives."""
    partial = destination.with_suffix(".part")
    last_error = None
    for attempt in range(1, DOWNLOAD_ATTEMPTS + 1):
        try:
            request = urllib.request.Request(
                url, headers={"User-Agent": "hfs-benchmark-datasets"}
            )
            with urllib.request.urlopen(request, timeout=120) as response:
                with partial.open("wb") as out:
                    shutil.copyfileobj(response, out, 1 << 20)
            partial.replace(destination)
            return
        except (urllib.error.URLError, http.client.HTTPException, OSError) as error:
            last_error = error
            print(f"download attempt {attempt} failed: {error}", file=sys.stderr)
            if attempt < DOWNLOAD_ATTEMPTS:
                time.sleep(2**attempt)
    partial.unlink(missing_ok=True)
    raise Failure(
        f"could not download {url}: {last_error}. Fetch the jar another way "
        f"(gh release download {RELEASE_TAG} --repo synthetichealth/synthea "
        f"--pattern {JAR_NAME}) and pass it with --jar."
    )


def obtain_jar(supplied: Optional[Path], cache_dir: Path) -> Path:
    """Return a jar whose SHA-256 is JAR_SHA256, or refuse."""
    if supplied is not None:
        if not supplied.is_file():
            raise Failure(f"--jar {supplied} is not a file")
        actual = sha256_of(supplied)
        if actual != JAR_SHA256:
            raise Failure(
                f"refusing {supplied}: SHA-256 {actual}, expected {JAR_SHA256}"
            )
        return supplied
    jar = cache_dir / RELEASE_TAG / JAR_NAME
    if jar.is_file() and sha256_of(jar) == JAR_SHA256:
        return jar
    jar.parent.mkdir(parents=True, exist_ok=True)
    print(f"downloading {JAR_URL}", file=sys.stderr)
    download(JAR_URL, jar)
    actual = sha256_of(jar)
    if actual != JAR_SHA256:
        jar.unlink()
        raise Failure(
            f"refusing the downloaded {JAR_NAME}: SHA-256 {actual}, "
            f"expected {JAR_SHA256}"
        )
    return jar


def java_version() -> str:
    java = shutil.which("java")
    if java is None:
        raise Failure("java is not on PATH; Synthea needs Java 17 or later")
    result = subprocess.run(
        [java, "-XshowSettings:properties", "-version"],
        capture_output=True,
        text=True,
    )
    found = re.search(r"java\.runtime\.version = (\S+)", result.stderr)
    return found.group(1) if found else "unknown"


def module_list(jar: Path) -> List[str]:
    """Every built-in generic module and submodule the jar carries."""
    with zipfile.ZipFile(jar) as archive:
        names = [
            name
            for name in archive.namelist()
            if name.startswith("modules/") and name.endswith(".json")
        ]
    return sorted(names)


def list_sha256(names: List[str]) -> str:
    return hashlib.sha256("".join(f"{n}\n" for n in names).encode()).hexdigest()


def run_synthea(jar: Path, work_dir: Path, threads: int) -> Path:
    """Run Synthea in an empty directory; return its output directory."""
    output = work_dir / "output"
    command = ["java", f"-Xmx{MAX_HEAP}", *JVM_PROPERTIES, "-jar", str(jar)]
    command += ["-s", str(SEED), "-cs", str(CLINICIAN_SEED)]
    command += ["-r", REFERENCE_DATE, "-e", END_DATE]
    command += ["-p", str(POPULATION)]
    command += [f"--generate.thread_pool_size={threads}"]
    command += [f"--exporter.baseDirectory={output}/"]
    command += [f"--{key}={value}" for key, value in sorted(SYNTHEA_CONFIG.items())]
    command += [STATE]
    log = work_dir / "synthea.log"
    print(f"running Synthea, {POPULATION} people, {threads} threads", file=sys.stderr)
    started = time.monotonic()
    run_dir = work_dir / "cwd"
    run_dir.mkdir()
    with log.open("wb") as stream:
        finished = subprocess.run(
            command, cwd=run_dir, stdout=stream, stderr=subprocess.STDOUT
        )
    if finished.returncode != 0:
        tail = log.read_text(errors="replace")[-2000:]
        raise Failure(f"Synthea exited {finished.returncode}:\n{tail}")
    print(f"Synthea took {time.monotonic() - started:.0f} s", file=sys.stderr)
    return output


def metadata_version(output: Path) -> str:
    files = sorted((output / "metadata").glob("*.json"))
    if len(files) != 1:
        raise Failure(f"expected one Synthea metadata file, found {len(files)}")
    return json.loads(files[0].read_text())["version"].strip()


def resolve_references(node: object, full_urls: Dict[str, str], where: str) -> None:
    """Turn each urn:uuid reference into Type/id, in place.

    Only an object's `reference` field is touched, as
    crates/hfs/tests/inferno/to_batch.jq does. A reference the Bundle cannot
    resolve, or a conditional one, stops the run: a batch cannot carry either.
    """
    if isinstance(node, dict):
        reference = node.get("reference")
        if isinstance(reference, str):
            if reference.startswith(URN_UUID):
                if reference not in full_urls:
                    raise Failure(f"{where}: unresolved reference {reference}")
                node["reference"] = full_urls[reference]
            else:
                raise Failure(f"{where}: unexpected reference {reference}")
        for value in node.values():
            resolve_references(value, full_urls, where)
    elif isinstance(node, list):
        for value in node:
            resolve_references(value, full_urls, where)


class Decimal(str):
    """A decimal kept as Synthea wrote it: 704.20 must not become 704.2."""


def encode(value: object) -> str:
    """Compact JSON with sorted keys and each decimal's original text."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        items = sorted(value.items())
        return "{" + ",".join(f"{encode(k)}:{encode(v)}" for k, v in items) + "}"
    if isinstance(value, list):
        return "[" + ",".join(encode(item) for item in value) + "]"
    return json.dumps(value, ensure_ascii=False)


def canonical_line(resource: dict) -> bytes:
    return encode(resource).encode("utf-8") + b"\n"


def patient_files(fhir_dir: Path) -> List[Tuple[str, Path]]:
    """Every patient Bundle as (Patient.id, path), sorted by Patient.id."""
    found = []
    for path in fhir_dir.iterdir():
        match = PATIENT_FILE.search(path.name)
        if match is None:
            raise Failure(f"not a patient Bundle: {path.name}")
        found.append((match.group(1), path))
    return sorted(found)


def resources_of_patient(patient_id: str, path: Path) -> List[dict]:
    """The Patient, then its Observations sorted by id, references resolved."""
    with path.open(encoding="utf-8") as stream:
        bundle = json.load(stream, parse_float=Decimal)
    if bundle.get("type") != "transaction":
        raise Failure(f"{path.name}: expected a transaction Bundle")
    full_urls = {}
    for entry in bundle["entry"]:
        resource = entry["resource"]
        full_urls[entry["fullUrl"]] = f"{resource['resourceType']}/{resource['id']}"
    kept = [
        entry["resource"]
        for entry in bundle["entry"]
        if entry["resource"]["resourceType"] in KEPT_TYPES
    ]
    patients = [r for r in kept if r["resourceType"] == "Patient"]
    if len(patients) != 1 or patients[0]["id"] != patient_id:
        raise Failure(f"{path.name}: expected exactly Patient/{patient_id}")
    observations = sorted(
        (r for r in kept if r["resourceType"] == "Observation"),
        key=lambda r: r["id"],
    )
    ordered = patients + observations
    for resource in ordered:
        resolve_references(resource, full_urls, f"{path.name} {resource['id']}")
    return ordered


def stream_resources(fhir_dir: Path, limit: int) -> Iterator[dict]:
    """Patients in id order, each followed by its Observations, up to limit."""
    emitted = 0
    for patient_id, path in patient_files(fhir_dir):
        for resource in resources_of_patient(patient_id, path):
            yield resource
            emitted += 1
            if emitted == limit:
                return
    raise Failure(f"population {POPULATION} yields only {emitted} resources")


def references_in(node: object, path: str) -> Iterator[Tuple[str, str]]:
    """Each `reference` under node, as (element path, reference)."""
    if isinstance(node, dict):
        reference = node.get("reference")
        if isinstance(reference, str):
            yield path, reference
        for key, value in node.items():
            yield from references_in(value, f"{path}.{key}")
    elif isinstance(node, list):
        for value in node:
            yield from references_in(value, path)


class DatasetStats:
    """What one dataset holds, and where its references point."""

    def __init__(self) -> None:
        self.counts: Counter = Counter()
        self.keys: Set[str] = set()
        self.references: List[Tuple[str, str]] = []

    def add(self, resource: dict) -> None:
        kind = resource["resourceType"]
        key = f"{kind}/{resource['id']}"
        if key in self.keys:
            raise Failure(f"duplicate resource {key}")
        self.keys.add(key)
        self.counts[kind] += 1
        self.references.extend(references_in(resource, kind))

    def as_record(self) -> dict:
        targets: Counter = Counter()
        for field, reference in self.references:
            state = "loaded" if reference in self.keys else "not_loaded"
            targets[f"{field}->{reference.split('/')[0]}:{state}"] += 1
        return {
            "resources": sum(self.counts.values()),
            "patients": self.counts["Patient"],
            "observations": self.counts["Observation"],
            "references": dict(sorted(targets.items())),
        }


def write_datasets(fhir_dir: Path, out_dir: Path) -> Dict[str, dict]:
    """Write one file per size; each smaller file is a prefix of the larger."""
    out_dir.mkdir(parents=True, exist_ok=True)
    sizes = sorted(SIZES)
    paths = {size: out_dir / f"patient-observation-{size}.ndjson" for size in sizes}
    streams = {size: paths[size].open("wb") for size in sizes}
    stats = {size: DatasetStats() for size in sizes}
    try:
        for index, resource in enumerate(stream_resources(fhir_dir, sizes[-1])):
            line = canonical_line(resource)
            for size in sizes:
                if index < size:
                    streams[size].write(line)
                    stats[size].add(resource)
    finally:
        for stream in streams.values():
            stream.close()
    records = {}
    for size in sizes:
        record = stats[size].as_record()
        if record["resources"] != size:
            raise Failure(f"{paths[size]} has {record['resources']} resources")
        record["file"] = paths[size].name
        record["bytes"] = paths[size].stat().st_size
        record["sha256"] = sha256_of(paths[size])
        records[str(size)] = record
    return records


def build_manifest(jar: Path, output: Path, threads: int, datasets: dict) -> dict:
    modules = module_list(jar)
    return {
        "synthea": {
            "release_tag": RELEASE_TAG,
            "jar": JAR_NAME,
            "jar_url": JAR_URL,
            "jar_sha256": JAR_SHA256,
            "metadata_version": metadata_version(output),
        },
        "settings": {
            "seed": SEED,
            "clinician_seed": CLINICIAN_SEED,
            "reference_date": REFERENCE_DATE,
            "end_date": END_DATE,
            "population": POPULATION,
            "state": STATE,
            "threads": threads,
            "jvm_properties": list(JVM_PROPERTIES),
            "config": SYNTHEA_CONFIG,
        },
        "modules": {
            "count": len(modules),
            "sha256_of_list": list_sha256(modules),
            "list": modules,
        },
        "filter": {
            "resource_types": list(KEPT_TYPES),
            "order": "Patient.id; each Patient, then its Observations by id",
            "format": "NDJSON, UTF-8, sorted keys, no spaces, LF line endings, "
            "decimals as Synthea wrote them",
            "cut": "the stream stops at each size, cutting the last patient's "
            "Observations; each smaller file is a prefix of the larger",
        },
        "datasets": datasets,
    }


def comparable(manifest: dict) -> dict:
    copied = json.loads(json.dumps(manifest))
    for field in UNCOMPARED_SETTINGS:
        copied["settings"].pop(field, None)
    return copied


def differences(expected: object, actual: object, path: str = "") -> List[str]:
    if isinstance(expected, dict) and isinstance(actual, dict):
        found = []
        for key in sorted(set(expected) | set(actual)):
            where = f"{path}.{key}" if path else key
            if key not in expected:
                found.append(f"{where}: only in this run: {actual[key]!r}")
            elif key not in actual:
                found.append(f"{where}: only in the manifest: {expected[key]!r}")
            else:
                found += differences(expected[key], actual[key], where)
        return found
    if expected != actual:
        return [f"{path}: manifest {expected!r}, this run {actual!r}"]
    return []


def write_outputs(out_dir: Path, manifest: dict) -> None:
    text = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    (out_dir / "manifest.json").write_text(text, encoding="utf-8")
    lines = [
        f"{record['sha256']}  {record['file']}\n"
        for _, record in sorted(manifest["datasets"].items(), key=lambda i: int(i[0]))
    ]
    (out_dir / "SHA256SUMS").write_text("".join(lines), encoding="ascii")


def generate(args: argparse.Namespace, out_dir: Path, jar: Path) -> dict:
    if args.work_dir is not None:
        Path(args.work_dir).mkdir(parents=True, exist_ok=True)
    work_dir = Path(tempfile.mkdtemp(prefix="synthea-", dir=args.work_dir))
    try:
        output = run_synthea(jar, work_dir, args.threads)
        datasets = write_datasets(output / "fhir", out_dir)
        manifest = build_manifest(jar, output, args.threads, datasets)
    finally:
        if args.keep_raw:
            print(f"Synthea output kept in {work_dir}", file=sys.stderr)
        else:
            shutil.rmtree(work_dir, ignore_errors=True)
    write_outputs(out_dir, manifest)
    return manifest


def verify(args: argparse.Namespace, jar: Path) -> int:
    if not COMMITTED_MANIFEST.is_file():
        raise Failure(f"no committed manifest at {COMMITTED_MANIFEST}")
    expected = json.loads(COMMITTED_MANIFEST.read_text(encoding="utf-8"))
    temporary = None
    if args.out_dir is None:
        temporary = tempfile.mkdtemp(prefix="datasets-verify-", dir=args.work_dir)
    try:
        actual = generate(args, Path(args.out_dir or temporary), jar)
    finally:
        if temporary is not None:
            shutil.rmtree(temporary, ignore_errors=True)
    found = differences(comparable(expected), comparable(actual))
    if found:
        print(f"MISMATCH: {len(found)} difference(s) from {COMMITTED_MANIFEST}:")
        for line in found:
            print(f"  {line}")
        return 1
    for size, record in sorted(actual["datasets"].items(), key=lambda i: int(i[0])):
        print(f"ok {size}: {record['resources']} resources, sha256 {record['sha256']}")
    print(f"regenerated to the recorded checksums ({COMMITTED_MANIFEST.name})")
    return 0


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate or verify the Synthea benchmark datasets (row 0.7)."
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="regenerate and compare with the committed manifest.json",
    )
    parser.add_argument(
        "--out-dir",
        help="where the NDJSON goes; required unless --verify (several GB free "
        "are needed while Synthea runs, about 100 MB after it)",
    )
    parser.add_argument(
        "--work-dir",
        help="where Synthea's raw output goes while it runs (default: the "
        "system's temporary directory)",
    )
    parser.add_argument("--jar", type=Path, help="a jar already downloaded")
    parser.add_argument(
        "--jar-cache",
        type=Path,
        default=Path.home() / ".cache" / "hfs-benchmark-datasets",
        help="where a downloaded jar is kept (default: %(default)s)",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=THREADS,
        help="Synthea's thread pool (default: %(default)s); the checksums do "
        "not depend on it, and --verify accepts another value to show that",
    )
    parser.add_argument(
        "--keep-raw",
        action="store_true",
        help="keep Synthea's raw output instead of deleting it",
    )
    args = parser.parse_args(argv)
    if not args.verify and args.out_dir is None:
        parser.error("--out-dir is required unless --verify is given")
    if args.threads < 1:
        parser.error("--threads must be at least 1")
    return args


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    print(
        f"java {java_version()}, python {platform.python_version()}, "
        f"{platform.system()} {platform.machine()}",
        file=sys.stderr,
    )
    jar = obtain_jar(args.jar, args.jar_cache)
    if args.verify:
        return verify(args, jar)
    manifest = generate(args, Path(args.out_dir), jar)
    for size, record in sorted(manifest["datasets"].items(), key=lambda i: int(i[0])):
        print(f"{record['file']}: {record['resources']} resources, {record['sha256']}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except (Failure, OSError) as failure:
        print(f"error: {failure}", file=sys.stderr)
        sys.exit(2)
