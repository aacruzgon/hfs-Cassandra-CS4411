# Benchmark datasets

The Synthea datasets of roadmap row 0.7: 10,000 and 100,000 resources, Patient and Observation only, as sorted NDJSON. Step 8's benchmarks and row 0.8's PostgreSQL baseline load these two files.

The repository holds the recipe and the checksums, not the data:

| File | What it is |
| --- | --- |
| `generate_datasets.py` | Downloads Synthea, runs it with every input fixed, and writes the NDJSON, `manifest.json` and `SHA256SUMS`. `--verify` regenerates and compares. |
| `ndjson_to_batches.py` | Turns one NDJSON file into batch Bundles of `PUT` entries for the load phase. |
| `manifest.json` | The recorded inputs: Synthea's release, version and modules, the seeds, the population, each file's checksum and its counts. |
| `SHA256SUMS` | The two checksums, for `shasum -a 256 -c`. |

The NDJSON itself is 10 MB and 104 MB, and is published as assets of the release [`benchmark-datasets-2026-11-09`](https://github.com/aacruzgon/hfs-Cassandra-CS4411/releases/tag/benchmark-datasets-2026-11-09).

## Regenerate or verify

You need Python 3.9 or later, Java 17 or later, and network access for the 201 MB jar (kept in `~/.cache/hfs-benchmark-datasets/`).

```bash
# Write the two files, plus manifest.json and SHA256SUMS, to a directory of your choice.
python3 docs/Cassandra/datasets/generate_datasets.py --out-dir /tmp/datasets

# Regenerate into a temporary directory and compare with manifest.json. This is Step 0's exit criterion.
python3 docs/Cassandra/datasets/generate_datasets.py --verify
```

A run takes about a minute, peaks at about 2.2 GB of memory, and writes 1.6 GB of Synthea output to the temporary directory (or `--work-dir`), which it deletes unless you pass `--keep-raw`.

The exit status is 0 on success, 1 when `--verify` finds a difference, and 2 when the run could not be done.

If the jar download fails, fetch it with `gh release download v4.0.0 --repo synthetichealth/synthea --pattern synthea-with-dependencies.jar` and pass it with `--jar`. The checksum is enforced either way.

## Publish the release

Regenerate from `main`, check the files against the committed checksums, and attach them to the release:

```bash
python3 docs/Cassandra/datasets/generate_datasets.py --verify --out-dir "$HOME/hfs-datasets"
(cd "$HOME/hfs-datasets" && shasum -a 256 -c "$OLDPWD/docs/Cassandra/datasets/SHA256SUMS")
gh release create benchmark-datasets-2026-11-09 \
  --repo aacruzgon/hfs-Cassandra-CS4411 --target main \
  --title "Benchmark datasets, Synthea v4.0.0 (row 0.7)" \
  --notes "Patient and Observation only, 10,000 and 100,000 resources. Recipe and checksums: docs/Cassandra/datasets/." \
  "$HOME/hfs-datasets/patient-observation-10000.ndjson" \
  "$HOME/hfs-datasets/patient-observation-100000.ndjson" \
  "$HOME/hfs-datasets/manifest.json" \
  "$HOME/hfs-datasets/SHA256SUMS"
```

This creates `https://github.com/aacruzgon/hfs-Cassandra-CS4411/releases/tag/benchmark-datasets-2026-11-09`.

## What is fixed, and why

The inputs are constants at the top of `generate_datasets.py` and are all recorded in `manifest.json`.

| Input | Value | Why it has to be fixed |
| --- | --- | --- |
| Synthea jar | release v4.0.0, by SHA-256 | `master-branch-latest` moves. The jar's own metadata reports `v3.4.0-18-ga07a65555`, so the manifest records both. |
| `-s`, `-cs` | 20261108 | The seeds. |
| `-r`, `-e` | 20260101 | `-r` does not set the end of the simulation. Without `-e`, Synthea simulates up to the moment it runs, and the same command on a later date gives different patients. |
| `-p` | 400 | 469 patient records with 246,995 Patient and Observation resources, comfortably more than 100,000. |
| `user.timezone` | UTC | Synthea writes dates in the JVM's zone, so `effectiveDateTime` and even `birthDate` change with the machine's zone. |
| thread pool | 4 | Recorded for the record. The checksums do not depend on it, and `--verify --threads N` lets you check that. |

The other Synthea settings, listed in `manifest.json`, are stated so that the record is complete.

The checksums cover the filtered NDJSON, not Synthea's output. Whole Bundles differ between runs, even at one thread count, in resource types other than Patient and Observation.

The filter also removes Synthea's two quality-of-life extensions from Patient (`disability-adjusted-life-years` and `quality-adjusted-life-years`). They hold 16-digit results of `Math.exp` and `Math.pow`, whose last digit differs between x86-64 and arm64, and they were the only values that differed. With them gone, the checksums were the same on arm64 and x86-64 (in a container under Rosetta), on Java 17 and 21, on macOS and Linux. On real x86-64 CPUs, the report-only workflow `.github/workflows/benchmark-datasets.yml` runs `--verify` on the self-hosted runners whenever these files change.

## What the files contain

Each line is one resource: UTF-8, sorted keys, no spaces, `\n` line endings, and decimals exactly as Synthea wrote them. `urn:uuid:` references are rewritten to `Patient/<id>` and `Encounter/<id>`, because a batch cannot carry them.

Patients are in `Patient.id` order, each followed by its Observations in `id` order. Each file stops at its exact size, which cuts the last patient's Observations, and the 10,000 file is a byte prefix of the 100,000 file.

`Observation.encounter` points at Encounters that are not in the dataset, because the filter keeps only Patient and Observation. Both backends accept these references and store them unchanged, and a read of such an Encounter answers 404. On PostgreSQL, a search by that reference value still matches.

## Loading

The load phase uses batch Bundles of `PUT` entries, so the dataset's ids survive and loading twice overwrites rather than duplicates. The benchmark's create operation measures `POST` of single resources instead.

```bash
python3 docs/Cassandra/datasets/ndjson_to_batches.py patient-observation-10000.ndjson /tmp/batches
```

The default is 500 entries per Bundle. HFS sets no limit on entries per Bundle. What bounds a Bundle is `HFS_MAX_BODY_SIZE` (128 MiB) and `HFS_REQUEST_TIMEOUT` (30 s, covering the whole Bundle), and both backends run at most 8 entries of a batch at once, so a larger Bundle saves only HTTP round trips. A resource here is about 1 KB, so 500 entries is about 0.5 MB. Measured with a debug build of HFS on a laptop: about 0.7 s per Bundle on Cassandra (10,000 resources) and about 1 s on PostgreSQL (10,000 and 100,000).

A batch answers 200 even when entries fail. Whatever posts these Bundles has to check `entry[].response.status`.
