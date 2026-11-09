# Benchmark harness

The harness of roadmap row 0.6 (`docs/Cassandra/development-roadmap.md`): open-loop load from k6, a data reset per run, warm-up, three runs, per-request CSV, and a summary. Step 8 and row 0.8 use it. Everything it does is in two files:

| File | What it is |
| --- | --- |
| `bench.py` | The driver: fetches k6, resets the database, loads the dataset, warms up, measures, records the environment, writes the CSVs. Python 3.9 or later, standard library only. |
| `load.js` | The k6 script: one operation at a time, open-loop or closed-loop. |
| `rates.json` | The calibrated arrival rates, one per dataset size, operation and concurrency. **Committed.** Written by `bench.py calibrate` on the benchmark machine. |
| `test_bench.py` | Tests for the analysis (no Docker or server): `python3 docs/Cassandra/benchmark/test_bench.py`. |

## What a run does

Before each of the three measured runs, in this order (roadmap Step 8):

1. **Reset.** `docker compose down -v`, then `up -d --wait`, using `docker/cassandra/` or `docker/postgres/` unchanged, under the harness's own Compose project (`hfsbench-cassandra`, `hfsbench-postgres`) and ports (59542, 55632). On Cassandra it applies `docs/Cassandra/schema/` next. The harness refuses the shared `hfs-cassandra`/`hfs-postgres` projects and ports 59042 and 55432.
2. **Start HFS** on the freshly reset database. HFS is never restarted on a Cassandra node that has seen load (bug #74); a reset comes first.
3. **Probe,** for the search operations only. A 501 (Cassandra until Step 6) or other refusal is written to `run.json` with `status: not_implemented` or `refused`, and the cell is skipped. It is not a failure.
4. **Load** the dataset through batch Bundles of `PUT` entries (`docs/Cassandra/datasets/ndjson_to_batches.py`, cached under the output directory) and check each entry's status, since a batch answers 200 even when an entry fails.
5. **Settle.** Cassandra: `nodetool flush`, then wait for `pending tasks: 0`. PostgreSQL: `CHECKPOINT` and `VACUUM (ANALYZE)`, its counterpart.
6. **JIT warm-up:** the same operation on the same schedule for `--warmup` seconds (60 by default), results thrown away. Then wait again for pending compactions to reach zero, so that compaction from the warm-up does not run into the measurement.
7. **Measure** with `constant-arrival-rate`: `--duration` seconds (120 by default) at the calibrated rate, at most `concurrency` virtual users. Memory is sampled every 2 s throughout.

`--no-reset` keeps one data set across the three runs. Use it only for the LWT steady-state case below; it is the setting under which ADR-aacruzgon-0001 saw its decline.

**What the measurement is.** The arrival rate is fixed before the run and doesn't depend on how fast the server answers. "Concurrency N" is k6's `maxVUs = N`. A backend that can't keep up runs out of virtual users, and the iterations it couldn't start are counted as `dropped_iterations`: read them next to the latency. A closed loop understates p95, which is why row 0.6 rules it out.

**Dataset check.** Before every load the harness hashes the NDJSON and compares it with `docs/Cassandra/datasets/SHA256SUMS` (`--sha256sums` to point elsewhere), and refuses the run on a mismatch or a missing file or line, so every number names exactly the published bytes (`dataset_file`, `dataset_sha256` and `dataset_verified` in the CSVs). It only hashes; regenerating is `generate_datasets.py --verify`. `--unverified-dataset` is for development with a hand-cut file, requires `--profile 3g`, and marks its runs `dataset_verified False`: they never go in the report.

## The operations

| Name | Request | Input |
| --- | --- | --- |
| `create` | `POST /<type>`, one resource | The dataset's own bodies, without their id. So 99.8% Observations, as in the dataset. |
| `read` | `GET /<type>/<id>` | Every id of the dataset, in a fixed scattered order. |
| `update` | `PUT /<type>/<id>`, with new narrative text | The same. Narrative is not a search parameter here, so indexes don't change. |
| `token` | `Observation?code=<system|code>&_count=20` | The 20 most frequent codes in the dataset. |
| `date` | `Observation?date=ge<month>&date=lt<next month>&_count=20` | The 12 busiest months. |
| `uri` | `Observation?_profile=<url>&_count=20` | The 19 profiles in the dataset (the fewer-than-20 is the dataset's own count). |
| `hotkey` | Unconditional `PUT /Patient/<one id>` | Not in the Step 8 matrix and has no calibrated rate: run it with `--ops hotkey --rate N`. It shows the 409s. |

Search queries come from the dataset by rule (most frequent first, ties by value), so every machine and backend runs the same list. Only the first page of each query is fetched.

## What it writes

Under `--out`:

```text
environment-<leg>.json          the HFS commit and build profile, k6, machine, container image and limits, Compose files
runs.csv                        one row per run
summary.csv                     one row per cell, over its runs
<leg>/<size>/<op>/c<N>/run<K>/
    run.json                    every metric, the container's limits and the status of this run
    requests.csv.gz             one row per request: end_ms, op, status, duration_ms, error_code
    bins.csv                    10 s bins: throughput, percentiles, each status class, drops
    memory.csv                  2 s samples: cgroup current, used, anon, file; hfs and k6 RSS
    db-settings.json            nodetool version/info/status and the config, or pg_settings
    proxyhistograms.txt, tablestats.txt, compactionstats.txt, gcstats.txt
    pg_stat_statements.txt, pg_stat_database.txt
    k6.log, hfs.log
```

`<leg>` is `cassandra`, `postgres-sync-on` or `postgres-sync-off`, so every number carries its baseline (ADR-aacruzgon-0005).

**Run status.** `ok` is a measured run. `not_implemented` (501) and `refused` (other 4xx) mean the server declined the search, so no load ran. `no_success` means no request got a 2xx, and `failed` means the run could not finish (the reason is in `run.json`); both make the command exit 1.

**Status classes.** `runs.csv` and `summary.csv` count `s2xx`, `s409`, `s412`, `s4xx_other`, `s501`, `s5xx_other` and `s_other` (network errors, 3xx), so a 409 is never lost in `5xx` (roadmap Step 0, Spec). `statuses` carries the full histogram.

**Steady state.** `bins.csv` has one row per 10 s for every run. `steady_change_pct` compares the first and last third of its full bins; a negative value is a decline. On an open-loop run below capacity the rate holds by construction, and the decline shows only as latency and drops. To see a decline in throughput, use the case in the next section.

**Peak container memory.** `memory.peak` counts page cache and reads the limit on any database that has read that much, so it is recorded but is not the headline. `mem_peak_used_mib` is `memory.current` less `inactive_file`, which is what `docker stats` shows, and `mem_peak_anon_mib` is the anonymous memory alone. `oom_killed` is recorded for each run.

## Running the full matrix

On the designated 32 GB machine (roadmap section 1, ADR-aacruzgon-0002). Laptop numbers never go in the report. Needs Docker with Compose, Python 3.9 or later, a release `hfs`, and nothing else running in Docker.

```bash
# Build HFS with both backends, once.
cargo build --release -p helios-hfs --features cassandra,postgres

# Pin k6 and verify its checksum. Prints the path of the binary.
python3 docs/Cassandra/benchmark/bench.py fetch-k6 --dest ~/bench-tools
K6=~/bench-tools/k6-v2.3.0-macos-arm64/k6   # or k6-v2.3.0-linux-amd64

# The datasets, from row 0.7 (docs/Cassandra/datasets/README.md).
python3 docs/Cassandra/datasets/generate_datasets.py --out-dir ~/bench-data
OUT=~/bench-out DATA=~/bench-data

# 1. Calibrate on PostgreSQL. Writes docs/Cassandra/benchmark/rates.json: commit it.
python3 docs/Cassandra/benchmark/bench.py calibrate --k6 $K6 --dataset-dir $DATA --out $OUT/calibration

# 2. The three legs of the matrix, on the same schedule.
python3 docs/Cassandra/benchmark/bench.py run --backend cassandra --k6 $K6 --dataset-dir $DATA --out $OUT/matrix
python3 docs/Cassandra/benchmark/bench.py run --backend postgres --baseline on --k6 $K6 --dataset-dir $DATA --out $OUT/matrix
python3 docs/Cassandra/benchmark/bench.py run --backend postgres --baseline off --ops create,update --k6 $K6 --dataset-dir $DATA --out $OUT/matrix

# 3. LWT steady state, closed-loop: three 600 s runs on one data set, on both backends.
#    ADR-aacruzgon-0001's decline showed up in consecutive runs without a reset.
python3 docs/Cassandra/benchmark/bench.py run --backend cassandra --ops create --sizes 10000 --concurrency 32 --mode closed --duration 600 --warmup 60 --no-reset --label lwt-steady --k6 $K6 --dataset-dir $DATA --out $OUT/lwt-steady
python3 docs/Cassandra/benchmark/bench.py run --backend postgres --baseline on --ops create --sizes 10000 --concurrency 32 --mode closed --duration 600 --warmup 60 --no-reset --label lwt-steady --k6 $K6 --dataset-dir $DATA --out $OUT/lwt-steady
```

The defaults are three runs, 120 s measured, 60 s warm-up, the 5 GiB profile, both sizes, all six operations, concurrency 1, 8 and 32. Add `--resume` to continue a matrix that stopped; recorded runs are skipped.

**On a shared machine** add `--docker-budget-gib N` to wait until the sum of the running containers' memory limits plus this leg's fits. On the 18 GB Macs, use `--profile 3g` and never report those numbers.

**Roughly how long.** About 27 hours in total, on the timings below, which were measured on a laptop and are an upper estimate for a 32 GB desktop:

| Part | Runs | Roughly |
| --- | --- | --- |
| Calibration | 36 cells, 1 run, 60 s each | 3 h |
| PostgreSQL `on` | 108 | 11 h |
| PostgreSQL `off`, create and update only | 36 | 4 h |
| Cassandra | 108 once Step 6 lands; until then 54, with the 18 search cells recorded as 501 | 8 h (5 h before Step 6) |
| LWT steady state, both backends | 3 runs of 600 s, one load | 1 h |

Per run, on top of 60 s of warm-up and 120 s measured: PostgreSQL 40 s of reset and settle at 10,000 resources and 3 to 6 minutes at 100,000, where loading dominates; Cassandra about 70 s of reset and boot, and 45 s of load at 100,000. Add `--resume` to continue a part that stopped, and split the parts over several nights if the machine's window needs it.

## Reading a smoke run

To try it on a laptop, use a fixed rate and short runs. Numbers from this are for checking that the harness works and go nowhere:

```bash
python3 docs/Cassandra/benchmark/bench.py run --backend postgres --profile 3g --ops create,read --concurrency 8 --sizes 10000 --runs 2 --duration 20 --warmup 5 --rate 50 --k6 $K6 --dataset-dir $DATA --out /tmp/smoke
```

## Not in this harness

- Step 8's named cases other than `hotkey` (the mass-update tombstone case, the broad `_has` case). The mass-update case waits for bug #74's fix.
- Tombstones scanned per read are in `tablestats.txt` (`Average tombstones per slice`). Batch sizes (Step 8) are not recorded.
- Comparison with upstream's published numbers. Upstream's `fhir-benchmark.yml` stays disabled (see the ADR for this row).
