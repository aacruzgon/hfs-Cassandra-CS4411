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

1. **Reset.** Restore the snapshot of the loaded, settled database (the default, see "Reset by snapshot"). Building that snapshot, once per size, and `--reset reload`, every run, do `docker compose down -v`, then `up -d --wait`, using `docker/cassandra/` or `docker/postgres/` unchanged, under the harness's own Compose project (`hfsbench-cassandra`, `hfsbench-postgres`) and ports (59542, 55632). On Cassandra it applies `docs/Cassandra/schema/` next. The harness refuses the shared `hfs-cassandra`/`hfs-postgres` projects and ports 59042 and 55432.
2. **Start HFS** on the freshly reset database. HFS is never restarted on a Cassandra node that has seen load (bug #74); a reset comes first.
3. **Probe,** for the search operations only. A 501 (Cassandra until Step 6) or other refusal is written to `run.json` with `status: not_implemented` or `refused`, and the cell is skipped. It is not a failure.
4. **Load,** when building the snapshot or with `--reset reload`: the dataset goes through batch Bundles of `PUT` entries (`docs/Cassandra/datasets/ndjson_to_batches.py`, cached under the output directory) and each entry's status is checked, since a batch answers 200 even when an entry fails.
5. **Settle,** before the snapshot is taken, or after each reload. Cassandra: `nodetool flush`, then wait for `pending tasks: 0`. PostgreSQL: `CHECKPOINT` and `VACUUM (ANALYZE)`, its counterpart.
6. **JIT warm-up:** the same operation on the same schedule for `--warmup` seconds (60 by default), results thrown away. Then wait again for pending compactions to reach zero, so that compaction from the warm-up does not run into the measurement.
7. **Mark the window.** `pg_stat_statements` is reset and the `nodetool` readings are taken (`*-before.txt`), so the load and the warm-up are out of the figures collected after the run.
8. **Measure** with `constant-arrival-rate`: `--duration` seconds (120 by default) at the calibrated rate, at most `concurrency` virtual users. Memory is sampled every 2 s throughout.

`--no-reset` keeps one data set across the three runs. Use it only for the LWT steady-state case below; it is the setting under which ADR-aacruzgon-0001 saw its decline.

**Host load.** Before each run the harness records the host's 1, 5 and 15 minute load average, its CPU count, the processes above 5% CPU from `ps` without its own, and the sum of the running containers' memory limits (`host` in `run.json`). A host is quiet when the other processes use at most 200% CPU together. The idle Mac (18 GB, 11 cores, with a desktop session) read a median of 95% over 4 minutes, most of it WindowServer, and a compile uses 700% or more. The load average is recorded but doesn't decide: after a run it still holds the harness's own work (12.5 at 92% other-process CPU, measured). If the host isn't quiet, it waits up to `--host-wait-cap` seconds (default 120), then runs and records `host_busy`, which `runs.csv` and `summary.csv` carry. On Linux `ps` reports a process's lifetime CPU, so a brief spike there may not show.

**Reset by snapshot (the default; `--reset reload` reloads every run).** Step 8's "load before each run" is read as "the loaded state before each run". Instead of reloading through HFS for every run, the harness loads each size once, settles it, stops the database and keeps a copy of the data volume, then restores that copy before each run. It is the same on both backends and needs no database command, and it is the state of a settled load, so HFS is never started on a node with tombstones (bug #74). `bench.py snapshot-check` measures it against a reload and proves it exact (row counts and checksums of every table, markers written by a dirty run gone).

| 100,000 resources, laptop, 3 GiB | Reload | Restore |
| --- | ---: | ---: |
| PostgreSQL, ready to measure | 123 s | 8 s |
| Cassandra, ready to measure | 98 s | 14 to 28 s |

**What the measurement is.** The arrival rate is fixed before the run and doesn't depend on how fast the server answers. "Concurrency N" is k6's `maxVUs = N`. A backend that can't keep up runs out of virtual users, and the iterations it couldn't start are counted as `dropped_iterations`: read them next to the latency. A closed loop understates p95, which is why row 0.6 rules it out.

**Dataset check.** Before every run the harness hashes the NDJSON and compares it with `docs/Cassandra/datasets/SHA256SUMS` (`--sha256sums` to point elsewhere), and refuses the run on a mismatch or a missing file or line, so every number names exactly the published bytes (`dataset_file`, `dataset_sha256` and `dataset_verified` in the CSVs). The Bundles and query lists derived from it are hashed too, and rebuilt if partial or changed. It only hashes the published file; regenerating that is `generate_datasets.py --verify`. `--unverified-dataset` is for development with a hand-cut file, requires `--profile 3g`, and marks its runs `dataset_verified False`: they never go in the report.

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
    <name>-before.txt           the same readings just before the measured window
    k6.log, hfs.log
```

`<leg>` is `cassandra`, `postgres-sync-on` or `postgres-sync-off`, so every number carries its baseline (ADR-aacruzgon-0005).

**Run status.** `ok` is a measured run. `not_implemented` (501) and `refused` (other 4xx) mean the server declined the search, so no load ran. `no_success` means no request got a 2xx, `failed` means the run could not finish (the reason is in `run.json`), and `host_slept` means the host slept during the measurement (more than 2 s between the wall clock and the monotonic clock, `host_slept_s` in `run.json` and `runs.csv`, `host_slept_runs` in `summary.csv`): its bins hold a gap, so it is not a measurement, and `--resume` measures it again. All three make the command exit 1.

**Saturated runs.** A dropped iteration never got a virtual user, so its latency is in no percentile, and the more a backend sheds the better what is left looks. A run that dropped more than 1% of its schedule (`drop_pct`, of `offered` = completed + dropped) is marked `latency_reading saturated` in `runs.csv` and `summary.csv`, and counted in `morning-summary.md`. Read its percentiles as what the requests that got a VU saw, never as the backend's latency at that rate. Healthy runs dropped 0.1 to 0.9% (start-up); runs that couldn't hold the rate dropped 6 to 56%.

**Percentiles.** `p50_ms`, `p95_ms` and `p99_ms` cover every response, and `p50_ok_ms`, `p95_ok_ms` and `p99_ok_ms` only the 2xx ones, so a fast 503 or 409 can't pull the figure down.

**Status classes.** `runs.csv` and `summary.csv` count `s2xx`, `s409`, `s412`, `s4xx_other`, `s501`, `s5xx_other` and `s_other` (network errors, 3xx), so a 409 is never lost in `5xx` (roadmap Step 0, Spec). `statuses` carries the full histogram.

**Steady state.** `bins.csv` has one row per 10 s for every run. `steady_change_pct` compares the first and last third of its full bins; a negative value is a decline. On an open-loop run below capacity the rate holds by construction, and the decline shows only as latency and drops. To see a decline in throughput, use the case in the next section.

**Peak container memory.** `memory.peak` counts page cache and reads the limit on any database that has read that much, so it is recorded but is not the headline. `mem_peak_used_mib` is `memory.current` less `inactive_file`, which is what `docker stats` shows, and `mem_peak_anon_mib` is the anonymous memory alone. `oom_killed` is recorded for each run. **The reset method sits beside every memory figure** (`reset` in both CSVs, and they never average together): a restored database starts cold, so it reads low (PostgreSQL at 100,000: 360 MiB used after a restore against 960 after a reload, with the same latency). **ADR-aacruzgon-0002's memory confirmation uses `--reset reload`.** Samples are 2 s apart, so a spike shorter than that can be missed; `memory.peak` still has it, page cache included.

## Running the full matrix

On the designated 32 GB machine (roadmap section 1, ADR-aacruzgon-0002). Laptop numbers never go in the report. Needs Docker with Compose, Python 3.9 or later, a release `hfs`, and nothing else running in Docker.

```bash
# Build HFS with both backends, once.
cargo build --release -p helios-hfs --features cassandra,postgres

# Pin k6 and verify its checksum. Prints the path of the binary.
python3 docs/Cassandra/benchmark/bench.py fetch-k6 --dest ~/hfs-bench/tools
K6=~/hfs-bench/tools/k6-v2.3.0-macos-arm64/k6   # or k6-v2.3.0-linux-amd64

# The datasets, from row 0.7 (docs/Cassandra/datasets/README.md).
python3 docs/Cassandra/datasets/generate_datasets.py --out-dir ~/hfs-bench/datasets
OUT=~/hfs-bench/report DATA=~/hfs-bench/datasets

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

**Roughly how long.** About 17 hours in total (about 14 until Step 6's searches), on timings measured on a laptop:

| Part | Runs | Roughly |
| --- | --- | --- |
| Calibration | 36 cells, 1 run, 60 s each | 1.4 h |
| PostgreSQL `on` | 108 | 6 h |
| PostgreSQL `off`, create and update only | 36 | 2 h |
| Cassandra | 108 once Step 6 lands; until then 54, with the 18 search cells recorded as 501 | 6.3 h (3.3 h before Step 6) |
| LWT steady state, both backends | 3 runs of 600 s, one load | 1.1 h |

A restore and boot takes about 10 s on PostgreSQL and 21 s on Cassandra. With `--reset reload` the matrix takes about 27 hours.

Per run with reloading, on top of 60 s of warm-up and 120 s measured: PostgreSQL 40 s of reset and settle at 10,000 resources and 3 to 6 minutes at 100,000, where loading dominates; Cassandra about 70 s of reset and boot, and 45 s of load at 100,000. Add `--resume` to continue a part that stopped, and split the parts over several nights if the machine's window needs it.

## An overnight dry run on a laptop

**Two blocks, from the root of a checkout that has `docs/Cassandra/datasets/` (PR #75).** The first takes about 15 minutes, 8 of them the build. Run it any time before bed:

```bash
cargo build --release -p helios-hfs --features cassandra,postgres
python3 docs/Cassandra/benchmark/bench.py fetch-k6 --dest ~/hfs-bench/tools
python3 docs/Cassandra/datasets/generate_datasets.py --out-dir ~/hfs-bench/datasets
```

The second, at bedtime, on mains power with the lid open (or closed only with an external display):

```bash
python3 docs/Cassandra/benchmark/bench.py overnight --hours 8 --dataset-dir ~/hfs-bench/datasets --k6 ~/hfs-bench/tools/k6-v2.3.0-macos-arm64/k6
```

`bench.py overnight` runs the matrix unattended within a time budget. **These are laptop numbers, for timings and bugs, and never go in the report** (ADR-aacruzgon-0002): every run is labelled `laptop-dry-run`, and the output folder holds `NOT-FOR-REPORT.txt`.

`caffeinate -dimsu -w <pid>` keeps the Mac awake while the run lasts (`-d` keeps the display on too; it's harmless, and dimming the screen by hand does not stop the run). The harness uses the 3 GiB profile, the only one that fits an 8.8 GiB Docker VM, and counts the shared dev node's 3 GiB without touching it.

Before starting it checks the datasets against `SHA256SUMS`, the release `hfs`, the pinned k6, the Docker budget and that the HFS port is free, and refuses if any fails. It runs these parts in order, one run at a time. It starts a run only if that run can finish inside the budget, and a steady-state part only if all its runs can:

1. Calibration (PostgreSQL, closed-loop, 60 s per cell, written to `rates-laptop-dry-run.json` in the output folder, never to `rates.json`).
2. PostgreSQL `on` at 10,000, then at 100,000.
3. Cassandra at 10,000, then at 100,000.
4. PostgreSQL `off`, create and update.
5. LWT steady state, on each backend (three no-reset runs of `--lwt-duration`, 600 s by default).

Results go to `~/hfs-bench/<date>/` (`--out` to change it) and nothing is committed.

**To stop it:** Ctrl-C, `kill <pid>`, or closing the terminal. Each stops HFS and k6, takes the database container down, saves the state and writes the morning summary; continue with the summary's `--resume` command. `--resume` refuses to start while something else holds the HFS port, and names it. The progress is also in `overnight.log` in the output folder.

**In the morning,** read `morning-summary.md`: what ran, how long each part took, when and why it stopped against the budget, failed, busy-host and saturated runs, and the exact `--resume` command for what's left. Snapshots stay in Docker until the matrix completes; a `run` removes only those it built.

## Reading a smoke run

To try it on a laptop, use a fixed rate and short runs. Numbers from this are for checking that the harness works and go nowhere:

```bash
python3 docs/Cassandra/benchmark/bench.py run --backend postgres --profile 3g --ops create,read --concurrency 8 --sizes 10000 --runs 2 --duration 20 --warmup 5 --rate 50 --k6 $K6 --dataset-dir $DATA --out /tmp/smoke
```

## Not in this harness

- Step 8's named cases other than `hotkey` (the mass-update tombstone case, the broad `_has` case). The mass-update case waits for bug #74's fix.
- Tombstones scanned per read are in `tablestats.txt` (`Average tombstones per slice`). Batch sizes (Step 8) are not recorded.
- Comparison with upstream's published numbers. Upstream's `fhir-benchmark.yml` stays disabled (see the ADR for this row).
