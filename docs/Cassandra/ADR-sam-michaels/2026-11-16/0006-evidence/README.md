# Evidence for ADR-sam-michaels-0006

Scratch scripts and outputs behind the numbers in `../0006-step-5-design.md`. All runs were on the single-node dev node (Cassandra 5.0.9) on 2026-11-12 and 13. Nothing here is built or run by CI. Three-node figures were not measured (Steps 5 and 8 do that).

## Files

| File | What it is |
| --- | --- |
| `iwbench/` | A standalone cargo crate (empty `[workspace]`, not a member of the repository's workspace). `src/main.rs` runs the shared extractor over a file of resources and reports key-set sizes, rows per table, batch sizes and the values the P5 drops remove. `src/bin/latency.rs` and `src/bin/throughput.rs` time ways of sending one write's index rows. |
| `iwbench-out.txt` | Output of `main.rs` on the three fixtures and the 10k Synthea dataset, first run (extraction timed in a debug build). |
| `iwbench-opt.txt` | Output of `main.rs` again with `opt-level = 2` for the extractor's dependencies, and with the extra batch-packing lines for options A, B and B'. The row and drop counts are the same as in `iwbench-out.txt`. Its "option B (pairs kept together)" line is pair-only packing, not 0006's whole partitions (see `iw11.md`, update of 2026-11-18). |
| `iwbench-whole-partitions.txt` | Added 2026-11-18. Output of `main.rs` after it gained the "update option B, whole partitions" line, which packs whole partitions as 0006 chose (IW9, IW11), on the same files. Every other line matches `iwbench-opt.txt` except the extraction times. Run with the 10k dataset at the SHA-256 in `docs/Cassandra/datasets/SHA256SUMS`. |
| `latency-out.txt` | Output of `latency.rs`: p50, p95 and p99 per scenario for 1, 8, 16, 32 and 64 rows per write, 300 samples each. |
| `throughput-out.txt` | Output of `throughput.rs`: writes per second for 32 writers of 500 writes each, 16 rows per write, in two passes (forward, then reverse order). |
| `iw11.md` | The research note on how a batch's size is measured (IW11), with the node-formula checks. |
| `sw/exp.py` | The sweeper experiments (`tombstones`, `bounds`, `ttl`, `latency`, `load`). |

## How to run

- **`iwbench` extraction report.** From this directory: `cargo run --manifest-path iwbench/Cargo.toml -- <file>...`, where each file is a JSON array, a bundle-like JSON object or an `.ndjson` file of resources. It reads the SearchParameter specs from the repository's `data/` directory (the path is relative to the crate, six levels up). The fixtures are `crates/persistence/tests/fixtures/{patients,observations,organizations}.json`. The 10k dataset is `patient-observation-10000.ndjson`, which is **not committed** (10 MB). Its recipe, generator and checksums are in `docs/Cassandra/datasets/` (`generate_datasets.py`, `README.md`, `SHA256SUMS`); the outputs here used a copy at `datasets/patient-observation-10000.ndjson`.
- **`latency` and `throughput`.** `cargo run --manifest-path iwbench/Cargo.toml --bin latency` (and `--bin throughput`, with optional `PER`, `REVERSE` and `BASE` environment variables). They connect to `127.0.0.1:59042` (the dev node's published port) and use the keyspace `item5_iw`, which must hold a `token_code_index` table shaped like the repository's. The keyspace was created by hand and is not scripted here.
- **`sw/exp.py`.** Copy it into the Cassandra container and run `python3 exp.py <tombstones|bounds|ttl|latency|load [seconds rate poll_s grace_s]>` there. It uses the driver bundled with Cassandra and connects to `127.0.0.1:9042`. It writes only to its own keyspace `item5_sw`, never `helios`; it does not create the keyspace, so create `item5_sw` and the sweeper's outbox table first.

## What each number in 0006 comes from

See the "Evidence" paragraph in 0006's Context. Numbers that are chosen budgets, not measurements: RV1's 64 keys, RV5's 32 resources, 512 rows and 4 at a time, and IW16's 10 and 20 ms pauses (the pauses reuse ADR-aacruzgon-0034's).
