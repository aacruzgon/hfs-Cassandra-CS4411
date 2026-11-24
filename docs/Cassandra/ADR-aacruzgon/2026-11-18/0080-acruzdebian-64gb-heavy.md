# ADR-aacruzgon-0080: `acruzdebian` has 64 GB: its runner cap goes to 40G, it takes heavy jobs, and `cargo clippy (helios-fhir)` sets its cargo job count from the runner's cap

- **Date:** 2026-11-18
- **Status:** accepted; amended by [ADR-aacruzgon-0082](../2026-11-20/0082-mutation-flakiness-medium-tier.md) (the jobs it called heavy fit 20G and move to `hfs-cassandra-medium`; `hfs-cassandra-heavy` comes off `sam-7900x`, so heavy means `acruzdebian` only); amended by [ADR-aacruzgon-0086](../2026-11-24/0086-acruzdebian-second-runner-instance.md) (`acruzdebian`'s 40G is split into two runner instances of 20G each, so `cargo clippy (helios-fhir)` picks 1 job there too)
- **Context:**
    - **`acruzdebian` now has 64 GB of RAM** (`MemTotal` 65,773,824 kB), up from the 15 GB that ADR-aacruzgon-0023 sized it for. Its runner service was still capped at `MemoryMax=9G`, and it still had only `hfs-cassandra-light`.
    - **The heavy queue still waited for `sam-7900x`.** On 2026-11-18 the two clippy checks and `Cassandra tests (all FHIR versions)` of several runs from 2026-11-17 sat queued for `hfs-cassandra-heavy` while `sam-7900x` was off, and every light job of the same runs had already passed on `acruzdebian`. So no pull request could merge without Sam's rig or an admin bypass (ADR-aacruzgon-0019, 0023).
    - **The clippy split holds because of `sam-7900x`'s 20G cap.** `helios-fhir`'s lib and lib-test units with `--all-features` need about 12.8 GB each, so `cargo clippy (helios-fhir)` builds them one at a time (`CARGO_BUILD_JOBS: 1`), and `cargo clippy (workspace)` uses 8 jobs (ADR-sam-michaels-0003). Heavy jobs can land on either runner once both carry the label, so the workflow cannot simply raise those numbers.
    - **The machine runs another project's runner.** `actions.runner.aacruzgon-cs4471-dynamism.alan-debian-host-b.service` (user `dynamism-runner`) is capped at `MemoryMax=2G`. The Cassandra container (3 GiB, ADR-aacruzgon-0002) runs outside this runner's cap, in `user@1005.service`.
- **Decision:**
    - **The runner service's cap goes from 9G to 40G.** The drop-in `memory.conf` keeps `MemorySwapMax=0` and `OOMPolicy=continue`; only `MemoryMax` changes. With the other runner's 2G and the 3 GiB container, CI can use about 45 GiB at once, which leaves about 18 GiB for my desktop.
    - **`acruzdebian` gets `hfs-cassandra-heavy`.** Both runners now take both labels. It still never gets the bare `hfs-cassandra`.
    - **The light and heavy labels and the "more than about 9 GB is heavy" rule stay** (ADR-aacruzgon-0023). No runner needs them today, but a smaller runner added later could take light jobs only without any job changing its label. Since either runner may take any job, every job must fit the smaller cap, `sam-7900x`'s 20G.
    - **`cargo clippy (helios-fhir)` picks `CARGO_BUILD_JOBS` from the cap of the runner it lands on.** A step reads `memory.max` of the job's own cgroup (the runner service's, which `ci-report.py` already reads) and passes the count to the clippy step as a step output: 2 at a cap of 32 GiB or more (`acruzdebian`), so the lib and lib-test units build side by side; 1, as before, under 32 GiB or when the cap is unlimited or unreadable (`sam-7900x`). Cold on `acruzdebian`, 2 jobs took 160 s instead of 286 s, at a peak of 26.0 GiB `anon`.
    - **`cargo clippy (workspace)` keeps a fixed 8 on every runner.** The first version of this change gave it the same step, with 16 jobs (`acruzdebian`'s thread count) at 32 GiB or more. Cold, 16 took 236 s against 241 s with 8, at the same 13.4 GiB peak `anon`, so the step was removed again.
    - No job's `name:` changes, so the required checks stay as they are. The step `cargo clippy -p helios-fhir --all-targets --all-features, one job at a time` loses its last words.
- **Alternatives considered:**
    - **Keep `acruzdebian` light, with only the bigger cap:** lost. The light jobs already fit under 9G, so the memory would go unused, and merges would still need `sam-7900x`.
    - **A cap of 20G, the same as `sam-7900x`:** lost. A peak measured on one machine would then hold on the other, but the clippy pair could not build faster on `acruzdebian`, which is the only gain the extra memory gives a job that must also fit 20G.
    - **A cap of 32G:** lost. It is just enough for the two 12.8 GB units side by side, with too thin a margin once the rest of the build is added.
    - **Run the `CONTRIBUTING.md` clippy command as one job on an `acruzdebian`-only label:** lost. It would make `acruzdebian` the only machine that can run a required check, which is the single point of failure ADR-aacruzgon-0023 removed. It would also rename required checks.
    - **A second runner instance on `acruzdebian`:** not taken now. Two instances on one machine would share `CASSANDRA_HOST_PORT` (`fhir-versions.yml` and `cassandra-coverage.yml` both use 59242), the Compose project names and the persistent target directories, which all assume one job at a time per machine. *Update 2026-11-24:* taken in ADR-aacruzgon-0086, with each instance at 20G. The persistent target directories turned out to be per instance already (each runner's own `_work`). The ports and Compose projects get a per-instance slot, `HFS_CI_SLOT`, and every CI port moves below 32768. rustup, `taiki-e/install-action`'s temporary directory and the weekly prune were the other shared parts.
    - **Move `coverage.yml` from GitHub-hosted runners to the self-hosted ones:** not taken in this change. It would put the Codecov token on a personal desktop and is its own decision.
- **Consequences:**
    - **Every required check runs while either machine is on.** A merge no longer needs `sam-7900x`.
    - **Light jobs can wait behind heavy ones on `acruzdebian`.** When `sam-7900x` is off, a cold clippy pair or the all-versions test job holds `acruzdebian`'s single executor, and the light checks of the next push queue behind them.
    - **Each machine keeps its own warm `target/clippy` and `target/fhir-versions-all`.** The first heavy run on `acruzdebian` is cold; disk use there grows by up to the 30 GB and 25 GB caps (ADR-sam-michaels-0002), out of about 674 GB free.
    - **The same commit can lint with a different job count on each machine.** The count changes only how many units cargo builds at once, not what clippy checks, so the result is the same; the CI report records it (`cargo_build_jobs` in `ci-report.py`'s build section).
    - **`cargo clippy (helios-fhir)` has about 14 GiB of margin on `acruzdebian`** (26.0 GiB `anon` under 40 GiB). If a cold run comes within 5 GiB of the cap, its count goes back to 1.
    - **`cargo clippy (workspace)` is bound by its slowest unit, not by its job count.** Doubling the jobs saved 5 s, so a faster workspace job needs a smaller `helios-fhir` dependency or a warm target directory, not more memory.
    - **Changing a runner's cap now changes its `helios-fhir` clippy job count.** The 32 GiB threshold is in one step in `fmt-clippy-cassandra.yml`; a third runner gets 1 below it.
- **AI involvement:**
    - **Tool and model:** Claude Code (Opus 5.5).
    - **What the AI proposed:**
        - the 40G cap, against the other two caps it offered (20G and 32G);
        - reading the cap from the job's own cgroup, and the 32 GiB threshold with 2 and 16 jobs above it; then, from the cold run, removing the workspace job's step again;
        - keeping the light and heavy labels and the 9 GB rule;
        - the script that raises the cap without restarting the runner, and checks the cgroup;
        - the two alternatives not taken (a second instance, moving `coverage.yml`).
    - **What the human changed:** I chose the 40G cap, and to have `acruzdebian` take heavy jobs and to scale the clippy job counts. I left moving `coverage.yml` off GitHub-hosted runners for later. I ran the `sudo` step; the AI added the label through the API once I had logged `gh` in.
    - **Verified:**
        - before the change, on `acruzdebian`: `systemctl show` gave `MemoryMax=9663676416`, `MemorySwapMax=0`, `OOMPolicy=continue`; the runner's `Runner.Listener` sits in `/system.slice/actions.runner.aacruzgon-hfs-Cassandra-CS4411.acruzdebian.service`, whose `memory.max` read 9663676416; the other runner's service showed `MemoryMax=2147483648`;
        - the queued jobs, read from the Actions API on 2026-11-18: every `hfs-cassandra-heavy` job queued with no runner, every light job of the same runs completed on `acruzdebian`;
        - the changed workflows parse (PyYAML), and `shellcheck -s bash` passes on both new steps (the workspace job's was later removed); `actionlint` was not run locally;
        - both new steps, run locally against a stand-in cgroup: `memory.max` of 9 GiB, 20 GiB and 32 GiB minus one byte gives 1 and 8 jobs; exactly 32 GiB and 40 GiB give 2 and 16; `max` and a missing file give 1 and 8; against the real runner cgroup, still at 9G, it printed `memory.max 9663676416: CARGO_BUILD_JOBS=1`;
        - after the change: `systemctl show` reports `MemoryMax=42949672960`, `MemorySwapMax=0`, `OOMPolicy=continue`, `User=gh-runner`, `ActiveState=active`, with no restart; the service cgroup's `memory.max` is 42949672960 and `memory.swap.max` 0; the old drop-in is kept as `memory.conf.9G.bak`, which systemd ignores (only `*.conf` is read); against that cgroup the two steps print `CARGO_BUILD_JOBS=2` and `CARGO_BUILD_JOBS=16`;
        - `gh api .../actions/runners` after `POST .../runners/22/labels`: `acruzdebian` online with `self-hosted, Linux, X64, hfs-cassandra-light, hfs-runner-acruzdebian, hfs-cassandra-heavy`; `sam-7900x` offline, labels unchanged;
        - **The first heavy jobs on `acruzdebian`,** from `ci-report-clippy-*` (`build-clippy-*.json`). The queued runs from before this PR ran first, with `main`'s workflow and its fixed counts:

          | Run | Job | `CARGO_BUILD_JOBS` | Units built | Wall time | Peak `anon` | Peak `memory.current` | OOM kills |
          | --- | --- | --- | --- | --- | --- | --- | --- |
          | 44156440683 (`main`), cold | `cargo clippy (helios-fhir)` | 1 | 252 | 285.9 s | 13,744 MiB | 34,003 MiB | 0 |
          | 44156440683 (`main`), cold | `cargo clippy (workspace)` | 8 | 1,341 | 241.2 s | 13,782 MiB | 40,959 MiB | 0 |
          | 44243120084 (this PR), warm | `cargo clippy (helios-fhir)` | 2 | 0 | 0.2 s | 117 MiB | 28,175 MiB | 0 |
          | 44243120084 (this PR), warm | `cargo clippy (workspace)` | 16 | 0 | 0.7 s | 120 MiB | 34,290 MiB | 0 |

          On this PR's run both jobs landed on `acruzdebian`, read `memory.max` 42949672960, and picked 2 and 16; all four required checks of the workflow passed. The target directory was already warm from the earlier runs, so cargo built nothing, and this run measures neither the time nor the memory at 2 and 16 jobs. The cold build at 1 and 8 jobs peaked at 13.4 GiB `anon`, so the cap that held it back on `sam-7900x` was 20G, not the machine. `memory.current` reaching the 40 GiB cap is page cache, which the kernel reclaims; `anon` stayed 26 GiB under it;
        - **The cold run at 2 and 16 jobs,** run 44251053616 on this PR, after I emptied `acruzdebian`'s `target/clippy` (2.8 GB) by hand; the reset step reported "no stamp: a new or emptied directory":

          | Job | `CARGO_BUILD_JOBS` | Units built | Wall time | Peak `anon` | Peak `memory.current` | OOM kills |
          | --- | --- | --- | --- | --- | --- | --- |
          | `cargo clippy (helios-fhir)` | 2 | 252 | 159.8 s (285.9 s with 1) | 26,662 MiB (13,744 with 1) | 40,960 MiB | 0 |
          | `cargo clippy (workspace)` | 16 | 1,341 | 236.0 s (241.2 s with 8) | 13,766 MiB (13,782 with 8) | 29,639 MiB | 0 |

          Two jobs nearly doubled `helios-fhir`'s peak, as two 12.8 GB units side by side would, and cut its wall time by 44 %. Sixteen jobs changed neither the workspace job's time nor its peak, which is why that job is back to 8.
- **Links:**
    - ADR-aacruzgon-0023 (amended), 0018 and 0019 (update notes added), 0002.
    - ADR-sam-michaels-0002, 0003.
    - `.github/workflows/fmt-clippy-cassandra.yml`, `fhir-versions.yml`, `cassandra-coverage.yml`.
    - `development-roadmap.md` section 1 ("Machines") and the Step 0 CI bullet; `CLAUDE.md` and `AGENTS.md` "Commits and CI"; `.coderabbit.yaml`'s CI rules.
