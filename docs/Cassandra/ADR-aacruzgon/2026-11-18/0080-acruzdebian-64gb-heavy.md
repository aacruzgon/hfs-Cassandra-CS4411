# ADR-aacruzgon-0080: `acruzdebian` has 64 GB: its runner cap goes to 40G, it takes heavy jobs, and the clippy pair sets its cargo job count from the runner's cap

- **Date:** 2026-11-18
- **Status:** proposed
- **Context:**
    - **`acruzdebian` now has 64 GB of RAM** (`MemTotal` 65,773,824 kB), up from the 15 GB that ADR-aacruzgon-0023 sized it for. Its runner service was still capped at `MemoryMax=9G`, and it still had only `hfs-cassandra-light`.
    - **The heavy queue still waited for `sam-7900x`.** On 2026-11-18 the two clippy checks and `Cassandra tests (all FHIR versions)` of several runs from 2026-11-17 sat queued for `hfs-cassandra-heavy` while `sam-7900x` was off, and every light job of the same runs had already passed on `acruzdebian`. So no pull request could merge without Sam's rig or an admin bypass (ADR-aacruzgon-0019, 0023).
    - **The clippy split holds because of `sam-7900x`'s 20G cap.** `helios-fhir`'s lib and lib-test units with `--all-features` need about 12.8 GB each, so `cargo clippy (helios-fhir)` builds them one at a time (`CARGO_BUILD_JOBS: 1`), and `cargo clippy (workspace)` uses 8 jobs (ADR-sam-michaels-0003). Heavy jobs can land on either runner once both carry the label, so the workflow cannot simply raise those numbers.
    - **The machine runs another project's runner.** `actions.runner.aacruzgon-cs4471-dynamism.alan-debian-host-b.service` (user `dynamism-runner`) is capped at `MemoryMax=2G`. The Cassandra container (3 GiB, ADR-aacruzgon-0002) runs outside this runner's cap, in `user@1005.service`.
- **Decision:**
    - **The runner service's cap goes from 9G to 40G.** The drop-in `memory.conf` keeps `MemorySwapMax=0` and `OOMPolicy=continue`; only `MemoryMax` changes. With the other runner's 2G and the 3 GiB container, CI can use about 45 GiB at once, which leaves about 18 GiB for my desktop.
    - **`acruzdebian` gets `hfs-cassandra-heavy`.** Both runners now take both labels. It still never gets the bare `hfs-cassandra`.
    - **The light and heavy labels and the "more than about 9 GB is heavy" rule stay** (ADR-aacruzgon-0023). No runner needs them today, but a smaller runner added later could take light jobs only without any job changing its label. Since either runner may take any job, every job must fit the smaller cap, `sam-7900x`'s 20G.
    - **Each clippy job picks `CARGO_BUILD_JOBS` from the cap of the runner it lands on.** A step reads `memory.max` of the job's own cgroup (the runner service's, which `ci-report.py` already reads) and passes the count to the clippy step as a step output:

      | Job | Cap under 32 GiB, unlimited or unreadable (`sam-7900x`) | Cap of 32 GiB or more (`acruzdebian`) |
      | --- | --- | --- |
      | `cargo clippy (helios-fhir)` | 1, as before | 2: the lib and lib-test units build side by side |
      | `cargo clippy (workspace)` | 8, as before | 16, `acruzdebian`'s thread count |

      No job's `name:` changes, so the required checks stay as they are. The step `cargo clippy -p helios-fhir --all-targets --all-features, one job at a time` loses its last words.
- **Alternatives considered:**
    - **Keep `acruzdebian` light, with only the bigger cap:** lost. The light jobs already fit under 9G, so the memory would go unused, and merges would still need `sam-7900x`.
    - **A cap of 20G, the same as `sam-7900x`:** lost. A peak measured on one machine would then hold on the other, but the clippy pair could not build faster on `acruzdebian`, which is the only gain the extra memory gives a job that must also fit 20G.
    - **A cap of 32G:** lost. It is just enough for the two 12.8 GB units side by side, with too thin a margin once the rest of the build is added.
    - **Run the `CONTRIBUTING.md` clippy command as one job on an `acruzdebian`-only label:** lost. It would make `acruzdebian` the only machine that can run a required check, which is the single point of failure ADR-aacruzgon-0023 removed. It would also rename required checks.
    - **A second runner instance on `acruzdebian`:** not taken now. Two instances on one machine would share `CASSANDRA_HOST_PORT` (`fhir-versions.yml` and `cassandra-coverage.yml` both use 59242), the Compose project names and the persistent target directories, which all assume one job at a time per machine.
    - **Move `coverage.yml` from GitHub-hosted runners to the self-hosted ones:** not taken in this change. It would put the Codecov token on a personal desktop and is its own decision.
- **Consequences:**
    - **Every required check runs while either machine is on.** A merge no longer needs `sam-7900x`.
    - **Light jobs can wait behind heavy ones on `acruzdebian`.** When `sam-7900x` is off, a cold clippy pair or the all-versions test job holds `acruzdebian`'s single executor, and the light checks of the next push queue behind them.
    - **Each machine keeps its own warm `target/clippy` and `target/fhir-versions-all`.** The first heavy run on `acruzdebian` is cold; disk use there grows by up to the 30 GB and 25 GB caps (ADR-sam-michaels-0002), out of about 674 GB free.
    - **The same commit can lint with a different job count on each machine.** The count changes only how many units cargo builds at once, not what clippy checks, so the result is the same; the CI report records it (`cargo_build_jobs` in `ci-report.py`'s build section).
    - **To measure:** the clippy pair's peak `anon` on `acruzdebian` with 2 and 16 jobs, cold and warm, against the 40 GiB cap. If `cargo clippy (workspace)` comes within 5 GiB of the cap, its count goes back to 8.
    - **Changing a runner's cap now changes its clippy job count.** The 32 GiB threshold is in the two steps in `fmt-clippy-cassandra.yml`; a third runner gets 1 and 8 below it.
- **AI involvement:**
    - **Tool and model:** Claude Code (Opus 5.5).
    - **What the AI proposed:**
        - the 40G cap, against the other two caps it offered (20G and 32G);
        - reading the cap from the job's own cgroup, and the 32 GiB threshold with 2 and 16 jobs above it;
        - keeping the light and heavy labels and the 9 GB rule;
        - the script that raises the cap without restarting the runner, and checks the cgroup;
        - the two alternatives not taken (a second instance, moving `coverage.yml`).
    - **What the human changed:** I chose the 40G cap, and to have `acruzdebian` take heavy jobs and to scale the clippy job counts. I left moving `coverage.yml` off GitHub-hosted runners for later. I ran the `sudo` step; the AI added the label through the API once I had logged `gh` in.
    - **Verified:**
        - before the change, on `acruzdebian`: `systemctl show` gave `MemoryMax=9663676416`, `MemorySwapMax=0`, `OOMPolicy=continue`; the runner's `Runner.Listener` sits in `/system.slice/actions.runner.aacruzgon-hfs-Cassandra-CS4411.acruzdebian.service`, whose `memory.max` read 9663676416; the other runner's service showed `MemoryMax=2147483648`;
        - the queued jobs, read from the Actions API on 2026-11-18: every `hfs-cassandra-heavy` job queued with no runner, every light job of the same runs completed on `acruzdebian`;
        - the changed workflows parse (PyYAML), and `shellcheck -s bash` passes on both new steps; `actionlint` was not run locally;
        - both new steps, run locally against a stand-in cgroup: `memory.max` of 9 GiB, 20 GiB and 32 GiB minus one byte gives 1 and 8 jobs; exactly 32 GiB and 40 GiB give 2 and 16; `max` and a missing file give 1 and 8; against the real runner cgroup, still at 9G, it printed `memory.max 9663676416: CARGO_BUILD_JOBS=1`;
        - after the change: `systemctl show` reports `MemoryMax=42949672960`, `MemorySwapMax=0`, `OOMPolicy=continue`, `User=gh-runner`, `ActiveState=active`, with no restart; the service cgroup's `memory.max` is 42949672960 and `memory.swap.max` 0; the old drop-in is kept as `memory.conf.9G.bak`, which systemd ignores (only `*.conf` is read); against that cgroup the two steps print `CARGO_BUILD_JOBS=2` and `CARGO_BUILD_JOBS=16`;
        - `gh api .../actions/runners` after `POST .../runners/22/labels`: `acruzdebian` online with `self-hosted, Linux, X64, hfs-cassandra-light, hfs-runner-acruzdebian, hfs-cassandra-heavy`; `sam-7900x` offline, labels unchanged;
        - *pending:* the PR's own run, with each clippy job's runner, `CARGO_BUILD_JOBS`, wall time and peak `anon`.
- **Links:**
    - ADR-aacruzgon-0023 (amended), 0018 and 0019 (update notes added), 0002.
    - ADR-sam-michaels-0002, 0003.
    - `.github/workflows/fmt-clippy-cassandra.yml`, `fhir-versions.yml`, `cassandra-coverage.yml`.
    - `development-roadmap.md` section 1 ("Machines") and the Step 0 CI bullet; `CLAUDE.md` and `AGENTS.md` "Commits and CI"; `.coderabbit.yaml`'s CI rules.
