# ADR-aacruzgon-0023: CI splits into light and heavy jobs, and `acruzdebian` joins as a second runner for light jobs only

- **Date:** 2026-11-04
- **Status:** accepted
- **Context:**
    - **One runner meant no CI whenever it was off.** Every job used `runs-on: [self-hosted, hfs-cassandra]`, which only `sam-7900x` carried (ADR-sam-michaels-0001). It is Sam's own desktop, and on 2026-11-04 it was offline: the five required checks (ADR-aacruzgon-0019, 0021) on PR #28 and the nightly canary sat queued, and no pull request could merge.
    - **The second machine is smaller.** `acruzdebian` is my desktop: Debian 13 (trixie), x86_64, AMD Ryzen 7 5800X with 16 threads, 15 GB of RAM, 674 GB free on `/`, Docker 26.1.5 from Debian's packages, AppArmor rather than SELinux.
    - **The clippy pair does not fit in 15 GB.** On `sam-7900x`, `cargo clippy (helios-fhir)` peaked at 14.8 GiB and `cargo clippy (workspace)` at 17.3 GiB, cold (ADR-sam-michaels-0003). The other jobs are far smaller: fmt builds nothing, the R4B check is one crate's `cargo check`, the canary is the same check on a merged tree, and the Cassandra job only runs `cqlsh` against a container limited to 3 GiB.
    - **A label that two runners share reaches only one of them.** GitHub gives a job to one idle runner that matches its labels, so the weekly `runner-docker-prune.yml` on a shared label would prune only one machine's Docker.
    - **This amends ADR-aacruzgon-0018,** which put all CI on one machine, Sam's 32 GB desktop. It also accepts, for `acruzdebian`, the risk that 0018 held against the team laptop: a personal desktop can be off or asleep. Suspend is disabled on `acruzdebian` (the sleep, suspend and hibernate targets are masked), and while either machine is on, the light checks run.
    - **The machine already ran three GitHub runners for another project.** The `ci-runner` user (uid 1004) ran `rig-1`, `rig-2` and `rig-3` for `aacruzgon/GreenLens`, each with its own rootless Docker and buildkitd. They would have shared the 15 GB with this runner.
- **Decision:**
    - **Jobs are light or heavy, by label.**

      | Job | Label |
      | --- | --- |
      | `cargo fmt --check` | `hfs-cassandra-light` |
      | `cargo check (cassandra, R4B)` | `hfs-cassandra-light` |
      | `Cassandra schema (single node)` | `hfs-cassandra-light` |
      | `upstream canary (merge + cassandra,R4B check)` | `hfs-cassandra-light` |
      | `cargo clippy (helios-fhir)` | `hfs-cassandra-heavy` |
      | `cargo clippy (workspace)` | `hfs-cassandra-heavy` |

      No job's `name:` changes, so the required checks stay as they are.
    - **`sam-7900x` takes light and heavy jobs; `acruzdebian` takes light jobs only.**
        - `sam-7900x` gained `hfs-cassandra-light`, `hfs-cassandra-heavy` and `hfs-runner-sam-7900x` through the API, and keeps `hfs-cassandra`.
        - `acruzdebian` has `hfs-cassandra-light` and `hfs-runner-acruzdebian`, plus the automatic `self-hosted, Linux, X64`. It never gets `hfs-cassandra` or `hfs-cassandra-heavy`, so neither a heavy job nor an old workflow that still asks for `hfs-cassandra` can land on it.
    - **Each runner has a per-machine label, `hfs-runner-<name>`,** and `runner-docker-prune.yml` is a matrix over those labels with `fail-fast: false`, so each machine prunes its own Docker and one machine's prune failing does not cancel the other's.
    - **`acruzdebian` is isolated the way `sam-7900x` is** (ADR-sam-michaels-0001, 0004):
        - a dedicated user `gh-runner` (uid 1005): no password, in no group but its own (not `sudo`, not `docker`), home `0700`;
        - its own rootless Docker daemon, set up with Debian `docker.io`'s `dockerd-rootless-setuptool.sh`, with `slirp4netns` and the `builtin` port driver, reached through `DOCKER_HOST=unix:///run/user/1005/docker.sock` in the runner's `.env`;
        - linger on, and cgroup delegation of `cpu cpuset io memory pids` to its user manager, so container `--cpus` and `--memory` limits are enforced;
        - its own rustup stable (1.99.1) with rustfmt and clippy, `uv` 0.13.7 and `cqlsh` 6.2.2 (`uv tool install`), all under `/home/gh-runner`; `libssl-dev` installed system-wide for `openssl-sys`.
    - **The runner is the systemd service `actions.runner.aacruzgon-hfs-Cassandra-CS4411.acruzdebian.service`** (runner v2.338.2), capped in the drop-in `memory.conf` at `MemoryMax=9G`, `MemorySwapMax=0`, `OOMPolicy=continue`: the same three settings as `sam-7900x`'s 20 GB cap (ADR-sam-michaels-0003), sized for 15 GB.
    - **The GreenLens runners are stopped and do not start at boot.** Their three user services and `ci-runner`'s rootless Docker are disabled, and `ci-runner`'s linger is off. The `ci-runner` user and its home are kept.
- **Alternatives considered:**
    - **Light jobs only on `acruzdebian`, heavy only on `sam-7900x`:** lost. The light checks would stop whenever my desktop is down, which only moves the single point of failure. With both runners on the light label, a light check runs as long as either machine is on.
    - **Run the runner as my own account, `acruzgon`:** lost. It contradicts ADR-aacruzgon-0018 ("a dedicated non-admin user"). `acruzgon` is in `sudo` and `docker`, so any job would be root-equivalent and could read my SSH signing key and `gh` token, the reasons ADR-sam-michaels-0001 moved Sam's runner off his own account.
    - **Give `acruzdebian` the existing `hfs-cassandra` label and move only clippy to a new label:** lost. Any workflow, branch or restored file that still says `hfs-cassandra`, including the clippy jobs on an old branch, could then land on the 15 GB machine.
    - **One shared label for the prune:** lost. It reaches only one runner per run.
    - **The system Docker daemon for CI:** lost, as in ADR-sam-michaels-0001. Membership in `docker` is root-equivalent, and CI's Compose project would share a daemon with my own containers.
- **Consequences:**
    - **Light checks run while either machine is on.** Pull requests that touch no Rust still wait for the two clippy checks, which only `sam-7900x` runs, so a merge still needs Sam's rig, or an admin bypass (ADR-aacruzgon-0019).
    - **Each machine keeps its own warm target directories.** A light job lands on whichever runner is idle, so the R4B check and the canary can be cold on one machine while warm on the other. The 10 GB caps (ADR-sam-michaels-0002) now apply per machine.
    - **The Cassandra container is outside the 9 GiB cap.** As on `sam-7900x`, `gh-runner`'s rootless Docker runs in its user manager (`user@1005.service`), not in the runner service, so the container's own 3 GiB limit (ADR-aacruzgon-0002) is what bounds it. CI can use up to about 12 GiB of the 15 GB at once.
    - **To revisit at Step 2.** When the Cassandra job starts compiling and running `tests/cassandra_tests.rs` (ADR-aacruzgon-0020), it compiles under the 9 GiB cap while the 3 GiB container is running. Measure its peak on `acruzdebian` then, and move it to `hfs-cassandra-heavy` if it does not fit.
    - **A job that may need more than about 9 GB is heavy.** New jobs pick a label by that rule; no new job uses the bare `hfs-cassandra` label. `CLAUDE.md` and `AGENTS.md` ("Commits and CI") say so.
    - **Upstream's `[self-hosted, Linux]` jobs match both runners now** (roadmap "Track upstream weekly", ADR-aacruzgon-0022), so the Sunday check for new workflow files matters as before, for two machines instead of one.
    - **Rust follows stable on its own; the rest is manual.** Every Rust job runs `dtolnay/rust-toolchain` with `toolchain: stable`, which installs a new stable release on the first job after it ships. The two runners can briefly build with different `rustc`, and each machine's target directories reset once, by their stamp. `cqlsh`, `uv` and rustup itself are updated by hand, as `gh-runner` through `sudo`, on each machine.
    - **The R4B check's margin on `acruzdebian` is about 2.6 GiB.** It runs with cargo's default job count (16 here) and peaked at 6.35 GiB under the 9 GiB cap. If it grows, the fix is `CARGO_BUILD_JOBS` in that job, as ADR-sam-michaels-0003 did for clippy.
    - **The runner updates itself.** It was registered without `--disableupdate`, as on `sam-7900x`.
- **AI involvement:**
    - **Tool and model:** Claude Code (Opus 5.5).
    - **What the AI proposed:**
        - the setup scripts (`root.sh`, `user.sh`, a `finish.sh`) and their built-in checks;
        - installing `libssl-dev`, which the R4B check's `openssl-sys` needed and the machine lacked;
        - passing the registration token on stdin and through `ACTIONS_RUNNER_INPUT_TOKEN`, so it is never on a command line;
        - the matrix with `fail-fast: false` for the prune;
        - the "more than about 9 GB is heavy" rule;
        - the Step 2 caveat for the Cassandra job;
        - the per-second memory sampling of the runner cgroup and `user@1005.service`.
    - **What the human changed:** I decided the split, the label scheme (light and heavy, never `hfs-cassandra` on `acruzdebian`), the isolation, the 9G cap and the two rejected alternatives before the session. During it, I identified the existing runners as GreenLens's and had them stopped and kept from starting at boot. I ran every `sudo` step. The AI's first script stopped after `config.sh` on a missing line continuation in `user.sh`; `finish.sh` ran the remaining steps.
    - **Verified:**
        - **`gh-runner`:**
            - `id gh-runner` shows only group `gh-runner`, and the password is locked (`passwd -S`: `L`);
            - its subuid/subgid range is 231072–296607;
            - it is refused on `/var/run/docker.sock` and on `/home/acruzgon`.
        - **Its rootless Docker:**
            - `docker version` reports `rootlesskit` 2.0.2 with `NetworkDriver: slirp4netns`, `PortDriver: builtin`, and cgroup v2 with the systemd driver;
            - `hello-world` runs;
            - a `busybox:1.37` `httpd` published on `127.0.0.1:59199` answered HTTP 200;
            - `--cpus 4 --memory 3g` gives `cpu.max=400000 100000` and `memory.max=3221225472`;
            - unbounded allocation under `--memory 64m` exits 137.
        - **Tools:** `rustc 1.99.1`, `cargo fmt` and `cargo clippy` present, `cqlsh 6.2.2`; the runner tarball's SHA-256 (`baebf204…147c40`) matches the v2.338.2 release notes and the asset's digest.
        - **The service:**
            - `systemctl show` reports `MemoryMax=9663676416`, `MemorySwapMax=0`, `OOMPolicy=continue`, `User=gh-runner`, `UnitFileState=enabled`, `ActiveState=active`;
            - `gh api .../actions/runners` lists `acruzdebian` online with the five labels, and `sam-7900x` with its four old labels plus the three new ones.
        - **Ports:** the Cassandra job's host port 59142 is free on this machine (`ss -ltn`), and the system Docker daemon runs no containers, so project `hfs-cassandra-ci` cannot clash.
        - **GreenLens:**
            - each `/home/ci-runner/runner-*/.runner` named `aacruzgon/GreenLens`;
            - after the stop, `rig-1..3` turned offline on GitHub;
            - `ci-runner` has no processes and no linger file.
        - **PR #29's own run (runs 41954995288 and 41954995214):** `runner_name` from `gh api .../actions/runs/<id>/jobs`.

          | Job | Runner | Result | Job time | Runner memory peak |
          | --- | --- | --- | --- | --- |
          | `cargo fmt --check` | `acruzdebian` | success | 5 min 27 s; the first checkout of the repository took 5 min 16 s and `cargo fmt` 6 s | 2.3 GiB |
          | `cargo check (cassandra, R4B)`, cold | `acruzdebian` | success | 1 min 38 s; `cargo check` 91 s (66 s on `sam-7900x`, ADR-sam-michaels-0002) | 6.3 GiB |
          | `Cassandra schema (single node)` | `acruzdebian` | success | 1 min 21 s; healthy after 69 s | 2.8 GiB in the runner, 2.2 GiB in `user@1005.service` (the container) |
          | `cargo clippy (helios-fhir)`, then `(workspace)` | none yet | queued for `hfs-cassandra-heavy`; `sam-7900x` was offline | | |

          The peaks come from sampling `memory.current` of the runner service's cgroup and of `user@1005.service` every second, windowed by each job's start and end time. Like ADR-sam-michaels-0003's peaks, they include reclaimable file cache, which is why the Cassandra job shows 2.8 GiB in the runner without compiling anything. `systemctl show -p MemoryPeak` on the service gave 6,813,794,304 bytes (6.35 GiB) over the whole run, 2.7 GiB under the cap. `MemAvailable` never fell below 9.6 GiB. No OOM kill.
- **Links:**
    - PR #29.
    - ADR-aacruzgon-0018 (amended; update note added), 0019 (update note added), 0021, 0022.
    - ADR-sam-michaels-0001, 0002, 0003, 0004.
    - `development-roadmap.md` section 1 ("Machines") and the Step 0 CI bullet.
    - `CLAUDE.md` and `AGENTS.md` "Commits and CI".
    - `.github/disabled-workflows/README.md`.
