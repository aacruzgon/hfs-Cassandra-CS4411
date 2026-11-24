# ADR-aacruzgon-0086: `acruzdebian` runs a second runner instance, each instance capped at 20G; each single-node job takes its Compose project and host port from the instance's `HFS_CI_SLOT`; and every CI host port moves below Linux's ephemeral range

- **Date:** 2026-11-24
- **Status:** accepted
- **Context:**
    - **One runner per machine left the queue on one executor.** `sam-7900x` is often offline, and then every job waits for `acruzdebian`'s single executor. On 2026-11-24 about 34 jobs were waiting; the Actions API listed 19 runs queued and 3 in progress.
    - **`acruzdebian` has the memory for two jobs at once.** Its runner service was capped at 40G (ADR-aacruzgon-0080), and every job must already fit 20G, because medium jobs also land on `sam-7900x` (ADR-aacruzgon-0082). Two 20G instances keep CI's total at 40G.
    - **The workflows assumed one job per machine at a time,** which is why ADR-aacruzgon-0080 deferred a second instance. Two instances under one user share:
        - **gh-runner's rootless Docker daemon.** That covers the Compose projects `hfs-cassandra-ci`, `-cov`, `-fhir-versions`, `-flaky` and `-mutants`, their containers (`<project>-cassandra-1`, also passed to `ci-report.py` and to `HFS_TEST_CASSANDRA_STOPPABLE_CONTAINER`) and their volumes. A job's opening `down -v --remove-orphans` would remove the other instance's live node. The three-node job's project, network and subnet are on the same daemon.
        - **The host's ports.** Rootless Docker publishes on the host itself, and coverage and the FHIR-versions job both used 59242.
        - **What `runner-docker-prune.yml` removes.** Its comment said "no CI container is running while this does". With two instances, a job can hold a container it stopped on purpose (the readiness test's `docker stop`), or a network that `compose up` has made but not yet attached.
        - **`~/.rustup`.** rustup takes no lock around a toolchain install (rust-lang/rustup#988, still open). In 1.29.3 the only file lock is on `settings.toml` (`settings.rs` through `utils/raw.rs`'s `read_locked_file` and `write_locked_file`). `dtolnay/rust-toolchain` runs `rustup toolchain install stable` in every Rust job, so the first jobs after a stable release on both instances could break the toolchain.
        - **`~/.install-action/tmp`.** `taiki-e/install-action` (the coverage job's cargo-llvm-cov) downloads into that one fixed directory and deletes it at the end. It has no "already installed" check (`main.sh` lines 642 and 197–244).
    - **The CI ports sat in Linux's ephemeral range,** 32768–60999 on `acruzdebian` (`ip_local_port_range`, no reserved ports) and the kernel default (`ip-sysctl.rst`).
        - **Why that matters:** `connect()` hands out even ports there first, and every CI port was even. A port held by a client socket in TIME-WAIT refuses a listener for about 60 s, with `SO_REUSEADDR` or without. ADR-aacruzgon-0082 measured 2,243 such sockets on `acruzdebian` during one job with a node, and run 44765908091 failed on it.
        - **Why it was tolerated:** 0082 kept the required job and the other two jobs without a retry, because "its build runs before its node starts, so the previous job's TIME-WAITs have expired by its bind". That holds only while the machine runs one job at a time.
        - **0082's plan:** "If a required job ever hits it, the CI ports move below 32768".
        - **What the audit found:** the independent audit of PR #111 reproduced the refusal in a Linux container (300 client ports, all even; 20 of 20 listens refused). With two instances, the other instance's tests can make those TIME-WAITs at the moment a job binds, and the readiness test's `docker start` binds again in the middle of a job.
    - **What was already per instance:**
        - each instance's `_work` folder: the checkout, every persistent target directory (`${{ runner.workspace }}/target/*`, ADR-sam-michaels-0002), `RUNNER_TEMP` and `RUNNER_TOOL_CACHE` (cargo-audit, cargo-mutants, the dataset job's Java);
        - the memory cap: each service's own cgroup, which `ci-report.py` and the clippy job-count step read.

      Two things are shared but safe. `~/.cargo` can be shared, because cargo locks its package cache (`cache_lock.rs`). `uv` and `cqlsh` are only read.
    - **The other projects on the machine.** On 2026-11-24 `acruzdebian` also ran eight other runners: `aacruzgon-cs4471-dynamism` (3 × 12G and 2 × 2G) and `aacruzgon-muster` (3 × 12G). That is 76G of caps (6 × 12884901888 + 2 × 2147483648 bytes) beside our 40G, on 64 GB. Their workflows publish only 2606, 4700 and 8000–8096. This entry changes nothing of theirs.
- **Decision:**
    - **A second instance, `acruzdebian-2`,** in `/home/gh-runner/actions-runner-2`, as the same user `gh-runner`.
        - **Labels:** `hfs-cassandra-light, hfs-cassandra-medium, hfs-cassandra-heavy, hfs-runner-acruzdebian-2`, never the bare `hfs-cassandra`.
        - **Unit:** `actions.runner.aacruzgon-hfs-Cassandra-CS4411.acruzdebian-2.service`, with the first instance's drop-in settings, `MemoryMax=20G`, `MemorySwapMax=0` and `OOMPolicy=continue`.
    - **The first instance goes from 40G to 20G,** without a restart: `memory.conf` and a `systemctl set-property --runtime`, as 0080 did. Its labels and `.env` don't change.
    - **A slot per instance, `HFS_CI_SLOT`, in the runner's `.env`.** `acruzdebian-2` gets `HFS_CI_SLOT=1`. `acruzdebian` and `sam-7900x` get none.
        - **Which jobs:** the five that start a single node: `Cassandra schema (single node)`, `Cassandra coverage (report only)`, `Cassandra tests (all FHIR versions)`, `Cassandra flaky tests (report only)` and `Cassandra mutants (report only)`.
        - **What the step does:** each job's first step reads the slot and writes `COMPOSE_PROJECT`, `CASSANDRA_HOST_PORT` and `CASSANDRA_CONTAINER` to `$GITHUB_ENV`.
            - Unset or 0 keeps today's project and container names, and the job's base port.
            - Slot N (1–9) adds `-sN` to the project and 10 × N to the port.
            - Any other value, or a port at 32768 or above, fails the job.
        - **No literals:** the job's `env:` holds only the bases, `COMPOSE_PROJECT_BASE` and `CASSANDRA_BASE_PORT`. Every command reads `"${COMPOSE_PROJECT:?}"` and `"${CASSANDRA_CONTAINER:?}"`, so a job without the slot step fails instead of reaching the Compose file's default project, `hfs-cassandra`.
        - **Why inline:** the step is written into each job, not kept in a script, because the flakiness and mutation jobs check out the pull request's head, which may not have a new script.
    - **Every CI host port moves below 32768,** out of the ephemeral range, which carries out ADR-aacruzgon-0082's plan. Each port is its old number minus 30000, so it still reads at a glance.
        - **Which ports:** the five single-node bases go to 29142, 29242, 29442 and 29842, and the three-node job's ports to 29642–29644.
        - **Which stay:** the developers' 59042, the cluster file's default 59342–59344 (a developer's cluster) and the benchmark harness's 59542 (PR #76, run by hand). None of them is CI-only.

      | Job | Slot 0 (unset) | Slot 1 | Slot 9 | Before |
      | --- | --- | --- | --- | --- |
      | `Cassandra schema (single node)` | `hfs-cassandra-ci`, 29142 | `hfs-cassandra-ci-s1`, 29152 | 29232 | 59142 |
      | coverage | `hfs-cassandra-cov`, 29242 | `hfs-cassandra-cov-s1`, 29252 | 29332 | 59242 |
      | FHIR versions | `hfs-cassandra-fhir-versions`, 29242 | `hfs-cassandra-fhir-versions-s1`, 29252 | 29332 | 59242 |
      | flakiness | `hfs-cassandra-flaky`, 29442 | `hfs-cassandra-flaky-s1`, 29452 | 29532 | 59442 |
      | mutation | `hfs-cassandra-mutants`, 29842 | `hfs-cassandra-mutants-s1`, 29852 | 29932 | 59842 |
      | three-node (unslotted) | `hfs-cassandra-3n`, 29642–29644, `10.59.142.0/24` | the same | the same | 59642–59644 |

      Every port in every slot lies in 29142–29932. None is shared across slots, and none is a port listened on by `acruzdebian` (22, 53, 80, 443, 631, 1883, 5355, plus ephemeral ones), published by the other projects, fixed elsewhere in this repository (all at 1xxxx, 27xxx or 5xxxx), or named in IANA's registry for 29000–29999 (29000, 29118, 29167–29169, 29999). Coverage and FHIR versions share a port within a slot, which is safe because an instance runs one job at a time.
    - **No bind retry for the required job, coverage or FHIR versions.** Below the ephemeral range no client socket is ever placed, so the TIME-WAIT refusal that made 0082 add retries can no longer happen, with one instance or two. The audit's report gives no other reason a bind fails. The flakiness and mutation jobs keep their three tries, which cost nothing when the first works; their comment now says why they were added.
    - **The three-node job stays unslotted.** Its concurrency group, `cassandra-three-node`, is one group for every ref, so no two runs overlap on any runner, and fixed names let the leading `down -v` find a killed run's cluster whichever instance ran it.
    - **The second instance has its own `RUSTUP_HOME`,** `/home/gh-runner/.rustup-2`, in its `.env`. The rustup proxies in the shared `~/.cargo/bin` read it, so no workflow changes for this.
    - **The coverage job's `taiki-e/install-action` step gets its own `HOME`** (`${{ runner.temp }}/install-action-home`). The binary still lands in gh-runner's `~/.cargo/bin` (the toolchain step exports `CARGO_HOME`), by `mv`, a rename, so a job already running the old copy keeps it.
    - **The weekly prune keeps anything created in the last 24 hours.** It runs `docker system prune --force --filter until=24h`, then `docker volume prune --force`, which removes anonymous volumes only on Docker 23 and later (`acruzdebian` has 26.1.5); `until` is refused together with `--volumes`. No job runs that long: the longest `timeout-minutes` is 160. The matrix keeps one entry per machine, `hfs-runner-acruzdebian`, since both instances share one daemon.
    - **Every job fits 20G, and heavy now only means "off `sam-7900x`".**
        - **The mutation job's `jobs`:** the default and the maximum go from 4 and 6 to 3. At 4 jobs it peaked at 15.4 GiB `anon` (ADR-aacruzgon-0082), 4.6 GiB under 20G, inside 0080's 5 GiB margin. At 0082's figure of about 3.85 GiB a job, 3 jobs should peak at about 11.6 GiB. That is an estimate, to be measured on the first run. A run should take about 33 minutes instead of about 25.
        - **`cargo clippy (helios-fhir)`** reads 20G and picks 1 job, as on `sam-7900x`, so a cold run on `acruzdebian` is back to about 286 s instead of 160 s.
    - **Rollout order.**
        - The new instance starts only after this PR merges, and only after every run created before the merge has finished, since such a run keeps its old workflow files, and so does a re-run of one. The team is told not to re-run those runs.
        - The PR merges right before the machine is set up. Until then `CLAUDE.md`, `AGENTS.md` and `.coderabbit.yaml` describe two 20G instances while `acruzdebian` still runs one at 40G.
        - No job's `name:`, `runs-on`, `if:` or `uses:` changes, so the five required checks stay as they are.
- **Alternatives considered:**
    - **A second user, with its own rootless Docker:** lost. It would isolate Docker, rustup and the install directories for free. But it repeats ADR-aacruzgon-0023's whole setup (user, subuid range, linger, cgroup delegation, rootless Docker, rustup, `uv`, `cqlsh`) and keeps a second copy of every image. Its jobs would still share the host's ports, so the slot would be needed anyway. Its one real gain covers an old workflow on the new instance: that would fail on a busy port instead of removing the other instance's node. The rollout order covers that case.
    - **Keep the 5xxxx ports and give the required job, coverage and FHIR versions the flakiness job's three-try loop** (the audit's first option): lost. It would leave every CI port where any client socket on the machine can take it, and cost up to 70 s whenever the bind failed. 0082 had already named the move below 32768 as the fix.
    - **Other ranges below 32768:** not taken. 29xxx is free on `acruzdebian` and in the other projects. The repository's other fixed ports sit at 1xxxx and 27xxx. "Old port minus 30000" keeps each job recognisable.
    - **A slot for the three-node job too:** lost. Its global group already prevents overlap, and a slotted project would leave a killed run's cluster for the other slot's runs to step around.
    - **The slot logic in a script under `.github/scripts/`:** lost. The flakiness and mutation jobs check out the pull request's head, which may predate the script.
    - **Derived names in the job's `env:`, overwritten through `$GITHUB_ENV`:** lost. The same name would be set in two places, and neither GitHub's documentation nor the parts of the runner's source I read say which a later step sees. With only the bases in `env:`, the order doesn't matter.
    - **Port offset 1 × N:** it would also have kept the slots apart (29142–29151, 29242–29251, …). It lost to 10 × N, which keeps every single-node port ending in 2, as every base does, and shows the slot in the tens digit (291**5**2 is slot 1 of 29142).
    - **Serialising the prune against the other instance (a lock or a shared concurrency group):** lost. `until=24h` needs nothing from the other workflows.
    - **Serialising rustup with a lock in each workflow:** lost. `dtolnay/rust-toolchain` is an action, not a `run:` step, so no lock can wrap it. A per-instance `RUSTUP_HOME` costs about 1.5 GB of disk and nothing in the workflows.
    - **4 jobs for the mutation job, as an explicit exception to 0080's margin:** lost. The job is report-only, and `OOMPolicy=continue` would fail only that job. But 3 keeps the rule, for about 8 more minutes a run.
    - **Keep one 40G instance:** lost. The queue is the problem, not memory.
    - **Two instances at 40G each:** lost. CI's caps would reach 80G on a 64 GB machine that already gives 76G of caps to other projects.
- **Consequences:**
    - **Easier:**
        - Two jobs run on `acruzdebian` at once, so a light check no longer waits behind a 26-minute mutation run or a cold clippy pair while `sam-7900x` is off.
        - A merge still needs only one machine on.
        - No CI port can be held by a client socket, so a failed bind on "address already in use" now means a real clash, not chance.
    - **Harder:**
        - `cargo clippy (helios-fhir)` builds one unit at a time on `acruzdebian` too (1 job under 20G), about 286 s cold instead of 160 s (0080's runs). Warm runs are unaffected.
        - The mutation job runs 3 jobs, about 33 minutes instead of about 25.
        - Two builds share 16 threads, so timings in the CI reports get noisier when both instances are busy. The host's `MemAvailable` in the three-node report includes the other instance.
        - Each instance keeps its own warm target directories, so the first jobs on `acruzdebian-2` are cold. Disk grows by up to the same caps, 155 GB, plus up to 33 GB of mutation scratch, against 689 GB free on 2026-11-24.
        - Memory, worst case: the three-node job on one instance (20G plus about 11 GiB of nodes and test container) beside a single-node job on the other (20G plus 3 GiB) is about 54 GiB, against 51 with one 40G instance.
        - Both instances run `~/.cargo/bin/cargo-llvm-cov`. A pull request that changes `LLVM_COV_VERSION` can replace it while a coverage run on the other instance is between two `cargo llvm-cov` calls; that run's later calls then use the new version. If that ever bites, the job installs into a directory of its own and calls the binary by path.
        - A new job that starts a node must take the slot step, and its base port must keep all ten slots below 32768. `CLAUDE.md`, `AGENTS.md`, `.coderabbit.yaml` and `docker/cassandra/README.md` say so.
        - Re-running a run created before the merge, or dispatching a branch that hasn't merged `main`, runs the old literal names and 5xxxx ports. On `acruzdebian-2` that would remove the other instance's node.
    - **To measure once it is online:**
        - the first run with two Cassandra jobs on `acruzdebian` at once (both projects in `docker ps`, both jobs green);
        - a cold `cargo clippy (helios-fhir)` at 1 job under 20G there;
        - the first mutation run at 3 jobs (its peak `anon`, against the 11.6 GiB estimate);
        - the queue depth on a day `sam-7900x` is off.
- **AI involvement:**
    - **Tool and model:** Claude Code (Opus 5.5), as the Rex worker `ci-second-runner`, with two Claude subagents: one read the rustup, cargo, `dtolnay/rust-toolchain` and `taiki-e/install-action` sources, and one read the runner's environment handling.
    - **What the AI proposed:**
        - the inventory of shared resources, and running both instances as one user;
        - the slot: the inline step, the 10 × N offset over slots 0–9, keeping the derived names out of `env:`, `${NAME:?}`, and the slot step first;
        - leaving the three-node job unslotted;
        - the per-instance `RUSTUP_HOME`, the install-action `HOME` and the prune's `until=24h`;
        - after Alan's port decision: the 29xxx range ("old port minus 30000"), keeping 59042, 59342–59344 and 59542, and the step's refusal of a port of 32768 or more;
        - the setup guide and its rollout order.

      Its first limit for the mutation job was 4, which broke 0080's margin. It first claimed that a job's `env:` ranks above `$GITHUB_ENV`, and withdrew the claim when it could not confirm it.
    - **What the human changed:**
        - I decided the second instance and the 20G split before the session, and accepted that `cargo clippy (helios-fhir)` drops to 1 job.
        - After the independent audit of #111, I chose to move every CI port out of the ephemeral range, rather than add retries, and to limit the mutation job to 3 jobs.
        - I approved this entry for #111.
        - I ran every `sudo` step. <What you changed in review, if anything.>
    - **Verified:**
        - **Lint:** `actionlint` 1.7.12 on the seven changed workflows reports only `runner.workspace`, in the same places as on `origin/main`. `zizmor` 1.32.0 reports no findings, with the same suppressions as `main`. PyYAML parses them, `.coderabbit.yaml` and the cluster Compose file. `shellcheck` 0.11.0 (`-s bash -S style`) is clean on the 61 extracted `run:` blocks that read a slot or project name.
        - **The slot step,** extracted and run:
            - unset, empty and `0` give `hfs-cassandra-ci`, 29142 and `hfs-cassandra-ci-cassandra-1`;
            - `1` gives `-s1` and 29152, and `9` gives 29232;
            - `10`, `-1`, `a`, `01`, `" 1"`, `"1 "` and `$'1\n'` fail;
            - a base of 59142, or 32700 at slot 9, fails as inside the ephemeral range;
            - a compose call without the step fails with `COMPOSE_PROJECT: parameter null or not set`;
            - the five copies are byte-identical and each is its job's step 0.
        - **Compose:** `docker compose config` publishes `127.0.0.1:29142` for `hfs-cassandra-ci`, 29152 for `-s1`, and 29642–29644 with subnet `10.59.142.0/24` for the three-node files.
        - **Ports:** a script over every job and slot 0–9 found no port shared across slots, none at 32768 or above, and none in the reserved, listening, other-project, repository or IANA sets above.
        - **`acruzdebian`, read-only:**
            - `ip_local_port_range` is `32768 60999`, `ip_local_reserved_ports` is empty, and no sysctl file sets either;
            - `ss -ltn` and `ss -lun`;
            - `systemctl show` gave `MemoryMax=42949672960`, `MemorySwapMax=0`, `OOMPolicy=continue` and `User=gh-runner`, with drop-ins `/run/…/50-MemoryMax.conf`, `memory.conf` (40G) and `timesync.conf`;
            - `df -h /`: 689 GB free.
        - **The other projects:** their workflow and Compose files, read through the API.
        - **The audit's reproduction:** the TIME-WAIT refusal in a Linux container (`report-pr111-audit.md`, S1).
        - **The sources:**
            - rustup #988 and 1.29.3 (`settings.rs`, `utils/raw.rs`);
            - cargo's `cache_lock.rs`;
            - `dtolnay/rust-toolchain` at `e2a55d2`;
            - `taiki-e/install-action` at `1168f68`, `main.sh` 622–664;
            - the kernel's `ip-sysctl.rst`.
        - **Target-directory sizes:** from the latest CI reports (runs 45322606272, 45314658823, 45277266997).
        - **The machine side:** <after setup: `systemctl show` and the cgroup's `memory.max` for both units (21474836480); `gh api .../actions/runners` with both instances online; the first run with jobs on both instances, run `<id>`; the first mutation run at 3 jobs, run `<id>`, peak `<…>` MiB `anon`.>
- **Links:**
    - PR #111, and its independent audit (`report-pr111-audit.md`).
    - ADR-aacruzgon-0080 and 0082 (update notes), 0018, 0019, 0021, 0022, 0023, 0083.
    - ADR-sam-michaels-0001, 0002, 0003.
    - rust-lang/rustup#988.
    - `.github/workflows/fmt-clippy-cassandra.yml`, `cassandra-coverage.yml`, `fhir-versions.yml`, `cassandra-flaky.yml`, `cassandra-mutants.yml`, `cassandra-three-node.yml`, `runner-docker-prune.yml`.
    - `docker/cassandra/README.md` ("CI host ports") and `docker-compose.cluster.yml`.
    - `development-roadmap.md` section 1 ("Machines"), row 0.5 and the Step 0 CI bullet.
    - `CLAUDE.md` and `AGENTS.md`, "Commits and CI"; `.coderabbit.yaml`'s CI rules.
