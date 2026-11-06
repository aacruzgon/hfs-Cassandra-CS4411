# ADR-aacruzgon-0042: Milestone A runs in the required Cassandra job, which stays on the light runners, and stops that job's own node

- **Date:** 2026-11-06
- **Status:** accepted
- **Context:**
    - **Why CI.** Roadmap section 1 says a step is done when its exit criterion passes on CI. Step 2's criterion includes "Milestone A passes by script" and "`/_readiness` reports down when Cassandra is stopped". Item 2.7's script is `crates/hfs/tests/cassandra_milestone_a.rs`, so by section 1 it has to run in CI.
    - **What stays open.** Three questions remain:
        1. which job runs it;
        2. which node the readiness test may stop;
        3. whether the extra build still fits `acruzdebian`.
    - **The memory constraint.** `acruzdebian` is a light runner whose runner service is capped at 9 GiB (ADR-aacruzgon-0023). The test needs `hfs` built with the `cassandra` backend, a second large build next to the persistence tests (ADR-aacruzgon-0030).
- **Decision:**
    - **The job.** The steps go into the existing required job `Cassandra schema (single node)` (`.github/workflows/fmt-clippy-cassandra.yml`). Its name, label (`[self-hosted, hfs-cassandra-light]`) and required status are unchanged:
        - its build step also runs `cargo test -p helios-hfs --no-default-features --features R4,cassandra --test cassandra_milestone_a --no-run`, inside the existing memory sampler;
        - after the existing tests, it runs the round trip (`--exact milestone_a_create_then_read_through_rest`) against the job's node;
        - as its last test step, it runs the readiness test (`--exact readiness_reports_down_while_cassandra_is_stopped`).
    - **The node the readiness test may stop.** It stops and starts the job's own node, `hfs-cassandra-ci-cassandra-1`, and starts no second container:
        - **opt-in:** only when `HFS_TEST_CASSANDRA_STOPPABLE_CONTAINER` names the container;
        - **identity check:** `docker port` must show that container publishing `HFS_TEST_CASSANDRA_URL`'s port;
        - **refusal:** the developer's shared node, `hfs-cassandra-cassandra-1`, is refused outright.

      Without the variable, the test starts and stops a container of its own, as before.
- **Measurements:**
    - **Method.** A temporary commit (`e3682066f`, since dropped from the branch with a lease force-push) pinned the job to `hfs-runner-acruzdebian` and set the target-dir `CAP_GB` to 0, which forces a cold build. The step's sampler prints the cgroup's peaks. Run: https://github.com/aacruzgon/hfs-Cassandra-CS4411/actions/runs/42197542942

  | Run | Runner | Build | Build time | Peak anon | Peak `memory.current` | Cap | OOM kills |
  | --- | --- | --- | --- | --- | --- | --- | --- |
  | 42197542942 (measurement) | `acruzdebian` | cold (dir reset) | 227 s (persistence tests 2 m 00 s, then `hfs` + test 1 m 46 s) | **4,872 MiB** | 9,215 MiB (page cache) | 9,216 MiB | 0 |
  | 42178609043, PR #36's measurement (ADR-aacruzgon-0030), for comparison | `acruzdebian` | cold, persistence tests only | 118 s | 4,817 MiB | 9,215 MiB | 9,216 MiB | 0 |
  | 42198342684 (final, `dbe46d18d`, before the restack) | `acruzdebian` | warm | 13 s | — | — | 9,216 MiB | — |

  - **Margin.** The anon peak rose only 55 MiB over PR #36's, because cargo builds the two packages one after the other and the peak is each build's own. It leaves about 4.2 GiB under the cap, more than the 2 GiB margin this entry adopts for a light job (set by the Step 2 orchestrator; ADR-aacruzgon-0023 says only that a job moves to heavy if it does not fit). So the job stays light.
  - **Time added.**
      - **Cold build:** about 110 s more (the `hfs` build).
      - **Warm build:** seconds.
      - **Test steps:** about 1 s for the round trip, and about 21 to 23 s for the readiness test. Of that, the node takes 14 to 16 s to answer again after `docker start`, and the seed about 2 s. These runs predate the stored-SearchParameter load at startup (ADR-aacruzgon-0041); the head `bb6cc8a3d`, which has it, passed (run 42266369865).
      - **Whole job:** 5 min 29 s cold (the measurement), 1 min 55 s warm (the final run). The warm baseline without these steps was 2 min 36 s on `sam-7900x` (42197514131), which had a partly warm build.
      - The 40-minute timeout is untouched.
  - **Readiness in CI:** down 12 ms after `docker stop` returned, up 14.0 s and 16.1 s after `docker start` (two runs).
- **Alternatives considered:**
    - **A new job on `hfs-cassandra-heavy`:** lost. ADR-aacruzgon-0023 moves a job to heavy only when it does not fit, and the measured anon peak (4,872 MiB of 9,216 MiB) fits. A non-required job would not block a merge on a failing Milestone A, as roadmap section 1 requires. It would also start a second node, and run only on `sam-7900x`.
    - **The readiness test starting its own container in CI:** lost. A second 2 GiB container next to the job's 3 GiB node on a 9 GiB runner, for a check the job's own node can serve.
    - **Stopping a node named only by `HFS_TEST_CASSANDRA_URL`:** lost. A URL can point at a shared node; stopping it must take an explicit, separate opt-in that names the container.
    - **Build `hfs` with default features:** lost. A Cassandra deployment builds `--no-default-features --features R4,cassandra`, and only that build caught the "at least one database backend" guard and the imports unused without the other backends (commit `0fa2631dc`, now `958e22b85` in PR #43; the PR #42 audit found them). No other CI job builds it. The persistence tests keep the default features (ADR-aacruzgon-0030), because there the roadmap names the developers' command.
- **Consequences:**
    - **Required checks.** Step 2's exit criterion now runs on every push and every pull request to `main` through a required check. A failing Milestone A blocks the merge, as roadmap section 1 intends.
    - **Order.** The readiness step must stay last among the job's test steps, because it stops and restarts the node. A test step added later goes before it (as ADR-aacruzgon-0045 and 0046 do).
    - **Watch the margin.** Re-measure if the job's build grows again. The margin is 4.2 GiB now.
    - **The readiness step and the driver upgrade.** On `cdrs-tokio` 8.1.9 the readiness step passes (up 14.0 s and 16.1 s after `docker start`) only because of the backend's session rebuild. On 10.0.0 it is the acceptance test for removing that rebuild, so the upgrade had to re-run it with the rebuild off; ADR-aacruzgon-0044 records that run (up 29.3 s after the start).
- **AI involvement:**
    - **Who.** The Claude Code worker `s2-wiring` (Opus 5.5) wrote the workflow steps and the opt-in, and wrote up this entry. The Step 2 orchestrator (a Claude Code session) put Milestone A into CI under roadmap section 1, and set the 2 GiB margin under the runner cap that decided light against heavy. Alan confirmed that margin on 2026-11-06, so this entry adopts it as the rule for a light job.
    - **Local checks.** On a Compose node of its own (project `s2w-ci`, port 59242, removed afterwards), the worker ran both CI commands. The refusals were checked against the shared container's name and against a port mismatch, and both fail before anything is stopped. The default path, with no opt-in and the test's own container, still passes against the shared node.
    - **CI checks.** The CI numbers come from the runs above.
    - **What Alan changed:** Alan reviewed every Step 2 decision record on 2026-11-06, before it was committed. From his own review he found that several alternatives were stale, because `cdrs-tokio` 9.1.0 and 10.0.0 (released 2026-11-04) had already fixed driver behaviour the records relied on. He found that some alternatives were not real options or did not make sense: strawmen, restatements of the decision, and the workers' own debugging steps listed as alternatives. And he found that some records cited sources outside the repository. He had a review agent re-audit every alternative against the code, the design documents and `cdrs-tokio` 10.0.0, accepted its recommendations, and had each record rewritten from them. For this entry:
        - **Counts:** 2 alternatives kept, 2 rewritten, none moved or dropped.
        - **The heavy-job alternative** now rests on ADR-aacruzgon-0023 and roadmap section 1 instead of the orchestrator's brief.
        - **The default-features alternative** now gives the real reason (the Cassandra-only build caught real `cfg` bugs, commit `958e22b85`), which also settles its apparent conflict with ADR-aacruzgon-0030.
        - **Added:** the Consequence that the upgrade had to re-run the readiness step with the session rebuild off.
- **Links:** PR #44 (branch `cassandra/step2-milestone-a`, head `bb6cc8a3d`; `dbe46d18d` before the restack, when measured); `.github/workflows/fmt-clippy-cassandra.yml`; `crates/hfs/tests/cassandra_milestone_a.rs`; ADR-aacruzgon-0019, 0021, 0023, 0030, 0044; roadmap section 1 and Step 2's exit criterion.
