# ADR-aacruzgon-0043: The startup budget is 10 s of seeding per fresh tenant on the single node (provisional), and the conformance seed keeps the default `create_many`

- **Date:** 2026-11-06
- **Status:** accepted (the 10 s figure is provisional, see Consequences)
- **Context:**
    - **What the design asks.** Roadmap item 2.6 and the design ("Defaults we must override", `create_many` row; "What else the server expects", conformance seeding) say that the startup seed writes about 1,400 SearchParameter and CompartmentDefinition resources per provisioned tenant, each one a lightweight-transaction `create`. The boot time must be measured, and if it is unacceptable, this is where `create_many` gets a concurrent implementation. The seed can run at Step 2 only because `count` is real (ADR-aacruzgon-0033).
    - **The trigger needs a figure.** Section 7's trigger is "Boot time from the conformance seeder above the agreed startup budget → concurrent `create_many`" (`development-roadmap.md:445`). No budget had been agreed, so the trigger could not be checked.
- **Decision:**
    - **The budget, adopted provisionally.** 10 s of seeding per fresh tenant on the single node is the startup budget that section 7 refers to. The figure is checked in two places:
        1. it is re-measured on the three-node cluster (RF 3: `LOCAL_QUORUM` and `LOCAL_SERIAL` across replicas), with a release build, in the Step 8 benchmarks;
        2. it is the threshold of section 7's trigger: a seed above it builds a concurrent `create_many`.
    - **Keep the default `create_many`** (the trait's loop, run at `bulk_write_concurrency` 8, which the backend overrides) and build nothing more. The measured seed takes 1.3 to 4.5 s per fresh tenant (below), under the budget.
    - **Design edit, in this entry's commit.** The design's conformance-seeding bullet replaces "We will measure it." with the measured range and this budget.
    - **Roadmap edit, in this entry's commit.** In section 7 (`development-roadmap.md:445`), "above the agreed startup budget" becomes "above the startup budget (10 s of seeding per fresh tenant on the single node, ADR-aacruzgon-0043, provisional)".
- **Measurements:**
    - **Machine:** Apple M3 Pro, 11 cores, 18 GiB, macOS 27.2; OrbStack (Docker 29.8.2, VM with 11 CPUs and 8.8 GiB).
    - **Node:** a node of the worker's own from `docker/cassandra/docker-compose.yml` (project `s2w-measure`, host port 59342), with the shared node's settings: Cassandra 5.0.9, `cassandra_latest.yaml`, Paxos v2 (checked in `system_views.settings`), 1 GiB heap, 4 CPUs.
    - **Schema and teardown:** the schema was applied fresh for each round, and the node was removed afterwards (`down -v` on that project only).
    - **Spans:**
        - **Seed:** from the "Connected to Cassandra" log line to "Seeded spec CompartmentDefinitions into storage". It also includes `initialize` (the ten-table check), so it is an upper bound on the seed.
        - **Reload:** the `elapsed_ms` of "Stored SearchParameters loaded", the all-tenant stored-SearchParameter load that runs after the seed (ADR-aacruzgon-0041; PR #43, `748be1a4b` and its audit follow-up `733cfe668`).
        - **Boot:** process launch to `/metadata` answering 200.
    - **Round 1:** `cassandra/step2-wiring` at `3ddbafc98` (before the restack; now `958e22b85`), before any stored-SearchParameter load at startup existed. Memory limit 2.5 GiB; the machine was otherwise idle.

      | Run | Boot to `/metadata` | Seed | Created |
      | --- | --- | --- | --- |
      | 1 (fresh keyspace) | 2.43 s | 1.83 s | 1,372 SearchParameter + 5 CompartmentDefinition |
      | 2 | 1.97 s | 1.48 s | 1,372 + 5 |
      | 3 | 1.81 s | 1.32 s | 1,372 + 5 |
      | warm (tenant of run 3 again) | 0.69 s | skipped: one `count` per type finds the set | 0 |

    - **Round 2:** `cassandra/step2-milestone-a` at `8c18196b2` (before the restack; now `bb6cc8a3d`), with the all-tenant load, before PR #43's second round (`89bbc9acb`). Memory limit 3 GiB. The machine was busy with other builds (load average 11.5 on 11 cores), which slows every span.

      | Run | Boot to `/metadata` | Seed | Reload: tenants, ms |
      | --- | --- | --- | --- |
      | 1 (fresh keyspace) | 8.06 s | 4.53 s | 1, 534 ms |
      | 2 | 5.33 s | 3.64 s | 2, 1,097 ms |
      | 3 | 4.72 s | 2.80 s | 3, 1,312 ms |
      | 4 | 5.11 s | 2.59 s | 4, 1,643 ms |
      | 5 | 5.32 s | 2.61 s | 5, 1,949 ms |
      | warm (tenant of run 3 again, 3 seeded tenants) | 2.15 s | skipped | 3, 1,179 ms |

    - **What round 2 shows.**
        - **The seed stays under the budget,** at 2.6 to 4.5 s even on a loaded machine.
        - **The reload adds about 0.2 to 0.55 s per seeded tenant on this node** (0.39 to 0.55 s on average per tenant in the table, 0.22 to 0.56 s from one run to the next). It reads every one of a seeded tenant's 1,372 SearchParameters to find the ones the base does not hold. It is not part of the seed, so the budget does not cover it.
        - **The warm boot is slower,** 2.15 s against 0.69 s, mostly from that reload.
    - **CQL check.** `SELECT count(*)` on `resources_by_type` for round 1's first tenant gives 1,372 SearchParameter and 5 CompartmentDefinition.
    - **Shared node** (`127.0.0.1:59042`, fresh tenant, other workers' tests also running):
        - **Round 1:** seed 2.57 s; `/metadata` after 6.22 s (the first start after a rebuild).
        - **Round 2:** seed 3.05 s; reload of 66 tenants (64 stored definitions) 1.22 s; `/metadata` after 4.92 s.
    - **Arithmetic.** 1,377 creates in about 1.5 s (round 1) is about 900 creates per second of wall-clock throughput, with 8 in flight. Each create therefore takes about 8.7 ms end to end: the intent row, the CAS, the version row and the intent delete.
- **Alternatives considered:**
    - **A concurrent `create_many` (a wider fan-out, or batching the non-conditional rows):** not built. The seed takes 1.3 to 1.8 s per fresh tenant on an idle node (up to 4.5 s on a loaded one), far under the budget, and a wider fan-out would only load the node harder at boot.
    - **Skipping the seed on Cassandra** (`HFS_SEED_CONFORMANCE=false` by default): lost. `GET /SearchParameter` and `GET /CompartmentDefinition` read what is stored, and the seed is idempotent and short-circuits on a warm start.
    - **No budget, only the numbers:** lost. Section 7's trigger cannot be checked without a threshold.
- **Consequences:**
    - **Not enforced.** Nothing in the code or CI enforces the figure; it is a review threshold only. From PR #46 the CI boot report compares every run's worst fresh seed with it and warns, without failing (ADR-aacruzgon-0045).
    - **How it can change.** Changing it later is a new entry that supersedes this one, plus the matching edit to roadmap section 7.
    - **Provisional,** pending the view of the repository owner, Alan's supervisor.
    - **Boot time.** A tenant's first boot costs about 1.5 s of seeding on an idle single node (up to 4.5 s on a loaded one), and later boots one `count` per type. The cost scales with the number of provisioned tenants. Cassandra has no tenant registry, so that number is 1, the default tenant.
    - **Not covered by the budget: the stored-SearchParameter reload.** It costs about 0.2 to 0.55 s per seeded tenant at every boot and every TTL refresh (ADR-aacruzgon-0041, 0048). With many seeded tenants it, not the seed, dominates boot. ADR-aacruzgon-0048 gives it a warning line of its own.
    - **Re-measure** with a release build on the three-node cluster in the Step 8 benchmarks. The trigger fires if the cluster's seed exceeds 10 s.
    - **The seed covers create only.** It goes through `create`, which resolves unknown CAS outcomes; a create that still fails after two more attempts is logged and counted, a failed set is logged, and boot continues (`seeder.rs:211-218`, `:370-377`).
- **AI involvement:**
    - **Who.** The Claude Code worker `s2-wiring` (Opus 5.5) measured, proposed the 10 s figure and wrote up this entry.
    - **What it did.**
        - It started and removed its own node twice and applied the schema with `cqlsh`.
        - It ran a measurement script (round 1: three fresh tenants and one warm boot; round 2: five fresh and one warm, plus one boot on the shared node), and checked the counts in CQL.
        - It read `seed_tenant_conformance` and its count short-circuit (`crates/persistence/src/search/seeder.rs:325-343`, `355-389`).
    - **Not measured.** A release build was not measured. A debug build only adds client-side CPU, so the release figures should be no slower [I].
    - **What Alan changed:** Alan reviewed every Step 2 decision record on 2026-11-06, before it was committed. From his own review he found that several alternatives were stale, because `cdrs-tokio` 9.1.0 and 10.0.0 (released 2026-11-04) had already fixed driver behaviour the records relied on. He found that some alternatives were not real options or did not make sense: strawmen, restatements of the decision, and the workers' own debugging steps listed as alternatives. And he found that some records cited sources outside the repository. He had a review agent re-audit every alternative against the code, the design documents and `cdrs-tokio` 10.0.0, accepted its recommendations, and had each record rewritten from them. For this entry:
        - **Counts:** 2 alternatives kept, 1 rewritten (the concurrent `create_many`: the seed's creates write distinct partitions and do not contend in Paxos, so the cost of a wider fan-out is load, not contention), none moved or dropped.
        - **The review's verdict:** the entry is an ADR only if a budget is adopted.
        - **Alan's decision:** he adopted the worker's 10 s figure provisionally, pending a discussion with the repository owner, his supervisor. So the entry is now about the budget, and the roadmap edit above is added.
- **Links:** PR #44 (branch `cassandra/step2-milestone-a`); `development-roadmap.md` item 2.6 and section 7 (the boot-time trigger); `cassandra-discussion.md`, "Defaults we must override" (`create_many`) and the conformance-seeding bullet; ADR-aacruzgon-0033, 0041, 0045, 0048.
