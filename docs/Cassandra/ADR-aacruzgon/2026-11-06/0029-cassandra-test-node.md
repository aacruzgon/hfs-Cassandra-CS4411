# ADR-aacruzgon-0029: How the Cassandra test node is reached, started and shared

- **Date:** 2026-11-06
- **Status:** accepted
- **Context:**
    - Roadmap item 2.0 (Step 2, "Cassandra test node") and ADR-aacruzgon-0020 fix two rules: use the running node named by `HFS_TEST_CASSANDRA_URL` when it is set, and otherwise start a `GenericImage` container in `tests/common/`. Each test writes under its own tenant or ids.
    - They leave open:
        - the format of the variable;
        - what the harness does on a URL node that is down or has no schema;
        - what it does when the variable is unset and Docker is missing;
        - how its own container is configured and how fast it boots;
        - how it applies the schema;
        - what tests share;
        - what the isolation helper produces.
    - The pull requests above it build on these answers (the backend's connection setting, ADR-aacruzgon-0031; `TestableBackend`, ADR-aacruzgon-0038), so they had to be fixed first.
- **Decision:**
    - **The variable is one `host:port` with no scheme,** for example `127.0.0.1:59042` locally or `127.0.0.1:59142` in CI. An IPv6 host goes in brackets (`[::1]:9042`). The port is required, because this project never uses 9042 on the host (ADR-aacruzgon-0007). A scheme, a missing port, port 0 or a list is refused with a message. The backend's own contact-point setting (item 2.1) should accept the same form; `HFS_CASSANDRA_CONTACT_POINTS` takes it, as a comma-separated list (ADR-aacruzgon-0031).
    - **On a URL node the harness runs no DDL.** It waits up to 60 s for a `SELECT release_version FROM system.local`, then checks that keyspace `helios` holds exactly the ten tables. If either check fails, every test panics with a message saying to apply `keyspace-single-node.cql`, then `schema.cql`, and to drop the keyspace after a schema change.
    - **With the variable unset and no Docker, the node tests skip** with a message, as the MongoDB suite does. "No Docker" means two things together: `start()` failed with a client error, and the daemon does not answer a bollard ping. Anything else panics in every test: a startup timeout, an OOM kill, a failed `entrypoint.sh` assertion, or an image that a reachable daemon cannot pull. There is no `HFS_TEST_REQUIRE_CASSANDRA`, because CI always sets the variable, so a skip can only happen on a laptop.
    - **The first outcome is cached for the whole binary,** a failure included (`OnceCell<Outcome>`). Every later test panics at once with the first message. It does not wait another 60 s, and it does not boot another container. `tokio::sync::OnceCell` would otherwise retry the start after each panic.
    - **The harness's own container** uses the image `docker/cassandra` pins (`cassandra:5.0.9-trixie@sha256:618a493b…`). It copies that directory's `entrypoint.sh` into the container and runs it through `sh`, as the Compose file does, so it gets `cassandra_latest.yaml` and the `paxos_variant: v2` assertion. It uses the same cluster, snitch, DC and rack names and the same GC threads. It differs from `docker/cassandra` in four ways:
        - `MAX_HEAP_SIZE=512M` and `MAX_DIRECT_MEMORY_SIZE=256M`, half the Compose values;
        - a 2 GiB memory limit, swap disabled, 4 CPUs;
        - `-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.ring_delay_ms=0`. Both are in `CassandraRelevantProperties` in 5.0.9. They skip the 30 s ring delay and three 8 s gossip-settle waits, which a lone seed node does not need. Once the node is up, `ring_delay_ms` is read only for quarantine, replacement, decommission and moves, and none of these happens to a single node;
        - a random host port, mapped by testcontainers.

      Readiness is two steps:
        - first the log line `Starting listening for CQL clients`, within 240 s;
        - then a `SELECT` retried every 0.5 s for up to 60 s.

      The container is labelled through `container_cleanup::with_cleanup_label`, so the `#[ctor::dtor]` hook removes it, and its anonymous data volume, at exit.
    - **The harness applies the schema itself, through the driver.** It splits `keyspace-single-node.cql` and `schema.cql` into statements the way `cqlsh -f` reads them: it handles `--`, `//` and `/* */` comments, `'…'` strings with `''` escapes and `"…"` identifiers. It then runs them in order, once per test binary, in a `tokio::sync::OnceCell`. This is the P6 rule (ADR-aacruzgon-0027): the harness is one of the processes allowed to apply the schema.
    - **Only the address is shared between tests.** Each test opens its own session (`CassandraNode::session`), because a `cdrs-tokio` session runs its background tasks on the runtime that built it, and every `#[tokio::test]` has its own runtime. The harness session keeps the driver's defaults. It is for DDL, `system_schema` reads and assertions, never for the backend's own statements.
    - **Isolation helpers:**
        - `unique_tenant(label)` gives a full-access `TenantContext` with id `test-<label>-<UUID v4, 32 hex>`, at most 64 bytes and valid for `TenantId::parse`;
        - `unique_id(label)` gives `<label>-<32 hex>`, at most 64 characters in the FHIR id alphabet.

      The `test-` prefix tells test data apart from anything else on a shared node. Nothing is ever cleaned up (ADR-aacruzgon-0020).
    - **Layout:**
        - the helper is `crates/persistence/tests/common/cassandra.rs`, `#[path]`-included by `tests/cassandra_tests.rs` together with `common/container_cleanup.rs`;
        - no line is added to `tests/common/mod.rs`, because no test target declares `mod common;` (`tests/backend_capability_contract.rs:9-10`). PR #42 later declares an inline `mod common` for three other files of `tests/common/`, still not `mod.rs` (ADR-aacruzgon-0039);
        - no `Cargo.toml` change: `#![cfg(feature = "cassandra")]` gives the binary `cdrs-tokio`, and `testcontainers`, `ctor` and `uuid` are already dependencies.
- **Alternatives considered:**
    - **A `cassandra://` URL, or a comma-separated list of contact points:** lost. Nothing reads a scheme, and the single node has one address. A list can be added when the three-node cluster gets tests.
    - **Applying the schema on a URL node too (`IF NOT EXISTS` makes it harmless):** lost. Rule 1 says that node already has it, and a test process should not be a second, silent schema writer on a shared node. It would also hide a node left on an old schema, because `CREATE TABLE IF NOT EXISTS` does not change an existing table.
    - **`HFS_TEST_REQUIRE_CASSANDRA`, as MongoDB has:** not chosen. CI sets the URL, and with the URL set every failure is already fatal.
    - **The Compose limits for the fallback (1G heap, 3 GiB):** lost. Roadmap Step 2 asks for `MAX_HEAP_SIZE` "set small for tests". A 512M heap measured about 1.1 GiB at idle under a 2 GiB limit (Apple-silicon laptop, OrbStack), and with it the node boots, applies the schema and passes both node tests.
    - **No startup properties:** lost. A first boot then took 56 s, of which 54 s were the ring delay and the gossip-settle waits (the boot log: 06:08:51.9 to 06:09:22, then 06:09:22 to 06:09:46). With the properties it takes about 5 s. They change nothing a single-node test measures.
    - **`cqlsh` in the container (`exec`) to apply the schema:** lost. The harness would depend on the image's `cqlsh` and its Python. The driver path is the one the backend uses.
    - **A `WaitFor::Healthcheck` with the Compose `cqlsh` healthcheck:** lost. The log line is available sooner, and the `SELECT` retry through the driver is the readiness the tests need.
    - **One session shared by every test:** lost, for the runtime reason above. It is the same choice the MongoDB suite made: it shares the connection string, not a client.
- **Consequences:**
    - **The pull requests above it get:**
        - `cassandra_node::node()`, which returns `Option<&'static CassandraNode>`, with `contact_point()`, `origin()` and `session()`;
        - `unique_tenant`, `unique_tenant_id` and `unique_id`;
        - the CQL helpers `select`, `text` and `int`;
        - `schema_listing`, which prints `system_schema` in the `expected-schema.txt` format.

      The backend parses the same `host:port` (ADR-aacruzgon-0031).
    - **A laptop without the shared node pays about 5 s plus the image pull.** That is no longer the "budget a day" the roadmap feared.
    - **The fallback is not the benchmark configuration** (half the heap, startup properties). Only `docker/cassandra` and the CI node are. The node tests check that both run 5.0.9, `paxos_variant` `v2`, DC `helios-dc1` and rack `helios-rack1`.
    - **The smoke test makes the CI job's schema comparison through the driver,** so a local run catches a node on an old schema. When `schema.cql` changes, `expected-schema.txt` changes with it, and the test follows.
    - **Revisit** if a suite needs the three-node cluster (a list of contact points), or if nextest is adopted (ADR-aacruzgon-0020: the fallback is per process).
- **AI involvement:**
    - **Tool and model:** Claude Code (Opus 5.5), as the Step 2 worker `s2-testnode`.
    - **What the AI proposed:** every point above. The Step 2 orchestrating session suggested `host:port`.
    - **What Alan changed:** Alan reviewed every Step 2 decision record on 2026-11-06, before it was committed. From his own review he found that several alternatives were stale, because `cdrs-tokio` 9.1.0 and 10.0.0 (released 2026-11-04) had already fixed driver behaviour the records relied on. He found that some alternatives were not real options or did not make sense: strawmen, restatements of the decision, and the workers' own debugging steps listed as alternatives. And he found that some records cited sources outside the repository. He had a review agent re-audit every alternative against the code, the design documents and `cdrs-tokio` 10.0.0, accepted its recommendations, and had each record rewritten from them. For this entry: of its eight alternatives, seven were kept, one was rewritten and none was moved or dropped. The rewritten one, the Compose limits for the fallback, now cites roadmap Step 2 (`MAX_HEAP_SIZE` "set small for tests") instead of a source outside the repository, and AI involvement no longer cites one either. The review agent checked the one-session-per-test reason at 10.0.0: `Session::new` still spawns its control-connection task on the runtime that builds it (`session.rs:1002` @10.0.0), so that alternative stays lost after the upgrade (ADR-aacruzgon-0044).
    - **Verified:**
        - the driver and testcontainers APIs, read in the `cdrs-tokio` 8.1.9 and `testcontainers` 0.27.3 sources at the versions then in `Cargo.lock`;
        - `HFS_TEST_CASSANDRA_URL=127.0.0.1:59042 cargo test -p helios-persistence --features cassandra --test cassandra_tests`: 24 passed;
        - with the variable unset: both node tests passed in 4.3 s, boot included, and afterwards no labelled container and no new volume were left (`docker ps -a --filter label=…`, `docker volume ls` before and after);
        - two probe containers with the same settings: 56 s and 6 s to the CQL line, about 1.1 GiB at idle;
        - an invalid URL and an unreachable one each fail with their message. With the URL unreachable, both node tests failed within 62 s in all, because the failure is cached;
        - with `DOCKER_HOST` pointing at a missing socket, both node tests skipped with "Docker cannot be reached";
        - an audit subagent reviewed the diff. Every finding was fixed before the push except two, left as notes: the pull by digest is untested, and the fallback container's port is published on all host interfaces.
- **Links:**
    - PR #36 (branch `cassandra/step2-test-node`, merged as `fb16da9d1`);
    - `development-roadmap.md` Step 2 "Cassandra test node" and row 2.0; ADR-aacruzgon-0001, 0002, 0003, 0007, 0020, 0027 (P6);
    - `crates/persistence/tests/common/cassandra.rs`, `crates/persistence/tests/cassandra_tests.rs`;
    - ADR-aacruzgon-0030 (the CI job that runs these tests), 0031, 0038.
