# ADR-aacruzgon-0057: Upstream's CRUD suite runs on Cassandra as an adapted copy, with a parity test and four Cassandra-strict tests

- **Date:** 2026-11-06
- **Status:** accepted (Alan chose D2 of the Step 3 plan on 2026-11-06 and approved this entry on 2026-11-07).
- **Context:**
    - **The exit criterion.** Roadmap Step 3's exit criterion and row 3.3 say "`crud_suite.rs` passes". That binary is `#![cfg(feature = "sqlite")]` (`crates/persistence/tests/crud_suite.rs:12`). Each of its 88 tests builds its own `SqliteBackend::in_memory()`: 16 + 23 + 17 + 14 + 18 in `tests/crud/{create,read,update,delete,conditional}_tests.rs`, two of them `#[ignore]`d for #306.
    - **The fixed tenants.** The tests use fixed tenants (`test-tenant`, `tenant-1`, `tenant-2`, `my-tenant`, `patch-tenant-1`), assert literal tenant ids (`crud/create_tests.rs:366-367`), and count from an empty store (`crud/conditional_tests.rs:71-72`).
        - On the shared Cassandra node those tenants would collide across tests and across runs (ADR-aacruzgon-0020, rule 2).
        - ADR-aacruzgon-0038 left it to Step 3 to decide how the suite reaches Cassandra (`0038-testable-reset-rotates-tenant.md:30`). It rejected namespacing tenants inside the wrapper, because of those literal assertions (`:22`).
    - **Shared assertions that accept two answers.** Some shared assertions accept two answers, or leave one out, so they cannot pin Cassandra's:
        - `crud/read_tests.rs:585`: a read after a delete may be `None` or `Gone`;
        - `crud/update_tests.rs:399-410`: an update of a deleted resource may be `NotFound` or `Gone`;
        - `crud/delete_tests.rs:310-345`: a revive's `created` is not asserted.

      Since PR #55, the delete-twice and delete-unknown tests (`crud/delete_tests.rs:142-187`) expect `NotFound` only. But they do not check that nothing was written, and they do not test `delete_versioned`. The Step 3 HL7 matrix names the Cassandra answers it wants pinned: T-4, T-9 and T-14.
    - **Alan's decision.** He decided D2 of the Step 3 plan on 2026-11-06: "An adapted copy of `crud_suite` for Cassandra (as recommended): fresh tenants per test, a parity test against upstream's `tests/crud/`, and the three `cassandra_strict_*` tests."
- **Decision:**
    - **The copy: `crates/persistence/tests/cassandra/crud.rs`.** It is included by `tests/cassandra_tests.rs`, and holds one module per upstream file, with the same test names and bodies. It changes only:
        - the backend: a `CassandraBackend` on the test node, and a skip without a node;
        - every tenant: each is fresh, from `unique_tenant`/`unique_tenant_id`, and a literal tenant assertion compares with the tenant the test made;
        - `test_read_system_tenant`: it keeps `TenantContext::system()`, and gives its ValueSet a `unique_id` (ADR-aacruzgon-0038);
        - the two #306 tests, which run, since Cassandra stores `created_at`.
    - **A parity test: `crud::parity::crud_copy_matches_upstream`.** It reads `tests/crud/` with `std::fs::read_dir` when it runs, and finds tests by their test attribute, whatever their name. It fails when:
        - an upstream file has no module here;
        - an upstream test has no copy and no reason on the `EXCLUDED` list (empty today);
        - the copy has a test upstream lacks that is not on `ADDITIONS`;
        - any copied test is `#[ignore]`d;
        - an upstream file's content differs from the copy's baseline. The baseline is a 64-bit FNV-1a hash per file, with `\r\n` read as `\n`, pinned at `e32b02184`. This catches a test that changes without being renamed, as PR #55 changed `test_delete_already_deleted_fails`.
    - **Four Cassandra-strict tests,** `crud::strict::cassandra_strict_*`, all on `ADDITIONS`:
        - a read after a delete is `Err(Gone)` (T-4);
        - a `delete` or `delete_versioned` of an unknown id, and of a deleted one, is `NotFound` and writes no version (T-14, the storage half of ADR-aacruzgon-0052);
        - a revive is `created = true` at N+2, and keeps `created_at` and the FHIR version (T-9, D16);
        - an update of a deleted resource is `NotFound`, and the marker stays (D8, G9).
    - **The race suite** (`search/versioned_write_race_suite.rs`) runs in the same binary as five wrappers, as on SQLite (`sqlite_tests.rs:666-724`):
        - four run on `multi_thread` runtimes with 8 workers;
        - `versioned_delete_is_a_compare_and_swap` runs on a plain one;
        - they run 10 rounds, as on PostgreSQL and MongoDB;
        - they run on the test wrapper `CassandraTestBackend`, which now forwards `VersionedStorage`.

      Each takes a fresh base, `test-<label>-<uuid32>`, at most 46 bytes, so that every tenant the suite derives stays within 64 bytes. A unit test checks that the four keep their runtime attribute, since on a current-thread runtime they pass without racing. Each wrapper prints its own backend's CAS counters over the race, as `[cas case=race:<test> …]`, for CI's CAS report (ADR-aacruzgon-0059).
- **Alternatives considered:**
    - **Make upstream's `tests/crud/*.rs` generic over a backend and its tenants:** lost.
        - It widens roadmap section 8's touch list into five upstream files the fork does not own.
        - The literal tenant assertions would change for SQLite too.
        - It stays possible later as an upstream PR, which would then replace this copy.
    - **Run the SQLite file through the harness's `backend_test!` macro:** lost.
        - Nothing uses it, and it generates only a SQLite variant (`tests/common/harness.rs:315-352`).
        - The fixed tenants would still collide.
    - **A copy without a parity test:** lost. An upstream sync that adds or renames a CRUD test would leave the copy silently behind. The parity test makes the drift a failure.
- **Consequences:**
    - **Easier:** the Step 3 exit criterion is checked on Cassandra, test for test, and where upstream accepts two answers, Cassandra's one is pinned.
    - **Harder:** every upstream sync that touches `tests/crud/` must bring the change into the copy, and the parity test fails until it does.
    - **The two multitenancy wrappers that need only CRUD** (D23) run in the same binary:
        - `tenant_id_fidelity_suite::distinct_tenant_ids_never_share_data`;
        - `tenant_existence_suite::child_data_is_not_the_parents`.
    - **Two of the fidelity suite's five tenants (`/<base>`, `//<base>`) do not start with `test-`,** because the suite derives them. **This exception to the prefix rule is accepted.** They are unique per run, through the UUID in the base, so ADR-aacruzgon-0020's rule 2 holds. Report R4 and ADR-aacruzgon-0029 recognise test rows by the prefix, so they do not see these two tenants.
    - **Measured on the assembled Step 3 stack** (layer L4, `cassandra/step3-suites`, PR #60):
        - **The suite's time.** CI run [42382031387](https://github.com/aacruzgon/hfs-Cassandra-CS4411/actions/runs/42382031387) on `sam-7900x`: `cassandra_tests` passes 173 of 173 in 2.95 s, inside a 4 s step. That is 94 `crud::` tests (88 copied, 4 strict, 2 parity) and 9 `step3_suites::` tests (5 race, 2 multitenancy, 2 unit) among them.
        - **Locally,** against the shared node, 173 of 173 in 7.5 to 7.7 s (three runs). Also 173 passed in 55.9 s with `--test-threads=1`, which shows no test depends on another running beside it.
        - **The race tests' retries** under 8 contending writers, from the same run's CAS report:

          | Wrapper | Attempts | Applied | Miss retries | Budget exhausted | Re-runs |
          | --- | ---: | ---: | ---: | ---: | ---: |
          | updates from the same version | 90 | 20 | 0 | 0 | 0 |
          | update and versioned delete | 90 | 20 | 0 | 0 | 0 |
          | update and plain delete | 98 | 22 | 8 | 0 | 0 |
          | PUTs onto a tombstone | 120 | 60 | 60 | 0 | 0 |
          | versioned delete is a CAS | 3 | 3 | 0 | 0 | 0 |

          No budget ran out, so the 10 rounds stay. No budget changes without an ADR (ADR-aacruzgon-0024).
- **AI involvement:**
    - **Tool and model:** Claude Code (Opus 5.5), worker `s3-suites` (W4), started by the Step 3 orchestrator.
    - **What the AI proposed:**
        - the copy's layout: one module per upstream file, generated from the upstream text and then adapted by hand;
        - the parity test's checks. Discovery by attribute, the no-`#[ignore]` check and the content pin go beyond the plan;
        - the plain backend for the copy, and the wrapper for the race suite;
        - the base format;
        - the race wrappers' `[cas case=race:…]` lines (asked for by the CI worker, W5).
    - **The audit findings that changed the design:**
        - **From the first audit subagent:** attribute discovery, the content pin and the fourth strict test. Both parity gaps were reproduced before they were fixed: the first version of the parity test passed with an un-prefixed test added to `tests/crud/delete_tests.rs`, and with an assertion changed; the fixed one fails on both.
        - **From the L4 assembly's audit:** no lost assertion. A script compared all 88 upstream test bodies with the copy, and they differ only by tenant and id.
    - **What Alan changed:**
        - he chose D2 (the copy) over the plan's two other options;
        - he chose D8 (ADR-aacruzgon-0052), which makes T-14's storage answer `NotFound` and the REST answer 204;
        - he accepted the fidelity suite's two tenants without the `test-` prefix, as recommended.
    - **Verified:**
        - the 88 upstream tests and two `#[ignore]`s, by `grep -c "async fn test_"` on `tests/crud/`;
        - the parity test, which passes against the copy and fails against a mutated upstream file (above);
        - every test of the copy, the strict tests and the race wrappers, against the shared node at `127.0.0.1:59042` and in the CI run above, on the real `update`, `delete`, `vread`, `list_versions`, `delete_with_match` and `create_or_update` of Step 3's layers L2 and L3.
- **Links:**
    - Step 3 plan D2 and D23; HL7 matrix rows T-4, T-9 and T-14;
    - `crates/persistence/tests/cassandra/crud.rs`, `crates/persistence/tests/cassandra_tests.rs` (`step3_suites`), `crates/persistence/tests/common/cassandra_testable.rs`;
    - branch `cassandra/step3-suites`, PR #60;
    - ADR-aacruzgon-0020, 0024, 0029, 0038, 0052, 0059;
    - `development-roadmap.md`: row 3.3, Step 3's exit criterion, and section 5's `crud_suite.rs` row, all updated in this commit.
