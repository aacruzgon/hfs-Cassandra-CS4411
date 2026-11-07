# ADR-aacruzgon-0053: An 8 MiB bound on what a compare-and-swap carries, refused with 400 before any write

- **Date:** 2026-11-06
- **Status:** accepted (Alan approved it on 2026-11-06, as recommended)
- **Context:**
    - **The open comment.** CodeRabbit's outside-diff comment on PR #33, "Bound version-write sizes before the CAS", was never applied (the Step 3 plan's D14). REST accepts bodies up to `max_body_size`, 128 MiB (`crates/rest/src/config.rs:2089`), and the backend sent whatever it received.
    - **What Cassandra 5.0.9 does with a large write.** From the source, at `cassandra-5.0.9`:
        - `max_mutation_size` defaults to half of `commitlog_segment_size`. Both yamls set that to 32 MiB, so the limit is 16 MiB, checked strictly with 12 bytes of overhead (`DatabaseDescriptor.java:898-899`, `Mutation.java:169-177`).
        - A plain write over it is refused at the coordinator as `Invalid`, before anything is sent (`SingleTableUpdatesCollector.java:112`).
        - A CAS has no such check at the coordinator. The replicas fail when they save the Paxos proposal (`PaxosState.java:661`), which the client sees as `WriteFailure`/CAS (`Paxos.java:553-559`). The engine reads that as an unknown outcome, 503, for a request that can never succeed.
        - In front of both, the native protocol refuses any CQL message over `native_transport_max_message_size`. That defaults to `min(max_mutation_size, heap / 10, heap / 40)`, so 16 MiB on any heap of 640 MiB or more (`CQLMessageHandler.java:551-556`, `:826`; `DatabaseDescriptor.java:3237-3244`).
    - **Measured on the shared 5.0.9 node** (single node, 1 GiB heap), by `write.rs` `node_tests::the_size_bound_refuses_before_the_node_would`, 2026-11-06:

      | Payload of `data` | C1 (`INSERT … IF NOT EXISTS`) | A plain V1 of the same row |
      | --- | --- | --- |
      | 9 MiB | applied (`[applied] = true`); the row reads back | written |
      | 17 MiB | `Invalid`: "CQL Message of size 17826056 bytes exceeds allowed maximum of 16777216 bytes"; nothing written | `Invalid`, the same message (17826062 bytes) |

      So on this node the message limit fires first, and it fires before execution: `classify_cas_failure` maps `Invalid` to not-applied, and the generic table gives `QueryError`, which is 500. The Paxos-save failure the source predicts lies between about 16 MiB of mutation and the 16 MiB message limit, so a single node cannot reach it with one statement. Larger heaps, or a raised `native_transport_max_message_size`, would make the `WriteFailure` path reachable.
    - **Why it is a decision.** It adds a server restriction the FHIR spec does not set, and it changes what a client sees for a large resource. It deviates from the design, which has no bound.
- **Decision:**
    - **The bound.** Before step 2 of every write, the engine refuses a write whose CAS would carry more than `MAX_CAS_PAYLOAD_BYTES` = 8 MiB (`write.rs`, `check_cas_size`), counting the serialized `data` plus the `index_keys` blob. The refusal is `StorageError::Validation(ValidationError::InvalidResource { … })`, 400 with issue `invalid`, and one `ValidationDetail` on `Resource` with the byte count. No statement is sent. The live test checks this with the statement counters: 0 statements for a 9 MiB create.
    - **A constant, not a setting.** No fixture comes near it. A configuration name (`HFS_CASSANDRA_*`, ADR-aacruzgon-0031) is added only if Step 8's data needs one.
    - **Where it applies.** It is in `run_cas`, so it covers every CAS: `create` now, and `update`, `delete` (only its 2-byte key set), revive and `create_or_update` from L2 and L3.
- **Alternatives considered:**
    - **No bound; let Cassandra refuse:** lost.
        - On this node a body between 16 MiB and 128 MiB is a 500 (`QueryError`) after the intent row was written. Only a body under 16 MiB but over the Paxos limit would reach the unknown path, and that path is a 503 the client would retry.
        - Either way the answer is a server error for a client error.
    - **A bound of 16 MiB, the node's own limit:** lost. It leaves no room for the rest of the CAS message (keys, the other columns) or for Paxos's copy of the update in `system.paxos`, which is a larger mutation than the CAS itself (`SystemKeyspace.java:1430-1456`).
    - **413 Payload Too Large:** not available. No `StorageError` maps to REST's 413 variant (`crates/rest/src/error.rs`), and 400 matches the other bounds the backend sets: the id (G35), the index values (G35) and the SearchParameter `code` (G38). The issue code `too-long` would fit better than `invalid`, but REST chooses the code, not the backend.
    - **A configurable bound:** deferred, as in the Decision.
- **Consequences:**
    - **What it guarantees.** A body that can never be stored is refused in microseconds with a client error, before the intent row. No CAS can reach the Paxos-save failure, so no write that can never succeed reads as an unknown outcome.
    - **The cost.** Clients lose resources of 8 to 16 MiB of JSON, which a single node could store. This is a stated server restriction. A `Binary` or `DocumentReference` with inline data that large belongs in an attachment store.
    - **To revisit.** Re-measure on the three-node cluster in Step 8, and if a fixture or a user needs more, raise it with an ADR. The `too_large` counter in `cas_stats()` reports every refusal.
- **AI involvement:**
    - **Who and what.** A Claude Code worker (W1, `s3-engine`, Opus 5.5) drafted this entry for roadmap Step 3, layer L1. It implemented the bound and the live test, from the Step 3 plan's D14, which Alan accepted on 2026-11-06 ("an 8 MiB constant, 400 above it, measured live in L1").
    - **Verified:**
        - The Cassandra limits, read in the 5.0.9 source by a reading agent and listed in `reading/cassandra.md`.
        - The node's answers at 9 and 17 MiB, by the live test above, on the shared node at `127.0.0.1:59042`.
        - That the bound sends no statement, by `statement_counts()` in the same test.
        - The pure boundary, by the unit test `what_a_cas_carries_is_bounded_at_8_mib`: exactly 8 MiB passes, one byte more is refused.
    - **Not verified:** the `WriteFailure`/CAS answer itself. It is unreachable on this node, because the message limit fires first.
    - **What Alan changed:** approved as recommended, on 2026-11-06, after reviewing the draft and its measurements. D14's options were (a) a constant, (b) no bound, (c) configurable, and he chose (a); this entry records the node's measured behaviour, as D14 asked.
- **Links:** Step 3 layer L1, branch `cassandra/step3-write-engine`; `crates/persistence/src/backends/cassandra/write.rs` (`MAX_CAS_PAYLOAD_BYTES`, `check_cas_size`, `node_tests::the_size_bound_refuses_before_the_node_would`); PR #33 (CodeRabbit outside-diff comment); `cassandra-discussion.md`, "Write path" step 1; walkthrough 3.1, "Size check"; ADR-aacruzgon-0026 (G35), 0027 (G38), 0031, 0050.
