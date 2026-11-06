# ADR-aacruzgon-0041: Cassandra reloads every tenant's stored SearchParameters before it serves, listing the tenants from `resources_by_type`, and startup fails if the load fails

- **Date:** 2026-11-06
- **Status:** accepted (Alan confirmed the startup rule, and the two behaviours of `89bbc9acb` under Consequences, on 2026-11-06)
- **Context:**
    - **The gap.** CodeRabbit on PR #43 (thread PRRT_kwDOVAIJX860doQ4, https://github.com/aacruzgon/hfs-Cassandra-CS4411/pull/43#discussion_r4491308056): after a restart the Cassandra overlay of stored SearchParameters started empty. A stored SearchParameter that changes the patient compartment did not apply, so a patient-scoped request could get a wrong 403, or be allowed under the wrong rule.
    - **The first fix and what it left.** `1a6d1c24d` (now `4d9c64753`) loaded the default tenant before `serve` and reloaded the tenants this process had seen on each TTL refresh. CodeRabbit's reply: a non-default tenant still started with only the shared base until a refresh ran, and with `HFS_SEARCH_PARAM_CACHE_TTL=0` no refresh ever runs. PostgreSQL's and MongoDB's reloads cover every tenant, because they can list them.
    - **The constraints.**
        - The design has no tenant registry (`cassandra-discussion.md`, "Tenant registry": no registry in this project).
        - The schema is signed off (ADR-aacruzgon-0028), so no new table.
        - The registry loader is sync (`StoredParamLoader`, `tenant_registries.rs:40`). It must not block: the tests use current-thread runtimes.
    - **The data.** `create` writes each resource's `resources_by_type` row (X1, `write.rs`, ADR-aacruzgon-0033), keyed `((tenant_id, resource_type), last_updated, id)` (`schema.cql:222-232`). So the table's partition keys name every tenant that has stored a SearchParameter.
- **Decision:**
    1. **List the tenants by a scan.** `CassandraBackend::stored_search_parameter_tenants` runs `SELECT DISTINCT tenant_id, resource_type FROM resources_by_type` and keeps the `SearchParameter` rows client-side.
        - **Valid without `ALLOW FILTERING`.** `DISTINCT` on the whole partition key is a key-range read: `SelectStatement.java` (`cassandra-5.0`), `validateDistinctSelection`, requires every partition key column on a key range. It reads one row per partition (`DataLimits.DISTINCT_NONE`).
        - **No server-side filter.** A key range cannot restrict half of a composite partition key, so the filter runs in the client.
    2. **Page it.**
        - **Why paging is needed.** Without a page size the node returns the whole result in one message (native protocol v5, section 7). `CassandraSession::rows` sets none, and `cdrs-tokio` 8.1.9's own pager (`cluster/pager.rs:199-223`) builds its parameters from `StatementParamsBuilder::new()`, which drops the consistency. At 10.0.0 the pager clones the caller's parameters (`pager.rs:210-216`), but it still runs outside the backend's per-statement deadline (ADR-aacruzgon-0032), so the backend keeps its own pager (PR #45, `session.rs:341-346`).
        - **How.** The new `CassandraSession::rows_paged` sends each page through the session's execute path, so every page keeps its consistency (`LOCAL_QUORUM`) and its own deadline (and, until PR #45 removed it, the session-rebuild retry). Pages are 5,000 rows (`PAGE_SIZE`), the DataStax Java driver's default.
        - **The per-tenant id read** of `load_stored_search_parameters` is paged the same way. Each tenant's SearchParameter point reads run 16 at a time (`STORED_SEARCH_PARAMETER_READS`).
    3. **Load every listed tenant, plus every tenant whose overlay holds a definition.** The reload takes, one tenant after another:
        - the scan's tenants;
        - the tenants the overlay already holds. So a tenant whose SearchParameters are all gone has its overlay cleared. Step 2 has no delete, so that cannot happen yet, but it stays correct.

       It then drops every cached per-tenant registry (`invalidate_all`, on failure too), so each is rebuilt from the new overlay on its next use.
    4. **At startup and on every refresh.**
        - **Startup.** `start_cassandra` runs `load_all_stored_search_parameters` after the conformance seed and before `serve` (ADR-aacruzgon-0040). **If it fails, startup fails**, as PostgreSQL's and MongoDB's `initialize` do (`self.reload_stored_cache().await?`, `backends/postgres/backend.rs:1126`, `backends/mongodb/backend.rs:1701`), rather than serving with the wrong compartment rules.
        - **Refresh.** `refresh_stored_search_parameters` (the TTL task) runs the same reload over the same tenants, but continues past a failed tenant (see Consequences, decided with this entry).
        - **TTL 0.** Every tenant still loads at startup. Writes from this process update the overlay at once, as before. Writes from another process need the refresh, as on every other backend.
    5. **A concurrent create survives a reload.**
        - **The mechanism.** Each definition `note_stored_search_parameter` adds carries a sequence number. The reload reads the counter (`since`) before its id scan, and keeps every definition noted after `since` when it replaces the overlay.
        - **Why nothing is lost.** `create` notes a SearchParameter after it writes its `resources_by_type` row. So one noted before `since` is listed by the scan, and one noted after is kept.
        - **One exception.** `create` also notes a SearchParameter whose index row failed (it only warns, and leaves the intent row for the sweeper), or was skipped because a later version exists (`write.rs:552-583` at PR #43's head). The scan cannot list that one, so a reload drops it until the Step 5 sweeper writes the row. Fixing that needs `create` to pass the outcome to the note (`write.rs`).
- **Measurements:**
    - **Shared node** (`127.0.0.1:59042`, debug build, five runs): 371 partitions and 30 tenants with SearchParameters.
        - **The scan:** 3.2 to 8.2 ms.
        - **The whole load:** 308 to 340 ms. Of that, the one seeded tenant (1,372 SearchParameter rows, every one read and then skipped because the base holds its `url`) took 161 to 232 ms.
    - **A node of the worker's own with 20,000 partitions** (2,000 tenants by 10 types, project `s2w-scan`, port 59542, removed afterwards; flushed to an SSTable; the server's trace for a 5,000-row page took 13.7 ms):
        - **Through the backend's session on 8.1.9:**
            - 1,000-row pages: 167 ms;
            - 2,000 to 4,000-row pages: 91 to 97 ms;
            - **4,500 and 5,000-row pages: never returned.** They hit the 10 s request deadline.
        - **Standalone client, cdrs-tokio 10.0.0:** 64 to 69 ms with 5,000-row pages, 49 to 56 ms unpaged.
    - **The 8.1.9 driver bug behind that.** A response larger than one v5 frame (128 KiB) is lost: `cassandra-protocol` 3.3.0 gates a multi-frame envelope on its body length without the 9-byte header, and never resets that length (`frame/frame_decoder.rs:271`, `:277`).
        - **Reproduced** with standalone 8.1.9 and 10.0.0 clients on the same node. 8.1.9 lost the first page that spanned frames ("Found self-contained frame while waiting for non self-contained continuation!", then "No nodes available in query plan!" on every later request). 10.0.0 read all 20,000 partitions, paged and unpaged.
        - **Fixed upstream in 9.0.2** (krojew/cdrs-tokio `31d69ea89`, "correctly assemble multiple non-self-contained envelopes", and `1bbe426dc`), and present in 9.1.0 and 10.0.0. MSRV 1.80 in both, under the workspace's 1.91.1.
        - **No workaround kept.** Per Alan's dependency rule, the page size stays at 5,000 and the fix is the 10.0.0 upgrade, PR #45 (ADR-aacruzgon-0044). A smaller page would not help: any response over 128 KiB fails on 8.1.9, a large resource read included.
- **Alternatives considered:**
    - **Serve with an empty overlay and log a warning:** lost. Patient-scoped requests would get 403 until the first refresh, up to `HFS_SEARCH_PARAM_CACHE_TTL` (3,600 s by default), and never with TTL 0: the compartment rules would be wrong without any error.
    - **Load each tenant on its first request, or fail closed at an async point before the registry is used:** lost. A seeded tenant's first request would pay about 1,400 reads.
        - **Too many sync call sites.** `search_param_registry(tenant)` is called synchronously from 31 sites in `crates/rest/src/handlers` (`create.rs:89`, `read.rs:87`, `search.rs:375`, `compartment.rs:116`, `batch.rs:785`, `bulk_export.rs:235`, `everything/walk.rs:89` and `capabilities.rs:166`, among others).
        - **No common async hook.** The trait (`core/search.rs:546`) has none, so guaranteeing the load before every use would need a new trait method and a call in the tenant extractor, which is upstream code.
        - **Paths outside a request** (subscriptions, bulk workers) would still need the scan.
    - **A tenant registry table, so the tenants come from one partition:** lost for now. The design keeps no tenant registry in this project (`cassandra-discussion.md`, "Tenant registry"), and the schema is signed off (ADR-aacruzgon-0028). It is the follow-up if the scan grows too costly.
    - **A smaller page size to dodge the 8.1.9 bug:** lost, as above.
- **Consequences:**
    - **A new access pattern.** A full partition-key scan of `resources_by_type` at startup and on every refresh (hourly by default). It grows with tenants times resource types (one row per partition), not with resources. It touches every token range, at `LOCAL_QUORUM`, so on a three-node cluster each range is read from two replicas. The design's search-parameter-registry bullet names it, in this entry's commit. It is the one read on a data table that crosses tenants, and it is listed as R5 in the walkthrough's statement catalogue (section 2) and in `coverage.md`, in this entry's commit; it changes no table or key.
    - **The cost per seeded tenant.** Every tenant with the conformance seed costs about 1,372 point reads per load, to learn that those definitions are already in the base: about 0.2 s each on the single node (0.2 to 0.55 s in ADR-aacruzgon-0043's runs on a loaded machine). With 100 seeded tenants that would be about 20 s at startup and on each refresh [I, extrapolated]. CI reports the load on every run and warns above 5 s (ADR-aacruzgon-0048).
        - **What would remove it:** the url in the index row, or a SearchParameter-per-tenant table. Both are schema changes.
    - **Startup now depends on the scan.** If any page or read fails, the server does not start.
    - **Large results need the driver upgrade.** On cdrs-tokio 8.1.9, a 5,000-row page over 128 KiB hangs until the deadline, so startup fails. That is from about 2,000 partitions with ids of today's length (the audit's estimate; about 1,200 with 64-character tenant ids), and about 4,000 with the short ids measured. PR #45 (10.0.0) is required before a deployment that large. It is stacked two pull requests above PR #43, so PR #43 merges first, on 8.1.9 with 5,000-row pages.
    - **Known shared-code race, not changed.** `TenantSearchRegistries::for_tenant` (`tenant_registries.rs:91-101`) can cache a registry built from the old overlay if an `invalidate` lands between its loader call and its insert. That is upstream code, shared by every backend; the fix there would be a generation check in `build_and_cache`.
        - **When it can happen.** Not at startup: the load finishes before `serve`. A reload ends with `invalidate_all`, so a registry it raced is dropped at the latest by the next refresh.
        - **The one long case.** A race with a SearchParameter create on this process. With TTL 0 that lasts until the tenant's next SearchParameter write.
    - **No retry of the startup load.** One transient error among the reads stops the server, as PostgreSQL's single-statement `initialize` does. A whole-load retry before failing is possible; not done.
    - **Decided with this entry: two behaviours from PR #43's second round** (`89bbc9acb`, after CodeRabbit's second review of PR #43, thread PRRT_kwDOVAIJX860doQ4). Alan accepted both on 2026-11-06:
        - **A TTL refresh continues past a failed tenant, and that tenant keeps its old overlay; startup still fails fast.** `refresh_stored_search_parameters` runs `reload_stored_search_parameters(false)`: a tenant whose reload fails is logged with its id and keeps its previous overlay, the other tenants are still reloaded, and a summary warning gives the failed and total counts. A failed tenant *scan* still fails the whole refresh, and the server keeps serving its stale cache with a warning. The startup load (`reload(true)`) still stops at the first failure.
        - **Of two stored SearchParameters with the same `url`, the newest `last_updated` wins, and the higher `id` on a tie** (tested by `cassandra_the_newest_of_two_same_url_search_parameters_wins`, `stored_search_parameters.rs:256`). Of two stored SearchParameters with the same `url`, the one with the newest `last_updated` is in force, and the higher `id` breaks a tie (`Precedence`, `backend.rs:47-51`). The reload, the merge of noted definitions and `note_stored_search_parameter` (which now takes `last_updated`) all apply it. Before, the reload kept whichever read finished last; PostgreSQL and MongoDB let the first one in scan order win.
- **AI involvement:**
    - **Who.** CodeRabbit raised the gap twice. The Step 2 orchestrator (a Claude Code session) recommended the scan; the worker `s2-wiring` (Opus 5.5) checked it and built it.
    - **The first version.** `1a6d1c24d` (the orchestrator's) loaded only the default tenant at startup, and on each refresh the tenants this process had seen. CodeRabbit's second comment showed that a non-default tenant then starts with only the shared base, and never loads with TTL 0. This entry replaces it.
    - **What it checked.** The CQL validity (`SelectStatement.java`), the driver's paging (`pager.rs`, `query_params.rs`) and the call paths in `crates/rest`.
    - **What it found.** The 8.1.9 frame bug, reproduced against 10.0.0 and traced to upstream's fix under Alan's dependency rule.
    - **Tests.**
        - In `tests/cassandra/stored_search_parameters.rs`:
            - a non-default tenant loaded by the startup function with no request and no refresh (TTL 0);
            - a refresh picking up another process's write;
            - a stored SearchParameter that shadows Observation's `subject` compartment parameter, checked through `PatientCompartmentMatcher`, which `crates/rest/src/patient_compartment.rs:128` builds;
        - a lib node test of the paged scan (`the_partition_key_scan_follows_every_page`). At PR #43's head it creates 16 partitions of its own, pages by 7, and checks that more than two pages come back, with no duplicate, nothing the unpaged read has missing, and all 16 of its own present. Before `89bbc9acb` it made only 4 and held only on a node that already had data (ADR-aacruzgon-0047 found it failing on a fresh node).
        - **Mutation check.** With the scan replaced by an empty set, the startup and compartment tests failed.
    - **Audit.** One general-purpose subagent, given the same dependency rule. No blocking findings.
        1. **Should fix, or blocking if #43 merges before PR #45:** the 5,000-row page on 8.1.9.
            - It confirmed the frame bug and its fix in 9.0.2, independently.
            - It estimates a page passes 128 KiB at about 2,000 rows of real-length ids, or about 1,200 with 64-character tenant ids.
            - **Not changed.** Under the dependency rule this went to the orchestrator as "merge PR #45 with or before #43", with a 1,000-row page as the fallback if not.
        2. **Should fix:** `known_tenants` grew with every tenant id a request named, and each refresh read one partition per entry. **Fixed** in `c7c463285` (now `733cfe668`): the set is gone, and the overlay's tenants cover the clearing case.
        3. **Minor:** the concurrent-create comment claimed every noted SearchParameter has its index row. **Fixed** in the same commit: the comment now names the exception (see Decision 5).
        4. **Minor:** the shared registry race. **Recorded** under Consequences. `invalidate_all` at the end of a reload bounds it.
        5. **Minor:** tests that run the full load read every tenant on the shared node. **Fixed** for the compartment test, which now loads only its own tenant. The startup test keeps the full load, since that is what it tests.
        6. **Minor:** the cost. Stated under Consequences.
        7. **Minor:** no retry of the startup load. Kept, as Alan's rule; recorded under Consequences.
    - **Startup rule.** The worker proposed the fail-on-error startup rule; Alan confirmed it on 2026-11-06.
    - **What Alan changed:** Alan reviewed every Step 2 decision record on 2026-11-06, before it was committed. From his own review he found that several alternatives were stale, because `cdrs-tokio` 9.1.0 and 10.0.0 (released 2026-11-04) had already fixed driver behaviour the records relied on. He found that some alternatives were not real options or did not make sense: strawmen, restatements of the decision, and the workers' own debugging steps listed as alternatives. And he found that some records cited sources outside the repository. He had a review agent re-audit every alternative against the code, the design documents and `cdrs-tokio` 10.0.0, accepted its recommendations, and had each record rewritten from them. For this entry:
        - **What the review found.** The record had no Alternatives or Links field when it was reviewed.
        - **Alternatives now:**
            - added from the review: "serve with an empty overlay";
            - merged: its "load on first request" into the lazy-loading alternative;
            - the tenant-registry alternative now cites the design's "Tenant registry" bullet;
            - kept: the smaller-page alternative;
            - moved: the first version (`1a6d1c24d`), which was history, to AI involvement.
        - **Alan's decisions:** he confirmed the fail-on-error startup rule, and, after the code check of this entry, accepted the two behaviours of `89bbc9acb` as part of the decision. The driver upgrade (PR #45) is his, under his dependency rule, so no workaround is kept for the 8.1.9 frame bug. PR #45 is stacked above #43, so #43 merges first; whether to merge them together was still open when this entry was written.
    - **Verified by** (at `c7c463285`, before the second round):
        - `cassandra_tests` (65 of 65) and `backend_capability_contract` (10 of 10) against the shared 5.0 node;
        - both Milestone A tests;
        - the R4B check;
        - clippy with CI's allow-list, with no hits;
        - CI on `c7c463285` and `8c18196b2` (now `733cfe668` and `bb6cc8a3d`). The head of PR #44, which contains the second round, passed all three workflows (run 42266369865).
- **Links:** PR #43 (branch `cassandra/step2-wiring`, head `89bbc9acb`), thread PRRT_kwDOVAIJX860doQ4; commits `4d9c64753`, `748be1a4b`, `733cfe668` and `89bbc9acb` (before the restack `1a6d1c24d`, `d718f1a31`, `c7c463285` and `51199208b`); at PR #43's head `crates/persistence/src/backends/cassandra/backend.rs:293-509` (`refresh_stored_search_parameters` `:293`, `load_all_stored_search_parameters` `:325`, `stored_search_parameter_tenants` `:358`, `load_stored_search_parameters` `:416`), `session.rs:619` (`rows_paged`), `crates/hfs/src/main.rs:4706-4715` (the load in `start_cassandra`); the precedents `backends/postgres/backend.rs:1126` and `backends/mongodb/backend.rs:1701`; `cassandra-discussion.md` "Search parameter registry" and "Tenant registry"; ADR-aacruzgon-0020, 0028, 0033, 0040, 0043, 0044, 0047, 0048; krojew/cdrs-tokio `31d69ea89`.
