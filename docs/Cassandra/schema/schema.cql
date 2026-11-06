-- Helios Cassandra backend: table definitions.
-- DRAFT for the schema session (roadmap Step 1, item 1.1). Not signed off.
--
-- The design document fixes the keys and clustering; this file states what
-- it leaves implicit (roadmap Step 1 checklist). Reasons are in README.md
-- in this directory.
--
-- Table names are not qualified with a keyspace. Create the keyspace first
-- (keyspace-single-node.cql for one local node), then apply this file to it:
--
--   cqlsh -k helios -f schema.cql
--
-- Every statement is CREATE ... IF NOT EXISTS, so re-applying is a no-op.
--
-- Rules for every statement the backend runs against these tables:
--   * every write to resources is a lightweight transaction at serial
--     consistency LOCAL_SERIAL, never a plain write and never SERIAL (a plain
--     write bypasses Paxos and breaks the compare-and-swap);
--   * every CAS on resources sets index_keys to K(N+1), the key set of the
--     version it creates (a delete sets the empty set, 0x01 0x00), so the
--     current row always names the index rows its version writes
--     (ADR-aacruzgon-0027);
--   * never a CQL DELETE against resources (soft delete only: 410 needs the row);
--   * every update, delete and revive also rewrites resource_versions row N
--     from current, USING TIMESTAMP write_ts(N) - 1, so a crash cannot lose
--     it and an existing row always wins; a rewrite built from a read of
--     resources (delete, create_or_update, update_with_match) also carries
--     that read's write_id, one built from update's StoredResource leaves it
--     unset (ADR-aacruzgon-0026);
--   * every resource_versions row carries index_keys, the key set its version
--     wrote, never unset: V1 and V2 write the writer's own K(V), the sweeper's
--     replay writes the index_keys of the resources row it read, and a
--     rewrite of row N writes the index_keys the writer read from resources
--     at version N. The rewrite is one microsecond older, so it never
--     replaces a stored blob (ADR-aacruzgon-0027);
--   * the old keys of a write, K(N), are the index_keys of the resources row
--     the writer read, and that read's version_id is the one its CAS is
--     conditioned on. Repair's keys are the tag check's resources.index_keys,
--     and the sweeper's K(V-1) is row V-1's index_keys. None is ever
--     re-extracted (ADR-aacruzgon-0027);
--   * every write to resource_versions and the index tables carries USING
--     TIMESTAMP derived from write_ts; a plain write would lose to them;
--   * never bind null; leave absent columns unset (null writes a cell tombstone).
--     The one deliberate null is the literal "SET deleted_at = null" in the
--     revive CAS, which clears the delete and writes exactly one cell
--     tombstone, on purpose (checked on 5.0.9, 2026-10-13; ADR-aacruzgon-0024);
--   * never ALLOW FILTERING;
--   * never write an index row whose code, system or uri value is longer than
--     2,048 bytes in UTF-8, or whose param is longer than 128 bytes: the write
--     is refused with 400 before its intent row, so nothing is written
--     (ADR-aacruzgon-0026, 0027);
--   * never store a SearchParameter whose code is longer than 128 bytes in
--     UTF-8: it is refused with 400, so no param over 128 bytes reaches the
--     registry (ADR-aacruzgon-0027).
--
-- Common types:
--   tenant_id, resource_type, id   text (id is never uuid: FHIR ids are
--                                  case-sensitive strings, and text sorts
--                                  bytewise like Rust strings)
--   version_id                     int (sorts numerically)
--   last_updated, created_at,
--   deleted_at, start, end         timestamp (milliseconds)
--   fhir_version                   text, FhirVersion::as_mime_param() ("4.0");
--                                  fixed at create, so it is the same on every
--                                  row of a resource
--   write_ts                       bigint, microseconds, used in USING TIMESTAMP;
--                                  always last_updated x 1000, and
--                                  last_updated(N+1) = max(now, last_updated(N) + 1 ms)
--   write_id                       uuid, one per CAS attempt, set by the CAS
--                                  and copied into that version's
--                                  resource_versions row
--
-- Compaction: no table sets it, so every table gets the node default from
-- cassandra_latest.yaml: UnifiedCompactionStrategy (UCS) with scaling
-- parameter T4 (tiered, fan factor 4). Setting any compaction sub-option
-- erases all the others, so an explicit map would silently drop the yaml's
-- other UCS settings. README.md gives the reason per table and what
-- measurement would change it.


-- Current version of every resource, and the compare-and-swap target for
-- every write. Point reads only: read, exists, _id, and the tag check.
--
-- index_keys (ADR-aacruzgon-0027, walkthrough G22) is K(V) of the current
-- version V, written by the CAS that created V. It is the same blob as
-- resource_versions row V's index_keys, encoded as described below that
-- table. Because the CAS writes it, a writer that reads version N here and
-- whose CAS is conditioned on version_id = N deletes exactly the rows N
-- wrote, even when row N of resource_versions is not written yet.
CREATE TABLE IF NOT EXISTS resources (
    tenant_id     text,
    resource_type text,
    id            text,
    version_id    int,
    data          text,       -- resource JSON
    last_updated  timestamp,
    created_at    timestamp,
    deleted_at    timestamp,  -- unset while live; set by a delete
    fhir_version  text,
    write_ts      bigint,     -- timestamp of this version's index writes
    write_id      uuid,       -- the CAS attempt that wrote this version; read back
                              -- after an LWT timeout to learn the outcome
    index_keys    blob,       -- K(V) of this version, set by every CAS; the empty
                              -- set after a delete (ADR-aacruzgon-0027)
    PRIMARY KEY ((tenant_id, resource_type, id))
) WITH comment = 'Current version per resource; LWT target for every write';


-- Every version, newest first: vread, list_versions, and delete markers
-- (a version row with deleted_at set and no data).
--
-- index_keys (ADR-aacruzgon-0027, walkthrough G22) is K(V), the exact index
-- rows version V wrote, so that a later write, the sweeper and repair delete
-- what was written, not what today's registry would extract. The sweeper
-- reads K(V-1) from here, since resources holds only the current version's.
-- Encoding, format 1, for this column and resources.index_keys:
--   byte    format version, 0x01
--   varint  row count (unsigned LEB128)
--   rows, each a table tag byte followed by the key columns that are not
--   implied by the version row (tenant_id, resource_type, id, and
--   last_updated = this row's last_updated are implied):
--     0x01 resources_by_type        nothing
--     0x02 token_code_index         param, code, system
--     0x03 token_system_code_index  param, system, code
--     0x04 token_system_index       param, system, code
--     0x05 date_index               param, start, end
--     0x06 date_index_by_end        param, end, start
--     0x07 uri_index                param, uri
--   text = varint byte length + UTF-8 bytes; timestamp = 8-byte big-endian
--   signed milliseconds. A delete marker's K(V) is empty: 0x01 0x00.
-- The worked example's K(1) (walkthrough section 7, 8 rows) is 289 bytes.
CREATE TABLE IF NOT EXISTS resource_versions (
    tenant_id     text,
    resource_type text,
    id            text,
    version_id    int,
    data          text,       -- unset for a delete marker
    last_updated  timestamp,
    deleted_at    timestamp,  -- set only on a delete marker
    fhir_version  text,
    write_ts      bigint,
    write_id      uuid,       -- the CAS attempt that wrote this version; unset when
                              -- the row came from a rewrite built from update's
                              -- StoredResource. Decides an unknown CAS outcome
                              -- that a later version superseded (ADR-aacruzgon-0026)
    index_keys    blob,       -- the index rows this version wrote, encoded as
                              -- below; never unset (ADR-aacruzgon-0027)
    PRIMARY KEY ((tenant_id, resource_type, id), version_id)
) WITH CLUSTERING ORDER BY (version_id DESC)
   AND comment = 'All versions per resource; vread, list_versions, delete markers';


-- Outbox for the index sweeper. One intent row is written before every CAS
-- attempt and deleted after the index writes, or at once when that attempt's
-- CAS is not applied. A create_or_update retry writes a new row with its own
-- enqueued_at and never rewrites an earlier attempt's row (ADR-aacruzgon-0024).
-- Not led by tenant_id: the sweeper must find work without a tenant list.
-- Hourly buckets (a 24 h TTL spans up to 25) x 16 shards.
-- bucket = UTC hour 'YYYY-MM-DDTHH'; shard = hash(tenant_id, id) mod 16.
-- enqueued_at leads the clustering key so the sweeper reads only a time
-- slice past its low-water mark, never the tombstones of finished intents
-- (measured on a node: a whole-partition read scanned 3,000 of them).
-- version_id is the version the attempt's CAS will create. The sweeper
-- replays that version's index writes while it is current, and only deletes
-- the previous version's keys once it is superseded (ADR-aacruzgon-0013).
-- write_id is kept for diagnostics only, so an operator can match a stuck
-- intent to the request that wrote it; the sweeper never reads it
-- (ADR-aacruzgon-0027, walkthrough G32).
-- gc_grace_seconds is lowered here too, which the roadmap checklist does not
-- list: a resurrected intent row is harmless, because the sweeper either
-- deletes it or replays writes that change nothing.
CREATE TABLE IF NOT EXISTS pending_index_writes (
    bucket        text,
    shard         smallint,
    enqueued_at   timeuuid,
    tenant_id     text,
    resource_type text,
    id            text,
    write_id      uuid,
    version_id    int,
    PRIMARY KEY ((bucket, shard), enqueued_at, tenant_id, resource_type, id)
) WITH default_time_to_live = 86400
   AND gc_grace_seconds = 10800
   AND comment = 'Intent rows for the index sweeper; keys, write_id and version_id, never content';


-- ---------------------------------------------------------------------------
-- Index tables. Rewritten on every create, update and delete, all USING
-- TIMESTAMP write_ts, in logged batches under 5 KiB. Every row carries the
-- resource's version_id; search compares it with resources.version_id (the
-- tag check), so a stale or resurrected row never surfaces. That is why
-- these seven tables lower gc_grace_seconds to 10800 s (3 h, the node's
-- max_hint_window). resources and resource_versions keep the 10-day default.
-- A hint lives no longer than the smallest gc_grace_seconds of its tables
-- (Hint.java in 5.0.9), so a hint for these tables expires at 3 h even after
-- a short outage. On the cluster a replica returning from any outage is
-- repaired promptly; resurrection is narrowed, not ruled out
-- (ADR-aacruzgon-0027).
-- resources_by_type also carries fhir_version, so count can apply the
-- request's FHIR version (SearchQuery.fhir_version, upstream #2254) without a
-- point read per row (ADR-aacruzgon-0026).
-- '' stands for "no system" (key columns cannot be null).
-- ---------------------------------------------------------------------------

-- Unfiltered type search, _lastUpdated, count, default-order paging.
CREATE TABLE IF NOT EXISTS resources_by_type (
    tenant_id     text,
    resource_type text,
    last_updated  timestamp,
    id            text,
    version_id    int,
    fhir_version  text,       -- the resource's, as on resources; read by count
    PRIMARY KEY ((tenant_id, resource_type), last_updated, id)
) WITH CLUSTERING ORDER BY (last_updated DESC, id DESC)
   AND gc_grace_seconds = 10800
   AND comment = 'Type search, _lastUpdated, count, default order';

-- Token form "code" (any system). system is clustering so two codings with
-- the same code in different systems stay two rows.
CREATE TABLE IF NOT EXISTS token_code_index (
    tenant_id     text,
    resource_type text,
    param         text,
    code          text,
    last_updated  timestamp,
    id            text,
    system        text,
    version_id    int,
    PRIMARY KEY ((tenant_id, resource_type, param, code), last_updated, id, system)
) WITH CLUSTERING ORDER BY (last_updated DESC, id DESC, system ASC)
   AND gc_grace_seconds = 10800
   AND comment = 'Token search, form code';

-- Token forms "system|code" and "|code" (system '' or the implicit marker).
CREATE TABLE IF NOT EXISTS token_system_code_index (
    tenant_id     text,
    resource_type text,
    param         text,
    system        text,
    code          text,
    last_updated  timestamp,
    id            text,
    version_id    int,
    PRIMARY KEY ((tenant_id, resource_type, param, system, code), last_updated, id)
) WITH CLUSTERING ORDER BY (last_updated DESC, id DESC)
   AND gc_grace_seconds = 10800
   AND comment = 'Token search, forms system|code and |code';

-- Token form "system|".
CREATE TABLE IF NOT EXISTS token_system_index (
    tenant_id     text,
    resource_type text,
    param         text,
    system        text,
    last_updated  timestamp,
    id            text,
    code          text,
    version_id    int,
    PRIMARY KEY ((tenant_id, resource_type, param, system), last_updated, id, code)
) WITH CLUSTERING ORDER BY (last_updated DESC, id DESC, code ASC)
   AND gc_grace_seconds = 10800
   AND comment = 'Token search, form system|';

-- Date-only search, prefixes that bound the range start (eq, lt, le, ap, sa,
-- and the start half of ne and ge). [start, end) is half-open and finite:
-- indexed_range maps an open Period end to its open_end sentinel.
CREATE TABLE IF NOT EXISTS date_index (
    tenant_id     text,
    resource_type text,
    param         text,
    start         timestamp,
    id            text,
    end           timestamp,
    version_id    int,
    PRIMARY KEY ((tenant_id, resource_type, param), start, id, end)
) WITH CLUSTERING ORDER BY (start ASC, id ASC, end ASC)
   AND gc_grace_seconds = 10800
   AND comment = 'Date-only search, bounded by range start';

-- Date-only search, prefixes that bound the range end (gt, eb, and the end
-- half of ne and ge).
CREATE TABLE IF NOT EXISTS date_index_by_end (
    tenant_id     text,
    resource_type text,
    param         text,
    end           timestamp,
    id            text,
    start         timestamp,
    version_id    int,
    PRIMARY KEY ((tenant_id, resource_type, param), end, id, start)
) WITH CLUSTERING ORDER BY (end ASC, id ASC, start ASC)
   AND gc_grace_seconds = 10800
   AND comment = 'Date-only search, bounded by range end';

-- URI search: exact, case-sensitive whole-URI match, so the URI is the key.
CREATE TABLE IF NOT EXISTS uri_index (
    tenant_id     text,
    resource_type text,
    param         text,
    uri           text,
    last_updated  timestamp,
    id            text,
    version_id    int,
    PRIMARY KEY ((tenant_id, resource_type, param, uri), last_updated, id)
) WITH CLUSTERING ORDER BY (last_updated DESC, id DESC)
   AND gc_grace_seconds = 10800
   AND comment = 'URI search, exact match';
