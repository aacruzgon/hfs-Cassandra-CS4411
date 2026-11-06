# Query coverage table

**DRAFT for the schema session (roadmap item 1.3).** Not yet reviewed by the team. Open items carry an ID: `C1`, `C2`, … are specific to this table (section 7); `G1`, `G2`, … are gaps in [`walkthrough.md`](walkthrough.md) section 8. The directory overview is [`README.md`](README.md).

For every search the backend must serve or refuse, this says which tables serve it and what the client gets. It follows the design in [`cassandra-discussion.md`](../cassandra-discussion.md) ("Proposed Cassandra data model" and "Consistency, failure handling and rejection") and the roadmap's Step 1 **Spec.** paragraph: the rows are the [Patient](https://hl7.org/fhir/R4/patient.html#search) and [Observation](https://hl7.org/fhir/R4/observation.html#search) parameters of type token, date or uri, plus `_id` and `_lastUpdated` from [R4 search.html#all](https://hl7.org/fhir/R4/search.html#all). The parameter list comes from `data/search-parameters-r4.json`.

This file holds expected results. The CQL for each case is in [`walkthrough.md`](walkthrough.md) section 5.

**Columns.**

- **Request**: an example query string.
- **Table(s)**: the table read. Every partition key starts with `tenant_id, resource_type`; only the rest is shown.
- **Slice**: the clustering restriction on that partition. "Cursor" means `(last_updated, id) < (?, ?)` from the page cursor.
- **Residual**: what is checked in Rust after the read.
- **Expected**: the HTTP result.

**True of every row that returns results.** Each candidate's `resources` row is loaded. It is dropped if its `version_id` differs from the index row's tag or if `deleted_at > 0` (a live row carries `0`, walkthrough G39). It is also dropped if the query names a FHIR version (`query.fhir_version`) that differs from `resources.fhir_version`, or carries a compartment filter (`query.compartment`) the resource is not a member of (section 5). Results come in `(last_updated DESC, id DESC)` order, deduplicated on `(last_updated, id)`. The exception is a date-only search sliced on a date table (section 3, G20): it comes in that table's order, `(start, id)` or `(end, id)` ascending, with each resource once, at its first matching row. `_count` is the page size, and `next` is minted only when a `_count + 1`th match exists.

## 1. The parameters

### Token ([R4 search.html#token](https://hl7.org/fhir/R4/search.html#token))

The "Stored system" column is what the shared converter (`ValueConverter::convert_to_token` in `crates/persistence/src/search/converters.rs`) writes. `''` is the design's sentinel for "no system". `IMPLICIT` is the marker system for `code`-typed elements, `IMPLICIT_TOKEN_SYSTEM` = `"urn:x-helios:implicit code system"`.

| Resource | Parameter | Element (type) | Stored system | Forms that apply |
| --- | --- | --- | --- | --- |
| Patient | `active` | `Patient.active` (boolean) | `''` | `code` only (pipe SHALL NOT be used) |
| Patient | `deceased` | `deceased.exists() and deceased != false` (boolean result) | `''` | `code` only |
| Patient | `email` | `telecom.where(system='email')` (ContactPoint) | the ContactPoint's `system` value, here `email` | `code` only |
| Patient | `phone` | `telecom.where(system='phone')` (ContactPoint) | `phone` | `code` only |
| Patient | `telecom` | `Patient.telecom` (ContactPoint) | `phone`, `email`, `fax`, … | `code` only |
| Patient | `gender` | `Patient.gender` (code) | `IMPLICIT` | all four |
| Patient | `address-use` | `Patient.address.use` (code) | `IMPLICIT` | all four |
| Patient | `identifier` | `Patient.identifier` (Identifier) | `Identifier.system` or `''` | all four |
| Patient | `language` | `communication.language` (CodeableConcept) | `Coding.system` or `''` | all four |
| Observation | `status` | `Observation.status` (code) | `IMPLICIT` | all four |
| Observation | `identifier` | `Observation.identifier` (Identifier) | `Identifier.system` or `''` | all four |
| Observation | `code`, `category`, `method`, `data-absent-reason`, `value-concept` | CodeableConcept | `Coding.system` or `''` | all four |
| Observation | `component-code`, `component-data-absent-reason`, `component-value-concept` | `Observation.component.*` (CodeableConcept) | as above | all four |
| Observation | `combo-code`, `combo-data-absent-reason`, `combo-value-concept` | the element on Observation **and** its components (CodeableConcept) | as above | all four |
| both | `_id` | `Resource.id` (id) | The extractor returns it as a token; those rows are dropped before the write (P5, ADR-aacruzgon-0027). Served by point reads on `resources`, never from an index table | `code`; a value with a pipe is compared whole and matches nothing (C2, section 2) |

| both | `_tag` | `Resource.meta.tag` (Coding) | `Coding.system` or `''` | all four (in scope, C1) |
| both | `_security` | `Resource.meta.security` (Coding) | `Coding.system` or `''` | all four (in scope, C1) |

That is 9 Patient, 13 Observation and 3 shared token parameters (`_id`, `_tag`, `_security`). `_tag` and `_security` are served by the three token tables like any Coding (`meta_params_suite.rs` asserts both directions).

### Date ([R4 search.html#date](https://hl7.org/fhir/R4/search.html#date))

| Resource | Parameter | Element (type) |
| --- | --- | --- |
| Patient | `birthdate` | `Patient.birthDate` (date: a day, month or year range) |
| Patient | `death-date` | `Patient.deceased as dateTime` |
| Observation | `date` | `Observation.effective` (dateTime, Period, Timing or instant) |
| Observation | `value-date` | `Observation.value as dateTime` or `as Period` |
| both | `_lastUpdated` | `Resource.meta.lastUpdated` (instant). The extractor returns it as a date; those rows are dropped before the write (P5). Served from `resources_by_type`, never from the date tables |

### URI ([R4 search.html#uri](https://hl7.org/fhir/R4/search.html#uri))

**Neither Patient nor Observation has a uri parameter in R4.** The only uri parameters that apply to them are the Resource-level `_profile` (`Resource.meta.profile`, canonical) and `_source` (`Resource.meta.source`). Section 4 uses them. The extractor already indexes `_profile`, `_source` and the Resource-level tokens `_tag` and `_security` (`search/extractor.rs`; its tests assert it). They are in scope (C1, ADR-aacruzgon-0027), so section 4 stands.

## 2. Token forms

Forms that read two partitions run one query per partition and merge the sorted streams in Rust: a multi-partition `IN` returns rows partition by partition, not in page order (walkthrough 5.2, checked on a 5.0.9 node, 2026-10-13).

| Form | Request | Table(s) and partition(s) | Slice | Residual | Expected |
| --- | --- | --- | --- | --- | --- |
| `code` | `Observation?code=8867-4` | `token_code_index`, one partition `(param, code)` | cursor | collapse rows of one resource (different `system`) | 200 searchset; the code in any system, including `''` and `IMPLICIT` |
| `system\|code` | `Observation?code=http://loinc.org\|8867-4` | `token_system_code_index`, two partitions: `(param, 'http://loinc.org', code)` and `(param, IMPLICIT, code)`; merge two sorted streams | cursor on each | dedupe | 200; matches system `http://loinc.org` or `IMPLICIT` |
| `system\|code` on a code element | `Patient?gender=http://hl7.org/fhir/administrative-gender\|female` | as above | cursor | dedupe | 200; matches through the `IMPLICIT` partition, whatever system is named |
| `\|code` | `Patient?identifier=\|12345` | `token_system_code_index`, partitions `(param, '', code)` and `(param, IMPLICIT, code)` | cursor | dedupe | 200; matches values with no system or the `IMPLICIT` marker |
| `system\|` | `Patient?identifier=http://hospital.example/mrn\|` | `token_system_index`, one partition `(param, system)` | cursor | collapse rows of one resource (different `code`) | 200; matches that system only, never `IMPLICIT` |
| `system\|` on a code element | `Patient?gender=http://hl7.org/fhir/administrative-gender\|` | `token_system_index` | cursor | — | 200 with no gender matches (the marker is not matched). Same as PostgreSQL and MongoDB (C3) |
| case | `Patient?gender=Female` | `token_code_index` | cursor | — | 200, no match: matching is case-sensitive (design decision) |
| boolean | `Patient?active=true` | `token_code_index`, `(param, 'true')` | cursor | — | 200; matches `active: true` |
| ContactPoint | `Patient?phone=555-0100` | `token_code_index`, `(param, '555-0100')` | cursor | — | 200; exact, case-sensitive value |
| pipe on ContactPoint or boolean | `Patient?phone=phone\|555-0100`, `Patient?active=\|true` | as the form says | cursor | — | 200, matched by the general token rules, as SQLite and PostgreSQL do: neither has a case for ContactPoint or boolean (`backends/sqlite/search/parameter_handlers/token.rs:12-19`, `86-120`; `backends/postgres/search/query_builder.rs:2344-2394`). A ContactPoint is stored with its own `system` (`phone`), and a boolean with no system. So `phone\|555-0100` matches the phone `555-0100`, `\|true` matches `active: true`, and `email\|` matches any email. The spec says clients SHALL NOT send the pipe here; the server does not refuse it (C4, decided) |
| `_id` | `Patient?_id=abc` | `resources`, point read `(id)` | — | `deleted_at` 0 (live) | 200; zero or one entry; case-sensitive |
| `_id` with a pipe | `Patient?_id=sys\|abc`, `Patient?_id=\|abc` | `resources`, point read of the whole value as the id | — | — | 200, empty searchset. The value is compared whole with `Resource.id`, pipe included, as SQLite and PostgreSQL do (`backends/sqlite/search/query_builder.rs:1159-1224`, `backends/postgres/search/query_builder.rs:1870-1929`, both `id IN (…)` on `value.value`); an id never contains `\|` ([R4 datatypes.html#id](https://hl7.org/fhir/R4/datatypes.html#id)), so nothing matches (C2, decided) |
| `_id` list | `Patient?_id=a,b,c` | `resources`, concurrent point reads (`read_batch`) | — | sort in Rust, cut at cursor | 200; existing, live ids |
| two `_id` parameters (AND) | `Patient?_id=a,b&_id=b`; every Patient search under a `patient/` scope, to which REST appends `_id=<patient>` | intersect the id sets in Rust, then point-read only the intersection; never an index table | — | sort in Rust, cut at cursor | 200; only ids in every `_id` parameter (`b` here); disjoint sets give an empty searchset (walkthrough 5.4, G37) |

## 3. Date prefixes ([R4 search.html#prefix](https://hl7.org/fhir/R4/search.html#prefix))

The stored value is the half-open range `[ts, te)` from `indexed_range(…, Millis)`; an open end is the `open_end` sentinel (year 9999), an open start `open_start` (year 1). The query value is `[s, e)`: `birthdate=1970` is `[1970-01-01, 1971-01-01)`. The test is the shared `range_predicate` (`search/date_value.rs`), which encodes R4's range-overlap rules. The slices follow the design's date rules.

**Date-only search** (one date parameter, nothing else). One partition `(param)` per table.

| Prefix | Match when | Table | Slice | Residual |
| --- | --- | --- | --- | --- |
| `eq` (default) | `ts ≥ s` and `te ≤ e` | `date_index` | `start ≥ s AND start < e` | `te ≤ e` |
| `ne` | `ts < s` or `te > e` | `date_index` ∪ `date_index_by_end` | `start < s`; `end > e` | dedupe |
| `gt` | `te > e` | `date_index_by_end` | `end > e` | — |
| `lt` | `ts < s` | `date_index` | `start < s` | — |
| `ge` | `te > e`, or `ts ≥ s` and `te ≤ e` | `date_index_by_end` | `end > s` (every match has `te > s`) | full predicate |
| `le` | `ts < s`, or `ts ≥ s` and `te ≤ e` | `date_index` | `start < e` | full predicate |
| `sa` | `ts ≥ e` | `date_index` | `start ≥ e` | — |
| `eb` | `te ≤ s` | `date_index_by_end` | `end ≤ s` | — |
| `ap` | `ts < high` and `te > low`; the window widens `[s, e)` by 12 months (year), 1 month (month), 1 day (day), 10 min (minute) or 10 s (finer) | `date_index` | `start < high` | `te > low` |

`ge` is the single slice `end > s` (ADR-aacruzgon-0027). Every row is also checked with `RangePredicate::matches`, whatever the slice. **Expected for all nine:** 200 searchset.
- **Paging.** The page comes in its date table's own order, `(start, id)` or `(end, id)` ascending, keyset-paged with the table named in the cursor's third element. A resource with several values appears once, at its first matching row (walkthrough 5.7).
- **Cap.** The search counts distinct candidates point-read per page; over the cap it is refused with 422 `SearchError::TooManyResults` (G27).
- **`ne`.** It reads `date_index`'s slice `start < s` to the end, then `date_index_by_end`'s slice `end > e`, each in its own order. A resource is emitted once, at its first matching row in that order, and the cursor's third element names the table being read (G20, ADR-aacruzgon-0027; walkthrough 5.7).
- **Comma OR** (`birthdate=1970,1980`). Slices on one table are merged in its order. A list that spans both tables pages like `ne`: all of `date_index`'s slices, then all of `date_index_by_end`'s.
- **Open Periods.** A Period with no end is stored with the `open_end` sentinel, so it falls in every `end >` slice.

**Date with any other parameter** (`Patient?gender=female&birthdate=ge1970`): the date tables are not read. The other parameter drives. The date is a residual filter: re-extract the date values from the loaded `resources` row and apply `RangePredicate::matches`. Expected: 200, keyset-paged. Two date parameters, a repeated date, or a date with `_lastUpdated`, with nothing else, is still date-only. `_lastUpdated` is sliced if present, otherwise the first date parameter in query order, and the rest are residuals (G20, walkthrough 5.7).

**`_lastUpdated`.** One partition of `resources_by_type`, clustered on `last_updated DESC, id DESC`. A stored `last_updated` `t` is a millisecond point, the range `[t, t + 1 ms)`; the query value `[s, e)` comes from `FhirDateValue::range_at(StorageResolution::Millis)`. Putting that range into `range_predicate` gives one or two bounds on `last_updated`, so paging stays native.

| Prefix | `range_predicate` on `[t, t + 1 ms)` | Slice on `last_updated` | Bound kind |
| --- | --- | --- | --- |
| `eq` | `t ≥ s` and `t + 1 ms ≤ e` | `≥ s AND < e` | lower and upper |
| `ne` | `t < s` or `t + 1 ms > e` | `≥ e`, then `< s` (two slices; in `DESC` order a concatenation) | lower; upper |
| `gt`, `sa` | `t + 1 ms > e`; `t ≥ e` | `≥ e` | lower |
| `lt`, `eb` | `t < s`; `t + 1 ms ≤ s` | `< s` | upper |
| `ge` | `t ≥ e` or (`t ≥ s` and `t < e`) | `≥ s` | lower |
| `le` | `t < s` or (`t ≥ s` and `t < e`) | `< e` | upper |
| `ap` | `t < high` and `t + 1 ms > low` | `≥ low AND < high` | lower and upper |

Expected: 200 searchset, cursor-paged. **Resolved (G19, checked on a 5.0.9 node, 2026-10-13):** a lower bound and the cursor go in one query. An upper bound and the cursor are both end bounds and the node accepts only one, so the backend sends whichever is tighter. `RangePredicate::matches` still runs on every row. With another parameter, `_lastUpdated` is a residual filter on `resources.last_updated`, as the design says for every date predicate combined with another parameter; a slice on the driving token or URI partition is an optimisation option recorded in G19.

## 4. URI

| Request | Table(s) | Slice | Expected |
| --- | --- | --- | --- |
| `Patient?_profile=http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient` | `uri_index`, one partition `(param, uri)` | cursor | 200; whole-URI match, case-sensitive |
| the same URI with a trailing `/`, or in different case | a different partition | cursor | 200, no match: no normalization (roadmap 6c) |
| `Observation?_source=urn:uuid:…` | `uri_index` | cursor | 200; exact |
| comma OR of two URIs | two partitions, merged | cursor | 200; union |

## 5. Combining, counting and paging

Spec: [R4 search.html#combining](https://hl7.org/fhir/R4/search.html#combining), [#count](https://hl7.org/fhir/R4/search.html#count), [R4 http.html#paging](https://hl7.org/fhir/R4/http.html#paging), [build #paging](https://build.fhir.org/http.html#paging).

| Case | Request | Tables and method | Expected |
| --- | --- | --- | --- |
| AND of parameters | `Observation?code=8867-4&status=final` | One token partition drives from the cursor. Each candidate is checked against the other with a clustering point read (`… AND last_updated = ? AND id = ?`). The driver is the first parameter in this order: `_id`; `system\|code` and `\|code` tokens; identity URIs (a canonical resource's own `url`); code-only tokens, `_profile`, `_source` and other URIs; `system\|` tokens; `_lastUpdated` only when nothing else can drive. Ties by query order (C5; walkthrough 5.8 lists the identity URIs) | 200; intersection; keyset-paged |
| repeated parameter | `Observation?code=8867-4&code=8310-5` | as AND | 200; resources having both |
| comma OR | `Observation?code=8867-4,8310-5` | union of partitions, one query each, merged in the shared order, deduped | 200; union |
| OR across forms | `code=http://loinc.org\|8867-4,8310-5` | three partitions in two tables, merged | 200; union |
| token + URI | `Patient?gender=female&_profile=…` | either drives; the other is a point read | 200 |
| `_id` + other | `Patient?_id=a,b&gender=female` | `_id` drives; others checked | 200 |
| token + date, or token + `_lastUpdated` | see section 3 | the date is a residual filter | 200 |
| two date parameters, or a repeated date (`date=ge2020&date=lt2021`) | — | date-only: the first date parameter is sliced, the rest are residuals; with `_lastUpdated` among them, `_lastUpdated` is sliced (G20) | 200; paged in the sliced table's order |
| unfiltered type search | `GET /Patient` | `resources_by_type`, one partition, cursor slice | 200; all live Patients |
| `_count=n` | `Patient?gender=female&_count=50` | `LIMIT` is a batch size; read until 51 distinct matches | 200; at most 50 entries. REST clamps `_count` to `HFS_MAX_PAGE_SIZE` (default 1000, `crates/rest/src/config.rs:1331-1332`) before the backend is called (`handlers/search.rs:606-612`); the backend has no limit of its own and declares `MaxPageSize(1000)`, as the other backends do (C6, decided) |
| `_count=0` | `Patient?gender=female&_count=0` | `search_count`: stream every match | 200 searchset with `total` and no entries (behaves as `_summary=count`). Tag checks and the candidate cap apply (G7, ADR-aacruzgon-0027); over the cap, 422 |
| `_count=0`, unfiltered | `GET /Patient?_count=0` | full scan of the `resources_by_type` partition, reading its tombstones | 200; `total` only |
| next page | the `next` link | `PageCursor` with `sort_values = [last_updated RFC 3339 ms, reserved bucket slot]`, `resource_id = id`; slice `(last_updated, id) < (?, ?)`. The page key is `(last_updated, id, bucket)`; `id` travels in `resource_id`, so the design's "third slot" is the second element of `sort_values`. A date-only search's cursor adds a third element, `"start"` or `"end"`, naming its table, and slices that table in its own ascending order (section 3) | 200; no duplicates or gaps; no `previous` on page 1; no `next` after the last page |
| malformed cursor | `_cursor=garbage` | backend | 400 `InvalidCursor` |
| `count(tenant, Some(type))` | internal | full scan of `resources_by_type`, no tag check | the number of live rows; it differs from a filtered `search_count` only while a write is in flight, while a crashed write waits for the sweeper, and in the design's stated limitations (G7, walkthrough 5.9) |
| `count(tenant, None)` | internal (all types) | the scan above for each resource type name of every enabled FHIR version (`FhirResourceTypeProvider::get_resource_type_names`, `crates/persistence/src/search/type_qualifier.rs:98-118`), each name once (G7, decided) | the number of live rows |
| `_total=accurate` or `_total=estimate` | `Patient?gender=female&_total=accurate` | the page as usual, plus one `search_count` of the same query (`SearchQuery::wants_total()`); costs and caveats as `_count=0` | 200; `Bundle.total` set. `_total=none` or absent: no `total` |
| FHIR version named | `GET /Patient?gender=female` with `Accept: application/fhir+json; fhirVersion=4.0` (`query.fhir_version`, upstream #2254) | as the search; residual `resources.fhir_version = '4.0'` after the tag check | 200; only resources stored under that version (walkthrough 5.9, G34) |
| FHIR version named, unfiltered count | `GET /Patient?_count=0` with `fhirVersion=4.0` | `resources_by_type`, whole partition, counting rows whose `fhir_version` column is `'4.0'`, in Rust; no point reads | 200; `total` of that version only |
| compartment search | `GET /Patient/123/Observation?code=8867-4` | the token partition drives; residual: re-extract `subject` and `performer` (R4 Patient compartment for Observation) and keep members of `Patient/123` (walkthrough 5.11, G33) | 200; members only, keyset-paged |
| `patient/` scope | `GET /Observation?status=final` with a token scoped `patient/Observation.rs` and launch patient 123 | as the compartment search: REST sets `query.compartment` | 200; the patient's Observations only. Never unfiltered. Chained and `_has` searches are confined only in their result, not in their intermediate searches: a known upstream defect ([HeliosSoftware/hfs#2501](https://github.com/HeliosSoftware/hfs/issues/2501); section 6 and C7) |
| compartment, no other parameter | `GET /Patient/123/Observation` | `resources_by_type` drives; residual as above | 200, or the cap row below |
| compartment count | `GET /Patient/123/Observation?_count=0`, or `_total=accurate` | the residual stream run to the end, under the cap | 200 with the members' `total`; over the cap, the cap row below. Never a partial count |
| compartment over the cap | a compartment search or count that reads more candidates than the cap before it finishes | — | 422 `SearchError::TooManyResults` (issue `processing`, `crates/rest/src/error.rs:1123-1125`); never a page or count that was only partly filtered |
| candidate cap exceeded | e.g. `Observation?date=lt2030` on a large tenant | date tables, a compartment search or a filtered `search_count`; the cap counts distinct candidates point-read, per page or per count | 422 `SearchError::TooManyResults`, issue `processing` (`crates/rest/src/error.rs:1123-1125`). No search produces `too-costly` (G27, ADR-aacruzgon-0027) |

## 6. Rejections

"Layer" is where the refusal is produced. The backend's `search()` and `search_count()` run, in this order: `validate_date_values`, `validate_value_presence`, `reject_unsupported_metadata_modifier`, then the type check, the modifier check, includes, text, and `_sort`/`_offset` (design, "Capabilities and rejection"). Every refusal carries an OperationOutcome.

| Request | Layer | Status | Error |
| --- | --- | --- | --- |
| String: `Patient?name=smith`, `family=`, `address-city=` | backend | 400 | `UnsupportedParameterType` ([#string](https://hl7.org/fhir/R4/search.html#string)) |
| Reference: `Observation?subject=Patient/1`, `patient=`, `Patient?general-practitioner=` | backend | 400 | `UnsupportedParameterType` ([#reference](https://hl7.org/fhir/R4/search.html#reference)) |
| Quantity: `Observation?value-quantity=gt5.4\|http://unitsofmeasure.org\|mg` | backend | 400 | `UnsupportedParameterType` ([#quantity](https://hl7.org/fhir/R4/search.html#quantity)) |
| Composite: `Observation?code-value-quantity=8480-6$gt100` | backend | 400 | `UnsupportedParameterType` ([#composite](https://hl7.org/fhir/R4/search.html#composite)) |
| Composite with the wrong part count: `Observation?code-value-quantity=8480-6` | REST, before the backend | 400 | `SearchError::InvalidComposite` ("composite value '8480-6' has 1 component(s) separated by '$', but this parameter expects 2"), from the shared gate `validate_composite_values` (`crates/persistence/src/search/composite_value.rs:127-141`), which REST runs in `search_query_builder.rs:243`; REST maps it to `BadRequest`, issue `invalid` (`crates/rest/src/error.rs:1109-1115`, `570-571`). Pinned by a Step 6 test row (C7, decided) |
| Number: none on Patient or Observation in R4; e.g. `RiskAssessment?probability=gt0.8` | backend | 400 | `UnsupportedParameterType` ([#number](https://hl7.org/fhir/R4/search.html#number)) |
| `:exact` on token, date or uri: `gender:exact=female` | REST (`validate_modifier`) | 400 | `RestError::InvalidParameter` ([#modifiers](https://hl7.org/fhir/R4/search.html#modifiers)) |
| `:exact` on string: `name:exact=Smith` | backend | 400 | `UnsupportedParameterType` (the type check runs first) |
| `:contains` on token or date | REST | 400 | `InvalidParameter` |
| `:contains`, `:above`, `:below` on uri: `_profile:below=http://…` | backend | 400 | `UnsupportedModifier` |
| `:missing`: `gender:missing=true`, `birthdate:missing=false`, `_id:missing=true`, `_lastUpdated:missing=true` | backend | 400 | `UnsupportedModifier` |
| `:not`: `gender:not=male`, `_id:not=abc` | backend | 400 | `UnsupportedModifier` |
| `:text`, `:of-type`, `:code-text`, `:text-advanced` on token: `code:text=glucose` | backend | 400 | `UnsupportedModifier` |
| `:identifier` or `:[type]` on token | REST | 400 | `InvalidParameter` |
| `:in`, `:above`, `:below` on token, no `HFS_TERMINOLOGY_SERVER` | REST | 501 | `SearchError::TerminologyRequired` → `NotImplemented` |
| the same with a terminology server set | REST expands to a plain comma OR (`crates/rest/src/handlers/search.rs:428-429`) | 200 | served as `system\|code` OR; the design now says the same (C8, decided) |
| the same when the expansion fails: the terminology server is unreachable, or answers an error (404 for an unknown ValueSet) | REST drops the parameter (`crates/rest/src/handlers/search.rs:1302-1310` for `:in`, `1401-1407` for `:above` and `:below`) | 200 | **Wider than asked, with no warning.** The search runs without that filter and returns every match of the rest of the query, with no OperationOutcome, even under `Prefer: handling=strict`: `expand_terminology_params` never sees the handling preference. The spec does not describe this: [#modifiers](https://hl7.org/fhir/R4/search.html#modifiers) says a server "SHALL reject" a modifier it does not support, and [#errors](https://hl7.org/fhir/R4/search.html#errors) says servers "SHOULD honor" `handling=strict`. The backend cannot refuse it, since the parameter is gone before it is called. Upstream closed the report as not planned ([HeliosSoftware/hfs#2019](https://github.com/HeliosSoftware/hfs/issues/2019), 2026-10-16), so it is documented here and not refiled. Tenant and compartment filters still apply. The Inferno kit leaves it out of the supported-search group (C8) |
| `:not-in` | REST | 501 | `NotImplemented`, always |
| `_sort=birthdate` | backend | 400 | `QueryParseError` ([#sort](https://hl7.org/fhir/R4/search.html#sort)). Only a `_sort` equal to the search's native order is accepted: none, or `-_lastUpdated` where the order is `(last_updated DESC, id DESC)`. On a date-only search sliced on a date table, `-_lastUpdated` is refused too (C9, decided) |
| `_offset=20` | backend | 400 | `QueryParseError` |
| `_include=Observation:patient`, `_revinclude=Provenance:target` | backend (`query.includes` non-empty) | 501 | `IncludeNotSupported` ([#include](https://hl7.org/fhir/R4/search.html#include), [#revinclude](https://hl7.org/fhir/R4/search.html#revinclude)) |
| `_text=fever`, `_content=fever` | backend | 501 | `TextSearchNotAvailable` ([#text](https://hl7.org/fhir/R4/search.html#text), [#content](https://hl7.org/fhir/R4/search.html#content)). The backend's `search()` and `search_count()` check both by name before the type check, so the registry's typing does not turn them into 400. REST passes both through: they are in `GLOBAL_SEARCH_PARAMS` (`crates/rest/src/extractors/search_query_builder.rs:297-298`), and SQLite serves them (C10, decided) |
| `_filter=…` | backend | 501 | `BackendError::UnsupportedCapability`, which REST maps to 501, issue `not-supported` (`crates/rest/src/error.rs:1209-1211`, `656-659`). The backend's `search()` and `search_count()` check it by name with `_text` and `_content`, before the type check. REST is unchanged: it passes `_filter` through (`search_query_builder.rs:184`, `299`), and SQLite (`backends/sqlite/search/query_builder.rs:1237-1240`) and MongoDB serve it (C11, decided) |
| `_query=…` | REST | 400 | `InvalidParameter` |
| Chain: `Observation?subject.name=smith`, `subject:Patient.gender=male` | REST chain resolver, then backend | 400, or 200 empty | Not 501 `ChainedSearchNotSupported` ([#chaining](https://hl7.org/fhir/R4/search.html#chaining)). The resolver runs the terminal search first. `subject.name=smith` fails there: `Patient?name=smith` is a string search, 400 `UnsupportedParameterType`. `subject:Patient.gender=male` passes it, since `Patient?gender=male` is a token search; if that matches nothing, the resolver stops and the answer is 200 with an empty searchset (`crates/persistence/src/search/chain_resolver.rs:583-585`). If it matches, the hop back searches `Observation?subject=Patient/…`, a reference parameter, and fails with 400 `UnsupportedParameterType` (`chain_resolver.rs:603`). The terminal search is not confined to the token's compartment or scopes, so under a scope the 400 or 200 tells whether any Patient in the tenant matches (the scoped-search row below) |
| `_has` with a served terminal: `Patient?_has:Observation:patient:code=8867-4` | REST chain resolver, through the backend's ordinary `search()` | 200 | Served, not refused, without a scope; under a scope, see the scoped-search row below (#2501). The resolver searches `Observation?code=8867-4` page by page through our `search()` (`crates/persistence/src/search/chain_resolver.rs:693-735`, `268-310`), extracts each match's `patient` references in Rust (`758-770`), and REST passes the ids on as `_id` (`crates/rest/src/handlers/search.rs:642-680`). The backend sees only a token search and an `_id` search, so it cannot refuse this, and need not: R4 says "Servers are not required to implement any of the standard search parameters" and has no `_has`-specific rule ([R4 search.html](https://hl7.org/fhir/R4/search.html), [#has](https://hl7.org/fhir/R4/search.html#has)), and [Discussion #28](https://github.com/HeliosSoftware/hfs/discussions/28) puts chaining in an extension trait above our base `SearchProvider`. The resolver drains every page of the terminal search into memory (`search_all_pages`, `chain_resolver.rs:268`, called at `758`), so a broad terminal is costly; it is a Step 8 benchmark case (C7, decided) |
| `_has` whose source reference is versioned or absolute: an Observation with `subject` = `Patient/a/_history/1` or `http://<base>/Patient/a`, then `Patient?_has:Observation:subject:code=8867-4` | REST chain resolver (shared code) | 200, **without Patient `a`** | **Known upstream defect, on every backend.** The resolver turns each reference into an id with `reference.split_once('/')` (`crates/persistence/src/search/chain_resolver.rs:739-745`). `Patient/a/_history/1` becomes `_id=a/_history/1`, which matches no id, and an absolute reference splits into the type `http:` and is dropped. The spec expects both to match: "A relative reference resolving to the same value as a specified absolute URL, or vice versa, qualifies as a match" ([#reference](https://hl7.org/fhir/R4/search.html#reference)), and search acts on the current version of a versioned reference ([#versions](https://hl7.org/fhir/R4/search.html#versions)). Filed as [HeliosSoftware/hfs#2499](https://github.com/HeliosSoftware/hfs/issues/2499). A Step 6 test row expects the corrected result and is ignored until upstream fixes it (roadmap section 5) |
| `_has` or a chain under a `patient/` scope, or under a scope that does not grant a hop's type: `Patient?_has:Observation:focus:code=X` with p1's `patient/*.rs` token, where only p2's Observation, with `focus` = `Patient/p1`, has code X | REST chain resolver (shared code) | 200 **with p1** | **Known upstream security defect, on every backend** ([HeliosSoftware/hfs#2501](https://github.com/HeliosSoftware/hfs/issues/2501)). REST confines only the base query: `PatientCompartment::confine_query` (`crates/rest/src/patient_compartment.rs:168-202`, called at `handlers/search.rs:562`) adds `_id=p1` to a Patient search and `query.compartment` to any other. The resolver builds each hop with a fresh `SearchQuery` and no compartment (`chain_resolver.rs:570`, `603`, `684`, `728`), and the middleware checks only the path's resource type against the scopes (`crates/rest/src/middleware/auth.rs:245-246`). So a hop selects over the whole tenant. `focus` is not an Observation compartment parameter (R4 Patient CompartmentDefinition: `subject`, `performer`), so p1's token cannot see that Observation directly, but the `_has` reveals it, one code per query. Shown on SQLite with a temporary test (ADR-aacruzgon-0027). A token with only `user/Patient.rs` can likewise run `_has:Observation:…` with no Observation scope. On Cassandra a forward chain under a scope also answers 400 or 200 depending on whether any resource in the tenant matches its terminal (the chain row above). [R4 security.html](https://hl7.org/fhir/R4/security.html): "A security implementation must consider whether a client has the permission to access the resource being searched on, as well as the chained resource(s)". **This is a gate** (C7): Cassandra search is not signed off while its four Step 6 rows fail (roadmap section 5). If upstream has not fixed it by then, the fork carries a small REST patch that refuses these searches with 403 |
| `_has` with an unserved terminal: `Patient?_has:Observation:patient:value-quantity=gt5` | REST chain resolver, then backend | 400 | `UnsupportedParameterType` from the terminal hop's search, mapped through `RestError::from` (`handlers/search.rs:678`) to `BadRequest`, issue `invalid` (`crates/rest/src/error.rs:1109-1115`). Pinned by a Step 6 test row (C7, decided) |
| Transaction Bundle, `POST /` with `type: transaction` | REST batch handler (`supports_atomic_transactions()` is false) | 501 | `TransactionError::AtomicityUnsupported`, issue `not-supported`, before any entry is applied ([http.html#transaction](https://hl7.org/fhir/R4/http.html#transaction)). A batch Bundle works. |
| Invalid date: `birthdate=1970-13` | REST, then backend | 400 | `InvalidParameter` at REST; `InvalidDateValue` if the backend is called directly |
| Empty OR value: `gender=male,` | REST, then backend | 400 | `InvalidParameter`; `validate_value_presence` in the backend |
| Unknown parameter: `Patient?foo=bar` | REST | 200, ignored; 400 with `Prefer: handling=strict` | ([#errors](https://hl7.org/fhir/R4/search.html#errors)) |
| A write whose indexed `code`, `system` or `uri` value is longer than 2,048 bytes in UTF-8, e.g. an `Identifier.value` of 3,000 characters | backend, step 1, before the intent row | 400 | `StorageError::Validation(ValidationError::InvalidResource)`, one detail per element, OperationOutcome issue `invalid` (`crates/rest/src/error.rs:1048`, `570-571`); nothing is written (walkthrough 3.1, G35). The same check refuses a row whose `param` is longer than 128 bytes, a backstop for G38 |
| Storing a SearchParameter whose `code` is longer than 128 bytes in UTF-8 (`create`, `update`, `update_with_match`, `create_or_update`, and batch entries) | backend, step 1, before the intent row | 400 | `StorageError::Validation(ValidationError::InvalidResource)`, one detail on `SearchParameter.code` with its byte length, issue `invalid` (`crates/rest/src/error.rs:1048`, `570-571`); nothing is written. FHIR sets no limit on a [`code`](https://hl7.org/fhir/R4/datatypes.html#code), so this is a stated server restriction: the code becomes the `param` component of every index key, and a 61,328-byte one overflows Cassandra's 65,535-byte key limit (walkthrough G38) |

## 7. Open points for the session

### Specific to this table

| ID | Point | Status |
| --- | --- | --- |
| C1 | **URI and Resource-level scope.** R4 Patient and Observation have no uri parameter of their own. The extractor already indexes `_profile`, `_source`, `_tag` and `_security`. Scope them in (section 4 then stands), or say there are no URI rows. In scope: section 4 stands, and the token tables serve `_tag` and `_security`. | **decided** (ADR-aacruzgon-0027) |
| C2 | **`_id` forms.** Only `code` is covered; what `_id=sys\|abc` or `_id=\|abc` should do is not. As SQLite and PostgreSQL: the value is compared whole with the id, so both match nothing (200, empty; section 2). | **decided** (ADR-aacruzgon-0027) |
| C3 | **`system\|` on code elements** never matches, as in PostgreSQL and MongoDB. Confirm. The write-side consequence (rows nobody reads) is G18. | **decided** (design, "Shared helpers": `sys\|` matches `sys` only, as PostgreSQL, SQLite and MongoDB do) |
| C4 | **Pipe form on ContactPoint and boolean.** Accept, or 400? The converter stores a ContactPoint's own `system` and a boolean with no system. Accepted, and matched by the general token rules, as SQLite and PostgreSQL do (section 2). | **decided** (ADR-aacruzgon-0027) |
| C5 | **AND driver choice.** No selectivity source exists. A fixed order with no statistics, so the most specific key drives: `_id`; `system\|code` and `\|code` tokens; identity URIs (a canonical resource's own `url`, walkthrough 5.8); code-only tokens, `_profile`, `_source` and other URIs (shared values); `system\|` tokens; `_lastUpdated` only when nothing else can drive. Ties by query order; revisited only if the Step 8 benchmarks show it matters. The spec leaves the order to the server, and [Discussion #28](https://github.com/HeliosSoftware/hfs/discussions/28) asks for a lightweight planner without the cost hooks it sketches, which do not exist in the code. | **decided** (ADR-aacruzgon-0027) |
| C6 | **`MaxPageSize`.** Pick the value; the REST maximum is 1000. REST's `HFS_MAX_PAGE_SIZE` (default 1000, `crates/rest/src/config.rs:1331-1332`) clamps `_count` before the backend is called; no separate backend limit. The backend declares `MaxPageSize(1000)` as every other backend does (`backends/sqlite/backend.rs:936`). | **decided** (ADR-aacruzgon-0027) |
| C7 | **Composite part-count check and `_has` failing hop.** Confirm the exact 400 by test. The part count: 400 `InvalidComposite`, issue `invalid`, from REST's shared gate. A `_has` fails at its terminal hop only when the terminal's type is not served: 400 `UnsupportedParameterType`, issue `invalid`. A `_has` whose terminal is served is answered (200; section 6). Both 400s are pinned by Step 6 test rows. A versioned or absolute source reference is missed by the shared resolver, a known upstream defect ([#2499](https://github.com/HeliosSoftware/hfs/issues/2499)); an ignored Step 6 row expects the fix. **Under a scope, `_has` and chains are not confined** in their intermediate searches, a known upstream security defect ([HeliosSoftware/hfs#2501](https://github.com/HeliosSoftware/hfs/issues/2501); section 6). Four Step 6 rows are a **gate**, not ignored tests: under p1's `patient/` token, `Patient?_has:Observation:focus:code=X` gives 403 or a searchset without p1, never p1; with no scope the same request returns p1; `Observation?subject:Patient.identifier=…` under p1's token gives the same answer whether or not the identifier exists, never 400 for one and 200 for the other; and a token with only `user/Patient.rs` gets 403 for `Patient?_has:Observation:subject:code=X`. Cassandra search is not signed off while they fail. If upstream has not fixed it by then, the fork carries a small REST patch: right after `confine_query` (`handlers/search.rs:562`), refuse a query with chains or `_has` under a confined grant with `RestError::Forbidden` (403, issue `forbidden`, `crates/rest/src/error.rs:612-614`), and check each `_has` source type and chain target type with `SmartScopePolicy::check(…, FhirOperation::Search)` for every scope, refusing a denied type the same way. | **decided** (ADR-aacruzgon-0027) |
| C8 | **Terminology modifiers.** The design says 400. The code gives a REST 501 for token `:in`/`:above`/`:below` without a terminology server, serves them as a plain OR with one, and always gives 501 for `:not-in`. The design now matches the code: `crates/rest/src/handlers/search.rs:411-426` (`:not-in`), `428-466` (expansion, or `TerminologyRequired` without a server). A failed expansion drops the filter and widens the result with no warning, even under `Prefer: handling=strict` (`1302-1310`, `1401-1407`; upstream [#2019](https://github.com/HeliosSoftware/hfs/issues/2019), closed as not planned; section 6). | **decided** (ADR-aacruzgon-0027) |
| C9 | **`_sort` default.** Which values count as the default order is undefined. Accepted only when equal to the native order: absent, or `-_lastUpdated`. Anything else is the design's existing rule: 400 `QueryParseError`. | **decided** (ADR-aacruzgon-0027) |
| C10 | **`_text`/`_content`.** Typed string in R4, so the type check answers 400 before the intended 501 unless they are checked by name first. Checked by name in the backend's `search()` and `search_count()`, before the type check: 501. | **decided** (ADR-aacruzgon-0027) |
| C11 | **`_filter`.** The design says "rejected" but names no layer, status or error. Refused by name in the Cassandra backend with `BackendError::UnsupportedCapability`: 501, issue `not-supported`; REST unchanged (section 6). | **decided** (ADR-aacruzgon-0027) |

### Already in the walk-through's gap list

| Topic | Gap | Status |
| --- | --- | --- |
| `_lastUpdated` beside the cursor | G19 | **resolved** on the node: lower bound plus cursor in one query; for an upper bound, send the tighter of bound and cursor |
| `_lastUpdated` combined with another parameter | G19 | **resolved**: a residual filter, as the design says; a token-table slice is an optional optimisation |
| `_id` and `_lastUpdated` index rows from the extractor | G17 | **resolved** (P5, ADR-aacruzgon-0027): dropped |
| Display-only tokens (`CodeableConcept.text`, empty code) and other rows no query reads | G18 | **resolved** (P5, ADR-aacruzgon-0027): display-only tokens, the `IMPLICIT` and `''` rows in `token_system_index`, and empty URIs are dropped |
| "Date-only" with two date parameters, a repeated date, or a date with `_lastUpdated` | G20 | **resolved** (ADR-aacruzgon-0027), `ne` and cross-table comma lists included (section 3) |
| `count(tenant, None)`; filtered `search_count` and the candidate cap | G7 | **resolved** (ADR-aacruzgon-0027): tag checks and the cap for a filtered `search_count`; `count(tenant, None)` over every enabled version's type names |
| The candidate cap's error and status (`too-costly`) | G27 | **resolved** (ADR-aacruzgon-0027): 422 `TooManyResults`, issue `processing`; no `too-costly`; the cap counts distinct candidates point-read; the compartment cap follows it |
| Compartment filter (`query.compartment`) | G33 | **resolved** (ADR-aacruzgon-0026): residual on each loaded row, under the cap |
| `query.fhir_version` on searches and counts | G34 | **resolved** (ADR-aacruzgon-0026): residual, and `resources_by_type.fhir_version` for counts |
| Index values over 2,048 bytes | G35 | **resolved** (ADR-aacruzgon-0026): the write is refused with 400 `InvalidResource` before step 2 (section 6) |
| A revived resource read as deleted after a missed revive | G39 | **resolved** (ADR-aacruzgon-0027): `deleted_at = 0` means live; `resources` writes no tombstone |
| A paused writer, then a crashed successor, left orphans | G40 | **resolved** (ADR-aacruzgon-0027): the rewrite of row N runs before the CAS |
| A custom SearchParameter's `code` over 128 bytes | G38 | **resolved** (ADR-aacruzgon-0027): refused with 400 `InvalidResource` when stored; `param` measured in the step 1 check (section 6) |
| Two `_id` parameters | G37 | **resolved** (ADR-aacruzgon-0026): intersected before any read |
