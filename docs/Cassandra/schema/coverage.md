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

**True of every row that returns results.** Each candidate's `resources` row is loaded. It is dropped if its `version_id` differs from the index row's tag or if `deleted_at` is set. It is also dropped if the query names a FHIR version (`query.fhir_version`) that differs from `resources.fhir_version`, or carries a compartment filter (`query.compartment`) the resource is not a member of (section 5). Results come in `(last_updated DESC, id DESC)` order, deduplicated on `(last_updated, id)`. `_count` is the page size, and `next` is minted only when a `_count + 1`th match exists.

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
| both | `_id` | `Resource.id` (id) | The extractor returns it as a token today; dropping those rows before the write is G17. Served by point reads on `resources`, never from an index table | `code` only (C2) |

That is 9 Patient, 13 Observation and 1 shared token parameter (plus `_tag` and `_security` if C1 brings them in).

### Date ([R4 search.html#date](https://hl7.org/fhir/R4/search.html#date))

| Resource | Parameter | Element (type) |
| --- | --- | --- |
| Patient | `birthdate` | `Patient.birthDate` (date: a day, month or year range) |
| Patient | `death-date` | `Patient.deceased as dateTime` |
| Observation | `date` | `Observation.effective` (dateTime, Period, Timing or instant) |
| Observation | `value-date` | `Observation.value as dateTime` or `as Period` |
| both | `_lastUpdated` | `Resource.meta.lastUpdated` (instant). The extractor returns it as a date today; dropping those rows is G17. Served from `resources_by_type`, never from the date tables |

### URI ([R4 search.html#uri](https://hl7.org/fhir/R4/search.html#uri))

**Neither Patient nor Observation has a uri parameter in R4.** The only uri parameters that apply to them are the Resource-level `_profile` (`Resource.meta.profile`, canonical) and `_source` (`Resource.meta.source`). Section 4 uses them. The extractor already indexes `_profile`, `_source` and the Resource-level tokens `_tag` and `_security` (`search/extractor.rs`; its tests assert it). Whether they are in scope is C1.

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
| pipe on ContactPoint or boolean | `Patient?phone=phone\|555-0100`, `Patient?active=\|true` | as the form says | cursor | — | The spec says the pipe form SHALL NOT be used here. The shared converter makes both match today. Accept, or reject with 400? (C4) |
| `_id` | `Patient?_id=abc` | `resources`, point read `(id)` | — | `deleted_at` unset | 200; zero or one entry; case-sensitive |
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
| `ge` | `te > e`, or `ts ≥ s` and `te ≤ e` | `date_index_by_end` ∪ `date_index` | `end > e`; `start ≥ s AND start < e` | `te ≤ e` on the second; dedupe |
| `le` | `ts < s`, or `ts ≥ s` and `te ≤ e` | `date_index` | `start < e` | full predicate |
| `sa` | `ts ≥ e` | `date_index` | `start ≥ e` | — |
| `eb` | `te ≤ s` | `date_index_by_end` | `end ≤ s` | — |
| `ap` | `ts < high` and `te > low`; the window widens `[s, e)` by 12 months (year), 1 month (month), 1 day (day), 10 min (minute) or 10 s (finer) | `date_index` | `start < high` | `te > low` |

`ge` is the design's union of an `end` slice and the `eq` slice; walkthrough G20 records a single-slice option. Every row is also checked with `RangePredicate::matches`, whatever the slice. **Expected for all nine:** 200 searchset. The page is sorted in Rust by `(last_updated DESC, id DESC)`, using the `resources` rows already loaded for the tag check, and cut at the cursor. If the candidates exceed the configured cap the search is refused; which error and status is G27 (today's closest, `SearchError::TooManyResults`, is 422, not `too-costly`). Comma OR (`birthdate=1970,1980`) is a union of slices, deduplicated. A Period with no end is stored with the `open_end` sentinel, so it falls in every `end >` slice.

**Date with any other parameter** (`Patient?gender=female&birthdate=ge1970`): the date tables are not read. The other parameter drives. The date is a residual filter: re-extract the date values from the loaded `resources` row and apply `RangePredicate::matches`. Expected: 200, keyset-paged. Two date parameters, a repeated date, or a date with `_lastUpdated` is G20.

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
| AND of parameters | `Observation?code=8867-4&status=final` | One token partition drives from the cursor. Each candidate is checked against the other with a clustering point read (`… AND last_updated = ? AND id = ?`). How the driver is chosen with no statistics is C5 | 200; intersection; keyset-paged |
| repeated parameter | `Observation?code=8867-4&code=8310-5` | as AND | 200; resources having both |
| comma OR | `Observation?code=8867-4,8310-5` | union of partitions, one query each, merged in the shared order, deduped | 200; union |
| OR across forms | `code=http://loinc.org\|8867-4,8310-5` | three partitions in two tables, merged | 200; union |
| token + URI | `Patient?gender=female&_profile=…` | either drives; the other is a point read | 200 |
| `_id` + other | `Patient?_id=a,b&gender=female` | `_id` drives; others checked | 200 |
| token + date, or token + `_lastUpdated` | see section 3 | the date is a residual filter | 200 |
| two date parameters, or a repeated date (`date=ge2020&date=lt2021`) | — | is this "date-only"? (G20) | 200 |
| unfiltered type search | `GET /Patient` | `resources_by_type`, one partition, cursor slice | 200; all live Patients |
| `_count=n` | `Patient?gender=female&_count=50` | `LIMIT` is a batch size; read until 51 distinct matches | 200; at most 50 entries. REST defaults are 20 and max 1000 (`crates/rest/src/config.rs`); the value Cassandra declares as `MaxPageSize` is C6 |
| `_count=0` | `Patient?gender=female&_count=0` | `search_count`: stream every match | 200 searchset with `total` and no entries (behaves as `_summary=count`). Tag checks and the cap here: G7 |
| `_count=0`, unfiltered | `GET /Patient?_count=0` | full scan of the `resources_by_type` partition, reading its tombstones | 200; `total` only |
| next page | the `next` link | `PageCursor` with `sort_values = [last_updated RFC 3339 ms, reserved bucket slot]`, `resource_id = id`; slice `(last_updated, id) < (?, ?)`. The page key is `(last_updated, id, bucket)`; `id` travels in `resource_id`, so the design's "third slot" is the second element of `sort_values` | 200; no duplicates or gaps; no `previous` on page 1; no `next` after the last page |
| malformed cursor | `_cursor=garbage` | backend | 400 `InvalidCursor` |
| `count(tenant, Some(type))` | internal | full scan of `resources_by_type`, no tag check | the number of live rows; in-flight and orphan miscounts are G7 |
| `count(tenant, None)` | internal (all types) | no table serves this (G7) | the number of live rows |
| `_total=accurate` or `_total=estimate` | `Patient?gender=female&_total=accurate` | the page as usual, plus one `search_count` of the same query (`SearchQuery::wants_total()`); costs and caveats as `_count=0` | 200; `Bundle.total` set. `_total=none` or absent: no `total` |
| FHIR version named | `GET /Patient?gender=female` with `Accept: application/fhir+json; fhirVersion=4.0` (`query.fhir_version`, upstream #2254) | as the search; residual `resources.fhir_version = '4.0'` after the tag check | 200; only resources stored under that version (walkthrough 5.9, G34) |
| FHIR version named, unfiltered count | `GET /Patient?_count=0` with `fhirVersion=4.0` | `resources_by_type`, whole partition, counting rows whose `fhir_version` column is `'4.0'`, in Rust; no point reads | 200; `total` of that version only |
| compartment search | `GET /Patient/123/Observation?code=8867-4` | the token partition drives; residual: re-extract `subject` and `performer` (R4 Patient compartment for Observation) and keep members of `Patient/123` (walkthrough 5.11, G33) | 200; members only, keyset-paged |
| `patient/` scope | `GET /Observation?status=final` with a token scoped `patient/Observation.rs` and launch patient 123 | as the compartment search: REST sets `query.compartment` | 200; the patient's Observations only. Never unfiltered |
| compartment, no other parameter | `GET /Patient/123/Observation` | `resources_by_type` drives; residual as above | 200, or the cap row below |
| compartment count | `GET /Patient/123/Observation?_count=0`, or `_total=accurate` | the residual stream run to the end, under the cap | 200 with the members' `total`; over the cap, the cap row below. Never a partial count |
| compartment over the cap | a compartment search or count that reads more candidates than the cap before it finishes | — | 422 `SearchError::TooManyResults` (issue `processing`, `crates/rest/src/error.rs:1123-1125`); never a page or count that was only partly filtered |
| candidate cap exceeded | e.g. `Observation?date=lt2030` on a large tenant | date tables or a filtered `search_count` | An OperationOutcome. The design asks for issue type `too-costly`; no search error produces it today (G27) |

## 6. Rejections

"Layer" is where the refusal is produced. The backend's `search()` and `search_count()` run, in this order: `validate_date_values`, `validate_value_presence`, `reject_unsupported_metadata_modifier`, then the type check, the modifier check, includes, text, and `_sort`/`_offset` (design, "Capabilities and rejection"). Every refusal carries an OperationOutcome.

| Request | Layer | Status | Error |
| --- | --- | --- | --- |
| String: `Patient?name=smith`, `family=`, `address-city=` | backend | 400 | `UnsupportedParameterType` ([#string](https://hl7.org/fhir/R4/search.html#string)) |
| Reference: `Observation?subject=Patient/1`, `patient=`, `Patient?general-practitioner=` | backend | 400 | `UnsupportedParameterType` ([#reference](https://hl7.org/fhir/R4/search.html#reference)) |
| Quantity: `Observation?value-quantity=gt5.4\|http://unitsofmeasure.org\|mg` | backend | 400 | `UnsupportedParameterType` ([#quantity](https://hl7.org/fhir/R4/search.html#quantity)) |
| Composite: `Observation?code-value-quantity=8480-6$gt100` | backend | 400 | `UnsupportedParameterType` ([#composite](https://hl7.org/fhir/R4/search.html#composite)). REST checks the `$` part count first, so a malformed value gets a REST 400 instead (C7) |
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
| the same with a terminology server set | REST expands to a plain comma OR | 200 | served as `system\|code` OR. The design says 400 (C8) |
| `:not-in` | REST | 501 | `NotImplemented`, always |
| `_sort=birthdate` | backend | 400 | `QueryParseError` ([#sort](https://hl7.org/fhir/R4/search.html#sort)). Is `_sort=-_lastUpdated`, the default order, accepted? (C9) |
| `_offset=20` | backend | 400 | `QueryParseError` |
| `_include=Observation:patient`, `_revinclude=Provenance:target` | backend (`query.includes` non-empty) | 501 | `IncludeNotSupported` ([#include](https://hl7.org/fhir/R4/search.html#include), [#revinclude](https://hl7.org/fhir/R4/search.html#revinclude)) |
| `_text=fever`, `_content=fever` | backend | 501 | `TextSearchNotAvailable` ([#text](https://hl7.org/fhir/R4/search.html#text), [#content](https://hl7.org/fhir/R4/search.html#content)). The R4 registry types both as string, so the type check may answer 400 first (C10) |
| `_filter=…` | ? | ? | The design scope table says "rejected" but not how (C11) |
| `_query=…` | REST | 400 | `InvalidParameter` |
| Chain: `Observation?subject.name=smith`, `subject:Patient.gender=male` | REST chain resolver, then backend | 400 | `UnsupportedParameterType` from the hop that needs a reference parameter, not 501 `ChainedSearchNotSupported` ([#chaining](https://hl7.org/fhir/R4/search.html#chaining)) |
| `_has`: `Patient?_has:Observation:patient:code=8867-4` | REST chain resolver, then backend | 400 | `UnsupportedParameterType`, as for chains ([#has](https://hl7.org/fhir/R4/search.html#has)); which hop fails is C7 |
| Transaction Bundle, `POST /` with `type: transaction` | REST batch handler (`supports_atomic_transactions()` is false) | 501 | `TransactionError::AtomicityUnsupported`, issue `not-supported`, before any entry is applied ([http.html#transaction](https://hl7.org/fhir/R4/http.html#transaction)). A batch Bundle works. |
| Invalid date: `birthdate=1970-13` | REST, then backend | 400 | `InvalidParameter` at REST; `InvalidDateValue` if the backend is called directly |
| Empty OR value: `gender=male,` | REST, then backend | 400 | `InvalidParameter`; `validate_value_presence` in the backend |
| Unknown parameter: `Patient?foo=bar` | REST | 200, ignored; 400 with `Prefer: handling=strict` | ([#errors](https://hl7.org/fhir/R4/search.html#errors)) |
| A write whose indexed `code`, `system` or `uri` value is longer than 2,048 bytes in UTF-8, e.g. an `Identifier.value` of 3,000 characters | backend, step 1, before the intent row | 400 | `StorageError::Validation(ValidationError::InvalidResource)`, one detail per element, OperationOutcome issue `invalid` (`crates/rest/src/error.rs:1048`, `570-571`); nothing is written (walkthrough 3.1, G35) |

## 7. Open points for the session

### Specific to this table

| ID | Point | Status |
| --- | --- | --- |
| C1 | **URI and Resource-level scope.** R4 Patient and Observation have no uri parameter of their own. The extractor already indexes `_profile`, `_source`, `_tag` and `_security`. Scope them in (section 4 then stands), or say there are no URI rows. | open |
| C2 | **`_id` forms.** Only `code` is covered; what `_id=sys\|abc` or `_id=\|abc` should do is not. | open |
| C3 | **`system\|` on code elements** never matches, as in PostgreSQL and MongoDB. Confirm. The write-side consequence (rows nobody reads) is G18. | open |
| C4 | **Pipe form on ContactPoint and boolean.** Accept, or 400? The converter stores a ContactPoint's own `system` and a boolean with no system. | open |
| C5 | **AND driver choice.** No selectivity source exists. | open |
| C6 | **`MaxPageSize`.** Pick the value; the REST maximum is 1000. | open |
| C7 | **Composite part-count check and `_has` failing hop.** Confirm the exact 400 by test. | open |
| C8 | **Terminology modifiers.** The design says 400. The code gives a REST 501 for token `:in`/`:above`/`:below` without a terminology server, serves them as a plain OR with one, and always gives 501 for `:not-in`. | open |
| C9 | **`_sort` default.** Which values count as the default order is undefined. | open |
| C10 | **`_text`/`_content`.** Typed string in R4, so the type check answers 400 before the intended 501 unless they are checked by name first. | open |
| C11 | **`_filter`.** The design says "rejected" but names no layer, status or error. | open |

### Already in the walk-through's gap list

| Topic | Gap | Status |
| --- | --- | --- |
| `_lastUpdated` beside the cursor | G19 | **resolved** on the node: lower bound plus cursor in one query; for an upper bound, send the tighter of bound and cursor |
| `_lastUpdated` combined with another parameter | G19 | **resolved**: a residual filter, as the design says; a token-table slice is an optional optimisation |
| `_id` and `_lastUpdated` index rows from the extractor | G17 | open |
| Display-only tokens (`CodeableConcept.text`, empty code) and other rows no query reads | G18 | open |
| "Date-only" with two date parameters, a repeated date, or a date with `_lastUpdated` | G20 | open |
| `count(tenant, None)`; filtered `search_count` and the candidate cap | G7 | open |
| The candidate cap's error and status (`too-costly`) | G27 | open; code fact: `TooManyResults` maps to 422, and no search path produces `too-costly`. The compartment cap uses `TooManyResults` today (G33) and follows G27 if it changes |
| Compartment filter (`query.compartment`) | G33 | **resolved** (ADR-aacruzgon-0026): residual on each loaded row, under the cap |
| `query.fhir_version` on searches and counts | G34 | **resolved** (ADR-aacruzgon-0026): residual, and `resources_by_type.fhir_version` for counts |
| Index values over 2,048 bytes | G35 | **resolved** (ADR-aacruzgon-0026): the write is refused with 400 `InvalidResource` before step 2 (section 6) |
| Two `_id` parameters | G37 | **resolved** (ADR-aacruzgon-0026): intersected before any read |
