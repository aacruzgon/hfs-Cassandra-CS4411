#!/usr/bin/env python3
"""The demonstration script: one HTTP session against a running HFS server.

Roadmap row 0.9 (passing on PostgreSQL) and row 8.6 (the same script on
Cassandra and on PostgreSQL in CI). See README.md next to this file.

    python3 docs/Cassandra/demo/demo.py --backend postgres \
        --base-url http://127.0.0.1:58080 --wait 120 --report demo.jsonl

It covers create, read, vread, an update with an If-Match conflict, a batch
bundle, a transaction bundle, token, date and URI searches with cursor paging
through `next` links, a rejected :exact, a delete then 410, and isolation
between two tenants.

Every step checks one status and names where that status is documented. Where
the two backends are documented to answer differently, the step states both
answers (see `Differs`), and the run checks the one for --backend. Nothing is
ever accepted as "either".

Every run uses new tenant names and new resource values, so it depends on
nothing an earlier run left, and two runs never clash. The script talks only
to the URL it is given. It needs Python 3.9 or later and nothing else.

Exit status: 0 when every step passed, 1 when any step failed, 2 when it could
not start (bad arguments, or no server answered within --wait).
"""

import argparse
import datetime
import http.client
import json
import secrets
import sys
import time
import urllib.parse
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Union

FHIR_JSON = "application/fhir+json"
BACKENDS = ("postgres", "cassandra")

# A next link is followed at most this many times, so a server that keeps
# answering with `next` fails the step instead of hanging the run.
MAX_PAGES = 20

# Where each expected status is documented. Paths are relative to the
# repository root; each `#anchor` is on https://hl7.org/fhir/R4/.
HTTP = "R4 http.html"
SEARCH = "R4 search.html"
DISCUSSION = "docs/Cassandra/cassandra-discussion.md"
WALKTHROUGH = "docs/Cassandra/schema/walkthrough.md"
COVERAGE = "docs/Cassandra/schema/coverage.md"
SKILL = ".claude/skills/run-hfs-server/SKILL.md"
ROADMAP = "docs/Cassandra/development-roadmap.md"

DOC_METADATA = f"{HTTP}#capabilities"
DOC_CREATE = f"{HTTP}#create (201, Location with id and version)"
DOC_READ = f"{HTTP}#read"
DOC_VREAD = f"{HTTP}#vread"
DOC_UPDATE = f"{HTTP}#update (200 updated, 201 created)"
DOC_CONFLICT = f"{HTTP}#concurrency (412 and no update); {DISCUSSION}, Optimistic locking"
DOC_BATCH = f"{HTTP}#transaction (200, a status per entry); {DISCUSSION}, Batch"
DOC_SEARCH = f"{SEARCH}; {ROADMAP}, Step 8 Spec"
DOC_PAGING = f"{HTTP}#paging; {WALKTHROUGH} 5.10"
DOC_NEXT_LINK = (
    f"{SEARCH}, 'repeat the original _count parameter in its returned page links'; "
    f"{COVERAGE} section 5, malformed cursor row (the parameter is _cursor)"
)
DOC_UNKNOWN_PARAM = f"{SEARCH}#errors; {COVERAGE} section 6, 'Unknown parameter'"
DOC_COMBINING = f"{SEARCH}#combining"
DOC_URI = f"{SEARCH}; {WALKTHROUGH} 5.3; {COVERAGE} section 4"
DOC_EXACT_TOKEN = (
    f"{SEARCH}#modifiers (reject with 400 and an OperationOutcome); "
    f"{COVERAGE} section 6, ':exact on token, date or uri'; "
    f"{DISCUSSION}, ':exact modifier'"
)
DOC_DELETE = f"{HTTP}#delete; {SKILL}, 'DELETE [type]/[id]'; {WALKTHROUGH} 3.4"
DOC_GONE = f"{HTTP}#delete (410 on later reads); {WALKTHROUGH} 4.1"
DOC_VREAD_GONE = f"{HTTP}#vread (410 on a deleted version); {WALKTHROUGH} 4.2"
DOC_SEARCH_DELETED = f"{HTTP}#delete (no longer found through search)"
DOC_ISOLATION = f"CLAUDE.md, Tenant-First Design; {HTTP}#read (404)"


@dataclass(frozen=True)
class Differs:
    """A value the backends are documented to give differently."""

    postgres: object
    cassandra: object

    def on(self, backend: str) -> object:
        return getattr(self, backend)


@dataclass
class Answer:
    """One HTTP response, read once."""

    status: int
    headers: Dict[str, str]
    body: Optional[dict]
    elapsed_ms: float
    error: Optional[str] = None

    def header(self, name: str) -> str:
        return self.headers.get(name.lower(), "-")

    def link(self, relation: str) -> Optional[str]:
        for link in (self.body or {}).get("link", []):
            if link.get("relation") == relation:
                return link.get("url")
        return None

    def entry_ids(self) -> List[str]:
        return [
            entry["resource"]["id"]
            for entry in (self.body or {}).get("entry", [])
            if entry.get("resource", {}).get("resourceType") != "OperationOutcome"
            and "id" in entry.get("resource", {})
        ]

    def entry_statuses(self) -> List[str]:
        return [
            entry.get("response", {}).get("status", "-").split(" ")[0]
            for entry in (self.body or {}).get("entry", [])
        ]


# A check returns a description of what is wrong, or None.
Check = Optional[str]


def equal(label: str, actual: object, expected: object) -> Check:
    if actual == expected:
        return None
    return f"{label}: expected {expected!r}, got {actual!r}"


def contains(label: str, actual: str, needle: str) -> Check:
    if needle in actual:
        return None
    return f"{label}: expected it to contain {needle!r}, got {actual!r}"


def same_ids(label: str, actual: List[str], expected: List[str]) -> Check:
    if sorted(actual) == sorted(expected):
        return None
    return f"{label}: expected {sorted(expected)}, got {sorted(actual)}"


def outcome(answer: Answer) -> Check:
    """Every error body is an OperationOutcome (roadmap, Step 8 Spec)."""
    return equal("body resourceType", (answer.body or {}).get("resourceType"), "OperationOutcome")


def searchset(answer: Answer) -> Check:
    """An empty result only means something if the answer really is a search result."""
    return equal("Bundle.type", (answer.body or {}).get("type"), "searchset")


def version(answer: Answer) -> object:
    return (answer.body or {}).get("meta", {}).get("versionId")


def family(answer: Answer) -> object:
    names = (answer.body or {}).get("name") or [{}]
    return names[0].get("family")


class SectionFailed(Exception):
    """A step the rest of its section needs has failed."""


def new_id(created: Answer) -> str:
    """The id the server gave a resource it just created."""
    pid = (created.body or {}).get("id")
    if not pid:
        raise SectionFailed("the create response has no id")
    return pid


class Demo:
    """Sends the requests, checks the answers and keeps the results."""

    def __init__(self, base_url: str, backend: str, timeout: float, report_path: Optional[str]):
        self.base_url = base_url.rstrip("/")
        parsed = urllib.parse.urlsplit(self.base_url)
        try:
            parsed.port  # raises ValueError for a port that is not a number
        except ValueError:
            raise ValueError(f"--base-url has a bad port, got {base_url!r}") from None
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError(f"--base-url must be an http(s) URL, got {base_url!r}")
        self.parsed = parsed
        self.backend = backend
        self.timeout = timeout
        self.run_id = (
            datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dt%H%M%S")
            + "-"
            + secrets.token_hex(3)
        )
        self.tenant_a = f"demo-{self.run_id}-a"
        self.tenant_b = f"demo-{self.run_id}-b"
        self.system = f"urn:demo:{self.run_id}"
        self.report = open(report_path, "w", encoding="utf-8") if report_path else None
        self.section_name = ""
        self.seq = 0
        self.failed = 0
        self.started = time.monotonic()

    # ---- HTTP -----------------------------------------------------------

    def send(
        self,
        method: str,
        target: str,
        tenant: Optional[str],
        body: Optional[dict],
        headers: Optional[Dict[str, str]],
    ) -> Answer:
        """One request on a new connection. `target` starts with `/`."""
        connection_type = (
            http.client.HTTPSConnection
            if self.parsed.scheme == "https"
            else http.client.HTTPConnection
        )
        connection = connection_type(self.parsed.hostname, self.parsed.port, timeout=self.timeout)
        request_headers = {"Accept": FHIR_JSON, **(headers or {})}
        if tenant:
            request_headers["X-Tenant-ID"] = tenant
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            request_headers["Content-Type"] = FHIR_JSON
        started = time.monotonic()
        try:
            connection.request(
                method, self.parsed.path + target, body=data, headers=request_headers
            )
            response = connection.getresponse()
            raw = response.read()
        except (OSError, http.client.HTTPException) as error:
            elapsed = (time.monotonic() - started) * 1000
            return Answer(0, {}, None, elapsed, error=f"{type(error).__name__}: {error}")
        finally:
            connection.close()
        elapsed = (time.monotonic() - started) * 1000
        try:
            parsed_body = json.loads(raw) if raw else None
        except ValueError:
            parsed_body = None
        if not isinstance(parsed_body, dict):
            parsed_body = None
        headers_read = {name.lower(): value for name, value in response.getheaders()}
        return Answer(response.status, headers_read, parsed_body, elapsed)

    # ---- Steps ----------------------------------------------------------

    def step(
        self,
        name: str,
        expected: Union[int, Differs],
        doc: Union[str, Differs],
        method: str,
        target: str,
        tenant: Optional[str] = None,
        body: Optional[dict] = None,
        headers: Optional[Dict[str, str]] = None,
        then: Optional[Callable[[Answer], List[Check]]] = None,
        required: bool = False,
    ) -> Answer:
        """Runs one request and records whether it answered as documented.

        `then` adds checks on the body and headers, run only when the status
        was the expected one. `required` ends the section if this step fails,
        because its later steps need what this one made.
        """
        want = expected.on(self.backend) if isinstance(expected, Differs) else expected
        where = doc.on(self.backend) if isinstance(doc, Differs) else doc
        if not target.startswith("/"):
            target = "/" + target
        answer = self.send(method, target, tenant, body, headers)
        problems = [answer.error] if answer.error else []
        if answer.status != want and not answer.error:
            problems.append(f"status: expected {want}, got {answer.status}")
            detail = diagnostics(answer)
            if detail:
                problems.append(detail)
        if answer.status == want and then:
            problems += [problem for problem in then(answer) if problem]
        self.record(
            name=name,
            method=method,
            target=urllib.parse.unquote(target),
            tenant=tenant,
            expected=want,
            actual=answer.status,
            doc=where,
            problems=problems,
            elapsed_ms=answer.elapsed_ms,
        )
        if problems and required:
            raise SectionFailed(f"{name} is needed by the rest of this section")
        return answer

    def assertion(self, name: str, doc: str, problems: List[Check]) -> None:
        """A check over several responses, recorded as a step with no request."""
        self.record(
            name=name,
            method="-",
            target="-",
            tenant=None,
            expected="-",
            actual="-",
            doc=doc,
            problems=[problem for problem in problems if problem],
            elapsed_ms=0.0,
        )

    def record(self, **fields) -> None:
        self.seq += 1
        problems = fields["problems"]
        result = "FAIL" if problems else "PASS"
        if problems:
            self.failed += 1
        tenant = fields["tenant"]
        request = f"{fields['method']} {fields['target']}"
        if tenant:
            request += f"  [X-Tenant-ID: {tenant}]"
        print(f"{self.seq:3d} {result} {self.section_name}: {fields['name']}")
        print(f"      request  {request}")
        print(
            f"      expected {fields['expected']}  actual {fields['actual']}"
            f"  ({fields['elapsed_ms']:.0f} ms)  on {self.backend}"
        )
        print(f"      doc      {fields['doc']}")
        for problem in problems:
            print(f"      problem  {problem}")
        sys.stdout.flush()
        if self.report:
            row = {
                "seq": self.seq,
                "section": self.section_name,
                "result": result,
                "backend": self.backend,
                **fields,
            }
            self.report.write(json.dumps(row) + "\n")
            self.report.flush()

    def section(self, name: str, run: Callable[[], None]) -> None:
        """Runs one section. Sections share nothing, so one failing costs only itself."""
        self.section_name = name
        print(f"\n=== {name} ===")
        try:
            run()
        except SectionFailed as stop:
            self.assertion("section stopped", "-", [f"the rest was not run: {stop}"])

    # ---- Paging ---------------------------------------------------------

    def pages(self, name: str, query: str, tenant: str, want_ids: List[str], per_page: int) -> None:
        """Follows `next` links from `query` to the last page.

        Every page must be 200 and no longer than `per_page`, and together the
        pages must hold each of `want_ids` exactly once.
        """
        target = "/" + query
        seen: List[str] = []
        for page in range(1, MAX_PAGES + 1):
            answer = self.step(
                f"{name}, page {page}",
                200,
                DOC_PAGING,
                "GET",
                target,
                tenant,
                then=lambda a: [
                    equal("Bundle.type", (a.body or {}).get("type"), "searchset"),
                    None
                    if len(a.entry_ids()) <= per_page
                    else f"page has {len(a.entry_ids())} entries, more than _count {per_page}",
                ],
            )
            if answer.status != 200:
                self.assertion(f"{name}, stopped", DOC_PAGING, [f"page {page} was not a 200"])
                return
            seen += answer.entry_ids()
            link = answer.link("next")
            if link is None:
                break
            link_query = urllib.parse.parse_qs(urllib.parse.urlsplit(link).query)
            self.assertion(
                f"{name}, next link of page {page}",
                DOC_NEXT_LINK,
                [
                    None if "_cursor" in link_query else f"next link has no _cursor: {link!r}",
                    equal("next link _count", link_query.get("_count"), [str(per_page)]),
                ],
            )
            target = self.next_target(link, name, page)
            if target is None:
                return
        else:
            self.assertion(f"{name}, ends", DOC_PAGING, [f"still a next link after {MAX_PAGES} pages"])
        self.assertion(
            f"{name}, every match exactly once",
            DOC_PAGING,
            [same_ids("ids across all pages", seen, want_ids)],
        )
        expected_pages = (len(want_ids) + per_page - 1) // per_page
        self.assertion(
            f"{name}, page count",
            DOC_PAGING,
            [equal("pages", page, expected_pages)],
        )

    def next_target(self, link: str, name: str, page: int) -> Optional[str]:
        """Turns a `next` link into a request target on the given URL, or None if it is elsewhere."""
        absolute = urllib.parse.urljoin(self.base_url + "/", link)
        parsed = urllib.parse.urlsplit(absolute)
        if (parsed.scheme, parsed.netloc) != (self.parsed.scheme, self.parsed.netloc):
            # The server builds links from HFS_BASE_URL. Following one elsewhere
            # would break the promise to talk only to the URL given.
            self.assertion(
                f"{name}, next link of page {page}",
                DOC_PAGING,
                [f"next link is {link!r}, not on {self.base_url}: start HFS with "
                 f"HFS_BASE_URL={self.base_url}"],
            )
            return None
        path = parsed.path
        if self.parsed.path and path.startswith(self.parsed.path):
            path = path[len(self.parsed.path):]
        return path + ("?" + parsed.query if parsed.query else "")

    # ---- Small builders -------------------------------------------------

    def patient(self, family_name: str, **extra) -> dict:
        return {
            "resourceType": "Patient",
            "name": [{"family": family_name, "given": ["Demo"]}],
            **extra,
        }

    def identifier(self, code: str) -> dict:
        return {"system": self.system, "value": code}


def diagnostics(answer: Answer) -> Optional[str]:
    issues = (answer.body or {}).get("issue") or []
    if issues:
        issue = issues[0]
        return f"outcome: {issue.get('code')}: {issue.get('diagnostics') or issue.get('details')}"
    return None


def q(path: str, **params: str) -> str:
    """Builds `path?a=b`, percent-encoding values; a key's trailing `__x` is a modifier."""
    parts = []
    for key, value in params.items():
        name = key.replace("__", ":")
        parts.append(f"{name}={urllib.parse.quote(str(value), safe=':/')}")
    return f"{path}?{'&'.join(parts)}"


# ---- The sections, in the order the roadmap names them ------------------


def capabilities(demo: Demo) -> None:
    demo.step(
        "the server answers",
        200,
        DOC_METADATA,
        "GET",
        "metadata",
        demo.tenant_a,
        then=lambda a: [
            equal("resourceType", (a.body or {}).get("resourceType"), "CapabilityStatement")
        ],
        required=True,
    )


def crud(demo: Demo) -> None:
    tenant = demo.tenant_a
    created = demo.step(
        "create",
        201,
        DOC_CREATE,
        "POST",
        "Patient",
        tenant,
        body=demo.patient("Original", identifier=[demo.identifier("crud")]),
        then=lambda a: [
            equal("ETag", a.header("etag"), 'W/"1"'),
            contains("Location", a.header("location"), f"Patient/{(a.body or {}).get('id')}/_history/1"),
            equal("meta.versionId", version(a), "1"),
        ],
        required=True,
    )
    pid = new_id(created)
    path = f"Patient/{pid}"

    demo.step(
        "read",
        200,
        DOC_READ,
        "GET",
        path,
        tenant,
        then=lambda a: [equal("ETag", a.header("etag"), 'W/"1"'), equal("family", family(a), "Original")],
    )
    demo.step(
        "update with the right If-Match",
        200,
        DOC_UPDATE,
        "PUT",
        path,
        tenant,
        body=demo.patient("Updated", id=pid),
        headers={"If-Match": 'W/"1"'},
        then=lambda a: [equal("ETag", a.header("etag"), 'W/"2"')],
        required=True,
    )
    demo.step(
        "vread version 1 keeps the old content",
        200,
        DOC_VREAD,
        "GET",
        f"{path}/_history/1",
        tenant,
        then=lambda a: [equal("meta.versionId", version(a), "1"), equal("family", family(a), "Original")],
    )
    demo.step(
        "vread version 2",
        200,
        DOC_VREAD,
        "GET",
        f"{path}/_history/2",
        tenant,
        then=lambda a: [equal("meta.versionId", version(a), "2"), equal("family", family(a), "Updated")],
    )
    demo.step(
        "update with a stale If-Match is refused",
        412,
        DOC_CONFLICT,
        "PUT",
        path,
        tenant,
        body=demo.patient("Lost", id=pid),
        headers={"If-Match": 'W/"1"'},
        then=lambda a: [outcome(a)],
    )
    demo.step(
        "the refused update changed nothing",
        200,
        DOC_CONFLICT,
        "GET",
        path,
        tenant,
        then=lambda a: [equal("ETag", a.header("etag"), 'W/"2"'), equal("family", family(a), "Updated")],
    )


def batch(demo: Demo) -> None:
    tenant = demo.tenant_a
    run = demo.run_id
    existing, readable, doomed, stale, new, never_created, never_deleted = (
        f"batch-existing-{run}",
        f"batch-readable-{run}",
        f"batch-doomed-{run}",
        f"batch-stale-{run}",
        f"batch-new-{run}",
        f"batch-unknown-{run}",
        f"batch-absent-{run}",
    )
    for pid in (existing, readable, doomed, stale):
        demo.step(
            f"setup: create {pid.split('-')[1]}",
            201,
            DOC_UPDATE,
            "PUT",
            f"Patient/{pid}",
            tenant,
            body=demo.patient("Setup", id=pid),
            required=True,
        )
    demo.step(
        "setup: move the stale patient to version 2",
        200,
        DOC_UPDATE,
        "PUT",
        f"Patient/{stale}",
        tenant,
        body=demo.patient("Bumped", id=stale),
        required=True,
    )

    def entry(method: str, pid: str, resource: Optional[dict] = None, if_match: Optional[str] = None) -> dict:
        request = {"method": method, "url": f"Patient/{pid}"}
        if if_match:
            request["ifMatch"] = if_match
        return {"request": request, **({"resource": resource} if resource else {})}

    bundle = {
        "resourceType": "Bundle",
        "type": "batch",
        "entry": [
            entry("PUT", new, demo.patient("BatchNew", id=new)),
            entry("PUT", existing, demo.patient("BatchUpdated", id=existing)),
            entry("GET", readable),
            entry("DELETE", doomed),
            entry("GET", never_created),
            entry("DELETE", never_deleted),
            entry("PUT", stale, demo.patient("BatchStale", id=stale), if_match='W/"1"'),
        ],
    }
    # No two entries touch the same id: R4's batch rules allow no interdependencies.
    demo.step(
        "batch: a status per entry (201, 200, 200, 204, 404, 204, 412)",
        200,
        f"{DOC_BATCH}; {SKILL}, batch ifMatch and DELETE of an absent id",
        "POST",
        "",
        tenant,
        body=bundle,
        then=lambda a: [
            equal("Bundle.type", (a.body or {}).get("type"), "batch-response"),
            equal("entry statuses", a.entry_statuses(), ["201", "200", "200", "204", "404", "204", "412"]),
        ],
    )
    demo.step(
        "the batch PUT created its patient",
        200,
        DOC_READ,
        "GET",
        f"Patient/{new}",
        tenant,
        then=lambda a: [equal("family", family(a), "BatchNew")],
    )
    demo.step(
        "the batch PUT updated its patient",
        200,
        DOC_READ,
        "GET",
        f"Patient/{existing}",
        tenant,
        then=lambda a: [equal("family", family(a), "BatchUpdated")],
    )
    demo.step(
        "the batch DELETE removed its patient",
        410,
        DOC_GONE,
        "GET",
        f"Patient/{doomed}",
        tenant,
        then=lambda a: [outcome(a)],
    )
    demo.step(
        "the stale entry changed nothing",
        200,
        DOC_CONFLICT,
        "GET",
        f"Patient/{stale}",
        tenant,
        then=lambda a: [equal("ETag", a.header("etag"), 'W/"2"'), equal("family", family(a), "Bumped")],
    )

    transaction = {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": [
            {
                "resource": demo.patient("InTransaction"),
                "request": {"method": "POST", "url": "Patient"},
            }
        ],
    }
    demo.step(
        "transaction bundle",
        Differs(postgres=200, cassandra=501),
        Differs(
            postgres=f"{HTTP}#transaction (200, transaction-response)",
            cassandra=(
                f"{COVERAGE} section 6, 'Transaction Bundle' (501, not-supported, before any "
                "entry is applied); crates/rest/src/handlers/batch.rs, AtomicityUnsupported. "
                f"Departs from {HTTP}#transaction, where a server without transactions SHOULD "
                "answer 400"
            ),
        ),
        "POST",
        "",
        tenant,
        body=transaction,
        then=lambda a: (
            [
                equal("Bundle.type", (a.body or {}).get("type"), "transaction-response"),
                equal("entry statuses", a.entry_statuses(), ["201"]),
            ]
            if demo.backend == "postgres"
            else [outcome(a), equal("issue code", ((a.body or {}).get("issue") or [{}])[0].get("code"), "not-supported")]
        ),
    )


def search(demo: Demo) -> None:
    tenant = demo.tenant_a
    profile = f"http://example.org/fhir/StructureDefinition/demo-{demo.run_id}"
    source = f"urn:demo:{demo.run_id}:source"
    cohort = demo.identifier("cohort")

    # birthDate, gender, whether meta.source is set, and family, chosen so each
    # search below matches a known, different set of these five.
    rows = [
        ("1960-01-15", "female", False, "Smith"),
        ("1985-06-20", "male", False, "SMITH"),
        ("2000-02-29", "female", True, "Smithson"),
        ("2005-07-04", "male", True, "Jones"),
        ("2012-11-30", "female", True, "Brown"),
    ]
    ids: List[str] = []
    for number, (birth_date, gender, sourced, family_name) in enumerate(rows):
        meta: Dict[str, object] = {"profile": [profile]}
        if sourced:
            meta["source"] = source
        created = demo.step(
            f"setup: create patient {number + 1} of {len(rows)}",
            201,
            DOC_CREATE,
            "POST",
            "Patient",
            tenant,
            body=demo.patient(
                family_name,
                meta=meta,
                identifier=[cohort, demo.identifier(f"person-{number}")],
                gender=gender,
                birthDate=birth_date,
            ),
            required=True,
        )
        ids.append(new_id(created))

    token = f"{cohort['system']}|{cohort['value']}"

    def matches(indexes: List[int]) -> List[str]:
        return [ids[i] for i in indexes]

    def found(name: str, doc: str, query: str, expected_ids: List[str]) -> None:
        demo.step(
            name,
            200,
            doc,
            "GET",
            query,
            tenant,
            then=lambda a: [
                equal("Bundle.type", (a.body or {}).get("type"), "searchset"),
                same_ids("matches", a.entry_ids(), expected_ids),
            ],
        )

    demo.pages("token system|code", q("Patient", identifier=token, _count=2), tenant, ids, 2)
    found(
        "token code only (gender=male)",
        DOC_SEARCH,
        q("Patient", gender="male"),
        matches([1, 3]),
    )
    demo.pages(
        "date prefix ge",
        q("Patient", birthdate="ge2000-01-01", _count=2),
        tenant,
        matches([2, 3, 4]),
        2,
    )
    found(
        "date prefix lt",
        DOC_SEARCH,
        q("Patient", birthdate="lt1990-01-01"),
        matches([0, 1]),
    )
    found(
        "date equal",
        DOC_SEARCH,
        q("Patient", birthdate="2000-02-29"),
        matches([2]),
    )
    demo.pages("uri _profile", q("Patient", _profile=profile, _count=2), tenant, ids, 2)
    found(
        "uri _source",
        DOC_URI,
        q("Patient", _source=source),
        matches([2, 3, 4]),
    )
    found(
        "uri is a whole, case-sensitive match",
        DOC_URI,
        q("Patient", _profile=profile.upper()),
        [],
    )
    found(
        "two parameters are ANDed",
        DOC_COMBINING,
        q("Patient", identifier=token, gender="female", birthdate="ge2000-01-01"),
        matches([2, 4]),
    )

    demo.step(
        "an unknown parameter is ignored by default",
        200,
        DOC_UNKNOWN_PARAM,
        "GET",
        q("Patient", identifier=token, foo="bar"),
        tenant,
        then=lambda a: [same_ids("matches", a.entry_ids(), ids)],
    )
    demo.step(
        "an unknown parameter is refused with handling=strict",
        400,
        DOC_UNKNOWN_PARAM,
        "GET",
        q("Patient", identifier=token, foo="bar"),
        tenant,
        headers={"Prefer": "handling=strict"},
        then=lambda a: [outcome(a)],
    )
    demo.step(
        ":exact on a token parameter is rejected",
        400,
        DOC_EXACT_TOKEN,
        "GET",
        q("Patient", gender__exact="female"),
        tenant,
        then=lambda a: [outcome(a)],
    )

    # R4 defines :exact for string parameters only. PostgreSQL implements it;
    # Cassandra has no string search at all, so the same request is a 400 there.
    def exact_string_checks(a: Answer) -> List[Check]:
        if demo.backend == "postgres":
            return [same_ids("matches (not SMITH, not Smithson)", a.entry_ids(), matches([0]))]
        return [outcome(a)]

    demo.step(
        ":exact on a string parameter",
        Differs(postgres=200, cassandra=400),
        Differs(
            postgres=(
                f"{SEARCH}#modifiers (:exact matches the entire string, including casing); "
                "crates/persistence/src/backends/postgres/search/query_builder.rs, SearchModifier::Exact"
            ),
            cassandra=(
                f"{SEARCH}#modifiers; {COVERAGE} section 6, ':exact on string' "
                f"(UnsupportedParameterType); {DISCUSSION}, ':exact modifier'"
            ),
        ),
        "GET",
        q("Patient", family__exact="Smith"),
        tenant,
        then=exact_string_checks,
    )


def delete(demo: Demo) -> None:
    tenant = demo.tenant_a
    marker = demo.identifier("deleted")
    token = f"{marker['system']}|{marker['value']}"
    created = demo.step(
        "setup: create the patient to delete",
        201,
        DOC_CREATE,
        "POST",
        "Patient",
        tenant,
        body=demo.patient("Doomed", identifier=[marker]),
        required=True,
    )
    pid = new_id(created)
    path = f"Patient/{pid}"
    demo.step(
        "search finds it before the delete",
        200,
        DOC_SEARCH,
        "GET",
        q("Patient", identifier=token),
        tenant,
        then=lambda a: [same_ids("matches", a.entry_ids(), [pid])],
    )
    demo.step("delete", 204, DOC_DELETE, "DELETE", path, tenant, required=True)
    demo.step("read after delete is 410", 410, DOC_GONE, "GET", path, tenant, then=lambda a: [outcome(a)])
    demo.step(
        "vread of the delete version is 410",
        410,
        DOC_VREAD_GONE,
        "GET",
        f"{path}/_history/2",
        tenant,
        then=lambda a: [outcome(a)],
    )
    demo.step(
        "vread of the version before the delete still works",
        200,
        DOC_VREAD,
        "GET",
        f"{path}/_history/1",
        tenant,
        then=lambda a: [equal("family", family(a), "Doomed")],
    )
    demo.step(
        "search no longer finds it",
        200,
        DOC_SEARCH_DELETED,
        "GET",
        q("Patient", identifier=token),
        tenant,
        then=lambda a: [searchset(a), same_ids("matches", a.entry_ids(), [])],
    )
    demo.step("deleting it again is a no-op", 204, DOC_DELETE, "DELETE", path, tenant)


def isolation(demo: Demo) -> None:
    a, b = demo.tenant_a, demo.tenant_b
    shared_id = f"shared-{demo.run_id}"
    path = f"Patient/{shared_id}"
    marker = demo.identifier("isolation")
    token = f"{marker['system']}|{marker['value']}"

    only_in_a = new_id(
        demo.step(
            "tenant A creates a patient",
            201,
            DOC_CREATE,
            "POST",
            "Patient",
            a,
            body=demo.patient("OnlyInA", identifier=[marker]),
            required=True,
        )
    )
    demo.step(
        "tenant B cannot read it",
        404,
        DOC_ISOLATION,
        "GET",
        f"Patient/{only_in_a}",
        b,
        then=lambda r: [outcome(r)],
    )
    demo.step(
        "tenant B cannot find it by search",
        200,
        DOC_ISOLATION,
        "GET",
        q("Patient", identifier=token),
        b,
        then=lambda r: [searchset(r), same_ids("matches", r.entry_ids(), [])],
    )
    demo.step(
        "tenant A can find it by search",
        200,
        DOC_ISOLATION,
        "GET",
        q("Patient", identifier=token),
        a,
        then=lambda r: [same_ids("matches", r.entry_ids(), [only_in_a])],
    )

    for tenant, name, label in ((a, "InTenantA", "A"), (b, "InTenantB", "B")):
        demo.step(
            f"tenant {label} creates the same id at version 1",
            201,
            f"{DOC_UPDATE}; {DOC_ISOLATION}",
            "PUT",
            path,
            tenant,
            body=demo.patient(name, id=shared_id),
            then=lambda r: [equal("ETag", r.header("etag"), 'W/"1"')],
            required=True,
        )
    for tenant, name, label in ((a, "InTenantA", "A"), (b, "InTenantB", "B")):
        demo.step(
            f"tenant {label} reads its own copy",
            200,
            DOC_ISOLATION,
            "GET",
            path,
            tenant,
            then=lambda r: [equal("family", family(r), name)],
        )
    demo.step("tenant A deletes its copy", 204, f"{DOC_DELETE}; {DOC_ISOLATION}", "DELETE", path, a, required=True)
    demo.step("tenant A's copy is gone", 410, DOC_GONE, "GET", path, a, then=lambda r: [outcome(r)])
    demo.step(
        "tenant B's copy is untouched",
        200,
        DOC_ISOLATION,
        "GET",
        path,
        b,
        then=lambda r: [equal("family", family(r), "InTenantB"), equal("ETag", r.header("etag"), 'W/"1"')],
    )


def wait_for_server(demo: Demo, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while True:
        answer = demo.send("GET", "/metadata", None, None, None)
        if answer.status == 200:
            return True
        if time.monotonic() >= deadline:
            print(f"no 200 from {demo.base_url}/metadata within {seconds:.0f} s: "
                  f"{answer.error or answer.status}", file=sys.stderr)
            return False
        time.sleep(1)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the demonstration against a running HFS server.")
    parser.add_argument("--backend", required=True, choices=BACKENDS, help="which backend's documented answers to expect")
    parser.add_argument("--base-url", default="http://127.0.0.1:58080", help="the HFS URL; every request goes only here (default: %(default)s)")
    parser.add_argument("--wait", type=float, default=0.0, metavar="SECONDS", help="retry GET /metadata for this long before starting (default: no wait)")
    parser.add_argument("--timeout", type=float, default=60.0, metavar="SECONDS", help="one request's timeout (default: %(default)s)")
    parser.add_argument("--report", metavar="FILE", help="also write one JSON object per step, then a summary, to FILE")
    args = parser.parse_args()

    try:
        demo = Demo(args.base_url, args.backend, args.timeout, args.report)
    except (ValueError, OSError) as error:
        print(error, file=sys.stderr)
        return 2
    print(f"run {demo.run_id}  backend {demo.backend}  url {demo.base_url}")
    print(f"tenants {demo.tenant_a} and {demo.tenant_b}")

    if args.wait > 0 and not wait_for_server(demo, args.wait):
        return 2
    demo.started = time.monotonic()  # the summary's seconds exclude the wait

    sections = [
        ("capabilities", capabilities),
        ("create, read, vread, update", crud),
        ("batch and transaction", batch),
        ("search, paging and :exact", search),
        ("delete", delete),
        ("two tenants", isolation),
    ]
    for name, run in sections:
        demo.section(name, lambda run=run: run(demo))

    passed = demo.seq - demo.failed
    elapsed = time.monotonic() - demo.started
    print(f"\n{demo.seq} steps, {passed} passed, {demo.failed} failed, {elapsed:.1f} s  (run {demo.run_id}, {demo.backend})")
    if demo.report:
        summary = {
            "summary": True,
            "run_id": demo.run_id,
            "backend": demo.backend,
            "base_url": demo.base_url,
            "steps": demo.seq,
            "passed": passed,
            "failed": demo.failed,
            "seconds": round(elapsed, 3),
        }
        demo.report.write(json.dumps(summary) + "\n")
        demo.report.close()
    return 1 if demo.failed else 0


if __name__ == "__main__":
    sys.exit(main())
