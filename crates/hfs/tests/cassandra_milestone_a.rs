//! Milestone A of the Cassandra backend (`docs/Cassandra/development-roadmap.md`,
//! Step 2, item 2.7): the real `hfs` binary, started with
//! `HFS_STORAGE_BACKEND=cassandra`, does one create/read round trip through
//! REST, and `/_readiness` reports down while Cassandra is stopped.
//!
//! - **The round trip** ([R4 http.html#create], [#read]): `POST /Patient` is
//!   `201 Created` with the versioned `Location`
//!   `[base]/Patient/[id]/_history/1`, `ETag: W/"1"`, `meta.versionId` "1"
//!   and a `meta.lastUpdated` instant; `GET /Patient/[id]` is `200 OK` with
//!   the same resource, version and ETag. A `GET` of the `Location` itself
//!   is a [#vread] of version 1, `200 OK` with the same resource (Step 3
//!   plan D1, D19), and a version past it is `404 Not Found`.
//! - **Readiness**: `/_readiness` is `200` with the node up, `503` once it is
//!   stopped, and `200` again once it is back, after which the round trip
//!   works again (the `cdrs-tokio` session recovers by itself once the node
//!   is back).
//!
//! # Step 3 through REST
//!
//! `step3_crud_through_rest` (roadmap Step 3, D24 of its plan) runs update,
//! delete, revive, PATCH, HEAD, `Prefer`, `If-Match`, the id check, one batch
//! and `/metadata` through the same `hfs`, each check against its HL7 FHIR R4
//! rule, and prints one `[spec row=… expected=… got=…]` line per check. A
//! `DELETE` of an unknown id, and of an already-deleted one, answers 204 and
//! writes no version (ADR-aacruzgon-0052). It needs every Step 3 method:
//! roadmap Step 3's write path, `create_or_update` and the versioned reads.
//!
//! # Where the node comes from
//!
//! - The round trip and the Step 3 test use `HFS_TEST_CASSANDRA_URL` (one
//!   `host:port`, the schema already applied; ADR-aacruzgon-0020) when it is
//!   set, and otherwise a container of its own.
//! - The readiness test stops the node, so it uses a container of its own,
//!   never the `HFS_TEST_CASSANDRA_URL` node, unless
//!   `HFS_TEST_CASSANDRA_STOPPABLE_CONTAINER` opts in: that names the container
//!   serving the URL node, which the test checks with `docker port` and then
//!   stops and starts. CI sets it to its Cassandra job's own node. The shared
//!   `docker/cassandra` node (`hfs-cassandra-cassandra-1`) is refused.
//!
//! A container of this file's runs the image `docker/cassandra` pins, through
//! that directory's `entrypoint.sh`, with a 512M heap and a 2 GiB limit. The
//! test applies `docs/Cassandra/schema/keyspace-single-node.cql` and
//! `schema.cql` with the container's own `cqlsh` (P6, ADR-aacruzgon-0027: a
//! test harness may apply the schema; `hfs` never does). Its CQL port is
//! published on a fixed host port, so it is the same after a restart, and the
//! container and its volume are removed when the test ends. Only one container
//! runs at a time. Without Docker, a test that needs a container skips.
//!
//! Each run writes under its own tenant (`HFS_DEFAULT_TENANT`, prefix `test-`),
//! so a reused node's earlier rows change nothing. Every `HFS_*`, `OTEL_*` and
//! `RUST_LOG` variable of the caller is removed from `hfs`'s environment, which
//! also keeps `HFS_SOF_ENABLED` unset (the Cassandra backend refuses it).
//!
//! Without Docker the readiness test skips, unless `HFS_TEST_CASSANDRA_URL` is
//! set: then it fails, so a run meant to check the exit criterion cannot pass
//! without stopping a node. The evidence of a real run is its
//! `/_readiness down …` and `/_readiness up …` lines on stderr.
//!
//! # The boot-time report
//!
//! `boot_time_report` (roadmap item 2.6) runs only when `HFS_TEST_BOOT_REPORT`
//! names a directory, and skips otherwise. It boots `hfs` with the conformance
//! seed three times, each on a fresh tenant, then once more on the third
//! tenant (the warm start), and writes `boot-time.json` and `boot-time.md`
//! there: each boot's spans, read from `hfs`'s own log lines, and the worst
//! fresh-tenant seed against the provisional budget of 10 s (roadmap section
//! 7). Over the budget is a warning in the report, never a failure; the test
//! fails only if `hfs` does not boot or a log line it needs is missing.
//!
//! # The REST latency report
//!
//! `rest_latency_report` runs only when `HFS_TEST_LATENCY_REPORT` names a
//! directory, and otherwise returns without a line. It boots `hfs` once on a
//! fresh tenant without the seed and times 50 `POST /Patient` + `GET` round
//! trips, one at a time: a smoke trend for CI, not the Step 8 benchmark.
//!
//! Run with:
//! `HFS_TEST_CASSANDRA_URL=127.0.0.1:59042 cargo test -p helios-hfs --features cassandra --test cassandra_milestone_a`
//!
//! and the Step 3 test alone with:
//! `HFS_TEST_CASSANDRA_URL=127.0.0.1:59042 cargo test -p helios-hfs --features cassandra --test cassandra_milestone_a -- --exact step3_crud_through_rest --show-output`
//!
//! and the boot-time report with:
//! `HFS_TEST_CASSANDRA_URL=127.0.0.1:59042 HFS_TEST_BOOT_REPORT=target/boot-report cargo test -p helios-hfs --features cassandra --test cassandra_milestone_a -- --exact boot_time_report`
//!
//! [R4 http.html#create]: https://hl7.org/fhir/R4/http.html#create
//! [#read]: https://hl7.org/fhir/R4/http.html#read
//! [#vread]: https://hl7.org/fhir/R4/http.html#vread

#![cfg(feature = "cassandra")]

use std::io::{BufRead, BufReader, Read};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::StatusCode;
use reqwest::header::{CONTENT_TYPE, ETAG, LAST_MODIFIED, LOCATION};
use serde_json::{Value, json};
use testcontainers::core::{CmdWaitFor, ExecCommand, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt, TestcontainersError};

/// The variable that names a running node (ADR-aacruzgon-0020, Rule 1).
const URL_VAR: &str = "HFS_TEST_CASSANDRA_URL";

/// The opt-in that lets the readiness test stop and start an existing
/// container instead of one of its own: the name of the container that
/// serves `URL_VAR`'s node. CI sets it to its job's own node, which no other
/// job uses.
const STOPPABLE_VAR: &str = "HFS_TEST_CASSANDRA_STOPPABLE_CONTAINER";

/// `docker/cassandra`'s default Compose project's container, the shared node
/// developers and other workers use; the readiness test refuses to stop it.
const SHARED_NODE_CONTAINER: &str = "hfs-cassandra-cassandra-1";

/// `docker/cassandra/docker-compose.yml`'s image, pinned by the same digest.
/// Change both together.
const IMAGE_NAME: &str = "cassandra";
const IMAGE_TAG: &str =
    "5.0.9-trixie@sha256:618a493bcf1f817ea2f07288a46fa76fb2d9c4dfd5a04c28227336405782889c";
const CQL_PORT: u16 = 9042;
const ENTRYPOINT_IN_CONTAINER: &str = "/usr/local/bin/hfs-entrypoint.sh";
/// The line Cassandra logs once the native transport listens.
const CQL_LISTENING: &str = "Starting listening for CQL clients";
/// Skips the 30 s ring delay and the gossip-settle waits a lone seed node does
/// not need, as `crates/persistence/tests/common/cassandra.rs` does.
const FAST_START: &str = "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.ring_delay_ms=0";
const MEMORY_LIMIT_BYTES: i64 = 2 * 1024 * 1024 * 1024;
const NANO_CPUS: i64 = 4_000_000_000;

/// From `docker run` (or `docker start`) to the CQL log line, image pull
/// included.
const NODE_STARTUP_TIMEOUT: Duration = Duration::from_secs(240);
/// From launch to `/metadata` answering, the conformance seed included.
const HFS_STARTUP_TIMEOUT: Duration = Duration::from_secs(180);
/// From `docker stop` to `/_readiness` reporting down.
const DOWN_TIMEOUT: Duration = Duration::from_secs(60);
/// From `docker start` to `/_readiness` reporting up again.
const UP_TIMEOUT: Duration = Duration::from_secs(240);

/// One container of this file's at a time: each holds up to 2 GiB, and the
/// readiness test stops its own.
static CONTAINER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn milestone_a_create_then_read_through_rest() {
    let _lock;
    let _container;
    let contact_point = match test_url() {
        Some(url) => url,
        None => {
            _lock = CONTAINER_LOCK.lock().await;
            let Some(node) = OwnNode::start().await else {
                return;
            };
            let contact_point = node.contact_point();
            _container = node;
            contact_point
        }
    };

    // No conformance seed: on a reused node every run would add about 1,400
    // rows under a new tenant. The readiness test, on its own node, boots with
    // the seed.
    let mut hfs = Hfs::start(&contact_point, &unique_tenant("milestone-a"), false);
    hfs.wait_until_serving().await;
    round_trip(&hfs).await;
}

#[tokio::test]
async fn readiness_reports_down_while_cassandra_is_stopped() {
    let _lock = CONTAINER_LOCK.lock().await;
    let node = match stoppable_container() {
        Some(name) => StoppableNode::named(name).await,
        None => match OwnNode::start().await {
            Some(node) => StoppableNode::Own(Box::new(node)),
            None => {
                // A skip must not pass for the exit criterion: a caller who
                // named a node meant the Cassandra tests to run, and this one
                // needs Docker (or STOPPABLE_VAR).
                assert!(
                    test_url().is_none(),
                    "{URL_VAR} is set, but Docker cannot be reached, and this test needs a node \
                     it may stop (a container of its own, or {STOPPABLE_VAR}); it cannot run"
                );
                return;
            }
        },
    };
    let mut hfs = Hfs::start(&node.contact_point(), &unique_tenant("readiness"), true);
    hfs.wait_until_serving().await;
    assert!(
        hfs.output()
            .contains("Seeded spec SearchParameters into storage"),
        "the conformance seed did not run; hfs output:\n{}",
        hfs.output()
    );
    let (status, body) = hfs.readiness().await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["checks"]["storage"], "ok", "{body}");

    // The bracketed `key=ms` pairs are read by `.github/scripts/ci-report.py
    // readiness`; keep their names.
    let stopping = Instant::now();
    node.stop().await;
    let stop_took = stopping.elapsed();
    let stopped = Instant::now();
    let body = hfs
        .wait_for_readiness(StatusCode::SERVICE_UNAVAILABLE, DOWN_TIMEOUT)
        .await;
    assert_eq!(body["status"], "not_ready", "{body}");
    assert_eq!(body["checks"]["storage"], "unavailable", "{body}");
    let down_after = stopped.elapsed();
    eprintln!(
        "/_readiness down {down_after:?} after the stop returned; the stop took {stop_took:?} \
         [stop_ms={} stopped_to_503_ms={}]",
        stop_took.as_millis(),
        down_after.as_millis()
    );

    let starting = Instant::now();
    node.start().await;
    let start_took = starting.elapsed();
    let started = Instant::now();
    let body = hfs.wait_for_readiness(StatusCode::OK, UP_TIMEOUT).await;
    assert_eq!(body["checks"]["storage"], "ok", "{body}");
    let up_after = started.elapsed();
    eprintln!(
        "/_readiness up {up_after:?} after the start returned; the start took {start_took:?} \
         [start_ms={} started_to_200_ms={}]",
        start_took.as_millis(),
        up_after.as_millis()
    );

    // The backend's session survived the outage for writes too, not only for
    // the readiness probe.
    round_trip(&hfs).await;
}

/// The node the readiness test stops and starts.
enum StoppableNode {
    /// A container of this test's.
    Own(Box<OwnNode>),
    /// The container `STOPPABLE_VAR` names, which serves `URL_VAR`'s node.
    Named { name: String, contact_point: String },
}

impl StoppableNode {
    /// Checks that `name` is the container publishing `URL_VAR`'s port, and
    /// that it is not the developer's shared node, before anything stops it.
    async fn named(name: String) -> Self {
        assert_ne!(
            name, SHARED_NODE_CONTAINER,
            "{STOPPABLE_VAR} names the shared docker/cassandra node, which no test may stop"
        );
        let contact_point = test_url()
            .unwrap_or_else(|| panic!("{STOPPABLE_VAR} is set, so {URL_VAR} must name its node"));
        let published = docker(&["port", &name, &format!("{CQL_PORT}/tcp")]).await;
        let url_port = contact_point.rsplit_once(':').map(|(_, port)| port);
        assert!(
            published
                .lines()
                .any(|line| line.rsplit_once(':').map(|(_, port)| port) == url_port),
            "{STOPPABLE_VAR}={name} publishes {published:?}, which is not {URL_VAR}={contact_point}"
        );
        Self::Named {
            name,
            contact_point,
        }
    }

    fn contact_point(&self) -> String {
        match self {
            Self::Own(node) => node.contact_point(),
            Self::Named { contact_point, .. } => contact_point.clone(),
        }
    }

    /// SIGTERM, then SIGKILL after 30 s: a node going away, as an operator's
    /// stop or a crash looks to the server.
    async fn stop(&self) {
        match self {
            Self::Own(node) => node
                .container
                .stop_with_timeout(Some(30))
                .await
                .expect("stop the test's Cassandra container"),
            Self::Named { name, .. } => {
                docker(&["stop", "--time", "30", name]).await;
            }
        }
    }

    async fn start(&self) {
        match self {
            Self::Own(node) => node
                .container
                .start()
                .await
                .expect("start the test's Cassandra container again"),
            Self::Named { name, .. } => {
                docker(&["start", name]).await;
            }
        }
    }
}

/// Runs the `docker` CLI and returns its stdout; panics if it fails.
async fn docker(args: &[&str]) -> String {
    let output = tokio::process::Command::new("docker")
        .args(args)
        .output()
        .await
        .unwrap_or_else(|e| panic!("docker {args:?}: {e}"));
    assert!(
        output.status.success(),
        "docker {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// `HFS_TEST_CASSANDRA_STOPPABLE_CONTAINER`, trimmed; empty counts as unset.
fn stoppable_container() -> Option<String> {
    std::env::var(STOPPABLE_VAR)
        .ok()
        .map(|name| name.trim().to_owned())
        .filter(|name| !name.is_empty())
}

/// `POST /Patient`, then `GET` of the id the response names, then `GET` of
/// its `Location`.
async fn round_trip(hfs: &Hfs) {
    let client = http();
    let family = format!("Milestone-{}", unique_suffix());
    let created = client
        .post(format!("{}/Patient", hfs.base))
        .header(CONTENT_TYPE, "application/fhir+json")
        .json(&json!({"resourceType": "Patient", "name": [{"family": family}]}))
        .send()
        .await
        .expect("POST /Patient");
    assert_eq!(
        created.status(),
        StatusCode::CREATED,
        "POST /Patient; hfs output:\n{}",
        hfs.output()
    );
    let location = header(&created, LOCATION);
    let etag = header(&created, ETAG);
    let body: Value = created.json().await.expect("the create's body is JSON");

    let id = body["id"].as_str().expect("the created resource has an id");
    assert!(!id.is_empty());
    // R4 http.html#create: the server "SHALL also return a Location header
    // which contains the new Logical Id and Version Id", that is
    // `[base]/Patient/[id]/_history/[vid]`. Upstream HFS sends
    // `[base]/Patient/[id]` (HeliosSoftware/hfs#2517). Its fix,
    // HeliosSoftware/hfs#2518, is backported here ahead of its upstream merge;
    // the next upstream sync brings the same change.
    assert_eq!(
        location,
        format!("{}/Patient/{id}/_history/1", hfs.base),
        "Location of the create"
    );
    assert_eq!(etag, "W/\"1\"", "ETag of the create");
    assert_eq!(body["meta"]["versionId"], "1", "{body}");
    let last_updated = body["meta"]["lastUpdated"]
        .as_str()
        .unwrap_or_else(|| panic!("the create has no meta.lastUpdated: {body}"));
    chrono::DateTime::parse_from_rfc3339(last_updated)
        .unwrap_or_else(|e| panic!("meta.lastUpdated {last_updated} is not an instant: {e}"));
    assert_eq!(body["name"][0]["family"], family.as_str(), "{body}");

    let read = client
        .get(format!("{}/Patient/{id}", hfs.base))
        .send()
        .await
        .expect("GET /Patient/{id}");
    assert_eq!(read.status(), StatusCode::OK, "GET /Patient/{id}");
    assert_eq!(header(&read, ETAG), "W/\"1\"", "ETag of the read");
    // R4 http.html#read: the server SHOULD return `Last-Modified`; it is
    // `meta.lastUpdated` at HTTP-date (second) precision.
    let last_modified = header(&read, LAST_MODIFIED);
    let last_modified = chrono::DateTime::parse_from_rfc2822(&last_modified)
        .unwrap_or_else(|e| panic!("Last-Modified {last_modified} is not an HTTP date: {e}"));
    assert_eq!(
        last_modified.timestamp(),
        chrono::DateTime::parse_from_rfc3339(last_updated)
            .expect("checked above")
            .timestamp(),
        "Last-Modified of the read"
    );
    let read: Value = read.json().await.expect("the read's body is JSON");
    assert_eq!(read["resourceType"], "Patient", "{read}");
    assert_eq!(read["id"], id, "{read}");
    assert_eq!(read["meta"]["versionId"], "1", "{read}");
    assert_eq!(read["meta"]["lastUpdated"], last_updated, "{read}");
    assert_eq!(read["name"], body["name"], "{read}");

    // The `Location` is a vread (R4 http.html#vread), real from Step 3
    // (plan D1, D19): version 1, the resource as the read gave it. "The
    // returned resource SHALL have an id element with a value that is the
    // [id], and a meta.versionId element with a value of [vid]."
    let vread = client
        .get(&location)
        .send()
        .await
        .expect("GET of the create's Location");
    assert_eq!(vread.status(), StatusCode::OK, "GET {location} (vread)");
    assert_eq!(header(&vread, ETAG), "W/\"1\"", "ETag of the vread");
    let vread: Value = vread.json().await.expect("the vread's body is JSON");
    assert_eq!(vread, read, "GET {location} (vread)");

    // A version the resource never had is 404 (R4 http.html, the summary
    // table's vread row: 200, 404).
    let unknown = client
        .get(format!("{}/Patient/{id}/_history/2", hfs.base))
        .send()
        .await
        .expect("GET of version 2");
    assert_eq!(unknown.status(), StatusCode::NOT_FOUND, "GET _history/2");
}

// Roadmap Step 3 through REST (D24 of the Step 3 plan).

/// Roadmap Step 3 (CRUD core) through the real `hfs`: update, delete,
/// revive, PATCH, HEAD, `Prefer`, `If-Match`, the id check, one batch, and
/// what `/metadata` says, each against the HL7 FHIR R4 rule it checks (the
/// Step 3 matrix's rows; R4B's text is the same).
///
/// Every check prints one `[spec row=<row> expected=<value> got=<value>]`
/// line before it asserts, for the CI report that tabulates them; keep the
/// shape. Values never contain a space.
///
/// It runs on the node `HFS_TEST_CASSANDRA_URL` names, or a container of its
/// own, under a fresh tenant and without the conformance seed, as
/// [`milestone_a_create_then_read_through_rest`] does.
#[tokio::test]
async fn step3_crud_through_rest() {
    let _lock;
    let _container;
    let contact_point = match test_url() {
        Some(url) => url,
        None => {
            _lock = CONTAINER_LOCK.lock().await;
            let Some(node) = OwnNode::start().await else {
                return;
            };
            let contact_point = node.contact_point();
            _container = node;
            contact_point
        }
    };
    let mut hfs = Hfs::start(&contact_point, &unique_tenant("step3-rest"), false);
    hfs.wait_until_serving().await;
    let rest = Rest::new(&hfs);
    let suffix = unique_suffix().replace('-', "");

    update_through_rest(&rest, &suffix).await;
    patch_through_rest(&rest, &suffix).await;
    delete_and_revive_through_rest(&rest, &suffix).await;
    invalid_ids_through_rest(&rest).await;
    batch_through_rest(&rest, &suffix).await;
    metadata_through_rest(&rest).await;
}

/// One printed, then asserted, spec check.
fn spec(row: &str, expected: impl std::fmt::Display, got: impl std::fmt::Display, hfs: &Hfs) {
    let (expected, got) = (expected.to_string(), got.to_string());
    println!("[spec row={row} expected={expected} got={got}]");
    assert_eq!(
        got,
        expected,
        "spec row {row}; hfs output:\n{}",
        hfs.output()
    );
}

/// A response's status, headers and body, read once.
struct Answer {
    status: StatusCode,
    headers: reqwest::header::HeaderMap,
    body: Value,
    /// The body's length in bytes, before any JSON parse.
    len: usize,
}

impl Answer {
    async fn of(response: reqwest::Response) -> Self {
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = response.bytes().await.expect("read the body");
        let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        Self {
            status,
            headers,
            body,
            len: bytes.len(),
        }
    }

    fn code(&self) -> u16 {
        self.status.as_u16()
    }

    /// A header's value, or `-` when absent.
    fn header(&self, name: reqwest::header::HeaderName) -> String {
        self.headers
            .get(&name)
            .map(|v| v.to_str().expect("an ASCII header").to_owned())
            .unwrap_or_else(|| "-".to_owned())
    }

    /// The first issue's `code` of an OperationOutcome body, or `-`.
    fn issue(&self) -> String {
        self.body["issue"][0]["code"]
            .as_str()
            .unwrap_or("-")
            .to_owned()
    }
}

/// `hfs`'s REST surface for these checks.
struct Rest<'a> {
    hfs: &'a Hfs,
    client: reqwest::Client,
}

impl<'a> Rest<'a> {
    fn new(hfs: &'a Hfs) -> Self {
        Self {
            hfs,
            client: http(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{path}", self.hfs.base)
    }

    async fn send(&self, request: reqwest::RequestBuilder) -> Answer {
        Answer::of(request.send().await.expect("the request is answered")).await
    }

    async fn get(&self, path: &str) -> Answer {
        self.send(self.client.get(self.url(path))).await
    }

    async fn head(&self, path: &str) -> Answer {
        self.send(self.client.head(self.url(path))).await
    }

    async fn post(&self, path: &str, body: &Value) -> Answer {
        self.send(
            self.client
                .post(self.url(path))
                .header(CONTENT_TYPE, "application/fhir+json")
                .json(body),
        )
        .await
    }

    /// `PUT`, with each `(name, value)` of `headers` added.
    async fn put(&self, path: &str, body: &Value, headers: &[(&str, &str)]) -> Answer {
        let mut request = self
            .client
            .put(self.url(path))
            .header(CONTENT_TYPE, "application/fhir+json")
            .json(body);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        self.send(request).await
    }

    async fn delete(&self, path: &str, headers: &[(&str, &str)]) -> Answer {
        let mut request = self.client.delete(self.url(path));
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        self.send(request).await
    }

    async fn patch(
        &self,
        path: &str,
        content_type: &str,
        body: &Value,
        headers: &[(&str, &str)],
    ) -> Answer {
        let mut request = self
            .client
            .patch(self.url(path))
            .header(CONTENT_TYPE, content_type)
            .body(body.to_string());
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        self.send(request).await
    }
}

/// A JSON value as a spec line shows it: a string without its quotes.
fn text(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

fn patient(id: &str, family: &str) -> Value {
    json!({"resourceType": "Patient", "id": id, "name": [{"family": family}]})
}

/// `meta.lastUpdated` of a body, as Unix seconds.
fn last_updated_seconds(body: &Value) -> i64 {
    let instant = body["meta"]["lastUpdated"]
        .as_str()
        .unwrap_or_else(|| panic!("no meta.lastUpdated: {body}"));
    chrono::DateTime::parse_from_rfc3339(instant)
        .unwrap_or_else(|e| panic!("meta.lastUpdated {instant}: {e}"))
        .timestamp()
}

/// PUT as create and as update, client `meta` ignored, `Prefer`, a stale
/// `If-Match` on PUT and DELETE, HEAD (matrix rows U-7, U-8, U-3, C-2, G4,
/// CC-4, G13; T-5, T-6, T-7, T-12).
async fn update_through_rest(rest: &Rest<'_>, suffix: &str) {
    let hfs = rest.hfs;
    let id = format!("s3-put-{suffix}");
    let path = format!("Patient/{id}");

    // R4 #update: a PUT to an id that does not exist creates it: 201, with
    // the versioned Location (#create's "SHALL also return a Location") and
    // the ETag.
    let created = rest.put(&path, &patient(&id, "One"), &[]).await;
    spec("U-7:put-new", 201, created.code(), hfs);
    spec(
        "U-8:location",
        format!("{}/{path}/_history/1", hfs.base),
        created.header(LOCATION),
        hfs,
    );
    spec("CC-2:etag", "W/\"1\"", created.header(ETAG), hfs);
    // R4 #prefer: with no `Prefer`, the server may return the resource, but
    // never an OperationOutcome; Helios returns the resource.
    spec(
        "G4:default-body",
        "Patient",
        created.body["resourceType"].as_str().unwrap_or("-"),
        hfs,
    );

    // R4 #update: "200 OK HTTP status code if the resource was updated …
    // with a Last-Modified header, and an ETag header which contains the new
    // versionId". No Location on a 200 (R4: "If the resource was created").
    let updated = rest.put(&path, &patient(&id, "Two"), &[]).await;
    spec("U-7:put-existing", 200, updated.code(), hfs);
    spec("U-7:etag", "W/\"2\"", updated.header(ETAG), hfs);
    spec("U-8:no-location", "-", updated.header(LOCATION), hfs);
    let last_modified = chrono::DateTime::parse_from_rfc2822(&updated.header(LAST_MODIFIED))
        .unwrap_or_else(|e| panic!("Last-Modified {}: {e}", updated.header(LAST_MODIFIED)))
        .timestamp();
    spec(
        "U-7:last-modified",
        last_updated_seconds(&updated.body),
        last_modified,
        hfs,
    );
    spec(
        "U-4:versionId",
        "2",
        text(&updated.body["meta"]["versionId"]),
        hfs,
    );

    // R4 #update and #create: "the server SHALL ignore the provided versionId
    // and lastUpdated values", on PUT and on POST.
    let mut with_meta = patient(&id, "Three");
    with_meta["meta"] = json!({"versionId": "99", "lastUpdated": "2001-01-01T00:00:00Z"});
    let put_meta = rest.put(&path, &with_meta, &[]).await;
    spec("U-3:put", 200, put_meta.code(), hfs);
    spec(
        "U-3:versionId",
        "3",
        text(&put_meta.body["meta"]["versionId"]),
        hfs,
    );
    spec(
        "U-3:lastUpdated-not-2001",
        true,
        last_updated_seconds(&put_meta.body) > 978_307_200,
        hfs,
    );
    let reread_put = rest.get(&path).await;
    spec(
        "U-3:read-versionId",
        "3",
        text(&reread_put.body["meta"]["versionId"]),
        hfs,
    );
    spec(
        "U-3:read-lastUpdated-not-2001",
        true,
        last_updated_seconds(&reread_put.body) > 978_307_200,
        hfs,
    );
    let mut post_meta = json!({"resourceType": "Patient", "name": [{"family": "Posted"}]});
    post_meta["meta"] = json!({"versionId": "99", "lastUpdated": "2001-01-01T00:00:00Z"});
    let posted = rest.post("Patient", &post_meta).await;
    spec("C-2:post", 201, posted.code(), hfs);
    spec(
        "C-2:versionId",
        "1",
        text(&posted.body["meta"]["versionId"]),
        hfs,
    );
    let posted_id = posted.body["id"]
        .as_str()
        .expect("the POST's id")
        .to_owned();
    let reread = rest.get(&format!("Patient/{posted_id}")).await;
    spec(
        "C-2:read-versionId",
        "1",
        text(&reread.body["meta"]["versionId"]),
        hfs,
    );
    spec(
        "C-2:read-lastUpdated-not-2001",
        true,
        last_updated_seconds(&reread.body) > 978_307_200,
        hfs,
    );

    // R4 #prefer: "Servers SHOULD honor this header": no body for minimal,
    // an OperationOutcome when asked for one.
    let minimal = rest
        .put(
            &path,
            &patient(&id, "Four"),
            &[("Prefer", "return=minimal")],
        )
        .await;
    spec("G4:minimal-status", 200, minimal.code(), hfs);
    spec("G4:minimal-body-bytes", 0, minimal.len, hfs);
    spec("G4:minimal-etag", "W/\"4\"", minimal.header(ETAG), hfs);
    let outcome = rest
        .put(
            &path,
            &patient(&id, "Five"),
            &[("Prefer", "return=OperationOutcome")],
        )
        .await;
    spec("G4:outcome-status", 200, outcome.code(), hfs);
    spec(
        "G4:outcome-body",
        "OperationOutcome",
        outcome.body["resourceType"].as_str().unwrap_or("-"),
        hfs,
    );
    spec("G4:outcome-etag", "W/\"5\"", outcome.header(ETAG), hfs);

    // R4 #concurrency: "If the version id given in the If-Match header does
    // not match, the server returns a 412 Precondition Failed status code
    // instead of updating the resource." DELETE honours it the same way.
    let stale_put = rest
        .put(&path, &patient(&id, "Stale"), &[("If-Match", "W/\"1\"")])
        .await;
    spec("CC-4:put-stale", 412, stale_put.code(), hfs);
    let stale_delete = rest.delete(&path, &[("If-Match", "W/\"1\"")]).await;
    spec("CC-4:delete-stale", 412, stale_delete.code(), hfs);
    let still = rest.get(&path).await;
    spec("CC-4:unchanged-status", 200, still.code(), hfs);
    spec("CC-4:unchanged-etag", "W/\"5\"", still.header(ETAG), hfs);
    spec(
        "CC-4:unchanged-body",
        "Five",
        still.body["name"][0]["family"].as_str().unwrap_or("-"),
        hfs,
    );
    let matching = rest
        .put(&path, &patient(&id, "Six"), &[("If-Match", "W/\"5\"")])
        .await;
    spec("CC-4:put-matching", 200, matching.code(), hfs);
    spec(
        "CC-4:put-matching-etag",
        "W/\"6\"",
        matching.header(ETAG),
        hfs,
    );

    // R4 #head: "same response as a GET, but with no body".
    let head = rest.head(&path).await;
    spec("G13:head-status", 200, head.code(), hfs);
    spec("G13:head-etag", "W/\"6\"", head.header(ETAG), hfs);
    spec("G13:head-body-bytes", 0, head.len, hfs);
}

/// PATCH goes through `update` (D17): JSON Patch and FHIRPath Patch, a stale
/// `If-Match`, and a PATCH of a deleted resource, which never revives it
/// (matrix row CC-11; T-11).
async fn patch_through_rest(rest: &Rest<'_>, suffix: &str) {
    let hfs = rest.hfs;
    let id = format!("s3-patch-{suffix}");
    let path = format!("Patient/{id}");
    let created = rest.put(&path, &patient(&id, "Original"), &[]).await;
    spec("CC-11:setup", 201, created.code(), hfs);

    let json_patch = json!([{"op": "replace", "path": "/name/0/family", "value": "JsonPatched"}]);
    let patched = rest
        .patch(&path, "application/json-patch+json", &json_patch, &[])
        .await;
    spec("CC-11:json-patch", 200, patched.code(), hfs);
    spec(
        "CC-11:json-patch-etag",
        "W/\"2\"",
        patched.header(ETAG),
        hfs,
    );
    spec(
        "CC-11:json-patch-body",
        "JsonPatched",
        patched.body["name"][0]["family"].as_str().unwrap_or("-"),
        hfs,
    );

    let fhirpath_patch = json!({"resourceType": "Parameters", "parameter": [{"name": "operation", "part": [
        {"name": "type", "valueCode": "replace"},
        {"name": "path", "valueString": "Patient.active"},
        {"name": "value", "valueBoolean": true}
    ]}]});
    let mut with_active = patient(&id, "JsonPatched");
    with_active["active"] = json!(false);
    let reset = rest
        .put(&path, &with_active, &[("If-Match", "W/\"2\"")])
        .await;
    spec("CC-11:setup-active", 200, reset.code(), hfs);
    let patched = rest
        .patch(&path, "application/fhir+json", &fhirpath_patch, &[])
        .await;
    spec("CC-11:fhirpath-patch", 200, patched.code(), hfs);
    spec(
        "CC-11:fhirpath-patch-etag",
        "W/\"4\"",
        patched.header(ETAG),
        hfs,
    );
    spec(
        "CC-11:fhirpath-patch-body",
        true,
        text(&patched.body["active"]),
        hfs,
    );

    // R4 #patch: "servers SHALL support conditional PATCH, which works exactly
    // the same as specified for update in Concurrency Management".
    let stale = rest
        .patch(
            &path,
            "application/json-patch+json",
            &json_patch,
            &[("If-Match", "W/\"1\"")],
        )
        .await;
    spec("CC-11:stale", 412, stale.code(), hfs);
    let still = rest.get(&path).await;
    spec("CC-11:stale-unchanged", "W/\"4\"", still.header(ETAG), hfs);

    // A deleted resource: its read is `Gone`, so the PATCH is 410, and it
    // writes nothing. A PATCH never brings a resource back.
    let deleted = rest.delete(&path, &[]).await;
    spec("CC-11:delete", 204, deleted.code(), hfs);
    let on_deleted = rest
        .patch(&path, "application/json-patch+json", &json_patch, &[])
        .await;
    spec("CC-11:patch-deleted", 410, on_deleted.code(), hfs);
    let after = rest.get(&path).await;
    spec("CC-11:patch-deleted-no-revive", 410, after.code(), hfs);
}

/// DELETE, the 410 read, a second DELETE and a DELETE of an unknown id (each
/// a 204 no-op that writes no version, ADR-aacruzgon-0052), HEAD after the
/// delete, the revive's 201 at N+2, and vread of the marker (matrix rows
/// D-3, D-4, R-3, G11, G13, U-7, U-14, V-1; T-4, T-9, T-12).
async fn delete_and_revive_through_rest(rest: &Rest<'_>, suffix: &str) {
    let hfs = rest.hfs;
    let id = format!("s3-delete-{suffix}");
    let path = format!("Patient/{id}");
    let created = rest.put(&path, &patient(&id, "One"), &[]).await;
    spec("D-3:setup", 201, created.code(), hfs);
    let updated = rest.put(&path, &patient(&id, "Two"), &[]).await;
    spec("D-3:setup-update", "W/\"2\"", updated.header(ETAG), hfs);

    // R4 #delete: "Upon successful deletion … a 204 No Content".
    let deleted = rest.delete(&path, &[]).await;
    spec("D-3:delete", 204, deleted.code(), hfs);
    spec("D-3:delete-body-bytes", 0, deleted.len, hfs);

    // R4 #read: "a GET for a deleted resource returns a 410 status code";
    // issue type `deleted`.
    let gone = rest.get(&path).await;
    spec("R-3:read-deleted", 410, gone.code(), hfs);
    spec("G11:issue", "deleted", gone.issue(), hfs);
    let head = rest.head(&path).await;
    spec("G13:head-deleted", 410, head.code(), hfs);

    // R4 #delete: "Performing this interaction on a resource that is already
    // deleted has no effect, and the server should return either a 200 OK …
    // or a 204 No Content" (ADR-aacruzgon-0052).
    let again = rest.delete(&path, &[]).await;
    spec("D-4:delete-again", 204, again.code(), hfs);
    let no_new_version = rest.get(&format!("{path}/_history/4")).await;
    spec("D-4:no-new-version", 404, no_new_version.code(), hfs);
    let still_gone = rest.get(&path).await;
    spec("D-4:still-gone", 410, still_gone.code(), hfs);

    // R4 #delete: "if the resource does not exist at all, the server should
    // return either a 200 OK … or a 204 No Content"; nothing is written, so the
    // id stays unknown (404), not deleted (410).
    let unknown = format!("Patient/s3-unknown-{suffix}");
    let delete_unknown = rest.delete(&unknown, &[]).await;
    spec("D-3:delete-unknown", 204, delete_unknown.code(), hfs);
    let read_unknown = rest.get(&unknown).await;
    spec("D-3:unknown-stays-unknown", 404, read_unknown.code(), hfs);
    let vread_unknown = rest.get(&format!("{unknown}/_history/1")).await;
    spec("D-3:unknown-no-version", 404, vread_unknown.code(), hfs);

    // R4 #delete: "Resources that have been deleted may be "brought back to
    // life" by a subsequent update interaction using an HTTP PUT", and #update:
    // "201 Created … (or brought back to life/re-created)". N = 2, the marker
    // is 3, so the revive is 4: neither second DELETE wrote a version.
    let revived = rest.put(&path, &patient(&id, "Back"), &[]).await;
    spec("U-14:revive", 201, revived.code(), hfs);
    spec(
        "U-14:revive-location",
        format!("{}/{path}/_history/4", hfs.base),
        revived.header(LOCATION),
        hfs,
    );
    spec("U-14:revive-etag", "W/\"4\"", revived.header(ETAG), hfs);
    let read = rest.get(&path).await;
    spec("U-14:read-revived", 200, read.code(), hfs);
    spec(
        "U-14:read-revived-body",
        "Back",
        read.body["name"][0]["family"].as_str().unwrap_or("-"),
        hfs,
    );

    // R4 #vread: "If the version referred to is actually one where the
    // resource was deleted, the server should return a 410 status code."
    let marker = rest.get(&format!("{path}/_history/3")).await;
    spec("V-1:vread-marker", 410, marker.code(), hfs);
    let before = rest.get(&format!("{path}/_history/2")).await;
    spec("V-1:vread-before", 200, before.code(), hfs);
    spec(
        "V-1:vread-before-body",
        "Two",
        before.body["name"][0]["family"].as_str().unwrap_or("-"),
        hfs,
    );
}

/// The id check on PUT (D11): an id that is not a FHIR id is 400 `invalid`
/// (resource.html#id; #rejecting-updates "failed basic FHIR validation
/// rules"; matrix row RS1; T-3), where SQLite stores it.
async fn invalid_ids_through_rest(rest: &Rest<'_>) {
    let hfs = rest.hfs;
    for (row, id) in [
        ("RS1:underscore", "a_b".to_owned()),
        ("RS1:65-characters", "a".repeat(65)),
    ] {
        let answer = rest
            .put(&format!("Patient/{id}"), &patient(&id, "Invalid"), &[])
            .await;
        spec(row, 400, answer.code(), hfs);
        spec(&format!("{row}-issue"), "invalid", answer.issue(), hfs);
    }
}

/// One batch with each entry kind Step 3 serves (matrix rows B-4 to B-7;
/// T-15): PUT new 201, PUT existing 200 (both with a versioned `location`
/// and an `etag`), DELETE 204, DELETE of an unknown id 204
/// (ADR-aacruzgon-0052), GET of a deleted resource 410, and a stale `ifMatch`
/// 412. No two entries name the same resource (R4 #brules: "there SHALL be no
/// interdependencies between the different entries").
async fn batch_through_rest(rest: &Rest<'_>, suffix: &str) {
    let hfs = rest.hfs;
    let existing = format!("s3-batch-existing-{suffix}");
    let to_delete = format!("s3-batch-delete-{suffix}");
    let deleted = format!("s3-batch-deleted-{suffix}");
    let stale = format!("s3-batch-stale-{suffix}");
    let new = format!("s3-batch-new-{suffix}");
    for id in [&existing, &to_delete, &deleted, &stale] {
        let created = rest
            .put(&format!("Patient/{id}"), &patient(id, "Setup"), &[])
            .await;
        spec("B-7:setup", 201, created.code(), hfs);
    }
    let gone = rest.delete(&format!("Patient/{deleted}"), &[]).await;
    spec("B-7:setup-delete", 204, gone.code(), hfs);
    let bumped = rest
        .put(&format!("Patient/{stale}"), &patient(&stale, "Bumped"), &[])
        .await;
    spec("B-7:setup-bump", "W/\"2\"", bumped.header(ETAG), hfs);

    let bundle = json!({
        "resourceType": "Bundle",
        "type": "batch",
        "entry": [
            {"resource": patient(&new, "BatchNew"),
             "request": {"method": "PUT", "url": format!("Patient/{new}")}},
            {"resource": patient(&existing, "BatchUpdated"),
             "request": {"method": "PUT", "url": format!("Patient/{existing}")}},
            {"request": {"method": "DELETE", "url": format!("Patient/{to_delete}")}},
            {"request": {"method": "DELETE", "url": format!("Patient/s3-batch-unknown-{suffix}")}},
            {"request": {"method": "GET", "url": format!("Patient/{deleted}")}},
            {"resource": patient(&stale, "BatchStale"),
             "request": {"method": "PUT", "url": format!("Patient/{stale}"), "ifMatch": "W/\"1\""}}
        ]
    });
    let answer = rest.post("", &bundle).await;
    spec("B-4:batch", 200, answer.code(), hfs);
    spec(
        "B-5:type",
        "batch-response",
        answer.body["type"].as_str().unwrap_or("-"),
        hfs,
    );
    let entries = answer.body["entry"].as_array().cloned().unwrap_or_default();
    spec("B-5:entries", 6, entries.len(), hfs);
    let status = |n: usize| {
        entries[n]["response"]["status"]
            .as_str()
            .unwrap_or("-")
            .split_whitespace()
            .next()
            .unwrap_or("-")
            .to_owned()
    };
    spec("B-7:put-new", "201", status(0), hfs);
    spec("B-7:put-existing", "200", status(1), hfs);
    spec("B-7:delete", "204", status(2), hfs);
    spec("B-7:delete-unknown", "204", status(3), hfs);
    spec("B-7:get-deleted", "410", status(4), hfs);
    spec("B-7:stale-if-match", "412", status(5), hfs);
    for (n, (id, version)) in [(0, (&new, 1)), (1, (&existing, 2))] {
        let response = &entries[n]["response"];
        spec(
            "B-6:location",
            true,
            response["location"]
                .as_str()
                .is_some_and(|l| l.ends_with(&format!("Patient/{id}/_history/{version}"))),
            hfs,
        );
        spec(
            "B-6:etag",
            format!("W/\"{version}\""),
            response["etag"].as_str().unwrap_or("-"),
            hfs,
        );
    }

    // Each entry did what its status says, and the refused one nothing.
    let read = rest.get(&format!("Patient/{to_delete}")).await;
    spec("B-7:deleted-reads-410", 410, read.code(), hfs);
    let read = rest.get(&format!("Patient/{stale}")).await;
    spec("B-7:stale-unchanged", "W/\"2\"", read.header(ETAG), hfs);
    let read = rest.get(&format!("Patient/{existing}")).await;
    spec(
        "B-7:existing-updated",
        "BatchUpdated",
        read.body["name"][0]["family"].as_str().unwrap_or("-"),
        hfs,
    );
}

/// What `/metadata` says for Patient once Step 3 is in, where it is true
/// (the matrix's section 4): the six interactions Step 3 serves,
/// `updateCreate`, no conditional create, update or delete, and no
/// `transaction`. Its remaining false claims (history interactions and every
/// registered search parameter) are Step 7's.
async fn metadata_through_rest(rest: &Rest<'_>) {
    let hfs = rest.hfs;
    let metadata = rest.get("metadata").await;
    spec("G1:metadata", 200, metadata.code(), hfs);
    let server = &metadata.body["rest"][0];
    let patient = server["resource"]
        .as_array()
        .and_then(|types| types.iter().find(|t| t["type"] == "Patient"))
        .unwrap_or_else(|| panic!("no Patient in /metadata: {}", metadata.body));
    let interactions: Vec<&str> = patient["interaction"]
        .as_array()
        .map(|list| list.iter().filter_map(|i| i["code"].as_str()).collect())
        .unwrap_or_default();
    for code in ["read", "vread", "create", "update", "patch", "delete"] {
        spec(
            &format!("CS-4:{code}"),
            true,
            interactions.contains(&code),
            hfs,
        );
    }
    spec(
        "CS-3:updateCreate",
        true,
        text(&patient["updateCreate"]),
        hfs,
    );
    spec(
        "CS-1:versioning",
        "versioned",
        text(&patient["versioning"]),
        hfs,
    );
    spec(
        "R-6:conditionalRead",
        "full-support",
        text(&patient["conditionalRead"]),
        hfs,
    );
    spec(
        "CD-2:conditionalCreate",
        false,
        text(&patient["conditionalCreate"]),
        hfs,
    );
    spec(
        "CD-2:conditionalUpdate",
        false,
        text(&patient["conditionalUpdate"]),
        hfs,
    );
    spec(
        "CD-2:conditionalDelete",
        "not-supported",
        text(&patient["conditionalDelete"]),
        hfs,
    );
    let system: Vec<&str> = server["interaction"]
        .as_array()
        .map(|list| list.iter().filter_map(|i| i["code"].as_str()).collect())
        .unwrap_or_default();
    spec(
        "B-1:no-transaction",
        false,
        system.contains(&"transaction"),
        hfs,
    );
    spec("B-4:batch", true, system.contains(&"batch"), hfs);
}

fn header(response: &reqwest::Response, name: reqwest::header::HeaderName) -> String {
    response
        .headers()
        .get(&name)
        .unwrap_or_else(|| panic!("no {name} header"))
        .to_str()
        .expect("an ASCII header")
        .to_owned()
}

/// A running `hfs`, killed on drop. Its output, stdout and stderr together,
/// is kept for failure messages.
struct Hfs {
    child: Child,
    base: String,
    output: Arc<Mutex<String>>,
    /// The threads copying stdout and stderr into `output`.
    readers: Vec<thread::JoinHandle<()>>,
}

impl Hfs {
    fn start(contact_point: &str, tenant: &str, seed: bool) -> Self {
        let port = free_port();
        let base = format!("http://127.0.0.1:{port}");
        let mut command = Command::new(env!("CARGO_BIN_EXE_hfs"));
        // Nothing in the caller's environment decides how this `hfs` runs, as
        // in `elasticsearch_startup.rs`. That also keeps HFS_SOF_ENABLED unset:
        // the Cassandra backend refuses it when it is set.
        for (key, _) in std::env::vars_os() {
            if let Some(key) = key.to_str()
                && (key.starts_with("HFS_") || key.starts_with("OTEL_") || key == "RUST_LOG")
            {
                command.env_remove(key);
            }
        }
        let mut child = command
            .env("HFS_STORAGE_BACKEND", "cassandra")
            .env("HFS_CASSANDRA_CONTACT_POINTS", contact_point)
            .env("HFS_DEFAULT_TENANT", tenant)
            .env("HFS_SEED_CONFORMANCE", seed.to_string())
            .env("HFS_SERVER_HOST", "127.0.0.1")
            .env("HFS_SERVER_PORT", port.to_string())
            .env("HFS_BASE_URL", &base)
            .env("HFS_LOG_LEVEL", "info")
            .current_dir(repo_path(""))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("start hfs");
        let output = Arc::new(Mutex::new(String::new()));
        let readers = vec![
            collect(child.stdout.take().expect("stdout"), output.clone()),
            collect(child.stderr.take().expect("stderr"), output.clone()),
        ];
        Self {
            child,
            base,
            output,
            readers,
        }
    }

    fn output(&self) -> String {
        self.output.lock().expect("output lock").clone()
    }

    /// `VmHWM` from `/proc/<pid>/status`, in KiB: the peak resident set so
    /// far. Linux only.
    fn peak_rss_kib(&self) -> Option<u64> {
        if !cfg!(target_os = "linux") {
            return None;
        }
        std::fs::read_to_string(format!("/proc/{}/status", self.child.id()))
            .ok()?
            .lines()
            .find_map(|line| line.strip_prefix("VmHWM:"))
            .and_then(|value| value.trim().trim_end_matches("kB").trim().parse().ok())
    }

    /// Kills `hfs` and returns its whole output, once both pipes are read to
    /// the end, so no line it wrote is still in a pipe.
    fn stop_and_output(mut self) -> String {
        let _ = self.child.kill();
        let _ = self.child.wait();
        for reader in std::mem::take(&mut self.readers) {
            let _ = reader.join();
        }
        self.output()
    }

    /// Waits until `/metadata` answers 200; fails if `hfs` exits first.
    async fn wait_until_serving(&mut self) {
        let deadline = Instant::now() + HFS_STARTUP_TIMEOUT;
        let client = http();
        loop {
            if let Ok(response) = client.get(format!("{}/metadata", self.base)).send().await
                && response.status() == StatusCode::OK
            {
                return;
            }
            if let Some(status) = self.child.try_wait().expect("poll hfs") {
                panic!(
                    "hfs exited ({status}) before serving; output:\n{}",
                    self.output()
                );
            }
            assert!(
                Instant::now() < deadline,
                "/metadata did not answer within {HFS_STARTUP_TIMEOUT:?}; hfs output:\n{}",
                self.output()
            );
            // Short, so the boot-time report's launch-to-`/metadata` span is
            // within 50 ms of the first answer.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn readiness(&self) -> (StatusCode, Value) {
        let response = http()
            .get(format!("{}/_readiness", self.base))
            .send()
            .await
            .expect("GET /_readiness");
        let status = response.status();
        let body = response.json().await.expect("/_readiness answers JSON");
        (status, body)
    }

    /// Polls `/_readiness` until it answers `status`, and returns that body.
    async fn wait_for_readiness(&self, status: StatusCode, timeout: Duration) -> Value {
        let deadline = Instant::now() + timeout;
        loop {
            let (now, body) = self.readiness().await;
            if now == status {
                return body;
            }
            assert!(
                Instant::now() < deadline,
                "/_readiness still {now} ({body}) after {timeout:?}, expected {status}; hfs \
                 output:\n{}",
                self.output()
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

impl Drop for Hfs {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn collect(
    stream: impl Read + Send + 'static,
    output: Arc<Mutex<String>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        for line in BufReader::new(stream).lines().map_while(Result::ok) {
            let mut output = output.lock().expect("output lock");
            output.push_str(&line);
            output.push('\n');
        }
    })
}

/// A Cassandra container of this test's, with the schema applied. Dropping it
/// removes the container and its volume.
struct OwnNode {
    container: ContainerAsync<GenericImage>,
    host_port: u16,
}

impl OwnNode {
    /// `None` (the test skips) when Docker cannot be reached.
    async fn start() -> Option<Self> {
        // `with_mapped_port` publishes on every interface, so the port must be
        // free on all of them, not only on loopback.
        let host_port = free_port_on("0.0.0.0");
        let request = GenericImage::new(IMAGE_NAME, IMAGE_TAG)
            .with_exposed_port(CQL_PORT.tcp())
            .with_wait_for(WaitFor::message_on_stdout(CQL_LISTENING))
            .with_entrypoint("sh")
            .with_cmd([ENTRYPOINT_IN_CONTAINER, "cassandra", "-f"])
            .with_copy_to(
                ENTRYPOINT_IN_CONTAINER,
                repo_path("docker/cassandra/entrypoint.sh"),
            )
            .with_copy_to(
                "/tmp/keyspace-single-node.cql",
                repo_path("docs/Cassandra/schema/keyspace-single-node.cql"),
            )
            .with_copy_to(
                "/tmp/schema.cql",
                repo_path("docs/Cassandra/schema/schema.cql"),
            )
            // A fixed host port, so a restarted container is where hfs
            // already looks.
            .with_mapped_port(host_port, CQL_PORT.tcp())
            .with_env_var("CASSANDRA_CLUSTER_NAME", "helios")
            .with_env_var("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
            .with_env_var("CASSANDRA_DC", "helios-dc1")
            .with_env_var("CASSANDRA_RACK", "helios-rack1")
            .with_env_var("MAX_HEAP_SIZE", "512M")
            .with_env_var("MAX_DIRECT_MEMORY_SIZE", "256M")
            .with_env_var(
                "JVM_EXTRA_OPTS",
                format!("-XX:ParallelGCThreads=4 -XX:ConcGCThreads=4 {FAST_START}"),
            )
            .with_host_config_modifier(|host_config| {
                host_config.memory = Some(MEMORY_LIMIT_BYTES);
                host_config.memory_swap = Some(MEMORY_LIMIT_BYTES);
                host_config.nano_cpus = Some(NANO_CPUS);
            })
            .with_startup_timeout(NODE_STARTUP_TIMEOUT);
        let container = match request.start().await {
            Ok(container) => container,
            Err(TestcontainersError::Client(error)) if !docker_answers().await => {
                eprintln!("Skipping: Docker cannot be reached ({error})");
                return None;
            }
            Err(error) => panic!("the test's Cassandra container did not start: {error}"),
        };
        apply_schema(&container).await;
        Some(Self {
            container,
            host_port,
        })
    }

    fn contact_point(&self) -> String {
        format!("127.0.0.1:{}", self.host_port)
    }
}

/// Applies the keyspace, then the schema, with the container's `cqlsh`. The
/// first statement is retried: the node logs that it listens a moment before
/// `cqlsh` can log in.
async fn apply_schema(container: &ContainerAsync<GenericImage>) {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let keyspace = container
            .exec(
                ExecCommand::new(["cqlsh", "-f", "/tmp/keyspace-single-node.cql"])
                    .with_cmd_ready_condition(CmdWaitFor::exit_code(0)),
            )
            .await;
        match keyspace {
            Ok(_) => break,
            Err(error) if Instant::now() < deadline => {
                eprintln!("cqlsh not ready yet ({error}); retrying");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            Err(error) => panic!("applying keyspace-single-node.cql failed: {error}"),
        }
    }
    container
        .exec(
            ExecCommand::new(["cqlsh", "-k", "helios", "-f", "/tmp/schema.cql"])
                .with_cmd_ready_condition(CmdWaitFor::exit_code(0)),
        )
        .await
        .expect("apply schema.cql");
}

/// Whether the Docker daemon answers a ping. Only asked after a client error,
/// to tell "no Docker" (skip) from a Docker that refused the request.
async fn docker_answers() -> bool {
    match testcontainers::bollard::Docker::connect_with_defaults() {
        Ok(docker) => tokio::time::timeout(Duration::from_secs(10), docker.ping())
            .await
            .is_ok_and(|ping| ping.is_ok()),
        Err(_) => false,
    }
}

/// A loopback port nothing is listening on.
fn free_port() -> u16 {
    free_port_on("127.0.0.1")
}

fn free_port_on(address: &str) -> u16 {
    TcpListener::bind(format!("{address}:0"))
        .and_then(|listener| listener.local_addr())
        .expect("reserve a local port")
        .port()
}

/// `relative` under the repository root (`hfs` reads `./data` from there).
fn repo_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(relative)
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("a clock after 1970")
        .as_nanos();
    format!("{nanos:x}-{}", std::process::id())
}

/// A tenant no earlier run used, with the L1 harness's `test-` prefix so test
/// rows on a shared node can be told apart. Tenant ids allow letters, digits
/// and `-`.
fn unique_tenant(test: &str) -> String {
    format!("test-{test}-{}", unique_suffix())
}

/// `HFS_TEST_CASSANDRA_URL`, trimmed; empty counts as unset, as in the L1
/// harness.
fn test_url() -> Option<String> {
    std::env::var(URL_VAR)
        .ok()
        .map(|url| url.trim().to_owned())
        .filter(|url| !url.is_empty())
}

/// The client every request goes through: no proxy (a developer's
/// `HTTP_PROXY` must not route loopback requests), and a bound on each request.
fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("HTTP client")
}

// The boot-time report (roadmap Step 2, item 2.6).

/// The directory `boot_time_report` writes to; unset, the test skips.
const BOOT_REPORT_VAR: &str = "HFS_TEST_BOOT_REPORT";

/// Seeding per fresh tenant on the single node: the startup budget of roadmap
/// section 7, adopted provisionally on 2026-11-06. A review threshold: over it
/// is a warning in the report, never a test failure.
const SEED_BUDGET: Duration = Duration::from_secs(10);

/// A warning, not a gate, when one boot's stored-SearchParameter load takes
/// longer. The load runs on every boot, warm ones included, and on every TTL
/// refresh. It scans `resources_by_type`'s partition keys (`SELECT DISTINCT`
/// over the whole table) for the tenants with a SearchParameter partition,
/// then reads each one: an id scan, then one point read per row. A seeded
/// tenant's partition holds 1,372 spec definitions, all read and then
/// skipped, so the seeded tenants are nearly all of the cost. 5 s is half the
/// 10 s seeding budget: the seed is paid once per tenant, the load on every
/// start, so it gets the stricter line. At the 0.2 s per seeded tenant
/// measured so far, that is about 25 seeded tenants on one node. CI's fresh
/// node only ever has three, so there the line catches a slowdown per tenant
/// (about 8×), not growth in their number.
const STORED_LOAD_WARN: Duration = Duration::from_secs(5);

/// Fresh-tenant boots; one warm boot follows, on the last fresh tenant.
const FRESH_BOOTS: usize = 3;

/// The log lines the report reads, all logged by `hfs` before it serves.
/// They are matched here only, so a reworded line is changed in one place.
mod startup_line {
    /// `start_cassandra` (`crates/hfs/src/main.rs`), before the session opens.
    pub const BACKEND_INIT: &str = "Initializing Cassandra backend";
    /// `CassandraBackend::connect`, once the session is open.
    pub const CONNECTED: &str = "Connected to Cassandra";
    /// `crates/persistence/src/search/seeder.rs`, after each set's creates.
    /// Neither is logged when the set's `count` finds it already stored.
    pub const SEEDED: [(&str, &str); 2] = [
        (
            "SearchParameter",
            "Seeded spec SearchParameters into storage",
        ),
        (
            "CompartmentDefinition",
            "Seeded spec CompartmentDefinitions into storage",
        ),
    ];
    /// `start_cassandra`, after the seed. Its `elapsed_ms` is the load's own
    /// time, so its start is the line's time minus `elapsed_ms`.
    pub const STORED_LOADED: &str = "Stored SearchParameters loaded";
    /// `serve`, once the listener is bound.
    pub const LISTENING: &str = "Server listening";
    /// In a `WARN` line: the seeder's failed create or failed set, or a
    /// failed tenant listing for the seed.
    pub const SEED_WARNING: &str = "seeding";
}

#[tokio::test]
async fn boot_time_report() {
    let Some(dir) = std::env::var_os(BOOT_REPORT_VAR)
        .map(PathBuf::from)
        .filter(|dir| !dir.as_os_str().is_empty())
    else {
        eprintln!("Skipping boot_time_report: {BOOT_REPORT_VAR} is unset");
        return;
    };
    let _lock;
    let _container;
    let (contact_point, node) = match test_url() {
        Some(url) => (url, "HFS_TEST_CASSANDRA_URL"),
        None => {
            _lock = CONTAINER_LOCK.lock().await;
            let Some(node) = OwnNode::start().await else {
                return;
            };
            let contact_point = node.contact_point();
            _container = node;
            (contact_point, "a container of this test's")
        }
    };

    let base = unique_tenant("boot");
    let mut runs = Vec::new();
    for run in 1..=FRESH_BOOTS {
        let tenant = format!("{base}-{run}");
        runs.push(Boot::measure(&contact_point, &tenant, format!("fresh {run}"), true).await);
    }
    let last = runs.last().expect("FRESH_BOOTS > 0").tenant.clone();
    runs.push(Boot::measure(&contact_point, &last, "warm".to_owned(), false).await);

    let report = boot_report(&runs, &contact_point, node).await;
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("create {}: {e}", dir.display()));
    let json_path = dir.join("boot-time.json");
    let md_path = dir.join("boot-time.md");
    std::fs::write(
        &json_path,
        serde_json::to_string_pretty(&report).expect("the report serialises"),
    )
    .unwrap_or_else(|e| panic!("write {}: {e}", json_path.display()));
    for run in &runs {
        let path = dir.join(format!("hfs-{}.log", run.label.replace(' ', "-")));
        std::fs::write(&path, &run.log).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
    }
    let markdown = boot_markdown(&report, &runs);
    std::fs::write(&md_path, &markdown)
        .unwrap_or_else(|e| panic!("write {}: {e}", md_path.display()));
    eprintln!("{markdown}");
    eprintln!("Wrote {} and {}", json_path.display(), md_path.display());
}

/// What one boot measured. Times are in milliseconds from `hfs`'s launch, or
/// between two of its log lines.
struct Boot {
    label: String,
    tenant: String,
    fresh: bool,
    /// Launch to the first `/metadata` 200, as the client saw it.
    to_metadata_ms: f64,
    /// Launch to `BACKEND_INIT`: process start, configuration, logging.
    to_backend_init_ms: f64,
    /// `BACKEND_INIT` to `CONNECTED`: the driver session opening, the
    /// local-DC check, and the base search registry's load from the spec
    /// file (`CassandraBackend::connect` logs `CONNECTED` after all three).
    /// No line separates them; the parse is most of it.
    connect_ms: f64,
    /// `CONNECTED` to the start of the stored-SearchParameter load: the
    /// ten-table check (`initialize`, which logs no line of its own) and the
    /// seed. On a warm boot, the seed is one `count` per set.
    seed_ms: f64,
    /// How `seed_ms` ends, for the reader.
    seed_end: &'static str,
    /// `CONNECTED` to the last `SEEDED` line, the span of the earlier hand
    /// measurement (s2-wiring, roadmap item 2.6); `None` on a warm boot.
    connected_to_last_seeded_ms: Option<f64>,
    /// The stored-SearchParameter load: `elapsed_ms`, tenants, definitions.
    stored_load: Option<(f64, u64, u64)>,
    /// Launch to `LISTENING`.
    to_listening_ms: f64,
    /// Per seeded type: created, existing, failed.
    seeded: Vec<(&'static str, u64, u64, u64)>,
    /// The seed's `WARN` lines.
    seed_warnings: Vec<String>,
    /// `hfs`'s whole output, kept next to the report as the figures' source.
    log: String,
    /// The process's peak resident set (`VmHWM`) once it serves, in KiB, on
    /// Linux; `None` elsewhere.
    peak_rss_kib: Option<u64>,
}

impl Boot {
    /// Boots `hfs` with the seed on `tenant`, waits for `/metadata`, stops it,
    /// and reads the spans from its log. Panics if it does not boot, or if a
    /// line a span needs is missing.
    async fn measure(contact_point: &str, tenant: &str, label: String, fresh: bool) -> Self {
        let launched_at = chrono::Utc::now();
        let launched = Instant::now();
        let mut hfs = Hfs::start(contact_point, tenant, true);
        hfs.wait_until_serving().await;
        let to_metadata = launched.elapsed();
        // Read while the process is alive: the seed and the load are done by
        // the time it serves, so this is the boot's peak.
        let peak_rss_kib = hfs.peak_rss_kib();
        // Killed before the next boot, so two never share the node; the
        // output is read once both pipes are drained.
        let output = hfs.stop_and_output();

        let lines = log_lines(&output);
        let need = |marker: &str| -> &LogLine<'_> {
            lines
                .iter()
                .find(|line| line.text.contains(marker))
                .unwrap_or_else(|| {
                    panic!("{label}: no {marker:?} line in hfs's log; output:\n{output}")
                })
        };
        let backend_init = need(startup_line::BACKEND_INIT).at;
        let connected = need(startup_line::CONNECTED).at;
        let listening = need(startup_line::LISTENING).at;

        let mut seeded = Vec::new();
        let mut last_seeded = None;
        for (kind, marker) in startup_line::SEEDED {
            if let Some(line) = lines.iter().find(|line| line.text.contains(marker)) {
                seeded.push((
                    kind,
                    line.number("created"),
                    line.number("existing"),
                    line.number("failed"),
                ));
                last_seeded = last_seeded.max(Some(line.at));
            }
        }
        if fresh {
            let created: u64 = seeded.iter().map(|(_, created, ..)| created).sum();
            assert!(
                created > 0,
                "{label}: a fresh tenant's boot created nothing, so the seed did not run; \
                 output:\n{output}"
            );
        }

        let stored = lines
            .iter()
            .find(|line| line.text.contains(startup_line::STORED_LOADED));
        let (seed_end_at, seed_end, stored_load) = match stored {
            Some(line) => {
                let elapsed = line.number("elapsed_ms");
                let start = line.at
                    - chrono::Duration::milliseconds(
                        i64::try_from(elapsed).expect("elapsed_ms fits an i64"),
                    );
                (
                    start,
                    "the start of the stored-SearchParameter load",
                    Some((
                        elapsed as f64,
                        line.number("tenants"),
                        line.number("stored"),
                    )),
                )
            }
            // A base whose load logs no such line: a fresh boot's seed still
            // ends at its last `SEEDED` line; a warm boot's cannot be read.
            None => match last_seeded {
                Some(at) => (at, "the last \"Seeded spec …\" line", None),
                None => panic!(
                    "{label}: no {:?} line and no \"Seeded\" line, so the seed's end cannot be \
                     read from hfs's log; output:\n{output}",
                    startup_line::STORED_LOADED
                ),
            },
        };
        let seed_warnings = lines
            .iter()
            .filter(|line| line.level == "WARN" && line.text.contains(startup_line::SEED_WARNING))
            .map(|line| line.text.to_owned())
            .collect();

        Self {
            label,
            tenant: tenant.to_owned(),
            fresh,
            to_metadata_ms: to_metadata.as_secs_f64() * 1000.0,
            to_backend_init_ms: millis_between(launched_at, backend_init),
            connect_ms: millis_between(backend_init, connected),
            seed_ms: millis_between(connected, seed_end_at),
            seed_end,
            connected_to_last_seeded_ms: last_seeded.map(|at| millis_between(connected, at)),
            stored_load,
            to_listening_ms: millis_between(launched_at, listening),
            seeded,
            seed_warnings,
            log: output,
            peak_rss_kib,
        }
    }

    fn created(&self) -> u64 {
        self.seeded.iter().map(|(_, created, ..)| created).sum()
    }

    fn to_json(&self) -> Value {
        let seeded: serde_json::Map<String, Value> = self
            .seeded
            .iter()
            .map(|(kind, created, existing, failed)| {
                (
                    (*kind).to_owned(),
                    json!({"created": created, "existing": existing, "failed": failed}),
                )
            })
            .collect();
        json!({
            "run": self.label,
            "tenant": self.tenant,
            "fresh_tenant": self.fresh,
            "launch_to_metadata_200_ms": round(self.to_metadata_ms),
            "launch_to_backend_init_ms": round(self.to_backend_init_ms),
            "connect_ms": round(self.connect_ms),
            "initialize_and_seed_ms": round(self.seed_ms),
            "initialize_and_seed_ends_at": self.seed_end,
            "connected_to_last_seeded_line_ms": self.connected_to_last_seeded_ms.map(round),
            "stored_search_parameter_load": self.stored_load.map(|(ms, tenants, stored)| json!({
                "elapsed_ms": ms, "tenants": tenants, "stored": stored,
            })),
            "launch_to_listening_ms": round(self.to_listening_ms),
            "created": self.created(),
            "seeded": seeded,
            "seed_warnings": self.seed_warnings,
            "peak_rss_mib": self.peak_rss_kib.map(|kib| round(kib as f64 / 1024.0)),
        })
    }
}

/// One line of `hfs`'s log in tracing's default format:
/// `<RFC 3339 time> <LEVEL> <target>: <message> <key>=<value> …`.
struct LogLine<'a> {
    at: chrono::DateTime<chrono::Utc>,
    level: &'a str,
    text: &'a str,
}

impl LogLine<'_> {
    /// The unsigned field `key=<n>`; panics if it is missing, since a span
    /// would otherwise be guessed.
    fn number(&self, key: &str) -> u64 {
        let prefix = format!("{key}=");
        self.text
            .split_whitespace()
            .find_map(|token| token.strip_prefix(&prefix))
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| panic!("no numeric {key}= in the log line {:?}", self.text))
    }
}

/// The lines of `output` that start with a timestamp; the rest (a panic,
/// a line split by the writer) are left out.
fn log_lines(output: &str) -> Vec<LogLine<'_>> {
    output
        .lines()
        .filter_map(|text| {
            let mut tokens = text.split_whitespace();
            let at = chrono::DateTime::parse_from_rfc3339(tokens.next()?).ok()?;
            Some(LogLine {
                at: at.with_timezone(&chrono::Utc),
                level: tokens.next()?,
                text,
            })
        })
        .collect()
}

fn millis_between(from: chrono::DateTime<chrono::Utc>, to: chrono::DateTime<chrono::Utc>) -> f64 {
    (to - from)
        .num_microseconds()
        .expect("a span under 292,000 years") as f64
        / 1000.0
}

/// To 0.1 ms, which is finer than any of the spans can be trusted to.
fn round(ms: f64) -> f64 {
    (ms * 10.0).round() / 10.0
}

/// The report: the runs, the budget, and where they were measured.
async fn boot_report(runs: &[Boot], contact_point: &str, node: &str) -> Value {
    let worst = runs
        .iter()
        .filter(|run| run.fresh)
        .map(|run| run.seed_ms)
        .fold(0.0_f64, f64::max);
    let budget_ms = SEED_BUDGET.as_secs_f64() * 1000.0;
    let mut warnings = Vec::new();
    if worst > budget_ms {
        warnings.push(format!(
            "Boot-time report: the worst fresh-tenant seed took {:.2} s, over the provisional \
             {} s budget (roadmap section 7)",
            worst / 1000.0,
            SEED_BUDGET.as_secs()
        ));
    }
    for run in runs {
        if !run.seed_warnings.is_empty() {
            warnings.push(format!(
                "Boot-time report: {} logged {} seed warning(s)",
                run.label,
                run.seed_warnings.len()
            ));
        }
        if !run.fresh && run.created() > 0 {
            warnings.push(format!(
                "Boot-time report: the warm boot created {} resources; the fresh boot before it \
                 did not seed the whole set",
                run.created()
            ));
        }
    }
    let worst_load = runs
        .iter()
        .filter_map(|run| run.stored_load.map(|(ms, ..)| ms))
        .fold(0.0_f64, f64::max);
    let load_warn_ms = STORED_LOAD_WARN.as_secs_f64() * 1000.0;
    if worst_load > load_warn_ms {
        warnings.push(format!(
            "Boot-time report: a boot's stored-SearchParameter load took {:.2} s, over the {} s \
             line proposed for it; it reads every seeded tenant's SearchParameter partition",
            worst_load / 1000.0,
            STORED_LOAD_WARN.as_secs()
        ));
    }
    let (cassandra_version, cassandra_version_source) = cassandra_version(contact_point).await;
    json!({
        "report": "boot-time",
        "roadmap_item": "2.6",
        "budget": {
            "seed_per_fresh_tenant_ms": budget_ms,
            "basis": "roadmap section 7, adopted provisionally on 2026-11-06; a review \
                      threshold, not a gate",
        },
        "worst_fresh_tenant_seed_ms": round(worst),
        "stored_load_warning": {
            "above_ms": load_warn_ms,
            "basis": "proposed: half the seeding budget, because the load runs on every boot \
                      and every TTL refresh",
        },
        "worst_stored_load_ms": round(worst_load),
        "within_budget": worst <= budget_ms,
        "context": {
            "commit": commit(),
            "runner": runner(),
            "build_profile": if cfg!(debug_assertions) { "debug" } else { "release" },
            "cpus": std::thread::available_parallelism().map(usize::from).ok(),
            "cassandra_version": cassandra_version,
            "cassandra_version_source": cassandra_version_source,
            "contact_point": contact_point,
            "node": node,
        },
        "spans": {
            "launch_to_metadata_200_ms": "hfs launched to the first GET /metadata 200, polled \
                                          every 50 ms",
            "launch_to_backend_init_ms": "launch to \"Initializing Cassandra backend\"",
            "connect_ms": "\"Initializing Cassandra backend\" to \"Connected to Cassandra\": the \
                           driver session, the local-DC check and the base search registry's \
                           load from the spec file, which no line separates",
            "initialize_and_seed_ms": "\"Connected to Cassandra\" to the start of the stored \
                                       SearchParameter load; includes initialize (the \
                                       ten-table check), which logs no line of its own",
            "connected_to_last_seeded_line_ms": "\"Connected to Cassandra\" to the last \
                                                 \"Seeded spec …\" line, the span of the \
                                                 earlier hand measurement; null on a warm boot",
            "stored_search_parameter_load": "the \"Stored SearchParameters loaded\" line's \
                                             elapsed_ms, tenants and stored",
            "launch_to_listening_ms": "launch to \"Server listening\"",
        },
        "runs": runs.iter().map(Boot::to_json).collect::<Vec<_>>(),
        "warnings": warnings,
    })
}

fn boot_markdown(report: &Value, runs: &[Boot]) -> String {
    let seconds = |ms: f64| format!("{:.2} s", ms / 1000.0);
    let context = &report["context"];
    let worst = report["worst_fresh_tenant_seed_ms"].as_f64().unwrap_or(0.0);
    let verdict = if report["within_budget"] == true {
        "within the budget"
    } else {
        "**over the budget**"
    };
    let mut md = format!(
        "Provisional budget (roadmap section 7): {} s of seeding per fresh tenant on the single \
         node. Worst fresh-tenant seed: **{}**, {verdict}. Report only: this never fails the job.\n\n",
        SEED_BUDGET.as_secs(),
        seconds(worst),
    );
    md.push_str(
        "| Run | Launch → `/metadata` 200 | Launch → backend init | Connect + base registry | `initialize` + seed \
         | Stored SearchParameter load | Launch → listening | Created | Peak RSS |\n\
         | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n",
    );
    for run in runs {
        let load = run.stored_load.map_or_else(
            || "no line".to_owned(),
            |(ms, tenants, stored)| format!("{} ({tenants} tenants, {stored} stored)", seconds(ms)),
        );
        let created = if run.seeded.is_empty() {
            "0 (the seeder's count found the sets)".to_owned()
        } else {
            run.seeded
                .iter()
                .map(|(kind, created, _, failed)| {
                    let failed = if *failed > 0 {
                        format!(", {failed} failed")
                    } else {
                        String::new()
                    };
                    format!("{created} {kind}{failed}")
                })
                .collect::<Vec<_>>()
                .join(" + ")
        };
        let rss = run.peak_rss_kib.map_or_else(
            || "n/a".to_owned(),
            |kib| format!("{:.0} MiB", kib as f64 / 1024.0),
        );
        md.push_str(&format!(
            "| {} | {} | {} | {} | {} | {load} | {} | {created} | {rss} |\n",
            run.label,
            seconds(run.to_metadata_ms),
            seconds(run.to_backend_init_ms),
            seconds(run.connect_ms),
            seconds(run.seed_ms),
            seconds(run.to_listening_ms),
        ));
    }

    let worst_load = report["worst_stored_load_ms"].as_f64().unwrap_or(0.0);
    md.push_str(&format!(
        "\n**The stored-SearchParameter load against the node's tenants.** It runs on every boot \
         and every TTL refresh, and reads every seeded tenant's 1,372-row SearchParameter \
         partition: an id scan, then a point read per row, all spec definitions it then skips. \
         Each fresh run adds one seeded tenant, so the change from the run before is the cost \
         of one; tenants without the seed hold a few rows and cost little. Worst load: **{}**; \
         a load over {} s (half the seeding budget, a proposed line) is a warning.\n\n\
         | Run | Tenants with a SearchParameter | Stored (non-spec) | Load | Change from the run before |\n\
         | --- | --- | --- | --- | --- |\n",
        seconds(worst_load),
        STORED_LOAD_WARN.as_secs(),
    ));
    let mut previous: Option<(f64, u64)> = None;
    for run in runs {
        match run.stored_load {
            Some((ms, tenants, stored)) => {
                let change = match previous {
                    Some((before, before_tenants)) => format!(
                        "{:+.2} s for {:+} tenant(s)",
                        (ms - before) / 1000.0,
                        tenants as i64 - before_tenants as i64
                    ),
                    None => "–".to_owned(),
                };
                md.push_str(&format!(
                    "| {} | {tenants} | {stored} | {} | {change} |\n",
                    run.label,
                    seconds(ms)
                ));
                previous = Some((ms, tenants));
            }
            None => md.push_str(&format!("| {} | – | – | no line | – |\n", run.label)),
        }
    }
    let text = |value: &Value| match value {
        Value::String(s) => s.clone(),
        Value::Null => "unknown".to_owned(),
        other => other.to_string(),
    };
    md.push_str(&format!(
        "\nEach run is a new `hfs` process with the conformance seed on; `warm` boots again on \
         the last fresh tenant. `initialize` + seed ends at {}; it includes the ten-table check, \
         which logs no line of its own. Connect + base registry is the driver session, the \
         local-DC check and the spec file's SearchParameter parse, which no line separates. \
         The first run's launch includes the first start of a newly built binary. The \
         stored-SearchParameter load reads every tenant on the node, so it grows with each \
         fresh run. Peak RSS is `VmHWM` once `hfs` serves (Linux only). Each boot's `hfs` log is in the artifact (`hfs-<run>.log`). Commit `{}`, runner `{}`, `{}` build, {} \
         CPUs, Cassandra {} ({}), node {} at `{}`.\n",
        runs.first().map_or("?", |run| run.seed_end),
        text(&context["commit"]),
        text(&context["runner"]),
        text(&context["build_profile"]),
        text(&context["cpus"]),
        text(&context["cassandra_version"]),
        text(&context["cassandra_version_source"]),
        text(&context["node"]),
        text(&context["contact_point"]),
    ));
    md
}

/// `CI_REPORT_COMMIT` in CI (the pull request's head, not the merge commit
/// `GITHUB_SHA` names on a pull request), otherwise `git rev-parse HEAD`.
fn commit() -> Option<String> {
    std::env::var("CI_REPORT_COMMIT")
        .or_else(|_| std::env::var("GITHUB_SHA"))
        .ok()
        .filter(|sha| !sha.is_empty())
        .or_else(|| {
            Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(repo_path(""))
                .output()
                .ok()
                .filter(|out| out.status.success())
                .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        })
}

/// `RUNNER_NAME` in CI, otherwise `local (<hostname>)`.
fn runner() -> String {
    std::env::var("RUNNER_NAME").unwrap_or_else(|_| {
        let host = Command::new("hostname")
            .output()
            .ok()
            .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
            .unwrap_or_default();
        format!("local ({host})")
    })
}

/// The node's `ReleaseVersion`, from `nodetool version` in the container that
/// publishes the contact point's port, and how it was found. `None` when no
/// such container is visible to `docker`.
async fn cassandra_version(contact_point: &str) -> (Option<String>, String) {
    let Some((_, port)) = contact_point.rsplit_once(':') else {
        return (None, format!("{contact_point} has no port"));
    };
    let published = format!(":{port}->{CQL_PORT}/tcp");
    let Some(containers) = docker_output(&["ps", "--format", "{{.Names}}\t{{.Ports}}"]).await
    else {
        return (None, "docker ps failed".to_owned());
    };
    let Some(name) = containers
        .lines()
        .find(|line| line.contains(&published))
        .and_then(|line| line.split('\t').next())
    else {
        return (None, format!("no container publishes {published}"));
    };
    let version = docker_output(&["exec", name, "nodetool", "version"])
        .await
        .and_then(|out| {
            out.lines()
                .find_map(|line| line.strip_prefix("ReleaseVersion:"))
                .map(|version| version.trim().to_owned())
        });
    (version, format!("nodetool version in {name}"))
}

/// The `docker` CLI's stdout, or `None` if it cannot run or fails.
async fn docker_output(args: &[&str]) -> Option<String> {
    let output = tokio::process::Command::new("docker")
        .args(args)
        .output()
        .await
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).into_owned())
}

// The REST latency report (CI's per-operation latency, layer L10).

/// The directory `rest_latency_report` writes to; unset, it returns at once.
/// The same variable as `helios-persistence`'s `cassandra_latency_report`, so
/// one CI setting asks for both.
const LATENCY_REPORT_VAR: &str = "HFS_TEST_LATENCY_REPORT";

/// Measured `POST /Patient` + `GET /Patient/{id}` round trips.
const REST_ROUND_TRIPS: usize = 50;
/// Unmeasured round trips first: the first of each statement prepares it.
const REST_WARM_UP: usize = 5;

/// Nearest-rank percentile of sorted samples, `p` in (0, 1].
fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let rank = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

fn ms(d: Duration) -> f64 {
    (d.as_secs_f64() * 1_000_000.0).round() / 1000.0
}

fn latency_row(operation: &str, samples: &[Duration]) -> Value {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    json!({
        "operation": operation,
        "n": sorted.len(),
        "p50_ms": ms(percentile(&sorted, 0.50)),
        "p95_ms": ms(percentile(&sorted, 0.95)),
        "p99_ms": ms(percentile(&sorted, 0.99)),
        "max_ms": ms(*sorted.last().expect("samples")),
    })
}

/// A smoke trend of REST latency through the real `hfs` binary, **not** the
/// Step 8 benchmark: on a fresh tenant, without the conformance seed,
/// [`REST_WARM_UP`] unmeasured round trips, then [`REST_ROUND_TRIPS`] of
/// `POST /Patient` then `GET /Patient/{id}`, one request at a time over
/// loopback. Writes `hfs-rest-latency.json` and `.md` into the directory
/// `HFS_TEST_LATENCY_REPORT` names. No figure fails it; a request that does not
/// answer 201 or 200 does, since that is a broken measurement.
#[tokio::test]
async fn rest_latency_report() {
    let Some(dir) = std::env::var_os(LATENCY_REPORT_VAR)
        .map(PathBuf::from)
        .filter(|dir| !dir.as_os_str().is_empty())
    else {
        // Not requested; deliberately no skip line, because this is a report,
        // not a live test the CI node should have run.
        return;
    };
    let _lock;
    let _container;
    let contact_point = match test_url() {
        Some(url) => url,
        None => {
            _lock = CONTAINER_LOCK.lock().await;
            let node = OwnNode::start().await.unwrap_or_else(|| {
                panic!("{LATENCY_REPORT_VAR} is set, but {URL_VAR} is unset and Docker cannot be reached")
            });
            let contact_point = node.contact_point();
            _container = node;
            contact_point
        }
    };

    let mut hfs = Hfs::start(&contact_point, &unique_tenant("rest-latency"), false);
    hfs.wait_until_serving().await;
    let client = http();
    let started = Instant::now();
    let mut posts = Vec::with_capacity(REST_ROUND_TRIPS);
    let mut gets = Vec::with_capacity(REST_ROUND_TRIPS);
    for i in 0..REST_WARM_UP + REST_ROUND_TRIPS {
        let t = Instant::now();
        let created = client
            .post(format!("{}/Patient", hfs.base))
            .header(CONTENT_TYPE, "application/fhir+json")
            .json(&json!({"resourceType": "Patient", "name": [{"family": format!("Latency-{i}")}]}))
            .send()
            .await
            .expect("POST /Patient");
        let status = created.status();
        let body: Value = created.json().await.expect("the create's body is JSON");
        let post = t.elapsed();
        assert_eq!(
            status,
            StatusCode::CREATED,
            "POST {i}: {body}; hfs output:\n{}",
            hfs.output()
        );
        let id = body["id"].as_str().expect("the created resource has an id");

        let t = Instant::now();
        let read = client
            .get(format!("{}/Patient/{id}", hfs.base))
            .send()
            .await
            .expect("GET /Patient/{id}");
        let status = read.status();
        let _ = read.bytes().await.expect("the read's body");
        let get = t.elapsed();
        assert_eq!(
            status,
            StatusCode::OK,
            "GET Patient/{id}; hfs output:\n{}",
            hfs.output()
        );

        if i >= REST_WARM_UP {
            posts.push(post);
            gets.push(get);
        }
    }
    let total = started.elapsed();
    drop(hfs);

    let rows = [
        latency_row("POST /Patient", &posts),
        latency_row("GET /Patient/{id}", &gets),
    ];
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let report = json!({
        "report": "hfs-rest-latency",
        "caveat": "single node, debug build, not the Step 8 benchmark",
        "through": "the hfs binary over loopback HTTP, one request at a time",
        "build_profile": profile,
        "contact_point": contact_point,
        "warm_up_round_trips": REST_WARM_UP,
        "total_s": (total.as_secs_f64() * 10.0).round() / 10.0,
        "operations": rows,
    });
    let mut md = format!(
        "_Single node, {profile} build, not the Step 8 benchmark._ Through the `hfs` binary \
         over loopback, one request at a time, on a fresh tenant without the conformance \
         seed, after {REST_WARM_UP} unmeasured round trips. Total {:.1} s (boot not \
         included).\n\n\
         | Operation | n | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) |\n\
         | --- | ---: | ---: | ---: | ---: | ---: |\n",
        total.as_secs_f64()
    );
    for row in &rows {
        md.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} |\n",
            row["operation"].as_str().expect("operation"),
            row["n"],
            row["p50_ms"],
            row["p95_ms"],
            row["p99_ms"],
            row["max_ms"],
        ));
    }
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("create {}: {e}", dir.display()));
    let json_path = dir.join("hfs-rest-latency.json");
    let md_path = dir.join("hfs-rest-latency.md");
    std::fs::write(
        &json_path,
        serde_json::to_string_pretty(&report).expect("the report serialises") + "\n",
    )
    .unwrap_or_else(|e| panic!("write {}: {e}", json_path.display()));
    std::fs::write(&md_path, &md).unwrap_or_else(|e| panic!("write {}: {e}", md_path.display()));
    eprintln!("{md}");
    eprintln!("Wrote {} and {}", json_path.display(), md_path.display());
}

#[test]
fn percentile_is_nearest_rank() {
    let samples: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
    assert_eq!(percentile(&samples, 0.50), Duration::from_millis(50));
    assert_eq!(percentile(&samples, 0.99), Duration::from_millis(99));
    assert_eq!(percentile(&samples, 1.0), Duration::from_millis(100));
    assert_eq!(
        percentile(&[Duration::from_millis(7)], 0.5),
        Duration::from_millis(7)
    );
}
