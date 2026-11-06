//! Milestone A of the Cassandra backend (`docs/Cassandra/development-roadmap.md`,
//! Step 2, item 2.7): the real `hfs` binary, started with
//! `HFS_STORAGE_BACKEND=cassandra`, does one create/read round trip through
//! REST, and `/_readiness` reports down while Cassandra is stopped.
//!
//! - **The round trip** ([R4 http.html#create], [#read]): `POST /Patient` is
//!   `201 Created` with a `Location` naming the new id, `ETag: W/"1"`,
//!   `meta.versionId` "1" and a `meta.lastUpdated` instant; `GET` of that
//!   `Location`'s id is `200 OK` with the same resource, version and ETag.
//! - **Readiness**: `/_readiness` is `200` with the node up, `503` once it is
//!   stopped, and `200` again once it is back, after which the round trip
//!   works again (the `cdrs-tokio` session recovers by itself once the node
//!   is back).
//!
//! # Where the node comes from
//!
//! - The round trip uses `HFS_TEST_CASSANDRA_URL` (one `host:port`, the schema
//!   already applied; ADR-aacruzgon-0020) when it is set, and otherwise a
//!   container of its own.
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
//! Run with:
//! `HFS_TEST_CASSANDRA_URL=127.0.0.1:59042 cargo test -p helios-hfs --features cassandra --test cassandra_milestone_a`
//!
//! [R4 http.html#create]: https://hl7.org/fhir/R4/http.html#create
//! [#read]: https://hl7.org/fhir/R4/http.html#read

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

    node.stop().await;
    let stopped = Instant::now();
    let body = hfs
        .wait_for_readiness(StatusCode::SERVICE_UNAVAILABLE, DOWN_TIMEOUT)
        .await;
    assert_eq!(body["status"], "not_ready", "{body}");
    assert_eq!(body["checks"]["storage"], "unavailable", "{body}");
    eprintln!("/_readiness down {:?} after the stop", stopped.elapsed());

    node.start().await;
    let started = Instant::now();
    let body = hfs.wait_for_readiness(StatusCode::OK, UP_TIMEOUT).await;
    assert_eq!(body["checks"]["storage"], "ok", "{body}");
    eprintln!("/_readiness up {:?} after the start", started.elapsed());

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

/// `POST /Patient`, then `GET` of the id the response names.
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
    // `[base]/Patient/[id]/_history/[vid]`. HFS's REST layer, shared by every
    // backend, sends `[base]/Patient/[id]` (`crates/rest/src/handlers/create.rs`),
    // a deviation upstream owns; the same server already versions a batch
    // entry's `response.location`. An upstream issue is drafted, not yet filed
    // ("rest: `Location` from create (`POST [type]`) omits the version",
    // roadmap Step 2 report L7). This pins what it sends today: when the
    // version is added, update the expected value here and link the issue.
    assert_eq!(
        location,
        format!("{}/Patient/{id}", hfs.base),
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
        collect(child.stdout.take().expect("stdout"), output.clone());
        collect(child.stderr.take().expect("stderr"), output.clone());
        Self {
            child,
            base,
            output,
        }
    }

    fn output(&self) -> String {
        self.output.lock().expect("output lock").clone()
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
            tokio::time::sleep(Duration::from_millis(200)).await;
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

fn collect(stream: impl Read + Send + 'static, output: Arc<Mutex<String>>) {
    thread::spawn(move || {
        for line in BufReader::new(stream).lines().map_while(Result::ok) {
            let mut output = output.lock().expect("output lock");
            output.push_str(&line);
            output.push('\n');
        }
    });
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
