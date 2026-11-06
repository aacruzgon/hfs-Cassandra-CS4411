//! The Cassandra node that every Cassandra test runs against (roadmap item
//! 2.0, ADR-aacruzgon-0020).
//!
//! `#[path]`-included by `tests/cassandra_tests.rs`, which must also include
//! `common/container_cleanup.rs` as `container_cleanup` (once per binary; see
//! that file). No test target declares `mod common;`, so this file is compiled
//! only where it is included.
//!
//! # Where the node comes from
//!
//! - **`HFS_TEST_CASSANDRA_URL` set:** the tests use that running node. The
//!   value is one `host:port`, with no scheme, for example `127.0.0.1:59042`
//!   (`docker/cassandra`) or `127.0.0.1:59142` (the CI job). The schema must
//!   already be applied there (`docs/Cassandra/schema/keyspace-single-node.cql`,
//!   then `schema.cql`); the harness never runs DDL against that node. A node
//!   that does not answer, or lacks one of the ten tables, fails every test.
//! - **Unset:** the harness starts its own container from the image
//!   `docker/cassandra` pins, through that directory's `entrypoint.sh`
//!   (`cassandra_latest.yaml`, `paxos_variant: v2`), with the same cluster, DC
//!   and rack names, a 512M heap and a 2 GiB memory limit. It waits for the
//!   CQL port, retries a `SELECT` until the node answers, and applies the
//!   keyspace and the schema once per test binary (P6, ADR-aacruzgon-0027: the
//!   test harness is one of the processes allowed to apply it). The container
//!   is held in a `static` and removed at process exit by
//!   `container_cleanup`'s `#[ctor::dtor]` hook. When Docker is not available
//!   the tests skip.
//!
//! Only the address is shared. A `cdrs-tokio` session spawns its background
//! tasks on the runtime that builds it, and every `#[tokio::test]` has its own
//! runtime, so each test opens its own session with [`CassandraNode::session`].
//!
//! # Isolation
//!
//! Tests run in parallel against one node, which may also hold rows from
//! earlier runs. Each test writes under its own tenant ([`unique_tenant`]) or
//! its own resource ids ([`unique_id`]), never cleans up, and never truncates
//! or drops anything (ADR-aacruzgon-0020, Rule 2). `pending_index_writes` is
//! partitioned across tenants, so a sweeper test asserts only on its own
//! tenant's intents.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use cdrs_tokio::cluster::session::{Session, SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::{NodeTcpConfigBuilder, TcpConnectionManager};
use cdrs_tokio::frame::Envelope;
use cdrs_tokio::load_balancing::RoundRobinLoadBalancingStrategy;
use cdrs_tokio::transport::TransportTcp;
use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::rows::Row;
use helios_persistence::tenant::{TenantContext, TenantId, TenantPermissions};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt, TestcontainersError};
use tokio::sync::OnceCell;

/// The variable that names a running node (ADR-aacruzgon-0020, Rule 1).
pub const URL_VAR: &str = "HFS_TEST_CASSANDRA_URL";

/// The keyspace `keyspace-single-node.cql` creates.
pub const KEYSPACE: &str = "helios";

/// The ten tables of `docs/Cassandra/schema/schema.cql`, sorted.
pub const TABLES: [&str; 10] = [
    "date_index",
    "date_index_by_end",
    "pending_index_writes",
    "resource_versions",
    "resources",
    "resources_by_type",
    "token_code_index",
    "token_system_code_index",
    "token_system_index",
    "uri_index",
];

/// The Cassandra release of the pinned image (ADR-aacruzgon-0001).
pub const RELEASE_VERSION: &str = "5.0.9";

/// The data centre the keyspace replicates to; `docker/cassandra`'s snitch
/// setting (ADR-aacruzgon-0003).
pub const DATA_CENTER: &str = "helios-dc1";

/// `docker/cassandra/docker-compose.yml`'s image, pinned by the same digest.
/// Change both together.
const IMAGE_NAME: &str = "cassandra";
const IMAGE_TAG: &str =
    "5.0.9-trixie@sha256:618a493bcf1f817ea2f07288a46fa76fb2d9c4dfd5a04c28227336405782889c";

const CQL_PORT: u16 = 9042;

/// Where `docker/cassandra/entrypoint.sh` is copied in the container; the
/// path the Compose file mounts it at.
const ENTRYPOINT_IN_CONTAINER: &str = "/usr/local/bin/hfs-entrypoint.sh";

/// The line Cassandra logs once the native transport listens
/// (`PipelineConfigurator`, 5.0.9).
const CQL_LISTENING: &str = "Starting listening for CQL clients";

/// The harness's own container: half of `docker/cassandra`'s heap and direct
/// memory, the same 4 CPUs and GC threads. Measured with
/// `cassandra_latest.yaml` on an Apple-silicon laptop under OrbStack: about
/// 1.1 GiB at idle.
const HEAP: &str = "512M";
const DIRECT_MEMORY: &str = "256M";
const MEMORY_LIMIT_BYTES: i64 = 2 * 1024 * 1024 * 1024;
const NANO_CPUS: i64 = 4_000_000_000;

/// Properties that skip the waits a lone seed node does not need: the 30 s
/// ring delay and three 8 s gossip-settle waits. They bring a first boot from
/// about 56 s to about 5 s. Once the node is up, `ring_delay_ms` is read again
/// only for quarantine, node replacement, decommission and moves, none of which
/// happens to a single node.
const FAST_START: &str = "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.ring_delay_ms=0";

/// From `docker run` to the CQL log line. A first boot takes about 5 s here,
/// plus the image pull on a machine that does not have it yet.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(240);

/// From the CQL log line (or the first attempt on a URL node) to a `SELECT`
/// that succeeds.
const READY_TIMEOUT: Duration = Duration::from_secs(60);

/// One connection attempt; a node that accepts TCP but never answers must not
/// hang the suite.
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);

/// One statement through [`select`] or the schema apply. `cdrs-tokio` 8 has
/// only a connect timeout, so a node that stops answering would hang a test.
const STATEMENT_TIMEOUT: Duration = Duration::from_secs(60);

/// The session type the harness hands out. The backend's own connection code
/// (roadmap item 2.1) sets consistency, retries and prepared statements; this
/// one keeps the driver's defaults and is for DDL, `system_schema` reads and
/// assertions.
pub type TestSession = Session<
    TransportTcp,
    TcpConnectionManager,
    RoundRobinLoadBalancingStrategy<TransportTcp, TcpConnectionManager>,
>;

/// Where the node came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeOrigin {
    /// `HFS_TEST_CASSANDRA_URL`.
    Url,
    /// The harness's own container.
    Container,
}

/// A ready node with the schema applied.
pub struct CassandraNode {
    contact_point: String,
    origin: NodeOrigin,
    /// Kept alive for the whole test binary; `container_cleanup` removes it at
    /// exit. `None` for a URL node.
    _container: Option<ContainerAsync<GenericImage>>,
}

impl CassandraNode {
    /// The node's address as `host:port`, the format of `HFS_TEST_CASSANDRA_URL`.
    pub fn contact_point(&self) -> &str {
        &self.contact_point
    }

    pub fn origin(&self) -> NodeOrigin {
        self.origin
    }

    /// A new session with no keyspace selected; qualify table names with
    /// [`KEYSPACE`]. Panics when the node does not accept it.
    pub async fn session(&self) -> TestSession {
        connect(&self.contact_point, None)
            .await
            .unwrap_or_else(|error| panic!("cannot connect to {}: {error}", self.contact_point))
    }
}

/// What the first [`node`] call found, kept for every later call. A failure
/// is cached too: `OnceCell` would otherwise run the whole start again for
/// each test, a 60 s wait or a new container every time.
enum Outcome {
    Ready(CassandraNode),
    /// `HFS_TEST_CASSANDRA_URL` unset and Docker not reachable.
    Skip,
    Failed(String),
}

static NODE: OnceCell<Outcome> = OnceCell::const_new();

/// The shared node, started or connected to on the first call in this binary.
///
/// `None` only when `HFS_TEST_CASSANDRA_URL` is unset and Docker cannot be
/// reached; the caller then skips. Every other failure panics, in every test,
/// with the first attempt's message, because it means the node or the harness
/// is broken.
pub async fn node() -> Option<&'static CassandraNode> {
    match NODE.get_or_init(init).await {
        Outcome::Ready(node) => Some(node),
        Outcome::Skip => None,
        Outcome::Failed(message) => panic!("{message}"),
    }
}

async fn init() -> Outcome {
    let result = match std::env::var(URL_VAR) {
        Ok(raw) if !raw.trim().is_empty() => connect_to_url(&raw).await.map(Some),
        _ => start_container().await,
    };
    match result {
        Ok(Some(node)) => Outcome::Ready(node),
        Ok(None) => Outcome::Skip,
        Err(message) => Outcome::Failed(message),
    }
}

async fn connect_to_url(raw: &str) -> Result<CassandraNode, String> {
    let contact_point =
        parse_url(raw).map_err(|error| format!("{URL_VAR}={raw:?} is invalid: {error}"))?;
    let session = wait_until_ready(&contact_point)
        .await
        .map_err(|error| format!("{URL_VAR}={contact_point}: {error}"))?;
    // Rule 1: the schema is already there. Check it rather than apply it.
    check_tables(&session).await.map_err(|error| {
        format!(
            "{URL_VAR}={contact_point}: {error}. Apply docs/Cassandra/schema/keyspace-single-node.cql, \
             then schema.cql (docker/cassandra/README.md); after a change to schema.cql, drop \
             keyspace {KEYSPACE} and apply both again"
        )
    })?;
    Ok(CassandraNode {
        contact_point,
        origin: NodeOrigin::Url,
        _container: None,
    })
}

/// `Ok(None)` when Docker cannot be reached. A container that was created but
/// did not become ready (startup timeout, an OOM kill, a failing
/// `entrypoint.sh` assertion), or an image that cannot be pulled from a
/// reachable daemon, is an error, never a skip.
async fn start_container() -> Result<Option<CassandraNode>, String> {
    let request = GenericImage::new(IMAGE_NAME, IMAGE_TAG)
        .with_exposed_port(CQL_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout(CQL_LISTENING))
        // `sh <script>`, as the Compose file does, so the copied file's mode
        // does not matter.
        .with_entrypoint("sh")
        .with_cmd([ENTRYPOINT_IN_CONTAINER, "cassandra", "-f"])
        .with_copy_to(
            ENTRYPOINT_IN_CONTAINER,
            repo_path("docker/cassandra/entrypoint.sh"),
        )
        .with_env_var("CASSANDRA_CLUSTER_NAME", "helios")
        .with_env_var("CASSANDRA_ENDPOINT_SNITCH", "GossipingPropertyFileSnitch")
        .with_env_var("CASSANDRA_DC", DATA_CENTER)
        .with_env_var("CASSANDRA_RACK", "helios-rack1")
        .with_env_var("MAX_HEAP_SIZE", HEAP)
        .with_env_var("MAX_DIRECT_MEMORY_SIZE", DIRECT_MEMORY)
        .with_env_var(
            "JVM_EXTRA_OPTS",
            format!("-XX:ParallelGCThreads=4 -XX:ConcGCThreads=4 {FAST_START}"),
        )
        .with_host_config_modifier(|host_config| {
            host_config.memory = Some(MEMORY_LIMIT_BYTES);
            host_config.memory_swap = Some(MEMORY_LIMIT_BYTES);
            host_config.nano_cpus = Some(NANO_CPUS);
        })
        .with_startup_timeout(STARTUP_TIMEOUT);
    // `NODE` is a static and never dropped; the label lets the exit hook
    // remove the container and its anonymous data volume.
    let container = match super::container_cleanup::with_cleanup_label(request)
        .start()
        .await
    {
        Ok(container) => container,
        Err(error @ TestcontainersError::Client(_)) if !docker_answers().await => {
            eprintln!(
                "Skipping Cassandra tests: {URL_VAR} is unset and Docker cannot be reached \
                 ({error})"
            );
            return Ok(None);
        }
        Err(error) => return Err(format!("the Cassandra container did not start: {error}")),
    };
    let port = container
        .get_host_port_ipv4(CQL_PORT)
        .await
        .map_err(|error| format!("the Cassandra container has no mapped port: {error}"))?;
    let host = container
        .get_host()
        .await
        .map_err(|error| format!("the Cassandra container has no host: {error}"))?
        .to_string();
    // The port is published on IPv4; `localhost` can resolve to `::1` first
    // (docker/cassandra/README.md).
    let host = if host == "localhost" {
        "127.0.0.1".to_owned()
    } else {
        host
    };
    let contact_point = format!("{host}:{port}");
    let session = wait_until_ready(&contact_point)
        .await
        .map_err(|error| format!("the harness's Cassandra container: {error}"))?;
    apply_schema(&session, &contact_point).await?;
    check_tables(&session)
        .await
        .map_err(|error| format!("after applying the schema: {error}"))?;
    Ok(Some(CassandraNode {
        contact_point,
        origin: NodeOrigin::Container,
        _container: Some(container),
    }))
}

/// Whether the Docker daemon answers a ping, through bollard's defaults
/// (`DOCKER_HOST`, else the platform's socket). Only asked after a client
/// error, to tell "no Docker" (skip) from a Docker that refused the request.
async fn docker_answers() -> bool {
    match testcontainers::bollard::Docker::connect_with_defaults() {
        Ok(docker) => tokio::time::timeout(ATTEMPT_TIMEOUT, docker.ping())
            .await
            .is_ok_and(|ping| ping.is_ok()),
        Err(_) => false,
    }
}

/// Parses `HFS_TEST_CASSANDRA_URL`: one `host:port`, no scheme, a port from
/// 1 to 65535. An IPv6 host goes in brackets, `[::1]:9042`. Returns the value
/// trimmed. The backend's own setting (roadmap item 2.1) should accept the
/// same form.
pub fn parse_url(raw: &str) -> Result<String, String> {
    let value = raw.trim();
    if value.contains("://") {
        return Err("expected host:port with no scheme, for example 127.0.0.1:59042".into());
    }
    if value.contains(',') || value.contains(char::is_whitespace) {
        return Err("expected exactly one host:port".into());
    }
    let (host, port) = value
        .rsplit_once(':')
        .ok_or("expected host:port, for example 127.0.0.1:59042; the port is required")?;
    let bracketed = host.starts_with('[') && host.ends_with(']');
    if host.is_empty() || (host.contains(':') && !bracketed) {
        return Err("expected host:port; write an IPv6 host in brackets, [::1]:9042".into());
    }
    match port.parse::<u16>() {
        Ok(0) | Err(_) => Err(format!("{port:?} is not a port from 1 to 65535")),
        Ok(_) => Ok(value.to_owned()),
    }
}

/// Opens a session, with `keyspace` selected when given.
async fn connect(contact_point: &str, keyspace: Option<&str>) -> Result<TestSession, String> {
    let attempt = async {
        let config = NodeTcpConfigBuilder::new()
            .with_contact_point(contact_point.into())
            .build()
            .await
            .map_err(|error| error.to_string())?;
        let mut builder = TcpSessionBuilder::new(RoundRobinLoadBalancingStrategy::new(), config);
        if let Some(keyspace) = keyspace {
            builder = builder.with_keyspace(keyspace.to_owned());
        }
        builder.build().await.map_err(|error| error.to_string())
    };
    tokio::time::timeout(ATTEMPT_TIMEOUT, attempt)
        .await
        .map_err(|_| format!("no session within {ATTEMPT_TIMEOUT:?}"))?
}

/// Retries a session and `SELECT release_version FROM system.local` until both
/// succeed: the CQL port can be open before the node serves requests.
async fn wait_until_ready(contact_point: &str) -> Result<TestSession, String> {
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        let error = match connect(contact_point, None).await {
            Ok(session) => {
                let probe = tokio::time::timeout(
                    ATTEMPT_TIMEOUT,
                    session.query("SELECT release_version FROM system.local"),
                )
                .await;
                match probe {
                    Ok(Ok(_)) => return Ok(session),
                    Ok(Err(error)) => error.to_string(),
                    Err(_) => format!("no answer within {ATTEMPT_TIMEOUT:?}"),
                }
            }
            Err(error) => error,
        };
        if Instant::now() >= deadline {
            return Err(format!(
                "the node did not answer a SELECT within {READY_TIMEOUT:?}; last error: {error}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Applies `keyspace-single-node.cql`, then `schema.cql` with the keyspace
/// selected, as the CI job's two `cqlsh -f` calls do. Only for the harness's
/// own container.
async fn apply_schema(session: &TestSession, contact_point: &str) -> Result<(), String> {
    for statement in cql_statements(&read_schema_file("keyspace-single-node.cql")) {
        select(session, &statement).await?;
    }
    let keyspace_session = connect(contact_point, Some(KEYSPACE))
        .await
        .map_err(|error| format!("cannot select keyspace {KEYSPACE}: {error}"))?;
    for statement in cql_statements(&read_schema_file("schema.cql")) {
        select(&keyspace_session, &statement).await?;
    }
    Ok(())
}

/// Checks that keyspace `helios` holds exactly [`TABLES`].
async fn check_tables(session: &TestSession) -> Result<(), String> {
    let rows = select(
        session,
        &format!("SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{KEYSPACE}'"),
    )
    .await?;
    let mut found = rows
        .iter()
        .map(|row| text(row, "table_name"))
        .collect::<Result<Vec<_>, _>>()?;
    found.sort();
    if found == TABLES {
        Ok(())
    } else {
        Err(format!(
            "keyspace {KEYSPACE} holds the tables {found:?}, expected {TABLES:?}"
        ))
    }
}

/// Runs a statement and returns its rows (none for a statement without a
/// result set), within [`STATEMENT_TIMEOUT`].
pub async fn select(session: &TestSession, query: &str) -> Result<Vec<Row>, String> {
    let envelope: Envelope = tokio::time::timeout(STATEMENT_TIMEOUT, session.query(query))
        .await
        .map_err(|_| format!("{query}: no answer within {STATEMENT_TIMEOUT:?}"))?
        .map_err(|error| format!("{query}: {error}"))?;
    let body = envelope
        .response_body()
        .map_err(|error| format!("{query}: {error}"))?;
    Ok(body.into_rows().unwrap_or_default())
}

/// A non-null `text` column.
pub fn text(row: &Row, column: &str) -> Result<String, String> {
    row.get_r_by_name(column)
        .map_err(|error| format!("column {column}: {error}"))
}

/// A non-null `int` column.
pub fn int(row: &Row, column: &str) -> Result<i32, String> {
    row.get_r_by_name(column)
        .map_err(|error| format!("column {column}: {error}"))
}

/// Keyspace `helios` as recorded in `system_schema`, in the format of
/// `docs/Cassandra/schema/expected-schema.txt`: what the CI job's `q()`
/// function prints (`.github/workflows/fmt-clippy-cassandra.yml`). Each row's
/// fields are joined by single spaces, and each query's rows are sorted
/// bytewise, as `LC_ALL=C sort` does.
pub async fn schema_listing(session: &TestSession) -> Result<String, String> {
    let columns = select(
        session,
        &format!(
            "SELECT table_name, column_name, kind, position, clustering_order, type \
             FROM system_schema.columns WHERE keyspace_name = '{KEYSPACE}'"
        ),
    )
    .await?;
    let mut column_lines = columns
        .iter()
        .map(|row| {
            Ok(format!(
                "{} {} {} {} {} {}",
                text(row, "table_name")?,
                text(row, "column_name")?,
                text(row, "kind")?,
                int(row, "position")?,
                text(row, "clustering_order")?,
                text(row, "type")?,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    column_lines.sort();

    let tables = select(
        session,
        &format!(
            "SELECT table_name, gc_grace_seconds, default_time_to_live, \
             compaction['class'] AS compaction_class, \
             compaction['scaling_parameters'] AS scaling_parameters \
             FROM system_schema.tables WHERE keyspace_name = '{KEYSPACE}'"
        ),
    )
    .await?;
    let mut option_lines = tables
        .iter()
        .map(|row| {
            Ok(format!(
                "{} {} {} {} {}",
                text(row, "table_name")?,
                int(row, "gc_grace_seconds")?,
                int(row, "default_time_to_live")?,
                text(row, "compaction_class")?,
                text(row, "scaling_parameters")?,
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    option_lines.sort();

    let mut listing = String::from("# columns: table column kind position clustering_order type\n");
    for line in column_lines {
        listing.push_str(&line);
        listing.push('\n');
    }
    listing.push_str(
        "# options: table gc_grace_seconds default_time_to_live compaction_class scaling_parameters\n",
    );
    for line in option_lines {
        listing.push_str(&line);
        listing.push('\n');
    }
    Ok(listing)
}

/// Splits a CQL script into statements, as `cqlsh -f` reads it: `--` and
/// `//` comments run to the end of the line, `/* */` comments may span lines,
/// a `;` ends a statement, and none of these count inside a `'…'` string
/// (where `''` is a quote) or a `"…"` identifier. Statements come back
/// trimmed, without their `;`, empty ones dropped.
pub fn cql_statements(script: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = script.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\'' | '"' => {
                current.push(c);
                while let Some(inner) = chars.next() {
                    current.push(inner);
                    if inner == c {
                        if chars.peek() == Some(&c) {
                            current.push(c);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                }
            }
            '-' if chars.peek() == Some(&'-') => skip_line(&mut chars),
            '/' if chars.peek() == Some(&'/') => skip_line(&mut chars),
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut previous = '\0';
                for inner in chars.by_ref() {
                    if previous == '*' && inner == '/' {
                        break;
                    }
                    previous = inner;
                }
                current.push(' ');
            }
            ';' => {
                let statement = current.trim();
                if !statement.is_empty() {
                    statements.push(statement.to_owned());
                }
                current.clear();
            }
            _ => current.push(c),
        }
    }
    let rest = current.trim();
    if !rest.is_empty() {
        statements.push(rest.to_owned());
    }
    statements
}

fn skip_line(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) {
    for inner in chars.by_ref() {
        if inner == '\n' {
            break;
        }
    }
}

/// A path relative to the repository root. Read at run time, not with
/// `include_str!`, like `container_cleanup`'s copies check.
pub fn repo_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(relative)
}

/// A file of `docs/Cassandra/schema/`.
pub fn read_schema_file(name: &str) -> String {
    let path = repo_path(&format!("docs/Cassandra/schema/{name}"));
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
}

/// Tenant ids from [`unique_tenant_id`] start with this, so test data on a
/// shared node can be told apart from anything else stored there.
pub const TEST_TENANT_PREFIX: &str = "test-";

/// A tenant id no other test, and no earlier run on a reused node, has used:
/// `test-<label>-<32 hex digits of a random UUID>`, at most 64 bytes, and
/// valid for `TenantId::parse`. `label` (usually the test's name) is reduced
/// to ASCII letters, digits and `-`, and shortened to fit.
pub fn unique_tenant_id(label: &str) -> TenantId {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let room =
        helios_persistence::tenant::MAX_TENANT_ID_LEN - TEST_TENANT_PREFIX.len() - 1 - suffix.len();
    let id = format!("{TEST_TENANT_PREFIX}{}-{suffix}", label_part(label, room));
    TenantId::parse(&id).unwrap_or_else(|error| panic!("unique tenant id {id:?}: {error}"))
}

/// A fresh tenant with full access (Rule 2 of ADR-aacruzgon-0020): give every
/// test its own, so parallel tests and earlier runs never share rows.
pub fn unique_tenant(label: &str) -> TenantContext {
    TenantContext::new(unique_tenant_id(label), TenantPermissions::full_access())
}

/// The longest FHIR resource id (`resource.html#id`, R4: `[A-Za-z0-9\-\.]{1,64}`).
pub const MAX_FHIR_ID_LEN: usize = 64;

/// A resource id no other test has used: `<label>-<32 hex digits>`, at most
/// 64 characters from the FHIR id alphabet. For a test that must share a
/// tenant with others.
pub fn unique_id(label: &str) -> String {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let room = MAX_FHIR_ID_LEN - 1 - suffix.len();
    format!("{}-{suffix}", label_part(label, room))
}

/// `label` reduced to ASCII letters, digits and `-`, at most `room` bytes, and
/// `t` when nothing is left.
fn label_part(label: &str, room: usize) -> String {
    let cleaned: String = label
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .take(room)
        .collect();
    let cleaned = cleaned.trim_matches('-');
    if cleaned.is_empty() {
        "t".to_owned()
    } else {
        cleaned.to_owned()
    }
}
