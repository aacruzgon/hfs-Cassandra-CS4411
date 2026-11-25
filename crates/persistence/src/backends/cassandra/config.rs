//! Configuration for the Cassandra backend.
//!
//! [`CassandraConfig`] holds what the backend needs to open a `cdrs-tokio`
//! session: the contact points, the keyspace, the local data centre, optional
//! credentials, the pool size and two timeouts. The server's default FHIR
//! version and data directory are not read from the environment here; the
//! server sets them from its own configuration, as it does for MongoDB.
//!
//! # Environment variables
//!
//! [`CassandraConfig::from_env`] reads these. Every one is optional.
//!
//! | Variable | Default | Meaning |
//! | --- | --- | --- |
//! | `HFS_CASSANDRA_CONTACT_POINTS` | `127.0.0.1:9042` | Comma-separated `host:port` list. The local single node (`docker/cassandra`) listens on `127.0.0.1:59042`. |
//! | `HFS_CASSANDRA_KEYSPACE` | `helios` | Keyspace holding the ten tables of `docs/Cassandra/schema/schema.cql`. |
//! | `HFS_CASSANDRA_LOCAL_DC` | `helios-dc1` | Data centre whose nodes serve every request (`LOCAL_QUORUM`, `LOCAL_SERIAL`). |
//! | `HFS_CASSANDRA_USERNAME`, `HFS_CASSANDRA_PASSWORD` | unset | `PasswordAuthenticator` credentials; set both or neither. |
//! | `HFS_CASSANDRA_POOL_SIZE` | `2` | Connections per local node. Each connection multiplexes many requests. |
//! | `HFS_CASSANDRA_CONNECT_TIMEOUT_MS` | `5000` | Bound on opening the session and each connection. |
//! | `HFS_CASSANDRA_REQUEST_TIMEOUT_MS` | `10000` | The backend's own deadline for one statement. Kept above the node's `read_request_timeout` (5 s), `write_request_timeout` (2 s) and `cas_contention_timeout` (1 s), so the node reports a typed timeout first. |
//! | `HFS_CASSANDRA_SWEEPER_ENABLED` | `true` | Whether hfs runs the index sweeper (one per process). |
//! | `HFS_CASSANDRA_SWEEPER_POLL_INTERVAL_MS` | `5000` | Pause between two polls of pending_index_writes. |
//! | `HFS_CASSANDRA_SWEEPER_GRACE_MS` | `30000` | How old an intent must be before the sweeper takes it: at least twice the request timeout, under one hour, and not under the poll interval. |

use std::fmt;
use std::path::PathBuf;

use helios_fhir::FhirVersion;
use serde::{Deserialize, Serialize};

/// Default contact point: Cassandra's standard native port on loopback.
pub const DEFAULT_CONTACT_POINT: &str = "127.0.0.1:9042";
/// Default keyspace, as `docs/Cassandra/schema/keyspace-single-node.cql` creates it.
pub const DEFAULT_KEYSPACE: &str = "helios";
/// Default local data centre (ADR-aacruzgon-0003).
pub const DEFAULT_LOCAL_DC: &str = "helios-dc1";
/// Default connections per local node.
pub const DEFAULT_POOL_SIZE: usize = 2;
/// Default connect timeout, in milliseconds.
pub const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 5_000;
/// Default per-statement deadline, in milliseconds. Kept above the node's
/// own request timeouts (`read_request_timeout` 5 s, `write_request_timeout`
/// 2 s, `cas_contention_timeout` 1 s, `range_request_timeout` 10 s by
/// default; `crates/persistence/README.md`, "Node settings HFS depends on").
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;
/// Default `HFS_CASSANDRA_SWEEPER_POLL_INTERVAL_MS`.
pub const DEFAULT_SWEEPER_POLL_INTERVAL_MS: u64 = 5_000;
/// Default `HFS_CASSANDRA_SWEEPER_GRACE_MS`.
pub const DEFAULT_SWEEPER_GRACE_MS: u64 = 30_000;

/// Longest keyspace name Cassandra accepts.
const MAX_KEYSPACE_LEN: usize = 48;

/// Configuration for [`CassandraBackend`](super::CassandraBackend).
///
/// `Debug` is written by hand so that the password never reaches a log line.
///
/// **Safe to log:** `contact_points`, `keyspace`, `local_dc`, `username`,
/// `pool_size` and the timeouts. Credentials live only in `username` and
/// `password`: [`validate`](Self::validate) refuses a contact point that
/// carries a user or password (`user:pass@host:9042`), and the keyspace and
/// data-centre names cannot hold one. Never log `password`; it is also skipped
/// when the config is serialized.
#[derive(Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CassandraConfig {
    /// Contact points, each `host:port`. The driver discovers the other nodes
    /// from them.
    pub contact_points: Vec<String>,
    /// Keyspace holding the ten tables. The backend never creates it.
    pub keyspace: String,
    /// Data centre local to this process. Only its nodes serve requests.
    pub local_dc: String,
    /// `PasswordAuthenticator` user name; `None` for a node without
    /// authentication.
    pub username: Option<String>,
    /// `PasswordAuthenticator` password; set together with `username`.
    #[serde(skip_serializing)]
    pub password: Option<String>,
    /// Connections per local node.
    pub pool_size: usize,
    /// Bound on opening the session and each pooled connection, in
    /// milliseconds.
    pub connect_timeout_ms: u64,
    /// The backend's own deadline for one statement, in milliseconds.
    pub request_timeout_ms: u64,
    /// Whether `hfs` runs the index sweeper (ADR-sam-michaels-0006, SW1, SW4). The backend never starts it itself.
    pub sweeper_enabled: bool,
    /// Pause between two polls of the outbox, in milliseconds.
    pub sweeper_poll_interval_ms: u64,
    /// How old an intent must be before the sweeper takes it, in milliseconds.
    pub sweeper_grace_ms: u64,
    /// The server's default FHIR version. It decides which search-parameter
    /// bundle seeds the base registry; every stored resource still carries its
    /// own version.
    pub fhir_version: FhirVersion,
    /// Directory holding the search-parameter spec bundles and custom
    /// SearchParameters (`./data` when `None`).
    pub data_dir: Option<PathBuf>,
}

impl Default for CassandraConfig {
    fn default() -> Self {
        Self {
            contact_points: vec![DEFAULT_CONTACT_POINT.to_string()],
            keyspace: DEFAULT_KEYSPACE.to_string(),
            local_dc: DEFAULT_LOCAL_DC.to_string(),
            username: None,
            password: None,
            pool_size: DEFAULT_POOL_SIZE,
            connect_timeout_ms: DEFAULT_CONNECT_TIMEOUT_MS,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            sweeper_enabled: true,
            sweeper_poll_interval_ms: DEFAULT_SWEEPER_POLL_INTERVAL_MS,
            sweeper_grace_ms: DEFAULT_SWEEPER_GRACE_MS,
            fhir_version: FhirVersion::default_enabled(),
            data_dir: None,
        }
    }
}

impl fmt::Debug for CassandraConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CassandraConfig")
            .field("contact_points", &self.contact_points)
            .field("keyspace", &self.keyspace)
            .field("local_dc", &self.local_dc)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("pool_size", &self.pool_size)
            .field("connect_timeout_ms", &self.connect_timeout_ms)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("sweeper_enabled", &self.sweeper_enabled)
            .field("sweeper_poll_interval_ms", &self.sweeper_poll_interval_ms)
            .field("sweeper_grace_ms", &self.sweeper_grace_ms)
            .field("fhir_version", &self.fhir_version)
            .field("data_dir", &self.data_dir)
            .finish()
    }
}

/// A configuration value that cannot be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid Cassandra configuration: {0}")]
pub struct CassandraConfigError(pub String);

impl CassandraConfig {
    /// Reads the `HFS_CASSANDRA_*` variables (see the module docs) over the
    /// defaults.
    pub fn from_env() -> Result<Self, CassandraConfigError> {
        Self::from_env_with(|name| std::env::var(name).ok())
    }

    /// [`from_env`](Self::from_env) with an injectable lookup, for tests and
    /// for callers that read their environment some other way.
    pub fn from_env_with<F>(env: F) -> Result<Self, CassandraConfigError>
    where
        F: Fn(&str) -> Option<String>,
    {
        let mut config = Self::default();
        let present = |name: &str| env(name).filter(|value| !value.trim().is_empty());

        if let Some(raw) = present("HFS_CASSANDRA_CONTACT_POINTS") {
            config.contact_points = raw
                .split(',')
                .map(str::trim)
                .filter(|point| !point.is_empty())
                .map(str::to_string)
                .collect();
        }
        if let Some(keyspace) = present("HFS_CASSANDRA_KEYSPACE") {
            config.keyspace = keyspace.trim().to_string();
        }
        if let Some(local_dc) = present("HFS_CASSANDRA_LOCAL_DC") {
            config.local_dc = local_dc.trim().to_string();
        }
        config.username = present("HFS_CASSANDRA_USERNAME");
        config.password = env("HFS_CASSANDRA_PASSWORD").filter(|value| !value.is_empty());
        if let Some(raw) = present("HFS_CASSANDRA_POOL_SIZE") {
            config.pool_size = parse_number("HFS_CASSANDRA_POOL_SIZE", &raw)?;
        }
        if let Some(raw) = present("HFS_CASSANDRA_CONNECT_TIMEOUT_MS") {
            config.connect_timeout_ms = parse_number("HFS_CASSANDRA_CONNECT_TIMEOUT_MS", &raw)?;
        }
        if let Some(raw) = present("HFS_CASSANDRA_REQUEST_TIMEOUT_MS") {
            config.request_timeout_ms = parse_number("HFS_CASSANDRA_REQUEST_TIMEOUT_MS", &raw)?;
        }
        if let Some(raw) = present("HFS_CASSANDRA_SWEEPER_ENABLED") {
            config.sweeper_enabled = parse_bool("HFS_CASSANDRA_SWEEPER_ENABLED", &raw)?;
        }
        if let Some(raw) = present("HFS_CASSANDRA_SWEEPER_POLL_INTERVAL_MS") {
            config.sweeper_poll_interval_ms =
                parse_number("HFS_CASSANDRA_SWEEPER_POLL_INTERVAL_MS", &raw)?;
        }
        if let Some(raw) = present("HFS_CASSANDRA_SWEEPER_GRACE_MS") {
            config.sweeper_grace_ms = parse_number("HFS_CASSANDRA_SWEEPER_GRACE_MS", &raw)?;
        }

        config.validate()?;
        Ok(config)
    }

    /// Checks the values [`connect`](super::CassandraBackend::connect) relies
    /// on.
    ///
    /// The keyspace name is checked because the driver sends it in a `USE`
    /// statement, which cannot take a bind marker.
    pub fn validate(&self) -> Result<(), CassandraConfigError> {
        if self.contact_points.is_empty() {
            return Err(CassandraConfigError(
                "HFS_CASSANDRA_CONTACT_POINTS names no contact point".to_string(),
            ));
        }
        for point in &self.contact_points {
            // A `user:password@host` form would put a secret in a field the
            // server logs; refuse it without echoing the value.
            if point.contains('@') || point.contains('/') {
                return Err(CassandraConfigError(
                    "a contact point must be host:port, with no user, password, scheme or path; \
                     set HFS_CASSANDRA_USERNAME and HFS_CASSANDRA_PASSWORD for credentials"
                        .to_string(),
                ));
            }
            if let Err(reason) = check_host_port(point) {
                return Err(CassandraConfigError(format!(
                    "contact point '{point}' is not host:port: {reason}"
                )));
            }
        }
        if !is_valid_keyspace_name(&self.keyspace) {
            return Err(CassandraConfigError(format!(
                "keyspace '{}' is not 1 to {MAX_KEYSPACE_LEN} ASCII letters, digits or '_'",
                self.keyspace
            )));
        }
        if self.local_dc.trim().is_empty() {
            return Err(CassandraConfigError(
                "HFS_CASSANDRA_LOCAL_DC is empty".to_string(),
            ));
        }
        if self.username.is_some() != self.password.is_some() {
            return Err(CassandraConfigError(
                "set both HFS_CASSANDRA_USERNAME and HFS_CASSANDRA_PASSWORD, or neither"
                    .to_string(),
            ));
        }
        if self.pool_size == 0 {
            return Err(CassandraConfigError(
                "HFS_CASSANDRA_POOL_SIZE must be at least 1".to_string(),
            ));
        }
        if self.connect_timeout_ms == 0 || self.request_timeout_ms == 0 {
            return Err(CassandraConfigError(
                "Cassandra timeouts must be at least 1 ms".to_string(),
            ));
        }
        // SW4, RW11: checked whether or not the sweeper is enabled.
        if self.sweeper_poll_interval_ms == 0 {
            return Err(CassandraConfigError(
                "HFS_CASSANDRA_SWEEPER_POLL_INTERVAL_MS must be at least 1".to_string(),
            ));
        }
        if self.sweeper_grace_ms < self.request_timeout_ms.saturating_mul(2) {
            return Err(CassandraConfigError(format!(
                "HFS_CASSANDRA_SWEEPER_GRACE_MS ({}) must be at least twice \
                 HFS_CASSANDRA_REQUEST_TIMEOUT_MS ({})",
                self.sweeper_grace_ms, self.request_timeout_ms
            )));
        }
        if self.sweeper_grace_ms >= 3_600_000 {
            return Err(CassandraConfigError(format!(
                "HFS_CASSANDRA_SWEEPER_GRACE_MS ({}) must be under one hour",
                self.sweeper_grace_ms
            )));
        }
        if self.sweeper_grace_ms < self.sweeper_poll_interval_ms {
            return Err(CassandraConfigError(format!(
                "HFS_CASSANDRA_SWEEPER_GRACE_MS ({}) must not be shorter than \
                 HFS_CASSANDRA_SWEEPER_POLL_INTERVAL_MS ({})",
                self.sweeper_grace_ms, self.sweeper_poll_interval_ms
            )));
        }
        Ok(())
    }

    /// The pause between two polls of the outbox.
    pub(crate) fn sweeper_poll_interval(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.sweeper_poll_interval_ms)
    }

    /// How old an intent must be before the sweeper takes it.
    pub(crate) fn sweeper_grace(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.sweeper_grace_ms)
    }
}

fn parse_bool(name: &str, raw: &str) -> Result<bool, CassandraConfigError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(CassandraConfigError(format!(
            "{name}='{raw}' is not true or false"
        ))),
    }
}

fn parse_number<T: std::str::FromStr>(name: &str, raw: &str) -> Result<T, CassandraConfigError> {
    raw.trim()
        .parse::<T>()
        .map_err(|_| CassandraConfigError(format!("{name}='{raw}' is not a number")))
}

/// Checks one contact point in the form `HFS_TEST_CASSANDRA_URL` takes
/// (`tests/common/cassandra.rs`, `parse_url`): `host:port` with no scheme, a
/// port from 1 to 65535, and an IPv6 host in brackets (`[::1]:9042`). The
/// caller has already split the list on commas and trimmed each entry.
fn check_host_port(point: &str) -> Result<(), &'static str> {
    if point.contains(char::is_whitespace) {
        return Err("it contains whitespace");
    }
    let Some((host, port)) = point.rsplit_once(':') else {
        return Err("the port is required, for example 127.0.0.1:9042");
    };
    let bracketed = host.len() > 2 && host.starts_with('[') && host.ends_with(']');
    let has_bracket = host.contains('[') || host.contains(']');
    if host.is_empty() || ((host.contains(':') || has_bracket) && !bracketed) {
        return Err("write an IPv6 host in brackets, for example [::1]:9042");
    }
    match port.parse::<u16>() {
        Ok(0) | Err(_) => Err("the port must be a number from 1 to 65535"),
        Ok(_) => Ok(()),
    }
}

/// Cassandra's rule for an unquoted keyspace name.
fn is_valid_keyspace_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_KEYSPACE_LEN
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn defaults_match_the_local_node_and_the_design() {
        let config = CassandraConfig::from_env_with(env_of(&[])).unwrap();
        assert_eq!(config.contact_points, vec!["127.0.0.1:9042".to_string()]);
        assert_eq!(config.keyspace, "helios");
        assert_eq!(config.local_dc, "helios-dc1");
        assert_eq!(config.username, None);
        assert_eq!(config.password, None);
        assert_eq!(config.pool_size, 2);
        assert_eq!(config.connect_timeout_ms, 5_000);
        assert_eq!(config.request_timeout_ms, 10_000);
        assert_eq!(config.fhir_version, FhirVersion::default_enabled());
    }

    /// SW4, RW11: the sweeper's three settings default, parse and validate.
    #[test]
    fn sweeper_settings_default_parse_and_validate() {
        let triple = |c: &CassandraConfig| {
            (
                c.sweeper_enabled,
                c.sweeper_poll_interval_ms,
                c.sweeper_grace_ms,
            )
        };
        assert_eq!(triple(&CassandraConfig::default()), (true, 5_000, 30_000));
        let config = CassandraConfig::from_env_with(env_of(&[])).unwrap();
        assert_eq!(triple(&config), (true, 5_000, 30_000));
        let config = CassandraConfig::from_env_with(env_of(&[
            ("HFS_CASSANDRA_SWEEPER_ENABLED", "FALSE"),
            ("HFS_CASSANDRA_SWEEPER_POLL_INTERVAL_MS", "1000"),
            ("HFS_CASSANDRA_SWEEPER_GRACE_MS", "25000"),
        ]))
        .unwrap();
        assert_eq!(triple(&config), (false, 1_000, 25_000));
        for (raw, expected) in [("0", false), ("1", true)] {
            let config =
                CassandraConfig::from_env_with(env_of(&[("HFS_CASSANDRA_SWEEPER_ENABLED", raw)]))
                    .unwrap();
            assert_eq!(config.sweeper_enabled, expected, "{raw}");
        }
        let error =
            CassandraConfig::from_env_with(env_of(&[("HFS_CASSANDRA_SWEEPER_ENABLED", "maybe")]))
                .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("HFS_CASSANDRA_SWEEPER_ENABLED='maybe' is not true or false"),
            "{error}"
        );

        let with = |poll: u64, grace: u64, request: u64| CassandraConfig {
            sweeper_poll_interval_ms: poll,
            sweeper_grace_ms: grace,
            request_timeout_ms: request,
            ..CassandraConfig::default()
        };
        for (config, name) in [
            (
                with(0, 30_000, 10_000),
                "HFS_CASSANDRA_SWEEPER_POLL_INTERVAL_MS",
            ),
            (
                with(5_000, 19_999, 10_000),
                "HFS_CASSANDRA_SWEEPER_GRACE_MS",
            ),
            (
                with(5_000, 3_600_000, 10_000),
                "HFS_CASSANDRA_SWEEPER_GRACE_MS",
            ),
            (with(5_000, 4_000, 2_000), "HFS_CASSANDRA_SWEEPER_GRACE_MS"),
        ] {
            let error = config.validate().unwrap_err();
            assert!(error.to_string().contains(name), "{error}");
        }
        // A request timeout of u64::MAX must not overflow the twice-the-timeout check.
        assert!(with(5_000, 20_000, u64::MAX).validate().is_err());
        assert!(with(5_000, 20_000, 10_000).validate().is_ok());
        assert!(with(5_000, 3_599_999, 10_000).validate().is_ok());
    }

    #[test]
    fn reads_every_variable() {
        let config = CassandraConfig::from_env_with(env_of(&[
            (
                "HFS_CASSANDRA_CONTACT_POINTS",
                " 10.0.0.1:9042, node2:9043 ,",
            ),
            ("HFS_CASSANDRA_KEYSPACE", "helios_test"),
            ("HFS_CASSANDRA_LOCAL_DC", "dc-east"),
            ("HFS_CASSANDRA_USERNAME", "helios"),
            ("HFS_CASSANDRA_PASSWORD", "secret"),
            ("HFS_CASSANDRA_POOL_SIZE", "4"),
            ("HFS_CASSANDRA_CONNECT_TIMEOUT_MS", "1500"),
            ("HFS_CASSANDRA_REQUEST_TIMEOUT_MS", "8000"),
        ]))
        .unwrap();
        assert_eq!(
            config.contact_points,
            vec!["10.0.0.1:9042".to_string(), "node2:9043".to_string()]
        );
        assert_eq!(config.keyspace, "helios_test");
        assert_eq!(config.local_dc, "dc-east");
        assert_eq!(config.username.as_deref(), Some("helios"));
        assert_eq!(config.password.as_deref(), Some("secret"));
        assert_eq!(config.pool_size, 4);
        assert_eq!(config.connect_timeout_ms, 1_500);
        assert_eq!(config.request_timeout_ms, 8_000);
    }

    #[test]
    fn rejects_bad_values() {
        for (name, value) in [
            ("HFS_CASSANDRA_CONTACT_POINTS", "localhost"),
            ("HFS_CASSANDRA_CONTACT_POINTS", ":9042"),
            ("HFS_CASSANDRA_CONTACT_POINTS", ","),
            (
                "HFS_CASSANDRA_CONTACT_POINTS",
                "helios:secret@127.0.0.1:9042",
            ),
            ("HFS_CASSANDRA_CONTACT_POINTS", "cassandra://127.0.0.1:9042"),
            ("HFS_CASSANDRA_CONTACT_POINTS", "127.0.0.1"),
            ("HFS_CASSANDRA_CONTACT_POINTS", "127.0.0.1:"),
            ("HFS_CASSANDRA_CONTACT_POINTS", "127.0.0.1:0"),
            ("HFS_CASSANDRA_CONTACT_POINTS", "127.0.0.1:65536"),
            ("HFS_CASSANDRA_CONTACT_POINTS", "127.0.0.1:port"),
            ("HFS_CASSANDRA_CONTACT_POINTS", "::1:9042"),
            ("HFS_CASSANDRA_CONTACT_POINTS", "[]:9042"),
            ("HFS_CASSANDRA_CONTACT_POINTS", "a b:9042"),
            ("HFS_CASSANDRA_KEYSPACE", "helios; DROP KEYSPACE helios"),
            ("HFS_CASSANDRA_KEYSPACE", "a-b"),
            ("HFS_CASSANDRA_POOL_SIZE", "0"),
            ("HFS_CASSANDRA_POOL_SIZE", "many"),
            ("HFS_CASSANDRA_REQUEST_TIMEOUT_MS", "0"),
            ("HFS_CASSANDRA_USERNAME", "helios"),
        ] {
            assert!(
                CassandraConfig::from_env_with(env_of(&[(name, value)])).is_err(),
                "{name}={value} was accepted"
            );
        }
    }

    /// Each entry takes `HFS_TEST_CASSANDRA_URL`'s form, so the test URL can be
    /// used as the contact point unchanged.
    #[test]
    fn each_contact_point_takes_the_test_url_form() {
        let config = CassandraConfig::from_env_with(env_of(&[(
            "HFS_CASSANDRA_CONTACT_POINTS",
            " 127.0.0.1:59042 , cassandra:9042,[::1]:9042 ",
        )]))
        .unwrap();
        assert_eq!(
            config.contact_points,
            ["127.0.0.1:59042", "cassandra:9042", "[::1]:9042"]
        );
    }

    #[test]
    fn a_contact_point_with_credentials_is_refused_without_echoing_them() {
        let error = CassandraConfig::from_env_with(env_of(&[(
            "HFS_CASSANDRA_CONTACT_POINTS",
            "helios:hunter2@127.0.0.1:9042",
        )]))
        .unwrap_err();
        assert!(!error.to_string().contains("hunter2"), "{error}");
    }

    #[test]
    fn debug_redacts_the_password() {
        let config = CassandraConfig {
            username: Some("helios".to_string()),
            password: Some("hunter2".to_string()),
            ..Default::default()
        };
        let debug = format!("{config:?}");
        assert!(!debug.contains("hunter2"), "{debug}");
        assert!(debug.contains("<redacted>"), "{debug}");
    }
}
