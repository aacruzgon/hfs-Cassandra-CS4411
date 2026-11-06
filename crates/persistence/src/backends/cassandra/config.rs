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
/// Default per-statement deadline, in milliseconds.
pub const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

/// Longest keyspace name Cassandra accepts.
const MAX_KEYSPACE_LEN: usize = 48;

/// Configuration for [`CassandraBackend`](super::CassandraBackend).
///
/// `Debug` is written by hand so that the password never reaches a log line.
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
            let has_port = point
                .rsplit_once(':')
                .is_some_and(|(host, port)| !host.is_empty() && port.parse::<u16>().is_ok());
            if !has_port {
                return Err(CassandraConfigError(format!(
                    "contact point '{point}' is not host:port"
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
        Ok(())
    }
}

fn parse_number<T: std::str::FromStr>(name: &str, raw: &str) -> Result<T, CassandraConfigError> {
    raw.trim()
        .parse::<T>()
        .map_err(|_| CassandraConfigError(format!("{name}='{raw}' is not a number")))
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
