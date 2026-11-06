//! Apache Cassandra backend (`cassandra` feature).
//!
//! A standalone primary over the ten tables of
//! `docs/Cassandra/schema/schema.cql`, through the `cdrs-tokio` 8 driver. The
//! design is `docs/Cassandra/cassandra-discussion.md`; the order the work
//! lands in is `docs/Cassandra/development-roadmap.md`.
//!
//! # Status: roadmap Step 2
//!
//! The backend implements every trait the server's bound requires (the twelve
//! of `helios_rest::create_app`), plus [`Backend`](crate::core::Backend) and
//! [`SearchCapabilityProvider`](crate::core::SearchCapabilityProvider). In
//! this step the connection, the health and schema checks, the capability
//! declaration and the per-tenant search-parameter registries are real; every
//! storage, search and history method refuses with
//! [`BackendError::UnsupportedCapability`](crate::error::BackendError), which
//! the REST layer answers with 501. Later steps replace the stubs in their own
//! files.
//!
//! # What the backend assumes
//!
//! - The keyspace and tables already exist. The backend never applies the
//!   schema; [`Backend::initialize`](crate::core::Backend::initialize) only
//!   checks that the ten tables are there (P6, ADR-aacruzgon-0027).
//! - Every statement sets its consistency explicitly: `LOCAL_QUORUM` for
//!   reads and writes, `LOCAL_SERIAL` for the Paxos phase of a
//!   compare-and-swap. A compare-and-swap is never re-sent by the driver
//!   (ADR-aacruzgon-0024). See `session.rs`.
//! - Requests go to the replicas in one data centre, `local_dc` in
//!   [`CassandraConfig`], `helios-dc1` by default (ADR-aacruzgon-0003).
//!
//! # Example
//!
//! ```no_run
//! use helios_persistence::backends::cassandra::{CassandraBackend, CassandraConfig};
//! use helios_persistence::core::Backend;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = CassandraConfig::from_env()?;
//! let backend = CassandraBackend::connect(config).await?;
//! backend.initialize().await?; // fails unless the ten tables exist
//! # Ok(())
//! # }
//! ```

mod backend;
mod config;
mod errors;
mod schema;
mod search;
mod session;
mod storage;
mod stubs;

pub use backend::{CassandraBackend, CassandraConnection};
pub use config::{
    CassandraConfig, CassandraConfigError, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_CONTACT_POINT,
    DEFAULT_KEYSPACE, DEFAULT_LOCAL_DC, DEFAULT_POOL_SIZE, DEFAULT_REQUEST_TIMEOUT_MS,
};
pub use schema::TABLES;
