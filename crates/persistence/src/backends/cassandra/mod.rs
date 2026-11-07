//! Apache Cassandra backend (`cassandra` feature).
//!
//! A standalone primary over the ten tables of
//! `docs/Cassandra/schema/schema.cql`, through the `cdrs-tokio` 10 driver. The
//! design is `docs/Cassandra/cassandra-discussion.md`; the order the work
//! lands in is `docs/Cassandra/development-roadmap.md`.
//!
//! # Status: roadmap Steps 1 to 3 done
//!
//! The backend implements every trait the server's bound requires (the twelve
//! of `helios_rest::create_app`), plus [`Backend`](crate::core::Backend) and
//! [`SearchCapabilityProvider`](crate::core::SearchCapabilityProvider). Real
//! so far: the connection, the health and schema checks, the capability
//! declaration, the per-tenant search-parameter registries, the shared write
//! engine every compare-and-swap goes through (`write.rs`,
//! ADR-aacruzgon-0050), and, through it, `create`, `update`, `delete`,
//! `delete_versioned` and `create_or_update`; `read`, `count`, `exists`,
//! `read_batch`, `vread`, `list_versions` and `delete_with_match`; and the
//! stored-SearchParameter overlay. `create_many` works through the trait
//! default's logic, on `create`. `update_with_match` (Step 4) and every
//! other storage, search and history method refuse with
//! [`BackendError::UnsupportedCapability`](crate::error::BackendError), which
//! the REST layer answers with 501. Each is replaced in its own file
//! (`storage.rs` lists them).
//!
//! What comes next, by roadmap step: Step 4 makes `VersionedStorage` whole
//! (`update_with_match`, `versioning_suite.rs`); Step 5 writes the six index
//! tables on every write and adds the sweeper, which finishes the intent rows
//! a write leaves behind; Step 6 adds search; Step 7 the CapabilityStatement
//! hooks; Step 8 the benchmarks and failure tests.
//!
//! [`CassandraBackend::cas_stats`] and [`CassandraBackend::statement_counts`]
//! count the engine's outcomes and the statements sent, for the CI reports.
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
mod delete;
mod errors;
mod index_keys;
mod read;
mod schema;
mod search;
mod session;
mod stats;
mod storage;
mod stubs;
mod update;
mod upsert;
mod versions;
mod write;

pub use backend::{CassandraBackend, CassandraConnection, StoredSearchParameterReload};
pub use config::{
    CassandraConfig, CassandraConfigError, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_CONTACT_POINT,
    DEFAULT_KEYSPACE, DEFAULT_LOCAL_DC, DEFAULT_POOL_SIZE, DEFAULT_REQUEST_TIMEOUT_MS,
};
pub use schema::TABLES;
pub use session::StatementCounts;
pub use stats::CasStats;
