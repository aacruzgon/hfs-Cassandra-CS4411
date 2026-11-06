//! [`CassandraBackend`]: construction, the search-parameter registries and
//! the [`Backend`] implementation.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::value::Value;
use parking_lot::RwLock;

use super::config::CassandraConfig;
use super::errors::{BACKEND_NAME, map_driver_error};
use super::schema;
use super::session::{CassandraSession, DriverError, StatementKind};
use crate::core::{Backend, BackendCapability, BackendKind};
use crate::error::{BackendError, StorageError, StorageResult};
use crate::search::{
    SearchParameterDefinition, SearchParameterLoader, SearchParameterRegistry,
    TenantSearchRegistries,
};

/// The cheap health probe: one row of the coordinator's own `system.local`.
const HEALTH_CHECK: &str = "SELECT release_version FROM system.local";

/// Each tenant's stored, `active` SearchParameters, read by the registry
/// loader. A map lookup cannot fail, so the loader always reports success.
type StoredByTenant = Arc<RwLock<HashMap<String, Vec<SearchParameterDefinition>>>>;

/// The Cassandra backend: a `cdrs-tokio` 8 session over the ten tables of
/// `docs/Cassandra/schema/schema.cql`.
///
/// Built with [`CassandraBackend::connect`]. It implements the twelve traits
/// the server needs (`helios_rest::create_app`'s bound), plus [`Backend`] and
/// [`SearchCapabilityProvider`](crate::core::SearchCapabilityProvider). Step 2
/// serves only what the design's Milestone A needs; every other method returns
/// [`BackendError::UnsupportedCapability`], which the REST layer answers with
/// 501.
pub struct CassandraBackend {
    pub(super) config: CassandraConfig,
    pub(super) session: Arc<CassandraSession>,
    /// Per-tenant search-parameter registries: a shared base plus each
    /// tenant's stored overlay.
    pub(super) registries: Arc<TenantSearchRegistries>,
    /// The overlay the registry loader reads.
    stored_by_tenant: StoredByTenant,
}

impl fmt::Debug for CassandraBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CassandraBackend")
            .field("config", &self.config)
            .field("session", &self.session)
            .field("base_registry_len", &self.registries.base().read().len())
            .finish()
    }
}

/// The handle [`Backend::acquire`] returns. The session pools and multiplexes
/// its own connections, so this is only a shared reference to it.
#[derive(Debug, Clone)]
pub struct CassandraConnection {
    _session: Arc<CassandraSession>,
}

impl CassandraBackend {
    /// Connects to the cluster `config` names.
    ///
    /// Opens the session (routing to `config.local_dc`, with `config.keyspace`
    /// as the session keyspace) and loads the base search-parameter registry
    /// for `config.fhir_version`. It does not check the schema; call
    /// [`Backend::initialize`] for that before serving.
    ///
    /// # Errors
    ///
    /// [`BackendError::Internal`] for an invalid configuration, and the
    /// mapped driver error (usually [`BackendError::ConnectionFailed`]) when
    /// no node accepts the session within `config.connect_timeout_ms`.
    pub async fn connect(config: CassandraConfig) -> StorageResult<Self> {
        config.validate().map_err(|error| {
            StorageError::Backend(BackendError::Internal {
                backend_name: BACKEND_NAME.to_string(),
                message: error.to_string(),
                source: None,
            })
        })?;

        let session = CassandraSession::connect(&config)
            .await
            .map_err(|error| StorageError::Backend(connect_error(&error, &config)))?;
        verify_local_dc(&session, &config.local_dc)
            .await
            .map_err(StorageError::Backend)?;

        let stored_by_tenant: StoredByTenant = Arc::new(RwLock::new(HashMap::new()));
        let loader_cache = stored_by_tenant.clone();
        let registries = Arc::new(TenantSearchRegistries::new(Arc::new(
            move |tenant_id: &str| {
                Some(
                    loader_cache
                        .read()
                        .get(tenant_id)
                        .cloned()
                        .unwrap_or_default(),
                )
            },
        )));
        initialize_search_registry(registries.base(), &config);

        tracing::info!(
            contact_points = ?config.contact_points,
            keyspace = %config.keyspace,
            local_dc = %config.local_dc,
            pool_size = config.pool_size,
            "Connected to Cassandra"
        );

        Ok(Self {
            config,
            session: Arc::new(session),
            registries,
            stored_by_tenant,
        })
    }

    /// The configuration this backend was built with.
    pub fn config(&self) -> &CassandraConfig {
        &self.config
    }

    /// The per-tenant registry container, for the server's operations that
    /// take it directly (`$reindex`, the console).
    pub fn tenant_registries(&self) -> &Arc<TenantSearchRegistries> {
        &self.registries
    }

    /// The capabilities every Cassandra backend has, whatever its
    /// configuration (design, "Structs and enums": `BackendCapability`).
    ///
    /// There is no `TokenSearch` or `UriSearch` variant; `BasicSearch` covers
    /// token and URI search. `SharedSchema` is the one placement: one keyspace,
    /// with the tenant in every partition key.
    pub fn declared_capabilities() -> Vec<BackendCapability> {
        vec![
            BackendCapability::Crud,
            BackendCapability::Versioning,
            BackendCapability::OptimisticLocking,
            BackendCapability::BasicSearch,
            BackendCapability::DateSearch,
            BackendCapability::CursorPagination,
            BackendCapability::SharedSchema,
        ]
    }

    /// The TTL refresh for the stored SearchParameter overlay
    /// (`HFS_SEARCH_PARAM_CACHE_TTL`, as `spawn_mongodb_search_param_refresh`
    /// drives MongoDB's): drops every cached per-tenant registry so the next
    /// access rebuilds it from the overlay. Returns how many stored
    /// definitions the overlay holds.
    ///
    /// Step 2 has no way to list a tenant's stored SearchParameters yet: that
    /// needs the `resources_by_type` rows Step 5 writes. Until then the
    /// overlay holds only the SearchParameters this process has stored itself,
    /// and this keeps them.
    pub async fn refresh_stored_search_parameters(&self) -> StorageResult<usize> {
        self.registries.invalidate_all();
        Ok(self.stored_by_tenant.read().values().map(Vec::len).sum())
    }

    /// Records a SearchParameter `tenant_id` just stored in the overlay, if it
    /// can change that tenant's registry, and only then drops the tenant's
    /// cached registry.
    ///
    /// `create_affects_overlay` skips anything that is not `active` and
    /// anything whose `url` the shared base already holds, so seeding the spec
    /// SearchParameters (R6 ships them `active`) does not rebuild the registry
    /// once per resource (#667).
    #[allow(dead_code)] // Called by the write path from Step 2's create on.
    pub(super) fn note_stored_search_parameter(
        &self,
        tenant_id: &str,
        resource: &serde_json::Value,
    ) {
        use crate::search::registry::{SearchParameterSource, SearchParameterStatus};

        if !self.registries.create_affects_overlay(resource) {
            return;
        }
        let loader = SearchParameterLoader::new(self.config.fhir_version);
        let Ok(mut definition) = loader.parse_resource(resource) else {
            return;
        };
        if definition.status != SearchParameterStatus::Active {
            return;
        }
        definition.source = SearchParameterSource::Stored;
        {
            let mut stored = self.stored_by_tenant.write();
            let defs = stored.entry(tenant_id.to_string()).or_default();
            defs.retain(|existing| existing.url != definition.url);
            defs.push(definition);
        }
        self.registries.invalidate(tenant_id);
    }

    /// Lists the tables of the configured keyspace and fails unless all ten
    /// are there.
    async fn check_schema(&self) -> Result<(), BackendError> {
        let rows = self
            .session
            .rows(
                schema::SELECT_TABLES,
                vec![Value::from(self.config.keyspace.as_str())],
                StatementKind::Read,
            )
            .await
            .map_err(|error| map_driver_error(&error, "schema check"))?;
        let mut found = Vec::with_capacity(rows.len());
        for row in &rows {
            let name: String =
                row.get_r_by_name("table_name")
                    .map_err(|error| BackendError::Internal {
                        backend_name: BACKEND_NAME.to_string(),
                        message: format!("schema check: unreadable table_name: {error}"),
                        source: None,
                    })?;
            found.push(name);
        }
        let missing = schema::missing_tables(found.iter().map(String::as_str));
        if missing.is_empty() {
            Ok(())
        } else {
            Err(BackendError::MigrationError {
                message: schema::missing_tables_message(&self.config.keyspace, &missing),
            })
        }
    }
}

/// The error for a session that could not be opened.
///
/// The driver sends `USE "<keyspace>"` on every new connection, and a keyspace
/// that does not exist makes it keep retrying until the connect deadline
/// (checked against the shared 5.0.9 node, 2026-11-06), so a deadline here is
/// a startup failure, not a statement timeout, and the keyspace is one of
/// its two likely causes.
fn connect_error(error: &DriverError, config: &CassandraConfig) -> BackendError {
    match error {
        DriverError::Deadline { after, .. } => {
            tracing::warn!(
                contact_points = ?config.contact_points,
                keyspace = %config.keyspace,
                "Cassandra session not open after {} ms",
                after.as_millis()
            );
            BackendError::ConnectionFailed {
                backend_name: BACKEND_NAME.to_string(),
                message: format!(
                    "no Cassandra session within {} ms: no contact point answered, or keyspace \
                     '{}' does not exist (apply docs/Cassandra/schema/keyspace-single-node.cql)",
                    after.as_millis(),
                    config.keyspace
                ),
            }
        }
        other => map_driver_error(other, "connect"),
    }
}

/// Fails unless some node of the cluster is in `local_dc`.
///
/// Every node outside `local_dc` is ignored (`session.rs`, `LocalDcOnly`), so
/// a misspelt `HFS_CASSANDRA_LOCAL_DC` would otherwise surface later as "no
/// Cassandra node could be reached" from every request. `system.local` and
/// `system.peers_v2` are node-local tables, so this reads what the
/// coordinator knows of the whole cluster.
async fn verify_local_dc(session: &CassandraSession, local_dc: &str) -> Result<(), BackendError> {
    let mut datacenters = std::collections::BTreeSet::new();
    for cql in [
        "SELECT data_center FROM system.local",
        "SELECT data_center FROM system.peers_v2",
    ] {
        let rows = match session.rows(cql, Vec::new(), StatementKind::Read).await {
            Ok(rows) => rows,
            Err(error) => {
                let mapped = map_driver_error(&error, "local data centre check");
                return Err(match mapped {
                    BackendError::ConnectionFailed { backend_name, .. } => {
                        BackendError::ConnectionFailed {
                            backend_name,
                            message: format!(
                                "no Cassandra node in data centre '{local_dc}' could be reached; \
                                 check HFS_CASSANDRA_LOCAL_DC"
                            ),
                        }
                    }
                    other => other,
                });
            }
        };
        for row in &rows {
            if let Ok(Some(dc)) = IntoRustByName::<String>::get_by_name(row, "data_center") {
                datacenters.insert(dc);
            }
        }
    }
    if datacenters.contains(local_dc) {
        Ok(())
    } else {
        Err(BackendError::Internal {
            backend_name: BACKEND_NAME.to_string(),
            message: format!(
                "HFS_CASSANDRA_LOCAL_DC is '{local_dc}', but the cluster's data centres are {:?}",
                datacenters
            ),
            source: None,
        })
    }
}

/// Fills the shared base registry: the embedded fallbacks, the spec bundle for
/// `config.fhir_version`, and any custom SearchParameters in the data
/// directory, the order every other backend uses.
fn initialize_search_registry(
    registry: &Arc<RwLock<SearchParameterRegistry>>,
    config: &CassandraConfig,
) {
    let loader = SearchParameterLoader::new(config.fhir_version);
    let mut reg = registry.write();

    let mut fallback_count = 0;
    match loader.load_embedded() {
        Ok(params) => {
            for param in params {
                if reg.register(param).is_ok() {
                    fallback_count += 1;
                }
            }
        }
        Err(e) => tracing::error!("Failed to load embedded SearchParameters: {e}"),
    }

    let data_dir = config
        .data_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from("./data"));
    let mut spec_count = 0;
    match loader.load_from_spec_file(&data_dir) {
        Ok(params) => {
            for param in params {
                if reg.register(param).is_ok() {
                    spec_count += 1;
                }
            }
        }
        Err(e) => tracing::warn!(
            "Could not load spec SearchParameters from {}: {e}. Using minimal fallback.",
            data_dir.join(loader.spec_filename()).display()
        ),
    }

    let mut custom_count = 0;
    match loader.load_custom_from_directory_with_files(&data_dir) {
        Ok((params, _files)) => {
            for param in params {
                if reg.register(param).is_ok() {
                    custom_count += 1;
                }
            }
        }
        Err(e) => tracing::warn!(
            "Error loading custom SearchParameters from {}: {e}",
            data_dir.display()
        ),
    }

    tracing::info!(
        total = reg.len(),
        spec = spec_count,
        fallback = fallback_count,
        custom = custom_count,
        resource_types = reg.resource_types().len(),
        fhir_version = %config.fhir_version.as_str(),
        "Cassandra SearchParameter registry initialized"
    );
}

#[async_trait]
impl Backend for CassandraBackend {
    type Connection = CassandraConnection;

    fn kind(&self) -> BackendKind {
        BackendKind::Cassandra
    }

    fn name(&self) -> &'static str {
        BACKEND_NAME
    }

    fn supports(&self, capability: BackendCapability) -> bool {
        Self::declared_capabilities().contains(&capability)
    }

    fn capabilities(&self) -> Vec<BackendCapability> {
        Self::declared_capabilities()
    }

    async fn acquire(&self) -> Result<Self::Connection, BackendError> {
        Ok(CassandraConnection {
            _session: self.session.clone(),
        })
    }

    async fn release(&self, _conn: Self::Connection) {}

    /// One read of `system.local` on a local-DC node, at `LOCAL_QUORUM` like
    /// every statement. It answers only when a node accepts a request, so
    /// `/_readiness` reports down while Cassandra is stopped.
    async fn health_check(&self) -> Result<(), BackendError> {
        self.session
            .rows(HEALTH_CHECK, Vec::new(), StatementKind::Read)
            .await
            .map(|_| ())
            .map_err(|error| map_driver_error(&error, "health check"))
    }

    /// Checks that the configured keyspace holds the ten tables. It never
    /// creates or alters anything (P6, ADR-aacruzgon-0027).
    async fn initialize(&self) -> Result<(), BackendError> {
        self.check_schema().await
    }

    /// There are no migrations: the schema is applied outside the backend.
    /// This runs the same check as [`initialize`](Backend::initialize).
    async fn migrate(&self) -> Result<(), BackendError> {
        self.check_schema().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declared_capabilities_are_exactly_the_designs() {
        let declared = CassandraBackend::declared_capabilities();
        assert_eq!(
            declared,
            vec![
                BackendCapability::Crud,
                BackendCapability::Versioning,
                BackendCapability::OptimisticLocking,
                BackendCapability::BasicSearch,
                BackendCapability::DateSearch,
                BackendCapability::CursorPagination,
                BackendCapability::SharedSchema,
            ]
        );
        for absent in [
            BackendCapability::Transactions,
            BackendCapability::InstanceHistory,
            BackendCapability::ConditionalCreate,
            BackendCapability::ConditionalUpdate,
            BackendCapability::ConditionalDelete,
            BackendCapability::OffsetPagination,
            BackendCapability::Sorting,
            BackendCapability::Include,
        ] {
            assert!(!declared.contains(&absent), "{absent:?}");
        }
    }

    #[test]
    fn base_registry_loads_for_the_configured_version() {
        let registry = Arc::new(RwLock::new(SearchParameterRegistry::new()));
        let config = CassandraConfig {
            data_dir: Some(PathBuf::from(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../data"
            ))),
            ..Default::default()
        };
        initialize_search_registry(&registry, &config);
        let reg = registry.read();
        assert!(!reg.get_active_params("Patient").is_empty());
        assert!(!reg.get_active_params("Observation").is_empty());
    }
}
