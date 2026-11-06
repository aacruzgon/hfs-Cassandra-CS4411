//! [`CassandraBackend`]: construction, the search-parameter registries and
//! the [`Backend`] implementation.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use cdrs_tokio::types::IntoRustByName;
use cdrs_tokio::types::value::Value;
use futures::stream::{self, StreamExt};
use parking_lot::RwLock;

use super::config::CassandraConfig;
use super::errors::{BACKEND_NAME, map_driver_error};
use super::schema;
use super::session::{CassandraSession, DriverError, StatementKind};
use crate::core::{Backend, BackendCapability, BackendKind};
use crate::error::{BackendError, ResourceError, StorageError, StorageResult};
use crate::search::{
    SearchParameterDefinition, SearchParameterLoader, SearchParameterRegistry,
    TenantSearchRegistries,
};

/// The cheap health probe: one row of the coordinator's own `system.local`.
const HEALTH_CHECK: &str = "SELECT release_version FROM system.local";

/// Every SearchParameter id a tenant has stored, from its `resources_by_type`
/// partition, which `create` writes in Step 2 (write.rs, X1).
const SELECT_STORED_SEARCH_PARAMETER_IDS: &str =
    "SELECT id FROM resources_by_type WHERE tenant_id = ? AND resource_type = ?";

/// Every partition key of `resources_by_type`, so every tenant that has
/// stored a resource of a type. `DISTINCT` on the whole partition key is a
/// key-range read that needs no `ALLOW FILTERING` (Cassandra 5.0
/// `SelectStatement.java`, `validateDistinctSelection`: a key range must
/// select every partition key column) and reads one row per partition
/// (`DataLimits.DISTINCT_NONE`). The `resource_type` filter is applied here,
/// because a key range cannot restrict half of a composite partition key.
const SELECT_TENANT_TYPES: &str = "SELECT DISTINCT tenant_id, resource_type FROM resources_by_type";

/// How many SearchParameter reads a reload runs at once.
const STORED_SEARCH_PARAMETER_READS: usize = 16;

/// Which stored SearchParameter is in force when two of a tenant's carry the
/// same `url`: the newest `last_updated`, then the higher `id`. Two stored
/// resources cannot share both, so the choice never depends on the order the
/// reads finish in.
type Precedence = (chrono::DateTime<chrono::Utc>, String);

/// One stored, `active` SearchParameter in the overlay.
#[derive(Clone)]
struct StoredDefinition {
    definition: SearchParameterDefinition,
    /// The stored resource's `last_updated` and `id` ([`Precedence`]).
    precedence: Precedence,
    /// The [`CassandraBackend::note_seq`] value when this process stored it,
    /// or 0 when a reload read it. A reload keeps the definitions noted after
    /// it began (see [`CassandraBackend::load_stored_search_parameters`]).
    noted_at: u64,
}

/// Each tenant's stored, `active` SearchParameters, read by the registry
/// loader. A map lookup cannot fail, so the loader always reports success.
type StoredByTenant = Arc<RwLock<HashMap<String, Vec<StoredDefinition>>>>;

/// What [`CassandraBackend::load_all_stored_search_parameters`] loaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredSearchParameterReload {
    /// Tenants reloaded: those with a stored SearchParameter, plus those
    /// whose overlay held a definition.
    pub tenants: usize,
    /// Stored, `active`, non-spec definitions the overlay now holds across
    /// them.
    pub definitions: usize,
    /// Tenants whose reload failed and who keep their previous overlay. Only
    /// a refresh continues past a failure; the startup load stops at the
    /// first one, so it is always 0 there.
    pub failed: usize,
}

/// Puts `candidate` into `defs`, unless a definition with the same `url` and
/// a higher [`Precedence`] is already there, in which case `defs` is left as
/// it was. Returns whether `candidate` went in.
fn insert_by_precedence(defs: &mut Vec<StoredDefinition>, candidate: StoredDefinition) -> bool {
    match defs
        .iter()
        .position(|existing| existing.definition.url == candidate.definition.url)
    {
        Some(at) if defs[at].precedence > candidate.precedence => false,
        Some(at) => {
            defs[at] = candidate;
            true
        }
        None => {
            defs.push(candidate);
            true
        }
    }
}

/// Reloads each of `tenants` with `load`, summing what it returns. With
/// `fail_fast` the first error is returned; otherwise each failure is logged
/// with its tenant, counted in [`StoredSearchParameterReload::failed`], and
/// the next tenant is loaded.
async fn reload_each<F, Fut>(
    tenants: &BTreeSet<String>,
    fail_fast: bool,
    mut load: F,
) -> StorageResult<StoredSearchParameterReload>
where
    F: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = StorageResult<usize>>,
{
    let mut reload = StoredSearchParameterReload {
        tenants: tenants.len(),
        definitions: 0,
        failed: 0,
    };
    for tenant_id in tenants {
        match load(tenant_id.clone()).await {
            Ok(count) => reload.definitions += count,
            Err(error) if fail_fast => return Err(error),
            Err(error) => {
                reload.failed += 1;
                tracing::warn!(
                    tenant_id = %tenant_id,
                    "stored SearchParameter reload failed for this tenant; it keeps its \
                     previous overlay: {error}"
                );
            }
        }
    }
    Ok(reload)
}

/// The Cassandra backend: a `cdrs-tokio` 10 session over the ten tables of
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
    /// Counts the SearchParameters this process has noted in the overlay
    /// ([`StoredDefinition::noted_at`]).
    note_seq: AtomicU64,
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
                        .map(|defs| defs.iter().map(|d| d.definition.clone()).collect())
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
            note_seq: AtomicU64::new(0),
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
    /// drives MongoDB's): the reload of
    /// [`Self::load_all_stored_search_parameters`], except that **a tenant
    /// whose reload fails does not stop the others**. Its failure is logged
    /// with the tenant, it keeps its previous overlay, and the tenants after
    /// it are still reloaded; a summary warning gives the count. Returns how
    /// many stored definitions the reloaded tenants hold.
    ///
    /// # Errors
    ///
    /// The tenant scan's error. Without it there is no list to continue
    /// through, so every overlay stays as it was.
    pub async fn refresh_stored_search_parameters(&self) -> StorageResult<usize> {
        let reload = self.reload_stored_search_parameters(false).await?;
        if reload.failed > 0 {
            tracing::warn!(
                failed = reload.failed,
                tenants = reload.tenants,
                "SearchParameter refresh: some tenants kept their previous overlay"
            );
        }
        Ok(reload.definitions)
    }

    /// Reloads the overlay of every tenant that has stored a SearchParameter,
    /// and of every tenant whose overlay holds a definition (so one whose
    /// SearchParameters are all gone is cleared), with
    /// [`Self::load_stored_search_parameters`]. The server runs it before it
    /// serves, so a tenant's stored SearchParameters are in its registry from
    /// its first request after a restart, whatever the tenant and even with
    /// the refresh off (TTL 0). The TTL refresh runs the same reload
    /// ([`Self::refresh_stored_search_parameters`]).
    ///
    /// Cassandra keeps no table of tenants (design, "Tenant registry"), so
    /// the tenants come from [`Self::stored_search_parameter_tenants`], a
    /// paged scan of `resources_by_type`'s partition keys.
    ///
    /// Then it drops every cached per-tenant registry.
    ///
    /// # Errors
    ///
    /// The first failed scan or reload: **the startup load is fail-fast**, as
    /// PostgreSQL's and MongoDB's `initialize` are. The tenants already
    /// reloaded keep their new overlay, the others their previous one.
    pub async fn load_all_stored_search_parameters(
        &self,
    ) -> StorageResult<StoredSearchParameterReload> {
        self.reload_stored_search_parameters(true).await
    }

    /// The reload behind [`Self::load_all_stored_search_parameters`]
    /// (`fail_fast`) and [`Self::refresh_stored_search_parameters`].
    async fn reload_stored_search_parameters(
        &self,
        fail_fast: bool,
    ) -> StorageResult<StoredSearchParameterReload> {
        let mut tenants = self.stored_search_parameter_tenants().await?;
        tenants.extend(self.stored_by_tenant.read().keys().cloned());
        let reload = reload_each(&tenants, fail_fast, |tenant_id| async move {
            self.load_stored_search_parameters(&tenant_id).await
        })
        .await;
        // Every cached registry is rebuilt from the new overlay on its next
        // use, also a tenant's that has no stored definitions, and one cached
        // from an older overlay while this ran (`TenantSearchRegistries::
        // for_tenant` builds outside its lock).
        self.registries.invalidate_all();
        reload
    }

    /// Every tenant with a `SearchParameter` partition in `resources_by_type`,
    /// from a paged scan of the table's partition keys (one row per tenant
    /// and type that has a resource).
    ///
    /// # Errors
    ///
    /// The mapped driver error of any page.
    pub async fn stored_search_parameter_tenants(&self) -> StorageResult<BTreeSet<String>> {
        let rows = self
            .session
            .rows_paged(SELECT_TENANT_TYPES, Vec::new(), StatementKind::Read)
            .await
            .map_err(|error| {
                StorageError::Backend(map_driver_error(&error, "SearchParameter tenants"))
            })?;
        let mut tenants = BTreeSet::new();
        for row in &rows {
            let column = |name: &str| -> StorageResult<String> {
                row.get_r_by_name(name).map_err(|error| {
                    StorageError::Backend(BackendError::Internal {
                        backend_name: BACKEND_NAME.to_string(),
                        message: format!("SearchParameter tenants: {name}: {error}"),
                        source: None,
                    })
                })
            };
            if column("resource_type")? == "SearchParameter" {
                tenants.insert(column("tenant_id")?);
            }
        }
        Ok(tenants)
    }

    /// Reads `tenant_id`'s stored, `active` SearchParameters from Cassandra
    /// into the overlay, replacing what it held, and drops that tenant's
    /// cached registry so the next access rebuilds it. Returns how many the
    /// overlay now holds for the tenant.
    ///
    /// [`Self::load_all_stored_search_parameters`] runs this for every tenant
    /// before the server serves, so a stored SearchParameter that changes the
    /// patient compartment applies from the first request after a restart,
    /// as PostgreSQL's and MongoDB's startup reloads do. A SearchParameter
    /// whose `url` the shared base
    /// already holds is skipped, as [`Self::note_stored_search_parameter`]
    /// skips it, so the spec definitions the conformance seed stores never
    /// enter the overlay. Of two with the same `url`, the one with the newest
    /// `last_updated`, then the higher `id`, is kept ([`Precedence`]).
    ///
    /// **A concurrent create is kept.** A SearchParameter this process stores
    /// while the reload runs is noted after the reload took `since`, so the
    /// replacement keeps it, under the same [`Precedence`] as the reloaded
    /// definitions.
    /// One noted before `since` was noted after `create` wrote its
    /// `resources_by_type` row, so the id scan, sent after `since`, lists it.
    ///
    /// **Except when that row was not written.** `create` also notes a
    /// SearchParameter whose index row failed (it only warns, and the intent
    /// row is left for the sweeper) or was skipped because a later version
    /// exists. The scan cannot list it, so a reload drops it from the overlay
    /// until the sweeper (Step 5) writes the row.
    ///
    /// # Errors
    ///
    /// The mapped driver error of the id scan or of any read. The overlay is
    /// unchanged then.
    pub async fn load_stored_search_parameters(&self, tenant_id: &str) -> StorageResult<usize> {
        use crate::search::registry::{SearchParameterSource, SearchParameterStatus};
        use crate::tenant::{TenantContext, TenantId, TenantPermissions};

        let since = self.note_seq.load(Ordering::SeqCst);
        let rows = self
            .session
            .rows_paged(
                SELECT_STORED_SEARCH_PARAMETER_IDS,
                vec![Value::from(tenant_id), Value::from("SearchParameter")],
                StatementKind::Read,
            )
            .await
            .map_err(|error| {
                StorageError::Backend(map_driver_error(&error, "stored SearchParameters"))
            })?;
        let mut ids = BTreeSet::new();
        for row in &rows {
            let id: String = row.get_r_by_name("id").map_err(|error| {
                StorageError::Backend(BackendError::Internal {
                    backend_name: BACKEND_NAME.to_string(),
                    message: format!("stored SearchParameters: id: {error}"),
                    source: None,
                })
            })?;
            ids.insert(id);
        }

        let tenant = TenantContext::new(TenantId::new(tenant_id), TenantPermissions::full_access());
        let loader = SearchParameterLoader::new(self.config.fhir_version);
        let tenant = &tenant;
        let pending: Vec<_> = ids
            .into_iter()
            .map(|id| async move { self.read_resource(tenant, "SearchParameter", &id).await })
            .collect();
        let mut reads = stream::iter(pending).buffer_unordered(STORED_SEARCH_PARAMETER_READS);
        let mut defs: Vec<StoredDefinition> = Vec::new();
        while let Some(read) = reads.next().await {
            let stored = match read {
                Ok(Some(stored)) => stored,
                // Deleted since the scan, or never finished: not stored.
                Ok(None) | Err(StorageError::Resource(ResourceError::Gone { .. })) => continue,
                Err(error) => return Err(error),
            };
            if !self.registries.create_affects_overlay(stored.content()) {
                continue;
            }
            let Ok(mut definition) = loader.parse_resource(stored.content()) else {
                tracing::warn!(
                    tenant_id,
                    id = stored.id(),
                    "stored SearchParameter does not parse; leaving it out of the registry"
                );
                continue;
            };
            if definition.status != SearchParameterStatus::Active {
                continue;
            }
            definition.source = SearchParameterSource::Stored;
            let precedence = (stored.last_modified(), stored.id().to_string());
            insert_by_precedence(
                &mut defs,
                StoredDefinition {
                    definition,
                    precedence,
                    noted_at: 0,
                },
            );
        }

        let count = {
            let mut stored = self.stored_by_tenant.write();
            let noted: Vec<StoredDefinition> = stored
                .get(tenant_id)
                .into_iter()
                .flatten()
                .filter(|d| d.noted_at > since)
                .cloned()
                .collect();
            let mut merged = defs;
            for noted in noted {
                insert_by_precedence(&mut merged, noted);
            }
            let count = merged.len();
            if merged.is_empty() {
                stored.remove(tenant_id);
            } else {
                stored.insert(tenant_id.to_string(), merged);
            }
            count
        };
        self.registries.invalidate(tenant_id);
        Ok(count)
    }

    /// Records a SearchParameter `tenant_id` just stored in the overlay, if it
    /// can change that tenant's registry, and only then drops the tenant's
    /// cached registry.
    ///
    /// `create_affects_overlay` skips anything that is not `active` and
    /// anything whose `url` the shared base already holds, so seeding the spec
    /// SearchParameters (R6 ships them `active`) does not rebuild the registry
    /// once per resource (#667).
    pub(super) fn note_stored_search_parameter(
        &self,
        tenant_id: &str,
        resource: &serde_json::Value,
        last_updated: chrono::DateTime<chrono::Utc>,
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
        let id = resource
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        let inserted = {
            let mut stored = self.stored_by_tenant.write();
            let noted_at = self.note_seq.fetch_add(1, Ordering::SeqCst) + 1;
            let defs = stored.entry(tenant_id.to_string()).or_default();
            insert_by_precedence(
                defs,
                StoredDefinition {
                    definition,
                    precedence: (last_updated, id),
                    noted_at,
                },
            )
        };
        // A newer stored definition of the same `url` stays in force, so the
        // registry is unchanged.
        if inserted {
            self.registries.invalidate(tenant_id);
        }
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
/// The driver sends `USE "<keyspace>"` on every new connection, so a keyspace
/// that does not exist, or credentials the node refuses, fail the connection
/// like a closed port does: with
/// `cdrs-tokio` 10.0.0 the build ends at once with "Session control
/// connection died before completing initialization" when no contact point
/// connects (`cluster/control_connection.rs:170-176`; checked against the
/// shared 5.0.9 node, 2026-11-06), and the driver does not say why. A
/// deadline means a contact point connected but the session did not start
/// in time. Either is a startup failure, not a statement failure, so the
/// message names the likely causes.
fn connect_error(error: &DriverError, config: &CassandraConfig) -> BackendError {
    let opened_nothing = |why: String| BackendError::ConnectionFailed {
        backend_name: BACKEND_NAME.to_string(),
        message: format!(
            "no Cassandra session ({why}): no contact point answered, keyspace '{}' does not \
             exist (apply docs/Cassandra/schema/keyspace-single-node.cql), or the node refused \
             the credentials",
            config.keyspace
        ),
    };
    match error {
        // The only arm `map_driver_error` does not log.
        DriverError::Deadline { after, .. } => {
            tracing::warn!(
                contact_points = ?config.contact_points,
                keyspace = %config.keyspace,
                "Cassandra session not open after {} ms",
                after.as_millis()
            );
            opened_nothing(format!("not open within {} ms", after.as_millis()))
        }
        other => match map_driver_error(other, "connect") {
            BackendError::ConnectionFailed { .. } => {
                opened_nothing("no contact point accepted a session".to_string())
            }
            mapped => mapped,
        },
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

/// The driver's own recovery after a real restart (`session.rs`, "After a
/// restart"): `cdrs-tokio` 8.1.9 lost a single node for good there, 9.1.0 and
/// later must not. It stops and starts a Cassandra container, so it runs only on a
/// node of its own, never the shared one; it is skipped unless both of these
/// are set:
///
/// - `HFS_TEST_CASSANDRA_RESTART_URL`: the node's `host:port`, schema applied;
/// - `HFS_TEST_CASSANDRA_RESTART_CONTAINER`: its container name, for
///   `docker stop` and `docker start`.
///
/// ```text
/// CASSANDRA_HOST_PORT=59342 docker compose -p s2be-restart -f docker/cassandra/docker-compose.yml up -d
/// # apply keyspace-single-node.cql, then schema.cql
/// HFS_TEST_CASSANDRA_RESTART_URL=127.0.0.1:59342 \
/// HFS_TEST_CASSANDRA_RESTART_CONTAINER=s2be-restart-cassandra-1 \
///     cargo test -p helios-persistence --features cassandra --lib \
///     backends::cassandra::backend::restart_tests -- --nocapture
/// docker compose -p s2be-restart -f docker/cassandra/docker-compose.yml down -v
/// ```
#[cfg(test)]
mod restart_tests {
    use std::time::{Duration, Instant};

    use super::*;
    use cdrs_tokio::types::value::Value;

    const URL: &str = "HFS_TEST_CASSANDRA_RESTART_URL";
    const CONTAINER: &str = "HFS_TEST_CASSANDRA_RESTART_CONTAINER";

    /// How long the backend must stay healthy after the node returns.
    /// `cdrs-tokio` 8.1.9 lost the node 40 s after a restart when roadmap
    /// Step 2's test node layer reproduced its bug.
    const STAY_HEALTHY: Duration = Duration::from_secs(120);

    fn docker(args: &[&str]) {
        let status = std::process::Command::new("docker")
            .args(args)
            .stdout(std::process::Stdio::null())
            .status()
            .unwrap_or_else(|e| panic!("docker {args:?}: {e}"));
        assert!(status.success(), "docker {args:?}: {status}");
    }

    async fn read_one(backend: &CassandraBackend) -> Result<usize, String> {
        backend
            .session
            .rows(
                "SELECT id FROM resources WHERE tenant_id = ? AND resource_type = ? AND id = ?",
                vec![
                    Value::from("s2-restart"),
                    Value::from("Patient"),
                    Value::from("p1"),
                ],
                StatementKind::Read,
            )
            .await
            .map(|rows| rows.len())
            .map_err(|e| map_driver_error(&e, "read").to_string())
    }

    /// Callers per poll in [`wait_for_health`]: a burst of probes and
    /// requests reaching the session together while the node comes back.
    const CALLERS: usize = 16;

    /// Every 500 ms, runs [`CALLERS`] `health_check`s at once until all
    /// return `want_ok`, or panics after `limit`. Returns how long that took
    /// and how many polls it made.
    async fn wait_for_health(
        backend: &CassandraBackend,
        want_ok: bool,
        limit: Duration,
    ) -> (Duration, usize) {
        let started = Instant::now();
        let mut polls = 0;
        loop {
            polls += 1;
            let results =
                futures::future::join_all((0..CALLERS).map(|_| backend.health_check())).await;
            if results.iter().all(|result| result.is_ok() == want_ok) {
                return (started.elapsed(), polls);
            }
            assert!(
                started.elapsed() < limit,
                "health_check still {:?} after {limit:?}",
                results.last()
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_backend_recovers_after_its_node_restarts() {
        let (Ok(url), Ok(container)) = (std::env::var(URL), std::env::var(CONTAINER)) else {
            eprintln!("skipping: set {URL} and {CONTAINER} to a node of the test's own");
            return;
        };
        let backend = CassandraBackend::connect(CassandraConfig {
            contact_points: vec![url.clone()],
            connect_timeout_ms: 2_000,
            request_timeout_ms: 3_000,
            data_dir: Some(concat!(env!("CARGO_MANIFEST_DIR"), "/../../data").into()),
            ..CassandraConfig::default()
        })
        .await
        .unwrap_or_else(|e| panic!("connect to {url}: {e}"));

        backend.health_check().await.expect("health_check before");
        assert_eq!(read_one(&backend).await, Ok(0), "read before");
        backend.check_schema().await.expect("schema check before");
        let prepared_before = backend.session.prepared_count();
        assert!(prepared_before >= 3, "{prepared_before} prepared");

        docker(&["stop", &container]);
        let (down_after, _) = wait_for_health(&backend, false, Duration::from_secs(60)).await;
        let failed = backend
            .health_check()
            .await
            .expect_err("health_check while down");
        eprintln!("down: health_check failed {down_after:?} after the stop: {failed}");
        assert!(read_one(&backend).await.is_err(), "a read while down");

        docker(&["start", &container]);
        // The readiness budget of the Milestone A test (`UP_TIMEOUT`).
        let (up_after, polls) = wait_for_health(&backend, true, Duration::from_secs(240)).await;
        eprintln!(
            "up: health_check passed {up_after:?} after the start, after {polls} polls of \
             {CALLERS} concurrent callers"
        );

        // 8.1.9 lost the node some seconds after recovering in L1's
        // reproduction; the backend must stay healthy through that.
        let started = Instant::now();
        let mut checks = 0;
        while started.elapsed() < STAY_HEALTHY {
            backend.health_check().await.unwrap_or_else(|e| {
                panic!("health_check {:?} after recovery: {e}", started.elapsed())
            });
            assert_eq!(read_one(&backend).await, Ok(0), "read after recovery");
            checks += 1;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        eprintln!(
            "stayed healthy for {STAY_HEALTHY:?} ({checks} health checks and reads); \
             {} statements prepared now, {prepared_before} before",
            backend.session.prepared_count()
        );
        // The same driver session served throughout: nothing replaced it, so
        // its prepared statements survived.
        assert_eq!(backend.session.prepared_count(), prepared_before);
    }
}

/// The overlay's duplicate-`url` rule and the refresh's continue-on-error
/// loop, without a node.
#[cfg(test)]
mod reload_rules {
    use super::*;
    use chrono::{TimeZone, Utc};
    use std::cell::RefCell;

    fn stored(url: &str, id: &str, millis: i64) -> StoredDefinition {
        let resource = serde_json::json!({
            "resourceType": "SearchParameter",
            "id": id,
            "url": url,
            "name": "x",
            "status": "active",
            "description": "test",
            "code": format!("code-{id}"),
            "base": ["Patient"],
            "type": "token",
            "expression": "Patient.gender"
        });
        let definition = SearchParameterLoader::new(helios_fhir::FhirVersion::default_enabled())
            .parse_resource(&resource)
            .expect("parse");
        StoredDefinition {
            definition,
            precedence: (
                Utc.timestamp_millis_opt(millis).single().expect("time"),
                id.to_string(),
            ),
            noted_at: 0,
        }
    }

    /// The ids in force after inserting `order`, whatever order that is.
    fn winners(order: &[StoredDefinition]) -> Vec<String> {
        let mut defs = Vec::new();
        for candidate in order {
            insert_by_precedence(&mut defs, candidate.clone());
        }
        defs.iter().map(|d| d.precedence.1.clone()).collect()
    }

    #[test]
    fn the_newest_last_updated_wins_in_any_order() {
        let old = stored("http://example.org/sp", "a-old", 1_000);
        let new = stored("http://example.org/sp", "b-new", 2_000);
        assert_eq!(winners(&[old.clone(), new.clone()]), ["b-new"]);
        assert_eq!(winners(&[new, old]), ["b-new"]);
    }

    #[test]
    fn the_higher_id_breaks_a_tie() {
        let low = stored("http://example.org/sp", "aaa", 1_000);
        let high = stored("http://example.org/sp", "zzz", 1_000);
        assert_eq!(winners(&[low.clone(), high.clone()]), ["zzz"]);
        assert_eq!(winners(&[high, low]), ["zzz"]);
    }

    #[test]
    fn different_urls_are_kept_side_by_side() {
        let one = stored("http://example.org/one", "x", 1_000);
        let two = stored("http://example.org/two", "y", 500);
        assert_eq!(winners(&[one, two]), ["x", "y"]);
    }

    #[test]
    fn an_older_candidate_is_refused() {
        let mut defs = vec![stored("http://example.org/sp", "new", 2_000)];
        assert!(!insert_by_precedence(
            &mut defs,
            stored("http://example.org/sp", "old", 1_000)
        ));
        assert_eq!(defs[0].precedence.1, "new");
    }

    fn failing_on<'a>(
        bad: &'static str,
        loaded: &'a RefCell<Vec<String>>,
    ) -> impl FnMut(String) -> std::future::Ready<StorageResult<usize>> + 'a {
        move |tenant: String| {
            loaded.borrow_mut().push(tenant.clone());
            std::future::ready(if tenant == bad {
                Err(StorageError::Backend(BackendError::Internal {
                    backend_name: BACKEND_NAME.to_string(),
                    message: "injected".to_string(),
                    source: None,
                }))
            } else {
                Ok(2)
            })
        }
    }

    fn tenants() -> BTreeSet<String> {
        ["a", "b", "c"].map(String::from).into()
    }

    #[tokio::test]
    async fn a_refresh_continues_past_a_failed_tenant() {
        let loaded = RefCell::new(Vec::new());
        let reload = reload_each(&tenants(), false, failing_on("b", &loaded))
            .await
            .expect("a refresh does not fail on one tenant");
        assert_eq!(*loaded.borrow(), ["a", "b", "c"], "c is still reloaded");
        assert_eq!(
            reload,
            StoredSearchParameterReload {
                tenants: 3,
                definitions: 4,
                failed: 1,
            }
        );
    }

    #[tokio::test]
    async fn the_startup_load_stops_at_the_first_failure() {
        let loaded = RefCell::new(Vec::new());
        let result = reload_each(&tenants(), true, failing_on("b", &loaded)).await;
        assert!(result.is_err(), "startup is fail-fast");
        assert_eq!(*loaded.borrow(), ["a", "b"], "nothing after the failure");
    }
}

/// The partition-key scan's paging, against the node at
/// `HFS_TEST_CASSANDRA_URL` (skipped when unset).
#[cfg(test)]
mod node_tests {
    use super::*;
    use std::collections::BTreeSet;

    async fn backend() -> Option<CassandraBackend> {
        let url = std::env::var("HFS_TEST_CASSANDRA_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())?;
        Some(
            CassandraBackend::connect(CassandraConfig {
                contact_points: vec![url],
                data_dir: Some(concat!(env!("CARGO_MANIFEST_DIR"), "/../../data").into()),
                ..CassandraConfig::default()
            })
            .await
            .expect("connect"),
        )
    }

    fn keys(rows: &[cdrs_tokio::types::rows::Row]) -> BTreeSet<(String, String)> {
        rows.iter()
            .map(|row| {
                (
                    row.get_r_by_name("tenant_id").expect("tenant_id"),
                    row.get_r_by_name("resource_type").expect("resource_type"),
                )
            })
            .collect()
    }

    /// Small pages return the same partition keys as one unpaged read, so
    /// the paging state is followed to the end.
    #[tokio::test]
    async fn the_partition_key_scan_follows_every_page() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let page_size = 7;
        // 16 partitions of its own (4 tenants by 4 types, one resource each),
        // so even an empty node gives more than two pages of 7.
        let version = helios_fhir::FhirVersion::default_enabled();
        let types = ["Patient", "Observation", "Encounter", "Condition"];
        let run = uuid::Uuid::new_v4().simple().to_string();
        let tenants: Vec<crate::tenant::TenantContext> = (0..4)
            .map(|n| {
                crate::tenant::TenantContext::new(
                    crate::tenant::TenantId::new(format!("s2-paging-{run}-{n}")),
                    crate::tenant::TenantPermissions::full_access(),
                )
            })
            .collect();
        for tenant in &tenants {
            for resource_type in types {
                crate::core::ResourceStorage::create(
                    &backend,
                    tenant,
                    resource_type,
                    serde_json::json!({ "resourceType": resource_type }),
                    version,
                )
                .await
                .expect("create");
            }
        }
        let whole = backend
            .session
            .rows(SELECT_TENANT_TYPES, Vec::new(), StatementKind::Read)
            .await
            .expect("unpaged");
        let paged = backend
            .session
            .rows_paged_by(
                SELECT_TENANT_TYPES,
                Vec::new(),
                StatementKind::Read,
                page_size,
            )
            .await
            .expect("paged");
        assert!(
            paged.len() > 2 * page_size as usize,
            "several pages: {} rows",
            paged.len()
        );
        assert_eq!(paged.len(), keys(&paged).len(), "no partition twice");
        let (whole, paged) = (keys(&whole), keys(&paged));
        // Other tests may add partitions between the two reads, never remove.
        assert!(whole.is_subset(&paged), "a page was dropped");
        for tenant in &tenants {
            for resource_type in types {
                assert!(paged.contains(&(
                    tenant.tenant_id().as_str().to_string(),
                    resource_type.to_string()
                )));
            }
        }
    }
}
