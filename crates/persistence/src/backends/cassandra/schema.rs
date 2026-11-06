//! What the backend expects of the schema, and the check that it is there.
//!
//! The backend never creates or alters a table. `docs/Cassandra/schema/` holds
//! the keyspace and table definitions, and one process applies them: a setup
//! command, a developer with `cqlsh`, or the test harness (P6,
//! ADR-aacruzgon-0027; `docs/Cassandra/schema/README.md`, "Apply it").
//! [`Backend::initialize`](crate::core::Backend::initialize) only checks that
//! the ten tables exist in the configured keyspace, so a server pointed at an
//! empty or half-applied keyspace fails at startup instead of on its first
//! write.

use std::collections::BTreeSet;

/// The ten tables of `docs/Cassandra/schema/schema.cql`, in file order.
pub const TABLES: [&str; 10] = [
    "resources",
    "resource_versions",
    "pending_index_writes",
    "resources_by_type",
    "token_code_index",
    "token_system_code_index",
    "token_system_index",
    "date_index",
    "date_index_by_end",
    "uri_index",
];

/// Lists the tables of one keyspace. `system_schema` is node-local, so this
/// reads the coordinator's own schema copy.
pub(crate) const SELECT_TABLES: &str =
    "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?";

/// The tables of [`TABLES`] missing from `found`, in [`TABLES`] order.
pub(crate) fn missing_tables<'a, I>(found: I) -> Vec<&'static str>
where
    I: IntoIterator<Item = &'a str>,
{
    let found: BTreeSet<&str> = found.into_iter().collect();
    TABLES
        .iter()
        .copied()
        .filter(|table| !found.contains(table))
        .collect()
}

/// The `initialize` failure for a keyspace that lacks `missing`.
pub(crate) fn missing_tables_message(keyspace: &str, missing: &[&str]) -> String {
    if missing.len() == TABLES.len() {
        format!(
            "keyspace '{keyspace}' has none of the Cassandra backend's ten tables; apply \
             docs/Cassandra/schema/keyspace-single-node.cql and schema.cql first \
             (docs/Cassandra/schema/README.md); the backend never applies the schema itself"
        )
    } else {
        format!(
            "keyspace '{keyspace}' is missing the table(s) {}; apply \
             docs/Cassandra/schema/schema.cql to it (docs/Cassandra/schema/README.md); the \
             backend never applies the schema itself",
            missing.join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tables_match_the_signed_off_schema_file() {
        let schema = include_str!("../../../../../docs/Cassandra/schema/schema.cql");
        let declared: Vec<&str> = schema
            .lines()
            .filter_map(|line| line.strip_prefix("CREATE TABLE IF NOT EXISTS "))
            .map(|rest| rest.split_whitespace().next().unwrap_or_default())
            .collect();
        assert_eq!(declared, TABLES.to_vec());
    }

    #[test]
    fn reports_what_is_missing() {
        assert!(missing_tables(TABLES.iter().copied()).is_empty());
        assert_eq!(missing_tables(std::iter::empty()), TABLES.to_vec());
        let without_uri: Vec<&str> = TABLES[..9].to_vec();
        assert_eq!(missing_tables(without_uri), vec!["uri_index"]);
        // Extra tables in the keyspace are not this check's business.
        let mut more: Vec<&str> = TABLES.to_vec();
        more.push("something_else");
        assert!(missing_tables(more).is_empty());
    }

    #[test]
    fn messages_point_at_the_schema_files() {
        let none = missing_tables_message("helios", &TABLES);
        assert!(none.contains("keyspace-single-node.cql"), "{none}");
        let some = missing_tables_message("helios", &["uri_index"]);
        assert!(some.contains("uri_index"), "{some}");
        assert!(some.contains("schema.cql"), "{some}");
    }
}
