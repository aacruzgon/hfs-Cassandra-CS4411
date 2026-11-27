//! Step 6b parity checks using the same R4 fixtures as PostgreSQL.

#![cfg(feature = "R4")]

#[path = "../search/date_boundary_suite.rs"]
mod date_boundary_suite;
#[path = "../search/date_minute_index_suite.rs"]
mod date_minute_index_suite;
#[path = "../search/date_period_suite.rs"]
mod date_period_suite;
#[path = "../search/date_precision_suite.rs"]
mod date_precision_suite;

use helios_fhir::FhirVersion;

use crate::cassandra_node::{self, URL_VAR, test_fhir_version, unique_tenant_id};

macro_rules! date_parity_test {
    ($test:ident, $suite:path) => {
        #[tokio::test]
        #[ignore = "requires Cassandra's Step 6 search framework (#128)"]
        async fn $test() {
            if test_fhir_version() != FhirVersion::R4 {
                eprintln!("Skipping R4 PostgreSQL reference fixtures on another FHIR version");
                return;
            }
            let node = node_or_skip!("date parity");
            let backend = crate::backend_skeleton::backend(node).await;
            let tenant = unique_tenant_id("date-parity");
            $suite(&backend, tenant.as_str()).await;
        }
    };
}

date_parity_test!(
    date_parity_day_boundaries,
    date_boundary_suite::day_precision_boundaries
);
date_parity_test!(
    date_parity_sub_day_precision,
    date_precision_suite::sub_day_precision_and_validation
);
date_parity_test!(
    date_parity_stored_minutes,
    date_minute_index_suite::minute_precision_stored_values_are_indexed
);
date_parity_test!(date_parity_periods, date_period_suite::date_period_prefixes);
