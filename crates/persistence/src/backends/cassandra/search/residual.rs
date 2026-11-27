//! Residual filters' input: the values of a tag-checked resource, extracted
//! once per candidate (walkthrough 5.1, step 5; #128, item 3).
//!
//! The date residuals (dates beside another parameter, 5.8) and the
//! compartment filter (5.11, G33) both read one extraction of the loaded
//! `resources` row, never a second. It is the write path's extraction: the
//! shared `SearchParameterExtractor` with the tenant's registry
//! (`CassandraBackend::index_keys_of`, `C/write.rs`), so a residual sees the
//! values the index rows were written from. A debug build runs it on a
//! thread with a large stack, as the write path does
//! (`C/index_keys.rs`, `extract_index_keys`): a deep FHIRPath union in a
//! SearchParameter expression overflows a 2 MiB debug stack
//! (HeliosSoftware/hfs#3025), and a stack overflow aborts the process.
//!
//! This is the framework's own file (ADR-anastasiabryz-0001; #128).

use serde_json::Value as Json;

use super::LoadedResource;
use crate::backends::cassandra::backend::CassandraBackend;
use crate::backends::cassandra::read::ResourceRow;
use crate::search::{ExtractedValue, SearchParameterExtractor};

/// The stack a debug build extracts on: the write path's
/// `EXTRACTION_STACK_BYTES` (`C/index_keys.rs`), for the same reason.
const EXTRACTION_STACK_BYTES: usize = 64 * 1024 * 1024;

impl CassandraBackend {
    /// `row`, with `body` (its parsed `data`) extracted once for the
    /// residuals, under the tenant's registry.
    #[allow(dead_code)] // The compartment residual (6.3b) calls it.
    pub(super) fn load_for_residuals(
        &self,
        tenant_id: &str,
        resource_type: &str,
        row: ResourceRow,
        body: &Json,
    ) -> LoadedResource {
        let extractor = SearchParameterExtractor::new(self.registries.for_tenant(tenant_id));
        let extracted = residual_values(&extractor, resource_type, body).unwrap_or_else(|error| {
            // No values: every residual then fails to match, so a resource
            // whose values cannot be read is hidden, never shown unfiltered.
            tracing::warn!(
                tenant_id,
                resource_type,
                error = %error,
                "search could not extract a resource for its residual filters; it is not returned"
            );
            Vec::new()
        });
        LoadedResource { row, extracted }
    }
}

/// `extractor.extract(body, resource_type)`, on a thread with an
/// [`EXTRACTION_STACK_BYTES`] stack in a debug build, and on the caller's
/// stack in a release build, which FHIRPath's depth limit already covers.
/// A panic inside the extractor is resumed on the caller's thread.
fn residual_values(
    extractor: &SearchParameterExtractor,
    resource_type: &str,
    body: &Json,
) -> Result<Vec<ExtractedValue>, String> {
    let extract = || {
        extractor
            .extract(body, resource_type)
            .map_err(|error| error.to_string())
    };
    if !cfg!(debug_assertions) {
        return extract();
    }
    std::thread::scope(|scope| {
        match std::thread::Builder::new()
            .name("hfs-search-extraction".to_string())
            .stack_size(EXTRACTION_STACK_BYTES)
            .spawn_scoped(scope, extract)
        {
            Ok(handle) => handle
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic)),
            Err(error) => Err(format!("the extraction thread did not start: {error}")),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::cassandra::test_support::test_extractor;
    use crate::search::IndexValue;
    use serde_json::json;

    #[test]
    fn a_resource_is_extracted_once_with_every_parameter_the_residuals_read() {
        let body = json!({
            "resourceType": "Observation",
            "id": "o1",
            "status": "final",
            "subject": { "reference": "Patient/123" },
            "effectiveDateTime": "2026-11-27T10:00:00Z"
        });
        let values = residual_values(&test_extractor(), "Observation", &body).unwrap();
        let named = |name: &str| -> Vec<&ExtractedValue> {
            values
                .iter()
                .filter(|value| value.param_name == name)
                .collect()
        };

        assert!(
            named("subject").iter().any(|value| matches!(
                &value.value,
                IndexValue::Reference { reference, .. } if reference == "Patient/123"
            )),
            "the compartment residual reads the references: {values:?}"
        );
        assert!(
            !named("date").is_empty(),
            "a date residual reads the dates: {values:?}"
        );
    }

    #[test]
    fn a_body_the_extractor_refuses_is_an_error() {
        // A body that is not an object.
        let refused = residual_values(&test_extractor(), "Patient", &json!("not a resource"));
        assert_eq!(
            refused.unwrap_err(),
            "Invalid resource: Resource must be a JSON object"
        );
    }
}
