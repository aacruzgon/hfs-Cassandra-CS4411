//! Residual filters' input: the values of a tag-checked resource, extracted
//! once per candidate (walkthrough 5.1, step 5; #128, item 3).
//!
//! The date residuals (dates beside another parameter, 5.8) and the
//! compartment filter (5.11, G33) both read one extraction of the loaded
//! `resources` row, never a second. It uses the write path's extractor, the
//! shared `SearchParameterExtractor` with the tenant's registry
//! (`CassandraBackend::index_keys_of`, `C/write.rs`), but under the
//! resource's own `fhir_version` (#128, item 3), where the write path uses
//! the backend's configured one. The two differ only for a resource stored
//! under another version, whose `instant` elements (HeliosSoftware/hfs#2962)
//! a date residual may then read differently from its index rows.
//!
//! A debug build runs it on a thread with a large stack, as the write path
//! does (`C/index_keys.rs`, `extract_index_keys`): a deep FHIRPath union in
//! a SearchParameter expression overflows a 2 MiB debug stack
//! (HeliosSoftware/hfs#3025), and a stack overflow aborts the process.
//!
//! This is the framework's own file (ADR-anastasiabryz-0001; #128).

use serde_json::Value as Json;

use super::LoadedResource;
use crate::backends::cassandra::backend::CassandraBackend;
use crate::backends::cassandra::read::ResourceRow;
use crate::search::{ExtractedValue, IndexValue, SearchParameterExtractor};
use crate::types::{CompartmentMembership, strip_reference_version};

/// The stack a debug build extracts on: the write path's
/// `EXTRACTION_STACK_BYTES` (`C/index_keys.rs`), for the same reason.
///
/// A copy, with its own thread below, not a shared helper: the write path's
/// `extract_index_keys` returns index keys, not the values a residual reads,
/// and keeps its stack size private to `C/index_keys.rs`, which is Sam's.
/// Sharing one helper is a change to that file, so it is his to make.
const EXTRACTION_STACK_BYTES: usize = 64 * 1024 * 1024;

impl CassandraBackend {
    /// `row`, with `body` (its parsed `data`) extracted once for the
    /// residuals, under the tenant's registry.
    pub(super) fn load_for_residuals(
        &self,
        tenant_id: &str,
        resource_type: &str,
        row: ResourceRow,
        body: &Json,
    ) -> LoadedResource {
        // The resource's own version decides which of its elements are
        // `instant`s, extracted as points (HeliosSoftware/hfs#2962; #128,
        // item 3: "under the resource's own `fhir_version`").
        let extractor =
            SearchParameterExtractor::new(self.registries.for_tenant(tenant_id), row.fhir_version);
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

/// Whether `loaded` is a member of `compartment` (walkthrough 5.11, G33):
/// one of its `IndexValue::Reference` values of the compartment's params
/// equals `strip_reference_version(reference)`, or starts with it followed
/// by `/_history/`. That is MongoDB's rule (`compartment_membership_filter`,
/// `backends/mongodb/search_impl.rs`). SQLite also matches an absolute-URL
/// reference such as `https://example.org/fhir/Patient/123`, through its
/// `value_reference_target` column; Cassandra, like MongoDB, does not. A
/// compartment with no params has no members: it is never read as "no
/// compartment".
pub(super) fn in_compartment(loaded: &LoadedResource, compartment: &CompartmentMembership) -> bool {
    let base = strip_reference_version(&compartment.reference);
    let versioned = format!("{base}/_history/");
    loaded
        .extracted
        .iter()
        .filter(|value| compartment.params.contains(&value.param_name))
        .any(|value| match &value.value {
            IndexValue::Reference { reference, .. } => {
                reference == base || reference.starts_with(&versioned)
            }
            _ => false,
        })
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

    fn loaded(body: serde_json::Value) -> LoadedResource {
        let extracted = residual_values(&test_extractor(), "Observation", &body).unwrap();
        LoadedResource {
            row: crate::backends::cassandra::read::ResourceRow {
                version_id: 1,
                data: None,
                last_updated: chrono::Utc::now(),
                created_at: None,
                deleted_at: None,
                fhir_version: crate::backends::cassandra::test_support::test_fhir_version(),
                stored_fhir_version: None,
                write_ts: None,
                write_id: None,
                index_keys: None,
            },
            extracted,
        }
    }

    fn observation(subject: Option<&str>, performer: Option<&str>) -> serde_json::Value {
        let mut body = json!({ "resourceType": "Observation", "status": "final" });
        if let Some(subject) = subject {
            body["subject"] = json!({ "reference": subject });
        }
        if let Some(performer) = performer {
            body["performer"] = json!([{ "reference": performer }]);
        }
        body
    }

    fn patient_123(params: &[&str]) -> CompartmentMembership {
        CompartmentMembership {
            params: params.iter().map(|param| param.to_string()).collect(),
            reference: "Patient/123".to_string(),
        }
    }

    #[test]
    fn a_member_references_the_compartment_through_one_of_its_params() {
        let compartment = patient_123(&["subject", "performer"]);
        for (case, body, member) in [
            ("subject", observation(Some("Patient/123"), None), true),
            ("performer", observation(None, Some("Patient/123")), true),
            (
                "a version",
                observation(Some("Patient/123/_history/2"), None),
                true,
            ),
            (
                "another patient",
                observation(Some("Patient/1234"), None),
                false,
            ),
            (
                "a prefix only",
                observation(Some("Patient/12"), None),
                false,
            ),
            ("no reference", observation(None, None), false),
        ] {
            assert_eq!(
                in_compartment(&loaded(body), &compartment),
                member,
                "{case}"
            );
        }
    }

    #[test]
    fn only_the_compartments_own_params_count() {
        let body = observation(None, Some("Patient/123"));
        assert!(!in_compartment(
            &loaded(body.clone()),
            &patient_123(&["subject"])
        ));
        assert!(
            !in_compartment(&loaded(body), &patient_123(&[])),
            "no params, no members"
        );
    }

    #[test]
    fn a_versioned_compartment_reference_is_compared_without_its_version() {
        let compartment = CompartmentMembership {
            params: vec!["subject".to_string()],
            reference: "Patient/123/_history/7".to_string(),
        };
        assert!(in_compartment(
            &loaded(observation(Some("Patient/123"), None)),
            &compartment
        ));
    }
}
