//! The FHIR version and the backend configuration the Cassandra unit tests
//! use (fhir-versions, ADR-aacruzgon-0065).
//!
//! `HFS_TEST_FHIR_VERSION` names the version, in any form
//! [`FhirVersion::from_storage`] accepts (`R5`, `r5`, `5.0`). Unset, the
//! tests use [`FhirVersion::default_enabled`], exactly as before this module,
//! so a build with R4 tests R4. Set to a value this build cannot parse, or to
//! a version it does not enable, the first test that asks panics: a typo must
//! not quietly run R4 and report green. One all-versions build therefore runs
//! the same tests under each version, one run per value.
//!
//! The integration tests have the same rule in `tests/common/cassandra.rs`;
//! they are another crate and cannot reach this `cfg(test)` module.

use std::path::PathBuf;
use std::sync::OnceLock;

use helios_fhir::FhirVersion;

use super::CassandraConfig;

/// The variable that chooses the tests' FHIR version.
pub(crate) const TEST_FHIR_VERSION_VAR: &str = "HFS_TEST_FHIR_VERSION";

/// The FHIR version the tests create, expect and load the registry for,
/// decided once per test binary. When the variable is set, the choice is
/// printed once, so every log names the version it ran.
pub(crate) fn test_fhir_version() -> FhirVersion {
    static CHOSEN: OnceLock<FhirVersion> = OnceLock::new();
    *CHOSEN.get_or_init(|| match std::env::var(TEST_FHIR_VERSION_VAR) {
        Ok(value) => {
            let version =
                parse_test_fhir_version(&value).unwrap_or_else(|message| panic!("{message}"));
            eprintln!(
                "fhir-versions: {TEST_FHIR_VERSION_VAR}={value} -> {version:?} ({})",
                version.as_mime_param()
            );
            version
        }
        Err(_) => FhirVersion::default_enabled(),
    })
}

/// `value` as a FHIR version this build enables, or the message to panic
/// with.
fn parse_test_fhir_version(value: &str) -> Result<FhirVersion, String> {
    FhirVersion::from_storage(value.trim()).ok_or_else(|| {
        let enabled: Vec<&str> = FhirVersion::enabled_versions()
            .iter()
            .map(FhirVersion::as_str)
            .collect();
        format!(
            "{TEST_FHIR_VERSION_VAR}={value:?} is not a FHIR version this build enables \
             (enabled: {})",
            enabled.join(", ")
        )
    })
}

/// The configuration every unit test builds its backend from: the defaults,
/// the repository's `data/` directory, and [`test_fhir_version`], so the
/// search registry and the stored-SearchParameter parser follow the version
/// the test writes. A test adds its contact point with
/// `CassandraConfig { contact_points: …, ..test_config() }`.
pub(crate) fn test_config() -> CassandraConfig {
    CassandraConfig {
        data_dir: Some(PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../data"
        ))),
        fhir_version: test_fhir_version(),
        ..CassandraConfig::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_enabled_version_parses_in_each_form() {
        for version in FhirVersion::enabled_versions() {
            for form in [
                version.as_str().to_string(),
                version.as_str().to_lowercase(),
                version.as_mime_param().to_string(),
            ] {
                assert_eq!(parse_test_fhir_version(&form), Ok(*version), "{form}");
            }
        }
    }

    #[test]
    fn a_value_that_is_no_enabled_version_is_refused() {
        for value in ["R7", "4.1", "", "default"] {
            let message = parse_test_fhir_version(value).unwrap_err();
            assert!(message.contains(TEST_FHIR_VERSION_VAR), "{message}");
            assert!(message.contains("enabled:"), "{message}");
        }
    }

    #[test]
    fn the_config_follows_the_test_version() {
        let config = test_config();
        assert_eq!(config.fhir_version, test_fhir_version());
        assert!(config.data_dir.is_some());
    }
}
