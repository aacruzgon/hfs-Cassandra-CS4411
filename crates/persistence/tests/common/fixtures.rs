//! Test fixtures for persistence layer testing.
//!
//! This module provides predefined FHIR resources for use in tests,
//! along with builders for creating custom test data.

use serde_json::{json, Value};

use helios_fhir::FhirVersion;
use helios_persistence::tenant::TenantId;
use helios_persistence::types::StoredResource;

/// A patient fixture for testing.
#[derive(Debug, Clone)]
pub struct PatientFixture {
    /// Patient ID.
    pub id: String,
    /// Patient family name.
    pub family: String,
    /// Patient given names.
    pub given: Vec<String>,
    /// Birth date (YYYY-MM-DD format).
    pub birth_date: Option<String>,
    /// Patient gender.
    pub gender: Option<String>,
    /// Patient identifiers (system, value pairs).
    pub identifiers: Vec<(String, String)>,
    /// Reference to managing organization.
    pub organization_ref: Option<String>,
    /// Whether the patient is active.
    pub active: bool,
}

impl PatientFixture {
    /// Creates a new patient fixture with minimal required fields.
    pub fn new(id: impl Into<String>, family: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            family: family.into(),
            given: vec![],
            birth_date: None,
            gender: None,
            identifiers: vec![],
            organization_ref: None,
            active: true,
        }
    }

    /// Sets given names.
    pub fn with_given(mut self, given: Vec<&str>) -> Self {
        self.given = given.into_iter().map(String::from).collect();
        self
    }

    /// Sets birth date.
    pub fn with_birth_date(mut self, date: impl Into<String>) -> Self {
        self.birth_date = Some(date.into());
        self
    }

    /// Sets gender.
    pub fn with_gender(mut self, gender: impl Into<String>) -> Self {
        self.gender = Some(gender.into());
        self
    }

    /// Adds an identifier.
    pub fn with_identifier(mut self, system: impl Into<String>, value: impl Into<String>) -> Self {
        self.identifiers.push((system.into(), value.into()));
        self
    }

    /// Sets managing organization reference.
    pub fn with_organization(mut self, org_ref: impl Into<String>) -> Self {
        self.organization_ref = Some(org_ref.into());
        self
    }

    /// Sets active status.
    pub fn with_active(mut self, active: bool) -> Self {
        self.active = active;
        self
    }

    /// Converts to FHIR JSON.
    pub fn to_json(&self) -> Value {
        let mut patient = json!({
            "resourceType": "Patient",
            "id": self.id,
            "active": self.active,
            "name": [{
                "family": self.family,
                "given": self.given,
            }],
        });

        if let Some(birth_date) = &self.birth_date {
            patient["birthDate"] = json!(birth_date);
        }

        if let Some(gender) = &self.gender {
            patient["gender"] = json!(gender);
        }

        if !self.identifiers.is_empty() {
            patient["identifier"] = json!(
                self.identifiers.iter().map(|(system, value)| {
                    json!({
                        "system": system,
                        "value": value,
                    })
                }).collect::<Vec<_>>()
            );
        }

        if let Some(org_ref) = &self.organization_ref {
            patient["managingOrganization"] = json!({
                "reference": org_ref,
            });
        }

        patient
    }

    /// Converts to a StoredResource.
    pub fn to_stored_resource(&self, tenant_id: &TenantId) -> StoredResource {
        StoredResource::new("Patient", &self.id, tenant_id.clone(), self.to_json(), FhirVersion::default_enabled())
    }
}

/// An observation fixture for testing.
#[derive(Debug, Clone)]
pub struct ObservationFixture {
    /// Observation ID.
    pub id: String,
    /// Observation status.
    pub status: String,
    /// LOINC code.
    pub code: String,
    /// Code display text.
    pub code_display: String,
    /// Patient reference.
    pub patient_ref: String,
    /// Effective date/time.
    pub effective: Option<String>,
    /// Value (for quantity observations).
    pub value: Option<f64>,
    /// Value unit.
    pub unit: Option<String>,
}

impl ObservationFixture {
    /// Creates a new observation fixture.
    pub fn new(id: impl Into<String>, code: impl Into<String>, patient_ref: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            status: "final".to_string(),
            code: code.into(),
            code_display: String::new(),
            patient_ref: patient_ref.into(),
            effective: None,
            value: None,
            unit: None,
        }
    }

    /// Sets the observation status.
    pub fn with_status(mut self, status: impl Into<String>) -> Self {
        self.status = status.into();
        self
    }

    /// Sets the code display text.
    pub fn with_display(mut self, display: impl Into<String>) -> Self {
        self.code_display = display.into();
        self
    }

    /// Sets the effective date/time.
    pub fn with_effective(mut self, effective: impl Into<String>) -> Self {
        self.effective = Some(effective.into());
        self
    }

    /// Sets a quantity value.
    pub fn with_value(mut self, value: f64, unit: impl Into<String>) -> Self {
        self.value = Some(value);
        self.unit = Some(unit.into());
        self
    }

    /// Converts to FHIR JSON.
    pub fn to_json(&self) -> Value {
        let mut obs = json!({
            "resourceType": "Observation",
            "id": self.id,
            "status": self.status,
            "code": {
                "coding": [{
                    "system": "http://loinc.org",
                    "code": self.code,
                    "display": self.code_display,
                }],
            },
            "subject": {
                "reference": self.patient_ref,
            },
        });

        if let Some(effective) = &self.effective {
            obs["effectiveDateTime"] = json!(effective);
        }

        if let (Some(value), Some(unit)) = (&self.value, &self.unit) {
            obs["valueQuantity"] = json!({
                "value": value,
                "unit": unit,
                "system": "http://unitsofmeasure.org",
            });
        }

        obs
    }

    /// Converts to a StoredResource.
    pub fn to_stored_resource(&self, tenant_id: &TenantId) -> StoredResource {
        StoredResource::new("Observation", &self.id, tenant_id.clone(), self.to_json(), FhirVersion::default_enabled())
    }
}

/// An organization fixture for testing.
#[derive(Debug, Clone)]
pub struct OrganizationFixture {
    /// Organization ID.
    pub id: String,
    /// Organization name.
    pub name: String,
    /// Organization type code.
    pub type_code: Option<String>,
    /// Whether active.
    pub active: bool,
    /// Identifiers.
    pub identifiers: Vec<(String, String)>,
}

impl OrganizationFixture {
    /// Creates a new organization fixture.
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            name: name.into(),
            type_code: None,
            active: true,
            identifiers: vec![],
        }
    }

    /// Sets the organization type.
    pub fn with_type(mut self, type_code: impl Into<String>) -> Self {
        self.type_code = Some(type_code.into());
        self
    }

    /// Sets active status.
    pub fn with_active(mut self, active: bool) -> Self {
        self.active = active;
        self
    }

    /// Adds an identifier.
    pub fn with_identifier(mut self, system: impl Into<String>, value: impl Into<String>) -> Self {
        self.identifiers.push((system.into(), value.into()));
        self
    }

    /// Converts to FHIR JSON.
    pub fn to_json(&self) -> Value {
        let mut org = json!({
            "resourceType": "Organization",
            "id": self.id,
            "name": self.name,
            "active": self.active,
        });

        if let Some(type_code) = &self.type_code {
            org["type"] = json!([{
                "coding": [{
                    "system": "http://terminology.hl7.org/CodeSystem/organization-type",
                    "code": type_code,
                }],
            }]);
        }

        if !self.identifiers.is_empty() {
            org["identifier"] = json!(
                self.identifiers.iter().map(|(system, value)| {
                    json!({
                        "system": system,
                        "value": value,
                    })
                }).collect::<Vec<_>>()
            );
        }

        org
    }

    /// Converts to a StoredResource.
    pub fn to_stored_resource(&self, tenant_id: &TenantId) -> StoredResource {
        StoredResource::new("Organization", &self.id, tenant_id.clone(), self.to_json(), FhirVersion::default_enabled())
    }
}

/// A practitioner fixture for testing.
#[derive(Debug, Clone)]
pub struct PractitionerFixture {
    /// Practitioner ID.
    pub id: String,
    /// Family name.
    pub family: String,
    /// Given names.
    pub given: Vec<String>,
    /// NPI identifier.
    pub npi: Option<String>,
    /// Whether active.
    pub active: bool,
}

impl PractitionerFixture {
    /// Creates a new practitioner fixture.
    pub fn new(id: impl Into<String>, family: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            family: family.into(),
            given: vec![],
            npi: None,
            active: true,
        }
    }

    /// Sets given names.
    pub fn with_given(mut self, given: Vec<&str>) -> Self {
        self.given = given.into_iter().map(String::from).collect();
        self
    }

    /// Sets NPI identifier.
    pub fn with_npi(mut self, npi: impl Into<String>) -> Self {
        self.npi = Some(npi.into());
        self
    }

    /// Sets active status.
    pub fn with_active(mut self, active: bool) -> Self {
        self.active = active;
        self
    }

    /// Converts to FHIR JSON.
    pub fn to_json(&self) -> Value {
        let mut pract = json!({
            "resourceType": "Practitioner",
            "id": self.id,
            "active": self.active,
            "name": [{
                "family": self.family,
                "given": self.given,
            }],
        });

        if let Some(npi) = &self.npi {
            pract["identifier"] = json!([{
                "system": "http://hl7.org/fhir/sid/us-npi",
                "value": npi,
            }]);
        }

        pract
    }

    /// Converts to a StoredResource.
    pub fn to_stored_resource(&self, tenant_id: &TenantId) -> StoredResource {
        StoredResource::new("Practitioner", &self.id, tenant_id.clone(), self.to_json(), FhirVersion::default_enabled())
    }
}

/// An encounter fixture for testing.
#[derive(Debug, Clone)]
pub struct EncounterFixture {
    /// Encounter ID.
    pub id: String,
    /// Encounter status.
    pub status: String,
    /// Encounter class code.
    pub class_code: String,
    /// Patient reference.
    pub patient_ref: String,
    /// Practitioner references.
    pub practitioner_refs: Vec<String>,
    /// Period start.
    pub period_start: Option<String>,
    /// Period end.
    pub period_end: Option<String>,
}

impl EncounterFixture {
    /// Creates a new encounter fixture.
    pub fn new(
        id: impl Into<String>,
        status: impl Into<String>,
        patient_ref: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            status: status.into(),
            class_code: "AMB".to_string(),
            patient_ref: patient_ref.into(),
            practitioner_refs: vec![],
            period_start: None,
            period_end: None,
        }
    }

    /// Sets the encounter class.
    pub fn with_class(mut self, class_code: impl Into<String>) -> Self {
        self.class_code = class_code.into();
        self
    }

    /// Adds a practitioner.
    pub fn with_practitioner(mut self, pract_ref: impl Into<String>) -> Self {
        self.practitioner_refs.push(pract_ref.into());
        self
    }

    /// Sets the period.
    pub fn with_period(
        mut self,
        start: impl Into<String>,
        end: Option<impl Into<String>>,
    ) -> Self {
        self.period_start = Some(start.into());
        self.period_end = end.map(|e| e.into());
        self
    }

    /// Converts to FHIR JSON.
    pub fn to_json(&self) -> Value {
        let mut enc = json!({
            "resourceType": "Encounter",
            "id": self.id,
            "status": self.status,
            "class": {
                "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
                "code": self.class_code,
            },
            "subject": {
                "reference": self.patient_ref,
            },
        });

        if !self.practitioner_refs.is_empty() {
            enc["participant"] = json!(
                self.practitioner_refs.iter().map(|pract_ref| {
                    json!({
                        "individual": {
                            "reference": pract_ref,
                        },
                    })
                }).collect::<Vec<_>>()
            );
        }

        if self.period_start.is_some() {
            let mut period = json!({});
            if let Some(start) = &self.period_start {
                period["start"] = json!(start);
            }
            if let Some(end) = &self.period_end {
                period["end"] = json!(end);
            }
            enc["period"] = period;
        }

        enc
    }

    /// Converts to a StoredResource.
    pub fn to_stored_resource(&self, tenant_id: &TenantId) -> StoredResource {
        StoredResource::new("Encounter", &self.id, tenant_id.clone(), self.to_json(), FhirVersion::default_enabled())
    }
}

/// Collection of test fixtures.
#[derive(Debug, Clone, Default)]
pub struct TestFixtures {
    /// Patient fixtures.
    pub patients: Vec<PatientFixture>,
    /// Observation fixtures.
    pub observations: Vec<ObservationFixture>,
    /// Organization fixtures.
    pub organizations: Vec<OrganizationFixture>,
    /// Practitioner fixtures.
    pub practitioners: Vec<PractitionerFixture>,
    /// Encounter fixtures.
    pub encounters: Vec<EncounterFixture>,
}

impl TestFixtures {
    /// Creates empty fixtures.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a default set of fixtures for comprehensive testing.
    ///
    /// This includes:
    /// - 5 patients with varying demographics
    /// - 10 observations linked to patients
    /// - 3 organizations
    /// - 3 practitioners
    /// - 5 encounters
    pub fn rich() -> Self {
        Self {
            patients: vec![
                PatientFixture::new("patient-1", "Smith")
                    .with_given(vec!["John", "Jacob"])
                    .with_birth_date("1980-01-15")
                    .with_gender("male")
                    .with_identifier("http://example.org/mrn", "MRN001")
                    .with_organization("Organization/org-1"),
                PatientFixture::new("patient-2", "Jones")
                    .with_given(vec!["Jane"])
                    .with_birth_date("1990-05-20")
                    .with_gender("female")
                    .with_identifier("http://example.org/mrn", "MRN002")
                    .with_organization("Organization/org-1"),
                PatientFixture::new("patient-3", "Williams")
                    .with_given(vec!["Robert"])
                    .with_birth_date("1975-12-01")
                    .with_gender("male")
                    .with_identifier("http://example.org/mrn", "MRN003")
                    .with_organization("Organization/org-2"),
                PatientFixture::new("patient-4", "Brown")
                    .with_given(vec!["Emily", "Rose"])
                    .with_birth_date("2000-07-10")
                    .with_gender("female")
                    .with_identifier("http://example.org/mrn", "MRN004"),
                PatientFixture::new("patient-5", "Davis")
                    .with_given(vec!["Michael"])
                    .with_birth_date("1965-03-25")
                    .with_gender("male")
                    .with_active(false),
            ],
            observations: vec![
                ObservationFixture::new("obs-1", "8867-4", "Patient/patient-1")
                    .with_display("Heart rate")
                    .with_effective("2024-01-15T10:30:00Z")
                    .with_value(72.0, "bpm"),
                ObservationFixture::new("obs-2", "8310-5", "Patient/patient-1")
                    .with_display("Body temperature")
                    .with_effective("2024-01-15T10:30:00Z")
                    .with_value(37.0, "Cel"),
                ObservationFixture::new("obs-3", "8867-4", "Patient/patient-2")
                    .with_display("Heart rate")
                    .with_effective("2024-01-16T09:00:00Z")
                    .with_value(68.0, "bpm"),
                ObservationFixture::new("obs-4", "29463-7", "Patient/patient-2")
                    .with_display("Body weight")
                    .with_effective("2024-01-16T09:00:00Z")
                    .with_value(65.0, "kg"),
                ObservationFixture::new("obs-5", "8302-2", "Patient/patient-3")
                    .with_display("Body height")
                    .with_effective("2024-01-10T14:00:00Z")
                    .with_value(175.0, "cm"),
                ObservationFixture::new("obs-6", "8867-4", "Patient/patient-3")
                    .with_display("Heart rate")
                    .with_effective("2024-01-10T14:00:00Z")
                    .with_value(80.0, "bpm"),
                ObservationFixture::new("obs-7", "8867-4", "Patient/patient-4")
                    .with_display("Heart rate")
                    .with_effective("2024-01-20T11:30:00Z")
                    .with_value(75.0, "bpm"),
                ObservationFixture::new("obs-8", "29463-7", "Patient/patient-4")
                    .with_display("Body weight")
                    .with_effective("2024-01-20T11:30:00Z")
                    .with_value(58.0, "kg"),
                ObservationFixture::new("obs-9", "8310-5", "Patient/patient-5")
                    .with_display("Body temperature")
                    .with_effective("2024-01-05T08:00:00Z")
                    .with_value(36.5, "Cel"),
                ObservationFixture::new("obs-10", "8867-4", "Patient/patient-5")
                    .with_display("Heart rate")
                    .with_status("preliminary")
                    .with_effective("2024-01-05T08:00:00Z")
                    .with_value(88.0, "bpm"),
            ],
            organizations: vec![
                OrganizationFixture::new("org-1", "General Hospital")
                    .with_type("prov")
                    .with_identifier("http://example.org/npi", "1234567890"),
                OrganizationFixture::new("org-2", "City Clinic")
                    .with_type("prov")
                    .with_identifier("http://example.org/npi", "0987654321"),
                OrganizationFixture::new("org-3", "Health Insurance Co")
                    .with_type("ins")
                    .with_identifier("http://example.org/payer-id", "INS001"),
            ],
            practitioners: vec![
                PractitionerFixture::new("pract-1", "Wilson")
                    .with_given(vec!["James"])
                    .with_npi("1112223334"),
                PractitionerFixture::new("pract-2", "Taylor")
                    .with_given(vec!["Sarah", "Ann"])
                    .with_npi("2223334445"),
                PractitionerFixture::new("pract-3", "Anderson")
                    .with_given(vec!["Michael"])
                    .with_npi("3334445556"),
            ],
            encounters: vec![
                EncounterFixture::new("enc-1", "finished", "Patient/patient-1")
                    .with_class("AMB")
                    .with_practitioner("Practitioner/pract-1")
                    .with_period("2024-01-15T10:00:00Z", Some("2024-01-15T11:00:00Z")),
                EncounterFixture::new("enc-2", "finished", "Patient/patient-2")
                    .with_class("AMB")
                    .with_practitioner("Practitioner/pract-2")
                    .with_period("2024-01-16T09:00:00Z", Some("2024-01-16T09:30:00Z")),
                EncounterFixture::new("enc-3", "in-progress", "Patient/patient-3")
                    .with_class("IMP")
                    .with_practitioner("Practitioner/pract-1")
                    .with_practitioner("Practitioner/pract-3")
                    .with_period("2024-01-10T14:00:00Z", None::<String>),
                EncounterFixture::new("enc-4", "finished", "Patient/patient-4")
                    .with_class("EMER")
                    .with_practitioner("Practitioner/pract-2")
                    .with_period("2024-01-20T11:00:00Z", Some("2024-01-20T15:00:00Z")),
                EncounterFixture::new("enc-5", "cancelled", "Patient/patient-5")
                    .with_class("AMB")
                    .with_period("2024-01-05T08:00:00Z", Some("2024-01-05T08:30:00Z")),
            ],
        }
    }

    /// Creates a minimal set of fixtures for fast tests.
    pub fn minimal() -> Self {
        Self {
            patients: vec![
                PatientFixture::new("patient-1", "Smith").with_given(vec!["John"]),
            ],
            observations: vec![
                ObservationFixture::new("obs-1", "8867-4", "Patient/patient-1")
                    .with_value(72.0, "bpm"),
            ],
            organizations: vec![OrganizationFixture::new("org-1", "Test Hospital")],
            practitioners: vec![],
            encounters: vec![],
        }
    }

    /// Creates fixtures specifically for chained search testing.
    pub fn for_chained_search() -> Self {
        Self {
            patients: vec![
                PatientFixture::new("patient-chain-1", "ChainTest")
                    .with_given(vec!["Alice"])
                    .with_organization("Organization/org-chain-1"),
                PatientFixture::new("patient-chain-2", "ChainTest")
                    .with_given(vec!["Bob"])
                    .with_organization("Organization/org-chain-2"),
                PatientFixture::new("patient-chain-3", "Other")
                    .with_given(vec!["Charlie"])
                    .with_organization("Organization/org-chain-1"),
            ],
            observations: vec![
                ObservationFixture::new("obs-chain-1", "8867-4", "Patient/patient-chain-1"),
                ObservationFixture::new("obs-chain-2", "8867-4", "Patient/patient-chain-2"),
                ObservationFixture::new("obs-chain-3", "8310-5", "Patient/patient-chain-3"),
            ],
            organizations: vec![
                OrganizationFixture::new("org-chain-1", "Chain Hospital A"),
                OrganizationFixture::new("org-chain-2", "Chain Hospital B"),
            ],
            practitioners: vec![],
            encounters: vec![],
        }
    }

    /// Creates fixtures for terminology/hierarchy testing.
    pub fn for_terminology() -> Self {
        // Observations with hierarchical LOINC codes
        Self {
            patients: vec![PatientFixture::new("patient-term", "TermTest")],
            observations: vec![
                // Heart rate family
                ObservationFixture::new("obs-term-1", "8867-4", "Patient/patient-term")
                    .with_display("Heart rate"),
                ObservationFixture::new("obs-term-2", "8889-8", "Patient/patient-term")
                    .with_display("Heart rate rhythm"),
                // Vital signs category
                ObservationFixture::new("obs-term-3", "8310-5", "Patient/patient-term")
                    .with_display("Body temperature"),
                ObservationFixture::new("obs-term-4", "8302-2", "Patient/patient-term")
                    .with_display("Body height"),
            ],
            organizations: vec![],
            practitioners: vec![],
            encounters: vec![],
        }
    }

    /// Adds a patient fixture.
    pub fn with_patient(mut self, patient: PatientFixture) -> Self {
        self.patients.push(patient);
        self
    }

    /// Adds an observation fixture.
    pub fn with_observation(mut self, observation: ObservationFixture) -> Self {
        self.observations.push(observation);
        self
    }

    /// Adds an organization fixture.
    pub fn with_organization(mut self, organization: OrganizationFixture) -> Self {
        self.organizations.push(organization);
        self
    }

    /// Adds a practitioner fixture.
    pub fn with_practitioner(mut self, practitioner: PractitionerFixture) -> Self {
        self.practitioners.push(practitioner);
        self
    }

    /// Adds an encounter fixture.
    pub fn with_encounter(mut self, encounter: EncounterFixture) -> Self {
        self.encounters.push(encounter);
        self
    }

    /// Returns the total count of all fixtures.
    pub fn total_count(&self) -> usize {
        self.patients.len()
            + self.observations.len()
            + self.organizations.len()
            + self.practitioners.len()
            + self.encounters.len()
    }

    /// Returns all fixtures as JSON values with their resource types.
    pub fn all_resources(&self) -> Vec<(&str, String, Value)> {
        let mut resources = Vec::new();

        for p in &self.patients {
            resources.push(("Patient", p.id.clone(), p.to_json()));
        }
        for o in &self.observations {
            resources.push(("Observation", o.id.clone(), o.to_json()));
        }
        for o in &self.organizations {
            resources.push(("Organization", o.id.clone(), o.to_json()));
        }
        for p in &self.practitioners {
            resources.push(("Practitioner", p.id.clone(), p.to_json()));
        }
        for e in &self.encounters {
            resources.push(("Encounter", e.id.clone(), e.to_json()));
        }

        resources
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_patient_fixture_to_json() {
        let patient = PatientFixture::new("test-1", "Test")
            .with_given(vec!["John"])
            .with_birth_date("1980-01-01")
            .with_gender("male");

        let json = patient.to_json();

        assert_eq!(json["resourceType"], "Patient");
        assert_eq!(json["id"], "test-1");
        assert_eq!(json["name"][0]["family"], "Test");
        assert_eq!(json["birthDate"], "1980-01-01");
    }

    #[test]
    fn test_observation_fixture_to_json() {
        let obs = ObservationFixture::new("obs-1", "8867-4", "Patient/p1")
            .with_value(72.0, "bpm");

        let json = obs.to_json();

        assert_eq!(json["resourceType"], "Observation");
        assert_eq!(json["code"]["coding"][0]["code"], "8867-4");
        assert_eq!(json["valueQuantity"]["value"], 72.0);
    }

    #[test]
    fn test_rich_fixtures() {
        let fixtures = TestFixtures::rich();

        assert_eq!(fixtures.patients.len(), 5);
        assert_eq!(fixtures.observations.len(), 10);
        assert_eq!(fixtures.organizations.len(), 3);
        assert_eq!(fixtures.practitioners.len(), 3);
        assert_eq!(fixtures.encounters.len(), 5);
    }

    #[test]
    fn test_minimal_fixtures() {
        let fixtures = TestFixtures::minimal();

        assert_eq!(fixtures.patients.len(), 1);
        assert_eq!(fixtures.observations.len(), 1);
        assert_eq!(fixtures.organizations.len(), 1);
    }

    #[test]
    fn test_all_resources() {
        let fixtures = TestFixtures::minimal();
        let resources = fixtures.all_resources();

        assert_eq!(resources.len(), 3);
        assert_eq!(resources[0].0, "Patient");
        assert_eq!(resources[1].0, "Observation");
        assert_eq!(resources[2].0, "Organization");
    }
}
