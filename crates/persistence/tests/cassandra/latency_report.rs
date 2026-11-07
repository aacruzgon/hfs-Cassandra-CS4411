//! A per-operation latency report for CI: a smoke trend, **not** the Step 8
//! benchmark (`docs/Cassandra/development-roadmap.md`, Step 8).
//!
//! Included by `tests/cassandra_tests.rs`. It runs only when
//! `HFS_TEST_LATENCY_REPORT` names an output directory, and otherwise returns
//! at once, so a plain `cargo test` is unchanged. It is a report, not a
//! check: no figure fails it, and it fails only when an operation itself
//! fails. Every operation it measures is real from roadmap Step 3 on, so any
//! error fails it, a 501 (`UnsupportedCapability`) included: that would be a
//! regression, not a layer still to land (ADR-aacruzgon-0059).
//!
//! On a fresh tenant ([`unique_tenant`]; ADR-aacruzgon-0020, rule 2), one
//! operation at a time, through [`CassandraBackend`] (Step 3 plan, report R1):
//! - [`CREATES`] `create`s of a small Patient, after [`WARM_UP`] unmeasured
//!   ones that prepare the statements; one `read` of each created id;
//!   [`COUNTS`] `count`s;
//! - `exists` of each created id, and `read_batch` of 10 and of 100 ids;
//! - [`SAMPLES`] each of `update`, `vread` (version 1), `list_versions`,
//!   `create_or_update` on new ids (the create branch) and again on them (the
//!   update branch), `delete`, `delete_versioned`, and `create_or_update` on
//!   the deleted ids (the revive branch). Each of these first runs
//!   [`WARM_EACH`] unmeasured calls, which prepare its statements.
//!
//! No two calls run at once. `update`, `vread`, `list_versions`, `delete` and
//! the revive work in turn on one slice of the created ids, `delete_versioned`
//! on another, and the two other `create_or_update` branches on ids of their
//! own. Each call's result is checked against the trait's contract (a new
//! version, the `created` flag, a found version), so a wrong answer fails
//! the report like a failed call does.
//!
//! **Statements and CAS outcomes per operation** (reports R5 and R2). Around
//! each measured stretch it reads the backend's per-kind statement counters
//! and CAS outcome counters, through [`counters`], the one place that knows
//! their API (`CassandraBackend::statement_counts` and `cas_stats`, from Step 3
//! layer L1). For each operation, and for the
//! whole report, it prints a `[cas case=… key=value …]` line, the shape the
//! engine's own node tests print (`CasStats`'s `Display`), which
//! `ci-report.py cas` tabulates.
//!
//! It writes `cassandra-latency.json` and `cassandra-latency.md` into the
//! directory: p50, p95, p99 and max per operation (nearest rank), with the
//! sample size. The node is a single one, the build is whatever `cargo test`
//! built (debug in CI), and the operations run one after another, so the
//! figures follow trends between commits on one runner; they are not
//! throughput or a production latency.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use helios_fhir::FhirVersion;
use helios_persistence::backends::cassandra::CassandraBackend;
use helios_persistence::core::{ResourceStorage, VersionedStorage};
use helios_persistence::error::StorageResult;
use helios_persistence::types::StoredResource;
use serde_json::json;

use crate::backend_skeleton::backend;
use crate::cassandra_node::{self, unique_tenant};

/// The variable naming the directory the report is written to.
pub const REPORT_VAR: &str = "HFS_TEST_LATENCY_REPORT";

/// Measured `create`s, and so measured `read`s and `exists`.
const CREATES: usize = 200;
/// Unmeasured `create`s first: the first of each statement prepares it.
const WARM_UP: usize = 10;
/// Measured `count`s of the tenant's Patients.
const COUNTS: usize = 20;
/// Measured calls of each Step 3 operation.
const SAMPLES: usize = 100;
/// Unmeasured calls before each Step 3 write operation's measured ones.
const WARM_EACH: usize = 3;
/// `read_batch` calls per batch size.
const BATCHES: usize = 20;

/// The one place that reads the engine's counters (Step 3 plan, section 7):
/// [`CassandraBackend::cas_stats`] and [`CassandraBackend::statement_counts`],
/// from Step 3 layer L1. Everything else in the report sees name-to-count
/// maps, so when those types change, only this module does. Each returns an
/// `Option`, so a backend without counters would only blank the columns.
mod counters {
    use std::collections::BTreeMap;

    use helios_persistence::backends::cassandra::CassandraBackend;

    /// Counter name to value.
    pub type Counts = BTreeMap<String, u64>;

    /// Statements sent so far, by kind (R5): `read`, `serial_read`, `write`
    /// and `cas`, every request counted, paged pages included.
    pub fn statements(backend: &CassandraBackend) -> Option<Counts> {
        let counts = backend.statement_counts();
        Some(
            [
                ("read", counts.read),
                ("serial_read", counts.serial_read),
                ("write", counts.write),
                ("cas", counts.cas),
            ]
            .into_iter()
            .map(|(kind, n)| (kind.to_string(), n))
            .collect(),
        )
    }

    /// CAS outcomes so far (R2, R3, R8), every `CasStats` counter by its
    /// field name (`CasStats::pairs`): attempts, applied, misses by kind,
    /// unknown outcomes by trigger, serial and version reads, re-runs,
    /// undecidable, `miss_budget_exhausted`, `post_cas_failures`. The names
    /// become `ci-report.py cas` columns; it warns on any `undecidable`,
    /// `exhausted` or `post_cas` counter above 0.
    pub fn cas(backend: &CassandraBackend) -> Option<Counts> {
        Some(
            backend
                .cas_stats()
                .pairs()
                .into_iter()
                .map(|(name, n)| (name.to_string(), n))
                .collect(),
        )
    }

    /// `after - before`, per counter.
    pub fn delta(before: &Counts, after: &Counts) -> Counts {
        after
            .iter()
            .map(|(name, value)| {
                let start = before.get(name).copied().unwrap_or(0);
                (name.clone(), value.saturating_sub(start))
            })
            .collect()
    }
}

/// Both counter sets at one moment.
#[derive(Clone)]
struct Snapshot {
    statements: Option<counters::Counts>,
    cas: Option<counters::Counts>,
}

impl Snapshot {
    fn take(backend: &CassandraBackend) -> Self {
        Snapshot {
            statements: counters::statements(backend),
            cas: counters::cas(backend),
        }
    }
}

/// Nearest-rank percentile of sorted samples, `p` in (0, 1].
fn percentile(sorted: &[Duration], p: f64) -> Duration {
    let rank = ((p * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

fn ms(d: Duration) -> f64 {
    (d.as_secs_f64() * 1_000_000.0).round() / 1000.0
}

/// One operation's figures.
struct Figures {
    operation: &'static str,
    samples: Vec<Duration>,
    /// Statements per measured call, by kind.
    statements: Option<BTreeMap<String, f64>>,
    /// CAS outcome counters over the measured calls.
    cas: Option<counters::Counts>,
}

impl Figures {
    fn json(&self) -> serde_json::Value {
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        json!({
            "operation": self.operation,
            "status": "measured",
            "n": sorted.len(),
            "p50_ms": ms(percentile(&sorted, 0.50)),
            "p95_ms": ms(percentile(&sorted, 0.95)),
            "p99_ms": ms(percentile(&sorted, 0.99)),
            "max_ms": ms(*sorted.last().expect("samples")),
            "statements_per_op": self.statements,
            "cas": self.cas,
        })
    }

    /// The `[cas case=…]` line for `ci-report.py cas`, if there are counters.
    /// Only for an operation that sent a CAS or touched a CAS counter, so the
    /// read operations' all-zero lines do not crowd the report.
    fn cas_line(&self) -> Option<String> {
        self.cas
            .as_ref()
            .filter(|cas| cas.values().any(|n| *n > 0))
            .map(|cas| cas_line(&format!("latency-report:{}", self.operation), cas))
    }
}

/// `[cas case=<case> key=value …]`, the shape the engine's own node tests
/// print. The case is one token: anything but letters, digits and `_:.-`
/// becomes `-`, so `read_batch (10 ids)` reads `read_batch-10-ids`.
fn cas_line(case: &str, cas: &counters::Counts) -> String {
    let mut token = String::with_capacity(case.len());
    for c in case.chars() {
        let keep = c.is_ascii_alphanumeric() || "_:.-".contains(c);
        if keep {
            token.push(c);
        } else if !token.ends_with('-') {
            token.push('-');
        }
    }
    let token = token.trim_end_matches('-');
    let pairs: Vec<String> = cas.iter().map(|(k, v)| format!("{k}={v}")).collect();
    format!("[cas case={token} {}]", pairs.join(" "))
}

/// One operation being measured: its first [`Stage::warm`] calls are not
/// timed, and the counters are read when the timed calls start and end.
struct Stage<'a> {
    backend: &'a CassandraBackend,
    operation: &'static str,
    warm: usize,
    calls: usize,
    samples: Vec<Duration>,
    before: Option<Snapshot>,
}

impl<'a> Stage<'a> {
    fn new(backend: &'a CassandraBackend, operation: &'static str, warm: usize) -> Self {
        Stage {
            backend,
            operation,
            warm,
            calls: 0,
            samples: Vec::new(),
            before: (warm == 0).then(|| Snapshot::take(backend)),
        }
    }

    /// Times one call. Any error, a 501 included, is a broken measurement
    /// and panics.
    async fn call<T>(&mut self, what: &str, future: impl Future<Output = StorageResult<T>>) -> T {
        let started = Instant::now();
        let result = future.await;
        let took = started.elapsed();
        match result {
            Ok(value) => {
                if self.calls >= self.warm {
                    self.samples.push(took);
                }
                self.calls += 1;
                if self.calls == self.warm {
                    self.before = Some(Snapshot::take(self.backend));
                }
                value
            }
            Err(error) => panic!("{} {what}: {error}", self.operation),
        }
    }

    fn finish(self) -> Figures {
        assert!(
            !self.samples.is_empty(),
            "{}: no measured call",
            self.operation
        );
        let after = Snapshot::take(self.backend);
        let before = self.before.unwrap_or_else(|| after.clone());
        let n = self.samples.len() as f64;
        let statements = match (&before.statements, &after.statements) {
            (Some(b), Some(a)) => Some(
                counters::delta(b, a)
                    .into_iter()
                    .map(|(kind, count)| (kind, (count as f64 / n * 100.0).round() / 100.0))
                    .collect(),
            ),
            _ => None,
        };
        let cas = match (&before.cas, &after.cas) {
            (Some(b), Some(a)) => Some(counters::delta(b, a)),
            _ => None,
        };
        Figures {
            operation: self.operation,
            samples: self.samples,
            statements,
            cas,
        }
    }
}

fn patient(i: usize) -> serde_json::Value {
    json!({
        "resourceType": "Patient",
        "name": [{"family": format!("Latency-{i}")}],
        "gender": "female",
        "birthDate": "1970-01-01"
    })
}

fn patient_with_id(id: &str, i: usize) -> serde_json::Value {
    let mut body = patient(i);
    body["id"] = json!(id);
    body
}

#[tokio::test]
async fn cassandra_latency_report() {
    let Some(dir) = std::env::var_os(REPORT_VAR)
        .map(PathBuf::from)
        .filter(|dir| !dir.as_os_str().is_empty())
    else {
        // Not requested; deliberately no "Skipping" line, because this is a
        // report, not a live test that a node should have run.
        return;
    };
    // The report is asked for where a node is meant to be; a missing node is
    // a broken measurement, not a skip.
    let node = cassandra_node::node().await.unwrap_or_else(|| {
        panic!("{REPORT_VAR} is set, but no Cassandra node can be had (HFS_TEST_CASSANDRA_URL unset and no Docker)")
    });
    let backend = backend(node).await;
    let tenant = unique_tenant("latency");
    let version = FhirVersion::default_enabled();
    let started = Instant::now();
    let report_start = Snapshot::take(&backend);
    let mut figures = Vec::new();

    // create, after WARM_UP unmeasured ones. Every created resource is kept:
    // the later operations each take their own slice of them.
    let mut stage = Stage::new(&backend, "create", WARM_UP);
    let mut stored: Vec<StoredResource> = Vec::with_capacity(WARM_UP + CREATES);
    for i in 0..WARM_UP + CREATES {
        let resource = stage
            .call(
                &format!("{i}"),
                backend.create(&tenant, "Patient", patient(i), version),
            )
            .await;
        stored.push(resource);
    }
    figures.push(stage.finish());
    let measured_ids: Vec<String> = stored[WARM_UP..]
        .iter()
        .map(|r| r.id().to_string())
        .collect();

    let mut stage = Stage::new(&backend, "read", 0);
    for id in &measured_ids {
        let found = stage.call(id, backend.read(&tenant, "Patient", id)).await;
        assert!(
            found.is_some(),
            "Patient/{id} was created but reads as absent"
        );
    }
    figures.push(stage.finish());

    let mut stage = Stage::new(&backend, "count", 0);
    for i in 0..COUNTS {
        let n = stage
            .call(&format!("{i}"), backend.count(&tenant, Some("Patient")))
            .await;
        assert_eq!(n, (WARM_UP + CREATES) as u64, "count {i}");
    }
    figures.push(stage.finish());

    let mut stage = Stage::new(&backend, "exists", 0);
    for id in &measured_ids {
        let found = stage.call(id, backend.exists(&tenant, "Patient", id)).await;
        assert!(found, "Patient/{id} was created but does not exist");
    }
    figures.push(stage.finish());

    for (operation, size) in [("read_batch (10 ids)", 10), ("read_batch (100 ids)", 100)] {
        let mut stage = Stage::new(&backend, operation, 0);
        for i in 0..BATCHES {
            let ids: Vec<&str> = (0..size)
                .map(|k| measured_ids[(i * size + k) % measured_ids.len()].as_str())
                .collect();
            let found = stage
                .call(
                    &format!("{i}"),
                    backend.read_batch(&tenant, "Patient", &ids),
                )
                .await;
            assert_eq!(found.len(), size, "{operation} {i}");
        }
        figures.push(stage.finish());
    }

    // The Step 3 operations, each on its own ids. `stored[0..per]` is
    // updated, then deleted, then revived; `stored[per..2 * per]` is deleted
    // with delete_versioned.
    let per = WARM_EACH + SAMPLES;
    assert!(
        stored.len() >= 2 * per,
        "{} resources, need {}",
        stored.len(),
        2 * per
    );

    let mut stage = Stage::new(&backend, "update", WARM_EACH);
    for (i, current) in stored[..per].iter().enumerate() {
        let updated = stage
            .call(
                current.id(),
                backend.update(&tenant, current, patient_with_id(current.id(), 1000 + i)),
            )
            .await;
        assert_ne!(
            updated.version_id(),
            current.version_id(),
            "update of Patient/{}",
            current.id()
        );
    }
    figures.push(stage.finish());

    let mut stage = Stage::new(&backend, "vread (version 1)", WARM_EACH);
    for current in &stored[..per] {
        let found = stage
            .call(
                current.id(),
                backend.vread(&tenant, "Patient", current.id(), "1"),
            )
            .await;
        assert!(found.is_some(), "vread Patient/{}/_history/1", current.id());
    }
    figures.push(stage.finish());

    let mut stage = Stage::new(&backend, "list_versions", WARM_EACH);
    for current in &stored[..per] {
        let versions = stage
            .call(
                current.id(),
                backend.list_versions(&tenant, "Patient", current.id()),
            )
            .await;
        assert!(
            !versions.is_empty(),
            "list_versions Patient/{}",
            current.id()
        );
    }
    figures.push(stage.finish());

    // create_or_update on ids nobody created: the create branch; then on the
    // same ids again: the update branch.
    let upsert_ids: Vec<String> = (0..per).map(|i| format!("latency-upsert-{i}")).collect();
    for (operation, expect_created) in [
        ("create_or_update (create branch)", true),
        ("create_or_update (update branch)", false),
    ] {
        let mut stage = Stage::new(&backend, operation, WARM_EACH);
        for (i, id) in upsert_ids.iter().enumerate() {
            let (_, created) = stage
                .call(
                    id,
                    backend.create_or_update(
                        &tenant,
                        "Patient",
                        id,
                        patient_with_id(id, 2000 + i),
                        version,
                    ),
                )
                .await;
            assert_eq!(created, expect_created, "{operation} Patient/{id}");
        }
        figures.push(stage.finish());
    }

    let mut stage = Stage::new(&backend, "delete", WARM_EACH);
    let mut deleted = Vec::with_capacity(per);
    for current in &stored[..per] {
        stage
            .call(
                current.id(),
                backend.delete(&tenant, "Patient", current.id()),
            )
            .await;
        deleted.push(current.id().to_string());
    }
    figures.push(stage.finish());

    let mut stage = Stage::new(&backend, "delete_versioned", WARM_EACH);
    for current in &stored[per..2 * per] {
        let call = backend.delete_versioned(&tenant, "Patient", current.id(), current.version_id());
        stage.call(current.id(), call).await;
    }
    figures.push(stage.finish());

    let operation = "create_or_update (revive branch)";
    let mut stage = Stage::new(&backend, operation, WARM_EACH);
    for (i, id) in deleted.iter().enumerate() {
        let (_, created) = stage
            .call(
                id,
                backend.create_or_update(
                    &tenant,
                    "Patient",
                    id,
                    patient_with_id(id, 3000 + i),
                    version,
                ),
            )
            .await;
        assert!(created, "a revive of Patient/{id} is a create (201)");
    }
    figures.push(stage.finish());

    let total = started.elapsed();
    let report_end = Snapshot::take(&backend);
    let whole_cas = match (&report_start.cas, &report_end.cas) {
        (Some(b), Some(a)) => Some(counters::delta(b, a)),
        _ => None,
    };

    let rows: Vec<serde_json::Value> = figures.iter().map(Figures::json).collect();
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let counters_exposed = report_end.statements.is_some() || report_end.cas.is_some();
    let report = json!({
        "report": "cassandra-latency",
        "caveat": "single node, debug build, not the Step 8 benchmark",
        "through": "CassandraBackend (helios-persistence), one operation at a time",
        "build_profile": profile,
        "contact_point": node.contact_point(),
        "warm_up_creates": WARM_UP,
        "warm_up_per_write_operation": WARM_EACH,
        "counters_exposed": counters_exposed,
        "total_s": (total.as_secs_f64() * 10.0).round() / 10.0,
        "cas_whole_report": whole_cas,
        "operations": rows,
    });

    let mut md = format!(
        "_Single node, {profile} build, not the Step 8 benchmark._ Through \
         `CassandraBackend`, one operation at a time, on a fresh tenant, after \
         {WARM_UP} unmeasured creates and {WARM_EACH} unmeasured calls of each \
         later write operation. Total {:.1} s.\n\n\
         | Operation | n | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | Statements per call (R5) |\n\
         | --- | ---: | ---: | ---: | ---: | ---: | --- |\n",
        total.as_secs_f64()
    );
    for (row, figure) in rows.iter().zip(&figures) {
        let statements = match &figure.statements {
            Some(kinds) if !kinds.is_empty() => kinds
                .iter()
                .filter(|(_, n)| **n > 0.0)
                .map(|(kind, n)| format!("{kind} {n}"))
                .collect::<Vec<_>>()
                .join(", "),
            Some(_) => "none".to_string(),
            None => "–".to_string(),
        };
        md.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} |\n",
            figure.operation,
            row["n"],
            row["p50_ms"],
            row["p95_ms"],
            row["p99_ms"],
            row["max_ms"],
            statements,
        ));
    }
    if !counters_exposed {
        md.push_str(
            "\nStatements per call and CAS outcomes: the backend's counters could not be read.\n",
        );
    }

    // For `ci-report.py cas` (R2): per operation, then the whole report.
    for figure in &figures {
        if let Some(line) = figure.cas_line() {
            eprintln!("{line}");
        }
    }
    if let Some(cas) = &whole_cas {
        eprintln!("{}", cas_line("latency-report", cas));
    }

    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("create {}: {e}", dir.display()));
    let json_path = dir.join("cassandra-latency.json");
    let md_path = dir.join("cassandra-latency.md");
    std::fs::write(
        &json_path,
        serde_json::to_string_pretty(&report).unwrap() + "\n",
    )
    .unwrap_or_else(|e| panic!("write {}: {e}", json_path.display()));
    std::fs::write(&md_path, &md).unwrap_or_else(|e| panic!("write {}: {e}", md_path.display()));
    eprintln!("{md}");
    eprintln!("cassandra latency report written to {}", dir.display());
}

#[test]
fn percentile_is_nearest_rank() {
    let samples: Vec<Duration> = (1..=100).map(Duration::from_millis).collect();
    assert_eq!(percentile(&samples, 0.50), Duration::from_millis(50));
    assert_eq!(percentile(&samples, 0.95), Duration::from_millis(95));
    assert_eq!(percentile(&samples, 0.99), Duration::from_millis(99));
    assert_eq!(percentile(&samples, 1.0), Duration::from_millis(100));
    let one = [Duration::from_millis(7)];
    assert_eq!(percentile(&one, 0.5), Duration::from_millis(7));
}

#[test]
fn counter_delta_and_cas_line() {
    let before: counters::Counts = [("applied".to_string(), 3), ("attempts".to_string(), 4)].into();
    let after: counters::Counts = [
        ("applied".to_string(), 10),
        ("attempts".to_string(), 12),
        ("re_runs".to_string(), 1),
    ]
    .into();
    let delta = counters::delta(&before, &after);
    assert_eq!(delta["applied"], 7);
    assert_eq!(delta["attempts"], 8);
    assert_eq!(
        delta["re_runs"], 1,
        "a counter first seen after the start counts from 0"
    );
    assert_eq!(
        cas_line("latency-report:update", &delta),
        "[cas case=latency-report:update applied=7 attempts=8 re_runs=1]"
    );
    assert_eq!(
        cas_line("latency-report:read_batch (10 ids)", &delta),
        "[cas case=latency-report:read_batch-10-ids applied=7 attempts=8 re_runs=1]"
    );
}
