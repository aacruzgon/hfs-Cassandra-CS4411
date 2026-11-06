//! A per-operation latency report for CI: a smoke trend, **not** the Step 8
//! benchmark (`docs/Cassandra/development-roadmap.md`, Step 8).
//!
//! Included by `tests/cassandra_tests.rs`. It runs only when
//! `HFS_TEST_LATENCY_REPORT` names an output directory, and otherwise returns
//! at once, so a plain `cargo test` is unchanged. It is a report, not a
//! check: no figure fails it, and it fails only when an operation itself
//! fails.
//!
//! On a fresh tenant ([`unique_tenant`]; ADR-aacruzgon-0020, rule 2), one
//! operation at a time, through [`CassandraBackend`]:
//! - [`CREATES`] `create`s of a small Patient, after [`WARM_UP`] unmeasured
//!   ones that prepare the statements;
//! - one `read` of each created id;
//! - [`COUNTS`] `count(tenant, Some("Patient"))`s.
//!
//! It writes `cassandra-latency.json` and `cassandra-latency.md` into the
//! directory: p50, p95, p99 and max per operation (nearest rank), with the
//! sample size. The node is a single one, the build is whatever `cargo test`
//! built (debug in CI), and the operations run one after another, so the
//! figures follow trends between commits on one runner; they are not
//! throughput or a production latency.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use helios_fhir::FhirVersion;
use helios_persistence::core::ResourceStorage;
use serde_json::json;

use crate::backend_skeleton::backend;
use crate::cassandra_node::{self, unique_tenant};

/// The variable naming the directory the report is written to.
pub const REPORT_VAR: &str = "HFS_TEST_LATENCY_REPORT";

/// Measured `create`s, and so measured `read`s.
const CREATES: usize = 200;
/// Unmeasured `create`s first: the first of each statement prepares it.
const WARM_UP: usize = 10;
/// Measured `count`s of the tenant's Patients.
const COUNTS: usize = 20;

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
}

impl Figures {
    fn json(&self) -> serde_json::Value {
        let mut sorted = self.samples.clone();
        sorted.sort_unstable();
        json!({
            "operation": self.operation,
            "n": sorted.len(),
            "p50_ms": ms(percentile(&sorted, 0.50)),
            "p95_ms": ms(percentile(&sorted, 0.95)),
            "p99_ms": ms(percentile(&sorted, 0.99)),
            "max_ms": ms(*sorted.last().expect("samples")),
        })
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

#[tokio::test]
async fn cassandra_latency_report() {
    let Some(dir) = std::env::var_os(REPORT_VAR).map(PathBuf::from) else {
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

    for i in 0..WARM_UP {
        backend
            .create(&tenant, "Patient", patient(i), version)
            .await
            .unwrap_or_else(|e| panic!("warm-up create {i}: {e}"));
    }

    let mut creates = Vec::with_capacity(CREATES);
    let mut ids = Vec::with_capacity(CREATES);
    for i in 0..CREATES {
        let t = Instant::now();
        let stored = backend
            .create(&tenant, "Patient", patient(WARM_UP + i), version)
            .await
            .unwrap_or_else(|e| panic!("create {i}: {e}"));
        creates.push(t.elapsed());
        ids.push(stored.id().to_string());
    }

    let mut reads = Vec::with_capacity(CREATES);
    for id in &ids {
        let t = Instant::now();
        let found = backend
            .read(&tenant, "Patient", id)
            .await
            .unwrap_or_else(|e| panic!("read {id}: {e}"));
        reads.push(t.elapsed());
        assert!(
            found.is_some(),
            "Patient/{id} was created but reads as absent"
        );
    }

    let mut counts = Vec::with_capacity(COUNTS);
    for i in 0..COUNTS {
        let t = Instant::now();
        let n = backend
            .count(&tenant, Some("Patient"))
            .await
            .unwrap_or_else(|e| panic!("count {i}: {e}"));
        counts.push(t.elapsed());
        assert_eq!(n, (WARM_UP + CREATES) as u64, "count {i}");
    }
    let total = started.elapsed();

    let figures = [
        Figures {
            operation: "create",
            samples: creates,
        },
        Figures {
            operation: "read",
            samples: reads,
        },
        Figures {
            operation: "count",
            samples: counts,
        },
    ];
    let rows: Vec<serde_json::Value> = figures.iter().map(Figures::json).collect();
    let profile = if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    };
    let report = json!({
        "report": "cassandra-latency",
        "caveat": "single node, debug build, not the Step 8 benchmark",
        "through": "CassandraBackend (helios-persistence), one operation at a time",
        "build_profile": profile,
        "contact_point": node.contact_point(),
        "warm_up_creates": WARM_UP,
        "total_s": (total.as_secs_f64() * 10.0).round() / 10.0,
        "operations": rows,
    });

    let mut md = format!(
        "_Single node, {profile} build, not the Step 8 benchmark._ Through \
         `CassandraBackend`, one operation at a time, on a fresh tenant, after \
         {WARM_UP} unmeasured creates. Total {:.1} s.\n\n\
         | Operation | n | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) |\n\
         | --- | ---: | ---: | ---: | ---: | ---: |\n",
        total.as_secs_f64()
    );
    for row in &rows {
        md.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} |\n",
            row["operation"].as_str().unwrap(),
            row["n"],
            row["p50_ms"],
            row["p95_ms"],
            row["p99_ms"],
            row["max_ms"],
        ));
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
