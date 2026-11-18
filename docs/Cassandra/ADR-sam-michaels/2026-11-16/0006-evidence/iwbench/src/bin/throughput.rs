//! Scratch latency probe for item 5.1, keyspace item5_iw only.
//! Compares ways of sending one write's index rows on the single node.

use std::time::{Duration, Instant};

use cdrs_tokio::cluster::session::{SessionBuilder, TcpSessionBuilder};
use cdrs_tokio::cluster::NodeTcpConfigBuilder;
use cdrs_tokio::consistency::Consistency;
use cdrs_tokio::frame::message_batch::BatchType;
use cdrs_tokio::frame::Version;
use cdrs_tokio::load_balancing::TopologyAwareLoadBalancingStrategy;
use cdrs_tokio::query::{BatchQueryBuilder, PreparedQuery, QueryValues};
use cdrs_tokio::retry::DefaultRetryPolicy;
use cdrs_tokio::statement::StatementParamsBuilder;
use cdrs_tokio::types::value::Value;
use futures::future::join_all;

const INSERT: &str = "INSERT INTO token_code_index (tenant_id, resource_type, param, code, last_updated, id, system, version_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";
const DELETE: &str = "DELETE FROM token_code_index USING TIMESTAMP ? WHERE tenant_id = ? AND resource_type = ? AND param = ? AND code = ? AND last_updated = ? AND id = ? AND system = ?";

fn ins(id: &str, code: &str, lu: i64) -> Vec<Value> {
    vec![
        Value::from("test-iw-latency-0123456789abcdef0123456789ab"),
        Value::from("Observation"),
        Value::from("code"),
        Value::from(code),
        Value::from(lu),
        Value::from(id),
        Value::from("http://loinc.org"),
        Value::from(1i32),
        Value::from(lu * 1000),
    ]
}
fn del(id: &str, code: &str, lu: i64, ts: i64) -> Vec<Value> {
    vec![
        Value::from(ts),
        Value::from("test-iw-latency-0123456789abcdef0123456789ab"),
        Value::from("Observation"),
        Value::from("code"),
        Value::from(code),
        Value::from(lu),
        Value::from(id),
        Value::from("http://loinc.org"),
    ]
}

fn pct(v: &mut [Duration], p: f64) -> f64 {
    v.sort();
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i].as_secs_f64() * 1000.0
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let node = NodeTcpConfigBuilder::new()
        .with_contact_points(vec!["127.0.0.1:59042".into()])
        .with_version(Version::V5)
        .build()
        .await
        .unwrap();
    let session = std::sync::Arc::new(TcpSessionBuilder::new(TopologyAwareLoadBalancingStrategy::new(None, false), node)
        .with_retry_policy(Box::new(DefaultRetryPolicy))
        .with_keyspace("item5_iw".to_string())
        .build()
        .await
        .unwrap());
    let pi = std::sync::Arc::new(session.prepare(INSERT).await.unwrap());
    let pd = std::sync::Arc::new(session.prepare(DELETE).await.unwrap());
    let k = 16usize;
    let writers = 32usize;
    let per: usize = std::env::var("PER").ok().and_then(|v| v.parse().ok()).unwrap_or(500);
    let scenarios = vec!["sequential", "logged ins+del (A: 2 batches)", "logged one batch ins+del", "unlogged one batch ins+del", "concurrent ins then concurrent del", "pairs: per-partition batch, concurrent", "chains: per key ins then del, concurrent"];
    let n_scenarios = scenarios.len();
    let pass = usize::from(std::env::var("REVERSE").is_ok());
    // Each scenario of each pass writes its own range of row keys (ids, timestamps and
    // codes), so no scenario overwrites or deletes over another's rows. BASE shifts every range.
    let mut scenarios: Vec<(usize, &str)> = scenarios.into_iter().enumerate().collect();
    if pass == 1 { scenarios.reverse(); }
    let base: usize = std::env::var("BASE").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    for (si, scenario) in scenarios {
        let range = (pass * n_scenarios + si) * 100_000_000;
        let t0 = Instant::now();
        let mut handles = vec![];
        for w in 0..writers {
            let session = session.clone();
            let pi = pi.clone();
            let pd = pd.clone();
            handles.push(tokio::spawn(async move {
                let params = |v: Vec<Value>| {
                    StatementParamsBuilder::new()
                        .with_consistency(Consistency::LocalQuorum)
                        .idempotent(true)
                        .with_values(QueryValues::SimpleValues(v))
                        .build()
                };
                let bparams = StatementParamsBuilder::new().idempotent(true).build();
                let mut lat = vec![];
                for i in 0..per {
                    let seq = (range + w * 1_000_000 + i + base) as i64 + 50_000_000;
                    let id = format!("t{seq:010}");
                    let lu = 1_700_100_000_000 + seq;
                    let codes: Vec<String> = (0..k).map(|j| format!("c{:04}", (seq as usize * 7 + j) % 1000)).collect();
                    let t = Instant::now();
                    match scenario {
                        "sequential" => {
                            for c in &codes {
                                session.exec_with_params(&pi, &params(ins(&id, c, lu + 1))).await.unwrap();
                            }
                            for c in &codes {
                                session.exec_with_params(&pd, &params(del(&id, c, lu, (lu + 1) * 1000))).await.unwrap();
                            }
                        }
                        "logged ins+del (A: 2 batches)" => {
                            let mut b = BatchQueryBuilder::new().with_consistency(Consistency::LocalQuorum);
                            for c in &codes { b = b.add_query_prepared(&pi, QueryValues::SimpleValues(ins(&id, c, lu + 1))); }
                            session.batch_with_params(b.build().unwrap(), &bparams).await.unwrap();
                            let mut d = BatchQueryBuilder::new().with_consistency(Consistency::LocalQuorum);
                            for c in &codes { d = d.add_query_prepared(&pd, QueryValues::SimpleValues(del(&id, c, lu, (lu + 1) * 1000))); }
                            session.batch_with_params(d.build().unwrap(), &bparams).await.unwrap();
                        }
                        "logged one batch ins+del" | "unlogged one batch ins+del" => {
                            let mut b = BatchQueryBuilder::new().with_consistency(Consistency::LocalQuorum)
                                .with_batch_type(if scenario.starts_with("logged") { BatchType::Logged } else { BatchType::Unlogged });
                            for c in &codes {
                                b = b.add_query_prepared(&pi, QueryValues::SimpleValues(ins(&id, c, lu + 1)));
                                b = b.add_query_prepared(&pd, QueryValues::SimpleValues(del(&id, c, lu, (lu + 1) * 1000)));
                            }
                            session.batch_with_params(b.build().unwrap(), &bparams).await.unwrap();
                        }
                        "concurrent ins then concurrent del" => {
                            let ps: Vec<_> = codes.iter().map(|c| params(ins(&id, c, lu + 1))).collect();
                            for r in join_all(ps.iter().map(|p| session.exec_with_params(&pi, p))).await { r.unwrap(); }
                            let ds: Vec<_> = codes.iter().map(|c| params(del(&id, c, lu, (lu + 1) * 1000))).collect();
                            for r in join_all(ds.iter().map(|p| session.exec_with_params(&pd, p))).await { r.unwrap(); }
                        }
                        "pairs: per-partition batch, concurrent" => {
                            let batches: Vec<_> = codes.iter().map(|c| {
                                BatchQueryBuilder::new().with_batch_type(BatchType::Unlogged).with_consistency(Consistency::LocalQuorum)
                                    .add_query_prepared(&pi, QueryValues::SimpleValues(ins(&id, c, lu + 1)))
                                    .add_query_prepared(&pd, QueryValues::SimpleValues(del(&id, c, lu, (lu + 1) * 1000)))
                                    .build().unwrap()
                            }).collect();
                            for r in join_all(batches.into_iter().map(|b| session.batch_with_params(b, &bparams))).await { r.unwrap(); }
                        }
                        _ => {
                            let chains = codes.iter().map(|c| {
                                let (pi, pd, session, id, c) = (&pi, &pd, &session, id.clone(), c.clone());
                                async move {
                                    session.exec_with_params(pi, &params(ins(&id, &c, lu + 1))).await.unwrap();
                                    session.exec_with_params(pd, &params(del(&id, &c, lu, (lu + 1) * 1000))).await.unwrap();
                                }
                            });
                            join_all(chains).await;
                        }
                    }
                    lat.push(t.elapsed());
                }
                lat
            }));
        }
        let mut all = vec![];
        for h in handles { all.extend(h.await.unwrap()); }
        let secs = t0.elapsed().as_secs_f64();
        let n = all.len();
        println!("  {scenario:42} {:7.0} writes/s  p50 {:6.2} ms  p95 {:6.2} ms  p99 {:6.2} ms",
            n as f64 / secs, pct(&mut all, 0.5), pct(&mut all, 0.95), pct(&mut all, 0.99));
    }
}
