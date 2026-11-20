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

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let node = NodeTcpConfigBuilder::new()
        .with_contact_points(vec!["127.0.0.1:59042".into()])
        .with_version(Version::V5)
        .build()
        .await
        .unwrap();
    let session = TcpSessionBuilder::new(TopologyAwareLoadBalancingStrategy::new(None, false), node)
        .with_retry_policy(Box::new(DefaultRetryPolicy))
        .with_keyspace("item5_iw".to_string())
        .build()
        .await
        .unwrap();
    let pi: PreparedQuery = session.prepare(INSERT).await.unwrap();
    let pd: PreparedQuery = session.prepare(DELETE).await.unwrap();
    let params = |v: Vec<Value>| {
        StatementParamsBuilder::new()
            .with_consistency(Consistency::LocalQuorum)
            .idempotent(true)
            .with_values(QueryValues::SimpleValues(v))
            .build()
    };
    let bparams = StatementParamsBuilder::new().idempotent(true).build();
    let iters = 300usize;
    let warm = 30usize;
    let mut seq = 0i64;
    for k in [1usize, 8, 16, 32, 64] {
        let mut results: Vec<(&str, Vec<Duration>)> = vec![];
        for scenario in ["sequential", "concurrent", "logged", "unlogged", "logged ins+del (A: 2 batches)", "logged one batch ins+del", "concurrent ins then concurrent del", "pairs: per-partition batch, concurrent", "chains: per key ins then del, concurrent"] {
            let mut lat = vec![];
            for it in 0..(iters + warm) {
                seq += 1;
                let id = format!("r{seq:010}");
                let lu = 1_700_000_000_000 + seq;
                let codes: Vec<String> = (0..k).map(|j| format!("c{:04}", (seq as usize * 7 + j) % 1000)).collect();
                let t0 = Instant::now();
                match scenario {
                    "sequential" => {
                        for c in &codes {
                            session.exec_with_params(&pi, &params(ins(&id, c, lu))).await.unwrap();
                        }
                    }
                    "concurrent" => {
                        let ps: Vec<_> = codes.iter().map(|c| params(ins(&id, c, lu))).collect();
                        let futs = ps.iter().map(|p| session.exec_with_params(&pi, p));
                        for r in join_all(futs).await {
                            r.unwrap();
                        }
                    }
                    "logged" | "unlogged" => {
                        let mut b = BatchQueryBuilder::new()
                            .with_batch_type(if scenario == "logged" { BatchType::Logged } else { BatchType::Unlogged })
                            .with_consistency(Consistency::LocalQuorum);
                        for c in &codes {
                            b = b.add_query_prepared(&pi, QueryValues::SimpleValues(ins(&id, c, lu)));
                        }
                        session.batch_with_params(b.build().unwrap(), &bparams).await.unwrap();
                    }
                    "logged ins+del (A: 2 batches)" => {
                        let mut b = BatchQueryBuilder::new().with_consistency(Consistency::LocalQuorum);
                        for c in &codes {
                            b = b.add_query_prepared(&pi, QueryValues::SimpleValues(ins(&id, c, lu + 1)));
                        }
                        session.batch_with_params(b.build().unwrap(), &bparams).await.unwrap();
                        let mut d = BatchQueryBuilder::new().with_consistency(Consistency::LocalQuorum);
                        for c in &codes {
                            d = d.add_query_prepared(&pd, QueryValues::SimpleValues(del(&id, c, lu, (lu + 1) * 1000)));
                        }
                        session.batch_with_params(d.build().unwrap(), &bparams).await.unwrap();
                    }
                    "logged one batch ins+del" => {
                        let mut b = BatchQueryBuilder::new().with_consistency(Consistency::LocalQuorum);
                        for c in &codes {
                            b = b.add_query_prepared(&pi, QueryValues::SimpleValues(ins(&id, c, lu + 1)));
                            b = b.add_query_prepared(&pd, QueryValues::SimpleValues(del(&id, c, lu, (lu + 1) * 1000)));
                        }
                        session.batch_with_params(b.build().unwrap(), &bparams).await.unwrap();
                    }
                    "pairs: per-partition batch, concurrent" => {
                        let batches: Vec<_> = codes
                            .iter()
                            .map(|c| {
                                BatchQueryBuilder::new()
                                    .with_batch_type(BatchType::Unlogged)
                                    .with_consistency(Consistency::LocalQuorum)
                                    .add_query_prepared(&pi, QueryValues::SimpleValues(ins(&id, c, lu + 1)))
                                    .add_query_prepared(&pd, QueryValues::SimpleValues(del(&id, c, lu, (lu + 1) * 1000)))
                                    .build()
                                    .unwrap()
                            })
                            .collect();
                        for r in join_all(batches.into_iter().map(|b| session.batch_with_params(b, &bparams))).await {
                            r.unwrap();
                        }
                    }
                    "chains: per key ins then del, concurrent" => {
                        let chains = codes.iter().map(|c| {
                            let pi = &pi;
                            let pd = &pd;
                            let session = &session;
                            let id = id.clone();
                            let c = c.clone();
                            async move {
                                session.exec_with_params(pi, &params(ins(&id, &c, lu + 1))).await.unwrap();
                                session.exec_with_params(pd, &params(del(&id, &c, lu, (lu + 1) * 1000))).await.unwrap();
                            }
                        });
                        join_all(chains).await;
                    }
                    _ => {
                        let ps: Vec<_> = codes.iter().map(|c| params(ins(&id, c, lu + 1))).collect();
                        for r in join_all(ps.iter().map(|p| session.exec_with_params(&pi, p))).await {
                            r.unwrap();
                        }
                        let ds: Vec<_> = codes.iter().map(|c| params(del(&id, c, lu, (lu + 1) * 1000))).collect();
                        for r in join_all(ds.iter().map(|p| session.exec_with_params(&pd, p))).await {
                            r.unwrap();
                        }
                    }
                }
                if it >= warm {
                    lat.push(t0.elapsed());
                }
            }
            results.push((scenario, lat));
        }
        println!("--- rows per write k={k} (token_code_index, one partition per row; {iters} samples) ---");
        for (s, mut l) in results {
            println!("  {s:40} p50 {:6.2} ms  p95 {:6.2} ms  p99 {:6.2} ms", pct(&mut l, 0.5), pct(&mut l, 0.95), pct(&mut l, 0.99));
        }
    }
}
