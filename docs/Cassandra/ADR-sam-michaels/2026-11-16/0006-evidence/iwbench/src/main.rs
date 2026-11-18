//! Scratch benchmark for item 5.1 (not part of the repository).
//! Extracts every resource with the shared extractor, maps the values to the
//! seven Cassandra row kinds with the walkthrough's P5 filters, and reports
//! key-set sizes, rows per table, batch sizes as Cassandra 5.0.9 computes them
//! (IMutation.dataSize), and extraction time.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use helios_fhir::FhirVersion;
use helios_persistence::search::{
    IMPLICIT_TOKEN_SYSTEM, IndexValue, SearchParameterExtractor, SearchParameterLoader,
    SearchParameterRegistry, StorageResolution, indexed_range,
};
use helios_persistence::types::SearchParamType;
use parking_lot::RwLock;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Key {
    ResourcesByType,
    TokenCode { param: String, code: String, system: String },
    TokenSystemCode { param: String, system: String, code: String },
    TokenSystem { param: String, system: String, code: String },
    Date { param: String, start: i64, end: i64 },
    DateByEnd { param: String, end: i64, start: i64 },
    Uri { param: String, uri: String },
}

impl Key {
    fn table(&self) -> &'static str {
        match self {
            Key::ResourcesByType => "resources_by_type",
            Key::TokenCode { .. } => "token_code_index",
            Key::TokenSystemCode { .. } => "token_system_code_index",
            Key::TokenSystem { .. } => "token_system_index",
            Key::Date { .. } => "date_index",
            Key::DateByEnd { .. } => "date_index_by_end",
            Key::Uri { .. } => "uri_index",
        }
    }
    fn is_date(&self) -> bool {
        matches!(self, Key::Date { .. } | Key::DateByEnd { .. })
    }
    /// Partition key beyond (tenant, type): what makes two rows share a mutation.
    fn partition(&self) -> Vec<String> {
        match self {
            Key::ResourcesByType => vec![],
            Key::TokenCode { param, code, .. } => vec![param.clone(), code.clone()],
            Key::TokenSystemCode { param, system, code } => {
                vec![param.clone(), system.clone(), code.clone()]
            }
            Key::TokenSystem { param, system, .. } => vec![param.clone(), system.clone()],
            Key::Date { param, .. } | Key::DateByEnd { param, .. } => vec![param.clone()],
            Key::Uri { param, uri } => vec![param.clone(), uri.clone()],
        }
    }
    /// Clustering bytes beyond id (and last_updated where the table has it).
    fn clustering_bytes(&self, id_len: usize) -> usize {
        match self {
            Key::ResourcesByType | Key::TokenSystemCode { .. } | Key::Uri { .. } => 8 + id_len,
            Key::TokenCode { system, .. } => 8 + id_len + system.len(),
            Key::TokenSystem { code, .. } => 8 + id_len + code.len(),
            Key::Date { .. } | Key::DateByEnd { .. } => 16 + id_len,
        }
    }
    /// Server data size of the row (5.0.9): 8 liveness + 13 row deletion +
    /// clustering + cells (20 + value).
    fn server_row(&self, id_len: usize, insert: bool) -> usize {
        let cells = if insert {
            match self {
                Key::ResourcesByType => 24 + 23,
                _ => 24,
            }
        } else {
            0
        };
        21 + self.clustering_bytes(id_len) + cells
    }
    /// Bind values of the statement, as bytes.
    fn binds(&self, tenant: usize, rtype: usize, id: usize, insert: bool) -> Vec<usize> {
        let mut v = vec![tenant, rtype];
        match self {
            Key::ResourcesByType => v.extend([8, id]),
            Key::TokenCode { param, code, system } => {
                v.extend([param.len(), code.len(), 8, id, system.len()])
            }
            Key::TokenSystemCode { param, system, code } => {
                v.extend([param.len(), system.len(), code.len(), 8, id])
            }
            Key::TokenSystem { param, system, code } => {
                v.extend([param.len(), system.len(), 8, id, code.len()])
            }
            Key::Date { param, .. } | Key::DateByEnd { param, .. } => {
                v.extend([param.len(), 8, id, 8])
            }
            Key::Uri { param, uri } => v.extend([param.len(), uri.len(), 8, id]),
        }
        if insert {
            v.push(4); // version_id
            if matches!(self, Key::ResourcesByType) {
                v.push(3); // fhir_version
            }
        }
        v.push(8); // USING TIMESTAMP ?
        v
    }
}

fn put_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}
fn put_text(out: &mut Vec<u8>, t: &str) {
    put_varint(out, t.len() as u64);
    out.extend_from_slice(t.as_bytes());
}
fn encode(keys: &[Key]) -> Vec<u8> {
    let mut out = vec![1u8];
    put_varint(&mut out, keys.len() as u64);
    for k in keys {
        match k {
            Key::ResourcesByType => out.push(1),
            Key::TokenCode { param, code, system } => {
                out.push(2);
                put_text(&mut out, param);
                put_text(&mut out, code);
                put_text(&mut out, system);
            }
            Key::TokenSystemCode { param, system, code } => {
                out.push(3);
                put_text(&mut out, param);
                put_text(&mut out, system);
                put_text(&mut out, code);
            }
            Key::TokenSystem { param, system, code } => {
                out.push(4);
                put_text(&mut out, param);
                put_text(&mut out, system);
                put_text(&mut out, code);
            }
            Key::Date { param, start, end } => {
                out.push(5);
                put_text(&mut out, param);
                out.extend_from_slice(&start.to_be_bytes());
                out.extend_from_slice(&end.to_be_bytes());
            }
            Key::DateByEnd { param, end, start } => {
                out.push(6);
                put_text(&mut out, param);
                out.extend_from_slice(&end.to_be_bytes());
                out.extend_from_slice(&start.to_be_bytes());
            }
            Key::Uri { param, uri } => {
                out.push(7);
                put_text(&mut out, param);
                put_text(&mut out, uri);
            }
        }
    }
    out
}

#[derive(Default)]
struct Drops {
    id_and_lastupdated: usize,
    composite: usize,
    other_types: BTreeMap<String, usize>,
    display_only: usize,
    empty_code: usize,
    empty_uri: usize,
    bad_date: usize,
    ts_index_implicit_or_empty: usize,
    duplicates: usize,
    over_2048: usize,
}

fn keys_of(values: &[helios_persistence::search::ExtractedValue], drops: &mut Drops) -> Vec<Key> {
    let mut set = BTreeSet::new();
    set.insert(Key::ResourcesByType);
    let mut produced = 1usize;
    for v in values {
        if v.param_name == "_id" || v.param_name == "_lastUpdated" {
            drops.id_and_lastupdated += 1;
            continue;
        }
        if v.composite_group.is_some() {
            drops.composite += 1;
            continue;
        }
        let param = v.param_name.clone();
        match (&v.param_type, &v.value) {
            (SearchParamType::Token, IndexValue::Token { system, code, .. }) => {
                if code.is_empty() && system.is_none() {
                    drops.display_only += 1;
                    continue;
                }
                if code.is_empty() {
                    drops.empty_code += 1;
                    continue;
                }
                let sys = system.clone().unwrap_or_default();
                if code.len() > 2048 || sys.len() > 2048 {
                    drops.over_2048 += 1;
                }
                set.insert(Key::TokenCode { param: param.clone(), code: code.clone(), system: sys.clone() });
                set.insert(Key::TokenSystemCode { param: param.clone(), system: sys.clone(), code: code.clone() });
                produced += 2;
                if !sys.is_empty() && sys != IMPLICIT_TOKEN_SYSTEM {
                    set.insert(Key::TokenSystem { param, system: sys, code: code.clone() });
                    produced += 1;
                } else {
                    drops.ts_index_implicit_or_empty += 1;
                }
            }
            (SearchParamType::Date, value @ IndexValue::Date { .. }) => {
                match indexed_range(value, StorageResolution::Millis) {
                    Some((s, e)) => {
                        let (start, end) = (s.timestamp_millis(), e.timestamp_millis());
                        set.insert(Key::Date { param: param.clone(), start, end });
                        set.insert(Key::DateByEnd { param, end, start });
                        produced += 2;
                    }
                    None => drops.bad_date += 1,
                }
            }
            (SearchParamType::Uri, IndexValue::Uri(uri)) => {
                if uri.is_empty() {
                    drops.empty_uri += 1;
                    continue;
                }
                if uri.len() > 2048 {
                    drops.over_2048 += 1;
                }
                set.insert(Key::Uri { param, uri: uri.clone() });
                produced += 1;
            }
            (t, _) => {
                *drops.other_types.entry(format!("{t:?}")).or_default() += 1;
            }
        }
    }
    drops.duplicates += produced - set.len();
    set.into_iter().collect()
}

const TENANT: usize = 45; // "test-iw-" + 32 hex + slack
const ID: usize = 36;
const WARN: usize = 5120;

/// Greedy packing of statements into batches under `limit` by the server's
/// measure; returns (batches, max server size, max wire bytes, mutations of the largest).
fn pack(stmts: &[(Key, bool)], rtype: usize, limit: usize) -> (usize, usize, usize, Vec<usize>) {
    pack_by(stmts, rtype, limit, false)
}

fn pack_by(stmts: &[(Key, bool)], rtype: usize, limit: usize, wire: bool) -> (usize, usize, usize, Vec<usize>) {
    let mut batches: Vec<Vec<(Key, bool)>> = vec![];
    let mut cur: Vec<(Key, bool)> = vec![];
    for s in stmts {
        let mut trial = cur.clone();
        trial.push(s.clone());
        let m = if wire { wire_size(&trial, rtype) } else { server_size(&trial, rtype) };
        if !cur.is_empty() && m > limit {
            batches.push(std::mem::take(&mut cur));
        }
        cur.push(s.clone());
    }
    if !cur.is_empty() {
        batches.push(cur);
    }
    let maxs = batches.iter().map(|b| server_size(b, rtype)).max().unwrap_or(0);
    let maxw = batches.iter().map(|b| wire_size(b, rtype)).max().unwrap_or(0);
    let muts = batches.iter().map(|b| mutations(b)).collect();
    (batches.len(), maxs, maxw, muts)
}

fn mutations(b: &[(Key, bool)]) -> usize {
    b.iter().map(|(k, _)| k.partition()).collect::<BTreeSet<_>>().len()
}

/// IMutation.dataSize: per (table, partition) 29 bytes, plus each row.
fn server_size(b: &[(Key, bool)], _rtype: usize) -> usize {
    let mut updates: BTreeSet<(&'static str, Vec<String>)> = BTreeSet::new();
    let mut size = 0;
    for (k, ins) in b {
        updates.insert((k.table(), k.partition()));
        size += k.server_row(ID, *ins);
    }
    size + 29 * updates.len()
}

/// Bytes of the batch's query entries on the wire (protocol v5 BATCH body):
/// per entry 1 kind + 2+16 prepared id + 2 value count + (4 + len) per value.
fn wire_size(b: &[(Key, bool)], rtype: usize) -> usize {
    b.iter()
        .map(|(k, ins)| 1 + 18 + 2 + k.binds(TENANT, rtype, ID, *ins).iter().map(|l| 4 + l).sum::<usize>())
        .sum()
}

/// Packs units in order, never splitting one; a unit over the bound goes alone.
/// Returns (batches, units over the bound, largest wire size of a batch of more than one unit).
fn pack_whole(units: &[Vec<(Key, bool)>], rtype: usize, wire: bool) -> (usize, usize, usize) {
    let size = |b: &[(Key, bool)]| if wire { wire_size(b, rtype) } else { server_size(b, rtype) };
    let mut batches: Vec<(usize, Vec<(Key, bool)>)> = vec![];
    let mut cur: (usize, Vec<(Key, bool)>) = (0, vec![]);
    let mut over = 0;
    for u in units {
        if size(u) > WARN - 256 {
            over += 1;
        }
        let mut t = cur.1.clone();
        t.extend(u.iter().cloned());
        if !cur.1.is_empty() && size(&t) > WARN - 256 {
            batches.push(std::mem::take(&mut cur));
        }
        cur.0 += 1;
        cur.1.extend(u.iter().cloned());
    }
    if !cur.1.is_empty() {
        batches.push(cur);
    }
    let max_multi = batches.iter().filter(|b| b.0 > 1).map(|b| wire_size(&b.1, rtype)).max().unwrap_or(0);
    (batches.len(), over, max_multi)
}

fn pct(v: &mut Vec<f64>, p: f64) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i]
}
fn pcti(v: &mut Vec<usize>, p: f64) -> usize {
    if v.is_empty() {
        return 0;
    }
    v.sort();
    let i = ((v.len() as f64 - 1.0) * p).round() as usize;
    v[i]
}

fn load(path: &Path) -> Vec<Value> {
    let text = std::fs::read_to_string(path).unwrap();
    if path.extension().is_some_and(|e| e == "ndjson") {
        return text.lines().filter(|l| !l.trim().is_empty()).map(|l| serde_json::from_str(l).unwrap()).collect();
    }
    let v: Value = serde_json::from_str(&text).unwrap();
    match v {
        Value::Array(a) => a,
        Value::Object(o) if o.contains_key("resourceType") => vec![Value::Object(o)],
        Value::Object(o) => o.into_values().flat_map(|v| v.as_array().cloned().unwrap_or_default()).collect(),
        _ => vec![],
    }
}

#[derive(Default)]
struct Stats {
    n: usize,
    extract_us: Vec<f64>,
    keys: Vec<usize>,
    blob: Vec<usize>,
    data: Vec<usize>,
    per_table: BTreeMap<&'static str, usize>,
    create_batches: Vec<usize>,
    update_batches_a: Vec<usize>,
    update_batches_c: Vec<usize>,
    delete_batches: Vec<usize>,
    max_server_batch: usize,
    max_wire_batch: usize,
    create_server_total: Vec<usize>,
    create_wire_total: Vec<usize>,
    partitions: Vec<usize>,
    update_deletes: Vec<usize>,
    wire_create: Vec<usize>,
    pair_batches_wire: Vec<usize>,
    pair_batches_server: Vec<usize>,
    part_batches_wire: Vec<usize>,
    part_batches_server: Vec<usize>,
    parts_over_bound: usize,
    max_part_wire: usize,
    max_part_batch_wire: usize,
    upd_partitions: Vec<usize>,
    max_group_wire: usize,
    groups_over_4k: usize,
    wire_update_a: Vec<usize>,
    wire_delete: Vec<usize>,
    update_inserts: Vec<usize>,
    largest: Vec<(usize, String)>,
    failures: usize,
    failing_params: BTreeMap<String, usize>,
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../../../..");
    let loader = SearchParameterLoader::new(FhirVersion::R4);
    let mut reg = SearchParameterRegistry::new();
    let mut n_params = 0;
    for p in loader.load_embedded().unwrap() {
        if reg.register(p).is_ok() {
            n_params += 1;
        }
    }
    for p in loader.load_from_spec_file(&repo.join("data")).unwrap() {
        if reg.register(p).is_ok() {
            n_params += 1;
        }
    }
    eprintln!("registered {n_params} SearchParameters");
    let extractor = SearchParameterExtractor::new(Arc::new(RwLock::new(reg)));

    for file in &args {
        let resources = load(Path::new(file));
        let mut by_type: HashMap<String, Stats> = HashMap::new();
        let mut drops = Drops::default();
        // warm-up pass: prepares every expression once.
        for r in &resources {
            let rt = r["resourceType"].as_str().unwrap_or("");
            let _ = extractor.extract(r, rt);
        }
        for r in &resources {
            let rt = r["resourceType"].as_str().unwrap_or("").to_string();
            let st = by_type.entry(rt.clone()).or_default();
            let t0 = Instant::now();
            let values = extractor.extract(r, &rt).unwrap();
            let us = t0.elapsed().as_secs_f64() * 1e6;
            let full = extractor.extract_with_missing(r, &rt).unwrap();
            if !full.failures.is_empty() {
                st.failures += 1;
                for f in &full.failures {
                    *st.failing_params.entry(f.code.clone()).or_default() += 1;
                }
            }
            let keys = keys_of(&values, &mut drops);
            let blob = encode(&keys);
            let data = serde_json::to_string(r).unwrap().len();
            st.n += 1;
            st.extract_us.push(us);
            st.keys.push(keys.len());
            st.blob.push(blob.len());
            st.data.push(data);
            st.largest.push((blob.len(), r["id"].as_str().unwrap_or("").to_string()));
            for k in &keys {
                *st.per_table.entry(k.table()).or_default() += 1;
            }
            let parts: BTreeSet<_> = keys.iter().map(|k| (k.table(), k.partition())).collect();
            st.partitions.push(parts.len());
            let rtl = rt.len();
            let inserts: Vec<(Key, bool)> = keys.iter().map(|k| (k.clone(), true)).collect();
            // update of the same content: date keys survive, everything else moves.
            let deletes: Vec<(Key, bool)> = keys.iter().filter(|k| !k.is_date()).map(|k| (k.clone(), false)).collect();
            let all_deletes: Vec<(Key, bool)> = keys.iter().map(|k| (k.clone(), false)).collect();
            let (cb, ms, mw, _) = pack(&inserts, rtl, WARN - 256);
            st.create_batches.push(cb);
            st.max_server_batch = st.max_server_batch.max(ms);
            st.max_wire_batch = st.max_wire_batch.max(mw);
            st.create_server_total.push(server_size(&inserts, rtl));
            st.create_wire_total.push(wire_size(&inserts, rtl));
            let (db, ..) = pack(&deletes, rtl, WARN - 256);
            st.update_batches_a.push(cb + db);
            let mut both = inserts.clone();
            both.extend(deletes.iter().cloned());
            let (bb, ..) = pack(&both, rtl, WARN - 256);
            st.update_batches_c.push(bb);
            let (wc, ..) = pack_by(&inserts, rtl, WARN - 256, true);
            let (wd, ..) = pack_by(&deletes, rtl, WARN - 256, true);
            let (wx, ..) = pack_by(&all_deletes, rtl, WARN - 256, true);
            st.wire_create.push(wc);
            st.wire_update_a.push(wc + wd);
            st.wire_delete.push(wx);
            // Option B, pair-only packing: each non-date key's delete next to its replacement
            // insert; a pair is never split, but one partition's pairs can be split across batches.
            let mut units: Vec<Vec<(Key, bool)>> = vec![];
            for k in &keys {
                if k.is_date() { units.push(vec![(k.clone(), true)]); } else { units.push(vec![(k.clone(), true), (k.clone(), false)]); }
            }
            let pack_units = |wire: bool| -> usize {
                let mut n = 0; let mut cur: Vec<(Key, bool)> = vec![];
                for u in &units {
                    let mut t = cur.clone(); t.extend(u.iter().cloned());
                    let m = if wire { wire_size(&t, rtl) } else { server_size(&t, rtl) };
                    if !cur.is_empty() && m > WARN - 256 { n += 1; cur.clear(); }
                    cur.extend(u.iter().cloned());
                }
                if !cur.is_empty() { n += 1; }
                n
            };
            st.pair_batches_wire.push(pack_units(true));
            st.pair_batches_server.push(pack_units(false));
            // Option B as 0006 chose it (IW9, IW11): one unit per (table, partition key), all its
            // inserts and deletes; a unit is never split, and a unit over the bound goes alone.
            let mut parts: BTreeMap<(&'static str, Vec<String>), Vec<(Key, bool)>> = BTreeMap::new();
            for u in &units { for x in u { parts.entry((x.0.table(), x.0.partition())).or_default().push(x.clone()); } }
            let part_units: Vec<Vec<(Key, bool)>> = parts.into_values().collect();
            let (pw, over, maxw) = pack_whole(&part_units, rtl, true);
            let (ps, ..) = pack_whole(&part_units, rtl, false);
            st.part_batches_wire.push(pw);
            st.part_batches_server.push(ps);
            st.parts_over_bound += over;
            st.max_part_batch_wire = st.max_part_batch_wire.max(maxw);
            for u in &part_units { st.max_part_wire = st.max_part_wire.max(wire_size(u, rtl)); }
            // Option B': one group per partition key (tables sharing the key value merge).
            let mut groups: BTreeMap<Vec<String>, Vec<(Key, bool)>> = BTreeMap::new();
            for u in &units { for x in u { groups.entry(x.0.partition()).or_default().push(x.clone()); } }
            st.upd_partitions.push(groups.len());
            for g in groups.values() {
                let w = wire_size(g, rtl);
                st.max_group_wire = st.max_group_wire.max(w);
                if w > 4096 { st.groups_over_4k += 1; }
            }
            st.update_deletes.push(deletes.len());
            st.update_inserts.push(inserts.len());
            let (xb, ..) = pack(&all_deletes, rtl, WARN - 256);
            st.delete_batches.push(xb);
        }
        println!("\n=== {file}: {} resources ===", resources.len());
        let mut types: Vec<_> = by_type.into_iter().collect();
        types.sort_by(|a, b| a.0.cmp(&b.0));
        for (rt, mut s) in types {
            let mean = |v: &Vec<usize>| v.iter().sum::<usize>() as f64 / v.len().max(1) as f64;
            println!("-- {rt}: n={}", s.n);
            println!(
                "   extract us (debug build): p50 {:.0} p90 {:.0} p99 {:.0} max {:.0}",
                pct(&mut s.extract_us.clone(), 0.5),
                pct(&mut s.extract_us.clone(), 0.9),
                pct(&mut s.extract_us.clone(), 0.99),
                pct(&mut s.extract_us, 1.0)
            );
            println!(
                "   keys/resource: mean {:.1} p50 {} p99 {} max {}",
                mean(&s.keys), pcti(&mut s.keys.clone(), 0.5), pcti(&mut s.keys.clone(), 0.99), pcti(&mut s.keys, 1.0)
            );
            println!(
                "   index_keys bytes: mean {:.0} p50 {} p99 {} max {}; data bytes mean {:.0} p50 {} max {}",
                mean(&s.blob), pcti(&mut s.blob.clone(), 0.5), pcti(&mut s.blob.clone(), 0.99), pcti(&mut s.blob.clone(), 1.0),
                mean(&s.data), pcti(&mut s.data.clone(), 0.5), pcti(&mut s.data, 1.0)
            );
            println!("   partitions touched by a create: mean {:.1} max {}", mean(&s.partitions), pcti(&mut s.partitions, 1.0));
            println!("   rows per table (total): {:?}", s.per_table);
            println!(
                "   create: server dataSize total mean {:.0} max {}; wire bytes total mean {:.0} max {}",
                mean(&s.create_server_total), pcti(&mut s.create_server_total, 1.0),
                mean(&s.create_wire_total), pcti(&mut s.create_wire_total, 1.0)
            );
            println!(
                "   batches (server size <= 4864): create mean {:.2} max {}; update rule A (inserts then deletes) mean {:.2} max {}; update one-pass mean {:.2} max {}; delete mean {:.2} max {}",
                mean(&s.create_batches), pcti(&mut s.create_batches.clone(), 1.0),
                mean(&s.update_batches_a), pcti(&mut s.update_batches_a.clone(), 1.0),
                mean(&s.update_batches_c), pcti(&mut s.update_batches_c.clone(), 1.0),
                mean(&s.delete_batches), pcti(&mut s.delete_batches.clone(), 1.0)
            );
            println!(
                "   batches (wire bytes <= 4864): create mean {:.2} max {}; update rule A mean {:.2} max {}; delete mean {:.2} max {}",
                mean(&s.wire_create), pcti(&mut s.wire_create.clone(), 1.0),
                mean(&s.wire_update_a), pcti(&mut s.wire_update_a.clone(), 1.0),
                mean(&s.wire_delete), pcti(&mut s.wire_delete.clone(), 1.0)
            );
            println!(
                "   update option B (pairs kept together, <= 4864): wire-packed batches mean {:.2} max {}; server-packed mean {:.2} max {}",
                mean(&s.pair_batches_wire), pcti(&mut s.pair_batches_wire.clone(), 1.0),
                mean(&s.pair_batches_server), pcti(&mut s.pair_batches_server.clone(), 1.0)
            );
            println!(
                "   update option B, whole partitions (IW9/IW11 as chosen, a partition never split, <= 4864): wire-packed batches mean {:.2} max {}; server-packed mean {:.2} max {}; partitions over 4864 wire bytes (sent alone) {}, largest partition {} wire bytes; largest batch of more than one partition {} wire bytes",
                mean(&s.part_batches_wire), pcti(&mut s.part_batches_wire.clone(), 1.0),
                mean(&s.part_batches_server), pcti(&mut s.part_batches_server.clone(), 1.0),
                s.parts_over_bound, s.max_part_wire, s.max_part_batch_wire
            );
            println!(
                "   update option B' (one group per partition): groups mean {:.1} max {}; largest group wire {} bytes; groups over 4 KiB {}",
                mean(&s.upd_partitions), pcti(&mut s.upd_partitions.clone(), 1.0), s.max_group_wire, s.groups_over_4k
            );
            println!("   largest batch: server {} bytes, wire {} bytes", s.max_server_batch, s.max_wire_batch);
            println!(
                "   update: deletes mean {:.1} inserts mean {:.1}",
                mean(&s.update_deletes), mean(&s.update_inserts)
            );
            s.largest.sort();
            println!("   largest index_keys: {:?}", s.largest.iter().rev().take(3).collect::<Vec<_>>());
            println!("   resources with a failing parameter: {} {:?}", s.failures, s.failing_params);
        }
        println!(
            "   drops: _id/_lastUpdated {} composite {} display-only {} empty-code {} empty-uri {} bad-date {} ts-index implicit/empty {} duplicates {} over-2048 {} other types {:?}",
            drops.id_and_lastupdated, drops.composite, drops.display_only, drops.empty_code, drops.empty_uri,
            drops.bad_date, drops.ts_index_implicit_or_empty, drops.duplicates, drops.over_2048, drops.other_types
        );
    }
}
