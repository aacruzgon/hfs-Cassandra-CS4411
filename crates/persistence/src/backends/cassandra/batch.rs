//! Index batches (roadmap Step 5, item 5.1): which statements a batch may
//! hold, how many bytes one may carry, and what a failed one means.
//!
//! Step 5 of the write path sends a version's index rows in logged batches at
//! `LOCAL_QUORUM`, each under 5 KiB, sent concurrently (design step 5;
//! `docs/Cassandra/schema/walkthrough.md` section 2, "Index rows"). The
//! session sends one batch, [`CassandraSession::batch`]; this module holds
//! the parts that need no node, each tested on its own. The decisions are
//! recorded under their plan IDs in ADR-sam-michaels-0006.
//!
//! - [`IndexCql`] and [`IndexStatement`] (IW13, IW20): the only statements a
//!   batch takes. An `IndexCql` is checked when the program is compiled to be
//!   an `INSERT INTO` or a `DELETE FROM` one of the seven index tables, so no
//!   batch can ever carry a write to `resources`, `resource_versions` or the
//!   intent table.
//! - [`wire_bytes`], [`MAX_BATCH_BYTES`] and [`pack`] (IW11): a batch is
//!   measured by every byte its statements take on the wire, and units (one
//!   partition's rows, IW9) are packed into batches without ever splitting
//!   one.
//! - [`classify_failure`] (IW15): what a failed batch means for the writer,
//!   from the error the node sent.
//!
//! [`CassandraSession::batch`]: super::session::CassandraSession::batch

// Pull request 3 builds this module before the index writer uses it: the
// session's batch call and its tests use the statement types now, and the
// writer takes up `pack` and `classify_failure` in the same pull request.
#![allow(dead_code)]

use std::ops::Range;

use cdrs_tokio::error::Error as CdrsError;
use cdrs_tokio::frame::message_error::{ErrorType, WriteType};
use cdrs_tokio::types::value::Value;

use super::session::DriverError;

/// The index tables of `docs/Cassandra/schema/schema.cql`, the only tables a
/// batch writes.
pub(crate) const INDEX_TABLES: [&str; 7] = [
    "resources_by_type",
    "token_code_index",
    "token_system_code_index",
    "token_system_index",
    "date_index",
    "date_index_by_end",
    "uri_index",
];

/// The two statement shapes an index batch holds: a row inserted, or a row
/// deleted, each `USING TIMESTAMP ts(V)` of its version.
const INDEX_VERBS: [&str; 2] = ["INSERT INTO ", "DELETE FROM "];

/// The CQL text of an index statement: an `INSERT INTO` or a `DELETE FROM`
/// one of the [`INDEX_TABLES`], with no `;` anywhere.
///
/// **Why a type (IW20).** A failed native-protocol batch leaves one entry in
/// the node's audit log with no statement text (`AuditLogManager.batchFailure`,
/// 5.0.9), so CI's CQL audit check cannot see whether it held a `DELETE` on
/// `resources` or `resource_versions`, and fails the job on it (rule (d) in
/// `.github/scripts/ci-report.py`; ADR-aacruzgon-0058, 0066). That rule stays
/// strict. What this type adds is the proof that no batch can hold such a
/// statement: [`CassandraSession::batch`] takes only [`IndexStatement`]s, an
/// [`IndexStatement`] holds an `IndexCql`, and `IndexCql::new` is a
/// `const fn` that panics on any other text. Every `IndexCql` is written as a
/// `const` item, so the check runs when the program is compiled and a wrong
/// statement fails the build, never a request. The test
/// `only_index_table_statements_can_reach_a_batch` scans the backend's source
/// for both halves, without trusting this function.
///
/// The match is exact and upper case, the way the backend writes its CQL: a
/// keyspace-qualified table (`helios.resources_by_type`) or a lower-case verb
/// is refused, which costs nothing, since the session sets the keyspace.
///
/// [`CassandraSession::batch`]: super::session::CassandraSession::batch
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct IndexCql(&'static str);

impl IndexCql {
    /// `cql`, which must write an index table; see the type's docs. Use it
    /// only to initialize a `const` item, so a wrong text fails the build.
    pub(crate) const fn new(cql: &'static str) -> Self {
        assert!(
            writes_an_index_table(cql),
            "an index batch takes only an INSERT INTO or a DELETE FROM an index table"
        );
        IndexCql(cql)
    }

    /// The CQL text.
    pub(crate) const fn as_str(self) -> &'static str {
        self.0
    }
}

/// Whether `cql` starts with one of the [`INDEX_VERBS`] followed by one of
/// the [`INDEX_TABLES`] and then a space or `(`, and holds no `;`. The space
/// or `(` keeps a table whose name starts with another's apart: `date_index`
/// is a prefix of `date_index_by_end`, and `resources` of
/// `resources_by_type`.
const fn writes_an_index_table(cql: &str) -> bool {
    let bytes = cql.as_bytes();
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b';' {
            return false;
        }
        at += 1;
    }
    let mut verb = 0;
    while verb < INDEX_VERBS.len() {
        let verb_bytes = INDEX_VERBS[verb].as_bytes();
        if has_at(bytes, 0, verb_bytes) {
            let start = verb_bytes.len();
            let mut table = 0;
            while table < INDEX_TABLES.len() {
                let name = INDEX_TABLES[table].as_bytes();
                let end = start + name.len();
                if has_at(bytes, start, name)
                    && end < bytes.len()
                    && (bytes[end] == b' ' || bytes[end] == b'(')
                {
                    return true;
                }
                table += 1;
            }
        }
        verb += 1;
    }
    false
}

/// Whether `bytes` holds `part` at `at`.
const fn has_at(bytes: &[u8], at: usize, part: &[u8]) -> bool {
    if at + part.len() > bytes.len() {
        return false;
    }
    let mut i = 0;
    while i < part.len() {
        if bytes[at + i] != part[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// One statement of an index batch: an [`IndexCql`] and the values bound to
/// it, its own `USING TIMESTAMP` among them.
#[derive(Debug, Clone)]
pub(crate) struct IndexStatement {
    cql: IndexCql,
    values: Vec<Value>,
}

impl IndexStatement {
    pub(crate) fn new(cql: IndexCql, values: Vec<Value>) -> Self {
        Self { cql, values }
    }

    /// The CQL text.
    pub(crate) fn cql(&self) -> &'static str {
        self.cql.as_str()
    }

    /// The bound values, in marker order.
    pub(crate) fn values(&self) -> &[Value] {
        &self.values
    }

    /// The CQL text and the values, for a statement sent on its own.
    pub(crate) fn into_parts(self) -> (&'static str, Vec<Value>) {
        (self.cql.as_str(), self.values)
    }
}

/// The node's statement id is the MD5 digest of the statement
/// (`MD5Digest`, `QueryProcessor.computeId`, 5.0.9): 16 bytes.
const PREPARED_ID_BYTES: usize = 16;

/// What every statement of a batch takes before its values (native protocol
/// v5, section 4.1.7, BATCH): the kind byte (1, "prepared"), the statement
/// id as `[short bytes]` (2 + 16) and the value count as `[short]` (2).
const STATEMENT_HEAD_BYTES: usize = 1 + 2 + PREPARED_ID_BYTES + 2;

/// The bytes `statements` take in a BATCH request (IW11): for each, its head
/// and every value as a `[value]`, a 4-byte length and then its bytes (a
/// null is the length alone). Partition-key values count like any other,
/// which is the point: the node's own size check leaves them out, and they
/// are most of an index row (walkthrough section 2, "Index rows").
///
/// The batch's own few bytes (type, count, consistency, flags) are not a
/// statement's and are left out. This is the one measure every batch is
/// sized by. The plan keeps the node's own formula as a measure-first
/// fallback (IW11), and switching to it means changing this function alone.
pub(crate) fn wire_bytes(statements: &[IndexStatement]) -> usize {
    statements
        .iter()
        .map(|statement| {
            STATEMENT_HEAD_BYTES
                + statement
                    .values
                    .iter()
                    .map(|value| {
                        4 + match value {
                            Value::Some(bytes) => bytes.len(),
                            Value::Null | Value::NotSet => 0,
                        }
                    })
                    .sum::<usize>()
        })
        .sum()
}

/// The most bytes one batch may carry by [`wire_bytes`] (IW11): the node's
/// `batch_size_warn_threshold` of 5 KiB, less a 256-byte margin.
pub(crate) const MAX_BATCH_BYTES: usize = 5 * 1024 - 256;

/// Packs units into batches of at most `bound` bytes, given each unit's size
/// by [`wire_bytes`]. Returns each batch as the run of unit indices it holds
/// (IW11).
///
/// - **A unit is never split.** A unit is every row of one partition key, its
///   inserts and its deletes (IW9 B). Cassandra applies one partition's
///   mutation whole on each replica, so a reader never sees half of it; most
///   of all, `count` never sees an updated resource's `resources_by_type`
///   row at both `last_updated` values at once.
/// - **A unit over `bound` goes alone**, as a batch of its own. Such a unit
///   is one partition, so the node neither size-checks it nor writes it to
///   the batchlog (`BatchStatement.java:331-367`). No fixture comes near:
///   it takes values close to G35's 2,048-byte bound.
/// - **Order is kept.** Units fill a batch in the order given until the next
///   one would pass `bound` (next fit). Packing tighter would reorder units
///   and buy little: the batches of one write are sent at once (IW12).
///
/// No unit is left out, and none appears twice.
pub(crate) fn pack(unit_bytes: &[usize], bound: usize) -> Vec<Range<usize>> {
    let mut batches = Vec::new();
    let mut start = 0;
    let mut filled = 0;
    for (unit, &bytes) in unit_bytes.iter().enumerate() {
        if unit > start && filled + bytes > bound {
            batches.push(start..unit);
            start = unit;
            filled = 0;
        }
        filled += bytes;
    }
    if start < unit_bytes.len() {
        batches.push(start..unit_bytes.len());
    }
    batches
}

/// What a failed index batch, or a lone index statement, means for the
/// writer (IW15). Whatever the answer, the batch is not confirmed, so the
/// write keeps its intent row and still answers success (G23): the sweeper
/// finishes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BatchFailure {
    /// `WriteTimeout` with write type `BATCH`: the coordinator wrote the
    /// batch to the batchlog, then timed out waiting for the replicas
    /// (native protocol v5, section 9, "write_type"). The batchlog replays
    /// it (batches older than 2 × `write_request_timeout`, checked every
    /// 10 s: `BatchlogManager.java:90`, `:95`), so it is not sent again. It
    /// is not taken as done either: if the batchlog's replicas die before the
    /// replay, deleting the intent would lose the rows for good, so the
    /// intent stays and the sweeper replays it, a no-op if the batchlog got
    /// there first.
    InBatchlog,
    /// Worth another try: `WriteTimeout` with `BATCH_LOG` (the batchlog write
    /// itself timed out, so nothing was applied), with `SIMPLE` (a lone
    /// statement, or a batch of one partition, which the node runs without a
    /// batchlog) or with another write type; `WriteFailure`, `Unavailable`,
    /// `Overloaded`, a node-side `Server` error, a dropped connection, the
    /// backend's own deadline, and anything else but `Invalid`. Every index
    /// statement carries a fixed `USING TIMESTAMP`, so applying one twice
    /// writes the same cells.
    Retry,
    /// `Invalid`: the node refused the batch or statement as it stands
    /// ("Batch too large", a key over 64 KiB). Another try gets the same
    /// answer, so none is made, and the writer logs it at `error`. G35's
    /// bound and [`MAX_BATCH_BYTES`] should make it impossible.
    Refused,
}

/// Classifies a failed index batch or index statement (IW15); see
/// [`BatchFailure`].
pub(crate) fn classify_failure(error: &DriverError) -> BatchFailure {
    let DriverError::Driver(error) = error else {
        // The backend's deadline: the batch may or may not have been
        // applied, and sending it again is safe.
        return BatchFailure::Retry;
    };
    let CdrsError::Server { body, .. } = error.as_ref() else {
        return BatchFailure::Retry;
    };
    match &body.ty {
        ErrorType::WriteTimeout(timeout) if timeout.write_type == WriteType::Batch => {
            BatchFailure::InBatchlog
        }
        ErrorType::Invalid => BatchFailure::Refused,
        _ => BatchFailure::Retry,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};

    use cdrs_tokio::consistency::Consistency;
    use cdrs_tokio::frame::message_error::{
        ErrorBody, FailureInfo, UnavailableError, WriteFailureError, WriteTimeoutError,
    };

    use super::*;

    /// A `resources_by_type` insert, as the writer sends it.
    const INSERT_ROW: IndexCql = IndexCql::new(
        "INSERT INTO resources_by_type \
         (tenant_id, resource_type, last_updated, id, version_id, fhir_version) \
         VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
    );

    #[test]
    fn every_index_table_and_verb_is_accepted() {
        for verb in INDEX_VERBS {
            for table in INDEX_TABLES {
                for after in [
                    " USING TIMESTAMP ?",
                    " (tenant_id) VALUES (?)",
                    "(a) VALUES (?)",
                ] {
                    let cql = format!("{verb}{table}{after}");
                    assert!(writes_an_index_table(&cql), "{cql}");
                }
            }
        }
        assert_eq!(INSERT_ROW.as_str().len(), INSERT_ROW.0.len());
    }

    #[test]
    fn anything_else_is_refused() {
        // Built from pieces, so the backend's own scan of DELETE statements
        // (`write.rs`, D6) never sees a forbidden literal here.
        let delete = ["DE", "LETE FROM "].concat();
        for cql in [
            format!("{delete}resources WHERE id = ?"),
            format!("{delete}resource_versions WHERE id = ?"),
            format!("{delete}pending_index_writes WHERE id = ?"),
            "INSERT INTO resources (id) VALUES (?)".to_string(),
            "INSERT INTO resource_versions (id) VALUES (?)".to_string(),
            "INSERT INTO pending_index_writes (id) VALUES (?)".to_string(),
            "UPDATE resources_by_type SET version_id = ? WHERE id = ?".to_string(),
            "SELECT id FROM resources_by_type".to_string(),
            // A name that only starts like an index table's.
            "INSERT INTO resources_by_type_old (id) VALUES (?)".to_string(),
            "INSERT INTO date_indexes (id) VALUES (?)".to_string(),
            // A table name with nothing after it.
            "INSERT INTO uri_index".to_string(),
            // Qualified, lower case, or two statements in one.
            "INSERT INTO helios.resources_by_type (id) VALUES (?)".to_string(),
            "insert into resources_by_type (id) VALUES (?)".to_string(),
            format!("INSERT INTO uri_index (id) VALUES (?); {delete}resources WHERE id = ?"),
            String::new(),
        ] {
            assert!(!writes_an_index_table(&cql), "{cql}");
        }
    }

    /// `n` statements, each with values of the given byte lengths (`None` is
    /// a null).
    fn statements(lengths: &[Option<usize>], n: usize) -> Vec<IndexStatement> {
        let values: Vec<Value> = lengths
            .iter()
            .map(|length| match length {
                Some(length) => Value::Some(vec![b'x'; *length]),
                None => Value::Null,
            })
            .collect();
        (0..n)
            .map(|_| IndexStatement::new(INSERT_ROW, values.clone()))
            .collect()
    }

    #[test]
    fn wire_bytes_count_every_value_byte_and_each_statement_head() {
        assert_eq!(wire_bytes(&[]), 0);
        // No values: the head alone, 1 + (2 + 16) + 2.
        assert_eq!(wire_bytes(&statements(&[], 1)), 21);
        // A 64-byte tenant, a 7-byte type, an 8-byte timestamp, a null.
        let one = wire_bytes(&statements(&[Some(64), Some(7), Some(8), None], 1));
        assert_eq!(one, 21 + (4 + 64) + (4 + 7) + (4 + 8) + 4);
        assert_eq!(
            wire_bytes(&statements(&[Some(64), Some(7), Some(8), None], 3)),
            3 * one
        );
    }

    /// The measure is the driver's own encoding of a batch's statements, byte
    /// for byte (`cassandra-protocol` 5.0.0, `BatchQuery::serialize`), with a
    /// 16-byte statement id like the node's.
    #[test]
    fn wire_bytes_equal_the_driver_encoding_of_the_statements() {
        use cdrs_tokio::frame::Serialize;
        use cdrs_tokio::frame::message_batch::{BatchQuery, BatchQuerySubj};
        use cdrs_tokio::query::QueryValues;
        use cdrs_tokio::types::CBytesShort;

        let batch = statements(&[Some(64), Some(0), None, Some(2_048)], 2);
        let encoded: usize = batch
            .iter()
            .map(|statement| {
                BatchQuery::new(
                    BatchQuerySubj::PreparedId(CBytesShort::new(vec![7; PREPARED_ID_BYTES])),
                    QueryValues::SimpleValues(statement.values().to_vec()),
                )
                .serialize_to_vec(super::super::session::PROTOCOL_VERSION)
                .len()
            })
            .sum();
        assert_eq!(wire_bytes(&batch), encoded);
    }

    /// Every unit appears once, in order, in runs.
    fn assert_partition(batches: &[Range<usize>], units: usize) {
        let flat: Vec<usize> = batches.iter().flat_map(|batch| batch.clone()).collect();
        assert_eq!(flat, (0..units).collect::<Vec<_>>(), "{batches:?}");
        assert!(batches.iter().all(|batch| !batch.is_empty()), "{batches:?}");
    }

    /// The bytes of each batch.
    fn filled(sizes: &[usize], batches: &[Range<usize>]) -> Vec<usize> {
        batches
            .iter()
            .map(|batch| sizes[batch.clone()].iter().sum())
            .collect()
    }

    #[test]
    fn units_that_fit_exactly_share_a_batch() {
        let sizes = [1_000, 2_000, 1_864];
        let batches = pack(&sizes, MAX_BATCH_BYTES);
        assert_eq!(batches, vec![0..3]);
        assert_eq!(filled(&sizes, &batches), vec![MAX_BATCH_BYTES]);
    }

    #[test]
    fn one_byte_over_starts_the_next_batch() {
        let sizes = [1_000, 2_000, 1_865, 10];
        let batches = pack(&sizes, MAX_BATCH_BYTES);
        assert_eq!(batches, vec![0..2, 2..4]);
        assert_partition(&batches, sizes.len());
    }

    #[test]
    fn a_unit_over_the_bound_goes_alone() {
        // Alone at the start, in the middle, at the end, and twice in a row.
        let sizes = [6_000, 100, 200, 5_000, 300, 9_000, 9_000, 400, 4_865];
        let batches = pack(&sizes, MAX_BATCH_BYTES);
        assert_eq!(
            batches,
            vec![0..1, 1..3, 3..4, 4..5, 5..6, 6..7, 7..8, 8..9]
        );
        assert_partition(&batches, sizes.len());
        for (batch, bytes) in batches.iter().zip(filled(&sizes, &batches)) {
            assert!(
                bytes <= MAX_BATCH_BYTES || batch.len() == 1,
                "{batch:?} holds {bytes} bytes"
            );
        }
    }

    #[test]
    fn packing_keeps_order_and_never_splits_or_drops_a_unit() {
        assert!(pack(&[], MAX_BATCH_BYTES).is_empty());
        assert_eq!(pack(&[MAX_BATCH_BYTES + 1], MAX_BATCH_BYTES), vec![0..1]);
        // A deterministic spread of sizes, from one small row to an
        // oversized unit.
        let sizes: Vec<usize> = (0..500).map(|i| 21 + (i * 7_919) % 6_000).collect();
        let batches = pack(&sizes, MAX_BATCH_BYTES);
        assert_partition(&batches, sizes.len());
        let bytes = filled(&sizes, &batches);
        for (i, batch) in batches.iter().enumerate() {
            assert!(bytes[i] <= MAX_BATCH_BYTES || batch.len() == 1, "{batch:?}");
            // Next fit: a batch closes only when the next unit would not fit.
            if let Some(next) = batches.get(i + 1) {
                assert!(bytes[i] + sizes[next.start] > MAX_BATCH_BYTES, "{batch:?}");
            }
        }
    }

    fn server(ty: ErrorType) -> DriverError {
        DriverError::from(CdrsError::Server {
            body: ErrorBody::new("from the node", ty),
            addr: "127.0.0.1:9042".parse().expect("an address"),
        })
    }

    fn write_timeout(write_type: WriteType) -> DriverError {
        server(ErrorType::WriteTimeout(WriteTimeoutError::new(
            Consistency::LocalQuorum,
            1,
            2,
            write_type,
            None,
        )))
    }

    #[test]
    fn a_batch_timeout_is_left_to_the_batchlog_and_never_retried() {
        assert_eq!(
            classify_failure(&write_timeout(WriteType::Batch)),
            BatchFailure::InBatchlog
        );
    }

    #[test]
    fn a_batchlog_or_simple_timeout_and_every_transient_error_are_retried() {
        let retried = [
            write_timeout(WriteType::BatchLog),
            write_timeout(WriteType::Simple),
            write_timeout(WriteType::UnloggedBatch),
            write_timeout(WriteType::Cas),
            server(ErrorType::WriteFailure(WriteFailureError::new(
                Consistency::LocalQuorum,
                0,
                2,
                FailureInfo::NumFailures(1),
                WriteType::Batch,
            ))),
            server(ErrorType::Unavailable(UnavailableError::new(
                Consistency::LocalQuorum,
                2,
                1,
            ))),
            server(ErrorType::Overloaded),
            server(ErrorType::Server),
            server(ErrorType::IsBootstrapping),
            // Refusals other than `Invalid` are retried as the plan words
            // IW15; the backend's constant CQL never draws them.
            server(ErrorType::Syntax),
            server(ErrorType::Unauthorized),
            DriverError::from(CdrsError::General(
                "Connection closed while waiting for response!".to_string(),
            )),
            DriverError::Deadline {
                after: std::time::Duration::from_secs(10),
                sent: true,
            },
            DriverError::Deadline {
                after: std::time::Duration::from_secs(10),
                sent: false,
            },
        ];
        for error in &retried {
            assert_eq!(classify_failure(error), BatchFailure::Retry, "{error:?}");
        }
    }

    #[test]
    fn an_invalid_batch_is_refused_and_not_retried() {
        assert_eq!(
            classify_failure(&server(ErrorType::Invalid)),
            BatchFailure::Refused
        );
    }

    /// Every `.rs` file of the backend, subdirectories included.
    fn backend_sources() -> Vec<(PathBuf, String)> {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/backends/cassandra");
        let mut pending = vec![dir];
        let mut files = Vec::new();
        while let Some(next) = pending.pop() {
            for entry in std::fs::read_dir(&next).expect("the backend's directory") {
                let path = entry.expect("a directory entry").path();
                if path.is_dir() {
                    pending.push(path);
                } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                    let source = std::fs::read_to_string(&path).expect("a source file");
                    files.push((path, source));
                }
            }
        }
        files
    }

    /// The `"…"` string literal that starts at `source[at..]` after any
    /// whitespace, with escapes resolved and `\`-newline continuations folded
    /// as the compiler folds them; `None` if no literal starts there.
    fn literal_at(source: &str, at: usize) -> Option<String> {
        let mut chars = source[at..].trim_start().chars().peekable();
        if chars.next() != Some('"') {
            return None;
        }
        let mut literal = String::new();
        while let Some(c) = chars.next() {
            match c {
                '"' => return Some(literal),
                '\\' => match chars.next()? {
                    '\n' => {
                        while chars.peek().is_some_and(|c| c.is_whitespace()) {
                            chars.next();
                        }
                    }
                    'n' => literal.push('\n'),
                    escaped => literal.push(escaped),
                },
                c => literal.push(c),
            }
        }
        None
    }

    /// An independent reading of what an index statement is, which does not
    /// call [`writes_an_index_table`].
    fn is_index_write(cql: &str) -> bool {
        let pattern = format!(
            r"^(INSERT INTO|{} FROM) ({})[ (]",
            ["DE", "LETE"].concat(),
            INDEX_TABLES.join("|")
        );
        regex::Regex::new(&pattern).unwrap().is_match(cql) && !cql.contains(';')
    }

    /// IW20, the source half of audit rule (d) (`ci-report.py`): a failed
    /// native batch leaves no statement text in the audit log, so the proof
    /// that none could hold a write to `resources` or `resource_versions`
    /// comes from the source. Scanned in every file of the backend
    /// (`read_dir`, so a new file is never missed):
    ///
    /// 1. every call of `IndexCql::new` initializes a `const` item, so the
    ///    compiler runs its check, and its text writes an index table by a
    ///    pattern of this test's own;
    /// 2. an `IndexCql` is built nowhere else: its tuple constructor appears
    ///    only in `batch.rs`, once in its declaration and once in `new`;
    /// 3. the driver's batch call and builder appear only in `session.rs`,
    ///    once, behind `CassandraSession::batch`, which takes
    ///    `IndexStatement`s, so nothing sends a batch around the type.
    #[test]
    fn only_index_table_statements_can_reach_a_batch() {
        let files = backend_sources();
        assert!(files.len() >= 15, "scanned only {} files", files.len());
        // A `const` item's name and type, then `=`, right before the call.
        let const_item = regex::Regex::new(r"\bconst\s+[A-Z][A-Z0-9_]*\s*:\s*IndexCql\s*=\s*$")
            .expect("a pattern");
        let call = ["IndexCql", "::new("].concat();
        let constructor = ["IndexCql", "("].concat();
        let driver_batch = ["batch", "_with_params("].concat();
        let builder = ["BatchQuery", "Builder"].concat();

        let mut texts = BTreeSet::new();
        let mut problems = Vec::new();
        let mut constructors = Vec::new();
        let mut driver_batches = Vec::new();
        for (path, source) in &files {
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("?");
            for (at, _) in source.match_indices(&call) {
                // The item the call ends: from the last `;`, `{` or `}`.
                let item = &source[source[..at].rfind([';', '{', '}']).map_or(0, |n| n + 1)..at];
                if !const_item.is_match(item) {
                    problems.push(format!("{name}: not a const item: {}", item.trim()));
                }
                match literal_at(source, at + call.len()) {
                    Some(cql) if is_index_write(&cql) => {
                        texts.insert(cql);
                    }
                    Some(cql) => problems.push(format!("{name}: not an index write: {cql}")),
                    None => problems.push(format!("{name}: no literal after {call}")),
                }
            }
            for (at, _) in source.match_indices(&constructor) {
                // Only the tuple constructor itself, not a longer name.
                let ends_a_name = source[..at]
                    .chars()
                    .next_back()
                    .is_none_or(|c| !(c.is_alphanumeric() || c == '_'));
                if ends_a_name {
                    constructors.push(name.to_string());
                }
            }
            for _ in source.match_indices(&driver_batch) {
                driver_batches.push(name.to_string());
            }
            if source.contains(&builder) && name != "session.rs" {
                problems.push(format!("{name} builds a driver batch"));
            }
        }
        assert!(problems.is_empty(), "{problems:#?}");
        assert!(!texts.is_empty(), "no IndexCql found");
        assert_eq!(
            constructors,
            vec!["batch.rs", "batch.rs"],
            "the tuple constructor outside its declaration and new"
        );
        assert_eq!(driver_batches, vec!["session.rs"], "driver batch calls");

        // The scan's own pattern: what it lets through and what it refuses.
        let delete = ["DE", "LETE FROM "].concat();
        assert!(is_index_write(INSERT_ROW.as_str()));
        assert!(is_index_write(&format!(
            "{delete}resources_by_type USING TIMESTAMP ? WHERE id = ?"
        )));
        assert!(!is_index_write(&format!("{delete}resources WHERE id = ?")));
        assert!(!is_index_write(&format!(
            "{delete}resource_versions WHERE id = ?"
        )));
        assert!(!is_index_write("INSERT INTO resources (id) VALUES (?)"));
        assert!(!is_index_write(&format!(
            "INSERT INTO uri_index (id) VALUES (?); {delete}resources"
        )));
        assert_eq!(
            literal_at(" \"A \\\n     B\\\"\" x", 0).as_deref(),
            Some("A B\"")
        );
        assert!(const_item.is_match("    const INSERT_ROW: IndexCql = "));
        assert!(!const_item.is_match("    let cql = "));
    }
}
