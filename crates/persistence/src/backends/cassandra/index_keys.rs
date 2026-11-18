//! `index_keys`: the key set `K(V)` a version writes, in the stored encoding.
//!
//! Every compare-and-swap on `resources` writes the key set of the version it
//! creates into `resources.index_keys`, and the version's `resource_versions`
//! row carries the same blob, so a later write, the sweeper and repair delete
//! exactly what was written instead of re-extracting it with a registry that
//! may have changed (ADR-aacruzgon-0027, walkthrough G22).
//!
//! **Format 1** (`docs/Cassandra/schema/schema.cql`, above `resource_versions`):
//!
//! ```text
//! byte    format version, 0x01
//! varint  row count (unsigned LEB128)
//! rows, each a table tag byte followed by the key columns that the version
//! row does not imply (tenant_id, resource_type, id and last_updated are the
//! row's own):
//!   0x01 resources_by_type        nothing
//!   0x02 token_code_index         param, code, system
//!   0x03 token_system_code_index  param, system, code
//!   0x04 token_system_index       param, system, code
//!   0x05 date_index               param, start, end
//!   0x06 date_index_by_end        param, end, start
//!   0x07 uri_index                param, uri
//! text = varint byte length + UTF-8 bytes; timestamp = 8-byte big-endian
//! signed milliseconds. A delete marker's K(V) is empty: 0x01 0x00.
//! ```
//!
//! Step 2 writes only the `resources_by_type` row (roadmap Step 2, pulled
//! forward so `count` works); Step 5 adds the token, date and URI rows. The
//! encoding is complete now, so Step 5 extends the key set, not the format.
//!
//! **Which row a key names** is decided here and nowhere else
//! ([`IndexKey::row_at`], [`IndexKey::partition`]), from each index table's
//! primary key in `schema.cql`. The write path's index delta, the sweeper,
//! repair on read and the consistency check all compare rows through these
//! two functions, so a rule written once cannot drift between them (IW7,
//! IW9, ADR-sam-michaels-0006).

use std::fmt;

use chrono::{DateTime, Utc};

/// The format byte this module writes.
pub(crate) const FORMAT_1: u8 = 0x01;

/// The empty key set, `0x01 0x00`: a delete marker's.
pub(crate) const EMPTY: [u8; 2] = [FORMAT_1, 0x00];

/// One index row, by the key columns the version row does not imply.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[allow(dead_code)] // The token, date and URI rows are written from Step 5 on.
pub(crate) enum IndexKey {
    /// `resources_by_type`, keyed by the version's own `last_updated` and id.
    ResourcesByType,
    /// `token_code_index`.
    TokenCode {
        param: String,
        code: String,
        system: String,
    },
    /// `token_system_code_index`.
    TokenSystemCode {
        param: String,
        system: String,
        code: String,
    },
    /// `token_system_index`.
    TokenSystem {
        param: String,
        system: String,
        code: String,
    },
    /// `date_index`, `[start, end)` in milliseconds.
    Date { param: String, start: i64, end: i64 },
    /// `date_index_by_end`, `[start, end)` in milliseconds.
    DateByEnd { param: String, end: i64, start: i64 },
    /// `uri_index`.
    Uri { param: String, uri: String },
}

impl IndexKey {
    fn tag(&self) -> u8 {
        match self {
            IndexKey::ResourcesByType => 0x01,
            IndexKey::TokenCode { .. } => 0x02,
            IndexKey::TokenSystemCode { .. } => 0x03,
            IndexKey::TokenSystem { .. } => 0x04,
            IndexKey::Date { .. } => 0x05,
            IndexKey::DateByEnd { .. } => 0x06,
            IndexKey::Uri { .. } => 0x07,
        }
    }

    /// The index row this key names for a version whose `last_updated` is
    /// `last_updated`: the one rule that says whether two index rows of a
    /// resource are the same row (IW7 = RV2, ADR-sam-michaels-0006).
    ///
    /// A row is its table's primary key. Every field of an `IndexKey` is a
    /// primary-key column of its table, and `tenant_id`, `resource_type` and
    /// `id` are the resource's own, so what is left to decide is whether the
    /// version's `last_updated` is part of the key. It is for
    /// `resources_by_type`, the three token tables and `uri_index`, where it
    /// is a clustering column. It is not for `date_index` and
    /// `date_index_by_end` (`schema.cql`: `((tenant_id, resource_type,
    /// param), start, id, end)` and `(…, end, id, start)`), so a date key
    /// names the same row in every version that has it.
    ///
    /// Comparing `(key, last_updated)` pairs instead, as the index delta did
    /// until Step 5, treats a date key that two versions share as two rows:
    /// the update deletes the row and re-inserts it, both at `ts(N+1)`, and
    /// Cassandra lets a delete win a tie at the same timestamp, so the row is
    /// gone for good (measured on the dev node: 0 rows left, as separate
    /// statements and in one batch; walkthrough section 2, "Index rows": "only
    /// date keys survive an update unchanged").
    ///
    /// `version_id` is a regular column, not part of the row: rewriting a row
    /// with a new tag is an insert of the same row.
    pub(crate) fn row_at(&self, last_updated: DateTime<Utc>) -> RowId<'_> {
        let last_updated = match self {
            IndexKey::ResourcesByType
            | IndexKey::TokenCode { .. }
            | IndexKey::TokenSystemCode { .. }
            | IndexKey::TokenSystem { .. }
            | IndexKey::Uri { .. } => Some(last_updated),
            IndexKey::Date { .. } | IndexKey::DateByEnd { .. } => None,
        };
        RowId {
            key: self,
            last_updated,
        }
    }

    /// The partition this key's row lives in, within the resource's tenant
    /// and type: its table's partition-key columns from `schema.cql`.
    ///
    /// The index writer sends all rows of one partition, inserts and deletes,
    /// as one unit, which Cassandra applies whole on each replica (IW9 B,
    /// ADR-sam-michaels-0006). An update's `resources_by_type` rows at
    /// `lu(N)` and `lu(N+1)` share the `(tenant_id, resource_type)`
    /// partition, so once the batch has reached a quorum an update alone
    /// never lets `count` see both. `count` is still off by one either way,
    /// twice or not at all, after an unfinished write or through read repair
    /// (ADR-sam-michaels-0006, IW9). Rows of one key at two
    /// versions always share a partition, since `last_updated` is never a
    /// partition-key column; a changed value may or may not (a token's `code`
    /// is in the partition key of `token_code_index`, but a clustering column
    /// of `token_system_index`).
    pub(crate) fn partition(&self) -> Partition<'_> {
        match self {
            IndexKey::ResourcesByType => Partition::ResourcesByType,
            IndexKey::TokenCode { param, code, .. } => Partition::TokenCode { param, code },
            IndexKey::TokenSystemCode {
                param,
                system,
                code,
            } => Partition::TokenSystemCode {
                param,
                system,
                code,
            },
            IndexKey::TokenSystem { param, system, .. } => Partition::TokenSystem { param, system },
            IndexKey::Date { param, .. } => Partition::Date { param },
            IndexKey::DateByEnd { param, .. } => Partition::DateByEnd { param },
            IndexKey::Uri { param, uri } => Partition::Uri { param, uri },
        }
    }
}

/// One index row of a resource, as [`IndexKey::row_at`] names it: two rows
/// are the same row exactly when their `RowId`s are equal. Built only by
/// `row_at`, so no caller can compare rows by another rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct RowId<'a> {
    key: &'a IndexKey,
    /// `None` for the two date tables, whose primary key has no
    /// `last_updated`.
    last_updated: Option<DateTime<Utc>>,
}

#[cfg(test)]
impl RowId<'_> {
    /// The `last_updated` this row is keyed by; `None` for a date row (IW7).
    pub(crate) fn last_updated(&self) -> Option<DateTime<Utc>> {
        self.last_updated
    }
}

/// One index partition of a resource's tenant and type, as
/// [`IndexKey::partition`] names it: the table and its partition-key columns
/// other than `tenant_id` and `resource_type`. Two tables never share a
/// partition, even with the same columns (`date_index` and
/// `date_index_by_end`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Partition<'a> {
    /// `resources_by_type`: `(tenant_id, resource_type)`.
    ResourcesByType,
    /// `token_code_index`: `(…, param, code)`.
    TokenCode { param: &'a str, code: &'a str },
    /// `token_system_code_index`: `(…, param, system, code)`.
    TokenSystemCode {
        param: &'a str,
        system: &'a str,
        code: &'a str,
    },
    /// `token_system_index`: `(…, param, system)`.
    TokenSystem { param: &'a str, system: &'a str },
    /// `date_index`: `(…, param)`.
    Date { param: &'a str },
    /// `date_index_by_end`: `(…, param)`.
    DateByEnd { param: &'a str },
    /// `uri_index`: `(…, param, uri)`.
    Uri { param: &'a str, uri: &'a str },
}

/// A blob that is not a format-1 key set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DecodeError(String);

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid index_keys blob: {}", self.0)
    }
}

/// Encodes `keys` in format 1, in the order given.
pub(crate) fn encode(keys: &[IndexKey]) -> Vec<u8> {
    let mut out = vec![FORMAT_1];
    put_varint(&mut out, keys.len() as u64);
    for key in keys {
        out.push(key.tag());
        match key {
            IndexKey::ResourcesByType => {}
            IndexKey::TokenCode {
                param,
                code,
                system,
            } => {
                put_text(&mut out, param);
                put_text(&mut out, code);
                put_text(&mut out, system);
            }
            IndexKey::TokenSystemCode {
                param,
                system,
                code,
            }
            | IndexKey::TokenSystem {
                param,
                system,
                code,
            } => {
                put_text(&mut out, param);
                put_text(&mut out, system);
                put_text(&mut out, code);
            }
            IndexKey::Date { param, start, end } => {
                put_text(&mut out, param);
                out.extend_from_slice(&start.to_be_bytes());
                out.extend_from_slice(&end.to_be_bytes());
            }
            IndexKey::DateByEnd { param, end, start } => {
                put_text(&mut out, param);
                out.extend_from_slice(&end.to_be_bytes());
                out.extend_from_slice(&start.to_be_bytes());
            }
            IndexKey::Uri { param, uri } => {
                put_text(&mut out, param);
                put_text(&mut out, uri);
            }
        }
    }
    out
}

/// Decodes a format-1 key set.
pub(crate) fn decode(blob: &[u8]) -> Result<Vec<IndexKey>, DecodeError> {
    let mut reader = Reader { blob, at: 0 };
    let format = reader.byte()?;
    if format != FORMAT_1 {
        return Err(DecodeError(format!("unknown format byte 0x{format:02x}")));
    }
    let count = reader.varint()?;
    let mut keys = Vec::with_capacity(count.min(1024) as usize);
    for _ in 0..count {
        let key = match reader.byte()? {
            0x01 => IndexKey::ResourcesByType,
            0x02 => IndexKey::TokenCode {
                param: reader.text()?,
                code: reader.text()?,
                system: reader.text()?,
            },
            0x03 => IndexKey::TokenSystemCode {
                param: reader.text()?,
                system: reader.text()?,
                code: reader.text()?,
            },
            0x04 => IndexKey::TokenSystem {
                param: reader.text()?,
                system: reader.text()?,
                code: reader.text()?,
            },
            0x05 => IndexKey::Date {
                param: reader.text()?,
                start: reader.millis()?,
                end: reader.millis()?,
            },
            0x06 => IndexKey::DateByEnd {
                param: reader.text()?,
                end: reader.millis()?,
                start: reader.millis()?,
            },
            0x07 => IndexKey::Uri {
                param: reader.text()?,
                uri: reader.text()?,
            },
            tag => return Err(DecodeError(format!("unknown table tag 0x{tag:02x}"))),
        };
        keys.push(key);
    }
    if reader.at != blob.len() {
        return Err(DecodeError(format!(
            "{} trailing bytes",
            blob.len() - reader.at
        )));
    }
    Ok(keys)
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

fn put_text(out: &mut Vec<u8>, text: &str) {
    put_varint(out, text.len() as u64);
    out.extend_from_slice(text.as_bytes());
}

struct Reader<'a> {
    blob: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    fn byte(&mut self) -> Result<u8, DecodeError> {
        let byte = *self
            .blob
            .get(self.at)
            .ok_or_else(|| DecodeError("truncated".to_string()))?;
        self.at += 1;
        Ok(byte)
    }

    fn varint(&mut self) -> Result<u64, DecodeError> {
        let mut value = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = self.byte()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(DecodeError("varint longer than 64 bits".to_string()))
    }

    fn take(&mut self, len: usize) -> Result<&[u8], DecodeError> {
        let end = self
            .at
            .checked_add(len)
            .filter(|end| *end <= self.blob.len())
            .ok_or_else(|| DecodeError("truncated".to_string()))?;
        let slice = &self.blob[self.at..end];
        self.at = end;
        Ok(slice)
    }

    fn text(&mut self) -> Result<String, DecodeError> {
        let len = usize::try_from(self.varint()?)
            .map_err(|_| DecodeError("text length overflows".to_string()))?;
        String::from_utf8(self.take(len)?.to_vec())
            .map_err(|_| DecodeError("text is not UTF-8".to_string()))
    }

    fn millis(&mut self) -> Result<i64, DecodeError> {
        let bytes: [u8; 8] = self.take(8)?.try_into().expect("eight bytes");
        Ok(i64::from_be_bytes(bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MRN: &str = "http://hospital.example/mrn";
    const IMPLICIT: &str = "urn:x-helios:implicit code system";
    const B0: i64 = 326_073_600_000; // 1980-05-01T00:00:00Z
    const B1: i64 = 326_160_000_000; // 1980-05-02T00:00:00Z

    /// The worked example's K(1), walkthrough section 7.
    fn worked_example() -> Vec<IndexKey> {
        let text = |s: &str| s.to_string();
        vec![
            IndexKey::ResourcesByType,
            IndexKey::TokenCode {
                param: text("identifier"),
                code: text("12345"),
                system: text(MRN),
            },
            IndexKey::TokenSystemCode {
                param: text("identifier"),
                system: text(MRN),
                code: text("12345"),
            },
            IndexKey::TokenSystem {
                param: text("identifier"),
                system: text(MRN),
                code: text("12345"),
            },
            IndexKey::TokenCode {
                param: text("gender"),
                code: text("male"),
                system: text(IMPLICIT),
            },
            IndexKey::TokenSystemCode {
                param: text("gender"),
                system: text(IMPLICIT),
                code: text("male"),
            },
            IndexKey::Date {
                param: text("birthdate"),
                start: B0,
                end: B1,
            },
            IndexKey::DateByEnd {
                param: text("birthdate"),
                end: B1,
                start: B0,
            },
        ]
    }

    #[test]
    fn the_worked_example_is_289_bytes_as_the_walkthrough_says() {
        let blob = encode(&worked_example());
        assert_eq!(blob.len(), 289);
        assert_eq!(decode(&blob).unwrap(), worked_example());
    }

    #[test]
    fn the_empty_set_and_the_create_set() {
        assert_eq!(encode(&[]), EMPTY.to_vec());
        assert_eq!(decode(&EMPTY).unwrap(), Vec::<IndexKey>::new());
        assert_eq!(encode(&[IndexKey::ResourcesByType]), vec![0x01, 0x01, 0x01]);
    }

    #[test]
    fn varints_and_long_text_round_trip() {
        let long = "u".repeat(2_048);
        let keys = vec![IndexKey::Uri {
            param: "url".to_string(),
            uri: long.clone(),
        }];
        let blob = encode(&keys);
        // 2,048 needs a two-byte varint: 0x80 0x10.
        assert_eq!(&blob[7..9], &[0x80, 0x10]);
        assert_eq!(decode(&blob).unwrap(), keys);
    }

    const LU1: &str = "2026-11-16T10:00:00.000Z";
    const LU2: &str = "2026-11-16T10:00:00.001Z";

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339).unwrap().to_utc()
    }

    /// What `row_at` and `partition` must say about one table, against its
    /// primary key in `schema.cql`. `last_updated_in_key`: whether one key
    /// at two versions names two rows. `changes`: `base` with one field
    /// changed, each with whether that field is a clustering column (the row
    /// changes, the partition does not) or a partition-key column (both
    /// change). Every `IndexKey` field is a primary-key column, so every
    /// change gives another row.
    fn check_table(base: IndexKey, last_updated_in_key: bool, changes: &[(&str, IndexKey, bool)]) {
        let (lu1, lu2) = (at(LU1), at(LU2));
        assert_eq!(base.row_at(lu1), base.row_at(lu1), "{base:?}");
        assert_eq!(
            base.row_at(lu1) != base.row_at(lu2),
            last_updated_in_key,
            "{base:?}: is last_updated part of the row?"
        );
        // `partition` takes no `last_updated`: a key's rows at two versions
        // always share a partition.
        for (field, changed, clustering) in changes {
            assert_ne!(
                base.row_at(lu1),
                changed.row_at(lu1),
                "{base:?}: {field} is part of the row"
            );
            assert_eq!(
                base.partition() == changed.partition(),
                *clustering,
                "{base:?}: is {field} a clustering column?"
            );
        }
    }

    fn token_code(param: &str, code: &str, system: &str) -> IndexKey {
        IndexKey::TokenCode {
            param: param.to_string(),
            code: code.to_string(),
            system: system.to_string(),
        }
    }

    fn token_system_code(param: &str, system: &str, code: &str) -> IndexKey {
        IndexKey::TokenSystemCode {
            param: param.to_string(),
            system: system.to_string(),
            code: code.to_string(),
        }
    }

    fn token_system(param: &str, system: &str, code: &str) -> IndexKey {
        IndexKey::TokenSystem {
            param: param.to_string(),
            system: system.to_string(),
            code: code.to_string(),
        }
    }

    fn date(param: &str, start: i64, end: i64) -> IndexKey {
        IndexKey::Date {
            param: param.to_string(),
            start,
            end,
        }
    }

    fn date_by_end(param: &str, end: i64, start: i64) -> IndexKey {
        IndexKey::DateByEnd {
            param: param.to_string(),
            end,
            start,
        }
    }

    fn uri(param: &str, uri: &str) -> IndexKey {
        IndexKey::Uri {
            param: param.to_string(),
            uri: uri.to_string(),
        }
    }

    /// `((tenant_id, resource_type), last_updated, id)`: an update moves the
    /// row, and both versions' rows share the type's one partition, which is
    /// what keeps `count` exact under IW9.
    #[test]
    fn a_resources_by_type_row_is_its_last_updated() {
        check_table(IndexKey::ResourcesByType, true, &[]);
    }

    /// `((…, param, code), last_updated, id, system)`.
    #[test]
    fn a_token_code_row_is_param_code_system_and_last_updated() {
        check_table(
            token_code("gender", "male", IMPLICIT),
            true,
            &[
                ("param", token_code("other", "male", IMPLICIT), false),
                ("code", token_code("gender", "female", IMPLICIT), false),
                ("system", token_code("gender", "male", MRN), true),
            ],
        );
    }

    /// `((…, param, system, code), last_updated, id)`.
    #[test]
    fn a_token_system_code_row_is_param_system_code_and_last_updated() {
        check_table(
            token_system_code("identifier", MRN, "12345"),
            true,
            &[
                ("param", token_system_code("other", MRN, "12345"), false),
                (
                    "system",
                    token_system_code("identifier", IMPLICIT, "12345"),
                    false,
                ),
                ("code", token_system_code("identifier", MRN, "67890"), false),
            ],
        );
    }

    /// `((…, param, system), last_updated, id, code)`.
    #[test]
    fn a_token_system_row_is_param_system_code_and_last_updated() {
        check_table(
            token_system("identifier", MRN, "12345"),
            true,
            &[
                ("param", token_system("other", MRN, "12345"), false),
                (
                    "system",
                    token_system("identifier", IMPLICIT, "12345"),
                    false,
                ),
                ("code", token_system("identifier", MRN, "67890"), true),
            ],
        );
    }

    /// `((…, param), start, id, end)`: no `last_updated`, so a date both
    /// versions have is one row, which an update only re-inserts (IW7).
    #[test]
    fn a_date_row_is_param_start_and_end_without_last_updated() {
        check_table(
            date("birthdate", B0, B1),
            false,
            &[
                ("param", date("death-date", B0, B1), false),
                ("start", date("birthdate", B0 - 1, B1), true),
                ("end", date("birthdate", B0, B1 + 1), true),
            ],
        );
    }

    /// `((…, param), end, id, start)`: as `date_index`.
    #[test]
    fn a_date_by_end_row_is_param_end_and_start_without_last_updated() {
        check_table(
            date_by_end("birthdate", B1, B0),
            false,
            &[
                ("param", date_by_end("death-date", B1, B0), false),
                ("end", date_by_end("birthdate", B1 + 1, B0), true),
                ("start", date_by_end("birthdate", B1, B0 - 1), true),
            ],
        );
    }

    /// `((…, param, uri), last_updated, id)`.
    #[test]
    fn a_uri_row_is_param_uri_and_last_updated() {
        check_table(
            uri("url", "http://example.org/a"),
            true,
            &[
                ("param", uri("other", "http://example.org/a"), false),
                ("uri", uri("url", "http://example.org/b"), false),
            ],
        );
    }

    /// The same columns in two tables are two rows in two partitions:
    /// `date_index` and `date_index_by_end` share `(…, param)`, and the token
    /// tables share their fields.
    #[test]
    fn two_tables_never_share_a_row_or_a_partition() {
        let lu = at(LU1);
        let pairs = [
            (date("birthdate", B0, B1), date_by_end("birthdate", B1, B0)),
            (
                token_system_code("identifier", MRN, "12345"),
                token_system("identifier", MRN, "12345"),
            ),
            (
                token_code("identifier", "12345", MRN),
                token_system_code("identifier", MRN, "12345"),
            ),
        ];
        for (a, b) in pairs {
            assert_ne!(a.row_at(lu), b.row_at(lu), "{a:?} / {b:?}");
            assert_ne!(a.partition(), b.partition(), "{a:?} / {b:?}");
        }
    }

    #[test]
    fn malformed_blobs_are_refused() {
        for blob in [
            vec![],
            vec![0x02, 0x00],
            vec![0x01],
            vec![0x01, 0x01],
            vec![0x01, 0x01, 0x09],
            vec![0x01, 0x01, 0x07, 0x05, b'a'],
            vec![0x01, 0x00, 0x00],
        ] {
            assert!(decode(&blob).is_err(), "{blob:?}");
        }
    }
}
