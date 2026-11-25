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
//! Every live version's key set comes from the shared extractor
//! ([`extract_index_keys`]), its long texts mapped once by [`index_text`]
//! (ADR-aacruzgon-0079); a key read back from `index_keys` is never mapped
//! again.
//!
//! **Which row a key names** is decided here and nowhere else
//! ([`IndexKey::row_at`], [`IndexKey::partition`]), from each index table's
//! primary key in `schema.cql`. The write path's index delta, the sweeper,
//! repair on read and the consistency check all compare rows through these
//! two functions, so a rule written once cannot drift between them (IW7,
//! IW9, ADR-sam-michaels-0006).

use std::collections::BTreeSet;
use std::fmt;

use chrono::{DateTime, Utc};
use serde_json::Value as Json;
use sha2::{Digest, Sha256};

use crate::search::{
    ExtractedValue, IMPLICIT_TOKEN_SYSTEM, IndexValue, SearchParameterExtractor, StorageResolution,
    indexed_range,
};
use crate::types::SearchParamType;

/// The format byte this module writes.
pub(crate) const FORMAT_1: u8 = 0x01;

/// The empty key set, `0x01 0x00`: a delete marker's.
pub(crate) const EMPTY: [u8; 2] = [FORMAT_1, 0x00];

/// The longest indexed text stored as it is, in UTF-8 bytes (ADR-aacruzgon-0079).
pub(crate) const MAX_INDEX_TEXT_BYTES: usize = 1024;
/// What a hashed indexed text starts with (ADR-aacruzgon-0079).
pub(crate) const HASHED_TEXT_PREFIX: &str = "sha256:";
/// Parameters no key is made for: `resources` and `resources_by_type` serve them (P5, G18).
const UNINDEXED_PARAMS: [&str; 2] = ["_id", "_lastUpdated"];

/// One index row, by the key columns the version row does not imply.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    /// never lets `count` see both. `count` can still be off
    /// (ADR-aacruzgon-0081, decision 2): too high, by up to one per
    /// unfinished update or delete, and one too low after an unfinished create
    /// or revive, until the sweeper or repair finishes it; and on RF 3, twice
    /// or not at all through read repair after a write that reached fewer than
    /// a quorum. Rows of one key at two versions always share a partition,
    /// since `last_updated` is never a
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

/// ADR-aacruzgon-0079's mapping (IW6): `value` itself when it is at most
/// [`MAX_INDEX_TEXT_BYTES`] bytes and does not start with
/// [`HASHED_TEXT_PREFIX`]; otherwise the prefix and the 64 lowercase hex
/// digits of SHA-256 over its UTF-8 bytes. No other normalisation. Not
/// idempotent (a digest starts with the prefix), so it is applied once, to
/// fresh extractions, never to keys decoded from `index_keys`.
pub(crate) fn index_text(value: &str) -> String {
    if value.len() <= MAX_INDEX_TEXT_BYTES && !value.starts_with(HASHED_TEXT_PREFIX) {
        return value.to_string();
    }
    let digest = Sha256::digest(value.as_bytes());
    let mut text = String::with_capacity(HASHED_TEXT_PREFIX.len() + 2 * digest.len());
    text.push_str(HASHED_TEXT_PREFIX);
    for byte in digest {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

/// `K(V)` of a live version from its extracted values (IW2 to IW6, P5),
/// sorted and deduplicated, with the `resources_by_type` key (IW5).
pub(crate) fn index_keys_from(values: &[ExtractedValue]) -> Vec<IndexKey> {
    let mut keys = BTreeSet::from([IndexKey::ResourcesByType]);
    for extracted in values {
        // A composite's components are not indexed on their own (IW3).
        if extracted.composite_group.is_some()
            || UNINDEXED_PARAMS.contains(&extracted.param_name.as_str())
        {
            continue;
        }
        let param = index_text(&extracted.param_name);
        match (&extracted.param_type, &extracted.value) {
            (SearchParamType::Token, IndexValue::Token { system, code, .. }) => {
                if code.is_empty() {
                    continue;
                }
                let code = index_text(code);
                let system = index_text(system.as_deref().unwrap_or(""));
                keys.insert(IndexKey::TokenCode {
                    param: param.clone(),
                    code: code.clone(),
                    system: system.clone(),
                });
                keys.insert(IndexKey::TokenSystemCode {
                    param: param.clone(),
                    system: system.clone(),
                    code: code.clone(),
                });
                if !system.is_empty() && system != IMPLICIT_TOKEN_SYSTEM {
                    keys.insert(IndexKey::TokenSystem {
                        param,
                        system,
                        code,
                    });
                }
            }
            (SearchParamType::Date, value @ IndexValue::Date { .. }) => {
                // A value the date grammar does not accept has no range (IW4).
                let Some((start, end)) = indexed_range(value, StorageResolution::Millis) else {
                    continue;
                };
                let (start, end) = (start.timestamp_millis(), end.timestamp_millis());
                keys.insert(IndexKey::Date {
                    param: param.clone(),
                    start,
                    end,
                });
                keys.insert(IndexKey::DateByEnd { param, end, start });
            }
            (SearchParamType::Uri, IndexValue::Uri(uri)) => {
                if uri.is_empty() {
                    continue;
                }
                keys.insert(IndexKey::Uri {
                    param,
                    uri: index_text(uri),
                });
            }
            _ => {}
        }
    }
    keys.into_iter().collect()
}

/// What one extraction gave a write (IW1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Extraction {
    /// `K(V)`: [`index_keys_from`], or `[ResourcesByType]` alone when the whole resource failed.
    pub(crate) keys: Vec<IndexKey>,
    /// The codes of the parameters the extractor skipped because they failed.
    pub(crate) failed_params: Vec<String>,
    /// Why the whole resource could not be extracted (D6), if it could not.
    pub(crate) failed_resource: Option<String>,
}

/// The stack of [`extract_index_keys`]'s thread in a debug build.
///
/// FHIRPath evaluates a union or operator chain with one stack frame per
/// term, about 32 KB each in a debug build. On a 2 MiB thread, a tokio
/// worker's or a test's, FHIRPath measured that debug survives a chain of 64
/// and aborts at 80, while release survives 300; its parser refuses more than
/// `MAX_EXPRESSION_DEPTH` (250), so release is covered and debug is not
/// (`crates/fhirpath/src/lib.rs`). R5's shared parameters (`url`, `name`,
/// `status` and the like) are unions over some 60 resource types, so creating
/// an R5 `SearchParameter` overflowed a debug test thread. A 250-term chain
/// needs about 8 MB in debug. Pages are committed only as they are touched.
/// This is the size FHIRPath gives its own deep-parse thread (`run_parse`).
const EXTRACTION_STACK_BYTES: usize = 64 * 1024 * 1024;

/// Runs `extractor.extract_with_missing(body, resource_type)` and maps it (IW1, D6).
/// A debug build runs it on a scoped thread with an [`EXTRACTION_STACK_BYTES`]
/// stack, since a stack overflow aborts the whole process and cannot unwind.
/// A release build, which FHIRPath's depth limit already covers, runs it on
/// the caller's stack and starts no thread (about 45 µs a write).
/// Never returns an error: a resource that cannot be extracted keeps only its
/// `resources_by_type` key, and so does one whose thread cannot be started.
/// A panic inside the extractor is resumed on the caller's thread.
pub(crate) fn extract_index_keys(
    extractor: &SearchParameterExtractor,
    resource_type: &str,
    body: &Json,
) -> Extraction {
    if !cfg!(debug_assertions) {
        return extract_on_this_stack(extractor, resource_type, body);
    }
    std::thread::scope(|scope| {
        let spawned = std::thread::Builder::new()
            .name("hfs-extraction".to_string())
            .stack_size(EXTRACTION_STACK_BYTES)
            .spawn_scoped(scope, || {
                extract_on_this_stack(extractor, resource_type, body)
            });
        match spawned {
            Ok(handle) => handle
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic)),
            Err(error) => Extraction {
                keys: vec![IndexKey::ResourcesByType],
                failed_params: Vec::new(),
                failed_resource: Some(format!("the extraction thread did not start: {error}")),
            },
        }
    })
}

/// [`extract_index_keys`]'s work, on whatever stack calls it.
fn extract_on_this_stack(
    extractor: &SearchParameterExtractor,
    resource_type: &str,
    body: &Json,
) -> Extraction {
    match extractor.extract_with_missing(body, resource_type) {
        Ok(extraction) => Extraction {
            keys: index_keys_from(&extraction.values),
            failed_params: extraction
                .failures
                .into_iter()
                .map(|failure| failure.code)
                .collect(),
            failed_resource: None,
        },
        Err(error) => Extraction {
            keys: vec![IndexKey::ResourcesByType],
            failed_params: Vec::new(),
            failed_resource: Some(error.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::cassandra::test_support::{test_extractor, test_registry};
    use crate::search::registry::SearchParameterDefinition;
    use crate::search::{DateEnd, OPEN_START};
    use crate::types::DatePrecision;
    use serde_json::json;

    const MRN: &str = "http://hospital.example/mrn";
    const IMPLICIT: &str = "urn:x-helios:implicit code system";
    const B0: i64 = 325_987_200_000; // 1980-05-01T00:00:00Z
    const B1: i64 = 326_073_600_000; // 1980-05-02T00:00:00Z

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
    /// row, and both versions' rows share the type's one partition, so an
    /// update alone never lets a quorum read see both once its batch has
    /// reached a quorum (IW9; the other cases are in ADR-aacruzgon-0081,
    /// decision 2).
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

    /// `"sha256:"` and the lowercase hex of SHA-256 over `value`'s bytes,
    /// computed here and not by [`index_text`].
    fn digest(value: &str) -> String {
        let hex: String = Sha256::digest(value.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        format!("sha256:{hex}")
    }

    fn long(n: usize, c: char) -> String {
        c.to_string().repeat(n)
    }

    #[test]
    fn index_text_keeps_short_values_and_hashes_long_or_marked_ones() {
        assert_eq!(index_text(""), "");
        assert_eq!(index_text(&long(1024, 'a')), long(1024, 'a'));
        let hashed = index_text(&long(1025, 'a'));
        assert_eq!(hashed, digest(&long(1025, 'a')));
        assert_eq!(hashed.len(), 71);
        assert!(
            hashed[7..]
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        );
        let two_bytes = "é".repeat(512);
        assert_eq!(two_bytes.len(), 1_024);
        assert_eq!(index_text(&two_bytes), two_bytes);
        let too_long = "é".repeat(513);
        assert_eq!(index_text(&too_long), digest(&too_long));
        assert_eq!(index_text("sha256:short"), digest("sha256:short"));
        assert_eq!(index_text("Male "), "Male ");
        // Not idempotent: a digest starts with the prefix, so it is hashed again.
        assert_ne!(index_text(&hashed), hashed);
        assert_eq!(index_text(&hashed), digest(&hashed));
    }

    fn value(name: &str, ty: SearchParamType, value: IndexValue) -> ExtractedValue {
        ExtractedValue::new(name, format!("http://example.org/sp/{name}"), ty, value)
    }

    fn token(name: &str, system: Option<&str>, code: &str) -> ExtractedValue {
        value(
            name,
            SearchParamType::Token,
            IndexValue::token(system.map(str::to_string), code),
        )
    }

    fn ev_date(name: &str, text: &str) -> ExtractedValue {
        value(name, SearchParamType::Date, IndexValue::date(text))
    }

    fn ev_uri(name: &str, text: &str) -> ExtractedValue {
        value(name, SearchParamType::Uri, IndexValue::uri(text))
    }

    /// IW2 to IW5 and P5 (gate): each value form maps to its rows.
    #[test]
    fn token_forms_map_to_the_three_tables() {
        let values = vec![
            token("identifier", Some(MRN), "12345"),
            token("identifier", Some(MRN), "12345"),
            value(
                "gender",
                SearchParamType::Token,
                IndexValue::token_implicit_system("male"),
            ),
            token("active", None, "true"),
            token("code", Some("http://loinc.org"), ""),
            value(
                "code",
                SearchParamType::Token,
                IndexValue::token_display_only("BP"),
            ),
            value(
                "_id",
                SearchParamType::Token,
                IndexValue::token_implicit_system("p1"),
            ),
            ev_date("_lastUpdated", "2026-11-22T10:00:00Z"),
            ev_uri("_profile", "http://example.org/profile"),
            ev_uri("_source", ""),
            token("code-value-quantity", Some("http://loinc.org"), "8480-6")
                .with_composite_group(1),
            ev_date("birthdate", "1980-05-01"),
            // Four fraction digits: stored in milliseconds, the value's own
            // millisecond (IW4). In microseconds, `timestamp_millis()` would
            // give the empty range `[…123, …123)`.
            ev_date("effective", "2020-01-01T10:00:00.1234Z"),
            ev_date("death-date", "not a date"),
            value("name", SearchParamType::String, IndexValue::string("Smith")),
            value(
                "general-practitioner",
                SearchParamType::Reference,
                IndexValue::reference("Practitioner/1"),
            ),
            value(
                "period",
                SearchParamType::Date,
                IndexValue::Date {
                    value: OPEN_START.into(),
                    precision: DatePrecision::Second,
                    end: DateEnd::Open,
                },
            ),
        ];
        let mut expected = vec![
            IndexKey::ResourcesByType,
            token_code("identifier", "12345", MRN),
            token_system_code("identifier", MRN, "12345"),
            token_system("identifier", MRN, "12345"),
            token_code("gender", "male", IMPLICIT),
            token_system_code("gender", IMPLICIT, "male"),
            token_code("active", "true", ""),
            token_system_code("active", "", "true"),
            IndexKey::Uri {
                param: "_profile".into(),
                uri: "http://example.org/profile".into(),
            },
            IndexKey::Date {
                param: "birthdate".into(),
                start: B0,
                end: B1,
            },
            IndexKey::DateByEnd {
                param: "birthdate".into(),
                end: B1,
                start: B0,
            },
            IndexKey::Date {
                param: "effective".into(),
                start: 1_577_872_800_123,
                end: 1_577_872_800_124,
            },
            IndexKey::DateByEnd {
                param: "effective".into(),
                end: 1_577_872_800_124,
                start: 1_577_872_800_123,
            },
            IndexKey::Date {
                param: "period".into(),
                start: -62_135_596_800_000,
                end: 253_402_300_799_999,
            },
            IndexKey::DateByEnd {
                param: "period".into(),
                end: 253_402_300_799_999,
                start: -62_135_596_800_000,
            },
        ];
        expected.sort();
        let keys = index_keys_from(&values);
        assert_eq!(keys, expected);
        assert_eq!(keys.len(), 15);
        assert_eq!(keys[0], IndexKey::ResourcesByType);
        assert!(
            !keys.iter().any(|key| matches!(key,
                IndexKey::TokenSystem { system, .. } if system.is_empty() || system == IMPLICIT)),
            "no token_system_index row for the implicit or empty system"
        );
    }

    #[test]
    fn long_texts_are_hashed_once_when_a_key_set_is_made() {
        let (p, s, c) = (long(1025, 'p'), long(1025, 's'), long(1025, 'c'));
        let (u, d) = (long(2000, 'u'), long(1025, 'd'));
        let values = vec![
            value(
                &p,
                SearchParamType::Token,
                IndexValue::token(Some(s.clone()), c.clone()),
            ),
            ev_uri("_source", &u),
            ev_date(&d, "1980-05-01"),
            token("identifier", Some("http://example.org/ids"), "sha256:short"),
        ];
        let keys = index_keys_from(&values);
        let (hp, hs, hc) = (index_text(&p), index_text(&s), index_text(&c));
        for expected in [
            token_code(&hp, &hc, &hs),
            token_system_code(&hp, &hs, &hc),
            token_system(&hp, &hs, &hc),
            IndexKey::Uri {
                param: "_source".into(),
                uri: index_text(&u),
            },
            IndexKey::Date {
                param: index_text(&d),
                start: B0,
                end: B1,
            },
            IndexKey::DateByEnd {
                param: index_text(&d),
                end: B1,
                start: B0,
            },
            token_code(
                "identifier",
                &digest("sha256:short"),
                "http://example.org/ids",
            ),
        ] {
            assert!(keys.contains(&expected), "{expected:?}");
        }
        assert_eq!(hp, digest(&p), "hashed once, not twice");
        let texts = |key: &IndexKey| -> Vec<String> {
            match key {
                IndexKey::ResourcesByType | IndexKey::Date { .. } | IndexKey::DateByEnd { .. } => {
                    vec![]
                }
                IndexKey::TokenCode { code, system, .. }
                | IndexKey::TokenSystemCode { code, system, .. }
                | IndexKey::TokenSystem { code, system, .. } => {
                    vec![code.clone(), system.clone()]
                }
                IndexKey::Uri { uri, .. } => vec![uri.clone()],
            }
        };
        for key in &keys {
            assert!(texts(key).iter().all(|t| t.len() <= MAX_INDEX_TEXT_BYTES));
        }
        assert_eq!(decode(&encode(&keys)).unwrap(), keys);
        assert_eq!(index_keys_from(&values), keys);
    }

    /// The golden test: the walkthrough's 8 rows plus `deceased`'s 2. Section
    /// 7's Patient gives the worked example's eight rows, and two more: `deceased`, whose spec expression
    /// (`Patient.deceased.exists() and Patient.deceased != false`) is `false`
    /// for a Patient without `deceased[x]`, so every such Patient gets
    /// `deceased = false` rows (roadmap row 5.1, RV13).
    #[test]
    fn the_worked_example_extracts_its_eight_rows() {
        let body = json!({
            "resourceType": "Patient",
            "id": "p1",
            "identifier": [{"system": MRN, "value": "12345"}],
            "gender": "male",
            "birthDate": "1980-05-01"
        });
        let extraction = extract_index_keys(&test_extractor(), "Patient", &body);
        assert!(extraction.failed_params.is_empty());
        assert_eq!(extraction.failed_resource, None);
        let mut expected = worked_example();
        expected.push(token_code("deceased", "false", ""));
        expected.push(token_system_code("deceased", "", "false"));
        expected.sort();
        for key in worked_example() {
            assert!(extraction.keys.contains(&key), "{key:?}");
        }
        assert_eq!(extraction.keys, expected);
        assert_eq!(encode(&extraction.keys).len(), 323);
    }

    /// IW1 and D6: a parameter that fails is skipped and named; a resource
    /// that cannot be extracted keeps only its type key.
    #[test]
    fn a_failed_parameter_is_skipped_and_a_failed_resource_keeps_its_type_key() {
        let registry = test_registry();
        registry
            .write()
            .register(
                SearchParameterDefinition::new(
                    "http://example.org/SearchParameter/broken",
                    "broken",
                    SearchParamType::Token,
                    "Patient.name.single()",
                )
                .with_base(vec!["Patient"]),
            )
            .unwrap();
        let extractor = SearchParameterExtractor::new(registry);
        let body = json!({
            "resourceType": "Patient", "id": "p", "gender": "male",
            "name": [{"family": "One"}, {"family": "Two"}]
        });
        let extraction = extract_index_keys(&extractor, "Patient", &body);
        assert_eq!(extraction.failed_params, vec!["broken".to_string()]);
        assert_eq!(extraction.failed_resource, None);
        assert!(
            extraction
                .keys
                .contains(&token_code("gender", "male", IMPLICIT))
        );
        let has_broken = extraction.keys.iter().any(|key| match key {
            IndexKey::TokenCode { param, .. }
            | IndexKey::TokenSystemCode { param, .. }
            | IndexKey::TokenSystem { param, .. }
            | IndexKey::Date { param, .. }
            | IndexKey::DateByEnd { param, .. }
            | IndexKey::Uri { param, .. } => param == "broken",
            IndexKey::ResourcesByType => false,
        });
        assert!(!has_broken);

        for (body, resource_type) in [
            (json!(["x"]), "Patient"),
            (json!({"resourceType": "Observation", "id": "p"}), "Patient"),
        ] {
            let extraction = extract_index_keys(&extractor, resource_type, &body);
            assert_eq!(extraction.keys, vec![IndexKey::ResourcesByType]);
            assert!(extraction.failed_resource.is_some(), "{body}");
            assert!(extraction.failed_params.is_empty());
        }
    }

    /// FHIRPath evaluates a union with one stack frame per term, about 32 KB
    /// each in a debug build, so a 200-term union (under FHIRPath's chain
    /// limit of 250) needs about 6.4 MB there. A debug build's
    /// `extract_index_keys` runs it on its own stack, so it extracts from a
    /// 2 MiB thread, a tokio worker's size; run inline, it overflows that
    /// thread and aborts the test binary. A release build runs it inline,
    /// where FHIRPath measured that a 2 MiB thread survives 300.
    #[test]
    fn a_deep_union_extracts_from_a_two_mib_thread() {
        let registry = test_registry();
        let expression = vec!["Patient.gender"; 200].join(" | ");
        registry
            .write()
            .register(
                SearchParameterDefinition::new(
                    "http://example.org/SearchParameter/deep",
                    "deep",
                    SearchParamType::Token,
                    expression.as_str(),
                )
                .with_base(vec!["Patient"]),
            )
            .unwrap();
        let extractor = SearchParameterExtractor::new(registry);
        let body = json!({"resourceType": "Patient", "id": "p", "gender": "male"});
        let extraction = std::thread::Builder::new()
            .stack_size(2 << 20)
            .spawn(move || extract_index_keys(&extractor, "Patient", &body))
            .unwrap()
            .join()
            .unwrap();
        assert_eq!(extraction.failed_resource, None);
        assert!(extraction.failed_params.is_empty(), "{extraction:?}");
        assert!(
            extraction
                .keys
                .contains(&token_code("deep", "male", IMPLICIT)),
            "{extraction:?}"
        );
    }
}
