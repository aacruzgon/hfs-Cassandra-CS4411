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

use std::fmt;

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
