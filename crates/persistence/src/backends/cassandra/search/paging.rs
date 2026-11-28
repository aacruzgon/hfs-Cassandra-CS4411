//! Cursor paging for Cassandra search (roadmap 6d, #129).
//!
//! Every Cassandra search pages by keyset, never by offset. The cursor is a
//! plain [`PageCursor`] (`cassandra-discussion.md`, "The cursor is a plain
//! `PageCursor`"; walkthrough 5.7 and 5.10):
//!
//! - in the default order, `(last_updated DESC, id DESC)`:
//!   `sort_values = [<last_updated, RFC 3339 ms>, <reserved bucket slot>]`;
//! - in a date-only search, a date table's own order, `(start ASC, id ASC)` or
//!   `(end ASC, id ASC)`: a third element, `"start"` or `"end"`, names the
//!   table;
//! - `resource_id` is the id in both.
//!
//! The bucket slot is reserved for partition bucketing, which is not in
//! effect ("measure partition growth first, bucket on a trigger"), so it is
//! always `null`.
//!
//! Only this file encodes or decodes a cursor (ADR-anastasiabryz-0001), so a
//! cursor minted by one order can be refused with `InvalidCursor` (400) in
//! another.
//!
//! [`PageCursor`]: crate::types::PageCursor

use cdrs_tokio::types::value::Value;
use chrono::{DateTime, SecondsFormat, Utc};

use super::Candidate;
use crate::backends::cassandra::errors::BACKEND_NAME;
use crate::error::{BackendError, SearchError, StorageResult};
use crate::types::{CursorDirection, CursorValue, PageCursor};

/// The order a search's candidates stream in, which the plan decides.
#[allow(dead_code)] // 6b's date plans (#132) return the date orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Order {
    /// `(last_updated DESC, id DESC)`, on every table but the two date tables
    /// (walkthrough 5.1, step 2).
    Default,
    /// A date-only search sliced on `date_index`: `(start ASC, id ASC)`.
    DateStart,
    /// A date-only search sliced on `date_index_by_end`: `(end ASC, id ASC)`.
    DateEnd,
    /// `ne`, or a date comma list whose values slice both tables:
    /// `date_index` to its end, then `date_index_by_end` (walkthrough 5.7).
    DateStartThenEnd,
}

impl Order {
    /// Whether a position in `table` (`None`: no date table) belongs to this
    /// order.
    fn reads(self, table: Option<DateTable>) -> bool {
        matches!(
            (self, table),
            (Order::Default, None)
                | (Order::DateStart, Some(DateTable::Start))
                | (Order::DateEnd, Some(DateTable::End))
                | (Order::DateStartThenEnd, Some(_))
        )
    }
}

/// Which date table a date-order position is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DateTable {
    /// `date_index`, ordered by `start`; the cursor's `"start"`.
    Start,
    /// `date_index_by_end`, ordered by `end`; the cursor's `"end"`.
    End,
}

impl DateTable {
    /// The table's name in the cursor (walkthrough 5.7).
    fn name(self) -> &'static str {
        match self {
            DateTable::Start => "start",
            DateTable::End => "end",
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        match name {
            "start" => Some(DateTable::Start),
            "end" => Some(DateTable::End),
            _ => None,
        }
    }
}

/// Where a candidate sits in its stream's order: the instant it is ordered
/// by, and, in a date order, the table it was read from. The id breaks ties
/// and travels in [`super::Candidate::id`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrderKey {
    /// `last_updated` in the default order; `start` or `end` in a date order.
    pub(crate) at: DateTime<Utc>,
    /// `None` in the default order; the date table otherwise.
    pub(crate) table: Option<DateTable>,
}

/// A page's starting point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Position {
    /// The first page: no cursor.
    First(Order),
    /// Strictly after `(at, id)` in `order`. `table` names the date table,
    /// `None` in the default order.
    After {
        order: Order,
        at: DateTime<Utc>,
        id: String,
        table: Option<DateTable>,
    },
}

impl Position {
    /// The order this position is in.
    #[allow(dead_code)] // For 6b's date plans (#132).
    pub(crate) fn order(&self) -> Order {
        match self {
            Position::First(order) | Position::After { order, .. } => *order,
        }
    }
}

/// Decodes the request's cursor for a plan of order `expected`.
///
/// `None` is the first page. Everything else must be a cursor [`mint`] could
/// have made for `expected`, or it is `InvalidCursor` (400; `coverage.md`
/// section 5):
/// - not a [`PageCursor`] at all;
/// - a `Previous` cursor: Cassandra pages forward only and never mints one;
/// - an empty `resource_id`;
/// - `sort_values` that are not `[<instant>, null]` (default order) or
///   `[<instant>, null, "start" | "end"]` (a date order), the instant written
///   exactly as [`mint`] writes it, RFC 3339 with milliseconds and `Z`;
/// - a cursor of another order, or naming a date table the plan doesn't read
///   (walkthrough 5.7, "Cursor").
pub(crate) fn decode(cursor: Option<&str>, expected: Order) -> Result<Position, SearchError> {
    let Some(text) = cursor else {
        return Ok(Position::First(expected));
    };
    let invalid = || SearchError::InvalidCursor {
        cursor: text.to_string(),
    };

    let cursor = PageCursor::decode(text)?;
    if cursor.direction() != CursorDirection::Next || cursor.resource_id().is_empty() {
        return Err(invalid());
    }
    let (at, table) = match cursor.sort_values() {
        [CursorValue::String(at), CursorValue::Null] => (at, None),
        [
            CursorValue::String(at),
            CursorValue::Null,
            CursorValue::String(name),
        ] => (at, Some(DateTable::from_name(name).ok_or_else(invalid)?)),
        _ => return Err(invalid()),
    };
    if !expected.reads(table) {
        return Err(invalid());
    }
    let at = parse_instant(at).ok_or_else(invalid)?;

    Ok(Position::After {
        order: expected,
        at,
        id: cursor.resource_id().to_string(),
        table,
    })
}

/// Mints the `next` cursor from the last candidate of a page, in `order`.
///
/// The instant is written with milliseconds, the precision of a CQL
/// `timestamp`, so every instant read from the node round-trips exactly.
/// Fails when the candidate's order key does not belong to `order` (a date
/// table in the default order, or none in a date order): that is a bug in
/// the caller, never the client's doing.
pub(crate) fn mint(last: &Candidate, order: Order) -> StorageResult<String> {
    let table = last.order.table;
    if !order.reads(table) {
        return Err(BackendError::Internal {
            backend_name: BACKEND_NAME.to_string(),
            message: format!(
                "cannot mint a {order:?} cursor from a candidate whose order key is in {table:?}"
            ),
            source: None,
        }
        .into());
    }

    let mut sort_values = vec![
        CursorValue::String(format_instant(last.order.at)),
        CursorValue::Null,
    ];
    if let Some(table) = table {
        sort_values.push(CursorValue::String(table.name().to_string()));
    }
    Ok(PageCursor::new(sort_values, last.id.clone()).encode())
}

/// The keyset relation a read continues after `position` with, and its two
/// values; `None` on the first page.
///
/// - Default order: `(last_updated, id) < (?, ?)`. Multi-column relations
///   compare by value, so on the `DESC, DESC` order this is exactly the rows
///   after the cursor: it excludes every row of the cursor's own resource and
///   places ties within a millisecond by `id` (walkthrough 5.10, spike A3).
/// - Date orders: `(start, id) > (?, ?)` on `date_index` or
///   `(end, id) > (?, ?)` on `date_index_by_end`, by the position's table
///   (walkthrough 5.7). The plan keeps its slice's own bounds beside it.
pub(crate) fn keyset(position: &Position) -> Option<(&'static str, Vec<Value>)> {
    let Position::After { at, id, table, .. } = position else {
        return None;
    };
    let relation = match table {
        None => "(last_updated, id) < (?, ?)",
        Some(DateTable::Start) => "(start, id) > (?, ?)",
        Some(DateTable::End) => "(end, id) > (?, ?)",
    };
    Some((relation, vec![Value::from(*at), Value::from(id.as_str())]))
}

/// RFC 3339 with milliseconds and `Z`, e.g. `2026-11-26T10:00:00.123Z`.
fn format_instant(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// The instant `text` names, only if it is written exactly as
/// [`format_instant`] writes it.
fn parse_instant(text: &str) -> Option<DateTime<Utc>> {
    let at = DateTime::parse_from_rfc3339(text).ok()?.with_timezone(&Utc);
    (format_instant(at) == text).then_some(at)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    const ALL_ORDERS: [Order; 4] = [
        Order::Default,
        Order::DateStart,
        Order::DateEnd,
        Order::DateStartThenEnd,
    ];

    /// 2026-11-26T10:00:00.123Z.
    fn instant() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 26, 10, 0, 0).unwrap() + chrono::Duration::milliseconds(123)
    }

    fn candidate(at: DateTime<Utc>, table: Option<DateTable>) -> Candidate {
        Candidate {
            order: OrderKey { at, table },
            id: "pat-1".to_string(),
            rows: Vec::new(),
        }
    }

    /// A cursor with the given parts, as a client could send it.
    fn forged(sort_values: Vec<CursorValue>, id: &str) -> String {
        PageCursor::new(sort_values, id).encode()
    }

    fn text(value: &str) -> CursorValue {
        CursorValue::String(value.to_string())
    }

    fn assert_invalid(cursor: &str, expected: Order) {
        match decode(Some(cursor), expected) {
            Err(SearchError::InvalidCursor { cursor: refused }) => assert_eq!(refused, cursor),
            other => panic!("{expected:?}: expected InvalidCursor, got {other:?}"),
        }
    }

    #[test]
    fn no_cursor_is_the_first_page_in_the_plans_order() {
        for order in ALL_ORDERS {
            let position = decode(None, order).unwrap();
            assert_eq!(position, Position::First(order));
            assert_eq!(position.order(), order);
        }
    }

    #[test]
    fn a_default_order_cursor_round_trips() {
        let cursor = mint(&candidate(instant(), None), Order::Default).unwrap();
        assert_eq!(
            decode(Some(&cursor), Order::Default).unwrap(),
            Position::After {
                order: Order::Default,
                at: instant(),
                id: "pat-1".to_string(),
                table: None,
            }
        );
    }

    #[test]
    fn a_date_order_cursor_round_trips_with_its_table() {
        for (order, table) in [
            (Order::DateStart, DateTable::Start),
            (Order::DateEnd, DateTable::End),
            (Order::DateStartThenEnd, DateTable::Start),
            (Order::DateStartThenEnd, DateTable::End),
        ] {
            let cursor = mint(&candidate(instant(), Some(table)), order).unwrap();
            assert_eq!(
                decode(Some(&cursor), order).unwrap(),
                Position::After {
                    order,
                    at: instant(),
                    id: "pat-1".to_string(),
                    table: Some(table),
                },
                "{order:?} in {table:?}"
            );
        }
    }

    #[test]
    fn the_cursor_is_the_designs_plain_page_cursor() {
        let cursor = mint(&candidate(instant(), None), Order::Default).unwrap();
        let decoded = PageCursor::decode(&cursor).unwrap();
        assert_eq!(decoded.resource_id(), "pat-1");
        assert_eq!(decoded.direction(), CursorDirection::Next);
        assert!(matches!(
            decoded.sort_values(),
            [CursorValue::String(at), CursorValue::Null] if at == "2026-11-26T10:00:00.123Z"
        ));

        let cursor = mint(&candidate(instant(), Some(DateTable::End)), Order::DateEnd).unwrap();
        assert!(matches!(
            PageCursor::decode(&cursor).unwrap().sort_values(),
            [CursorValue::String(at), CursorValue::Null, CursorValue::String(table)]
                if at == "2026-11-26T10:00:00.123Z" && table == "end"
        ));
    }

    #[test]
    fn the_instant_is_kept_to_the_millisecond() {
        let finer = instant() + chrono::Duration::microseconds(456);
        let cursor = mint(&candidate(finer, None), Order::Default).unwrap();
        let Position::After { at, .. } = decode(Some(&cursor), Order::Default).unwrap() else {
            panic!("expected a position after the cursor");
        };
        assert_eq!(at, instant());
    }

    #[test]
    fn a_malformed_cursor_is_invalid() {
        let lu = text("2026-11-26T10:00:00.123Z");
        let cases = [
            "garbage".to_string(),
            String::new(),
            // Wrong number of sort values.
            forged(vec![], "pat-1"),
            forged(vec![lu.clone()], "pat-1"),
            forged(
                vec![lu.clone(), CursorValue::Null, text("start"), text("x")],
                "pat-1",
            ),
            // The bucket slot is reserved: always null.
            forged(vec![lu.clone(), CursorValue::Number(0)], "pat-1"),
            forged(vec![lu.clone(), text("")], "pat-1"),
            // The instant is not a string, or not written as mint writes it.
            forged(vec![CursorValue::Number(1), CursorValue::Null], "pat-1"),
            forged(vec![text("yesterday"), CursorValue::Null], "pat-1"),
            forged(
                vec![text("2026-11-26T10:00:00Z"), CursorValue::Null],
                "pat-1",
            ),
            forged(
                vec![text("2026-11-26T10:00:00.123456Z"), CursorValue::Null],
                "pat-1",
            ),
            forged(
                vec![text("2026-11-26T11:00:00.123+01:00"), CursorValue::Null],
                "pat-1",
            ),
            // No id.
            forged(vec![lu.clone(), CursorValue::Null], ""),
        ];
        for cursor in &cases {
            assert_invalid(cursor, Order::Default);
        }
        // An unknown table name, in a date order.
        assert_invalid(
            &forged(vec![lu, CursorValue::Null, text("middle")], "pat-1"),
            Order::DateStartThenEnd,
        );
    }

    #[test]
    fn a_cursor_of_another_order_or_table_is_invalid() {
        let default = mint(&candidate(instant(), None), Order::Default).unwrap();
        let start = mint(
            &candidate(instant(), Some(DateTable::Start)),
            Order::DateStart,
        )
        .unwrap();
        let end = mint(&candidate(instant(), Some(DateTable::End)), Order::DateEnd).unwrap();

        assert_invalid(&start, Order::Default);
        assert_invalid(&end, Order::Default);
        assert_invalid(&default, Order::DateStart);
        assert_invalid(&end, Order::DateStart);
        assert_invalid(&default, Order::DateEnd);
        assert_invalid(&start, Order::DateEnd);
        assert_invalid(&default, Order::DateStartThenEnd);
    }

    #[test]
    fn a_previous_cursor_is_invalid() {
        let previous = PageCursor::previous(
            vec![text("2026-11-26T10:00:00.123Z"), CursorValue::Null],
            "pat-1",
        )
        .encode();
        assert_invalid(&previous, Order::Default);
    }

    #[test]
    fn the_keyset_follows_the_positions_table() {
        assert!(keyset(&Position::First(Order::Default)).is_none());
        for (table, relation) in [
            (None, "(last_updated, id) < (?, ?)"),
            (Some(DateTable::Start), "(start, id) > (?, ?)"),
            (Some(DateTable::End), "(end, id) > (?, ?)"),
        ] {
            let position = Position::After {
                order: Order::Default,
                at: instant(),
                id: "pat-1".to_string(),
                table,
            };
            let (found, values) = keyset(&position).unwrap();
            assert_eq!(found, relation);
            assert_eq!(values.len(), 2);
        }
    }

    #[test]
    fn mint_refuses_a_candidate_from_another_order() {
        for (order, table) in [
            (Order::Default, Some(DateTable::Start)),
            (Order::Default, Some(DateTable::End)),
            (Order::DateStart, None),
            (Order::DateStart, Some(DateTable::End)),
            (Order::DateEnd, None),
            (Order::DateEnd, Some(DateTable::Start)),
            (Order::DateStartThenEnd, None),
        ] {
            let error = mint(&candidate(instant(), table), order).unwrap_err();
            assert!(
                matches!(
                    error,
                    crate::error::StorageError::Backend(BackendError::Internal { .. })
                ),
                "{order:?} from {table:?}: {error:?}"
            );
        }
    }
}

/// Keyset paging on `resources_by_type`, against the node at
/// `HFS_TEST_CASSANDRA_URL` (skipped when unset): walkthrough 5.6's query,
/// continued page after page through [`mint`], [`decode`] and [`keyset`], so
/// the cursor a client would carry is what moves the read on.
#[cfg(test)]
mod node_tests {
    use super::*;
    use crate::backends::cassandra::CassandraBackend;
    use crate::backends::cassandra::session::StatementKind;
    use crate::backends::cassandra::test_support::{test_config, test_fhir_version};
    use crate::tenant::{TenantContext, TenantId, TenantPermissions};
    use cdrs_tokio::types::IntoRustByName;
    use std::collections::BTreeSet;

    async fn backend() -> Option<CassandraBackend> {
        let url = std::env::var("HFS_TEST_CASSANDRA_URL")
            .ok()
            .filter(|url| !url.trim().is_empty())?;
        Some(
            CassandraBackend::connect(crate::backends::cassandra::CassandraConfig {
                contact_points: vec![url],
                ..test_config()
            })
            .await
            .expect("connect"),
        )
    }

    fn run_id() -> String {
        uuid::Uuid::new_v4().simple().to_string()
    }

    /// Every id of `(tenant, resource_type)` in the default order, read
    /// `page_size` rows at a time, and the number of reads it took.
    async fn page_through(
        backend: &CassandraBackend,
        tenant: &str,
        resource_type: &str,
        page_size: usize,
    ) -> (Vec<String>, usize) {
        let mut position = decode(None, Order::Default).expect("first page");
        let mut ids = Vec::new();
        let mut reads = 0;
        loop {
            let mut values = vec![Value::from(tenant), Value::from(resource_type)];
            let after = match keyset(&position) {
                None => String::new(),
                Some((relation, bound)) => {
                    values.extend(bound);
                    format!(" AND {relation}")
                }
            };
            values.push(Value::from(i32::try_from(page_size).expect("page size")));
            let cql = format!(
                "SELECT last_updated, id FROM resources_by_type \
                 WHERE tenant_id = ? AND resource_type = ?{after} LIMIT ?"
            );
            let rows = backend
                .session
                .rows(&cql, values, StatementKind::Read)
                .await
                .expect("page");
            reads += 1;
            let Some(last) = rows.last() else {
                break;
            };
            for row in &rows {
                let id: String = row.get_r_by_name("id").expect("id");
                ids.push(id);
            }
            let last = Candidate {
                order: OrderKey {
                    at: last.get_r_by_name("last_updated").expect("last_updated"),
                    table: None,
                },
                id: last.get_r_by_name("id").expect("id"),
                rows: Vec::new(),
            };
            let cursor = mint(&last, Order::Default).expect("mint");
            position = decode(Some(&cursor), Order::Default).expect("decode");
            if rows.len() < page_size {
                break;
            }
        }
        (ids, reads)
    }

    /// 60 rows on 20 instants, three ids per millisecond, read 7 at a time,
    /// so ties straddle pages: the pages give every row once, in
    /// `(last_updated DESC, id DESC)` order. The rows are planted with no
    /// `resources` row, so the tenant is `test-planted-` (#127).
    #[tokio::test]
    async fn pages_place_ties_within_a_millisecond_exactly() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = format!("test-planted-paging-{}", run_id());
        let base = Utc::now() - chrono::Duration::days(1);
        let mut expected = Vec::new();
        for instant in 0..20 {
            let at = base + chrono::Duration::milliseconds(instant);
            for n in 0..3 {
                let id = format!("r-{instant:02}-{n}");
                backend
                    .session
                    .execute(
                        "INSERT INTO resources_by_type \
                         (tenant_id, resource_type, last_updated, id, version_id, fhir_version) \
                         VALUES (?, ?, ?, ?, ?, ?)",
                        vec![
                            Value::from(tenant.as_str()),
                            Value::from("Patient"),
                            Value::from(at),
                            Value::from(id.as_str()),
                            Value::from(1_i32),
                            Value::from("R4"),
                        ],
                        StatementKind::Write,
                    )
                    .await
                    .expect("plant");
                expected.push((at.timestamp_millis(), id));
            }
        }
        expected.sort_by(|a, b| b.cmp(a));
        let expected: Vec<String> = expected.into_iter().map(|(_, id)| id).collect();

        let (ids, reads) = page_through(&backend, &tenant, "Patient", 7).await;
        assert_eq!(ids, expected, "every row once, in the default order");
        assert_eq!(
            reads, 9,
            "60 rows at 7 a page: 8 full pages and a short one"
        );
    }

    /// Resources made through the write path: paging `resources_by_type`
    /// 4 at a time yields each created id exactly once.
    #[tokio::test]
    async fn pages_through_created_resources_with_no_duplicate_or_gap() {
        let Some(backend) = backend().await else {
            eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
            return;
        };
        let tenant = TenantContext::new(
            TenantId::new(format!("test-paging-{}", run_id())),
            TenantPermissions::full_access(),
        );
        let mut created = BTreeSet::new();
        for _ in 0..25 {
            let stored = crate::core::ResourceStorage::create(
                &backend,
                &tenant,
                "Patient",
                serde_json::json!({ "resourceType": "Patient" }),
                test_fhir_version(),
            )
            .await
            .expect("create");
            created.insert(stored.id().to_string());
        }

        let (ids, _) = page_through(&backend, tenant.tenant_id().as_str(), "Patient", 4).await;
        let distinct: BTreeSet<String> = ids.iter().cloned().collect();
        assert_eq!(ids.len(), distinct.len(), "no id on two pages");
        assert_eq!(distinct, created, "no created id missed");
    }
}
