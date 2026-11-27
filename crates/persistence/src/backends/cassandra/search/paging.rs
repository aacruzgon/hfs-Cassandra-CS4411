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
//! Only this file encodes or decodes a cursor (ADR-anastasiabryz-0001), so a
//! cursor minted by one order can be refused with `InvalidCursor` (400) in
//! another. The interface PR (#128) defines the types; [`decode`] and [`mint`]
//! are filled in by #129, and until then refuse every cursor.
//!
//! [`PageCursor`]: crate::types::PageCursor

#![allow(dead_code)] // The search engine (#128) calls it; until then only its tests do. #128 removes this.

use chrono::{DateTime, Utc};

use super::Candidate;
use crate::backends::cassandra::errors::unsupported;
use crate::error::{SearchError, StorageResult};

/// The order a search's candidates stream in, which the plan decides.
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

/// Which date table a date-order position is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DateTable {
    /// `date_index`, ordered by `start`; the cursor's `"start"`.
    Start,
    /// `date_index_by_end`, ordered by `end`; the cursor's `"end"`.
    End,
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
    pub(crate) fn order(&self) -> Order {
        match self {
            Position::First(order) | Position::After { order, .. } => *order,
        }
    }
}

/// Decodes the request's cursor for a plan of order `expected`.
///
/// `None` is the first page. A cursor that is malformed, a `Previous` cursor,
/// or one minted by another order or naming a table the plan doesn't read is
/// `InvalidCursor` (400; `coverage.md` section 5). Until #129 fills this in,
/// every cursor is refused.
pub(crate) fn decode(cursor: Option<&str>, expected: Order) -> Result<Position, SearchError> {
    match cursor {
        None => Ok(Position::First(expected)),
        Some(cursor) => Err(SearchError::InvalidCursor {
            cursor: cursor.to_string(),
        }),
    }
}

/// Mints the `next` cursor from the last candidate of a page, in `order`.
///
/// Fails when the candidate's order key does not belong to `order` (a date
/// table in the default order, or none in a date order). Until #129 fills
/// this in, it always fails.
pub(crate) fn mint(_last: &Candidate, _order: Order) -> StorageResult<String> {
    Err(unsupported("cursor paging (#129)"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_cursor_is_the_first_page_in_the_plans_order() {
        for order in [
            Order::Default,
            Order::DateStart,
            Order::DateEnd,
            Order::DateStartThenEnd,
        ] {
            let position = decode(None, order).unwrap();
            assert_eq!(position, Position::First(order));
            assert_eq!(position.order(), order);
        }
    }

    #[test]
    fn every_cursor_is_refused_until_paging_lands() {
        let error = decode(Some("garbage"), Order::Default).unwrap_err();
        assert!(matches!(error, SearchError::InvalidCursor { cursor } if cursor == "garbage"));
    }
}
