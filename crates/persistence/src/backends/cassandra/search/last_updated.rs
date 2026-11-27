//! `_lastUpdated` on `resources_by_type` (roadmap row 6.2; walkthrough 5.5;
//! `coverage.md` section 3, "`_lastUpdated`").
//!
//! A stored `last_updated` `t` is a millisecond point, the range
//! `[t, t + 1 ms)`. Each query value `[s, e)` gives one or two slices of
//! `t` on the partition's `last_updated DESC` clustering:
//!
//! | Prefix | Slice |
//! | --- | --- |
//! | `eq` | `s ≤ t < e` |
//! | `ne` | `t ≥ e`, then `t < s` |
//! | `gt`, `sa` | `t ≥ e` |
//! | `lt`, `eb` | `t < s` |
//! | `ge` | `t ≥ s` |
//! | `le` | `t < e` |
//! | `ap` | `low ≤ t < high`, the `ap` window |
//!
//! A comma list's slices are joined into disjoint slices, read newest
//! first, so the page stays in the native `(last_updated DESC, id DESC)`
//! order with no merge: `ne` reads `t ≥ e` to its end and then `t < s`, a
//! concatenation in `DESC` order. With several `_lastUpdated` parameters the
//! first is sliced and every one, the first included, is checked on each
//! row with the shared `range_predicate`, so the slices only narrow the read
//! and never decide a match.
//!
//! This is the framework's own file (ADR-anastasiabryz-0001; #128).

use chrono::{DateTime, Duration, Utc};

use crate::error::{SearchError, StorageResult};
use crate::search::{FhirDateValue, RangePredicate, StorageResolution};
use crate::types::{SearchParameter, SearchPrefix, SearchQuery};

/// The parameter's name.
pub(crate) const PARAM: &str = "_lastUpdated";

/// `from ≤ t < until`, either side open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Slice {
    pub(crate) from: Option<DateTime<Utc>>,
    pub(crate) until: Option<DateTime<Utc>>,
}

impl Slice {
    /// The whole partition: no `_lastUpdated`.
    pub(crate) const ALL: Slice = Slice {
        from: None,
        until: None,
    };

    fn at_or_after(from: DateTime<Utc>) -> Self {
        Slice {
            from: Some(from),
            until: None,
        }
    }

    fn before(until: DateTime<Utc>) -> Self {
        Slice {
            from: None,
            until: Some(until),
        }
    }

    fn within(from: DateTime<Utc>, until: DateTime<Utc>) -> Self {
        Slice {
            from: Some(from),
            until: Some(until),
        }
    }
}

/// The query's `_lastUpdated` parameters: the slices to read and the check
/// every row must pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LastUpdatedFilter {
    /// One entry per `_lastUpdated` parameter (ANDed), each its values'
    /// predicates (ORed).
    params: Vec<Vec<RangePredicate>>,
    /// Disjoint, newest first.
    slices: Vec<Slice>,
}

impl LastUpdatedFilter {
    /// From `query`'s `_lastUpdated` parameters; with none, every row
    /// matches and the one slice is the whole partition.
    pub(crate) fn of(query: &SearchQuery) -> StorageResult<Self> {
        let mut params = Vec::new();
        let mut slices = None;
        for param in query.parameters.iter().filter(|param| param.name == PARAM) {
            let values = parsed(param)?;
            params.push(
                values
                    .iter()
                    .map(|(prefix, value)| {
                        value.range_predicate(*prefix, StorageResolution::Millis)
                    })
                    .collect(),
            );
            if slices.is_none() {
                slices = Some(newest_first(
                    values
                        .iter()
                        .flat_map(|(prefix, value)| value_slices(*prefix, value))
                        .collect(),
                ));
            }
        }
        Ok(Self {
            params,
            slices: slices.unwrap_or_else(|| vec![Slice::ALL]),
        })
    }

    /// Whether a stored `last_updated` passes every `_lastUpdated`
    /// parameter.
    pub(crate) fn matches(&self, t: DateTime<Utc>) -> bool {
        let point_end = t + Duration::milliseconds(1);
        self.params
            .iter()
            .all(|values| values.iter().any(|value| value.matches(t, point_end)))
    }

    /// The slices to read, disjoint and newest first.
    pub(crate) fn slices(&self) -> &[Slice] {
        &self.slices
    }
}

/// Each value of `param` with its prefix. `validate_date_values` has
/// already refused a value that does not parse, so a failure here is
/// reported the same way.
fn parsed(param: &SearchParameter) -> StorageResult<Vec<(SearchPrefix, FhirDateValue)>> {
    param
        .values
        .iter()
        .map(|value| {
            FhirDateValue::parse(&value.value)
                .map(|parsed| (value.prefix, parsed))
                .map_err(|error| {
                    SearchError::InvalidDateValue {
                        param: param.name.clone(),
                        value: value.value.clone(),
                        reason: error.to_string(),
                    }
                    .into()
                })
        })
        .collect()
}

/// The slices of `t` one value matches (the table in the module docs).
fn value_slices(prefix: SearchPrefix, value: &FhirDateValue) -> Vec<Slice> {
    let (s, e) = value.range_at(StorageResolution::Millis);
    match prefix {
        SearchPrefix::Eq => vec![Slice::within(s, e)],
        SearchPrefix::Ne => vec![Slice::at_or_after(e), Slice::before(s)],
        SearchPrefix::Gt | SearchPrefix::Sa => vec![Slice::at_or_after(e)],
        SearchPrefix::Lt | SearchPrefix::Eb => vec![Slice::before(s)],
        SearchPrefix::Ge => vec![Slice::at_or_after(s)],
        SearchPrefix::Le => vec![Slice::before(e)],
        SearchPrefix::Ap => {
            let (low, high) = value.approx_window(StorageResolution::Millis);
            vec![Slice::within(low, high)]
        }
    }
}

/// `slices` joined where they overlap or touch, newest first.
fn newest_first(slices: Vec<Slice>) -> Vec<Slice> {
    let low = |slice: &Slice| slice.from.unwrap_or(DateTime::<Utc>::MIN_UTC);
    let high = |slice: &Slice| slice.until.unwrap_or(DateTime::<Utc>::MAX_UTC);
    let mut sorted: Vec<Slice> = slices
        .into_iter()
        .filter(|slice| low(slice) < high(slice))
        .collect();
    sorted.sort_by_key(low);
    let mut joined: Vec<Slice> = Vec::new();
    for slice in sorted {
        match joined.last_mut() {
            Some(last) if low(&slice) <= high(last) => {
                if high(&slice) > high(last) {
                    last.until = slice.until;
                }
            }
            _ => joined.push(slice),
        }
    }
    joined.reverse();
    joined
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SearchValue;
    use chrono::TimeZone;

    fn at(hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 27, hour, 0, 0).unwrap()
    }

    fn day(day: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, day, 0, 0, 0).unwrap()
    }

    fn query(values: &[&[&str]]) -> SearchQuery {
        values
            .iter()
            .fold(SearchQuery::new("Patient"), |query, values| {
                query.with_parameter(SearchParameter {
                    name: PARAM.to_string(),
                    param_type: crate::types::SearchParamType::Date,
                    values: values
                        .iter()
                        .map(|value| SearchValue::parse(value))
                        .collect(),
                    ..Default::default()
                })
            })
    }

    fn filter(values: &[&[&str]]) -> LastUpdatedFilter {
        LastUpdatedFilter::of(&query(values)).unwrap()
    }

    fn in_slices(slices: &[Slice], t: DateTime<Utc>) -> bool {
        slices.iter().any(|slice| {
            slice.from.is_none_or(|from| from <= t) && slice.until.is_none_or(|until| t < until)
        })
    }

    /// For every prefix, a row is in the slices exactly when the shared
    /// `range_predicate` matches it, checked millisecond by millisecond
    /// around the value's range: the slices never drop a match, and here
    /// they read nothing extra either.
    #[test]
    fn each_prefix_slices_exactly_what_range_predicate_matches() {
        let value = "2026-11-27T10:00:00.500Z";
        let center =
            Utc.with_ymd_and_hms(2026, 11, 27, 10, 0, 0).unwrap() + Duration::milliseconds(500);
        for prefix in ["eq", "ne", "gt", "lt", "ge", "le", "sa", "eb"] {
            let filter = filter(&[&[&format!("{prefix}{value}")]]);
            for offset in -3..=3 {
                let t = center + Duration::milliseconds(offset);
                assert_eq!(
                    in_slices(filter.slices(), t),
                    filter.matches(t),
                    "{prefix} at {offset} ms"
                );
            }
        }
        // `ap` at second precision: a 10 s window each side.
        let ap = filter(&[&["ap2026-11-27T10:00:00Z"]]);
        for seconds in [-11, -10, -9, 0, 9, 10, 11] {
            let t = at(10) + Duration::seconds(seconds);
            assert_eq!(
                in_slices(ap.slices(), t),
                ap.matches(t),
                "ap at {seconds} s"
            );
        }
    }

    #[test]
    fn slices_are_disjoint_and_newest_first() {
        // ne: the part above, then the part below.
        let ne = filter(&[&["ne2026-11-27"]]);
        assert_eq!(
            ne.slices(),
            [Slice::at_or_after(day(28)), Slice::before(day(27))]
        );

        // A comma list: two separate days, newest first; overlapping or
        // touching values are joined.
        let two = filter(&[&["eq2026-11-25", "eq2026-11-28"]]);
        assert_eq!(
            two.slices(),
            [
                Slice::within(day(28), day(29)),
                Slice::within(day(25), day(26)),
            ]
        );
        let touching = filter(&[&["eq2026-11-25", "eq2026-11-26"]]);
        assert_eq!(touching.slices(), [Slice::within(day(25), day(27))]);
        let covering = filter(&[&["ge2026-11-25", "lt2026-11-28"]]);
        assert_eq!(covering.slices(), [Slice::ALL]);

        assert_eq!(filter(&[]).slices(), [Slice::ALL]);
    }

    #[test]
    fn several_parameters_are_anded_and_the_first_is_sliced() {
        let both = filter(&[&["ge2026-11-25"], &["lt2026-11-28"]]);
        assert_eq!(both.slices(), [Slice::at_or_after(day(25))]);
        assert!(!both.matches(day(24)));
        assert!(both.matches(day(25)));
        assert!(both.matches(day(27)));
        assert!(!both.matches(day(28)), "the second parameter is a residual");
        assert!(filter(&[]).matches(day(3)), "no _lastUpdated: every row");
    }
}
