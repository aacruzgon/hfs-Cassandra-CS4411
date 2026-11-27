use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::ops::RangeBounds;

use cdrs_tokio::types::{rows::Row, value::Value};
use chrono::{DateTime, Utc};

use super::super::session::{CassandraSession, DriverError, StatementKind};

use crate::search::date_value::{FhirDateValue, RangePredicate, StorageResolution};
use crate::types::SearchPrefix;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum DateIndex {
    Start,
    End,
}

type DateBound = Bound<DateTime<Utc>>;

#[derive(Debug, PartialEq, Eq)]
struct DateSlice {
    index: DateIndex,
    lower: DateBound,
    upper: DateBound,
}

impl DateSlice {
    fn new(index: DateIndex, lower: DateBound, upper: DateBound) -> Self {
        Self {
            index,
            lower,
            upper,
        }
    }
}

/// Internal position of the last raw row. Public cursors are handled by
/// Step 6d and identify the last emitted resource.
type DateRowPosition<'a> = (DateTime<Utc>, &'a str, DateTime<Utc>);

impl DateSlice {
    fn statement(
        &self,
        partition: [&str; 3],
        after: Option<DateRowPosition<'_>>,
        limit: i32,
    ) -> (String, Vec<Value>) {
        let (table, column, other_column) = match self.index {
            DateIndex::Start => ("date_index", "start", "end"),
            DateIndex::End => ("date_index_by_end", "end", "start"),
        };
        let mut cql = format!(
            "SELECT start, id, end, version_id FROM {table} \
             WHERE tenant_id = ? AND resource_type = ? AND param = ?"
        );
        let mut values: Vec<Value> = partition.into_iter().map(Value::from).collect();

        // Cassandra accepts one lower clustering bound. Use the cursor only
        // once it reaches that bound.
        let cursor = after.filter(|(time, _, _)| match self.lower {
            Included(bound) => *time >= bound,
            Excluded(bound) => *time > bound,
            Unbounded => true,
        });
        if let Some((time, id, other_time)) = cursor {
            cql.push_str(&format!(" AND ({column}, id, {other_column}) > (?, ?, ?)"));
            values.extend([Value::from(time), Value::from(id), Value::from(other_time)]);
        } else {
            match self.lower {
                Included(time) => {
                    cql.push_str(&format!(" AND {column} >= ?"));
                    values.push(Value::from(time));
                }
                Excluded(time) => {
                    cql.push_str(&format!(" AND {column} > ?"));
                    values.push(Value::from(time));
                }
                Unbounded => {}
            }
        }

        match self.upper {
            Included(time) => {
                cql.push_str(&format!(" AND {column} <= ?"));
                values.push(Value::from(time));
            }
            Excluded(time) => {
                cql.push_str(&format!(" AND {column} < ?"));
                values.push(Value::from(time));
            }
            Unbounded => {}
        }
        cql.push_str(" LIMIT ?");
        values.push(Value::from(limit));
        (cql, values)
    }

    async fn read(
        &self,
        session: &CassandraSession,
        partition: [&str; 3],
        after: Option<DateRowPosition<'_>>,
        limit: i32,
    ) -> Result<Vec<Row>, DriverError> {
        let (cql, values) = self.statement(partition, after, limit);
        session.rows(&cql, values, StatementKind::Read).await
    }
}

/// Choose a superset of matching rows. The reader must still apply the shared
/// range predicate to every row. Slice order also sets the table order for ne.
fn date_slices(value: &FhirDateValue, prefix: SearchPrefix) -> Vec<DateSlice> {
    use DateIndex::{End, Start};
    use SearchPrefix::*;

    let (start, end) = value.range_at(StorageResolution::Millis);
    match prefix {
        Eq => vec![DateSlice::new(Start, Included(start), Excluded(end))],
        Ne => vec![
            DateSlice::new(Start, Unbounded, Excluded(start)),
            DateSlice::new(End, Excluded(end), Unbounded),
        ],
        Gt => vec![DateSlice::new(End, Excluded(end), Unbounded)],
        Lt => vec![DateSlice::new(Start, Unbounded, Excluded(start))],
        Ge => vec![DateSlice::new(End, Excluded(start), Unbounded)],
        Le => vec![DateSlice::new(Start, Unbounded, Excluded(end))],
        Sa => vec![DateSlice::new(Start, Included(end), Unbounded)],
        Eb => vec![DateSlice::new(End, Unbounded, Included(start))],
        Ap => {
            let (_, high) = value.approx_window(StorageResolution::Millis);
            vec![DateSlice::new(Start, Unbounded, Excluded(high))]
        }
    }
}

type IndexedRange = (DateTime<Utc>, DateTime<Utc>);

/// The public position identifies an emitted resource in the order of its
/// table. It is separate from the internal position of a physical row.
type DateResourcePosition<'a> = (DateIndex, DateTime<Utc>, &'a str);

struct DatePlan {
    slices: Vec<(DateSlice, RangePredicate)>,
}

impl DatePlan {
    fn new(values: &[(SearchPrefix, FhirDateValue)]) -> Self {
        let mut slices = Vec::new();
        for (prefix, value) in values {
            let predicate = value.range_predicate(*prefix, StorageResolution::Millis);
            slices.extend(
                date_slices(value, *prefix)
                    .into_iter()
                    .map(|slice| (slice, predicate.clone())),
            );
        }
        slices.sort_by_key(|(slice, _)| slice.index);
        Self { slices }
    }

    /// Recompute the first matching row from the candidate's current values.
    /// Earlier matches can be recognized even after a page or table change.
    fn first_match(
        &self,
        ranges: &[IndexedRange],
    ) -> Option<(DateIndex, DateTime<Utc>, DateTime<Utc>)> {
        self.slices
            .iter()
            .flat_map(|(slice, predicate)| {
                ranges.iter().filter_map(move |&(start, end)| {
                    let (time, other_time) = match slice.index {
                        DateIndex::Start => (start, end),
                        DateIndex::End => (end, start),
                    };
                    ((slice.lower, slice.upper).contains(&time) && predicate.matches(start, end))
                        .then_some((slice.index, time, other_time))
                })
            })
            .min()
    }

    fn emits_row(
        &self,
        index: DateIndex,
        row: DateRowPosition<'_>,
        ranges: &[IndexedRange],
        after: Option<DateResourcePosition<'_>>,
    ) -> bool {
        let (time, id, other_time) = row;
        after.is_none_or(|cursor| (index, time, id) > cursor)
            && self.first_match(ranges) == Some((index, time, other_time))
    }
}

#[test]
fn date_prefixes_select_the_documented_slices() {
    use DateIndex::{End, Start};
    use SearchPrefix::*;

    let value = FhirDateValue::parse("2020-03").unwrap();
    let (start, end) = value.range_at(StorageResolution::Millis);
    let (_, high) = value.approx_window(StorageResolution::Millis);
    let cases = [
        (
            Eq,
            vec![DateSlice::new(Start, Included(start), Excluded(end))],
        ),
        (
            Ne,
            vec![
                DateSlice::new(Start, Unbounded, Excluded(start)),
                DateSlice::new(End, Excluded(end), Unbounded),
            ],
        ),
        (Gt, vec![DateSlice::new(End, Excluded(end), Unbounded)]),
        (Lt, vec![DateSlice::new(Start, Unbounded, Excluded(start))]),
        (Ge, vec![DateSlice::new(End, Excluded(start), Unbounded)]),
        (Le, vec![DateSlice::new(Start, Unbounded, Excluded(end))]),
        (Sa, vec![DateSlice::new(Start, Included(end), Unbounded)]),
        (Eb, vec![DateSlice::new(End, Unbounded, Included(start))]),
        (Ap, vec![DateSlice::new(Start, Unbounded, Excluded(high))]),
    ];

    for (prefix, expected) in cases {
        assert_eq!(date_slices(&value, prefix), expected, "{prefix:?}");
    }
}

#[test]
fn date_slices_cover_every_matching_range_at_precision_boundaries() {
    use chrono::TimeDelta;

    use crate::search::date_value::{open_end, open_start};

    let resolution = StorageResolution::Millis;
    let millisecond = TimeDelta::milliseconds(1);
    let prefixes = [
        SearchPrefix::Eq,
        SearchPrefix::Ne,
        SearchPrefix::Gt,
        SearchPrefix::Lt,
        SearchPrefix::Ge,
        SearchPrefix::Le,
        SearchPrefix::Sa,
        SearchPrefix::Eb,
        SearchPrefix::Ap,
    ];
    let mut queries = [
        "2020",
        "2020-03",
        "2020-03-15",
        "2020-03-15T10:20Z",
        "2020-03-15T10:20:30Z",
    ]
    .map(str::to_owned)
    .to_vec();
    queries
        .extend((1..=9).map(|digits| format!("2020-03-15T10:20:30.{}Z", &"123456789"[..digits])));

    for raw in queries {
        let value = FhirDateValue::parse(&raw).unwrap();
        let (start, end) = value.range_at(resolution);
        let (low, high) = value.approx_window(resolution);
        let mut edges = vec![open_start(), open_end(resolution)];
        for point in [low, start, end, high] {
            edges.extend([point - millisecond, point, point + millisecond]);
        }
        edges.sort_unstable();
        edges.dedup();

        for prefix in prefixes {
            let predicate = value.range_predicate(prefix, resolution);
            let slices = date_slices(&value, prefix);
            for (i, &stored_start) in edges.iter().enumerate() {
                for &stored_end in &edges[i + 1..] {
                    if predicate.matches(stored_start, stored_end) {
                        assert!(
                            slices.iter().any(|slice| {
                                let key = match slice.index {
                                    DateIndex::Start => stored_start,
                                    DateIndex::End => stored_end,
                                };
                                (slice.lower, slice.upper).contains(&key)
                            }),
                            "{prefix:?}{raw} omitted [{stored_start}, {stored_end})"
                        );
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn date_slices_read_written_rows_and_resume_on_both_tables() {
    use cdrs_tokio::types::IntoRustByName;
    use serde_json::json;
    use uuid::Uuid;

    use crate::backends::cassandra::{CassandraBackend, CassandraConfig, test_support};
    use crate::core::ResourceStorage;
    use crate::tenant::{TenantContext, TenantId, TenantPermissions};

    let Ok(url) = std::env::var("HFS_TEST_CASSANDRA_URL") else {
        eprintln!("skipping: HFS_TEST_CASSANDRA_URL is not set");
        return;
    };
    let backend = CassandraBackend::connect(CassandraConfig {
        contact_points: vec![url],
        ..test_support::test_config()
    })
    .await
    .expect("connect to the Cassandra test node");
    let tenant = TenantContext::new(
        TenantId::new(format!("test-date-slice-{}", Uuid::new_v4())),
        TenantPermissions::full_access(),
    );
    let mut march_ids = Vec::new();
    for birthdate in ["2020-03-10", "2020-03-10", "2020-04-01"] {
        let resource = backend
            .create(
                &tenant,
                "Patient",
                json!({"resourceType": "Patient", "birthDate": birthdate}),
                test_support::test_fhir_version(),
            )
            .await
            .expect("create patient");
        if birthdate == "2020-03-10" {
            march_ids.push(resource.id().to_string());
        }
    }
    march_ids.sort_unstable();
    let value = FhirDateValue::parse("2020-03").unwrap();
    let partition = [tenant.tenant_id().as_str(), "Patient", "birthdate"];
    for (prefix, field, other_field) in [
        (SearchPrefix::Eq, "start", "end"),
        (SearchPrefix::Eb, "end", "start"),
    ] {
        // The end-table upper bound is midnight following March 31.
        let end_value = FhirDateValue::parse("2020-04-01").unwrap();
        let query = if prefix == SearchPrefix::Eq {
            &value
        } else {
            &end_value
        };
        let slice = &date_slices(query, prefix)[0];
        let first = slice
            .read(&backend.session, partition, None, 1)
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        let id: String = first[0].get_r_by_name("id").unwrap();
        let time: DateTime<Utc> = first[0].get_r_by_name(field).unwrap();
        let other_time: DateTime<Utc> = first[0].get_r_by_name(other_field).unwrap();
        assert_eq!(id, march_ids[0]);
        let rest = slice
            .read(
                &backend.session,
                partition,
                Some((time, &id, other_time)),
                10,
            )
            .await
            .unwrap();
        let ids: Vec<String> = rest
            .iter()
            .map(|row| row.get_r_by_name("id").unwrap())
            .collect();
        assert_eq!(ids, march_ids[1..]);
    }

    // Two physical rows for one resource share (end, id). LIMIT 1 returns
    // the February start first, which fails ge2020-03; the March start matches.
    let planted = format!("test-planted-date-cursor-{}", Uuid::new_v4());
    let partition = [planted.as_str(), "Encounter", "date"];
    let timestamp = |raw| {
        FhirDateValue::parse(raw)
            .unwrap()
            .range_at(StorageResolution::Millis)
            .0
    };
    let february = timestamp("2020-02-01");
    let march = timestamp("2020-03-10");
    let end = timestamp("2020-03-20");
    for start in [february, march] {
        backend.session.execute(
            "INSERT INTO date_index_by_end (tenant_id, resource_type, param, end, id, start, version_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            vec![planted.as_str().into(), "Encounter".into(), "date".into(), end.into(), "same-id".into(), start.into(), 1_i32.into()],
            StatementKind::Write,
        ).await.unwrap();
    }

    let slice = &date_slices(&value, SearchPrefix::Ge)[0];
    let predicate = value.range_predicate(SearchPrefix::Ge, StorageResolution::Millis);
    let first = slice
        .read(&backend.session, partition, None, 1)
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    let first_start: DateTime<Utc> = first[0].get_r_by_name("start").unwrap();
    assert_eq!(first_start, february);
    assert!(!predicate.matches(first_start, end));
    let second = slice
        .read(
            &backend.session,
            partition,
            Some((end, "same-id", first_start)),
            1,
        )
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    let second_start: DateTime<Utc> = second[0].get_r_by_name("start").unwrap();
    assert_eq!(second_start, march);
    assert!(predicate.matches(second_start, end));

    #[cfg(feature = "R4")]
    if test_support::test_fhir_version() == helios_fhir::FhirVersion::R4 {
        let mut encounter_ids = Vec::new();
        for period in [
            json!({"start": "2020-03-10", "end": "2020-03-20"}),
            json!({"start": "2020-03-10"}),
            json!({"start": "2020-04-01", "end": "2020-04-02"}),
        ] {
            let resource = backend
                .create(
                    &tenant,
                    "Encounter",
                    json!({
                        "resourceType": "Encounter",
                        "status": "finished",
                        "class": {"code": "AMB"},
                        "period": period
                    }),
                    test_support::test_fhir_version(),
                )
                .await
                .expect("create encounter");
            encounter_ids.push(resource.id().to_string());
        }

        use SearchPrefix::*;
        let partition = [tenant.tenant_id().as_str(), "Encounter", "date"];
        let cases: [(SearchPrefix, &[usize]); 9] = [
            (Eq, &[0]),
            (Ne, &[1, 2]),
            (Gt, &[1, 2]),
            (Lt, &[]),
            (Ge, &[0, 1, 2]),
            (Le, &[0]),
            (Sa, &[2]),
            (Eb, &[]),
            (Ap, &[0, 1, 2]),
        ];
        for (prefix, expected) in cases {
            let predicate = value.range_predicate(prefix, StorageResolution::Millis);
            let mut found = std::collections::BTreeSet::new();
            for slice in date_slices(&value, prefix) {
                for row in slice
                    .read(&backend.session, partition, None, 20)
                    .await
                    .unwrap()
                {
                    let start: DateTime<Utc> = row.get_r_by_name("start").unwrap();
                    let end: DateTime<Utc> = row.get_r_by_name("end").unwrap();
                    if predicate.matches(start, end) {
                        let id: String = row.get_r_by_name("id").unwrap();
                        found.insert(id);
                    }
                }
            }
            assert_eq!(
                found,
                expected.iter().map(|&i| encounter_ids[i].clone()).collect(),
                "{prefix:?}2020-03"
            );
        }
    }
}

#[test]
fn date_pages_cross_tables_without_repeating_multivalued_resources() {
    use DateIndex::{End, Start};
    use SearchPrefix::*;

    let value = FhirDateValue::parse("2020-03").unwrap();
    let at = |raw| {
        FhirDateValue::parse(raw)
            .unwrap()
            .range_at(StorageResolution::Millis)
            .0
    };
    let ranges: [(&str, &[IndexedRange]); 4] = [
        (
            "both",
            &[
                (at("2020-01-01"), at("2020-01-02")),
                (at("2020-02-01"), at("2020-02-02")),
                (at("2020-04-01"), at("2020-04-02")),
            ],
        ),
        ("start", &[(at("2020-02-01"), at("2020-02-02"))]),
        ("end", &[(at("2020-05-01"), at("2020-05-02"))]),
        ("inside", &[(at("2020-03-10"), at("2020-03-11"))]),
    ];

    // The fixture models the rows from both Cassandra tables in native
    // clustering order. Each page resumes from the last emitted resource.
    let mut rows = Vec::new();
    for (id, values) in ranges {
        for &(start, end) in values {
            rows.extend([
                (Start, start, id, end, values),
                (End, end, id, start, values),
            ]);
        }
    }
    rows.sort_by_key(|&(table, time, id, other_time, _)| (table, time, id, other_time));

    for query in [
        vec![(Ne, value.clone())],
        vec![
            (Gt, value.clone()),
            (Lt, value.clone()),
            (Eb, value.clone()),
        ],
    ] {
        let plan = DatePlan::new(&query);
        assert_eq!(plan.slices.first().unwrap().0.index, Start);
        assert_eq!(plan.slices.last().unwrap().0.index, End);
        let mut cursor = None;
        let mut emitted = Vec::new();
        // Request one resource at a time, forcing a page boundary at the
        // table transition and before the second physical row of "both".
        for _ in 0..4 {
            let next = rows.iter().find(|&&(table, time, id, other_time, values)| {
                plan.emits_row(table, (time, id, other_time), values, cursor)
            });
            let Some(&(table, time, id, _, _)) = next else {
                break;
            };
            emitted.push((table, id));
            cursor = Some((table, time, id));
        }
        assert_eq!(emitted, [(Start, "both"), (Start, "start"), (End, "end")]);
    }
}
