//! The planner: which compiled parameter drives a search (roadmap row 6.3;
//! walkthrough 5.8, C5; G20; ADR-aacruzgon-0027).
//!
//! The driver is the first parameter in C5's fixed order, with no
//! statistics, so the most specific key drives: `_id`; `system|code` and
//! `|code` tokens; identity URIs; code-only tokens, `_profile`, `_source`
//! and every other URI; `system|` tokens; and `_lastUpdated` only when
//! nothing else can. Ties go by query order. A module's plan says its rank
//! ([`super::Drive`]); `_id` and `_lastUpdated` are the framework's own.
//!
//! Dates beside any other parameter never drive: they are residuals
//! (walkthrough 5.8). A search of dates only is date-only (G20): with
//! `_lastUpdated` among them, `_lastUpdated` is sliced and every date is a
//! residual; otherwise the first date parameter in query order drives, and
//! the other dates are residuals.
//!
//! This is the framework's own file (ADR-anastasiabryz-0001; #128).

use super::ParamPlan;

/// One parameter compiled by its module.
pub(super) struct Compiled {
    pub(super) plan: Box<dyn ParamPlan>,
    /// A date parameter (not `_lastUpdated`, which is the framework's).
    pub(super) is_date: bool,
}

/// What drives a search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Driver {
    /// `_id`: point reads on `resources` (walkthrough 5.4).
    Ids,
    /// `resources_by_type`, sliced by `_lastUpdated` if present (5.5, 5.6).
    TypeScan,
    /// The compiled plan at this index of the query's compiled parameters.
    Plan(usize),
}

/// The driver for a query whose module parameters compiled to `compiled`,
/// in query order, and which has `_id` and `_lastUpdated` parameters or not.
pub(super) fn choose(compiled: &[Compiled], has_ids: bool, has_last_updated: bool) -> Driver {
    if has_ids {
        return Driver::Ids;
    }
    if compiled.iter().all(|parameter| parameter.is_date) {
        // Date-only, or nothing but `_lastUpdated` (G20).
        return match compiled.first() {
            Some(_) if !has_last_updated => Driver::Plan(0),
            _ => Driver::TypeScan,
        };
    }
    compiled
        .iter()
        .enumerate()
        .filter(|(_, parameter)| !parameter.is_date)
        .min_by_key(|(index, parameter)| (parameter.plan.drive(), *index))
        .map(|(index, _)| Driver::Plan(index))
        .expect("a parameter that is not a date")
}

/// The plans a candidate is checked against besides its driver
/// (walkthrough 5.8): every other token or URI parameter by a clustering
/// point read, and every date parameter as a residual on the loaded
/// resource.
#[derive(Default)]
pub(super) struct Checks<'p> {
    /// Token and URI plans: `contains`.
    pub(super) contains: Vec<&'p dyn ParamPlan>,
    /// Date plans: `residual`.
    pub(super) residuals: Vec<&'p dyn ParamPlan>,
}

impl<'p> Checks<'p> {
    /// No checks: a search whose parameters are all the framework's.
    pub(super) fn none() -> Self {
        Self::default()
    }

    /// Every compiled parameter but the one `driver` names.
    pub(super) fn besides(compiled: &'p [Compiled], driver: Driver) -> Self {
        let mut checks = Self::default();
        for (index, parameter) in compiled.iter().enumerate() {
            if driver == Driver::Plan(index) {
                continue;
            }
            if parameter.is_date {
                checks.residuals.push(parameter.plan.as_ref());
            } else {
                checks.contains.push(parameter.plan.as_ref());
            }
        }
        checks
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::cassandra::search::paging::Position;
    use crate::backends::cassandra::search::{Candidate, Drive, ReadCtx};
    use crate::error::StorageResult;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use futures::stream::{self, BoxStream, StreamExt};

    /// A plan that only has a rank.
    struct Ranked(Drive);

    #[async_trait]
    impl ParamPlan for Ranked {
        fn param(&self) -> &str {
            "ranked"
        }
        fn drive(&self) -> Drive {
            self.0
        }
        fn stream<'a>(
            &'a self,
            _ctx: &'a ReadCtx<'a>,
            _from: &'a Position,
        ) -> BoxStream<'a, StorageResult<Candidate>> {
            stream::empty().boxed()
        }
        async fn contains(
            &self,
            _ctx: &ReadCtx<'_>,
            _last_updated: DateTime<Utc>,
            _id: &str,
        ) -> StorageResult<bool> {
            Ok(false)
        }
    }

    fn token(drive: Drive) -> Compiled {
        Compiled {
            plan: Box::new(Ranked(drive)),
            is_date: false,
        }
    }

    fn date() -> Compiled {
        Compiled {
            plan: Box::new(Ranked(Drive::DateOnly)),
            is_date: true,
        }
    }

    #[test]
    fn the_most_specific_parameter_drives_ties_by_query_order() {
        // code-only (Shared), then system|code: system|code drives.
        assert_eq!(
            choose(
                &[token(Drive::Shared), token(Drive::SystemCode)],
                false,
                false
            ),
            Driver::Plan(1)
        );
        // Two of the same rank: the first in the query.
        assert_eq!(
            choose(
                &[
                    token(Drive::SystemOnly),
                    token(Drive::Shared),
                    token(Drive::Shared)
                ],
                false,
                false
            ),
            Driver::Plan(1)
        );
        assert_eq!(
            choose(
                &[token(Drive::Shared), token(Drive::IdentityUri)],
                false,
                false
            ),
            Driver::Plan(1),
            "an identity URI before a code-only token"
        );
    }

    #[test]
    fn id_drives_first_and_last_updated_only_when_nothing_else_can() {
        assert_eq!(
            choose(&[token(Drive::SystemCode)], true, false),
            Driver::Ids
        );
        assert_eq!(choose(&[], true, true), Driver::Ids);
        assert_eq!(
            choose(&[token(Drive::SystemOnly)], false, true),
            Driver::Plan(0)
        );
        assert_eq!(choose(&[], false, true), Driver::TypeScan);
        assert_eq!(choose(&[], false, false), Driver::TypeScan);
    }

    #[test]
    fn a_date_never_drives_beside_another_parameter() {
        assert_eq!(
            choose(&[date(), token(Drive::SystemOnly)], false, false),
            Driver::Plan(1)
        );
        assert_eq!(
            choose(&[date(), date(), token(Drive::Shared)], false, true),
            Driver::Plan(2)
        );
    }

    #[test]
    fn a_date_only_search_slices_last_updated_or_its_first_date() {
        // G20: the first date parameter in query order drives.
        assert_eq!(choose(&[date(), date()], false, false), Driver::Plan(0));
        // With `_lastUpdated` among them, `_lastUpdated` is sliced.
        assert_eq!(choose(&[date(), date()], false, true), Driver::TypeScan);
    }

    #[test]
    fn every_other_parameter_is_a_check_dates_as_residuals() {
        let compiled = [
            date(),
            token(Drive::Shared),
            token(Drive::SystemCode),
            date(),
        ];
        let driver = choose(&compiled, false, false);
        assert_eq!(driver, Driver::Plan(2));
        let checks = Checks::besides(&compiled, driver);
        assert_eq!(checks.contains.len(), 1, "the other token");
        assert_eq!(checks.residuals.len(), 2, "both dates");

        let by_id = Checks::besides(&compiled, Driver::Ids);
        assert_eq!(
            (by_id.contains.len(), by_id.residuals.len()),
            (2, 2),
            "_id drives: all checked"
        );
        assert!(Checks::none().contains.is_empty() && Checks::none().residuals.is_empty());
    }
}
