//! Token search (roadmap 6a, #130, Sara).
//!
//! A stub of the interface PR (#128, ADR-anastasiabryz-0001): `compile`
//! refuses every parameter until its issue fills it in. The module turns one
//! [`SearchParameter`], with all its comma values, into a [`ParamPlan`].

#![allow(dead_code)] // The search engine (#128) calls it. #128 removes this.

use super::{CompileCtx, ParamPlan};
use crate::backends::cassandra::errors::unsupported;
use crate::error::StorageResult;
use crate::types::SearchParameter;

/// Compiles one parameter into its plan.
pub(crate) fn compile(
    _param: &SearchParameter,
    _ctx: &CompileCtx<'_>,
) -> StorageResult<Box<dyn ParamPlan>> {
    Err(unsupported("token search (#130)"))
}
