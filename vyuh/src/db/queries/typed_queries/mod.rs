//! Experimental source-first SQL AST for typed query construction.
//!
//! This module is intentionally isolated from Vyuh's stable query builders.
//! It keeps raw SQL and string paths out of the typed query surface; advanced
//! SQL remains the job of `db::query(...)`.
//!
//! The public surface is intentionally small and re-exported from this module.
//! Implementation details live in private modules for planning, handles,
//! sources, expressions, dialect rendering, binding, and validation.

mod api;
mod executables;
mod expr;
mod extension;
mod functions;
mod handles;
mod plan;
mod scope;
mod source;
mod traits;

mod binds;
mod dialect;
mod render;
mod terminals;
mod validate;

pub use api::{from, meta, val, var};
pub use expr::{Expr, IntoExpr};
pub use extension::{
    DbExpression, DbFunction, ExprRenderCtx, FunctionArgs, IntoFunctionArgs, custom, func,
};
pub use functions::common::{count, now};
pub use functions::postgres;
pub use plan::{ParamSource, ParamSpec, QueryPlan};
pub use source::{SourceKind, SourceMeta};

#[doc(hidden)]
pub use api::__private;
#[doc(hidden)]
pub use executables::{
    All, BatchInsert, BatchUpsert, Count, Delete, Exists, First, Insert, One, OwnedBatchInsert,
    OwnedBatchUpsert, OwnedInsert, OwnedUpdate, ReturningBatchInsert, ReturningBatchUpsert,
    ReturningDelete, ReturningInsert, ReturningUpdate, Scalar, Slice, Update,
};
#[doc(hidden)]
pub use expr::{OrderExpr, Predicate};
#[doc(hidden)]
pub use handles::{Column, ModelTable, Var};
#[doc(hidden)]
pub use scope::{QueryScope, ReturningScope};
#[doc(hidden)]
pub use source::{Cte, Picked, ProjectedColumn, ProjectionSource, Subquery};
#[doc(hidden)]
pub use traits::Projectable;

pub use crate::db::placeholders::Dialect;
