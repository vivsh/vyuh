pub(crate) mod raw;
pub mod typed_queries;

pub use raw::RawQuery;
pub use typed_queries as typed;
pub use typed_queries::{
    DbExpression, DbFunction, Expr, ExprRenderCtx, FunctionArgs, ParamSource, ParamSpec, QueryPlan,
    SourceKind, SourceMeta, count, custom, from, func, meta, now, val, var,
};

use crate::db::argvalue::ArgValue;
use crate::db::commons::{Arguments, Database};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct Statement {
    pub sql: String,
    pub args: Arguments<'static>,
    pub(crate) error: Option<Arc<sqlx::error::BoxDynError>>,
}

impl Statement {
    pub fn new(sql: &str, args: Arguments<'static>) -> Self {
        Self {
            sql: sql.to_string(),
            args,
            error: None,
        }
    }

    pub fn bind<T>(mut self, val: T) -> Self
    where
        T: for<'q> sqlx::Encode<'q, Database> + sqlx::Type<Database> + Send + 'static,
    {
        use sqlx::Arguments as _;
        match self.args.add(val) {
            Ok(()) => self,
            Err(e) => {
                self.error = Some(Arc::new(e));
                self
            }
        }
    }

    pub fn from_str(sql: &str) -> Self {
        Self {
            sql: sql.to_string(),
            args: Arguments::default(),
            error: None,
        }
    }

    /// Returns the SQL and arguments, or a bind error if one occurred.
    pub fn into_parts(self) -> Result<(String, Arguments<'static>), QueryError> {
        if let Some(err) = self.error {
            return Err(QueryError::BindError(err.to_string()));
        }
        Ok((self.sql, self.args))
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum QueryError {
    #[error("bind error: {0}")]
    BindError(String),
    #[error("source not set")]
    SourceNotSet,
    #[error("table metadata not set for {0}")]
    TableNotSet(&'static str),
    #[error("query source table mismatch: expected {expected}, got {got}")]
    TableMismatch { expected: String, got: String },
    #[error("unknown query alias or logical prefix '{0}'")]
    UnknownAlias(String),
    #[error("invalid projection field '{0}'")]
    InvalidProjection(String),
    #[error("reference '{reference}' is missing {field}")]
    MissingReference {
        reference: &'static str,
        field: &'static str,
    },
    #[error("unsupported filter operator '{0}' for this value")]
    UnsupportedFilter(&'static str),
    #[error("placeholder error: {0}")]
    PlaceholderError(#[from] crate::db::placeholders::PlaceholderError),
    #[error("missing binding for {0}")]
    MissingBinding(String),
    #[error("unused binding: {0}")]
    UnusedBinding(String),
    #[error("bind count mismatch: expected {expected}, got {got}")]
    BindCountMismatch { expected: usize, got: usize },
    #[error(
        "invalid identifier '{0}': only alphanumerics, underscores, dots, and spaces are allowed"
    )]
    InvalidIdentifier(String),
}

/// A page of results from a paginated query.
#[derive(Debug, serde::Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub total: i64,
    pub page: usize,
    pub per_page: usize,
    pub total_pages: usize,
}

/// Row locking mode for SELECT ... FOR UPDATE / FOR SHARE.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockMode {
    Update,
    Share,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    Like,
    ILike,
    In,
}

pub enum FilterValue {
    One(ArgValue),
    Many(Vec<ArgValue>),
}

pub struct FilterPredicate {
    pub column: &'static str,
    pub op: FilterOp,
    pub value: FilterValue,
}

pub trait Filterable {
    fn filters(&self) -> Vec<FilterPredicate>;
}
