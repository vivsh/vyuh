use std::collections::HashMap;

use super::{QueryError, Statement};
use crate::db::argvalue::ArgValue;
use crate::db::commons::{Arguments, Row};
use crate::db::executor::{DBSession, DbError};
use crate::db::placeholders::{Dialect, has_named_placeholder, resolve_placeholders};

/// Builder for raw SQL with Vyuh named-bind support.
pub struct RawQuery {
    sql: String,
    args: Arguments<'static>,
    named_args: HashMap<String, ArgValue>,
    error: Option<QueryError>,
}

impl RawQuery {
    pub(crate) fn new(sql: &str) -> Self {
        Self {
            sql: sql.to_string(),
            args: Arguments::default(),
            named_args: HashMap::new(),
            error: None,
        }
    }

    /// Bind a named argument used by `:name` placeholders.
    pub fn bind<T>(mut self, name: &str, val: T) -> Self
    where
        T: Clone
            + for<'q> sqlx::Encode<'q, crate::db::commons::Database>
            + sqlx::Type<crate::db::commons::Database>
            + Send
            + Sync
            + 'static,
    {
        if self.error.is_some() {
            return self;
        }
        self.named_args.insert(name.to_string(), ArgValue::new(val));
        self
    }

    fn into_statement(mut self) -> Result<Statement, QueryError> {
        if let Some(err) = self.error {
            return Err(err);
        }
        if self.named_args.is_empty() && !has_named_placeholder(&self.sql) {
            return Ok(Statement::new(&self.sql, self.args));
        }
        let final_sql = resolve_placeholders(
            &self.sql,
            &mut self.args,
            &self.named_args,
            Dialect::active(),
        )?;
        Ok(Statement::new(&final_sql, self.args))
    }

    pub async fn execute(self, session: &mut impl DBSession) -> Result<u64, DbError> {
        let stmt = self.into_statement()?;
        session.execute(stmt).await
    }

    pub async fn one<M>(self, session: &mut impl DBSession) -> Result<M, DbError>
    where
        M: for<'r> sqlx::FromRow<'r, Row> + Send + Unpin + 'static,
    {
        let stmt = self.into_statement()?;
        session.fetch_one(stmt).await
    }

    pub async fn all<M>(self, session: &mut impl DBSession) -> Result<Vec<M>, DbError>
    where
        M: for<'r> sqlx::FromRow<'r, Row> + Send + Unpin + 'static,
    {
        let stmt = self.into_statement()?;
        session.fetch_all(stmt).await
    }

    pub async fn first<M>(self, session: &mut impl DBSession) -> Result<Option<M>, DbError>
    where
        M: for<'r> sqlx::FromRow<'r, Row> + Send + Unpin + 'static,
    {
        let stmt = self.into_statement()?;
        session.fetch_optional(stmt).await
    }

    pub async fn scalar<T>(self, session: &mut impl DBSession) -> Result<T, DbError>
    where
        for<'r> (T,): sqlx::FromRow<'r, Row>,
        T: Send + Unpin + 'static,
    {
        let stmt = self.into_statement()?;
        let row: (T,) = session.fetch_one(stmt).await?;
        Ok(row.0)
    }
}
