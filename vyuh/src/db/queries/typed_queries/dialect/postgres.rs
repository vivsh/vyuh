//! PostgreSQL typed-query dialect renderer.

use crate::db::placeholders::Dialect;

use super::super::super::QueryError;
use super::super::expr::ColumnRef;
use super::{DialectFeature, DialectRenderer, common};

pub(super) struct PostgresSpec;

impl DialectRenderer for PostgresSpec {
    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }

    fn placeholder(&self, position: usize) -> String {
        format!("${position}")
    }

    fn validate_feature(&self, _feature: DialectFeature) -> Result<(), QueryError> {
        Ok(())
    }

    fn render_upsert(
        &self,
        conflict: &[ColumnRef],
        update_columns: &[&str],
    ) -> Result<String, QueryError> {
        common::render_on_conflict(conflict, update_columns)
    }
}
