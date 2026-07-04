//! SQLite typed-query dialect renderer.

use crate::db::placeholders::Dialect;

use super::super::super::QueryError;
use super::super::expr::ColumnRef;
use super::{DialectFeature, DialectRenderer, common};

pub(super) struct SqliteSpec;

impl DialectRenderer for SqliteSpec {
    fn dialect(&self) -> Dialect {
        Dialect::Sqlite
    }

    fn placeholder(&self, _position: usize) -> String {
        "?".to_string()
    }

    fn validate_feature(&self, feature: DialectFeature) -> Result<(), QueryError> {
        match feature {
            DialectFeature::Ilike => Err(common::unsupported(self.dialect(), feature.name())),
            DialectFeature::Returning | DialectFeature::Upsert | DialectFeature::RecursiveCte => {
                Ok(())
            }
        }
    }

    fn render_upsert(
        &self,
        conflict: &[ColumnRef],
        update_columns: &[&str],
    ) -> Result<String, QueryError> {
        common::render_on_conflict(conflict, update_columns)
    }
}
