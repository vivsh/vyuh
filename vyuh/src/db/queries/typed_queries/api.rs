//! Public typed-query constructors and hidden macro support helpers.
use std::any::type_name;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::db::argvalue::ArgValue;

use super::expr::{Expr, ExprNode, ValueNode};
use super::handles::{Var, VarData};
use super::scope::QueryScope;
use super::source::SourceMeta;
use super::traits::{IntoSourceMeta, IntoTableSource};

/// Creates a typed named placeholder.
pub fn var<T>(name: &str) -> Var<T> {
    Var {
        data: Arc::new(VarData {
            name: Arc::from(name),
            rust_type: Some(type_name::<T>()),
        }),
        _marker: PhantomData,
    }
}

/// Creates an immediately bound typed SQL value.
pub fn val<T>(value: T) -> Expr<T>
where
    T: Clone
        + for<'q> sqlx::Encode<'q, crate::db::commons::Database>
        + sqlx::Type<crate::db::commons::Database>
        + Send
        + Sync
        + 'static,
{
    Expr::new(ExprNode::Value(ValueNode::Val {
        name: None,
        rust_type: type_name::<T>(),
        value: ArgValue::new(value),
    }))
}

/// Starts a source-first typed query scope.
pub fn from<S>(source: S) -> QueryScope
where
    S: IntoTableSource,
{
    QueryScope::new(source.into_table_source())
}

/// Returns read-only metadata for a typed query source.
pub fn meta<S>(source: S) -> SourceMeta
where
    S: IntoSourceMeta,
{
    source.source_meta()
}

/// Hidden construction helpers used by generated typed-query metadata.
#[doc(hidden)]
pub mod __private {
    use std::sync::Arc;

    pub use super::super::expr::{ColumnRef, IntoSourceColumn};
    pub use super::super::handles::{Column, ModelTable, Reference, Table, Var};
    pub use super::super::source::{ProjectedColumn, ProjectionSource};
    pub use super::super::traits::HasCols;
    pub use super::super::traits::{IntoColumnRef, IntoTableSource, Projectable};

    /// Creates a table handle from macro-generated metadata.
    pub fn table(name: &str) -> Table {
        Table::new(None, name)
    }

    /// Creates a schema-qualified table handle from macro-generated metadata.
    pub fn table_schema(schema: &str, name: &str) -> Table {
        Table::new(Some(schema), name)
    }

    /// Creates a reference handle from macro-generated metadata.
    pub fn reference(name: &str) -> Reference {
        Reference {
            name: Arc::from(name),
        }
    }
}
