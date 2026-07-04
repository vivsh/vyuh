//! Public traits implemented by generated typed-query metadata.
use crate::db::interfaces::Record;

use super::expr::ColumnRef;
use super::handles::{Reference, Table};
use super::source::{ProjectionSource, Source, SourceMeta};

/// Provides typed projected columns for CTE and subquery row shapes.
pub trait Projectable: Record {
    /// Generated projected column struct for this row shape.
    type Columns: Clone;

    /// Builds projected columns owned by the provided source.
    fn projected_columns(source: ProjectionSource) -> <Self as Projectable>::Columns;
}

/// Provides generated table-owned and reference-owned typed columns.
///
/// This trait is public only so derive expansions can name it. Application code
/// should get typed columns from a query source such as `Model::table().cols()`,
/// a CTE handle, or a subquery handle.
#[doc(hidden)]
pub trait HasCols: Record {
    /// Generated column struct for table/reference query scopes.
    type Columns: Clone;

    /// Builds columns owned by a concrete table handle.
    #[doc(hidden)]
    fn cols_for_table(table: &Table) -> Self::Columns;

    /// Builds columns owned by a logical reference handle.
    #[doc(hidden)]
    fn cols_for_reference(reference: &Reference) -> Self::Columns;
}

/// Converts table-like values into a typed query root.
pub trait IntoTableSource {
    #[doc(hidden)]
    fn into_table_source(self) -> Source;
}

/// Converts typed query sources into metadata descriptors.
pub trait IntoSourceMeta {
    #[doc(hidden)]
    fn source_meta(&self) -> SourceMeta;
}

/// Converts column-like values into an untyped column reference.
pub trait IntoColumnRef {
    #[doc(hidden)]
    fn into_column_ref(self) -> ColumnRef;
}

impl IntoColumnRef for ColumnRef {
    fn into_column_ref(self) -> ColumnRef {
        self
    }
}
