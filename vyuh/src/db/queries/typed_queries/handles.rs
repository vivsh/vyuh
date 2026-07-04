//! Table, column, reference, and variable handles.
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use crate::db::interfaces::Model;

use super::expr::{
    ColumnRef, Expr, ExprNode, IntoExpr, IntoSourceColumn, OrderExpr, Predicate, ValueNode,
};
use super::source::{Source, SourceMeta};
use super::traits::{HasCols, IntoColumnRef, IntoSourceMeta, IntoTableSource};

/// Database table source handle.
#[derive(Debug, Clone, Eq)]
pub struct Table {
    pub(super) data: Arc<TableData>,
}

/// Typed table handle returned by `Model::table()`.
///
/// It carries the model type so `table.cols()` can return source-bound typed
/// columns without relying on static column constructors.
pub struct ModelTable<M>
where
    M: HasCols,
{
    table: Table,
    columns: <M as HasCols>::Columns,
    _marker: PhantomData<fn() -> M>,
}

impl<M> fmt::Debug for ModelTable<M>
where
    M: HasCols,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ModelTable").field(&self.table).finish()
    }
}

impl<M> Clone for ModelTable<M>
where
    M: HasCols,
{
    fn clone(&self) -> Self {
        Self {
            table: self.table.clone(),
            columns: self.columns.clone(),
            _marker: PhantomData,
        }
    }
}

#[derive(Debug, Eq)]
pub(super) struct TableData {
    pub(super) schema: Option<Arc<str>>,
    pub(super) name: Arc<str>,
    pub(super) columns: Option<Arc<[String]>>,
}

impl PartialEq for TableData {
    fn eq(&self, other: &Self) -> bool {
        self.schema == other.schema && self.name == other.name
    }
}

impl Hash for TableData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.schema.hash(state);
        self.name.hash(state);
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Hash for Table {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state);
    }
}

impl<M> PartialEq for ModelTable<M>
where
    M: HasCols,
{
    fn eq(&self, other: &Self) -> bool {
        self.table == other.table
    }
}

impl<M> Eq for ModelTable<M> where M: HasCols {}

impl<M> Hash for ModelTable<M>
where
    M: HasCols,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.table.hash(state);
    }
}

/// Logical reference source handle for generated column shapes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Reference {
    pub(super) name: Arc<str>,
}

/// Typed database column handle.
#[derive(Debug)]
pub struct Column<T> {
    pub(super) data: Arc<ColumnData>,
    _marker: PhantomData<fn() -> T>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct ColumnData {
    pub(super) owner: ColumnOwner,
    pub(super) name: Arc<str>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum ColumnOwner {
    Root(Table),
    Source(Arc<str>),
    Reference(Arc<str>),
}

impl<T> Clone for Column<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T> PartialEq for Column<T> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<T> Eq for Column<T> {}

impl<T> Hash for Column<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state);
    }
}

/// Typed named placeholder handle.
#[derive(Debug)]
pub struct Var<T> {
    pub(super) data: Arc<VarData>,
    pub(super) _marker: PhantomData<fn() -> T>,
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(super) struct VarData {
    pub(super) name: Arc<str>,
    pub(super) rust_type: Option<&'static str>,
}

impl<T> Clone for Var<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T> PartialEq for Var<T> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<T> Eq for Var<T> {}

impl<T> Hash for Var<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state);
    }
}

impl Table {
    pub(super) fn new(schema: Option<&str>, name: &str) -> Self {
        Self {
            data: Arc::new(TableData {
                schema: schema.map(Arc::from),
                name: Arc::from(name),
                columns: None,
            }),
        }
    }

    fn with_columns(&self, columns: Vec<String>) -> Self {
        Self {
            data: Arc::new(TableData {
                schema: self.data.schema.clone(),
                name: self.data.name.clone(),
                columns: Some(Arc::from(columns)),
            }),
        }
    }

    /// Returns a table handle with a schema name.
    #[doc(hidden)]
    pub fn schema(&self, schema: &str) -> Self {
        Self::new(Some(schema), &self.data.name)
    }

    /// Returns a typed column handle for this table.
    #[doc(hidden)]
    pub fn col<T>(&self, name: &str) -> Column<T> {
        Column::new(ColumnOwner::Root(self.clone()), name)
    }
}

impl<M> ModelTable<M>
where
    M: HasCols,
{
    /// Creates a model-typed table wrapper around an internal table source.
    #[doc(hidden)]
    pub fn new(table: Table) -> Self {
        let columns = M::cols_for_table(&table);
        Self {
            table,
            columns,
            _marker: PhantomData,
        }
    }

    /// Creates a model table with known record columns for validation.
    #[doc(hidden)]
    pub fn new_with_columns(table: Table, columns: Vec<String>) -> Self {
        Self::new(table.with_columns(columns))
    }

    fn as_table(&self) -> &Table {
        &self.table
    }
}

impl<M> ModelTable<M>
where
    M: Model + HasCols,
{
    /// Returns source-bound typed columns for this table handle.
    pub fn cols(&self) -> <M as HasCols>::Columns {
        self.columns.clone()
    }
}

impl<M> IntoSourceMeta for ModelTable<M>
where
    M: HasCols,
{
    fn source_meta(&self) -> SourceMeta {
        SourceMeta::table(&self.table)
    }
}

impl<M> IntoSourceMeta for &ModelTable<M>
where
    M: HasCols,
{
    fn source_meta(&self) -> SourceMeta {
        SourceMeta::table(&self.table)
    }
}

impl<M> Deref for ModelTable<M>
where
    M: HasCols,
{
    type Target = <M as HasCols>::Columns;

    fn deref(&self) -> &Self::Target {
        &self.columns
    }
}

impl Reference {
    /// Returns a typed column handle owned by this logical reference.
    #[doc(hidden)]
    pub fn col<T>(&self, name: &str) -> Column<T> {
        Column::new(ColumnOwner::Reference(self.name.clone()), name)
    }
}

impl<T> Column<T> {
    pub(super) fn new(owner: ColumnOwner, name: &str) -> Self {
        Self {
            data: Arc::new(ColumnData {
                owner,
                name: Arc::from(name),
            }),
            _marker: PhantomData,
        }
    }

    /// Equality predicate.
    pub fn eq<R>(&self, rhs: R) -> Predicate
    where
        R: IntoExpr<T>,
    {
        self.compare("=", rhs)
    }

    /// Inequality predicate.
    pub fn ne<R>(&self, rhs: R) -> Predicate
    where
        R: IntoExpr<T>,
    {
        self.compare("!=", rhs)
    }

    /// Less-than predicate.
    pub fn lt<R>(&self, rhs: R) -> Predicate
    where
        R: IntoExpr<T>,
    {
        self.compare("<", rhs)
    }

    /// Less-than-or-equal predicate.
    pub fn lte<R>(&self, rhs: R) -> Predicate
    where
        R: IntoExpr<T>,
    {
        self.compare("<=", rhs)
    }

    /// Greater-than predicate.
    pub fn gt<R>(&self, rhs: R) -> Predicate
    where
        R: IntoExpr<T>,
    {
        self.compare(">", rhs)
    }

    /// Greater-than-or-equal predicate.
    pub fn gte<R>(&self, rhs: R) -> Predicate
    where
        R: IntoExpr<T>,
    {
        self.compare(">=", rhs)
    }

    /// SQL `IN (subquery)` predicate.
    pub fn in_<R>(&self, rhs: R) -> Predicate
    where
        R: IntoSourceColumn<T>,
    {
        Predicate::new(ExprNode::InSource {
            left: Box::new(self.expr().node),
            source: rhs.into_source_column(),
        })
    }

    /// Adds this column to another typed expression.
    pub fn add<R>(&self, rhs: R) -> Expr<T>
    where
        R: IntoExpr<T>,
    {
        self.expr().add(rhs)
    }

    /// Ascending order expression.
    pub fn asc(&self) -> OrderExpr {
        self.expr().asc()
    }

    /// Descending order expression.
    pub fn desc(&self) -> OrderExpr {
        self.expr().desc()
    }

    fn compare<R>(&self, op: &'static str, rhs: R) -> Predicate
    where
        R: IntoExpr<T>,
    {
        Predicate::new(ExprNode::Binary {
            left: Box::new(self.expr().node),
            op,
            right: Box::new(rhs.into_expr().node),
        })
    }

    fn expr(&self) -> Expr<T> {
        Expr::new(ExprNode::Column(ColumnRef {
            owner: self.data.owner.clone(),
            name: self.data.name.clone(),
        }))
    }
}

impl Column<String> {
    /// SQL LIKE predicate for text columns.
    pub fn like<R>(&self, rhs: R) -> Predicate
    where
        R: IntoExpr<String>,
    {
        self.compare("LIKE", rhs)
    }

    /// SQL ILIKE predicate for text columns.
    pub fn ilike<R>(&self, rhs: R) -> Predicate
    where
        R: IntoExpr<String>,
    {
        self.compare("ILIKE", rhs)
    }
}

impl<T> IntoExpr<T> for Column<T> {
    fn into_expr(self) -> Expr<T> {
        self.expr()
    }
}

impl<T> IntoExpr<T> for &Column<T> {
    fn into_expr(self) -> Expr<T> {
        self.expr()
    }
}

impl<T> IntoExpr<T> for Var<T> {
    fn into_expr(self) -> Expr<T> {
        (&self).into_expr()
    }
}

impl<T> IntoExpr<T> for &Var<T> {
    fn into_expr(self) -> Expr<T> {
        Expr::new(ExprNode::Value(ValueNode::Var {
            name: self.data.name.to_string(),
            rust_type: self.data.rust_type,
        }))
    }
}

impl IntoTableSource for Table {
    fn into_table_source(self) -> Source {
        Source::Table(self)
    }
}

impl IntoTableSource for &Table {
    fn into_table_source(self) -> Source {
        Source::Table(self.clone())
    }
}

impl<M> IntoTableSource for ModelTable<M>
where
    M: HasCols,
{
    fn into_table_source(self) -> Source {
        Source::Table(self.table)
    }
}

impl<M> IntoTableSource for &ModelTable<M>
where
    M: HasCols,
{
    fn into_table_source(self) -> Source {
        Source::Table(self.as_table().clone())
    }
}

impl<T> IntoColumnRef for Column<T> {
    fn into_column_ref(self) -> ColumnRef {
        (&self).into_column_ref()
    }
}

impl<T> IntoColumnRef for &Column<T> {
    fn into_column_ref(self) -> ColumnRef {
        ColumnRef {
            owner: self.data.owner.clone(),
            name: self.data.name.clone(),
        }
    }
}
