//! Table, CTE, and subquery sources and projected source columns.
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use super::expr::{ColumnRef, Expr, ExprNode, IntoExpr, IntoSourceColumn, OrderExpr, Predicate};
use super::handles::{ColumnOwner, Table};
use super::render::SelectModel;
use super::scope::QueryScope;
use super::traits::{IntoSourceMeta, IntoTableSource, Projectable};
use super::validate::{source_key, table_name};

/// Typed common table expression source.
pub struct Cte<T>
where
    T: Projectable,
{
    pub(super) data: Arc<CteData>,
    pub(super) columns: <T as Projectable>::Columns,
    pub(super) _marker: PhantomData<fn() -> T>,
}

/// Typed subquery source.
pub struct Subquery<T>
where
    T: Projectable,
{
    pub(super) data: Arc<SubqueryData>,
    pub(super) columns: <T as Projectable>::Columns,
    pub(super) _marker: PhantomData<fn() -> T>,
}

#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SourceColumnRef {
    pub(super) source: Source,
    pub(super) owner: Option<Source>,
    pub(super) name: Arc<str>,
}

/// Typed projected output column generated from a CTE or subquery row shape.
pub struct ProjectedColumn<T> {
    pub(super) data: Arc<SourceColumnData>,
    _marker: PhantomData<fn() -> T>,
}

/// One-column projection picked from a CTE or subquery source.
pub struct Picked<T> {
    pub(super) data: Arc<PickedData>,
    _marker: PhantomData<fn() -> T>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct PickedData {
    pub(super) source: Source,
    pub(super) column: SourceColumnRef,
}

/// Source handle passed to generated projection metadata.
#[derive(Clone)]
pub struct ProjectionSource {
    pub(super) source: Source,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct SourceColumnData {
    pub(super) source: Source,
    pub(super) name: Arc<str>,
}

#[derive(Clone)]
pub enum Source {
    #[doc(hidden)]
    Table(Table),
    #[doc(hidden)]
    Cte(CteSource),
    #[doc(hidden)]
    Subquery(SubquerySource),
}

#[derive(Clone)]
pub struct CteSource {
    pub(super) data: Arc<CteData>,
}

#[derive(Clone)]
pub struct SubquerySource {
    pub(super) data: Arc<SubqueryData>,
}

pub(super) struct CteData {
    pub(super) name: Arc<str>,
    pub(super) scope: QueryScope,
    pub(super) model: SelectModel,
    pub(super) columns: Vec<String>,
    pub(super) recursive: bool,
}

pub(super) struct SubqueryData {
    pub(super) name: Arc<str>,
    pub(super) scope: QueryScope,
    pub(super) model: SelectModel,
    pub(super) columns: Vec<String>,
}

pub(super) struct SelectSource {
    pub(super) model: SelectModel,
    pub(super) columns: Vec<String>,
}

/// Typed query source kind exposed through [`SourceMeta`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceKind {
    /// A concrete database table source.
    Table,
    /// A common table expression source.
    Cte,
    /// An inline subquery source.
    Subquery,
}

/// Read-only typed query source metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceMeta {
    kind: SourceKind,
    name: String,
    schema: Option<String>,
    writable_columns: Vec<String>,
    output_columns: Vec<String>,
}

impl fmt::Debug for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Source::Table(table) => f.debug_tuple("Table").field(table).finish(),
            Source::Cte(cte) => f.debug_tuple("Cte").field(&cte.data.name).finish(),
            Source::Subquery(subquery) => f
                .debug_tuple("Subquery")
                .field(&subquery.data.name)
                .finish(),
        }
    }
}

impl PartialEq for Source {
    fn eq(&self, other: &Self) -> bool {
        source_key(self) == source_key(other)
    }
}

impl Eq for Source {}

impl Hash for Source {
    fn hash<H: Hasher>(&self, state: &mut H) {
        source_key(self).hash(state);
    }
}

impl Source {
    fn column_owner(&self) -> ColumnOwner {
        match self {
            Source::Table(table) => ColumnOwner::Root(table.clone()),
            Source::Cte(cte) => ColumnOwner::Source(cte.data.name.clone()),
            Source::Subquery(subquery) => ColumnOwner::Source(subquery.data.name.clone()),
        }
    }
}

impl fmt::Debug for CteSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CteSource").field(&self.data.name).finish()
    }
}

impl PartialEq for CteSource {
    fn eq(&self, other: &Self) -> bool {
        self.data.name == other.data.name
    }
}

impl Eq for CteSource {}

impl Hash for CteSource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.name.hash(state);
    }
}

impl CteSource {
    pub(super) fn recursive(mut self) -> Self {
        self.data = Arc::new(CteData {
            name: self.data.name.clone(),
            scope: self.data.scope.clone(),
            model: self.data.model.clone(),
            columns: self.data.columns.clone(),
            recursive: true,
        });
        self
    }
}

impl fmt::Debug for SubquerySource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("SubquerySource")
            .field(&self.data.name)
            .finish()
    }
}

impl PartialEq for SubquerySource {
    fn eq(&self, other: &Self) -> bool {
        self.data.name == other.data.name
    }
}

impl Eq for SubquerySource {}

impl Hash for SubquerySource {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.name.hash(state);
    }
}

impl<T> Clone for Cte<T>
where
    T: Projectable,
{
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            columns: self.columns.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T> fmt::Debug for Cte<T>
where
    T: Projectable,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Cte").field(&self.data.name).finish()
    }
}

impl<T> Clone for Subquery<T>
where
    T: Projectable,
{
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            columns: self.columns.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T> fmt::Debug for Subquery<T>
where
    T: Projectable,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Subquery").field(&self.data.name).finish()
    }
}

impl<T> Clone for ProjectedColumn<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T> fmt::Debug for ProjectedColumn<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectedColumn")
            .field("source", &self.data.source)
            .field("name", &self.data.name)
            .finish()
    }
}

impl<T> PartialEq for ProjectedColumn<T> {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl<T> Eq for ProjectedColumn<T> {}

impl<T> Hash for ProjectedColumn<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state);
    }
}

impl<T> Clone for Picked<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T> fmt::Debug for Picked<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Picked")
            .field("source", &self.data.source)
            .field("column", &self.data.column)
            .finish()
    }
}

impl fmt::Debug for ProjectionSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ProjectionSource")
            .field(&self.source)
            .finish()
    }
}

impl<T> Cte<T>
where
    T: Projectable,
{
    /// Returns typed projected columns for this CTE row shape.
    pub fn cols(&self) -> <T as Projectable>::Columns {
        self.columns.clone()
    }

    /// Picks one projected column from this CTE for an expression such as `IN`.
    pub fn pick<U>(&self, col: &ProjectedColumn<U>) -> Picked<U> {
        Picked::new(Source::Cte(self.as_source()), col.clone())
    }

    pub(super) fn as_source(&self) -> CteSource {
        CteSource {
            data: self.data.clone(),
        }
    }
}

impl<T> Subquery<T>
where
    T: Projectable,
{
    /// Returns typed projected columns for this subquery row shape.
    pub fn cols(&self) -> <T as Projectable>::Columns {
        self.columns.clone()
    }

    /// Picks one projected column from this subquery for an expression such as `IN`.
    pub fn pick<U>(&self, col: &ProjectedColumn<U>) -> Picked<U> {
        Picked::new(Source::Subquery(self.as_source()), col.clone())
    }

    pub(super) fn as_source(&self) -> SubquerySource {
        SubquerySource {
            data: self.data.clone(),
        }
    }
}

impl SourceMeta {
    pub(super) fn table(table: &Table) -> Self {
        let writable_columns = table
            .data
            .columns
            .as_deref()
            .map(|columns| columns.iter().cloned().collect())
            .unwrap_or_default();
        Self {
            kind: SourceKind::Table,
            name: table.data.name.to_string(),
            schema: table.data.schema.as_deref().map(str::to_string),
            writable_columns,
            output_columns: Vec::new(),
        }
    }

    pub(super) fn cte(source: &CteSource) -> Self {
        Self {
            kind: SourceKind::Cte,
            name: source.data.name.to_string(),
            schema: None,
            writable_columns: Vec::new(),
            output_columns: source.data.columns.clone(),
        }
    }

    pub(super) fn subquery(source: &SubquerySource) -> Self {
        Self {
            kind: SourceKind::Subquery,
            name: source.data.name.to_string(),
            schema: None,
            writable_columns: Vec::new(),
            output_columns: source.data.columns.clone(),
        }
    }

    /// Returns the source kind.
    pub fn kind(&self) -> SourceKind {
        self.kind
    }

    /// Returns the source name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the schema name for table sources when one exists.
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    /// Returns the schema-qualified source name when a schema exists.
    pub fn qualified_name(&self) -> String {
        table_name(self.schema(), &self.name)
    }

    /// Returns known writable column names for table sources.
    pub fn writable_columns(&self) -> &[String] {
        &self.writable_columns
    }

    /// Returns projected output column names for CTE and subquery sources.
    pub fn output_columns(&self) -> &[String] {
        &self.output_columns
    }
}

impl<T> Deref for Cte<T>
where
    T: Projectable,
{
    type Target = <T as Projectable>::Columns;

    fn deref(&self) -> &Self::Target {
        &self.columns
    }
}

impl<T> Deref for Subquery<T>
where
    T: Projectable,
{
    type Target = <T as Projectable>::Columns;

    fn deref(&self) -> &Self::Target {
        &self.columns
    }
}

impl<T> ProjectedColumn<T> {
    fn new(source: Source, name: &'static str) -> Self {
        Self {
            data: Arc::new(SourceColumnData {
                source,
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
            owner: self.data.source.column_owner(),
            name: self.data.name.clone(),
        }))
    }
}

impl ProjectedColumn<String> {
    /// SQL LIKE predicate for text source columns.
    pub fn like<R>(&self, rhs: R) -> Predicate
    where
        R: IntoExpr<String>,
    {
        self.compare("LIKE", rhs)
    }

    /// SQL ILIKE predicate for text source columns.
    pub fn ilike<R>(&self, rhs: R) -> Predicate
    where
        R: IntoExpr<String>,
    {
        self.compare("ILIKE", rhs)
    }
}

impl<T> IntoExpr<T> for ProjectedColumn<T> {
    fn into_expr(self) -> Expr<T> {
        self.expr()
    }
}

impl<T> IntoExpr<T> for &ProjectedColumn<T> {
    fn into_expr(self) -> Expr<T> {
        self.expr()
    }
}

impl<T> Picked<T> {
    fn new(source: Source, col: ProjectedColumn<T>) -> Self {
        Self {
            data: Arc::new(PickedData {
                source: source.clone(),
                column: SourceColumnRef {
                    source,
                    owner: Some(col.data.source.clone()),
                    name: col.data.name.clone(),
                },
            }),
            _marker: PhantomData,
        }
    }
}

impl ProjectionSource {
    pub(super) fn new(source: Source) -> Self {
        Self { source }
    }

    /// Returns a projected output column for generated source column structs.
    pub fn col<T>(&self, name: &'static str) -> ProjectedColumn<T> {
        ProjectedColumn::new(self.source.clone(), name)
    }
}

impl<T> IntoSourceColumn<T> for Picked<T> {
    fn into_source_column(self) -> SourceColumnRef {
        self.data.column.clone()
    }
}

impl<T> IntoSourceColumn<T> for &Picked<T> {
    fn into_source_column(self) -> SourceColumnRef {
        self.data.column.clone()
    }
}

impl<T> IntoTableSource for Cte<T>
where
    T: Projectable,
{
    fn into_table_source(self) -> Source {
        Source::Cte(self.as_source())
    }
}

impl<T> IntoTableSource for &Cte<T>
where
    T: Projectable,
{
    fn into_table_source(self) -> Source {
        Source::Cte(self.as_source())
    }
}

impl<T> IntoTableSource for Subquery<T>
where
    T: Projectable,
{
    fn into_table_source(self) -> Source {
        Source::Subquery(self.as_source())
    }
}

impl<T> IntoTableSource for &Subquery<T>
where
    T: Projectable,
{
    fn into_table_source(self) -> Source {
        Source::Subquery(self.as_source())
    }
}

impl<T> IntoSourceMeta for Cte<T>
where
    T: Projectable,
{
    fn source_meta(&self) -> SourceMeta {
        SourceMeta::cte(&self.as_source())
    }
}

impl<T> IntoSourceMeta for &Cte<T>
where
    T: Projectable,
{
    fn source_meta(&self) -> SourceMeta {
        SourceMeta::cte(&self.as_source())
    }
}

impl<T> IntoSourceMeta for Subquery<T>
where
    T: Projectable,
{
    fn source_meta(&self) -> SourceMeta {
        SourceMeta::subquery(&self.as_source())
    }
}

impl<T> IntoSourceMeta for &Subquery<T>
where
    T: Projectable,
{
    fn source_meta(&self) -> SourceMeta {
        SourceMeta::subquery(&self.as_source())
    }
}
