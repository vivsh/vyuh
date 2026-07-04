//! Synchronous typed-query terminal builders.
use std::marker::PhantomData;

use crate::db::interfaces::Record;

use super::executables::{
    All, BatchInsert, BatchUpsert, Count, Delete, Exists, First, Insert, One, ReturningBatchInsert,
    ReturningBatchUpsert, ReturningDelete, ReturningInsert, ReturningUpdate, Scalar, Slice, Update,
};
use super::expr::IntoExpr;
use super::scope::{QueryScope, ReturningScope};
use super::traits::IntoColumnRef;

impl QueryScope {
    /// Builds an executable that fetches all selected rows.
    pub fn all<T>(self) -> All<T>
    where
        T: Record,
    {
        All {
            scope: self,
            _marker: PhantomData,
        }
    }

    /// Builds an executable that fetches exactly one selected row.
    pub fn one<T>(self) -> One<T>
    where
        T: Record,
    {
        One {
            scope: self,
            _marker: PhantomData,
        }
    }

    /// Builds an executable that fetches the first selected row.
    pub fn first<T>(self) -> First<T>
    where
        T: Record,
    {
        First {
            scope: self,
            _marker: PhantomData,
        }
    }

    /// Builds an executable that fetches a limited selected row slice.
    pub fn slice<T>(self, offset: usize, count: usize) -> Slice<T>
    where
        T: Record,
    {
        Slice {
            scope: self,
            offset,
            count,
            _marker: PhantomData,
        }
    }

    /// Builds an executable that inserts one record.
    pub fn insert<T>(self, row: &T) -> Insert<'_, T>
    where
        T: Record,
    {
        Insert { scope: self, row }
    }

    /// Builds an executable that updates matching rows from one record.
    pub fn update<T>(self, row: &T) -> Update<'_, T>
    where
        T: Record,
    {
        Update { scope: self, row }
    }

    /// Builds an executable that deletes matching rows.
    pub fn delete(self) -> Delete {
        Delete { scope: self }
    }

    /// Builds a `COUNT(*)` executable.
    pub fn count(self) -> Count {
        Count { scope: self }
    }

    /// Builds an `EXISTS(...)` executable.
    pub fn exists(self) -> Exists {
        Exists { scope: self }
    }

    /// Builds a scalar-select executable.
    pub fn scalar<V>(self, expr: impl IntoExpr<V>) -> Scalar<V> {
        Scalar {
            scope: self,
            expr: expr.into_expr().node,
            _marker: PhantomData,
        }
    }

    /// Builds an executable that inserts multiple records.
    pub fn batch_insert<T>(self, rows: &[T]) -> BatchInsert<'_, T>
    where
        T: Record,
    {
        BatchInsert { scope: self, rows }
    }

    /// Builds an executable that upserts multiple records.
    pub fn batch_upsert<T, I, C>(self, rows: &[T], conflict: I) -> BatchUpsert<'_, T>
    where
        T: Record,
        I: IntoIterator<Item = C>,
        C: IntoColumnRef,
    {
        let conflict = conflict
            .into_iter()
            .map(IntoColumnRef::into_column_ref)
            .collect();
        BatchUpsert {
            scope: self,
            rows,
            conflict,
        }
    }
}

impl<R> ReturningScope<R>
where
    R: Record,
{
    /// Builds a returning insert executable.
    pub fn insert<T>(self, row: &T) -> ReturningInsert<'_, R, T>
    where
        T: Record,
    {
        ReturningInsert {
            returning: self,
            row,
        }
    }

    /// Builds a returning update executable.
    pub fn update<T>(self, row: &T) -> ReturningUpdate<'_, R, T>
    where
        T: Record,
    {
        ReturningUpdate {
            returning: self,
            row,
        }
    }

    /// Builds a returning delete executable.
    pub fn delete(self) -> ReturningDelete<R> {
        ReturningDelete { returning: self }
    }

    /// Builds a returning batch insert executable.
    pub fn batch_insert<T>(self, rows: &[T]) -> ReturningBatchInsert<'_, R, T>
    where
        T: Record,
    {
        ReturningBatchInsert {
            returning: self,
            rows,
        }
    }

    /// Builds a returning batch upsert executable.
    pub fn batch_upsert<T, I, C>(self, rows: &[T], conflict: I) -> ReturningBatchUpsert<'_, R, T>
    where
        T: Record,
        I: IntoIterator<Item = C>,
        C: IntoColumnRef,
    {
        let conflict = conflict
            .into_iter()
            .map(IntoColumnRef::into_column_ref)
            .collect();
        ReturningBatchUpsert {
            returning: self,
            rows,
            conflict,
        }
    }
}
