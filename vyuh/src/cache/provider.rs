//! Async byte-storage contract shared by in-process and remote cache providers.

use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use bytes::Bytes;

use super::{CacheError, key::CacheKey, key::CacheScope};

/// A cache entry lifetime selected by a typed cache operation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CacheTtl {
    /// Uses the selected provider's configured default lifetime.
    #[default]
    Default,
    /// Uses one explicit positive lifetime.
    For(Duration),
    /// Retains the entry until it is evicted or explicitly deleted.
    Forever,
}

impl CacheTtl {
    /// Selects an explicit lifetime. Zero is rejected by the terminal operation.
    pub const fn for_duration(value: Duration) -> Self {
        Self::For(value)
    }

    pub(crate) fn validate(self) -> Result<(), CacheError> {
        match self {
            Self::For(value) if value.is_zero() => Err(CacheError::InvalidTtl),
            _ => Ok(()),
        }
    }
}

/// Boxed async result used to keep [`CacheProvider`] object-safe.
pub type CacheFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// One opaque cache write used by provider-level batch operations.
#[derive(Clone, Debug)]
pub struct CacheWrite {
    /// Canonical key prepared by the typed facade.
    pub key: CacheKey,
    /// Serialized opaque value bytes.
    pub value: Bytes,
    /// Requested cache lifetime.
    pub ttl: CacheTtl,
}

/// Async byte-storage provider for one configured cache namespace.
pub trait CacheProvider: Send + Sync + 'static {
    /// Validates source-owned provider configuration during site construction.
    fn validate(&self) -> Result<(), CacheError> {
        Ok(())
    }

    /// Creates fresh provider state for one built site when configuration reuse
    /// must not share mutable storage. Stateless and external providers use the
    /// default and are shared as configured.
    fn for_site(&self) -> Option<Arc<dyn CacheProvider>> {
        None
    }

    /// Reads one opaque entry.
    fn get<'a>(&'a self, key: &'a CacheKey) -> CacheFuture<'a, Result<Option<Bytes>, CacheError>>;

    /// Stores or replaces one opaque entry.
    fn set<'a>(
        &'a self,
        key: &'a CacheKey,
        value: Bytes,
        ttl: CacheTtl,
    ) -> CacheFuture<'a, Result<(), CacheError>>;

    /// Stores an entry only when the key is absent or expired.
    fn add<'a>(
        &'a self,
        key: &'a CacheKey,
        value: Bytes,
        ttl: CacheTtl,
    ) -> CacheFuture<'a, Result<bool, CacheError>>;

    /// Deletes one opaque entry.
    fn delete<'a>(&'a self, key: &'a CacheKey) -> CacheFuture<'a, Result<(), CacheError>>;

    /// Changes one existing entry's lifetime.
    fn touch<'a>(
        &'a self,
        key: &'a CacheKey,
        ttl: CacheTtl,
    ) -> CacheFuture<'a, Result<bool, CacheError>>;

    /// Adds a signed delta to one canonical integer entry atomically.
    fn increment<'a>(
        &'a self,
        key: &'a CacheKey,
        delta: i64,
    ) -> CacheFuture<'a, Result<i64, CacheError>>;

    /// Clears entries belonging to exactly one configured provider scope.
    fn clear<'a>(&'a self, scope: &'a CacheScope) -> CacheFuture<'a, Result<(), CacheError>>;

    /// Reads multiple keys in request order. The default is sequential and non-atomic.
    fn get_many<'a>(
        &'a self,
        keys: &'a [CacheKey],
    ) -> CacheFuture<'a, Result<Vec<Option<Bytes>>, CacheError>> {
        Box::pin(async move {
            let mut values = Vec::with_capacity(keys.len());
            for key in keys {
                values.push(self.get(key).await?);
            }
            Ok(values)
        })
    }

    /// Writes multiple keys. The default is sequential and may partially succeed.
    fn set_many<'a>(&'a self, values: &'a [CacheWrite]) -> CacheFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            for value in values {
                self.set(&value.key, value.value.clone(), value.ttl).await?;
            }
            Ok(())
        })
    }

    /// Deletes multiple keys. The default is sequential and may partially succeed.
    fn delete_many<'a>(&'a self, keys: &'a [CacheKey]) -> CacheFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            for key in keys {
                self.delete(key).await?;
            }
            Ok(())
        })
    }
}

impl<T> CacheProvider for std::sync::Arc<T>
where
    T: CacheProvider + ?Sized,
{
    fn validate(&self) -> Result<(), CacheError> {
        self.as_ref().validate()
    }

    fn for_site(&self) -> Option<Arc<dyn CacheProvider>> {
        self.as_ref().for_site()
    }

    fn get<'a>(&'a self, key: &'a CacheKey) -> CacheFuture<'a, Result<Option<Bytes>, CacheError>> {
        self.as_ref().get(key)
    }

    fn set<'a>(
        &'a self,
        key: &'a CacheKey,
        value: Bytes,
        ttl: CacheTtl,
    ) -> CacheFuture<'a, Result<(), CacheError>> {
        self.as_ref().set(key, value, ttl)
    }

    fn add<'a>(
        &'a self,
        key: &'a CacheKey,
        value: Bytes,
        ttl: CacheTtl,
    ) -> CacheFuture<'a, Result<bool, CacheError>> {
        self.as_ref().add(key, value, ttl)
    }

    fn delete<'a>(&'a self, key: &'a CacheKey) -> CacheFuture<'a, Result<(), CacheError>> {
        self.as_ref().delete(key)
    }

    fn touch<'a>(
        &'a self,
        key: &'a CacheKey,
        ttl: CacheTtl,
    ) -> CacheFuture<'a, Result<bool, CacheError>> {
        self.as_ref().touch(key, ttl)
    }

    fn increment<'a>(
        &'a self,
        key: &'a CacheKey,
        delta: i64,
    ) -> CacheFuture<'a, Result<i64, CacheError>> {
        self.as_ref().increment(key, delta)
    }

    fn clear<'a>(&'a self, scope: &'a CacheScope) -> CacheFuture<'a, Result<(), CacheError>> {
        self.as_ref().clear(scope)
    }

    fn get_many<'a>(
        &'a self,
        keys: &'a [CacheKey],
    ) -> CacheFuture<'a, Result<Vec<Option<Bytes>>, CacheError>> {
        self.as_ref().get_many(keys)
    }

    fn set_many<'a>(&'a self, values: &'a [CacheWrite]) -> CacheFuture<'a, Result<(), CacheError>> {
        self.as_ref().set_many(values)
    }

    fn delete_many<'a>(&'a self, keys: &'a [CacheKey]) -> CacheFuture<'a, Result<(), CacheError>> {
        self.as_ref().delete_many(keys)
    }
}
