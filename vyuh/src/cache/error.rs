//! Errors returned by cache configuration and terminal cache operations.

use std::error::Error as StdError;

use thiserror::Error;

/// A safe cache failure that never includes an application key or cached value.
#[derive(Debug, Error)]
pub enum CacheError {
    /// A configured cache name is empty or contains unsupported characters.
    #[error("invalid cache provider name")]
    InvalidProviderName,
    /// A named cache provider was selected but is not configured.
    #[error("cache provider is not configured")]
    ProviderNotFound,
    /// More than one provider was registered under the same name.
    #[error("cache provider is registered more than once")]
    DuplicateProvider,
    /// A configuration selected no default cache provider.
    #[error("cache configuration has no default provider")]
    MissingDefaultProvider,
    /// The configured default provider was not registered.
    #[error("configured default cache provider is not registered")]
    InvalidDefaultProvider,
    /// A namespace or application key is malformed or exceeds a configured bound.
    #[error("cache key is invalid")]
    InvalidKey,
    /// A bulk operation contains more entries than the configured safety limit.
    #[error("cache bulk operation exceeds the configured limit")]
    BulkLimitExceeded,
    /// A cache entry exceeds the configured provider limit.
    #[error("cache value exceeds the configured size limit")]
    ValueTooLarge,
    /// A configured cache duration must be positive.
    #[error("cache TTL must be positive")]
    InvalidTtl,
    /// A cached JSON value could not be encoded.
    #[error("cache value could not be serialized")]
    Serialize(#[source] serde_json::Error),
    /// A cached JSON value could not be decoded as the requested type.
    #[error("cached value has an incompatible format")]
    Deserialize(#[source] serde_json::Error),
    /// A numeric cache entry is missing or not stored in Vyuh's integer format.
    #[error("cached value is not a signed integer")]
    InvalidInteger,
    /// Incrementing or decrementing a stored integer overflowed.
    #[error("cached integer overflowed")]
    IntegerOverflow,
    /// A provider cannot perform the requested operation.
    #[error("cache provider does not support this operation")]
    UnsupportedOperation,
    /// A configured provider rejected its own startup configuration.
    #[error("invalid cache provider configuration")]
    InvalidProviderConfig,
    /// A provider failed without exposing sensitive key or value material.
    #[error("cache provider failed during {operation}")]
    Provider {
        /// Stable operation name for diagnostics.
        operation: &'static str,
    },
}

impl CacheError {
    /// Wraps a provider failure without retaining cache keys or values.
    pub fn provider<E>(operation: &'static str, _source: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Self::Provider { operation }
    }
}

/// Failure returned by [`crate::cache::Cache::get_or_try_set`].
#[derive(Debug, Error)]
pub enum CacheGetOrSetError<E> {
    /// The cache operation failed.
    #[error(transparent)]
    Cache(#[from] CacheError),
    /// The application initializer failed.
    #[error("cache initializer failed")]
    Initializer(#[source] E),
}

impl From<CacheGetOrSetError<crate::Error>> for crate::Error {
    fn from(error: CacheGetOrSetError<crate::Error>) -> Self {
        match error {
            CacheGetOrSetError::Cache(error) => Self::from(error),
            CacheGetOrSetError::Initializer(error) => error,
        }
    }
}

impl From<CacheError> for crate::Error {
    fn from(error: CacheError) -> Self {
        Self::other(error)
    }
}
