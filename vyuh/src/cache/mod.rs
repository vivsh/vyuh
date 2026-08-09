//! Async, site-owned cache providers and typed cache handles.

mod config;
mod error;
mod key;
mod memory;
pub(crate) mod memory_store;
mod metrics;
mod provider;
mod runtime;

pub use config::{CacheConf, CacheName, DEFAULT_CACHE};
pub use error::{CacheError, CacheGetOrSetError};
pub use key::{CacheKey, CacheScope};
pub use memory::MemoryCache;
pub use provider::{CacheProvider, CacheTtl, CacheWrite};
pub use runtime::{Cache, NamespacedCache};

pub(crate) use runtime::CacheRegistry;

#[cfg(test)]
mod tests;
