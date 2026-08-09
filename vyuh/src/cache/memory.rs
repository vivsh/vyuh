//! Bounded per-site in-process cache provider.

use std::time::{Duration, Instant};

use bytes::Bytes;

use super::{
    CacheError, CacheProvider, CacheTtl,
    key::{CacheKey, CacheScope},
    memory_store::{MemoryAction, MemoryLimits, MemoryStore},
    provider::CacheFuture,
};

const DEFAULT_MAX_ENTRIES: usize = 1_024;
const DEFAULT_MAX_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_ENTRY_BYTES: usize = 1024 * 1024;
const DEFAULT_TTL: Duration = Duration::from_secs(300);

/// A bounded LRU cache that lives only inside one built site.
pub struct MemoryCache {
    limits: MemoryCacheLimits,
    store: MemoryStore<MemoryEntry>,
}

impl Clone for MemoryCache {
    fn clone(&self) -> Self {
        Self {
            limits: self.limits,
            store: MemoryStore::new(self.limits.store_limits()),
        }
    }
}

#[derive(Clone, Copy)]
struct MemoryCacheLimits {
    max_entries: usize,
    max_bytes: usize,
    max_entry_bytes: usize,
    default_ttl: Duration,
}

#[derive(Clone)]
struct MemoryEntry {
    value: Bytes,
    expires_at: Option<Instant>,
}

impl Default for MemoryCache {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryCache {
    /// Creates a memory provider with Vyuh's bounded default limits.
    pub fn new() -> Self {
        let limits = MemoryCacheLimits {
            max_entries: DEFAULT_MAX_ENTRIES,
            max_bytes: DEFAULT_MAX_BYTES,
            max_entry_bytes: DEFAULT_MAX_ENTRY_BYTES,
            default_ttl: DEFAULT_TTL,
        };
        Self {
            store: MemoryStore::new(limits.store_limits()),
            limits,
        }
    }

    /// Sets the maximum number of retained entries.
    pub fn max_entries(mut self, value: usize) -> Self {
        self.limits.max_entries = value;
        self.update_limits();
        self
    }

    /// Sets the maximum retained value bytes across all entries.
    pub fn max_bytes(mut self, value: usize) -> Self {
        self.limits.max_bytes = value;
        self.update_limits();
        self
    }

    /// Sets the largest value accepted for one entry.
    pub fn max_entry_bytes(mut self, value: usize) -> Self {
        self.limits.max_entry_bytes = value;
        self
    }

    /// Sets the lifetime used by [`CacheTtl::Default`].
    pub fn default_ttl(mut self, value: Duration) -> Self {
        self.limits.default_ttl = value;
        self
    }

    fn expiry(&self, ttl: CacheTtl) -> Result<Option<Instant>, CacheError> {
        ttl.validate()?;
        let ttl = match ttl {
            CacheTtl::Default => self.limits.default_ttl,
            CacheTtl::For(value) => value,
            CacheTtl::Forever => return Ok(None),
        };
        Instant::now()
            .checked_add(ttl)
            .ok_or(CacheError::InvalidTtl)
            .map(Some)
    }

    fn validate_value(&self, value: &Bytes) -> Result<(), CacheError> {
        (value.len() <= self.limits.max_entry_bytes)
            .then_some(())
            .ok_or(CacheError::ValueTooLarge)
    }

    fn update_limits(&self) {
        self.store.set_limits(self.limits.store_limits());
    }

    /// Reads a non-expired value without exposing cache state outside its lock.
    fn get_value(&self, key: &str) -> Option<Bytes> {
        let now = Instant::now();
        self.store.access(key, |entry| match entry {
            Some(entry) if !entry.expired(now) => MemoryAction::Keep(Some(entry.value.clone())),
            Some(_) => MemoryAction::Remove(None),
            None => MemoryAction::Keep(None),
        })
    }

    /// Inserts only when no unexpired entry owns the key.
    fn add_value(&self, key: &str, value: Bytes, expiry: Option<Instant>) -> bool {
        let now = Instant::now();
        self.store.access(key, |entry| match entry {
            Some(entry) if !entry.expired(now) => MemoryAction::Keep(false),
            _ => MemoryAction::Replace {
                bytes: value.len(),
                value: MemoryEntry {
                    value,
                    expires_at: expiry,
                },
                result: true,
            },
        })
    }

    /// Updates expiry only when the entry remains valid.
    fn touch_value(&self, key: &str, expiry: Option<Instant>) -> bool {
        let now = Instant::now();
        self.store.access(key, |entry| match entry {
            Some(entry) if !entry.expired(now) => MemoryAction::Replace {
                bytes: entry.value.len(),
                value: MemoryEntry {
                    value: entry.value.clone(),
                    expires_at: expiry,
                },
                result: true,
            },
            Some(_) => MemoryAction::Remove(false),
            None => MemoryAction::Keep(false),
        })
    }

    /// Atomically updates the canonical signed-integer representation.
    fn increment_value(
        &self,
        key: &str,
        delta: i64,
        default_expiry: Option<Instant>,
    ) -> Result<i64, CacheError> {
        let now = Instant::now();
        self.store.access(key, |entry| match entry {
            Some(entry) if !entry.expired(now) => Self::increment_entry(entry, delta),
            _ => Self::new_integer(delta, default_expiry),
        })
    }

    /// Produces a replacement entry for a parsed integer value.
    fn increment_entry(
        entry: &MemoryEntry,
        delta: i64,
    ) -> MemoryAction<MemoryEntry, Result<i64, CacheError>> {
        let value = match Self::read_integer(entry) {
            Ok(value) => value,
            Err(error) => return MemoryAction::Keep(Err(error)),
        };
        let value = match value.checked_add(delta) {
            Some(value) => value,
            None => return MemoryAction::Keep(Err(CacheError::IntegerOverflow)),
        };
        Self::replace_integer(value, entry.expires_at)
    }

    /// Produces an initial integer entry using the provider default expiry.
    fn new_integer(
        value: i64,
        expiry: Option<Instant>,
    ) -> MemoryAction<MemoryEntry, Result<i64, CacheError>> {
        Self::replace_integer(value, expiry)
    }

    /// Serializes an integer replacement in the common cache representation.
    fn replace_integer(
        value: i64,
        expiry: Option<Instant>,
    ) -> MemoryAction<MemoryEntry, Result<i64, CacheError>> {
        let bytes = Bytes::copy_from_slice(&value.to_be_bytes());
        MemoryAction::Replace {
            bytes: bytes.len(),
            value: MemoryEntry {
                value: bytes,
                expires_at: expiry,
            },
            result: Ok(value),
        }
    }

    /// Decodes the cache's fixed-width signed integer representation.
    fn read_integer(entry: &MemoryEntry) -> Result<i64, CacheError> {
        let bytes: [u8; 8] = entry
            .value
            .as_ref()
            .try_into()
            .map_err(|_| CacheError::InvalidInteger)?;
        Ok(i64::from_be_bytes(bytes))
    }
}

impl MemoryCacheLimits {
    /// Converts provider limits into the shared store's retention limits.
    fn store_limits(self) -> MemoryLimits {
        MemoryLimits::new(self.max_entries, Some(self.max_bytes))
    }
}

impl MemoryEntry {
    /// Reports whether this value has reached its optional expiry boundary.
    fn expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|expiry| expiry <= now)
    }
}

impl CacheProvider for MemoryCache {
    fn validate(&self) -> Result<(), CacheError> {
        let limits = self.limits;
        let valid = limits.max_entries > 0
            && limits.max_bytes > 0
            && limits.max_entry_bytes > 0
            && limits.max_entry_bytes <= limits.max_bytes
            && !limits.default_ttl.is_zero();
        valid.then_some(()).ok_or(CacheError::InvalidProviderConfig)
    }

    fn for_site(&self) -> Option<std::sync::Arc<dyn CacheProvider>> {
        Some(std::sync::Arc::new(self.clone()))
    }

    fn get<'a>(&'a self, key: &'a CacheKey) -> CacheFuture<'a, Result<Option<Bytes>, CacheError>> {
        Box::pin(async move { Ok(self.get_value(key.as_str())) })
    }

    fn set<'a>(
        &'a self,
        key: &'a CacheKey,
        value: Bytes,
        ttl: CacheTtl,
    ) -> CacheFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            self.validate_value(&value)?;
            let expiry = self.expiry(ttl)?;
            let bytes = value.len();
            self.store.insert(
                key.as_str(),
                MemoryEntry {
                    value,
                    expires_at: expiry,
                },
                bytes,
            );
            Ok(())
        })
    }

    fn add<'a>(
        &'a self,
        key: &'a CacheKey,
        value: Bytes,
        ttl: CacheTtl,
    ) -> CacheFuture<'a, Result<bool, CacheError>> {
        Box::pin(async move {
            self.validate_value(&value)?;
            let expiry = self.expiry(ttl)?;
            Ok(self.add_value(key.as_str(), value, expiry))
        })
    }

    fn delete<'a>(&'a self, key: &'a CacheKey) -> CacheFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            self.store.remove(key.as_str());
            Ok(())
        })
    }

    fn touch<'a>(
        &'a self,
        key: &'a CacheKey,
        ttl: CacheTtl,
    ) -> CacheFuture<'a, Result<bool, CacheError>> {
        Box::pin(async move {
            let expiry = self.expiry(ttl)?;
            Ok(self.touch_value(key.as_str(), expiry))
        })
    }

    fn increment<'a>(
        &'a self,
        key: &'a CacheKey,
        delta: i64,
    ) -> CacheFuture<'a, Result<i64, CacheError>> {
        Box::pin(async move {
            let expiry = self.expiry(CacheTtl::Default)?;
            self.increment_value(key.as_str(), delta, expiry)
        })
    }

    fn clear<'a>(&'a self, scope: &'a CacheScope) -> CacheFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            self.store.retain(|key| !key.starts_with(scope.prefix()));
            Ok(())
        })
    }
}
