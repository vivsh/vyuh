//! Typed cache facades backed by the immutable per-site provider registry.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::Arc,
    time::Instant,
};

use bytes::Bytes;
use indexmap::IndexMap;
use serde::{Serialize, de::DeserializeOwned};

use super::{
    CacheConf, CacheError, CacheGetOrSetError, CacheName, CacheProvider, CacheTtl,
    key::{CacheKey, MAX_BULK_ITEMS, cache_key, cache_scope},
    metrics::{CacheMetrics, CacheOperation},
    provider::CacheWrite,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CacheId(&'static str);

impl CacheId {
    const fn declared(value: CacheName) -> Self {
        Self(value.as_str())
    }
}

/// Immutable runtime registry built once for each site.
pub(crate) struct CacheRegistry {
    providers: BTreeMap<CacheId, Arc<dyn CacheProvider>>,
    default: CacheName,
    metrics: CacheMetrics,
}

impl CacheRegistry {
    pub(crate) fn build(conf: &CacheConf) -> Result<Self, CacheError> {
        conf.validate()?;
        let mut providers = BTreeMap::new();
        for definition in conf.definitions() {
            let provider = definition
                .provider
                .as_ref()
                .for_site()
                .unwrap_or_else(|| definition.provider.clone());
            provider.validate()?;
            providers.insert(CacheId::declared(definition.name), provider);
        }
        let names = conf
            .definitions()
            .iter()
            .map(|definition| definition.name.as_str().to_owned());
        Ok(Self {
            providers,
            default: conf
                .default_name()
                .ok_or(CacheError::MissingDefaultProvider)?,
            metrics: CacheMetrics::new(names),
        })
    }

    fn provider(&self, name: CacheName) -> Result<&Arc<dyn CacheProvider>, CacheError> {
        self.providers
            .get(&CacheId::declared(name))
            .ok_or(CacheError::ProviderNotFound)
    }

    pub(crate) fn default_handle(self: &Arc<Self>) -> Cache {
        Cache::new(self.clone(), self.default)
    }

    pub(crate) fn render_metrics(&self) -> String {
        self.metrics.render()
    }
}

/// A typed async cache handle selected from one site-owned provider registry.
#[derive(Clone)]
pub struct Cache {
    registry: Arc<CacheRegistry>,
    name: CacheName,
    namespaces: Arc<Vec<String>>,
}

/// A typed async cache handle scoped beneath one or more runtime namespaces.
#[derive(Clone)]
pub struct NamespacedCache {
    cache: Cache,
}

impl Cache {
    pub(crate) fn new(registry: Arc<CacheRegistry>, name: CacheName) -> Self {
        Self {
            registry,
            name,
            namespaces: Arc::new(Vec::new()),
        }
    }

    /// Selects another configured cache provider. Validation happens at the terminal call.
    pub fn using(mut self, name: CacheName) -> Self {
        self.name = name;
        self
    }

    /// Adds one composable namespace segment to this handle.
    pub fn namespace(self, value: impl Into<String>) -> NamespacedCache {
        NamespacedCache {
            cache: self.with_namespace(value.into()),
        }
    }

    /// Reads and deserializes one JSON value.
    pub async fn get<T: DeserializeOwned>(
        &self,
        key: impl AsRef<str>,
    ) -> Result<Option<T>, CacheError> {
        let key = self.key(key.as_ref())?;
        let provider = self.provider()?;
        let value = self.get_bytes(provider, &key, CacheOperation::Get).await?;
        value.as_deref().map(decode).transpose()
    }

    /// Serializes and stores one JSON value.
    pub async fn set<T: Serialize>(
        &self,
        key: impl AsRef<str>,
        value: &T,
        ttl: CacheTtl,
    ) -> Result<(), CacheError> {
        ttl.validate()?;
        let key = self.key(key.as_ref())?;
        let value = encode(value)?;
        let provider = self.provider()?;
        self.execute(CacheOperation::Set, provider.set(&key, value, ttl))
            .await
    }

    /// Stores a JSON value only when no live entry already exists.
    pub async fn add<T: Serialize>(
        &self,
        key: impl AsRef<str>,
        value: &T,
        ttl: CacheTtl,
    ) -> Result<bool, CacheError> {
        ttl.validate()?;
        let key = self.key(key.as_ref())?;
        let value = encode(value)?;
        let provider = self.provider()?;
        self.execute(CacheOperation::Add, provider.add(&key, value, ttl))
            .await
    }

    /// Reads requested keys in order, omitting cache misses from the returned map.
    pub async fn get_many<T, I, K>(&self, keys: I) -> Result<IndexMap<String, T>, CacheError>
    where
        T: DeserializeOwned,
        I: IntoIterator<Item = K>,
        K: AsRef<str>,
    {
        let pairs = self.keys(keys)?;
        let values = pairs.iter().map(|(_, key)| key.clone()).collect::<Vec<_>>();
        let provider = self.provider()?;
        let values = self
            .execute(CacheOperation::GetMany, provider.get_many(&values))
            .await?;
        self.decode_many(pairs, values)
    }

    /// Serializes and stores multiple values through the selected provider.
    pub async fn set_many<T, I, K>(&self, values: I, ttl: CacheTtl) -> Result<(), CacheError>
    where
        T: Serialize,
        I: IntoIterator<Item = (K, T)>,
        K: AsRef<str>,
    {
        ttl.validate()?;
        let writes = self.writes(values, ttl)?;
        let provider = self.provider()?;
        self.execute(CacheOperation::SetMany, provider.set_many(&writes))
            .await
    }

    /// Deletes one key. Missing keys are treated as successfully absent.
    pub async fn delete(&self, key: impl AsRef<str>) -> Result<(), CacheError> {
        let key = self.key(key.as_ref())?;
        let provider = self.provider()?;
        self.execute(CacheOperation::Delete, provider.delete(&key))
            .await
    }

    /// Deletes multiple keys through the selected provider.
    pub async fn delete_many<I, K>(&self, keys: I) -> Result<(), CacheError>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<str>,
    {
        let keys = self
            .keys(keys)?
            .into_iter()
            .map(|(_, key)| key)
            .collect::<Vec<_>>();
        let provider = self.provider()?;
        self.execute(CacheOperation::DeleteMany, provider.delete_many(&keys))
            .await
    }

    /// Changes an existing entry's lifetime and returns whether it existed.
    pub async fn touch(&self, key: impl AsRef<str>, ttl: CacheTtl) -> Result<bool, CacheError> {
        ttl.validate()?;
        let key = self.key(key.as_ref())?;
        let provider = self.provider()?;
        self.execute(CacheOperation::Touch, provider.touch(&key, ttl))
            .await
    }

    /// Atomically adds a signed delta to one canonical integer entry.
    pub async fn increment(&self, key: impl AsRef<str>, delta: i64) -> Result<i64, CacheError> {
        let key = self.key(key.as_ref())?;
        let provider = self.provider()?;
        self.execute(CacheOperation::Increment, provider.increment(&key, delta))
            .await
    }

    /// Atomically subtracts a signed amount from one canonical integer entry.
    pub async fn decrement(&self, key: impl AsRef<str>, amount: i64) -> Result<i64, CacheError> {
        let delta = amount.checked_neg().ok_or(CacheError::IntegerOverflow)?;
        self.increment(key, delta).await
    }

    /// Reads a value or computes and attempts to store it without serializing concurrent work.
    pub async fn get_or_try_set<T, E, F, Fut>(
        &self,
        key: impl AsRef<str>,
        ttl: CacheTtl,
        initializer: F,
    ) -> Result<T, CacheGetOrSetError<E>>
    where
        T: Serialize + DeserializeOwned,
        E: std::error::Error + Send + Sync + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if let Some(value) = self.get(key.as_ref()).await? {
            return Ok(value);
        }
        let value = initializer()
            .await
            .map_err(CacheGetOrSetError::Initializer)?;
        if self.add(key.as_ref(), &value, ttl).await? {
            return Ok(value);
        }
        Ok(self.get(key.as_ref()).await?.unwrap_or(value))
    }

    /// Clears all entries in the selected provider. Namespaced handles intentionally omit this operation.
    pub async fn clear(&self) -> Result<(), CacheError> {
        if !self.namespaces.is_empty() {
            return Err(CacheError::UnsupportedOperation);
        }
        let provider = self.provider()?;
        let scope = cache_scope(self.name);
        self.execute(CacheOperation::Clear, provider.clear(&scope))
            .await
    }

    fn with_namespace(&self, value: String) -> Self {
        let mut namespaces = self.namespaces.as_ref().clone();
        namespaces.push(value);
        Self {
            registry: self.registry.clone(),
            name: self.name,
            namespaces: Arc::new(namespaces),
        }
    }

    fn key(&self, key: &str) -> Result<CacheKey, CacheError> {
        cache_key(self.name, &self.namespaces, key)
    }

    fn provider(&self) -> Result<&Arc<dyn CacheProvider>, CacheError> {
        self.registry.provider(self.name)
    }

    fn keys<I, K>(&self, values: I) -> Result<Vec<(String, CacheKey)>, CacheError>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<str>,
    {
        let values = values
            .into_iter()
            .map(|value| value.as_ref().to_owned())
            .collect::<Vec<_>>();
        if values.len() > MAX_BULK_ITEMS {
            return Err(CacheError::BulkLimitExceeded);
        }
        let mut seen = BTreeSet::new();
        let mut keys = Vec::with_capacity(values.len());
        for value in values {
            if !seen.insert(value.clone()) {
                return Err(CacheError::InvalidKey);
            }
            keys.push((value.clone(), self.key(&value)?));
        }
        Ok(keys)
    }

    fn writes<T, I, K>(&self, values: I, ttl: CacheTtl) -> Result<Vec<CacheWrite>, CacheError>
    where
        T: Serialize,
        I: IntoIterator<Item = (K, T)>,
        K: AsRef<str>,
    {
        let values = values.into_iter().collect::<Vec<_>>();
        if values.len() > MAX_BULK_ITEMS {
            return Err(CacheError::BulkLimitExceeded);
        }
        let mut seen = BTreeSet::new();
        values
            .into_iter()
            .map(|(key, value)| {
                let key = key.as_ref().to_owned();
                if !seen.insert(key.clone()) {
                    return Err(CacheError::InvalidKey);
                }
                Ok(CacheWrite {
                    key: self.key(&key)?,
                    value: encode(&value)?,
                    ttl,
                })
            })
            .collect()
    }

    async fn get_bytes(
        &self,
        provider: &Arc<dyn CacheProvider>,
        key: &CacheKey,
        operation: CacheOperation,
    ) -> Result<Option<Bytes>, CacheError> {
        let started = Instant::now();
        let result = provider.get(key).await;
        self.registry.metrics.record(
            self.name.as_str(),
            operation,
            result.is_ok(),
            result.as_ref().ok().map(Option::is_some),
            started.elapsed(),
        );
        result
    }

    async fn execute<T>(
        &self,
        operation: CacheOperation,
        future: super::provider::CacheFuture<'_, Result<T, CacheError>>,
    ) -> Result<T, CacheError> {
        let started = Instant::now();
        let result = future.await;
        self.registry.metrics.record(
            self.name.as_str(),
            operation,
            result.is_ok(),
            None,
            started.elapsed(),
        );
        result
    }

    fn decode_many<T>(
        &self,
        keys: Vec<(String, CacheKey)>,
        values: Vec<Option<Bytes>>,
    ) -> Result<IndexMap<String, T>, CacheError>
    where
        T: DeserializeOwned,
    {
        if keys.len() != values.len() {
            return Err(CacheError::InvalidProviderConfig);
        }
        keys.into_iter()
            .zip(values)
            .filter_map(|((name, _), value)| {
                value.map(|value| decode(&value).map(|value| (name, value)))
            })
            .collect()
    }
}

impl NamespacedCache {
    /// Adds one further namespace segment without affecting the provider selection.
    pub fn namespace(mut self, value: impl Into<String>) -> Self {
        self.cache = self.cache.with_namespace(value.into());
        self
    }

    /// Reads and deserializes one JSON value.
    pub async fn get<T: DeserializeOwned>(
        &self,
        key: impl AsRef<str>,
    ) -> Result<Option<T>, CacheError> {
        self.cache.get(key).await
    }
    /// Serializes and stores one JSON value.
    pub async fn set<T: Serialize>(
        &self,
        key: impl AsRef<str>,
        value: &T,
        ttl: CacheTtl,
    ) -> Result<(), CacheError> {
        self.cache.set(key, value, ttl).await
    }
    /// Stores a JSON value only when no live entry exists.
    pub async fn add<T: Serialize>(
        &self,
        key: impl AsRef<str>,
        value: &T,
        ttl: CacheTtl,
    ) -> Result<bool, CacheError> {
        self.cache.add(key, value, ttl).await
    }
    /// Reads requested keys in order, omitting misses.
    pub async fn get_many<T, I, K>(&self, keys: I) -> Result<IndexMap<String, T>, CacheError>
    where
        T: DeserializeOwned,
        I: IntoIterator<Item = K>,
        K: AsRef<str>,
    {
        self.cache.get_many(keys).await
    }
    /// Serializes and stores multiple values.
    pub async fn set_many<T, I, K>(&self, values: I, ttl: CacheTtl) -> Result<(), CacheError>
    where
        T: Serialize,
        I: IntoIterator<Item = (K, T)>,
        K: AsRef<str>,
    {
        self.cache.set_many(values, ttl).await
    }
    /// Deletes one cache key.
    pub async fn delete(&self, key: impl AsRef<str>) -> Result<(), CacheError> {
        self.cache.delete(key).await
    }
    /// Deletes multiple cache keys.
    pub async fn delete_many<I, K>(&self, keys: I) -> Result<(), CacheError>
    where
        I: IntoIterator<Item = K>,
        K: AsRef<str>,
    {
        self.cache.delete_many(keys).await
    }
    /// Changes one entry lifetime.
    pub async fn touch(&self, key: impl AsRef<str>, ttl: CacheTtl) -> Result<bool, CacheError> {
        self.cache.touch(key, ttl).await
    }
    /// Atomically adds a signed delta.
    pub async fn increment(&self, key: impl AsRef<str>, delta: i64) -> Result<i64, CacheError> {
        self.cache.increment(key, delta).await
    }
    /// Atomically subtracts a signed amount.
    pub async fn decrement(&self, key: impl AsRef<str>, amount: i64) -> Result<i64, CacheError> {
        self.cache.decrement(key, amount).await
    }
    /// Reads a value or computes and attempts to store it.
    pub async fn get_or_try_set<T, E, F, Fut>(
        &self,
        key: impl AsRef<str>,
        ttl: CacheTtl,
        initializer: F,
    ) -> Result<T, CacheGetOrSetError<E>>
    where
        T: Serialize + DeserializeOwned,
        E: std::error::Error + Send + Sync + 'static,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        self.cache.get_or_try_set(key, ttl, initializer).await
    }
}

fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Bytes, CacheError> {
    serde_json::to_vec(value)
        .map(Bytes::from)
        .map_err(CacheError::Serialize)
}
fn decode<T: DeserializeOwned>(value: &[u8]) -> Result<T, CacheError> {
    serde_json::from_slice(value).map_err(CacheError::Deserialize)
}
