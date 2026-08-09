use std::{
    collections::HashMap,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use bytes::Bytes;
use parking_lot::Mutex;

use super::{
    CacheConf, CacheError, CacheKey, CacheName, CacheProvider, CacheTtl, CacheWrite, MemoryCache,
    key::CacheScope, provider::CacheFuture, runtime::CacheRegistry,
};
use crate::{Site, SiteConf, bundles::Bundle};

const SECOND: CacheName = CacheName::new("second");

fn cache(conf: CacheConf) -> super::Cache {
    let registry = CacheRegistry::build(&conf).expect("test cache configuration must be valid");
    std::sync::Arc::new(registry).default_handle()
}

/// Verifies each built site receives an independent default memory-cache runtime.
#[tokio::test]
async fn sites_own_independent_default_memory_caches() {
    let conf = SiteConf::default().log_init(false);
    let first = Site::build(conf.clone(), Bundle::default()).await.unwrap();
    let second = Site::build(conf, Bundle::default()).await.unwrap();
    first
        .cache()
        .set("site-only", &1_u8, CacheTtl::Default)
        .await
        .unwrap();

    assert_eq!(first.cache().get::<u8>("site-only").await.unwrap(), Some(1));
    assert_eq!(second.cache().get::<u8>("site-only").await.unwrap(), None);
}

/// Verifies invalid provider registrations and missing default selection fail before runtime use.
#[test]
fn cache_configuration_requires_one_valid_default_provider() {
    assert!(matches!(
        CacheRegistry::build(&CacheConf::empty()),
        Err(CacheError::MissingDefaultProvider)
    ));
    let invalid = CacheConf::empty()
        .provider(CacheName::new("invalid name"), MemoryCache::new())
        .default_provider(CacheName::new("invalid name"));
    assert!(matches!(
        CacheRegistry::build(&invalid),
        Err(CacheError::InvalidProviderName)
    ));
}

/// Verifies typed values, selected providers, and namespace scopes remain isolated.
#[tokio::test]
async fn typed_handles_isolate_provider_and_namespace_values() {
    let cache = cache(
        CacheConf::empty()
            .provider(super::DEFAULT_CACHE, MemoryCache::new())
            .provider(SECOND, MemoryCache::new())
            .default_provider(super::DEFAULT_CACHE),
    );
    cache
        .set("product", &7_u32, CacheTtl::Default)
        .await
        .unwrap();
    cache
        .clone()
        .using(SECOND)
        .set("product", &9_u32, CacheTtl::Default)
        .await
        .unwrap();
    let tenant = cache.clone().namespace("tenant:42");
    tenant
        .set("product", &11_u32, CacheTtl::Default)
        .await
        .unwrap();

    assert_eq!(cache.get::<u32>("product").await.unwrap(), Some(7));
    assert_eq!(
        cache.using(SECOND).get::<u32>("product").await.unwrap(),
        Some(9)
    );
    assert_eq!(tenant.get::<u32>("product").await.unwrap(), Some(11));
}

/// Verifies the memory provider applies expiry, bounded LRU eviction, and atomic integer storage.
#[tokio::test]
async fn memory_cache_expires_evicts_and_increments() {
    let cache = cache(
        CacheConf::empty()
            .provider(super::DEFAULT_CACHE, MemoryCache::new().max_entries(1))
            .default_provider(super::DEFAULT_CACHE),
    );
    cache
        .set(
            "short",
            &"value",
            CacheTtl::for_duration(Duration::from_millis(1)),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(cache.get::<String>("short").await.unwrap(), None);

    cache.set("first", &1_u8, CacheTtl::Forever).await.unwrap();
    cache.set("second", &2_u8, CacheTtl::Forever).await.unwrap();
    assert_eq!(cache.get::<u8>("first").await.unwrap(), None);
    assert_eq!(cache.increment("counter", 4).await.unwrap(), 4);
    assert_eq!(cache.decrement("counter", 1).await.unwrap(), 3);
}

/// Verifies a read keeps an entry ahead of an older entry in LRU eviction.
#[tokio::test]
async fn memory_cache_evicts_least_recently_used_entry() {
    let cache = cache(
        CacheConf::empty()
            .provider(super::DEFAULT_CACHE, MemoryCache::new().max_entries(2))
            .default_provider(super::DEFAULT_CACHE),
    );
    cache.set("one", &1_u8, CacheTtl::Forever).await.unwrap();
    cache.set("two", &2_u8, CacheTtl::Forever).await.unwrap();
    assert_eq!(cache.get::<u8>("one").await.unwrap(), Some(1));
    cache.set("three", &3_u8, CacheTtl::Forever).await.unwrap();

    assert_eq!(cache.get::<u8>("one").await.unwrap(), Some(1));
    assert_eq!(cache.get::<u8>("two").await.unwrap(), None);
    assert_eq!(cache.get::<u8>("three").await.unwrap(), Some(3));
}

/// Verifies bulk reads preserve request order while omitting misses.
#[tokio::test]
async fn bulk_reads_preserve_order_and_omit_misses() {
    let cache = cache(CacheConf::default());
    cache.set("first", &1_u8, CacheTtl::Default).await.unwrap();
    cache.set("third", &3_u8, CacheTtl::Default).await.unwrap();
    let values = cache
        .get_many::<u8, _, _>(["third", "missing", "first"])
        .await
        .unwrap();
    assert_eq!(values.keys().collect::<Vec<_>>(), vec!["third", "first"]);
    assert_eq!(values.values().copied().collect::<Vec<_>>(), vec![3, 1]);
}

/// Verifies provider-wide clearing reaches its namespaces but never another selected provider.
#[tokio::test]
async fn clear_is_limited_to_the_selected_provider() {
    let cache = cache(
        CacheConf::empty()
            .provider(super::DEFAULT_CACHE, MemoryCache::new())
            .provider(SECOND, MemoryCache::new())
            .default_provider(super::DEFAULT_CACHE),
    );
    cache
        .clone()
        .namespace("tenant")
        .set("one", &1_u8, CacheTtl::Forever)
        .await
        .unwrap();
    cache
        .clone()
        .using(SECOND)
        .set("one", &2_u8, CacheTtl::Forever)
        .await
        .unwrap();
    cache.clear().await.unwrap();

    assert_eq!(
        cache
            .clone()
            .namespace("tenant")
            .get::<u8>("one")
            .await
            .unwrap(),
        None
    );
    assert_eq!(cache.using(SECOND).get::<u8>("one").await.unwrap(), Some(2));
}

/// Verifies concurrent cache misses may compute independently without a local cache lock.
#[tokio::test]
async fn get_or_try_set_permits_concurrent_initializers() {
    let cache = cache(CacheConf::default());
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let calls = std::sync::Arc::new(AtomicUsize::new(0));
    let left_cache = cache.clone();
    let left_barrier = barrier.clone();
    let left_calls = calls.clone();
    let right_barrier = barrier.clone();
    let right_calls = calls.clone();
    let (left, right) = tokio::join!(
        left_cache.get_or_try_set("race", CacheTtl::Default, move || async move {
            left_calls.fetch_add(1, Ordering::Relaxed);
            left_barrier.wait().await;
            Ok::<_, std::io::Error>(7_u8)
        }),
        cache.get_or_try_set("race", CacheTtl::Default, move || async move {
            right_calls.fetch_add(1, Ordering::Relaxed);
            right_barrier.wait().await;
            Ok::<_, std::io::Error>(7_u8)
        }),
    );
    assert_eq!(left.unwrap(), 7);
    assert_eq!(right.unwrap(), 7);
    assert_eq!(calls.load(Ordering::Relaxed), 2);
}

/// Verifies cache metrics use configured provider labels and never include application keys.
#[tokio::test]
async fn cache_metrics_are_bounded_by_configured_provider_names() {
    let registry = std::sync::Arc::new(CacheRegistry::build(&CacheConf::default()).unwrap());
    let cache = registry.default_handle();
    let _ = cache.get::<u8>("customer:42:secret").await.unwrap();
    let metrics = registry.render_metrics();

    assert!(metrics.contains("cache=\"default\""));
    assert!(!metrics.contains("customer:42:secret"));
}

/// Verifies the default provider batch fallback remains sequential and can partially write.
#[tokio::test]
async fn default_batch_write_can_partially_succeed() {
    let provider = PartialProvider::default();
    let observed = provider.clone();
    let cache = cache(
        CacheConf::empty()
            .provider(super::DEFAULT_CACHE, provider)
            .default_provider(super::DEFAULT_CACHE),
    );
    let result = cache
        .set_many::<u8, _, _>([("one", 1_u8), ("two", 2_u8)], CacheTtl::Default)
        .await;
    assert!(matches!(result, Err(CacheError::UnsupportedOperation)));
    assert_eq!(observed.writes.load(Ordering::Relaxed), 2);
    assert_eq!(cache.get::<u8>("one").await.unwrap(), Some(1));
}

/// Verifies a provider can replace the sequential fallback with one optimized bulk operation.
#[tokio::test]
async fn providers_can_override_bulk_operations() {
    let provider = BulkProvider::default();
    let observed = provider.clone();
    let cache = cache(
        CacheConf::empty()
            .provider(super::DEFAULT_CACHE, provider)
            .default_provider(super::DEFAULT_CACHE),
    );
    cache
        .set_many::<u8, _, _>([("one", 1_u8), ("two", 2_u8)], CacheTtl::Default)
        .await
        .unwrap();
    assert_eq!(observed.bulk_writes.load(Ordering::Relaxed), 1);
    assert_eq!(observed.writes.load(Ordering::Relaxed), 0);
}

#[derive(Clone, Default)]
struct PartialProvider {
    entries: std::sync::Arc<Mutex<HashMap<String, Bytes>>>,
    writes: std::sync::Arc<AtomicUsize>,
}

impl CacheProvider for PartialProvider {
    fn get<'a>(&'a self, key: &'a CacheKey) -> CacheFuture<'a, Result<Option<Bytes>, CacheError>> {
        Box::pin(async move { Ok(self.entries.lock().get(key.as_str()).cloned()) })
    }

    fn set<'a>(
        &'a self,
        key: &'a CacheKey,
        value: Bytes,
        _: CacheTtl,
    ) -> CacheFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            let attempt = self.writes.fetch_add(1, Ordering::Relaxed);
            if attempt == 1 {
                return Err(CacheError::UnsupportedOperation);
            }
            self.entries.lock().insert(key.as_str().into(), value);
            Ok(())
        })
    }

    fn add<'a>(
        &'a self,
        key: &'a CacheKey,
        value: Bytes,
        ttl: CacheTtl,
    ) -> CacheFuture<'a, Result<bool, CacheError>> {
        Box::pin(async move { self.set(key, value, ttl).await.map(|_| true) })
    }

    fn delete<'a>(&'a self, key: &'a CacheKey) -> CacheFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            self.entries.lock().remove(key.as_str());
            Ok(())
        })
    }

    fn touch<'a>(
        &'a self,
        _: &'a CacheKey,
        _: CacheTtl,
    ) -> CacheFuture<'a, Result<bool, CacheError>> {
        Box::pin(async { Ok(false) })
    }

    fn increment<'a>(
        &'a self,
        _: &'a CacheKey,
        _: i64,
    ) -> CacheFuture<'a, Result<i64, CacheError>> {
        Box::pin(async { Err(CacheError::UnsupportedOperation) })
    }

    fn clear<'a>(&'a self, scope: &'a CacheScope) -> CacheFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            self.entries
                .lock()
                .retain(|key, _| !key.starts_with(scope.prefix()));
            Ok(())
        })
    }
}

#[derive(Clone, Default)]
struct BulkProvider {
    inner: PartialProvider,
    bulk_writes: std::sync::Arc<AtomicUsize>,
    writes: std::sync::Arc<AtomicUsize>,
}

impl CacheProvider for BulkProvider {
    fn get<'a>(&'a self, key: &'a CacheKey) -> CacheFuture<'a, Result<Option<Bytes>, CacheError>> {
        self.inner.get(key)
    }
    fn set<'a>(
        &'a self,
        key: &'a CacheKey,
        value: Bytes,
        ttl: CacheTtl,
    ) -> CacheFuture<'a, Result<(), CacheError>> {
        let writes = self.writes.clone();
        Box::pin(async move {
            writes.fetch_add(1, Ordering::Relaxed);
            self.inner.set(key, value, ttl).await
        })
    }
    fn add<'a>(
        &'a self,
        key: &'a CacheKey,
        value: Bytes,
        ttl: CacheTtl,
    ) -> CacheFuture<'a, Result<bool, CacheError>> {
        self.inner.add(key, value, ttl)
    }
    fn delete<'a>(&'a self, key: &'a CacheKey) -> CacheFuture<'a, Result<(), CacheError>> {
        self.inner.delete(key)
    }
    fn touch<'a>(
        &'a self,
        key: &'a CacheKey,
        ttl: CacheTtl,
    ) -> CacheFuture<'a, Result<bool, CacheError>> {
        self.inner.touch(key, ttl)
    }
    fn increment<'a>(
        &'a self,
        key: &'a CacheKey,
        delta: i64,
    ) -> CacheFuture<'a, Result<i64, CacheError>> {
        self.inner.increment(key, delta)
    }
    fn clear<'a>(&'a self, scope: &'a CacheScope) -> CacheFuture<'a, Result<(), CacheError>> {
        self.inner.clear(scope)
    }
    fn set_many<'a>(&'a self, values: &'a [CacheWrite]) -> CacheFuture<'a, Result<(), CacheError>> {
        Box::pin(async move {
            self.bulk_writes.fetch_add(1, Ordering::Relaxed);
            let mut entries = self.inner.entries.lock();
            for value in values {
                entries.insert(value.key.as_str().into(), value.value.clone());
            }
            Ok(())
        })
    }
}
