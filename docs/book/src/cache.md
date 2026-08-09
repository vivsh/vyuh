# Cache

Vyuh caches are site-owned, asynchronous, and provider-agnostic. Every site
has one default provider and may register additional named providers. The
default is a bounded in-process LRU cache; remote providers can implement the
same async contract without changing application code.

```rust
use vyuh::{SiteConf, cache::{CacheConf, CacheName, MemoryCache}};

const PRODUCTS: CacheName = CacheName::new("products");

let conf = SiteConf::default().cache(
    CacheConf::default()
        .provider(PRODUCTS, MemoryCache::new()),
);
```

`CacheConf::default()` already registers the `default` memory cache.
`CacheConf::empty()` requires both explicit provider registration and
`.default_provider(...)`. Builder calls accumulate configuration; duplicate or
invalid provider names and an invalid default fail from `Site::build`.

## Reading and writing values

`site.cache()` selects the configured default. Values use JSON serialization,
so a key must consistently be read using the type it was stored with.

```rust
use std::time::Duration;
use vyuh::cache::CacheTtl;

site.cache()
    .set("product:7", &product, CacheTtl::for_duration(Duration::from_secs(60)))
    .await?;

let product: Option<Product> = site.cache().get("product:7").await?;
```

Use `CacheTtl::Default` for the provider default (five minutes for
`MemoryCache`) or `CacheTtl::Forever` for entries that only eviction or an
explicit delete removes.

Named providers and namespace segments compose through infallible selectors:

```rust
let product: Option<Product> = site
    .cache()
    .using(PRODUCTS)
    .namespace("tenant:42")
    .get("product:7")
    .await?;
```

Vyuh scopes every backend key by provider name, namespace segments, and the
application key. Namespace handles expose ordinary key operations but omit
`clear`; only an un-namespaced provider handle can clear its own provider
scope.

## Bulk values and counters

`get_many` preserves requested-key order and omits misses. `set_many` and
`delete_many` use native/pipelined provider implementations when available.
The default provider-trait fallback is deliberately sequential and non-atomic:
a later error may follow earlier writes or deletes.

```rust
let products = site.cache()
    .get_many::<Product, _, _>(["product:7", "product:8"])
    .await?;

site.cache()
    .set_many([("product:7", product_a), ("product:8", product_b)], CacheTtl::Default)
    .await?;
```

Counters use a private canonical signed-integer encoding. Start or change one
with `increment` or `decrement`; do not store a JSON number at the same key.

```rust
let count = site.cache().increment("download-count", 1).await?;
```

`get_or_try_set` performs a read, runs its async initializer on a miss, then
uses `add`. It deliberately permits duplicate concurrent computation and does
not introduce local or distributed locks.

## Providers

`CacheProvider` is an object-safe asynchronous byte-storage contract. Vyuh
handles key validation, canonical scoping, JSON serialization, and metrics;
providers handle their own transport, storage, TTL, atomic integer, and clear
semantics. Implement only the single-key operations plus `touch`, arithmetic,
and scoped `clear`. The trait provides sequential defaults for `get_many`,
`set_many`, and `delete_many`; remote providers should override them with
pipelined or native bulk calls.

Provider errors are strict: a read or write failure is never silently treated
as a miss. Cache keys and values are deliberately absent from framework
errors, metrics labels, and console output.

The cache subsystem is separate from OAuth/JWKS verifier state. OAuth uses a
private per-site Huskarl verifier. The cache subsystem does not include Redis,
database, filesystem, HTTP-response caching, decorators, tags, namespace
versioning, or distributed locks. Those can be added as providers or
higher-level application policies later.
