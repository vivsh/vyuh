# Services

Vyuh services are site-lifetime application components. Use them for shared
clients, coordinators, in-process state, and background loops. Service instances
are constructed during site assembly; registered loops start only when serving.

Services are not durable work queues. Use [Tasks](tasks.md) for work that must
survive process restarts, retries, sleeps, or external resume.
Use [Cache](cache.md) for ordinary named cache providers instead of creating a
service solely for caching.

Use services for site-lifetime dependencies and workers that should be built
once with the site. Do not use `Data<T>` for services; handlers should extract
`ServiceRef<T>` or use `site.service::<T>()`.

## Overview

The main public pieces are:

- `#[bundles::service]` for ergonomic service registration.
- `bundles::service(handler)` for direct registration.
- `ServiceInstance<T>` for returning a built service from a constructor.
- `Service` for optional facade exposure and worker registration.
- `ServiceRef<T>` and `Site::service<T>()` for using services.
- `ServiceRunner` for service-owned background workers.

Services are constructed during site assembly. A service is stored as an
`Arc<T>` and can be extracted by routes and commands before the serving runtime
starts; its registered workers start only when Vyuh serves the site.

## Registration

Service constructors return `ServiceInstance<T>` or
`Result<ServiceInstance<T>, ServiceError>`. Existing constructors with a declared
`ServiceInstance<T>` return type need no changes:

```rust
use vyuh::prelude::*;
use vyuh::services::ServiceInstance;

#[derive(Default)]
struct Counter {
    value: std::sync::atomic::AtomicUsize,
}

impl vyuh::services::Service for Counter {}

#[bundles::service]
async fn counter() -> ServiceInstance<Counter> {
    Counter::default().into()
}

let bundle = bundles::bundle! {
    counter,
};
```

The direct API is equivalent:

```rust
use vyuh::prelude::*;

let bundle = bundles::bundle([bundles::service(counter)]);
```

Only one service can be registered for a concrete service type. Duplicate
registrations fail site build.

Inline closures must make their output type clear. With both fallible and
infallible outputs accepted, a bare `.into()` may no longer infer its target:

```rust
let bundle = bundles::bundle([bundles::service(|| async {
    ServiceInstance(Counter::default())
})]);
```

Named factories with declared return types retain their existing inference.

## Construction

Constructors run while the site is being built. They can extract
`ServiceBuildContext` or `DbPool`:

```rust
use vyuh::prelude::*;
use vyuh::db::DbPool;
use vyuh::services::ServiceInstance;

struct SearchIndex {
    db: DbPool,
}

impl vyuh::services::Service for SearchIndex {}

async fn search_index(db: DbPool) -> ServiceInstance<SearchIndex> {
    SearchIndex { db }.into()
}
```

The full `Site` is intentionally unavailable during service construction,
because services are part of building the site.

### Fallible Construction

Return `Result` when initialization can fail. Both the macro and direct API
accept the same factory:

```rust
use vyuh::{bundles, callables::CallError};
use vyuh::services::{Service, ServiceError, ServiceInstance};

struct LocalApp {
    config: String,
}

impl LocalApp {
    async fn build() -> std::io::Result<Self> {
        let config = tokio::fs::read_to_string("app.toml").await?;
        Ok(Self { config })
    }
}

impl Service for LocalApp {}

#[bundles::service]
async fn local_service() -> Result<ServiceInstance<LocalApp>, ServiceError> {
    let app = LocalApp::build()
        .await
        .map_err(|error| CallError::Other(Box::new(error)))?;
    Ok(app.into())
}

let bundle = bundles::bundle! { local_service };
// Alternatively, register the same factory directly:
let bundle = bundles::bundle([bundles::service(local_service)]);
```

This example reads configuration; application-specific validation, vault
unlocking, and lock acquisition belong in the application's constructor.
`?` converts `CallError` to `ServiceError`; other application errors need an
explicit conversion, as shown, which preserves the concrete source error.

A constructor error propagates as `SiteError::ServiceError` from site assembly,
including `Site::run` when it builds the site. No requests are served and no
registered workers start. Previously constructed services and worker captures
owned by the failed assembly are dropped. Successful construction retains the
same exclusive initialization, facade exposure, and worker registration as an
infallible factory.

Keep resource guards owned by the service or constructor locals so dropping
them releases resources on failure. This is ordinary Rust drop cleanup, not
asynchronous rollback of external side effects. Constructors must not spawn
detached background tasks; use `ServiceRunner` for runtime work.

## Using Services

Routes can extract `ServiceRef<T>`:

```rust
use vyuh::prelude::*;
use vyuh::services::ServiceRef;

#[bundles::route(path = "/count")]
async fn count(counter: ServiceRef<Counter>) -> Html<String> {
    let next = counter.value.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
    Html(next.to_string())
}
```

Code that already has a `Site` can use `Site::service<T>()`:

```rust
let counter = site.service::<Counter>()?;
```

Missing services return `ServiceError::NotFound`.

## Facades

A service can expose a trait object facade. This lets routes depend on a narrow
interface instead of the concrete service type:

```rust
use std::sync::Arc;
use vyuh::services::{Service, ServiceError, ServiceExposer};

trait Mailer: Send + Sync {
    fn send(&self, to: &str);
}

struct SmtpMailer;

impl Mailer for SmtpMailer {
    fn send(&self, to: &str) {
        println!("send mail to {to}");
    }
}

impl Service for SmtpMailer {
    fn expose(exposer: &mut ServiceExposer<Self>) -> Result<(), ServiceError> {
        exposer.expose(|service| service as Arc<dyn Mailer>)
    }
}
```

Consumers can then request `ServiceRef<dyn Mailer>` or
`site.service::<dyn Mailer>()`. Duplicate exposed facade types fail site build.

## Workers

Services may register background workers from `Service::run`:

```rust
use vyuh::prelude::*;
use vyuh::services::{Service, ServiceError, ServiceRunner};

impl Service for SearchIndex {
    fn run(&mut self, runner: &mut ServiceRunner) -> Result<(), ServiceError> {
        runner.run("search-index-refresh", |site: Site| async move {
            let shutdown = site.shutdown_notifier();
            loop {
                tokio::select! {
                    _ = shutdown.notified() => break,
                    _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                        // refresh in-process state
                    }
                }
            }
            Ok(())
        })
    }
}
```

`Service::run` is invoked during site assembly and must only register workers
through `ServiceRunner`; it must not perform runtime work directly. Registered
workers are simple Tokio tasks spawned once when `serve` starts. If a worker
returns `Err`, Vyuh logs the error and the worker stops. Vyuh does not restart
service workers automatically; long-running workers should own their loop and
listen for shutdown.

## Examples

The snippets in this chapter cover concrete service registration, direct
registration through `bundles::service`, trait-object facades, and
service-owned workers with shutdown handling.

## Failure Modes

- Duplicate concrete service registrations fail site build.
- Duplicate exposed facade types fail site build.
- Missing service lookups return `ServiceError::NotFound`.
- Service constructor extraction errors fail site build.
- Fallible constructor errors fail site build and release assembly-owned resources.
- Worker errors are logged and stop that worker.

## Current Limitations

- Services are in-process and per-site-instance only.
- Services are not durable and are not retried after process restart.
- Service workers are not automatically restarted.
- Vyuh does not provide distributed singleton coordination for services.
