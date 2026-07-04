# Services

Vyuh services are site-lifetime application components. Use them for shared
clients, caches, coordinators, in-process state, and background loops that
should be created once when the site starts.

Services are not durable work queues. Use [Tasks](tasks.md) for work that must
survive process restarts, retries, sleeps, or external resume.

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

Services are built during site startup before routes are served. A service is
stored as an `Arc<T>` and can be extracted by routes, workers, and other
callable handlers.

## Registration

Service constructors return `ServiceInstance<T>`:

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

Workers are simple Tokio tasks spawned once at site startup. If a worker returns
`Err`, Vyuh logs the error and the worker stops. Vyuh does not restart service
workers automatically; long-running workers should own their loop and listen for
shutdown.

## Examples

The snippets in this chapter cover concrete service registration, direct
registration through `bundles::service`, trait-object facades, and
service-owned workers with shutdown handling.

## Failure Modes

- Duplicate concrete service registrations fail site build.
- Duplicate exposed facade types fail site build.
- Missing service lookups return `ServiceError::NotFound`.
- Service constructor extraction errors fail site build.
- Worker errors are logged and stop that worker.

## Current Limitations

- Services are in-process and per-site-instance only.
- Services are not durable and are not retried after process restart.
- Service workers are not automatically restarted.
- Vyuh does not provide distributed singleton coordination for services.
