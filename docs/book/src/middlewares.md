# Middlewares

Vyuh separates global HTTP transport policy from feature-level route
composition. Site-wide middleware is configured with `SiteConf::http(...)`.
Bundle and route middleware remain available for feature-specific behavior.

OpenAPI stays focused on the public client contract. Middleware appears in
OpenAPI only when it explicitly contributes request or security metadata through
`LayerSpec`. The console is broader: it shows enabled site policies and
operation middleware as runtime metadata so you can see what affects a route
without treating those policies as handler inputs.

## Overview

The main public pieces are:

- `SiteConf::http(HttpConf)` for global middleware configuration.
- Declared route paths for canonical trailing-slash behavior.
- `RouteConf::trim` and `#[bundles::route(..., trim = false)]` for the
  route-only strict opt-out.
- `routes::Middleware` and `routes::layer_from(...)` for route or bundle
  middleware.

Site-wide middleware is applied through the shared internal router path used by
`Site::serve`, `site.start()`, and test router construction.
Enabled site-wide policies are visible in the console operation details. They
are not copied into handler arguments and are not automatically added to
OpenAPI.

## Site HTTP Configuration

Start from defaults and enable only the transport behavior the application
needs:

```rust
use schemars::JsonSchema;
use vyuh::prelude::*;
use vyuh::middlewares::{BodyLimitConf, CompressionConf, HttpConf, TraceConf};

let conf = SiteConf::default().http(HttpConf {
    trace: TraceConf { enabled: true },
    compression: CompressionConf { enabled: true },
    body_limit: BodyLimitConf {
        enabled: true,
        max_bytes: 1024 * 1024,
    },
    ..HttpConf::default()
});
```

Default behavior:

| Option | Default |
| --- | --- |
| panic catching | enabled |
| request id | enabled, `x-request-id` |
| trace | disabled |
| compression | disabled |
| CORS | disabled |
| timeout | disabled |
| body limit | disabled |
| security headers | disabled |
| shutdown grace period | `10000` ms |

## Request Ids And Panics

Request IDs are enabled by default. Vyuh reads the configured header when it is
present, otherwise it generates a new ID and writes it to the response:

```rust
use vyuh::prelude::*;
use vyuh::middlewares::{HttpConf, RequestIdConf};

let conf = SiteConf::default().http(HttpConf {
    request_id: RequestIdConf {
        enabled: true,
        header: "x-request-id".into(),
    },
    ..HttpConf::default()
});
```

Panic catching is also enabled by default so panics are converted into framework
errors instead of tearing down the server task.

## Trace, Compression, CORS, Timeout, And Limits

Trace, compression, CORS, timeout, and body limit are opt-in:

```rust
use vyuh::prelude::*;
use vyuh::middlewares::{CorsConf, HttpConf, TimeoutConf};

let conf = SiteConf::default().http(HttpConf {
    cors: CorsConf {
        enabled: true,
        permissive: true,
    },
    timeout: TimeoutConf {
        enabled: true,
        timeout_ms: 10_000,
    },
    ..HttpConf::default()
});
```

Timeout and body-limit failures flow through `ErrorReport` and the site error
handler, so custom API or HTML error pages can render them consistently.

## Shutdown

Vyuh starts graceful shutdown on the first `Ctrl+C`, `SIGTERM`, touch-reload
event, or programmatic `site.shutdown()`. The default grace period is 10
seconds; after that Vyuh forces server shutdown so long-lived HTTP connections
cannot keep the process alive forever.

```rust
use vyuh::prelude::*;
use vyuh::middlewares::{HttpConf, ShutdownConf};

let conf = SiteConf::default().http(HttpConf {
    shutdown: ShutdownConf {
        grace_period_ms: 5_000,
    },
    ..HttpConf::default()
});
```

During graceful shutdown, channel transports close themselves: SSE streams end,
WebSockets close, and long-poll requests return promptly.

## Security Headers

Security headers are disabled by default because applications often need
deployment-specific policy. Enable the built-in defaults when they fit:

```rust
use vyuh::prelude::*;
use vyuh::middlewares::{HttpConf, SecurityHeadersConf};

let conf = SiteConf::default().http(HttpConf {
    security_headers: SecurityHeadersConf {
        enabled: true,
        ..SecurityHeadersConf::default()
    },
    ..HttpConf::default()
});
```

The default header policy includes `x-content-type-options: nosniff`,
`x-frame-options: DENY`, and `referrer-policy: same-origin`.

## Canonical Trailing Slashes

The declared path is the canonical public URL. There is no site or bundle slash
policy:

- Declared `/items` serves both `/items` and `/items/` without redirecting.
- Declared `/docs/` serves `/docs/`; `/docs` redirects permanently to `/docs/`.
- A slashless route with `trim = false` rejects `/route/` with Vyuh's structured
  `404`. Use this for file-like resources where the alternate form is invalid.

```rust
#[bundles::route(path = "/files/{*path}", trim = false)]
async fn file(Path(path): Path<String>) -> FileResponse {
    // ...
}
```

The direct API sets the same route-local flag:

```rust
let route = bundles::route(
    file,
    RouteConf {
        name: "file".into(),
        path: "/files/{*path}".into(),
        methods: Methods::GET,
        trim: false,
    },
);
```

`trim = false` cannot be combined with a path ending in `/`. Vyuh validates
direct registrations during bundle construction, while the macro rejects the
contradictory declaration at compile time.

Vyuh stores each framework operation once at a slashless internal path. A small
service removes one terminal slash before Axum matches the request, preserving
`OriginalUri`, and dispatches exactly once. Only slashful declarations and
strict routes receive a static route guard. There are no slash aliases, route
indexes, route scans, fallback re-entry, or request-time locks.

Raw `axum::Router` bundles must use slashless physical paths. They receive the
global internal trim but cannot declare Vyuh's redirect or strict behavior.

## Route And Bundle Middleware

Use site-wide middleware for global transport policy. Use bundle middleware for
feature-specific behavior. If a middleware implements `routes::Middleware` and
returns a `LayerSpec`, Vyuh attaches that metadata to the wrapped operations so
OpenAPI and console can show the API-visible request or security contract:

```rust
use vyuh::prelude::*;
use vyuh::routes::CorsMiddleware;
use tower_http::cors::CorsLayer;
use schemars::JsonSchema;

#[derive(Serialize, JsonSchema)]
struct PingOut {
    ok: bool,
}

#[bundles::route(path = "/ping")]
async fn ping() -> Data<PingOut> {
    Data::new(PingOut { ok: true })
}

let bundle = bundles::bundle! {
    ping
}
.layer(CorsMiddleware::new(CorsLayer::permissive()));
```

Plain Tower or Axum layers remain escape hatches for behavior Vyuh does not
wrap yet:

```rust
use vyuh::prelude::*;
use vyuh::routes::layer_from;

let bundle = app_bundle().layer(layer_from(my_tower_layer));
```

Plain layers are intentionally undocumented. Wrap them in a Vyuh middleware with
`LayerSpec` when they affect what clients must send.

## OpenAPI And Console Visibility

Handler arguments remain handler inputs. Middleware metadata is separate.

- OpenAPI includes explicit `LayerSpec` request parts and security schemes.
- OpenAPI does not include operational site policies such as timeout,
  compression, tracing, panic catching, body limit, slash handling, or security
  response headers as fake request parameters.
- Console operation details show enabled site policies and operation middleware
  as operational metadata.
- Site-wide CORS is visible in console. Bundle-level `CorsMiddleware` is
  OpenAPI-visible because the bundle explicitly opted into documented
  middleware metadata.

## Examples

The snippets in this chapter cover site-wide HTTP middleware configuration,
canonical paths, bundle middleware, and direct Tower layer escape hatches.

## Failure Modes

- Contradictory strict slash declarations and normalized route collisions fail
  during bundle construction.
- Timeout and body-limit failures are rendered through the normal error
  pipeline.
- Panics are converted to framework errors when panic catching is enabled.

## Current Limitations

- Built-in middleware configuration covers common transport policy, not every
  Tower layer.
- Direct Tower layers remain available, but they do not automatically provide
  Vyuh OpenAPI metadata.
- Strict slash handling is route-only; raw Axum routes cannot opt into it.
