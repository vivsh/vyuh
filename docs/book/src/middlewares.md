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
- `SlashPolicy` for deterministic trailing-slash behavior.
- `Bundle::with_slash_policy(...)` for bundle-level slash policy.
- `RouteConf { slash: Some(...), .. }` and `#[bundles::route(..., slash = "...")]`
  for route-level slash policy.
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
| slash policy | `Auto` |
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

## Slash Policy

Vyuh does not silently hard-code one trailing-slash rule for the whole server.
Slash behavior is route metadata:

| Policy | Behavior |
| --- | --- |
| `Exact` | only the declared path matches |
| `Trim` | alternate trailing slash rewrites internally |
| `RedirectAppend` | missing slash redirects to slash form with `308` |
| `RedirectRemove` | trailing slash redirects to non-slash form with `308` |
| `Auto` | HTML routes redirect to the declared path shape; API/unknown routes trim |

Site default:

```rust
use vyuh::prelude::*;
use vyuh::middlewares::{HttpConf, SlashConf, SlashPolicy};

let conf = SiteConf::default().http(HttpConf {
    slash: SlashConf {
        policy: SlashPolicy::Auto,
    },
    ..HttpConf::default()
});
```

Bundle override:

```rust
use vyuh::prelude::*;
use vyuh::middlewares::SlashPolicy;

let bundle = app_bundle().with_slash_policy(SlashPolicy::RedirectAppend);
```

Route override with the macro:

```rust
use vyuh::prelude::*;
use vyuh::routes::Html;

#[bundles::route(path = "/docs/", slash = "redirect_append")]
async fn docs() -> Html<&'static str> {
    Html("docs")
}
```

Route override with direct registration:

```rust
use std::borrow::Cow;
use vyuh::prelude::*;
use vyuh::bundles;
use vyuh::middlewares::SlashPolicy;
use vyuh::routes::{Methods, RouteConf};

let route = bundles::route(
    docs,
    RouteConf {
        name: Cow::Borrowed("docs"),
        path: Cow::Borrowed("/docs/"),
        methods: Methods::GET,
        slash: Some(SlashPolicy::RedirectAppend),
    },
);
```

Slash aliases and redirects are validated at site build. Conflicting generated
rules fail build instead of producing ambiguous runtime behavior.

## API And HTML Defaults

`Auto` is designed for mixed applications:

- API and unknown-response routes trim, so `/api/items/` can serve `/api/items`.
- HTML routes canonicalize to the declared path shape. A declared `/docs/`
  redirects `/docs` to `/docs/`; a declared `/about` redirects `/about/` to
  `/about`.

HTML detection uses route return metadata with `text/html`. Vyuh does not infer
slash policy from request `Accept` headers.

## Route And Bundle Middleware

Use site-wide middleware for global transport policy. Use bundle middleware for
feature-specific behavior. If a middleware implements `routes::Middleware` and
returns a `LayerSpec`, Vyuh attaches that metadata to the wrapped operations so
OpenAPI and console can show the API-visible request or security contract:

```rust
use vyuh::prelude::*;
use vyuh::routes::CorsMiddleware;
use tower_http::cors::CorsLayer;

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
slash policy behavior, bundle middleware, and direct Tower layer escape
hatches.

## Failure Modes

- Invalid slash policies or generated slash aliases fail during site build.
- Timeout and body-limit failures are rendered through the normal error
  pipeline.
- Panics are converted to framework errors when panic catching is enabled.

## Current Limitations

- Built-in middleware configuration covers common transport policy, not every
  Tower layer.
- Direct Tower layers remain available, but they do not automatically provide
  Vyuh OpenAPI metadata.
- Slash policy is based on route metadata, not request `Accept` headers.
