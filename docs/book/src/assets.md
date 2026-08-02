# Assets

Vyuh assets are bundle-owned files that ship with a feature. They are used for
CSS, JavaScript, images, templates, SQL snippets, and other resource files that
belong beside the routes, services, tasks, and commands that use them.

An asset dir is a structured resource root, not a plain static directory. Only
files under `public/` are web-accessible. Everything else is private framework
or application resource data.

## Overview

The main public pieces are:

- `#[bundles::asset_dir]` for registering a bundle asset directory.
- `vyuh::embed::embed_assets!("path")` for debug-filesystem and release-embedded assets.
- Runtime serving of `public/**` under the site's configured static URL.
- `collect_assets` for copying bundled public assets to a deployment directory.
- Minijinja loading of private `templates/**` files.
- Mool/Gaman desired-schema loading from private `schema/**` files.

Asset dirs are part of bundle composition. A feature can register routes,
templates, public CSS, and private resources as one bundle.

Vyuh itself also uses this convention for shared framework web assets. The
runtime crate owns `vyuh/web/`, which contains public CSS, JavaScript, images,
landing-page source, and private console templates. When the built-in console is
enabled, that directory is registered as an internal asset dir so the console can
serve its stylesheet, logos, and helper scripts without requiring application
projects to copy them.

## Directory Layout

Use convention folders inside each asset dir:

```text
assets/
  public/
    dashboard/
      dashboard.css
  templates/
    dashboard/
      layouts/
        base.html
  schema/
    users.yaml
    reporting.sql
  sql/
    reports.sql
```

The folders have different visibility:

- `public/**` is served over HTTP and copied by `collect_assets`.
- `templates/**` is loaded into Minijinja and is not public.
- `schema/**` is parsed as desired database schema when the `migrations` feature is enabled.
- `sql/**` and other non-public folders are private resources.

Database migrations do not live under assets. They are crate-owned database
history files in a flat top-level `migrations/` directory. Schema assets are
desired state; migrations are immutable history. See [Migrations](migrations.md)
for the migration model.

Public namespacing is done by folders under `public/`. For example,
`public/dashboard/dashboard.css` is served as `/static/dashboard/dashboard.css`
with the default static URL.

## Registration

Register an asset dir in a bundle:

```rust
use vyuh::prelude::*;
use vyuh::embed;

const ASSETS: embed::Dir = embed::embed_assets!("assets");

#[bundles::asset_dir]
fn assets() -> embed::Dir {
    ASSETS.clone()
}

let bundle = bundles::bundle! {
    assets,
};
```

`embed_assets!` reads from the filesystem in debug builds and embeds the files
in release builds. That keeps local frontend iteration fast while making
release binaries self-contained. Pass `force = true` to always embed files.

Later registered asset directories override earlier files with the same relative
path. This applies consistently to templates and schema assets.

## Schema Assets

Files below `schema/` are private desired-schema inputs for the migration engine:

- `.yaml` and `.yml` files use Gaman's authored schema format.
- `.sql` files use authored DDL parsed for the configured database dialect.
- Files are merged through Mool; they are never executed as ad-hoc SQL.

Schema assets participate in `make_migration`, `--dry-run`, and `--check`. They
never apply migrations or modify a database while a site starts. They are not
served under `/static` and are not copied by `collect_assets`.

## Runtime Serving

Vyuh serves bundled public assets under `/static` by default. Configure one
site-wide public base when an application uses another local mount or a CDN:

```rust
let conf = SiteConf::default()
    .static_url("/static");

let cdn_conf = SiteConf::default()
    .static_url("https://cdn.example.com/static");
```

Relative static URLs use the browser's current host. Absolute static URLs keep
their configured origin. In both cases the URL path is the local development
mount. The `public/` prefix is stripped from asset paths:

```text
public/dashboard/dashboard.css -> /static/dashboard/dashboard.css
public/images/logo.svg -> /static/images/logo.svg
```

Use `site.assets()` whenever Rust code needs an asset URL:

```rust
let script = site.assets().url("dashboard/app.js")?;
assert_eq!(site.assets().static_url(), "/static/");
```

Paths are relative to `public/`; leading slashes, traversal segments, query
strings, and fragments are rejected. URL construction does not require the
asset to be present, which keeps it suitable for external static deployments.

Only `public/**` participates in runtime serving. Requests cannot reach
`templates/**`, `sql/**`, or other private folders through the asset route.

Built-in framework assets follow the same rule:

```text
vyuh/web/public/css/vyuh.css -> /static/css/vyuh.css
vyuh/web/public/console/js/console.js -> /static/console/js/console.js
vyuh/web/templates/console/layout.html -> private Minijinja template
```

Static serving is intentionally bundle-owned. Register application assets
through bundle asset dirs so public files and private templates ship through the
same debug-filesystem and release-embedding machinery.

## Templates

Minijinja templates are loaded from `templates/**`. The `templates/` prefix is
stripped when the template is registered:

```text
templates/dashboard/layouts/base.html -> dashboard/layouts/base.html
templates/dashboard/login.html -> dashboard/login.html
```

Template namespacing is done by folders under `templates/`. Public asset
namespacing is done by folders under `public/`. The two namespaces are
independent.

See [Templates](templates.md) for rendering APIs, template source rules, and
template failure modes.

A dashboard layout can refer to a public asset like this:

```html
<link rel="stylesheet" href="{{ asset('dashboard/dashboard.css') }}" />
```

## Collect Assets

`collect_assets` copies all bundled `public/**` files to a target directory for
deployment through a CDN, reverse proxy, or dedicated static file host.

The same behavior is available as a built-in command:

```sh
cargo run -- collect_assets --output dist/static
```

Use `--glob` for a partial asset update. The pattern matches the path after
`public/` is stripped:

```sh
cargo run -- collect_assets --output dist/static --glob 'dashboard/**'
```

The destination path strips the `public/` prefix:

```text
public/dashboard/dashboard.css -> <output-dir>/dashboard/dashboard.css
public/images/logo.svg -> <output-dir>/images/logo.svg
```

`collect_assets` does not copy templates, schema files, SQL files, database
migrations, or other private resources. It copies the same public asset surface
that runtime serving exposes.

For `collect_pages`, Vyuh writes collected assets under the configured static
URL path inside the page-export root. `/static` writes `dist/static/**`, while
`https://cdn.example.com/static` writes `dist/static/**`; the CDN host is not a
filesystem path.

Use `collect_assets` when the application server should not serve assets
directly in production, or when a deployment platform expects a static asset
directory.

## Debug And Release Behavior

Assets registered through `vyuh::embed::embed_assets!` have different storage
behavior by build mode:

- Debug builds read from the source filesystem.
- Release builds serve embedded bytes from the compiled binary.

The logical asset paths stay the same in both modes. A file such as
`public/dashboard/dashboard.css` is addressed through the configured static URL
whether it is read from disk during development or served from the binary in
production.

## Failure Modes

- Files outside `public/**` are not publicly served or collected.
- Missing public files return not found.
- Invalid paths and traversal attempts are rejected.
- Template names come from `templates/**`; public asset names come from
  `public/**`.
- Static files must live under registered bundle asset dirs and `public/**` to
  be served by the runtime asset route.

## Current Limitations

- Asset dirs do not have package metadata.
- Public URL namespacing is folder-based under `public/`.
- Private resource folders are reserved for framework and application use; they
  are not exposed over HTTP.
