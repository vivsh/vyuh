# Templates

Vyuh templates provide server-side HTML rendering through Minijinja. Templates
are loaded when a site is built and are available through `site.templates()` or
the `Templates` route extractor.

Templates are private bundle resources. They live inside registered bundle
asset dirs under `templates/**`. They are not served as public assets and are
not copied by `collect_assets`.

## Overview

The main public pieces are:

- `SiteConf::templates(TemplateConf)` for template environment configuration.
- Asset dir `templates/**` for bundle-owned template files.
- `Site::templates()` and the `Templates` route extractor for rendering.
- Built-in helpers and filters for assets, reverse URLs, date/time formatting,
  and common display transforms.
- `TemplateError` and `TemplateFormatError` for loading, rendering, and
  formatting failures.

Minijinja is the only template engine in v0.

## Configuration

Use `TemplateConf` when the application needs explicit environment behavior:

```rust
use vyuh::prelude::*;
use vyuh::templates::{TemplateAutoEscape, TemplateConf, TemplateDateFormats, TemplateUndefined};

let conf = SiteConf::default().templates(TemplateConf {
    auto_escape: TemplateAutoEscape::ByExtension,
    undefined: TemplateUndefined::Strict,
    trim_blocks: true,
    lstrip_blocks: true,
    keep_trailing_newline: true,
    date_formats: TemplateDateFormats {
        date: "%d %b %Y".into(),
        time: "%H:%M".into(),
        datetime: "%d %b %Y, %H:%M".into(),
    },
});
```

Defaults:

- autoescape by extension.
- strict undefined values.
- no block trimming or left stripping.
- keep trailing newline.
- date/time patterns use Chrono strftime syntax:
  - date: `%Y-%m-%d`
  - time: `%H:%M`
  - datetime: `%Y-%m-%d %H:%M`

`SiteConf::timezone(...)` controls local date/time formatting.

## Template Sources

Templates are loaded from bundle asset dirs under `templates/**`. The
`templates/` prefix is stripped:

```text
assets/templates/dashboard/base.html -> dashboard/base.html
assets/templates/dashboard/login.html -> dashboard/login.html
```

Non-template files in asset dirs are ignored by the template loader.

## Rendering

Render through `site.templates()`:

```rust
let html = site.templates().render(
    "hello.html",
    &serde_json::json!({ "name": "Vyuh" }),
)?;
```

Or extract `Templates` in a route:

```rust
use vyuh::prelude::*;
use vyuh::templates::{TemplateError, Templates};

#[bundles::route(path = "/hello")]
async fn hello(templates: Templates) -> Result<Html<String>, TemplateError> {
    templates.html("hello.html", &serde_json::json!({ "name": "Vyuh" }))
}
```

`Templates` exposes:

- `render(name, context)` - render to `String`.
- `html(name, context)` - render to `Html<String>`.
- `exists(name)` - check if a template is loaded.
- `names()` - list loaded template names for diagnostics.

## Includes And Inheritance

Includes, imports, macros, and inheritance use the same template names Vyuh
registers at site build time. A template loaded as `dashboard/layouts/base.html`
is referenced by that exact name:

```html
{% extends "dashboard/layouts/base.html" %}

{% block title %}Dashboard{% endblock %}

{% block content %}
  {% include "dashboard/components/flash.html" %}
  <h1>Hello {{ user.name }}</h1>
{% endblock %}
```

Macros use Minijinja's normal import syntax:

```html
{% from "dashboard/components/forms.html" import field %}
{{ field("email", "Email address") }}
```

## Built-In Helpers

Vyuh registers helpers that are available to every template:

```jinja
<link rel="stylesheet" href="{{ asset("dashboard/app.css") }}">
<a href="{{ url_for("user_detail", {"id": user.id}) }}">Profile</a>
Generated at {{ now()|format_datetime }}
```

Helpers:

- `STATIC_URL` is the normalized configured static base.
- `asset(path)` resolves a safe path relative to `public/` through that base.
- `url_for(name, params={})` reverses a named route and fails rendering if the
  route cannot be resolved.
- `now()` returns the current UTC datetime.

Common filters:

- `slugify`
- `filesizeformat`
- `linebreaksbr`
- `truncatechars`

## Date And Time Formatting

Date/time helpers use `SiteConf::timezone(...)` and
`TemplateConf::date_formats`:

```jinja
{{ post.published_at|format_datetime }}
{{ post.published_at|format_datetime("%d %b %Y, %H:%M") }}
{{ post.published_at|datetime }}
{{ post.published_at|format_date }}
{{ post.published_at|date }}
{{ post.published_at|format_time }}
{{ localdate() }}
{{ localdatetime() }}
```

The same formatting path is available from Rust:

```rust
let label = vyuh::templates::format_datetime(&site, created_at, None)?;
let custom = vyuh::templates::format_date(&site, created_at, Some("%d %b %Y"))?;
let today = vyuh::templates::localdate::<chrono::DateTime<chrono::Utc>>(&site, None)?;
```

Invalid or unsupported values return `TemplateFormatError` in Rust and a
Minijinja render error in templates.

## Assets Boundary

Templates and public assets share asset dirs but not visibility:

- `templates/**` is private and loaded into Minijinja.
- `public/**` is public and served or collected as an asset.

Use public asset URLs from templates:

```html
<link rel="stylesheet" href="{{ asset("dashboard/dashboard.css") }}">
```

See [Assets](assets.md) for public asset serving and `collect_assets`.

## Naming And Overrides

Template names are explicit paths such as `dashboard/base.html`. There are no
package names or hidden namespace rules.

Later registered asset directories override earlier templates with the same
relative name. This gives an application a deliberate, deterministic way to
replace a dependency template without adding package namespaces.

## Examples

The snippets in this chapter cover bundle-owned templates, embedded assets,
route extraction of `Templates`, environment configuration, and date/time
formatting.

## Failure Modes

- Missing templates return `TemplateError::NotFound`.
- Invalid UTF-8 template files fail during site build.
- Invalid template syntax fails during site build.
- Render-time template errors return `TemplateError::RenderError`.
- Date/time formatting failures return `TemplateFormatError` or render errors.

## Current Limitations

- Minijinja is the only supported engine.
- Template filters and globals are framework-provided in v0; arbitrary custom
  filter/global registration is not yet a stable public API.
- Templates are loaded at site build time; dynamic template reloading is not a
  public runtime feature.
