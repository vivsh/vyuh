# Collectors

Collectors are a small capability that falls out of Vyuh's architecture.
Bundles can contribute URL metadata, and Vyuh can collect bundle-owned public
assets or rendered page files through the same site, router, templates, assets,
services, and configuration used at runtime.

## Overview

The main public pieces are:

- `#[bundles::url_info]` and `bundles::url_info(...)` for registering URL
  metadata providers.
- `UrlInfo` and `UrlRoles` for marking URL purpose.
- `Site::collectors().collect_assets(...)` for copying matching public assets.
- `Site::collectors().collect_pages(...)` for rendering matching static pages.
- `collect_assets` for copying bundle-owned `public/**` assets.
- Built-in commands `collect_assets` and `collect_pages`.

Page collection only renders URLs marked with `UrlRoles::STATIC`. Sitemap-visible
URLs can be marked with `UrlRoles::SITEMAP` for other tooling; Vyuh does not
generate sitemap XML in this pass.

## URL Info

URL info providers are bundle parts. They return bundle-local URL paths and
compose with `merge` and `with_prefix` like routes:

```rust
use vyuh::prelude::*;
use vyuh::collectors::{UrlInfo, UrlRoles};

#[bundles::url_info]
async fn urls(site: Site) -> Result<Vec<UrlInfo>, Error> {
    Ok(vec![
        UrlInfo::new("/", UrlRoles::STATIC | UrlRoles::SITEMAP),
        UrlInfo::static_page("/about").with_sitemap(),
        UrlInfo::sitemap("/sitemap.xml"),
    ])
}

let blog = bundles::bundle! {
    urls,
}
.with_prefix("/blog");
```

In the example above, `/about` becomes `/blog/about` after prefixing. If multiple
providers return the same final URL, Vyuh merges their `UrlRoles`.

## Collect Pages

Use `Site::collectors()` when application code needs to render pages without copying
assets:

```rust
use vyuh::prelude::*;

# async fn export(site: Site) -> Result<(), Error> {
site.collectors()
    .output("dist")
    .collect_pages(Some("/blog/**".to_string()))
    .await
    .map_err(Error::other)?;
# Ok(())
# }
```

The optional glob matches final URL paths after bundle prefixing. `*` matches
one path segment and `**` matches across slashes. Passing `None` exports all
static URLs.

## Collect Assets

Use the same facade when application code needs to copy only a subset of public
assets:

```rust
use vyuh::prelude::*;

# async fn export(site: Site) -> Result<(), Error> {
site.collectors()
    .output("dist/static")
    .collect_assets(Some("css/**".to_string()))
    .await
    .map_err(Error::other)?;
# Ok(())
# }
```

Asset globs match paths after `public/` is stripped, such as `css/app.css` or
`images/logo.svg`. Passing `None` copies all bundled public assets.

## Commands

Copy only public assets:

```sh
cargo run -- collect_assets --output dist/static
```

Copy only matching public assets:

```sh
cargo run -- collect_assets --output dist/static --glob 'css/**'
```

Collect all static page URLs and public assets:

```sh
cargo run -- collect_pages --output dist
```

`collect_pages` derives the asset directory from `SiteConf::static_url(...)`.
The default `/static` writes `dist/static/**`; a CDN `/static` base also writes
`dist/static/**`.

Update only matching pages:

```sh
cargo run -- collect_pages --output dist --glob '/blog/**'
```

Partial page collection with `--glob` does not copy assets. Partial collection
with `--glob` never cleans the output directory; `--clean` is rejected when
`--glob` is present.

## Path Rules

URL paths must start with `/`. Page collection rejects query strings, fragments,
parent directory segments, backslashes, Windows drive prefixes, and paths that
would escape the output directory.

Extensionless URLs are written as HTML files:

| URL          | Output            |
| ------------ | ----------------- |
| `/`          | `index.html`      |
| `/blog/`     | `blog/index.html` |
| `/about`     | `about.html`      |
| `/blog/post` | `blog/post.html`  |
| `/feed.xml`  | `feed.xml`        |

## Non-Goals

Vyuh does not crawl routes, parse markdown, generate feeds, generate sitemap
XML, paginate content, hydrate JavaScript, or do incremental dependency
tracking. Those can be implemented as ordinary bundles on top of routes, assets,
templates, and URL info providers.
