use axum::{
    body::Body,
    extract::Request,
    http::{Method, StatusCode, header},
    response::Response,
};
use bytes::Bytes;
use parking_lot::RwLock;
use rust_silos::SiloSet;
use std::{
    collections::HashMap,
    convert::Infallible,
    future::Future,
    io::Read,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tower::Service;

use crate::embed;

pub const PUBLIC_ASSETS_FOLDER: &str = "public";
pub const DEFAULT_STATIC_URL: &str = "/static";

// Add these deps (recommended):
// percent-encoding = "2"
// mime_guess = "2"
// blake3 = "1"
use blake3::Hasher as Blake3;
use mime_guess::MimeGuess;
use percent_encoding::{AsciiSet, CONTROLS, percent_decode_str, utf8_percent_encode};
use thiserror::Error;
use url::Url;

const ASSET_SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'#')
    .add(b'%')
    .add(b'/')
    .add(b'<')
    .add(b'>')
    .add(b'?')
    .add(b'\\')
    .add(b'`')
    .add(b'{')
    .add(b'}');

/// Reports an invalid relative path passed to [`Assets::url`].
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum AssetUrlError {
    #[error("invalid asset path '{path}': {reason}")]
    InvalidPath { path: String, reason: &'static str },

    #[error("invalid static URL '{value}': {reason}")]
    InvalidStaticUrl { value: String, reason: &'static str },
}

/// Read-only URL construction for this built site's public assets.
pub struct Assets<'a> {
    urls: &'a AssetUrls,
}

impl<'a> Assets<'a> {
    pub(crate) const fn new(urls: &'a AssetUrls) -> Self {
        Self { urls }
    }

    /// Returns the normalized public static URL, including its trailing slash.
    pub fn static_url(&self) -> &str {
        self.urls.static_url()
    }

    /// Returns a public URL for a path relative to bundle-owned `public/` assets.
    pub fn url(&self, path: &str) -> Result<String, AssetUrlError> {
        self.urls.url(path)
    }
}

/// Validated static URL state shared by the site, templates, and built-in console.
#[derive(Clone, Debug)]
pub(crate) struct AssetUrls {
    static_url: Arc<str>,
    mount_path: Arc<str>,
}

impl AssetUrls {
    pub(crate) fn parse(value: &str) -> Result<Self, AssetUrlError> {
        if value.starts_with('/') {
            return Self::relative(value);
        }
        Self::absolute(value)
    }

    pub(crate) fn default_url() -> Self {
        Self {
            static_url: Arc::from("/static/"),
            mount_path: Arc::from(DEFAULT_STATIC_URL),
        }
    }

    pub(crate) fn mount_path(&self) -> &str {
        &self.mount_path
    }

    pub(crate) fn output_path(&self) -> PathBuf {
        PathBuf::from(self.mount_path.trim_start_matches('/'))
    }

    pub(crate) fn static_url(&self) -> &str {
        &self.static_url
    }

    pub(crate) fn url(&self, path: &str) -> Result<String, AssetUrlError> {
        validate_asset_path(path)?;
        let encoded = path
            .split('/')
            .map(|segment| utf8_percent_encode(segment, ASSET_SEGMENT).to_string())
            .collect::<Vec<_>>()
            .join("/");
        Ok(format!("{}{encoded}", self.static_url))
    }

    fn relative(value: &str) -> Result<Self, AssetUrlError> {
        let mount_path = normalize_mount(value)?;
        Ok(Self {
            static_url: Arc::from(format!("{mount_path}/")),
            mount_path: Arc::from(mount_path),
        })
    }

    fn absolute(value: &str) -> Result<Self, AssetUrlError> {
        let mut url = Url::parse(value).map_err(|_| {
            invalid_static(
                value,
                "must be an absolute HTTP(S) URL or root-relative path",
            )
        })?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(invalid_static(value, "must use http or https"));
        }
        if url.host().is_none() || !url.username().is_empty() || url.password().is_some() {
            return Err(invalid_static(
                value,
                "must include a host and no user credentials",
            ));
        }
        if url.query().is_some() || url.fragment().is_some() {
            return Err(invalid_static(
                value,
                "must not contain a query or fragment",
            ));
        }
        let mount_path = normalize_mount(url.path())?;
        url.set_path(&mount_path);
        Ok(Self {
            static_url: Arc::from(format!("{}/", url.as_str().trim_end_matches('/'))),
            mount_path: Arc::from(mount_path),
        })
    }
}

fn invalid_static(value: &str, reason: &'static str) -> AssetUrlError {
    AssetUrlError::InvalidStaticUrl {
        value: value.to_string(),
        reason,
    }
}

fn normalize_mount(value: &str) -> Result<String, AssetUrlError> {
    if !value.starts_with('/') || value.contains('?') || value.contains('#') {
        return Err(invalid_static(
            value,
            "must be a root-relative path without query or fragment",
        ));
    }
    let mount = value.trim_end_matches('/');
    if mount.is_empty() {
        return Err(invalid_static(value, "must not be the site root"));
    }
    validate_segments(mount.trim_start_matches('/'), value, invalid_static)?;
    Ok(mount.to_string())
}

fn validate_asset_path(path: &str) -> Result<(), AssetUrlError> {
    if path.is_empty() || path.starts_with('/') || path.contains('?') || path.contains('#') {
        return Err(AssetUrlError::InvalidPath {
            path: path.to_string(),
            reason: "must be a non-empty relative path without query or fragment",
        });
    }
    validate_segments(path, path, |value, reason| AssetUrlError::InvalidPath {
        path: value.to_string(),
        reason,
    })
}

fn validate_segments<E>(
    value: &str,
    original: &str,
    invalid: impl Fn(&str, &'static str) -> E,
) -> Result<(), E> {
    for segment in value.split('/') {
        let decoded = percent_decode_str(segment)
            .decode_utf8()
            .map_err(|_| invalid(original, "must use valid UTF-8 percent encoding"))?;
        if segment.is_empty() || decoded.is_empty() || matches!(decoded.as_ref(), "." | "..") {
            return Err(invalid(
                original,
                "must not contain empty or traversal segments",
            ));
        }
        if decoded.contains('/') || decoded.contains('\\') || decoded.contains('\0') {
            return Err(invalid(
                original,
                "must not contain path separators or NUL bytes",
            ));
        }
    }
    Ok(())
}

#[derive(Clone)]
pub struct AssetServe {
    silos: Arc<SiloSet>,
    prefix: Arc<str>,
    url_prefix: Arc<str>,
    precompressed: bool,
    etag: bool,
    etag_cache: Arc<RwLock<HashMap<String, String>>>,
}

impl AssetServe {
    /// `folder` is the silo-root folder (e.g. "www" or "www/assets")
    pub fn new(silos: SiloSet, folder: &str) -> Self {
        Self {
            silos: Arc::new(silos),
            prefix: normalize_prefix(folder).into(),
            url_prefix: Arc::from(""),
            precompressed: false,
            etag: false,
            etag_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn from_dirs(dirs: Vec<embed::Dir>, folder: &str) -> Self {
        let silos = dirs.into_iter().map(embed::Dir::into_silo).collect();
        Self::new(SiloSet::new(silos), folder)
    }

    pub fn strip_url_prefix(mut self, prefix: &str) -> Self {
        self.url_prefix = normalize_prefix(prefix).into();
        self
    }

    /// If enabled, will try `.br` then `.gz` variants based on Accept-Encoding.
    pub fn precompressed(mut self, enabled: bool) -> Self {
        self.precompressed = enabled;
        self
    }

    /// If enabled, returns strong ETags computed from served bytes (works for br/gz too).
    pub fn with_etag(mut self, enabled: bool) -> Self {
        self.etag = enabled;
        self
    }
}

impl Service<Request> for AssetServe {
    type Response = Response;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Response, Infallible>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let method = req.method().clone();
        let raw_path = req.uri().path().to_string();

        let silos = Arc::clone(&self.silos);
        let prefix = Arc::clone(&self.prefix);
        let url_prefix = Arc::clone(&self.url_prefix);
        let precompressed = self.precompressed;
        let use_etag = self.etag;
        let cache = Arc::clone(&self.etag_cache);

        let accept_encoding = req
            .headers()
            .get(header::ACCEPT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let if_none_match = if use_etag {
            req.headers()
                .get(header::IF_NONE_MATCH)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
        } else {
            None
        };

        Box::pin(async move {
            Ok(serve_file_impl(
                &silos,
                &prefix,
                &url_prefix,
                &method,
                &raw_path,
                precompressed,
                accept_encoding.as_deref(),
                if_none_match.as_deref(),
                use_etag,
                &cache,
            )
            .await)
        })
    }
}

async fn serve_file_impl(
    silos: &SiloSet,
    prefix: &str,
    url_prefix: &str,
    method: &Method,
    raw_path: &str,
    precompressed: bool,
    accept_encoding: Option<&str>,
    if_none_match: Option<&str>,
    use_etag: bool,
    cache: &RwLock<HashMap<String, String>>,
) -> Response {
    // Only GET/HEAD for static
    if *method != Method::GET && *method != Method::HEAD {
        return Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Body::empty())
            .unwrap();
    }

    // Decode & normalize path safely
    let clean_rel =
        match clean_rel_path(raw_path).and_then(|path| strip_url_prefix(path, url_prefix)) {
            Some(p) => p,
            None => return not_found(),
        };

    // Build lookup path inside silo root
    let logical_path = join_prefix(prefix, &clean_rel);

    // Select and read bytes (possibly precompressed variant)
    let (served_path, bytes, content_encoding) =
        match read_best_variant(silos, &logical_path, precompressed, accept_encoding).await {
            Some(v) => v,
            None => return not_found(),
        };

    // Compute or retrieve cached ETag
    let etag_val = if use_etag {
        Some(get_or_compute_etag(cache, &served_path, &bytes, silos).await)
    } else {
        None
    };

    if let (Some(etag), Some(client_etag)) = (etag_val.as_deref(), if_none_match) {
        // Very simple exact match. (If client sends a list, you can extend later.)
        if client_etag.trim() == etag {
            return not_modified(etag, precompressed);
        }
    }

    // Build response headers
    let mut builder = Response::builder().status(StatusCode::OK);

    // Content-Type
    let mime = guess_mime(&served_path);
    builder = builder.header(header::CONTENT_TYPE, mime);

    // Content-Encoding for br/gz variants
    if let Some(enc) = content_encoding {
        builder = builder.header(header::CONTENT_ENCODING, enc);
    }

    // Vary if we do content negotiation
    if precompressed {
        builder = builder.header(header::VARY, "Accept-Encoding");
    }

    // Cache-Control policy
    builder = builder.header(header::CACHE_CONTROL, cache_control_for(&served_path));

    // ETag
    if let Some(etag) = etag_val.as_deref() {
        builder = builder.header(header::ETAG, etag);
    }

    // Content-Length
    builder = builder.header(header::CONTENT_LENGTH, bytes.len().to_string());

    // HEAD returns headers only
    if *method == Method::HEAD {
        return builder.body(Body::empty()).unwrap();
    }

    builder.body(Body::from(bytes)).unwrap()
}

async fn read_best_variant(
    silos: &SiloSet,
    logical_path: &str,
    precompressed: bool,
    accept_encoding: Option<&str>,
) -> Option<(String, Bytes, Option<&'static str>)> {
    if !precompressed {
        let bytes = try_read_file(silos, logical_path).await?;
        return Some((logical_path.to_string(), bytes, None));
    }

    // Prefer br then gzip, but respect Accept-Encoding q=0
    let ae = AcceptEncoding::parse(accept_encoding);

    if ae.allows("br") {
        let p = format!("{logical_path}.br");
        if let Some(bytes) = try_read_file(silos, &p).await {
            return Some((p, bytes, Some("br")));
        }
    }

    if ae.allows("gzip") || ae.allows("gz") {
        let p = format!("{logical_path}.gz");
        if let Some(bytes) = try_read_file(silos, &p).await {
            return Some((p, bytes, Some("gzip")));
        }
    }

    // Fallback to identity
    let bytes = try_read_file(silos, logical_path).await?;
    Some((logical_path.to_string(), bytes, None))
}

async fn try_read_file(silos: &SiloSet, path: &str) -> Option<Bytes> {
    let file = silos.get_file(path)?;

    if file.is_embedded() {
        let mut reader = file.reader().ok()?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).ok()?;
        Some(Bytes::from(buf))
    } else {
        let path = file.absolute_path()?.to_path_buf();
        tokio::fs::read(path).await.ok().map(Bytes::from)
    }
}

/// Safely convert a URL path ("/a/b/../c") into a clean relative path ("a/b/c").
/// - percent-decodes
/// - rejects parent-dir and backslashes
/// - strips leading slashes
fn clean_rel_path(raw_path: &str) -> Option<String> {
    // raw_path is URI path (no query). Still, percent-decoding is needed.
    let stripped = raw_path.trim_start_matches('/');

    // Percent-decode. If invalid UTF-8, reject.
    let decoded = percent_decode_str(stripped).decode_utf8().ok()?;

    // Reject any backslashes early (Windows path games)
    if decoded.contains('\\') {
        return None;
    }

    // Normalize segments, rejecting ".." and "."
    let mut out = String::with_capacity(decoded.len());
    for seg in decoded.split('/') {
        if seg.is_empty() {
            continue;
        }
        if seg == "." || seg == ".." {
            return None;
        }
        // Disallow NUL or other weirdness
        if seg.contains('\0') {
            return None;
        }
        if !out.is_empty() {
            out.push('/');
        }
        out.push_str(seg);
    }

    Some(out)
}

fn normalize_prefix(folder: &str) -> String {
    if folder.is_empty() {
        return String::new();
    }
    let trimmed = folder.trim_matches('/');
    let mut s = String::with_capacity(trimmed.len() + 1);
    s.push_str(trimmed);
    s.push('/');
    s
}

/// Removes one complete nested-service path prefix without producing a leading slash.
fn strip_url_prefix(path: String, prefix: &str) -> Option<String> {
    if prefix.is_empty() {
        return Some(path);
    }
    if path == prefix.trim_end_matches('/') {
        return Some(String::new());
    }
    let Some(rest) = path.strip_prefix(prefix) else {
        return Some(path);
    };
    rest.strip_prefix('/').map(str::to_string).or(Some(path))
}

fn join_prefix(prefix: &str, rel: &str) -> String {
    if prefix.is_empty() {
        rel.to_string()
    } else if rel.is_empty() {
        // allow serving prefix root? usually not used; kept for completeness
        prefix.trim_end_matches('/').to_string()
    } else {
        let mut result = String::with_capacity(prefix.len() + rel.len());
        result.push_str(prefix);
        result.push_str(rel);
        result
    }
}

fn guess_mime(path: &str) -> String {
    // mime_guess returns a Mime; include charset for text types if you want.
    let guess: MimeGuess = mime_guess::from_path(path);
    guess.first_or_octet_stream().essence_str().to_string()
}

fn cache_control_for(path: &str) -> &'static str {
    // Conservative rule:
    // - HTML: no-cache (avoid hard-stale pages)
    // - Everything else: long cache, immutable-ish (best when filenames are hashed)
    if path.ends_with(".html") {
        "no-cache"
    } else {
        "public, max-age=31536000, immutable"
    }
}

async fn get_or_compute_etag(
    cache: &RwLock<HashMap<String, String>>,
    path: &str,
    bytes: &Bytes,
    silos: &SiloSet,
) -> String {
    let file = silos.get_file(path);
    let is_embedded = file.as_ref().map(|f| f.is_embedded()).unwrap_or(false);

    let cache_key = if is_embedded {
        path.to_string()
    } else {
        match file.as_ref().and_then(|f| std::fs::metadata(f.path()).ok()) {
            Some(meta) => {
                let mtime = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|d| d.as_secs())
                    .unwrap_or(0);
                format!("{path}:{mtime}:{}", meta.len())
            }
            None => return strong_etag(bytes),
        }
    };

    {
        let cache_read = cache.read();
        if let Some(etag) = cache_read.get(&cache_key) {
            return etag.clone();
        }
    }

    let etag = strong_etag(bytes);
    cache.write().insert(cache_key, etag.clone());
    etag
}

fn strong_etag(bytes: &Bytes) -> String {
    let mut h = Blake3::new();
    h.update(bytes);
    let digest = h.finalize();
    format!("\"{}\"", digest.to_hex())
}

fn not_modified(etag: &str, precompressed: bool) -> Response {
    let mut builder = Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .header(header::ETAG, etag);

    if precompressed {
        builder = builder.header(header::VARY, "Accept-Encoding");
    }

    builder.body(Body::empty()).unwrap()
}

fn not_found() -> Response {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .unwrap()
}

/// Minimal Accept-Encoding parser that respects `q=0` disable.
/// Not a full RFC implementation, but avoids the biggest correctness bug.
#[derive(Debug, Clone)]
struct AcceptEncoding {
    br_q: f32,
    gzip_q: f32,
    star_q: f32,
}

impl AcceptEncoding {
    fn parse(h: Option<&str>) -> Self {
        // Defaults: identity implied; encodings not listed are not allowed unless '*'
        let mut ae = AcceptEncoding {
            br_q: -1.0,
            gzip_q: -1.0,
            star_q: -1.0,
        };

        let Some(s) = h else { return ae };

        for part in s.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            let mut pieces = part.split(';').map(|x| x.trim());
            let enc = pieces.next().unwrap_or("");
            let mut q = 1.0f32;

            for p in pieces {
                if let Some(v) = p.strip_prefix("q=") {
                    if let Ok(val) = v.parse::<f32>() {
                        q = val;
                    }
                }
            }

            match enc {
                "br" => ae.br_q = q,
                "gzip" | "gz" => ae.gzip_q = q,
                "*" => ae.star_q = q,
                _ => {}
            }
        }

        ae
    }

    fn allows(&self, enc: &str) -> bool {
        let q = match enc {
            "br" => self.br_q,
            "gzip" | "gz" => self.gzip_q,
            _ => -1.0,
        };

        if q >= 0.0 {
            return q > 0.0;
        }

        // Not explicitly mentioned: only allowed if '*' has q>0
        if self.star_q >= 0.0 {
            return self.star_q > 0.0;
        }

        // Otherwise: treat as not allowed
        false
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use rust_silos::{EmbedEntry, Silo, SiloSet};

    use super::AssetUrls;

    static EMBEDDED_ASSETS: [(&str, EmbedEntry); 1] = [(
        "public/console/app.js",
        EmbedEntry {
            path: "public/console/app.js",
            contents: b"embedded asset",
            size: b"embedded asset".len(),
            modified: 0,
        },
    )];

    /// Verifies root-relative static URLs build safe relative asset links.
    #[test]
    fn relative_static_urls_build_asset_links() {
        let urls = AssetUrls::parse("/static");
        assert!(urls.is_ok());
        let Some(urls) = urls.ok() else {
            return;
        };

        assert_eq!(urls.static_url(), "/static/");
        assert_eq!(urls.mount_path(), "/static");
        assert_eq!(
            urls.url("console/js/app file.js"),
            Ok("/static/console/js/app%20file.js".to_string())
        );
    }

    /// Verifies absolute static URLs preserve their configured CDN origin.
    #[test]
    fn absolute_static_urls_build_cdn_links() {
        let urls = AssetUrls::parse("https://cdn.example.com/static");
        assert!(urls.is_ok());
        let Some(urls) = urls.ok() else {
            return;
        };

        assert_eq!(urls.static_url(), "https://cdn.example.com/static/");
        assert_eq!(urls.mount_path(), "/static");
        assert_eq!(
            urls.url("blog/app.js"),
            Ok("https://cdn.example.com/static/blog/app.js".to_string())
        );
    }

    /// Verifies static URL and asset path traversal attempts are rejected.
    #[test]
    fn static_urls_reject_unsafe_paths() {
        assert!(AssetUrls::parse("/").is_err());
        assert!(AssetUrls::parse("/static/../private").is_err());
        let urls = AssetUrls::default_url();
        assert!(urls.url("/logo.svg").is_err());
        assert!(urls.url("console/../logo.svg").is_err());
        assert!(urls.url("console/app.js?cache=1").is_err());
    }

    /// Verifies embedded assets use their in-memory reader and dynamic assets use absolute paths.
    #[tokio::test]
    async fn reads_embedded_and_dynamic_silo_assets() -> Result<(), io::Error> {
        let embedded = SiloSet::new(vec![Silo::from_embedded(&EMBEDDED_ASSETS, "embedded")]);
        assert_eq!(
            super::try_read_file(&embedded, "public/console/app.js").await,
            Some(bytes::Bytes::from_static(b"embedded asset"))
        );

        let root = tempfile::tempdir()?;
        let file = root.path().join("public/console/app.js");
        std::fs::create_dir_all(
            file.parent()
                .ok_or_else(|| io::Error::other("asset parent"))?,
        )?;
        std::fs::write(&file, "dynamic asset")?;
        let root = root
            .path()
            .to_str()
            .ok_or_else(|| io::Error::other("non-UTF-8 asset root"))?;
        let dynamic = SiloSet::new(vec![Silo::new(root)]);

        assert_eq!(
            super::try_read_file(&dynamic, "public/console/app.js").await,
            Some(bytes::Bytes::from_static(b"dynamic asset"))
        );
        Ok(())
    }
}
