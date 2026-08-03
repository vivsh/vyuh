use std::borrow::Cow;
use std::ops::Deref;

use axum::body::Body;
use axum::body::Bytes;
use axum::extract::{FromRequest, FromRequestParts, Request};
use axum::http::{HeaderValue, StatusCode, header, request::Parts};
use axum::response::{IntoResponse, Response};
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::errors::ErrorReport;
use crate::middlewares::SlashPolicy;

use super::methods::Methods;

#[derive(Debug, Clone, Copy, Default)]
pub struct Json<T>(pub T);

#[derive(Debug, Clone, Copy, Default)]
pub struct Query<T>(pub T);

#[derive(Debug, Clone, Copy, Default)]
pub struct Path<T>(pub T);

#[derive(Debug, Clone, Copy, Default)]
pub struct Form<T>(pub T);

/// Standard JSON success payload for mutation endpoints.
#[derive(Debug, Clone, Copy, Default, Serialize, JsonSchema)]
pub struct OkOut {
    pub ok: bool,
}

/// JSON success response with a stable `{ "ok": true }` body.
#[derive(Debug, Clone, Copy, Default)]
pub struct OkJson;

/// JSON response wrapper that can be mutated before it is returned.
///
/// This is useful for login/logout handlers that need to attach cookies while
/// keeping a concrete JSON response schema for generated API docs.
pub struct CookieJson<T> {
    response: Response,
    _marker: std::marker::PhantomData<fn() -> T>,
}

/// Raw pagination query input shared by list endpoints.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema)]
pub struct PageParams {
    #[serde(default)]
    pub page: Option<usize>,
    #[serde(default)]
    pub per_page: Option<usize>,
}

/// Resolved one-indexed pagination bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageBounds {
    pub page: usize,
    pub per_page: usize,
}

/// Backendless equivalent of Mool's canonical paginated result envelope.
#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
#[derive(Clone, Debug, Serialize, JsonSchema)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub total: i64,
    pub page: usize,
    pub per_page: usize,
    pub total_pages: usize,
}

#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
impl<T> Page<T> {
    /// Builds the canonical page metadata for a backendless task store.
    pub fn new(items: Vec<T>, total: i64, page: usize, per_page: usize) -> Self {
        let total_pages = if per_page == 0 {
            0
        } else {
            usize::try_from(total.max(0)).map_or(usize::MAX, |value| value.div_ceil(per_page))
        };
        Self {
            items,
            total,
            page,
            per_page,
            total_pages,
        }
    }

    /// Maps page items while preserving pagination metadata.
    pub fn map<U>(self, mapper: impl FnMut(T) -> U) -> Page<U> {
        Page {
            items: self.items.into_iter().map(mapper).collect(),
            total: self.total,
            page: self.page,
            per_page: self.per_page,
            total_pages: self.total_pages,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BodyBytes(pub Bytes);

macro_rules! impl_wrapper {
    ($name:ident) => {
        impl<T> $name<T> {
            pub fn into_inner(self) -> T {
                self.0
            }
        }

        impl<T> Deref for $name<T> {
            type Target = T;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl<T> AsRef<T> for $name<T> {
            fn as_ref(&self) -> &T {
                &self.0
            }
        }
    };
}

impl_wrapper!(Json);
impl_wrapper!(Query);
impl_wrapper!(Path);
impl_wrapper!(Form);

impl OkOut {
    /// Returns a successful mutation payload.
    pub fn ok() -> Self {
        Self { ok: true }
    }
}

impl IntoResponse for OkJson {
    fn into_response(self) -> Response {
        Json(OkOut::ok()).into_response()
    }
}

impl<T> CookieJson<T>
where
    T: Serialize,
{
    /// Creates a JSON response whose headers or cookies can still be changed.
    pub fn new(body: T) -> Self {
        Self {
            response: Json(body).into_response(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns the mutable response for cookie/header changes.
    pub fn response_mut(&mut self) -> &mut Response {
        &mut self.response
    }
}

impl<T> IntoResponse for CookieJson<T> {
    fn into_response(self) -> Response {
        self.response
    }
}

impl PageParams {
    /// Resolves raw query input into bounded pagination values.
    ///
    /// Page numbers are one-indexed. `default_per_page` and `max_per_page` are
    /// clamped to at least one so callers cannot accidentally create a
    /// zero-sized page.
    pub fn resolve(self, default_per_page: usize, max_per_page: usize) -> PageBounds {
        let max_per_page = max_per_page.max(1);
        let default_per_page = default_per_page.clamp(1, max_per_page);
        PageBounds {
            page: self.page.unwrap_or(1).max(1),
            per_page: self
                .per_page
                .unwrap_or(default_per_page)
                .clamp(1, max_per_page),
        }
    }
}

impl PageBounds {
    /// Returns the zero-indexed row offset for this page.
    pub fn offset(self) -> usize {
        (self.page - 1) * self.per_page
    }
}

impl BodyBytes {
    pub fn into_inner(self) -> Bytes {
        self.0
    }
}

impl Deref for BodyBytes {
    type Target = Bytes;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<Bytes> for BodyBytes {
    fn as_ref(&self) -> &Bytes {
        &self.0
    }
}

impl<T> IntoResponse for Json<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        axum::Json(self.0).into_response()
    }
}

impl<T, S> FromRequest<S> for Json<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ErrorReport;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        axum::Json::<T>::from_request(req, state)
            .await
            .map(|axum::Json(value)| Self(value))
            .map_err(|err| ErrorReport::bad_request(err.to_string()))
    }
}

impl<T, S> FromRequestParts<S> for Query<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ErrorReport;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        axum_extra::extract::Query::<T>::from_request_parts(parts, state)
            .await
            .map(|axum_extra::extract::Query(value)| Self(value))
            .map_err(|err| ErrorReport::bad_request(err.to_string()))
    }
}

impl<T, S> FromRequestParts<S> for Path<T>
where
    T: DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = ErrorReport;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        axum::extract::Path::<T>::from_request_parts(parts, state)
            .await
            .map(|axum::extract::Path(value)| Self(value))
            .map_err(|err| ErrorReport::bad_request(err.to_string()))
    }
}

impl<T, S> FromRequest<S> for Form<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ErrorReport;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        axum_extra::extract::Form::<T>::from_request(req, state)
            .await
            .map(|axum_extra::extract::Form(value)| Self(value))
            .map_err(|err| ErrorReport::bad_request(err.to_string()))
    }
}

impl<S> FromRequest<S> for BodyBytes
where
    S: Send + Sync,
{
    type Rejection = ErrorReport;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        Bytes::from_request(req, state)
            .await
            .map(Self)
            .map_err(|err| ErrorReport::bad_request(err.to_string()))
    }
}

/// Returns a string body with `application/json` content type.
///
/// Intentionally lightweight — assumes the inner value is already valid JSON.
/// Useful for returning pre-serialized JSON strings without additional overhead.
#[derive(Debug, Clone)]
pub struct JsonStr {
    inner: Cow<'static, str>,
}

impl From<&'static str> for JsonStr {
    fn from(value: &'static str) -> Self {
        Self {
            inner: Cow::Borrowed(value),
        }
    }
}

impl From<String> for JsonStr {
    fn from(value: String) -> Self {
        Self {
            inner: Cow::Owned(value),
        }
    }
}

impl IntoResponse for JsonStr {
    fn into_response(self) -> Response {
        let mut res = self.inner.into_owned().into_response();
        res.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        res
    }
}

/// JSON response with HTTP `201 Created`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Created<T>(pub T);

impl<T> Created<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> IntoResponse for Created<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        (StatusCode::CREATED, Json(self.0)).into_response()
    }
}

/// JSON response with HTTP `202 Accepted`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Accepted<T>(pub T);

impl<T> Accepted<T> {
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> IntoResponse for Accepted<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        (StatusCode::ACCEPTED, Json(self.0)).into_response()
    }
}

/// Redirect response using `307 Temporary Redirect`.
#[derive(Debug, Clone)]
pub struct TemporaryRedirect {
    location: Cow<'static, str>,
}

impl TemporaryRedirect {
    pub fn to(location: impl Into<Cow<'static, str>>) -> Self {
        Self {
            location: location.into(),
        }
    }
}

impl IntoResponse for TemporaryRedirect {
    fn into_response(self) -> Response {
        (
            StatusCode::TEMPORARY_REDIRECT,
            [(header::LOCATION, self.location.into_owned())],
        )
            .into_response()
    }
}

/// Redirect response using `308 Permanent Redirect`.
#[derive(Debug, Clone)]
pub struct PermanentRedirect {
    location: Cow<'static, str>,
}

impl PermanentRedirect {
    pub fn to(location: impl Into<Cow<'static, str>>) -> Self {
        Self {
            location: location.into(),
        }
    }
}

impl IntoResponse for PermanentRedirect {
    fn into_response(self) -> Response {
        (
            StatusCode::PERMANENT_REDIRECT,
            [(header::LOCATION, self.location.into_owned())],
        )
            .into_response()
    }
}

/// Redirect response constructors using Vyuh's 307/308 redirect types.
pub mod redirect {
    use super::{PermanentRedirect, TemporaryRedirect};

    /// Creates a `307 Temporary Redirect` response.
    pub fn to(location: impl Into<std::borrow::Cow<'static, str>>) -> TemporaryRedirect {
        TemporaryRedirect::to(location)
    }

    /// Creates a `308 Permanent Redirect` response.
    pub fn permanent(location: impl Into<std::borrow::Cow<'static, str>>) -> PermanentRedirect {
        PermanentRedirect::to(location)
    }
}

/// Binary file response with a known content type.
pub struct FileResponse {
    body: Body,
    content_type: Cow<'static, str>,
}

impl FileResponse {
    pub fn new(body: Body, content_type: impl Into<Cow<'static, str>>) -> Self {
        Self {
            body,
            content_type: content_type.into(),
        }
    }
}

impl IntoResponse for FileResponse {
    fn into_response(self) -> Response {
        (
            StatusCode::OK,
            [(header::CONTENT_TYPE, self.content_type.into_owned())],
            self.body,
        )
            .into_response()
    }
}

/// Streaming response with a known content type.
pub struct StreamResponse {
    inner: FileResponse,
}

impl StreamResponse {
    pub fn new(body: Body, content_type: impl Into<Cow<'static, str>>) -> Self {
        Self {
            inner: FileResponse::new(body, content_type),
        }
    }
}

impl IntoResponse for StreamResponse {
    fn into_response(self) -> Response {
        self.inner.into_response()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RouteConf {
    /// Logical name (used for reverse URLs, docs, etc.)
    pub name: Cow<'static, str>,
    /// HTTP methods supported by this view.
    pub methods: Methods,
    /// Full path, including base path if any (e.g. "/api/users/{id}").
    pub path: Cow<'static, str>,
    /// Optional route-level slash behavior.
    pub slash: Option<SlashPolicy>,
}

impl Default for RouteConf {
    fn default() -> Self {
        Self {
            name: Cow::Borrowed(""),
            methods: Methods::GET,
            path: Cow::Borrowed("/"),
            slash: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::header;

    use super::{CookieJson, IntoResponse, Json, OkJson, OkOut, PageParams};

    /// Verifies that page query input is normalized to safe bounds.
    #[test]
    fn page_params_resolve_bounds() {
        let params = PageParams {
            page: Some(0),
            per_page: Some(500),
        };
        let page = params.resolve(20, 100);
        assert_eq!(page.page, 1);
        assert_eq!(page.per_page, 100);
        assert_eq!(page.offset(), 0);
    }

    /// Verifies that default page size is clamped when routes configure it badly.
    #[test]
    fn page_params_clamp_default() {
        let page = PageParams::default().resolve(0, 0);
        assert_eq!(page.page, 1);
        assert_eq!(page.per_page, 1);
    }

    /// Verifies that cookie JSON exposes a mutable response before return.
    #[test]
    fn cookie_json_mutates_headers() {
        let mut response = CookieJson::new(OkOut::ok());
        response.response_mut().headers_mut().insert(
            header::CACHE_CONTROL,
            axum::http::HeaderValue::from_static("no-store"),
        );
        assert!(
            response
                .into_response()
                .headers()
                .contains_key(header::CACHE_CONTROL)
        );
    }

    /// Verifies that the standard OK response uses JSON response semantics.
    #[test]
    fn ok_json_returns_success() {
        let response = OkJson.into_response();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let json = Json(OkOut::ok()).into_response();
        assert_eq!(json.status(), response.status());
    }

    /// Verifies the route pagination export remains Mool's exact result envelope.
    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    #[test]
    fn page_reexports_mool_envelope() -> Result<(), serde_json::Error> {
        let page: crate::routes::Page<&str> = crate::db::Page::new(vec!["one"], 3, 1, 2);
        let value = serde_json::to_value(page)?;
        let object = value.as_object();

        assert_eq!(
            object.and_then(|value| value.get("items")),
            Some(&serde_json::json!(["one"]))
        );
        assert_eq!(
            object.and_then(|value| value.get("total")),
            Some(&serde_json::json!(3))
        );
        assert_eq!(
            object.and_then(|value| value.get("page")),
            Some(&serde_json::json!(1))
        );
        assert_eq!(
            object.and_then(|value| value.get("per_page")),
            Some(&serde_json::json!(2))
        );
        assert_eq!(
            object.and_then(|value| value.get("total_pages")),
            Some(&serde_json::json!(2))
        );
        Ok(())
    }
}
