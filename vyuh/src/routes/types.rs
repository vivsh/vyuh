use std::borrow::Cow;
use std::ops::Deref;

use axum::body::Body;
use axum::body::Bytes;
use axum::extract::{FromRequest, FromRequestParts, Request};
use axum::http::{HeaderValue, StatusCode, header, request::Parts};
use axum::response::{IntoResponse, Response};
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
