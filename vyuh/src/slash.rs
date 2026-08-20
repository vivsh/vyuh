//! Canonical trailing-slash normalization around the finalized Axum router.

use std::{
    convert::Infallible,
    future::{Ready, ready},
    task::{Context, Poll},
};

use axum::{
    BoxError, Router,
    body::HttpBody,
    extract::OriginalUri,
    http::{Request, Uri},
    response::{IntoResponse, Redirect, Response},
    routing::MethodRouter,
};
use bytes::Bytes;
use futures::future::Either;
use tower::{Layer, Service};

use crate::ErrorReport;

#[derive(Clone, Copy)]
pub(crate) enum RouteSlash {
    Redirect,
    Reject,
}

#[derive(Clone, Copy)]
pub(crate) struct RouteSlashLayer {
    mode: RouteSlash,
}

impl RouteSlashLayer {
    pub(crate) const fn redirect() -> Self {
        Self {
            mode: RouteSlash::Redirect,
        }
    }

    pub(crate) const fn reject() -> Self {
        Self {
            mode: RouteSlash::Reject,
        }
    }
}

/// Registers one framework route at its normalized internal path.
pub(crate) fn route<S>(router: Router<S>, path: &str, method: MethodRouter<S>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let method = if has_slash(path) {
        method.route_layer(RouteSlashLayer::redirect())
    } else {
        method
    };
    router.route(internal_path(path), method)
}

impl<S> Layer<S> for RouteSlashLayer {
    type Service = RouteSlashService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RouteSlashService {
            inner,
            mode: self.mode,
        }
    }
}

#[derive(Clone)]
pub(crate) struct RouteSlashService<S> {
    inner: S,
    mode: RouteSlash,
}

impl<S> Service<axum::extract::Request> for RouteSlashService<S>
where
    S: Service<axum::extract::Request, Response = Response, Error = Infallible>,
{
    type Response = Response;
    type Error = Infallible;
    type Future = Either<S::Future, Ready<Result<Response, Infallible>>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: axum::extract::Request) -> Self::Future {
        let trimmed = req.extensions().get::<TrailingSlashTrimmed>().is_some();
        match (self.mode, trimmed) {
            (RouteSlash::Redirect, false) => Either::Right(ready(Ok(redirect(&req)))),
            (RouteSlash::Reject, true) => {
                Either::Right(ready(Ok(ErrorReport::not_found().into_response())))
            }
            _ => Either::Left(self.inner.call(req)),
        }
    }
}

#[derive(Clone, Copy)]
struct TrailingSlashTrimmed;

/// Cloneable HTTP service that normalizes a request before Axum route selection.
#[derive(Clone)]
pub struct SiteService {
    inner: Router,
}

impl SiteService {
    pub(crate) const fn new(inner: Router) -> Self {
        Self { inner }
    }
}

impl<B> Service<Request<B>> for SiteService
where
    B: HttpBody<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    type Response = Response;
    type Error = Infallible;
    type Future = <Router as Service<Request<B>>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <Router as Service<Request<B>>>::poll_ready(&mut self.inner, cx)
    }

    fn call(&mut self, mut req: Request<B>) -> Self::Future {
        normalize_request(&mut req);
        self.inner.call(req)
    }
}

/// Normalizes one terminal slash while retaining the original request URI.
fn normalize_request<B>(req: &mut Request<B>) {
    if !has_slash(req.uri().path()) {
        return;
    }
    let original = std::mem::take(req.uri_mut());
    let Some(normalized) = trim_uri(&original) else {
        *req.uri_mut() = original;
        return;
    };
    *req.uri_mut() = normalized;
    req.extensions_mut().insert(OriginalUri(original));
    req.extensions_mut().insert(TrailingSlashTrimmed);
}

/// Returns the single slashless path stored in Axum's route table.
pub(crate) fn internal_path(path: &str) -> &str {
    if path == "/" {
        path
    } else {
        path.strip_suffix('/').unwrap_or(path)
    }
}

fn has_slash(path: &str) -> bool {
    path != "/" && path.ends_with('/')
}

fn trim_uri(uri: &Uri) -> Option<Uri> {
    let target = internal_path(uri.path());
    let path_and_query = with_query(target, uri.query()).parse().ok()?;
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = Some(path_and_query);
    Uri::from_parts(parts).ok()
}

/// Redirects to the public slashful declaration while retaining its query string.
fn redirect(request: &axum::extract::Request) -> Response {
    let uri = request
        .extensions()
        .get::<OriginalUri>()
        .map_or_else(|| request.uri(), |original| &original.0);
    let query = uri.query();
    let mut target = String::with_capacity(
        uri.path().len() + query.map_or(1, |value| value.len().saturating_add(2)),
    );
    target.push_str(uri.path());
    target.push('/');
    if let Some(query) = query {
        target.push('?');
        target.push_str(query);
    }
    Redirect::permanent(&target).into_response()
}

fn with_query(path: &str, query: Option<&str>) -> String {
    match query {
        Some(query) => format!("{path}?{query}"),
        None => path.to_owned(),
    }
}
