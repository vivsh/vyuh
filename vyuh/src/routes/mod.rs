mod client_ip;
mod methods;
pub mod middleware;
pub mod multipart;
mod registry;
mod subscriber;
mod types;

#[cfg(feature = "cors")]
pub mod builtin;

// Response types
pub use axum::response::{AppendHeaders, Html, IntoResponse, NoContent, Redirect, Response};

// Routing helpers
pub use axum::routing::{
    Router as AxumRouter, any, delete, get, method_routing::MethodRouter, patch, post, put,
};

// Core extractors
pub use axum::extract::{
    Extension, FromRequest, FromRequestParts, MatchedPath, OriginalUri, RawQuery, Request, State,
};

// Extra extractors
pub use axum_extra::extract::TypedHeader;

// HTTP primitives
pub use axum::http::{HeaderMap, HeaderName, Method as HttpMethod, StatusCode, Uri};

// Body types
pub use axum::body::Body;

// Local types
pub use crate::Data;
/// Canonical Mool result envelope for paginated JSON responses.
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub use crate::db::Page;
pub use crate::validation::Valid;
pub use client_ip::ClientIp;
pub use methods::{MethodIter, Methods, ParseMethodError};
pub use middleware::{Middleware, RawLayer, layer_from};
pub use multipart::{JsonPart, MultipartForm, MultipartMap, UploadedFile, UploadedText};
pub(crate) use registry::RouteRegistry;
pub use registry::Routes;
pub use subscriber::{ChannelAttach, Subscriber};
#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
pub use types::Page;
pub use types::{
    Accepted, BodyBytes, CookieJson, Created, FileResponse, Form, Json, JsonStr, OkJson, OkOut,
    PageBounds, PageParams, Path, PermanentRedirect, Query, RouteConf, StreamResponse,
    TemporaryRedirect, redirect,
};

pub use crate::channels::{POLL, SSE, WS};

/// Explicit Axum escape hatch for applications that need raw Axum extractors.
pub mod axum_extractors {
    pub use axum::Json;
    pub use axum::extract::{
        Extension, FromRequest, FromRequestParts, MatchedPath, OriginalUri, Path, RawQuery,
        Request, State,
    };
    pub use axum_extra::extract::{Form, Multipart, Query, TypedHeader};
}

#[cfg(feature = "cors")]
pub use builtin::CorsMiddleware;
