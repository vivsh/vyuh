//! Bounded HTTPS transport for Huskarl discovery, JWKS, and introspection.

use std::time::Duration;

#[cfg(feature = "oauth-resource")]
use axum::http::{Method, header};
use axum::http::{Request, StatusCode};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use huskarl_resource_server::core::{
    Error, RetryAdvice,
    http::{HttpClient, HttpResponse, Idempotency},
    platform::MaybeSendBoxFuture,
};
use reqwest::redirect::Policy;
use thiserror::Error;

const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;
#[cfg(feature = "oauth-resource")]
const MAX_INTROSPECTION_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub(crate) struct AuthHttpClient {
    client: reqwest::Client,
}

impl AuthHttpClient {
    pub(crate) fn build() -> Result<Self, Error> {
        Self::build_client(reqwest::Client::builder())
    }

    #[cfg(feature = "oauth-resource")]
    pub(super) fn introspection() -> Result<Self, Error> {
        Self::build_client(
            reqwest::Client::builder()
                .https_only(true)
                .retry(reqwest::retry::never()),
        )
    }

    fn build_client(builder: reqwest::ClientBuilder) -> Result<Self, Error> {
        let client = builder
            .redirect(Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(transport_setup_error)?;
        Ok(Self { client })
    }
}

impl HttpClient for AuthHttpClient {
    fn execute(
        &self,
        request: Request<Bytes>,
        idempotency: Idempotency,
    ) -> MaybeSendBoxFuture<'_, Result<HttpResponse, Error>> {
        Box::pin(async move { self.execute_inner(request, idempotency).await })
    }
}

impl AuthHttpClient {
    async fn execute_inner(
        &self,
        request: Request<Bytes>,
        idempotency: Idempotency,
    ) -> Result<HttpResponse, Error> {
        let (parts, body) = request.into_parts();
        validate_transport_url(&parts.uri.to_string())?;
        // In this adapter GET loads metadata or JWKS; only introspection sends POST.
        #[cfg(feature = "oauth-resource")]
        let introspection = parts.method == Method::POST;
        #[cfg(feature = "oauth-resource")]
        let limit = if introspection {
            MAX_INTROSPECTION_BYTES
        } else {
            MAX_DOCUMENT_BYTES
        };
        #[cfg(not(feature = "oauth-resource"))]
        let limit = MAX_DOCUMENT_BYTES;
        let response = self
            .client
            .request(parts.method, parts.uri.to_string())
            .headers(parts.headers)
            .body(body)
            .send()
            .await
            .map_err(|error| transport_error(error, idempotency))?;
        if !response.status().is_success() {
            return Err(status_error(response.status()));
        }
        let status = response.status();
        let headers = response.headers().clone();
        let body = read_bounded(response, idempotency, limit).await?;
        #[cfg(feature = "oauth-resource")]
        if introspection {
            validate_introspection_body(&headers, &body)?;
        }
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
}

#[derive(Debug, Error)]
enum OAuthHttpError {
    #[error("OAuth HTTP client construction failed")]
    Setup(#[source] reqwest::Error),
    #[error("OAuth HTTP request failed")]
    Request(#[source] reqwest::Error),
    #[error("OAuth endpoint returned HTTP {0}")]
    Status(StatusCode),
    #[error("OAuth response exceeded the configured size limit")]
    Oversized,
    #[error("OAuth endpoint URL is unsafe")]
    UnsafeUrl,
    #[cfg(feature = "oauth-resource")]
    #[error("OAuth introspection returned an invalid JSON response")]
    InvalidIntrospection,
}

fn transport_setup_error(source: reqwest::Error) -> Error {
    Error::new(RetryAdvice::No, OAuthHttpError::Setup(source))
}

fn transport_error(source: reqwest::Error, idempotency: Idempotency) -> Error {
    let retryable = source.is_connect()
        || (matches!(idempotency, Idempotency::Idempotent)
            && (source.is_timeout() || source.is_body()));
    Error::new(
        RetryAdvice::retry_if(retryable),
        OAuthHttpError::Request(source),
    )
}

pub(super) fn status_error(status: StatusCode) -> Error {
    Error::new(RetryAdvice::No, OAuthHttpError::Status(status))
}

async fn read_bounded(
    response: reqwest::Response,
    idempotency: Idempotency,
    limit: usize,
) -> Result<Bytes, Error> {
    if response
        .content_length()
        .is_some_and(|size| size > limit as u64)
    {
        return Err(Error::new(RetryAdvice::No, OAuthHttpError::Oversized));
    }
    let mut result = BytesMut::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| transport_error(error, idempotency))?;
        if result.len().saturating_add(chunk.len()) > limit {
            return Err(Error::new(RetryAdvice::No, OAuthHttpError::Oversized));
        }
        result.extend_from_slice(&chunk);
    }
    Ok(result.freeze())
}

/// Accepts only bounded JSON objects with unique top-level members.
#[cfg(feature = "oauth-resource")]
pub(super) fn validate_introspection_body(
    headers: &axum::http::HeaderMap,
    body: &[u8],
) -> Result<(), Error> {
    if body.len() > MAX_INTROSPECTION_BYTES {
        return Err(Error::new(RetryAdvice::No, OAuthHttpError::Oversized));
    }
    let json = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"));
    if !json || crate::auth::codecs::unique_json_fields(body).is_err() {
        return Err(Error::new(
            RetryAdvice::No,
            OAuthHttpError::InvalidIntrospection,
        ));
    }
    Ok(())
}

#[cfg(feature = "oauth-resource")]
pub(crate) fn unsupported_discovery(error: &Error) -> bool {
    error
        .chain()
        .find_map(|source| {
            // Huskarl displays its cause directly and skips it in source().
            let cause = source.downcast_ref::<Error>().map_or(source, Error::cause);
            match cause.downcast_ref::<OAuthHttpError>()? {
                OAuthHttpError::Status(status) => Some(*status),
                _ => None,
            }
        })
        .is_some_and(|status| matches!(status, StatusCode::NOT_FOUND | StatusCode::GONE))
}

fn validate_transport_url(value: &str) -> Result<(), Error> {
    validate_remote_url(value).map_err(|_| Error::new(RetryAdvice::No, OAuthHttpError::UnsafeUrl))
}

/// Validates one clean HTTPS endpoint or an explicit loopback development endpoint.
pub(crate) fn validate_remote_url(value: &str) -> Result<(), crate::auth::AuthError> {
    match url::Url::parse(value) {
        Ok(url)
            if secure_scheme(&url)
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none() =>
        {
            Ok(())
        }
        _ => Err(crate::auth::AuthError::InvalidProviderConfig(
            "remote authentication URLs require HTTPS except for clean loopback development URLs"
                .into(),
        )),
    }
}

fn secure_scheme(url: &url::Url) -> bool {
    url.scheme() == "https"
        || (url.scheme() == "http"
            && url.host_str().is_some_and(|host| {
                host == "localhost"
                    || host
                        .parse::<std::net::IpAddr>()
                        .is_ok_and(|address| address.is_loopback())
            }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Authentication transport accepts HTTPS and explicit loopback development URLs.
    #[test]
    fn accepts_secure_and_loopback_urls() {
        assert!(validate_remote_url("https://identity.example.com").is_ok());
        assert!(validate_remote_url("http://localhost:8080").is_ok());
        assert!(validate_remote_url("http://127.0.0.1:8080").is_ok());
    }

    /// Authentication transport rejects untrusted schemes and ambiguous URL components.
    #[test]
    fn rejects_unsafe_remote_urls() {
        assert!(validate_remote_url("http://identity.example.com").is_err());
        assert!(validate_remote_url("https://user@identity.example.com").is_err());
        assert!(validate_remote_url("https://identity.example.com?key=value").is_err());
        assert!(validate_remote_url("https://identity.example.com#fragment").is_err());
    }
}
