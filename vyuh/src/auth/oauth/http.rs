//! Bounded HTTPS transport adapter for Huskarl discovery and JWKS loading.

use std::time::Duration;

#[cfg(feature = "oauth")]
use std::error::Error as _;

use axum::http::{Request, StatusCode};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use huskarl_resource_server::core::{
    Error, ErrorKind,
    http::{HttpClient, HttpResponse, Idempotency},
    platform::MaybeSendBoxFuture,
};
use reqwest::redirect::Policy;
use thiserror::Error;

const MAX_DOCUMENT_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub(crate) struct AuthHttpClient {
    client: reqwest::Client,
}

impl AuthHttpClient {
    pub(crate) fn build() -> Result<Self, Error> {
        let client = reqwest::Client::builder()
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
        let body = read_bounded(response, idempotency).await?;
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
}

fn transport_setup_error(source: reqwest::Error) -> Error {
    Error::new(ErrorKind::Config, OAuthHttpError::Setup(source))
}

fn transport_error(source: reqwest::Error, idempotency: Idempotency) -> Error {
    let retryable = source.is_connect()
        || (matches!(idempotency, Idempotency::Idempotent)
            && (source.is_timeout() || source.is_body()));
    Error::new(
        ErrorKind::Transport { retryable },
        OAuthHttpError::Request(source),
    )
}

pub(super) fn status_error(status: StatusCode) -> Error {
    Error::new(ErrorKind::Protocol, OAuthHttpError::Status(status))
}

async fn read_bounded(
    response: reqwest::Response,
    idempotency: Idempotency,
) -> Result<Bytes, Error> {
    if response
        .content_length()
        .is_some_and(|size| size > MAX_DOCUMENT_BYTES as u64)
    {
        return Err(Error::new(ErrorKind::Protocol, OAuthHttpError::Oversized));
    }
    let mut result = BytesMut::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| transport_error(error, idempotency))?;
        if result.len().saturating_add(chunk.len()) > MAX_DOCUMENT_BYTES {
            return Err(Error::new(ErrorKind::Protocol, OAuthHttpError::Oversized));
        }
        result.extend_from_slice(&chunk);
    }
    Ok(result.freeze())
}

#[cfg(feature = "oauth")]
pub(crate) fn unsupported_discovery(error: &Error) -> bool {
    let mut source = error.source();
    while let Some(current) = source {
        if let Some(OAuthHttpError::Status(status)) = current.downcast_ref::<OAuthHttpError>() {
            return matches!(*status, StatusCode::NOT_FOUND | StatusCode::GONE);
        }
        source = current.source();
    }
    false
}

fn validate_transport_url(value: &str) -> Result<(), Error> {
    validate_remote_url(value)
        .map_err(|_| Error::new(ErrorKind::Protocol, OAuthHttpError::UnsafeUrl))
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
