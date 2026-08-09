//! Bounded HTTP transport for federated discovery, exchange, and profiles.

use std::{future::Future, pin::Pin, time::Duration};

use axum::http::{Error as HttpError, Response, Uri};
use futures::StreamExt;
use oauth2::{AsyncHttpClient, HttpRequest, HttpResponse};
use reqwest::redirect::Policy;
use thiserror::Error;

const MAX_RESPONSE_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub(super) struct FederatedHttpClient {
    client: reqwest::Client,
}

impl FederatedHttpClient {
    pub(super) fn build() -> Result<Self, FederatedHttpError> {
        let client = reqwest::Client::builder()
            .redirect(Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(FederatedHttpError::Transport)?;
        Ok(Self { client })
    }

    pub(super) const fn client(&self) -> &reqwest::Client {
        &self.client
    }
}

impl<'client> AsyncHttpClient<'client> for FederatedHttpClient {
    type Error = FederatedHttpError;
    type Future =
        Pin<Box<dyn Future<Output = Result<HttpResponse, Self::Error>> + Send + Sync + 'client>>;

    fn call(&'client self, request: HttpRequest) -> Self::Future {
        Box::pin(async move { self.execute(request).await })
    }
}

impl FederatedHttpClient {
    /// Executes one provider request with URL and response-size enforcement.
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, FederatedHttpError> {
        validate_request_url(request.uri())?;
        let response = self
            .client
            .execute(request.try_into().map_err(FederatedHttpError::Transport)?)
            .await
            .map_err(FederatedHttpError::Transport)?;
        let status = response.status();
        let version = response.version();
        let headers = response.headers().clone();
        let body = read_bounded(response).await?;
        let mut output = Response::builder().status(status).version(version);
        for (name, value) in headers {
            if let Some(name) = name {
                output = output.header(name, value);
            }
        }
        output.body(body).map_err(FederatedHttpError::Response)
    }
}

/// Reads one provider response without permitting unbounded buffering.
async fn read_bounded(response: reqwest::Response) -> Result<Vec<u8>, FederatedHttpError> {
    if response
        .content_length()
        .is_some_and(|value| value > MAX_RESPONSE_BYTES as u64)
    {
        return Err(FederatedHttpError::Oversized);
    }
    let mut output = Vec::with_capacity(4096);
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(FederatedHttpError::Transport)?;
        if output.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(FederatedHttpError::Oversized);
        }
        output.extend_from_slice(&chunk);
    }
    Ok(output)
}

/// Allows HTTPS endpoints and explicit loopback development endpoints only.
fn validate_request_url(uri: &Uri) -> Result<(), FederatedHttpError> {
    let url = url::Url::parse(&uri.to_string()).map_err(|_| FederatedHttpError::UnsafeUrl)?;
    let loopback = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    if url.scheme() == "https" || (url.scheme() == "http" && loopback) {
        Ok(())
    } else {
        Err(FederatedHttpError::UnsafeUrl)
    }
}

#[derive(Debug, Error)]
pub(super) enum FederatedHttpError {
    #[error("federated HTTP transport failed")]
    Transport(#[source] reqwest::Error),
    #[error("federated HTTP response construction failed")]
    Response(#[source] HttpError),
    #[error("federated HTTP response exceeded its size limit")]
    Oversized,
    #[error("federated HTTP request URL is unsafe")]
    UnsafeUrl,
}
