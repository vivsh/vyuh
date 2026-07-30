use std::net::{IpAddr, SocketAddr};

use axum::{
    extract::{ConnectInfo, FromRequestParts},
    http::{StatusCode, request::Parts},
};

use crate::{
    Site,
    callables::{IntoArgPart, specs::ArgPart},
};

/// Resolved client address from a single `X-Forwarded-For` value or the TCP peer.
///
/// Vyuh uses `X-Forwarded-For` when present and otherwise falls back to the
/// address attached by Axum's connection-aware server. Multiple or malformed
/// forwarded values are rejected because they do not identify one client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientIp(pub IpAddr);

impl FromRequestParts<Site> for ClientIp {
    type Rejection = StatusCode;

    async fn from_request_parts(parts: &mut Parts, _state: &Site) -> Result<Self, Self::Rejection> {
        resolve(parts)
            .map(Self)
            .map_err(|_| StatusCode::BAD_REQUEST)
    }
}

impl IntoArgPart for ClientIp {
    fn into_arg_part() -> ArgPart {
        ArgPart::Ignore
    }
}

pub(crate) fn resolve(parts: &Parts) -> Result<IpAddr, ClientIpError> {
    forwarded(parts)?.map_or_else(|| peer(parts), Ok)
}

fn peer(parts: &Parts) -> Result<IpAddr, ClientIpError> {
    parts
        .extensions
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.ip())
        .ok_or(ClientIpError::Unavailable)
}

fn forwarded(parts: &Parts) -> Result<Option<IpAddr>, ClientIpError> {
    let mut values = parts.headers.get_all("x-forwarded-for").iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(ClientIpError::InvalidForwarded);
    }
    let value = value
        .to_str()
        .map_err(|_| ClientIpError::InvalidForwarded)?;
    if value.contains(',') {
        return Err(ClientIpError::InvalidForwarded);
    }
    value
        .trim()
        .parse()
        .map(Some)
        .map_err(|_| ClientIpError::InvalidForwarded)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ClientIpError {
    #[error("client address is unavailable")]
    Unavailable,
    #[error("X-Forwarded-For must contain exactly one IP address")]
    InvalidForwarded,
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use axum::{extract::ConnectInfo, http::Request};

    use super::resolve;

    /// Verifies forwarded client IP takes precedence over the direct TCP peer.
    #[test]
    fn forwarded_ip_overrides_peer() {
        let mut request = Request::new(());
        request
            .headers_mut()
            .insert("x-forwarded-for", "203.0.113.42".parse().unwrap());
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))));
        assert_eq!(
            resolve(&request.into_parts().0).unwrap().to_string(),
            "203.0.113.42"
        );
    }

    /// Verifies the TCP peer supplies the client IP when no forwarded header exists.
    #[test]
    fn peer_ip_is_the_fallback() {
        let mut request = Request::new(());
        request
            .extensions_mut()
            .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))));
        assert_eq!(
            resolve(&request.into_parts().0).unwrap().to_string(),
            "127.0.0.1"
        );
    }
}
