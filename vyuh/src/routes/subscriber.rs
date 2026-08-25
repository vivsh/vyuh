use std::{future::Future, pin::Pin};

use axum::extract::FromRequestParts;
use axum::extract::ws::WebSocketUpgrade;
use axum::http::{HeaderMap, header, request::Parts};

use crate::callables::{ArgPart, IntoArgPart};
use crate::channels::{
    ALL_TRANSPORTS, ChannelCursor, ChannelError, ChannelResponse, ChannelTransport, POLL, SSE,
    UserStream, WS,
};
use crate::errors::ErrorReport;
use crate::notifiers::CancellationNotifier;
use crate::{Error, Site};

/// Route extractor that negotiates channel transport from the request.
///
/// `Subscriber` hides Axum-specific WebSocket/SSE/poll mechanics from
/// application handlers. It selects WebSocket from upgrade headers, SSE from
/// `Accept: text/event-stream`, and poll as the fallback unless a `transport`
/// or `mode` query parameter is present.
pub struct Subscriber {
    mode: ChannelTransport,
    upgrade: Option<WebSocketUpgrade>,
    after: Option<ChannelCursor>,
    shutdown: CancellationNotifier,
}

/// Pending attachment of a user stream to a negotiated subscriber.
///
/// Awaiting this value allows all transports. Call `allow(...)` to restrict the
/// endpoint to a transport bitmask.
pub struct ChannelAttach {
    subscriber: Subscriber,
    stream: UserStream,
}

impl Subscriber {
    /// Prepares a user stream for this negotiated subscriber.
    ///
    /// The stream is not opened until the returned `ChannelAttach` is awaited
    /// or `allow(...)` is called.
    pub fn attach(self, stream: UserStream) -> ChannelAttach {
        ChannelAttach {
            subscriber: self,
            stream,
        }
    }
}

impl ChannelAttach {
    /// Opens the channel when the negotiated transport is allowed by `mask`.
    ///
    /// Use `WS | SSE | POLL` to accept every transport. A disallowed request
    /// returns a bad-request channel error.
    pub async fn allow(self, mask: ChannelTransport) -> Result<ChannelResponse, Error> {
        self.respond(mask).await
    }

    async fn respond(self, mask: ChannelTransport) -> Result<ChannelResponse, Error> {
        let subscriber = self.subscriber;
        if subscriber.mode & mask == 0 {
            return Err(Error::from(ChannelError::TransportNotAllowed));
        }
        let channels = self.stream.channels();
        let open = channels
            .open_stream(self.stream, subscriber.after)
            .await
            .map_err(Error::from)?;
        Self::into_response(subscriber, open).await
    }

    async fn into_response(
        subscriber: Subscriber,
        open: crate::channels::OpenStream,
    ) -> Result<ChannelResponse, Error> {
        match subscriber.mode {
            WS => Self::websocket_response(subscriber, open),
            SSE => Ok(ChannelResponse::Sse(open.into_sse(subscriber.shutdown))),
            POLL => Ok(ChannelResponse::Poll(
                open.into_poll(subscriber.shutdown).await,
            )),
            _ => Err(Error::from(ChannelError::TransportNotAllowed)),
        }
    }

    fn websocket_response(
        subscriber: Subscriber,
        open: crate::channels::OpenStream,
    ) -> Result<ChannelResponse, Error> {
        match subscriber.upgrade {
            Some(upgrade) => Ok(ChannelResponse::WebSocket(
                open.into_websocket(upgrade, subscriber.shutdown),
            )),
            None => Err(Error::bad_request("websocket upgrade is required")),
        }
    }
}

impl std::future::IntoFuture for ChannelAttach {
    type Output = Result<ChannelResponse, Error>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(self.allow(ALL_TRANSPORTS))
    }
}

impl FromRequestParts<Site> for Subscriber {
    type Rejection = ErrorReport;

    async fn from_request_parts(parts: &mut Parts, state: &Site) -> Result<Self, Self::Rejection> {
        let mode = request_mode(parts);
        let after =
            request_cursor(parts).map_err(|error| ErrorReport::bad_request(error.to_string()))?;
        let upgrade = if mode == WS {
            Some(extract_upgrade(parts, state).await?)
        } else {
            None
        };
        Ok(Self {
            mode,
            upgrade,
            after,
            shutdown: state.shutdown_notifier(),
        })
    }
}

impl IntoArgPart for Subscriber {
    fn into_arg_part() -> ArgPart {
        ArgPart::Ignore
    }
}

fn request_mode(parts: &Parts) -> ChannelTransport {
    match explicit_mode(parts) {
        Some(mode) => mode,
        None if wants_websocket(&parts.headers) => WS,
        None if wants_sse(&parts.headers) => SSE,
        None => POLL,
    }
}

fn explicit_mode(parts: &Parts) -> Option<ChannelTransport> {
    query_pairs(parts)
        .into_iter()
        .find_map(|(key, value)| match key.as_str() {
            "transport" | "mode" => transport_value(&value),
            _ => None,
        })
}

fn transport_value(value: &str) -> Option<ChannelTransport> {
    match value {
        "ws" | "websocket" => Some(WS),
        "sse" => Some(SSE),
        "poll" | "long_poll" | "long-poll" => Some(POLL),
        _ => None,
    }
}

fn request_cursor(parts: &Parts) -> Result<Option<ChannelCursor>, ChannelError> {
    for (key, value) in query_pairs(parts) {
        if matches!(key.as_str(), "after" | "cursor") {
            return value.parse::<ChannelCursor>().map(Some);
        }
    }
    Ok(None)
}

fn query_pairs(parts: &Parts) -> Vec<(String, String)> {
    let Some(query) = parts.uri.query() else {
        return Vec::new();
    };
    serde_urlencoded::from_str::<Vec<(String, String)>>(query).unwrap_or_default()
}

fn wants_websocket(headers: &HeaderMap) -> bool {
    header_contains(headers, header::UPGRADE, "websocket")
        && header_contains(headers, header::CONNECTION, "upgrade")
}

fn wants_sse(headers: &HeaderMap) -> bool {
    header_contains(headers, header::ACCEPT, "text/event-stream")
}

fn header_contains(headers: &HeaderMap, name: header::HeaderName, needle: &str) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_ascii_lowercase().contains(needle))
        .unwrap_or(false)
}

async fn extract_upgrade(parts: &mut Parts, state: &Site) -> Result<WebSocketUpgrade, ErrorReport> {
    WebSocketUpgrade::from_request_parts(parts, state)
        .await
        .map_err(|err| ErrorReport::bad_request(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    #[test]
    fn negotiation_chooses_sse_accept() -> Result<(), Box<dyn std::error::Error>> {
        let parts = request_parts("/events", &[(header::ACCEPT.as_str(), "text/event-stream")])?;
        assert_eq!(request_mode(&parts), SSE);
        Ok(())
    }

    #[test]
    fn negotiation_chooses_poll_fallback() -> Result<(), Box<dyn std::error::Error>> {
        let parts = request_parts("/events", &[])?;
        assert_eq!(request_mode(&parts), POLL);
        Ok(())
    }

    #[test]
    fn negotiation_chooses_explicit_ws() -> Result<(), Box<dyn std::error::Error>> {
        let parts = request_parts("/events?transport=ws", &[])?;
        assert_eq!(request_mode(&parts), WS);
        Ok(())
    }

    #[test]
    fn negotiation_chooses_upgrade_ws() -> Result<(), Box<dyn std::error::Error>> {
        let parts = request_parts(
            "/events",
            &[
                (header::UPGRADE.as_str(), "websocket"),
                (header::CONNECTION.as_str(), "Upgrade"),
            ],
        )?;
        assert_eq!(request_mode(&parts), WS);
        Ok(())
    }

    fn request_parts(
        uri: &str,
        headers: &[(&str, &str)],
    ) -> Result<Parts, Box<dyn std::error::Error>> {
        let mut builder = Request::builder().uri(uri);
        for (name, value) in headers {
            builder = builder.header(*name, *value);
        }
        let request = builder.body(())?;
        Ok(request.into_parts().0)
    }
}
