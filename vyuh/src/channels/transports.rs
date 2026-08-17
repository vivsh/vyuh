use std::{convert::Infallible, time::Duration};

use crate::{
    callables::{IntoReturnPart, ReturnPart, TypeSchema},
    notifiers::CancellationNotifier,
};
use axum::{
    extract::ws::{Message, WebSocket, WebSocketUpgrade},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
};
use futures::{SinkExt, StreamExt, stream};
use serde::{Deserialize, Serialize};

use super::{ChannelCursor, ChannelError, ChannelEvent, ChannelReceiver};

/// Unified route response for negotiated channel transports.
///
/// Handlers usually return this from `Subscriber::attach(stream).allow(...)`.
pub enum ChannelResponse {
    /// Server-sent events response.
    Sse(ChannelSse),
    /// WebSocket upgrade response.
    WebSocket(ChannelWebSocket),
    /// Long-poll JSON response.
    Poll(ChannelLongPoll),
}

impl IntoResponse for ChannelResponse {
    fn into_response(self) -> Response {
        match self {
            Self::Sse(response) => response.into_response(),
            Self::WebSocket(response) => response.into_response(),
            Self::Poll(response) => response.into_response(),
        }
    }
}

impl IntoReturnPart for ChannelResponse {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Unknown
    }
}

/// Server-sent events channel response.
///
/// SSE uses the schema name as the event name and sends the shared channel
/// envelope as JSON data.
pub struct ChannelSse {
    replay: Vec<ChannelEvent>,
    receiver: ChannelReceiver,
    keepalive: Duration,
    shutdown: CancellationNotifier,
}

impl ChannelSse {
    pub(crate) fn new(
        replay: Vec<ChannelEvent>,
        receiver: ChannelReceiver,
        keepalive: Duration,
        shutdown: CancellationNotifier,
    ) -> Self {
        Self {
            replay,
            receiver,
            keepalive,
            shutdown,
        }
    }
}

impl IntoResponse for ChannelSse {
    fn into_response(self) -> Response {
        let replay = stream::iter(self.replay.into_iter().map(event_to_sse));
        let live = sse_live_stream(self.receiver, self.shutdown);
        let stream = replay.chain(live);
        Sse::new(stream)
            .keep_alive(KeepAlive::new().interval(self.keepalive))
            .into_response()
    }
}

impl IntoReturnPart for ChannelSse {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Unknown
    }
}

fn sse_live_stream(
    receiver: ChannelReceiver,
    shutdown: CancellationNotifier,
) -> impl futures::Stream<Item = Result<Event, Infallible>> {
    stream::unfold(
        (receiver, shutdown),
        |(mut receiver, shutdown)| async move {
            let shutdown_wait = shutdown.clone();
            tokio::select! {
                _ = shutdown_wait.notified() => None,
                event = receiver.recv() => {
                    event.map(|event| (event_to_sse((*event).clone()), (receiver, shutdown)))
                },
            }
        },
    )
}

fn event_to_sse(event: ChannelEvent) -> Result<Event, Infallible> {
    let event_type = event.event_type.clone();
    let data = match serde_json::to_string(&event) {
        Ok(data) => data,
        Err(_) => "null".to_string(),
    };
    Ok(Event::default()
        .id(event.id.to_string())
        .event(event_type)
        .data(data))
}

/// WebSocket channel response.
///
/// The socket receives replay events first, then live channel envelopes as JSON
/// text frames.
pub struct ChannelWebSocket {
    upgrade: WebSocketUpgrade,
    replay: Vec<ChannelEvent>,
    receiver: ChannelReceiver,
    shutdown: CancellationNotifier,
}

impl ChannelWebSocket {
    pub(crate) fn new(
        upgrade: WebSocketUpgrade,
        replay: Vec<ChannelEvent>,
        receiver: ChannelReceiver,
        shutdown: CancellationNotifier,
    ) -> Self {
        Self {
            upgrade,
            replay,
            receiver,
            shutdown,
        }
    }
}

impl IntoResponse for ChannelWebSocket {
    fn into_response(self) -> Response {
        self.upgrade
            .on_upgrade(|socket| websocket_task(socket, self.replay, self.receiver, self.shutdown))
            .into_response()
    }
}

impl IntoReturnPart for ChannelWebSocket {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Unknown
    }
}

async fn websocket_task(
    socket: WebSocket,
    replay: Vec<ChannelEvent>,
    mut receiver: ChannelReceiver,
    shutdown: CancellationNotifier,
) {
    let (mut sender, mut incoming) = socket.split();
    let incoming_task = tokio::spawn(async move { while incoming.next().await.is_some() {} });

    for event in replay {
        if send_ws_event(&mut sender, &event).await.is_err() {
            incoming_task.abort();
            return;
        }
    }

    loop {
        let shutdown_wait = shutdown.clone();
        tokio::select! {
            _ = shutdown_wait.notified() => {
                let _ = sender.send(Message::Close(None)).await;
                break;
            },
            event = receiver.recv() => {
                let Some(event) = event else {
                    break;
                };
                if send_ws_event(&mut sender, event.as_ref()).await.is_err() {
                    break;
                }
            },
        }
    }

    incoming_task.abort();
}

async fn send_ws_event(
    sender: &mut futures::stream::SplitSink<WebSocket, Message>,
    event: &ChannelEvent,
) -> Result<(), ChannelError> {
    let text =
        serde_json::to_string(event).map_err(|err| ChannelError::Serialization(err.to_string()))?;
    sender
        .send(Message::Text(text.into()))
        .await
        .map_err(|err| ChannelError::Transport(err.to_string()))
}

/// Long-poll channel response body.
///
/// Clients pass `cursor` back as `after` or `cursor` on the next poll request.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ChannelLongPoll {
    /// Cursor for the last event in this response.
    pub cursor: Option<ChannelCursor>,
    /// Events accepted for the user since the requested cursor.
    pub events: Vec<ChannelEvent>,
}

impl ChannelLongPoll {
    pub(crate) fn from_events(events: Vec<ChannelEvent>) -> Self {
        let cursor = events.last().map(|event| ChannelCursor::new(event.id));
        Self { cursor, events }
    }

    pub(crate) async fn wait(
        mut receiver: ChannelReceiver,
        timeout: Duration,
        shutdown: CancellationNotifier,
    ) -> Vec<ChannelEvent> {
        tokio::select! {
            _ = shutdown.notified() => Vec::new(),
            event = tokio::time::timeout(timeout, receiver.recv()) => {
                match event {
                    Ok(Some(event)) => vec![event.as_ref().clone()],
                    Ok(None) | Err(_) => Vec::new(),
                }
            },
        }
    }
}

impl IntoResponse for ChannelLongPoll {
    fn into_response(self) -> Response {
        axum::Json(self).into_response()
    }
}

impl IntoReturnPart for ChannelLongPoll {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Body(
            TypeSchema::wrap::<ChannelLongPoll>(),
            "application/json".into(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::mpsc;

    use super::*;
    use crate::channels::ChannelEventId;

    fn test_receiver() -> (mpsc::Sender<Arc<ChannelEvent>>, ChannelReceiver) {
        let (tx, rx) = mpsc::channel(1);
        (tx, ChannelReceiver::detached(rx))
    }

    #[tokio::test]
    async fn long_poll_returns_on_shutdown() {
        let (_tx, receiver) = test_receiver();
        let shutdown = CancellationNotifier::new();
        shutdown.notify_waiters();
        let events = ChannelLongPoll::wait(receiver, Duration::from_secs(60), shutdown).await;
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn long_poll_returns_event_before_timeout() {
        let (tx, receiver) = test_receiver();
        let shutdown = CancellationNotifier::new();
        let event = Arc::new(ChannelEvent::new(
            ChannelEventId::new(1),
            "TestEvent".to_string(),
            serde_json::json!({"ok": true}),
        ));
        assert!(tx.send(event).await.is_ok());
        let events = ChannelLongPoll::wait(receiver, Duration::from_secs(60), shutdown).await;
        assert_eq!(events.len(), 1);
    }
}
