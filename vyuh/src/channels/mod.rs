mod beacon;
#[allow(dead_code)]
mod fanout;
mod log;
mod runtime;
mod transports;
mod types;

pub use beacon::{Beacon, BeaconBuilder, BeaconConf, BeaconError};
pub use transports::{ChannelLongPoll, ChannelResponse, ChannelSse, ChannelWebSocket};
pub use types::{
    ALL_TRANSPORTS, ChannelConf, ChannelCursor, ChannelError, ChannelEvent, ChannelEventId,
    ChannelKey, ChannelTransport, POLL, SSE, SlowSubscriberPolicy, UserKey, WS,
};

use runtime::ChannelReceiver;
pub(crate) use runtime::SubscriptionRuntime;

use axum::extract::ws::WebSocketUpgrade;

use crate::{
    Error, Site,
    callables::{self, DataValue},
    notifiers::CancellationNotifier,
};

/// Builds private Beacon channel keys after bundle path composition completes.
pub(crate) fn beacon_keys(
    operations: &std::collections::BTreeMap<crate::OperationId, crate::callables::Operation>,
    beacon_operations: &std::collections::BTreeSet<crate::OperationId>,
) -> std::collections::BTreeMap<crate::OperationId, ChannelKey> {
    operations
        .iter()
        .filter(|(id, operation)| {
            beacon_operations.contains(id)
                && matches!(operation.kind, crate::callables::OperationKind::Route)
                && operation.methods == crate::routes::Methods::GET
        })
        .map(|(id, operation)| (*id, ChannelKey::beacon(&operation.name, &operation.path)))
        .collect()
}

/// Site-scoped entry point for signal-backed channel delivery.
///
/// Applications publish typed signals through `site.signals().emit(T)`; channels
/// deliver accepted signal payloads to attached users.
#[derive(Clone)]
pub struct Channels {
    runtime: SubscriptionRuntime,
}

/// Builder for one logical channel's delivery policy.
///
/// Attaching replaces policy only for the selected `(UserKey, ChannelKey)`.
#[derive(Clone)]
pub struct UserStream {
    channels: Channels,
    user_key: UserKey,
    channel_key: Option<ChannelKey>,
    deliveries: Vec<types::DeliverySpec>,
}

pub(crate) struct OpenStream {
    pub(crate) replay: Vec<ChannelEvent>,
    pub(crate) receiver: ChannelReceiver,
    pub(crate) keepalive: std::time::Duration,
    pub(crate) poll_timeout: std::time::Duration,
}

impl Channels {
    pub(crate) fn new(runtime: SubscriptionRuntime) -> Self {
        Self { runtime }
    }

    /// Starts a logical delivery-policy builder for one authenticated user.
    ///
    /// The stream is inert until attached by `Subscriber::attach`.
    pub fn user(&self, user_key: UserKey) -> UserStream {
        UserStream {
            channels: self.clone(),
            user_key,
            channel_key: None,
            deliveries: Vec::new(),
        }
    }

    pub(crate) async fn publish_signal<T>(&self, data: &T) -> Result<(), ChannelError>
    where
        T: DataValue,
    {
        self.runtime.publish_signal(data).await
    }

    pub(crate) async fn publish_box(
        &self,
        payload: &crate::callables::DataBox,
    ) -> Result<(), ChannelError> {
        self.runtime.publish_box(payload).await
    }

    /// Delivers an inbound best-effort fanout envelope to local sessions.
    #[allow(dead_code)]
    pub(crate) async fn ingest_fanout(
        &self,
        envelope: fanout::FanoutEnvelope,
    ) -> Result<(), ChannelError> {
        self.runtime.ingest_fanout(envelope).await
    }

    pub(crate) async fn open_stream(
        &self,
        stream: UserStream,
        after: Option<ChannelCursor>,
    ) -> Result<OpenStream, ChannelError> {
        let channel_key = stream.channel_key.ok_or(ChannelError::MissingChannelKey)?;
        let open = self
            .runtime
            .open_user(stream.user_key, channel_key, stream.deliveries, after)
            .await?;
        Ok(OpenStream {
            replay: open.replay,
            receiver: open.receiver,
            keepalive: std::time::Duration::from_millis(self.runtime.conf().sse_keepalive_ms),
            poll_timeout: std::time::Duration::from_millis(
                self.runtime.conf().long_poll_timeout_ms,
            ),
        })
    }

    /// Resolves the private logical channel owned by one Beacon operation.
    pub(crate) fn beacon_channel(&self, operation: crate::OperationId) -> Option<ChannelKey> {
        self.runtime.beacon_channel(operation)
    }
}

impl UserStream {
    /// Assigns the stable application-owned logical channel identity.
    ///
    /// Sessions with the same key deliberately share policy and replay. An
    /// attachment without a key returns [`ChannelError::MissingChannelKey`].
    pub fn channel(mut self, channel_key: ChannelKey) -> Self {
        self.channel_key = Some(channel_key);
        self
    }

    /// Delivers every emitted signal payload of type `T` to this logical channel.
    ///
    /// The payload is retained only after it is accepted by the policy.
    pub fn deliver<T>(mut self) -> Self
    where
        T: DataValue,
    {
        self.deliveries.push(delivery::<T, _>(|_| true));
        self
    }

    /// Delivers signal payloads of type `T` accepted by `predicate`.
    ///
    /// The predicate runs before serialization and retention. It should be
    /// deterministic and should not perform blocking work.
    pub fn deliver_if<T, F>(mut self, predicate: F) -> Self
    where
        T: DataValue,
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        self.deliveries.push(delivery::<T, _>(predicate));
        self
    }

    /// Adds one internally constructed typed delivery rule.
    pub(crate) fn deliver_spec(mut self, delivery: types::DeliverySpec) -> Self {
        self.deliveries.push(delivery);
        self
    }

    pub(crate) fn channels(&self) -> Channels {
        self.channels.clone()
    }
}

impl OpenStream {
    pub(crate) fn into_sse(self, shutdown: CancellationNotifier) -> ChannelSse {
        ChannelSse::new(self.replay, self.receiver, self.keepalive, shutdown)
    }

    pub(crate) fn into_websocket(
        self,
        upgrade: WebSocketUpgrade,
        shutdown: CancellationNotifier,
    ) -> ChannelWebSocket {
        ChannelWebSocket::new(upgrade, self.replay, self.receiver, shutdown)
    }

    pub(crate) async fn into_poll(self, shutdown: CancellationNotifier) -> ChannelLongPoll {
        if !self.replay.is_empty() {
            return ChannelLongPoll::from_events(self.replay);
        }
        let events = ChannelLongPoll::wait(self.receiver, self.poll_timeout, shutdown).await;
        ChannelLongPoll::from_events(events)
    }
}

fn delivery<T, F>(predicate: F) -> types::DeliverySpec
where
    T: DataValue,
    F: Fn(&T) -> bool + Send + Sync + 'static,
{
    let predicate = std::sync::Arc::new(
        move |value: &dyn std::any::Any| matches!(value.downcast_ref::<T>(), Some(value) if predicate(value)),
    );
    types::DeliverySpec {
        type_id: std::any::TypeId::of::<T>(),
        signal_key: fanout::SignalKey::from_type_name(std::any::type_name::<T>()),
        predicate,
        decoder: decode_signal::<T>,
        debounce: None,
    }
}

fn decode_signal<T>(
    value: &serde_json::Value,
) -> Result<std::sync::Arc<dyn std::any::Any + Send + Sync>, ChannelError>
where
    T: DataValue,
{
    serde_json::from_value::<T>(value.clone())
        .map(|value| std::sync::Arc::new(value) as std::sync::Arc<dyn std::any::Any + Send + Sync>)
        .map_err(|error| ChannelError::Serialization(error.to_string()))
}

impl callables::FromSite for Channels {
    fn from_site(site: &Site) -> Result<Self, callables::CallError> {
        Ok(site.channels())
    }
}

impl axum::extract::FromRequestParts<Site> for Channels {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        _parts: &mut axum::http::request::Parts,
        state: &Site,
    ) -> Result<Self, Self::Rejection> {
        Ok(state.channels())
    }
}

impl callables::IntoArgPart for Channels {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}

impl callables::IntoArgPart for WebSocketUpgrade {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}

#[cfg(test)]
mod tests;

impl From<ChannelError> for Error {
    fn from(err: ChannelError) -> Self {
        match err {
            ChannelError::InvalidKey(_)
            | ChannelError::InvalidCursor(_)
            | ChannelError::TransportNotAllowed
            | ChannelError::MissingChannelKey
            | ChannelError::ChannelLimitExceeded { .. }
            | ChannelError::MessageTooLarge { .. } => Error::bad_request(err.to_string()),
            ChannelError::BackendUnavailable | ChannelError::DebounceQueueFull => {
                Error::unavailable(err.to_string())
            }
            ChannelError::SubscriptionUnavailable => Error::other(err),
            ChannelError::Serialization(_) | ChannelError::Transport(_) => Error::other(err),
        }
    }
}
