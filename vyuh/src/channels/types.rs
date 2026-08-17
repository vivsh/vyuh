use std::{
    any::TypeId,
    fmt,
    str::FromStr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::fanout::SignalKey;

/// Runtime configuration for signal-backed channel delivery.
///
/// Retention is bounded per user. Subscriber queues are also bounded so signal
/// emission cannot block indefinitely on slow clients.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ChannelConf {
    pub enabled: bool,
    pub subscriber_queue: usize,
    pub replay_limit: usize,
    pub retention_events: usize,
    /// Maximum independent logical channels one user may open locally.
    ///
    /// Direct channel keys are application-owned. This conservative bound
    /// prevents request-derived keys from retaining unbounded user state.
    pub max_channels_per_user: usize,
    pub max_message_bytes: usize,
    pub long_poll_timeout_ms: u64,
    pub sse_keepalive_ms: u64,
    pub slow_subscriber_policy: SlowSubscriberPolicy,
}

impl Default for ChannelConf {
    fn default() -> Self {
        Self {
            enabled: true,
            subscriber_queue: 256,
            replay_limit: 256,
            retention_events: 10_000,
            max_channels_per_user: 64,
            max_message_bytes: 1024 * 1024,
            long_poll_timeout_ms: 25_000,
            sse_keepalive_ms: 15_000,
            slow_subscriber_policy: SlowSubscriberPolicy::Disconnect,
        }
    }
}

/// Policy applied when a live channel cannot keep up with delivery.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SlowSubscriberPolicy {
    /// Drop the channel session when its bounded queue fills.
    Disconnect,
}

/// Stable application-owned identity for a user's retained channel queue.
///
/// All channel sessions for the same user share delivery policy and retention.
/// Re-registering a user stream replaces that user's previous delivery policy.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct UserKey(String);

impl UserKey {
    /// Creates a user key for channel delivery.
    ///
    /// Keys must be non-empty, no longer than 512 bytes, and free of control
    /// characters. Vyuh treats the key as opaque application data.
    pub fn new(key: impl Into<String>) -> Result<Self, ChannelError> {
        let key = key.into();
        validate_key("user key", &key)?;
        Ok(Self(key))
    }

    /// Returns the raw application-owned user key.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for UserKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Stable application-owned identity for one logical channel.
///
/// A logical channel owns its delivery policy, bounded reconnect replay, and
/// debounce state for one [`UserKey`]. Multiple physical attachments to the
/// same `(UserKey, ChannelKey)` deliberately share that state. Use stable
/// application constants, not request-derived values.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct ChannelKey(String);

impl ChannelKey {
    /// Creates a logical channel key.
    ///
    /// Keys must be non-empty, no longer than 512 bytes, and free of control
    /// characters.
    pub fn new(key: impl Into<String>) -> Result<Self, ChannelError> {
        let key = key.into();
        validate_key("channel key", &key)?;
        Ok(Self(key))
    }

    /// Returns the raw logical channel key.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ChannelKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn validate_key(name: &str, key: &str) -> Result<(), ChannelError> {
    if key.is_empty() {
        return Err(ChannelError::InvalidKey(format!("{name} cannot be empty")));
    }
    if key.len() > 512 {
        return Err(ChannelError::InvalidKey(format!(
            "{name} cannot exceed 512 bytes"
        )));
    }
    if key.chars().any(char::is_control) {
        return Err(ChannelError::InvalidKey(format!(
            "{name} cannot contain control characters"
        )));
    }
    Ok(())
}

/// Monotonic event id assigned by the local subscription runtime.
///
/// Event ids and cursors are process-local reconnect hints. They are not a
/// durable or cross-node ordering contract.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, JsonSchema,
)]
pub struct ChannelEventId(u64);

impl ChannelEventId {
    pub(crate) fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric event id.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for ChannelEventId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Opaque cursor used by clients to resume channel delivery.
///
/// Cursors currently wrap the last seen event id. Clients should pass them back
/// unchanged instead of interpreting them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ChannelCursor(ChannelEventId);

impl ChannelCursor {
    /// Creates a cursor after the given event id.
    pub fn new(id: ChannelEventId) -> Self {
        Self(id)
    }

    /// Returns the event id represented by this cursor.
    pub fn event_id(self) -> ChannelEventId {
        self.0
    }
}

impl fmt::Display for ChannelCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ChannelCursor {
    type Err = ChannelError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let id = value
            .parse::<u64>()
            .map_err(|_| ChannelError::InvalidCursor(value.to_string()))?;
        Ok(Self(ChannelEventId(id)))
    }
}

/// Wire envelope delivered by WebSocket, SSE, and polling transports.
///
/// The `type` field is the signal payload schema name. Internal routing uses
/// Rust type identity; schema names are client-facing display and dispatch
/// metadata.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ChannelEvent {
    pub id: ChannelEventId,
    #[serde(rename = "type")]
    pub event_type: String,
    pub data: serde_json::Value,
    pub created_at: u64,
}

impl ChannelEvent {
    pub(crate) fn new(id: ChannelEventId, event_type: String, data: serde_json::Value) -> Self {
        Self {
            id,
            event_type,
            data,
            created_at: unix_now(),
        }
    }
}

pub(crate) fn unix_now() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0,
    }
}

/// Bitmask describing a channel transport set.
pub type ChannelTransport = u8;

/// WebSocket transport bit.
pub const WS: ChannelTransport = 0b001;
/// Server-sent events transport bit.
pub const SSE: ChannelTransport = 0b010;
/// Long-poll transport bit.
pub const POLL: ChannelTransport = 0b100;
/// Default transport mask accepted by `Subscriber::attach(...).await`.
pub const ALL_TRANSPORTS: ChannelTransport = WS | SSE | POLL;

impl ChannelKey {
    /// Derives the private logical key used by a finalized Beacon endpoint.
    pub(crate) fn beacon(name: &str, path: &str) -> Self {
        let path = normalize_path(path);
        let material = format!("vyuh.channel.beacon.v1\0GET\0{name}\0{path}");
        Self(blake3::hash(material.as_bytes()).to_hex().to_string())
    }
}

fn normalize_path(path: &str) -> &str {
    if path == "/" {
        path
    } else {
        path.trim_end_matches('/')
    }
}

/// Type-erased predicate used by user-scoped delivery policy.
pub type DeliveryPredicate = Arc<dyn Fn(&dyn std::any::Any) -> bool + Send + Sync>;

/// Decodes a raw fanout payload into the matching local Rust value.
pub(crate) type SignalDecoder =
    fn(&serde_json::Value) -> Result<Arc<dyn std::any::Any + Send + Sync>, ChannelError>;

/// Delivery rule registered for a user and one signal payload type.
///
/// Application code creates delivery rules through `UserStream` and Beacon.
#[derive(Clone)]
pub(crate) struct DeliverySpec {
    pub(crate) type_id: TypeId,
    pub(crate) signal_key: SignalKey,
    pub(crate) predicate: DeliveryPredicate,
    pub(crate) decoder: SignalDecoder,
    /// Optional trailing-edge debounce window for this delivery rule.
    pub(crate) debounce: Option<std::time::Duration>,
}

#[derive(Clone)]
pub(crate) struct DeliveryRule {
    pub(crate) predicate: DeliveryPredicate,
    pub(crate) debounce: Option<std::time::Duration>,
}

/// Errors returned by channel registration, replay, transport negotiation, and
/// serialization.
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("invalid channel key: {0}")]
    InvalidKey(String),

    #[error("invalid channel cursor: {0}")]
    InvalidCursor(String),

    #[error("channel message is too large: maximum is {max} bytes, got {got}")]
    MessageTooLarge { max: usize, got: usize },

    #[error("channel backend unavailable")]
    BackendUnavailable,

    #[error("channel debounce scheduler is saturated")]
    DebounceQueueFull,

    #[error("channel serialization failed: {0}")]
    Serialization(String),

    #[error("channel transport failed: {0}")]
    Transport(String),

    #[error("channel transport is not allowed")]
    TransportNotAllowed,

    #[error("channel subscription is unavailable")]
    SubscriptionUnavailable,

    #[error("a logical channel key is required before attaching")]
    MissingChannelKey,

    #[error("channel limit exceeded: at most {max} logical channels per user")]
    ChannelLimitExceeded { max: usize },
}

#[cfg(test)]
mod tests {
    use super::ChannelKey;

    /// Verifies final route identity is deterministic and path-normalized.
    #[test]
    fn beacon_channel_key_is_stable_after_path_normalization() {
        let first = ChannelKey::beacon("live", "/api/live/");
        let second = ChannelKey::beacon("live", "/api/live");
        let different_name = ChannelKey::beacon("updates", "/api/live");

        assert_eq!(first, second);
        assert_ne!(first, different_name);
    }
}
