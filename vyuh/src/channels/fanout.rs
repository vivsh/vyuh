//! Internal boundary for future ephemeral cross-node channel fanout.

use std::{future::Future, pin::Pin, sync::Arc};

use serde::{Deserialize, Serialize};

use super::{ChannelError, types::unix_now};

/// Deterministic type identity shared by equivalent application builds.
///
/// The complete Rust type name is hashed instead of using `TypeId`, whose
/// representation is process-local and cannot cross a fanout boundary.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct SignalKey(Arc<str>);

impl SignalKey {
    /// Derives the key from the complete Rust type identity.
    pub(crate) fn from_type_name(type_name: &str) -> Self {
        Self(Arc::from(
            blake3::hash(type_name.as_bytes()).to_hex().to_string(),
        ))
    }

    /// Returns the opaque deterministic value used in future envelopes.
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// Raw best-effort notification for a future ephemeral transport.
///
/// The envelope intentionally contains no subscription identity or cursor.
/// Receiving nodes re-evaluate their own currently active local policies.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct FanoutEnvelope {
    pub(crate) delivery_id: uuid::Uuid,
    pub(crate) signal_key: SignalKey,
    pub(crate) event_type: String,
    pub(crate) created_at: u64,
    pub(crate) payload: Arc<str>,
    pub(crate) trace_context: Option<serde_json::Value>,
    pub(crate) origin_node: uuid::Uuid,
}

impl FanoutEnvelope {
    /// Builds an envelope for a locally serialized signal payload.
    pub(crate) fn new(
        signal_key: SignalKey,
        event_type: String,
        payload: serde_json::Value,
        origin_node: uuid::Uuid,
    ) -> Self {
        Self {
            delivery_id: uuid::Uuid::now_v7(),
            signal_key,
            event_type,
            created_at: unix_now(),
            payload: Arc::from(payload.to_string()),
            trace_context: None,
            origin_node,
        }
    }
}

/// Future endpoint for ephemeral raw-payload replication.
///
/// A transport implementation must deliver incoming envelopes to
/// `SubscriptionRuntime::ingest_fanout`; it must never call `SignalEngine`.
/// Implementations are intentionally private until a shared backend and its
/// explicit application namespace are designed together.
pub(crate) trait ChannelFanout: Send + Sync {
    /// Publishes one best-effort envelope without affecting local delivery.
    fn publish(
        &self,
        envelope: FanoutEnvelope,
    ) -> Pin<Box<dyn Future<Output = Result<(), ChannelError>> + Send + '_>>;
}
