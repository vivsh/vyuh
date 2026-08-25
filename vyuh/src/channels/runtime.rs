use std::{
    any::TypeId,
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::fanout::{FanoutEnvelope, SignalKey};
use super::log::SubscriptionLog;
use super::types::{DeliveryRule, DeliverySpec};
use super::{
    ChannelConf, ChannelCursor, ChannelError, ChannelEvent, ChannelEventId, ChannelKey, UserKey,
};
use crate::notifiers::CancellationNotifier;
use crate::utils::debounce::{DebounceConf, DebounceQueue, Debouncer};

/// Live receiver for accepted channel events. Its drop guard unregisters only
/// the physical attachment it owns.
pub(crate) struct ChannelReceiver {
    pub(crate) inner: mpsc::Receiver<Arc<ChannelEvent>>,
    lease: Option<SessionLease>,
}

impl ChannelReceiver {
    fn new(inner: mpsc::Receiver<Arc<ChannelEvent>>, lease: SessionLease) -> Self {
        Self {
            inner,
            lease: Some(lease),
        }
    }

    /// Receives the next live channel event, or `None` after the channel closes.
    pub(crate) async fn recv(&mut self) -> Option<Arc<ChannelEvent>> {
        self.inner.recv().await
    }

    #[cfg(test)]
    pub(crate) fn detached(inner: mpsc::Receiver<Arc<ChannelEvent>>) -> Self {
        Self { inner, lease: None }
    }
}

impl Drop for ChannelReceiver {
    fn drop(&mut self) {
        if let Some(lease) = self.lease.take() {
            lease.runtime.remove_session(&lease.target, lease.id);
        }
    }
}

/// Result of atomically opening a channel session.
pub(crate) struct ChannelOpen {
    pub(crate) replay: Vec<ChannelEvent>,
    pub(crate) receiver: ChannelReceiver,
}

/// Process-local state for logical channels, replay, session wakeup, and debounce.
#[derive(Clone)]
pub(crate) struct SubscriptionRuntime {
    inner: Arc<SubscriptionRuntimeInner>,
}

struct SubscriptionRuntimeInner {
    conf: ChannelConf,
    next_event: AtomicU64,
    next_session: AtomicU64,
    state: Mutex<LocalChannelState>,
    beacon_channels: Arc<BTreeMap<crate::OperationId, ChannelKey>>,
    debounce_tx: mpsc::Sender<DebounceInput>,
    debounce_rx: Mutex<Option<mpsc::Receiver<DebounceInput>>>,
}

#[derive(Default)]
struct LocalChannelState {
    users: HashMap<UserKey, HashMap<ChannelKey, ChannelState>>,
    type_index: HashMap<TypeId, HashSet<ChannelTarget>>,
    codecs: HashMap<SignalKey, SignalCodec>,
    delivered_fanout: FanoutDedup,
}

#[derive(Default)]
struct ChannelState {
    generation: u64,
    rules: HashMap<TypeId, DeliveryRule>,
    log: SubscriptionLog,
    sessions: HashMap<SessionId, Subscriber>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ChannelTarget {
    user_key: UserKey,
    channel_key: ChannelKey,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct SessionId(u64);

struct SessionLease {
    runtime: SubscriptionRuntime,
    target: ChannelTarget,
    id: SessionId,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct DeliveryTarget {
    channel: ChannelTarget,
    generation: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct DebounceKey {
    target: DeliveryTarget,
    type_id: TypeId,
}

struct DebounceInput {
    key: DebounceKey,
    window: std::time::Duration,
    payload: Arc<PendingPayload>,
}

struct PendingPayload {
    event_type: String,
    data: Arc<serde_json::Value>,
}

#[derive(Clone)]
struct AcceptedChannel {
    target: DeliveryTarget,
    debounce: Option<std::time::Duration>,
}

struct Subscriber {
    sender: mpsc::Sender<Arc<ChannelEvent>>,
}

#[derive(Clone, Copy)]
struct SignalCodec {
    type_id: TypeId,
    decoder: super::types::SignalDecoder,
}

const FANOUT_DEDUP_LIMIT: usize = 2_048;

#[derive(Default)]
struct FanoutDedup {
    ids: std::collections::VecDeque<uuid::Uuid>,
    known: HashSet<uuid::Uuid>,
}

impl FanoutDedup {
    fn seen_or_insert(&mut self, id: uuid::Uuid) -> bool {
        if !self.known.insert(id) {
            return true;
        }
        self.ids.push_back(id);
        if self.ids.len() > FANOUT_DEDUP_LIMIT
            && let Some(expired) = self.ids.pop_front()
        {
            self.known.remove(&expired);
        }
        false
    }
}

impl SubscriptionRuntime {
    /// Creates local subscription state from finalized site configuration.
    pub(crate) fn new(
        conf: ChannelConf,
        beacon_channels: BTreeMap<crate::OperationId, ChannelKey>,
    ) -> Self {
        let (debounce_tx, debounce_rx) = mpsc::channel(conf.retention_events.max(1));
        Self {
            inner: Arc::new(SubscriptionRuntimeInner {
                conf,
                next_event: AtomicU64::new(1),
                next_session: AtomicU64::new(1),
                state: Mutex::new(LocalChannelState::default()),
                beacon_channels: Arc::new(beacon_channels),
                debounce_tx,
                debounce_rx: Mutex::new(Some(debounce_rx)),
            }),
        }
    }

    /// Returns the local delivery configuration.
    pub(crate) fn conf(&self) -> &ChannelConf {
        &self.inner.conf
    }

    fn validate_payload_size(&self, value: &serde_json::Value) -> Result<(), ChannelError> {
        let size = serde_json::to_vec(value)
            .map_err(|error| ChannelError::Serialization(error.to_string()))?
            .len();
        if size > self.inner.conf.max_message_bytes {
            return Err(ChannelError::MessageTooLarge {
                max: self.inner.conf.max_message_bytes,
                got: size,
            });
        }
        Ok(())
    }

    fn candidates<T>(&self, value: &T) -> Vec<AcceptedChannel>
    where
        T: crate::callables::DataValue,
    {
        self.candidates_any(TypeId::of::<T>(), value as &dyn std::any::Any, false)
    }

    fn candidates_any(
        &self,
        type_id: TypeId,
        value: &dyn std::any::Any,
        attached_only: bool,
    ) -> Vec<AcceptedChannel> {
        self.delivery_candidates(type_id, attached_only)
            .into_iter()
            .filter_map(|(target, rule)| {
                (rule.predicate)(value).then_some(AcceptedChannel {
                    target,
                    debounce: rule.debounce,
                })
            })
            .collect()
    }

    fn delivery_candidates(
        &self,
        type_id: TypeId,
        attached_only: bool,
    ) -> Vec<(DeliveryTarget, DeliveryRule)> {
        let state = self.inner.state.lock();
        let Some(targets) = state.type_index.get(&type_id) else {
            return Vec::new();
        };
        targets
            .iter()
            .filter_map(|target| channel_rule(&state, target, type_id, attached_only))
            .collect()
    }

    fn append_event(&self, targets: &[DeliveryTarget], event: Arc<ChannelEvent>) {
        let mut state = self.inner.state.lock();
        for target in targets {
            let Some(channel) = channel_mut(&mut state, &target.channel) else {
                continue;
            };
            if channel.generation != target.generation {
                continue;
            }
            channel
                .log
                .append(Arc::clone(&event), self.inner.conf.retention_events);
            send_live(channel, Arc::clone(&event));
        }
    }

    fn enqueue_debounce(
        &self,
        target: DeliveryTarget,
        type_id: TypeId,
        window: std::time::Duration,
        payload: Arc<PendingPayload>,
    ) -> Result<(), ChannelError> {
        match self.inner.debounce_tx.try_send(DebounceInput {
            key: DebounceKey { target, type_id },
            window,
            payload,
        }) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => Err(ChannelError::DebounceQueueFull),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(ChannelError::BackendUnavailable),
        }
    }

    fn publish_value(
        &self,
        accepted: Vec<AcceptedChannel>,
        type_id: TypeId,
        event_type: String,
        data: serde_json::Value,
    ) -> Result<(), ChannelError> {
        let payload = Arc::new(PendingPayload {
            event_type,
            data: Arc::new(data),
        });
        let immediate = accepted
            .iter()
            .filter(|accepted| accepted.debounce.is_none())
            .map(|accepted| accepted.target.clone())
            .collect::<Vec<_>>();
        for accepted in accepted {
            if let Some(window) = accepted.debounce {
                self.enqueue_debounce(accepted.target, type_id, window, Arc::clone(&payload))?;
            }
        }
        if !immediate.is_empty() {
            let event = Arc::new(ChannelEvent::new(
                ChannelEventId::new(self.inner.next_event.fetch_add(1, Ordering::Relaxed)),
                payload.event_type.clone(),
                payload.data.as_ref().clone(),
            ));
            self.append_event(&immediate, event);
        }
        Ok(())
    }

    fn deliver_debounced(&self, key: DebounceKey, payload: Arc<PendingPayload>) {
        let mut state = self.inner.state.lock();
        let Some(channel) = channel_mut(&mut state, &key.target.channel) else {
            return;
        };
        if channel.generation != key.target.generation
            || channel
                .rules
                .get(&key.type_id)
                .is_none_or(|rule| rule.debounce.is_none())
        {
            return;
        }
        let event = Arc::new(ChannelEvent::new(
            ChannelEventId::new(self.inner.next_event.fetch_add(1, Ordering::Relaxed)),
            payload.event_type.clone(),
            payload.data.as_ref().clone(),
        ));
        channel
            .log
            .append(Arc::clone(&event), self.inner.conf.retention_events);
        send_live(channel, event);
    }

    /// Runs the one site-owned scheduler for delayed channel deliveries.
    pub(crate) async fn run_debounce(&self, shutdown: CancellationNotifier) {
        let Some(mut receiver) = self.inner.debounce_rx.lock().take() else {
            return;
        };
        let mut states: HashMap<DebounceKey, Debouncer<Arc<PendingPayload>>> = HashMap::new();
        let mut deadlines = DebounceQueue::<DebounceKey>::new();
        loop {
            tokio::select! {
                _ = shutdown.notified() => return,
                input = receiver.recv() => match input {
                    Some(input) => schedule_debounce(input, &mut states, &mut deadlines),
                    None => return,
                },
                Some(deadline) = deadlines.pop() => {
                    if let Some(payload) = states.get_mut(&deadline.key)
                        .and_then(|state| state.due(deadline.deadline.generation()))
                    {
                        self.deliver_debounced(deadline.key, payload);
                    }
                },
            }
        }
    }

    /// Atomically replaces one logical-channel policy and opens a physical session.
    pub(crate) async fn open_user(
        &self,
        user_key: UserKey,
        channel_key: ChannelKey,
        deliveries: Vec<DeliverySpec>,
        after: Option<ChannelCursor>,
    ) -> Result<ChannelOpen, ChannelError> {
        let (sender, receiver) = mpsc::channel(self.inner.conf.subscriber_queue.max(1));
        let target = ChannelTarget {
            user_key,
            channel_key,
        };
        let session_id = SessionId(self.inner.next_session.fetch_add(1, Ordering::Relaxed));
        let after = after
            .map(|cursor| cursor.event_id())
            .unwrap_or(ChannelEventId::new(0));
        let mut state = self.inner.state.lock();
        ensure_channel_limit(&state, &target, self.inner.conf.max_channels_per_user)?;
        replace_policy(&mut state, &target, deliveries);
        let channel = channel_mut(&mut state, &target).ok_or(ChannelError::BackendUnavailable)?;
        let replay = channel.log.after(after, self.inner.conf.replay_limit);
        channel.sessions.insert(session_id, Subscriber { sender });
        Ok(ChannelOpen {
            replay,
            receiver: ChannelReceiver::new(
                receiver,
                SessionLease {
                    runtime: self.clone(),
                    target,
                    id: session_id,
                },
            ),
        })
    }

    /// Offers one local typed signal to eligible logical channels.
    pub(crate) async fn publish_signal<T>(&self, value: &T) -> Result<(), ChannelError>
    where
        T: crate::callables::DataValue,
    {
        let accepted = self.candidates(value);
        if accepted.is_empty() {
            return Ok(());
        }
        let data = serde_json::to_value(value)
            .map_err(|error| ChannelError::Serialization(error.to_string()))?;
        self.validate_payload_size(&data)?;
        self.publish_value(accepted, TypeId::of::<T>(), event_type::<T>(), data)
    }

    /// Offers an erased locally-produced signal to eligible logical channels.
    pub(crate) async fn publish_box(
        &self,
        payload: &crate::callables::DataBox,
    ) -> Result<(), ChannelError> {
        let type_id = payload.payload_type_id();
        let accepted = self.candidates_any(type_id, payload.as_any(), false);
        if accepted.is_empty() {
            return Ok(());
        }
        let data = payload
            .to_json()
            .ok_or_else(|| ChannelError::Serialization("payload is not serializable".into()))?
            .map_err(ChannelError::Serialization)?;
        self.validate_payload_size(&data)?;
        let event_type = payload
            .schema_name()
            .map(|name| sanitize_event_type(&name))
            .ok_or_else(|| ChannelError::Serialization("payload schema is not available".into()))?;
        self.publish_value(accepted, type_id, event_type, data)
    }

    /// Applies one remote ephemeral payload to active local sessions only.
    pub(crate) async fn ingest_fanout(&self, envelope: FanoutEnvelope) -> Result<(), ChannelError> {
        let Some(codec) = self.fanout_codec(&envelope) else {
            return Ok(());
        };
        let payload = serde_json::from_str::<serde_json::Value>(&envelope.payload)
            .map_err(|error| ChannelError::Serialization(error.to_string()))?;
        let value = (codec.decoder)(&payload)?;
        let accepted = self.candidates_any(codec.type_id, value.as_ref(), true);
        if accepted.is_empty() {
            return Ok(());
        }
        self.validate_payload_size(&payload)?;
        self.publish_value(accepted, codec.type_id, envelope.event_type, payload)
    }

    fn fanout_codec(&self, envelope: &FanoutEnvelope) -> Option<SignalCodec> {
        let mut state = self.inner.state.lock();
        if state.delivered_fanout.seen_or_insert(envelope.delivery_id) {
            return None;
        }
        state.codecs.get(&envelope.signal_key).copied()
    }

    fn remove_session(&self, target: &ChannelTarget, id: SessionId) {
        let mut state = self.inner.state.lock();
        if let Some(channel) = channel_mut(&mut state, target) {
            channel.sessions.remove(&id);
        }
    }

    /// Resolves a finalized Beacon operation to its private logical channel key.
    pub(crate) fn beacon_channel(&self, operation: crate::OperationId) -> Option<ChannelKey> {
        self.inner.beacon_channels.get(&operation).cloned()
    }

    #[cfg(test)]
    pub(crate) fn session_count(&self, user_key: &UserKey, channel_key: &ChannelKey) -> usize {
        let state = self.inner.state.lock();
        state
            .users
            .get(user_key)
            .and_then(|channels| channels.get(channel_key))
            .map_or(0, |channel| channel.sessions.len())
    }
}

impl Default for SubscriptionRuntime {
    fn default() -> Self {
        Self::new(ChannelConf::default(), BTreeMap::new())
    }
}

fn ensure_channel_limit(
    state: &LocalChannelState,
    target: &ChannelTarget,
    limit: usize,
) -> Result<(), ChannelError> {
    let channels = state.users.get(&target.user_key);
    if channels.is_none_or(|channels| channels.contains_key(&target.channel_key)) {
        return Ok(());
    }
    if channels.is_some_and(|channels| channels.len() >= limit) {
        return Err(ChannelError::ChannelLimitExceeded { max: limit });
    }
    Ok(())
}

fn replace_policy(
    state: &mut LocalChannelState,
    target: &ChannelTarget,
    deliveries: Vec<DeliverySpec>,
) {
    remove_indexes(state, target);
    let channel = state
        .users
        .entry(target.user_key.clone())
        .or_default()
        .entry(target.channel_key.clone())
        .or_default();
    channel.generation = channel.generation.wrapping_add(1);
    channel.rules.clear();
    for delivery in deliveries {
        state
            .type_index
            .entry(delivery.type_id)
            .or_default()
            .insert(target.clone());
        channel.rules.insert(
            delivery.type_id,
            DeliveryRule {
                predicate: delivery.predicate,
                debounce: delivery.debounce,
            },
        );
        state.codecs.insert(
            delivery.signal_key,
            SignalCodec {
                type_id: delivery.type_id,
                decoder: delivery.decoder,
            },
        );
    }
}

fn remove_indexes(state: &mut LocalChannelState, target: &ChannelTarget) {
    for targets in state.type_index.values_mut() {
        targets.remove(target);
    }
    state.type_index.retain(|_, targets| !targets.is_empty());
}

fn channel_mut<'a>(
    state: &'a mut LocalChannelState,
    target: &ChannelTarget,
) -> Option<&'a mut ChannelState> {
    state
        .users
        .get_mut(&target.user_key)
        .and_then(|channels| channels.get_mut(&target.channel_key))
}

fn channel_rule(
    state: &LocalChannelState,
    target: &ChannelTarget,
    type_id: TypeId,
    attached_only: bool,
) -> Option<(DeliveryTarget, DeliveryRule)> {
    let channel = state
        .users
        .get(&target.user_key)
        .and_then(|channels| channels.get(&target.channel_key))?;
    if attached_only && channel.sessions.is_empty() {
        return None;
    }
    channel.rules.get(&type_id).cloned().map(|rule| {
        (
            DeliveryTarget {
                channel: target.clone(),
                generation: channel.generation,
            },
            rule,
        )
    })
}

fn schedule_debounce(
    input: DebounceInput,
    states: &mut HashMap<DebounceKey, Debouncer<Arc<PendingPayload>>>,
    deadlines: &mut DebounceQueue<DebounceKey>,
) {
    let state = states
        .entry(input.key.clone())
        .or_insert_with(|| Debouncer::new(DebounceConf::trailing(input.window)));
    if let Some(deadline) = state.push(input.payload).deadline {
        deadlines.push(input.key, deadline);
    }
}

fn send_live(channel: &mut ChannelState, event: Arc<ChannelEvent>) {
    let mut closed = Vec::new();
    for (id, subscriber) in &channel.sessions {
        match subscriber.sender.try_send(Arc::clone(&event)) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) | Err(mpsc::error::TrySendError::Closed(_)) => {
                closed.push(*id)
            }
        }
    }
    for id in closed {
        channel.sessions.remove(&id);
    }
}

fn event_type<T>() -> String
where
    T: crate::callables::DataValue,
{
    sanitize_event_type(<T as schemars::JsonSchema>::schema_name().as_ref())
}

fn sanitize_event_type(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '\r' | '\n' => '_',
            _ => character,
        })
        .collect()
}
