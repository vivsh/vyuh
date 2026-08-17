//! Process-local bounded replay storage for one subscription.

use std::{collections::VecDeque, sync::Arc};

use super::{ChannelEvent, ChannelEventId};

/// Bounded local reconnect convenience for one subscription policy.
///
/// This log is intentionally lossy and process-local. It exists only to make
/// short reconnects smoother; it is not a durable event history.
#[derive(Default)]
pub(crate) struct SubscriptionLog {
    events: VecDeque<Arc<ChannelEvent>>,
}

impl SubscriptionLog {
    /// Retains an accepted event while respecting the configured bound.
    pub(crate) fn append(&mut self, event: Arc<ChannelEvent>, limit: usize) {
        if limit == 0 {
            return;
        }
        self.events.push_back(event);
        while self.events.len() > limit {
            self.events.pop_front();
        }
    }

    /// Returns events newer than a local cursor, limited for one reconnect.
    pub(crate) fn after(&self, after: ChannelEventId, limit: usize) -> Vec<ChannelEvent> {
        self.events
            .iter()
            .filter(|event| event.id > after)
            .take(limit)
            .map(|event| event.as_ref().clone())
            .collect()
    }
}
