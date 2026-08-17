//! Reusable in-process debounce state and deadline scheduling.

use std::{collections::BinaryHeap, time::Duration};

use serde::{Deserialize, Serialize};

/// Selects which values a debounce window emits.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum DebounceMode {
    /// Emit the first value immediately and suppress later values in the window.
    Leading,
    /// Emit only the latest value after the quiet window.
    Trailing,
    /// Emit the first value immediately and the latest later value after the quiet window.
    LeadingAndTrailing,
}

impl DebounceMode {
    /// Returns the stable configuration spelling for this mode.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Leading => "leading",
            Self::Trailing => "trailing",
            Self::LeadingAndTrailing => "leading_trailing",
        }
    }
}

/// Time window and behavior for a debounce operation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DebounceConf {
    /// The quiet window applied after each accepted value.
    pub window: Duration,
    /// The value-selection behavior applied in that window.
    pub mode: DebounceMode,
}

impl DebounceConf {
    /// Returns whether this configuration can schedule a deadline.
    pub const fn is_valid(&self) -> bool {
        !self.window.is_zero()
    }

    /// Creates a trailing-edge configuration for the supplied quiet window.
    pub const fn trailing(window: Duration) -> Self {
        Self {
            window,
            mode: DebounceMode::Trailing,
        }
    }
}

/// One generation-guarded deadline produced by a [`Debouncer`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct DebounceDeadline {
    generation: u64,
    deadline: tokio::time::Instant,
}

impl DebounceDeadline {
    /// Returns the generation that must still be current at delivery time.
    pub(crate) const fn generation(self) -> u64 {
        self.generation
    }

    /// Returns when this deadline becomes due.
    pub(crate) const fn deadline(self) -> tokio::time::Instant {
        self.deadline
    }
}

/// Result of accepting one value into a [`Debouncer`].
pub(crate) struct DebouncePush<T> {
    pub(crate) emission: Option<T>,
    pub(crate) deadline: Option<DebounceDeadline>,
}

/// State for one independently debounced value stream.
pub(crate) struct Debouncer<T> {
    conf: DebounceConf,
    active: bool,
    generation: u64,
    pending: Option<T>,
    saw_extra: bool,
}

impl<T> Debouncer<T> {
    /// Creates state for one stream using a previously validated configuration.
    pub(crate) fn new(conf: DebounceConf) -> Self {
        Self {
            conf,
            active: false,
            generation: 0,
            pending: None,
            saw_extra: false,
        }
    }

    /// Returns the immutable configuration for this state.
    pub(crate) fn conf(&self) -> &DebounceConf {
        &self.conf
    }

    /// Returns the current deadline generation for internal verification.
    #[cfg(test)]
    pub(crate) const fn generation(&self) -> u64 {
        self.generation
    }

    /// Accepts one value and returns any immediate emission and replacement deadline.
    pub(crate) fn push(&mut self, value: T) -> DebouncePush<T> {
        match self.conf.mode {
            DebounceMode::Leading => self.push_leading(value),
            DebounceMode::Trailing => self.push_trailing(value),
            DebounceMode::LeadingAndTrailing => self.push_both(value),
        }
    }

    /// Completes the current generation when the matching deadline becomes due.
    pub(crate) fn due(&mut self, generation: u64) -> Option<T> {
        if generation != self.generation {
            return None;
        }
        match self.conf.mode {
            DebounceMode::Leading => {
                self.reset();
                None
            }
            DebounceMode::Trailing => {
                self.active = false;
                self.pending.take()
            }
            DebounceMode::LeadingAndTrailing => self.finish_both(),
        }
    }

    fn push_leading(&mut self, value: T) -> DebouncePush<T> {
        if self.active {
            return DebouncePush {
                emission: None,
                deadline: None,
            };
        }
        let deadline = self.start();
        DebouncePush {
            emission: Some(value),
            deadline: Some(deadline),
        }
    }

    fn push_trailing(&mut self, value: T) -> DebouncePush<T> {
        self.active = true;
        self.pending = Some(value);
        DebouncePush {
            emission: None,
            deadline: Some(self.deadline()),
        }
    }

    fn push_both(&mut self, value: T) -> DebouncePush<T> {
        if !self.active {
            let deadline = self.start();
            return DebouncePush {
                emission: Some(value),
                deadline: Some(deadline),
            };
        }
        self.saw_extra = true;
        self.pending = Some(value);
        DebouncePush {
            emission: None,
            deadline: Some(self.deadline()),
        }
    }

    fn finish_both(&mut self) -> Option<T> {
        let emit = self.saw_extra;
        self.active = false;
        self.saw_extra = false;
        if emit {
            self.pending.take()
        } else {
            self.pending = None;
            None
        }
    }

    fn start(&mut self) -> DebounceDeadline {
        self.active = true;
        self.pending = None;
        self.saw_extra = false;
        self.deadline()
    }

    fn deadline(&mut self) -> DebounceDeadline {
        self.generation = self.generation.wrapping_add(1);
        DebounceDeadline {
            generation: self.generation,
            deadline: tokio::time::Instant::now() + self.conf.window,
        }
    }

    fn reset(&mut self) {
        self.active = false;
        self.pending = None;
        self.saw_extra = false;
    }
}

/// A deadline paired with the owner that must process it.
pub(crate) struct QueuedDeadline<K> {
    pub(crate) key: K,
    pub(crate) deadline: DebounceDeadline,
}

impl<K> Eq for QueuedDeadline<K> {}

impl<K> PartialEq for QueuedDeadline<K> {
    fn eq(&self, other: &Self) -> bool {
        self.deadline.deadline() == other.deadline.deadline()
    }
}

impl<K> Ord for QueuedDeadline<K> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.deadline.deadline().cmp(&self.deadline.deadline())
    }
}

impl<K> PartialOrd for QueuedDeadline<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A single-task deadline heap shared by independent debounce streams.
pub(crate) struct DebounceQueue<K> {
    heap: BinaryHeap<QueuedDeadline<K>>,
    notifier: tokio::sync::Notify,
}

impl<K> DebounceQueue<K> {
    /// Creates an empty deadline queue.
    pub(crate) fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            notifier: tokio::sync::Notify::new(),
        }
    }

    /// Adds a replacement deadline and wakes the waiting scheduler.
    pub(crate) fn push(&mut self, key: K, deadline: DebounceDeadline) {
        self.heap.push(QueuedDeadline { key, deadline });
        self.notifier.notify_one();
    }

    /// Waits until the earliest deadline is due and returns it.
    pub(crate) async fn pop(&mut self) -> Option<QueuedDeadline<K>> {
        loop {
            if let Some(next) = self.heap.peek() {
                let now = tokio::time::Instant::now();
                if next.deadline.deadline() <= now {
                    return self.heap.pop();
                }
                tokio::select! {
                    _ = tokio::time::sleep_until(next.deadline.deadline()) => {},
                    _ = self.notifier.notified() => {},
                }
            } else {
                self.notifier.notified().await;
            }
        }
    }
}
