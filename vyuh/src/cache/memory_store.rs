//! Private bounded in-memory storage shared by framework-local caches.

use std::collections::{HashMap, VecDeque};

use parking_lot::Mutex;

/// Limits applied to one in-memory store instance.
#[derive(Clone, Copy)]
pub(crate) struct MemoryLimits {
    max_entries: usize,
    max_bytes: Option<usize>,
}

impl MemoryLimits {
    /// Creates limits with a required entry bound and an optional byte bound.
    pub(crate) fn new(max_entries: usize, max_bytes: Option<usize>) -> Self {
        Self {
            max_entries,
            max_bytes,
        }
    }
}

/// The state transition selected while accessing an entry.
pub(crate) enum MemoryAction<V, R> {
    Keep(R),
    Remove(R),
    Replace { value: V, bytes: usize, result: R },
}

/// A bounded LRU store with no expiry or serialization policy of its own.
pub(crate) struct MemoryStore<V> {
    state: Mutex<MemoryState<V>>,
}

struct MemoryState<V> {
    entries: HashMap<String, StoredValue<V>>,
    lru: VecDeque<(String, u64)>,
    retained_bytes: usize,
    sequence: u64,
    limits: MemoryLimits,
}

struct StoredValue<V> {
    value: V,
    bytes: usize,
    sequence: u64,
}

impl<V> MemoryStore<V> {
    /// Creates an independent bounded store.
    pub(crate) fn new(limits: MemoryLimits) -> Self {
        Self {
            state: Mutex::new(MemoryState::new(limits)),
        }
    }

    /// Replaces the limits and evicts entries that no longer fit.
    pub(crate) fn set_limits(&self, limits: MemoryLimits) {
        let mut state = self.state.lock();
        state.limits = limits;
        state.evict();
    }

    /// Inserts or replaces an entry and applies the current bounds.
    pub(crate) fn insert(&self, key: &str, value: V, bytes: usize) {
        self.state.lock().insert(key, value, bytes);
    }

    /// Removes an entry when it exists.
    pub(crate) fn remove(&self, key: &str) {
        self.state.lock().remove(key);
    }

    /// Retains entries selected by a caller-owned scope predicate.
    pub(crate) fn retain(&self, mut keep: impl FnMut(&str) -> bool) {
        self.state.lock().retain(|key| keep(key));
    }

    /// Applies one atomic read/update decision without exposing the lock.
    pub(crate) fn access<R>(
        &self,
        key: &str,
        action: impl FnOnce(Option<&V>) -> MemoryAction<V, R>,
    ) -> R {
        let mut state = self.state.lock();
        let action = action(state.entries.get(key).map(|entry| &entry.value));
        state.apply(key, action)
    }
}

impl<V> MemoryState<V> {
    /// Creates an empty state using the supplied bounds.
    fn new(limits: MemoryLimits) -> Self {
        Self {
            entries: HashMap::new(),
            lru: VecDeque::new(),
            retained_bytes: 0,
            sequence: 0,
            limits,
        }
    }

    /// Completes an atomic caller-selected access action.
    fn apply<R>(&mut self, key: &str, action: MemoryAction<V, R>) -> R {
        match action {
            MemoryAction::Keep(result) => {
                if self.entries.contains_key(key) {
                    self.touch(key);
                }
                result
            }
            MemoryAction::Remove(result) => {
                self.remove(key);
                result
            }
            MemoryAction::Replace {
                value,
                bytes,
                result,
            } => {
                self.insert(key, value, bytes);
                result
            }
        }
    }

    /// Inserts an entry after dropping any previous value for its key.
    fn insert(&mut self, key: &str, value: V, bytes: usize) {
        self.remove(key);
        let sequence = self.next_sequence();
        self.retained_bytes = self.retained_bytes.saturating_add(bytes);
        self.entries.insert(
            key.into(),
            StoredValue {
                value,
                bytes,
                sequence,
            },
        );
        self.lru.push_back((key.into(), sequence));
        self.evict();
    }

    /// Removes all entries rejected by the supplied predicate.
    fn retain(&mut self, mut keep: impl FnMut(&str) -> bool) {
        self.entries.retain(|key, _| keep(key));
        self.retained_bytes = self
            .entries
            .values()
            .fold(0_usize, |total, entry| total.saturating_add(entry.bytes));
        self.compact_lru();
    }

    /// Removes one entry while maintaining byte accounting.
    fn remove(&mut self, key: &str) {
        if let Some(entry) = self.entries.remove(key) {
            self.retained_bytes = self.retained_bytes.saturating_sub(entry.bytes);
        }
    }

    /// Updates the LRU location of an existing entry.
    fn touch(&mut self, key: &str) {
        let sequence = self.next_sequence();
        if let Some(entry) = self.entries.get_mut(key) {
            entry.sequence = sequence;
            self.lru.push_back((key.into(), sequence));
        }
        self.compact_lru();
    }

    /// Evicts least-recently-used entries until all configured bounds hold.
    fn evict(&mut self) {
        while self.over_limit() {
            if !self.evict_oldest() {
                break;
            }
        }
        self.compact_lru();
    }

    /// Reports whether entry or byte bounds are currently exceeded.
    fn over_limit(&self) -> bool {
        self.entries.len() > self.limits.max_entries
            || self
                .limits
                .max_bytes
                .is_some_and(|limit| self.retained_bytes > limit)
    }

    /// Removes the current least-recently-used entry, if any.
    fn evict_oldest(&mut self) -> bool {
        while let Some((key, sequence)) = self.lru.pop_front() {
            let current = self
                .entries
                .get(&key)
                .is_some_and(|entry| entry.sequence == sequence);
            if current {
                self.remove(&key);
                return true;
            }
        }
        false
    }

    /// Bounds stale LRU markers created by repeated reads.
    fn compact_lru(&mut self) {
        let limit = self.entries.len().saturating_mul(4).saturating_add(16);
        if self.lru.len() <= limit {
            return;
        }
        self.lru = self
            .entries
            .iter()
            .map(|(key, entry)| (key.clone(), entry.sequence))
            .collect();
    }

    /// Produces a monotonically cycling marker for LRU records.
    fn next_sequence(&mut self) -> u64 {
        self.sequence = self.sequence.wrapping_add(1);
        self.sequence
    }
}
