#![allow(dead_code)]

use std::collections::{BinaryHeap, HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ScheduleKey(u32);

struct ScheduleWork {
    key: ScheduleKey,
    deadline: tokio::time::Instant,
}

impl ScheduleWork {
    fn new(key: ScheduleKey, deadline: tokio::time::Instant) -> Self {
        Self { key, deadline }
    }
}

impl Eq for ScheduleWork {}

impl PartialEq for ScheduleWork {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Ord for ScheduleWork {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.deadline.cmp(&self.deadline)
    }
}

impl PartialOrd for ScheduleWork {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct ScheduledQueue {
    counter: u32,
    heap: BinaryHeap<ScheduleWork>,
    notifier: tokio::sync::Notify,
    updates: HashMap<ScheduleKey, tokio::time::Instant>,
}

impl ScheduledQueue {
    pub fn new() -> Self {
        Self {
            counter: 0,
            heap: BinaryHeap::new(),
            notifier: tokio::sync::Notify::new(),
            updates: HashMap::new(),
        }
    }

    pub fn insert(&mut self, deadline: tokio::time::Instant) -> ScheduleKey {
        let key = ScheduleKey(self.counter);
        self.counter = self.counter.wrapping_add(1);
        let work = ScheduleWork::new(key, deadline);
        self.heap.push(work);
        self.updates.insert(key, deadline);
        self.notifier.notify_one();
        key
    }

    pub fn remove(&mut self, key: ScheduleKey) -> bool {
        let mut removed = false;
        self.heap = self
            .heap
            .drain()
            .filter(|work| {
                if work.key == key {
                    removed = true;
                    false
                } else {
                    true
                }
            })
            .collect();
        if removed {
            self.notifier.notify_one();
        }
        removed
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub fn clear(&mut self) {
        self.heap.clear();
        self.notifier.notify_one();
    }

    /// Updates the deadline of the item with the given key if it exists, returning true if an update was made.
    /// IMPORTANT: Deadline revisions must always be extensions (i.e. new deadline must be >= current deadline)
    /// to ensure correct ordering and avoid starvation.
    pub fn update(&mut self, key: ScheduleKey, deadline: tokio::time::Instant) -> bool {
        let mut updated = false;
        if let Some(w) = self.updates.get_mut(&key) {
            *w = deadline.max(*w);
            updated = true;
            self.notifier.notify_one();
        }
        updated
    }

    pub async fn pop(&mut self) -> ScheduleKey {
        loop {
            if let Some(work) = self.heap.peek() {
                let now = tokio::time::Instant::now();
                if work.deadline <= now {
                    if let Some(mut work) = self.heap.pop() {
                        if let Some(updated_deadline) = self.updates.get(&work.key)
                            && *updated_deadline > work.deadline
                        {
                            work.deadline = *updated_deadline;
                            self.heap.push(work);
                            continue;
                        }
                        return work.key;
                    } else {
                        continue;
                    }
                }

                let wait = work.deadline - now;
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {},
                    _ = self.notifier.notified() => {},
                }
            } else {
                self.notifier.notified().await;
            }
        }
    }
}
