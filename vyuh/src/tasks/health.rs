//! Per-site task-runtime health derived from existing scheduler work.

use std::sync::Arc;

use parking_lot::Mutex;

use super::TaskReadiness;

/// Safe operational state for the durable task runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskHealthState {
    Initializing,
    Healthy,
    Degraded,
    Unhealthy,
}

impl TaskHealthState {
    /// Returns the stable diagnostic name for this state.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Initializing => "initializing",
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unhealthy => "unhealthy",
        }
    }
}

/// Safe class for the latest task-runtime failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TaskFailureClass {
    Initialization,
    Store,
}

impl TaskFailureClass {
    /// Returns the stable diagnostic name for this failure class.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Initialization => "initialization",
            Self::Store => "store",
        }
    }
}

/// Immutable copy of per-site task health for probes, metrics, and the console.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TaskHealthSnapshot {
    pub(crate) state: TaskHealthState,
    pub(crate) ready: bool,
    pub(crate) consecutive_failures: u32,
    pub(crate) last_success: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) last_failure: Option<TaskFailureClass>,
}

#[derive(Debug)]
struct HealthInner {
    state: TaskHealthState,
    consecutive_failures: u32,
    last_success: Option<chrono::DateTime<chrono::Utc>>,
    last_failure: Option<TaskFailureClass>,
}

/// Mutable per-site health state with no store interaction.
#[derive(Clone)]
pub(crate) struct TaskHealth {
    policy: TaskReadiness,
    enabled: bool,
    inner: Arc<Mutex<HealthInner>>,
}

impl TaskHealth {
    /// Creates task health in its initial state for one built site.
    pub(crate) fn new(policy: TaskReadiness, enabled: bool) -> Self {
        let state = if enabled {
            TaskHealthState::Initializing
        } else {
            TaskHealthState::Healthy
        };
        Self {
            policy,
            enabled,
            inner: Arc::new(Mutex::new(HealthInner {
                state,
                consecutive_failures: 0,
                last_success: None,
                last_failure: None,
            })),
        }
    }

    /// Records that task initialization completed successfully.
    pub(crate) fn initialized(&self) {
        self.succeeded();
    }

    /// Records an initialization failure without retaining the causal chain.
    pub(crate) fn initialization_failed(&self) {
        self.failed(TaskFailureClass::Initialization);
        self.inner.lock().state = TaskHealthState::Unhealthy;
    }

    /// Records one successful scheduler-store turn.
    pub(crate) fn succeeded(&self) {
        let mut inner = self.inner.lock();
        inner.state = TaskHealthState::Healthy;
        inner.consecutive_failures = 0;
        inner.last_success = Some(chrono::Utc::now());
        inner.last_failure = None;
    }

    /// Records one failed scheduler-store turn without preserving error text.
    pub(crate) fn store_failed(&self) {
        self.failed(TaskFailureClass::Store);
    }

    /// Returns whether this task runtime should affect site readiness.
    pub(crate) fn is_ready(&self) -> bool {
        self.ready_state(self.inner.lock().state)
    }

    /// Returns a lock-bounded snapshot without touching the task store.
    pub(crate) fn snapshot(&self) -> TaskHealthSnapshot {
        let inner = self.inner.lock();
        TaskHealthSnapshot {
            state: inner.state,
            ready: self.ready_state(inner.state),
            consecutive_failures: inner.consecutive_failures,
            last_success: inner.last_success,
            last_failure: inner.last_failure,
        }
    }

    /// Applies the configured threshold after one failed task-runtime operation.
    fn failed(&self, class: TaskFailureClass) {
        let mut inner = self.inner.lock();
        inner.consecutive_failures = inner.consecutive_failures.saturating_add(1);
        inner.last_failure = Some(class);
        inner.state = match self.policy {
            TaskReadiness::AfterFailures(limit) if inner.consecutive_failures >= limit => {
                TaskHealthState::Unhealthy
            }
            _ => TaskHealthState::Degraded,
        };
    }

    /// Applies the configured readiness policy to one already-recorded health state.
    fn ready_state(&self, state: TaskHealthState) -> bool {
        !self.enabled
            || matches!(self.policy, TaskReadiness::Disabled)
            || !matches!(
                state,
                TaskHealthState::Initializing | TaskHealthState::Unhealthy
            )
    }
}

#[cfg(test)]
mod tests {
    use super::{TaskHealth, TaskHealthState};
    use crate::tasks::TaskReadiness;

    /// Verifies startup-only readiness tolerates a later transient store failure.
    #[test]
    fn startup_only_stays_ready_after_store_failure() {
        let health = TaskHealth::new(TaskReadiness::startup_only(), true);
        health.initialized();
        health.store_failed();

        assert!(health.is_ready());
        assert_eq!(health.snapshot().state, TaskHealthState::Degraded);
    }

    /// Verifies a configured failure threshold changes and recovers readiness.
    #[test]
    fn threshold_recovers_after_success() {
        let health = TaskHealth::new(TaskReadiness::after_failures(2), true);
        health.initialized();
        health.store_failed();
        assert!(health.is_ready());
        health.store_failed();
        assert!(!health.is_ready());
        health.succeeded();

        assert!(health.is_ready());
        assert_eq!(health.snapshot().state, TaskHealthState::Healthy);
    }

    /// Verifies disabled task readiness never blocks the site probe.
    #[test]
    fn disabled_policy_never_blocks_readiness() {
        let health = TaskHealth::new(TaskReadiness::disabled(), true);
        health.initialization_failed();

        assert!(health.is_ready());
        assert_eq!(health.snapshot().state, TaskHealthState::Unhealthy);
    }
}
