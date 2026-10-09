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
