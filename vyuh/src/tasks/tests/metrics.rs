use super::*;

/// Verifies metric labels are fixed at startup and unknown names never become labels.
#[test]
fn task_metrics_use_only_preallocated_safe_labels() {
    let metrics = TaskMetrics::new(["known\"task".into()], ["default".into()]);
    metrics.submission(
        "known\"task",
        &Ok(vec![TaskReceipt::Queued(super::super::TaskId::new(
            uuid::Uuid::nil(),
        ))]),
    );
    metrics.claimed("unknown", 1, 1);
    metrics.started("unknown", Duration::ZERO);
    let health = super::super::TaskHealth::new(super::super::TaskReadiness::startup_only(), true);
    health.initialized();
    let rendered = metrics.render(health.snapshot());
    assert!(rendered.contains("handler=\"known\\\"task\",outcome=\"queued\"} 1"));
    assert!(!rendered.contains("handler=\"unknown\""));
    assert!(!rendered.contains("lane=\"unknown\""));
}

/// Verifies task-health diagnostics expose only a fixed failure class label.
#[test]
fn task_health_metrics_use_safe_failure_class() {
    let metrics = TaskMetrics::new(Vec::new(), Vec::new());
    let health = super::super::TaskHealth::new(super::super::TaskReadiness::startup_only(), true);
    health.initialized();
    health.store_failed();

    let rendered = metrics.render(health.snapshot());

    assert!(rendered.contains("vyuh_task_last_failure_info{class=\"store\"} 1"));
    assert!(!rendered.contains("failed to run durable task scheduler"));
}
