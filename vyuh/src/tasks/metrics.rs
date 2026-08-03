//! Bounded-label counters and timings for the durable task runtime.

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use super::{TaskOutcome, TaskReceipt};

const RECEIPTS: [&str; 4] = ["queued", "existing", "ignored", "error"];
const OUTCOMES: [&str; 5] = ["complete", "suspend", "sleep", "retry", "fail"];

pub(crate) struct TaskMetrics {
    handlers: BTreeMap<String, usize>,
    groups: BTreeMap<String, usize>,
    submissions: Vec<[AtomicU64; 4]>,
    starts: Vec<AtomicU64>,
    outcomes: Vec<[AtomicU64; 5]>,
    claims: Vec<AtomicU64>,
    reclaimed: Vec<AtomicU64>,
    idempotency_conflicts: Vec<AtomicU64>,
    renewals: Vec<AtomicU64>,
    ownership_losses: Vec<AtomicU64>,
    queue_micros: Timing,
    handler_micros: Timing,
    commit_micros: Timing,
    store_failures: AtomicU64,
}

#[derive(Default)]
struct Timing {
    count: AtomicU64,
    total: AtomicU64,
}

impl TaskMetrics {
    pub(crate) fn new(
        handlers: impl IntoIterator<Item = String>,
        groups: impl IntoIterator<Item = String>,
    ) -> Self {
        let handlers = indexes(handlers);
        let groups = indexes(groups);
        Self {
            submissions: atomic_matrix(handlers.len()),
            starts: atomic_vector(handlers.len()),
            outcomes: atomic_outcomes(handlers.len()),
            claims: atomic_vector(groups.len()),
            reclaimed: atomic_vector(groups.len()),
            idempotency_conflicts: atomic_vector(handlers.len()),
            renewals: atomic_vector(groups.len()),
            ownership_losses: atomic_vector(groups.len()),
            handlers,
            groups,
            queue_micros: Timing::default(),
            handler_micros: Timing::default(),
            commit_micros: Timing::default(),
            store_failures: AtomicU64::new(0),
        }
    }

    pub(crate) fn submission(
        &self,
        handler: &str,
        result: &Result<Vec<TaskReceipt>, super::TaskError>,
    ) {
        let Some(index) = self.handlers.get(handler).copied() else {
            return;
        };
        match result {
            Ok(receipts) => receipts.iter().for_each(|receipt| {
                self.submissions[index][receipt_index(*receipt)].fetch_add(1, Ordering::Relaxed);
            }),
            Err(super::TaskError::IdempotencyConflict(_)) => {
                self.idempotency_conflicts[index].fetch_add(1, Ordering::Relaxed);
                self.submissions[index][3].fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                self.submissions[index][3].fetch_add(1, Ordering::Relaxed);
                if matches!(
                    error,
                    super::TaskError::DatabaseError(_) | super::TaskError::StoreError(_)
                ) {
                    self.store_failure();
                }
            }
        }
    }

    pub(crate) fn claimed(&self, group: &str, total: usize, reclaimed: usize) {
        increment(&self.groups, &self.claims, group, total as u64);
        increment(&self.groups, &self.reclaimed, group, reclaimed as u64);
    }

    pub(crate) fn started(&self, handler: &str, queue: Duration) {
        increment(&self.handlers, &self.starts, handler, 1);
        self.queue_micros.record(queue);
    }

    pub(crate) fn completed(&self, handler: &str, outcome: &TaskOutcome, elapsed: Duration) {
        if let Some(index) = self.handlers.get(handler) {
            self.outcomes[*index][outcome_index(outcome)].fetch_add(1, Ordering::Relaxed);
        }
        self.handler_micros.record(elapsed);
    }

    pub(crate) fn renewed(&self, group: &str, lost: bool) {
        let counters = if lost {
            &self.ownership_losses
        } else {
            &self.renewals
        };
        increment(&self.groups, counters, group, 1);
    }

    pub(crate) fn commit(&self, elapsed: Duration, failed: bool) {
        self.commit_micros.record(elapsed);
        if failed {
            self.store_failure();
        }
    }

    pub(crate) fn store_failure(&self) {
        self.store_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn render(&self) -> String {
        let mut output = String::from("# TYPE vyuh_task_submissions_total counter\n");
        self.render_handler_metrics(&mut output);
        self.render_group_metrics(&mut output);
        render_timing(&mut output, "queue", &self.queue_micros);
        render_timing(&mut output, "handler", &self.handler_micros);
        render_timing(&mut output, "commit", &self.commit_micros);
        let failures = self.store_failures.load(Ordering::Relaxed);
        let _ = writeln!(
            output,
            "# TYPE vyuh_task_store_failures_total counter\nvyuh_task_store_failures_total {failures}"
        );
        output
    }

    fn render_handler_metrics(&self, output: &mut String) {
        render_matrix(
            output,
            "vyuh_task_submissions_total",
            "handler",
            &self.handlers,
            &self.submissions,
            &RECEIPTS,
        );
        output.push_str("# TYPE vyuh_task_starts_total counter\n");
        render_vector(
            output,
            "vyuh_task_starts_total",
            "handler",
            &self.handlers,
            &self.starts,
        );
        output.push_str("# TYPE vyuh_task_outcomes_total counter\n");
        render_matrix(
            output,
            "vyuh_task_outcomes_total",
            "handler",
            &self.handlers,
            &self.outcomes,
            &OUTCOMES,
        );
        output.push_str("# TYPE vyuh_task_idempotency_conflicts_total counter\n");
        render_vector(
            output,
            "vyuh_task_idempotency_conflicts_total",
            "handler",
            &self.handlers,
            &self.idempotency_conflicts,
        );
    }

    fn render_group_metrics(&self, output: &mut String) {
        output.push_str("# TYPE vyuh_task_claims_total counter\n");
        render_vector(
            output,
            "vyuh_task_claims_total",
            "group",
            &self.groups,
            &self.claims,
        );
        output.push_str("# TYPE vyuh_task_lease_renewals_total counter\n");
        render_vector(
            output,
            "vyuh_task_lease_renewals_total",
            "group",
            &self.groups,
            &self.renewals,
        );
        output.push_str("# TYPE vyuh_task_reclaimed_leases_total counter\n");
        render_vector(
            output,
            "vyuh_task_reclaimed_leases_total",
            "group",
            &self.groups,
            &self.reclaimed,
        );
        output.push_str("# TYPE vyuh_task_lease_losses_total counter\n");
        render_vector(
            output,
            "vyuh_task_lease_losses_total",
            "group",
            &self.groups,
            &self.ownership_losses,
        );
    }
}

impl Timing {
    fn record(&self, duration: Duration) {
        self.count.fetch_add(1, Ordering::Relaxed);
        let micros = u64::try_from(duration.as_micros()).unwrap_or(u64::MAX);
        self.total.fetch_add(micros, Ordering::Relaxed);
    }
}

fn indexes(values: impl IntoIterator<Item = String>) -> BTreeMap<String, usize> {
    values
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .enumerate()
        .map(|(index, name)| (name, index))
        .collect()
}

fn atomic_vector(size: usize) -> Vec<AtomicU64> {
    (0..size).map(|_| AtomicU64::new(0)).collect()
}

fn atomic_matrix(size: usize) -> Vec<[AtomicU64; 4]> {
    (0..size)
        .map(|_| std::array::from_fn(|_| AtomicU64::new(0)))
        .collect()
}

fn atomic_outcomes(size: usize) -> Vec<[AtomicU64; 5]> {
    (0..size)
        .map(|_| std::array::from_fn(|_| AtomicU64::new(0)))
        .collect()
}

fn increment(indexes: &BTreeMap<String, usize>, counters: &[AtomicU64], name: &str, value: u64) {
    if let Some(index) = indexes.get(name) {
        counters[*index].fetch_add(value, Ordering::Relaxed);
    }
}

fn receipt_index(receipt: TaskReceipt) -> usize {
    match receipt {
        TaskReceipt::Queued(_) => 0,
        TaskReceipt::Existing(_) => 1,
        TaskReceipt::Ignored(_) => 2,
    }
}

fn outcome_index(outcome: &TaskOutcome) -> usize {
    match outcome {
        TaskOutcome::Complete => 0,
        TaskOutcome::Suspend { .. } => 1,
        TaskOutcome::Sleep { .. } => 2,
        TaskOutcome::Retry { .. } => 3,
        TaskOutcome::Fail { .. } => 4,
    }
}

fn render_vector(
    output: &mut String,
    metric: &str,
    label: &str,
    indexes: &BTreeMap<String, usize>,
    counters: &[AtomicU64],
) {
    for (name, index) in indexes {
        let value = counters[*index].load(Ordering::Relaxed);
        let _ = writeln!(output, "{metric}{{{label}=\"{}\"}} {value}", escape(name));
    }
}

fn render_matrix<const N: usize>(
    output: &mut String,
    metric: &str,
    label: &str,
    indexes: &BTreeMap<String, usize>,
    counters: &[[AtomicU64; N]],
    outcomes: &[&str; N],
) {
    for (name, index) in indexes {
        for (outcome, counter) in outcomes.iter().zip(&counters[*index]) {
            let value = counter.load(Ordering::Relaxed);
            let _ = writeln!(
                output,
                "{metric}{{{label}=\"{}\",outcome=\"{outcome}\"}} {value}",
                escape(name)
            );
        }
    }
}

fn render_timing(output: &mut String, name: &str, timing: &Timing) {
    let count = timing.count.load(Ordering::Relaxed);
    let total = timing.total.load(Ordering::Relaxed);
    let _ = writeln!(
        output,
        "vyuh_task_{name}_duration_seconds_count {count}\nvyuh_task_{name}_duration_seconds_sum {}",
        total as f64 / 1_000_000.0
    );
}

fn escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
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
        let rendered = metrics.render();
        assert!(rendered.contains("handler=\"known\\\"task\",outcome=\"queued\"} 1"));
        assert!(!rendered.contains("handler=\"unknown\""));
        assert!(!rendered.contains("group=\"unknown\""));
    }
}
