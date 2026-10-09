//! Invocation dispatch and permit-carrying completion collection.
use super::engine::{
    Completion, DbRequest, LaneState, RunMode, RunningInvocation, TaskExecution, TaskRunner,
};
use crate::Site;
use crate::tasks::client::RunnerEvent;
use crate::tasks::{TaskCommit, TaskLane, TaskRecord, TaskRegistry, TaskStore};
use futures::FutureExt;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};
use tokio::sync::{OwnedSemaphorePermit, mpsc};

/// Keeps Flow runnable while Work lacks execution capacity or local rate permits.
fn next_invocation(
    queue: &mut LaneState,
    global_available: bool,
    now: tokio::time::Instant,
) -> Option<usize> {
    if global_available
        && queue.execution_slots.available_permits() > 0
        && queue.claim_limit(1, now) > 0
    {
        return Some(0);
    }
    let next = queue
        .tasks
        .iter()
        .position(|task| task.kind == crate::tasks::TaskKind::Flow);
    if next.is_none()
        && let Some(wait) = queue.local_rate_wake(now)
    {
        queue.poll_after = now + wait;
    }
    next
}

impl<S: TaskStore + Send + Sync + 'static> TaskRunner<S> {
    /// Collects a bounded control batch without retaining a cancellation cache between turns.
    pub(super) fn collect_events(&mut self) {
        let mut cancelled = HashSet::new();
        let mut wake = false;
        let mut lanes = HashSet::new();
        for _ in 0..super::engine::SCHEDULER_BUDGET {
            match self.events.try_recv() {
                Ok(RunnerEvent::Wake) => wake = true,
                Ok(RunnerEvent::WakeLane(lane)) => {
                    lanes.insert(lane);
                }
                Ok(RunnerEvent::Cancel(id)) => {
                    cancelled.insert(id);
                    wake = true;
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Lagged(count)) => {
                    self.control_lag(count);
                }
                Err(_) => break,
            }
        }
        if !cancelled.is_empty() && self.accepts_events() {
            self.cancel_queued(&cancelled);
        }
        if wake {
            self.control_wake();
        } else if self.mode == RunMode::Running && self.accepts_events() {
            for lane in lanes {
                self.wake_lane(lane);
            }
        }
    }

    /// Handles a selected control event; channel closure cannot cause a permanent ready branch.
    pub(super) fn accept_event(
        &mut self,
        event: Result<RunnerEvent, tokio::sync::broadcast::error::RecvError>,
    ) {
        if !self.accepts_events() {
            return;
        }
        match event {
            Ok(RunnerEvent::Wake) => self.control_wake(),
            Ok(RunnerEvent::WakeLane(lane)) => {
                if self.mode == RunMode::Running {
                    self.wake_lane(lane);
                }
            }
            Ok(RunnerEvent::Cancel(id)) => {
                self.cancel_queued(&HashSet::from([id]));
                self.control_wake();
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => self.control_lag(count),
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {}
        }
    }

    fn accepts_events(&self) -> bool {
        !self.stopped()
            && !self
                .in_flight
                .values()
                .any(|entry| matches!(&*entry.request, DbRequest::GiveBack))
    }

    fn control_wake(&mut self) {
        if self.mode == RunMode::Running && self.accepts_events() {
            self.wake_lanes();
        }
    }

    fn control_lag(&mut self, count: u64) {
        tracing::warn!(
            skipped = count,
            "task control notifications lagged; durable store state remains authoritative"
        );
        self.control_wake();
    }

    /// Drops queued snapshots only after the store acknowledged terminal cancellation.
    fn cancel_queued(&mut self, ids: &HashSet<crate::tasks::TaskId>) {
        for lane in &mut self.lanes {
            lane.tasks.retain(|record| !ids.contains(&record.id()));
            for id in ids {
                lane.forget_flow(*id);
            }
        }
    }

    pub(super) fn acknowledge_commits(&mut self, commits: &[TaskCommit]) {
        let mut committed = HashMap::<TaskLane, usize>::new();
        for commit in commits {
            if let Some(lane) = commit.lane() {
                *committed.entry(lane).or_default() += 1;
            }
        }
        for (lane, count) in committed {
            if let Some(queue) = self.lane_mut(lane) {
                queue.uncommitted = queue.uncommitted.saturating_sub(count);
            }
        }
    }

    /// Buffers accepted outcomes before releasing either invocation permit.
    pub(super) fn accept_completion(&mut self, mut completion: Completion) {
        let Some(invocation) = self.running_invocations.get(&completion.invocation_id) else {
            return;
        };
        completion.commits.retain(|commit| {
            self.running_tasks.get(&commit.task_id()) == Some(&completion.invocation_id)
        });
        let count = completion.commits.len();
        self.output_buffer.extend(completion.commits);
        for id in &invocation.task_ids {
            self.running_tasks.remove(id);
        }
        self.running_invocations.remove(&completion.invocation_id);
        if let Some(lane) = self.lane_mut(completion.lane) {
            lane.uncommitted = lane.uncommitted.saturating_add(count);
            lane.completed_work = true;
        }
        self.wake_lane(completion.lane);
        drop(completion.global_permit);
        drop(completion.lane_permit);
    }

    /// Gives every queued lane an opportunity without modifying fetch order.
    pub(super) fn dispatch_ready(&mut self, site: &Site, sender: &mpsc::Sender<Completion>) {
        if self.mode != RunMode::Running {
            return;
        }
        for _ in 0..super::engine::SCHEDULER_BUDGET {
            let mut progress = false;
            for index in 0..self.lanes.len() {
                let Some((lane, token, records, global, local)) = self.pop_lane(index) else {
                    continue;
                };
                self.spawn_task(
                    site.clone(),
                    sender.clone(),
                    lane,
                    token,
                    records,
                    global,
                    local,
                );
                progress = true;
            }
            if !progress {
                break;
            }
        }
    }

    /// Takes both permits without waiting; unsuccessful admission leaves input untouched.
    pub(super) fn pop_lane(
        &mut self,
        index: usize,
    ) -> Option<(
        TaskLane,
        Option<String>,
        Vec<Arc<TaskRecord>>,
        Option<OwnedSemaphorePermit>,
        Option<OwnedSemaphorePermit>,
    )> {
        let queue = self.lanes.get_mut(index)?;
        let now = tokio::time::Instant::now();
        let next = next_invocation(queue, self.execution_slots.available_permits() > 0, now)?;
        let flow = queue.tasks.get(next)?.kind == crate::tasks::TaskKind::Flow;
        let global = if flow {
            None
        } else {
            Some(self.execution_slots.clone().try_acquire_owned().ok()?)
        };
        let local = if flow {
            None
        } else {
            Some(queue.execution_slots.clone().try_acquire_owned().ok()?)
        };
        let first = queue.tasks.remove(next)?;
        let count = queue.tasks.len().saturating_add(1);
        let limit = if !flow {
            queue.claim_limit(count, now)
        } else {
            count
        };
        let records = collect_invocation(queue, first, limit, &self.registry);
        if !flow {
            queue.consume_local_rate(records.len(), now);
        }
        Some((
            queue.conf.lane(),
            queue.owner_token.clone(),
            records,
            global,
            local,
        ))
    }

    /// Spawns one invocation whose permits are transferred to its completion message.
    pub(super) fn spawn_task(
        &mut self,
        site: Site,
        sender: mpsc::Sender<Completion>,
        lane: TaskLane,
        owner_token: Option<String>,
        records: Vec<Arc<TaskRecord>>,
        global_permit: Option<OwnedSemaphorePermit>,
        lane_permit: Option<OwnedSemaphorePermit>,
    ) {
        let engine = self.registry.clone();
        let payload_limit = self.registry.config.payload_limit();
        let error_limit = self.registry.config.error_limit();
        let invocation_id = uuid::Uuid::now_v7();
        self.record_starts(&records);
        let task_ids = records.iter().map(|record| record.id()).collect::<Vec<_>>();
        for record in &records {
            if let Some(queue) = self.lane_mut(lane) {
                queue.remember_flow(record.clone());
            }
        }
        let future = execute_task(TaskExecution {
            node_id: self.node_id,
            invocation_id,
            engine,
            site,
            records,
            sender,
            lane,
            metrics: self.metrics.clone(),
            payload_limit,
            error_limit,
            owner_token,
            global_permit,
            lane_permit,
        });
        let handle = tokio::spawn(future);
        self.replace_duplicates(&task_ids);
        for task_id in &task_ids {
            self.running_tasks.insert(*task_id, invocation_id);
        }
        let running = RunningInvocation {
            lane,
            task_ids,
            abort: handle.abort_handle(),
        };
        self.running_invocations.insert(invocation_id, running);
    }
    pub(super) fn record_starts(&self, records: &[Arc<TaskRecord>]) {
        let now = chrono::Utc::now();
        for record in records {
            let queue_time = (now - record.created_at).to_std().unwrap_or_default();
            self.metrics.started(record.name(), queue_time);
        }
        if let Some(record) = records.first()
            && self.registry.is_batch(record.name())
        {
            self.metrics.batch_started(record.name(), records.len());
        }
    }
    pub(super) fn replace_duplicates(&mut self, task_ids: &[crate::tasks::TaskId]) {
        let duplicates = task_ids
            .iter()
            .filter_map(|task_id| self.running_tasks.get(task_id).copied())
            .collect::<HashSet<_>>();
        for invocation in duplicates {
            self.abort_invocation(invocation);
            tracing::error!(%invocation, "duplicate local task invocation was replaced");
        }
    }
    pub(super) fn abort_invocation(&mut self, id: uuid::Uuid) {
        let Some(invocation) = self.running_invocations.remove(&id) else {
            return;
        };
        invocation.abort.abort();
        for task_id in &invocation.task_ids {
            self.running_tasks.remove(task_id);
        }
    }
}
pub(super) fn collect_invocation(
    lane: &mut LaneState,
    first: Arc<TaskRecord>,
    limit: usize,
    registry: &TaskRegistry,
) -> Vec<Arc<TaskRecord>> {
    if limit <= 1 || !registry.is_batch(first.name()) {
        return vec![first];
    }
    let name = first.name().to_owned();
    let mut records = Vec::with_capacity(limit.min(lane.tasks.len().saturating_add(1)));
    let mut remaining = VecDeque::with_capacity(lane.tasks.len());
    records.push(first);
    while let Some(record) = lane.tasks.pop_front() {
        if records.len() < limit && record.name() == name {
            records.push(record);
        } else {
            remaining.push_back(record);
        }
    }
    lane.tasks = remaining;
    records
}
/// Contains handler panics and transfers permits with normalized outcomes.
pub(super) async fn execute_task(execution: TaskExecution) {
    let task_name = execution
        .records
        .first()
        .map_or("unknown", |record| record.name())
        .to_owned();
    let started = std::time::Instant::now();
    let results = match std::panic::AssertUnwindSafe(
        execution
            .engine
            .execute_many(execution.site.clone(), execution.records.clone()),
    )
    .catch_unwind()
    .await
    {
        Ok(results) => results,
        Err(_) => {
            tracing::error!(
                task = %task_name,
                count = execution.records.len(),
                "task handler panicked"
            );
            panic_results(execution.records.clone())
        }
    };
    let commits = task_commits(&execution, results);
    execution.metrics.handler_completed(started.elapsed());
    if execution
        .sender
        .send(Completion {
            invocation_id: execution.invocation_id,
            lane: execution.lane,
            commits,
            global_permit: execution.global_permit,
            lane_permit: execution.lane_permit,
        })
        .await
        .is_err()
    {
        tracing::error!(task = %task_name, "task completion channel closed before commit");
    }
}
pub(super) fn panic_results(
    records: Vec<Arc<TaskRecord>>,
) -> Vec<crate::tasks::handler::TaskExecutionResult> {
    records
        .into_iter()
        .map(|record| crate::tasks::handler::TaskExecutionResult {
            record,
            outcome: crate::tasks::TaskOutcome::fail("Task handler panicked"),
        })
        .collect()
}
pub(super) fn task_commits(
    execution: &TaskExecution,
    results: Vec<crate::tasks::handler::TaskExecutionResult>,
) -> Vec<TaskCommit> {
    results
        .into_iter()
        .map(|result| {
            let outcome = crate::tasks::store::normalize_outcome(
                result.outcome,
                execution.payload_limit,
                execution.error_limit,
            );
            execution.metrics.outcome(result.record.name(), &outcome);
            TaskCommit::Outcome {
                node_id: execution.node_id,
                attempts: result.record.attempts,
                task_id: result.record.id(),
                lane: execution.lane,
                outcome,
                owner_token: execution.owner_token.clone(),
            }
        })
        .collect()
}
