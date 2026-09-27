//! Local handler invocation grouping and dispatch.
use super::*;

impl<S: AbstractTaskStore + Send + Sync + 'static> AbstractTaskRunner<S> {
    /// Stops renewal for cancelled shared members without discarding unaffected execution.
    pub(super) fn detach_cancelled(&mut self, ids: &[crate::tasks::TaskId]) {
        for id in ids {
            let shared = self
                .running_tasks
                .get(id)
                .and_then(|invocation| self.running_invocations.get(invocation))
                .is_some_and(|invocation| invocation.task_ids.len() > 1);
            if shared {
                self.running_tasks.remove(id);
            }
        }
    }

    /// Accepts only results still owned locally; cancelled shared members have been detached.
    pub(super) fn accept_completion(
        &mut self,
        mut completion: Completion,
        commits: &mut Vec<TaskCommit>,
    ) {
        let Some(invocation) = self.running_invocations.remove(&completion.invocation_id) else {
            return;
        };
        if invocation.task_ids.len() > 1 {
            completion.commits.retain(|commit| {
                self.running_tasks.get(&commit.task_id) == Some(&completion.invocation_id)
            });
        }
        for task_id in invocation.task_ids {
            self.running_tasks.remove(&task_id);
        }
        self.running = self.running.saturating_sub(1);
        if let Some(lane) = self.lane_mut(completion.lane) {
            lane.running = lane.running.saturating_sub(1);
            lane.uncommitted = lane.uncommitted.saturating_add(completion.commits.len());
            lane.completed_work = true;
            lane.poll_after = tokio::time::Instant::now();
        }
        self.queue_commits(completion.commits, commits);
    }

    pub(super) fn dispatch_ready(
        &mut self,
        site: &Site,
        sender: &mpsc::Sender<Completion>,
    ) -> Option<tokio::time::Instant> {
        while self.running < self.concurrency {
            let Some((lane, owner_token, records)) = self.pop_ready() else {
                break;
            };
            self.running += 1;
            self.spawn_task(site.clone(), sender.clone(), lane, owner_token, records);
        }
        self.lanes
            .iter()
            .filter(|lane| lane.conf.lane_lock().is_some() && !lane.tasks.is_empty())
            .map(|lane| lane.poll_after)
            .min()
    }

    /// Pops one runnable invocation while respecting lane quotas and fair rotation.
    pub(super) fn pop_ready(&mut self) -> Option<(TaskLane, Option<String>, Vec<Arc<TaskRecord>>)> {
        let lane_count = self.lanes.len();
        let now = tokio::time::Instant::now();
        for offset in 0..self.lanes.len() {
            let index = self.rotated_index(offset)?;
            let queue = self.lanes.get_mut(index)?;
            if queue.running >= queue.conf.concurrency() {
                continue;
            }
            let locked = queue.conf.lane_lock().is_some();
            if locked && queue.claim_limit(1, now) == 0 {
                if let Some(wait) = queue.local_rate_wake(now) {
                    let rate_at = now + wait;
                    queue.poll_after = queue
                        .owner_renew_at
                        .map_or(rate_at, |renew| renew.min(rate_at));
                }
                continue;
            }
            let Some(record) = queue.tasks.pop_front() else {
                continue;
            };
            let limit = if locked {
                queue.claim_limit(queue.tasks.len().saturating_add(1), now)
            } else {
                queue.tasks.len().saturating_add(1)
            };
            let records = collect_invocation(queue, record, limit, &self.registry);
            if locked {
                queue.consume_local_rate(records.len(), now);
            }
            queue.running += 1;
            self.cursor = (index + 1) % lane_count;
            return Some((queue.conf.lane(), queue.owner_token.clone(), records));
        }
        None
    }

    /// Executes one claimed row and returns its lifecycle outcome.
    pub(super) fn spawn_task(
        &mut self,
        site: Site,
        sender: mpsc::Sender<Completion>,
        lane: TaskLane,
        owner_token: Option<String>,
        records: Vec<Arc<TaskRecord>>,
    ) {
        let engine = self.registry.clone();
        let payload_limit = self.registry.config.payload_limit();
        let error_limit = self.registry.config.error_limit();
        let invocation_id = uuid::Uuid::now_v7();
        self.record_starts(&records);
        let future = execute_task(TaskExecution {
            invocation_id,
            engine,
            site,
            records: records.clone(),
            sender,
            lane,
            metrics: self.metrics.clone(),
            payload_limit,
            error_limit,
            owner_token: owner_token.clone(),
        });
        let handle = tokio::spawn(future);
        let task_ids = records.iter().map(|record| record.id()).collect::<Vec<_>>();
        self.replace_duplicates(&task_ids);
        for task_id in &task_ids {
            self.running_tasks.insert(*task_id, invocation_id);
        }
        let running = RunningInvocation {
            lane,
            owner_token,
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
}
