//! Typed task submission facade and client-owned runtime notifications.

use std::{any::TypeId, sync::Arc};

use serde::Serialize;

use crate::callables::DataBox;

use super::{
    RegisteredTask, ScheduledTaskWrite, TaskFilter, TaskId, TaskInfo, TaskLane, TaskOptions,
    TaskReceipt, TaskRecord, TaskRegistry, TaskRuntimeError, TaskScheduleConf, TaskStore,
    submission::{
        build_box_write, build_writes, validate_filter, validate_payload, validate_spawn_options,
    },
};

/// Best-effort local control messages; durable task facts remain exclusively in the store.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RunnerEvent {
    Wake,
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    WakeLane(TaskLane),
    Cancel(TaskId),
}

/// Shared task registry, store, and bounded local worker control channel.
#[derive(Clone)]
pub struct TaskClient<S: TaskStore + Send + Sync + 'static> {
    pub(crate) store: Arc<S>,
    pub(crate) events: tokio::sync::broadcast::Sender<RunnerEvent>,
    pub(crate) registry: Arc<TaskRegistry>,
    pub(crate) metrics: Arc<super::TaskMetrics>,
    pub(crate) health: super::TaskHealth,
    pub(crate) schedules: Arc<[TaskScheduleConf]>,
    pub(crate) ingress: Option<(uuid::Uuid, tokio::sync::mpsc::Sender<Arc<TaskRecord>>)>,
}

/// Site facade for durable task submission and inspection.
#[derive(Clone)]
pub struct Tasks {
    client: TaskClient<super::ConfiguredTaskStore>,
}

impl Tasks {
    /// Prepares an ordered child group without writing to the task store.
    pub(super) fn prepare_all(
        &self,
        inputs: &[DataBox],
        state: &str,
    ) -> Result<super::TaskOutcome, TaskRuntimeError> {
        let client = &self.client;
        if inputs.len() > client.registry.config.all_limit() {
            return Err(TaskRuntimeError::InvalidOptions(
                "Too many all children".into(),
            ));
        }
        validate_payload(state, client.registry.config.payload_limit())?;
        let children = inputs
            .iter()
            .map(|input| {
                let service = client.task_for_payload(input)?;
                build_box_write(service, input, &client.registry.config)
            })
            .collect::<Result<Vec<_>, TaskRuntimeError>>()?;
        Ok(super::TaskOutcome::All {
            state: state.to_owned(),
            children,
        })
    }

    /// Commits terminal cancellation or an idempotent no-op, without interrupting execution.
    /// Already-claimed work may still run; store errors propagate.
    /// A commit acknowledgement error may follow persistence; retrying is idempotent.
    pub async fn cancel(&self, id: TaskId) -> Result<(), TaskRuntimeError> {
        self.client.cancel(id).await
    }

    /// Resolves a returned child request without submitting or waking any work.
    pub(super) fn prepare_child(
        &self,
        input: &DataBox,
        state: &str,
        options: &TaskOptions,
    ) -> Result<super::TaskOutcome, TaskRuntimeError> {
        self.client.prepare_child(input, state, options)
    }

    /// Resumes an ordinary suspension with a failure. Do not externally resume child waits.
    pub async fn resume_failed(
        &self,
        id: TaskId,
        failure: super::TaskFailure,
    ) -> Result<bool, TaskRuntimeError> {
        self.client.resume_failed(id, failure).await
    }

    pub(crate) fn new(client: TaskClient<super::ConfiguredTaskStore>) -> Self {
        Self { client }
    }

    /// Submits one typed task with default options.
    pub async fn submit<T: Serialize + 'static>(
        &self,
        input: T,
    ) -> Result<TaskReceipt, TaskRuntimeError> {
        self.client.submit(input).await
    }

    /// Submits one typed task with explicit execution options.
    pub async fn submit_with<T: Serialize + 'static>(
        &self,
        input: T,
        options: TaskOptions,
    ) -> Result<TaskReceipt, TaskRuntimeError> {
        self.client.submit_with(input, options).await
    }

    /// Submits a typed batch with default options and ordered receipts.
    /// Rejects batches exceeding 10,000 inputs before writing any input.
    pub async fn submit_many<T: Serialize + 'static>(
        &self,
        inputs: impl IntoIterator<Item = T>,
    ) -> Result<Vec<TaskReceipt>, TaskRuntimeError> {
        self.client.submit_many(inputs).await
    }

    /// Submits a typed batch with one shared execution policy.
    /// Rejects batches exceeding 10,000 inputs before writing any input.
    pub async fn submit_many_with<T: Serialize + 'static>(
        &self,
        inputs: impl IntoIterator<Item = T>,
        options: TaskOptions,
    ) -> Result<Vec<TaskReceipt>, TaskRuntimeError> {
        self.client.submit_many_with(inputs, options).await
    }

    /// Resumes an ordinary suspension with success and wakes the local runner.
    /// Do not externally resume a parent waiting for a child. Returns false if not suspended.
    pub async fn resume<T: Serialize>(
        &self,
        id: TaskId,
        input: T,
    ) -> Result<bool, TaskRuntimeError> {
        self.client.resume(id, input).await
    }

    /// Explicitly moves non-running work between configured lanes.
    pub async fn reassign_lane(
        &self,
        from: TaskLane,
        to: TaskLane,
    ) -> Result<u64, TaskRuntimeError> {
        self.client.reassign_lane(from, to).await
    }

    /// Lists persisted tasks through bounded filters.
    pub async fn list(
        &self,
        filter: TaskFilter,
    ) -> Result<crate::routes::Page<TaskInfo>, TaskRuntimeError> {
        self.client
            .list(filter)
            .await
            .map(|page| page.map(TaskInfo::from))
    }

    /// Returns one persisted task when it exists.
    pub async fn get(&self, id: TaskId) -> Result<Option<TaskInfo>, TaskRuntimeError> {
        self.client
            .get(id)
            .await
            .map(|record| record.map(TaskInfo::from))
    }

    /// Returns the immutable effective lane configuration selected during site construction.
    pub(crate) fn lane_configs(&self) -> &[super::TaskLaneConf] {
        self.client.registry.lanes()
    }

    /// Returns immutable task-targeted emitter schedule metadata for diagnostics.
    pub(crate) fn schedule_configs(&self) -> &[TaskScheduleConf] {
        self.client.schedules.as_ref()
    }

    /// Reads the durable submission cursors for configured task schedules.
    pub(crate) async fn schedule_snapshot(
        &self,
        names: &[String],
    ) -> Result<super::TaskScheduleSnapshot, TaskRuntimeError> {
        self.client.schedule_snapshot(names).await
    }

    /// Returns the finalized lane selected for one registered task handler.
    pub(crate) fn task_lane(&self, task: &str) -> Option<&str> {
        self.client
            .registry
            .tasks
            .get(task)
            .map(|registered| registered.effective_lane().as_str())
    }
}

impl<S: TaskStore + Send + Sync + 'static> TaskClient<S> {
    /// Commits terminal cancellation before notifying local queues; never interrupts a handler.
    /// Missing, terminal and repeated requests succeed as no-ops. Store errors propagate.
    /// A commit acknowledgement error may follow persistence; retrying is idempotent.
    pub async fn cancel(&self, id: TaskId) -> Result<(), TaskRuntimeError> {
        let commits = [super::TaskCommit::Cancel { task_id: id }];
        self.store
            .commit_tasks(&commits)
            .await
            .map_err(|failure| match failure {
                super::store::StoreFailure::BeforeCommit(error)
                | super::store::StoreFailure::CommitUnknown { error, .. } => error,
            })?;
        self.notify(RunnerEvent::Cancel(id));
        Ok(())
    }

    /// No subscriber is normal for producer-only sites; the committed store write is authoritative.
    fn notify(&self, event: RunnerEvent) {
        let _ = self.events.send(event);
    }

    /// Validates a child using ordinary submission preparation without touching storage.
    pub(super) fn prepare_child(
        &self,
        input: &DataBox,
        state: &str,
        options: &TaskOptions,
    ) -> Result<super::TaskOutcome, TaskRuntimeError> {
        validate_spawn_options(options)?;
        validate_payload(state, self.registry.config.payload_limit())?;
        let service = self.task_for_payload(input)?;
        let mut child = build_box_write(service, input, &self.registry.config)?;
        child.initial_delay = options.initial_delay;
        Ok(super::TaskOutcome::Spawn {
            state: state.to_owned(),
            child,
        })
    }

    /// Delivers a bounded failure through the independent external resume operation.
    pub async fn resume_failed(
        &self,
        id: TaskId,
        failure: super::TaskFailure,
    ) -> Result<bool, TaskRuntimeError> {
        let result: Result<(), _> = Err(failure.bounded(self.registry.config.error_limit()));
        self.resume_result(id, result).await
    }

    /// Reports whether the site registered any task handlers.
    pub fn has_tasks(&self) -> bool {
        !self.registry.is_empty()
    }

    /// Submits one typed task with default options.
    pub async fn submit<T: Serialize + 'static>(
        &self,
        input: T,
    ) -> Result<TaskReceipt, TaskRuntimeError> {
        self.submit_with(input, TaskOptions::new()).await
    }

    /// Submits one typed task with explicit options.
    pub async fn submit_with<T: Serialize + 'static>(
        &self,
        input: T,
        options: TaskOptions,
    ) -> Result<TaskReceipt, TaskRuntimeError> {
        let mut receipts = self.submit_many_with([input], options).await?;
        receipts.pop().ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("task store omitted a submission receipt".into())
        })
    }

    /// Submits a typed batch with default options.
    /// Rejects batches exceeding 10,000 inputs before writing any input.
    pub async fn submit_many<T: Serialize + 'static>(
        &self,
        inputs: impl IntoIterator<Item = T>,
    ) -> Result<Vec<TaskReceipt>, TaskRuntimeError> {
        self.submit_many_with(inputs, TaskOptions::new()).await
    }

    /// Submits a typed batch and preserves input order in its receipts.
    /// Rejects batches exceeding 10,000 inputs before writing any input.
    pub async fn submit_many_with<T: Serialize + 'static>(
        &self,
        inputs: impl IntoIterator<Item = T>,
        options: TaskOptions,
    ) -> Result<Vec<TaskReceipt>, TaskRuntimeError> {
        let mut inputs = inputs.into_iter().peekable();
        if inputs.peek().is_none() {
            return Ok(Vec::new());
        }
        let name = self.task_name::<T>()?;
        let service = self
            .registry
            .tasks
            .get(name)
            .ok_or_else(|| TaskRuntimeError::TaskNotFound(name.to_string()))?;
        let writes = build_writes(service, name, inputs, &options, &self.registry.config)?;
        let result = self.store_inputs(writes).await;
        self.metrics.submission(name, &result);
        let receipts = result?;
        if receipts
            .iter()
            .any(|receipt| matches!(receipt, TaskReceipt::Queued(_)))
        {
            self.notify(RunnerEvent::Wake);
        }
        Ok(receipts)
    }

    /// Reserves delivery before insertion; unavailable slots retain ordinary pending writes.
    async fn store_inputs(
        &self,
        writes: Vec<super::TaskWrite>,
    ) -> Result<Vec<TaskReceipt>, TaskRuntimeError> {
        let Some((node, sender)) = &self.ingress else {
            return self.store.store_tasks(writes).await;
        };
        let mut permits = Vec::with_capacity(writes.len().min(sender.capacity()));
        for _ in 0..writes.len() {
            let Ok(permit) = sender.clone().try_reserve_owned() else {
                break;
            };
            permits.push(permit);
        }
        if permits.is_empty() {
            return self.store.store_tasks(writes).await;
        }
        let (receipts, records) = self
            .store
            .store_admitted(writes, node, permits.len())
            .await?;
        if records.len() > permits.len() {
            return Err(TaskRuntimeError::TaskExecutionError(
                "task store exceeded reserved delivery capacity".into(),
            ));
        }
        for (record, permit) in records.into_iter().zip(permits) {
            permit.send(Arc::new(record));
        }
        Ok(receipts)
    }

    /// Resumes an ordinary suspension with success and wakes local workers when it changed.
    /// Do not externally resume a parent waiting for its child.
    pub async fn resume<T: Serialize>(
        &self,
        id: TaskId,
        input: T,
    ) -> Result<bool, TaskRuntimeError> {
        self.resume_result(id, Ok::<_, super::TaskFailure>(input))
            .await
    }

    /// Encodes the shared result envelope while retaining the conditional resume write.
    async fn resume_result<T: Serialize>(
        &self,
        id: TaskId,
        input: Result<T, super::TaskFailure>,
    ) -> Result<bool, TaskRuntimeError> {
        let serialized = serde_json::to_string(&input)?;
        super::result::validate_size(&serialized)?;
        let changed = self.store.resume(id, serialized).await?;
        if changed {
            self.notify(RunnerEvent::Wake);
        }
        Ok(changed)
    }

    /// Moves pending, sleeping, and suspended work to another configured lane.
    pub async fn reassign_lane(
        &self,
        from: TaskLane,
        to: TaskLane,
    ) -> Result<u64, TaskRuntimeError> {
        self.require_lane(to)?;
        let count = self.store.reassign_lane(from.as_str(), to.as_str()).await?;
        if count > 0 {
            self.notify(RunnerEvent::Wake);
        }
        Ok(count)
    }

    /// Lists persisted task records.
    pub async fn list(
        &self,
        filter: TaskFilter,
    ) -> Result<crate::routes::Page<TaskRecord>, TaskRuntimeError> {
        validate_filter(&filter)?;
        self.store.list_tasks(filter).await
    }

    /// Reads one persisted task record.
    pub async fn get(&self, id: TaskId) -> Result<Option<TaskRecord>, TaskRuntimeError> {
        self.store.get_task(id).await
    }

    fn task_name<T: 'static>(&self) -> Result<&str, TaskRuntimeError> {
        self.registry
            .typed_map
            .get(&TypeId::of::<T>())
            .map(String::as_str)
            .ok_or_else(|| TaskRuntimeError::TaskNotFound("Unknown task type".into()))
    }

    fn task_for_payload(&self, payload: &DataBox) -> Result<&RegisteredTask, TaskRuntimeError> {
        let name = self
            .registry
            .typed_map
            .get(&payload.payload_type_id())
            .ok_or_else(|| TaskRuntimeError::TaskNotFound("Unknown task type".into()))?;
        self.registry
            .tasks
            .get(name)
            .ok_or_else(|| TaskRuntimeError::TaskNotFound(name.clone()))
    }

    fn require_lane(&self, lane: TaskLane) -> Result<(), TaskRuntimeError> {
        self.registry
            .lanes()
            .iter()
            .any(|entry| entry.lane() == lane)
            .then_some(())
            .ok_or_else(|| TaskRuntimeError::UnknownLane(lane.to_string()))
    }

    /// Reads durable schedule cursors with the store-relative current time.
    pub(crate) async fn schedule_snapshot(
        &self,
        names: &[String],
    ) -> Result<super::TaskScheduleSnapshot, TaskRuntimeError> {
        self.store.schedule_snapshot(names).await
    }

    /// Stores one emitter payload and advances its cursor in the same transaction.
    pub(crate) async fn submit_scheduled(
        &self,
        schedule: &str,
        occurrence: chrono::DateTime<chrono::Utc>,
        payload: DataBox,
    ) -> Result<Option<TaskReceipt>, TaskRuntimeError> {
        let service = self.task_for_payload(&payload)?;
        self.require_schedule_target(schedule, service.name())?;
        let write = build_box_write(service, &payload, &self.registry.config)?;
        let result = self
            .store
            .store_scheduled(ScheduledTaskWrite {
                name: schedule.into(),
                occurrence,
                write,
            })
            .await;
        if matches!(result, Ok(Some(TaskReceipt::Queued(_)))) {
            self.notify(RunnerEvent::Wake);
        }
        result
    }

    /// Ensures framework-owned schedule metadata still targets this task type.
    fn require_schedule_target(&self, schedule: &str, task: &str) -> Result<(), TaskRuntimeError> {
        match self.schedules.iter().find(|entry| entry.name == schedule) {
            Some(entry) if entry.task == task => Ok(()),
            Some(_) => Err(TaskRuntimeError::TaskExecutionError(
                "task schedule payload type does not match its configured target".into(),
            )),
            None => Err(TaskRuntimeError::TaskExecutionError(
                "task schedule is not registered by this site".into(),
            )),
        }
    }

    pub(crate) fn render_metrics(&self) -> String {
        self.metrics.render(self.health.snapshot())
    }

    pub(crate) fn readiness(&self) -> bool {
        self.health.is_ready()
    }

    pub(crate) fn health_snapshot(&self) -> super::TaskHealthSnapshot {
        self.health.snapshot()
    }
}
