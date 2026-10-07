//! Typed task submission facade and dispatcher-owned runtime notifications.

use std::{any::TypeId, sync::Arc, time::Duration};

use serde::Serialize;

use crate::callables::DataBox;

use super::{
    AbstractTaskStore, RegisteredTask, ScheduledTaskWrite, TaskFilter, TaskId, TaskInfo, TaskLane,
    TaskOptions, TaskReceipt, TaskRecord, TaskRegistry, TaskRuntimeError, TaskScheduleConf,
    TaskStatus, TaskWrite,
    submission::{canonical_json, canonical_json_value},
};

/// Best-effort local control messages; durable task facts remain exclusively in the store.
#[derive(Clone, Copy, Debug)]
pub(crate) enum RunnerEvent {
    Wake,
    Cancel(TaskId),
}

/// Shared task registry, store, and bounded local worker control channel.
#[derive(Clone)]
pub struct TaskDispatcher<S: AbstractTaskStore + Send + Sync + 'static> {
    pub(crate) store: Arc<S>,
    pub(crate) events: tokio::sync::broadcast::Sender<RunnerEvent>,
    pub(crate) registry: Arc<TaskRegistry>,
    pub(crate) metrics: Arc<super::TaskMetrics>,
    pub(crate) health: super::TaskHealth,
    pub(crate) schedules: Arc<[TaskScheduleConf]>,
}

/// Site facade for durable task submission and inspection.
#[derive(Clone)]
pub struct Tasks {
    dispatcher: TaskDispatcher<super::TaskStore>,
}

impl Tasks {
    /// Prepares an ordered child group without writing to the task store.
    pub(super) fn prepare_all(
        &self,
        inputs: &[DataBox],
        state: &str,
    ) -> Result<super::TaskOutcome, TaskRuntimeError> {
        let dispatcher = &self.dispatcher;
        if inputs.len() > dispatcher.registry.config.all_limit() {
            return Err(TaskRuntimeError::InvalidOptions(
                "Too many all children".into(),
            ));
        }
        validate_payload(state, dispatcher.registry.config.payload_limit())?;
        let children = inputs
            .iter()
            .map(|input| {
                let service = dispatcher.task_for_payload(input)?;
                build_box_write(service, input, &dispatcher.registry.config)
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
        self.dispatcher.cancel(id).await
    }

    /// Resolves a returned child request without submitting or waking any work.
    pub(super) fn prepare_child(
        &self,
        input: &DataBox,
        state: &str,
        options: &TaskOptions,
    ) -> Result<super::TaskOutcome, TaskRuntimeError> {
        self.dispatcher.prepare_child(input, state, options)
    }

    /// Resumes an ordinary suspension with a failure. Do not externally resume child waits.
    pub async fn resume_failed(
        &self,
        id: TaskId,
        failure: super::TaskFailure,
    ) -> Result<bool, TaskRuntimeError> {
        self.dispatcher.resume_failed(id, failure).await
    }

    pub(crate) fn new(dispatcher: TaskDispatcher<super::TaskStore>) -> Self {
        Self { dispatcher }
    }

    /// Submits one typed task with default options.
    pub async fn submit<T: Serialize + 'static>(
        &self,
        input: T,
    ) -> Result<TaskReceipt, TaskRuntimeError> {
        self.dispatcher.submit(input).await
    }

    /// Submits one typed task with explicit execution options.
    pub async fn submit_with<T: Serialize + 'static>(
        &self,
        input: T,
        options: TaskOptions,
    ) -> Result<TaskReceipt, TaskRuntimeError> {
        self.dispatcher.submit_with(input, options).await
    }

    /// Submits a typed batch with default options and ordered receipts.
    pub async fn submit_many<T: Serialize + 'static>(
        &self,
        inputs: impl IntoIterator<Item = T>,
    ) -> Result<Vec<TaskReceipt>, TaskRuntimeError> {
        self.dispatcher.submit_many(inputs).await
    }

    /// Submits a typed batch with one shared execution policy.
    pub async fn submit_many_with<T: Serialize + 'static>(
        &self,
        inputs: impl IntoIterator<Item = T>,
        options: TaskOptions,
    ) -> Result<Vec<TaskReceipt>, TaskRuntimeError> {
        self.dispatcher.submit_many_with(inputs, options).await
    }

    /// Resumes an ordinary suspension with success and wakes the local runner.
    /// Do not externally resume a parent waiting for a child. Returns false if not suspended.
    pub async fn resume<T: Serialize>(
        &self,
        id: TaskId,
        input: T,
    ) -> Result<bool, TaskRuntimeError> {
        self.dispatcher.resume(id, input).await
    }

    /// Explicitly moves non-running work between configured lanes.
    pub async fn reassign_lane(
        &self,
        from: TaskLane,
        to: TaskLane,
    ) -> Result<u64, TaskRuntimeError> {
        self.dispatcher.reassign_lane(from, to).await
    }

    /// Lists persisted tasks through bounded filters.
    pub async fn list(
        &self,
        filter: TaskFilter,
    ) -> Result<crate::routes::Page<TaskInfo>, TaskRuntimeError> {
        self.dispatcher
            .list(filter)
            .await
            .map(|page| page.map(TaskInfo::from))
    }

    /// Returns one persisted task when it exists.
    pub async fn get(&self, id: TaskId) -> Result<Option<TaskInfo>, TaskRuntimeError> {
        self.dispatcher
            .get(id)
            .await
            .map(|record| record.map(TaskInfo::from))
    }

    /// Returns the immutable effective lane configuration selected during site construction.
    pub(crate) fn lane_configs(&self) -> &[super::TaskLaneConf] {
        self.dispatcher.registry.lanes()
    }

    /// Returns immutable task-targeted emitter schedule metadata for diagnostics.
    pub(crate) fn schedule_configs(&self) -> &[TaskScheduleConf] {
        self.dispatcher.schedules.as_ref()
    }

    /// Reads the durable submission cursors for configured task schedules.
    pub(crate) async fn schedule_snapshot(
        &self,
        names: &[String],
    ) -> Result<super::TaskScheduleSnapshot, TaskRuntimeError> {
        self.dispatcher.schedule_snapshot(names).await
    }

    /// Returns the finalized lane selected for one registered task handler.
    pub(crate) fn task_lane(&self, task: &str) -> Option<&str> {
        self.dispatcher
            .registry
            .tasks
            .get(task)
            .map(|registered| registered.effective_lane().as_str())
    }
}

impl<S: AbstractTaskStore + Send + Sync + 'static> TaskDispatcher<S> {
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
    pub async fn submit_many<T: Serialize + 'static>(
        &self,
        inputs: impl IntoIterator<Item = T>,
    ) -> Result<Vec<TaskReceipt>, TaskRuntimeError> {
        self.submit_many_with(inputs, TaskOptions::new()).await
    }

    /// Submits a typed batch and preserves input order in its receipts.
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
        let result = self.store.store_tasks(writes).await;
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

/// Normalizes a typed submission batch before any store mutation occurs.
fn build_writes<T: Serialize + 'static>(
    service: &RegisteredTask,
    name: &str,
    inputs: impl IntoIterator<Item = T>,
    options: &TaskOptions,
    config: &super::TaskConf,
) -> Result<Vec<TaskWrite>, TaskRuntimeError> {
    validate_options(options)?;
    let mut writes = Vec::new();
    for input in inputs {
        if writes.len() >= config.batch_size_value() {
            return Err(TaskRuntimeError::InvalidOptions(
                "task submission exceeds the configured batch size".into(),
            ));
        }
        service.validate_object(&input)?;
        let key = service.idempotency_key(&input)?;
        if key.is_none() && options.ignore_conflicts {
            return Err(TaskRuntimeError::InvalidOptions(
                "ignore_conflicts requires an idempotent task definition".into(),
            ));
        }
        let serialized = if key.is_some() {
            canonical_json(&input)?
        } else {
            serde_json::to_string(&input)?
        };
        validate_payload(&serialized, config.payload_limit())?;
        validate_key(key.as_deref())?;
        let fingerprint = key
            .as_ref()
            .map(|_| fingerprint(name, &serialized, service))
            .transpose()?;
        writes.push(TaskWrite {
            record: build_record(
                name,
                serialized,
                key,
                fingerprint,
                service.effective_lane(),
                service.kind(),
            )?,
            ignore_conflicts: options.ignore_conflicts,
            initial_delay: options.initial_delay,
        });
    }
    Ok(writes)
}

/// Converts one verified type-erased payload into a normal task write.
fn build_box_write(
    service: &RegisteredTask,
    input: &DataBox,
    config: &super::TaskConf,
) -> Result<TaskWrite, TaskRuntimeError> {
    service.validate_box(input)?;
    let value = input
        .to_json()
        .ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("task input cannot be serialized".into())
        })?
        .map_err(TaskRuntimeError::TaskExecutionError)?;
    let key = service.idempotency_key_box(input.as_any())?;
    let serialized = if key.is_some() {
        canonical_json_value(value)?
    } else {
        serde_json::to_string(&value)?
    };
    validate_payload(&serialized, config.payload_limit())?;
    validate_key(key.as_deref())?;
    let fingerprint = key
        .as_ref()
        .map(|_| fingerprint(service.name(), &serialized, service))
        .transpose()?;
    Ok(TaskWrite {
        record: build_record(
            service.name(),
            serialized,
            key,
            fingerprint,
            service.effective_lane(),
            service.kind(),
        )?,
        ignore_conflicts: false,
        initial_delay: None,
    })
}

/// Rejects incompatible conflict behavior before a child request can be returned.
pub(super) fn validate_spawn_options(options: &TaskOptions) -> Result<(), TaskRuntimeError> {
    if options.ignore_conflicts {
        return Err(TaskRuntimeError::InvalidOptions(
            "spawn cannot ignore idempotency conflicts".into(),
        ));
    }
    validate_options(options)
}

/// Surfaces all accumulated builder failures at the submission terminal.
fn validate_options(options: &TaskOptions) -> Result<(), TaskRuntimeError> {
    if options
        .initial_delay
        .is_some_and(|value| value > super::config::MAX_TASK_DELAY)
    {
        return Err(TaskRuntimeError::InvalidOptions(
            "task delays cannot exceed ten years".into(),
        ));
    }
    duration_ms(options.initial_delay)?;
    Ok(())
}

/// Creates one pending record without assigning store-relative readiness time.
fn build_record(
    name: &str,
    input: String,
    key: Option<String>,
    fingerprint: Option<String>,
    lane: TaskLane,
    kind: super::TaskKind,
) -> Result<TaskRecord, TaskRuntimeError> {
    let now = chrono::Utc::now();
    Ok(TaskRecord {
        id: TaskId::new(uuid::Uuid::now_v7()),
        parent_id: None,
        root_id: None,
        kind,
        name: name.into(),
        input,
        state: None,
        resume_input: None,
        status: TaskStatus::Pending,
        cancelled: false,
        attempts: 0,
        step_attempts: 0,
        lane: lane.to_string(),
        last_result: None,
        idempotency_key: key,
        idempotency_fingerprint: fingerprint,
        idempotency_expires_at: None,
        node_id: None,
        ready_at: Some(now),
        created_at: now,
        updated_at: now,
        completed_at: None,
    })
}

/// Fingerprints canonical input and immutable idempotency-key semantics.
fn fingerprint(
    name: &str,
    input: &str,
    service: &RegisteredTask,
) -> Result<String, TaskRuntimeError> {
    let revision = service
        .idempotency_policy()
        .ok_or_else(|| {
            TaskRuntimeError::TaskExecutionError("idempotent task is missing policy".into())
        })?
        .revision;
    let mut hasher = blake3::Hasher::new();
    for part in [name, revision, input] {
        hasher.update(part.as_bytes());
        hasher.update(&[0]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn validate_payload(payload: &str, limit: usize) -> Result<(), TaskRuntimeError> {
    if payload.len() > limit {
        Err(TaskRuntimeError::InvalidOptions(format!(
            "task payload exceeds the configured {limit}-byte limit"
        )))
    } else {
        Ok(())
    }
}

fn validate_filter(filter: &TaskFilter) -> Result<(), TaskRuntimeError> {
    if filter.page == 0 || filter.per_page == 0 || filter.per_page > 200 {
        return Err(TaskRuntimeError::InvalidOptions(
            "task pages are one-indexed and contain at most 200 records".into(),
        ));
    }
    if filter.query.as_ref().is_some_and(|query| query.len() > 256) {
        return Err(TaskRuntimeError::InvalidOptions(
            "task search text cannot exceed 256 bytes".into(),
        ));
    }
    for (value, limit, label) in [
        (filter.name.as_deref(), 191, "task name"),
        (filter.lane.as_deref(), 64, "task lane"),
        (
            filter.idempotency_key.as_deref(),
            512,
            "task idempotency key",
        ),
    ] {
        if value.is_some_and(|value| value.len() > limit) {
            return Err(TaskRuntimeError::InvalidOptions(format!(
                "{label} filters cannot exceed {limit} bytes"
            )));
        }
    }
    if filter
        .created_from
        .zip(filter.created_to)
        .is_some_and(|(from, to)| from > to)
    {
        return Err(TaskRuntimeError::InvalidOptions(
            "task creation range starts after it ends".into(),
        ));
    }
    Ok(())
}

fn validate_key(key: Option<&str>) -> Result<(), TaskRuntimeError> {
    if key.is_some_and(|value| value.is_empty() || value.len() > 512) {
        Err(TaskRuntimeError::InvalidOptions(
            "task idempotency keys must contain between 1 and 512 bytes".into(),
        ))
    } else {
        Ok(())
    }
}

fn duration_ms(duration: Option<Duration>) -> Result<Option<i64>, TaskRuntimeError> {
    duration
        .map(|value| {
            i64::try_from(value.as_millis()).map_err(|_| {
                TaskRuntimeError::InvalidOptions("task duration exceeds the supported range".into())
            })
        })
        .transpose()
}
