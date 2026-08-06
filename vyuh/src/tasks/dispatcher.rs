//! Typed task submission facade and dispatcher-owned runtime notifications.

use std::{any::TypeId, sync::Arc, time::Duration};

use serde::Serialize;

use super::{
    AbstractTaskStore, RegisteredTask, TaskError, TaskFilter, TaskId, TaskInfo, TaskLane,
    TaskOptions, TaskReceipt, TaskRecord, TaskRegistry, TaskStatus, TaskWrite,
    submission::canonical_json,
};

/// Shared task registry, store, and local worker wake channel.
#[derive(Clone)]
pub struct TaskDispatcher<S: AbstractTaskStore + Send + Sync + 'static> {
    pub(crate) store: Arc<S>,
    pub(crate) notifier: Arc<tokio::sync::Notify>,
    pub(crate) registry: Arc<TaskRegistry>,
    pub(crate) initialized: Arc<tokio::sync::OnceCell<()>>,
    pub(crate) metrics: Arc<super::TaskMetrics>,
    pub(crate) health: super::TaskHealth,
}

/// Site facade for durable task submission and inspection.
#[derive(Clone)]
pub struct Tasks {
    dispatcher: TaskDispatcher<super::TaskStore>,
}

impl Tasks {
    pub(crate) fn new(dispatcher: TaskDispatcher<super::TaskStore>) -> Self {
        Self { dispatcher }
    }

    /// Submits one typed task with default options.
    pub async fn submit<T: Serialize + 'static>(&self, input: T) -> Result<TaskReceipt, TaskError> {
        self.dispatcher.submit(input).await
    }

    /// Submits one typed task with explicit execution options.
    pub async fn submit_with<T: Serialize + 'static>(
        &self,
        input: T,
        options: TaskOptions,
    ) -> Result<TaskReceipt, TaskError> {
        self.dispatcher.submit_with(input, options).await
    }

    /// Submits a typed batch with default options and ordered receipts.
    pub async fn submit_many<T: Serialize + 'static>(
        &self,
        inputs: impl IntoIterator<Item = T>,
    ) -> Result<Vec<TaskReceipt>, TaskError> {
        self.dispatcher.submit_many(inputs).await
    }

    /// Submits a typed batch with one shared execution policy.
    pub async fn submit_many_with<T: Serialize + 'static>(
        &self,
        inputs: impl IntoIterator<Item = T>,
        options: TaskOptions,
    ) -> Result<Vec<TaskReceipt>, TaskError> {
        self.dispatcher.submit_many_with(inputs, options).await
    }

    /// Resumes one suspended task and wakes the local runner.
    pub async fn resume<T: Serialize>(&self, id: TaskId, input: T) -> Result<bool, TaskError> {
        self.dispatcher.resume(id, input).await
    }

    /// Explicitly moves non-running work between configured lanes.
    pub async fn reassign_lane(&self, from: TaskLane, to: TaskLane) -> Result<u64, TaskError> {
        self.dispatcher.reassign_lane(from, to).await
    }

    /// Lists persisted tasks through bounded filters.
    pub async fn list(
        &self,
        filter: TaskFilter,
    ) -> Result<crate::routes::Page<TaskInfo>, TaskError> {
        self.dispatcher
            .list(filter)
            .await
            .map(|page| page.map(TaskInfo::from))
    }

    /// Returns one persisted task when it exists.
    pub async fn get(&self, id: TaskId) -> Result<Option<TaskInfo>, TaskError> {
        self.dispatcher
            .get(id)
            .await
            .map(|record| record.map(TaskInfo::from))
    }

    /// Returns the immutable effective lane configuration selected during site construction.
    pub(crate) fn lane_configs(&self) -> &[super::TaskLaneConf] {
        self.dispatcher.registry.lanes()
    }
}

impl<S: AbstractTaskStore + Send + Sync + 'static> TaskDispatcher<S> {
    /// Reports whether the site registered any task handlers.
    pub fn has_tasks(&self) -> bool {
        !self.registry.is_empty()
    }

    /// Submits one typed task with default options.
    pub async fn submit<T: Serialize + 'static>(&self, input: T) -> Result<TaskReceipt, TaskError> {
        self.submit_with(input, TaskOptions::new()).await
    }

    /// Submits one typed task with explicit options.
    pub async fn submit_with<T: Serialize + 'static>(
        &self,
        input: T,
        options: TaskOptions,
    ) -> Result<TaskReceipt, TaskError> {
        let mut receipts = self.submit_many_with([input], options).await?;
        receipts.pop().ok_or_else(|| {
            TaskError::TaskExecutionError("task store omitted a submission receipt".into())
        })
    }

    /// Submits a typed batch with default options.
    pub async fn submit_many<T: Serialize + 'static>(
        &self,
        inputs: impl IntoIterator<Item = T>,
    ) -> Result<Vec<TaskReceipt>, TaskError> {
        self.submit_many_with(inputs, TaskOptions::new()).await
    }

    /// Submits a typed batch and preserves input order in its receipts.
    pub async fn submit_many_with<T: Serialize + 'static>(
        &self,
        inputs: impl IntoIterator<Item = T>,
        options: TaskOptions,
    ) -> Result<Vec<TaskReceipt>, TaskError> {
        let mut inputs = inputs.into_iter().peekable();
        if inputs.peek().is_none() {
            return Ok(Vec::new());
        }
        let name = self.task_name::<T>()?;
        let service = self
            .registry
            .tasks
            .get(name)
            .ok_or_else(|| TaskError::TaskNotFound(name.to_string()))?;
        let writes = build_writes(service, name, inputs, &options, &self.registry.config)?;
        self.ensure_initialized().await?;
        let result = self.store.store_tasks(writes).await;
        self.metrics.submission(name, &result);
        let receipts = result?;
        if receipts
            .iter()
            .any(|receipt| matches!(receipt, TaskReceipt::Queued(_)))
        {
            self.notifier.notify_one();
        }
        Ok(receipts)
    }

    /// Resumes one suspended task and wakes local workers when it changed.
    pub async fn resume<T: Serialize>(&self, id: TaskId, input: T) -> Result<bool, TaskError> {
        self.ensure_initialized().await?;
        let serialized = serde_json::to_string(&input)?;
        validate_payload(&serialized, self.registry.config.payload_limit())?;
        let changed = self.store.resume(id, serialized).await?;
        if changed {
            self.notifier.notify_waiters();
        }
        Ok(changed)
    }

    /// Moves pending, sleeping, and suspended work to another configured lane.
    pub async fn reassign_lane(&self, from: TaskLane, to: TaskLane) -> Result<u64, TaskError> {
        self.require_lane(to)?;
        self.ensure_initialized().await?;
        let count = self.store.reassign_lane(from.as_str(), to.as_str()).await?;
        if count > 0 {
            self.notifier.notify_waiters();
        }
        Ok(count)
    }

    /// Lists persisted task records.
    pub async fn list(
        &self,
        filter: TaskFilter,
    ) -> Result<crate::routes::Page<TaskRecord>, TaskError> {
        validate_filter(&filter)?;
        self.store.list_tasks(filter).await
    }

    /// Reads one persisted task record.
    pub async fn get(&self, id: TaskId) -> Result<Option<TaskRecord>, TaskError> {
        self.store.get_task(id).await
    }

    fn task_name<T: 'static>(&self) -> Result<&str, TaskError> {
        self.registry
            .typed_map
            .get(&TypeId::of::<T>())
            .map(String::as_str)
            .ok_or_else(|| TaskError::TaskNotFound("Unknown task type".into()))
    }

    fn require_lane(&self, lane: TaskLane) -> Result<(), TaskError> {
        self.registry
            .lanes()
            .iter()
            .any(|entry| entry.lane() == lane)
            .then_some(())
            .ok_or_else(|| TaskError::UnknownLane(lane.to_string()))
    }

    pub(crate) async fn ensure_initialized(&self) -> Result<(), TaskError> {
        let conf = self.store_conf()?;
        let result = self
            .initialized
            .get_or_try_init(|| self.store.initialize(conf))
            .await
            .map(|_| ());
        self.record_initialization(&result);
        result
    }

    pub(crate) fn store_conf(&self) -> Result<super::TaskStoreConf, TaskError> {
        Ok(super::TaskStoreConf {
            handlers: self.registry.tasks.keys().cloned().collect(),
            lanes: self.registry.lanes().to_vec(),
            idempotency: self.registry.idempotency_conf()?,
        })
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

    fn record_initialization(&self, result: &Result<(), TaskError>) {
        match result {
            Ok(()) => self.health.initialized(),
            Err(error) => {
                self.health.initialization_failed();
                super::diagnostics::log_runtime_error(
                    error,
                    "durable task runtime initialization failed",
                );
            }
        }
    }
}

/// Normalizes a typed submission batch before any store mutation occurs.
fn build_writes<T: Serialize + 'static>(
    service: &RegisteredTask,
    name: &str,
    inputs: impl IntoIterator<Item = T>,
    options: &TaskOptions,
    config: &super::TaskConf,
) -> Result<Vec<TaskWrite>, TaskError> {
    validate_options(options)?;
    let mut writes = Vec::new();
    for input in inputs {
        if writes.len() >= config.batch_size_value() {
            return Err(TaskError::InvalidOptions(
                "task submission exceeds the configured batch size".into(),
            ));
        }
        service.validate_object(&input)?;
        let key = service.idempotency_key(&input)?;
        if key.is_none() && options.ignore_conflicts {
            return Err(TaskError::InvalidOptions(
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
            record: build_record(name, serialized, key, fingerprint, service.effective_lane())?,
            ignore_conflicts: options.ignore_conflicts,
            initial_delay: options.initial_delay,
        });
    }
    Ok(writes)
}

/// Surfaces all accumulated builder failures at the submission terminal.
fn validate_options(options: &TaskOptions) -> Result<(), TaskError> {
    if options
        .initial_delay
        .is_some_and(|value| value > super::config::MAX_TASK_DELAY)
    {
        return Err(TaskError::InvalidOptions(
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
) -> Result<TaskRecord, TaskError> {
    let now = chrono::Utc::now();
    Ok(TaskRecord {
        id: TaskId::new(uuid::Uuid::now_v7()),
        name: name.into(),
        input,
        state: None,
        resume_input: None,
        status: TaskStatus::Pending,
        attempts: 0,
        lane: lane.to_string(),
        lease_duration_ms: None,
        last_error: None,
        idempotency_key: key,
        idempotency_fingerprint: fingerprint,
        idempotency_expires_at: None,
        locked_by: None,
        leased_until: None,
        ready_at: Some(now),
        created_at: now,
        updated_at: now,
        completed_at: None,
    })
}

/// Fingerprints canonical input and immutable idempotency-key semantics.
fn fingerprint(name: &str, input: &str, service: &RegisteredTask) -> Result<String, TaskError> {
    let revision = service
        .idempotency_policy()
        .ok_or_else(|| TaskError::TaskExecutionError("idempotent task is missing policy".into()))?
        .revision;
    let mut hasher = blake3::Hasher::new();
    for part in [name, revision, input] {
        hasher.update(part.as_bytes());
        hasher.update(&[0]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn validate_payload(payload: &str, limit: usize) -> Result<(), TaskError> {
    if payload.len() > limit {
        Err(TaskError::InvalidOptions(format!(
            "task payload exceeds the configured {limit}-byte limit"
        )))
    } else {
        Ok(())
    }
}

fn validate_filter(filter: &TaskFilter) -> Result<(), TaskError> {
    if filter.page == 0 || filter.per_page == 0 || filter.per_page > 200 {
        return Err(TaskError::InvalidOptions(
            "task pages are one-indexed and contain at most 200 records".into(),
        ));
    }
    if filter.query.as_ref().is_some_and(|query| query.len() > 256) {
        return Err(TaskError::InvalidOptions(
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
            return Err(TaskError::InvalidOptions(format!(
                "{label} filters cannot exceed {limit} bytes"
            )));
        }
    }
    if filter
        .created_from
        .zip(filter.created_to)
        .is_some_and(|(from, to)| from > to)
    {
        return Err(TaskError::InvalidOptions(
            "task creation range starts after it ends".into(),
        ));
    }
    Ok(())
}

fn validate_key(key: Option<&str>) -> Result<(), TaskError> {
    if key.is_some_and(|value| value.is_empty() || value.len() > 512) {
        Err(TaskError::InvalidOptions(
            "task idempotency keys must contain between 1 and 512 bytes".into(),
        ))
    } else {
        Ok(())
    }
}

fn duration_ms(duration: Option<Duration>) -> Result<Option<i64>, TaskError> {
    duration
        .map(|value| {
            i64::try_from(value.as_millis()).map_err(|_| {
                TaskError::InvalidOptions("task duration exceeds the supported range".into())
            })
        })
        .transpose()
}
