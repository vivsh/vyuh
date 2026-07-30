use serde::{Deserialize, Serialize};
use std::{any::TypeId, borrow::Cow, collections::HashMap, sync::Arc, time::Duration};

use crate::{
    Error, Site,
    callables::{self, Callable},
};

use super::{TaskListFilter, TaskListPage, TaskOptions, TaskRecord};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConf {
    pub poll_interval_ms: u32,
    pub capacity: usize,
    pub concurrency: usize,
    pub batch_size: usize,
    pub lease_duration_ms: u32,
}

impl Default for TaskConf {
    fn default() -> Self {
        Self {
            poll_interval_ms: 15000,
            capacity: 1000,
            concurrency: 10,
            batch_size: 250,
            lease_duration_ms: 300000,
        }
    }
}

#[derive(Clone)]
pub struct TaskContext {
    site: Site,
    payload: callables::DataBox,
    record: Arc<TaskRecord>,
    operation_id: crate::OperationId,
}

impl callables::IntoDataBox for TaskContext {
    fn into_data_box(self) -> callables::DataBox {
        self.payload
    }
}

impl callables::HasSite for TaskContext {
    fn site(&self) -> &Site {
        &self.site
    }
}

impl callables::FromContextParts<TaskContext> for crate::OperationId {
    fn from_context_parts(context: &TaskContext) -> Result<Self, callables::CallError> {
        Ok(context.operation_id)
    }
}

type TaskHandler = Callable<TaskContext, Error>;

#[derive(Debug, thiserror::Error)]
pub enum TaskError {
    #[error("Type mismatch: expected {0}, got {1}")]
    TypeMismatch(String, String),

    #[error("Task '{0}' not found")]
    TaskNotFound(String),

    #[error("Task JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Task execution error: {0}")]
    TaskExecutionError(String),

    #[error("Task already exists: {0}")]
    AlreadyExists(String),

    #[error("Identity already exists")]
    IdentityError,

    #[error(
        "Task migration provisioning is no longer performed at site startup; apply migrations before starting task workers"
    )]
    MigrationRequired,

    #[error(transparent)]
    CallError(#[from] crate::callables::CallError),

    #[error("Database error: {0}")]
    DatabaseError(#[from] crate::db::sqlx::Error),

    #[error(transparent)]
    StoreError(#[from] crate::db::DbError),

    #[error("Unknown task error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[repr(i16)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Pending = 0,
    Running = 1,
    Suspended = 2,
    Succeeded = 3,
    Failed = 4,
}

impl TaskStatus {
    pub const fn as_i16(self) -> i16 {
        self as i16
    }

    pub fn as_str(self) -> &'static str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::Running => "running",
            TaskStatus::Suspended => "suspended",
            TaskStatus::Succeeded => "succeeded",
            TaskStatus::Failed => "failed",
        }
    }

    /// Converts the persisted status representation into a task status.
    ///
    /// Invalid values indicate a corrupted or incompatible task row and are
    /// returned as a structured task error.
    #[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
    pub(crate) fn from_i16(value: i16) -> Result<Self, TaskError> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Running),
            2 => Ok(Self::Suspended),
            3 => Ok(Self::Succeeded),
            4 => Ok(Self::Failed),
            _ => Err(TaskError::TaskExecutionError(format!(
                "invalid task status value {value}"
            ))),
        }
    }
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub enum TaskOutcome {
    Complete {
        result: String,
    },
    Suspend {
        state: String,
        output: Option<String>,
    },
    Sleep {
        state: String,
        delay: Duration,
    },
    Retry {
        delay: Option<Duration>,
        error: String,
    },
    Fail {
        error: String,
    },
}

impl TaskOutcome {
    pub fn complete<T: Serialize>(result: &T) -> Result<Self, TaskError> {
        Ok(Self::Complete {
            result: serde_json::to_string(result)?,
        })
    }

    pub fn suspend<S: Serialize, O: Serialize>(
        state: &S,
        output: Option<&O>,
    ) -> Result<Self, TaskError> {
        Ok(Self::Suspend {
            state: serde_json::to_string(state)?,
            output: output.map(serde_json::to_string).transpose()?,
        })
    }

    pub fn sleep<S: Serialize>(state: &S, delay: Duration) -> Result<Self, TaskError> {
        Ok(Self::Sleep {
            state: serde_json::to_string(state)?,
            delay,
        })
    }

    pub fn retry(delay: Option<Duration>, error: impl Into<String>) -> Self {
        Self::Retry {
            delay,
            error: error.into(),
        }
    }

    pub fn fail(error: impl Into<String>) -> Self {
        Self::Fail {
            error: error.into(),
        }
    }

    pub(crate) fn fail_error(error: &Error) -> Self {
        Self::fail(error.display_compact())
    }
}

impl<E> callables::IntoOutput<E> for TaskOutcome {
    fn into_output(self) -> Result<callables::DataBox, E> {
        Ok(callables::DataBox::new(self))
    }
}

impl callables::IntoReturnPart for TaskOutcome {
    fn into_return_part() -> callables::ReturnPart {
        callables::ReturnPart::Empty
    }
}

#[doc(hidden)]
pub trait IntoTaskOutcomePart {
    fn into_task_outcome(data: callables::DataBox) -> TaskOutcome;
}

impl IntoTaskOutcomePart for () {
    fn into_task_outcome(data: callables::DataBox) -> TaskOutcome {
        if data.downcast_ref::<()>().is_some() {
            TaskOutcome::Complete {
                result: "null".to_string(),
            }
        } else {
            unexpected_output()
        }
    }
}

impl IntoTaskOutcomePart for TaskOutcome {
    fn into_task_outcome(data: callables::DataBox) -> TaskOutcome {
        match data.downcast_ref::<TaskOutcome>() {
            Some(output) => output.clone(),
            None => unexpected_output(),
        }
    }
}

impl<T: callables::DataValue> IntoTaskOutcomePart for crate::Data<T> {
    fn into_task_outcome(data: callables::DataBox) -> TaskOutcome {
        match data.downcast_ref::<T>() {
            Some(value) => match TaskOutcome::complete(value) {
                Ok(outcome) => outcome,
                Err(err) => TaskOutcome::fail(format!("Task output serialization error: {err}")),
            },
            None => unexpected_output(),
        }
    }
}

impl<T, E> IntoTaskOutcomePart for Result<T, E>
where
    T: IntoTaskOutcomePart,
{
    fn into_task_outcome(data: callables::DataBox) -> TaskOutcome {
        T::into_task_outcome(data)
    }
}

fn unexpected_output() -> TaskOutcome {
    TaskOutcome::fail("Task handler returned an unexpected output type")
}

/// Opaque return type for task handlers.
///
/// Create via static constructors: `TaskState::complete`, `TaskState::suspend`,
/// `TaskState::sleep`, `TaskState::retry`, `TaskState::fail`.
/// The type parameter `O` is the output payload type.
pub struct TaskState<O = ()> {
    inner: TaskOutcome,
    _phantom: std::marker::PhantomData<fn() -> O>,
}

impl<O: Serialize> TaskState<O> {
    pub fn complete(output: O) -> Result<Self, TaskError> {
        Ok(Self {
            inner: TaskOutcome::Complete {
                result: serde_json::to_string(&output)?,
            },
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn suspend<S: Serialize>(output: O, state: S) -> Result<Self, TaskError> {
        Ok(Self {
            inner: TaskOutcome::Suspend {
                state: serde_json::to_string(&state)?,
                output: Some(serde_json::to_string(&output)?),
            },
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn sleep<S: Serialize>(state: S, delay: Duration) -> Result<Self, TaskError> {
        Ok(Self {
            inner: TaskOutcome::Sleep {
                state: serde_json::to_string(&state)?,
                delay,
            },
            _phantom: std::marker::PhantomData,
        })
    }

    pub fn retry(delay: Option<Duration>, error: impl Into<String>) -> Self {
        Self {
            inner: TaskOutcome::Retry {
                delay,
                error: error.into(),
            },
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn fail(error: impl Into<String>) -> Self {
        Self {
            inner: TaskOutcome::Fail {
                error: error.into(),
            },
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<O, E: From<TaskError>> callables::IntoOutput<E> for TaskState<O> {
    fn into_output(self) -> Result<callables::DataBox, E> {
        Ok(callables::DataBox::new(self.inner))
    }
}

impl<O> callables::IntoReturnPart for TaskState<O> {
    fn into_return_part() -> callables::ReturnPart {
        callables::ReturnPart::Empty
    }
}

impl<O> IntoTaskOutcomePart for TaskState<O> {
    fn into_task_outcome(data: callables::DataBox) -> TaskOutcome {
        TaskOutcome::into_task_outcome(data)
    }
}

impl<O> TaskState<O> {
    /// Unwrap into the underlying [`TaskOutcome`] for use by store implementors and tests.
    pub fn into_outcome(self) -> TaskOutcome {
        self.inner
    }
}

/// Optional DI parameter for task handlers that need suspend/resume.
///
/// Inject via handler signature: `suspension: Suspension<O>`.
/// Use `suspension.get()` to retrieve the resume value if the task was resumed.
pub struct Suspension<T> {
    resume_input: Option<T>,
}

impl<T: serde::de::DeserializeOwned + Send> callables::FromContextParts<TaskContext>
    for Suspension<T>
{
    fn from_context_parts(ctx: &TaskContext) -> Result<Self, callables::CallError> {
        let resume_input = ctx
            .record
            .resume_input
            .as_deref()
            .map(serde_json::from_str::<T>)
            .transpose()
            .map_err(|_| callables::CallError::DeserializeFailed)?;
        Ok(Self { resume_input })
    }
}

impl<T> callables::IntoArgPart for Suspension<T> {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}

impl<T: Clone> Suspension<T> {
    /// Returns the resume payload if this task execution was triggered by a resume.
    /// Returns `None` on the first (non-resumed) execution.
    pub fn get(&self) -> Option<T> {
        self.resume_input.clone()
    }
}

pub struct TaskMeta {
    pub about: Cow<'static, str>,
    pub type_name: Cow<'static, str>,
    pub schema_fn: fn(&mut schemars::SchemaGenerator) -> schemars::Schema,
}

#[derive(Clone)]
pub struct TaskService {
    pub name: String,
    pub type_id: TypeId,
    pub type_name: String,
    pub coerce: fn(&str) -> Result<(), TaskError>,
    output: fn(callables::DataBox) -> TaskOutcome,
    handler: TaskHandler,
    operation: callables::Operation,
}

impl TaskService {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn operation(&self) -> callables::Operation {
        self.operation.clone()
    }

    pub fn validate_data(&self, data: &str) -> Result<(), TaskError> {
        (self.coerce)(data)
    }

    pub fn validate_object<T: 'static>(&self, _obj: &T) -> Result<(), TaskError> {
        if self.type_id != TypeId::of::<T>() {
            return Err(TaskError::TypeMismatch(
                self.type_name.clone(),
                std::any::type_name::<T>().to_string(),
            ));
        }
        Ok(())
    }

    pub async fn execute(&self, site: Site, record: Arc<TaskRecord>) -> TaskOutcome {
        let payload = match self.handler.deserialize_input(&record.input) {
            Ok(value) => value,
            Err(e) => return TaskOutcome::fail(format!("Task input error: {}", e)),
        };

        let ctx = TaskContext {
            site,
            payload,
            record,
            operation_id: self.operation.id,
        };

        let data = match self.handler.call(ctx).await {
            Ok(data) => data,
            Err(e) => return TaskOutcome::fail_error(&e),
        };

        (self.output)(data)
    }

    pub fn new<T, H, Args>(name: &str, handler: H) -> Self
    where
        T: callables::DataValue,
        H: callables::Specable<Args> + Send + Sync + 'static,
        H::Output: callables::IntoOutput<Error>
            + callables::IntoReturnPart
            + IntoTaskOutcomePart
            + Send
            + 'static,
        Args: callables::FromContext<TaskContext>
            + callables::IntoArgSpecs
            + callables::HasData<T>
            + Send
            + 'static,
    {
        let callable: callables::Callable<TaskContext, Error> = Callable::new(handler);
        let mut operation =
            callables::Operation::from_specs(callables::OperationKind::Task, callable.inspect());
        operation.name = name.to_string();
        let coerce = |data: &str| -> Result<(), TaskError> {
            let _: T = serde_json::from_str(data)?;
            Ok(())
        };
        TaskService {
            name: name.to_string(),
            type_id: TypeId::of::<T>(),
            type_name: std::any::type_name::<T>().to_string(),
            coerce,
            output: H::Output::into_task_outcome,
            handler: callable,
            operation,
        }
    }
}

#[derive(Clone)]
pub struct TaskRegistry {
    pub(crate) config: TaskConf,
    pub(crate) tasks: HashMap<String, TaskService>,
    pub(crate) typed_map: HashMap<TypeId, String>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self {
            config: TaskConf::default(),
            tasks: HashMap::new(),
            typed_map: HashMap::new(),
        }
    }

    pub fn with_config(self, config: TaskConf) -> Self {
        Self {
            config,
            tasks: self.tasks,
            typed_map: self.typed_map,
        }
    }

    pub fn iter_services(&self) -> impl Iterator<Item = &TaskService> {
        self.tasks.values()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn register(&mut self, service: TaskService) -> Result<(), TaskError> {
        let name = service.name().to_string();
        if self.tasks.contains_key(&name) || self.typed_map.contains_key(&service.type_id) {
            return Err(TaskError::AlreadyExists(name));
        }
        self.typed_map.insert(service.type_id, name.clone());
        self.tasks.insert(name, service);
        Ok(())
    }

    pub fn merge(&mut self, other: TaskRegistry) -> Result<(), TaskError> {
        for (name, task) in other.tasks {
            if self.tasks.contains_key(&name) {
                return Err(TaskError::AlreadyExists(name));
            }
            if self.typed_map.contains_key(&task.type_id) {
                return Err(TaskError::AlreadyExists(name));
            }
            self.typed_map.insert(task.type_id, name.clone());
            self.tasks.insert(name, task);
        }
        Ok(())
    }

    pub(crate) fn dispatcher<S: crate::tasks::store::AbstractTaskStore + Send + Sync + 'static>(
        self: Arc<Self>,
        store: Arc<S>,
    ) -> TaskDispatcher<S> {
        TaskDispatcher {
            store,
            registry: self.clone(),
            notifier: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub async fn execute(&self, site: Site, record: Arc<TaskRecord>) -> TaskOutcome {
        let task = match self.tasks.get(record.name()) {
            Some(task) => task,
            None => return TaskOutcome::fail(format!("Task '{}' not found", record.name())),
        };
        task.execute(site, record).await
    }
}

#[derive(Clone)]
pub struct TaskDispatcher<S: crate::tasks::store::AbstractTaskStore + Send + Sync + 'static> {
    pub(crate) store: Arc<S>,
    pub(crate) notifier: Arc<tokio::sync::Notify>,
    pub(crate) registry: Arc<TaskRegistry>,
}

#[derive(Clone)]
pub struct TaskClient<S: crate::tasks::store::AbstractTaskStore + Send + Sync + 'static> {
    dispatcher: TaskDispatcher<S>,
}

impl<S: crate::tasks::store::AbstractTaskStore + Send + Sync + 'static> TaskClient<S> {
    pub(crate) fn new(dispatcher: TaskDispatcher<S>) -> Self {
        Self { dispatcher }
    }

    pub async fn submit<T: Serialize + 'static>(&self, input: T) -> Result<uuid::Uuid, TaskError> {
        self.dispatcher.submit(input).await
    }

    pub async fn submit_with<T: Serialize + 'static>(
        &self,
        input: T,
        conf: TaskOptions,
    ) -> Result<uuid::Uuid, TaskError> {
        self.dispatcher.submit_with(input, conf).await
    }

    pub async fn resume<T: Serialize>(&self, id: uuid::Uuid, input: T) -> Result<u64, TaskError> {
        self.dispatcher.resume(id, input).await
    }

    pub async fn list(&self, filter: TaskListFilter) -> Result<TaskListPage, TaskError> {
        self.dispatcher.list(filter).await
    }

    pub async fn get(&self, id: uuid::Uuid) -> Result<Option<TaskRecord>, TaskError> {
        self.dispatcher.get(id).await
    }
}

impl<S: crate::tasks::store::AbstractTaskStore + Send + Sync + 'static> TaskDispatcher<S> {
    pub fn has_tasks(&self) -> bool {
        !self.registry.is_empty()
    }

    pub fn store(&self) -> Arc<S> {
        self.store.clone()
    }

    pub async fn submit<T: 'static + Serialize>(&self, input: T) -> Result<uuid::Uuid, TaskError> {
        let name = self
            .registry
            .typed_map
            .get(&TypeId::of::<T>())
            .ok_or_else(|| TaskError::TaskNotFound("Unknown task type".to_string()))?
            .clone();
        self.submit_registered::<T>(&name, input, TaskOptions::default())
            .await
    }

    pub async fn submit_with<T: 'static + Serialize>(
        &self,
        input: T,
        conf: TaskOptions,
    ) -> Result<uuid::Uuid, TaskError> {
        let name = self
            .registry
            .typed_map
            .get(&TypeId::of::<T>())
            .ok_or_else(|| TaskError::TaskNotFound("Unknown task type".to_string()))?
            .clone();
        self.submit_registered::<T>(&name, input, conf).await
    }

    async fn submit_registered<T: 'static + Serialize>(
        &self,
        name: &str,
        input: T,
        conf: TaskOptions,
    ) -> Result<uuid::Uuid, TaskError> {
        if let Some(s) = self.registry.tasks.get(name) {
            s.validate_object(&input)?;
        } else {
            return Err(TaskError::TaskNotFound(name.to_string()));
        }
        let data = serde_json::to_string(&input)?;
        self.submit_serialized(name, data, conf).await
    }

    async fn submit_serialized(
        &self,
        name: &str,
        input: String,
        conf: TaskOptions,
    ) -> Result<uuid::Uuid, TaskError> {
        let now = chrono::Utc::now();
        let ready_at = Some(match conf.initial_delay {
            Some(delay) => now + chrono::Duration::from_std(delay).unwrap_or_default(),
            None => now,
        });
        let retry_delay_ms = conf
            .retry_delay
            .map(|delay| delay.as_millis().min(i64::MAX as u128) as i64);
        let lease_duration_ms = conf
            .lease_duration
            .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64);
        let record = TaskRecord {
            id: uuid::Uuid::now_v7(),
            name: name.to_string(),
            input,
            state: conf.state,
            resume_input: None,
            output: None,
            result: None,
            status: TaskStatus::Pending,
            attempts: 0,
            priority: conf.priority,
            max_attempts: conf.max_attempts,
            retry_delay_ms,
            lease_duration_ms,
            last_error: None,
            identity: conf.identity,
            locked_by: None,
            leased_until: None,
            ready_at,
            created_at: now,
            updated_at: now,
            completed_at: None,
        };
        let task_id = record.id;
        self.store.store_task(record).await?;
        self.notifier.notify_one();
        Ok(task_id)
    }

    pub async fn resume<T: Serialize>(&self, id: uuid::Uuid, input: T) -> Result<u64, TaskError> {
        let input = serde_json::to_string(&input)?;
        let count = self.store.resume(id, input).await?;
        if count > 0 {
            self.notifier.notify_waiters();
        }
        Ok(count)
    }

    pub async fn list(&self, filter: TaskListFilter) -> Result<TaskListPage, TaskError> {
        self.store.list_tasks(filter).await
    }

    pub async fn get(&self, id: uuid::Uuid) -> Result<Option<TaskRecord>, TaskError> {
        self.store.get_task(id).await
    }
}

impl std::fmt::Debug for TaskRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRegistry")
            .field("tasks", &self.tasks.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{
        Data, SiteError,
        tasks::{MemoryTaskStore, store::AbstractTaskStore},
    };

    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
    struct DirectJob {
        id: i64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
    struct ReportOutput {
        value: String,
    }

    async fn direct_job(input: Data<DirectJob>) -> Result<TaskState<String>, crate::Error> {
        Ok(TaskState::complete(format!("direct:{}", input.id))?)
    }

    async fn unit_job(_input: Data<DirectJob>) {}

    async fn result_unit_job(_input: Data<DirectJob>) -> Result<(), crate::Error> {
        Ok(())
    }

    async fn data_job(input: Data<DirectJob>) -> Data<ReportOutput> {
        Data::new(ReportOutput {
            value: format!("data:{}", input.id),
        })
    }

    async fn result_data_job(input: Data<DirectJob>) -> Result<Data<ReportOutput>, crate::Error> {
        Ok(Data::new(ReportOutput {
            value: format!("result:{}", input.id),
        }))
    }

    async fn result_data_error(
        _input: Data<DirectJob>,
    ) -> Result<Data<ReportOutput>, crate::Error> {
        Err(crate::Error::invalid("data failed"))
    }

    fn record<T: Serialize>(name: &str, input: &T) -> Result<Arc<TaskRecord>, TaskError> {
        let now = chrono::Utc::now();
        Ok(Arc::new(TaskRecord {
            id: uuid::Uuid::now_v7(),
            name: name.to_string(),
            input: serde_json::to_string(input)?,
            state: None,
            resume_input: None,
            output: None,
            result: None,
            status: TaskStatus::Running,
            attempts: 0,
            priority: 0,
            max_attempts: None,
            retry_delay_ms: None,
            lease_duration_ms: None,
            last_error: None,
            identity: None,
            locked_by: Some("runner-a".to_string()),
            leased_until: None,
            ready_at: Some(now),
            created_at: now,
            updated_at: now,
            completed_at: None,
        }))
    }

    async fn test_site() -> Result<Site, SiteError> {
        Site::build(
            crate::SiteConf::default().log_init(false),
            crate::bundles::bundle([]),
        )
        .await
    }

    fn complete_result(outcome: TaskOutcome) -> Option<String> {
        match outcome {
            TaskOutcome::Complete { result } => Some(result),
            _ => None,
        }
    }

    fn failed_error(outcome: TaskOutcome) -> Option<String> {
        match outcome {
            TaskOutcome::Fail { error } => Some(error),
            _ => None,
        }
    }

    #[tokio::test]
    async fn direct_registration_supports_typed_submit() -> Result<(), TaskError> {
        let mut registry = TaskRegistry::new();
        registry.register(TaskService::new("direct_job", direct_job))?;

        let store = Arc::new(MemoryTaskStore::new(10));
        let dispatcher = Arc::new(registry).dispatcher(store.clone());
        let client = TaskClient::new(dispatcher);

        let task_id = client.submit(DirectJob { id: 42 }).await?;
        let claimed = store.claim_tasks("runner-a").await?;

        assert_eq!(claimed.len(), 1);
        assert_eq!(claimed[0].id, task_id);
        assert_eq!(claimed[0].name, "direct_job");
        assert_eq!(claimed[0].input::<DirectJob>()?.id, 42);

        store
            .commit_outcome(task_id, "runner-a", TaskOutcome::complete(&"done")?)
            .await?;
        Ok(())
    }

    /// Verifies a task handler receives the canonical ID stored in its operation metadata.
    #[tokio::test]
    async fn task_extracts_operation_id() -> Result<(), String> {
        let seen = Arc::new(parking_lot::Mutex::new(None));
        let captured = Arc::clone(&seen);
        let handler = move |id: crate::OperationId, _input: Data<DirectJob>| {
            let captured = Arc::clone(&captured);
            async move {
                *captured.lock() = Some(id);
            }
        };
        let service = TaskService::new("operation_job", handler);
        let expected = service.operation().id;
        let task =
            record("operation_job", &DirectJob { id: 1 }).map_err(|error| error.to_string())?;
        service
            .execute(test_site().await.map_err(|error| error.to_string())?, task)
            .await;
        assert_eq!(*seen.lock(), Some(expected));
        Ok(())
    }

    #[tokio::test]
    async fn task_unit_output_completes_with_null() -> Result<(), Box<dyn std::error::Error>> {
        let service = TaskService::new("unit_job", unit_job);
        let outcome = service
            .execute(
                test_site().await?,
                record("unit_job", &DirectJob { id: 7 })?,
            )
            .await;

        assert_eq!(complete_result(outcome).as_deref(), Some("null"));
        Ok(())
    }

    #[tokio::test]
    async fn task_result_unit_output_completes_with_null() -> Result<(), Box<dyn std::error::Error>>
    {
        let service = TaskService::new("result_unit_job", result_unit_job);
        let outcome = service
            .execute(
                test_site().await?,
                record("result_unit_job", &DirectJob { id: 7 })?,
            )
            .await;

        assert_eq!(complete_result(outcome).as_deref(), Some("null"));
        Ok(())
    }

    #[tokio::test]
    async fn task_state_output_still_controls_outcome() -> Result<(), Box<dyn std::error::Error>> {
        let service = TaskService::new("direct_job", direct_job);
        let outcome = service
            .execute(
                test_site().await?,
                record("direct_job", &DirectJob { id: 7 })?,
            )
            .await;

        assert_eq!(complete_result(outcome).as_deref(), Some("\"direct:7\""));
        Ok(())
    }

    #[tokio::test]
    async fn task_data_output_completes_with_payload() -> Result<(), Box<dyn std::error::Error>> {
        let service = TaskService::new("data_job", data_job);
        let outcome = service
            .execute(
                test_site().await?,
                record("data_job", &DirectJob { id: 7 })?,
            )
            .await;

        assert_eq!(
            complete_result(outcome).as_deref(),
            Some("{\"value\":\"data:7\"}")
        );
        Ok(())
    }

    #[tokio::test]
    async fn task_result_data_output_completes_with_payload()
    -> Result<(), Box<dyn std::error::Error>> {
        let service = TaskService::new("result_data_job", result_data_job);
        let outcome = service
            .execute(
                test_site().await?,
                record("result_data_job", &DirectJob { id: 7 })?,
            )
            .await;

        assert_eq!(
            complete_result(outcome).as_deref(),
            Some("{\"value\":\"result:7\"}")
        );
        Ok(())
    }

    #[tokio::test]
    async fn task_result_data_error_fails_task() -> Result<(), Box<dyn std::error::Error>> {
        let service = TaskService::new("result_data_error", result_data_error);
        let outcome = service
            .execute(
                test_site().await?,
                record("result_data_error", &DirectJob { id: 7 })?,
            )
            .await;

        assert!(failed_error(outcome).is_some_and(|error| error.contains("data failed")));
        Ok(())
    }
}
