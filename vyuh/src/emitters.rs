use serde::{Deserialize, Serialize};
use std::{
    any::TypeId,
    collections::{BinaryHeap, HashMap},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use crate::utils::debounce::{DebounceQueue, Debouncer};
use crate::{
    Data, Error, Site,
    callables::{self, CallError, Callable, DataBox, DataValue, HasSite, IntoArgPart, IntoDataBox},
};

pub use crate::utils::debounce::{DebounceConf, DebounceMode};

mod scheduled;

/// Computes the next normal task-schedule occurrence after one timestamp.
pub(crate) fn next_task_schedule(
    schedule: &crate::tasks::TaskScheduleConf,
    after: chrono::DateTime<chrono::Utc>,
) -> Result<chrono::DateTime<chrono::Utc>, EmitterError> {
    match schedule.source.as_str() {
        "cron" => next_cron_schedule(&schedule.expression, after),
        "periodic" => next_periodic_schedule(&schedule.expression, after),
        _ => Err(EmitterError::InvalidSchedule(
            "task schedule has an unsupported source".into(),
        )),
    }
}

/// Computes one cron occurrence from the normalized schedule expression.
fn next_cron_schedule(
    expression: &str,
    after: chrono::DateTime<chrono::Utc>,
) -> Result<chrono::DateTime<chrono::Utc>, EmitterError> {
    let schedule = cron::Schedule::from_str(expression)?;
    schedule.after(&after).next().ok_or_else(|| {
        EmitterError::InvalidSchedule("cron expression has no next occurrence".into())
    })
}

/// Computes one UTC-aligned periodic occurrence from normalized milliseconds.
fn next_periodic_schedule(
    expression: &str,
    after: chrono::DateTime<chrono::Utc>,
) -> Result<chrono::DateTime<chrono::Utc>, EmitterError> {
    let milliseconds = expression
        .parse::<u64>()
        .map_err(|_| EmitterError::InvalidSchedule("periodic interval is invalid".into()))?;
    scheduled::periodic_boundary(after, Duration::from_millis(milliseconds))
}

/// Selects where a timed emitter sends its produced data.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub enum EmitterExecutor {
    /// Delivers data to signals and channels, preserving the existing emitter behavior.
    #[default]
    Signal,
    /// Submits data to one registered durable task.
    Task,
}

/// Controls the first durable submission of a task-targeted schedule.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub enum ScheduleStart {
    /// Wait for the first normal cron occurrence or UTC-aligned periodic boundary.
    #[default]
    Next,
    /// Submit one task during initial activation before normal scheduling begins.
    Immediately,
}

impl ScheduleStart {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Next => "next",
            Self::Immediately => "immediately",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EmitterConf {
    #[serde(default = "default_notify_channel_capacity")]
    pub notify_channel_capacity: usize,
    #[serde(default = "default_max_in_flight_handlers")]
    pub max_in_flight_handlers: usize,
    #[serde(default = "default_pgnotify_reconnect_initial_ms")]
    pub pgnotify_reconnect_initial_ms: u64,
    #[serde(default = "default_pgnotify_reconnect_max_ms")]
    pub pgnotify_reconnect_max_ms: u64,
}

impl Default for EmitterConf {
    fn default() -> Self {
        Self {
            notify_channel_capacity: default_notify_channel_capacity(),
            max_in_flight_handlers: default_max_in_flight_handlers(),
            pgnotify_reconnect_initial_ms: default_pgnotify_reconnect_initial_ms(),
            pgnotify_reconnect_max_ms: default_pgnotify_reconnect_max_ms(),
        }
    }
}

const fn default_notify_channel_capacity() -> usize {
    1024
}

const fn default_max_in_flight_handlers() -> usize {
    64
}

const fn default_pgnotify_reconnect_initial_ms() -> u64 {
    250
}

const fn default_pgnotify_reconnect_max_ms() -> u64 {
    30_000
}

impl EmitterConf {
    pub(crate) fn notify_channel_capacity(&self) -> usize {
        self.notify_channel_capacity.max(1)
    }

    pub(crate) fn max_in_flight_handlers(&self) -> usize {
        self.max_in_flight_handlers.max(1)
    }

    pub(crate) fn pgnotify_reconnect_initial_ms(&self) -> u64 {
        self.pgnotify_reconnect_initial_ms.max(1)
    }

    pub(crate) fn pgnotify_reconnect_max_ms(&self) -> u64 {
        self.pgnotify_reconnect_max_ms
            .max(self.pgnotify_reconnect_initial_ms())
    }
}

pub struct IterCount(pub usize);

impl IntoArgPart for IterCount {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}

impl callables::FromContext<EmitterContext> for IterCount {
    fn from_context(ctx: EmitterContext) -> Result<Self, CallError> {
        Ok(IterCount(ctx.iter_count))
    }
}

impl callables::FromContextParts<EmitterContext> for IterCount {
    fn from_context_parts(ctx: &EmitterContext) -> Result<Self, CallError> {
        Ok(IterCount(ctx.iter_count))
    }
}

pub struct IterInstant(pub Option<tokio::time::Instant>);

impl IntoArgPart for IterInstant {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}

impl callables::FromContext<EmitterContext> for IterInstant {
    fn from_context(ctx: EmitterContext) -> Result<Self, CallError> {
        Ok(IterInstant(ctx.last_time))
    }
}

impl callables::FromContextParts<EmitterContext> for IterInstant {
    fn from_context_parts(ctx: &EmitterContext) -> Result<Self, CallError> {
        Ok(IterInstant(ctx.last_time))
    }
}

pub struct EmitterContext {
    site: Site,
    iter_count: usize,
    last_time: Option<tokio::time::Instant>,
    payload: DataBox,
}

impl HasSite for EmitterContext {
    fn site(&self) -> &Site {
        &self.site
    }
}

pub type EmitterHandler = Callable<EmitterContext, Error>;

impl IntoDataBox for EmitterContext {
    fn into_data_box(self) -> DataBox {
        self.payload
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EmitterError {
    #[error("Emitter data type mismatch")]
    PgNotifyError(#[from] crate::db::DbError),

    #[error("Cron expression error: {0}")]
    CronError(#[from] cron::error::Error),

    #[error("Emitter with the given type already exists")]
    AlreadyExists,

    #[error("Invalid debounce configuration: {0}")]
    InvalidDebounce(String),

    #[error("Invalid schedule configuration: {0}")]
    InvalidSchedule(String),

    #[error("Task schedule '{schedule}' has no registered task for {type_name}")]
    MissingTaskTarget { schedule: String, type_name: String },

    #[error("Other error: {0}")]
    OtherError(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error(transparent)]
    CallError(#[from] CallError),
}

#[derive(Debug)]
pub struct Emitter {
    type_id: TypeId,
    type_name: String,
    executor: EmitterExecutor,
    schedule: Option<TaskSchedule>,
    source: EmitterSource,
}

impl Emitter {
    fn key(&self) -> EmitterKey {
        match &self.schedule {
            Some(schedule) => EmitterKey::TaskSchedule(schedule.name.clone()),
            None => EmitterKey::Signal(self.type_id, self.source.discriminant()),
        }
    }

    pub fn operation(&self) -> callables::Operation {
        let specs = self.source.spec();
        let (kind, config) = match &self.source {
            EmitterSource::Cron { schedule, .. } => {
                let config = self.with_task_schedule_conf(serde_json::json!({
                    "expr": schedule.to_string(),
                }));
                (callables::OperationKind::Cron, Some(config))
            }
            EmitterSource::Periodic { interval, .. } => {
                let config = self.with_task_schedule_conf(serde_json::json!({
                    "interval_secs": interval.as_secs(),
                }));
                (callables::OperationKind::Periodic, Some(config))
            }
            EmitterSource::PgNotify {
                channel, debounce, ..
            } => {
                let mut config = serde_json::json!({ "channel": channel });
                if let Some(debounce) = debounce {
                    config["debounce"] = serde_json::json!({
                        "mode": debounce.mode.as_str(),
                        "window_ms": debounce.window.as_millis(),
                    });
                }
                (callables::OperationKind::PgNotify, Some(config))
            }
        };
        callables::Operation::from_specs(kind, specs).with_conf(&config)
    }
}

impl Emitter {
    fn with_task_schedule_conf(&self, mut config: serde_json::Value) -> serde_json::Value {
        let Some(schedule) = &self.schedule else {
            return config;
        };
        config["executor"] = serde_json::json!("task");
        config["schedule"] = serde_json::json!(schedule.name);
        config["start"] = serde_json::json!(schedule.start.as_str());
        config
    }
}

#[derive(Clone, Debug)]
struct TaskSchedule {
    name: String,
    start: ScheduleStart,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
enum EmitterKey {
    Signal(TypeId, u8),
    TaskSchedule(String),
}

#[repr(u8)]
enum EmitterSource {
    Cron {
        schedule: cron::Schedule,
        handler: EmitterHandler,
    },
    Periodic {
        interval: tokio::time::Duration,
        handler: EmitterHandler,
    },
    PgNotify {
        channel: String,
        handler: EmitterHandler,
        debounce: Option<DebounceConf>,
    },
}

impl EmitterSource {
    pub fn discriminant(&self) -> u8 {
        match self {
            EmitterSource::Cron { .. } => 0,
            EmitterSource::Periodic { .. } => 1,
            EmitterSource::PgNotify { .. } => 2,
        }
    }

    fn spec(&self) -> &callables::CallSpec {
        match self {
            EmitterSource::Cron { handler, .. } => handler.inspect(),
            EmitterSource::Periodic { handler, .. } => handler.inspect(),
            EmitterSource::PgNotify { handler, .. } => handler.inspect(),
        }
    }
}

impl std::fmt::Debug for EmitterSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmitterSource::Cron { schedule, .. } => f.debug_tuple("Cron").field(schedule).finish(),
            EmitterSource::Periodic { interval, .. } => {
                f.debug_tuple("Periodic").field(interval).finish()
            }
            EmitterSource::PgNotify { channel, .. } => {
                f.debug_tuple("PgNotify").field(channel).finish()
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct CronConf {
    expr: String,
    executor: EmitterExecutor,
    schedule: Option<String>,
    start: ScheduleStart,
}

impl CronConf {
    /// Creates one cron configuration with the existing signal executor.
    pub fn new(expr: impl Into<String>) -> Self {
        Self {
            expr: expr.into(),
            executor: EmitterExecutor::Signal,
            schedule: None,
            start: ScheduleStart::Next,
        }
    }

    /// Selects signal delivery or durable task submission.
    pub const fn executor(mut self, executor: EmitterExecutor) -> Self {
        self.executor = executor;
        self
    }

    /// Gives a durable task schedule a stable name independent of its handler name.
    pub fn schedule(mut self, name: impl Into<String>) -> Self {
        self.schedule = Some(name.into());
        self
    }

    /// Selects the initial activation policy for a durable task schedule.
    pub const fn on_start(mut self, start: ScheduleStart) -> Self {
        self.start = start;
        self
    }
}

#[derive(Clone, Debug)]
pub struct PeriodicConf {
    interval: tokio::time::Duration,
    executor: EmitterExecutor,
    schedule: Option<String>,
    start: ScheduleStart,
}

impl PeriodicConf {
    /// Creates one periodic configuration with the existing signal executor.
    pub const fn new(interval: tokio::time::Duration) -> Self {
        Self {
            interval,
            executor: EmitterExecutor::Signal,
            schedule: None,
            start: ScheduleStart::Next,
        }
    }

    /// Selects signal delivery or durable task submission.
    pub const fn executor(mut self, executor: EmitterExecutor) -> Self {
        self.executor = executor;
        self
    }

    /// Gives a durable task schedule a stable name independent of its handler name.
    pub fn schedule(mut self, name: impl Into<String>) -> Self {
        self.schedule = Some(name.into());
        self
    }

    /// Selects the initial activation policy for a durable task schedule.
    pub const fn on_start(mut self, start: ScheduleStart) -> Self {
        self.start = start;
        self
    }
}

impl std::str::FromStr for DebounceMode {
    type Err = EmitterError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "leading" => Ok(Self::Leading),
            "trailing" => Ok(Self::Trailing),
            "leading_trailing" | "leading-and-trailing" | "leading+trailing" => {
                Ok(Self::LeadingAndTrailing)
            }
            other => Err(EmitterError::InvalidDebounce(format!(
                "unsupported debounce mode '{}'",
                other
            ))),
        }
    }
}

/// Validates one public debounce configuration before it starts an emitter.
fn validate_debounce(conf: &DebounceConf) -> Result<(), EmitterError> {
    if conf.is_valid() {
        return Ok(());
    }
    Err(EmitterError::InvalidDebounce(
        "window must be greater than zero".to_string(),
    ))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PgNotifyConf {
    pub channel: String,
    pub debounce: Option<DebounceConf>,
}

#[doc(hidden)]
pub trait EmitsData<O: DataValue> {}

impl<O: DataValue> EmitsData<O> for Data<O> {}

impl<O, E> EmitsData<O> for Result<Data<O>, E> where O: DataValue {}

pub fn cron<H, Args, O>(handler: H, options: CronConf) -> Result<Emitter, EmitterError>
where
    O: DataValue,
    H: callables::Specable<Args> + Send + Sync + 'static,
    H::Output:
        callables::IntoOutput<Error> + callables::IntoReturnPart + EmitsData<O> + Send + 'static,
    Args: callables::FromContext<EmitterContext> + callables::IntoArgSpecs,
{
    let wrapper = Callable::new(handler);
    let schedule = task_schedule(&options, wrapper.inspect().name.as_str())?;
    Ok(Emitter {
        type_id: TypeId::of::<O>(),
        type_name: std::any::type_name::<O>().into(),
        executor: options.executor,
        schedule,
        source: EmitterSource::Cron {
            schedule: options.expr.parse::<cron::Schedule>()?,
            handler: wrapper,
        },
    })
}

pub fn periodic<H, Args, O>(handler: H, options: PeriodicConf) -> Result<Emitter, EmitterError>
where
    O: DataValue,
    H: callables::Specable<Args> + Send + Sync + 'static,
    H::Output:
        callables::IntoOutput<Error> + callables::IntoReturnPart + EmitsData<O> + Send + 'static,
    Args: callables::FromContext<EmitterContext> + callables::IntoArgSpecs,
{
    if options.interval.is_zero() {
        return Err(EmitterError::InvalidSchedule(
            "periodic intervals must be greater than zero".into(),
        ));
    }
    let wrapper = Callable::new(handler);
    let schedule = task_schedule_periodic(&options, wrapper.inspect().name.as_str())?;
    Ok(Emitter {
        type_id: TypeId::of::<O>(),
        type_name: std::any::type_name::<O>().into(),
        executor: options.executor,
        schedule,
        source: EmitterSource::Periodic {
            interval: options.interval,
            handler: wrapper,
        },
    })
}

pub fn pgnotify<H, Args, O>(handler: H, options: PgNotifyConf) -> Result<Emitter, EmitterError>
where
    O: DataValue,
    H: callables::Specable<Args> + Send + Sync + 'static,
    H::Output:
        callables::IntoOutput<Error> + callables::IntoReturnPart + EmitsData<O> + Send + 'static,
    Args: callables::FromContext<EmitterContext> + callables::IntoArgSpecs,
{
    if let Some(debounce) = &options.debounce {
        validate_debounce(debounce)?;
    }
    let wrapper = Callable::new(handler);
    Ok(Emitter {
        type_id: TypeId::of::<O>(),
        type_name: std::any::type_name::<O>().into(),
        executor: EmitterExecutor::Signal,
        schedule: None,
        source: EmitterSource::PgNotify {
            channel: options.channel,
            handler: wrapper,
            debounce: options.debounce,
        },
    })
}

fn task_schedule(
    options: &CronConf,
    handler_name: &str,
) -> Result<Option<TaskSchedule>, EmitterError> {
    if options.executor == EmitterExecutor::Signal {
        if options.schedule.is_some() || options.start != ScheduleStart::Next {
            return Err(EmitterError::InvalidSchedule(
                "schedule and start apply only to task executors".into(),
            ));
        }
        return Ok(None);
    }
    schedule_from_parts(options.schedule.as_deref(), handler_name, options.start)
}

fn task_schedule_periodic(
    options: &PeriodicConf,
    handler_name: &str,
) -> Result<Option<TaskSchedule>, EmitterError> {
    if options.executor == EmitterExecutor::Signal {
        if options.schedule.is_some() || options.start != ScheduleStart::Next {
            return Err(EmitterError::InvalidSchedule(
                "schedule and start apply only to task executors".into(),
            ));
        }
        return Ok(None);
    }
    schedule_from_parts(options.schedule.as_deref(), handler_name, options.start)
}

fn schedule_from_parts(
    configured: Option<&str>,
    handler_name: &str,
    start: ScheduleStart,
) -> Result<Option<TaskSchedule>, EmitterError> {
    let name = configured.unwrap_or(handler_name);
    if name.is_empty() || name.chars().count() > 191 {
        return Err(EmitterError::InvalidSchedule(
            "task schedule names must contain between 1 and 191 characters".into(),
        ));
    }
    Ok(Some(TaskSchedule {
        name: name.into(),
        start,
    }))
}

#[derive(Clone)]
pub struct EmitterRegistry {
    sources: HashMap<EmitterKey, Arc<Emitter>>,
}

impl EmitterRegistry {
    pub fn new() -> Self {
        Self {
            sources: HashMap::new(),
        }
    }

    pub fn iter_emitters(&self) -> impl Iterator<Item = &Emitter> {
        self.sources.values().map(|e| e.as_ref())
    }

    pub fn register(&mut self, emitter: Emitter) -> Result<(), EmitterError> {
        let key = emitter.key();
        if self.sources.contains_key(&key) {
            return Err(EmitterError::AlreadyExists);
        }
        self.sources.insert(key, Arc::new(emitter));
        Ok(())
    }

    pub fn merge(&mut self, other: EmitterRegistry) -> Result<(), EmitterError> {
        for (key, emitter) in other.sources {
            if self.sources.contains_key(&key) {
                return Err(EmitterError::AlreadyExists);
            }
            self.sources.insert(key, emitter);
        }
        Ok(())
    }

    pub fn create_engine(&self) -> EmitterEngine {
        self.create_engine_with_conf(EmitterConf::default())
    }

    pub(crate) fn create_engine_with_conf(&self, conf: EmitterConf) -> EmitterEngine {
        EmitterEngine {
            sources: self.sources.clone(),
            conf,
        }
    }

    /// Resolves every task-targeted emitter against the finalized task registry.
    pub(crate) fn task_schedules(
        &self,
        tasks: &crate::tasks::TaskRegistry,
    ) -> Result<Vec<crate::tasks::TaskScheduleConf>, EmitterError> {
        self.sources
            .values()
            .filter(|emitter| emitter.executor == EmitterExecutor::Task)
            .map(|emitter| emitter.task_schedule_conf(tasks))
            .collect()
    }
}

impl Emitter {
    fn task_schedule_conf(
        &self,
        tasks: &crate::tasks::TaskRegistry,
    ) -> Result<crate::tasks::TaskScheduleConf, EmitterError> {
        let schedule = self.schedule.as_ref().ok_or_else(|| {
            EmitterError::InvalidSchedule("task executor is missing schedule metadata".into())
        })?;
        let task =
            tasks
                .typed_map
                .get(&self.type_id)
                .ok_or_else(|| EmitterError::MissingTaskTarget {
                    schedule: schedule.name.clone(),
                    type_name: self.type_name.clone(),
                })?;
        let (source, expression) = match &self.source {
            EmitterSource::Cron { schedule, .. } => ("cron", schedule.to_string()),
            EmitterSource::Periodic { interval, .. } => {
                let milliseconds = interval.as_millis();
                if milliseconds == 0 {
                    return Err(EmitterError::InvalidSchedule(
                        "task periodic intervals must be at least one millisecond".into(),
                    ));
                }
                ("periodic", milliseconds.to_string())
            }
            EmitterSource::PgNotify { .. } => {
                return Err(EmitterError::InvalidSchedule(
                    "pgnotify emitters cannot submit durable tasks".into(),
                ));
            }
        };
        Ok(crate::tasks::TaskScheduleConf {
            name: schedule.name.clone(),
            task: task.clone(),
            source: source.into(),
            expression,
            start: schedule.start.as_str().into(),
        })
    }
}

#[derive(Clone)]
pub struct EmitterEngine {
    sources: HashMap<EmitterKey, Arc<Emitter>>,
    conf: EmitterConf,
}

impl EmitterEngine {
    async fn dispatch(&self, site: &Site, payload: DataBox) {
        if let Err(err) = site.dispatch_payload(payload).await {
            tracing::error!("Error dispatching emitter payload: {}", err);
        }
    }

    pub async fn run(self, site: Site) -> Result<(), EmitterError> {
        let mut timer_tasks = TimerQueue::new();
        let mut debounce_tasks = DebounceQueue::<usize>::new();
        let shutdown = site.shutdown_notifier();
        let handler_limit = Arc::new(tokio::sync::Semaphore::new(
            self.conf.max_in_flight_handlers(),
        ));
        let (completion_tx, mut completion_rx) =
            tokio::sync::mpsc::channel::<HandlerCompletion>(self.conf.max_in_flight_handlers());
        let mut notify_tasks: HashMap<String, Vec<usize>> = HashMap::new();
        let mut notify_works: Vec<NotifyWork> = Vec::new();

        let scheduled = self
            .sources
            .values()
            .filter(|emitter| emitter.executor == EmitterExecutor::Task)
            .cloned()
            .collect::<Vec<_>>();
        scheduled::start(&site, scheduled, handler_limit.clone()).await?;

        for emitter in self.sources.values() {
            if emitter.executor == EmitterExecutor::Task {
                continue;
            }
            match &emitter.source {
                EmitterSource::Periodic {
                    interval: dur,
                    handler,
                } => {
                    let work = TimerWork::new(
                        emitter.type_id,
                        TimerKind::Interval(dur.clone()),
                        handler.clone(),
                    );
                    timer_tasks.push(work);
                }
                EmitterSource::Cron { schedule, handler } => {
                    let work = TimerWork::new(
                        emitter.type_id,
                        TimerKind::Schedule(schedule.clone()),
                        handler.clone(),
                    );
                    timer_tasks.push(work);
                }
                EmitterSource::PgNotify {
                    channel,
                    handler,
                    debounce,
                } => {
                    let work_id = notify_works.len();
                    notify_works.push(NotifyWork::new(
                        work_id,
                        emitter.type_id,
                        channel.clone(),
                        handler.clone(),
                        debounce.clone(),
                    ));
                    notify_tasks
                        .entry(channel.clone())
                        .or_default()
                        .push(work_id);
                }
            }
        }
        let topics = notify_tasks.keys().cloned().collect::<Vec<_>>();
        let mut receiver = if topics.is_empty() {
            None
        } else {
            Some(site.consume_notify(&topics).await?)
        };
        let dummy_data = DataBox::new(String::new());
        loop {
            tokio::select! {
                Some(work) = timer_tasks.pop() => {
                    let source = HandlerSource::Timer {
                        kind: work.kind_label(),
                        type_id: work.type_id,
                    };
                    let ctx = EmitterContext{
                        site: site.clone(),
                        payload: dummy_data.clone(),
                        iter_count: work.iter_count,
                        last_time: work.last_time,
                    };
                    self.spawn_handler_call(
                        &site,
                        &handler_limit,
                        &completion_tx,
                        HandlerCall {
                            source,
                            handler: work.producer.clone(),
                            ctx,
                        },
                    );
                    timer_tasks.push(work);
                },
                _=shutdown.notified()=>{
                    tracing::info!("EmitterEngine shutting down");
                    break;
                }
                Some(deadline) = debounce_tasks.pop() => {
                    let call = notify_works
                        .get_mut(deadline.key)
                        .and_then(|work| work.on_debounce_deadline(deadline.deadline.generation()));
                    if let Some(call) = call {
                        self.spawn_notify_call(&site, &handler_limit, &completion_tx, call);
                    }
                }
                Some(completion) = completion_rx.recv() => {
                    match completion.result {
                        Ok(payload) => self.dispatch(&site, payload).await,
                        Err(err) => {
                            tracing::error!(
                                source = completion.source.as_str(),
                                source_detail = ?completion.source,
                                "Emitter handler failed: {}",
                                err
                            );
                        }
                    }
                }
                Some(notif) = async {
                    match &mut receiver {
                        Some(receiver) => receiver.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(work_ids) = notify_tasks.get(notif.channel.as_str()){
                        for work_id in work_ids {
                            let call = if let Some(work) = notify_works.get_mut(*work_id) {
                                timer_tasks.touch(work.type_id);
                                work.on_notify(notif.payload.clone(), &mut debounce_tasks)
                            } else {
                                None
                            };
                            if let Some(call) = call {
                                self.spawn_notify_call(&site, &handler_limit, &completion_tx, call);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn spawn_notify_call(
        &self,
        site: &Site,
        handler_limit: &Arc<tokio::sync::Semaphore>,
        completion_tx: &tokio::sync::mpsc::Sender<HandlerCompletion>,
        call: NotifyCall,
    ) {
        self.spawn_handler_call(
            site,
            handler_limit,
            completion_tx,
            HandlerCall {
                source: HandlerSource::PgNotify {
                    channel: call.channel,
                    type_id: call.type_id,
                    debounce: call.debounce,
                },
                handler: call.handler,
                ctx: EmitterContext {
                    site: site.clone(),
                    payload: DataBox::new(call.payload),
                    iter_count: call.iter_count,
                    last_time: call.last_time,
                },
            },
        );
    }

    fn spawn_handler_call(
        &self,
        site: &Site,
        handler_limit: &Arc<tokio::sync::Semaphore>,
        completion_tx: &tokio::sync::mpsc::Sender<HandlerCompletion>,
        call: HandlerCall,
    ) {
        let Ok(permit) = handler_limit.clone().try_acquire_owned() else {
            tracing::warn!(
                source = call.source.as_str(),
                source_detail = ?call.source,
                "Emitter handler skipped because max in-flight handler limit was reached"
            );
            return;
        };

        let completion_tx = completion_tx.clone();
        site.spawn(async move {
            let source = call.source;
            let result = call.handler.call(call.ctx).await;
            drop(permit);
            let _ = completion_tx
                .send(HandlerCompletion { source, result })
                .await;
        });
    }
}

struct HandlerCall {
    source: HandlerSource,
    handler: EmitterHandler,
    ctx: EmitterContext,
}

struct HandlerCompletion {
    source: HandlerSource,
    result: Result<DataBox, Error>,
}

#[derive(Clone)]
enum HandlerSource {
    Timer {
        kind: &'static str,
        type_id: TypeId,
    },
    PgNotify {
        channel: String,
        type_id: TypeId,
        debounce: Option<DebounceConf>,
    },
}

impl HandlerSource {
    fn as_str(&self) -> &'static str {
        match self {
            HandlerSource::Timer { kind, .. } => kind,
            HandlerSource::PgNotify { .. } => "pgnotify",
        }
    }
}

impl std::fmt::Debug for HandlerSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HandlerSource::Timer { kind, type_id } => f
                .debug_struct("Timer")
                .field("kind", kind)
                .field("type_id", type_id)
                .finish(),
            HandlerSource::PgNotify {
                channel,
                type_id,
                debounce,
            } => f
                .debug_struct("PgNotify")
                .field("channel", channel)
                .field("type_id", type_id)
                .field("debounce", debounce)
                .finish(),
        }
    }
}

struct NotifyWork {
    id: usize,
    type_id: TypeId,
    channel: String,
    handler: EmitterHandler,
    iter_count: usize,
    last_time: Option<tokio::time::Instant>,
    debounce: Option<Debouncer<String>>,
}

impl NotifyWork {
    fn new(
        id: usize,
        type_id: TypeId,
        channel: String,
        handler: EmitterHandler,
        debounce: Option<DebounceConf>,
    ) -> Self {
        Self {
            id,
            type_id,
            channel,
            handler,
            iter_count: 0,
            last_time: None,
            debounce: debounce.map(Debouncer::new),
        }
    }

    fn call(&self, payload: String) -> NotifyCall {
        NotifyCall {
            type_id: self.type_id,
            channel: self.channel.clone(),
            debounce: self.debounce.as_ref().map(|state| state.conf().clone()),
            handler: self.handler.clone(),
            iter_count: self.iter_count,
            last_time: self.last_time,
            payload,
        }
    }

    fn on_notify(
        &mut self,
        payload: String,
        queue: &mut DebounceQueue<usize>,
    ) -> Option<NotifyCall> {
        let Some(debounce) = self.debounce.as_mut() else {
            let call = self.call(payload);
            self.update();
            return Some(call);
        };
        let debounce_conf = debounce.conf().clone();
        let result = debounce.push(payload);
        if let Some(deadline) = result.deadline {
            queue.push(self.id, deadline);
        }
        let mut call = result.emission.map(|payload| self.call(payload));
        if let Some(call) = &mut call {
            call.debounce = Some(debounce_conf);
        }
        if call.is_some() {
            self.update();
        }
        call
    }

    fn on_debounce_deadline(&mut self, generation: u64) -> Option<NotifyCall> {
        let debounce = self.debounce.as_mut()?;
        let debounce_conf = debounce.conf().clone();
        let mut call = debounce.due(generation).map(|payload| self.call(payload));
        if let Some(call) = &mut call {
            call.debounce = Some(debounce_conf);
        }
        if call.is_some() {
            self.update();
        }
        call
    }

    fn update(&mut self) {
        self.iter_count = self.iter_count.wrapping_add(1);
        self.last_time = Some(tokio::time::Instant::now());
    }
}

struct NotifyCall {
    type_id: TypeId,
    channel: String,
    debounce: Option<DebounceConf>,
    handler: EmitterHandler,
    iter_count: usize,
    last_time: Option<tokio::time::Instant>,
    payload: String,
}

#[derive(Clone, Debug)]
enum TimerKind {
    Schedule(cron::Schedule),
    Interval(tokio::time::Duration),
}

impl TimerKind {
    fn label(&self) -> &'static str {
        match self {
            TimerKind::Schedule(_) => "cron",
            TimerKind::Interval(_) => "periodic",
        }
    }
}

struct TimerWork {
    type_id: TypeId,
    last: Option<tokio::time::Instant>,
    kind: TimerKind,
    producer: EmitterHandler,
    deadline: tokio::time::Instant,
    iter_count: usize,
    last_time: Option<tokio::time::Instant>,
}

impl TimerWork {
    fn new(type_id: TypeId, kind: TimerKind, producer: EmitterHandler) -> Self {
        let deadline = Self::make_deadline(None, &kind);
        Self {
            type_id,
            last: None,
            kind,
            producer,
            deadline,
            iter_count: 0,
            last_time: None,
        }
    }

    fn update_deadline(&mut self, last: tokio::time::Instant) -> bool {
        if self.last.map(|l| l >= last).unwrap_or(false) {
            return false;
        }
        self.last = Some(last);
        self.deadline = Self::make_deadline(self.last, &self.kind);
        true
    }

    fn update(&mut self) {
        self.iter_count = self.iter_count.wrapping_add(1);
        self.last_time = Some(tokio::time::Instant::now());
        self.last = Some(tokio::time::Instant::now());
        self.deadline = Self::make_deadline(self.last, &self.kind);
    }

    fn kind_label(&self) -> &'static str {
        self.kind.label()
    }

    fn make_deadline(last: Option<tokio::time::Instant>, kind: &TimerKind) -> tokio::time::Instant {
        match kind {
            TimerKind::Interval(dur) => {
                if let Some(last) = last {
                    last + *dur
                } else {
                    tokio::time::Instant::now()
                }
            }
            TimerKind::Schedule(schedule) => {
                let now = chrono::Utc::now();
                let next = schedule.upcoming(chrono::Utc).next().unwrap_or_default();
                let duration = next.signed_duration_since(now).to_std().unwrap_or_default();
                if let Some(_last) = last {
                    tokio::time::Instant::now() + duration
                } else {
                    tokio::time::Instant::now()
                }
            }
        }
    }
}

impl Eq for TimerWork {}

impl PartialEq for TimerWork {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline
    }
}

impl Ord for TimerWork {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.deadline.cmp(&self.deadline)
    }
}

impl PartialOrd for TimerWork {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct TimerQueue {
    heap: BinaryHeap<TimerWork>,
    timeout_map: HashMap<TypeId, tokio::time::Instant>,
    notifier: tokio::sync::Notify,
}

impl TimerQueue {
    fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            timeout_map: HashMap::new(),
            notifier: tokio::sync::Notify::new(),
        }
    }

    fn push(&mut self, work: TimerWork) {
        self.heap.push(work);
        self.notifier.notify_one();
    }

    /// It is intended to be used as a way to update the deadline when work is done elsewhere
    /// So the deadline may need to be adjusted based on the new time
    fn touch(&mut self, type_id: TypeId) {
        let key = type_id;
        self.timeout_map.insert(key, tokio::time::Instant::now());
    }

    async fn pop(&mut self) -> Option<TimerWork> {
        loop {
            if let Some(work) = self.heap.peek() {
                let now = tokio::time::Instant::now();

                if work.deadline <= now {
                    let mut popped = if let Some(p) = self.heap.pop() {
                        p
                    } else {
                        continue;
                    };
                    if let Some(updated) = self.timeout_map.remove(&popped.type_id) {
                        if popped.update_deadline(updated) {
                            self.heap.push(popped);
                            continue;
                        }
                    }
                    popped.update();
                    return Some(popped);
                }

                let wait = work.deadline - now;
                tokio::select! {
                    _ = tokio::time::sleep(wait) => {},
                    _ = self.notifier.notified() => {},
                }
            } else {
                self.notifier.notified().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::callables::Data;
    use schemars::JsonSchema;

    #[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
    struct TestEvent {
        count: usize,
    }

    async fn publish_event(IterCount(count): IterCount) -> Data<TestEvent> {
        Data::new(TestEvent { count })
    }

    async fn consume_event(Data(_event): Data<TestEvent>) {}

    #[test]
    fn periodic_registration_records_operation() {
        let emitter = periodic::<_, _, TestEvent>(
            publish_event,
            PeriodicConf::new(tokio::time::Duration::from_secs(30)),
        )
        .unwrap();
        let op = emitter.operation();

        assert_eq!(op.kind, callables::OperationKind::Periodic);
        assert_eq!(
            op.conf
                .as_ref()
                .and_then(|config| config.get("interval_secs"))
                .and_then(serde_json::Value::as_u64),
            Some(30)
        );
    }

    #[test]
    fn duplicate_source_for_data_type_is_rejected() {
        let first = periodic::<_, _, TestEvent>(
            publish_event,
            PeriodicConf::new(tokio::time::Duration::from_secs(30)),
        )
        .unwrap();
        let second = periodic::<_, _, TestEvent>(
            publish_event,
            PeriodicConf::new(tokio::time::Duration::from_secs(60)),
        )
        .unwrap();

        let mut registry = EmitterRegistry::new();
        registry.register(first).unwrap();
        assert!(matches!(
            registry.register(second),
            Err(EmitterError::AlreadyExists)
        ));
    }

    /// Verifies named task schedules may share one payload type without colliding.
    #[test]
    fn task_schedules_are_unique_by_stable_name() -> Result<(), EmitterError> {
        let first = periodic::<_, _, TestEvent>(
            publish_event,
            PeriodicConf::new(tokio::time::Duration::from_secs(30))
                .executor(EmitterExecutor::Task)
                .schedule("first"),
        )?;
        let second = periodic::<_, _, TestEvent>(
            publish_event,
            PeriodicConf::new(tokio::time::Duration::from_secs(60))
                .executor(EmitterExecutor::Task)
                .schedule("second"),
        )?;
        let duplicate = periodic::<_, _, TestEvent>(
            publish_event,
            PeriodicConf::new(tokio::time::Duration::from_secs(90))
                .executor(EmitterExecutor::Task)
                .schedule("first"),
        )?;
        let mut registry = EmitterRegistry::new();
        registry.register(first)?;
        registry.register(second)?;
        assert!(matches!(
            registry.register(duplicate),
            Err(EmitterError::AlreadyExists)
        ));
        Ok(())
    }

    /// Verifies task-targeted emitters reject an output type without a registered task.
    #[test]
    fn task_schedule_requires_registered_task_input() -> Result<(), EmitterError> {
        let emitter = periodic::<_, _, TestEvent>(
            publish_event,
            PeriodicConf::new(tokio::time::Duration::from_secs(30)).executor(EmitterExecutor::Task),
        )?;
        let mut registry = EmitterRegistry::new();
        registry.register(emitter)?;
        let tasks = crate::tasks::TaskRegistry::new()
            .finalize(crate::tasks::TaskConf::default())
            .map_err(|error| EmitterError::InvalidSchedule(error.to_string()))?;
        assert!(matches!(
            registry.task_schedules(&tasks),
            Err(EmitterError::MissingTaskTarget { .. })
        ));
        Ok(())
    }

    /// Verifies an omitted task schedule name uses the registered emitter handler name.
    #[test]
    fn task_schedule_defaults_to_emitter_handler_name() -> Result<(), EmitterError> {
        let emitter = periodic::<_, _, TestEvent>(
            publish_event,
            PeriodicConf::new(tokio::time::Duration::from_secs(30)).executor(EmitterExecutor::Task),
        )?;
        let mut emitters = EmitterRegistry::new();
        emitters.register(emitter)?;
        let mut tasks = crate::tasks::TaskRegistry::new();
        tasks
            .register(crate::tasks::RegisteredTask::new(
                crate::tasks::TaskDefinition::new("consume_event"),
                consume_event,
            ))
            .map_err(|error| EmitterError::InvalidSchedule(error.to_string()))?;
        let tasks = tasks
            .finalize(crate::tasks::TaskConf::default())
            .map_err(|error| EmitterError::InvalidSchedule(error.to_string()))?;
        let schedules = emitters.task_schedules(&tasks)?;
        assert!(
            schedules
                .first()
                .is_some_and(|schedule| schedule.name.ends_with("::publish_event"))
        );
        Ok(())
    }

    fn debounce_work(mode: DebounceMode) -> (NotifyWork, DebounceQueue<usize>) {
        (
            NotifyWork::new(
                0,
                TypeId::of::<TestEvent>(),
                "events".to_string(),
                Callable::new(publish_event),
                Some(DebounceConf {
                    window: tokio::time::Duration::from_millis(10),
                    mode,
                }),
            ),
            DebounceQueue::new(),
        )
    }

    /// Verifies leading debounce suppresses additional notifications until its deadline.
    #[test]
    fn leading_debounce_emits_first_only_inside_window() {
        let (mut work, mut queue) = debounce_work(DebounceMode::Leading);

        assert_eq!(
            work.on_notify("first".to_string(), &mut queue)
                .map(|call| call.payload),
            Some("first".to_string())
        );
        assert!(work.on_notify("second".to_string(), &mut queue).is_none());

        let generation = work
            .debounce
            .as_ref()
            .map(Debouncer::generation)
            .unwrap_or_default();
        assert!(work.on_debounce_deadline(generation).is_none());

        assert_eq!(
            work.on_notify("third".to_string(), &mut queue)
                .map(|call| call.payload),
            Some("third".to_string())
        );
    }

    /// Verifies trailing debounce emits only the latest payload at its current deadline.
    #[test]
    fn trailing_debounce_emits_latest_payload_on_matching_deadline() {
        let (mut work, mut queue) = debounce_work(DebounceMode::Trailing);

        assert!(work.on_notify("first".to_string(), &mut queue).is_none());
        let stale_generation = work
            .debounce
            .as_ref()
            .map(Debouncer::generation)
            .unwrap_or_default();
        assert!(work.on_notify("last".to_string(), &mut queue).is_none());

        assert!(work.on_debounce_deadline(stale_generation).is_none());

        let generation = work
            .debounce
            .as_ref()
            .map(Debouncer::generation)
            .unwrap_or_default();
        assert_eq!(
            work.on_debounce_deadline(generation)
                .map(|call| call.payload),
            Some("last".to_string())
        );
    }

    /// Verifies combined debounce emits a trailing value only after an additional notification.
    #[test]
    fn leading_and_trailing_emits_trailing_only_after_extra_payload() {
        let (mut single, mut queue) = debounce_work(DebounceMode::LeadingAndTrailing);
        assert_eq!(
            single
                .on_notify("only".to_string(), &mut queue)
                .map(|call| call.payload),
            Some("only".to_string())
        );
        let generation = single
            .debounce
            .as_ref()
            .map(Debouncer::generation)
            .unwrap_or_default();
        assert!(single.on_debounce_deadline(generation).is_none());

        let (mut burst, mut queue) = debounce_work(DebounceMode::LeadingAndTrailing);
        assert_eq!(
            burst
                .on_notify("first".to_string(), &mut queue)
                .map(|call| call.payload),
            Some("first".to_string())
        );
        assert!(burst.on_notify("middle".to_string(), &mut queue).is_none());
        assert!(burst.on_notify("last".to_string(), &mut queue).is_none());

        let generation = burst
            .debounce
            .as_ref()
            .map(Debouncer::generation)
            .unwrap_or_default();
        assert_eq!(
            burst
                .on_debounce_deadline(generation)
                .map(|call| call.payload),
            Some("last".to_string())
        );
    }

    #[test]
    fn pgnotify_operation_includes_debounce_metadata() {
        let emitter = pgnotify::<_, _, TestEvent>(
            |payload: Data<String>| async move {
                Data::new(TestEvent {
                    count: payload.len(),
                })
            },
            PgNotifyConf {
                channel: "events".to_string(),
                debounce: Some(DebounceConf {
                    window: tokio::time::Duration::from_millis(250),
                    mode: DebounceMode::LeadingAndTrailing,
                }),
            },
        )
        .unwrap();

        let op = emitter.operation();
        let config = op.conf.as_ref().unwrap();
        assert_eq!(config["channel"], "events");
        assert_eq!(config["debounce"]["mode"], "leading_trailing");
        assert_eq!(config["debounce"]["window_ms"], 250);
    }

    #[test]
    fn emitter_conf_sanitizes_runtime_limits() {
        let conf = EmitterConf {
            notify_channel_capacity: 0,
            max_in_flight_handlers: 0,
            pgnotify_reconnect_initial_ms: 0,
            pgnotify_reconnect_max_ms: 0,
        };

        assert_eq!(conf.notify_channel_capacity(), 1);
        assert_eq!(conf.max_in_flight_handlers(), 1);
        assert_eq!(conf.pgnotify_reconnect_initial_ms(), 1);
        assert_eq!(conf.pgnotify_reconnect_max_ms(), 1);
    }
}
