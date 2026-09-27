//! Task runner, lane, rate-limit, and idempotency configuration.

use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

use super::TaskRuntimeError;

/// Framework-owned lane used when a submission does not select one.
pub const DEFAULT_TASK_LANE: TaskLane = TaskLane::new("default");

const MAX_TASK_LANES: usize = 32;
const MAX_TASK_ATTEMPTS: u32 = 1_000;
const MAX_CONCURRENCY: usize = 4_096;
const MAX_BATCH_SIZE: usize = 10_000;
const MAX_INTERVAL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const MAX_LEASE: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const MAX_ERROR_BYTES: usize = 64 * 1024;
const MAX_IDEMPOTENCY_RETENTION: Duration = Duration::from_secs(10 * 365 * 24 * 60 * 60);
pub(crate) const MAX_TASK_DELAY: Duration = Duration::from_secs(10 * 365 * 24 * 60 * 60);
const DEFAULT_RETRY: TaskRetry = TaskRetry {
    max_attempts: 5,
    initial_delay: Duration::from_secs(1),
    max_delay: Duration::from_secs(5 * 60),
};

/// Stable name for one independently scheduled task lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskLane(&'static str);

impl TaskLane {
    /// Declares a reusable task lane descriptor.
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// Returns the configured lane name.
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for TaskLane {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

/// Token-bucket start rate for one task lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskRate {
    permits: u32,
    period: Duration,
    burst: u32,
}

impl TaskRate {
    /// Allows a sustained number of starts per second.
    pub const fn per_second(permits: u32) -> Self {
        Self::new(permits, Duration::from_secs(1))
    }

    /// Allows a sustained number of starts per minute.
    pub const fn per_minute(permits: u32) -> Self {
        Self::new(permits, Duration::from_secs(60))
    }

    /// Allows `permits` starts during each replenishment period.
    pub const fn new(permits: u32, period: Duration) -> Self {
        Self {
            permits,
            period,
            burst: permits,
        }
    }

    /// Sets the maximum immediately available permits.
    pub const fn burst(mut self, burst: u32) -> Self {
        self.burst = burst;
        self
    }

    /// Returns the sustained permit count for one period.
    pub const fn permits(self) -> u32 {
        self.permits
    }

    /// Returns the replenishment period.
    pub const fn period(self) -> Duration {
        self.period
    }

    /// Returns the maximum accumulated permit count.
    pub const fn burst_size(self) -> u32 {
        self.burst
    }
}

/// Lane-owned retry limit and exponential-backoff policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskRetry {
    max_attempts: u32,
    initial_delay: Duration,
    max_delay: Duration,
}

impl TaskRetry {
    /// Creates an exponential policy with a five-minute default delay cap.
    pub const fn exponential(max_attempts: u32, initial_delay: Duration) -> Self {
        Self {
            max_attempts,
            initial_delay,
            max_delay: Duration::from_secs(5 * 60),
        }
    }

    /// Sets the maximum delay between handler attempts.
    pub const fn max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    /// Returns the maximum number of handler invocations, including the first.
    pub const fn max_attempts(self) -> u32 {
        self.max_attempts
    }

    /// Returns the delay after the first retry request.
    pub const fn initial_delay(self) -> Duration {
        self.initial_delay
    }

    /// Returns the exponential-backoff delay cap.
    pub const fn maximum_delay(self) -> Duration {
        self.max_delay
    }

    /// Reports whether the completed invocation consumed the lane's attempt budget.
    pub(crate) fn exhausted(self, attempts: i32) -> Result<bool, TaskRuntimeError> {
        let attempts = u32::try_from(attempts).map_err(|_| {
            TaskRuntimeError::TaskExecutionError("task attempt count cannot be negative".into())
        })?;
        Ok(attempts >= self.max_attempts)
    }

    /// Calculates the bounded exponential delay after the completed invocation.
    pub(crate) fn delay(self, attempts: i32) -> Result<Duration, TaskRuntimeError> {
        let attempts = u32::try_from(attempts).map_err(|_| {
            TaskRuntimeError::TaskExecutionError("task attempt count cannot be negative".into())
        })?;
        let mut delay = self.initial_delay;
        for _ in 1..attempts {
            delay = delay.saturating_mul(2).min(self.max_delay);
            if delay == self.max_delay {
                break;
            }
        }
        Ok(delay)
    }
}

impl Default for TaskRetry {
    fn default() -> Self {
        DEFAULT_RETRY
    }
}

/// Runtime limits for one named task lane.
#[derive(Debug, Clone)]
pub struct TaskLaneConf {
    lane: TaskLane,
    concurrency: usize,
    lane_lock: Option<super::TaskLaneLock>,
    rate: Option<TaskRate>,
    global_rate: Option<TaskRate>,
    retry: TaskRetry,
    idempotency_retention: IdempotencyRetention,
}

impl TaskLaneConf {
    /// Creates one lane with its per-worker concurrency quota.
    pub const fn new(lane: TaskLane, concurrency: usize) -> Self {
        Self {
            lane,
            concurrency,
            lane_lock: None,
            rate: None,
            global_rate: None,
            retry: DEFAULT_RETRY,
            idempotency_retention: IdempotencyRetention::ActiveOnly,
        }
    }

    /// Enables durable single-owner coordination for this lane.
    pub fn lock(mut self, lane_lock: super::TaskLaneLock) -> Self {
        self.lane_lock = Some(lane_lock);
        self
    }

    /// Limits task starts within this site's local runner.
    pub const fn rate_limit(mut self, rate: TaskRate) -> Self {
        self.rate = Some(rate);
        self
    }

    /// Limits aggregate task starts across workers sharing the task store.
    pub const fn global_rate_limit(mut self, rate: TaskRate) -> Self {
        self.global_rate = Some(rate);
        self
    }

    /// Replaces this lane's retry limit and exponential-backoff policy.
    pub const fn retry(mut self, retry: TaskRetry) -> Self {
        self.retry = retry;
        self
    }

    /// Retains completed idempotency keys for this lane after terminal completion.
    pub const fn idempotency_retention(mut self, duration: Duration) -> Self {
        self.idempotency_retention = IdempotencyRetention::RetainFor(duration);
        self
    }

    /// Returns the lane descriptor.
    pub const fn lane(&self) -> TaskLane {
        self.lane
    }

    /// Returns this worker's concurrency quota.
    pub const fn concurrency(&self) -> usize {
        self.concurrency
    }

    /// Returns this lane's optional durable owner policy.
    pub const fn lane_lock(&self) -> Option<&super::TaskLaneLock> {
        self.lane_lock.as_ref()
    }

    /// Returns the optional local runner rate.
    pub const fn rate(&self) -> Option<TaskRate> {
        self.rate
    }

    /// Returns the optional shared-store rate.
    pub const fn global_rate(&self) -> Option<TaskRate> {
        self.global_rate
    }

    /// Returns this lane's retry policy.
    pub const fn retry_policy(&self) -> TaskRetry {
        self.retry
    }

    /// Returns this lane's completed-key retention policy.
    pub(crate) const fn idempotency_policy(&self) -> IdempotencyRetention {
        self.idempotency_retention
    }
}

/// Retention behavior for idempotent tasks in one effective lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdempotencyRetention {
    ActiveOnly,
    RetainFor(Duration),
}

/// Resolves task-declared lanes that are absent from the site configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TaskLanePolicy {
    /// Resolves an unconfigured declared lane through the default lane.
    #[default]
    UseDefault,
    /// Rejects site construction when a declared lane is not configured.
    RequireConfigured,
}

/// Controls how the durable task runtime contributes to site readiness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TaskReadiness {
    /// Require task initialization, but tolerate later transient store failures.
    #[default]
    StartupOnly,
    /// Mark the site unready after this many consecutive scheduler-store failures.
    AfterFailures(u32),
    /// Exclude tasks from the site readiness decision.
    Disabled,
}

impl TaskReadiness {
    /// Requires successful task initialization without failing readiness later.
    pub const fn startup_only() -> Self {
        Self::StartupOnly
    }

    /// Fails readiness after a bounded run of scheduler-store failures.
    pub const fn after_failures(failures: u32) -> Self {
        Self::AfterFailures(failures)
    }

    /// Keeps task health visible without changing site readiness.
    pub const fn disabled() -> Self {
        Self::Disabled
    }

    /// Returns the stable configured readiness-policy name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StartupOnly => "startup_only",
            Self::AfterFailures(_) => "after_failures",
            Self::Disabled => "disabled",
        }
    }
}

/// Runtime policy for durable task workers.
#[derive(Debug, Clone)]
pub struct TaskConf {
    poll_interval: Duration,
    fallback_poll_interval: Duration,
    concurrency: usize,
    batch_size: usize,
    max_all_children: usize,
    lease_duration: Duration,
    max_payload_bytes: usize,
    max_error_bytes: usize,
    lanes: Vec<TaskLaneConf>,
    missing_lane: TaskLanePolicy,
    readiness: TaskReadiness,
}

impl Default for TaskConf {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            fallback_poll_interval: Duration::from_secs(300),
            concurrency: 10,
            batch_size: 250,
            max_all_children: 256,
            lease_duration: Duration::from_secs(300),
            max_payload_bytes: 1024 * 1024,
            max_error_bytes: 8 * 1024,
            lanes: Vec::new(),
            missing_lane: TaskLanePolicy::default(),
            readiness: TaskReadiness::default(),
        }
    }
}

impl TaskConf {
    /// Limits one all-settled child group; validated at finalization in 1..=10,000.
    pub const fn max_all_children(mut self, maximum: usize) -> Self {
        self.max_all_children = maximum;
        self
    }

    pub(crate) const fn all_limit(&self) -> usize {
        self.max_all_children
    }

    /// Sets the maximum number of handlers running in this process.
    pub const fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Bounds claim rows, outcome count, and bulk-write chunks.
    /// An atomic `all` group may expand beyond this count up to `max_all_children`.
    pub const fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Sets the short delay used while a lane remains saturated.
    pub const fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Sets the maximum idle delay before the store is checked again.
    pub const fn fallback_poll_interval(mut self, interval: Duration) -> Self {
        self.fallback_poll_interval = interval;
        self
    }

    /// Sets the ownership lease renewed while a handler remains active.
    pub const fn lease_duration(mut self, duration: Duration) -> Self {
        self.lease_duration = duration;
        self
    }

    /// Sets the maximum serialized input, continuation, or resume payload size.
    pub const fn max_payload_bytes(mut self, bytes: usize) -> Self {
        self.max_payload_bytes = bytes;
        self
    }

    /// Sets the maximum persisted task error size.
    pub const fn max_error_bytes(mut self, bytes: usize) -> Self {
        self.max_error_bytes = bytes;
        self
    }

    /// Adds a complete application-owned lane definition or override.
    pub fn lane(mut self, lane: TaskLaneConf) -> Self {
        self.lanes.push(lane);
        self
    }

    /// Sets how unconfigured task-declared lanes are resolved at site construction.
    pub const fn missing_lane(mut self, policy: TaskLanePolicy) -> Self {
        self.missing_lane = policy;
        self
    }

    /// Sets how task runtime health contributes to site readiness.
    pub const fn readiness(mut self, policy: TaskReadiness) -> Self {
        self.readiness = policy;
        self
    }

    /// Validates task configuration that is independent of registered bundles.
    pub(crate) fn validate(&self) -> Result<(), TaskRuntimeError> {
        validate_scalars(self)?;
        validate_readiness(self.readiness)?;
        validate_overrides(&self.lanes, self.batch_size)?;
        Ok(())
    }

    /// Merges bundle defaults and application overrides into the complete lane set.
    pub(crate) fn resolve_lanes(
        &self,
        defaults: impl IntoIterator<Item = TaskLaneConf>,
    ) -> Result<Vec<TaskLaneConf>, TaskRuntimeError> {
        self.validate()?;
        let mut lanes = collect_lanes(defaults, "bundle")?;
        for lane in &self.lanes {
            lanes.insert(lane.lane(), lane.clone());
        }
        insert_default_lane(&mut lanes, self.concurrency)?;
        let lanes = lanes.into_values().collect::<Vec<_>>();
        validate_lanes(&lanes, self.concurrency, self.batch_size)?;
        Ok(lanes)
    }

    /// Resolves a declared task lane against the finalized site lane set.
    pub(crate) fn resolve_lane(
        &self,
        lanes: &[TaskLaneConf],
        lane: TaskLane,
    ) -> Result<(TaskLane, bool), TaskRuntimeError> {
        if lanes.iter().any(|entry| entry.lane() == lane) {
            return Ok((lane, false));
        }
        match self.missing_lane {
            TaskLanePolicy::UseDefault => Ok((DEFAULT_TASK_LANE, true)),
            TaskLanePolicy::RequireConfigured => {
                Err(TaskRuntimeError::UnknownLane(lane.to_string()))
            }
        }
    }

    pub(crate) const fn concurrency_value(&self) -> usize {
        self.concurrency
    }

    pub(crate) const fn batch_size_value(&self) -> usize {
        self.batch_size
    }

    pub(crate) const fn poll_interval_value(&self) -> Duration {
        self.poll_interval
    }

    pub(crate) const fn fallback_interval(&self) -> Duration {
        self.fallback_poll_interval
    }

    pub(crate) const fn lease_duration_value(&self) -> Duration {
        self.lease_duration
    }

    pub(crate) const fn payload_limit(&self) -> usize {
        self.max_payload_bytes
    }

    pub(crate) const fn error_limit(&self) -> usize {
        self.max_error_bytes
    }

    pub(crate) const fn readiness_policy(&self) -> TaskReadiness {
        self.readiness
    }
}

/// Rejects duplicate or invalid application lane overrides before merging defaults.
fn validate_overrides(lanes: &[TaskLaneConf], batch_size: usize) -> Result<(), TaskRuntimeError> {
    if lanes.len() > MAX_TASK_LANES {
        return Err(TaskRuntimeError::InvalidConfig(format!(
            "task lanes must contain at most {MAX_TASK_LANES} entries"
        )));
    }
    let mut names = HashSet::with_capacity(lanes.len());
    for lane in lanes {
        validate_lane(lane, &mut names)?;
        validate_lock(lane, batch_size)?;
    }
    Ok(())
}

/// Collects one source of complete lane definitions without allowing duplicates.
fn collect_lanes(
    defaults: impl IntoIterator<Item = TaskLaneConf>,
    source: &str,
) -> Result<BTreeMap<TaskLane, TaskLaneConf>, TaskRuntimeError> {
    let mut lanes = BTreeMap::new();
    for lane in defaults {
        if lane.lane() == DEFAULT_TASK_LANE {
            return Err(TaskRuntimeError::InvalidConfig(format!(
                "{source} task lanes cannot configure the default lane"
            )));
        }
        if lanes.insert(lane.lane(), lane).is_some() {
            return Err(TaskRuntimeError::InvalidConfig(format!(
                "duplicate {source} task lane definition"
            )));
        }
    }
    Ok(lanes)
}

/// Adds the application-owned default lane when it was not explicitly overridden.
fn insert_default_lane(
    lanes: &mut BTreeMap<TaskLane, TaskLaneConf>,
    concurrency: usize,
) -> Result<(), TaskRuntimeError> {
    if lanes.contains_key(&DEFAULT_TASK_LANE) {
        return Ok(());
    }
    let used = lanes.values().try_fold(0_usize, |sum, lane| {
        sum.checked_add(lane.concurrency()).ok_or_else(|| {
            TaskRuntimeError::InvalidConfig("task lane concurrency overflowed".into())
        })
    })?;
    let remaining = concurrency.checked_sub(used).ok_or_else(|| {
        TaskRuntimeError::InvalidConfig(
            "task lane concurrency exceeds global task concurrency".into(),
        )
    })?;
    if remaining == 0 {
        return Err(TaskRuntimeError::InvalidConfig(
            "named task lanes leave no capacity for the default lane".into(),
        ));
    }
    lanes.insert(
        DEFAULT_TASK_LANE,
        TaskLaneConf::new(DEFAULT_TASK_LANE, remaining),
    );
    Ok(())
}

/// Rejects scalar limits that could disable progress or exceed bounded policy.
fn validate_scalars(conf: &TaskConf) -> Result<(), TaskRuntimeError> {
    if conf.poll_interval.is_zero() || conf.fallback_poll_interval < conf.poll_interval {
        return Err(TaskRuntimeError::InvalidConfig(
            "task fallback interval must be at least the non-zero poll interval".into(),
        ));
    }
    if conf.max_all_children == 0 || conf.max_all_children > MAX_BATCH_SIZE {
        return Err(TaskRuntimeError::InvalidConfig(
            "maximum all children must be in 1..=10,000".into(),
        ));
    }
    if conf.concurrency == 0 || conf.batch_size == 0 {
        return Err(TaskRuntimeError::InvalidConfig(
            "task concurrency and batch size must be non-zero".into(),
        ));
    }
    if conf.concurrency > MAX_CONCURRENCY || conf.batch_size > MAX_BATCH_SIZE {
        return Err(TaskRuntimeError::InvalidConfig(format!(
            "task concurrency cannot exceed {MAX_CONCURRENCY} and batch size cannot exceed {MAX_BATCH_SIZE}"
        )));
    }
    if conf.poll_interval > MAX_INTERVAL || conf.fallback_poll_interval > MAX_INTERVAL {
        return Err(TaskRuntimeError::InvalidConfig(
            "task polling intervals cannot exceed seven days".into(),
        ));
    }
    if conf.lease_duration.is_zero() || conf.max_payload_bytes == 0 || conf.max_error_bytes == 0 {
        return Err(TaskRuntimeError::InvalidConfig(
            "task lease and payload limits must be non-zero".into(),
        ));
    }
    let minimum_lease = conf.poll_interval.saturating_mul(3);
    if conf.lease_duration < minimum_lease {
        return Err(TaskRuntimeError::InvalidConfig(
            "task lease duration must be at least three poll intervals".into(),
        ));
    }
    if conf.lease_duration > MAX_LEASE
        || conf.max_payload_bytes > MAX_PAYLOAD_BYTES
        || conf.max_error_bytes > MAX_ERROR_BYTES
    {
        return Err(TaskRuntimeError::InvalidConfig(
            "task lease or persisted payload limits exceed framework bounds".into(),
        ));
    }
    Ok(())
}

/// Rejects readiness policies that could never transition deterministically.
fn validate_readiness(policy: TaskReadiness) -> Result<(), TaskRuntimeError> {
    if matches!(policy, TaskReadiness::AfterFailures(0)) {
        return Err(TaskRuntimeError::InvalidConfig(
            "task readiness failure threshold must be greater than zero".into(),
        ));
    }
    Ok(())
}

/// Validates the complete lane set against global concurrency and count limits.
fn validate_lanes(
    lanes: &[TaskLaneConf],
    concurrency: usize,
    batch_size: usize,
) -> Result<(), TaskRuntimeError> {
    if lanes.is_empty() || lanes.len() > MAX_TASK_LANES {
        return Err(TaskRuntimeError::InvalidConfig(format!(
            "task lanes must contain between 1 and {MAX_TASK_LANES} entries"
        )));
    }
    if !lanes.iter().any(|lane| lane.lane() == DEFAULT_TASK_LANE) {
        return Err(TaskRuntimeError::InvalidConfig(
            "explicit task lanes must include the default lane".into(),
        ));
    }
    let mut names = HashSet::with_capacity(lanes.len());
    let mut total = 0_usize;
    for lane in lanes {
        validate_lane(lane, &mut names)?;
        validate_lock(lane, batch_size)?;
        total = total.checked_add(lane.concurrency()).ok_or_else(|| {
            TaskRuntimeError::InvalidConfig("task lane concurrency overflowed".into())
        })?;
    }
    if total > concurrency {
        return Err(TaskRuntimeError::InvalidConfig(
            "task lane concurrency exceeds global task concurrency".into(),
        ));
    }
    Ok(())
}

/// Validates one optional durable lane-owner policy.
fn validate_lock(conf: &TaskLaneConf, maximum: usize) -> Result<(), TaskRuntimeError> {
    let Some(lane_lock) = conf.lane_lock() else {
        return Ok(());
    };
    let paired = lane_lock.idle_hook().is_some() == lane_lock.busy_hook().is_some();
    let deadline_valid = lane_lock
        .batch_deadline()
        .is_none_or(|duration| !duration.is_zero() && duration <= MAX_INTERVAL);
    if lane_lock.batch_size() == 0
        || lane_lock.batch_size() > maximum
        || !deadline_valid
        || lane_lock.idle_duration() > MAX_INTERVAL
        || !paired
    {
        return Err(TaskRuntimeError::InvalidConfig(format!(
            "task lane '{}' has an invalid lane lock policy",
            conf.lane()
        )));
    }
    Ok(())
}

/// Validates one stable lane name, quota, and optional token-bucket policy.
fn validate_lane(
    conf: &TaskLaneConf,
    names: &mut HashSet<&'static str>,
) -> Result<(), TaskRuntimeError> {
    let name = conf.lane().as_str();
    let valid_name = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    if !valid_name || !names.insert(name) || conf.concurrency() == 0 {
        return Err(TaskRuntimeError::InvalidConfig(format!(
            "invalid or duplicate task lane '{name}'"
        )));
    }
    for (label, rate) in [("local", conf.rate()), ("global", conf.global_rate())] {
        validate_rate(name, label, rate)?;
    }
    let retry = conf.retry_policy();
    if retry.max_attempts() == 0
        || retry.max_attempts() > MAX_TASK_ATTEMPTS
        || retry.initial_delay().is_zero()
        || retry.maximum_delay() < retry.initial_delay()
        || retry.maximum_delay() > MAX_INTERVAL
    {
        return Err(TaskRuntimeError::InvalidConfig(format!(
            "task lane '{name}' has an invalid retry policy"
        )));
    }
    if let IdempotencyRetention::RetainFor(duration) = conf.idempotency_policy()
        && (duration.is_zero() || duration > MAX_IDEMPOTENCY_RETENTION)
    {
        return Err(TaskRuntimeError::InvalidConfig(format!(
            "task lane '{name}' has an invalid idempotency retention"
        )));
    }
    Ok(())
}

fn validate_rate(name: &str, label: &str, rate: Option<TaskRate>) -> Result<(), TaskRuntimeError> {
    let Some(rate) = rate else { return Ok(()) };
    let period_nanos = rate.period().as_nanos();
    if rate.permits() == 0
        || rate.burst_size() == 0
        || rate.period().as_micros() == 0
        || period_nanos > u128::from(u64::MAX)
    {
        return Err(TaskRuntimeError::InvalidConfig(format!(
            "task lane '{name}' has an invalid {label} rate limit"
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/config.rs"]
mod tests;
