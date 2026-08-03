//! Task runner, group, rate-limit, and idempotency configuration.

use std::time::Duration;

use super::TaskError;

/// Framework-owned group used when a submission does not select one.
pub const DEFAULT_TASK_GROUP: TaskGroup = TaskGroup::new("default");

const MAX_TASK_GROUPS: usize = 32;
const MAX_TASK_ATTEMPTS: u32 = 1_000;
const MAX_CONCURRENCY: usize = 4_096;
const MAX_BATCH_SIZE: usize = 10_000;
const MAX_INTERVAL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
const MAX_LEASE: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
const MAX_ERROR_BYTES: usize = 64 * 1024;
pub(crate) const MAX_TASK_DELAY: Duration = Duration::from_secs(10 * 365 * 24 * 60 * 60);
const DEFAULT_RETRY: TaskRetry = TaskRetry {
    max_attempts: 5,
    initial_delay: Duration::from_secs(1),
    max_delay: Duration::from_secs(5 * 60),
};

/// Stable name for one independently scheduled task lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskGroup(&'static str);

impl TaskGroup {
    /// Declares a reusable task group descriptor.
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// Returns the configured group name.
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl std::fmt::Display for TaskGroup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

/// Token-bucket start rate for one task group.
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

/// Group-owned retry limit and exponential-backoff policy.
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

    /// Reports whether the completed invocation consumed the group's attempt budget.
    pub(crate) fn exhausted(self, attempts: i32) -> Result<bool, TaskError> {
        let attempts = u32::try_from(attempts).map_err(|_| {
            TaskError::TaskExecutionError("task attempt count cannot be negative".into())
        })?;
        Ok(attempts >= self.max_attempts)
    }

    /// Calculates the bounded exponential delay after the completed invocation.
    pub(crate) fn delay(self, attempts: i32) -> Result<Duration, TaskError> {
        let attempts = u32::try_from(attempts).map_err(|_| {
            TaskError::TaskExecutionError("task attempt count cannot be negative".into())
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

/// Runtime limits for one named task group.
#[derive(Debug, Clone)]
pub struct TaskGroupConf {
    group: TaskGroup,
    concurrency: usize,
    rate: Option<TaskRate>,
    global_rate: Option<TaskRate>,
    retry: TaskRetry,
}

impl TaskGroupConf {
    /// Creates one group with its per-worker concurrency quota.
    pub const fn new(group: TaskGroup, concurrency: usize) -> Self {
        Self {
            group,
            concurrency,
            rate: None,
            global_rate: None,
            retry: DEFAULT_RETRY,
        }
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

    /// Replaces this group's retry limit and exponential-backoff policy.
    pub const fn retry(mut self, retry: TaskRetry) -> Self {
        self.retry = retry;
        self
    }

    /// Returns the group descriptor.
    pub const fn group(&self) -> TaskGroup {
        self.group
    }

    /// Returns this worker's concurrency quota.
    pub const fn concurrency(&self) -> usize {
        self.concurrency
    }

    /// Returns the optional local runner rate.
    pub const fn rate(&self) -> Option<TaskRate> {
        self.rate
    }

    /// Returns the optional shared-store rate.
    pub const fn global_rate(&self) -> Option<TaskRate> {
        self.global_rate
    }

    /// Returns this group's retry policy.
    pub const fn retry_policy(&self) -> TaskRetry {
        self.retry
    }
}

/// Retention policy for submitted idempotency keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TaskIdempotency {
    /// Holds a key while its task remains non-terminal.
    #[default]
    ActiveOnly,
    /// Holds a key for the configured age after terminal completion.
    RetainFor(Duration),
}

impl TaskIdempotency {
    /// Releases keys when tasks become terminal.
    pub const fn active_only() -> Self {
        Self::ActiveOnly
    }

    /// Retains terminal keys for the supplied age after completion.
    pub const fn retain_for(duration: Duration) -> Self {
        Self::RetainFor(duration)
    }
}

/// Runtime policy for durable task workers.
#[derive(Debug, Clone)]
pub struct TaskConf {
    poll_interval: Duration,
    fallback_poll_interval: Duration,
    concurrency: usize,
    batch_size: usize,
    lease_duration: Duration,
    max_payload_bytes: usize,
    max_error_bytes: usize,
    groups: Option<Vec<TaskGroupConf>>,
    idempotency: TaskIdempotency,
}

impl Default for TaskConf {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            fallback_poll_interval: Duration::from_secs(300),
            concurrency: 10,
            batch_size: 250,
            lease_duration: Duration::from_secs(300),
            max_payload_bytes: 1024 * 1024,
            max_error_bytes: 8 * 1024,
            groups: None,
            idempotency: TaskIdempotency::default(),
        }
    }
}

impl TaskConf {
    /// Sets the maximum number of handlers running in this process.
    pub const fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Sets the maximum rows accepted, claimed, or committed in one batch.
    pub const fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Sets the short delay used while a group remains saturated.
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

    /// Replaces the implicit default lane with an explicit complete group set.
    pub fn groups(mut self, groups: impl IntoIterator<Item = TaskGroupConf>) -> Self {
        self.groups = Some(groups.into_iter().collect());
        self
    }

    /// Sets the site-wide idempotency-key retention policy.
    pub const fn idempotency(mut self, policy: TaskIdempotency) -> Self {
        self.idempotency = policy;
        self
    }

    /// Resolves and validates all task groups at site construction.
    pub(crate) fn validate(&self) -> Result<Vec<TaskGroupConf>, TaskError> {
        validate_scalars(self)?;
        let groups = self.resolved_groups();
        validate_groups(&groups, self.concurrency)?;
        Ok(groups)
    }

    pub(crate) fn resolved_groups(&self) -> Vec<TaskGroupConf> {
        self.groups
            .clone()
            .unwrap_or_else(|| vec![TaskGroupConf::new(DEFAULT_TASK_GROUP, self.concurrency)])
    }

    pub(crate) fn has_group(&self, group: TaskGroup) -> bool {
        self.groups
            .as_ref()
            .map_or(group == DEFAULT_TASK_GROUP, |groups| {
                groups.iter().any(|entry| entry.group() == group)
            })
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

    pub(crate) const fn idempotency_value(&self) -> TaskIdempotency {
        self.idempotency
    }
}

/// Rejects scalar limits that could disable progress or exceed bounded policy.
fn validate_scalars(conf: &TaskConf) -> Result<(), TaskError> {
    if conf.poll_interval.is_zero() || conf.fallback_poll_interval < conf.poll_interval {
        return Err(TaskError::InvalidConfig(
            "task fallback interval must be at least the non-zero poll interval".into(),
        ));
    }
    if conf.concurrency == 0 || conf.batch_size == 0 {
        return Err(TaskError::InvalidConfig(
            "task concurrency and batch size must be non-zero".into(),
        ));
    }
    if conf.concurrency > MAX_CONCURRENCY || conf.batch_size > MAX_BATCH_SIZE {
        return Err(TaskError::InvalidConfig(format!(
            "task concurrency cannot exceed {MAX_CONCURRENCY} and batch size cannot exceed {MAX_BATCH_SIZE}"
        )));
    }
    if conf.poll_interval > MAX_INTERVAL || conf.fallback_poll_interval > MAX_INTERVAL {
        return Err(TaskError::InvalidConfig(
            "task polling intervals cannot exceed seven days".into(),
        ));
    }
    if conf.lease_duration.is_zero() || conf.max_payload_bytes == 0 || conf.max_error_bytes == 0 {
        return Err(TaskError::InvalidConfig(
            "task lease and payload limits must be non-zero".into(),
        ));
    }
    if conf.lease_duration > MAX_LEASE
        || conf.max_payload_bytes > MAX_PAYLOAD_BYTES
        || conf.max_error_bytes > MAX_ERROR_BYTES
    {
        return Err(TaskError::InvalidConfig(
            "task lease or persisted payload limits exceed framework bounds".into(),
        ));
    }
    if matches!(conf.idempotency, TaskIdempotency::RetainFor(duration) if duration.is_zero()) {
        return Err(TaskError::InvalidConfig(
            "task idempotency retention must be non-zero".into(),
        ));
    }
    Ok(())
}

/// Validates the complete lane set against global concurrency and count limits.
fn validate_groups(groups: &[TaskGroupConf], concurrency: usize) -> Result<(), TaskError> {
    if groups.is_empty() || groups.len() > MAX_TASK_GROUPS {
        return Err(TaskError::InvalidConfig(format!(
            "task groups must contain between 1 and {MAX_TASK_GROUPS} entries"
        )));
    }
    if !groups
        .iter()
        .any(|group| group.group() == DEFAULT_TASK_GROUP)
    {
        return Err(TaskError::InvalidConfig(
            "explicit task groups must include the default group".into(),
        ));
    }
    let mut names = std::collections::HashSet::with_capacity(groups.len());
    let mut total = 0_usize;
    for group in groups {
        validate_group(group, &mut names)?;
        total = total
            .checked_add(group.concurrency())
            .ok_or_else(|| TaskError::InvalidConfig("task group concurrency overflowed".into()))?;
    }
    if total > concurrency {
        return Err(TaskError::InvalidConfig(
            "task group concurrency exceeds global task concurrency".into(),
        ));
    }
    Ok(())
}

/// Validates one stable group name, quota, and optional token-bucket policy.
fn validate_group(
    conf: &TaskGroupConf,
    names: &mut std::collections::HashSet<&'static str>,
) -> Result<(), TaskError> {
    let name = conf.group().as_str();
    let valid_name = !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-');
    if !valid_name || !names.insert(name) || conf.concurrency() == 0 {
        return Err(TaskError::InvalidConfig(format!(
            "invalid or duplicate task group '{name}'"
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
        return Err(TaskError::InvalidConfig(format!(
            "task group '{name}' has an invalid retry policy"
        )));
    }
    Ok(())
}

fn validate_rate(name: &str, label: &str, rate: Option<TaskRate>) -> Result<(), TaskError> {
    let Some(rate) = rate else { return Ok(()) };
    let period_nanos = rate.period().as_nanos();
    if rate.permits() == 0
        || rate.burst_size() == 0
        || rate.period().as_micros() == 0
        || period_nanos > u128::from(u64::MAX)
    {
        return Err(TaskError::InvalidConfig(format!(
            "task group '{name}' has an invalid {label} rate limit"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const FAST: TaskGroup = TaskGroup::new("fast");
    const SLOW: TaskGroup = TaskGroup::new("slow");

    /// Verifies adaptive polling defaults use one-second backlog and five-minute fallback checks.
    #[test]
    fn task_config_uses_adaptive_polling_defaults() {
        let conf = TaskConf::default();
        assert_eq!(conf.poll_interval_value(), Duration::from_secs(1));
        assert_eq!(conf.fallback_interval(), Duration::from_secs(300));
        assert!(matches!(conf.validate().map(|groups| groups.len()), Ok(1)));
    }

    /// Verifies explicit group quotas may isolate work without exceeding global concurrency.
    #[test]
    fn task_config_accepts_bounded_named_groups() {
        let conf = TaskConf::default().concurrency(4).groups([
            TaskGroupConf::new(DEFAULT_TASK_GROUP, 0),
            TaskGroupConf::new(FAST, 3),
            TaskGroupConf::new(SLOW, 1).rate_limit(TaskRate::per_minute(60).burst(4)),
        ]);
        assert!(matches!(conf.validate(), Err(TaskError::InvalidConfig(_))));

        let conf = TaskConf::default().concurrency(4).groups([
            TaskGroupConf::new(DEFAULT_TASK_GROUP, 1),
            TaskGroupConf::new(FAST, 2),
            TaskGroupConf::new(SLOW, 1).rate_limit(TaskRate::per_minute(60).burst(4)),
        ]);
        assert!(matches!(conf.validate().map(|groups| groups.len()), Ok(3)));
    }

    /// Verifies local and shared-store rate limits remain independently composable.
    #[test]
    fn task_group_keeps_local_and_global_rates_distinct() {
        let local = TaskRate::per_second(10).burst(2);
        let global = TaskRate::per_minute(100).burst(20);
        let group = TaskGroupConf::new(FAST, 1)
            .rate_limit(local)
            .global_rate_limit(global);
        assert_eq!(group.rate(), Some(local));
        assert_eq!(group.global_rate(), Some(global));
    }

    /// Verifies duplicate names and overcommitted group quotas fail terminal configuration validation.
    #[test]
    fn task_config_rejects_invalid_group_sets() {
        let missing_default = TaskConf::default()
            .concurrency(1)
            .groups([TaskGroupConf::new(FAST, 1)]);
        assert!(matches!(
            missing_default.validate(),
            Err(TaskError::InvalidConfig(_))
        ));
        let duplicate = TaskConf::default().groups([
            TaskGroupConf::new(DEFAULT_TASK_GROUP, 1),
            TaskGroupConf::new(FAST, 1),
            TaskGroupConf::new(FAST, 1),
        ]);
        assert!(matches!(
            duplicate.validate(),
            Err(TaskError::InvalidConfig(_))
        ));
        let overcommitted = TaskConf::default().concurrency(2).groups([
            TaskGroupConf::new(DEFAULT_TASK_GROUP, 1),
            TaskGroupConf::new(FAST, 1),
            TaskGroupConf::new(SLOW, 1),
        ]);
        assert!(matches!(
            overcommitted.validate(),
            Err(TaskError::InvalidConfig(_))
        ));
    }

    /// Verifies the hard group-count bound rejects accidental queue proliferation.
    #[test]
    fn task_config_rejects_more_than_thirty_two_groups() {
        const NAMES: [&str; 33] = [
            "g00", "g01", "g02", "g03", "g04", "g05", "g06", "g07", "g08", "g09", "g10", "g11",
            "g12", "g13", "g14", "g15", "g16", "g17", "g18", "g19", "g20", "g21", "g22", "g23",
            "g24", "g25", "g26", "g27", "g28", "g29", "g30", "g31", "g32",
        ];
        let groups = NAMES
            .into_iter()
            .map(|name| TaskGroupConf::new(TaskGroup::new(name), 1));
        let conf = TaskConf::default().concurrency(33).groups(groups);
        assert!(matches!(conf.validate(), Err(TaskError::InvalidConfig(_))));
    }

    /// Verifies infallible scalar builders defer invalid limits to terminal validation.
    #[test]
    fn task_config_accumulates_invalid_scalar_values() {
        let conf = TaskConf::default()
            .concurrency(0)
            .batch_size(0)
            .poll_interval(Duration::ZERO);
        assert!(matches!(conf.validate(), Err(TaskError::InvalidConfig(_))));
    }

    /// Verifies retry delays grow from the group policy and stop at its configured cap.
    #[test]
    fn task_retry_uses_bounded_exponential_backoff() {
        let retry =
            TaskRetry::exponential(5, Duration::from_secs(2)).max_delay(Duration::from_secs(10));
        assert_eq!(retry.delay(1).ok(), Some(Duration::from_secs(2)));
        assert_eq!(retry.delay(2).ok(), Some(Duration::from_secs(4)));
        assert_eq!(retry.delay(3).ok(), Some(Duration::from_secs(8)));
        assert_eq!(retry.delay(4).ok(), Some(Duration::from_secs(10)));
        assert_eq!(retry.delay(30).ok(), Some(Duration::from_secs(10)));
        assert!(matches!(retry.exhausted(4), Ok(false)));
        assert!(matches!(retry.exhausted(5), Ok(true)));

        let long_growth = TaskRetry::exponential(100, Duration::from_nanos(1))
            .max_delay(Duration::from_secs(86_400));
        assert_eq!(
            long_growth.delay(60).ok(),
            Some(Duration::from_secs(86_400))
        );
    }

    /// Verifies malformed retry policies remain infallible until site configuration validation.
    #[test]
    fn task_config_rejects_invalid_group_retry_policy() {
        let conf = TaskConf::default().groups([TaskGroupConf::new(DEFAULT_TASK_GROUP, 1)
            .retry(TaskRetry::exponential(0, Duration::ZERO))]);
        assert!(matches!(conf.validate(), Err(TaskError::InvalidConfig(_))));
    }
}
