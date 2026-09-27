use super::*;

/// All limits are finalized independently of the ordinary store batch size.
#[test]
fn all_limit_bounds() {
    for size in [1, 256, 10_000] {
        assert!(
            TaskConf::default()
                .batch_size(1)
                .max_all_children(size)
                .validate()
                .is_ok()
        );
    }
    for size in [0, 10_001] {
        assert!(
            TaskConf::default()
                .max_all_children(size)
                .validate()
                .is_err()
        );
    }
    assert_eq!(TaskConf::default().all_limit(), 256);
}
use crate::tasks::{TaskLaneContext, TaskLaneLock};

const FAST: TaskLane = TaskLane::new("fast");
const SLOW: TaskLane = TaskLane::new("slow");

async fn lane_hook(_lane: TaskLaneContext) -> Result<(), crate::Error> {
    Ok(())
}

/// Verifies lane-lock builders accept typed callable extraction and bounded policy values.
#[test]
fn task_config_validates_lane_lock_policy() {
    let valid = TaskConf::default().lane(
        TaskLaneConf::new(FAST, 1).lock(
            TaskLaneLock::new(8)
                .deadline(Duration::from_secs(2))
                .idle_after(Duration::from_secs(30))
                .on_idle(lane_hook)
                .on_busy(lane_hook),
        ),
    );
    assert!(valid.validate().is_ok());

    let unpaired = TaskConf::default()
        .lane(TaskLaneConf::new(FAST, 1).lock(TaskLaneLock::new(8).on_idle(lane_hook)));
    assert!(matches!(
        unpaired.validate(),
        Err(TaskRuntimeError::InvalidConfig(_))
    ));

    let oversized = TaskConf::default()
        .batch_size(1)
        .lane(TaskLaneConf::new(FAST, 1).lock(TaskLaneLock::new(2)));
    assert!(matches!(
        oversized.validate(),
        Err(TaskRuntimeError::InvalidConfig(_))
    ));

    let zero_size = TaskConf::default().lane(TaskLaneConf::new(FAST, 1).lock(TaskLaneLock::new(0)));
    assert!(matches!(
        zero_size.validate(),
        Err(TaskRuntimeError::InvalidConfig(_))
    ));

    let zero_deadline = TaskConf::default()
        .lane(TaskLaneConf::new(FAST, 1).lock(TaskLaneLock::new(1).deadline(Duration::ZERO)));
    assert!(matches!(
        zero_deadline.validate(),
        Err(TaskRuntimeError::InvalidConfig(_))
    ));

    let long_idle = TaskConf::default().lane(
        TaskLaneConf::new(FAST, 1)
            .lock(TaskLaneLock::new(1).idle_after(MAX_INTERVAL + Duration::from_secs(1))),
    );
    assert!(matches!(
        long_idle.validate(),
        Err(TaskRuntimeError::InvalidConfig(_))
    ));
}

/// Verifies adaptive polling defaults use one-second backlog and five-minute fallback checks.
#[test]
fn task_config_uses_adaptive_polling_defaults() {
    let conf = TaskConf::default();
    assert_eq!(conf.poll_interval_value(), Duration::from_secs(1));
    assert_eq!(conf.fallback_interval(), Duration::from_secs(300));
    assert!(matches!(conf.validate(), Ok(())));
    assert_eq!(
        conf.resolve_lanes(std::iter::empty())
            .ok()
            .map(|lanes| lanes.len()),
        Some(1)
    );
}

/// Verifies explicit lane quotas may isolate work without exceeding global concurrency.
#[test]
fn task_config_accepts_bounded_named_lanes() {
    let conf = TaskConf::default()
        .concurrency(4)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 0))
        .lane(TaskLaneConf::new(FAST, 3))
        .lane(TaskLaneConf::new(SLOW, 1).rate_limit(TaskRate::per_minute(60).burst(4)));
    assert!(matches!(
        conf.validate(),
        Err(TaskRuntimeError::InvalidConfig(_))
    ));

    let conf = TaskConf::default()
        .concurrency(4)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 1))
        .lane(TaskLaneConf::new(FAST, 2))
        .lane(TaskLaneConf::new(SLOW, 1).rate_limit(TaskRate::per_minute(60).burst(4)));
    assert_eq!(
        conf.resolve_lanes(std::iter::empty())
            .ok()
            .map(|lanes| lanes.len()),
        Some(3)
    );
}

/// Verifies local and shared-store rate limits remain independently composable.
#[test]
fn task_lane_keeps_local_and_global_rates_distinct() {
    let local = TaskRate::per_second(10).burst(2);
    let global = TaskRate::per_minute(100).burst(20);
    let lane = TaskLaneConf::new(FAST, 1)
        .rate_limit(local)
        .global_rate_limit(global);
    assert_eq!(lane.rate(), Some(local));
    assert_eq!(lane.global_rate(), Some(global));
}

/// Verifies duplicate names and overcommitted lane quotas fail terminal configuration validation.
#[test]
fn task_config_rejects_invalid_lane_sets() {
    let derived_default = TaskConf::default()
        .concurrency(1)
        .lane(TaskLaneConf::new(FAST, 1));
    assert!(matches!(
        derived_default.resolve_lanes(std::iter::empty()),
        Err(TaskRuntimeError::InvalidConfig(_))
    ));
    let duplicate = TaskConf::default()
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 1))
        .lane(TaskLaneConf::new(FAST, 1))
        .lane(TaskLaneConf::new(FAST, 1));
    assert!(matches!(
        duplicate.validate(),
        Err(TaskRuntimeError::InvalidConfig(_))
    ));
    let overcommitted = TaskConf::default()
        .concurrency(2)
        .lane(TaskLaneConf::new(DEFAULT_TASK_LANE, 1))
        .lane(TaskLaneConf::new(FAST, 1))
        .lane(TaskLaneConf::new(SLOW, 1));
    assert!(matches!(
        overcommitted.resolve_lanes(std::iter::empty()),
        Err(TaskRuntimeError::InvalidConfig(_))
    ));
}

/// Verifies the hard lane-count bound rejects accidental queue proliferation.
#[test]
fn task_config_rejects_more_than_thirty_two_lanes() {
    const NAMES: [&str; 33] = [
        "g00", "g01", "g02", "g03", "g04", "g05", "g06", "g07", "g08", "g09", "g10", "g11", "g12",
        "g13", "g14", "g15", "g16", "g17", "g18", "g19", "g20", "g21", "g22", "g23", "g24", "g25",
        "g26", "g27", "g28", "g29", "g30", "g31", "g32",
    ];
    let conf = NAMES
        .into_iter()
        .fold(TaskConf::default().concurrency(33), |conf, name| {
            conf.lane(TaskLaneConf::new(TaskLane::new(name), 1))
        });
    assert!(matches!(
        conf.validate(),
        Err(TaskRuntimeError::InvalidConfig(_))
    ));
}

/// Verifies infallible scalar builders defer invalid limits to terminal validation.
#[test]
fn task_config_accumulates_invalid_scalar_values() {
    let conf = TaskConf::default()
        .concurrency(0)
        .batch_size(0)
        .poll_interval(Duration::ZERO);
    assert!(matches!(
        conf.validate(),
        Err(TaskRuntimeError::InvalidConfig(_))
    ));
}

/// Verifies a lease leaves enough time for two missed paced ticks before recovery.
#[test]
fn task_config_requires_a_recoverable_lease() {
    let conf = TaskConf::default()
        .poll_interval(Duration::from_secs(2))
        .lease_duration(Duration::from_secs(5));
    assert!(matches!(
        conf.validate(),
        Err(TaskRuntimeError::InvalidConfig(_))
    ));
}

/// Verifies retry delays grow from the lane policy and stop at its configured cap.
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

    let long_growth =
        TaskRetry::exponential(100, Duration::from_nanos(1)).max_delay(Duration::from_secs(86_400));
    assert_eq!(
        long_growth.delay(60).ok(),
        Some(Duration::from_secs(86_400))
    );
}

/// Verifies malformed retry policies remain infallible until site configuration validation.
#[test]
fn task_config_rejects_invalid_lane_retry_policy() {
    let conf = TaskConf::default().lane(
        TaskLaneConf::new(DEFAULT_TASK_LANE, 1).retry(TaskRetry::exponential(0, Duration::ZERO)),
    );
    assert!(matches!(
        conf.validate(),
        Err(TaskRuntimeError::InvalidConfig(_))
    ));
}

/// Verifies a task readiness threshold must permit at least one failure.
#[test]
fn task_config_rejects_zero_readiness_threshold() {
    let conf = TaskConf::default().readiness(TaskReadiness::after_failures(0));

    assert!(matches!(
        conf.validate(),
        Err(TaskRuntimeError::InvalidConfig(_))
    ));
}

/// Verifies an application lane replaces a reusable bundle's complete default.
#[test]
fn application_lane_replaces_bundle_default() -> Result<(), TaskRuntimeError> {
    let conf = TaskConf::default()
        .concurrency(10)
        .lane(TaskLaneConf::new(FAST, 6));
    let lanes = conf.resolve_lanes([TaskLaneConf::new(FAST, 2)])?;
    let fast = lanes
        .iter()
        .find(|lane| lane.lane() == FAST)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(FAST.to_string()))?;
    let default = lanes
        .iter()
        .find(|lane| lane.lane() == DEFAULT_TASK_LANE)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(DEFAULT_TASK_LANE.to_string()))?;

    assert_eq!(fast.concurrency(), 6);
    assert_eq!(default.concurrency(), 4);
    Ok(())
}

/// Verifies independently contributed definitions cannot silently share a lane name.
#[test]
fn duplicate_bundle_lane_defaults_fail_resolution() {
    let result =
        TaskConf::default().resolve_lanes([TaskLaneConf::new(FAST, 1), TaskLaneConf::new(FAST, 1)]);

    assert!(matches!(result, Err(TaskRuntimeError::InvalidConfig(_))));
}

/// Verifies strict lane policy rejects a task declaration absent from finalized lanes.
#[test]
fn strict_missing_lane_policy_rejects_unconfigured_task_lane() -> Result<(), TaskRuntimeError> {
    let conf = TaskConf::default().missing_lane(TaskLanePolicy::RequireConfigured);
    let lanes = conf.resolve_lanes(std::iter::empty())?;

    assert!(matches!(
        conf.resolve_lane(&lanes, FAST),
        Err(TaskRuntimeError::UnknownLane(_))
    ));
    Ok(())
}
