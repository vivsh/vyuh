//! Reference-store deadlines, validation, and initialization helpers.
use super::*;

pub(super) fn checked_deadline(
    now: chrono::DateTime<chrono::Utc>,
    delay: Duration,
) -> Result<Option<chrono::DateTime<chrono::Utc>>, TaskError> {
    now.checked_add_signed(chrono_duration(delay)?)
        .map(Some)
        .ok_or_else(|| TaskError::InvalidConfig("task delay exceeds timestamp range".into()))
}

/// Releases or archives a key when its task reaches a terminal status.
pub(super) fn finalize_idempotency(
    task: &mut TaskRecord,
    conf: &TaskStoreConf,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<(), TaskError> {
    if !matches!(task.status, TaskStatus::Succeeded | TaskStatus::Failed) {
        return Ok(());
    }
    if task.idempotency_key.is_none() {
        return Ok(());
    }
    let policy = conf.idempotency_for(&task.name).ok_or_else(|| {
        TaskError::InvalidConfig(format!("task '{}' has no idempotency policy", task.name))
    })?;
    task.idempotency_expires_at = match policy {
        IdempotencyRetention::ActiveOnly => None,
        IdempotencyRetention::RetainFor(duration) => checked_deadline(now, duration)?,
    };
    Ok(())
}

/// Selects a task only while the committing runner still owns its lease.
pub(super) fn owned_task_mut<'a>(
    tasks: &'a mut [TaskRecord],
    id: TaskId,
    runner_id: &str,
) -> Option<&'a mut TaskRecord> {
    tasks.iter_mut().find(|task| {
        task.id == id
            && task.status == TaskStatus::Running
            && task.locked_by.as_deref() == Some(runner_id)
    })
}

/// Returns the earliest future readiness or lease-expiry deadline.
pub(super) fn task_deadline(
    tasks: &[TaskRecord],
    lane: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<Duration> {
    tasks
        .iter()
        .filter(|task| task.lane == lane)
        .filter_map(|task| {
            let deadline = match task.status {
                TaskStatus::Pending => task.ready_at,
                TaskStatus::Running => task.leased_until,
                _ => None,
            }?;
            (deadline > now)
                .then(|| (deadline - now).to_std().ok())
                .flatten()
        })
        .min()
}

/// Combines future work and token readiness without polling a blocked lane early.
pub(super) fn effective_lane_wake(
    rate_blocked: bool,
    rate_wake: Option<Duration>,
    task_wake: Option<Duration>,
) -> Option<Duration> {
    if rate_blocked {
        return rate_wake;
    }
    task_wake.map(|task| rate_wake.map_or(task, |permit| permit.max(task)))
}

/// Applies every bounded console and inspection filter to one record.
pub(super) fn matches_filter(task: &TaskRecord, filter: &TaskFilter) -> bool {
    filter.status.is_none_or(|status| task.status == status)
        && filter.name.as_deref().is_none_or(|name| task.name == name)
        && filter.lane.as_deref().is_none_or(|lane| task.lane == lane)
        && filter
            .idempotency_key
            .as_deref()
            .is_none_or(|key| task.idempotency_key.as_deref() == Some(key))
        && filter
            .created_from
            .is_none_or(|time| task.created_at >= time)
        && filter.created_to.is_none_or(|time| task.created_at <= time)
        && matches_query(task, filter.query.as_deref())
}

/// Performs the in-memory store's case-insensitive diagnostic search.
pub(super) fn matches_query(task: &TaskRecord, query: Option<&str>) -> bool {
    let Some(query) = query else { return true };
    let query = query.to_lowercase();
    task.name.to_lowercase().contains(&query)
        || task.lane.to_lowercase().contains(&query)
        || task
            .idempotency_key
            .as_ref()
            .is_some_and(|value| value.to_lowercase().contains(&query))
}

/// Builds the canonical one-indexed task inspection page.
pub(super) fn page(
    records: Vec<TaskRecord>,
    filter: &TaskFilter,
) -> crate::routes::Page<TaskRecord> {
    let total = i64::try_from(records.len()).unwrap_or(i64::MAX);
    let offset = filter
        .page
        .saturating_sub(1)
        .saturating_mul(filter.per_page);
    let items = records
        .into_iter()
        .skip(offset)
        .take(filter.per_page)
        .collect();
    crate::routes::Page::new(items, total, filter.page, filter.per_page)
}

/// Creates buckets only for lanes with configured global rate limits.
pub(super) fn initialize_rates(state: &mut MemoryState, conf: &TaskStoreConf) {
    let now = chrono::Utc::now();
    for lane in &conf.lanes {
        if let Some(rate) = lane.global_rate() {
            state
                .rates
                .entry(lane.lane().to_string())
                .or_insert(RateBucket {
                    tokens_micros: i64::from(rate.burst_size())
                        .saturating_mul(crate::tasks::rate::TOKEN_SCALE),
                    updated_at: now,
                });
        }
    }
}

/// Adds missing in-memory lane-owner rows without resetting current lifecycle state.
pub(super) fn initialize_lane_locks(state: &mut MemoryState, conf: &TaskStoreConf) {
    for lane in conf.lanes.iter().filter(|lane| lane.lane_lock().is_some()) {
        state
            .lane_locks
            .entry(lane.lane().to_string())
            .or_insert(MemoryLaneLock {
                owner_id: None,
                owner_token: None,
                leased_until: None,
                phase: LaneOwnerPhase::Active,
                flushing: false,
                empty_since: None,
                generation: 0,
                hook_retry_at: None,
                last_hook_error: None,
            });
    }
}

/// Prevents active work from silently falling into another configured lane.
pub(super) fn reject_orphaned_tasks(
    tasks: &[TaskRecord],
    conf: &TaskStoreConf,
) -> Result<(), TaskError> {
    let configured = conf
        .lanes
        .iter()
        .map(|lane| lane.lane().as_str())
        .collect::<std::collections::HashSet<_>>();
    if let Some(task) = tasks
        .iter()
        .find(|task| is_active(task.status) && !configured.contains(task.lane.as_str()))
    {
        return Err(TaskError::UnknownLane(task.lane.clone()));
    }
    let handlers = conf
        .handlers
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    if let Some(task) = tasks
        .iter()
        .find(|task| is_active(task.status) && !handlers.contains(task.name.as_str()))
    {
        return Err(TaskError::TaskNotFound(task.name.clone()));
    }
    Ok(())
}

pub(super) fn is_active(status: TaskStatus) -> bool {
    matches!(
        status,
        TaskStatus::Pending | TaskStatus::Running | TaskStatus::Suspended
    )
}

pub(super) fn is_reassignable(status: TaskStatus) -> bool {
    matches!(status, TaskStatus::Pending | TaskStatus::Suspended)
}

pub(super) fn chrono_duration(duration: Duration) -> Result<chrono::Duration, TaskError> {
    chrono::Duration::from_std(duration).map_err(|_| {
        TaskError::InvalidConfig("task duration exceeds supported timestamp range".into())
    })
}
