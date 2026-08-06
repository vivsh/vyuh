//! Read-only projections for durable task-targeted emitter schedules.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Site, emitters::next_task_schedule, tasks::TaskError};

/// Bounded filters accepted by the console schedule views.
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub(crate) struct ScheduleQuery {
    pub source: Option<String>,
    pub task: Option<String>,
    pub lane: Option<String>,
    pub q: Option<String>,
    pub awaiting_first_run: Option<bool>,
    pub selected: Option<String>,
    pub page: Option<usize>,
    pub per_page: Option<usize>,
}

/// One durable task schedule with its immutable definition and cursor state.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(crate) struct ScheduleOut {
    pub name: String,
    pub task: String,
    pub source: String,
    pub expression: String,
    pub start: String,
    pub lane: String,
    pub last_submitted_at: Option<String>,
    pub next_expected_at: Option<String>,
}

/// One page of configured schedules and stable summary counts.
#[derive(Debug, Serialize, JsonSchema)]
pub(crate) struct SchedulePage {
    pub items: Vec<ScheduleOut>,
    pub tasks: Vec<String>,
    pub lanes: Vec<String>,
    pub total: usize,
    pub page: usize,
    pub total_pages: usize,
    pub configured: usize,
    pub cron: usize,
    pub periodic: usize,
    pub awaiting_first_run: usize,
}

/// Loads immutable schedule definitions with their durable submission cursors.
pub(crate) async fn page(
    site: &Site,
    query: &ScheduleQuery,
    default_size: usize,
    max_size: usize,
) -> Result<SchedulePage, TaskError> {
    let names = schedule_names(site);
    let snapshot = schedule_snapshot(site, &names).await?;
    let mut schedules = schedule_entries(site, &snapshot.cursors, snapshot.now)?;
    schedules.sort_by(|left, right| left.name.cmp(&right.name));
    let stats = ScheduleStats::from_entries(&schedules);
    let tasks = schedule_values(&schedules, |schedule| &schedule.task);
    let lanes = schedule_values(&schedules, |schedule| &schedule.lane);
    schedules.retain(|schedule| matches_query(schedule, query));
    Ok(paginate(
        schedules,
        query,
        default_size,
        max_size,
        stats,
        tasks,
        lanes,
    ))
}

/// Collects schedule cursor names once before a single batched store lookup.
fn schedule_names(site: &Site) -> Vec<String> {
    site.tasks()
        .schedule_configs()
        .iter()
        .map(|schedule| schedule.name.clone())
        .collect()
}

/// Uses the store clock only when one configured cursor can exist.
async fn schedule_snapshot(
    site: &Site,
    names: &[String],
) -> Result<crate::tasks::TaskScheduleSnapshot, TaskError> {
    if names.is_empty() {
        return Ok(crate::tasks::TaskScheduleSnapshot {
            now: Utc::now(),
            cursors: HashMap::new(),
        });
    }
    site.tasks().schedule_snapshot(names).await
}

/// Builds console-owned values without retaining scheduler worker state.
fn schedule_entries(
    site: &Site,
    cursors: &HashMap<String, DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<Vec<ScheduleOut>, TaskError> {
    site.tasks()
        .schedule_configs()
        .iter()
        .map(|schedule| schedule_entry(site, schedule, cursors, now))
        .collect()
}

/// Maps one finalized definition and optional durable cursor into a safe view.
fn schedule_entry(
    site: &Site,
    schedule: &crate::tasks::TaskScheduleConf,
    cursors: &HashMap<String, DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<ScheduleOut, TaskError> {
    let last_submitted_at = cursors.get(&schedule.name).copied();
    let next_expected_at = next_expected(schedule, last_submitted_at, now)?;
    let lane = site
        .tasks()
        .task_lane(&schedule.task)
        .map(str::to_owned)
        .ok_or_else(|| {
            TaskError::InvalidConfig("task schedule target has no finalized lane".into())
        })?;
    Ok(ScheduleOut {
        name: schedule.name.clone(),
        task: schedule.task.clone(),
        source: schedule.source.clone(),
        expression: schedule.expression.clone(),
        start: schedule.start.clone(),
        lane,
        last_submitted_at: last_submitted_at.map(|time| time.to_rfc3339()),
        next_expected_at: Some(next_expected_at.to_rfc3339()),
    })
}

/// Preserves an immediate first-start policy instead of disguising it as a normal slot.
fn next_expected(
    schedule: &crate::tasks::TaskScheduleConf,
    last_submitted_at: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<DateTime<Utc>, TaskError> {
    if last_submitted_at.is_none() && schedule.start == "immediately" {
        return Ok(now);
    }
    next_task_schedule(schedule, last_submitted_at.unwrap_or(now))
        .map_err(|error| TaskError::InvalidConfig(error.to_string()))
}

/// Keeps schedule filtering in memory because definitions are immutable site metadata.
fn matches_query(schedule: &ScheduleOut, query: &ScheduleQuery) -> bool {
    source_matches(schedule, query.source.as_deref())
        && option_matches(&schedule.task, query.task.as_deref())
        && option_matches(&schedule.lane, query.lane.as_deref())
        && query
            .awaiting_first_run
            .is_none_or(|awaiting| awaiting == schedule.last_submitted_at.is_none())
        && text_matches(schedule, query.q.as_deref())
}

/// Matches a known source family without accepting a partial source name.
fn source_matches(schedule: &ScheduleOut, source: Option<&str>) -> bool {
    source.is_none_or(|source| schedule.source == source)
}

/// Matches one exact task or lane value when a filter is supplied.
fn option_matches(value: &str, filter: Option<&str>) -> bool {
    filter.is_none_or(|filter| value == filter)
}

/// Matches free text across the small immutable schedule definition.
fn text_matches(schedule: &ScheduleOut, query: Option<&str>) -> bool {
    let Some(query) = query else {
        return true;
    };
    let query = query.to_lowercase();
    [
        &schedule.name,
        &schedule.task,
        &schedule.source,
        &schedule.lane,
    ]
    .into_iter()
    .any(|value| value.to_lowercase().contains(&query))
}

/// Produces deterministic unique selector values from all site schedule definitions.
fn schedule_values(schedules: &[ScheduleOut], value: impl Fn(&ScheduleOut) -> &str) -> Vec<String> {
    let mut values = schedules
        .iter()
        .map(|schedule| value(schedule).to_owned())
        .collect::<Vec<_>>();
    values.sort_unstable();
    values.dedup();
    values
}

/// Applies the console's bounded ordinary page contract to schedule metadata.
fn paginate(
    schedules: Vec<ScheduleOut>,
    query: &ScheduleQuery,
    default_size: usize,
    max_size: usize,
    stats: ScheduleStats,
    tasks: Vec<String>,
    lanes: Vec<String>,
) -> SchedulePage {
    let per_page = query
        .per_page
        .unwrap_or(default_size)
        .clamp(1, max_size.clamp(1, 100));
    let total = schedules.len();
    let total_pages = total.div_ceil(per_page);
    let page = query.page.unwrap_or(1).clamp(1, total_pages.max(1));
    let start = (page - 1).saturating_mul(per_page);
    let items = schedules.into_iter().skip(start).take(per_page).collect();
    SchedulePage {
        items,
        tasks,
        lanes,
        total,
        page,
        total_pages,
        configured: stats.configured,
        cron: stats.cron,
        periodic: stats.periodic,
        awaiting_first_run: stats.awaiting_first_run,
    }
}

#[derive(Clone, Copy)]
struct ScheduleStats {
    configured: usize,
    cron: usize,
    periodic: usize,
    awaiting_first_run: usize,
}

impl ScheduleStats {
    /// Summarizes immutable definitions and cursor availability before filtering.
    fn from_entries(entries: &[ScheduleOut]) -> Self {
        Self {
            configured: entries.len(),
            cron: entries
                .iter()
                .filter(|entry| entry.source == "cron")
                .count(),
            periodic: entries
                .iter()
                .filter(|entry| entry.source == "periodic")
                .count(),
            awaiting_first_run: entries
                .iter()
                .filter(|entry| entry.last_submitted_at.is_none())
                .count(),
        }
    }
}
