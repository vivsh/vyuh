use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    Operation, OperationKind,
    tasks::{TaskFilter, TaskStatus},
};

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct InspectionQuery {
    pub q: Option<String>,
    pub selected: Option<String>,
    pub tag: Option<String>,
    pub owner: Option<String>,
    pub hidden: Option<bool>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct TaskQuery {
    pub status: Option<TaskStatus>,
    pub name: Option<String>,
    pub lane: Option<String>,
    pub idempotency_key: Option<String>,
    pub created_from: Option<String>,
    pub created_to: Option<String>,
    pub selected: Option<String>,
    pub q: Option<String>,
    pub page: Option<usize>,
    pub per_page: Option<usize>,
}

/// Bounded filters accepted by the console's file-log views.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct LogQuery {
    pub rule: Option<String>,
    pub level: Option<String>,
    pub target: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub q: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub selected: Option<String>,
}

impl TaskQuery {
    pub fn to_filter(&self, default_limit: usize, max_limit: usize) -> TaskFilter {
        let per_page = task_per_page(self, default_limit, max_limit);
        let page = self.page.unwrap_or(1).max(1);
        TaskFilter::new()
            .optional_status(self.status)
            .optional_name(self.name.clone())
            .lane_name(self.lane.clone())
            .optional_key(self.idempotency_key.clone())
            .optional_range(
                parse_start(self.created_from.as_deref()),
                parse_end(self.created_to.as_deref()),
            )
            .optional_search(self.q.clone())
            .page(page)
            .per_page(per_page)
    }
}

pub fn task_limit_max(configured_max: usize) -> usize {
    configured_max.min(100)
}

pub fn task_per_page(query: &TaskQuery, default_limit: usize, configured_max: usize) -> usize {
    query
        .per_page
        .unwrap_or(default_limit)
        .clamp(1, task_limit_max(configured_max))
}

fn parse_start(value: Option<&str>) -> Option<chrono::DateTime<chrono::Utc>> {
    parse_date(value, 0, 0, 0)
}

fn parse_end(value: Option<&str>) -> Option<chrono::DateTime<chrono::Utc>> {
    parse_date(value, 23, 59, 59)
}

fn parse_date(
    value: Option<&str>,
    hour: u32,
    min: u32,
    sec: u32,
) -> Option<chrono::DateTime<chrono::Utc>> {
    let date = chrono::NaiveDate::parse_from_str(value?, "%Y-%m-%d").ok()?;
    let time = date.and_hms_opt(hour, min, sec)?;
    Some(time.and_utc())
}

pub fn filter_inspections<'a>(
    operations: impl Iterator<Item = &'a Operation>,
    query: &InspectionQuery,
    console_bundle_id: Option<uuid::Uuid>,
    default_limit: usize,
    max_limit: usize,
    matches_kind: fn(&OperationKind) -> bool,
) -> (Vec<&'a Operation>, Option<String>) {
    let offset = parse_cursor(query.cursor.as_deref());
    let limit = clamp_limit(query.limit, default_limit, max_limit);
    let q = query.q.as_deref().map(str::to_lowercase);
    let tag = query.tag.as_deref();
    let owner = query.owner.as_deref();

    let mut filtered = operations
        .filter(|op| !is_console_operation(op, console_bundle_id))
        .filter(|op| matches_kind(&op.kind))
        .filter(|op| query.hidden.is_none_or(|hidden| op.hidden == hidden))
        .filter(|op| owner.is_none_or(|owner| op.owner.as_deref() == Some(owner)))
        .filter(|op| tag.is_none_or(|tag| op.tags.iter().any(|candidate| candidate == tag)))
        .filter(|op| {
            q.as_ref().is_none_or(|q| {
                contains(&op.name, q)
                    || op.summary.as_ref().is_some_and(|value| contains(value, q))
                    || op
                        .description
                        .as_ref()
                        .is_some_and(|value| contains(value, q))
                    || contains(&op.path, q)
            })
        })
        .collect::<Vec<_>>();

    filtered.sort_by(|left, right| left.name.cmp(&right.name));

    let page = filtered
        .into_iter()
        .skip(offset)
        .take(limit + 1)
        .collect::<Vec<_>>();
    if page.len() > limit {
        (
            page.into_iter().take(limit).collect(),
            Some((offset + limit).to_string()),
        )
    } else {
        (page, None)
    }
}

pub fn is_console_operation(op: &Operation, console_bundle_id: Option<uuid::Uuid>) -> bool {
    console_bundle_id.is_some_and(|bundle_id| op.bundle_id == Some(bundle_id))
}

pub fn clamp_limit(limit: Option<usize>, default_limit: usize, max_limit: usize) -> usize {
    limit.unwrap_or(default_limit).clamp(1, max_limit.max(1))
}

pub fn parse_cursor(cursor: Option<&str>) -> usize {
    cursor
        .and_then(|cursor| cursor.parse::<usize>().ok())
        .unwrap_or(0)
}

fn contains(value: &str, needle_lower: &str) -> bool {
    value.to_lowercase().contains(needle_lower)
}
