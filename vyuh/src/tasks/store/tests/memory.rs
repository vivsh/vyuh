use super::*;
use crate::tasks::{DEFAULT_TASK_LANE, TaskKind, TaskLaneConf};

#[path = "pipeline.rs"]
mod pipeline;

pub(crate) fn record() -> TaskRecord {
    let now = chrono::Utc::now();
    TaskRecord {
        id: TaskId::new(uuid::Uuid::now_v7()),
        parent_id: None,
        root_id: None,
        kind: TaskKind::Work,
        name: "workflow".into(),
        input: "null".into(),
        state: None,
        resume_input: None,
        status: TaskStatus::Pending,
        cancelled: false,
        attempts: 0,
        step_attempts: 0,
        lane: DEFAULT_TASK_LANE.to_string(),
        last_result: None,
        idempotency_key: None,
        idempotency_fingerprint: None,
        idempotency_expires_at: None,
        node_id: None,
        ready_at: Some(now),
        created_at: now,
        updated_at: now,
        completed_at: None,
    }
}

pub(crate) fn write(record: TaskRecord) -> TaskWrite {
    TaskWrite {
        record,
        ignore_conflicts: false,
        initial_delay: None,
    }
}

/// Builds orchestration fixtures without changing ordinary work fixture defaults.
pub(crate) fn flow_record() -> TaskRecord {
    TaskRecord {
        name: "flow".into(),
        kind: TaskKind::Flow,
        ..record()
    }
}

pub(crate) fn conf() -> TaskStoreConf {
    TaskStoreConf {
        max_all_children: 256,
        handlers: vec![
            ("workflow".into(), TaskKind::Work),
            ("flow".into(), TaskKind::Flow),
        ],
        lanes: vec![TaskLaneConf::new(DEFAULT_TASK_LANE, 8)],
        idempotency: vec![crate::tasks::store::TaskIdempotencyConf {
            handler: "workflow".into(),
            lane: DEFAULT_TASK_LANE.to_string(),
            revision: "v1".into(),
            retention: crate::tasks::IdempotencyRetention::ActiveOnly,
        }],
        poll_interval: Duration::from_millis(10),
    }
}

pub(crate) fn claim() -> LaneClaim {
    LaneClaim {
        lane: DEFAULT_TASK_LANE,
        limit: 32,
        owner: None,
    }
}

pub(crate) fn commit(id: TaskId, outcome: TaskOutcome) -> TaskCommit {
    TaskCommit::Outcome {
        node_id: uuid::Uuid::from_u128(1),
        attempts: 1,
        task_id: id,
        lane: DEFAULT_TASK_LANE,
        outcome,
        owner_token: None,
    }
}
