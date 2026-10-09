//! Immutable task registration, deployment validation, and client construction.
use super::handler::RegisteredTask;
use super::models::IdempotencyPolicy;
use super::{TaskClient, TaskConf, TaskRuntimeError};
use std::{
    any::TypeId,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

/// Bounds advisory wake/cancellation messages independently of task input capacity.
const CONTROL_CHANNEL_CAPACITY: usize = 250;

#[derive(Clone)]
pub(crate) struct TaskRegistry {
    pub(crate) config: TaskConf,
    pub(crate) tasks: HashMap<String, RegisteredTask>,
    pub(crate) typed_map: HashMap<TypeId, String>,
    lane_defaults: BTreeMap<super::TaskLane, super::TaskLaneConf>,
    lanes: Vec<super::TaskLaneConf>,
}

impl TaskRegistry {
    /// Replaces deferred declarations using operation-local policy deduplication scratch.
    pub(crate) fn prepare_flows(
        mut self,
        site: &crate::PartialSite,
    ) -> Result<Self, TaskRuntimeError> {
        let mut scratch = super::flow::factory::DispatcherScratch::new();
        let mut names: Vec<_> = self.tasks.keys().cloned().collect();
        names.sort();
        for name in names {
            if let Some(task) = self.tasks.get_mut(&name) {
                task.prepare_flow(site, &mut scratch)?;
            }
        }
        Ok(self)
    }

    pub(crate) fn new() -> Self {
        Self {
            config: TaskConf::default(),
            tasks: HashMap::new(),
            typed_map: HashMap::new(),
            lane_defaults: BTreeMap::new(),
            lanes: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_config(mut self, config: TaskConf) -> Result<Self, TaskRuntimeError> {
        self.lanes = config.resolve_lanes(std::iter::empty())?;
        self.config = config;
        Ok(self)
    }

    /// Resolves every immutable task definition against validated site policy.
    pub(crate) fn finalize(mut self, config: TaskConf) -> Result<Self, TaskRuntimeError> {
        let lanes = config.resolve_lanes(self.lane_defaults.values().cloned())?;
        for task in self.tasks.values_mut() {
            let declared = task.declared_lane();
            validate_key_revision(task.idempotency_policy())?;
            let (effective, fallback) = config
                .resolve_lane(&lanes, declared)
                .map_err(|error| missing_lane_error(error, task.name(), declared, &lanes))?;
            if fallback {
                tracing::warn!(
                    task = task.name(),
                    declared_lane = %declared,
                    effective_lane = %effective,
                    "task lane is not configured; using the default lane"
                );
            }
            task.resolve_lane(effective);
        }
        self.config = config;
        self.lanes = lanes;
        Ok(self)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Returns whether one registered handler consumes local task batches.
    pub(crate) fn is_batch(&self, name: &str) -> bool {
        self.tasks.get(name).is_some_and(RegisteredTask::is_batch)
    }

    pub(crate) fn register(&mut self, service: RegisteredTask) -> Result<(), TaskRuntimeError> {
        let name = service.name().to_string();
        validate_task_name(&name)?;
        if self.tasks.contains_key(&name) || self.typed_map.contains_key(&service.type_id) {
            return Err(TaskRuntimeError::AlreadyExists(name));
        }
        self.typed_map.insert(service.type_id, name.clone());
        self.tasks.insert(name, service);
        Ok(())
    }

    /// Adds one bundle-owned default for a named non-default task lane.
    pub(crate) fn register_lane(
        &mut self,
        lane: super::TaskLaneConf,
    ) -> Result<(), TaskRuntimeError> {
        let name = lane.lane();
        if name == super::DEFAULT_TASK_LANE {
            return Err(TaskRuntimeError::InvalidConfig(
                "bundles cannot configure the default task lane".into(),
            ));
        }
        if self.lane_defaults.contains_key(&name) {
            return Err(TaskRuntimeError::AlreadyExists(format!(
                "task lane '{name}'"
            )));
        }
        self.lane_defaults.insert(name, lane);
        Ok(())
    }

    pub(crate) fn merge(&mut self, other: TaskRegistry) -> Result<(), TaskRuntimeError> {
        for lane in other.lane_defaults.into_values() {
            self.register_lane(lane)?;
        }
        for (name, task) in other.tasks {
            validate_task_name(&name)?;
            if self.tasks.contains_key(&name) {
                return Err(TaskRuntimeError::AlreadyExists(name));
            }
            if self.typed_map.contains_key(&task.type_id) {
                return Err(TaskRuntimeError::AlreadyExists(name));
            }
            self.typed_map.insert(task.type_id, name.clone());
            self.tasks.insert(name, task);
        }
        Ok(())
    }

    /// Returns the complete validated lane set used by the runtime and store.
    pub(crate) fn lanes(&self) -> &[super::TaskLaneConf] {
        &self.lanes
    }

    /// Produces the finalized per-handler idempotency policy shared with stores.
    pub(crate) fn idempotency_conf(
        &self,
    ) -> Result<Vec<super::store::TaskIdempotencyConf>, TaskRuntimeError> {
        self.tasks
            .values()
            .filter_map(|task| {
                task.idempotency_policy().map(|policy| {
                    lane_retention(&self.lanes, task.effective_lane()).map(|retention| {
                        super::store::TaskIdempotencyConf {
                            handler: task.name.clone(),
                            lane: task.effective_lane().to_string(),
                            revision: policy.revision.into(),
                            retention,
                        }
                    })
                })
            })
            .collect()
    }

    /// Builds immutable store configuration before producers or workers use it.
    pub(crate) fn store_conf(&self) -> Result<super::TaskStoreConf, TaskRuntimeError> {
        Ok(super::TaskStoreConf {
            max_all_children: self.config.all_limit(),
            handlers: self
                .tasks
                .values()
                .map(|task| (task.name.clone(), task.kind()))
                .collect(),
            lanes: self.lanes().to_vec(),
            idempotency: self.idempotency_conf()?,
            poll_interval: self.config.poll_interval_value(),
        })
    }

    pub(crate) fn client<S: crate::tasks::store::TaskStore + Send + Sync + 'static>(
        self: Arc<Self>,
        store: Arc<S>,
        schedules: Vec<super::store::TaskScheduleConf>,
    ) -> TaskClient<S> {
        let metrics = Arc::new(super::TaskMetrics::new(
            self.tasks.keys().cloned(),
            self.lanes.iter().map(|lane| lane.lane().to_string()),
        ));
        TaskClient {
            store,
            registry: self.clone(),
            events: tokio::sync::broadcast::channel(CONTROL_CHANNEL_CAPACITY).0,
            metrics,
            health: super::TaskHealth::new(self.config.readiness_policy(), !self.is_empty()),
            schedules: schedules.into(),
            ingress: None,
        }
    }
}

impl Default for TaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_task_name(name: &str) -> Result<(), TaskRuntimeError> {
    if name.is_empty() || name.chars().count() > 191 {
        return Err(TaskRuntimeError::InvalidConfig(
            "task handler names must contain between 1 and 191 characters".into(),
        ));
    }
    Ok(())
}

/// Validates the stable identifier used to distinguish idempotency-key semantics.
fn validate_key_revision(policy: Option<IdempotencyPolicy>) -> Result<(), TaskRuntimeError> {
    let Some(policy) = policy else {
        return Ok(());
    };
    let revision = policy.revision;
    if revision.is_empty() || revision.len() > 64 {
        return Err(TaskRuntimeError::InvalidConfig(
            "task idempotency revisions must contain between 1 and 64 bytes".into(),
        ));
    }
    if !revision.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
    }) {
        return Err(TaskRuntimeError::InvalidConfig(
            "task idempotency revisions use lowercase letters, digits, '-' or '_'".into(),
        ));
    }
    Ok(())
}

/// Reads the already validated retention policy for one effective registered lane.
fn lane_retention(
    lanes: &[super::TaskLaneConf],
    lane: super::TaskLane,
) -> Result<super::IdempotencyRetention, TaskRuntimeError> {
    lanes
        .iter()
        .find(|entry| entry.lane() == lane)
        .map(super::TaskLaneConf::idempotency_policy)
        .ok_or_else(|| TaskRuntimeError::UnknownLane(lane.to_string()))
}

/// Adds task-definition context when strict lane resolution rejects site construction.
fn missing_lane_error(
    error: TaskRuntimeError,
    task: &str,
    declared: super::TaskLane,
    lanes: &[super::TaskLaneConf],
) -> TaskRuntimeError {
    if !matches!(error, TaskRuntimeError::UnknownLane(_)) {
        return error;
    }
    let configured = lanes
        .iter()
        .map(|lane| lane.lane().as_str())
        .collect::<Vec<_>>()
        .join(", ");
    TaskRuntimeError::InvalidConfig(format!(
        "task '{task}' declares lane '{declared}', but configured lanes are: {configured}"
    ))
}

impl std::fmt::Debug for TaskRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRegistry")
            .field("tasks", &self.tasks.keys().collect::<Vec<_>>())
            .finish()
    }
}
