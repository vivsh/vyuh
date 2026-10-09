//! Durable lane-lease request and lifecycle response protocol.
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
use crate::tasks::TaskRuntimeError;

/// Runner evidence supplied while coordinating one locked lane.
#[derive(Debug, Clone)]
pub struct LaneOwnerRequest {
    /// Token currently held by this runner, when any.
    pub token: Option<String>,
    /// Whether the runner has no queued, running, or uncommitted lane work.
    pub quiescent: bool,
    /// Whether this scheduler turn has global batch budget to claim a cohort.
    pub allow_claim: bool,
    /// Whether handler work completed since the last acknowledged owner turn.
    pub completed_work: bool,
    /// Completed lifecycle hook waiting for a fenced durable transition.
    pub hook: Option<LaneHookResult>,
}

/// Result returned by one non-blocking lane lifecycle hook.
#[derive(Debug, Clone)]
pub struct LaneHookResult {
    /// Lifecycle generation passed to the hook.
    pub generation: i64,
    /// Hook edge that completed.
    pub action: LaneHookAction,
    /// Success or one bounded diagnostic failure.
    pub result: Result<(), String>,
}

/// Lifecycle hook requested for one durably owned lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaneHookAction {
    /// Reconcile the lane's external capacity to stopped.
    Idle,
    /// Reconcile the lane's external capacity to running.
    Busy,
}

/// Durable phase of one locked lane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum LaneOwnerPhase {
    Active = 0,
    Idling = 1,
    Idle = 2,
    Busying = 3,
    IdleFailed = 4,
    BusyFailed = 5,
}

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
impl LaneOwnerPhase {
    /// Converts one stable persisted phase value.
    pub(crate) fn from_i16(value: i16) -> Result<Self, TaskRuntimeError> {
        match value {
            0 => Ok(Self::Active),
            1 => Ok(Self::Idling),
            2 => Ok(Self::Idle),
            3 => Ok(Self::Busying),
            4 => Ok(Self::IdleFailed),
            5 => Ok(Self::BusyFailed),
            _ => Err(TaskRuntimeError::TaskExecutionError(format!(
                "invalid task lane owner phase {value}"
            ))),
        }
    }
}

/// Store response for one locked lane owner turn.
#[derive(Debug, Clone)]
pub struct LaneOwnerPoll {
    /// Current owner token, omitted when this runner no longer owns the lane.
    pub token: Option<String>,
    /// Current lifecycle generation.
    pub generation: i64,
    /// Current durable lifecycle phase.
    pub phase: LaneOwnerPhase,
    /// Hook the runner must spawn without blocking its scheduler.
    pub action: Option<LaneHookAction>,
    /// Whether this turn replaced an expired prior owner token.
    pub takeover: bool,
}
