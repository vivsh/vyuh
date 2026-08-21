//! Cluster-safe task-lane ownership and lifecycle-hook configuration.

use std::time::Duration;

use crate::{
    Error, Site,
    callables::{self, Callable},
};

use super::TaskLane;

type LaneHookCallable = Callable<LaneHookContext, Error>;

/// Context exposed to one task-lane lifecycle hook.
#[derive(Debug, Clone, Copy)]
pub struct TaskLaneContext {
    lane: TaskLane,
    generation: i64,
}

impl TaskLaneContext {
    /// Returns the lane whose external capacity is changing.
    pub const fn lane(&self) -> TaskLane {
        self.lane
    }

    /// Returns the durable lifecycle generation represented by this invocation.
    pub const fn generation(&self) -> i64 {
        self.generation
    }
}

impl callables::IntoArgPart for TaskLaneContext {
    fn into_arg_part() -> callables::ArgPart {
        callables::ArgPart::Ignore
    }
}

impl callables::FromContextParts<LaneHookContext> for TaskLaneContext {
    fn from_context_parts(context: &LaneHookContext) -> Result<Self, callables::CallError> {
        Ok(context.info)
    }
}

impl callables::FromContext<LaneHookContext> for TaskLaneContext {
    fn from_context(context: LaneHookContext) -> Result<Self, callables::CallError> {
        Ok(context.info)
    }
}

#[derive(Clone)]
pub(crate) struct LaneHook {
    identity: &'static str,
    callable: LaneHookCallable,
}

impl LaneHook {
    fn new<H, Args>(handler: H) -> Self
    where
        H: callables::Specable<Args> + Send + Sync + 'static,
        H::Output: callables::IntoOutput<Error> + callables::IntoReturnPart + Send + 'static,
        Args: callables::FromContext<LaneHookContext> + callables::IntoArgSpecs + Send + 'static,
    {
        Self {
            identity: std::any::type_name::<H>(),
            callable: Callable::new(handler),
        }
    }

    pub(crate) const fn identity(&self) -> &'static str {
        self.identity
    }

    pub(crate) async fn call(
        &self,
        site: Site,
        lane: TaskLane,
        generation: i64,
    ) -> Result<(), Error> {
        self.callable
            .call(LaneHookContext {
                site,
                info: TaskLaneContext { lane, generation },
            })
            .await?;
        Ok(())
    }
}

impl std::fmt::Debug for LaneHook {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LaneHook")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
#[doc(hidden)]
pub struct LaneHookContext {
    site: Site,
    info: TaskLaneContext,
}

impl callables::HasSite for LaneHookContext {
    fn site(&self) -> &Site {
        &self.site
    }
}

/// Durable ownership and batching policy for one configured task lane.
#[derive(Clone)]
pub struct TaskLaneLock {
    batch_size: usize,
    deadline: Option<Duration>,
    idle_after: Duration,
    idle_hook: Option<LaneHook>,
    busy_hook: Option<LaneHook>,
}

impl TaskLaneLock {
    /// Creates a lane owner that flushes after accumulating this many tasks.
    pub const fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            deadline: None,
            idle_after: Duration::ZERO,
            idle_hook: None,
            busy_hook: None,
        }
    }

    /// Allows a partial scheduling cohort after the oldest ready task waits this long.
    pub const fn deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Requires continuous quiescence before the lane becomes idle.
    pub const fn idle_after(mut self, duration: Duration) -> Self {
        self.idle_after = duration;
        self
    }

    /// Runs an asynchronous hook before an empty lane is released as idle.
    pub fn on_idle<H, Args>(mut self, handler: H) -> Self
    where
        H: callables::Specable<Args> + Send + Sync + 'static,
        H::Output: callables::IntoOutput<Error> + callables::IntoReturnPart + Send + 'static,
        Args: callables::FromContext<LaneHookContext> + callables::IntoArgSpecs + Send + 'static,
    {
        self.idle_hook = Some(LaneHook::new(handler));
        self
    }

    /// Runs an asynchronous hook before work resumes on an idle lane.
    pub fn on_busy<H, Args>(mut self, handler: H) -> Self
    where
        H: callables::Specable<Args> + Send + Sync + 'static,
        H::Output: callables::IntoOutput<Error> + callables::IntoReturnPart + Send + 'static,
        Args: callables::FromContext<LaneHookContext> + callables::IntoArgSpecs + Send + 'static,
    {
        self.busy_hook = Some(LaneHook::new(handler));
        self
    }

    /// Returns the scheduling-cohort threshold.
    pub const fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Returns the optional partial-cohort deadline.
    pub const fn batch_deadline(&self) -> Option<Duration> {
        self.deadline
    }

    /// Returns the continuous-empty debounce.
    pub const fn idle_duration(&self) -> Duration {
        self.idle_after
    }

    pub(crate) const fn idle_hook(&self) -> Option<&LaneHook> {
        self.idle_hook.as_ref()
    }

    pub(crate) const fn busy_hook(&self) -> Option<&LaneHook> {
        self.busy_hook.as_ref()
    }
}

impl std::fmt::Debug for TaskLaneLock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskLaneLock")
            .field("batch_size", &self.batch_size)
            .field("deadline", &self.deadline)
            .field("idle_after", &self.idle_after)
            .field("idle_hook", &self.idle_hook)
            .field("busy_hook", &self.busy_hook)
            .finish()
    }
}
