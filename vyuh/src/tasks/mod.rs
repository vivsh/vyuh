mod batch;
mod callable;
mod config;
mod diagnostics;
mod dispatcher;
mod failure;
mod flow_state;
mod handler;
mod handler_error;
mod health;
mod lane_lock;
#[cfg(test)]
mod lane_lock_tests;
mod metrics;
mod models;
mod rate;
mod result;
mod returns;
mod runner;
mod state;
pub(crate) mod store;
#[cfg(test)]
mod store_tests;
mod submission;

#[doc(hidden)]
pub use callable::{FlowCallable, WorkCallable};
pub use config::*;
pub(crate) use dispatcher::TaskDispatcher;
pub use dispatcher::Tasks;
pub use failure::{TaskFailure, TaskRuntimeError, TaskStatus};
pub use flow_state::FlowState;
#[doc(hidden)]
pub use handler::BatchWorkContext;
#[doc(hidden)]
pub use handler::FlowContext;
pub use handler::{Continuation, WorkContext};
pub(crate) use handler::{RegisteredTask, TaskOutcome, TaskRegistry};
pub use handler_error::{FlowError, WorkError};
pub(crate) use health::{TaskHealth, TaskHealthSnapshot};
pub use lane_lock::{TaskLaneContext, TaskLaneLock};
pub(crate) use metrics::TaskMetrics;
pub(crate) use models::TaskRecord;
pub use models::{TaskDefinition, TaskFilter, TaskId, TaskIdempotency, TaskInfo, TaskKind};
#[doc(hidden)]
pub use returns::{IntoFlowOutcomePart, IntoWorkOutcomePart};
pub(crate) use runner::AbstractTaskRunner;
pub use state::WorkState;
#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
pub(crate) use store::MemoryTaskStore;
pub(crate) use store::{
    AbstractTaskStore, LaneClaim, LaneHookAction, LaneHookResult, LaneOwnerPhase, LaneOwnerPoll,
    LaneOwnerRequest, LanePoll, ScheduledTaskWrite, TaskCommit, TaskLease, TaskPoll,
    TaskScheduleConf, TaskScheduleSnapshot, TaskStoreConf, TaskTick,
};
pub(crate) use submission::TaskWrite;
pub use submission::{TaskOptions, TaskReceipt};

#[cfg(feature = "postgres")]
pub(crate) type TaskStore = store::PgTaskStore;
#[cfg(feature = "mysql")]
pub(crate) type TaskStore = store::MySqlTaskStore;
#[cfg(feature = "sqlite")]
pub(crate) type TaskStore = store::SqliteTaskStore;
#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
pub(crate) type TaskStore = store::MemoryTaskStore;
pub(crate) type TaskRunner = AbstractTaskRunner<TaskStore>;
pub use batch::{Batch, IntoWorkBatchOutcomePart};
