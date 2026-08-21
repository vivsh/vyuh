mod batch;
mod config;
mod diagnostics;
mod dispatcher;
mod handler;
mod health;
mod lane_lock;
#[cfg(test)]
mod lane_lock_tests;
mod metrics;
mod models;
mod rate;
mod runner;
pub(crate) mod store;
#[cfg(test)]
mod store_tests;
mod submission;

pub use config::*;
pub(crate) use dispatcher::TaskDispatcher;
pub use dispatcher::Tasks;
#[doc(hidden)]
pub use handler::BatchTaskContext;
#[doc(hidden)]
pub use handler::IntoTaskOutcomePart;
pub use handler::{Continuation, TaskContext, TaskError, TaskState, TaskStatus};
pub(crate) use handler::{RegisteredTask, TaskOutcome, TaskRegistry};
pub(crate) use health::{TaskHealth, TaskHealthSnapshot};
pub use lane_lock::{TaskLaneContext, TaskLaneLock};
pub(crate) use metrics::TaskMetrics;
pub(crate) use models::TaskRecord;
pub use models::{TaskDefinition, TaskFilter, TaskId, TaskIdempotency, TaskInfo, TaskKind};
pub(crate) use runner::AbstractTaskRunner;
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
pub use batch::{Batch, IntoTaskBatchOutcomePart};
