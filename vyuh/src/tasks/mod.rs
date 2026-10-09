mod callable;
mod client;
mod config;
mod continuation;
mod diagnostics;
mod failure;
mod flow;
mod handler;
mod handler_error;
mod health;
mod lane_lock;
mod metrics;
mod models;
mod outcome;
mod rate;
mod registry;
mod result;
mod runner;
pub(crate) mod store;
mod submission;
mod work;

#[doc(hidden)]
pub use callable::{FlowCallable, WorkCallable};
pub(crate) use client::TaskClient;
pub use client::Tasks;
pub use config::*;
pub use continuation::Continuation;
pub use failure::{TaskFailure, TaskRuntimeError, TaskStatus};
pub use flow::FlowConf;
#[doc(hidden)]
pub use flow::FlowContext;
pub use flow::FlowState;
pub use flow::{Flow, IntoFlow};
#[doc(hidden)]
pub use flow::{FlowArguments, FlowReturn};
#[cfg(feature = "pravah")]
pub use flow::{PravahDispatcher, WorkRequest};
pub(crate) use handler::RegisteredTask;
pub use handler_error::{FlowError, WorkError};
pub(crate) use health::{TaskHealth, TaskHealthSnapshot};
pub use lane_lock::{TaskLaneContext, TaskLaneLock};
pub(crate) use metrics::TaskMetrics;
pub(crate) use models::TaskRecord;
pub use models::{TaskDefinition, TaskFilter, TaskId, TaskIdempotency, TaskInfo, TaskKind};
pub(crate) use outcome::TaskOutcome;
pub(crate) use registry::TaskRegistry;
pub(crate) use runner::TaskRunner;
#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
pub(crate) use store::MemoryTaskStore;
pub(crate) use store::{
    CommitResult, LaneClaim, LaneHookAction, LaneHookResult, LaneOwnerPhase, LaneOwnerPoll,
    LaneOwnerRequest, LanePoll, ScheduledTaskWrite, TaskCommit, TaskPoll, TaskScheduleConf,
    TaskScheduleSnapshot, TaskStore, TaskStoreConf,
};
pub(crate) use submission::TaskWrite;
pub use submission::{TaskOptions, TaskReceipt};
#[doc(hidden)]
pub use work::BatchWorkContext;
#[doc(hidden)]
pub use work::IntoWorkOutcomePart;
pub use work::WorkContext;
pub use work::WorkState;

#[cfg(feature = "postgres")]
pub(crate) type ConfiguredTaskStore = store::PgTaskStore;
#[cfg(feature = "mysql")]
pub(crate) type ConfiguredTaskStore = store::MySqlTaskStore;
#[cfg(feature = "sqlite")]
pub(crate) type ConfiguredTaskStore = store::SqliteTaskStore;
#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
pub(crate) type ConfiguredTaskStore = store::MemoryTaskStore;
pub use work::{Batch, IntoWorkBatchOutcomePart};
