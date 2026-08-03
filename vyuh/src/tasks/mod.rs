mod backends;
mod config;
mod dispatcher;
mod handler;
mod metrics;
mod models;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(crate) mod persistence;
mod rate;
mod runner;
pub mod store;
mod submission;

pub use config::*;
pub(crate) use dispatcher::TaskDispatcher;
pub use dispatcher::Tasks;
#[doc(hidden)]
pub use handler::IntoTaskOutcomePart;
pub use handler::{Continuation, TaskContext, TaskError, TaskState, TaskStatus};
pub(crate) use handler::{RegisteredTask, TaskOutcome, TaskRegistry};
pub(crate) use metrics::TaskMetrics;
pub(crate) use models::TaskRecord;
pub use models::{TaskFilter, TaskHandlerConf, TaskId, TaskInfo};
pub(crate) use runner::AbstractTaskRunner;
pub(crate) use store::{
    AbstractTaskStore, GroupClaim, GroupPoll, TaskCommit, TaskPoll, TaskStoreConf,
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
