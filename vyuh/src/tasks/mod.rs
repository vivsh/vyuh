mod backends;
mod models;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(crate) mod persistence;
pub(crate) mod store;
pub(crate) mod tasks;

pub use backends::memstore::MemoryTaskStore;
#[cfg(feature = "mysql")]
pub type MySqlTaskStore = persistence::DbTaskStore;
#[cfg(feature = "postgres")]
pub type PgTaskStore = persistence::DbTaskStore;
#[cfg(feature = "sqlite")]
pub type SqliteTaskStore = persistence::DbTaskStore;
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(crate) use models::sort_claimed_tasks;
pub use models::{TaskHandlerConf, TaskListFilter, TaskListPage, TaskOptions, TaskRecord};
pub use store::{AbstractTaskRunner, AbstractTaskStore};
pub use tasks::*;

#[cfg(feature = "postgres")]
pub type TaskStore = PgTaskStore;
#[cfg(feature = "mysql")]
pub type TaskStore = MySqlTaskStore;
#[cfg(feature = "sqlite")]
pub type TaskStore = SqliteTaskStore;
#[cfg(not(any(feature = "postgres", feature = "mysql", feature = "sqlite")))]
pub type TaskStore = MemoryTaskStore;
pub type TaskRunner = AbstractTaskRunner<TaskStore>;
