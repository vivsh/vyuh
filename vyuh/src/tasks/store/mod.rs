//! Framework-private durable task-store coordination contract.

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(crate) mod database;
#[cfg(any(
    test,
    not(any(feature = "postgres", feature = "mysql", feature = "sqlite"))
))]
mod memory;

pub use super::handler::TaskOutcome;
pub use super::models::TaskRecord;
pub use super::submission::TaskWrite;
#[cfg(any(
    test,
    not(any(feature = "postgres", feature = "mysql", feature = "sqlite"))
))]
pub(crate) use memory::MemoryTaskStore;

#[cfg(feature = "mysql")]
pub(crate) type MySqlTaskStore = database::DbTaskStore;
#[cfg(feature = "postgres")]
pub(crate) type PgTaskStore = database::DbTaskStore;
#[cfg(feature = "sqlite")]
pub(crate) type SqliteTaskStore = database::DbTaskStore;

mod contract;
pub use contract::*;
mod workflow;
