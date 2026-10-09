//! Framework-private durable task-store coordination contract.

#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlite"))]
pub(crate) mod database;
#[cfg(any(
    test,
    not(any(feature = "postgres", feature = "mysql", feature = "sqlite"))
))]
mod memory;

pub use super::models::TaskRecord;
pub use super::outcome::TaskOutcome;
pub use super::submission::TaskWrite;
#[cfg(any(
    test,
    not(any(feature = "postgres", feature = "mysql", feature = "sqlite"))
))]
pub(crate) use memory::MemoryTaskStore;
#[cfg(all(test, feature = "pravah"))]
pub(crate) use memory::tests as fixtures;

#[cfg(feature = "mysql")]
pub(crate) type MySqlTaskStore = database::DbTaskStore;
#[cfg(feature = "postgres")]
pub(crate) type PgTaskStore = database::DbTaskStore;
#[cfg(feature = "sqlite")]
pub(crate) type SqliteTaskStore = database::DbTaskStore;

mod contract;
mod lane;
pub use contract::*;
pub use lane::*;
pub(crate) mod transitions;

pub(crate) use super::outcome::{normalize_outcome, truncate_utf8};
