//! Database facade exposed by Vyuh.
//!
//! Vyuh re-exports the standalone DB toolkit through this module so framework
//! users can keep importing `vyuh::db`. Framework-native LISTEN/NOTIFY support
//! is layered back onto the shared database pool through an extension trait.

pub use crate::db_notify::{Notify, PgNotifyDbExt};
pub use crate::notifiers::CancellationNotifier;
pub use db_core::backend::{Arguments, Database, Pool, QueryResult, Row};
pub use db_core::migrations::*;
pub use db_core::*;
