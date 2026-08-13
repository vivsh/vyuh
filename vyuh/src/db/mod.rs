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
/// Derives Mool's [`Filterable`] trait through Vyuh's database facade.
pub use vyuh_macros::Filterable;
/// Derives Mool's [`Model`] trait through Vyuh's database facade.
pub use vyuh_macros::Model;
/// Derives Mool's [`Record`] trait through Vyuh's database facade.
pub use vyuh_macros::Record;
/// Derives Mool's request-sort vocabulary through Vyuh's database facade.
pub use vyuh_macros::SortKey;
/// Embeds crate-owned migrations through Vyuh's database facade.
#[cfg(feature = "migrations")]
pub use vyuh_macros::embed_migrations;
