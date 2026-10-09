//! Async Work return capabilities and local batch value conversion.
pub(in crate::tasks) mod batch;
pub(in crate::tasks) mod returns;
pub(in crate::tasks) mod state;
pub use batch::{Batch, IntoWorkBatchOutcomePart};
pub use returns::IntoWorkOutcomePart;
pub use state::WorkState;
mod handler;
pub use handler::{BatchWorkContext, WorkContext};
