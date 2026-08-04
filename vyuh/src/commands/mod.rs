pub(crate) mod core;

mod args;
mod error;
#[cfg(feature = "migrations")]
mod migration_prompt;
mod registry;
mod types;

#[cfg(test)]
mod tests;

pub use error::CommandError;
pub(crate) use registry::CommandRegistry;
pub(crate) use registry::builtin_registry;
pub use registry::{CommandArgInfo, CommandInfo};
pub(crate) use types::{Command, command};
pub use types::{CommandConf, CommandContext};
