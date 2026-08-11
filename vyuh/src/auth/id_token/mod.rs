//! Discovery-backed external identity-token authentication.

mod config;
mod runtime;

pub use config::{IdToken, IdTokenClaims, IdTokenMapper, IdTokenResource};
pub(crate) use runtime::IdTokenRuntime;
