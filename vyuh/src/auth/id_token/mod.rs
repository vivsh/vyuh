//! Discovery-backed external identity-token authentication.

mod config;
mod runtime;

pub use config::{IdToken, IdTokenClaims, IdTokenMapper};
pub(crate) use runtime::IdTokenRuntime;
