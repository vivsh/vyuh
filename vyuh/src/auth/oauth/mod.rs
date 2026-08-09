//! External OAuth authorization-server access-token validation.

#[cfg(feature = "oauth")]
mod config;
pub(crate) mod http;
#[cfg(feature = "oauth")]
mod runtime;

#[cfg(feature = "oauth")]
pub use config::{
    OAuthClaims, OAuthIdentityMapper, OAuthJwtAlgorithm, OAuthResource, OAuthResourceServer,
};
#[cfg(feature = "oauth")]
pub(crate) use runtime::OAuthRuntime;
