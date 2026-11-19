//! External OAuth authorization-server access-token validation.

#[cfg(feature = "oauth-resource")]
mod config;
pub(crate) mod http;
#[cfg(feature = "oauth-resource")]
mod introspection;
#[cfg(feature = "oauth-resource")]
mod runtime;

#[cfg(feature = "oauth-resource")]
pub use config::{
    OAuthClaims, OAuthIdentityMapper, OAuthJwtAlgorithm, OAuthResource, OAuthResourceServer,
};
#[cfg(feature = "oauth-resource")]
pub(crate) use runtime::OAuthRuntime;
