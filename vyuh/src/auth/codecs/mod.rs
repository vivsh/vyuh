//! Framework-owned token format implementations.

mod django;
mod jwt;

#[cfg(feature = "branca")]
mod branca;
#[cfg(feature = "paseto")]
mod paseto;

pub use django::DjangoSigning;
pub use jwt::{Jwt, JwtAlgorithm, JwtConf, JwtVerificationKey};

#[cfg(feature = "branca")]
pub use branca::Branca;
#[cfg(feature = "paseto")]
pub use paseto::Paseto;

use super::{AuthError, CodecDefinition, CodecRuntime, SecretRing};

pub(crate) fn build(
    definition: &CodecDefinition,
    secrets: &SecretRing,
) -> Result<CodecRuntime, AuthError> {
    match definition {
        CodecDefinition::Jwt(value) => jwt::build(value, secrets),
        CodecDefinition::Django(value) => django::build(value, secrets),
        #[cfg(feature = "paseto")]
        CodecDefinition::Paseto(value) => paseto::build(value, secrets),
        #[cfg(feature = "branca")]
        CodecDefinition::Branca(value) => branca::build(value, secrets),
        CodecDefinition::Custom(value) => Ok(CodecRuntime::custom(value.clone())),
    }
}
