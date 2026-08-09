//! Framework-owned token format implementations.

mod django;
mod jwt;

#[cfg(feature = "branca")]
mod branca;
#[cfg(feature = "paseto")]
mod paseto;

pub use django::DjangoSigning;
#[cfg(any(feature = "oauth", feature = "id-token"))]
pub(crate) use jwt::reject_duplicate_claims;
pub use jwt::{Jwt, JwtAlgorithm, JwtConf, JwtVerificationKey};

#[cfg(feature = "branca")]
pub use branca::Branca;
#[cfg(feature = "paseto")]
pub use paseto::Paseto;

use super::{AuthError, CodecDefinition, CodecRuntime, CustomClaims, ProviderId, SecretRing};

pub(crate) fn build(
    definition: &CodecDefinition,
    secrets: &SecretRing,
    claims: Option<&CustomClaims>,
    provider: ProviderId,
) -> Result<CodecRuntime, AuthError> {
    match definition {
        CodecDefinition::Jwt(value) => jwt::build(value, secrets, claims, provider),
        CodecDefinition::Django(value) => django::build(value, secrets, claims, provider),
        #[cfg(feature = "paseto")]
        CodecDefinition::Paseto(value) => paseto::build(value, secrets, claims, provider),
        #[cfg(feature = "branca")]
        CodecDefinition::Branca(value) => branca::build(value, secrets, claims, provider),
        CodecDefinition::Custom(value) => {
            if claims.is_some() {
                return Err(AuthError::InvalidProviderConfig(
                    "custom claims require a built-in JSON token codec".into(),
                ));
            }
            Ok(CodecRuntime::custom(value.clone()))
        }
    }
}
