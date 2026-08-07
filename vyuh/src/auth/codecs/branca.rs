//! BRANCA authenticated encrypted token codec.

use std::future::{Future, ready};

use crate::auth::{
    AuthError, AuthToken, CodecDefinition, CodecRuntime, CustomClaims, EncodedCredential,
    KeySource, PresentedCredential, ProviderId, SecretRing, TokenDecoder, TokenEncoder,
};

const KEY_LENGTH: usize = 32;
const KEY_CONTEXT: &[u8] = b"branca-v1";
const MAX_DECODED_PAYLOAD_BYTES: usize = 16 * 1024;

/// Configures authenticated encrypted BRANCA tokens.
#[derive(Clone, Debug)]
pub struct Branca {
    pub(crate) key: KeySource,
}

impl Branca {
    /// Uses a domain-separated key derived from the application secret ring.
    pub fn site_secret() -> Self {
        Self {
            key: KeySource::site_secret(),
        }
    }

    /// Uses an explicit 32-byte BRANCA key source.
    pub fn new(key: KeySource) -> Self {
        Self { key }
    }
}

impl From<Branca> for CodecDefinition {
    fn from(value: Branca) -> Self {
        Self::Branca(value)
    }
}

struct BrancaCodec {
    active: Vec<u8>,
    verification: Vec<Vec<u8>>,
}

struct ClaimsBrancaCodec {
    codec: BrancaCodec,
    claims: CustomClaims,
    provider: ProviderId,
}

impl TokenEncoder for BrancaCodec {
    fn encode<'a>(
        &'a self,
        token: &'a AuthToken,
    ) -> impl Future<Output = Result<EncodedCredential, AuthError>> + Send + 'a {
        ready(self.encode_inner(token))
    }
}

impl TokenDecoder for BrancaCodec {
    fn decode<'a>(
        &'a self,
        presented: &'a PresentedCredential<'a>,
    ) -> impl Future<Output = Result<AuthToken, AuthError>> + Send + 'a {
        ready(self.decode_inner(presented.expose()))
    }
}

impl TokenDecoder for ClaimsBrancaCodec {
    fn decode<'a>(
        &'a self,
        presented: &'a PresentedCredential<'a>,
    ) -> impl Future<Output = Result<AuthToken, AuthError>> + Send + 'a {
        ready(self.decode_inner(presented.expose()))
    }
}

impl BrancaCodec {
    fn encode_inner(&self, token: &AuthToken) -> Result<EncodedCredential, AuthError> {
        let payload = serde_json::to_vec(token).map_err(|_| AuthError::InvalidCredential)?;
        let timestamp = u32::try_from(token.issued_at()?.timestamp())
            .map_err(|_| AuthError::InvalidCredential)?;
        let encoded = ::branca::encode(&payload, &self.active, timestamp)
            .map_err(|_| AuthError::InvalidCredential)?;
        EncodedCredential::new(encoded)
    }

    fn decode_inner(&self, value: &str) -> Result<AuthToken, AuthError> {
        let (timestamp, payload) = self.payload(value)?;
        let token = serde_json::from_slice::<AuthToken>(&payload)
            .map_err(|_| AuthError::InvalidCredential)?;
        validate_timestamp(&token, timestamp)?;
        Ok(token)
    }

    /// Authenticates a BRANCA value and returns its transport timestamp and JSON payload.
    fn payload(&self, value: &str) -> Result<(u32, Vec<u8>), AuthError> {
        for key in &self.verification {
            if let Ok((timestamp, payload)) = ::branca::decode_with_timestamp(value, key) {
                if payload.len() > MAX_DECODED_PAYLOAD_BYTES {
                    return Err(AuthError::InvalidCredential);
                }
                return Ok((timestamp, payload));
            }
        }
        Err(AuthError::InvalidCredential)
    }
}

impl ClaimsBrancaCodec {
    fn decode_inner(&self, value: &str) -> Result<AuthToken, AuthError> {
        let (timestamp, payload) = self.codec.payload(value)?;
        let claims = serde_json::from_slice(&payload).map_err(|_| AuthError::InvalidCredential)?;
        let token = self.claims.auth_token(claims, self.provider.clone())?;
        validate_timestamp(&token, timestamp)?;
        Ok(token)
    }
}

pub(crate) fn build(
    value: &Branca,
    secrets: &SecretRing,
    claims: Option<&CustomClaims>,
    provider: ProviderId,
) -> Result<CodecRuntime, AuthError> {
    let active = key(secrets, &value.key)?;
    let verification = keys(secrets, &value.key)?;
    let codec = BrancaCodec {
        active,
        verification,
    };
    match claims {
        Some(claims) => Ok(CodecRuntime::decoder(ClaimsBrancaCodec {
            codec,
            claims: claims.clone(),
            provider,
        })),
        None => Ok(CodecRuntime::new(codec)),
    }
}

/// Ensures the normalized issuance time agrees with the authenticated transport timestamp.
fn validate_timestamp(token: &AuthToken, timestamp: u32) -> Result<(), AuthError> {
    let issued =
        u32::try_from(token.issued_at()?.timestamp()).map_err(|_| AuthError::InvalidCredential)?;
    if issued == timestamp {
        Ok(())
    } else {
        Err(AuthError::InvalidCredential)
    }
}

fn key(secrets: &SecretRing, source: &KeySource) -> Result<Vec<u8>, AuthError> {
    if source.is_site_secret() {
        return secrets.derived_active(source, KEY_CONTEXT, KEY_LENGTH);
    }
    let key = secrets.active(source)?;
    validate_key(key)
}

fn keys(secrets: &SecretRing, source: &KeySource) -> Result<Vec<Vec<u8>>, AuthError> {
    if source.is_site_secret() {
        return secrets.derived_verification(source, KEY_CONTEXT, KEY_LENGTH);
    }
    secrets
        .verification(source)?
        .into_iter()
        .map(validate_key)
        .collect()
}

fn validate_key(value: Vec<u8>) -> Result<Vec<u8>, AuthError> {
    if value.len() != KEY_LENGTH {
        return Err(AuthError::InvalidProviderConfig(
            "BRANCA keys must contain exactly 32 bytes".into(),
        ));
    }
    Ok(value)
}
