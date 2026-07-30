//! JWT token codec with pinned algorithms and bounded key rotation.

use std::{
    collections::BTreeMap,
    future::{Future, ready},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, decode_header, encode};
use serde::Deserializer as _;
use serde::de::{IgnoredAny, MapAccess, Visitor};

use crate::auth::{
    AuthError, AuthToken, CodecDefinition, CodecRuntime, EncodedCredential, KeySource,
    PresentedCredential, SecretRing, TokenDecoder, TokenEncoder,
};

/// JWT signing algorithms supported by the built-in codec.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum JwtAlgorithm {
    #[default]
    HS256,
    HS384,
    HS512,
    RS256,
    RS384,
    RS512,
    ES256,
    ES384,
    EdDSA,
}

impl JwtAlgorithm {
    pub(crate) const fn native(self) -> jsonwebtoken::Algorithm {
        match self {
            Self::HS256 => jsonwebtoken::Algorithm::HS256,
            Self::HS384 => jsonwebtoken::Algorithm::HS384,
            Self::HS512 => jsonwebtoken::Algorithm::HS512,
            Self::RS256 => jsonwebtoken::Algorithm::RS256,
            Self::RS384 => jsonwebtoken::Algorithm::RS384,
            Self::RS512 => jsonwebtoken::Algorithm::RS512,
            Self::ES256 => jsonwebtoken::Algorithm::ES256,
            Self::ES384 => jsonwebtoken::Algorithm::ES384,
            Self::EdDSA => jsonwebtoken::Algorithm::EdDSA,
        }
    }

    pub(crate) const fn hmac(self) -> bool {
        matches!(self, Self::HS256 | Self::HS384 | Self::HS512)
    }
}

/// JWT algorithm and key configuration.
#[derive(Clone, Debug)]
pub struct JwtConf {
    algorithm: JwtAlgorithm,
    signing_key: KeySource,
    verifying_key: Option<KeySource>,
    key_id: Option<String>,
    verification_keys: Vec<JwtVerificationKey>,
}

/// A retired JWT key retained only for verification.
#[derive(Clone, Debug)]
pub struct JwtVerificationKey {
    id: String,
    source: KeySource,
}

impl Default for JwtConf {
    fn default() -> Self {
        Self::hs256_site_secret()
    }
}

impl JwtConf {
    /// Uses HS256 with `SiteConf.secret_key` and its verification fallbacks.
    pub fn hs256_site_secret() -> Self {
        Self {
            algorithm: JwtAlgorithm::HS256,
            signing_key: KeySource::site_secret(),
            verifying_key: None,
            key_id: None,
            verification_keys: Vec::new(),
        }
    }

    /// Uses RS256 with one active private/public key pair.
    pub fn rs256(signing_key: KeySource, verifying_key: KeySource) -> Self {
        Self {
            algorithm: JwtAlgorithm::RS256,
            signing_key,
            verifying_key: Some(verifying_key),
            key_id: None,
            verification_keys: Vec::new(),
        }
    }
}

/// Configures the built-in JWT codec.
#[derive(Clone, Debug)]
pub struct Jwt {
    pub(crate) conf: JwtConf,
}

impl Jwt {
    /// Creates a JWT codec from explicit algorithm and key configuration.
    pub fn new(conf: JwtConf) -> Self {
        Self { conf }
    }

    /// Uses HS256 with the application secret ring.
    pub fn hs256_site_secret() -> Self {
        Self::new(JwtConf::hs256_site_secret())
    }

    /// Uses RS256 with one active signing key.
    pub fn rs256(signing_key: KeySource, verifying_key: KeySource) -> Self {
        Self::new(JwtConf::rs256(signing_key, verifying_key))
    }

    /// Assigns the active JWT key ID.
    pub fn key_id(mut self, value: impl Into<String>) -> Self {
        self.conf.key_id = Some(value.into());
        self
    }

    /// Adds a retired verification key selected by JWT `kid`.
    pub fn verification_key(mut self, id: impl Into<String>, source: KeySource) -> Self {
        self.conf.verification_keys.push(JwtVerificationKey {
            id: id.into(),
            source,
        });
        self
    }
}

impl From<Jwt> for CodecDefinition {
    fn from(value: Jwt) -> Self {
        Self::Jwt(value)
    }
}

struct JwtCodec {
    algorithm: jsonwebtoken::Algorithm,
    active_key_id: Option<String>,
    encoding: EncodingKey,
    decoding: VerificationKeys,
    validation: Validation,
}

impl TokenEncoder for JwtCodec {
    fn encode<'a>(
        &'a self,
        token: &'a AuthToken,
    ) -> impl Future<Output = Result<EncodedCredential, AuthError>> + Send + 'a {
        let mut header = Header::new(self.algorithm);
        header.kid = self.active_key_id.clone();
        let result = encode(&header, token, &self.encoding)
            .map_err(|_| AuthError::InvalidCredential)
            .and_then(EncodedCredential::new);
        ready(result)
    }
}

impl TokenDecoder for JwtCodec {
    fn decode<'a>(
        &'a self,
        presented: &'a PresentedCredential<'a>,
    ) -> impl Future<Output = Result<AuthToken, AuthError>> + Send + 'a {
        ready(self.decode_inner(presented.expose()))
    }
}

impl JwtCodec {
    fn decode_inner(&self, value: &str) -> Result<AuthToken, AuthError> {
        reject_duplicate_claims(value)?;
        let header = decode_header(value).map_err(|_| AuthError::InvalidCredential)?;
        if header.alg != self.algorithm {
            return Err(AuthError::InvalidCredential);
        }
        let keys = self.select(header.kid.as_deref())?;
        for key in keys {
            if let Ok(data) = decode::<AuthToken>(value, key, &self.validation) {
                return Ok(data.claims);
            }
        }
        Err(AuthError::InvalidCredential)
    }

    fn select(&self, key_id: Option<&str>) -> Result<&[DecodingKey], AuthError> {
        if let Some(key_id) = key_id {
            return self
                .decoding
                .named
                .get(key_id)
                .map(Vec::as_slice)
                .ok_or(AuthError::InvalidCredential);
        }
        self.decoding.without_id()
    }
}

struct VerificationKeys {
    unnamed: Option<Vec<DecodingKey>>,
    named: BTreeMap<String, Vec<DecodingKey>>,
}

impl VerificationKeys {
    fn without_id(&self) -> Result<&[DecodingKey], AuthError> {
        if let Some(keys) = &self.unnamed {
            return Ok(keys);
        }
        if self.named.len() == 1 {
            return self
                .named
                .values()
                .next()
                .map(Vec::as_slice)
                .ok_or(AuthError::InvalidCredential);
        }
        Err(AuthError::InvalidCredential)
    }
}

fn reject_duplicate_claims(value: &str) -> Result<(), AuthError> {
    let mut segments = value.split('.');
    let _header = segments.next().ok_or(AuthError::InvalidCredential)?;
    let claims = segments.next().ok_or(AuthError::InvalidCredential)?;
    let _signature = segments.next().ok_or(AuthError::InvalidCredential)?;
    if segments.next().is_some() {
        return Err(AuthError::InvalidCredential);
    }
    let claims = URL_SAFE_NO_PAD
        .decode(claims)
        .map_err(|_| AuthError::InvalidCredential)?;
    let mut deserializer = serde_json::Deserializer::from_slice(&claims);
    deserializer
        .deserialize_map(UniqueClaims)
        .map_err(|_| AuthError::InvalidCredential)
}

struct UniqueClaims;

impl<'de> Visitor<'de> for UniqueClaims {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a JWT object with unique claim names")
    }

    fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut names = std::collections::BTreeSet::new();
        while let Some(name) = map.next_key::<String>()? {
            if !names.insert(name) {
                return Err(serde::de::Error::custom("duplicate JWT claim"));
            }
            map.next_value::<IgnoredAny>()?;
        }
        Ok(())
    }
}

pub(crate) fn build(value: &Jwt, secrets: &SecretRing) -> Result<CodecRuntime, AuthError> {
    validate_key_ids(value)?;
    let encoding = encoding(value, secrets)?;
    let decoding = decoding(value, secrets)?;
    let algorithm = value.conf.algorithm.native();
    let mut validation = Validation::new(algorithm);
    validation.validate_aud = false;
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.required_spec_claims.clear();
    Ok(CodecRuntime::new(JwtCodec {
        algorithm,
        active_key_id: value.conf.key_id.clone(),
        encoding,
        decoding,
        validation,
    }))
}

fn validate_key_ids(value: &Jwt) -> Result<(), AuthError> {
    if value.conf.key_id.as_ref().is_some_and(String::is_empty) {
        return Err(AuthError::InvalidProviderConfig(
            "JWT key IDs must be non-empty".into(),
        ));
    }
    if !value.conf.verification_keys.is_empty() && value.conf.key_id.is_none() {
        return Err(AuthError::InvalidProviderConfig(
            "named JWT rotation requires an active key ID".into(),
        ));
    }
    Ok(())
}

fn encoding(value: &Jwt, secrets: &SecretRing) -> Result<EncodingKey, AuthError> {
    let material = secrets.active(&value.conf.signing_key)?;
    if value.conf.algorithm.hmac() {
        validate_hmac(value, material.len(), secrets.minimum())?;
        return Ok(EncodingKey::from_secret(&material));
    }
    asymmetric_encoding(value.conf.algorithm, &material)
}

fn decoding(value: &Jwt, secrets: &SecretRing) -> Result<VerificationKeys, AuthError> {
    let mut output = VerificationKeys {
        unnamed: None,
        named: BTreeMap::new(),
    };
    if value.conf.algorithm.hmac() {
        let keys = secrets.verification(&value.conf.signing_key)?;
        let keys = keys
            .iter()
            .map(|key| DecodingKey::from_secret(key))
            .collect();
        insert_active(&mut output, value.conf.key_id.as_deref(), keys);
    } else {
        let source = value.conf.verifying_key.as_ref().ok_or_else(|| {
            AuthError::InvalidProviderConfig("JWT verification key is required".into())
        })?;
        let material = secrets.active(source)?;
        let keys = vec![asymmetric_decoding(value.conf.algorithm, &material)?];
        insert_active(&mut output, value.conf.key_id.as_deref(), keys);
    }
    add_retired(value, secrets, &mut output)?;
    Ok(output)
}

fn insert_active(output: &mut VerificationKeys, id: Option<&str>, keys: Vec<DecodingKey>) {
    match id {
        Some(id) => {
            output.named.insert(id.to_owned(), keys);
        }
        None => output.unnamed = Some(keys),
    }
}

fn add_retired(
    value: &Jwt,
    secrets: &SecretRing,
    output: &mut VerificationKeys,
) -> Result<(), AuthError> {
    for retired in &value.conf.verification_keys {
        if retired.id.is_empty() || output.named.contains_key(&retired.id) {
            return Err(AuthError::InvalidProviderConfig(
                "JWT key IDs must be unique and non-empty".into(),
            ));
        }
        let material = secrets.active(&retired.source)?;
        let key = if value.conf.algorithm.hmac() {
            DecodingKey::from_secret(&material)
        } else {
            asymmetric_decoding(value.conf.algorithm, &material)?
        };
        output.named.insert(retired.id.clone(), vec![key]);
    }
    Ok(())
}

fn validate_hmac(value: &Jwt, length: usize, minimum: usize) -> Result<(), AuthError> {
    if value.conf.verifying_key.is_some() || length < minimum {
        return Err(AuthError::InvalidProviderConfig(
            "invalid HMAC JWT key configuration".into(),
        ));
    }
    Ok(())
}

fn asymmetric_encoding(algorithm: JwtAlgorithm, material: &[u8]) -> Result<EncodingKey, AuthError> {
    let result = match algorithm {
        JwtAlgorithm::RS256 | JwtAlgorithm::RS384 | JwtAlgorithm::RS512 => {
            EncodingKey::from_rsa_pem(material)
        }
        JwtAlgorithm::ES256 | JwtAlgorithm::ES384 => EncodingKey::from_ec_pem(material),
        JwtAlgorithm::EdDSA => EncodingKey::from_ed_pem(material),
        _ => {
            return Err(AuthError::InvalidProviderConfig(
                "invalid JWT algorithm".into(),
            ));
        }
    };
    result.map_err(|_| AuthError::InvalidProviderConfig("invalid JWT signing key".into()))
}

fn asymmetric_decoding(algorithm: JwtAlgorithm, material: &[u8]) -> Result<DecodingKey, AuthError> {
    let result = match algorithm {
        JwtAlgorithm::RS256 | JwtAlgorithm::RS384 | JwtAlgorithm::RS512 => {
            DecodingKey::from_rsa_pem(material)
        }
        JwtAlgorithm::ES256 | JwtAlgorithm::ES384 => DecodingKey::from_ec_pem(material),
        JwtAlgorithm::EdDSA => DecodingKey::from_ed_pem(material),
        _ => {
            return Err(AuthError::InvalidProviderConfig(
                "invalid JWT algorithm".into(),
            ));
        }
    };
    result.map_err(|_| AuthError::InvalidProviderConfig("invalid JWT verifying key".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies duplicate registered claims are rejected before signature processing.
    #[test]
    fn duplicate_claims_are_rejected() {
        let payload = URL_SAFE_NO_PAD.encode(br#"{"sub":"one","sub":"two"}"#);
        let token = format!("e30.{payload}.signature");
        assert!(matches!(
            reject_duplicate_claims(&token),
            Err(AuthError::InvalidCredential)
        ));
    }
}
