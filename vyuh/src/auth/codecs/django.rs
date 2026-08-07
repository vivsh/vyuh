//! Django `TimestampSigner` and `django.core.signing.dumps` compatibility.

use std::{
    future::{Future, ready},
    io::{Read, Write},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};
use ring::{constant_time, digest, hmac};

use crate::auth::{
    AuthError, AuthToken, CodecDefinition, CodecRuntime, CustomClaims, EncodedCredential,
    KeySource, PresentedCredential, ProviderId, SecretRing, TokenDecoder, TokenEncoder,
};

const BASE62: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
const MAX_DECODED_PAYLOAD_BYTES: usize = 16 * 1024;

/// Django-compatible timestamped JSON signing configuration.
#[derive(Clone, Debug)]
pub struct DjangoSigning {
    pub(crate) key: KeySource,
    pub(crate) salt: String,
    pub(crate) separator: char,
    pub(crate) compress: bool,
}

impl DjangoSigning {
    /// Uses `SiteConf.secret_key` with Django's default `signing.dumps` salt.
    pub fn site_secret() -> Self {
        Self {
            key: KeySource::site_secret(),
            salt: "django.core.signing".into(),
            separator: ':',
            compress: false,
        }
    }

    /// Sets the Django signing namespace.
    pub fn salt(mut self, value: impl Into<String>) -> Self {
        self.salt = value.into();
        self
    }

    /// Sets the separator used between payload, timestamp, and signature.
    pub fn separator(mut self, value: char) -> Self {
        self.separator = value;
        self
    }

    /// Enables Django-compatible opportunistic zlib compression when encoding.
    pub fn compress(mut self, value: bool) -> Self {
        self.compress = value;
        self
    }
}

impl From<DjangoSigning> for CodecDefinition {
    fn from(value: DjangoSigning) -> Self {
        Self::Django(value)
    }
}

struct DjangoCodec {
    active: Vec<u8>,
    verification: Vec<Vec<u8>>,
    salt: String,
    separator: char,
    compress: bool,
}

struct ClaimsDjangoCodec {
    codec: DjangoCodec,
    claims: CustomClaims,
    provider: ProviderId,
}

impl TokenEncoder for DjangoCodec {
    fn encode<'a>(
        &'a self,
        token: &'a AuthToken,
    ) -> impl Future<Output = Result<EncodedCredential, AuthError>> + Send + 'a {
        ready(self.encode_inner(token))
    }
}

impl TokenDecoder for DjangoCodec {
    fn decode<'a>(
        &'a self,
        presented: &'a PresentedCredential<'a>,
    ) -> impl Future<Output = Result<AuthToken, AuthError>> + Send + 'a {
        ready(self.decode_inner(presented.expose()))
    }
}

impl TokenDecoder for ClaimsDjangoCodec {
    fn decode<'a>(
        &'a self,
        presented: &'a PresentedCredential<'a>,
    ) -> impl Future<Output = Result<AuthToken, AuthError>> + Send + 'a {
        ready(self.decode_inner(presented.expose()))
    }
}

impl DjangoCodec {
    fn encode_inner(&self, token: &AuthToken) -> Result<EncodedCredential, AuthError> {
        let payload = django_json(token)?;
        let payload = encode_payload(payload.as_bytes(), self.compress)?;
        let timestamp = base62_encode(token.issued_at()?.timestamp())?;
        let value = format!("{payload}{}{timestamp}", self.separator);
        let signature = signature(&self.active, &self.salt, &value);
        EncodedCredential::new(format!("{value}{}{signature}", self.separator))
    }

    fn decode_inner(&self, value: &str) -> Result<AuthToken, AuthError> {
        let (signed, supplied) = value
            .rsplit_once(self.separator)
            .ok_or(AuthError::InvalidCredential)?;
        if !self.verification.iter().any(|key| {
            let expected = signature(key, &self.salt, signed);
            constant_time::verify_slices_are_equal(expected.as_bytes(), supplied.as_bytes()).is_ok()
        }) {
            return Err(AuthError::InvalidCredential);
        }
        let (payload, timestamp) = signed
            .rsplit_once(self.separator)
            .ok_or(AuthError::InvalidCredential)?;
        let timestamp = base62_decode(timestamp)?;
        let bytes = decode_payload(payload)?;
        let token = serde_json::from_slice::<AuthToken>(&bytes)
            .map_err(|_| AuthError::InvalidCredential)?;
        if token.issued_at()?.timestamp() != timestamp {
            return Err(AuthError::InvalidCredential);
        }
        Ok(token)
    }

    /// Authenticates a Django value and returns its transport timestamp and JSON payload.
    fn claims_payload(&self, value: &str) -> Result<(i64, serde_json::Value), AuthError> {
        let (signed, supplied) = value
            .rsplit_once(self.separator)
            .ok_or(AuthError::InvalidCredential)?;
        if !self.verification.iter().any(|key| {
            let expected = signature(key, &self.salt, signed);
            constant_time::verify_slices_are_equal(expected.as_bytes(), supplied.as_bytes()).is_ok()
        }) {
            return Err(AuthError::InvalidCredential);
        }
        let (payload, timestamp) = signed
            .rsplit_once(self.separator)
            .ok_or(AuthError::InvalidCredential)?;
        let timestamp = base62_decode(timestamp)?;
        let bytes = decode_payload(payload)?;
        let claims = serde_json::from_slice(&bytes).map_err(|_| AuthError::InvalidCredential)?;
        Ok((timestamp, claims))
    }
}

impl ClaimsDjangoCodec {
    fn decode_inner(&self, value: &str) -> Result<AuthToken, AuthError> {
        let (timestamp, claims) = self.codec.claims_payload(value)?;
        let token = self.claims.auth_token(claims, self.provider.clone())?;
        if token.issued_at()?.timestamp() != timestamp {
            return Err(AuthError::InvalidCredential);
        }
        Ok(token)
    }
}

fn django_json(token: &AuthToken) -> Result<String, AuthError> {
    let value = serde_json::to_string(token).map_err(|_| AuthError::InvalidCredential)?;
    let mut output = String::with_capacity(value.len());
    for character in value.chars() {
        if character.is_ascii() {
            output.push(character);
            continue;
        }
        for unit in character.encode_utf16(&mut [0_u16; 2]) {
            use std::fmt::Write as _;
            write!(output, "\\u{unit:04x}").map_err(|_| AuthError::InvalidCredential)?;
        }
    }
    Ok(output)
}

pub(crate) fn build(
    value: &DjangoSigning,
    secrets: &SecretRing,
    claims: Option<&CustomClaims>,
    provider: ProviderId,
) -> Result<CodecRuntime, AuthError> {
    validate_separator(value.separator)?;
    if value.salt.is_empty() {
        return Err(AuthError::InvalidProviderConfig(
            "Django signing salt cannot be empty".into(),
        ));
    }
    let active = secrets.active(&value.key)?;
    let verification = secrets.verification(&value.key)?;
    let codec = DjangoCodec {
        active,
        verification,
        salt: value.salt.clone(),
        separator: value.separator,
        compress: value.compress,
    };
    match claims {
        Some(claims) => Ok(CodecRuntime::decoder(ClaimsDjangoCodec {
            codec,
            claims: claims.clone(),
            provider,
        })),
        None => Ok(CodecRuntime::new(codec)),
    }
}

fn signature(secret: &[u8], salt: &str, value: &str) -> String {
    let mut material = Vec::with_capacity(salt.len() + 6 + secret.len());
    material.extend_from_slice(salt.as_bytes());
    material.extend_from_slice(b"signer");
    material.extend_from_slice(secret);
    let derived = digest::digest(&digest::SHA256, &material);
    let key = hmac::Key::new(hmac::HMAC_SHA256, derived.as_ref());
    URL_SAFE_NO_PAD.encode(hmac::sign(&key, value.as_bytes()).as_ref())
}

fn encode_payload(value: &[u8], compress: bool) -> Result<String, AuthError> {
    if !compress {
        return Ok(URL_SAFE_NO_PAD.encode(value));
    }
    let compressed = compress_payload(value)?;
    if compressed
        .len()
        .checked_add(1)
        .is_some_and(|length| length < value.len())
    {
        return Ok(format!(".{}", URL_SAFE_NO_PAD.encode(compressed)));
    }
    Ok(URL_SAFE_NO_PAD.encode(value))
}

fn compress_payload(value: &[u8]) -> Result<Vec<u8>, AuthError> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(value)
        .map_err(|_| AuthError::InvalidCredential)?;
    encoder.finish().map_err(|_| AuthError::InvalidCredential)
}

fn decode_payload(value: &str) -> Result<Vec<u8>, AuthError> {
    let (compressed, encoded) = value
        .strip_prefix('.')
        .map(|encoded| (true, encoded))
        .unwrap_or((false, value));
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| AuthError::InvalidCredential)?;
    if bytes.len() > MAX_DECODED_PAYLOAD_BYTES {
        return Err(AuthError::InvalidCredential);
    }
    if !compressed {
        return Ok(bytes);
    }
    let decoder = ZlibDecoder::new(bytes.as_slice());
    let mut output = Vec::new();
    decoder
        .take((MAX_DECODED_PAYLOAD_BYTES + 1) as u64)
        .read_to_end(&mut output)
        .map_err(|_| AuthError::InvalidCredential)?;
    if output.len() > MAX_DECODED_PAYLOAD_BYTES {
        return Err(AuthError::InvalidCredential);
    }
    Ok(output)
}

fn base62_encode(value: i64) -> Result<String, AuthError> {
    let mut value = u64::try_from(value).map_err(|_| AuthError::InvalidCredential)?;
    if value == 0 {
        return Ok("0".into());
    }
    let mut reversed = Vec::new();
    while value > 0 {
        let position = usize::try_from(value % 62).map_err(|_| AuthError::InvalidCredential)?;
        let byte = BASE62.get(position).ok_or(AuthError::InvalidCredential)?;
        reversed.push(*byte);
        value /= 62;
    }
    reversed.reverse();
    String::from_utf8(reversed).map_err(|_| AuthError::InvalidCredential)
}

fn base62_decode(value: &str) -> Result<i64, AuthError> {
    let mut output = 0_u64;
    for byte in value.bytes() {
        let position = BASE62
            .iter()
            .position(|candidate| *candidate == byte)
            .ok_or(AuthError::InvalidCredential)?;
        let digit = u64::try_from(position).map_err(|_| AuthError::InvalidCredential)?;
        output = output
            .checked_mul(62)
            .and_then(|current| current.checked_add(digit))
            .ok_or(AuthError::InvalidCredential)?;
    }
    i64::try_from(output).map_err(|_| AuthError::InvalidCredential)
}

fn validate_separator(value: char) -> Result<(), AuthError> {
    if value.is_ascii_alphanumeric() || matches!(value, '-' | '_') {
        return Err(AuthError::InvalidProviderConfig(
            "Django signing separator cannot be URL-safe base64".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Deserialize)]
    struct Fixture {
        secret: String,
        vyuh_token: String,
        payload: serde_json::Value,
    }

    /// Verifies Vyuh encoding reproduces the token accepted by the Django fixture check.
    #[test]
    fn vyuh_encoding_matches_django_fixture() -> Result<(), AuthError> {
        let fixture = serde_json::from_str::<Fixture>(include_str!(
            "../../../tests/fixtures/django_signing.json"
        ))
        .map_err(|_| AuthError::Internal("invalid Django fixture".into()))?;
        let token = serde_json::from_value::<AuthToken>(fixture.payload)
            .map_err(|_| AuthError::Internal("invalid Django token fixture".into()))?;
        let codec = DjangoCodec {
            active: fixture.secret.as_bytes().to_vec(),
            verification: vec![fixture.secret.into_bytes()],
            salt: "django.core.signing".into(),
            separator: ':',
            compress: false,
        };
        let encoded = codec.encode_inner(&token)?;
        assert_eq!(encoded.expose(), fixture.vyuh_token);
        Ok(())
    }
}
