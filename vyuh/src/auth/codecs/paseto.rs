//! PASETO v4 public and local token codecs.

use std::{
    collections::BTreeMap,
    future::{Future, ready},
};

use pasetors::{
    Local, Public,
    keys::{AsymmetricPublicKey, AsymmetricSecretKey, SymmetricKey},
    token::UntrustedToken,
    version4::{LocalToken, PublicToken, V4},
};

use crate::auth::{
    AuthError, AuthToken, CodecDefinition, CodecRuntime, EncodedCredential, KeySource,
    PresentedCredential, SecretRing, TokenDecoder, TokenEncoder,
};

const LOCAL_CONTEXT: &[u8] = b"paseto-v4-local";
const LOCAL_KEY_LENGTH: usize = 32;
const MAX_DECODED_PAYLOAD_BYTES: usize = 16 * 1024;

/// A retired PASETO public key retained only for verification.
#[derive(Clone, Debug)]
pub struct PasetoVerificationKey {
    id: String,
    source: KeySource,
}

#[derive(Clone, Debug)]
enum PasetoMode {
    Public {
        signing: KeySource,
        verifying: KeySource,
        key_id: Option<String>,
        verification_keys: Vec<PasetoVerificationKey>,
    },
    Local {
        key: KeySource,
    },
}

/// Configures PASETO v4 public signing or local authenticated encryption.
#[derive(Clone, Debug)]
pub struct Paseto {
    mode: PasetoMode,
    configuration_error: Option<&'static str>,
}

impl Paseto {
    /// Uses PASETO v4.public with one active secret/public key pair.
    pub fn v4_public(signing: KeySource, verifying: KeySource) -> Self {
        Self {
            mode: PasetoMode::Public {
                signing,
                verifying,
                key_id: None,
                verification_keys: Vec::new(),
            },
            configuration_error: None,
        }
    }

    /// Uses PASETO v4.local with an explicit 32-byte key source.
    pub fn v4_local(key: KeySource) -> Self {
        Self {
            mode: PasetoMode::Local { key },
            configuration_error: None,
        }
    }

    /// Uses a domain-separated v4.local key derived from the site secret ring.
    pub fn v4_local_site_secret() -> Self {
        Self::v4_local(KeySource::site_secret())
    }

    /// Assigns the active v4.public key ID stored in the authenticated footer.
    pub fn key_id(mut self, value: impl Into<String>) -> Self {
        if let PasetoMode::Public { key_id, .. } = &mut self.mode {
            *key_id = Some(value.into());
        } else {
            self.configuration_error = Some("PASETO local tokens do not support key IDs");
        }
        self
    }

    /// Adds a retired v4.public verification key.
    pub fn verification_key(mut self, id: impl Into<String>, source: KeySource) -> Self {
        if let PasetoMode::Public {
            verification_keys, ..
        } = &mut self.mode
        {
            verification_keys.push(PasetoVerificationKey {
                id: id.into(),
                source,
            });
        } else {
            self.configuration_error = Some("PASETO local tokens use site-secret fallbacks");
        }
        self
    }
}

impl From<Paseto> for CodecDefinition {
    fn from(value: Paseto) -> Self {
        Self::Paseto(value)
    }
}

enum PasetoCodec {
    Public {
        signing: AsymmetricSecretKey<V4>,
        verification: BTreeMap<Option<String>, AsymmetricPublicKey<V4>>,
        key_id: Option<String>,
    },
    Local {
        active: SymmetricKey<V4>,
        verification: Vec<SymmetricKey<V4>>,
    },
}

impl TokenEncoder for PasetoCodec {
    fn encode<'a>(
        &'a self,
        token: &'a AuthToken,
    ) -> impl Future<Output = Result<EncodedCredential, AuthError>> + Send + 'a {
        ready(self.encode_inner(token))
    }
}

impl TokenDecoder for PasetoCodec {
    fn decode<'a>(
        &'a self,
        presented: &'a PresentedCredential<'a>,
    ) -> impl Future<Output = Result<AuthToken, AuthError>> + Send + 'a {
        ready(self.decode_inner(presented.expose()))
    }
}

impl PasetoCodec {
    fn encode_inner(&self, token: &AuthToken) -> Result<EncodedCredential, AuthError> {
        let payload = serde_json::to_vec(token).map_err(|_| AuthError::InvalidCredential)?;
        let encoded = match self {
            Self::Public {
                signing, key_id, ..
            } => PublicToken::sign(
                signing,
                &payload,
                key_id.as_deref().map(str::as_bytes),
                None,
            ),
            Self::Local { active, .. } => LocalToken::encrypt(active, &payload, None, None),
        }
        .map_err(|_| AuthError::InvalidCredential)?;
        EncodedCredential::new(encoded)
    }

    fn decode_inner(&self, value: &str) -> Result<AuthToken, AuthError> {
        let payload = match self {
            Self::Public { verification, .. } => decode_public(value, verification)?,
            Self::Local { verification, .. } => decode_local(value, verification)?,
        };
        if payload.len() > MAX_DECODED_PAYLOAD_BYTES {
            return Err(AuthError::InvalidCredential);
        }
        serde_json::from_str(&payload).map_err(|_| AuthError::InvalidCredential)
    }
}

pub(crate) fn build(value: &Paseto, secrets: &SecretRing) -> Result<CodecRuntime, AuthError> {
    if let Some(error) = value.configuration_error {
        return Err(AuthError::InvalidProviderConfig(error.into()));
    }
    let codec = match &value.mode {
        PasetoMode::Public {
            signing,
            verifying,
            key_id,
            verification_keys,
        } => build_public(signing, verifying, key_id, verification_keys, secrets)?,
        PasetoMode::Local { key } => build_local(key, secrets)?,
    };
    Ok(CodecRuntime::new(codec))
}

fn build_public(
    signing: &KeySource,
    verifying: &KeySource,
    key_id: &Option<String>,
    retired: &[PasetoVerificationKey],
    secrets: &SecretRing,
) -> Result<PasetoCodec, AuthError> {
    if !retired.is_empty() && key_id.is_none() {
        return Err(AuthError::InvalidProviderConfig(
            "PASETO rotation requires an active key ID".into(),
        ));
    }
    let signing = secret_key(secrets.active(signing)?)?;
    let public = public_key(secrets.active(verifying)?)?;
    let mut verification = BTreeMap::new();
    verification.insert(key_id.clone(), public);
    for value in retired {
        let id = Some(value.id.clone());
        if value.id.is_empty() || verification.contains_key(&id) {
            return Err(AuthError::InvalidProviderConfig(
                "PASETO key IDs must be unique and non-empty".into(),
            ));
        }
        verification.insert(id, public_key(secrets.active(&value.source)?)?);
    }
    Ok(PasetoCodec::Public {
        signing,
        verification,
        key_id: key_id.clone(),
    })
}

fn build_local(source: &KeySource, secrets: &SecretRing) -> Result<PasetoCodec, AuthError> {
    let active = local_key(source, secrets, true)?;
    let materials = if source.is_site_secret() {
        secrets.derived_verification(source, LOCAL_CONTEXT, LOCAL_KEY_LENGTH)?
    } else {
        secrets.verification(source)?
    };
    let verification = materials
        .iter()
        .map(|value| SymmetricKey::<V4>::from(value).map_err(map_key))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(PasetoCodec::Local {
        active,
        verification,
    })
}

fn local_key(
    source: &KeySource,
    secrets: &SecretRing,
    active: bool,
) -> Result<SymmetricKey<V4>, AuthError> {
    let material = if source.is_site_secret() {
        secrets.derived_active(source, LOCAL_CONTEXT, LOCAL_KEY_LENGTH)?
    } else if active {
        secrets.active(source)?
    } else {
        return Err(AuthError::InvalidProviderConfig(
            "invalid PASETO key source".into(),
        ));
    };
    SymmetricKey::from(&material).map_err(map_key)
}

fn secret_key(material: Vec<u8>) -> Result<AsymmetricSecretKey<V4>, AuthError> {
    if material.iter().take(32).all(|value| *value == 0) {
        return Err(AuthError::InvalidProviderConfig(
            "PASETO secret keys cannot use an all-zero seed".into(),
        ));
    }
    if let Ok(value) = std::str::from_utf8(&material)
        && value.starts_with("k4.secret.")
    {
        return AsymmetricSecretKey::try_from(value).map_err(map_key);
    }
    AsymmetricSecretKey::from(&material).map_err(map_key)
}

fn public_key(material: Vec<u8>) -> Result<AsymmetricPublicKey<V4>, AuthError> {
    if let Ok(value) = std::str::from_utf8(&material)
        && value.starts_with("k4.public.")
    {
        return AsymmetricPublicKey::try_from(value).map_err(map_key);
    }
    AsymmetricPublicKey::from(&material).map_err(map_key)
}

fn decode_public(
    value: &str,
    keys: &BTreeMap<Option<String>, AsymmetricPublicKey<V4>>,
) -> Result<String, AuthError> {
    let token =
        UntrustedToken::<Public, V4>::try_from(value).map_err(|_| AuthError::InvalidCredential)?;
    let footer = footer_id(token.untrusted_footer())?;
    let key = keys.get(&footer).ok_or(AuthError::InvalidCredential)?;
    PublicToken::verify(key, &token, None, None)
        .map(|trusted| trusted.payload().to_owned())
        .map_err(|_| AuthError::InvalidCredential)
}

fn decode_local(value: &str, keys: &[SymmetricKey<V4>]) -> Result<String, AuthError> {
    let token =
        UntrustedToken::<Local, V4>::try_from(value).map_err(|_| AuthError::InvalidCredential)?;
    for key in keys {
        if let Ok(trusted) = LocalToken::decrypt(key, &token, None, None) {
            return Ok(trusted.payload().to_owned());
        }
    }
    Err(AuthError::InvalidCredential)
}

fn footer_id(value: &[u8]) -> Result<Option<String>, AuthError> {
    if value.is_empty() {
        return Ok(None);
    }
    std::str::from_utf8(value)
        .map(str::to_owned)
        .map(Some)
        .map_err(|_| AuthError::InvalidCredential)
}

fn map_key(_: pasetors::errors::Error) -> AuthError {
    AuthError::InvalidProviderConfig("invalid PASETO key material".into())
}
