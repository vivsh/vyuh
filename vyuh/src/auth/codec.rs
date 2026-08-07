//! Token codec contracts and resolved application secret rings.

use std::{
    future::Future,
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::Arc,
};

use futures::future::BoxFuture;
use ring::hkdf;

use super::{
    AuthError, AuthToken, AuthTokenBuilder, EncodedCredential, KeySource, KeySourceKind,
    PresentedCredential, ProviderId,
};

/// Converts one external token-claims schema into Vyuh's normalized token envelope.
pub trait TokenClaims: serde::de::DeserializeOwned + Send + 'static {
    /// Builds a complete token through the accepting provider's fixed builder.
    fn auth_token(self, builder: AuthTokenBuilder) -> Result<AuthToken, AuthError>;
}

/// Encodes a normalized authenticated token into one transport format.
pub trait TokenEncoder: Send + Sync + 'static {
    /// Produces an encoded credential without exposing it through formatting traits.
    fn encode<'a>(
        &'a self,
        token: &'a AuthToken,
    ) -> impl Future<Output = Result<EncodedCredential, AuthError>> + Send + 'a;
}

/// Authenticates and decodes one transport token into [`AuthToken`].
pub trait TokenDecoder: Send + Sync + 'static {
    /// Verifies the presented format before returning its normalized token.
    fn decode<'a>(
        &'a self,
        presented: &'a PresentedCredential<'a>,
    ) -> impl Future<Output = Result<AuthToken, AuthError>> + Send + 'a;
}

pub(crate) trait ErasedEncoder: Send + Sync {
    fn encode<'a>(
        &'a self,
        token: &'a AuthToken,
    ) -> BoxFuture<'a, Result<EncodedCredential, AuthError>>;
}

impl<T: TokenEncoder> ErasedEncoder for T {
    fn encode<'a>(
        &'a self,
        token: &'a AuthToken,
    ) -> BoxFuture<'a, Result<EncodedCredential, AuthError>> {
        Box::pin(TokenEncoder::encode(self, token))
    }
}

pub(crate) trait ErasedDecoder: Send + Sync {
    fn decode<'a>(
        &'a self,
        presented: &'a PresentedCredential<'a>,
    ) -> BoxFuture<'a, Result<AuthToken, AuthError>>;
}

impl<T: TokenDecoder> ErasedDecoder for T {
    fn decode<'a>(
        &'a self,
        presented: &'a PresentedCredential<'a>,
    ) -> BoxFuture<'a, Result<AuthToken, AuthError>> {
        Box::pin(TokenDecoder::decode(self, presented))
    }
}

#[derive(Clone)]
pub(crate) struct CustomClaims(Arc<dyn ErasedTokenClaims>);

impl CustomClaims {
    pub(crate) fn new<C: TokenClaims>() -> Self {
        Self(Arc::new(ClaimsAdapter::<C>(PhantomData)))
    }

    pub(crate) fn auth_token(
        &self,
        value: serde_json::Value,
        provider: ProviderId,
    ) -> Result<AuthToken, AuthError> {
        self.0.auth_token(value, provider)
    }
}

trait ErasedTokenClaims: Send + Sync {
    fn auth_token(
        &self,
        value: serde_json::Value,
        provider: ProviderId,
    ) -> Result<AuthToken, AuthError>;
}

struct ClaimsAdapter<C>(PhantomData<fn() -> C>);

impl<C: TokenClaims> ErasedTokenClaims for ClaimsAdapter<C> {
    fn auth_token(
        &self,
        value: serde_json::Value,
        provider: ProviderId,
    ) -> Result<AuthToken, AuthError> {
        let claims =
            serde_json::from_value::<C>(value).map_err(|_| AuthError::InvalidCredential)?;
        claims.auth_token(AuthTokenBuilder::bound(provider))
    }
}

#[derive(Clone)]
#[doc(hidden)]
pub struct CustomCodec {
    pub(crate) encoder: Option<Arc<dyn ErasedEncoder>>,
    pub(crate) decoder: Arc<dyn ErasedDecoder>,
    pub(crate) format: String,
}

/// Internal codec definition accepted by [`super::TokenProvider`].
#[doc(hidden)]
#[derive(Clone)]
pub enum CodecDefinition {
    Jwt(super::Jwt),
    Django(super::DjangoSigning),
    #[cfg(feature = "paseto")]
    Paseto(super::Paseto),
    #[cfg(feature = "branca")]
    Branca(super::Branca),
    #[doc(hidden)]
    Custom(CustomCodec),
}

#[derive(Clone)]
pub(crate) struct CodecRuntime {
    encoder: Option<Arc<dyn ErasedEncoder>>,
    decoder: Arc<dyn ErasedDecoder>,
}

impl CodecRuntime {
    pub(crate) fn new<T>(value: T) -> Self
    where
        T: TokenEncoder + TokenDecoder,
    {
        let value = Arc::new(value);
        Self {
            encoder: Some(value.clone()),
            decoder: value,
        }
    }

    pub(crate) fn custom(value: CustomCodec) -> Self {
        Self {
            encoder: value.encoder,
            decoder: value.decoder,
        }
    }

    pub(crate) fn decoder(value: impl TokenDecoder) -> Self {
        Self {
            encoder: None,
            decoder: Arc::new(value),
        }
    }

    pub(crate) async fn encode(&self, token: &AuthToken) -> Result<String, AuthError> {
        let encoder = self
            .encoder
            .as_ref()
            .ok_or(AuthError::UnsupportedProviderCapability)?;
        encoder
            .encode(token)
            .await
            .map(EncodedCredential::into_inner)
    }

    pub(crate) async fn decode(&self, value: &str) -> Result<AuthToken, AuthError> {
        let presented = PresentedCredential::new(value);
        self.decoder.decode(&presented).await
    }

    pub(crate) fn can_encode(&self) -> bool {
        self.encoder.is_some()
    }
}

impl CodecDefinition {
    pub(crate) fn format(&self) -> &str {
        match self {
            Self::Jwt(_) => "JWT",
            Self::Django(_) => "DjangoSigned",
            #[cfg(feature = "paseto")]
            Self::Paseto(_) => "PASETO",
            #[cfg(feature = "branca")]
            Self::Branca(_) => "BRANCA",
            Self::Custom(value) => &value.format,
        }
    }
}

#[derive(Clone)]
pub(crate) struct SecretRing {
    active: Arc<str>,
    fallbacks: Arc<[String]>,
    project_dir: Arc<PathBuf>,
    minimum: usize,
}

impl SecretRing {
    pub(crate) fn new(
        active: &str,
        fallbacks: &[String],
        project_dir: &Path,
        minimum: usize,
    ) -> Result<Self, AuthError> {
        validate_secrets(active, fallbacks, minimum)?;
        Ok(Self {
            active: Arc::from(active),
            fallbacks: Arc::from(fallbacks),
            project_dir: Arc::new(project_dir.to_path_buf()),
            minimum,
        })
    }

    pub(crate) fn active(&self, source: &KeySource) -> Result<Vec<u8>, AuthError> {
        self.resolve(source)
    }

    pub(crate) fn verification(&self, source: &KeySource) -> Result<Vec<Vec<u8>>, AuthError> {
        if !source.is_site_secret() {
            return self.resolve(source).map(|value| vec![value]);
        }
        let mut values = Vec::with_capacity(self.fallbacks.len() + 1);
        values.push(self.active.as_bytes().to_vec());
        values.extend(self.fallbacks.iter().map(|value| value.as_bytes().to_vec()));
        Ok(values)
    }

    pub(crate) fn derived_active(
        &self,
        source: &KeySource,
        context: &[u8],
        length: usize,
    ) -> Result<Vec<u8>, AuthError> {
        let material = self.active(source)?;
        derive_key(&material, context, length)
    }

    pub(crate) fn derived_verification(
        &self,
        source: &KeySource,
        context: &[u8],
        length: usize,
    ) -> Result<Vec<Vec<u8>>, AuthError> {
        self.verification(source)?
            .iter()
            .map(|material| derive_key(material, context, length))
            .collect()
    }

    pub(crate) fn minimum(&self) -> usize {
        self.minimum
    }

    fn resolve(&self, source: &KeySource) -> Result<Vec<u8>, AuthError> {
        match source.kind() {
            KeySourceKind::SiteSecret => Ok(self.active.as_bytes().to_vec()),
            KeySourceKind::Inline(value) => Ok(value.as_bytes().to_vec()),
            KeySourceKind::Env(name) => std::env::var(name).map(String::into_bytes).map_err(|_| {
                AuthError::InvalidProviderConfig(format!("authentication key '{name}' is unset"))
            }),
            KeySourceKind::File(path) => self.read(path),
        }
    }

    fn read(&self, value: &Path) -> Result<Vec<u8>, AuthError> {
        let path = if value.is_absolute() {
            value.to_path_buf()
        } else {
            self.project_dir.join(value)
        };
        std::fs::read(&path).map_err(|_| {
            AuthError::InvalidProviderConfig(format!(
                "unable to read authentication key '{}'",
                path.display()
            ))
        })
    }
}

struct OutputLength(usize);

impl hkdf::KeyType for OutputLength {
    fn len(&self) -> usize {
        self.0
    }
}

fn derive_key(material: &[u8], context: &[u8], length: usize) -> Result<Vec<u8>, AuthError> {
    let salt = hkdf::Salt::new(hkdf::HKDF_SHA256, b"vyuh-auth-key");
    let key = salt.extract(material);
    let info = [context];
    let output = key
        .expand(&info, OutputLength(length))
        .map_err(|_| AuthError::InvalidProviderConfig("invalid derived key length".into()))?;
    let mut bytes = vec![0_u8; length];
    output
        .fill(&mut bytes)
        .map_err(|_| AuthError::InvalidProviderConfig("key derivation failed".into()))?;
    Ok(bytes)
}

fn validate_secrets(active: &str, fallbacks: &[String], minimum: usize) -> Result<(), AuthError> {
    if active.len() < minimum {
        return Err(AuthError::InvalidProviderConfig(
            "active site secret is too short".into(),
        ));
    }
    if fallbacks.len() > 7 {
        return Err(AuthError::InvalidProviderConfig(
            "at most seven fallback secrets are supported".into(),
        ));
    }
    if fallbacks
        .iter()
        .any(|value| value.len() < minimum || value == active)
    {
        return Err(AuthError::InvalidProviderConfig(
            "fallback secrets must be strong and distinct".into(),
        ));
    }
    for (position, value) in fallbacks.iter().enumerate() {
        if fallbacks
            .iter()
            .skip(position + 1)
            .any(|other| other == value)
        {
            return Err(AuthError::InvalidProviderConfig(
                "fallback secrets must be unique".into(),
            ));
        }
    }
    Ok(())
}
