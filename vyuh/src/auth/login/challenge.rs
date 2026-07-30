//! Sealed login flow state and response-ready challenges.

use std::{fmt, future::Future, sync::Arc};

#[cfg(feature = "oidc")]
use axum::http::{StatusCode, header};
use axum::{
    Json,
    http::{HeaderName, HeaderValue},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::future::BoxFuture;
use ring::{aead, rand::SecureRandom};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::LoginTarget;
use crate::auth::{AuthError, KeySource, SecretRing};
use crate::callables::{IntoReturnPart, ReturnPart};

const KEY_CONTEXT: &[u8] = b"login-state-v1";
const NONCE_LENGTH: usize = 12;
const MAX_STATE_PAYLOAD_BYTES: usize = 16 * 1024;
const MAX_ENCODED_STATE_BYTES: usize = 24 * 1024;

/// The response shape produced when a login requires another step.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoginChallengeKind {
    /// Redirect the user agent to an external identity provider.
    Redirect,
    /// Return a JSON challenge for an additional local factor.
    Factor,
}

/// A response-ready continuation challenge for OIDC or MFA login.
pub struct LoginChallenge {
    challenge: Challenge,
    attachments: Vec<(HeaderName, HeaderValue)>,
}

enum Challenge {
    #[cfg(feature = "oidc")]
    Redirect { url: String, expires_in: i64 },
    Factor {
        token: String,
        methods: Vec<String>,
        expires_in: i64,
    },
}

impl LoginChallenge {
    /// Returns whether this challenge redirects or requests another factor.
    pub const fn kind(&self) -> LoginChallengeKind {
        match &self.challenge {
            #[cfg(feature = "oidc")]
            Challenge::Redirect { .. } => LoginChallengeKind::Redirect,
            Challenge::Factor { .. } => LoginChallengeKind::Factor,
        }
    }

    /// Returns an external redirect URL when this is an OIDC challenge.
    pub fn redirect_url(&self) -> Option<&str> {
        match &self.challenge {
            #[cfg(feature = "oidc")]
            Challenge::Redirect { url, .. } => Some(url),
            Challenge::Factor { .. } => None,
        }
    }

    /// Deliberately exposes an opaque local challenge token when present.
    pub fn token(&self) -> Option<&str> {
        match &self.challenge {
            Challenge::Factor { token, .. } => Some(token),
            #[cfg(feature = "oidc")]
            Challenge::Redirect { .. } => None,
        }
    }

    /// Returns the factor methods offered by a local challenge.
    pub fn methods(&self) -> &[String] {
        match &self.challenge {
            Challenge::Factor { methods, .. } => methods,
            #[cfg(feature = "oidc")]
            Challenge::Redirect { .. } => &[],
        }
    }

    /// Returns the number of seconds before this challenge expires.
    pub const fn expires_in(&self) -> i64 {
        match &self.challenge {
            #[cfg(feature = "oidc")]
            Challenge::Redirect { expires_in, .. } => *expires_in,
            Challenge::Factor { expires_in, .. } => *expires_in,
        }
    }

    /// Applies challenge-managed cookies or headers to an existing response.
    pub fn write(&self, response: &mut Response) {
        for (name, value) in &self.attachments {
            response.headers_mut().append(name, value.clone());
        }
    }

    pub(crate) fn factor(token: String, methods: Vec<String>, expires_in: i64) -> Self {
        Self {
            challenge: Challenge::Factor {
                token,
                methods,
                expires_in,
            },
            attachments: Vec::new(),
        }
    }

    #[cfg(feature = "oidc")]
    pub(crate) fn redirect(url: String, expires_in: i64) -> Self {
        Self {
            challenge: Challenge::Redirect { url, expires_in },
            attachments: Vec::new(),
        }
    }
}

#[derive(Serialize, JsonSchema)]
pub(crate) struct FactorChallengeSchema {
    challenge_token: String,
    methods: Vec<String>,
    expires_in: i64,
}

impl IntoResponse for LoginChallenge {
    fn into_response(self) -> Response {
        let mut response = match self.challenge {
            #[cfg(feature = "oidc")]
            Challenge::Redirect { url, .. } => redirect_response(url),
            Challenge::Factor {
                token,
                methods,
                expires_in,
            } => Json(FactorChallengeSchema {
                challenge_token: token,
                methods,
                expires_in,
            })
            .into_response(),
        };
        for (name, value) in self.attachments {
            response.headers_mut().append(name, value);
        }
        response
    }
}

impl IntoReturnPart for LoginChallenge {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Unknown
    }
}

#[cfg(feature = "oidc")]
fn redirect_response(url: String) -> Response {
    match HeaderValue::from_str(&url) {
        Ok(value) => {
            let mut response = StatusCode::SEE_OTHER.into_response();
            response.headers_mut().insert(header::LOCATION, value);
            response
        }
        Err(_) => AuthError::InvalidLoginState.into_response(),
    }
}

#[derive(Clone)]
pub(crate) struct ChallengeCodec {
    active: Arc<[u8]>,
    verification: Arc<[Vec<u8>]>,
}

impl ChallengeCodec {
    pub(crate) fn new(secrets: &SecretRing) -> Result<Self, AuthError> {
        let active = secrets.derived_active(&KeySource::site_secret(), KEY_CONTEXT, 32)?;
        let verification =
            secrets.derived_verification(&KeySource::site_secret(), KEY_CONTEXT, 32)?;
        Ok(Self {
            active: active.into(),
            verification: verification.into(),
        })
    }

    pub(crate) fn seal<T: Serialize>(&self, value: &T) -> Result<String, AuthError> {
        let mut payload = serde_json::to_vec(value).map_err(|_| AuthError::InvalidLoginState)?;
        if payload.len() > MAX_STATE_PAYLOAD_BYTES {
            return Err(AuthError::InvalidLoginState);
        }
        let nonce = random_nonce()?;
        let nonce_bytes = nonce.as_ref().to_vec();
        let key = sealing_key(&self.active)?;
        key.seal_in_place_append_tag(nonce, aead::Aad::empty(), &mut payload)
            .map_err(|_| AuthError::InvalidLoginState)?;
        let mut encoded = nonce_bytes;
        encoded.extend(payload);
        Ok(URL_SAFE_NO_PAD.encode(encoded))
    }

    pub(crate) fn open<T: for<'de> Deserialize<'de>>(&self, token: &str) -> Result<T, AuthError> {
        if token.len() > MAX_ENCODED_STATE_BYTES {
            return Err(AuthError::InvalidLoginState);
        }
        let encoded = URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| AuthError::InvalidLoginState)?;
        if encoded.len() <= NONCE_LENGTH + aead::AES_256_GCM.tag_len() {
            return Err(AuthError::InvalidLoginState);
        }
        self.open_with_keys(encoded)
    }

    fn open_with_keys<T: for<'de> Deserialize<'de>>(
        &self,
        encoded: Vec<u8>,
    ) -> Result<T, AuthError> {
        let (nonce_bytes, ciphertext) = encoded.split_at(NONCE_LENGTH);
        for key in self.verification.iter() {
            let mut payload = ciphertext.to_vec();
            let nonce = nonce_from_slice(nonce_bytes)?;
            if let Ok(opened) =
                opening_key(key)?.open_in_place(nonce, aead::Aad::empty(), &mut payload)
            {
                if opened.len() > MAX_STATE_PAYLOAD_BYTES {
                    return Err(AuthError::InvalidLoginState);
                }
                return serde_json::from_slice(opened).map_err(|_| AuthError::InvalidLoginState);
            }
        }
        Err(AuthError::InvalidLoginState)
    }
}

fn random_nonce() -> Result<aead::Nonce, AuthError> {
    let mut bytes = [0_u8; NONCE_LENGTH];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| AuthError::Internal("login state randomness failed".into()))?;
    Ok(aead::Nonce::assume_unique_for_key(bytes))
}

fn nonce_from_slice(value: &[u8]) -> Result<aead::Nonce, AuthError> {
    let bytes: [u8; NONCE_LENGTH] = value.try_into().map_err(|_| AuthError::InvalidLoginState)?;
    Ok(aead::Nonce::assume_unique_for_key(bytes))
}

fn sealing_key(value: &[u8]) -> Result<aead::LessSafeKey, AuthError> {
    aead::UnboundKey::new(&aead::AES_256_GCM, value)
        .map(aead::LessSafeKey::new)
        .map_err(|_| AuthError::InvalidProviderConfig("invalid login state key".into()))
}

fn opening_key(value: &[u8]) -> Result<aead::LessSafeKey, AuthError> {
    sealing_key(value)
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct SealedLoginState {
    pub(crate) version: u8,
    pub(crate) state_id: String,
    pub(crate) method: String,
    pub(crate) target: LoginTarget,
    pub(crate) expires_at: i64,
    pub(crate) payload: serde_json::Value,
}

/// Optional application storage for atomically consuming login continuation IDs.
pub trait LoginStateStore: Send + Sync + 'static {
    /// Returns `true` only when this continuation ID is consumed for the first time.
    fn consume<'a>(
        &'a self,
        state_id: &'a str,
        expires_at: i64,
    ) -> impl Future<Output = Result<bool, AuthError>> + Send + 'a;
}

trait ErasedLoginStateStore: Send + Sync {
    fn consume<'a>(
        &'a self,
        state_id: &'a str,
        expires_at: i64,
    ) -> BoxFuture<'a, Result<bool, AuthError>>;
}

impl<T: LoginStateStore> ErasedLoginStateStore for T {
    fn consume<'a>(
        &'a self,
        state_id: &'a str,
        expires_at: i64,
    ) -> BoxFuture<'a, Result<bool, AuthError>> {
        Box::pin(LoginStateStore::consume(self, state_id, expires_at))
    }
}

#[derive(Clone)]
pub(crate) struct LoginStateStoreRuntime(Arc<dyn ErasedLoginStateStore>);

impl LoginStateStoreRuntime {
    pub(crate) fn new(value: impl LoginStateStore) -> Self {
        Self(Arc::new(value))
    }

    pub(crate) async fn consume(&self, state: &SealedLoginState) -> Result<(), AuthError> {
        if self.0.consume(&state.state_id, state.expires_at).await? {
            return Ok(());
        }
        Err(AuthError::InvalidLoginState)
    }
}

impl fmt::Debug for LoginStateStoreRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("LoginStateStore(<redacted>)")
    }
}
