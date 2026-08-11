//! Durable passwordless email and phone login methods.

use std::{fmt, future::Future, sync::Arc};

#[cfg(feature = "email")]
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use futures::future::BoxFuture;
use ring::hmac;
#[cfg(feature = "email")]
use ring::rand::SecureRandom;
use serde::{Deserialize, Serialize};

use super::{
    BoxLoginInput, ChallengeCodec, ErasedLoginRuntime, LoginChallenge, LoginCompletion,
    LoginMethodId, LoginProviderDefinition, LoginRuntimeDefinition, LoginTarget, OtpDelivery,
    SealedLoginState, VerifiedLogin,
    runtime::{CompletedLogin, LoginFuture, completion_sealed},
};
use crate::auth::{AuthError, AuthUser, KeySource, SecretRing};

mod policy;
mod store;

pub use policy::OtpPolicy;
use policy::{MAX_OTP_LENGTH, random_code};
pub use store::{PasswordlessAttempt, PasswordlessChallenge, PasswordlessStart, PasswordlessStore};
pub(crate) use store::{PasswordlessChallengeParts, PasswordlessStoreRuntime};

const PROOF_CONTEXT: &[u8] = b"passwordless-proof-v1";
const PRINCIPAL_CONTEXT: &[u8] = b"passwordless-principal-v1";
#[cfg(feature = "email")]
const MAGIC_LINK_TTL: i64 = 15 * 60;
const OTP_TTL: i64 = 10 * 60;
const OTP_ATTEMPTS: u8 = 5;
const RESEND_DELAY: i64 = 60;
const DELIVERY_LIMIT: u8 = 5;
const DELIVERY_WINDOW: i64 = 60 * 60;

/// A validated email address used only by passwordless email methods.
#[cfg(feature = "email")]
#[derive(Clone, PartialEq, Eq)]
pub struct EmailAddress(String);

#[cfg(feature = "email")]
impl EmailAddress {
    /// Retains an email address until the selected login method validates it.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the address only to an application-owned login resolver.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<(), AuthError> {
        self.0
            .parse::<lettre::message::Mailbox>()
            .map(|_| ())
            .map_err(|_| AuthError::InvalidCredential)
    }
}

#[cfg(feature = "email")]
impl fmt::Debug for EmailAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EmailAddress(<redacted>)")
    }
}

/// A canonical E.164 phone number used by passwordless phone methods.
#[derive(Clone, PartialEq, Eq)]
pub struct PhoneNumber(String);

impl PhoneNumber {
    /// Retains a phone number until the selected login method validates E.164 syntax.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the number only to an application-owned resolver or sender.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<(), AuthError> {
        let bytes = self.0.as_bytes();
        let Some((&first, rest)) = bytes.split_first() else {
            return Err(AuthError::InvalidCredential);
        };
        if first != b'+' || !(2..=15).contains(&rest.len()) || rest.first() == Some(&b'0') {
            return Err(AuthError::InvalidCredential);
        }
        rest.iter()
            .all(u8::is_ascii_digit)
            .then_some(())
            .ok_or(AuthError::InvalidCredential)
    }
}

impl fmt::Debug for PhoneNumber {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PhoneNumber(<redacted>)")
    }
}

/// A magic-link token extracted from an application-owned callback route.
#[cfg(feature = "email")]
#[derive(Deserialize)]
pub struct MagicLinkCallback {
    token: String,
}

#[cfg(feature = "email")]
impl MagicLinkCallback {
    /// Creates callback input for applications that do not use a query extractor.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }

    fn parts(&self) -> Result<(&str, &str), AuthError> {
        self.token
            .split_once('.')
            .ok_or(AuthError::InvalidLoginState)
    }

    fn token(&self) -> &str {
        &self.token
    }
}

#[cfg(feature = "email")]
impl fmt::Debug for MagicLinkCallback {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MagicLinkCallback(<redacted>)")
    }
}

#[cfg(feature = "email")]
impl completion_sealed::Sealed for MagicLinkCallback {}
#[cfg(feature = "email")]
impl LoginCompletion for MagicLinkCallback {}

/// A redacted passwordless one-time-password completion response.
pub struct Otp(OtpResponse);

impl Otp {
    /// Creates OTP completion input from an opaque challenge and delivered code.
    pub fn new(challenge_token: impl Into<String>, code: impl Into<String>) -> Self {
        Self(OtpResponse::new(challenge_token, code))
    }
}

impl completion_sealed::Sealed for Otp {}
impl LoginCompletion for Otp {}

impl fmt::Debug for Otp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Otp(<redacted>)")
    }
}

struct OtpResponse {
    challenge_token: String,
    code: String,
}

impl OtpResponse {
    fn new(challenge_token: impl Into<String>, code: impl Into<String>) -> Self {
        Self {
            challenge_token: challenge_token.into(),
            code: code.into(),
        }
    }

    fn validate(&self) -> Result<(), AuthError> {
        valid_id(&self.challenge_token)?;
        (!self.code.is_empty()
            && self.code.len() <= usize::from(MAX_OTP_LENGTH)
            && self.code.bytes().all(|value| value.is_ascii_alphanumeric()))
        .then_some(())
        .ok_or(AuthError::InvalidCredential)
    }
}

impl fmt::Debug for OtpResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OtpResponse(<redacted>)")
    }
}

/// Resolves an existing application identity from an email address.
#[cfg(feature = "email")]
pub trait EmailLoginResolver: Send + Sync + 'static {
    /// Returns the current account identity, or `None` when no account is linked.
    fn resolve<'a>(
        &'a self,
        email: &'a EmailAddress,
    ) -> impl Future<Output = Result<Option<AuthUser>, AuthError>> + Send + 'a;
}

/// A validated principal supported by the passwordless OTP login method.
#[derive(Clone, PartialEq, Eq)]
pub enum PasswordlessAddress {
    /// An email address, available with Vyuh's `email` feature.
    #[cfg(feature = "email")]
    Email(EmailAddress),
    /// A canonical E.164 phone number.
    Phone(PhoneNumber),
}

impl PasswordlessAddress {
    /// Retains an email address until the selected OTP method validates it.
    #[cfg(feature = "email")]
    pub fn email(value: impl Into<String>) -> Self {
        Self::Email(EmailAddress::new(value))
    }

    /// Retains a phone number until the selected OTP method validates it.
    pub fn phone(value: impl Into<String>) -> Self {
        Self::Phone(PhoneNumber::new(value))
    }

    /// Returns the address only to an application-owned resolver or delivery handler.
    pub fn as_str(&self) -> &str {
        match self {
            #[cfg(feature = "email")]
            Self::Email(value) => value.as_str(),
            Self::Phone(value) => value.as_str(),
        }
    }

    fn validate(&self) -> Result<(), AuthError> {
        match self {
            #[cfg(feature = "email")]
            Self::Email(value) => value.validate(),
            Self::Phone(value) => value.validate(),
        }
    }

    fn channel(&self) -> &'static str {
        match self {
            #[cfg(feature = "email")]
            Self::Email(_) => "email",
            Self::Phone(_) => "phone",
        }
    }

    fn from_parts(channel: &str, value: String) -> Result<Self, AuthError> {
        match channel {
            #[cfg(feature = "email")]
            "email" => Ok(Self::Email(EmailAddress::new(value))),
            "phone" => Ok(Self::Phone(PhoneNumber::new(value))),
            _ => Err(AuthError::InvalidLoginState),
        }
    }
}

impl fmt::Debug for PasswordlessAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PasswordlessAddress(<redacted>)")
    }
}

/// Resolves an existing application identity from a passwordless address.
pub trait OtpLoginResolver: Send + Sync + 'static {
    /// Returns the current account identity, or `None` when no account is linked.
    fn resolve<'a>(
        &'a self,
        address: &'a PasswordlessAddress,
    ) -> impl Future<Output = Result<Option<AuthUser>, AuthError>> + Send + 'a;
}

#[cfg(feature = "email")]
trait ErasedEmailResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        email: &'a EmailAddress,
    ) -> BoxFuture<'a, Result<Option<AuthUser>, AuthError>>;
}

#[cfg(feature = "email")]
impl<T: EmailLoginResolver> ErasedEmailResolver for T {
    fn resolve<'a>(
        &'a self,
        email: &'a EmailAddress,
    ) -> BoxFuture<'a, Result<Option<AuthUser>, AuthError>> {
        Box::pin(EmailLoginResolver::resolve(self, email))
    }
}

trait ErasedOtpResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        address: &'a PasswordlessAddress,
    ) -> BoxFuture<'a, Result<Option<AuthUser>, AuthError>>;
}

impl<T: OtpLoginResolver> ErasedOtpResolver for T {
    fn resolve<'a>(
        &'a self,
        address: &'a PasswordlessAddress,
    ) -> BoxFuture<'a, Result<Option<AuthUser>, AuthError>> {
        Box::pin(OtpLoginResolver::resolve(self, address))
    }
}

/// Configures passwordless email magic-link login.
#[cfg(feature = "email")]
#[derive(Clone)]
pub struct MagicLinkLogin {
    resolver: Arc<dyn ErasedEmailResolver>,
    callback_url: Option<String>,
    stateful: bool,
}

/// Explicit acknowledgement that a magic link may be reused until expiry.
#[cfg(feature = "email")]
#[derive(Clone, Copy, Debug)]
pub struct UnsafeReusableMagicLinks(());

#[cfg(feature = "email")]
impl UnsafeReusableMagicLinks {
    /// Acknowledges replay risk and enables stateless reusable magic links.
    pub const fn allow() -> Self {
        Self(())
    }
}

#[cfg(feature = "email")]
impl MagicLinkLogin {
    /// Creates a one-time magic-link method backed by the passwordless store.
    pub fn new(resolver: impl EmailLoginResolver) -> Self {
        Self {
            resolver: Arc::new(resolver),
            callback_url: None,
            stateful: true,
        }
    }
    /// Sets the absolute callback URL used only for generated magic links.
    pub fn callback_url(mut self, value: impl Into<String>) -> Self {
        self.callback_url = Some(value.into());
        self
    }

    /// Requires durable one-time verification for generated magic links.
    pub fn stateful(mut self) -> Self {
        self.stateful = true;
        self
    }

    /// Uses a sealed link that remains reusable until expiry after explicit acknowledgement.
    pub fn stateless(mut self, _risk: UnsafeReusableMagicLinks) -> Self {
        self.stateful = false;
        self
    }
}

/// Configures passwordless OTP login with application-owned delivery.
#[derive(Clone)]
pub struct OtpLogin {
    resolver: Arc<dyn ErasedOtpResolver>,
    policy: OtpPolicy,
}

impl OtpLogin {
    /// Creates an OTP method with the default six-digit numeric policy.
    pub fn new(resolver: impl OtpLoginResolver) -> Self {
        Self {
            resolver: Arc::new(resolver),
            policy: OtpPolicy::numeric(6),
        }
    }

    /// Configures the generated code policy.
    pub fn policy(mut self, value: OtpPolicy) -> Self {
        self.policy = value;
        self
    }
}

#[derive(Serialize, Deserialize)]
struct PendingPasswordless {
    identifier: String,
    subject: Option<String>,
    method: String,
    channel: String,
}

struct ProofInput {
    id: String,
    value: Vec<u8>,
}

impl ProofInput {
    fn new(id: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            id: id.into(),
            value: value.into(),
        }
    }
}

#[cfg(feature = "email")]
struct MagicLinkRuntime {
    conf: MagicLinkLogin,
}

struct OtpRuntime {
    conf: OtpLogin,
}

#[cfg(feature = "email")]
impl ErasedLoginRuntime for MagicLinkRuntime {
    fn is_flow(&self) -> bool {
        true
    }
    fn validate(&self) -> Result<(), AuthError> {
        validate_magic_link_conf(&self.conf)
    }
    fn requires_passwordless_store(&self) -> bool {
        self.conf.stateful
    }
    fn begin<'a>(
        &'a self,
        method: &'a LoginMethodId,
        input: BoxLoginInput,
        target: LoginTarget,
        codec: &'a ChallengeCodec,
        secrets: &'a SecretRing,
        store: Option<&'a PasswordlessStoreRuntime>,
    ) -> LoginFuture<'a, LoginChallenge> {
        Box::pin(async move {
            self.begin_inner(method, input, target, codec, secrets, store)
                .await
        })
    }
    fn complete<'a>(
        &'a self,
        method: &'a LoginMethodId,
        input: BoxLoginInput,
        codec: &'a ChallengeCodec,
        secrets: &'a SecretRing,
        _state_store: Option<&'a super::LoginStateStoreRuntime>,
        store: Option<&'a PasswordlessStoreRuntime>,
    ) -> LoginFuture<'a, CompletedLogin> {
        Box::pin(async move {
            self.complete_inner(method, input, codec, secrets, store)
                .await
        })
    }
}

impl ErasedLoginRuntime for OtpRuntime {
    fn is_flow(&self) -> bool {
        true
    }
    fn validate(&self) -> Result<(), AuthError> {
        self.conf.policy.validate()
    }
    fn requires_passwordless_store(&self) -> bool {
        true
    }
    fn begin<'a>(
        &'a self,
        method: &'a LoginMethodId,
        input: BoxLoginInput,
        target: LoginTarget,
        codec: &'a ChallengeCodec,
        secrets: &'a SecretRing,
        store: Option<&'a PasswordlessStoreRuntime>,
    ) -> LoginFuture<'a, LoginChallenge> {
        Box::pin(async move {
            self.begin_inner(method, input, target, codec, secrets, store)
                .await
        })
    }
    fn complete<'a>(
        &'a self,
        method: &'a LoginMethodId,
        input: BoxLoginInput,
        codec: &'a ChallengeCodec,
        secrets: &'a SecretRing,
        _state_store: Option<&'a super::LoginStateStoreRuntime>,
        store: Option<&'a PasswordlessStoreRuntime>,
    ) -> LoginFuture<'a, CompletedLogin> {
        Box::pin(async move {
            self.complete_inner(method, input, codec, secrets, store)
                .await
        })
    }
}

#[cfg(feature = "email")]
impl MagicLinkRuntime {
    async fn begin_inner(
        &self,
        method: &LoginMethodId,
        input: BoxLoginInput,
        target: LoginTarget,
        codec: &ChallengeCodec,
        secrets: &SecretRing,
        store: Option<&PasswordlessStoreRuntime>,
    ) -> Result<LoginChallenge, AuthError> {
        let email = *input
            .downcast::<EmailAddress>()
            .map_err(|_| AuthError::InvalidCredential)?;
        email.validate()?;
        let user = self.conf.resolver.resolve(&email).await?;
        if !self.conf.stateful {
            let url = user
                .as_ref()
                .map(|user| self.stateless_link(method, target, &email, user, codec))
                .transpose()?;
            return Ok(LoginChallenge::magic_link(url, MAGIC_LINK_TTL));
        }
        let store = store.ok_or(AuthError::InvalidProviderConfig(
            "passwordless login requires AuthConf::passwordless_store".into(),
        ))?;
        let kind = self.kind()?;
        let mut prepared = PreparedChallenge::new(
            method,
            target,
            email.as_str(),
            user.as_ref(),
            kind,
            codec,
            secrets,
        )?;
        let challenge = prepared.take_challenge()?;
        let start = store.begin(challenge).await?;
        validate_start(&prepared, &start)?;
        Ok(prepared.challenge_response(start, false))
    }

    async fn complete_inner(
        &self,
        method: &LoginMethodId,
        input: BoxLoginInput,
        codec: &ChallengeCodec,
        secrets: &SecretRing,
        store: Option<&PasswordlessStoreRuntime>,
    ) -> Result<CompletedLogin, AuthError> {
        let state = if self.conf.stateful {
            let store = store.ok_or(AuthError::InvalidLoginState)?;
            let proof = self.proof(input)?;
            consume(store, &proof, secrets).await?
        } else {
            let callback = input
                .downcast::<MagicLinkCallback>()
                .map_err(|_| AuthError::InvalidCredential)?;
            callback.token().to_owned()
        };
        let (pending, target) = open_pending(codec, method, &state)?;
        let email = EmailAddress::new(pending.identifier.clone());
        let user = self
            .conf
            .resolver
            .resolve(&email)
            .await?
            .ok_or(AuthError::InvalidCredential)?;
        complete_user(user, pending, target, "email").await
    }

    fn kind(&self) -> Result<ChallengeKind, AuthError> {
        self.conf
            .callback_url
            .as_ref()
            .map(|value| ChallengeKind::MagicLink {
                callback_url: value.clone(),
            })
            .ok_or_else(|| {
                AuthError::InvalidProviderConfig("magic-link login requires callback_url".into())
            })
    }

    fn stateless_link(
        &self,
        method: &LoginMethodId,
        target: LoginTarget,
        email: &EmailAddress,
        user: &AuthUser,
        codec: &ChallengeCodec,
    ) -> Result<String, AuthError> {
        let callback = self.callback_url()?;
        let pending = PendingPasswordless {
            identifier: email.as_str().into(),
            subject: Some(user.subject().to_owned()),
            method: method.as_str().into(),
            channel: "email".into(),
        };
        let state = SealedLoginState {
            version: 3,
            state_id: uuid::Uuid::new_v4().to_string(),
            method: method.as_str().into(),
            target,
            expires_at: Utc::now().timestamp() + MAGIC_LINK_TTL,
            payload: serde_json::to_value(pending).map_err(|_| AuthError::InvalidLoginState)?,
        };
        append_raw_token(&callback, &codec.seal(&state)?)
    }

    fn callback_url(&self) -> Result<String, AuthError> {
        self.conf
            .callback_url
            .clone()
            .ok_or(AuthError::InvalidLoginState)
    }
    fn proof(&self, input: BoxLoginInput) -> Result<ProofInput, AuthError> {
        let callback = input
            .downcast::<MagicLinkCallback>()
            .map_err(|_| AuthError::InvalidCredential)?;
        let (id, proof) = callback.parts()?;
        Ok(ProofInput::new(id, proof.as_bytes()))
    }
}

impl OtpRuntime {
    async fn begin_inner(
        &self,
        method: &LoginMethodId,
        input: BoxLoginInput,
        target: LoginTarget,
        codec: &ChallengeCodec,
        secrets: &SecretRing,
        store: Option<&PasswordlessStoreRuntime>,
    ) -> Result<LoginChallenge, AuthError> {
        let address = *input
            .downcast::<PasswordlessAddress>()
            .map_err(|_| AuthError::InvalidCredential)?;
        address.validate()?;
        let store = store.ok_or(AuthError::InvalidProviderConfig(
            "passwordless login requires AuthConf::passwordless_store".into(),
        ))?;
        let user = self.conf.resolver.resolve(&address).await?;
        let mut prepared = PreparedChallenge::new(
            method,
            target,
            address.as_str(),
            user.as_ref(),
            ChallengeKind::Otp {
                channel: address.channel(),
                policy: self.conf.policy,
            },
            codec,
            secrets,
        )?;
        let challenge = prepared.take_challenge()?;
        let start = store.begin(challenge).await?;
        validate_start(&prepared, &start)?;
        Ok(prepared.challenge_response(start, user.is_some()))
    }

    async fn complete_inner(
        &self,
        method: &LoginMethodId,
        input: BoxLoginInput,
        codec: &ChallengeCodec,
        secrets: &SecretRing,
        store: Option<&PasswordlessStoreRuntime>,
    ) -> Result<CompletedLogin, AuthError> {
        let store = store.ok_or(AuthError::InvalidLoginState)?;
        let response = input
            .downcast::<Otp>()
            .map_err(|_| AuthError::InvalidCredential)?;
        response.0.validate()?;
        let proof = ProofInput::new(
            response.0.challenge_token.clone(),
            response.0.code.as_bytes(),
        );
        let state = consume(store, &proof, secrets).await?;
        let (pending, target) = open_pending(codec, method, &state)?;
        let address =
            PasswordlessAddress::from_parts(&pending.channel, pending.identifier.clone())?;
        let user = self
            .conf
            .resolver
            .resolve(&address)
            .await?
            .ok_or(AuthError::InvalidCredential)?;
        complete_user(user, pending, target, "otp").await
    }
}

async fn complete_user(
    user: AuthUser,
    pending: PendingPasswordless,
    target: LoginTarget,
    method: &str,
) -> Result<CompletedLogin, AuthError> {
    if pending.subject.as_deref() != Some(user.subject()) {
        return Err(AuthError::InvalidCredential);
    }
    user.validate()?;
    Ok(CompletedLogin {
        login: VerifiedLogin::new(user, method),
        target,
    })
}

async fn consume(
    store: &PasswordlessStoreRuntime,
    proof: &ProofInput,
    secrets: &SecretRing,
) -> Result<String, AuthError> {
    valid_id(&proof.id)?;
    let proofs = proof_digests(secrets, &proof.id, &proof.value)?;
    match store.attempt(&proof.id, &proofs).await? {
        PasswordlessAttempt::Accepted { state } => Ok(state),
        PasswordlessAttempt::Rejected => Err(AuthError::InvalidCredential),
    }
}

/// Rejects a store result that would send proof material for a different challenge.
fn validate_start(
    prepared: &PreparedChallenge,
    start: &PasswordlessStart,
) -> Result<(), AuthError> {
    if start.should_issue() && start.challenge_id() != prepared.id() {
        return Err(AuthError::Internal(
            "passwordless store returned a mismatched proof challenge".into(),
        ));
    }
    Ok(())
}

fn open_pending(
    codec: &ChallengeCodec,
    method: &LoginMethodId,
    sealed: &str,
) -> Result<(PendingPasswordless, LoginTarget), AuthError> {
    let state: SealedLoginState = codec.open(sealed)?;
    if state.method != method.as_str() || state.expires_at < Utc::now().timestamp() {
        return Err(AuthError::InvalidLoginState);
    }
    let pending =
        serde_json::from_value(state.payload.clone()).map_err(|_| AuthError::InvalidLoginState)?;
    Ok((pending, state.target))
}

enum ChallengeKind {
    #[cfg(feature = "email")]
    MagicLink { callback_url: String },
    Otp {
        channel: &'static str,
        policy: OtpPolicy,
    },
}

impl ChallengeKind {
    fn channel(&self) -> &'static str {
        match self {
            #[cfg(feature = "email")]
            Self::MagicLink { .. } => "email",
            Self::Otp { channel, .. } => channel,
        }
    }
}

struct PreparedChallenge {
    id: String,
    challenge: Option<PasswordlessChallenge>,
    kind: ChallengeKind,
    code: Option<String>,
    #[cfg(feature = "email")]
    link: Option<String>,
}

impl PreparedChallenge {
    fn new(
        method: &LoginMethodId,
        target: LoginTarget,
        identifier: &str,
        user: Option<&AuthUser>,
        kind: ChallengeKind,
        codec: &ChallengeCodec,
        secrets: &SecretRing,
    ) -> Result<Self, AuthError> {
        let now = Utc::now().timestamp();
        let id = uuid::Uuid::new_v4().to_string();
        let (proof, code, _link, ttl) = proof_for(&id, &kind)?;
        let pending = PendingPasswordless {
            identifier: identifier.into(),
            subject: user.map(|value| value.subject().to_owned()),
            method: method.as_str().into(),
            channel: kind.channel().into(),
        };
        let state = SealedLoginState {
            version: 3,
            state_id: id.clone(),
            method: method.as_str().into(),
            target,
            expires_at: now + ttl,
            payload: serde_json::to_value(pending).map_err(|_| AuthError::InvalidLoginState)?,
        };
        let sealed = codec.seal(&state)?;
        let challenge = PasswordlessChallenge::new(PasswordlessChallengeParts {
            id: id.clone(),
            principal: principal_digest(secrets, method.as_str(), identifier)?,
            proofs: proof_digests(secrets, &id, proof.as_bytes())?,
            state: sealed,
            expires_at: now + ttl,
            attempts: OTP_ATTEMPTS,
            next_issue_at: now + RESEND_DELAY,
            issue_window_ends_at: now + DELIVERY_WINDOW,
            issue_limit: DELIVERY_LIMIT,
        });
        Ok(Self {
            id,
            challenge: Some(challenge),
            kind,
            code,
            #[cfg(feature = "email")]
            link: _link,
        })
    }

    fn id(&self) -> &str {
        &self.id
    }
    fn take_challenge(&mut self) -> Result<PasswordlessChallenge, AuthError> {
        self.challenge.take().ok_or(AuthError::InvalidLoginState)
    }
    fn challenge_response(self, start: PasswordlessStart, expose_code: bool) -> LoginChallenge {
        let expires = start.expires_at().saturating_sub(Utc::now().timestamp());
        let resend = start.next_issue_at().saturating_sub(Utc::now().timestamp());
        match self.kind {
            #[cfg(feature = "email")]
            ChallengeKind::MagicLink { .. } => LoginChallenge::magic_link(self.link, expires),
            ChallengeKind::Otp { channel, .. } => {
                let delivery = (start.should_issue() && expose_code)
                    .then(|| self.code.map(|code| OtpDelivery::new(code, expires)))
                    .flatten();
                LoginChallenge::code(
                    start.challenge_id().into(),
                    channel,
                    expires,
                    resend,
                    delivery,
                )
            }
        }
    }
}

fn proof_for(
    _id: &str,
    kind: &ChallengeKind,
) -> Result<(String, Option<String>, Option<String>, i64), AuthError> {
    match kind {
        #[cfg(feature = "email")]
        ChallengeKind::MagicLink { callback_url } => {
            let proof = random_secret(32)?;
            let link = append_token(callback_url, _id, &proof)?;
            Ok((proof, None, Some(link), MAGIC_LINK_TTL))
        }
        ChallengeKind::Otp { policy, .. } => {
            let code = random_code(*policy)?;
            Ok((code.clone(), Some(code), None, OTP_TTL))
        }
    }
}

#[cfg(feature = "email")]
fn append_token(callback: &str, id: &str, proof: &str) -> Result<String, AuthError> {
    let mut url = url::Url::parse(callback)
        .map_err(|_| AuthError::InvalidProviderConfig("invalid magic-link callback URL".into()))?;
    url.query_pairs_mut()
        .append_pair("token", &format!("{id}.{proof}"));
    Ok(url.into())
}

#[cfg(feature = "email")]
fn append_raw_token(callback: &str, token: &str) -> Result<String, AuthError> {
    let mut url = url::Url::parse(callback)
        .map_err(|_| AuthError::InvalidProviderConfig("invalid magic-link callback URL".into()))?;
    url.query_pairs_mut().append_pair("token", token);
    Ok(url.into())
}

#[cfg(feature = "email")]
fn random_secret(length: usize) -> Result<String, AuthError> {
    let mut bytes = vec![0_u8; length];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| AuthError::Internal("passwordless randomness failed".into()))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn principal_digest(
    secrets: &SecretRing,
    method: &str,
    identifier: &str,
) -> Result<Vec<u8>, AuthError> {
    let key = secrets.derived_active(&KeySource::site_secret(), PRINCIPAL_CONTEXT, 32)?;
    Ok(hmac::sign(
        &hmac::Key::new(hmac::HMAC_SHA256, &key),
        format!("{method}\0{identifier}").as_bytes(),
    )
    .as_ref()
    .to_vec())
}

fn proof_digests(secrets: &SecretRing, id: &str, value: &[u8]) -> Result<Vec<Vec<u8>>, AuthError> {
    secrets
        .derived_verification(&KeySource::site_secret(), PROOF_CONTEXT, 32)?
        .into_iter()
        .map(|key| {
            Ok(hmac::sign(
                &hmac::Key::new(hmac::HMAC_SHA256, &key),
                &[id.as_bytes(), b"\0", value].concat(),
            )
            .as_ref()
            .to_vec())
        })
        .collect()
}

fn valid_id(value: &str) -> Result<(), AuthError> {
    uuid::Uuid::parse_str(value)
        .map(|_| ())
        .map_err(|_| AuthError::InvalidLoginState)
}

#[cfg(feature = "email")]
fn validate_magic_link_conf(value: &MagicLinkLogin) -> Result<(), AuthError> {
    validate_callback(value.callback_url.as_deref())
}

#[cfg(feature = "email")]
fn validate_callback(value: Option<&str>) -> Result<(), AuthError> {
    let value = value.ok_or_else(|| {
        AuthError::InvalidProviderConfig("magic-link login requires callback_url".into())
    })?;
    let url = url::Url::parse(value)
        .map_err(|_| AuthError::InvalidProviderConfig("invalid magic-link callback URL".into()))?;
    let loopback = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !(url.scheme() == "https" || loopback)
    {
        return Err(AuthError::InvalidProviderConfig(
            "magic-link callback URL must be absolute HTTPS without query or fragment".into(),
        ));
    }
    Ok(())
}

#[cfg(feature = "email")]
impl LoginProviderDefinition<EmailAddress, MagicLinkCallback> for MagicLinkLogin {
    fn define(self) -> LoginRuntimeDefinition {
        LoginRuntimeDefinition {
            runtime: Arc::new(MagicLinkRuntime { conf: self }),
        }
    }
}
#[cfg(feature = "email")]
impl super::model::definition_sealed::Sealed for MagicLinkLogin {}

impl LoginProviderDefinition<PasswordlessAddress, Otp> for OtpLogin {
    fn define(self) -> LoginRuntimeDefinition {
        LoginRuntimeDefinition {
            runtime: Arc::new(OtpRuntime { conf: self }),
        }
    }
}
impl super::model::definition_sealed::Sealed for OtpLogin {}
