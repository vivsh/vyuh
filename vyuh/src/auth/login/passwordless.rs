//! Durable passwordless email and phone login methods.

use std::{fmt, future::Future, sync::Arc};

#[cfg(feature = "email")]
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Utc;
use futures::future::BoxFuture;
use ring::{hmac, rand::SecureRandom};
use serde::{Deserialize, Serialize};

use super::{
    BoxLoginInput, ChallengeCodec, ErasedLoginRuntime, LoginChallenge, LoginCompletion,
    LoginMethodId, LoginProviderDefinition, LoginRuntimeDefinition, LoginTarget, SealedLoginState,
    VerifiedLogin,
    runtime::{CompletedLogin, LoginFuture, completion_sealed},
};
use crate::auth::{AuthError, AuthUser, KeySource, SecretRing};

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

/// A redacted email one-time-password completion response.
#[cfg(feature = "email")]
pub struct EmailOtp(OtpResponse);

#[cfg(feature = "email")]
impl EmailOtp {
    /// Creates email OTP completion input from the opaque challenge and delivered code.
    pub fn new(challenge_token: impl Into<String>, code: impl Into<String>) -> Self {
        Self(OtpResponse::new(challenge_token, code))
    }
}

#[cfg(feature = "email")]
impl completion_sealed::Sealed for EmailOtp {}
#[cfg(feature = "email")]
impl LoginCompletion for EmailOtp {}

#[cfg(feature = "email")]
impl fmt::Debug for EmailOtp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EmailOtp(<redacted>)")
    }
}

/// A redacted phone one-time-password completion response.
pub struct PhoneOtp(OtpResponse);

impl PhoneOtp {
    /// Creates phone OTP completion input from the opaque challenge and delivered code.
    pub fn new(challenge_token: impl Into<String>, code: impl Into<String>) -> Self {
        Self(OtpResponse::new(challenge_token, code))
    }
}

impl completion_sealed::Sealed for PhoneOtp {}
impl LoginCompletion for PhoneOtp {}

impl fmt::Debug for PhoneOtp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PhoneOtp(<redacted>)")
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
        (self.code.len() == 6 && self.code.bytes().all(|value| value.is_ascii_digit()))
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

/// Resolves an existing application identity from a canonical phone number.
pub trait PhoneLoginResolver: Send + Sync + 'static {
    /// Returns the current account identity, or `None` when no account is linked.
    fn resolve<'a>(
        &'a self,
        phone: &'a PhoneNumber,
    ) -> impl Future<Output = Result<Option<AuthUser>, AuthError>> + Send + 'a;
}

/// Delivers a phone one-time password through an application-selected channel.
pub trait PhoneOtpSender: Send + Sync + 'static {
    /// Sends one code without logging or retaining its secret value.
    fn send<'a>(
        &'a self,
        phone: &'a PhoneNumber,
        message: &'a PhoneOtpMessage,
    ) -> impl Future<Output = Result<(), AuthError>> + Send + 'a;
}

/// Delivers an email one-time password through an application-selected channel.
#[cfg(feature = "email")]
pub trait EmailOtpSender: Send + Sync + 'static {
    /// Sends one code without logging or retaining its secret value.
    fn send<'a>(
        &'a self,
        email: &'a EmailAddress,
        message: &'a EmailOtpMessage,
    ) -> impl Future<Output = Result<(), AuthError>> + Send + 'a;
}

/// A redacted email OTP delivery message.
#[cfg(feature = "email")]
pub struct EmailOtpMessage {
    code: String,
    expires_in: i64,
}

#[cfg(feature = "email")]
impl EmailOtpMessage {
    /// Exposes the OTP only to the configured delivery adapter.
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Returns the number of seconds before the code expires.
    pub const fn expires_in(&self) -> i64 {
        self.expires_in
    }
}

#[cfg(feature = "email")]
impl fmt::Debug for EmailOtpMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("EmailOtpMessage(<redacted>)")
    }
}

/// A redacted phone OTP delivery message.
pub struct PhoneOtpMessage {
    code: String,
    expires_in: i64,
}

impl PhoneOtpMessage {
    /// Exposes the OTP only to the configured delivery adapter.
    pub fn code(&self) -> &str {
        &self.code
    }

    /// Returns the number of seconds before the code expires.
    pub const fn expires_in(&self) -> i64 {
        self.expires_in
    }
}

impl fmt::Debug for PhoneOtpMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PhoneOtpMessage(<redacted>)")
    }
}

/// Opaque challenge state persisted by a durable passwordless store.
pub struct PasswordlessChallenge {
    id: String,
    principal: Vec<u8>,
    proofs: Vec<Vec<u8>>,
    state: String,
    expires_at: i64,
    attempts: u8,
    resend_at: i64,
    delivery_window_ends_at: i64,
    delivery_limit: u8,
}

impl PasswordlessChallenge {
    /// Returns the opaque challenge identifier.
    pub fn id(&self) -> &str {
        &self.id
    }
    /// Returns a keyed principal digest suitable only for store indexing.
    pub fn principal(&self) -> &[u8] {
        &self.principal
    }
    /// Returns active and fallback keyed proof digests for atomic verification.
    pub fn proofs(&self) -> &[Vec<u8>] {
        &self.proofs
    }
    /// Returns encrypted completion state without exposing identity fields.
    pub fn state(&self) -> &str {
        &self.state
    }
    /// Returns the absolute UTC expiry timestamp.
    pub const fn expires_at(&self) -> i64 {
        self.expires_at
    }
    /// Returns the maximum invalid proof attempts.
    pub const fn attempts(&self) -> u8 {
        self.attempts
    }
    /// Returns the earliest UTC timestamp for another delivery.
    pub const fn resend_at(&self) -> i64 {
        self.resend_at
    }
    /// Returns the end of the delivery-rate window.
    pub const fn delivery_window_ends_at(&self) -> i64 {
        self.delivery_window_ends_at
    }
    /// Returns the maximum deliveries within one window.
    pub const fn delivery_limit(&self) -> u8 {
        self.delivery_limit
    }
}

impl fmt::Debug for PasswordlessChallenge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PasswordlessChallenge(<redacted>)")
    }
}

/// The durable store's atomic result for challenge creation or resend suppression.
pub struct PasswordlessStart {
    challenge_id: String,
    expires_at: i64,
    resend_at: i64,
    deliver: bool,
}

impl PasswordlessStart {
    /// Creates a store result for one active passwordless challenge.
    pub fn new(
        challenge_id: impl Into<String>,
        expires_at: i64,
        resend_at: i64,
        deliver: bool,
    ) -> Self {
        Self {
            challenge_id: challenge_id.into(),
            expires_at,
            resend_at,
            deliver,
        }
    }
    /// Returns the opaque challenge identifier used by OTP completion.
    pub fn challenge_id(&self) -> &str {
        &self.challenge_id
    }
    /// Returns the challenge expiry timestamp.
    pub const fn expires_at(&self) -> i64 {
        self.expires_at
    }
    /// Returns the next permitted delivery timestamp.
    pub const fn resend_at(&self) -> i64 {
        self.resend_at
    }
    /// Returns whether this request should deliver a fresh message.
    pub const fn should_deliver(&self) -> bool {
        self.deliver
    }
}

impl fmt::Debug for PasswordlessStart {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PasswordlessStart(<redacted>)")
    }
}

/// The atomic result of one passwordless proof attempt.
pub enum PasswordlessAttempt {
    /// The proof matched and the store consumed the challenge exactly once.
    Accepted { state: String },
    /// The proof was invalid, expired, exhausted, or already consumed.
    Rejected,
}

impl PasswordlessAttempt {
    /// Creates an accepted store result with opaque sealed state.
    pub fn accepted(state: impl Into<String>) -> Self {
        Self::Accepted {
            state: state.into(),
        }
    }
}

impl fmt::Debug for PasswordlessAttempt {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PasswordlessAttempt(<redacted>)")
    }
}

/// Durable, atomic storage for passwordless challenges.
pub trait PasswordlessStore: Send + Sync + 'static {
    /// Starts or suppresses delivery for one principal without exposing that principal.
    fn begin<'a>(
        &'a self,
        challenge: PasswordlessChallenge,
    ) -> impl Future<Output = Result<PasswordlessStart, AuthError>> + Send + 'a;
    /// Verifies one proof, decrements attempts on failure, and consumes success atomically.
    fn attempt<'a>(
        &'a self,
        challenge_id: &'a str,
        proofs: &'a [Vec<u8>],
    ) -> impl Future<Output = Result<PasswordlessAttempt, AuthError>> + Send + 'a;
    /// Removes a challenge that could not be delivered.
    fn discard<'a>(
        &'a self,
        challenge_id: &'a str,
    ) -> impl Future<Output = Result<(), AuthError>> + Send + 'a;
}

trait ErasedPasswordlessStore: Send + Sync {
    fn begin<'a>(
        &'a self,
        challenge: PasswordlessChallenge,
    ) -> BoxFuture<'a, Result<PasswordlessStart, AuthError>>;
    fn attempt<'a>(
        &'a self,
        id: &'a str,
        proofs: &'a [Vec<u8>],
    ) -> BoxFuture<'a, Result<PasswordlessAttempt, AuthError>>;
    fn discard<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), AuthError>>;
}

impl<T: PasswordlessStore> ErasedPasswordlessStore for T {
    fn begin<'a>(
        &'a self,
        challenge: PasswordlessChallenge,
    ) -> BoxFuture<'a, Result<PasswordlessStart, AuthError>> {
        Box::pin(PasswordlessStore::begin(self, challenge))
    }
    fn attempt<'a>(
        &'a self,
        id: &'a str,
        proofs: &'a [Vec<u8>],
    ) -> BoxFuture<'a, Result<PasswordlessAttempt, AuthError>> {
        Box::pin(PasswordlessStore::attempt(self, id, proofs))
    }
    fn discard<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), AuthError>> {
        Box::pin(PasswordlessStore::discard(self, id))
    }
}

#[derive(Clone)]
pub(crate) struct PasswordlessStoreRuntime(Arc<dyn ErasedPasswordlessStore>);

impl fmt::Debug for PasswordlessStoreRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PasswordlessStore(<redacted>)")
    }
}

impl PasswordlessStoreRuntime {
    pub(crate) fn new(value: impl PasswordlessStore) -> Self {
        Self(Arc::new(value))
    }
    pub(crate) async fn begin(
        &self,
        challenge: PasswordlessChallenge,
    ) -> Result<PasswordlessStart, AuthError> {
        self.0.begin(challenge).await
    }
    pub(crate) async fn attempt(
        &self,
        id: &str,
        proofs: &[Vec<u8>],
    ) -> Result<PasswordlessAttempt, AuthError> {
        self.0.attempt(id, proofs).await
    }
    pub(crate) async fn discard(&self, id: &str) -> Result<(), AuthError> {
        self.0.discard(id).await
    }
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

trait ErasedPhoneResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        phone: &'a PhoneNumber,
    ) -> BoxFuture<'a, Result<Option<AuthUser>, AuthError>>;
}

impl<T: PhoneLoginResolver> ErasedPhoneResolver for T {
    fn resolve<'a>(
        &'a self,
        phone: &'a PhoneNumber,
    ) -> BoxFuture<'a, Result<Option<AuthUser>, AuthError>> {
        Box::pin(PhoneLoginResolver::resolve(self, phone))
    }
}

trait ErasedPhoneSender: Send + Sync {
    fn send<'a>(
        &'a self,
        phone: &'a PhoneNumber,
        message: &'a PhoneOtpMessage,
    ) -> BoxFuture<'a, Result<(), AuthError>>;
}

impl<T: PhoneOtpSender> ErasedPhoneSender for T {
    fn send<'a>(
        &'a self,
        phone: &'a PhoneNumber,
        message: &'a PhoneOtpMessage,
    ) -> BoxFuture<'a, Result<(), AuthError>> {
        Box::pin(PhoneOtpSender::send(self, phone, message))
    }
}

#[cfg(feature = "email")]
trait ErasedEmailSender: Send + Sync {
    fn send<'a>(
        &'a self,
        email: &'a EmailAddress,
        message: &'a EmailOtpMessage,
    ) -> BoxFuture<'a, Result<(), AuthError>>;
}

#[cfg(feature = "email")]
impl<T: EmailOtpSender> ErasedEmailSender for T {
    fn send<'a>(
        &'a self,
        email: &'a EmailAddress,
        message: &'a EmailOtpMessage,
    ) -> BoxFuture<'a, Result<(), AuthError>> {
        Box::pin(EmailOtpSender::send(self, email, message))
    }
}

/// Configures passwordless email magic-link or OTP login.
#[cfg(feature = "email")]
#[derive(Clone)]
pub struct EmailLogin {
    resolver: Arc<dyn ErasedEmailResolver>,
    kind: EmailKind,
}

#[cfg(feature = "email")]
#[derive(Clone)]
enum EmailKind {
    MagicLink {
        callback_url: Option<String>,
        stateful: bool,
    },
    Otp {
        sender: Arc<dyn ErasedEmailSender>,
    },
}

#[cfg(feature = "email")]
impl EmailLogin {
    /// Creates a delivery-agnostic magic-link method with stateless verification.
    pub fn magic_link(resolver: impl EmailLoginResolver) -> Self {
        Self {
            resolver: Arc::new(resolver),
            kind: EmailKind::MagicLink {
                callback_url: None,
                stateful: false,
            },
        }
    }
    /// Creates an email OTP method with application-owned delivery.
    pub fn otp(resolver: impl EmailLoginResolver, sender: impl EmailOtpSender) -> Self {
        Self {
            resolver: Arc::new(resolver),
            kind: EmailKind::Otp {
                sender: Arc::new(sender),
            },
        }
    }
    /// Sets the absolute callback URL used only for generated magic links.
    pub fn callback_url(mut self, value: impl Into<String>) -> Self {
        self.kind = match self.kind {
            EmailKind::MagicLink { stateful, .. } => EmailKind::MagicLink {
                callback_url: Some(value.into()),
                stateful,
            },
            EmailKind::Otp { sender } => EmailKind::Otp { sender },
        };
        self
    }

    /// Requires durable one-time verification for generated magic links.
    pub fn stateful(mut self) -> Self {
        self.kind = match self.kind {
            EmailKind::MagicLink { callback_url, .. } => EmailKind::MagicLink {
                callback_url,
                stateful: true,
            },
            EmailKind::Otp { sender } => EmailKind::Otp { sender },
        };
        self
    }

    /// Uses a sealed link that remains reusable until expiry.
    pub fn stateless(mut self) -> Self {
        self.kind = match self.kind {
            EmailKind::MagicLink { callback_url, .. } => EmailKind::MagicLink {
                callback_url,
                stateful: false,
            },
            EmailKind::Otp { sender } => EmailKind::Otp { sender },
        };
        self
    }
}

/// Configures passwordless phone OTP login through an application-owned sender.
#[derive(Clone)]
pub struct PhoneLogin {
    resolver: Arc<dyn ErasedPhoneResolver>,
    sender: Arc<dyn ErasedPhoneSender>,
}

impl PhoneLogin {
    /// Creates a phone OTP login method with application-owned identity lookup and delivery.
    pub fn otp(resolver: impl PhoneLoginResolver, sender: impl PhoneOtpSender) -> Self {
        Self {
            resolver: Arc::new(resolver),
            sender: Arc::new(sender),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct PendingPasswordless {
    identifier: String,
    subject: Option<String>,
    method: String,
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
struct EmailRuntime {
    conf: EmailLogin,
}

struct PhoneRuntime {
    conf: PhoneLogin,
}

#[cfg(feature = "email")]
impl ErasedLoginRuntime for EmailRuntime {
    fn is_flow(&self) -> bool {
        true
    }
    fn validate(&self) -> Result<(), AuthError> {
        validate_email_conf(&self.conf)
    }
    fn requires_passwordless_store(&self) -> bool {
        !matches!(
            self.conf.kind,
            EmailKind::MagicLink {
                stateful: false,
                ..
            }
        )
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

impl ErasedLoginRuntime for PhoneRuntime {
    fn is_flow(&self) -> bool {
        true
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
impl EmailRuntime {
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
        if !self.is_stateful() {
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
        if start.should_deliver() && user.is_some() {
            self.deliver(&email, &prepared, store).await?;
        }
        Ok(prepared.challenge_response(start))
    }

    async fn complete_inner(
        &self,
        method: &LoginMethodId,
        input: BoxLoginInput,
        codec: &ChallengeCodec,
        secrets: &SecretRing,
        store: Option<&PasswordlessStoreRuntime>,
    ) -> Result<CompletedLogin, AuthError> {
        let state = if self.is_stateful() {
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
        match &self.conf.kind {
            EmailKind::MagicLink { callback_url, .. } => callback_url
                .as_ref()
                .map(|value| ChallengeKind::MagicLink {
                    callback_url: value.clone(),
                })
                .ok_or_else(|| {
                    AuthError::InvalidProviderConfig(
                        "magic-link login requires callback_url".into(),
                    )
                }),
            EmailKind::Otp { .. } => Ok(ChallengeKind::Otp { channel: "email" }),
        }
    }

    fn is_stateful(&self) -> bool {
        matches!(
            self.conf.kind,
            EmailKind::MagicLink { stateful: true, .. } | EmailKind::Otp { .. }
        )
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
            subject: Some(user.key.to_string()),
            method: method.as_str().into(),
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
        match &self.conf.kind {
            EmailKind::MagicLink {
                callback_url: Some(value),
                ..
            } => Ok(value.clone()),
            _ => Err(AuthError::InvalidLoginState),
        }
    }
    fn proof(&self, input: BoxLoginInput) -> Result<ProofInput, AuthError> {
        match &self.conf.kind {
            EmailKind::MagicLink { .. } => {
                let callback = input
                    .downcast::<MagicLinkCallback>()
                    .map_err(|_| AuthError::InvalidCredential)?;
                let (id, proof) = callback.parts()?;
                Ok(ProofInput::new(id, proof.as_bytes()))
            }
            EmailKind::Otp { .. } => {
                let response = input
                    .downcast::<EmailOtp>()
                    .map_err(|_| AuthError::InvalidCredential)?;
                response.0.validate()?;
                Ok(ProofInput::new(
                    response.0.challenge_token.clone(),
                    response.0.code.as_bytes(),
                ))
            }
        }
    }

    async fn deliver(
        &self,
        email: &EmailAddress,
        prepared: &PreparedChallenge,
        store: &PasswordlessStoreRuntime,
    ) -> Result<(), AuthError> {
        let EmailKind::Otp { sender } = &self.conf.kind else {
            return Ok(());
        };
        let message = EmailOtpMessage {
            code: prepared.code.clone().ok_or(AuthError::InvalidCredential)?,
            expires_in: OTP_TTL,
        };
        if sender.send(email, &message).await.is_err() {
            tracing::warn!(method = %prepared.method, "passwordless email delivery failed");
            store.discard(prepared.id()).await?;
        }
        Ok(())
    }
}

impl PhoneRuntime {
    async fn begin_inner(
        &self,
        method: &LoginMethodId,
        input: BoxLoginInput,
        target: LoginTarget,
        codec: &ChallengeCodec,
        secrets: &SecretRing,
        store: Option<&PasswordlessStoreRuntime>,
    ) -> Result<LoginChallenge, AuthError> {
        let phone = *input
            .downcast::<PhoneNumber>()
            .map_err(|_| AuthError::InvalidCredential)?;
        phone.validate()?;
        let store = store.ok_or(AuthError::InvalidProviderConfig(
            "passwordless login requires AuthConf::passwordless_store".into(),
        ))?;
        let user = self.conf.resolver.resolve(&phone).await?;
        let mut prepared = PreparedChallenge::new(
            method,
            target,
            phone.as_str(),
            user.as_ref(),
            ChallengeKind::Otp { channel: "phone" },
            codec,
            secrets,
        )?;
        let challenge = prepared.take_challenge()?;
        let start = store.begin(challenge).await?;
        validate_start(&prepared, &start)?;
        if start.should_deliver() && user.is_some() {
            self.deliver(&phone, &prepared, store).await?;
        }
        Ok(prepared.challenge_response(start))
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
            .downcast::<PhoneOtp>()
            .map_err(|_| AuthError::InvalidCredential)?;
        response.0.validate()?;
        let proof = ProofInput::new(
            response.0.challenge_token.clone(),
            response.0.code.as_bytes(),
        );
        let state = consume(store, &proof, secrets).await?;
        let (pending, target) = open_pending(codec, method, &state)?;
        let phone = PhoneNumber::new(pending.identifier.clone());
        let user = self
            .conf
            .resolver
            .resolve(&phone)
            .await?
            .ok_or(AuthError::InvalidCredential)?;
        complete_user(user, pending, target, "phone").await
    }

    async fn deliver(
        &self,
        phone: &PhoneNumber,
        prepared: &PreparedChallenge,
        store: &PasswordlessStoreRuntime,
    ) -> Result<(), AuthError> {
        let message = PhoneOtpMessage {
            code: prepared
                .code
                .clone()
                .ok_or_else(|| AuthError::Internal("phone OTP missing code".into()))?,
            expires_in: OTP_TTL,
        };
        if self.conf.sender.send(phone, &message).await.is_err() {
            tracing::warn!(method = %prepared.method, "passwordless phone delivery failed");
            store.discard(prepared.id()).await?;
        }
        Ok(())
    }
}

async fn complete_user(
    user: AuthUser,
    pending: PendingPasswordless,
    target: LoginTarget,
    method: &str,
) -> Result<CompletedLogin, AuthError> {
    if pending.subject.as_deref() != Some(user.key.as_ref()) {
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
    if start.should_deliver() && start.challenge_id() != prepared.id() {
        return Err(AuthError::Internal(
            "passwordless store returned a mismatched delivery challenge".into(),
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
    MagicLink {
        callback_url: String,
    },
    Otp {
        channel: &'static str,
    },
}

struct PreparedChallenge {
    id: String,
    challenge: Option<PasswordlessChallenge>,
    method: String,
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
            subject: user.map(|value| value.key.to_string()),
            method: method.as_str().into(),
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
        let challenge = PasswordlessChallenge {
            id: id.clone(),
            principal: principal_digest(secrets, method.as_str(), identifier)?,
            proofs: proof_digests(secrets, &id, proof.as_bytes())?,
            state: sealed,
            expires_at: now + ttl,
            attempts: OTP_ATTEMPTS,
            resend_at: now + RESEND_DELAY,
            delivery_window_ends_at: now + DELIVERY_WINDOW,
            delivery_limit: DELIVERY_LIMIT,
        };
        Ok(Self {
            id,
            challenge: Some(challenge),
            method: method.as_str().into(),
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
    fn challenge_response(self, start: PasswordlessStart) -> LoginChallenge {
        let expires = start.expires_at().saturating_sub(Utc::now().timestamp());
        let resend = start.resend_at().saturating_sub(Utc::now().timestamp());
        match self.kind {
            #[cfg(feature = "email")]
            ChallengeKind::MagicLink { .. } => LoginChallenge::magic_link(self.link, expires),
            ChallengeKind::Otp { channel } => {
                LoginChallenge::code(start.challenge_id().into(), channel, expires, resend)
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
        ChallengeKind::Otp { .. } => {
            let code = random_code()?;
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

fn random_code() -> Result<String, AuthError> {
    let mut bytes = [0_u8; 4];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| AuthError::Internal("passwordless randomness failed".into()))?;
    let value = u32::from_be_bytes(bytes) % 1_000_000;
    Ok(format!("{value:06}"))
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
fn validate_email_conf(value: &EmailLogin) -> Result<(), AuthError> {
    match &value.kind {
        EmailKind::MagicLink { callback_url, .. } => validate_callback(callback_url.as_deref()),
        EmailKind::Otp { .. } => Ok(()),
    }
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
impl LoginProviderDefinition<EmailAddress, MagicLinkCallback> for EmailLogin {
    fn define(self) -> LoginRuntimeDefinition {
        LoginRuntimeDefinition {
            runtime: Arc::new(EmailRuntime { conf: self }),
        }
    }
}
#[cfg(feature = "email")]
impl LoginProviderDefinition<EmailAddress, EmailOtp> for EmailLogin {
    fn define(self) -> LoginRuntimeDefinition {
        LoginRuntimeDefinition {
            runtime: Arc::new(EmailRuntime { conf: self }),
        }
    }
}
#[cfg(feature = "email")]
impl super::model::definition_sealed::Sealed for EmailLogin {}

impl LoginProviderDefinition<PhoneNumber, PhoneOtp> for PhoneLogin {
    fn define(self) -> LoginRuntimeDefinition {
        LoginRuntimeDefinition {
            runtime: Arc::new(PhoneRuntime { conf: self }),
        }
    }
}
impl super::model::definition_sealed::Sealed for PhoneLogin {}
