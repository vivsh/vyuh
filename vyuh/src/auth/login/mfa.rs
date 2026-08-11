//! Password-first multi-factor login with sealed continuation state.

use std::{future::Future, marker::PhantomData, sync::Arc};

use chrono::{Duration, Utc};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};

use super::{
    BasicCredentials, BasicLogin, BoxLoginInput, ChallengeCodec, ErasedLoginRuntime,
    LoginChallenge, LoginCompletion, LoginMethodId, LoginProviderDefinition,
    LoginRuntimeDefinition, LoginTarget,
    MfaMethod::*,
    PasswordCredentials, PasswordLogin, PresentedSecret, SealedLoginState, VerifiedLogin,
    runtime::{CompletedLogin, LoginFuture, completion_sealed},
};
use crate::auth::{AuthError, AuthUser, LoginStateStoreRuntime, Scope, SecretRing};

/// A supported second-factor method.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfaMethod {
    /// RFC 6238 time-based one-time password.
    Totp,
    /// One application-managed, single-use recovery code.
    RecoveryCode,
}

impl MfaMethod {
    fn as_str(self) -> &'static str {
        match self {
            Totp => "totp",
            RecoveryCode => "recovery_code",
        }
    }
}

/// Redacted completion input for a multi-factor login challenge.
pub struct MfaResponse {
    challenge_token: String,
    method: MfaMethod,
    answer: PresentedSecret,
}

impl MfaResponse {
    /// Answers a challenge with a TOTP code.
    pub fn totp(challenge_token: impl Into<String>, code: impl Into<String>) -> Self {
        Self::new(challenge_token, Totp, code)
    }

    /// Answers a challenge with an application-managed recovery code.
    pub fn recovery_code(challenge_token: impl Into<String>, code: impl Into<String>) -> Self {
        Self::new(challenge_token, RecoveryCode, code)
    }

    /// Returns the selected factor method.
    pub const fn method(&self) -> MfaMethod {
        self.method
    }

    /// Deliberately exposes the factor answer to the configured verifier.
    pub fn answer(&self) -> &PresentedSecret {
        &self.answer
    }

    fn new(
        challenge_token: impl Into<String>,
        method: MfaMethod,
        answer: impl Into<String>,
    ) -> Self {
        let challenge_token = challenge_token.into();
        Self {
            challenge_token,
            method,
            answer: PresentedSecret::new(answer.into()),
        }
    }

    fn validate(&self) -> Result<(), AuthError> {
        if self.challenge_token.is_empty() || self.challenge_token.len() > 16 * 1024 {
            return Err(AuthError::InvalidLoginState);
        }
        self.answer.validate()
    }
}

impl completion_sealed::Sealed for MfaResponse {}
impl LoginCompletion for MfaResponse {}

/// Resolves available factors and verifies one application identity challenge.
pub trait MfaVerifier: Send + Sync + 'static {
    /// Returns factor methods currently available to the accepted user.
    fn methods<'a>(
        &'a self,
        user: &'a AuthUser,
    ) -> impl Future<Output = Result<Vec<MfaMethod>, AuthError>> + Send + 'a;

    /// Verifies and optionally refreshes the identity after the selected factor.
    fn verify<'a>(
        &'a self,
        user: &'a AuthUser,
        response: &'a MfaResponse,
    ) -> impl Future<Output = Result<AuthUser, AuthError>> + Send + 'a;
}

trait ErasedMfaVerifier: Send + Sync {
    fn methods<'a>(
        &'a self,
        user: &'a AuthUser,
    ) -> BoxFuture<'a, Result<Vec<MfaMethod>, AuthError>>;

    fn verify<'a>(
        &'a self,
        user: &'a AuthUser,
        response: &'a MfaResponse,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>>;
}

impl<T: MfaVerifier> ErasedMfaVerifier for T {
    fn methods<'a>(
        &'a self,
        user: &'a AuthUser,
    ) -> BoxFuture<'a, Result<Vec<MfaMethod>, AuthError>> {
        Box::pin(MfaVerifier::methods(self, user))
    }

    fn verify<'a>(
        &'a self,
        user: &'a AuthUser,
        response: &'a MfaResponse,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(MfaVerifier::verify(self, user, response))
    }
}

/// Configures factors following a successful primary login method.
#[derive(Clone)]
pub struct MfaLogin {
    verifier: Arc<dyn ErasedMfaVerifier>,
    allowed: Vec<MfaMethod>,
    ttl_seconds: i64,
}

impl MfaLogin {
    /// Creates a factor configuration backed by application verification.
    pub fn new(verifier: impl MfaVerifier) -> Self {
        Self {
            verifier: Arc::new(verifier),
            allowed: Vec::new(),
            ttl_seconds: Duration::minutes(5).num_seconds(),
        }
    }

    /// Enables TOTP responses for this flow.
    pub fn totp(mut self) -> Self {
        push_unique(&mut self.allowed, Totp);
        self
    }

    /// Enables single-use recovery-code responses for this flow.
    pub fn recovery_codes(mut self) -> Self {
        push_unique(&mut self.allowed, RecoveryCode);
        self
    }

    /// Sets the bounded lifetime of an unfinished factor challenge.
    pub fn ttl(mut self, value: Duration) -> Self {
        self.ttl_seconds = value.num_seconds();
        self
    }
}

/// A framework-owned one-step login decorated with a required second factor.
#[derive(Clone)]
pub struct ComposedMfaLogin<Start> {
    primary: Arc<dyn super::password::ErasedPasswordVerifier>,
    input: PrimaryInput,
    factors: MfaLogin,
    marker: PhantomData<fn(Start)>,
}

impl PasswordLogin {
    /// Composes password proof with a required local factor challenge.
    pub fn then(self, factors: MfaLogin) -> ComposedMfaLogin<PasswordCredentials> {
        ComposedMfaLogin {
            primary: self.verifier(),
            input: PrimaryInput::Password,
            factors,
            marker: PhantomData,
        }
    }
}

impl BasicLogin {
    /// Composes HTTP Basic proof with a required local factor challenge.
    pub fn then(self, factors: MfaLogin) -> ComposedMfaLogin<BasicCredentials> {
        ComposedMfaLogin {
            primary: self.verifier(),
            input: PrimaryInput::Basic,
            factors,
            marker: PhantomData,
        }
    }
}

struct PasswordMfaRuntime {
    primary: Arc<dyn super::password::ErasedPasswordVerifier>,
    input: PrimaryInput,
    factors: MfaLogin,
}

#[derive(Clone, Copy)]
enum PrimaryInput {
    Password,
    Basic,
}

#[derive(Serialize, Deserialize)]
struct PendingMfa {
    subject: String,
    scopes: Vec<Scope>,
    methods: Vec<MfaMethod>,
    auth_time: i64,
}

impl ErasedLoginRuntime for PasswordMfaRuntime {
    fn is_flow(&self) -> bool {
        true
    }

    fn validate(&self) -> Result<(), AuthError> {
        validate_mfa_conf(&self.factors)
    }

    fn requires_login_state_store(&self) -> bool {
        true
    }

    fn begin<'a>(
        &'a self,
        method: &'a LoginMethodId,
        input: BoxLoginInput,
        target: LoginTarget,
        codec: &'a ChallengeCodec,
        _secrets: &'a SecretRing,
        _passwordless_store: Option<&'a super::PasswordlessStoreRuntime>,
    ) -> LoginFuture<'a, LoginChallenge> {
        Box::pin(async move { self.begin_inner(method, input, target, codec).await })
    }

    fn complete<'a>(
        &'a self,
        method: &'a LoginMethodId,
        input: BoxLoginInput,
        codec: &'a ChallengeCodec,
        _secrets: &'a SecretRing,
        state_store: Option<&'a LoginStateStoreRuntime>,
        _passwordless_store: Option<&'a super::PasswordlessStoreRuntime>,
    ) -> LoginFuture<'a, CompletedLogin> {
        Box::pin(async move { self.complete_inner(method, input, codec, state_store).await })
    }
}

impl PasswordMfaRuntime {
    async fn begin_inner(
        &self,
        method: &LoginMethodId,
        input: BoxLoginInput,
        target: LoginTarget,
        codec: &ChallengeCodec,
    ) -> Result<LoginChallenge, AuthError> {
        validate_mfa_conf(&self.factors)?;
        let input = downcast_primary(input, self.input)?;
        input.validate()?;
        let (username, password) = input.parts();
        let user = self.primary.verify(username, password).await?;
        user.validate()?;
        let methods = self.available_methods(&user).await?;
        let pending = PendingMfa {
            subject: user.subject().to_owned(),
            scopes: user.scopes().to_vec(),
            methods: methods.clone(),
            auth_time: Utc::now().timestamp(),
        };
        let expires_at = Utc::now().timestamp() + self.factors.ttl_seconds;
        let state = SealedLoginState {
            version: 2,
            state_id: uuid::Uuid::new_v4().to_string(),
            method: method.as_str().into(),
            target,
            expires_at,
            payload: serde_json::to_value(pending).map_err(|_| AuthError::InvalidLoginState)?,
        };
        let token = codec.seal(&state)?;
        let names = methods.iter().map(|value| value.as_str().into()).collect();
        Ok(LoginChallenge::factor(
            token,
            names,
            self.factors.ttl_seconds,
        ))
    }

    async fn complete_inner(
        &self,
        method: &LoginMethodId,
        input: BoxLoginInput,
        codec: &ChallengeCodec,
        state_store: Option<&LoginStateStoreRuntime>,
    ) -> Result<CompletedLogin, AuthError> {
        let response = input
            .downcast::<MfaResponse>()
            .map_err(|_| AuthError::InvalidCredential)?;
        response.validate()?;
        let state: SealedLoginState = codec.open(&response.challenge_token)?;
        validate_state(&state, method)?;
        let pending: PendingMfa = serde_json::from_value(state.payload.clone())
            .map_err(|_| AuthError::InvalidLoginState)?;
        if !pending.methods.contains(&response.method) {
            return Err(AuthError::InvalidCredential);
        }
        let user = AuthUser::new(&pending.subject).with_scopes(pending.scopes);
        let user = self.factors.verifier.verify(&user, &response).await?;
        let store = state_store.ok_or(AuthError::InvalidLoginState)?;
        store.consume(&state).await?;
        let login = VerifiedLogin {
            user,
            methods: vec![self.input.method().into(), response.method.as_str().into()],
            auth_time: Some(pending.auth_time),
            acr: Some("urn:vyuh:acr:mfa".into()),
        };
        Ok(CompletedLogin {
            login,
            target: state.target,
        })
    }

    async fn available_methods(&self, user: &AuthUser) -> Result<Vec<MfaMethod>, AuthError> {
        let available = self.factors.verifier.methods(user).await?;
        let methods = available
            .into_iter()
            .filter(|value| self.factors.allowed.contains(value))
            .collect::<Vec<_>>();
        if methods.is_empty() {
            return Err(AuthError::InvalidCredential);
        }
        Ok(methods)
    }
}

impl LoginProviderDefinition<PasswordCredentials, MfaResponse>
    for ComposedMfaLogin<PasswordCredentials>
{
    fn define(self) -> LoginRuntimeDefinition {
        LoginRuntimeDefinition {
            runtime: Arc::new(PasswordMfaRuntime {
                primary: self.primary,
                input: self.input,
                factors: self.factors,
            }),
        }
    }
}

impl<T: Send + Sync + 'static> super::model::definition_sealed::Sealed for ComposedMfaLogin<T> {}

impl LoginProviderDefinition<BasicCredentials, MfaResponse> for ComposedMfaLogin<BasicCredentials> {
    fn define(self) -> LoginRuntimeDefinition {
        LoginRuntimeDefinition {
            runtime: Arc::new(PasswordMfaRuntime {
                primary: self.primary,
                input: self.input,
                factors: self.factors,
            }),
        }
    }
}

impl PrimaryInput {
    const fn method(self) -> &'static str {
        match self {
            Self::Password => "password",
            Self::Basic => "basic",
        }
    }
}

fn downcast_primary(
    input: BoxLoginInput,
    kind: PrimaryInput,
) -> Result<PasswordCredentials, AuthError> {
    match kind {
        PrimaryInput::Password => input
            .downcast::<PasswordCredentials>()
            .map(|value| *value)
            .map_err(|_| AuthError::InvalidCredential),
        PrimaryInput::Basic => input
            .downcast::<BasicCredentials>()
            .map(|value| value.into_password())
            .map_err(|_| AuthError::InvalidCredential),
    }
}

fn validate_state(state: &SealedLoginState, method: &LoginMethodId) -> Result<(), AuthError> {
    if state.version != 2 || state.method != method.as_str() {
        return Err(AuthError::InvalidLoginState);
    }
    if state.expires_at <= Utc::now().timestamp() {
        return Err(AuthError::ExpiredLoginState);
    }
    Ok(())
}

fn validate_mfa_conf(value: &MfaLogin) -> Result<(), AuthError> {
    if value.allowed.is_empty() || !(1..=900).contains(&value.ttl_seconds) {
        return Err(AuthError::InvalidProviderConfig(
            "MFA requires factors and a TTL between 1 and 900 seconds".into(),
        ));
    }
    Ok(())
}

fn push_unique(values: &mut Vec<MfaMethod>, value: MfaMethod) {
    if !values.contains(&value) {
        values.push(value);
    }
}
