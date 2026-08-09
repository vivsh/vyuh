//! Type-erased login runtimes and the typed `.via()` facade.

use std::{any::TypeId, future::Future, pin::Pin, sync::Arc};

use serde::{Deserialize, Serialize};

use super::{
    BoxLoginInput, ChallengeCodec, LoginChallenge, LoginMethod, LoginMethodId, NoChallenge,
};
use crate::auth::{
    Audience, AudienceId, AuthError, AuthProvider, AuthUser, Authenticator, LoginResponse,
    LoginStateStoreRuntime, ProviderId, SecretRing,
};

pub(crate) type LoginFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, AuthError>> + Send + 'a>>;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct LoginTarget {
    pub(crate) provider: ProviderId,
    pub(crate) audiences: Vec<AudienceId>,
}

pub(crate) struct VerifiedLogin {
    pub(crate) user: AuthUser,
    pub(crate) methods: Vec<String>,
    pub(crate) auth_time: Option<i64>,
    pub(crate) acr: Option<String>,
}

impl VerifiedLogin {
    pub(crate) fn new(user: AuthUser, method: impl Into<String>) -> Self {
        Self {
            user,
            methods: vec![method.into()],
            auth_time: Some(chrono::Utc::now().timestamp()),
            acr: None,
        }
    }
}

pub(crate) struct CompletedLogin {
    pub(crate) login: VerifiedLogin,
    pub(crate) target: LoginTarget,
}

pub(crate) trait ErasedLoginRuntime: Send + Sync {
    fn is_flow(&self) -> bool;

    fn prepare(&self, _secrets: &SecretRing) -> Result<(), AuthError> {
        Ok(())
    }

    fn validate(&self) -> Result<(), AuthError> {
        Ok(())
    }

    fn initialize(&self) -> LoginFuture<'_, ()> {
        Box::pin(async { Ok(()) })
    }

    fn login<'a>(&'a self, _input: BoxLoginInput) -> LoginFuture<'a, VerifiedLogin> {
        Box::pin(async { Err(AuthError::UnsupportedProviderCapability) })
    }

    fn begin<'a>(
        &'a self,
        _method: &'a LoginMethodId,
        _input: BoxLoginInput,
        _target: LoginTarget,
        _codec: &'a ChallengeCodec,
        _secrets: &'a SecretRing,
    ) -> LoginFuture<'a, LoginChallenge> {
        Box::pin(async { Err(AuthError::UnsupportedProviderCapability) })
    }

    fn complete<'a>(
        &'a self,
        _method: &'a LoginMethodId,
        _input: BoxLoginInput,
        _codec: &'a ChallengeCodec,
        _secrets: &'a SecretRing,
        _state_store: Option<&'a LoginStateStoreRuntime>,
    ) -> LoginFuture<'a, CompletedLogin> {
        Box::pin(async { Err(AuthError::UnsupportedProviderCapability) })
    }
}

/// A typed facade for one configured login method and credential provider.
///
/// One-step methods do not expose flow completion:
///
/// ```compile_fail
/// use vyuh::auth::{Authenticator, LoginMethod, PasswordCredentials};
///
/// const PASSWORD: LoginMethod<PasswordCredentials> = LoginMethod::new("password");
///
/// fn cannot_complete(auth: &Authenticator, input: PasswordCredentials) {
///     let selected = auth.via(PASSWORD);
///     let _ = selected.complete(input);
/// }
/// ```
///
/// Multi-step methods do not expose one-step login:
///
/// ```compile_fail
/// use vyuh::auth::{Audience, Authenticator, LoginMethod, MfaResponse, PasswordCredentials};
///
/// const API: Audience = Audience::new("api");
/// const PASSWORD_MFA: LoginMethod<PasswordCredentials, MfaResponse> =
///     LoginMethod::new("password-mfa");
///
/// fn cannot_login_once(auth: &Authenticator, input: PasswordCredentials) {
///     let selected = auth.via(PASSWORD_MFA);
///     let _ = selected.login(input, &[API]);
/// }
/// ```
pub struct LoginAuth<'a, Start, Complete = NoChallenge> {
    pub(crate) authenticator: &'a Authenticator,
    pub(crate) method: LoginMethod<Start, Complete>,
    pub(crate) provider: AuthProvider,
}

struct ResolvedLogin {
    method: LoginMethodId,
    provider: ProviderId,
    runtime: Arc<dyn ErasedLoginRuntime>,
}

impl<Start, Complete> LoginAuth<'_, Start, Complete>
where
    Start: Send + 'static,
    Complete: Send + 'static,
{
    /// Resolves and type-checks retained descriptors at the terminal operation boundary.
    fn resolve(&self) -> Result<ResolvedLogin, AuthError> {
        let provider = self.authenticator.resolve_login_provider(self.provider)?;
        let method = LoginMethodId::new(self.method.as_str())?;
        let definition = self.authenticator.login_method(&method)?;
        if definition.start_type != TypeId::of::<Start>()
            || definition.complete_type != TypeId::of::<Complete>()
        {
            return Err(AuthError::LoginMethodTypeMismatch(method.as_str().into()));
        }
        Ok(ResolvedLogin {
            method,
            provider,
            runtime: definition.runtime.clone(),
        })
    }

    fn record<T>(&self, result: &Result<T, AuthError>) {
        self.authenticator
            .record_provider(self.provider.as_str(), result);
        self.authenticator
            .record_login(self.method.as_str(), result);
    }
}

impl<Start> LoginAuth<'_, Start, NoChallenge>
where
    Start: Send + 'static,
{
    /// Proves identity once and returns credentials from the selected provider.
    pub async fn login(
        &self,
        input: Start,
        audiences: &[Audience],
    ) -> Result<LoginResponse, AuthError> {
        let result = async {
            let resolved = self.resolve()?;
            let audiences = self.authenticator.resolve_audiences(audiences)?;
            let verified = resolved.runtime.login(Box::new(input)).await?;
            self.authenticator
                .login_verified(&resolved.provider, verified, audiences)
                .await
        }
        .await;
        self.record(&result);
        result
    }
}

#[allow(private_bounds)]
impl<Start, Complete> LoginAuth<'_, Start, Complete>
where
    Start: Send + 'static,
    Complete: LoginCompletion,
{
    /// Starts a multi-step login and binds its credential provider and audiences.
    pub async fn begin(
        &self,
        input: Start,
        audiences: &[Audience],
    ) -> Result<LoginChallenge, AuthError> {
        let result = async {
            let resolved = self.resolve()?;
            let target = LoginTarget {
                provider: resolved.provider,
                audiences: self.authenticator.resolve_audiences(audiences)?,
            };
            resolved
                .runtime
                .begin(
                    &resolved.method,
                    Box::new(input),
                    target,
                    self.authenticator.challenge_codec(),
                    self.authenticator.secret_ring(),
                )
                .await
        }
        .await;
        self.record(&result);
        result
    }

    /// Completes a multi-step login and issues the credentials bound at `begin`.
    pub async fn complete(&self, input: Complete) -> Result<LoginResponse, AuthError> {
        let result = async {
            let resolved = self.resolve()?;
            let completed = resolved
                .runtime
                .complete(
                    &resolved.method,
                    Box::new(input),
                    self.authenticator.challenge_codec(),
                    self.authenticator.secret_ring(),
                    self.authenticator.login_state_store(),
                )
                .await?;
            if completed.target.provider != resolved.provider {
                return Err(AuthError::InvalidLoginState);
            }
            self.authenticator
                .login_verified(
                    &completed.target.provider,
                    completed.login,
                    completed.target.audiences,
                )
                .await
        }
        .await;
        self.record(&result);
        result
    }
}

pub(crate) mod completion_sealed {
    pub trait Sealed {}
}

/// Marker implemented by framework-owned inputs that complete a login flow.
pub(crate) trait LoginCompletion: completion_sealed::Sealed + Send + 'static {}

pub(crate) fn select<Start, Complete>(
    authenticator: &Authenticator,
    provider: AuthProvider,
    method: LoginMethod<Start, Complete>,
) -> LoginAuth<'_, Start, Complete>
where
    Start: Send + 'static,
    Complete: Send + 'static,
{
    LoginAuth {
        authenticator,
        method,
        provider,
    }
}
