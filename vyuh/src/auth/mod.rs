//! Composable authentication providers, verified identities, and scope guards.

#![deny(dead_code, unused_imports)]

mod codec;
mod codecs;
mod config;
#[cfg(feature = "id-token")]
mod id_token;
mod identity;
mod lifecycle;
mod location;
mod login;
mod metrics;
#[cfg(any(feature = "oauth", feature = "id-token"))]
mod oauth;
mod password;
mod request;
mod response;
mod runtime;
mod token;

pub use crate::scopes::{Permit, Scope, ScopeExpr, ScopeRule};
pub use codec::{CodecDefinition, TokenClaims, TokenDecoder, TokenEncoder};
#[cfg(feature = "branca")]
pub use codecs::Branca;
#[cfg(feature = "paseto")]
pub use codecs::Paseto;
pub use codecs::{DjangoSigning, Jwt, JwtAlgorithm, JwtConf, JwtVerificationKey};
pub use config::{
    AuthBinding, AuthConf, AuthKey, AuthProviderSummary, AuthSummary, BindingResolver, KeyRequest,
    KeySource, KeyVerifier, ProviderDefinition, TokenConf, TokenProvider, TokenVerifier,
};
#[cfg(feature = "id-token")]
pub use id_token::{IdToken, IdTokenClaims, IdTokenMapper};
pub use identity::{
    Audience, AuthProvider, AuthUser, AuthenticationContext, DEFAULT_AUDIENCE,
    DEFAULT_AUTH_PROVIDER,
};
pub use lifecycle::{KeyLifecycle, RefreshMetadata, TokenLifecycle};
pub use location::{CookieConf, CookieSameSite, CsrfConf, UnsafeQueryCredentials};
#[doc(hidden)]
pub use login::ComposedMfaLogin;
pub use login::{
    BasicCredentials, BasicLogin, LoginAuth, LoginChallenge, LoginChallengeKind, LoginMethod,
    LoginStateStore, MfaLogin, MfaMethod, MfaResponse, MfaVerifier, PasswordCredentials,
    PasswordLogin, PasswordVerifier, PresentedSecret,
};
#[cfg(feature = "federated")]
pub use login::{
    FederatedCallback, FederatedIdentity, FederatedLogin, FederatedProvider, FederatedStart,
    FederatedUserMapper,
};
#[cfg(feature = "oauth")]
pub use oauth::{
    OAuthClaims, OAuthIdentityMapper, OAuthJwtAlgorithm, OAuthResource, OAuthResourceServer,
};
pub use password::{
    PasswordVerification, check_password, check_password_with_upgrade, make_password,
    make_password_with_iterations, unusable_password,
};
pub use response::{Credentials, DefaultLoginData, LoginResponse, LogoutResponse};
pub use runtime::{Authenticator, ProviderAuth};
pub use token::{AuthToken, AuthTokenBuilder, EncodedCredential, PresentedCredential, TokenKind};

pub(crate) use codec::{
    CodecRuntime, CustomClaims, CustomCodec, ErasedDecoder, ErasedEncoder, SecretRing,
};
#[cfg(any(feature = "oauth", feature = "id-token"))]
pub(crate) use codecs::reject_duplicate_claims;
pub(crate) use config::{CredentialType, ProviderDoc};
pub(crate) use config::{
    ErasedTokenVerifier, KeySourceKind, ProviderDefinitionInner, ProviderKind, build_codec,
    validate_token_conf,
};
pub(crate) use identity::{AudienceId, ProviderId};
pub(crate) use lifecycle::{ErasedKeyLifecycle, ErasedLifecycle};
pub(crate) use location::{CredentialLocation, ProviderDocLocation, RequestCredentialScan};
pub(crate) use login::{
    ChallengeCodec, LoginDefinitionInner, LoginMethodId, LoginProviderDefinition,
    LoginStateStoreRuntime, NoChallenge,
};
pub(crate) use metrics::AuthMetrics;

#[cfg(feature = "mcp")]
/// Provider-owned OAuth protected-resource details consumed by protocol adapters.
#[derive(Clone, Debug)]
pub(crate) struct AuthProtectedResource {
    pub(crate) issuer: String,
    pub(crate) advertised_scopes: Vec<String>,
    pub(crate) required_scopes: Vec<String>,
}

#[cfg(feature = "mcp")]
/// Provider-owned challenge metadata for protocol adapters.
#[derive(Clone, Debug)]
pub(crate) struct AuthChallenge {
    pub(crate) scheme: &'static str,
}

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use thiserror::Error;

/// Structured authentication failures with deliberately safe HTTP rendering.
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("no credential was presented")]
    NoCredential,
    #[error("credential location is malformed")]
    MalformedLocation,
    #[error("credential is invalid")]
    InvalidCredential,
    #[error("credential has expired")]
    ExpiredCredential,
    #[error("credential is not valid yet")]
    CredentialNotYetValid,
    #[error("credential has the wrong token kind")]
    WrongTokenKind,
    #[error("credential audience does not match this operation")]
    AudienceMismatch,
    #[error("authenticated identity is forbidden from this operation")]
    Forbidden,
    #[error("credential lacks a required provider scope")]
    InsufficientScope,
    #[error("credential binding does not match this request")]
    BindingMismatch,
    #[error("credential CSRF validation failed")]
    InvalidCsrfToken,
    #[error("a binding is required for this provider")]
    BindingRequired,
    #[error("authenticated operation has no audience context")]
    MissingAudienceContext,
    #[error("provider '{0}' is not configured")]
    ProviderNotFound(String),
    #[error("invalid provider identifier '{0}'")]
    InvalidProviderId(String),
    #[error("provider identifier '{0}' uses the reserved 'vyuh-' prefix")]
    ReservedProviderId(String),
    #[error("invalid audience '{0}'")]
    InvalidAudience(String),
    #[error("provider '{0}' is registered more than once")]
    DuplicateProvider(String),
    #[error("login method '{0}' is not configured")]
    LoginMethodNotFound(String),
    #[error("invalid login method identifier '{0}'")]
    InvalidLoginMethod(String),
    #[error("login method '{0}' is registered more than once")]
    DuplicateLoginMethod(String),
    #[error("login method '{0}' was selected with incompatible input types")]
    LoginMethodTypeMismatch(String),
    #[error("login continuation state is invalid")]
    InvalidLoginState,
    #[error("login continuation state has expired")]
    ExpiredLoginState,
    #[error("multiple providers accept {0}")]
    AmbiguousProvider(String),
    #[error("invalid provider configuration: {0}")]
    InvalidProviderConfig(String),
    #[error("provider does not support this capability")]
    UnsupportedProviderCapability,
    #[error("external authentication provider is unavailable")]
    ProviderUnavailable,
    #[error("credential location cannot attach a response credential")]
    UnsupportedLocationCapability,
    #[error("credential could not be attached to the response")]
    DeliveryFailed,
    #[error("internal authentication failure: {0}")]
    Internal(String),
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::AudienceMismatch
            | Self::Forbidden
            | Self::InsufficientScope
            | Self::InvalidCsrfToken => StatusCode::FORBIDDEN,
            Self::NoCredential
            | Self::MalformedLocation
            | Self::InvalidCredential
            | Self::ExpiredCredential
            | Self::CredentialNotYetValid
            | Self::WrongTokenKind
            | Self::InvalidLoginState
            | Self::ExpiredLoginState
            | Self::BindingMismatch => StatusCode::UNAUTHORIZED,
            Self::ProviderUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        let (code, message) = if status == StatusCode::UNAUTHORIZED {
            ("unauthorized", "Authentication failed.")
        } else if status == StatusCode::FORBIDDEN {
            ("forbidden", "You are not allowed to access this resource.")
        } else if status == StatusCode::SERVICE_UNAVAILABLE {
            (
                "provider_unavailable",
                "Authentication provider is unavailable.",
            )
        } else {
            ("auth_error", "Authentication service failed.")
        };
        crate::errors::ErrorReport::new(status, crate::errors::ErrorSourceKind::Auth, code, message)
            .into_response()
    }
}
