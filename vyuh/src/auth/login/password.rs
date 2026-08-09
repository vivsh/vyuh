//! Password and HTTP Basic token-exchange login methods.

use std::{future::Future, sync::Arc};

use axum::{
    extract::FromRequestParts,
    http::{header, request::Parts},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures::future::BoxFuture;

use super::{
    BasicCredentials, BoxLoginInput, ErasedLoginRuntime, LoginProviderDefinition,
    LoginRuntimeDefinition, NoChallenge, PasswordCredentials, PresentedSecret, VerifiedLogin,
    runtime::LoginFuture,
};
use crate::{
    Site,
    auth::{AuthError, AuthUser},
    callables::{ArgPart, IntoArgPart, TypeSchema},
};

/// Resolves username/password credentials into an application identity.
pub trait PasswordVerifier: Send + Sync + 'static {
    /// Returns an accepted user without revealing account lookup details on failure.
    fn verify<'a>(
        &'a self,
        username: &'a str,
        password: &'a PresentedSecret,
    ) -> impl Future<Output = Result<AuthUser, AuthError>> + Send + 'a;
}

pub(crate) trait ErasedPasswordVerifier: Send + Sync {
    fn verify<'a>(
        &'a self,
        username: &'a str,
        password: &'a PresentedSecret,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>>;
}

impl<T: PasswordVerifier> ErasedPasswordVerifier for T {
    fn verify<'a>(
        &'a self,
        username: &'a str,
        password: &'a PresentedSecret,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(PasswordVerifier::verify(self, username, password))
    }
}

/// Configures a one-step password login method.
#[derive(Clone)]
pub struct PasswordLogin {
    pub(crate) verifier: Arc<dyn ErasedPasswordVerifier>,
}

impl PasswordLogin {
    /// Uses application-owned account lookup and password verification.
    pub fn new(verifier: impl PasswordVerifier) -> Self {
        Self {
            verifier: Arc::new(verifier),
        }
    }

    pub(crate) fn verifier(&self) -> Arc<dyn ErasedPasswordVerifier> {
        self.verifier.clone()
    }
}

/// Configures HTTP Basic credentials as a token-exchange login method.
#[derive(Clone)]
pub struct BasicLogin {
    verifier: Arc<dyn ErasedPasswordVerifier>,
}

impl BasicLogin {
    /// Uses the same verifier contract as password-body login.
    pub fn new(verifier: impl PasswordVerifier) -> Self {
        Self {
            verifier: Arc::new(verifier),
        }
    }

    pub(crate) fn verifier(&self) -> Arc<dyn ErasedPasswordVerifier> {
        self.verifier.clone()
    }
}

struct PasswordRuntime {
    verifier: Arc<dyn ErasedPasswordVerifier>,
    input: PasswordInputKind,
}

#[derive(Clone, Copy)]
enum PasswordInputKind {
    Password,
    Basic,
}

impl ErasedLoginRuntime for PasswordRuntime {
    fn is_flow(&self) -> bool {
        false
    }

    fn login<'a>(&'a self, input: BoxLoginInput) -> LoginFuture<'a, VerifiedLogin> {
        Box::pin(async move {
            let credentials = downcast_credentials(input, self.input)?;
            credentials.validate()?;
            let (username, password) = credentials.parts();
            let user = self.verifier.verify(username, password).await?;
            let method = match self.input {
                PasswordInputKind::Password => "password",
                PasswordInputKind::Basic => "basic",
            };
            Ok(VerifiedLogin::new(user, method))
        })
    }
}

fn downcast_credentials(
    input: BoxLoginInput,
    kind: PasswordInputKind,
) -> Result<PasswordCredentials, AuthError> {
    match kind {
        PasswordInputKind::Password => input
            .downcast::<PasswordCredentials>()
            .map(|value| *value)
            .map_err(|_| AuthError::InvalidCredential),
        PasswordInputKind::Basic => input
            .downcast::<BasicCredentials>()
            .map(|value| value.into_password())
            .map_err(|_| AuthError::InvalidCredential),
    }
}

impl LoginProviderDefinition<PasswordCredentials, NoChallenge> for PasswordLogin {
    fn define(self) -> LoginRuntimeDefinition {
        LoginRuntimeDefinition {
            runtime: Arc::new(PasswordRuntime {
                verifier: self.verifier,
                input: PasswordInputKind::Password,
            }),
        }
    }
}

impl super::model::definition_sealed::Sealed for PasswordLogin {}

impl LoginProviderDefinition<BasicCredentials, NoChallenge> for BasicLogin {
    fn define(self) -> LoginRuntimeDefinition {
        LoginRuntimeDefinition {
            runtime: Arc::new(PasswordRuntime {
                verifier: self.verifier,
                input: PasswordInputKind::Basic,
            }),
        }
    }
}

impl super::model::definition_sealed::Sealed for BasicLogin {}

impl FromRequestParts<Site> for BasicCredentials {
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, _site: &Site) -> Result<Self, Self::Rejection> {
        let value = parts
            .headers
            .get(header::AUTHORIZATION)
            .ok_or(AuthError::NoCredential)?
            .to_str()
            .map_err(|_| AuthError::MalformedLocation)?;
        parse_basic(value)
    }
}

fn parse_basic(value: &str) -> Result<BasicCredentials, AuthError> {
    let (scheme, encoded) = value.split_once(' ').ok_or(AuthError::MalformedLocation)?;
    if !scheme.eq_ignore_ascii_case("basic") || encoded.len() > 8192 {
        return Err(AuthError::MalformedLocation);
    }
    let decoded = STANDARD
        .decode(encoded)
        .map_err(|_| AuthError::InvalidCredential)?;
    let decoded = String::from_utf8(decoded).map_err(|_| AuthError::InvalidCredential)?;
    let (username, password) = decoded
        .split_once(':')
        .ok_or(AuthError::InvalidCredential)?;
    BasicCredentials::validated(username.to_owned(), password.to_owned())
}

impl IntoArgPart for BasicCredentials {
    fn into_arg_part() -> ArgPart {
        ArgPart::Composite(vec![
            ArgPart::Header(TypeSchema::wrap::<String>()),
            ArgPart::Security {
                scheme: "basicAuth".into(),
                scopes: Vec::new(),
                join_all: false,
            },
        ])
    }
}
