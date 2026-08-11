//! Test-only proof that stateful providers fit the private provider contract.

use axum::http::{Request, request::Parts};
use futures::future::BoxFuture;

use super::contract::{ProviderAudienceSet, ProviderRuntimeContract};
use super::*;
use crate::auth::Credentials;

#[derive(Clone)]
pub(super) struct MockSessionRuntime {
    pub(super) id: ProviderId,
    pub(super) location: super::super::CredentialLocation,
    audiences: ProviderAudienceSet,
}

impl MockSessionRuntime {
    fn new() -> Result<Self, AuthError> {
        Ok(Self {
            id: ProviderId::new("mock-session")?,
            location: super::super::CredentialLocation::bearer(),
            audiences: ProviderAudienceSet::Any,
        })
    }

    pub(super) fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            authenticate: true,
            login: true,
            refresh: true,
            logout: true,
        }
    }

    pub(super) async fn login(
        &self,
        _user: AuthUser,
        _audiences: Vec<AudienceId>,
    ) -> Result<LoginResponse, AuthError> {
        Ok(mock_login_response())
    }

    pub(super) async fn authenticate(
        &self,
        _raw: &str,
        _parts: &Parts,
        _audience: &AudienceId,
    ) -> Result<AuthUser, AuthError> {
        Ok(AuthUser::new("session-user").set_provider(self.id.clone()))
    }

    pub(super) async fn refresh(
        &self,
        _raw: &str,
        _parts: &Parts,
    ) -> Result<LoginResponse, AuthError> {
        Ok(mock_login_response())
    }

    pub(super) async fn logout(
        &self,
        _parts: &Parts,
    ) -> Result<Vec<(axum::http::HeaderName, axum::http::HeaderValue)>, AuthError> {
        Ok(Vec::new())
    }
}

impl ProviderRuntimeContract for MockSessionRuntime {
    fn id(&self) -> &ProviderId {
        &self.id
    }
    fn audiences(&self) -> &ProviderAudienceSet {
        &self.audiences
    }
    fn access_location(&self) -> &super::super::CredentialLocation {
        &self.location
    }
    fn refresh_location(&self) -> Option<&super::super::CredentialLocation> {
        Some(&self.location)
    }
    fn capabilities(&self) -> ProviderCapabilities {
        MockSessionRuntime::capabilities(self)
    }
    fn openapi(&self) -> super::super::ProviderDoc {
        super::super::ProviderDoc {
            id: self.id.to_string(),
            audiences: None,
            credential_type: super::super::CredentialType::Key,
            location: self.location.doc(),
            csrf_header: None,
        }
    }
    fn authenticate<'a>(
        &'a self,
        raw: &'a str,
        parts: &'a Parts,
        audience: &'a AudienceId,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(MockSessionRuntime::authenticate(self, raw, parts, audience))
    }
    fn login<'a>(
        &'a self,
        user: AuthUser,
        audiences: Vec<AudienceId>,
    ) -> BoxFuture<'a, Result<LoginResponse, AuthError>> {
        Box::pin(MockSessionRuntime::login(self, user, audiences))
    }
    fn refresh<'a>(
        &'a self,
        raw: &'a str,
        parts: &'a Parts,
    ) -> BoxFuture<'a, Result<LoginResponse, AuthError>> {
        Box::pin(MockSessionRuntime::refresh(self, raw, parts))
    }
    fn logout<'a>(
        &'a self,
        parts: &'a Parts,
    ) -> BoxFuture<'a, Result<Vec<(axum::http::HeaderName, axum::http::HeaderValue)>, AuthError>>
    {
        Box::pin(MockSessionRuntime::logout(self, parts))
    }
}

fn mock_login_response() -> LoginResponse {
    LoginResponse::new(
        Credentials::new("session-id".to_owned(), None),
        Some("session-id".to_owned()),
        None,
        3600,
        Vec::new(),
        super::super::CredentialLocation::bearer().request_selector(None),
    )
}

fn request_parts() -> Parts {
    Request::new(()).into_parts().0
}

/// Verifies a stateful provider can implement every private authenticator operation.
#[tokio::test]
async fn stateful_provider_fits_runtime_contract() -> Result<(), AuthError> {
    let runtime = ProviderRuntime::new(MockSessionRuntime::new()?);
    let parts = request_parts();
    let audience = AudienceId::new("api")?;
    let capabilities = runtime.capabilities();

    assert!(capabilities.authenticate);
    assert!(capabilities.login);
    assert!(capabilities.refresh);
    assert!(capabilities.logout);
    let login = runtime
        .login(AuthUser::new("user-123"), vec![audience.clone()])
        .await?;
    assert_eq!(login.credentials().access(), "session-id");
    let user = runtime
        .authenticate("session-id", &parts, &audience)
        .await?;
    assert_eq!(user.subject(), "session-user");
    let refreshed = runtime.refresh("session-id", &parts).await?;
    assert_eq!(refreshed.credentials().access(), "session-id");
    assert!(runtime.logout(&parts).await?.is_empty());
    Ok(())
}
