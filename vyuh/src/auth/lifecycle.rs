//! Optional stateful token rotation and revocation hooks.

use std::future::Future;

use futures::future::BoxFuture;

use super::{AuthError, AuthToken, PresentedCredential};

/// Metadata for the refresh token replacing an accepted credential.
#[derive(Clone, Debug)]
pub struct RefreshMetadata {
    token_id: String,
    family_id: String,
    expires_at: chrono::DateTime<chrono::Utc>,
    audiences: Vec<String>,
    provider: String,
    subject: String,
}

impl RefreshMetadata {
    pub(crate) fn from_token(token: &AuthToken) -> Result<Self, AuthError> {
        Ok(Self {
            token_id: token
                .token_id()
                .ok_or(AuthError::InvalidCredential)?
                .to_owned(),
            family_id: token
                .family_id()
                .ok_or(AuthError::InvalidCredential)?
                .to_owned(),
            expires_at: token.expires_at()?,
            audiences: token.audiences().map(str::to_owned).collect(),
            provider: token.provider().to_owned(),
            subject: token.subject().to_owned(),
        })
    }

    /// Returns the replacement refresh token identifier.
    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    /// Returns the preserved refresh-family identifier.
    pub fn family_id(&self) -> &str {
        &self.family_id
    }

    /// Returns when the replacement refresh token expires.
    pub const fn expires_at(&self) -> chrono::DateTime<chrono::Utc> {
        self.expires_at
    }

    /// Returns the audiences retained by the replacement token.
    pub fn audiences(&self) -> &[String] {
        &self.audiences
    }

    /// Returns the provider bound into the replacement token.
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Returns the replacement token subject.
    pub fn subject(&self) -> &str {
        &self.subject
    }
}

/// Optional application storage used for revocation and replay protection.
pub trait TokenLifecycle: Send + Sync + 'static {
    /// Checks whether an otherwise valid token remains accepted.
    fn validate<'a>(
        &'a self,
        token: &'a AuthToken,
    ) -> impl Future<Output = Result<(), AuthError>> + Send + 'a;

    /// Atomically records one refresh token being replaced by another.
    fn rotate<'a>(
        &'a self,
        current: &'a AuthToken,
        replacement: &'a RefreshMetadata,
    ) -> impl Future<Output = Result<(), AuthError>> + Send + 'a;

    /// Revokes the presented token or its containing family.
    fn revoke<'a>(
        &'a self,
        token: &'a AuthToken,
    ) -> impl Future<Output = Result<(), AuthError>> + Send + 'a;
}

pub(crate) trait ErasedLifecycle: Send + Sync {
    fn validate<'a>(&'a self, token: &'a AuthToken) -> BoxFuture<'a, Result<(), AuthError>>;
    fn rotate<'a>(
        &'a self,
        current: &'a AuthToken,
        replacement: &'a RefreshMetadata,
    ) -> BoxFuture<'a, Result<(), AuthError>>;
    fn revoke<'a>(&'a self, token: &'a AuthToken) -> BoxFuture<'a, Result<(), AuthError>>;
}

impl<T: TokenLifecycle> ErasedLifecycle for T {
    fn validate<'a>(&'a self, token: &'a AuthToken) -> BoxFuture<'a, Result<(), AuthError>> {
        Box::pin(TokenLifecycle::validate(self, token))
    }

    fn rotate<'a>(
        &'a self,
        current: &'a AuthToken,
        replacement: &'a RefreshMetadata,
    ) -> BoxFuture<'a, Result<(), AuthError>> {
        Box::pin(TokenLifecycle::rotate(self, current, replacement))
    }

    fn revoke<'a>(&'a self, token: &'a AuthToken) -> BoxFuture<'a, Result<(), AuthError>> {
        Box::pin(TokenLifecycle::revoke(self, token))
    }
}

/// Optional server-side revocation hook for an opaque authentication key.
pub trait KeyLifecycle: Send + Sync + 'static {
    /// Revokes the exact opaque credential presented during logout.
    fn revoke<'a>(
        &'a self,
        credential: &'a PresentedCredential<'a>,
    ) -> impl Future<Output = Result<(), AuthError>> + Send + 'a;
}

pub(crate) trait ErasedKeyLifecycle: Send + Sync {
    fn revoke<'a>(
        &'a self,
        credential: &'a PresentedCredential<'a>,
    ) -> BoxFuture<'a, Result<(), AuthError>>;
}

impl<T: KeyLifecycle> ErasedKeyLifecycle for T {
    fn revoke<'a>(
        &'a self,
        credential: &'a PresentedCredential<'a>,
    ) -> BoxFuture<'a, Result<(), AuthError>> {
        Box::pin(KeyLifecycle::revoke(self, credential))
    }
}
