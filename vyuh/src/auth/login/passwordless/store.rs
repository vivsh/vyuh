//! Durable storage contracts for passwordless proof challenges.

use std::{fmt, future::Future, sync::Arc};

use futures::future::BoxFuture;

use crate::auth::AuthError;

/// Opaque challenge state persisted by a durable passwordless store.
pub struct PasswordlessChallenge {
    id: String,
    principal: Vec<u8>,
    proofs: Vec<Vec<u8>>,
    state: String,
    expires_at: i64,
    attempts: u8,
    next_issue_at: i64,
    issue_window_ends_at: i64,
    issue_limit: u8,
}

impl PasswordlessChallenge {
    pub(crate) fn new(value: PasswordlessChallengeParts) -> Self {
        Self {
            id: value.id,
            principal: value.principal,
            proofs: value.proofs,
            state: value.state,
            expires_at: value.expires_at,
            attempts: value.attempts,
            next_issue_at: value.next_issue_at,
            issue_window_ends_at: value.issue_window_ends_at,
            issue_limit: value.issue_limit,
        }
    }

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

    /// Returns the earliest UTC timestamp for another proof issue.
    pub const fn next_issue_at(&self) -> i64 {
        self.next_issue_at
    }

    /// Returns the end of the proof-issuance rate window.
    pub const fn issue_window_ends_at(&self) -> i64 {
        self.issue_window_ends_at
    }

    /// Returns the maximum proof issues within one window.
    pub const fn issue_limit(&self) -> u8 {
        self.issue_limit
    }
}

impl fmt::Debug for PasswordlessChallenge {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PasswordlessChallenge(<redacted>)")
    }
}

pub(crate) struct PasswordlessChallengeParts {
    pub(crate) id: String,
    pub(crate) principal: Vec<u8>,
    pub(crate) proofs: Vec<Vec<u8>>,
    pub(crate) state: String,
    pub(crate) expires_at: i64,
    pub(crate) attempts: u8,
    pub(crate) next_issue_at: i64,
    pub(crate) issue_window_ends_at: i64,
    pub(crate) issue_limit: u8,
}

/// The durable store's atomic result for challenge creation or proof-issue suppression.
pub struct PasswordlessStart {
    challenge_id: String,
    expires_at: i64,
    next_issue_at: i64,
    issue: bool,
}

impl PasswordlessStart {
    /// Creates a store result for one active passwordless challenge.
    pub fn new(
        challenge_id: impl Into<String>,
        expires_at: i64,
        next_issue_at: i64,
        issue: bool,
    ) -> Self {
        Self {
            challenge_id: challenge_id.into(),
            expires_at,
            next_issue_at,
            issue,
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

    /// Returns the next permitted proof-issuance timestamp.
    pub const fn next_issue_at(&self) -> i64 {
        self.next_issue_at
    }

    /// Returns whether this request should expose a fresh proof to the handler.
    pub const fn should_issue(&self) -> bool {
        self.issue
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
    /// Starts or suppresses proof issuance for one principal without exposing that principal.
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
}
