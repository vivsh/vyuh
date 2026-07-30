//! Login method descriptors and redacted input values.

use std::{
    any::{Any, TypeId},
    fmt,
    marker::PhantomData,
    sync::Arc,
};

use super::ErasedLoginRuntime;
use crate::auth::AuthError;

/// Marks a login method that completes in one operation.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoChallenge;

/// A reusable, typed name for one configured identity-proof method.
pub struct LoginMethod<Start, Complete = NoChallenge> {
    name: &'static str,
    marker: PhantomData<fn(Start) -> Complete>,
}

impl<Start, Complete> LoginMethod<Start, Complete> {
    /// Declares a login method; registration or a terminal operation validates it.
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            marker: PhantomData,
        }
    }

    /// Returns the declared login method name.
    pub const fn as_str(self) -> &'static str {
        self.name
    }
}

impl<Start, Complete> Clone for LoginMethod<Start, Complete> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Start, Complete> Copy for LoginMethod<Start, Complete> {}

impl<Start, Complete> fmt::Debug for LoginMethod<Start, Complete> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("LoginMethod")
            .field(&self.name)
            .finish()
    }
}

/// A password supplied to a login verifier without diagnostic exposure.
pub struct PresentedSecret(String);

impl PresentedSecret {
    /// Deliberately exposes the secret to the configured verifier.
    pub fn expose(&self) -> &str {
        &self.0
    }

    pub(crate) fn new(value: String) -> Self {
        Self(value)
    }

    pub(crate) fn validate(&self) -> Result<(), AuthError> {
        if self.0.is_empty() || self.0.len() > 4096 {
            return Err(AuthError::InvalidCredential);
        }
        Ok(())
    }
}

/// Username and password input for a password login method.
pub struct PasswordCredentials {
    username: String,
    password: PresentedSecret,
}

impl PasswordCredentials {
    /// Creates redacted password login input; configured limits are enforced at login.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        let username = username.into();
        Self {
            username,
            password: PresentedSecret::new(password.into()),
        }
    }

    pub(crate) fn parts(&self) -> (&str, &PresentedSecret) {
        (&self.username, &self.password)
    }

    pub(crate) fn validate(&self) -> Result<(), AuthError> {
        if self.username.trim().is_empty() || self.username.len() > 512 {
            return Err(AuthError::InvalidCredential);
        }
        self.password.validate()
    }
}

/// Redacted credentials extracted from an HTTP Basic authorization header.
pub struct BasicCredentials(PasswordCredentials);

impl BasicCredentials {
    /// Creates redacted HTTP Basic input for an explicit token-exchange flow.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self(PasswordCredentials::new(username, password))
    }

    pub(crate) fn into_password(self) -> PasswordCredentials {
        self.0
    }

    pub(crate) fn validated(username: String, password: String) -> Result<Self, AuthError> {
        let value = Self::new(username, password);
        let password = &value.0;
        password.validate()?;
        Ok(value)
    }
}

pub(crate) type BoxLoginInput = Box<dyn Any + Send>;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LoginMethodId(String);

impl LoginMethodId {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, AuthError> {
        let value = value.into();
        if value.trim().is_empty()
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(AuthError::InvalidLoginMethod(value));
        }
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[doc(hidden)]
#[derive(Clone)]
pub struct LoginProviderKind {
    pub(crate) runtime: Arc<dyn ErasedLoginRuntime>,
}

/// Framework-owned login provider definition accepted by `AuthConf::method`.
pub trait LoginProviderDefinition<Start, Complete>: Send + Sync + 'static {
    #[doc(hidden)]
    fn define(self) -> LoginProviderKind;
}

#[derive(Clone)]
pub(crate) struct LoginDefinitionInner {
    pub(crate) name: &'static str,
    pub(crate) start_type: TypeId,
    pub(crate) complete_type: TypeId,
    pub(crate) runtime: Arc<dyn ErasedLoginRuntime>,
}

impl LoginDefinitionInner {
    pub(crate) fn new<Start, Complete, Definition>(
        method: LoginMethod<Start, Complete>,
        definition: Definition,
    ) -> Self
    where
        Start: Send + 'static,
        Complete: Send + 'static,
        Definition: LoginProviderDefinition<Start, Complete>,
    {
        Self {
            name: method.as_str(),
            start_type: TypeId::of::<Start>(),
            complete_type: TypeId::of::<Complete>(),
            runtime: definition.define().runtime,
        }
    }
}

impl fmt::Debug for LoginDefinitionInner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LoginDefinition")
            .field("name", &self.name)
            .field("flow", &self.runtime.is_flow())
            .finish()
    }
}
