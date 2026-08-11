use std::{
    any::Any,
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use serde::{Deserialize, Serialize};

use super::{AuthError, Scope};

/// A reusable name for an authenticated API surface.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Audience(&'static str);

impl Audience {
    /// Declares an audience. The name is validated when it is registered or issued.
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// Returns the declared audience name.
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

/// A reusable name for one configured authentication provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct AuthProvider(&'static str);

impl AuthProvider {
    /// Declares a provider; registration or a terminal operation validates its name.
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// Returns the declared provider name.
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

/// The built-in JWT provider installed by [`super::AuthConf::development`].
pub const DEFAULT_AUTH_PROVIDER: AuthProvider = AuthProvider::new("default");

/// The compatibility audience used when an application does not declare one.
pub const DEFAULT_AUDIENCE: Audience = Audience::new("default");

/// Authenticated assurance metadata carried across credential rotations.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthenticationContext {
    #[serde(skip_serializing_if = "Option::is_none")]
    auth_time: Option<i64>,
    methods: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    acr: Option<String>,
}

impl AuthenticationContext {
    /// Returns when the current identity proof completed.
    pub fn auth_time(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        self.auth_time
            .and_then(|value| chrono::DateTime::from_timestamp(value, 0))
    }

    /// Returns authenticated method-reference names such as `password` or `totp`.
    pub fn methods(&self) -> &[String] {
        &self.methods
    }

    /// Returns the authentication-context class when one was asserted.
    pub fn acr(&self) -> Option<&str> {
        self.acr.as_deref()
    }

    /// Returns whether identity proof included one named method.
    pub fn has_method(&self, method: &str) -> bool {
        self.methods.iter().any(|value| value == method)
    }

    pub(crate) fn new(auth_time: Option<i64>, methods: Vec<String>, acr: Option<String>) -> Self {
        Self {
            auth_time,
            methods,
            acr,
        }
    }

    pub(crate) const fn timestamp(&self) -> Option<i64> {
        self.auth_time
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct ProviderId(String);

impl ProviderId {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, AuthError> {
        let value = value.into();
        if value.trim().is_empty()
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(AuthError::InvalidProviderId(value));
        }
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProviderId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct AudienceId(String);

impl AudienceId {
    pub(crate) fn new(value: impl Into<String>) -> Result<Self, AuthError> {
        let value = value.into();
        if value.trim().is_empty()
            || value.len() > 128
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
            })
        {
            return Err(AuthError::InvalidAudience(value));
        }
        Ok(Self(value))
    }

    pub(crate) fn declared(value: Audience) -> Result<Self, AuthError> {
        Self::new(value.as_str())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// The provider-independent identity accepted by Vyuh.
#[derive(Clone, Serialize)]
pub struct AuthUser {
    subject: Arc<str>,
    scopes: Arc<[Scope]>,
    #[serde(skip)]
    provider: Option<ProviderId>,
    #[serde(skip)]
    extra: Option<Arc<dyn Any + Send + Sync>>,
    #[serde(skip)]
    authentication: AuthenticationContext,
}

impl AuthUser {
    /// Creates an identity without application scopes.
    pub fn new(subject: impl AsRef<str>) -> Self {
        Self {
            subject: Arc::from(subject.as_ref()),
            scopes: Arc::from([]),
            provider: None,
            extra: None,
            authentication: AuthenticationContext::default(),
        }
    }

    /// Returns the stable provider-independent authenticated subject.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Adds one exact application scope.
    pub fn with_scope(mut self, scope: Scope) -> Self {
        let mut values = self.scopes.to_vec();
        if let Err(position) = values.binary_search(&scope) {
            values.insert(position, scope);
            self.scopes = values.into();
        }
        self
    }

    /// Adds exact application scopes, preserving sorted unique storage.
    pub fn with_scopes<I>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = Scope>,
    {
        let values = self.scopes.iter().cloned().chain(scopes);
        self.scopes = crate::scopes::normalize(values);
        self
    }

    /// Returns normalized application scopes in lexical order.
    pub fn scopes(&self) -> &[Scope] {
        &self.scopes
    }

    /// Returns whether the identity has one exact scope.
    pub fn has_scope(&self, scope: &Scope) -> bool {
        self.scopes.binary_search(scope).is_ok()
    }

    /// Returns whether the identity has every supplied scope.
    pub fn has_all(&self, scopes: &[Scope]) -> bool {
        scopes.iter().all(|scope| self.has_scope(scope))
    }

    /// Returns whether the identity has at least one supplied scope.
    pub fn has_any(&self, scopes: &[Scope]) -> bool {
        scopes.iter().any(|scope| self.has_scope(scope))
    }

    /// Attaches cloneable request-only application data.
    pub fn with_extra<T: Any + Send + Sync>(mut self, extra: T) -> Self {
        self.extra = Some(Arc::new(extra));
        self
    }

    /// Returns attached request-only application data of type `T`.
    pub fn extra<T: Any + Send + Sync>(&self) -> Option<&T> {
        self.extra.as_deref()?.downcast_ref::<T>()
    }

    /// Removes request-only application data before forwarding an identity.
    pub fn without_extra(mut self) -> Self {
        self.extra = None;
        self
    }

    /// Returns the provider that accepted the presented credential.
    pub fn provider(&self) -> Option<&str> {
        self.provider.as_ref().map(ProviderId::as_str)
    }

    /// Returns authenticated method and assurance metadata for this identity.
    pub fn authentication(&self) -> &AuthenticationContext {
        &self.authentication
    }

    /// Returns whether this identity was accepted by `provider`.
    pub fn is_from(&self, provider: AuthProvider) -> bool {
        self.provider()
            .is_some_and(|value| value == provider.as_str())
    }

    /// Parses the subject into an application identifier.
    pub fn parse_subject<T>(&self) -> Result<T, AuthError>
    where
        T: std::str::FromStr,
    {
        self.subject
            .parse()
            .map_err(|_| AuthError::InvalidCredential)
    }

    pub(crate) fn set_provider(mut self, provider: ProviderId) -> Self {
        self.provider = Some(provider);
        self
    }

    pub(crate) fn with_authentication(mut self, value: AuthenticationContext) -> Self {
        self.authentication = value;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), AuthError> {
        if self.subject.trim().is_empty() {
            return Err(AuthError::InvalidCredential);
        }
        crate::scopes::validate_scopes(&self.scopes).map_err(|_| AuthError::InvalidCredential)
    }
}

impl fmt::Debug for AuthUser {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthUser")
            .field("subject", &self.subject)
            .field("scopes", &self.scopes)
            .field("provider", &self.provider)
            .field("extra", &self.extra.as_ref().map(|_| "<redacted>"))
            .field("authentication", &self.authentication)
            .finish()
    }
}

impl PartialEq for AuthUser {
    fn eq(&self, other: &Self) -> bool {
        self.subject == other.subject && self.provider == other.provider
    }
}

impl Eq for AuthUser {}

impl Hash for AuthUser {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.subject.hash(state);
        self.provider.hash(state);
    }
}

pub(crate) fn collect_audiences(values: &[Audience]) -> Result<Vec<AudienceId>, AuthError> {
    let mut output = Vec::with_capacity(values.len());
    for value in values {
        let audience = AudienceId::declared(*value)?;
        if !output.contains(&audience) {
            output.push(audience);
        }
    }
    if output.is_empty() {
        return Err(AuthError::InvalidProviderConfig(
            "credentials require at least one audience".into(),
        ));
    }
    Ok(output)
}

/// Resolves an audience slice through the site compatibility policy.
pub(crate) fn resolve_audiences(
    values: &[Audience],
    default: Option<&AudienceId>,
) -> Result<Vec<AudienceId>, AuthError> {
    if values.is_empty() {
        return default
            .cloned()
            .map(|value| vec![value])
            .ok_or(AuthError::InvalidProviderConfig(
                "an explicit audience is required".into(),
            ));
    }
    collect_audiences(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies a newly constructed identity serializes its subject without a provider.
    #[test]
    fn new_identity_has_subject_without_provider() -> Result<(), AuthError> {
        let user = AuthUser::new("user-42");
        assert_eq!(user.subject(), "user-42");
        assert_eq!(user.provider(), None);
        let value = serde_json::to_value(user).map_err(|_| AuthError::InvalidCredential)?;
        assert_eq!(value["subject"], "user-42");
        assert!(value.get("key").is_none());
        assert!(value.get("provider").is_none());
        Ok(())
    }

    /// Verifies removing runtime extras preserves grants, provider, and assurance metadata.
    #[test]
    fn without_extra_preserves_authenticated_identity() -> Result<(), AuthError> {
        let authentication = AuthenticationContext::new(
            Some(1_700_000_000),
            vec!["password".to_string(), "totp".to_string()],
            Some("urn:example:mfa".to_string()),
        );
        let user = AuthUser::new("user-42")
            .with_scope(Scope::of("users:read"))
            .with_extra("request-only")
            .set_provider(ProviderId::new("accounts")?)
            .with_authentication(authentication.clone())
            .without_extra();

        assert!(user.extra::<&str>().is_none());
        assert!(user.has_scope(&Scope::of("users:read")));
        assert_eq!(user.provider(), Some("accounts"));
        assert_eq!(user.authentication(), &authentication);
        Ok(())
    }
}
