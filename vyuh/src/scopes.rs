//! Exact application scopes and extractor-driven authorization.

use std::{borrow::Borrow, fmt, marker::PhantomData, sync::Arc};

use axum::{
    extract::{FromRequestParts, OptionalFromRequestParts},
    http::request::Parts,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{
    OperationId, Site,
    auth::{AuthError, AuthUser},
};

pub(crate) const MAX_SCOPES: usize = 128;
pub(crate) const MAX_SCOPE_BYTES: usize = 128;
pub(crate) const MAX_SCOPES_BYTES: usize = 8 * 1024;

#[derive(Clone)]
enum ScopeName {
    Static(&'static str),
    Shared(Arc<str>),
}

/// An exact application authorization grant.
#[derive(Clone)]
pub struct Scope(ScopeName);

impl Scope {
    /// Declares an allocation-free static scope.
    pub const fn of(value: &'static str) -> Self {
        Self(ScopeName::Static(value))
    }

    /// Creates a dynamically owned scope backed by shared string storage.
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(ScopeName::Shared(value.into()))
    }

    /// Returns the exact scope name.
    pub fn as_str(&self) -> &str {
        match &self.0 {
            ScopeName::Static(value) => value,
            ScopeName::Shared(value) => value,
        }
    }
}

impl fmt::Debug for Scope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("Scope")
            .field(&self.as_str())
            .finish()
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(formatter)
    }
}

impl AsRef<str> for Scope {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for Scope {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl PartialEq for Scope {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}

impl Eq for Scope {}

impl PartialOrd for Scope {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Scope {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl std::hash::Hash for Scope {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl Serialize for Scope {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Scope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self::from)
    }
}

impl From<String> for Scope {
    fn from(value: String) -> Self {
        Self::new(Arc::<str>::from(value))
    }
}

impl From<Arc<str>> for Scope {
    fn from(value: Arc<str>) -> Self {
        Self::new(value)
    }
}

/// A flat static application-scope requirement.
#[derive(Clone, Copy, Debug)]
pub enum ScopeExpr {
    /// Requires every declared scope.
    All(&'static [Scope]),
    /// Requires at least one declared scope.
    Any(&'static [Scope]),
}

impl ScopeExpr {
    /// Requires every scope in one static declaration.
    pub const fn all(scopes: &'static [Scope]) -> Self {
        Self::All(scopes)
    }

    /// Requires at least one scope in one static declaration.
    pub const fn any(scopes: &'static [Scope]) -> Self {
        Self::Any(scopes)
    }

    pub(crate) const fn scopes(self) -> &'static [Scope] {
        match self {
            Self::All(scopes) | Self::Any(scopes) => scopes,
        }
    }

    pub(crate) const fn requires_all(self) -> bool {
        matches!(self, Self::All(_))
    }

    pub(crate) fn allows(self, user: &AuthUser) -> bool {
        match self {
            Self::All(scopes) => user.has_all(scopes),
            Self::Any(scopes) => user.has_any(scopes),
        }
    }
}

/// Defines one statically introspectable application-scope rule.
pub trait ScopeRule: Send + Sync + 'static {
    /// The complete flat scope expression enforced by [`Permit`].
    const EXPR: ScopeExpr;
}

/// Authenticates a user and enforces one static application-scope rule.
pub struct Permit<R: ScopeRule> {
    user: AuthUser,
    marker: PhantomData<fn() -> R>,
}

impl<R: ScopeRule> Permit<R> {
    /// Returns the accepted identity.
    pub fn user(&self) -> &AuthUser {
        &self.user
    }

    /// Consumes the permit and returns the accepted identity.
    pub fn into_user(self) -> AuthUser {
        self.user
    }

    /// Creates an extractor result after framework authorization succeeds.
    pub(crate) fn new(user: AuthUser) -> Self {
        Self {
            user,
            marker: PhantomData,
        }
    }
}

impl<R: ScopeRule> FromRequestParts<Site> for Permit<R> {
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, site: &Site) -> Result<Self, Self::Rejection> {
        if parts.extensions.get::<OperationId>().is_none() {
            validate_expr(R::EXPR).map_err(|_| {
                AuthError::Internal("an application scope rule is invalid".to_string())
            })?;
        }
        let user = <AuthUser as FromRequestParts<Site>>::from_request_parts(parts, site).await?;
        if !R::EXPR.allows(&user) {
            return Err(AuthError::Forbidden);
        }
        Ok(Self::new(user))
    }
}

impl<R: ScopeRule> OptionalFromRequestParts<Site> for Permit<R> {
    type Rejection = AuthError;

    async fn from_request_parts(
        parts: &mut Parts,
        site: &Site,
    ) -> Result<Option<Self>, Self::Rejection> {
        match <Self as FromRequestParts<Site>>::from_request_parts(parts, site).await {
            Ok(permit) => Ok(Some(permit)),
            Err(AuthError::NoCredential) => Ok(None),
            Err(error) => Err(error),
        }
    }
}

impl<R: ScopeRule> crate::callables::IntoArgPart for Permit<R> {
    fn into_arg_part() -> crate::callables::ArgPart {
        crate::callables::ArgPart::Composite(vec![
            crate::callables::ArgPart::Authentication,
            crate::callables::ArgPart::Security {
                scheme: "vyuhAuth".into(),
                scopes: Vec::new(),
                join_all: false,
            },
            crate::callables::ArgPart::Authorization {
                scopes: R::EXPR.scopes().to_vec(),
                all: R::EXPR.requires_all(),
            },
            crate::callables::ArgPart::Response(vec![
                crate::callables::ReturnSpec::error(401, "Unauthorized."),
                crate::callables::ReturnSpec::error(403, "Forbidden."),
            ]),
        ])
    }
}

/// Sorts and deduplicates one identity's scopes for bounded binary-search lookup.
pub(crate) fn normalize(values: impl IntoIterator<Item = Scope>) -> Arc<[Scope]> {
    let mut scopes = values.into_iter().collect::<Vec<_>>();
    scopes.sort_unstable();
    scopes.dedup();
    scopes.into()
}

/// Validates one trusted identity or credential scope collection.
pub(crate) fn validate_scopes(scopes: &[Scope]) -> Result<(), ScopeRuleError> {
    if scopes.len() > MAX_SCOPES || scopes.iter().any(|scope| !valid_name(scope.as_str())) {
        return Err(ScopeRuleError::Invalid);
    }
    let bytes = scopes.iter().try_fold(0usize, |total, scope| {
        total
            .checked_add(scope.as_str().len())
            .and_then(|value| value.checked_add(1))
    });
    if bytes.is_none_or(|value| value.saturating_sub(1) > MAX_SCOPES_BYTES) {
        return Err(ScopeRuleError::Invalid);
    }
    Ok(())
}

/// Validates and deterministically orders one static authorization rule.
pub(crate) fn normalize_rule(scopes: &mut Vec<Scope>) -> Result<(), ScopeRuleError> {
    if scopes.is_empty() {
        return Err(ScopeRuleError::Empty);
    }
    validate_scopes(scopes)?;
    scopes.sort_unstable();
    if scopes.windows(2).any(|pair| pair.first() == pair.get(1)) {
        return Err(ScopeRuleError::Duplicate);
    }
    Ok(())
}

/// Defensively validates a static rule used outside finalized Vyuh operations.
pub(crate) fn validate_expr(expr: ScopeExpr) -> Result<(), ScopeRuleError> {
    let mut scopes = expr.scopes().to_vec();
    normalize_rule(&mut scopes)
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SCOPE_BYTES
        && value.bytes().all(|byte| {
            byte == b'!' || (b'#'..=b'[').contains(&byte) || (b']'..=b'~').contains(&byte)
        })
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum ScopeRuleError {
    Empty,
    Duplicate,
    Invalid,
}

impl fmt::Display for ScopeRuleError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("scope rules cannot be empty"),
            Self::Duplicate => formatter.write_str("scope rules cannot contain duplicates"),
            Self::Invalid => formatter.write_str("scope rule limits or syntax are invalid"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies static and dynamic scopes compare solely by their textual names.
    #[test]
    fn scope_identity_is_textual() {
        let static_scope = Scope::of("users:read");
        let dynamic_scope = Scope::new(Arc::<str>::from("users:read"));
        assert_eq!(static_scope, dynamic_scope);
        assert_eq!(format!("{static_scope:?}"), "Scope(\"users:read\")");
    }

    /// Verifies scopes use an exact string JSON representation across storage variants.
    #[test]
    fn scope_serde_is_string_based() -> Result<(), serde_json::Error> {
        let value = serde_json::to_value(Scope::of("users:read"))?;
        let decoded = serde_json::from_value::<Scope>(value.clone())?;
        assert_eq!(value, serde_json::json!("users:read"));
        assert_eq!(decoded, Scope::new("users:read"));
        Ok(())
    }

    /// Verifies normalized grants are sorted, deduplicated, and binary searchable.
    #[test]
    fn normalized_scopes_are_searchable() {
        let user = AuthUser::new("user-1").with_scopes([
            Scope::of("users:write"),
            Scope::of("users:read"),
            Scope::of("users:read"),
        ]);
        assert_eq!(
            user.scopes(),
            &[Scope::of("users:read"), Scope::of("users:write")]
        );
        assert!(user.has_scope(&Scope::of("users:read")));
        assert!(user.has_all(&[Scope::of("users:read"), Scope::of("users:write")]));
        assert!(user.has_any(&[Scope::of("missing"), Scope::of("users:write")]));
    }

    /// Verifies empty, duplicate, malformed, and oversized static rules are rejected.
    #[test]
    fn invalid_rules_are_rejected() {
        assert!(matches!(
            validate_expr(ScopeExpr::all(&[])),
            Err(ScopeRuleError::Empty)
        ));
        const DUPLICATE: &[Scope] = &[Scope::of("read"), Scope::of("read")];
        assert!(matches!(
            validate_expr(ScopeExpr::any(DUPLICATE)),
            Err(ScopeRuleError::Duplicate)
        ));
        const INVALID: &[Scope] = &[Scope::of("not valid")];
        assert!(matches!(
            validate_expr(ScopeExpr::all(INVALID)),
            Err(ScopeRuleError::Invalid)
        ));
    }

    /// Verifies trusted scope collections enforce fixed count, name, and aggregate bounds.
    #[test]
    fn trusted_scope_collections_are_bounded() {
        let too_many = (0..=MAX_SCOPES)
            .map(|index| Scope::new(format!("scope:{index}")))
            .collect::<Vec<_>>();
        assert!(validate_scopes(&too_many).is_err());
        assert!(validate_scopes(&[Scope::new("x".repeat(MAX_SCOPE_BYTES + 1))]).is_err());

        let aggregate = (0..MAX_SCOPES)
            .map(|index| Scope::new(format!("{index:03}:{}", "x".repeat(62))))
            .collect::<Vec<_>>();
        assert!(validate_scopes(&aggregate).is_err());
    }
}
