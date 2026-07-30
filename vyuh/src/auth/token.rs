//! Normalized authenticated token values shared by every parseable format.

use std::fmt;

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use super::{
    AudienceId, AuthError, AuthProvider, AuthUser, AuthenticationContext, ProviderId, RoleType,
};

const MAX_SUBJECT_BYTES: usize = 512;
const MAX_AUDIENCES: usize = 32;
const MAX_NAME_BYTES: usize = 128;
const MAX_AUTH_METHODS: usize = 16;
const MAX_PAYLOAD_BYTES: usize = 16 * 1024;

/// The protocol role carried by an authenticated token.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenKind {
    /// Authorizes normal protected operations.
    Access,
    /// Authorizes credential rotation through [`super::Authenticator::refresh`].
    Refresh,
}

/// A normalized, authenticated token decoded from JWT, PASETO, BRANCA, or Django signing.
#[derive(Clone, Serialize, Deserialize)]
pub struct AuthToken {
    version: u8,
    #[serde(rename = "prv")]
    provider: ProviderId,
    kind: TokenKind,
    #[serde(rename = "sub")]
    subject: String,
    roles: RoleType,
    #[serde(
        default,
        rename = "aud",
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_audiences"
    )]
    audiences: Option<Vec<AudienceId>>,
    #[serde(rename = "iat")]
    issued_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    auth_time: Option<i64>,
    #[serde(default, rename = "amr", skip_serializing_if = "Vec::is_empty")]
    authentication_methods: Vec<String>,
    #[serde(rename = "acr", skip_serializing_if = "Option::is_none")]
    authentication_class: Option<String>,
    #[serde(rename = "nbf", skip_serializing_if = "Option::is_none")]
    not_before: Option<i64>,
    #[serde(rename = "exp")]
    expires_at: i64,
    #[serde(rename = "jti", skip_serializing_if = "Option::is_none")]
    token_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    family_id: Option<String>,
    #[serde(rename = "iss", skip_serializing_if = "Option::is_none")]
    issuer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    binding: Option<String>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    payload: serde_json::Value,
}

impl AuthToken {
    /// Starts a complete normalized-token builder for an external decoder.
    pub fn builder(
        provider: AuthProvider,
        kind: TokenKind,
        subject: impl Into<String>,
        issued_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> AuthTokenBuilder {
        AuthTokenBuilder::new(provider, kind, subject, issued_at, expires_at)
    }

    /// Returns the token envelope version.
    pub const fn version(&self) -> u8 {
        self.version
    }

    /// Returns the provider identity authenticated by the codec.
    pub fn provider(&self) -> &str {
        self.provider.as_str()
    }

    /// Returns whether this token authorizes access or refresh.
    pub const fn kind(&self) -> TokenKind {
        self.kind
    }

    /// Returns the stable identity subject.
    pub fn subject(&self) -> &str {
        &self.subject
    }

    /// Returns the embedded role mask.
    pub const fn roles(&self) -> RoleType {
        self.roles
    }

    /// Returns the authenticated audience names.
    pub fn audiences(&self) -> impl Iterator<Item = &str> {
        self.audiences.iter().flatten().map(AudienceId::as_str)
    }

    /// Returns the token issuance time.
    pub fn issued_at(&self) -> Result<DateTime<Utc>, AuthError> {
        timestamp(self.issued_at)
    }

    /// Returns identity-proof context carried by this token.
    pub fn authentication(&self) -> AuthenticationContext {
        AuthenticationContext::new(
            self.auth_time,
            self.authentication_methods.clone(),
            self.authentication_class.clone(),
        )
    }

    /// Returns the earliest valid time when configured.
    pub fn not_before(&self) -> Result<Option<DateTime<Utc>>, AuthError> {
        self.not_before.map(timestamp).transpose()
    }

    /// Returns the mandatory token expiry.
    pub fn expires_at(&self) -> Result<DateTime<Utc>, AuthError> {
        timestamp(self.expires_at)
    }

    /// Returns the unique token identifier.
    pub fn token_id(&self) -> Option<&str> {
        self.token_id.as_deref()
    }

    /// Returns the identifier shared by one access/refresh rotation family.
    pub fn family_id(&self) -> Option<&str> {
        self.family_id.as_deref()
    }

    /// Returns the asserted issuer when present.
    pub fn issuer(&self) -> Option<&str> {
        self.issuer.as_deref()
    }

    /// Returns authenticated application payload data.
    pub fn payload(&self) -> &serde_json::Value {
        &self.payload
    }

    pub(crate) fn audience_ids(&self) -> Option<&[AudienceId]> {
        self.audiences.as_deref()
    }

    pub(crate) fn set_audiences(&mut self, values: Vec<AudienceId>) {
        self.audiences = Some(values);
    }

    pub(crate) fn binding_value(&self) -> Option<&str> {
        self.binding.as_deref()
    }

    pub(crate) fn authentication_time(&self) -> Result<Option<DateTime<Utc>>, AuthError> {
        self.auth_time.map(timestamp).transpose()
    }

    pub(crate) fn embedded_user(&self) -> AuthUser {
        AuthUser::new(&self.subject)
            .with_role_mask(self.roles)
            .with_authentication(self.authentication())
    }

    pub(crate) fn issued(input: LocalToken<'_>) -> Self {
        let issued_at = Utc::now().timestamp();
        Self {
            version: 1,
            provider: input.provider,
            kind: input.kind,
            subject: input.user.key.to_string(),
            roles: input.user.roles,
            audiences: Some(input.audiences),
            issued_at,
            auth_time: authentication_time(input.user, issued_at),
            authentication_methods: input.user.authentication().methods().to_vec(),
            authentication_class: input.user.authentication().acr().map(str::to_owned),
            not_before: None,
            expires_at: input.expires_at,
            token_id: Some(uuid::Uuid::new_v4().to_string()),
            family_id: input.family_id,
            issuer: input.issuer,
            binding: input.binding,
            payload: serde_json::Value::Null,
        }
    }
}

pub(crate) struct LocalToken<'a> {
    pub(crate) provider: ProviderId,
    pub(crate) kind: TokenKind,
    pub(crate) user: &'a AuthUser,
    pub(crate) audiences: Vec<AudienceId>,
    pub(crate) expires_at: i64,
    pub(crate) family_id: Option<String>,
    pub(crate) binding: Option<String>,
    pub(crate) issuer: Option<String>,
}

fn authentication_time(user: &AuthUser, fallback: i64) -> Option<i64> {
    match user.authentication().timestamp() {
        None => Some(fallback),
        value => value,
    }
}

/// Builder used by externally authenticated token decoders.
pub struct AuthTokenBuilder {
    token: Result<AuthToken, AuthError>,
}

impl AuthTokenBuilder {
    fn new(
        provider: AuthProvider,
        kind: TokenKind,
        subject: impl Into<String>,
        issued_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Self {
        let token = ProviderId::new(provider.as_str()).map(|provider| AuthToken {
            version: 1,
            provider,
            kind,
            subject: subject.into(),
            roles: 0,
            audiences: None,
            issued_at: issued_at.timestamp(),
            auth_time: None,
            authentication_methods: Vec::new(),
            authentication_class: None,
            not_before: None,
            expires_at: expires_at.timestamp(),
            token_id: None,
            family_id: None,
            issuer: None,
            binding: None,
            payload: serde_json::Value::Null,
        });
        Self { token }
    }

    /// Sets the embedded role mask.
    pub fn roles(mut self, roles: RoleType) -> Self {
        if let Ok(token) = &mut self.token {
            token.roles = roles;
        }
        self
    }

    /// Sets authenticated audience names; an explicitly empty collection is invalid.
    pub fn audiences<I, S>(mut self, audiences: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.token = self.token.and_then(|mut token| {
            let values = collect_wire_audiences(audiences)?;
            token.audiences = Some(values);
            Ok(token)
        });
        self
    }

    /// Sets the earliest accepted time.
    pub fn not_before(mut self, value: Option<DateTime<Utc>>) -> Self {
        if let Ok(token) = &mut self.token {
            token.not_before = value.map(|item| item.timestamp());
        }
        self
    }

    /// Sets the authenticated issuer.
    pub fn issuer(mut self, value: Option<impl Into<String>>) -> Self {
        if let Ok(token) = &mut self.token {
            token.issuer = value.map(Into::into);
        }
        self
    }

    /// Sets an optional token identifier.
    pub fn token_id(mut self, value: Option<impl Into<String>>) -> Self {
        if let Ok(token) = &mut self.token {
            token.token_id = value.map(Into::into);
        }
        self
    }

    /// Sets an optional refresh-family identifier.
    pub fn family_id(mut self, value: Option<impl Into<String>>) -> Self {
        if let Ok(token) = &mut self.token {
            token.family_id = value.map(Into::into);
        }
        self
    }

    /// Sets authenticated proof time, methods, and assurance class.
    pub fn authentication<I, S>(
        mut self,
        auth_time: Option<DateTime<Utc>>,
        methods: I,
        class: Option<impl Into<String>>,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        if let Ok(token) = &mut self.token {
            token.auth_time = auth_time.map(|value| value.timestamp());
            token.authentication_methods = methods.into_iter().map(Into::into).collect();
            token.authentication_class = class.map(Into::into);
        }
        self
    }

    /// Sets authenticated credential binding.
    pub fn binding(mut self, value: Option<impl Into<String>>) -> Self {
        if let Ok(token) = &mut self.token {
            token.binding = value.map(Into::into);
        }
        self
    }

    /// Sets bounded, authenticated application payload data.
    pub fn payload(mut self, value: impl Into<serde_json::Value>) -> Self {
        if let Ok(token) = &mut self.token {
            token.payload = value.into();
        }
        self
    }

    /// Validates and returns the normalized token.
    pub fn build(self) -> Result<AuthToken, AuthError> {
        let token = self.token?;
        validate_structure(&token)?;
        Ok(token)
    }
}

fn collect_wire_audiences<I, S>(audiences: I) -> Result<Vec<AudienceId>, AuthError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut output = Vec::new();
    for value in audiences {
        let value = AudienceId::new(value.as_ref())?;
        if !output.contains(&value) {
            output.push(value);
        }
    }
    if output.is_empty() || output.len() > MAX_AUDIENCES {
        return Err(AuthError::InvalidCredential);
    }
    Ok(output)
}

#[derive(Deserialize)]
#[serde(untagged)]
enum WireAudiences {
    One(String),
    Many(Vec<String>),
}

fn deserialize_audiences<'de, D>(deserializer: D) -> Result<Option<Vec<AudienceId>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<WireAudiences>::deserialize(deserializer)?;
    let values = match value {
        None => return Ok(None),
        Some(WireAudiences::One(value)) => vec![value],
        Some(WireAudiences::Many(values)) => values,
    };
    values
        .into_iter()
        .map(|value| AudienceId::new(value).map_err(serde::de::Error::custom))
        .collect::<Result<Vec<_>, _>>()
        .map(Some)
}

pub(crate) fn validate_structure(token: &AuthToken) -> Result<(), AuthError> {
    validate_names(token)?;
    validate_times(token)?;
    validate_payload(token)
}

fn validate_names(token: &AuthToken) -> Result<(), AuthError> {
    if token.version != 1
        || ProviderId::new(token.provider.as_str()).is_err()
        || token.subject.trim().is_empty()
        || token.subject.len() > MAX_SUBJECT_BYTES
        || invalid_optional_name(token.token_id.as_deref(), 512)
        || invalid_optional_name(token.family_id.as_deref(), 512)
        || invalid_optional_name(token.issuer.as_deref(), 2048)
        || invalid_optional_name(token.binding.as_deref(), 2048)
    {
        return Err(AuthError::InvalidCredential);
    }
    if token.audiences.as_ref().is_some_and(Vec::is_empty)
        || token
            .audiences
            .as_ref()
            .is_some_and(|value| value.len() > MAX_AUDIENCES)
        || token.authentication_methods.len() > MAX_AUTH_METHODS
    {
        return Err(AuthError::InvalidCredential);
    }
    if token
        .audiences
        .iter()
        .flatten()
        .any(|value| AudienceId::new(value.as_str()).is_err())
    {
        return Err(AuthError::InvalidCredential);
    }
    let names = token
        .authentication_methods
        .iter()
        .map(String::as_str)
        .chain(token.authentication_class.as_deref());
    if names
        .into_iter()
        .any(|value| value.is_empty() || value.len() > MAX_NAME_BYTES)
    {
        return Err(AuthError::InvalidCredential);
    }
    Ok(())
}

fn invalid_optional_name(value: Option<&str>, maximum: usize) -> bool {
    value.is_some_and(|value| value.is_empty() || value.len() > maximum)
}

fn validate_times(token: &AuthToken) -> Result<(), AuthError> {
    timestamp(token.issued_at)?;
    timestamp(token.expires_at)?;
    if token.issued_at >= token.expires_at
        || token
            .not_before
            .is_some_and(|value| value > token.expires_at)
    {
        return Err(AuthError::InvalidCredential);
    }
    if let Some(value) = token.not_before {
        timestamp(value)?;
    }
    if let Some(value) = token.auth_time {
        timestamp(value)?;
    }
    Ok(())
}

fn validate_payload(token: &AuthToken) -> Result<(), AuthError> {
    let size = serde_json::to_vec(&token.payload)
        .map_err(|_| AuthError::InvalidCredential)?
        .len();
    if size > MAX_PAYLOAD_BYTES {
        return Err(AuthError::InvalidCredential);
    }
    Ok(())
}

impl fmt::Debug for AuthToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthToken")
            .field("provider", &self.provider)
            .field("kind", &self.kind)
            .field("subject", &self.subject)
            .field("audiences", &self.audiences)
            .field("expires_at", &self.expires_at)
            .field("token_id", &self.token_id)
            .field("family_id", &self.family_id)
            .field("payload", &"<redacted>")
            .finish()
    }
}

/// A redacted credential supplied to a token decoder.
pub struct PresentedCredential<'a>(&'a str);

impl<'a> PresentedCredential<'a> {
    pub(crate) const fn new(value: &'a str) -> Self {
        Self(value)
    }

    /// Deliberately exposes the encoded credential to a decoder implementation.
    pub const fn expose(&self) -> &'a str {
        self.0
    }
}

/// A redacted credential produced by a token encoder.
pub struct EncodedCredential(String);

impl EncodedCredential {
    /// Wraps a credential produced by a custom encoder.
    pub fn new(value: impl Into<String>) -> Result<Self, AuthError> {
        let value = value.into();
        if value.is_empty() {
            return Err(AuthError::InvalidCredential);
        }
        Ok(Self(value))
    }

    /// Deliberately exposes the encoded value to application code.
    pub fn expose(&self) -> &str {
        &self.0
    }

    pub(crate) fn into_inner(self) -> String {
        self.0
    }
}

fn timestamp(value: i64) -> Result<DateTime<Utc>, AuthError> {
    Utc.timestamp_opt(value, 0)
        .single()
        .ok_or(AuthError::InvalidCredential)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token_json(audience: serde_json::Value) -> serde_json::Value {
        let now = Utc::now().timestamp();
        serde_json::json!({
            "version": 1,
            "prv": "default",
            "kind": "access",
            "sub": "user-1",
            "roles": 0,
            "aud": audience,
            "iat": now,
            "exp": now + 60
        })
    }

    /// Verifies the JWT-standard scalar audience form normalizes to one audience.
    #[test]
    fn scalar_audience_deserializes() -> Result<(), AuthError> {
        let token = serde_json::from_value::<AuthToken>(token_json("api".into()))
            .map_err(|_| AuthError::InvalidCredential)?;
        validate_structure(&token)?;
        assert_eq!(token.audiences().collect::<Vec<_>>(), vec!["api"]);
        Ok(())
    }

    /// Verifies the JWT-standard audience array preserves all named surfaces.
    #[test]
    fn array_audience_deserializes() -> Result<(), AuthError> {
        let token =
            serde_json::from_value::<AuthToken>(token_json(serde_json::json!(["api", "reports"])))
                .map_err(|_| AuthError::InvalidCredential)?;
        validate_structure(&token)?;
        assert_eq!(
            token.audiences().collect::<Vec<_>>(),
            vec!["api", "reports"]
        );
        Ok(())
    }
}
