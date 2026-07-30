use axum::http::{HeaderName, HeaderValue, Method, header, request::Parts};
use axum_extra::extract::cookie::{self, Cookie};
use ring::constant_time;
use serde::{Deserialize, Serialize};

use super::AuthError;

const MAX_CREDENTIAL_BYTES: usize = 16 * 1024;

/// Explicit acknowledgement that query credentials leak through URLs and logs.
#[derive(Clone, Copy, Debug)]
pub struct UnsafeQueryCredentials(());

impl UnsafeQueryCredentials {
    /// Acknowledges URL leakage risk and enables query credential extraction.
    pub const fn allow() -> Self {
        Self(())
    }
}

/// SameSite policy used by an authentication cookie.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "lowercase")]
pub enum CookieSameSite {
    #[default]
    Lax,
    Strict,
    None,
}

impl From<CookieSameSite> for cookie::SameSite {
    fn from(value: CookieSameSite) -> Self {
        match value {
            CookieSameSite::Lax => Self::Lax,
            CookieSameSite::Strict => Self::Strict,
            CookieSameSite::None => Self::None,
        }
    }
}

/// Secure cookie delivery options for an issued credential.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CookieConf {
    pub name: String,
    pub path: String,
    pub http_only: bool,
    pub secure: bool,
    pub same_site: CookieSameSite,
}

impl CookieConf {
    /// Creates a secure, HttpOnly, SameSite=Lax cookie rooted at `/`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            path: "/".into(),
            http_only: true,
            secure: true,
            same_site: CookieSameSite::Lax,
        }
    }

    pub fn path(mut self, value: impl Into<String>) -> Self {
        self.path = value.into();
        self
    }

    pub fn http_only(mut self, value: bool) -> Self {
        self.http_only = value;
        self
    }

    pub fn secure(mut self, value: bool) -> Self {
        self.secure = value;
        self
    }

    pub fn same_site(mut self, value: CookieSameSite) -> Self {
        self.same_site = value;
        self
    }
}

impl From<&str> for CookieConf {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for CookieConf {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Double-submit cookie policy for an ambient authentication credential.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CsrfConf {
    pub cookie: CookieConf,
    pub header_name: String,
}

impl CsrfConf {
    /// Creates a readable CSRF cookie checked against `X-CSRF-Token`.
    pub fn new(cookie_name: impl Into<String>) -> Self {
        Self {
            cookie: CookieConf::new(cookie_name).http_only(false),
            header_name: "x-csrf-token".into(),
        }
    }

    /// Replaces the request header carrying the double-submit value.
    pub fn header(mut self, name: impl Into<String>) -> Self {
        self.header_name = name.into();
        self
    }

    pub(crate) fn validate(&self) -> Result<(), AuthError> {
        validate_cookie(&self.cookie)?;
        if self.cookie.http_only {
            return Err(AuthError::InvalidProviderConfig(
                "CSRF cookies must remain readable by the client".into(),
            ));
        }
        validate_header(&self.header_name, None, None)
    }

    pub(crate) fn verify(&self, parts: &Parts) -> Result<(), AuthError> {
        if safe_method(&parts.method) {
            return Ok(());
        }
        let header =
            HeaderName::try_from(&self.header_name).map_err(|_| AuthError::InvalidCsrfToken)?;
        let mut supplied_values = parts.headers.get_all(header).iter();
        let supplied = supplied_values
            .next()
            .and_then(|value| value.to_str().ok())
            .ok_or(AuthError::InvalidCsrfToken)?;
        if supplied.is_empty() || supplied_values.next().is_some() {
            return Err(AuthError::InvalidCsrfToken);
        }
        let expected =
            extract_cookie(parts, &self.cookie.name)?.ok_or(AuthError::InvalidCsrfToken)?;
        constant_time::verify_slices_are_equal(expected.as_bytes(), supplied.as_bytes())
            .map_err(|_| AuthError::InvalidCsrfToken)
    }

    pub(crate) fn attachment(
        &self,
        value: &str,
        ttl_seconds: i64,
    ) -> Result<(HeaderName, HeaderValue), AuthError> {
        cookie_header(&self.cookie, value, ttl_seconds)
    }

    pub(crate) fn clear(&self) -> Result<(HeaderName, HeaderValue), AuthError> {
        clear_cookie(&self.cookie)
    }
}

#[derive(Clone, Debug)]
enum LocationKind {
    Header {
        name: String,
        scheme: Option<String>,
    },
    Cookie(CookieConf),
    Query(String),
}

/// The request location from which a provider accepts one credential.
#[derive(Clone, Debug)]
pub(crate) struct CredentialLocation(LocationKind);

impl CredentialLocation {
    /// Uses the conventional `Authorization: Bearer` header.
    pub(crate) fn bearer() -> Self {
        Self::header_with_scheme(header::AUTHORIZATION.as_str(), "Bearer")
    }

    /// Uses a header without an authorization scheme prefix.
    pub(crate) fn header(name: impl Into<String>) -> Self {
        Self(LocationKind::Header {
            name: name.into(),
            scheme: None,
        })
    }

    /// Uses a header with a case-insensitive scheme prefix.
    pub(crate) fn header_with_scheme(name: impl Into<String>, scheme: impl Into<String>) -> Self {
        Self(LocationKind::Header {
            name: name.into(),
            scheme: Some(scheme.into()),
        })
    }

    /// Uses an HttpOnly cookie.
    pub(crate) fn cookie(cookie: impl Into<CookieConf>) -> Self {
        Self(LocationKind::Cookie(cookie.into()))
    }

    /// Uses a query parameter. This is extraction-only and may leak through URLs.
    pub(crate) fn query(name: impl Into<String>, _risk: UnsafeQueryCredentials) -> Self {
        Self(LocationKind::Query(name.into()))
    }

    pub(crate) fn selector(&self) -> String {
        match &self.0 {
            LocationKind::Header { name, scheme, .. } => format!(
                "header:{}:{}",
                name.to_ascii_lowercase(),
                scheme.as_deref().unwrap_or_default().to_ascii_lowercase()
            ),
            LocationKind::Cookie(cookie) => format!("cookie:{}", cookie.name),
            LocationKind::Query(name) => format!("query:{name}"),
        }
    }

    pub(crate) fn extract(&self, parts: &Parts) -> Result<Option<String>, AuthError> {
        let value = match &self.0 {
            LocationKind::Header { name, scheme, .. } => {
                extract_header(parts, name, scheme.as_deref())?
            }
            LocationKind::Cookie(cookie) => extract_cookie(parts, &cookie.name)?,
            LocationKind::Query(name) => extract_query(parts, name)?,
        };
        if value
            .as_ref()
            .is_some_and(|value| value.len() > MAX_CREDENTIAL_BYTES)
        {
            return Err(AuthError::MalformedLocation);
        }
        Ok(value)
    }

    pub(crate) fn attachment(
        &self,
        value: &str,
        ttl_seconds: i64,
    ) -> Result<Option<(HeaderName, HeaderValue)>, AuthError> {
        match &self.0 {
            LocationKind::Header { .. } => Ok(None),
            LocationKind::Cookie(cookie) => cookie_header(cookie, value, ttl_seconds).map(Some),
            LocationKind::Query(_) => Ok(None),
        }
    }

    pub(crate) fn clear(&self) -> Result<Option<(HeaderName, HeaderValue)>, AuthError> {
        let LocationKind::Cookie(cookie) = &self.0 else {
            return Ok(None);
        };
        clear_cookie(cookie).map(Some)
    }

    pub(crate) fn is_cookie(&self) -> bool {
        matches!(self.0, LocationKind::Cookie(_))
    }

    pub(crate) fn default_csrf(&self) -> Option<CsrfConf> {
        let LocationKind::Cookie(cookie) = &self.0 else {
            return None;
        };
        Some(CsrfConf {
            cookie: CookieConf::new(format!("{}_csrf", cookie.name))
                .path(cookie.path.clone())
                .secure(cookie.secure)
                .same_site(cookie.same_site)
                .http_only(false),
            header_name: "x-csrf-token".into(),
        })
    }

    pub(crate) fn validate(&self) -> Result<(), AuthError> {
        match &self.0 {
            LocationKind::Header { name, scheme } => validate_header(name, scheme.as_deref(), None),
            LocationKind::Cookie(cookie) => validate_cookie(cookie),
            LocationKind::Query(name) if name.trim().is_empty() => Err(
                AuthError::InvalidProviderConfig("credential query name cannot be empty".into()),
            ),
            LocationKind::Query(_) => Ok(()),
        }
    }

    pub(crate) fn validate_production_cookie(&self) -> Result<(), AuthError> {
        let LocationKind::Cookie(cookie) = &self.0 else {
            return Ok(());
        };
        if !cookie.secure || !cookie.http_only {
            return Err(AuthError::InvalidProviderConfig(
                "authentication cookies must be Secure and HttpOnly in production".into(),
            ));
        }
        if matches!(cookie.same_site, CookieSameSite::None) && !cookie.secure {
            return Err(AuthError::InvalidProviderConfig(
                "SameSite=None authentication cookies must be Secure".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn doc(&self) -> ProviderDocLocation {
        match &self.0 {
            LocationKind::Header { name, scheme, .. } => ProviderDocLocation::Header {
                name: name.clone(),
                scheme: scheme.clone(),
            },
            LocationKind::Cookie(cookie) => ProviderDocLocation::Cookie(cookie.name.clone()),
            LocationKind::Query(name) => ProviderDocLocation::Query(name.clone()),
        }
    }

    pub(crate) fn response_attachment(
        name: &str,
        value: &str,
    ) -> Result<(HeaderName, HeaderValue), AuthError> {
        response_header(name, None, value)
    }
}

#[derive(Clone)]
pub(crate) enum ProviderDocLocation {
    Header {
        name: String,
        scheme: Option<String>,
    },
    Cookie(String),
    Query(String),
}

fn validate_header(
    name: &str,
    scheme: Option<&str>,
    response_header: Option<&str>,
) -> Result<(), AuthError> {
    HeaderName::try_from(name)
        .map_err(|_| AuthError::InvalidProviderConfig("invalid credential header name".into()))?;
    if scheme.is_some_and(str::is_empty) {
        return Err(AuthError::InvalidProviderConfig(
            "credential header scheme cannot be empty".into(),
        ));
    }
    if let Some(response_header) = response_header {
        HeaderName::try_from(response_header)
            .map_err(|_| AuthError::InvalidProviderConfig("invalid response header name".into()))?;
    }
    Ok(())
}

fn validate_cookie(cookie: &CookieConf) -> Result<(), AuthError> {
    if cookie.name.trim().is_empty() || !cookie.path.starts_with('/') {
        return Err(AuthError::InvalidProviderConfig(
            "credential cookie requires a name and absolute path".into(),
        ));
    }
    Ok(())
}

fn extract_header(
    parts: &Parts,
    name: &str,
    scheme: Option<&str>,
) -> Result<Option<String>, AuthError> {
    let name = HeaderName::try_from(name).map_err(|_| AuthError::MalformedLocation)?;
    let mut values = parts.headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(AuthError::MalformedLocation);
    }
    let value = value.to_str().map_err(|_| AuthError::MalformedLocation)?;
    let raw = match scheme {
        Some(scheme) => value
            .split_once(' ')
            .filter(|(actual, raw)| actual.eq_ignore_ascii_case(scheme) && !raw.is_empty())
            .map(|(_, raw)| raw)
            .ok_or(AuthError::MalformedLocation)?,
        None => value,
    };
    if raw.is_empty() {
        return Err(AuthError::MalformedLocation);
    }
    Ok(Some(raw.to_owned()))
}

fn extract_query(parts: &Parts, key: &str) -> Result<Option<String>, AuthError> {
    let Some(query) = parts.uri.query() else {
        return Ok(None);
    };
    let mut found = None;
    for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
        if name != key {
            continue;
        }
        if value.is_empty() || found.is_some() {
            return Err(AuthError::MalformedLocation);
        }
        found = Some(value.into_owned());
    }
    Ok(found)
}

fn extract_cookie(parts: &Parts, name: &str) -> Result<Option<String>, AuthError> {
    let mut found = None;
    for header in parts.headers.get_all(header::COOKIE) {
        let header = header.to_str().map_err(|_| AuthError::MalformedLocation)?;
        for item in header.split(';') {
            let Some((candidate, value)) = item.trim().split_once('=') else {
                return Err(AuthError::MalformedLocation);
            };
            if candidate != name {
                continue;
            }
            if value.is_empty() || found.is_some() {
                return Err(AuthError::MalformedLocation);
            }
            found = Some(value.to_owned());
        }
    }
    Ok(found)
}

fn response_header(
    name: &str,
    scheme: Option<&str>,
    value: &str,
) -> Result<(HeaderName, HeaderValue), AuthError> {
    let name = HeaderName::try_from(name).map_err(|_| AuthError::DeliveryFailed)?;
    let rendered = scheme
        .map(|prefix| format!("{prefix} {value}"))
        .unwrap_or_else(|| value.to_owned());
    let value = HeaderValue::try_from(rendered).map_err(|_| AuthError::DeliveryFailed)?;
    Ok((name, value))
}

fn cookie_header(
    cookie: &CookieConf,
    value: &str,
    ttl_seconds: i64,
) -> Result<(HeaderName, HeaderValue), AuthError> {
    let value = Cookie::build((cookie.name.clone(), value.to_owned()))
        .path(cookie.path.clone())
        .http_only(cookie.http_only)
        .secure(cookie.secure)
        .same_site(cookie.same_site.into())
        .max_age(time::Duration::seconds(ttl_seconds))
        .build();
    let value = HeaderValue::try_from(value.to_string()).map_err(|_| AuthError::DeliveryFailed)?;
    Ok((header::SET_COOKIE, value))
}

fn clear_cookie(cookie: &CookieConf) -> Result<(HeaderName, HeaderValue), AuthError> {
    let value = Cookie::build((cookie.name.clone(), String::new()))
        .path(cookie.path.clone())
        .http_only(cookie.http_only)
        .secure(cookie.secure)
        .same_site(cookie.same_site.into())
        .max_age(time::Duration::ZERO)
        .build();
    let value = HeaderValue::try_from(value.to_string()).map_err(|_| AuthError::DeliveryFailed)?;
    Ok((header::SET_COOKIE, value))
}

fn safe_method(method: &Method) -> bool {
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::OPTIONS | Method::TRACE
    )
}
