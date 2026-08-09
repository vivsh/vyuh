use std::borrow::Cow;

use axum::http::{HeaderName, HeaderValue, Method, header, request::Parts};
use axum_extra::extract::cookie::{self, Cookie};
use ring::constant_time;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use super::AuthError;

const MAX_CREDENTIAL_BYTES: usize = 16 * 1024;
const MAX_CREDENTIAL_SOURCE_BYTES: usize = 64 * 1024;
type CookieValues<'a> = SmallVec<[(&'a str, Option<&'a str>); 8]>;
type QueryValues<'a> = SmallVec<[(Cow<'a, str>, Option<Cow<'a, str>>); 8]>;

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
        parsed: Option<HeaderName>,
        scheme: Option<String>,
    },
    Cookie(CookieConf),
    Query(String),
}

/// The request location from which a provider accepts one credential.
#[derive(Clone, Debug)]
pub(crate) struct CredentialLocation(LocationKind);

/// A validated request selector retained with an issued login result for test clients.
#[derive(Clone)]
pub(crate) enum RequestCredentialLocation {
    Header {
        name: String,
        scheme: Option<String>,
    },
    Cookie {
        name: String,
        csrf: Option<(String, String, String)>,
    },
    Query {
        name: String,
    },
}

pub(crate) struct RequestCredentialScan<'a> {
    parts: &'a Parts,
    cookies: Option<ParsedCookies<'a>>,
    query: Option<ParsedQuery<'a>>,
}

struct ParsedCookies<'a> {
    values: CookieValues<'a>,
    malformed: bool,
}

struct ParsedQuery<'a> {
    values: QueryValues<'a>,
    malformed: bool,
}

impl<'a> RequestCredentialScan<'a> {
    pub(crate) const fn new(parts: &'a Parts) -> Self {
        Self {
            parts,
            cookies: None,
            query: None,
        }
    }

    fn cookie(&mut self, name: &str) -> Result<Option<Cow<'a, str>>, AuthError> {
        let parsed = self
            .cookies
            .get_or_insert_with(|| parse_cookies(self.parts));
        if parsed.malformed {
            return Err(AuthError::MalformedLocation);
        }
        selected_borrowed(
            parsed
                .values
                .iter()
                .find(|(candidate, _)| *candidate == name)
                .map(|(_, value)| value),
        )
    }

    fn query(&mut self, name: &str) -> Result<Option<Cow<'a, str>>, AuthError> {
        let parsed = self.query.get_or_insert_with(|| parse_query(self.parts));
        if parsed.malformed {
            return Err(AuthError::MalformedLocation);
        }
        match parsed
            .values
            .iter()
            .find(|(candidate, _)| candidate.as_ref() == name)
            .map(|(_, value)| value)
        {
            Some(Some(value)) => Ok(Some(value.clone())),
            Some(None) => Err(AuthError::MalformedLocation),
            None => Ok(None),
        }
    }
}

impl CredentialLocation {
    /// Uses the conventional `Authorization: Bearer` header.
    pub(crate) fn bearer() -> Self {
        Self::header_with_scheme(header::AUTHORIZATION.as_str(), "Bearer")
    }

    /// Uses a header without an authorization scheme prefix.
    pub(crate) fn header(name: impl Into<String>) -> Self {
        let name = name.into();
        let parsed = HeaderName::try_from(&name).ok();
        Self(LocationKind::Header {
            name,
            parsed,
            scheme: None,
        })
    }

    /// Uses a header with a case-insensitive scheme prefix.
    pub(crate) fn header_with_scheme(name: impl Into<String>, scheme: impl Into<String>) -> Self {
        let name = name.into();
        let parsed = HeaderName::try_from(&name).ok();
        Self(LocationKind::Header {
            name,
            parsed,
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

    pub(crate) fn extract<'a>(&self, parts: &'a Parts) -> Result<Option<Cow<'a, str>>, AuthError> {
        let value = match &self.0 {
            LocationKind::Header { parsed, scheme, .. } => {
                extract_header(parts, parsed.as_ref(), scheme.as_deref())?
            }
            LocationKind::Cookie(cookie) => extract_cookie(parts, &cookie.name)?,
            LocationKind::Query(name) => extract_query(parts, name)?,
        };
        validate_extracted_size(value)
    }

    pub(crate) fn extract_from<'a>(
        &self,
        scan: &mut RequestCredentialScan<'a>,
    ) -> Result<Option<Cow<'a, str>>, AuthError> {
        let value = match &self.0 {
            LocationKind::Header { parsed, scheme, .. } => {
                extract_header(scan.parts, parsed.as_ref(), scheme.as_deref())?
            }
            LocationKind::Cookie(cookie) => scan.cookie(&cookie.name)?,
            LocationKind::Query(name) => scan.query(name)?,
        };
        validate_extracted_size(value)
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

    /// Captures the configured request selector for an issued access credential.
    pub(crate) fn request_selector(
        &self,
        csrf: Option<(String, String, String)>,
    ) -> RequestCredentialLocation {
        match &self.0 {
            LocationKind::Header { name, scheme, .. } => RequestCredentialLocation::Header {
                name: name.clone(),
                scheme: scheme.clone(),
            },
            LocationKind::Cookie(cookie) => RequestCredentialLocation::Cookie {
                name: cookie.name.clone(),
                csrf,
            },
            LocationKind::Query(name) => RequestCredentialLocation::Query { name: name.clone() },
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

    #[cfg(feature = "mcp")]
    pub(crate) fn is_header(&self) -> bool {
        matches!(self.0, LocationKind::Header { .. })
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
            LocationKind::Header {
                name,
                parsed,
                scheme,
            } => {
                if parsed.is_none() {
                    return Err(AuthError::InvalidProviderConfig(
                        "invalid credential header name".into(),
                    ));
                }
                validate_header(name, scheme.as_deref(), None)
            }
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

fn extract_header<'a>(
    parts: &'a Parts,
    name: Option<&HeaderName>,
    scheme: Option<&str>,
) -> Result<Option<Cow<'a, str>>, AuthError> {
    let name = name.ok_or(AuthError::MalformedLocation)?;
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
    Ok(Some(Cow::Borrowed(raw)))
}

fn validate_extracted_size<'a>(
    value: Option<Cow<'a, str>>,
) -> Result<Option<Cow<'a, str>>, AuthError> {
    if value
        .as_ref()
        .is_some_and(|value| value.len() > MAX_CREDENTIAL_BYTES)
    {
        Err(AuthError::MalformedLocation)
    } else {
        Ok(value)
    }
}

/// Parses request cookies once while retaining borrowed credential values.
fn parse_cookies(parts: &Parts) -> ParsedCookies<'_> {
    let mut output = ParsedCookies {
        values: SmallVec::new(),
        malformed: false,
    };
    let mut scanned = 0_usize;
    for header in parts.headers.get_all(header::COOKIE) {
        scanned = scanned.saturating_add(header.as_bytes().len());
        if scanned > MAX_CREDENTIAL_SOURCE_BYTES {
            output.malformed = true;
            break;
        }
        let Ok(header) = header.to_str() else {
            output.malformed = true;
            continue;
        };
        parse_cookie_header(header, &mut output);
    }
    output
}

/// Adds one cookie header while preserving duplicate-name failures.
fn parse_cookie_header<'a>(header: &'a str, output: &mut ParsedCookies<'a>) {
    for item in header.split(';') {
        let item = item.trim();
        let Some((name, value)) = item.split_once('=') else {
            if !item.is_empty() {
                insert_cookie(&mut output.values, item, None);
            }
            continue;
        };
        if value.is_empty() {
            insert_cookie(&mut output.values, name, None);
        } else {
            insert_cookie(&mut output.values, name, Some(value));
        }
    }
}

/// Inserts one borrowed cookie while retaining duplicate-name failure state.
fn insert_cookie<'a>(values: &mut CookieValues<'a>, name: &'a str, value: Option<&'a str>) {
    if let Some((_, existing)) = values.iter_mut().find(|(candidate, _)| *candidate == name) {
        *existing = None;
    } else {
        values.push((name, value));
    }
}

fn selected_borrowed<'a>(
    value: Option<&Option<&'a str>>,
) -> Result<Option<Cow<'a, str>>, AuthError> {
    match value {
        Some(Some(value)) => Ok(Some(Cow::Borrowed(value))),
        Some(None) => Err(AuthError::MalformedLocation),
        None => Ok(None),
    }
}

/// Parses query credentials once and owns only percent-decoded values when necessary.
fn parse_query(parts: &Parts) -> ParsedQuery<'_> {
    let mut values = SmallVec::new();
    if let Some(query) = parts.uri.query() {
        if query.len() > MAX_CREDENTIAL_SOURCE_BYTES {
            return ParsedQuery {
                values,
                malformed: true,
            };
        }
        for (name, value) in url::form_urlencoded::parse(query.as_bytes()) {
            let value = (!value.is_empty()).then_some(value);
            insert_query(&mut values, name, value);
        }
    }
    ParsedQuery {
        values,
        malformed: false,
    }
}

/// Inserts one decoded query pair while retaining duplicate-name failure state.
fn insert_query<'a>(values: &mut QueryValues<'a>, name: Cow<'a, str>, value: Option<Cow<'a, str>>) {
    if let Some((_, existing)) = values
        .iter_mut()
        .find(|(candidate, _)| candidate.as_ref() == name.as_ref())
    {
        *existing = None;
    } else {
        values.push((name, value));
    }
}

fn extract_query<'a>(parts: &'a Parts, key: &str) -> Result<Option<Cow<'a, str>>, AuthError> {
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
        found = Some(value);
    }
    Ok(found)
}

fn extract_cookie<'a>(parts: &'a Parts, name: &str) -> Result<Option<Cow<'a, str>>, AuthError> {
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
            found = Some(Cow::Borrowed(value));
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    /// Malformed unrelated cookies do not invalidate a distinct configured selector.
    #[test]
    fn shared_scan_ignores_unrelated_malformed_cookie() -> Result<(), AuthError> {
        let request = Request::builder()
            .header(header::COOKIE, "unrelated; auth=credential")
            .body(())
            .map_err(|_| AuthError::MalformedLocation)?;
        let (parts, _) = request.into_parts();
        let mut scan = RequestCredentialScan::new(&parts);
        let location = CredentialLocation::cookie(CookieConf::new("auth"));
        assert_eq!(
            location.extract_from(&mut scan)?.as_deref(),
            Some("credential")
        );
        Ok(())
    }

    /// A malformed configured cookie remains a deterministic credential failure.
    #[test]
    fn shared_scan_rejects_malformed_selected_cookie() -> Result<(), AuthError> {
        let request = Request::builder()
            .header(header::COOKIE, "auth")
            .body(())
            .map_err(|_| AuthError::MalformedLocation)?;
        let (parts, _) = request.into_parts();
        let mut scan = RequestCredentialScan::new(&parts);
        let location = CredentialLocation::cookie(CookieConf::new("auth"));
        assert!(matches!(
            location.extract_from(&mut scan),
            Err(AuthError::MalformedLocation)
        ));
        Ok(())
    }
}
