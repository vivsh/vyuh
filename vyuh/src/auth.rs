use std::{
    future::Future,
    hash::Hash,
    path::{Path, PathBuf},
    sync::Arc,
};

use super::site::Site;
use axum::extract::{FromRequestParts, OptionalFromRequestParts};
use axum::http::request::Parts;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum_extra::extract::CookieJar;
use axum_extra::extract::cookie::{self, Cookie};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures::future::BoxFuture;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Validation, decode, encode};
use ring::{
    digest, pbkdf2,
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};
use std::fmt::Write as _;
use std::num::NonZeroU32;
use thiserror::Error;
use time;

pub use crate::permit;
pub use crate::roles::{BitRole, Permit, PermitAll, PermitAny, RoleType, format_roles};

const DEFAULT_PBKDF2_ITERATIONS: u32 = 260_000;
const UNUSABLE_PASSWORD_PREFIX: &str = "!";
const UNUSABLE_PASSWORD_SUFFIX_LEN: usize = 40;

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum CookieSameSite {
    Lax,
    Strict,
    None,
}

impl Default for CookieSameSite {
    fn default() -> Self {
        Self::Lax
    }
}

impl From<CookieSameSite> for cookie::SameSite {
    fn from(value: CookieSameSite) -> Self {
        match value {
            CookieSameSite::Lax => cookie::SameSite::Lax,
            CookieSameSite::Strict => cookie::SameSite::Strict,
            CookieSameSite::None => cookie::SameSite::None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CookieConf {
    pub name: String,
    pub path: String,
    pub http_only: bool,
    pub secure: bool,
    pub same_site: CookieSameSite,
}

impl Default for CookieConf {
    fn default() -> Self {
        CookieConf {
            name: "".to_string(),
            path: "/".to_string(),
            http_only: true,
            secure: true,
            same_site: CookieSameSite::Lax,
        }
    }
}

impl CookieConf {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    pub fn http_only(mut self, http_only: bool) -> Self {
        self.http_only = http_only;
        self
    }

    pub fn secure(mut self, secure: bool) -> Self {
        self.secure = secure;
        self
    }

    pub fn same_site(mut self, same_site: CookieSameSite) -> Self {
        self.same_site = same_site;
        self
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AuthAudiencePolicy {
    Optional,
    Required,
    Disabled,
}

impl Default for AuthAudiencePolicy {
    fn default() -> Self {
        Self::Optional
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum JwtAlgorithm {
    HS256,
    HS384,
    HS512,
    RS256,
    RS384,
    RS512,
    ES256,
    ES384,
    EdDSA,
}

impl Default for JwtAlgorithm {
    fn default() -> Self {
        Self::HS256
    }
}

impl JwtAlgorithm {
    fn as_jsonwebtoken(self) -> Algorithm {
        match self {
            JwtAlgorithm::HS256 => Algorithm::HS256,
            JwtAlgorithm::HS384 => Algorithm::HS384,
            JwtAlgorithm::HS512 => Algorithm::HS512,
            JwtAlgorithm::RS256 => Algorithm::RS256,
            JwtAlgorithm::RS384 => Algorithm::RS384,
            JwtAlgorithm::RS512 => Algorithm::RS512,
            JwtAlgorithm::ES256 => Algorithm::ES256,
            JwtAlgorithm::ES384 => Algorithm::ES384,
            JwtAlgorithm::EdDSA => Algorithm::EdDSA,
        }
    }

    fn is_hmac(self) -> bool {
        matches!(
            self,
            JwtAlgorithm::HS256 | JwtAlgorithm::HS384 | JwtAlgorithm::HS512
        )
    }

    fn key_family(self) -> JwtKeyFamily {
        match self {
            JwtAlgorithm::HS256 | JwtAlgorithm::HS384 | JwtAlgorithm::HS512 => JwtKeyFamily::Hmac,
            JwtAlgorithm::RS256 | JwtAlgorithm::RS384 | JwtAlgorithm::RS512 => JwtKeyFamily::Rsa,
            JwtAlgorithm::ES256 | JwtAlgorithm::ES384 => JwtKeyFamily::Ec,
            JwtAlgorithm::EdDSA => JwtKeyFamily::Ed,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JwtKeyFamily {
    Hmac,
    Rsa,
    Ec,
    Ed,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum JwtKeySource {
    SiteSecret,
    Inline(String),
    Env(String),
    File(String),
}

impl Default for JwtKeySource {
    fn default() -> Self {
        Self::SiteSecret
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JwtConf {
    pub algorithm: JwtAlgorithm,
    pub signing_key: JwtKeySource,
    pub verifying_key: Option<JwtKeySource>,
    pub key_id: Option<String>,
}

impl Default for JwtConf {
    fn default() -> Self {
        Self::hs256_site_secret()
    }
}

impl JwtConf {
    pub fn hs256_site_secret() -> Self {
        Self {
            algorithm: JwtAlgorithm::HS256,
            signing_key: JwtKeySource::SiteSecret,
            verifying_key: None,
            key_id: None,
        }
    }

    pub fn hs512(signing_key: JwtKeySource) -> Self {
        Self {
            algorithm: JwtAlgorithm::HS512,
            signing_key,
            verifying_key: None,
            key_id: None,
        }
    }

    pub fn rs256(signing_key: JwtKeySource, verifying_key: JwtKeySource) -> Self {
        Self {
            algorithm: JwtAlgorithm::RS256,
            signing_key,
            verifying_key: Some(verifying_key),
            key_id: None,
        }
    }

    pub fn algorithm(mut self, algorithm: JwtAlgorithm) -> Self {
        self.algorithm = algorithm;
        self
    }

    pub fn signing_key(mut self, key: JwtKeySource) -> Self {
        self.signing_key = key;
        self
    }

    pub fn verifying_key(mut self, key: JwtKeySource) -> Self {
        self.verifying_key = Some(key);
        self
    }

    pub fn key_id(mut self, key_id: impl Into<String>) -> Self {
        self.key_id = Some(key_id.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyPrincipal {
    pub key_id: Arc<str>,
    pub subject: Option<Arc<str>>,
    pub scopes: Vec<Arc<str>>,
    pub roles: u64,
}

impl ApiKeyPrincipal {
    pub fn new(key_id: impl Into<Arc<str>>) -> Self {
        Self {
            key_id: key_id.into(),
            subject: None,
            scopes: Vec::new(),
            roles: 0,
        }
    }

    pub fn subject(mut self, subject: impl Into<Arc<str>>) -> Self {
        self.subject = Some(subject.into());
        self
    }

    pub fn scopes<I, S>(mut self, scopes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Arc<str>>,
    {
        self.scopes = scopes.into_iter().map(Into::into).collect();
        self
    }

    pub fn roles(mut self, roles: u64) -> Self {
        self.roles = roles;
        self
    }
}

pub trait ApiKeyVerifier: Send + Sync + 'static {
    fn verify<'a>(
        &'a self,
        presented: &'a str,
    ) -> impl Future<Output = Result<ApiKeyPrincipal, AuthError>> + Send + 'a;
}

pub(crate) trait ErasedApiKeyVerifier: Send + Sync + 'static {
    fn verify_boxed<'a>(
        &'a self,
        presented: &'a str,
    ) -> BoxFuture<'a, Result<ApiKeyPrincipal, AuthError>>;
}

impl<T> ErasedApiKeyVerifier for T
where
    T: ApiKeyVerifier,
{
    fn verify_boxed<'a>(
        &'a self,
        presented: &'a str,
    ) -> BoxFuture<'a, Result<ApiKeyPrincipal, AuthError>> {
        Box::pin(self.verify(presented))
    }
}

#[derive(Clone)]
pub struct ApiKeyConf {
    pub enabled: bool,
    pub header: String,
    pub authorization_scheme: Option<String>,
    pub allow_query_param: bool,
    pub query_param: String,
    pub(crate) verifier: Option<Arc<dyn ErasedApiKeyVerifier>>,
}

impl std::fmt::Debug for ApiKeyConf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiKeyConf")
            .field("enabled", &self.enabled)
            .field("header", &self.header)
            .field("authorization_scheme", &self.authorization_scheme)
            .field("allow_query_param", &self.allow_query_param)
            .field("query_param", &self.query_param)
            .field("verifier", &self.verifier.as_ref().map(|_| "<configured>"))
            .finish()
    }
}

impl Default for ApiKeyConf {
    fn default() -> Self {
        Self {
            enabled: false,
            header: "X-API-Key".to_string(),
            authorization_scheme: Some("ApiKey".to_string()),
            allow_query_param: false,
            query_param: "api_key".to_string(),
            verifier: None,
        }
    }
}

impl Serialize for ApiKeyConf {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct ApiKeyConfOut<'a> {
            enabled: bool,
            header: &'a str,
            authorization_scheme: &'a Option<String>,
            allow_query_param: bool,
            query_param: &'a str,
        }

        ApiKeyConfOut {
            enabled: self.enabled,
            header: &self.header,
            authorization_scheme: &self.authorization_scheme,
            allow_query_param: self.allow_query_param,
            query_param: &self.query_param,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ApiKeyConf {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct ApiKeyConfIn {
            #[serde(default)]
            enabled: bool,
            #[serde(default = "default_api_key_header")]
            header: String,
            #[serde(default = "default_api_key_authorization_scheme")]
            authorization_scheme: Option<String>,
            #[serde(default)]
            allow_query_param: bool,
            #[serde(default = "default_api_key_query_param")]
            query_param: String,
        }

        let input = ApiKeyConfIn::deserialize(deserializer)?;
        Ok(Self {
            enabled: input.enabled,
            header: input.header,
            authorization_scheme: input.authorization_scheme,
            allow_query_param: input.allow_query_param,
            query_param: input.query_param,
            verifier: None,
        })
    }
}

fn default_api_key_header() -> String {
    "X-API-Key".to_string()
}

fn default_api_key_authorization_scheme() -> Option<String> {
    Some("ApiKey".to_string())
}

fn default_api_key_query_param() -> String {
    "api_key".to_string()
}

impl ApiKeyConf {
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn header(mut self, header: impl Into<String>) -> Self {
        self.header = header.into();
        self
    }

    pub fn authorization_scheme(mut self, scheme: impl Into<String>) -> Self {
        self.authorization_scheme = Some(scheme.into());
        self
    }

    pub fn no_authorization_scheme(mut self) -> Self {
        self.authorization_scheme = None;
        self
    }

    pub fn allow_query_param(mut self, allow: bool) -> Self {
        self.allow_query_param = allow;
        self
    }

    pub fn query_param(mut self, query_param: impl Into<String>) -> Self {
        self.query_param = query_param.into();
        self
    }

    pub fn verifier(mut self, verifier: impl ApiKeyVerifier) -> Self {
        self.enabled = true;
        self.verifier = Some(Arc::new(verifier));
        self
    }

    pub fn verifier_arc<T>(mut self, verifier: Arc<T>) -> Self
    where
        T: ApiKeyVerifier,
    {
        self.enabled = true;
        self.verifier = Some(verifier);
        self
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AuthConf {
    pub access_ttl: i64,
    pub refresh_ttl: i64,
    pub access_cookie: Option<CookieConf>,
    pub refresh_cookie: Option<CookieConf>,
    #[serde(default)]
    pub jwt: JwtConf,
    pub issuer: Option<String>,
    pub audience: AuthAudiencePolicy,
    pub leeway_seconds: u64,
    pub min_secret_len: usize,
    pub api_keys: ApiKeyConf,
}

impl Default for AuthConf {
    fn default() -> Self {
        AuthConf {
            access_ttl: 3600,
            refresh_ttl: 604800,
            access_cookie: None,
            refresh_cookie: None,
            jwt: JwtConf::default(),
            issuer: None,
            audience: AuthAudiencePolicy::Optional,
            leeway_seconds: 0,
            min_secret_len: 32,
            api_keys: ApiKeyConf::default(),
        }
    }
}

impl AuthConf {
    pub fn access_ttl(mut self, seconds: i64) -> Self {
        self.access_ttl = seconds;
        self
    }

    pub fn refresh_ttl(mut self, seconds: i64) -> Self {
        self.refresh_ttl = seconds;
        self
    }

    pub fn access_cookie(mut self, cookie: CookieConf) -> Self {
        self.access_cookie = Some(cookie);
        self
    }

    pub fn refresh_cookie(mut self, cookie: CookieConf) -> Self {
        self.refresh_cookie = Some(cookie);
        self
    }

    pub fn jwt(mut self, jwt: JwtConf) -> Self {
        self.jwt = jwt;
        self
    }

    pub fn cookie_pair(access: impl Into<String>, refresh: impl Into<String>) -> Self {
        Self::default()
            .access_cookie(CookieConf::new(access))
            .refresh_cookie(CookieConf::new(refresh).same_site(CookieSameSite::Strict))
    }

    pub fn issuer(mut self, issuer: impl Into<String>) -> Self {
        self.issuer = Some(issuer.into());
        self
    }

    pub fn audience(mut self, audience: AuthAudiencePolicy) -> Self {
        self.audience = audience;
        self
    }

    pub fn leeway_seconds(mut self, seconds: u64) -> Self {
        self.leeway_seconds = seconds;
        self
    }

    pub fn min_secret_len(mut self, len: usize) -> Self {
        self.min_secret_len = len;
        self
    }

    pub fn api_keys(mut self, api_keys: ApiKeyConf) -> Self {
        self.api_keys = api_keys;
        self
    }
}

fn extract_token(parts: &Parts) -> Option<&str> {
    let header = parts.headers.get(header::AUTHORIZATION)?;
    let value = header.as_bytes();
    if value.len() > 7 && value[..7].eq_ignore_ascii_case(b"Bearer ") {
        std::str::from_utf8(&value[7..]).ok()
    } else if value.len() > 4 && value[..4].eq_ignore_ascii_case(b"JWT ") {
        std::str::from_utf8(&value[4..]).ok()
    } else {
        None
    }
}

fn unix_timestamp() -> i64 {
    chrono::Utc::now().timestamp()
}

fn to_hex(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len() * 2);
    for b in input {
        let _ = write!(&mut out, "{:02x}", b);
    }
    out
}

/// Create a Django-compatible unusable password marker.
///
/// Django marks unusable passwords with a value that starts with `!`.
pub fn unusable_password() -> Result<String, AuthError> {
    let rng = SystemRandom::new();
    let mut buf = [0u8; UNUSABLE_PASSWORD_SUFFIX_LEN / 2];
    rng.fill(&mut buf)
        .map_err(|_| AuthError::InternalError("rng error".to_string()))?;
    Ok(format!("{}{}", UNUSABLE_PASSWORD_PREFIX, to_hex(&buf)))
}

/// Create a Django-compatible password hash using PBKDF2.
///
/// Format returned: `<algorithm>$<iterations>$<salt>$<hash>`
/// Supported algorithms: `pbkdf2_sha256`, `pbkdf2_sha1`
pub fn make_password(
    password: &str,
    salt: Option<&str>,
    algorithm: Option<&str>,
) -> Result<String, AuthError> {
    let alg = algorithm.unwrap_or("pbkdf2_sha256");
    let iterations = DEFAULT_PBKDF2_ITERATIONS;
    let salt = match salt {
        Some(s) => s.to_string(),
        None => {
            let rng = SystemRandom::new();
            let mut buf = [0u8; 16];
            rng.fill(&mut buf)
                .map_err(|_| AuthError::InternalError("rng error".to_string()))?;
            STANDARD.encode(buf)
        }
    };

    let n = NonZeroU32::new(iterations)
        .ok_or(AuthError::InternalError("invalid iterations".to_string()))?;

    match alg {
        "pbkdf2_sha256" => {
            let mut dk = [0u8; digest::SHA256_OUTPUT_LEN];
            pbkdf2::derive(
                pbkdf2::PBKDF2_HMAC_SHA256,
                n,
                salt.as_bytes(),
                password.as_bytes(),
                &mut dk,
            );
            let hash = STANDARD.encode(dk);
            Ok(format!("{}${}${}${}", alg, iterations, salt, hash))
        }
        "pbkdf2_sha1" => {
            let mut dk = [0u8; digest::SHA1_OUTPUT_LEN];
            pbkdf2::derive(
                pbkdf2::PBKDF2_HMAC_SHA1,
                n,
                salt.as_bytes(),
                password.as_bytes(),
                &mut dk,
            );
            let hash = STANDARD.encode(dk);
            Ok(format!("{}${}${}${}", alg, iterations, salt, hash))
        }
        _ => Err(AuthError::InternalError(format!(
            "unsupported algorithm: {}",
            alg
        ))),
    }
}

/// Verify a Django-compatible password hash. Returns `Ok(true)` when passwords match.
pub fn check_password(password: &str, encoded: &str) -> Result<bool, AuthError> {
    if encoded.starts_with(UNUSABLE_PASSWORD_PREFIX) {
        return Ok(false);
    }

    let parts: Vec<&str> = encoded.split('$').collect();
    if parts.len() != 4 {
        return Err(AuthError::InternalError(
            "invalid password hash format".to_string(),
        ));
    }
    let alg = parts[0];
    let iterations: u32 = parts[1]
        .parse()
        .map_err(|_| AuthError::InternalError("invalid iterations".to_string()))?;
    let salt = parts[2];
    let hash_b64 = parts[3];
    let decoded = STANDARD
        .decode(hash_b64)
        .map_err(|_| AuthError::InternalError("invalid base64".to_string()))?;

    let n = NonZeroU32::new(iterations)
        .ok_or(AuthError::InternalError("invalid iterations".to_string()))?;

    match alg {
        "pbkdf2_sha256" => {
            let res = pbkdf2::verify(
                pbkdf2::PBKDF2_HMAC_SHA256,
                n,
                salt.as_bytes(),
                password.as_bytes(),
                &decoded,
            );
            Ok(res.is_ok())
        }
        "pbkdf2_sha1" => {
            let res = pbkdf2::verify(
                pbkdf2::PBKDF2_HMAC_SHA1,
                n,
                salt.as_bytes(),
                password.as_bytes(),
                &decoded,
            );
            Ok(res.is_ok())
        }
        _ => Err(AuthError::InternalError(format!(
            "unsupported algorithm: {}",
            alg
        ))),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenPair {
    pub access_token: String,
    pub refresh_token: String,
}

#[derive(Clone)]
pub struct Authenticator {
    access_ttl: i64,
    refresh_ttl: i64,
    issuer: Option<String>,
    audience: AuthAudiencePolicy,
    access_cookie_conf: Option<CookieConf>,
    refresh_cookie_conf: Option<CookieConf>,
    access_cookie_same_site: cookie::SameSite,
    refresh_cookie_same_site: cookie::SameSite,
    api_keys: ApiKeyConf,
    algorithm: Algorithm,
    key_id: Option<String>,
    encoding_key: EncodingKey,
    decoding_key: DecodingKey,
    validation: Validation,
}

impl std::fmt::Debug for Authenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authenticator")
            .field("access_ttl", &self.access_ttl)
            .field("refresh_ttl", &self.refresh_ttl)
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field("access_cookie_conf", &self.access_cookie_conf)
            .field("refresh_cookie_conf", &self.refresh_cookie_conf)
            .field("api_keys", &self.api_keys)
            .field("algorithm", &self.algorithm)
            .field("key_id", &self.key_id)
            .finish()
    }
}

fn get_cookie_same_site(cookie_conf: &Option<CookieConf>) -> cookie::SameSite {
    cookie_conf
        .as_ref()
        .map(|conf| conf.same_site.into())
        .unwrap_or(cookie::SameSite::Lax)
}

fn resolve_jwt_key_source(
    source: &JwtKeySource,
    site_secret: &str,
    project_dir: &Path,
) -> Result<Vec<u8>, AuthError> {
    match source {
        JwtKeySource::SiteSecret => Ok(site_secret.as_bytes().to_vec()),
        JwtKeySource::Inline(value) => Ok(value.as_bytes().to_vec()),
        JwtKeySource::Env(name) => std::env::var(name)
            .map(|value| value.into_bytes())
            .map_err(|_| AuthError::JwtConfigError(format!("JWT key env var '{name}' is not set"))),
        JwtKeySource::File(path) => {
            let path = PathBuf::from(path);
            let path = if path.is_absolute() {
                path
            } else {
                project_dir.join(path)
            };
            std::fs::read(&path).map_err(|err| {
                AuthError::JwtConfigError(format!(
                    "failed to read JWT key file '{}': {}",
                    path.display(),
                    err
                ))
            })
        }
    }
}

fn build_jwt_keys(
    conf: &JwtConf,
    site_secret: &str,
    min_secret_len: usize,
    project_dir: &Path,
) -> Result<(Algorithm, EncodingKey, DecodingKey), AuthError> {
    let algorithm = conf.algorithm.as_jsonwebtoken();
    let signing_key = resolve_jwt_key_source(&conf.signing_key, site_secret, project_dir)?;

    if conf.algorithm.is_hmac() {
        if conf.verifying_key.is_some() {
            return Err(AuthError::JwtConfigError(
                "HMAC JWT algorithms use one symmetric signing key and must not configure verifying_key"
                    .to_string(),
            ));
        }
        if signing_key.len() < min_secret_len {
            return Err(AuthError::JwtConfigError(format!(
                "JWT signing key must be at least {min_secret_len} bytes for HMAC algorithms"
            )));
        }
        return Ok((
            algorithm,
            EncodingKey::from_secret(&signing_key),
            DecodingKey::from_secret(&signing_key),
        ));
    }

    let verifying_source = conf.verifying_key.as_ref().ok_or_else(|| {
        AuthError::JwtConfigError("asymmetric JWT algorithms require a verifying_key".to_string())
    })?;
    let verifying_key = resolve_jwt_key_source(verifying_source, site_secret, project_dir)?;

    let encoding_key = match conf.algorithm.key_family() {
        JwtKeyFamily::Hmac => unreachable!("HMAC algorithms returned earlier"),
        JwtKeyFamily::Rsa => EncodingKey::from_rsa_pem(&signing_key),
        JwtKeyFamily::Ec => EncodingKey::from_ec_pem(&signing_key),
        JwtKeyFamily::Ed => EncodingKey::from_ed_pem(&signing_key),
    }
    .map_err(|err| {
        AuthError::JwtConfigError(format!(
            "failed to parse JWT signing key for {:?}: {}",
            conf.algorithm, err
        ))
    })?;

    let decoding_key = match conf.algorithm.key_family() {
        JwtKeyFamily::Hmac => unreachable!("HMAC algorithms returned earlier"),
        JwtKeyFamily::Rsa => DecodingKey::from_rsa_pem(&verifying_key),
        JwtKeyFamily::Ec => DecodingKey::from_ec_pem(&verifying_key),
        JwtKeyFamily::Ed => DecodingKey::from_ed_pem(&verifying_key),
    }
    .map_err(|err| {
        AuthError::JwtConfigError(format!(
            "failed to parse JWT verifying key for {:?}: {}",
            conf.algorithm, err
        ))
    })?;

    Ok((algorithm, encoding_key, decoding_key))
}

impl Authenticator {
    pub(crate) fn new(
        conf: &AuthConf,
        secret_key: &str,
        project_dir: &Path,
    ) -> Result<Self, AuthError> {
        let access_ttl = conf.access_ttl;
        let refresh_ttl = conf.refresh_ttl;
        let issuer = conf.issuer.clone();
        let audience = conf.audience;
        let access_cookie_conf = conf.access_cookie.clone();
        let refresh_cookie_conf = conf.refresh_cookie.clone();
        let api_keys = conf.api_keys.clone();
        let (algorithm, encoding_key, decoding_key) =
            build_jwt_keys(&conf.jwt, secret_key, conf.min_secret_len, project_dir)?;
        let mut validation = Validation::new(algorithm);
        validation.validate_aud = false;
        validation.leeway = conf.leeway_seconds;
        if let Some(issuer) = &conf.issuer {
            validation.set_issuer(&[issuer]);
        }

        Ok(Self {
            access_ttl,
            refresh_ttl,
            issuer,
            audience,
            access_cookie_same_site: get_cookie_same_site(&access_cookie_conf),
            refresh_cookie_same_site: get_cookie_same_site(&refresh_cookie_conf),
            access_cookie_conf,
            refresh_cookie_conf,
            api_keys,
            algorithm,
            key_id: conf.jwt.key_id.clone(),
            encoding_key,
            decoding_key,
            validation,
        })
    }

    pub fn encode(&self, item: &JWTClaim) -> Result<String, AuthError> {
        let key = &self.encoding_key;
        let mut header = jsonwebtoken::Header::new(self.algorithm);
        header.kid = self.key_id.clone();
        encode(&header, item, &key).map_err(|e| AuthError::from(&e))
    }

    pub fn decode(&self, token: &str) -> Result<JWTClaim, AuthError> {
        let key = &self.decoding_key;
        decode::<JWTClaim>(&token, &key, &self.validation)
            .map(|o| o.claims)
            .map_err(|e| AuthError::from(&e))
    }

    pub fn extract_claims(&self, parts: &Parts, kind: TokenKind) -> Result<JWTClaim, AuthError> {
        let cookies_conf = if kind == TokenKind::Refresh {
            &self.refresh_cookie_conf
        } else {
            &self.access_cookie_conf
        };
        let token = extract_token(parts)
            .map(|t| t.to_owned())
            .or_else(|| {
                cookies_conf
                    .as_ref()
                    .map(|c| c.name.as_str())
                    .and_then(|cookie_name| {
                        CookieJar::from_headers(&parts.headers)
                            .get(cookie_name)
                            .map(|c| c.value().to_owned())
                    })
            })
            .ok_or(AuthError::MissingToken)?;
        let claims = self.decode(&token)?;
        if claims.token_kind() != kind {
            return Err(AuthError::WrongTokenKind);
        }
        Ok(claims)
    }

    pub fn extract_user(
        &self,
        parts: &Parts,
        aud: &[&str],
        refresh: bool,
    ) -> Result<AuthUser, AuthError> {
        let kind = if refresh {
            TokenKind::Refresh
        } else {
            TokenKind::Access
        };
        let claims = self.extract_claims(parts, kind)?;
        self.validate_audience(&claims, aud)?;
        let user = claims.into_auth_user();
        Ok(user)
    }

    pub fn create_token_pair(&self, user: AuthUser, aud: &[&str]) -> Result<TokenPair, AuthError> {
        let aud: Vec<String> = aud.iter().map(|&s| s.to_string()).collect();
        let access_claims = JWTClaim::new(
            &user,
            "",
            self.issuer.clone(),
            aud.clone(),
            self.access_ttl,
            TokenKind::Access,
        );
        let access_token = self.encode(&access_claims)?;
        let refresh_claims = JWTClaim::new(
            &user,
            "",
            self.issuer.clone(),
            aud,
            self.refresh_ttl,
            TokenKind::Refresh,
        );
        let refresh_token = self.encode(&refresh_claims)?;
        Ok(TokenPair {
            access_token,
            refresh_token,
        })
    }

    pub fn login_token(&self, token: &str, refresh: bool, resp: &mut Response) {
        let cookie_conf = if refresh {
            &self.refresh_cookie_conf
        } else {
            &self.access_cookie_conf
        };
        let same_site = if refresh {
            self.refresh_cookie_same_site
        } else {
            self.access_cookie_same_site
        };
        let access_ttl = if refresh {
            self.refresh_ttl
        } else {
            self.access_ttl
        };
        if let Some(conf) = cookie_conf {
            let c = Cookie::build((&conf.name, token))
                .path(conf.path.as_str())
                .max_age(time::Duration::seconds(access_ttl))
                .http_only(conf.http_only)
                .same_site(same_site)
                .secure(conf.secure)
                .build();

            match c.to_string().parse() {
                Ok(hv) => {
                    resp.headers_mut().append(header::SET_COOKIE, hv);
                }
                Err(err) => {
                    *resp.status_mut() = axum::http::StatusCode::INTERNAL_SERVER_ERROR;
                    resp.headers_mut().insert(
                        axum::http::header::CONTENT_TYPE,
                        axum::http::HeaderValue::from_static("text/plain"),
                    );
                    *resp.body_mut() = err.to_string().into();
                }
            }
        }
    }

    pub fn login_user(
        &self,
        user: AuthUser,
        aud: &[&str],
        resp: &mut Response,
    ) -> Result<TokenPair, AuthError> {
        let pair = self.create_token_pair(user, aud)?;
        self.login_token(&pair.access_token, false, resp);
        self.login_token(&pair.refresh_token, true, resp);
        Ok(pair)
    }

    pub fn refresh(&self, parts: &Parts, aud: &[&str]) -> Result<TokenPair, AuthError> {
        let user = self.extract_user(parts, aud, true)?;
        let pair = self.create_token_pair(user, aud)?;
        Ok(pair)
    }

    pub fn logout(&self, refresh: bool, resp: &mut Response) {
        let cookie_conf = if refresh {
            &self.refresh_cookie_conf
        } else {
            &self.access_cookie_conf
        };
        if let Some(conf) = cookie_conf.as_ref() {
            let c = Cookie::build((conf.name.as_str(), ""))
                .path(conf.path.as_str())
                .max_age(time::Duration::seconds(0))
                .build();
            match c.to_string().parse() {
                Ok(hv) => {
                    resp.headers_mut().append(header::SET_COOKIE, hv);
                }
                Err(err) => {
                    *resp.status_mut() = axum::http::StatusCode::INTERNAL_SERVER_ERROR;
                    resp.headers_mut().insert(
                        axum::http::header::CONTENT_TYPE,
                        axum::http::HeaderValue::from_static("text/plain"),
                    );
                    *resp.body_mut() = err.to_string().into();
                }
            }
        }
    }

    fn validate_audience(&self, claims: &JWTClaim, aud: &[&str]) -> Result<(), AuthError> {
        match self.audience {
            AuthAudiencePolicy::Disabled => Ok(()),
            AuthAudiencePolicy::Optional => {
                if !claims.aud.is_empty()
                    && !aud.is_empty()
                    && !claims.aud.iter().any(|a| aud.iter().any(|&b| a == b))
                {
                    Err(AuthError::Forbidden)
                } else {
                    Ok(())
                }
            }
            AuthAudiencePolicy::Required => {
                if claims.aud.is_empty() || aud.is_empty() {
                    return Err(AuthError::Forbidden);
                }
                if claims.aud.iter().any(|a| aud.iter().any(|&b| a == b)) {
                    Ok(())
                } else {
                    Err(AuthError::Forbidden)
                }
            }
        }
    }

    pub async fn extract_api_key(&self, parts: &Parts) -> Result<ApiKeyPrincipal, AuthError> {
        let presented = self
            .extract_presented_api_key(parts)
            .ok_or(AuthError::MissingApiKey)?;

        let verifier = self
            .api_keys
            .verifier
            .as_ref()
            .ok_or(AuthError::ApiKeyVerifierMissing)?;

        verifier.verify_boxed(&presented).await
    }

    fn extract_presented_api_key(&self, parts: &Parts) -> Option<String> {
        if !self.api_keys.enabled {
            return None;
        }

        if let Ok(header_name) =
            axum::http::header::HeaderName::from_bytes(self.api_keys.header.as_bytes())
        {
            if let Some(value) = parts.headers.get(header_name) {
                if let Ok(value) = value.to_str() {
                    if !value.is_empty() {
                        return Some(value.to_string());
                    }
                }
            }
        }

        if let Some(scheme) = &self.api_keys.authorization_scheme {
            if let Some(value) = parts.headers.get(header::AUTHORIZATION) {
                let bytes = value.as_bytes();
                let prefix = format!("{scheme} ");
                let prefix_bytes = prefix.as_bytes();
                if bytes.len() > prefix_bytes.len()
                    && bytes[..prefix_bytes.len()].eq_ignore_ascii_case(prefix_bytes)
                {
                    if let Ok(token) = std::str::from_utf8(&bytes[prefix_bytes.len()..]) {
                        if !token.is_empty() {
                            return Some(token.to_string());
                        }
                    }
                }
            }
        }

        if self.api_keys.allow_query_param {
            if let Some(query) = parts.uri.query() {
                for (key, value) in
                    serde_urlencoded::from_str::<Vec<(String, String)>>(query).unwrap_or_default()
                {
                    if key == self.api_keys.query_param && !value.is_empty() {
                        return Some(value);
                    }
                }
            }
        }

        None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthUser {
    pub key: Arc<str>,
    pub roles: u64,
}

impl AuthUser {
    pub fn new(key: &str, roles: u64) -> Self {
        Self {
            key: Arc::from(key),
            roles,
        }
    }

    /// Parses the subject key into an application identifier type.
    ///
    /// Vyuh stores principals as strings so applications can choose their own
    /// identifier format. This helper keeps route code concise without
    /// imposing a framework-wide key type.
    pub fn parse_key<T>(&self) -> Result<T, AuthError>
    where
        T: std::str::FromStr,
    {
        self.key.parse().map_err(|_| AuthError::InvalidToken)
    }
}

impl PartialEq for AuthUser {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key)
    }
}

impl Eq for AuthUser {}

impl Hash for AuthUser {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey(pub ApiKeyPrincipal);

impl ApiKey {
    pub fn into_inner(self) -> ApiKeyPrincipal {
        self.0
    }
}

impl std::ops::Deref for ApiKey {
    type Target = ApiKeyPrincipal;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<ApiKeyPrincipal> for ApiKey {
    fn as_ref(&self) -> &ApiKeyPrincipal {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TokenKind {
    Access,
    Refresh,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JWTClaim {
    #[serde(default)]
    kid: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    iss: Option<String>,
    #[serde(default)]
    jti: String,
    #[serde(default)]
    sub: String,
    #[serde(default)]
    aud: Vec<String>,
    iat: i64,
    exp: i64,
    #[serde(default)]
    refresh: bool,
    #[serde(default = "default_token_kind")]
    token_kind: TokenKind,
    #[serde(default)]
    roles: u64,
}

fn default_token_kind() -> TokenKind {
    TokenKind::Access
}

impl JWTClaim {
    pub fn new(
        user: &AuthUser,
        kid: &str,
        issuer: Option<String>,
        aud: Vec<String>,
        ttl: i64,
        token_kind: TokenKind,
    ) -> Self {
        let now = unix_timestamp();
        Self {
            kid: kid.to_string(),
            iss: issuer,
            jti: uuid::Uuid::new_v4().to_string(),
            sub: user.key.to_string(),
            aud,
            iat: now,
            exp: now + ttl,
            refresh: token_kind == TokenKind::Refresh,
            token_kind,
            roles: user.roles,
        }
    }

    pub fn token_kind(&self) -> TokenKind {
        if self.refresh {
            TokenKind::Refresh
        } else {
            self.token_kind
        }
    }

    fn into_auth_user(self) -> AuthUser {
        AuthUser {
            key: Arc::from(self.sub),
            roles: self.roles,
        }
    }
}

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("invalid token")]
    InvalidToken,
    #[error("missing token")]
    MissingToken,
    #[error("expired token")]
    ExpiredToken,
    #[error("invalid token signature")]
    InvalidSignature,
    #[error("wrong token kind")]
    WrongTokenKind,
    #[error("missing api key")]
    MissingApiKey,
    #[error("invalid api key")]
    InvalidApiKey,
    #[error("api key verifier is not configured")]
    ApiKeyVerifierMissing,
    #[error("invalid JWT configuration: {0}")]
    JwtConfigError(String),
    #[error("forbidden")]
    Forbidden,
    #[error("internal authentication error: {0}")]
    InternalError(String),
}

//convert jsonwebtoken::errors::Error to JWTError
impl From<&jsonwebtoken::errors::Error> for AuthError {
    fn from(err: &jsonwebtoken::errors::Error) -> Self {
        match err.kind() {
            jsonwebtoken::errors::ErrorKind::InvalidToken => AuthError::InvalidToken,
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthError::ExpiredToken,
            jsonwebtoken::errors::ErrorKind::InvalidSignature => AuthError::InvalidSignature,
            jsonwebtoken::errors::ErrorKind::InvalidAlgorithm => AuthError::InvalidToken,
            _ => AuthError::InternalError(err.to_string()),
        }
    }
}

impl FromRequestParts<Site> for AuthUser {
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, site: &Site) -> Result<Self, Self::Rejection> {
        if let Some(user) = parts.extensions.get::<AuthUser>() {
            return Ok(user.clone());
        }
        let refresh = false;
        let auth = site.auth();
        let user = auth.extract_user(parts, &[], refresh)?;
        parts.extensions.insert(user.clone());
        Ok(user)
    }
}

impl OptionalFromRequestParts<Site> for AuthUser {
    type Rejection = AuthError;

    async fn from_request_parts(
        parts: &mut Parts,
        site: &Site,
    ) -> Result<Option<Self>, Self::Rejection> {
        match <AuthUser as FromRequestParts<Site>>::from_request_parts(parts, site).await {
            Ok(user) => Ok(Some(user)),
            Err(AuthError::MissingToken) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

impl FromRequestParts<Site> for ApiKey {
    type Rejection = AuthError;

    async fn from_request_parts(parts: &mut Parts, site: &Site) -> Result<Self, Self::Rejection> {
        if let Some(principal) = parts.extensions.get::<ApiKeyPrincipal>() {
            return Ok(Self(principal.clone()));
        }
        let principal = site.auth().extract_api_key(parts).await?;
        parts.extensions.insert(principal.clone());
        Ok(Self(principal))
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            AuthError::InvalidToken => (StatusCode::UNAUTHORIZED, "Invalid token"),
            AuthError::MissingToken => (StatusCode::UNAUTHORIZED, "Missing token"),
            AuthError::ExpiredToken => (StatusCode::UNAUTHORIZED, "Expired token"),
            AuthError::InvalidSignature => (StatusCode::UNAUTHORIZED, "Invalid token signature"),
            AuthError::WrongTokenKind => (StatusCode::UNAUTHORIZED, "Wrong token kind"),
            AuthError::MissingApiKey => (StatusCode::UNAUTHORIZED, "Missing API key"),
            AuthError::InvalidApiKey => (StatusCode::UNAUTHORIZED, "Invalid API key"),
            AuthError::ApiKeyVerifierMissing => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "API key verifier is not configured",
            ),
            AuthError::JwtConfigError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.as_ref()),
            AuthError::Forbidden => (StatusCode::FORBIDDEN, "Forbidden"),
            AuthError::InternalError(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg.as_ref()),
        };
        crate::errors::ErrorReport::new(
            status,
            crate::errors::ErrorSourceKind::Auth,
            match status {
                StatusCode::FORBIDDEN => "forbidden",
                StatusCode::UNAUTHORIZED => "unauthorized",
                _ => "auth_error",
            },
            message.to_string(),
        )
        .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthError, AuthUser};

    /// Verifies that typed subject parsing succeeds for numeric principals.
    #[test]
    fn auth_user_parse_key() {
        let user = AuthUser::new("42", 0);
        assert!(matches!(user.parse_key::<i64>(), Ok(42)));
    }

    /// Verifies that invalid subject parsing is treated as an invalid token.
    #[test]
    fn auth_user_parse_key_rejects_invalid() {
        let user = AuthUser::new("not-an-id", 0);
        assert!(matches!(
            user.parse_key::<i64>(),
            Err(AuthError::InvalidToken)
        ));
    }
}
