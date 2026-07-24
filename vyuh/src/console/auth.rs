use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    extract::{FromRequestParts, State},
    http::{StatusCode, request::Parts},
    response::{IntoResponse, Response},
};
use axum_extra::extract::CookieJar;
use axum_extra::extract::cookie::{Cookie, SameSite};
use blake3::Hash;
use parking_lot::Mutex;
use serde::Serialize;
use uuid::Uuid;

use crate::{
    Site,
    auth::{BitRole, RoleType},
    callables::IntoArgPart,
    callables::specs::ArgPart,
    console::status::StatusOut,
};

#[derive(BitRole)]
pub enum ConsoleRole {
    Viewer,
    Operator,
    Admin,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConsoleUser {
    pub subject: String,
    pub roles: u64,
    pub role_names: Vec<&'static str>,
}

#[derive(Debug, Clone)]
pub(crate) struct ConsoleRuntime {
    bootstrap: Arc<Mutex<Option<BootstrapToken>>>,
    bundle_id: Uuid,
    sessions: Arc<Mutex<HashMap<Hash, ConsoleSession>>>,
    status_cache: Arc<Mutex<Option<StatusCache>>>,
}

#[derive(Debug, Clone)]
struct BootstrapToken {
    hash: Hash,
    expires_at: Instant,
    display_token: String,
}

#[derive(Debug, Clone)]
struct ConsoleSession {
    user: ConsoleUser,
    expires_at: Instant,
}

#[derive(Debug, Clone)]
struct StatusCache {
    refreshed_at: Instant,
    status: StatusOut,
}

impl ConsoleRuntime {
    pub(crate) fn new(ttl: Duration, bundle_id: Uuid) -> Self {
        let token = new_token();
        Self {
            bootstrap: Arc::new(Mutex::new(Some(BootstrapToken {
                hash: hash_token(&token),
                expires_at: Instant::now() + ttl,
                display_token: token,
            }))),
            bundle_id,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            status_cache: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn bundle_id(&self) -> Uuid {
        self.bundle_id
    }

    pub(crate) fn bootstrap_token(&self) -> Option<String> {
        self.bootstrap
            .lock()
            .as_ref()
            .filter(|token| token.expires_at > Instant::now())
            .map(|token| token.display_token.clone())
    }

    pub(crate) fn consume_bootstrap(&self, presented: &str, ttl: Duration) -> Option<String> {
        let mut bootstrap = self.bootstrap.lock();
        let token = bootstrap.as_ref()?;
        if token.expires_at <= Instant::now() || token.hash != hash_token(presented) {
            return None;
        }
        let _ = bootstrap.take();
        let session_token = new_token();
        let user = ConsoleUser {
            subject: "bootstrap".to_string(),
            roles: ConsoleRole::Admin.to_role_type(),
            role_names: role_names(ConsoleRole::Admin.to_role_type()),
        };
        self.sessions.lock().insert(
            hash_token(&session_token),
            ConsoleSession {
                user,
                expires_at: Instant::now() + ttl,
            },
        );
        Some(session_token)
    }

    fn get_session(&self, presented: &str) -> Option<ConsoleUser> {
        let hash = hash_token(presented);
        let mut sessions = self.sessions.lock();
        let session = sessions.get(&hash)?;
        if session.expires_at <= Instant::now() {
            sessions.remove(&hash);
            return None;
        }
        Some(session.user.clone())
    }

    pub(crate) fn clear_session(&self, presented: &str) {
        self.sessions.lock().remove(&hash_token(presented));
    }

    pub(crate) fn status(&self, site: &Site, ttl: Duration) -> StatusOut {
        let now = Instant::now();
        {
            let cache = self.status_cache.lock();
            if let Some(cache) = cache.as_ref()
                && now.duration_since(cache.refreshed_at) < ttl
            {
                return cache.status.clone();
            }
        }

        let status = crate::console::status::collect(site);
        *self.status_cache.lock() = Some(StatusCache {
            refreshed_at: now,
            status: status.clone(),
        });
        status
    }
}

#[derive(Clone)]
pub(crate) struct ConsoleSessionUser(pub ConsoleUser);

pub(crate) struct ConsoleCookies(pub CookieJar);

impl FromRequestParts<Site> for ConsoleSessionUser {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &Site) -> Result<Self, Self::Rejection> {
        let State(site) = State::<Site>::from_request_parts(parts, state)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())?;
        if local_debug_allowed(&site) {
            return Ok(ConsoleSessionUser(local_debug_user()));
        }

        let jar = CookieJar::from_headers(&parts.headers);
        let conf = &site.conf().console;
        let Some(cookie) = jar.get(&conf.cookie_name) else {
            return Err(error_response(&site, StatusCode::FORBIDDEN));
        };
        let Some(runtime) = site.console_runtime() else {
            return Err(error_response(&site, StatusCode::NOT_FOUND));
        };
        runtime
            .get_session(cookie.value())
            .map(ConsoleSessionUser)
            .ok_or_else(|| error_response(&site, StatusCode::FORBIDDEN))
    }
}

impl IntoArgPart for ConsoleSessionUser {
    fn into_arg_part() -> ArgPart {
        ArgPart::Ignore
    }
}

impl FromRequestParts<Site> for ConsoleCookies {
    type Rejection = StatusCode;

    async fn from_request_parts(parts: &mut Parts, _state: &Site) -> Result<Self, Self::Rejection> {
        Ok(ConsoleCookies(CookieJar::from_headers(&parts.headers)))
    }
}

impl IntoArgPart for ConsoleCookies {
    fn into_arg_part() -> ArgPart {
        ArgPart::Ignore
    }
}

pub(crate) fn session_cookie<'a>(
    name: &str,
    value: impl Into<std::borrow::Cow<'a, str>>,
    max_age: time::Duration,
    secure: bool,
) -> Cookie<'a> {
    Cookie::build((name.to_string(), value.into()))
        .path("/")
        .http_only(true)
        .same_site(SameSite::Lax)
        .secure(secure)
        .max_age(max_age)
        .build()
}

pub(crate) fn expired_cookie(name: &str, secure: bool) -> Cookie<'static> {
    Cookie::build((name.to_string(), ""))
        .path("/")
        .http_only(true)
        .same_site(SameSite::Lax)
        .secure(secure)
        .max_age(time::Duration::ZERO)
        .build()
}

fn new_token() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

fn hash_token(token: &str) -> Hash {
    blake3::hash(token.as_bytes())
}

fn local_debug_allowed(site: &Site) -> bool {
    cfg!(debug_assertions) && is_local_host(&site.conf().host)
}

fn is_local_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

fn local_debug_user() -> ConsoleUser {
    ConsoleUser {
        subject: "local-debug".to_string(),
        roles: ConsoleRole::Admin.to_role_type(),
        role_names: role_names(ConsoleRole::Admin.to_role_type()),
    }
}

fn error_response(site: &Site, status: StatusCode) -> Response {
    let (title, message) = error_copy(status);
    let context = serde_json::json!({
        "base_path": site.conf().console.path,
        "version": env!("CARGO_PKG_VERSION"),
        "stylesheet_path": crate::console::stylesheet_path(),
        "status": status.as_u16(),
        "title": title,
        "message": message,
    });
    match site
        .template_engine()
        .render("console/error.html", &context)
    {
        Ok(body) => (status, axum::response::Html(body)).into_response(),
        Err(_) => (status, title).into_response(),
    }
}

fn error_copy(status: StatusCode) -> (&'static str, &'static str) {
    match status {
        StatusCode::NOT_FOUND => (
            "Console page not found",
            "The requested console page or resource does not exist.",
        ),
        StatusCode::FORBIDDEN => (
            "Console access denied",
            "This console requires an authorized console session.",
        ),
        _ => (
            "Console unavailable",
            "The console could not complete this request.",
        ),
    }
}

fn role_names(mask: RoleType) -> Vec<&'static str> {
    let mut names = Vec::new();
    if mask & ConsoleRole::Viewer.to_role_type() != 0 {
        names.push("viewer");
    }
    if mask & ConsoleRole::Operator.to_role_type() != 0 {
        names.push("operator");
    }
    if mask & ConsoleRole::Admin.to_role_type() != 0 {
        names.push("admin");
    }
    names
}

#[cfg(test)]
mod tests {
    use super::{expired_cookie, session_cookie};

    /// Verifies that production console session cookies carry every browser transport protection.
    #[test]
    fn secure_session_cookie_sets_browser_protections() {
        let cookie = session_cookie("vyuh_console", "session", time::Duration::hours(1), true);
        let rendered = cookie.to_string();

        assert!(rendered.contains("HttpOnly"));
        assert!(rendered.contains("SameSite=Lax"));
        assert!(rendered.contains("Secure"));
    }

    /// Verifies that logout cookies keep the HTTPS restriction while expiring a session.
    #[test]
    fn secure_expired_cookie_keeps_https_restriction() {
        let rendered = expired_cookie("vyuh_console", true).to_string();

        assert!(rendered.contains("Max-Age=0"));
        assert!(rendered.contains("Secure"));
    }
}
