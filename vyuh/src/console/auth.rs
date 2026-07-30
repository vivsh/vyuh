//! Console-specific providers built on Vyuh's shared authentication system.

use axum::http::request::Parts;
use chrono::Duration;

use crate::{
    auth::{Audience, AuthError, AuthProvider, BitRole, CookieConf, Jwt, TokenConf, TokenProvider},
    routes::resolve_client_ip,
};

/// Roles granted by framework-owned console credentials.
#[derive(crate::auth::BitRole)]
pub enum ConsoleRole {
    Viewer,
    Operator,
    Admin,
}

/// Serializable console identity projection.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConsoleUser {
    pub subject: String,
    pub roles: u64,
    pub role_names: Vec<&'static str>,
}

pub(crate) const CONSOLE_AUDIENCE: Audience = Audience::new("vyuh-console");
pub(crate) const CONSOLE_LOGIN: AuthProvider = AuthProvider::new("console-login");
pub(crate) const CONSOLE_TOKEN: AuthProvider = AuthProvider::new("console-token");

pub(crate) fn login_provider() -> TokenProvider {
    TokenProvider::new(Jwt::hs256_site_secret())
        .without_refresh()
        .access(TokenConf::header("X-Vyuh-Console-Login").ttl(Duration::seconds(90)))
}

pub(crate) fn token_provider(cookie: CookieConf) -> TokenProvider {
    TokenProvider::new(Jwt::hs256_site_secret())
        .without_refresh()
        .access(TokenConf::cookie(cookie).ttl(Duration::minutes(90)))
        .binding(client_ip_binding)
}

fn client_ip_binding(parts: &Parts) -> Result<Option<String>, AuthError> {
    resolve_client_ip(parts)
        .map(|ip| Some(ip.to_string()))
        .map_err(|_| AuthError::BindingMismatch)
}
