//! Console authentication backed by Vyuh's shared token-provider runtime.

use axum::http::request::Parts;
use chrono::Duration;

use crate::{
    Site,
    auth::{
        Audience, AuthError, AuthProvider, AuthUser, CookieConf, Jwt, LoginResponse,
        LogoutResponse, TokenConf, TokenProvider,
    },
    routes::{ClientIp, resolve_client_ip},
};

pub(crate) const CONSOLE_AUDIENCE: Audience = Audience::new("vyuh-console");
pub(crate) const CONSOLE_LOGIN: AuthProvider = AuthProvider::new("vyuh-console-login");
pub(crate) const CONSOLE_TOKEN: AuthProvider = AuthProvider::new("vyuh-console");

/// Authenticates an already verified application user into the built-in console.
pub struct Console<'a> {
    site: &'a Site,
}

impl<'a> Console<'a> {
    pub(crate) const fn new(site: &'a Site) -> Self {
        Self { site }
    }

    /// Creates a short-lived credential accepted by the built-in console login page.
    pub async fn login_token(&self, user: AuthUser) -> Result<LoginResponse, AuthError> {
        self.site
            .auth()
            .using(CONSOLE_LOGIN)
            .login(user, &[CONSOLE_AUDIENCE])
            .await
    }

    /// Creates the console JWT cookie for an already verified user.
    pub async fn login(
        &self,
        user: AuthUser,
        client_ip: ClientIp,
    ) -> Result<LoginResponse, AuthError> {
        self.site
            .auth()
            .using(CONSOLE_TOKEN)
            .login_bound(user, &[CONSOLE_AUDIENCE], client_ip.0.to_string())
            .await
    }

    /// Clears the console JWT cookie and applies the shared logout lifecycle.
    pub async fn logout(&self, parts: &Parts) -> Result<LogoutResponse, AuthError> {
        self.site.auth().using(CONSOLE_TOKEN).logout(parts).await
    }
}

/// Builds the short-lived credential accepted only by the console login form.
pub(crate) fn login_provider() -> TokenProvider {
    TokenProvider::new(Jwt::hs256_site_secret())
        .without_refresh()
        .access(TokenConf::header("x-vyuh-console-login").ttl(Duration::seconds(90)))
}

/// Builds the IP-bound browser credential accepted by protected console routes.
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
