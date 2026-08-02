//! Console token-login handlers and safe return-path handling.

use axum::{
    body::{Body, to_bytes},
    http::{HeaderValue, StatusCode, request::Parts},
    response::{Html, IntoResponse, Redirect, Response},
};
use serde::Deserialize;
use serde_json::json;

use crate::{
    Site,
    console::auth::{CONSOLE_AUDIENCE, CONSOLE_LOGIN, CONSOLE_TOKEN},
    routes::{Request, resolve_client_ip},
    templates::TemplateError,
};

/// Optional return target supplied by a console authentication redirect.
#[derive(Debug, Deserialize)]
pub(crate) struct LoginQuery {
    next: Option<String>,
    token: Option<String>,
}

/// Form submitted with one short-lived console login credential.
#[derive(Debug, Deserialize)]
pub(crate) struct LoginForm {
    token: String,
    next: Option<String>,
}

/// Handles the public GET and POST console login exchange at one route.
pub(crate) async fn route(site: Site, request: Request) -> Response {
    let method = request.method().clone();
    let (parts, body) = request.into_parts();
    if method == axum::http::Method::GET {
        return page(&site, &parts).await;
    }
    if method == axum::http::Method::POST {
        return submit(&site, parts, body).await;
    }
    StatusCode::METHOD_NOT_ALLOWED.into_response()
}

/// Renders the public console token-login page.
async fn page(site: &Site, parts: &Parts) -> Response {
    let query = login_query(parts);
    if site
        .auth()
        .using(CONSOLE_TOKEN)
        .authenticate(parts, CONSOLE_AUDIENCE)
        .await
        .is_ok()
    {
        return redirect(site, query.next.as_deref());
    }
    if let Some(token) = query.token.as_deref() {
        return match exchange(site, parts, token, query.next.as_deref()).await {
            Ok(response) => response,
            Err(status) => render(
                site,
                query.next.as_deref(),
                Some("The console login token is invalid or has expired."),
                status,
            ),
        };
    }
    render(site, query.next.as_deref(), None, StatusCode::OK)
}

/// Exchanges a short-lived command credential for the console browser cookie.
async fn submit(site: &Site, parts: Parts, body: Body) -> Response {
    let Ok(bytes) = to_bytes(body, 8192).await else {
        return render(
            site,
            None,
            Some("The console login request is invalid."),
            StatusCode::BAD_REQUEST,
        );
    };
    let Ok(form) = serde_urlencoded::from_bytes::<LoginForm>(&bytes) else {
        return render(
            site,
            None,
            Some("The console login request is invalid."),
            StatusCode::BAD_REQUEST,
        );
    };
    match exchange(site, &parts, &form.token, form.next.as_deref()).await {
        Ok(response) => response,
        Err(StatusCode::UNAUTHORIZED) => render(
            site,
            form.next.as_deref(),
            Some("The console login token is invalid or has expired."),
            StatusCode::UNAUTHORIZED,
        ),
        Err(status) => render(
            site,
            form.next.as_deref(),
            Some("The console could not complete this sign-in."),
            status,
        ),
    }
}

/// Authenticates a short credential and attaches the long-lived console cookie.
async fn exchange(
    site: &Site,
    parts: &Parts,
    token: &str,
    next: Option<&str>,
) -> Result<Response, StatusCode> {
    let ip = resolve_client_ip(parts).map_err(|_| StatusCode::UNAUTHORIZED)?;
    let user = authenticate(site, token)
        .await
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let login = site
        .console()
        .login(user, crate::routes::ClientIp(ip))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut response = redirect(site, next);
    login.write(&mut response);
    Ok(response)
}

/// Parses an optional redirect target from the public login-page query string.
fn login_query(parts: &Parts) -> LoginQuery {
    parts
        .uri
        .query()
        .and_then(|query| serde_urlencoded::from_str(query).ok())
        .unwrap_or(LoginQuery {
            next: None,
            token: None,
        })
}

/// Authenticates the submitted value only through the private login provider.
async fn authenticate(
    site: &Site,
    token: &str,
) -> Result<crate::auth::AuthUser, crate::auth::AuthError> {
    let parts = login_parts(token)?;
    site.auth()
        .using(CONSOLE_LOGIN)
        .authenticate(&parts, CONSOLE_AUDIENCE)
        .await
}

/// Places the form value in the login provider's normal configured header source.
fn login_parts(token: &str) -> Result<Parts, crate::auth::AuthError> {
    let mut request = axum::http::Request::new(Body::empty());
    let value =
        HeaderValue::from_str(token).map_err(|_| crate::auth::AuthError::InvalidCredential)?;
    request.headers_mut().insert("x-vyuh-console-login", value);
    Ok(request.into_parts().0)
}

/// Renders the login form with a safe generic error message when necessary.
fn render(site: &Site, next: Option<&str>, error: Option<&str>, status: StatusCode) -> Response {
    match page_html(site, next, error) {
        Ok(page) => (status, page).into_response(),
        Err(_) => (status, "Console login is unavailable.").into_response(),
    }
}

/// Builds the template context without exposing credentials or internal errors.
fn page_html(
    site: &Site,
    next: Option<&str>,
    error: Option<&str>,
) -> Result<Html<String>, TemplateError> {
    let urls = site
        .console_urls()
        .ok_or_else(|| TemplateError::NotFound("console runtime".into()))?;
    site.template_engine().html(
        "console/login.html",
        &json!({
            "login_url": &urls.login,
            "next": next,
            "error": error,
            "assets": { "favicon": &urls.favicon_path, "logo": &urls.logo_path },
            "stylesheet_path": &urls.stylesheet_path,
            "version": env!("CARGO_PKG_VERSION"),
        }),
    )
}

/// Selects a registered console page or the console home route as the destination.
fn redirect(site: &Site, next: Option<&str>) -> Response {
    let destination = site.console_destination(next);
    Redirect::to(&destination).into_response()
}
