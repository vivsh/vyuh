//! Response-ready login credentials and logout attachments.

use axum::{
    http::{HeaderName, HeaderValue, Response as HttpResponse},
    response::{IntoResponse, Response},
};
use schemars::JsonSchema;
use serde::Serialize;

use crate::callables::{IntoReturnPart, ReturnPart};

/// Credentials issued together by one complete authentication provider.
pub struct Credentials {
    access: String,
    refresh: Option<String>,
}

impl Credentials {
    /// Deliberately exposes the issued access credential.
    pub fn access(&self) -> &str {
        &self.access
    }

    /// Deliberately exposes the issued refresh credential when configured.
    pub fn refresh(&self) -> Option<&str> {
        self.refresh.as_deref()
    }

    pub(crate) fn new(access: String, refresh: Option<String>) -> Self {
        Self { access, refresh }
    }
}

/// Default serializable data returned after login or refresh.
#[derive(Serialize, JsonSchema)]
pub struct DefaultLoginData {
    #[serde(skip_serializing_if = "Option::is_none")]
    access_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refresh_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token_type: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_in: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ok: Option<bool>,
}

impl DefaultLoginData {
    pub(crate) fn new(access: Option<String>, refresh: Option<String>, expires_in: i64) -> Self {
        let visible = access.is_some() || refresh.is_some();
        Self {
            access_token: access,
            refresh_token: refresh,
            token_type: visible.then_some("Bearer"),
            expires_in: visible.then_some(expires_in),
            ok: (!visible).then_some(true),
        }
    }
}

/// A login result that owns response data and credential attachment state.
pub struct LoginResponse<T = DefaultLoginData> {
    data: T,
    credentials: Credentials,
    attachments: Vec<(HeaderName, HeaderValue)>,
}

impl LoginResponse {
    pub(crate) fn new(
        credentials: Credentials,
        access_body: Option<String>,
        refresh_body: Option<String>,
        expires_in: i64,
        attachments: Vec<(HeaderName, HeaderValue)>,
    ) -> Self {
        Self {
            data: DefaultLoginData::new(access_body, refresh_body, expires_in),
            credentials,
            attachments,
        }
    }

    /// Replaces the default body while preserving credential delivery.
    pub fn data<T: Serialize>(self, data: T) -> LoginResponse<T> {
        LoginResponse {
            data,
            credentials: self.credentials,
            attachments: self.attachments,
        }
    }
}

impl<T> LoginResponse<T> {
    /// Returns the response body without consuming the login result.
    pub fn data_ref(&self) -> &T {
        &self.data
    }

    /// Returns the deliberately redacted issued credentials.
    pub fn credentials(&self) -> &Credentials {
        &self.credentials
    }

    /// Applies provider-managed headers or cookies to an existing response.
    pub fn write(&self, response: &mut Response) {
        for (name, value) in &self.attachments {
            response.headers_mut().append(name, value.clone());
        }
    }
}

impl<T: Serialize> IntoResponse for LoginResponse<T> {
    fn into_response(self) -> Response {
        let mut response = axum::Json(self.data).into_response();
        for (name, value) in self.attachments {
            response.headers_mut().append(name, value);
        }
        response
    }
}

impl<T: JsonSchema + Send + 'static> IntoReturnPart for LoginResponse<T> {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Body(
            crate::callables::TypeSchema::wrap::<T>(),
            "application/json".into(),
        )
    }
}

#[derive(Serialize, JsonSchema)]
struct DefaultLogoutData {
    ok: bool,
}

/// A completed provider logout with validated response attachments.
pub struct LogoutResponse {
    attachments: Vec<(HeaderName, HeaderValue)>,
}

impl LogoutResponse {
    /// Applies provider-managed cookie clearing or other response headers.
    pub fn write<B>(&self, response: &mut HttpResponse<B>) {
        for (name, value) in &self.attachments {
            response.headers_mut().append(name, value.clone());
        }
    }

    pub(crate) fn new(attachments: Vec<(HeaderName, HeaderValue)>) -> Self {
        Self { attachments }
    }
}

impl IntoResponse for LogoutResponse {
    fn into_response(self) -> Response {
        let mut response = axum::Json(DefaultLogoutData { ok: true }).into_response();
        self.write(&mut response);
        response
    }
}

impl IntoReturnPart for LogoutResponse {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Body(
            crate::callables::TypeSchema::wrap::<DefaultLogoutData>(),
            "application/json".into(),
        )
    }
}
