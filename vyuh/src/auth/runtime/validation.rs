//! Shared provider validation, credential delivery, and logout helpers.

use axum::http::request::Parts;
use chrono::Utc;
use ring::constant_time;

use super::KindRuntime;
use crate::auth::{
    AudienceId, AuthError, AuthToken, AuthUser, BindingResolver, CredentialLocation, CsrfConf,
    ProviderId, TokenKind,
};

type ResponseAttachments = Vec<(axum::http::HeaderName, axum::http::HeaderValue)>;
type Delivery = (
    Option<String>,
    ResponseAttachments,
    Option<(String, String, String)>,
);

pub(super) fn issue_token(
    id: &ProviderId,
    kind: TokenKind,
    user: &AuthUser,
    audiences: Vec<AudienceId>,
    conf: &KindRuntime,
    family: Option<String>,
    binding: Option<String>,
) -> Result<AuthToken, AuthError> {
    let expires_at = Utc::now()
        .timestamp()
        .checked_add(conf.ttl_seconds)
        .ok_or_else(|| AuthError::InvalidProviderConfig("token expiry overflow".into()))?;
    Ok(AuthToken::issued(crate::auth::token::LocalToken {
        provider: id.clone(),
        kind,
        user,
        audiences,
        expires_at,
        family_id: family,
        binding,
        issuer: conf.issuer.clone(),
    }))
}

pub(super) fn delivery(conf: &KindRuntime, value: &str) -> Result<Delivery, AuthError> {
    let attachment = match &conf.response_header {
        Some(name) => Some(CredentialLocation::response_attachment(name, value)?),
        None => conf.location.attachment(value, conf.ttl_seconds)?,
    };
    let body = (attachment.is_none() && !conf.location.is_cookie()).then(|| value.to_owned());
    let mut attachments = attachment.into_iter().collect::<Vec<_>>();
    let csrf_request = if let Some(csrf) = &conf.csrf {
        let token = uuid::Uuid::new_v4().simple().to_string();
        attachments.push(csrf.attachment(&token, conf.ttl_seconds)?);
        Some((csrf.cookie.name.clone(), csrf.header_name.clone(), token))
    } else {
        None
    };
    Ok((body, attachments, csrf_request))
}

pub(super) fn validate_token(
    token: &AuthToken,
    provider: &ProviderId,
    kind: TokenKind,
    audiences: &[AudienceId],
    leeway_seconds: i64,
    issuer: Option<&str>,
) -> Result<(), AuthError> {
    validate_common(token, provider, leeway_seconds)?;
    if token.kind() != kind {
        return Err(AuthError::WrongTokenKind);
    }
    if issuer.is_some() && token.issuer() != issuer {
        return Err(AuthError::InvalidCredential);
    }
    let token_audiences = token.audience_ids().ok_or(AuthError::AudienceMismatch)?;
    if audiences
        .iter()
        .any(|audience| !token_audiences.contains(audience))
    {
        return Err(AuthError::AudienceMismatch);
    }
    Ok(())
}

pub(super) fn validate_common(
    token: &AuthToken,
    provider: &ProviderId,
    leeway_seconds: i64,
) -> Result<(), AuthError> {
    if token.version() != 2
        || token.provider() != provider.as_str()
        || crate::auth::token::validate_structure(token).is_err()
    {
        return Err(AuthError::InvalidCredential);
    }
    let now = Utc::now();
    let leeway = chrono::Duration::seconds(leeway_seconds);
    let issued_at = token.issued_at()?;
    let authentication_time = token.authentication_time()?;
    if token.expires_at()? <= now - leeway {
        return Err(AuthError::ExpiredCredential);
    }
    if token
        .not_before()?
        .is_some_and(|value| value > now + leeway)
        || issued_at > now + leeway
        || authentication_time.is_some_and(|value| value > issued_at + leeway)
    {
        return Err(AuthError::CredentialNotYetValid);
    }
    Ok(())
}

pub(super) fn validate_binding(
    expected: Option<&str>,
    resolver: Option<BindingResolver>,
    parts: &Parts,
) -> Result<(), AuthError> {
    match (expected, resolver) {
        (None, None) => Ok(()),
        (Some(expected), Some(resolve)) => validate_resolved_binding(expected, resolve(parts)?),
        _ => Err(AuthError::BindingMismatch),
    }
}

fn validate_resolved_binding(
    expected: &str,
    current: Option<crate::auth::AuthBinding>,
) -> Result<(), AuthError> {
    let matches = current.as_ref().is_some_and(|current| {
        let current = current.as_str();
        constant_time::verify_slices_are_equal(expected.as_bytes(), current.as_bytes()).is_ok()
    });
    if matches {
        Ok(())
    } else {
        Err(AuthError::BindingMismatch)
    }
}

pub(super) fn validate_csrf(csrf: Option<&CsrfConf>, parts: &Parts) -> Result<(), AuthError> {
    match csrf {
        Some(csrf) => csrf.verify(parts),
        None => Ok(()),
    }
}

pub(super) fn validate_credential_size(value: &str, limit: usize) -> Result<(), AuthError> {
    if value.len() > limit {
        Err(AuthError::InvalidCredential)
    } else {
        Ok(())
    }
}

pub(super) fn validate_issued_binding(
    resolver: Option<BindingResolver>,
    binding: &Option<String>,
) -> Result<(), AuthError> {
    match (resolver, binding) {
        (None, None) | (Some(_), Some(_)) => Ok(()),
        (Some(_), None) => Err(AuthError::BindingRequired),
        (None, Some(_)) => Err(AuthError::UnsupportedProviderCapability),
    }
}

pub(super) fn validate_subject(user: &AuthUser) -> Result<(), AuthError> {
    user.validate()
}

pub(super) fn clear_locations(
    values: [(Option<&CredentialLocation>, Option<&CsrfConf>); 2],
) -> Result<Vec<(axum::http::HeaderName, axum::http::HeaderValue)>, AuthError> {
    let mut output = Vec::with_capacity(4);
    for (location, csrf) in values {
        if let Some(location) = location
            && let Some(attachment) = location.clear()?
        {
            push_unique(&mut output, attachment);
        }
        if let Some(csrf) = csrf {
            push_unique(&mut output, csrf.clear()?);
        }
    }
    Ok(output)
}

fn push_unique<T: PartialEq>(values: &mut Vec<T>, value: T) {
    if !values.contains(&value) {
        values.push(value);
    }
}
