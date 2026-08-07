//! Provider construction and startup registry validation.

use super::*;
use crate::auth::CustomClaims;

pub(super) fn build_provider(
    definition: ProviderDefinitionInner,
    secrets: &SecretRing,
    default_audience: Option<AudienceId>,
) -> Result<ProviderRuntime, AuthError> {
    let id = ProviderId::new(definition.name.as_str())?;
    match definition.kind {
        ProviderKind::Token(value) => {
            build_token(id, *value, secrets, default_audience).map(ProviderRuntime::new)
        }
        ProviderKind::Key(value) => {
            validate_key(&value)?;
            Ok(ProviderRuntime::new(KeyRuntime {
                id,
                definition: value,
            }))
        }
    }
}

fn validate_key(value: &super::super::AuthKey) -> Result<(), AuthError> {
    value.location.validate()?;
    if let Some(csrf) = &value.csrf {
        csrf.validate()?;
    }
    if value.max_credential_bytes == 0 || value.max_credential_bytes > 16 * 1024 {
        return Err(AuthError::InvalidProviderConfig(
            "opaque credential limit must be between 1 and 16384 bytes".into(),
        ));
    }
    Ok(())
}

fn build_token(
    id: ProviderId,
    value: TokenProvider,
    secrets: &SecretRing,
    default_audience: Option<AudienceId>,
) -> Result<TokenRuntime, AuthError> {
    let format = value.codec.format().to_owned();
    validate_token_conf(&value.access)?;
    let access = build_kind(
        &value.access,
        &value.codec,
        value.issuer.as_deref(),
        secrets,
        value.custom_claims.as_ref(),
        id.clone(),
    )?;
    let refresh = value
        .refresh
        .as_ref()
        .map(|conf| build_refresh(conf, &value, secrets, id.clone()))
        .transpose()?;
    if !access.codec.can_encode() && refresh.is_some() {
        return Err(AuthError::InvalidProviderConfig(
            "verify-only providers cannot configure refresh".into(),
        ));
    }
    Ok(TokenRuntime {
        id,
        format,
        access,
        refresh,
        verifier: value.verifier,
        lifecycle: value.lifecycle,
        binding: value.binding,
        leeway_seconds: value.leeway_seconds,
        default_audience,
    })
}

fn build_refresh(
    conf: &TokenConf,
    provider: &TokenProvider,
    secrets: &SecretRing,
    id: ProviderId,
) -> Result<KindRuntime, AuthError> {
    validate_token_conf(conf)?;
    build_kind(
        conf,
        &provider.codec,
        provider.issuer.as_deref(),
        secrets,
        provider.custom_claims.as_ref(),
        id,
    )
}

fn build_kind(
    conf: &TokenConf,
    default: &CodecDefinition,
    provider_issuer: Option<&str>,
    secrets: &SecretRing,
    claims: Option<&CustomClaims>,
    provider: ProviderId,
) -> Result<KindRuntime, AuthError> {
    let definition = conf.codec.as_ref().unwrap_or(default);
    Ok(KindRuntime {
        location: conf.location.clone(),
        response_header: conf.response_header.clone(),
        ttl_seconds: conf.ttl_seconds,
        codec: build_codec(definition, secrets, claims, provider)?,
        issuer: provider_issuer.map(str::to_owned),
        csrf: conf.csrf.clone(),
        max_credential_bytes: conf.max_credential_bytes,
    })
}

pub(super) fn validate_definitions(values: &[ProviderDefinitionInner]) -> Result<(), AuthError> {
    for (position, value) in values.iter().enumerate() {
        let id = ProviderId::new(value.name.as_str())?;
        value.kind.access_location().validate()?;
        if let Some(location) = value.kind.refresh_location() {
            location.validate()?;
        }
        let rest = values.iter().skip(position + 1);
        if rest
            .clone()
            .any(|other| other.name.as_str() == value.name.as_str())
        {
            return Err(AuthError::DuplicateProvider(id.to_string()));
        }
        validate_selectors(value, rest)?;
    }
    Ok(())
}

pub(super) fn validate_login_definitions(values: &[LoginDefinitionInner]) -> Result<(), AuthError> {
    for (position, value) in values.iter().enumerate() {
        let id = LoginMethodId::new(value.name)?;
        if values
            .iter()
            .skip(position + 1)
            .any(|other| other.name == value.name)
        {
            return Err(AuthError::DuplicateLoginMethod(id.as_str().into()));
        }
        validate_login_shape(value, &id)?;
        value.runtime.validate()?;
    }
    Ok(())
}

fn validate_login_shape(value: &LoginDefinitionInner, id: &LoginMethodId) -> Result<(), AuthError> {
    let is_one_step = value.complete_type == std::any::TypeId::of::<super::super::NoChallenge>();
    if value.runtime.is_flow() == is_one_step {
        return Err(AuthError::LoginMethodTypeMismatch(id.as_str().into()));
    }
    Ok(())
}

fn validate_selectors<'a>(
    value: &ProviderDefinitionInner,
    rest: impl Iterator<Item = &'a ProviderDefinitionInner>,
) -> Result<(), AuthError> {
    let access = value.kind.access_location().selector();
    let refresh = value.kind.refresh_location().map(|item| item.selector());
    for other in rest {
        let other_access = other.kind.access_location().selector();
        let other_refresh = other.kind.refresh_location().map(|item| item.selector());
        if other_access == access
            || refresh.as_ref().is_some_and(|value| value == &other_access)
            || other_refresh.as_ref().is_some_and(|value| value == &access)
        {
            return Err(AuthError::AmbiguousProvider(access));
        }
        if let (Some(left), Some(right)) = (&refresh, &other_refresh)
            && right == left
        {
            return Err(AuthError::AmbiguousProvider(left.clone()));
        }
    }
    Ok(())
}
