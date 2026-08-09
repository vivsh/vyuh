//! Provider construction and startup registry validation.

use super::{
    AudienceId, AuthError, KeyRuntime, KindRuntime, LoginDefinitionInner, LoginMethodId,
    ProviderId, ProviderRuntime, SecretRing, TokenRuntime, contract::ProviderAudienceSet,
    indexes::validate_access_selectors,
};
use crate::auth::{
    Audience, AuthKey, CodecDefinition, CustomClaims, ProviderDefinitionInner, ProviderKind,
    TokenConf, TokenProvider, build_codec, validate_token_conf,
};

pub(super) enum PreparedProvider {
    Ready(ProviderRuntime),
    #[cfg(feature = "oauth")]
    OAuth(ProviderId, Box<crate::auth::OAuthResourceServer>),
    #[cfg(feature = "id-token")]
    IdToken(ProviderId, Box<crate::auth::IdToken>),
}

impl PreparedProvider {
    /// Finishes providers that require asynchronous network initialization.
    pub(super) async fn finish(self) -> Result<ProviderRuntime, AuthError> {
        match self {
            Self::Ready(runtime) => Ok(runtime),
            #[cfg(feature = "oauth")]
            Self::OAuth(id, value) => crate::auth::oauth::OAuthRuntime::build(id, *value)
                .await
                .map(ProviderRuntime::new),
            #[cfg(feature = "id-token")]
            Self::IdToken(id, value) => crate::auth::id_token::IdTokenRuntime::build(id, *value)
                .await
                .map(ProviderRuntime::new),
        }
    }
}

/// Resolves local provider material during blocking authentication preparation.
pub(super) fn prepare_provider(
    definition: ProviderDefinitionInner,
    secrets: &SecretRing,
    default_audience: Option<AudienceId>,
) -> Result<PreparedProvider, AuthError> {
    let id = ProviderId::new(definition.name.as_str())?;
    match definition.kind {
        ProviderKind::Token(value) => build_token(id, *value, secrets, default_audience)
            .map(ProviderRuntime::new)
            .map(PreparedProvider::Ready),
        ProviderKind::Key(value) => {
            validate_key(&value)?;
            let audiences = provider_audiences(value.audiences.as_deref())?;
            Ok(PreparedProvider::Ready(ProviderRuntime::new(KeyRuntime {
                id,
                definition: *value,
                audiences,
            })))
        }
        #[cfg(feature = "oauth")]
        ProviderKind::OAuth(value) => Ok(PreparedProvider::OAuth(id, value)),
        #[cfg(feature = "id-token")]
        ProviderKind::IdToken(value) => Ok(PreparedProvider::IdToken(id, value)),
    }
}

fn validate_key(value: &AuthKey) -> Result<(), AuthError> {
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
    let audiences = provider_audiences(value.audiences.as_deref())?;
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
        audiences,
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
    if values.len() > 64 {
        return Err(AuthError::InvalidProviderConfig(
            "at most 64 authentication providers may be registered".into(),
        ));
    }
    for (position, value) in values.iter().enumerate() {
        let id = ProviderId::new(value.name.as_str())?;
        definition_audiences(&value.kind)?;
        value.kind.access_location().validate()?;
        if let Some(location) = value.kind.refresh_location() {
            location.validate()?;
        }
        if values
            .iter()
            .skip(position + 1)
            .any(|other| other.name.as_str() == value.name.as_str())
        {
            return Err(AuthError::DuplicateProvider(id.to_string()));
        }
    }
    let selectors = values
        .iter()
        .map(|value| {
            Ok((
                value.kind.access_location().selector(),
                definition_audiences(&value.kind)?,
            ))
        })
        .collect::<Result<Vec<_>, AuthError>>()?;
    validate_access_selectors(selectors)
}

pub(super) fn validate_login_definitions(values: &[LoginDefinitionInner]) -> Result<(), AuthError> {
    if values.len() > 64 {
        return Err(AuthError::InvalidProviderConfig(
            "at most 64 login methods may be registered".into(),
        ));
    }
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

/// Resolves one provider definition's static local route-audience coverage.
fn definition_audiences(kind: &ProviderKind) -> Result<ProviderAudienceSet, AuthError> {
    match kind {
        ProviderKind::Token(value) => provider_audiences(value.audiences.as_deref()),
        ProviderKind::Key(value) => provider_audiences(value.audiences.as_deref()),
        #[cfg(feature = "oauth")]
        ProviderKind::OAuth(value) => resource_audiences(value.route_audiences()),
        #[cfg(feature = "id-token")]
        ProviderKind::IdToken(value) => {
            resource_audiences(value.resources.iter().map(|item| item.0))
        }
    }
}

fn provider_audiences(values: Option<&[Audience]>) -> Result<ProviderAudienceSet, AuthError> {
    match values {
        None => Ok(ProviderAudienceSet::Any),
        Some(values) => resource_audiences(values.iter().copied()),
    }
}

/// Validates and normalizes finite route-audience descriptors for runtime indexing.
fn resource_audiences(
    values: impl IntoIterator<Item = Audience>,
) -> Result<ProviderAudienceSet, AuthError> {
    let values = values
        .into_iter()
        .map(AudienceId::declared)
        .collect::<Result<Vec<_>, _>>()?;
    ProviderAudienceSet::only(values)
}

#[cfg(all(test, feature = "oauth"))]
mod tests {
    use super::*;
    use crate::auth::{AuthConf, AuthProvider, OAuthResource, OAuthResourceServer};

    const REPORTS: Audience = Audience::new("reports");
    const ADMIN: Audience = Audience::new("admin");
    const FIRST: AuthProvider = AuthProvider::new("first-oauth");
    const SECOND: AuthProvider = AuthProvider::new("second-oauth");

    fn resource_server(audience: Audience) -> OAuthResourceServer {
        OAuthResourceServer::discovery("https://accounts.example.com")
            .resource(audience, OAuthResource::new("https://api.example.com"))
    }

    /// Verifies independent OAuth resource servers may share Bearer for disjoint audiences.
    #[test]
    fn oauth_bearer_selectors_are_unique_per_local_audience() {
        let conf = AuthConf::empty()
            .provider(FIRST, resource_server(REPORTS))
            .provider(SECOND, resource_server(ADMIN));
        assert!(validate_definitions(&conf.definitions()).is_ok());
    }

    /// Verifies OAuth resource servers cannot share Bearer on one local audience.
    #[test]
    fn oauth_bearer_overlap_is_rejected_before_discovery() {
        let conf = AuthConf::empty()
            .provider(FIRST, resource_server(REPORTS))
            .provider(SECOND, resource_server(REPORTS));
        assert!(validate_definitions(&conf.definitions()).is_err());
    }
}
