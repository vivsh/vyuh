//! Parseable-token provider runtime.

use std::sync::Arc;

use axum::http::request::Parts;
use futures::future::BoxFuture;

use super::{
    contract::{ProviderAudienceSet, ProviderCapabilities, ProviderRuntimeContract},
    validation::{
        clear_locations, delivery, issue_token, validate_binding, validate_common,
        validate_credential_size, validate_csrf, validate_issued_binding, validate_subject,
        validate_token,
    },
};
use crate::auth::{
    AudienceId, AuthError, AuthToken, AuthUser, BindingResolver, CodecRuntime, CredentialLocation,
    CredentialType, Credentials, CsrfConf, ErasedLifecycle, ErasedTokenVerifier, LoginResponse,
    ProviderDoc, ProviderId, RefreshMetadata, TokenKind,
};

#[derive(Clone)]
pub(super) struct TokenRuntime {
    pub(super) id: ProviderId,
    pub(super) format: String,
    pub(super) access: KindRuntime,
    pub(super) refresh: Option<KindRuntime>,
    pub(super) verifier: Arc<dyn ErasedTokenVerifier>,
    pub(super) lifecycle: Option<Arc<dyn ErasedLifecycle>>,
    pub(super) binding: Option<BindingResolver>,
    pub(super) leeway_seconds: i64,
    pub(super) default_audience: Option<AudienceId>,
    pub(super) audiences: ProviderAudienceSet,
}

#[derive(Clone)]
pub(super) struct KindRuntime {
    pub(super) location: CredentialLocation,
    pub(super) response_header: Option<String>,
    pub(super) ttl_seconds: i64,
    pub(super) codec: CodecRuntime,
    pub(super) issuer: Option<String>,
    pub(super) csrf: Option<CsrfConf>,
    pub(super) max_credential_bytes: usize,
}

struct TokenPair {
    access: AuthToken,
    refresh: Option<AuthToken>,
}

impl ProviderRuntimeContract for TokenRuntime {
    fn id(&self) -> &ProviderId {
        &self.id
    }

    fn audiences(&self) -> &ProviderAudienceSet {
        &self.audiences
    }

    fn access_location(&self) -> &CredentialLocation {
        &self.access.location
    }

    fn refresh_location(&self) -> Option<&CredentialLocation> {
        self.refresh.as_ref().map(|value| &value.location)
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            authenticate: true,
            login: self.access.codec.can_encode(),
            refresh: self
                .refresh
                .as_ref()
                .is_some_and(|value| value.codec.can_encode()),
            logout: true,
        }
    }

    fn openapi(&self) -> ProviderDoc {
        ProviderDoc {
            id: self.id.to_string(),
            audiences: self.audiences.restricted().map(|values| {
                values
                    .iter()
                    .map(|value| value.as_str().to_owned())
                    .collect()
            }),
            credential_type: CredentialType::Token(Some(self.format.clone())),
            location: self.access.location.doc(),
            csrf_header: self
                .access
                .csrf
                .as_ref()
                .map(|csrf| csrf.header_name.clone()),
        }
    }

    fn authenticate<'a>(
        &'a self,
        raw: &'a str,
        parts: &'a Parts,
        audience: &'a AudienceId,
    ) -> BoxFuture<'a, Result<AuthUser, AuthError>> {
        Box::pin(TokenRuntime::authenticate(self, raw, parts, audience))
    }

    fn login<'a>(
        &'a self,
        user: AuthUser,
        audiences: Vec<AudienceId>,
        binding: Option<String>,
    ) -> BoxFuture<'a, Result<LoginResponse, AuthError>> {
        Box::pin(TokenRuntime::login(self, user, audiences, binding))
    }

    fn refresh<'a>(
        &'a self,
        raw: &'a str,
        parts: &'a Parts,
        audiences: &'a [AudienceId],
    ) -> BoxFuture<'a, Result<LoginResponse, AuthError>> {
        Box::pin(TokenRuntime::refresh(self, raw, parts, audiences))
    }

    fn logout<'a>(
        &'a self,
        parts: &'a Parts,
    ) -> BoxFuture<'a, Result<Vec<(axum::http::HeaderName, axum::http::HeaderValue)>, AuthError>>
    {
        Box::pin(TokenRuntime::logout(self, parts))
    }
}

impl TokenRuntime {
    /// Decodes and accepts one access credential for the requested route audience.
    async fn authenticate(
        &self,
        raw: &str,
        parts: &Parts,
        audience: &AudienceId,
    ) -> Result<AuthUser, AuthError> {
        validate_credential_size(raw, self.access.max_credential_bytes)?;
        let token = self.normalize(self.access.codec.decode(raw).await?)?;
        validate_csrf(self.access.csrf.as_ref(), parts)?;
        self.accept(
            &token,
            TokenKind::Access,
            parts,
            std::slice::from_ref(audience),
        )
        .await
    }

    /// Issues this provider's configured access and optional refresh credentials.
    async fn login(
        &self,
        user: AuthUser,
        audiences: Vec<AudienceId>,
        binding: Option<String>,
    ) -> Result<LoginResponse, AuthError> {
        validate_subject(&user)?;
        validate_issued_binding(self.binding, &binding)?;
        let pair = self.tokens(&user, audiences, binding, None)?;
        self.response(pair).await.map(|(response, _)| response)
    }

    /// Verifies one refresh credential and rotates the complete credential pair.
    async fn refresh(
        &self,
        raw: &str,
        parts: &Parts,
        audiences: &[AudienceId],
    ) -> Result<LoginResponse, AuthError> {
        let refresh = self
            .refresh
            .as_ref()
            .ok_or(AuthError::UnsupportedProviderCapability)?;
        validate_credential_size(raw, refresh.max_credential_bytes)?;
        let current = self.normalize(refresh.codec.decode(raw).await?)?;
        validate_csrf(refresh.csrf.as_ref(), parts)?;
        let user = self
            .accept(&current, TokenKind::Refresh, parts, audiences)
            .await?;
        let pair = self.tokens(
            &user,
            audiences.to_vec(),
            current.binding_value().map(str::to_owned),
            current.family_id().map(str::to_owned),
        )?;
        let (response, replacement) = self.response(pair).await?;
        self.rotate(&current, replacement.as_ref()).await?;
        Ok(response)
    }

    /// Applies provider, lifecycle, binding, and application identity validation.
    async fn accept(
        &self,
        token: &AuthToken,
        kind: TokenKind,
        parts: &Parts,
        audiences: &[AudienceId],
    ) -> Result<AuthUser, AuthError> {
        let expected = self.kind(kind)?;
        validate_token(
            token,
            &self.id,
            kind,
            audiences,
            self.leeway_seconds,
            expected.issuer.as_deref(),
        )?;
        if kind == TokenKind::Refresh && token.family_id().is_none() {
            return Err(AuthError::InvalidCredential);
        }
        if self.lifecycle.is_some() && token.token_id().is_none() {
            return Err(AuthError::InvalidCredential);
        }
        validate_binding(token.binding_value(), self.binding, parts)?;
        if let Some(lifecycle) = &self.lifecycle {
            lifecycle.validate(token).await?;
        }
        let authentication = token.authentication();
        let user = self.verifier.verify(token).await?;
        validate_subject(&user)?;
        Ok(user
            .set_provider(self.id.clone())
            .with_authentication(authentication))
    }

    fn kind(&self, kind: TokenKind) -> Result<&KindRuntime, AuthError> {
        match kind {
            TokenKind::Access => Ok(&self.access),
            TokenKind::Refresh => self
                .refresh
                .as_ref()
                .ok_or(AuthError::UnsupportedProviderCapability),
        }
    }

    /// Constructs one normalized local credential pair with shared family state.
    fn tokens(
        &self,
        user: &AuthUser,
        audiences: Vec<AudienceId>,
        binding: Option<String>,
        family: Option<String>,
    ) -> Result<TokenPair, AuthError> {
        let family = self
            .refresh
            .as_ref()
            .map(|_| family.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()));
        let access = issue_token(
            &self.id,
            TokenKind::Access,
            user,
            audiences.clone(),
            &self.access,
            family.clone(),
            binding.clone(),
        )?;
        let refresh = self
            .refresh
            .as_ref()
            .map(|conf| {
                issue_token(
                    &self.id,
                    TokenKind::Refresh,
                    user,
                    audiences,
                    conf,
                    family,
                    binding,
                )
            })
            .transpose()?;
        Ok(TokenPair { access, refresh })
    }

    fn normalize(&self, mut token: AuthToken) -> Result<AuthToken, AuthError> {
        if token.audience_ids().is_none() {
            let audience = self
                .default_audience
                .clone()
                .ok_or(AuthError::AudienceMismatch)?;
            token.set_audiences(vec![audience]);
        }
        crate::auth::token::validate_structure(&token)?;
        Ok(token)
    }

    /// Encodes a credential pair and prepares its validated response attachments.
    async fn response(
        &self,
        pair: TokenPair,
    ) -> Result<(LoginResponse, Option<AuthToken>), AuthError> {
        let access = self.access.codec.encode(&pair.access).await?;
        validate_credential_size(&access, self.access.max_credential_bytes)?;
        let refresh_value = match (&self.refresh, &pair.refresh) {
            (Some(conf), Some(token)) => {
                let encoded = conf.codec.encode(token).await?;
                validate_credential_size(&encoded, conf.max_credential_bytes)?;
                Some(encoded)
            }
            _ => None,
        };
        let (access_body, access_attachments) = delivery(&self.access, &access)?;
        let (refresh_body, refresh_attachments) = match (&self.refresh, &refresh_value) {
            (Some(conf), Some(value)) => delivery(conf, value)?,
            _ => (None, Vec::new()),
        };
        let attachments = access_attachments
            .into_iter()
            .chain(refresh_attachments)
            .collect();
        let credentials = Credentials::new(access, refresh_value);
        let response = LoginResponse::new(
            credentials,
            access_body,
            refresh_body,
            self.access.ttl_seconds,
            attachments,
        );
        Ok((response, pair.refresh))
    }

    /// Applies lifecycle rotation after replacement credentials have been encoded.
    async fn rotate(
        &self,
        current: &AuthToken,
        replacement: Option<&AuthToken>,
    ) -> Result<(), AuthError> {
        let Some(lifecycle) = &self.lifecycle else {
            return Ok(());
        };
        let replacement = replacement.ok_or(AuthError::UnsupportedProviderCapability)?;
        let metadata = RefreshMetadata::from_token(replacement)?;
        lifecycle.rotate(current, &metadata).await
    }

    /// Revokes a selected credential when configured and prepares client-state removal.
    async fn logout(
        &self,
        parts: &Parts,
    ) -> Result<Vec<(axum::http::HeaderName, axum::http::HeaderValue)>, AuthError> {
        if let Some(token) = self.presented(parts).await?
            && let Some(lifecycle) = &self.lifecycle
        {
            lifecycle.revoke(&token).await?;
        }
        clear_locations([
            (Some(&self.access.location), self.access.csrf.as_ref()),
            (
                self.refresh.as_ref().map(|item| &item.location),
                self.refresh.as_ref().and_then(|item| item.csrf.as_ref()),
            ),
        ])
    }

    /// Extracts and validates at most one selected-provider credential for logout.
    async fn presented(&self, parts: &Parts) -> Result<Option<AuthToken>, AuthError> {
        if let Some(raw) = self.access.location.extract(parts)? {
            validate_credential_size(&raw, self.access.max_credential_bytes)?;
            let token = self.normalize(self.access.codec.decode(&raw).await?)?;
            validate_csrf(self.access.csrf.as_ref(), parts)?;
            self.validate_logout_token(&token, parts)?;
            return Ok(Some(token));
        }
        let Some(refresh) = &self.refresh else {
            return Ok(None);
        };
        let Some(raw) = refresh.location.extract(parts)? else {
            return Ok(None);
        };
        validate_credential_size(&raw, refresh.max_credential_bytes)?;
        let token = self.normalize(refresh.codec.decode(&raw).await?)?;
        validate_csrf(refresh.csrf.as_ref(), parts)?;
        self.validate_logout_token(&token, parts)?;
        Ok(Some(token))
    }

    /// Validates the authenticated token properties required before revocation.
    fn validate_logout_token(&self, token: &AuthToken, parts: &Parts) -> Result<(), AuthError> {
        validate_common(token, &self.id, self.leeway_seconds)?;
        let conf = self.kind(token.kind())?;
        if conf.issuer.is_some() && token.issuer() != conf.issuer.as_deref() {
            return Err(AuthError::InvalidCredential);
        }
        if token.kind() == TokenKind::Refresh && token.family_id().is_none() {
            return Err(AuthError::InvalidCredential);
        }
        if self.lifecycle.is_some() && token.token_id().is_none() {
            return Err(AuthError::InvalidCredential);
        }
        validate_binding(token.binding_value(), self.binding, parts)?;
        Ok(())
    }
}
