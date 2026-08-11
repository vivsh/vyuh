//! OpenID Connect discovery, PKCE exchange, verification, and key rotation.

use std::{
    sync::Arc,
    time::{Duration as StdDuration, Instant},
};

use openidconnect::{
    AccessTokenHash, AuthorizationCode, ClaimsVerificationError, ClientId, ClientSecret, CsrfToken,
    IssuerUrl, Nonce, OAuth2TokenResponse, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope,
    SignatureVerificationError, TokenResponse as OidcTokenResponse,
    core::{CoreAuthenticationFlow, CoreClient, CoreProviderMetadata},
};

use super::{
    ChallengeCodec, FederatedIdentity, FederatedProvider, FederatedRuntime, LoginMethodId,
    LoginTarget, PendingFederated,
};
use crate::auth::AuthError;

#[derive(Default)]
pub(super) struct DiscoveryState {
    in_flight: bool,
    last_key_refresh: Option<Instant>,
}

type ConfiguredClient = openidconnect::core::CoreClient<
    openidconnect::EndpointSet,
    openidconnect::EndpointNotSet,
    openidconnect::EndpointNotSet,
    openidconnect::EndpointNotSet,
    openidconnect::EndpointMaybeSet,
    openidconnect::EndpointMaybeSet,
>;

pub(super) struct CachedClient {
    client: ConfiguredClient,
    loaded_at: Instant,
}

enum RefreshAction {
    Discover,
    Wait,
    Reuse(Arc<CachedClient>),
}

impl FederatedRuntime {
    pub(super) async fn initialize_oidc(&self) -> Result<(), AuthError> {
        self.client().await.map(|_| ())
    }

    pub(super) async fn begin_oidc(
        &self,
        return_to: Option<String>,
        method: &LoginMethodId,
        target: LoginTarget,
        codec: &ChallengeCodec,
    ) -> Result<String, AuthError> {
        let client = self.client().await?;
        let nonce = Nonce::new_random();
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        let pending = PendingFederated::Oidc {
            nonce: nonce.secret().to_owned(),
            pkce_verifier: verifier.secret().to_owned(),
            return_to,
        };
        let state = self.seal_state(method, target, pending, codec)?;
        Ok(authorization_url(
            &client.client,
            &self.conf.scopes,
            challenge,
            nonce,
            state,
        ))
    }

    pub(super) async fn exchange_oidc(
        &self,
        code: &str,
        nonce: String,
        pkce_verifier: String,
        return_to: Option<String>,
    ) -> Result<FederatedIdentity, AuthError> {
        let client = self.client().await?;
        let response = exchange_code(self, &client.client, code, pkce_verifier).await?;
        let id_token = response.id_token().ok_or(AuthError::InvalidCredential)?;
        let scopes = granted_scopes(&response);
        let verifier = client.client.id_token_verifier();
        let nonce = Nonce::new(nonce);
        let error = match id_token.claims(&verifier, &nonce) {
            Ok(claims) => {
                verify_access_hash(&response, id_token, claims, &verifier)?;
                return identity_from_claims(self.conf.provider, claims, scopes, return_to);
            }
            Err(error) => error,
        };
        if !missing_signing_key(&error) {
            return Err(AuthError::InvalidCredential);
        }
        self.retry_with_refreshed_key(client.loaded_at, &response, nonce, scopes, return_to)
            .await
    }

    /// Revalidates one token after a bounded single-flight metadata refresh.
    async fn retry_with_refreshed_key(
        &self,
        observed: Instant,
        response: &openidconnect::core::CoreTokenResponse,
        nonce: Nonce,
        scopes: Vec<String>,
        return_to: Option<String>,
    ) -> Result<FederatedIdentity, AuthError> {
        let refreshed = self.refresh_for_missing_key(observed).await?;
        let verifier = refreshed.client.id_token_verifier();
        let id_token = response.id_token().ok_or(AuthError::InvalidCredential)?;
        let claims = id_token
            .claims(&verifier, &nonce)
            .map_err(|_| AuthError::InvalidCredential)?;
        verify_access_hash(response, id_token, claims, &verifier)?;
        identity_from_claims(self.conf.provider, claims, scopes, return_to)
    }

    async fn client(&self) -> Result<Arc<CachedClient>, AuthError> {
        if let Some(value) = self.fresh_client() {
            return Ok(value);
        }
        self.refresh_client(None).await
    }

    fn configured_client(
        &self,
        metadata: CoreProviderMetadata,
    ) -> Result<ConfiguredClient, AuthError> {
        let secret = self.client_secret.get().cloned().flatten();
        Ok(CoreClient::from_provider_metadata(
            metadata,
            ClientId::new(self.conf.client_id.clone()),
            secret.map(ClientSecret::new),
        )
        .set_redirect_uri(
            RedirectUrl::new(self.conf.redirect_uri.clone()).map_err(|_| {
                AuthError::InvalidProviderConfig("invalid OIDC redirect URI".into())
            })?,
        ))
    }

    /// Refreshes discovery once for one observed key-set generation and throttles misses.
    async fn refresh_for_missing_key(
        &self,
        observed: Instant,
    ) -> Result<Arc<CachedClient>, AuthError> {
        self.refresh_client(Some(observed)).await
    }

    async fn refresh_client(
        &self,
        observed: Option<Instant>,
    ) -> Result<Arc<CachedClient>, AuthError> {
        loop {
            let notified = self.oidc_notify.notified();
            match self.claim_refresh(observed).await? {
                RefreshAction::Reuse(value) => return Ok(value),
                RefreshAction::Wait => notified.await,
                RefreshAction::Discover => {
                    let previous = self.oidc_client.load_full();
                    let result = self.discover_client().await;
                    self.finish_refresh(result.as_ref().ok()).await;
                    return result.or_else(|error| match (observed, previous) {
                        (None, Some(value)) => Ok(value),
                        _ => Err(error),
                    });
                }
            }
        }
    }

    async fn claim_refresh(&self, observed: Option<Instant>) -> Result<RefreshAction, AuthError> {
        let current = self.oidc_client.load_full();
        if let Some(value) = reusable_client(current.as_ref(), observed) {
            return Ok(RefreshAction::Reuse(value));
        }
        let mut state = self.oidc_discovery.lock().await;
        let current = self.oidc_client.load_full();
        if let Some(value) = reusable_client(current.as_ref(), observed) {
            return Ok(RefreshAction::Reuse(value));
        }
        if state.in_flight {
            return Ok(RefreshAction::Wait);
        }
        if observed.is_some()
            && state
                .last_key_refresh
                .is_some_and(|value| value.elapsed() < StdDuration::from_secs(60))
        {
            return current
                .map(RefreshAction::Reuse)
                .ok_or(AuthError::ProviderUnavailable);
        }
        state.in_flight = true;
        if observed.is_some() {
            state.last_key_refresh = Some(Instant::now());
        }
        Ok(RefreshAction::Discover)
    }

    async fn discover_client(&self) -> Result<Arc<CachedClient>, AuthError> {
        let metadata = self.discover().await?;
        Ok(Arc::new(CachedClient {
            client: self.configured_client(metadata)?,
            loaded_at: Instant::now(),
        }))
    }

    async fn finish_refresh(&self, value: Option<&Arc<CachedClient>>) {
        if let Some(value) = value {
            self.oidc_client.store(Some(value.clone()));
        }
        self.oidc_discovery.lock().await.in_flight = false;
        self.oidc_notify.notify_waiters();
    }

    fn fresh_client(&self) -> Option<Arc<CachedClient>> {
        let value = self.oidc_client.load_full()?;
        (value.loaded_at.elapsed() < StdDuration::from_secs(3600)).then_some(value)
    }

    async fn discover(&self) -> Result<CoreProviderMetadata, AuthError> {
        let issuer = IssuerUrl::new(self.conf.issuer.clone())
            .map_err(|_| AuthError::InvalidProviderConfig("invalid OIDC issuer".into()))?;
        CoreProviderMetadata::discover_async(issuer, self.http()?)
            .await
            .map_err(|_| AuthError::ProviderUnavailable)
    }
}

/// Exchanges one authorization code without exposing the provider access token.
async fn exchange_code(
    runtime: &FederatedRuntime,
    client: &ConfiguredClient,
    code: &str,
    pkce_verifier: String,
) -> Result<openidconnect::core::CoreTokenResponse, AuthError> {
    client
        .exchange_code(AuthorizationCode::new(code.to_owned()))
        .map_err(|_| AuthError::InvalidCredential)?
        .set_pkce_verifier(PkceCodeVerifier::new(pkce_verifier))
        .request_async(runtime.http()?)
        .await
        .map_err(|error| match error {
            openidconnect::RequestTokenError::ServerResponse(_) => AuthError::InvalidCredential,
            _ => AuthError::ProviderUnavailable,
        })
}

fn granted_scopes(response: &openidconnect::core::CoreTokenResponse) -> Vec<String> {
    response
        .scopes()
        .into_iter()
        .flatten()
        .map(|scope| scope.as_ref().to_owned())
        .collect()
}

fn reusable_client(
    value: Option<&Arc<CachedClient>>,
    observed: Option<Instant>,
) -> Option<Arc<CachedClient>> {
    let value = value?;
    match observed {
        Some(observed) if value.loaded_at != observed => Some(value.clone()),
        None if value.loaded_at.elapsed() < StdDuration::from_secs(3600) => Some(value.clone()),
        _ => None,
    }
}

fn missing_signing_key(error: &ClaimsVerificationError) -> bool {
    matches!(
        error,
        ClaimsVerificationError::SignatureVerification(SignatureVerificationError::NoMatchingKey)
    )
}

fn authorization_url(
    client: &ConfiguredClient,
    scopes: &[String],
    challenge: PkceCodeChallenge,
    nonce: Nonce,
    state: String,
) -> String {
    let mut request = client.authorize_url(
        CoreAuthenticationFlow::AuthorizationCode,
        move || CsrfToken::new(state),
        move || nonce,
    );
    for scope in scopes {
        request = request.add_scope(Scope::new(scope.clone()));
    }
    request.set_pkce_challenge(challenge).url().0.to_string()
}

fn identity_from_claims(
    provider: FederatedProvider,
    claims: &openidconnect::core::CoreIdTokenClaims,
    scopes: Vec<String>,
    return_to: Option<String>,
) -> Result<FederatedIdentity, AuthError> {
    let value = serde_json::to_value(claims).map_err(|_| AuthError::InvalidCredential)?;
    if serde_json::to_vec(&value)
        .map_err(|_| AuthError::InvalidCredential)?
        .len()
        > 16 * 1024
    {
        return Err(AuthError::InvalidCredential);
    }
    Ok(FederatedIdentity {
        provider,
        subject: claims.subject().as_str().to_owned(),
        issuer: claims.issuer().as_str().to_owned(),
        email: claims.email().map(|value| value.as_str().to_owned()),
        email_verified: claims.email_verified(),
        name: string_claim(&value, "name"),
        picture: string_claim(&value, "picture"),
        scopes,
        return_to,
        claims: value,
    })
}

fn string_claim(value: &serde_json::Value, name: &str) -> Option<String> {
    value
        .get(name)
        .and_then(|item| item.as_str())
        .map(str::to_owned)
}

fn verify_access_hash(
    response: &openidconnect::core::CoreTokenResponse,
    id_token: &openidconnect::core::CoreIdToken,
    claims: &openidconnect::core::CoreIdTokenClaims,
    verifier: &openidconnect::core::CoreIdTokenVerifier,
) -> Result<(), AuthError> {
    let Some(expected) = claims.access_token_hash() else {
        return Ok(());
    };
    let actual = AccessTokenHash::from_token(
        response.access_token(),
        id_token
            .signing_alg()
            .map_err(|_| AuthError::InvalidCredential)?,
        id_token
            .signing_key(verifier)
            .map_err(|_| AuthError::InvalidCredential)?,
    )
    .map_err(|_| AuthError::InvalidCredential)?;
    if actual != *expected {
        return Err(AuthError::InvalidCredential);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only an unknown signing key is eligible for a bounded metadata refresh.
    #[test]
    fn refreshes_only_for_missing_signing_keys() {
        let missing = ClaimsVerificationError::SignatureVerification(
            SignatureVerificationError::NoMatchingKey,
        );
        let forged = ClaimsVerificationError::SignatureVerification(
            SignatureVerificationError::CryptoError("invalid signature".into()),
        );
        assert!(missing_signing_key(&missing));
        assert!(!missing_signing_key(&forged));
        assert!(!missing_signing_key(
            &ClaimsVerificationError::InvalidNonce("wrong nonce".into())
        ));
    }
}
