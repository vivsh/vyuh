//! Provider-specific OAuth2 code exchange and identity profile retrieval.

use futures::StreamExt;
use oauth2::{
    AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken, EndpointNotSet, EndpointSet,
    PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope, TokenResponse, TokenUrl,
    basic::BasicClient,
};
use serde::Deserialize;

use super::http::FederatedHttpClient;
use super::{FederatedIdentity, FederatedLogin, FederatedProvider};
use crate::auth::AuthError;

const MAX_PROFILE_BYTES: usize = 16 * 1024;
const GITHUB_AUTH: &str = "https://github.com/login/oauth/authorize";
const GITHUB_TOKEN: &str = "https://github.com/login/oauth/access_token";
const GITHUB_USER: &str = "https://api.github.com/user";
const GITHUB_EMAILS: &str = "https://api.github.com/user/emails";
const FACEBOOK_AUTH: &str = "https://www.facebook.com/dialog/oauth";
const FACEBOOK_TOKEN: &str = "https://graph.facebook.com/oauth/access_token";
const FACEBOOK_USER: &str = "https://graph.facebook.com/me?fields=id,name,email,picture";

type SocialClient =
    BasicClient<EndpointSet, EndpointNotSet, EndpointNotSet, EndpointNotSet, EndpointSet>;

pub(super) fn authorization_url(
    conf: &FederatedLogin,
    challenge: PkceCodeChallenge,
    state: String,
) -> Result<String, AuthError> {
    let client = client(conf, None)?;
    let mut request = client
        .authorize_url(move || CsrfToken::new(state))
        .set_pkce_challenge(challenge);
    for scope in &conf.scopes {
        request = request.add_scope(Scope::new(scope.clone()));
    }
    Ok(request.url().0.to_string())
}

pub(super) async fn exchange(
    conf: &FederatedLogin,
    http: &FederatedHttpClient,
    secret: Option<&str>,
    code: &str,
    verifier: String,
    return_to: Option<String>,
) -> Result<FederatedIdentity, AuthError> {
    let token = client(conf, secret)?
        .exchange_code(AuthorizationCode::new(code.to_owned()))
        .set_pkce_verifier(PkceCodeVerifier::new(verifier))
        .request_async(http)
        .await
        .map_err(|error| match error {
            oauth2::RequestTokenError::ServerResponse(_) => AuthError::InvalidCredential,
            _ => AuthError::ProviderUnavailable,
        })?;
    let scopes = granted_scopes(conf.provider, token.scopes());
    match conf.provider {
        FederatedProvider::GitHub => {
            github_identity(http, token.access_token().secret(), scopes, return_to).await
        }
        FederatedProvider::Facebook => {
            facebook_identity(http, token.access_token().secret(), scopes, return_to).await
        }
        _ => Err(AuthError::Internal(
            "OIDC login reached the OAuth profile engine".into(),
        )),
    }
}

/// Builds the provider-specific confidential OAuth client without exposing its secret.
fn client(conf: &FederatedLogin, secret: Option<&str>) -> Result<SocialClient, AuthError> {
    let (auth, token) = endpoints(conf.provider)?;
    let client = BasicClient::new(ClientId::new(conf.client_id.clone()))
        .set_auth_uri(AuthUrl::new(auth.into()).map_err(|_| invalid_config())?)
        .set_token_uri(TokenUrl::new(token.into()).map_err(|_| invalid_config())?)
        .set_redirect_uri(
            RedirectUrl::new(conf.redirect_uri.clone()).map_err(|_| invalid_config())?,
        );
    Ok(match secret {
        Some(value) => client.set_client_secret(ClientSecret::new(value.to_owned())),
        None => client,
    })
}

fn endpoints(provider: FederatedProvider) -> Result<(&'static str, &'static str), AuthError> {
    match provider {
        FederatedProvider::GitHub => Ok((GITHUB_AUTH, GITHUB_TOKEN)),
        FederatedProvider::Facebook => Ok((FACEBOOK_AUTH, FACEBOOK_TOKEN)),
        _ => Err(AuthError::InvalidProviderConfig(
            "social OAuth endpoints require GitHub or Facebook".into(),
        )),
    }
}

/// Normalizes granted scopes while handling GitHub's comma-separated response form.
fn granted_scopes(provider: FederatedProvider, values: Option<&Vec<Scope>>) -> Vec<String> {
    let separator = matches!(provider, FederatedProvider::GitHub);
    let mut output = values
        .into_iter()
        .flatten()
        .flat_map(|scope| {
            let value = scope.as_ref();
            if separator {
                value.split(',').collect::<Vec<_>>()
            } else {
                vec![value]
            }
        })
        .map(str::to_owned)
        .collect::<Vec<_>>();
    output.sort_unstable();
    output.dedup();
    output
}

#[derive(Deserialize, serde::Serialize)]
struct GithubUser {
    id: u64,
    login: String,
    name: Option<String>,
    email: Option<String>,
    avatar_url: Option<String>,
}

#[derive(Clone, Deserialize, serde::Serialize)]
struct GithubEmail {
    email: String,
    primary: bool,
    verified: bool,
}

/// Retrieves and normalizes GitHub's stable account identity and verified email.
async fn github_identity(
    http: &FederatedHttpClient,
    token: &str,
    scopes: Vec<String>,
    return_to: Option<String>,
) -> Result<FederatedIdentity, AuthError> {
    let user: GithubUser = github_get(http, GITHUB_USER, token).await?;
    let emails = if scopes.iter().any(|scope| scope == "user:email") {
        github_get::<Vec<GithubEmail>>(http, GITHUB_EMAILS, token).await?
    } else {
        Vec::new()
    };
    let selected = verified_email(&emails);
    let email = selected
        .map(|value| value.email.clone())
        .or_else(|| user.email.clone());
    let email_verified = selected.map(|value| value.verified);
    let claims = serde_json::json!({ "user": user, "emails": emails });
    validate_claims(&claims)?;
    Ok(FederatedIdentity {
        provider: FederatedProvider::GitHub,
        subject: user.id.to_string(),
        issuer: "https://github.com".into(),
        email,
        email_verified,
        name: user.name.or(Some(user.login)),
        picture: user.avatar_url,
        scopes,
        return_to,
        claims,
    })
}

fn verified_email(values: &[GithubEmail]) -> Option<&GithubEmail> {
    values
        .iter()
        .find(|value| value.primary && value.verified)
        .or_else(|| values.iter().find(|value| value.verified))
}

/// Retrieves one bounded authenticated GitHub API document.
async fn github_get<T: serde::de::DeserializeOwned>(
    http: &FederatedHttpClient,
    url: &str,
    token: &str,
) -> Result<T, AuthError> {
    let response = http
        .client()
        .get(url)
        .bearer_auth(token)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .header(reqwest::header::USER_AGENT, "vyuh-federated-login")
        .send()
        .await
        .map_err(|_| AuthError::ProviderUnavailable)?;
    decode_response(response).await
}

#[derive(Deserialize, serde::Serialize)]
struct FacebookUser {
    id: String,
    name: Option<String>,
    email: Option<String>,
    picture: Option<FacebookPicture>,
}

#[derive(Deserialize, serde::Serialize)]
struct FacebookPicture {
    data: FacebookPictureData,
}

#[derive(Deserialize, serde::Serialize)]
struct FacebookPictureData {
    url: Option<String>,
}

/// Retrieves and normalizes Facebook's stable account profile.
async fn facebook_identity(
    http: &FederatedHttpClient,
    token: &str,
    scopes: Vec<String>,
    return_to: Option<String>,
) -> Result<FederatedIdentity, AuthError> {
    let response = http
        .client()
        .get(FACEBOOK_USER)
        .bearer_auth(token)
        .send()
        .await
        .map_err(|_| AuthError::ProviderUnavailable)?;
    let user: FacebookUser = decode_response(response).await?;
    let claims = serde_json::to_value(&user).map_err(|_| AuthError::InvalidCredential)?;
    validate_claims(&claims)?;
    let picture = user.picture.and_then(|value| value.data.url);
    Ok(FederatedIdentity {
        provider: FederatedProvider::Facebook,
        subject: user.id,
        issuer: "https://www.facebook.com".into(),
        email: user.email,
        email_verified: None,
        name: user.name,
        picture,
        scopes,
        return_to,
        claims,
    })
}

/// Decodes one bounded successful provider response with safe failure classification.
async fn decode_response<T: serde::de::DeserializeOwned>(
    response: reqwest::Response,
) -> Result<T, AuthError> {
    let status = response.status();
    if matches!(
        status,
        reqwest::StatusCode::BAD_REQUEST
            | reqwest::StatusCode::UNAUTHORIZED
            | reqwest::StatusCode::FORBIDDEN
    ) {
        return Err(AuthError::InvalidCredential);
    }
    if !status.is_success() {
        return Err(AuthError::ProviderUnavailable);
    }
    let bytes = bounded_bytes(response).await?;
    serde_json::from_slice(&bytes).map_err(|_| AuthError::ProviderUnavailable)
}

/// Buffers at most one small identity profile response.
async fn bounded_bytes(response: reqwest::Response) -> Result<Vec<u8>, AuthError> {
    if response
        .content_length()
        .is_some_and(|value| value > MAX_PROFILE_BYTES as u64)
    {
        return Err(AuthError::ProviderUnavailable);
    }
    let mut output = Vec::with_capacity(1024);
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| AuthError::ProviderUnavailable)?;
        if output.len().saturating_add(chunk.len()) > MAX_PROFILE_BYTES {
            return Err(AuthError::ProviderUnavailable);
        }
        output.extend_from_slice(&chunk);
    }
    Ok(output)
}

fn validate_claims(value: &serde_json::Value) -> Result<(), AuthError> {
    if serde_json::to_vec(value)
        .map_err(|_| AuthError::InvalidCredential)?
        .len()
        > MAX_PROFILE_BYTES
    {
        return Err(AuthError::InvalidCredential);
    }
    Ok(())
}

fn invalid_config() -> AuthError {
    AuthError::InvalidProviderConfig("invalid social login endpoint configuration".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::KeySource;

    /// GitHub authorization uses the provider preset, PKCE, state, and declared scopes.
    #[test]
    fn github_authorization_is_bounded_to_the_preset() -> Result<(), AuthError> {
        let conf = FederatedLogin::github()
            .client_id("client-id")
            .client_secret(KeySource::inline("secret"))
            .redirect_uri("https://app.example.com/auth/github/callback");
        let (challenge, _) = PkceCodeChallenge::new_random_sha256();
        let url = authorization_url(&conf, challenge, "sealed-state".into())?;
        assert!(url.starts_with(GITHUB_AUTH));
        assert!(url.contains("code_challenge_method=S256"));
        assert!(url.contains("state=sealed-state"));
        assert!(url.contains("read%3Auser"));
        assert!(url.contains("user%3Aemail"));
        Ok(())
    }

    /// GitHub's non-standard comma-separated token scope response is normalized once.
    #[test]
    fn github_granted_scopes_are_sorted_and_deduplicated() {
        let values = vec![Scope::new("user:email,read:user,user:email".into())];
        assert_eq!(
            granted_scopes(FederatedProvider::GitHub, Some(&values)),
            vec!["read:user", "user:email"]
        );
    }

    /// Account mapping prefers a verified primary email and then any verified email.
    #[test]
    fn github_verified_email_selection_is_deterministic() {
        let values = vec![
            GithubEmail {
                email: "secondary@example.com".into(),
                primary: false,
                verified: true,
            },
            GithubEmail {
                email: "primary@example.com".into(),
                primary: true,
                verified: true,
            },
        ];
        assert_eq!(
            verified_email(&values).map(|value| value.email.as_str()),
            Some("primary@example.com")
        );
    }
}
