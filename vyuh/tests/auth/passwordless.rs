use std::{collections::BTreeMap, sync::Mutex};

use super::*;
#[cfg(feature = "email")]
use vyuh::auth::{
    EmailAddress, EmailLoginResolver, LoginChallengeKind, MagicLinkCallback, MagicLinkLogin,
};
use vyuh::auth::{
    Otp, OtpLogin, OtpLoginResolver, OtpPolicy, PasswordlessAddress, PasswordlessAttempt,
    PasswordlessChallenge, PasswordlessStart, PasswordlessStore,
};

const OTP: LoginMethod<PasswordlessAddress, Otp> = LoginMethod::new("otp");

struct Accounts;

#[cfg(feature = "email")]
impl EmailLoginResolver for Accounts {
    fn resolve(
        &self,
        email: &EmailAddress,
    ) -> impl std::future::Future<Output = Result<Option<AuthUser>, AuthError>> + Send + '_ {
        let known = email.as_str() == "user@example.com";
        async move { Ok(known.then(|| AuthUser::new("email-user"))) }
    }
}

impl OtpLoginResolver for Accounts {
    fn resolve(
        &self,
        address: &PasswordlessAddress,
    ) -> impl std::future::Future<Output = Result<Option<AuthUser>, AuthError>> + Send + '_ {
        let user = match address.as_str() {
            "+15551234567" => Some(AuthUser::new("phone-user")),
            #[cfg(feature = "email")]
            "user@example.com" => Some(AuthUser::new("email-user")),
            _ => None,
        };
        async move { Ok(user) }
    }
}

#[derive(Default)]
struct ChallengeStore {
    values: Mutex<BTreeMap<String, StoredChallenge>>,
}

struct StoredChallenge {
    proofs: Vec<Vec<u8>>,
    state: String,
}

impl PasswordlessStore for ChallengeStore {
    fn begin(
        &self,
        challenge: PasswordlessChallenge,
    ) -> impl std::future::Future<Output = Result<PasswordlessStart, AuthError>> + Send + '_ {
        let result = self
            .values
            .lock()
            .map_err(|_| AuthError::Internal("test store lock failed".into()))
            .map(|mut values| {
                let id = challenge.id().to_owned();
                values.insert(
                    id.clone(),
                    StoredChallenge {
                        proofs: challenge.proofs().to_vec(),
                        state: challenge.state().to_owned(),
                    },
                );
                PasswordlessStart::new(id, challenge.expires_at(), challenge.next_issue_at(), true)
            });
        async move { result }
    }

    fn attempt(
        &self,
        challenge_id: &str,
        proofs: &[Vec<u8>],
    ) -> impl std::future::Future<Output = Result<PasswordlessAttempt, AuthError>> + Send + '_ {
        let result = self
            .values
            .lock()
            .map_err(|_| AuthError::Internal("test store lock failed".into()))
            .map(|mut values| {
                let matched = values.get(challenge_id).is_some_and(|stored| {
                    stored
                        .proofs
                        .iter()
                        .any(|expected| proofs.iter().any(|proof| proof == expected))
                });
                match matched {
                    true => values
                        .remove(challenge_id)
                        .map(|stored| PasswordlessAttempt::accepted(stored.state))
                        .ok_or(AuthError::InvalidCredential),
                    false => Ok(PasswordlessAttempt::Rejected),
                }
            });
        async move { result? }
    }
}

/// Verifies one OTP method handles a phone address and issues normal credentials.
#[tokio::test]
async fn otp_completes_through_the_selected_provider() -> Result<(), AuthError> {
    let auth = AuthConf::development()
        .passwordless_store(ChallengeStore::default())
        .method(OTP, OtpLogin::new(Accounts));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;

    let mut challenge = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .via(OTP)
        .begin(PasswordlessAddress::phone("+15551234567"), &[REPORTS])
        .await?;
    let token = challenge
        .token()
        .ok_or(AuthError::InvalidLoginState)?
        .to_owned();
    let code = challenge
        .take_otp_delivery()
        .ok_or(AuthError::InvalidCredential)?
        .code()
        .to_owned();
    let login = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .via(OTP)
        .complete(Otp::new(token, code))
        .await?;

    assert!(!login.credentials().access().is_empty());
    Ok(())
}

/// Verifies atomic proof consumption permits exactly one concurrent OTP completion.
#[tokio::test]
async fn otp_challenge_is_consumed_once() -> Result<(), AuthError> {
    let auth = AuthConf::development()
        .passwordless_store(ChallengeStore::default())
        .method(OTP, OtpLogin::new(Accounts));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;

    let mut challenge = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .via(OTP)
        .begin(PasswordlessAddress::phone("+15551234567"), &[REPORTS])
        .await?;
    let token = challenge
        .token()
        .ok_or(AuthError::InvalidLoginState)?
        .to_owned();
    let code = challenge
        .take_otp_delivery()
        .ok_or(AuthError::InvalidCredential)?
        .code()
        .to_owned();

    let selected = site.auth().using(DEFAULT_AUTH_PROVIDER).via(OTP);
    let first = selected.complete(Otp::new(token.clone(), code.clone()));
    let second = selected.complete(Otp::new(token, code));
    let (first, second) = tokio::join!(first, second);
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    assert!(first.is_err() || second.is_err());
    Ok(())
}

/// Verifies OTP methods cannot build without durable challenge storage.
#[tokio::test]
async fn otp_requires_a_durable_challenge_store() -> Result<(), AuthError> {
    let auth = AuthConf::development().method(OTP, OtpLogin::new(Accounts));
    let error = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .err()
        .ok_or(AuthError::InvalidCredential)?;

    assert!(error.to_string().contains("passwordless_store"));
    Ok(())
}

/// Verifies invalid OTP policies are rejected when the site builds.
#[tokio::test]
async fn invalid_otp_policy_fails_site_construction() -> Result<(), AuthError> {
    let auth =
        AuthConf::development().method(OTP, OtpLogin::new(Accounts).policy(OtpPolicy::numeric(3)));
    let error = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .err()
        .ok_or(AuthError::InvalidCredential)?;

    assert!(error.to_string().contains("OTP policy"));
    Ok(())
}

/// Verifies unresolved principals never receive a proof delivery value.
#[tokio::test]
async fn unknown_otp_address_has_no_delivery_value() -> Result<(), AuthError> {
    let auth = AuthConf::development()
        .passwordless_store(ChallengeStore::default())
        .method(OTP, OtpLogin::new(Accounts));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;

    let mut challenge = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .via(OTP)
        .begin(PasswordlessAddress::phone("+15550000000"), &[REPORTS])
        .await?;

    assert!(challenge.token().is_some());
    assert!(challenge.take_otp_delivery().is_none());
    Ok(())
}

/// Verifies the Base32 policy generates the requested unambiguous code shape.
#[tokio::test]
async fn base32_otp_policy_generates_a_bounded_code() -> Result<(), AuthError> {
    let auth = AuthConf::development()
        .passwordless_store(ChallengeStore::default())
        .method(
            OTP,
            OtpLogin::new(Accounts).policy(OtpPolicy::crockford_base32(8)),
        );
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let mut challenge = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .via(OTP)
        .begin(PasswordlessAddress::phone("+15551234567"), &[REPORTS])
        .await?;
    let delivery = challenge
        .take_otp_delivery()
        .ok_or(AuthError::InvalidCredential)?;

    assert_eq!(delivery.code().len(), 8);
    assert!(
        delivery
            .code()
            .bytes()
            .all(|value| b"0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(&value))
    );
    Ok(())
}

/// Verifies passwordless challenge JSON contains only the opaque challenge protocol fields.
#[tokio::test]
async fn otp_challenge_json_never_contains_a_code() -> Result<(), AuthError> {
    use axum::{body::to_bytes, response::IntoResponse};

    let auth = AuthConf::development()
        .passwordless_store(ChallengeStore::default())
        .method(OTP, OtpLogin::new(Accounts));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let challenge = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .via(OTP)
        .begin(PasswordlessAddress::phone("+15551234567"), &[REPORTS])
        .await?;
    let body = to_bytes(challenge.into_response().into_body(), 1024)
        .await
        .map_err(|_| AuthError::Internal("challenge response was too large".into()))?;
    let json = std::str::from_utf8(&body).map_err(|_| AuthError::InvalidCredential)?;

    assert!(!json.contains("\"code\""));
    Ok(())
}

/// Verifies an application handler receives but cannot serialize an issued email OTP.
#[cfg(feature = "email")]
#[tokio::test]
async fn otp_delivery_is_application_owned() -> Result<(), AuthError> {
    let auth = AuthConf::development()
        .passwordless_store(ChallengeStore::default())
        .method(OTP, OtpLogin::new(Accounts).policy(OtpPolicy::numeric(8)));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;

    let mut challenge = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .via(OTP)
        .begin(PasswordlessAddress::email("user@example.com"), &[REPORTS])
        .await?;

    assert_eq!(challenge.kind(), LoginChallengeKind::Code);
    assert!(challenge.token().is_some());
    let delivery = challenge
        .take_otp_delivery()
        .ok_or(AuthError::InvalidCredential)?;
    assert_eq!(delivery.code().len(), 8);
    Ok(())
}

/// Verifies magic links reject a missing absolute callback URL during site construction.
#[cfg(feature = "email")]
#[tokio::test]
async fn magic_link_requires_an_explicit_callback_url() -> Result<(), AuthError> {
    const EMAIL_LINK: LoginMethod<EmailAddress, MagicLinkCallback> = LoginMethod::new("email-link");
    let auth = AuthConf::development()
        .passwordless_store(ChallengeStore::default())
        .method(EMAIL_LINK, MagicLinkLogin::new(Accounts));
    let error = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .err()
        .ok_or(AuthError::InvalidCredential)?;

    assert!(error.to_string().contains("callback_url"));
    Ok(())
}

/// Verifies default magic links are stateful and permit exactly one completion.
#[cfg(feature = "email")]
#[tokio::test]
async fn magic_link_is_one_time_by_default() -> Result<(), AuthError> {
    const EMAIL_LINK: LoginMethod<EmailAddress, MagicLinkCallback> =
        LoginMethod::new("email-link-stateful");
    let auth = AuthConf::development()
        .passwordless_store(ChallengeStore::default())
        .method(
            EMAIL_LINK,
            MagicLinkLogin::new(Accounts).callback_url("https://example.com/auth/email/callback"),
        );
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let challenge = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .via(EMAIL_LINK)
        .begin(EmailAddress::new("user@example.com"), &[REPORTS])
        .await?;
    let url = url::Url::parse(
        challenge
            .magic_link_url()
            .ok_or(AuthError::InvalidCredential)?,
    )
    .map_err(|_| AuthError::InvalidCredential)?;
    let token = url
        .query_pairs()
        .find(|(name, _)| name == "token")
        .map(|(_, value)| value.into_owned())
        .ok_or(AuthError::InvalidCredential)?;
    let selected = site.auth().using(DEFAULT_AUTH_PROVIDER).via(EMAIL_LINK);
    let first = selected.complete(MagicLinkCallback::new(token.clone()));
    let second = selected.complete(MagicLinkCallback::new(token));
    let (first, second) = tokio::join!(first, second);
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    Ok(())
}

/// Verifies explicitly reusable magic links need no durable challenge store.
#[cfg(feature = "email")]
#[tokio::test]
async fn stateless_magic_link_completes_without_a_store() -> Result<(), AuthError> {
    const EMAIL_LINK: LoginMethod<EmailAddress, MagicLinkCallback> =
        LoginMethod::new("email-link-stateless");
    let auth = AuthConf::development().method(
        EMAIL_LINK,
        MagicLinkLogin::new(Accounts)
            .callback_url("https://example.com/auth/email/callback")
            .stateless(UnsafeReusableMagicLinks::allow()),
    );
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let challenge = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .via(EMAIL_LINK)
        .begin(EmailAddress::new("user@example.com"), &[REPORTS])
        .await?;
    let url = url::Url::parse(
        challenge
            .magic_link_url()
            .ok_or(AuthError::InvalidCredential)?,
    )
    .map_err(|_| AuthError::InvalidCredential)?;
    let token = url
        .query_pairs()
        .find(|(name, _)| name == "token")
        .map(|(_, value)| value.into_owned())
        .ok_or(AuthError::InvalidCredential)?;
    let login = site
        .auth()
        .using(DEFAULT_AUTH_PROVIDER)
        .via(EMAIL_LINK)
        .complete(MagicLinkCallback::new(token))
        .await?;
    assert!(!login.credentials().access().is_empty());
    Ok(())
}
