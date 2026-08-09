use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use super::*;
#[cfg(feature = "email")]
use vyuh::auth::{
    EmailAddress, EmailLogin, EmailLoginResolver, EmailOtp, EmailOtpMessage, EmailOtpSender,
    LoginChallengeKind, MagicLinkCallback,
};
use vyuh::auth::{
    PasswordlessAttempt, PasswordlessChallenge, PasswordlessStart, PasswordlessStore, PhoneLogin,
    PhoneLoginResolver, PhoneNumber, PhoneOtp, PhoneOtpMessage, PhoneOtpSender,
};

const PHONE_OTP: LoginMethod<PhoneNumber, PhoneOtp> = LoginMethod::new("phone-otp");
#[cfg(feature = "email")]
const EMAIL_OTP: LoginMethod<EmailAddress, EmailOtp> = LoginMethod::new("email-otp");

struct PhoneAccounts;

#[cfg(feature = "email")]
struct EmailAccounts;

#[cfg(feature = "email")]
impl EmailLoginResolver for EmailAccounts {
    fn resolve(
        &self,
        email: &EmailAddress,
    ) -> impl std::future::Future<Output = Result<Option<AuthUser>, AuthError>> + Send + '_ {
        let known = email.as_str() == "user@example.com";
        async move { Ok(known.then(|| AuthUser::new("email-user"))) }
    }
}

#[cfg(feature = "email")]
struct EmailSender;

#[cfg(feature = "email")]
impl EmailOtpSender for EmailSender {
    fn send(
        &self,
        _email: &EmailAddress,
        _message: &EmailOtpMessage,
    ) -> impl std::future::Future<Output = Result<(), AuthError>> + Send + '_ {
        async { Ok(()) }
    }
}

impl PhoneLoginResolver for PhoneAccounts {
    fn resolve(
        &self,
        phone: &PhoneNumber,
    ) -> impl std::future::Future<Output = Result<Option<AuthUser>, AuthError>> + Send + '_ {
        let known = phone.as_str() == "+15551234567";
        async move { Ok(known.then(|| AuthUser::new("phone-user"))) }
    }
}

#[derive(Clone, Default)]
struct CapturedPhoneSender(Arc<Mutex<Option<String>>>);

impl PhoneOtpSender for CapturedPhoneSender {
    fn send(
        &self,
        _phone: &PhoneNumber,
        message: &PhoneOtpMessage,
    ) -> impl std::future::Future<Output = Result<(), AuthError>> + Send + '_ {
        let slot = self.0.clone();
        let code = message.code().to_owned();
        async move {
            *slot
                .lock()
                .map_err(|_| AuthError::Internal("test sender lock failed".into()))? = Some(code);
            Ok(())
        }
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
                PasswordlessStart::new(id, challenge.expires_at(), challenge.resend_at(), true)
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

    fn discard(
        &self,
        challenge_id: &str,
    ) -> impl std::future::Future<Output = Result<(), AuthError>> + Send + '_ {
        let result = self
            .values
            .lock()
            .map_err(|_| AuthError::Internal("test store lock failed".into()))
            .map(|mut values| {
                values.remove(challenge_id);
            });
        async move { result }
    }
}

/// Verifies phone OTP uses durable one-time state and issues normal credentials after completion.
#[tokio::test]
async fn phone_otp_completes_through_the_selected_provider() -> Result<(), AuthError> {
    let sender = CapturedPhoneSender::default();
    let auth = AuthConf::default()
        .passwordless_store(ChallengeStore::default())
        .method(PHONE_OTP, PhoneLogin::otp(PhoneAccounts, sender.clone()));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;

    let challenge = site
        .auth()
        .via(PHONE_OTP)
        .begin(PhoneNumber::new("+15551234567"), &[REPORTS])
        .await?;
    let token = challenge
        .token()
        .ok_or(AuthError::InvalidLoginState)?
        .to_owned();
    let code = sender
        .0
        .lock()
        .map_err(|_| AuthError::Internal("test sender lock failed".into()))?
        .clone()
        .ok_or(AuthError::InvalidCredential)?;
    let login = site
        .auth()
        .via(PHONE_OTP)
        .complete(PhoneOtp::new(token, code))
        .await?;

    assert!(!login.credentials().access().is_empty());
    Ok(())
}

/// Verifies replaying a consumed phone challenge fails without issuing another credential.
#[tokio::test]
async fn phone_otp_challenge_is_consumed_once() -> Result<(), AuthError> {
    let sender = CapturedPhoneSender::default();
    let auth = AuthConf::default()
        .passwordless_store(ChallengeStore::default())
        .method(PHONE_OTP, PhoneLogin::otp(PhoneAccounts, sender.clone()));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;

    let challenge = site
        .auth()
        .via(PHONE_OTP)
        .begin(PhoneNumber::new("+15551234567"), &[REPORTS])
        .await?;
    let token = challenge
        .token()
        .ok_or(AuthError::InvalidLoginState)?
        .to_owned();
    let code = sender
        .0
        .lock()
        .map_err(|_| AuthError::Internal("test sender lock failed".into()))?
        .clone()
        .ok_or(AuthError::InvalidCredential)?;

    site.auth()
        .via(PHONE_OTP)
        .complete(PhoneOtp::new(token.clone(), code.clone()))
        .await?;
    let replay = site
        .auth()
        .via(PHONE_OTP)
        .complete(PhoneOtp::new(token, code))
        .await;
    assert!(matches!(replay, Err(AuthError::InvalidCredential)));
    Ok(())
}

/// Verifies passwordless methods cannot build without durable challenge storage.
#[tokio::test]
async fn phone_otp_requires_a_durable_challenge_store() -> Result<(), AuthError> {
    let auth = AuthConf::default().method(
        PHONE_OTP,
        PhoneLogin::otp(PhoneAccounts, CapturedPhoneSender::default()),
    );
    let error = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .err()
        .ok_or(AuthError::InvalidCredential)?;

    assert!(error.to_string().contains("passwordless_store"));
    Ok(())
}

/// Verifies email OTP gives its code only to an application-owned sender.
#[cfg(feature = "email")]
#[tokio::test]
async fn email_otp_uses_an_application_sender() -> Result<(), AuthError> {
    let auth = AuthConf::default()
        .passwordless_store(ChallengeStore::default())
        .method(EMAIL_OTP, EmailLogin::otp(EmailAccounts, EmailSender));
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;

    let challenge = site
        .auth()
        .via(EMAIL_OTP)
        .begin(EmailAddress::new("user@example.com"), &[REPORTS])
        .await?;

    assert_eq!(challenge.kind(), LoginChallengeKind::Code);
    assert!(challenge.token().is_some());
    Ok(())
}

/// Verifies magic links reject a missing absolute callback URL during site construction.
#[cfg(feature = "email")]
#[tokio::test]
async fn magic_link_requires_an_explicit_callback_url() -> Result<(), AuthError> {
    const EMAIL_LINK: LoginMethod<EmailAddress, MagicLinkCallback> = LoginMethod::new("email-link");
    let auth = AuthConf::default()
        .passwordless_store(ChallengeStore::default())
        .method(EMAIL_LINK, EmailLogin::magic_link(EmailAccounts));
    let error = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .err()
        .ok_or(AuthError::InvalidCredential)?;

    assert!(error.to_string().contains("callback_url"));
    Ok(())
}

/// Verifies a stateless magic link needs no challenge store and completes once verified.
#[cfg(feature = "email")]
#[tokio::test]
async fn stateless_magic_link_completes_without_a_store() -> Result<(), AuthError> {
    const EMAIL_LINK: LoginMethod<EmailAddress, MagicLinkCallback> =
        LoginMethod::new("email-link-stateless");
    let auth = AuthConf::default().method(
        EMAIL_LINK,
        EmailLogin::magic_link(EmailAccounts)
            .callback_url("https://example.com/auth/email/callback")
            .stateless(),
    );
    let site = Site::build(config().auth(auth), bundles::Bundle::default())
        .await
        .map_err(auth_error)?;
    let challenge = site
        .auth()
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
        .via(EMAIL_LINK)
        .complete(MagicLinkCallback::new(token))
        .await?;
    assert!(!login.credentials().access().is_empty());
    Ok(())
}
