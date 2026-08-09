//! Named identity-proof methods used before credential issuance.

mod challenge;
#[cfg(feature = "federated")]
mod federated;
mod mfa;
mod model;
mod password;
mod passwordless;
mod runtime;

pub use challenge::{LoginChallenge, LoginChallengeKind, LoginStateStore};
#[cfg(feature = "federated")]
pub use federated::{
    FederatedCallback, FederatedIdentity, FederatedLogin, FederatedProvider, FederatedStart,
    FederatedUserMapper,
};
#[doc(hidden)]
pub use mfa::ComposedMfaLogin;
pub use mfa::{MfaLogin, MfaMethod, MfaResponse, MfaVerifier};
pub use model::{BasicCredentials, LoginMethod, NoChallenge, PasswordCredentials, PresentedSecret};
pub use password::{BasicLogin, PasswordLogin, PasswordVerifier};
#[cfg(feature = "email")]
pub use passwordless::{EmailAddress, EmailLogin, EmailLoginResolver, EmailOtp, MagicLinkCallback};
#[cfg(feature = "email")]
pub use passwordless::{EmailOtpMessage, EmailOtpSender};
pub use passwordless::{
    PasswordlessAttempt, PasswordlessChallenge, PasswordlessStart, PasswordlessStore, PhoneLogin,
    PhoneLoginResolver, PhoneNumber, PhoneOtp, PhoneOtpMessage, PhoneOtpSender,
};
pub use runtime::LoginAuth;

pub(crate) use challenge::{ChallengeCodec, LoginStateStoreRuntime, SealedLoginState};
pub(crate) use model::{
    BoxLoginInput, LoginDefinitionInner, LoginMethodId, LoginProviderDefinition,
    LoginRuntimeDefinition,
};
pub(crate) use passwordless::PasswordlessStoreRuntime;
pub(crate) use runtime::{ErasedLoginRuntime, LoginCompletion, LoginTarget, VerifiedLogin, select};
