//! Named identity-proof methods used before credential issuance.

mod challenge;
mod mfa;
mod model;
#[cfg(feature = "oidc")]
mod oidc;
mod password;
mod runtime;

pub use challenge::{LoginChallenge, LoginChallengeKind, LoginStateStore};
#[doc(hidden)]
pub use mfa::ComposedMfaLogin;
pub use mfa::{MfaLogin, MfaMethod, MfaResponse, MfaVerifier};
pub use model::{BasicCredentials, LoginMethod, NoChallenge, PasswordCredentials, PresentedSecret};
#[cfg(feature = "oidc")]
pub use oidc::{OidcCallback, OidcIdentity, OidcLogin, OidcStart, OidcUserMapper};
pub use password::{BasicLogin, PasswordLogin, PasswordVerifier};
pub use runtime::LoginAuth;
#[doc(hidden)]
pub use runtime::LoginCompletion;

pub(crate) use challenge::{ChallengeCodec, LoginStateStoreRuntime, SealedLoginState};
pub(crate) use model::{
    BoxLoginInput, LoginDefinitionInner, LoginMethodId, LoginProviderDefinition, LoginProviderKind,
};
pub(crate) use runtime::{ErasedLoginRuntime, LoginTarget, VerifiedLogin, select};
