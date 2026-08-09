//! Named identity-proof methods used before credential issuance.

mod challenge;
#[cfg(feature = "federated")]
mod federated;
mod mfa;
mod model;
mod password;
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
pub use runtime::LoginAuth;

pub(crate) use challenge::{ChallengeCodec, LoginStateStoreRuntime, SealedLoginState};
pub(crate) use model::{
    BoxLoginInput, LoginDefinitionInner, LoginMethodId, LoginProviderDefinition,
    LoginRuntimeDefinition,
};
pub(crate) use runtime::{ErasedLoginRuntime, LoginCompletion, LoginTarget, VerifiedLogin, select};
