//! Two-phase authentication runtime construction.

use std::{path::Path, sync::Arc};

use super::{
    AuthConf, AuthError, AuthMetrics, Authenticator, LoginDefinitionInner, SecretRing,
    indexes::RuntimeIndexes,
    registry::{
        PreparedProvider, prepare_provider, validate_definitions, validate_login_definitions,
    },
};
use crate::auth::{AudienceId, ChallengeCodec, LoginStateStoreRuntime, ProviderDefinitionInner};

struct PreparedAuth {
    providers: Vec<PreparedProvider>,
    login_methods: Vec<LoginDefinitionInner>,
    login_state_store: Option<LoginStateStoreRuntime>,
    passwordless_store: Option<crate::auth::PasswordlessStoreRuntime>,
    secrets: SecretRing,
    challenge_codec: ChallengeCodec,
    default_audience: Option<AudienceId>,
    metric_providers: Vec<String>,
    metric_methods: Vec<String>,
}

pub(super) async fn authenticator(
    conf: &AuthConf,
    secret: &str,
    fallbacks: &[String],
    project_dir: &Path,
) -> Result<Authenticator, AuthError> {
    let conf = conf.clone();
    let secret = secret.to_owned();
    let fallbacks = fallbacks.to_vec();
    let project_dir = project_dir.to_path_buf();
    let prepared =
        tokio::task::spawn_blocking(move || prepare(&conf, &secret, &fallbacks, &project_dir))
            .await
            .map_err(|_| AuthError::Internal("authentication startup task failed".into()))??;
    finish(prepared).await
}

/// Resolves blocking key sources and prepares login methods before async providers.
fn prepare(
    conf: &AuthConf,
    secret: &str,
    fallbacks: &[String],
    project_dir: &Path,
) -> Result<PreparedAuth, AuthError> {
    let definitions = conf.definitions();
    let login_methods = conf.login_definitions();
    validate_definitions(&definitions)?;
    validate_login_definitions(&login_methods)?;
    validate_passwordless_store(conf)?;
    let secrets = SecretRing::new(secret, fallbacks, project_dir, conf.minimum_secret_length())?;
    for method in &login_methods {
        method.runtime.prepare(&secrets)?;
    }
    let default_audience = conf.default_audience_id()?;
    let challenge_codec = ChallengeCodec::new(&secrets)?;
    let providers = prepare_providers(definitions, &secrets, default_audience.clone())?;
    Ok(PreparedAuth {
        metric_providers: provider_names(&providers),
        metric_methods: method_names(&login_methods),
        providers,
        login_methods,
        login_state_store: conf.login_state_store_runtime(),
        passwordless_store: conf.passwordless_store_runtime(),
        secrets,
        challenge_codec,
        default_audience,
    })
}

/// Ensures passwordless methods cannot start without durable proof storage.
fn validate_passwordless_store(conf: &AuthConf) -> Result<(), AuthError> {
    if conf.requires_passwordless_store() && conf.passwordless_store_runtime().is_none() {
        return Err(AuthError::InvalidProviderConfig(
            "passwordless login requires AuthConf::passwordless_store".into(),
        ));
    }
    Ok(())
}

/// Builds network-backed providers sequentially and freezes all runtime indexes.
async fn finish(prepared: PreparedAuth) -> Result<Authenticator, AuthError> {
    initialize_methods(&prepared.login_methods).await?;
    let mut providers = Vec::with_capacity(prepared.providers.len());
    for provider in prepared.providers {
        providers.push(provider.finish().await?);
    }
    let indexes = RuntimeIndexes::build(&providers, &prepared.login_methods)?;
    Ok(Authenticator {
        providers: Arc::new(providers),
        login_methods: Arc::new(prepared.login_methods),
        indexes: Arc::new(indexes),
        login_state_store: prepared.login_state_store,
        passwordless_store: prepared.passwordless_store,
        secrets: prepared.secrets,
        challenge_codec: prepared.challenge_codec,
        metrics: Arc::new(AuthMetrics::new(
            prepared.metric_providers,
            prepared.metric_methods,
        )),
        default_audience: prepared.default_audience,
    })
}

/// Initializes network-backed login methods before request handling begins.
async fn initialize_methods(values: &[LoginDefinitionInner]) -> Result<(), AuthError> {
    for value in values {
        value.runtime.initialize().await?;
    }
    Ok(())
}

fn prepare_providers(
    definitions: Vec<ProviderDefinitionInner>,
    secrets: &SecretRing,
    default_audience: Option<AudienceId>,
) -> Result<Vec<PreparedProvider>, AuthError> {
    definitions
        .into_iter()
        .map(|value| prepare_provider(value, secrets, default_audience.clone()))
        .collect()
}

fn provider_names(values: &[PreparedProvider]) -> Vec<String> {
    values
        .iter()
        .map(|value| match value {
            PreparedProvider::Ready(runtime) => runtime.id().to_string(),
            #[cfg(feature = "oauth")]
            PreparedProvider::OAuth(id, _) => id.to_string(),
            #[cfg(feature = "id-token")]
            PreparedProvider::IdToken(id, _) => id.to_string(),
        })
        .collect()
}

fn method_names(values: &[LoginDefinitionInner]) -> Vec<String> {
    values.iter().map(|value| value.name.to_owned()).collect()
}
