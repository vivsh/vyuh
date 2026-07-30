//! Immutable provider, selector, and login-method startup indexes.

use std::collections::BTreeMap;

use super::{LoginDefinitionInner, LoginMethodId, ProviderId, ProviderRuntime};
use crate::auth::AuthError;

#[derive(Clone, Default)]
pub(super) struct RuntimeIndexes {
    pub(super) providers: BTreeMap<ProviderId, usize>,
    pub(super) login_methods: BTreeMap<LoginMethodId, usize>,
    pub(super) access: BTreeMap<String, usize>,
    pub(super) refresh: BTreeMap<String, usize>,
    pub(super) access_order: Vec<usize>,
}

impl RuntimeIndexes {
    pub(super) fn build(
        providers: &[ProviderRuntime],
        login_methods: &[LoginDefinitionInner],
    ) -> Result<Self, AuthError> {
        let mut output = Self::default();
        for (position, provider) in providers.iter().enumerate() {
            output.insert_provider(provider, position)?;
        }
        for (position, method) in login_methods.iter().enumerate() {
            output
                .login_methods
                .insert(LoginMethodId::new(method.name)?, position);
        }
        Ok(output)
    }

    pub(super) fn insert_provider(
        &mut self,
        provider: &ProviderRuntime,
        position: usize,
    ) -> Result<(), AuthError> {
        let metadata = provider.openapi();
        if metadata.id != provider.id().as_str() {
            return Err(AuthError::InvalidProviderConfig(
                "provider runtime metadata ID does not match its registry ID".into(),
            ));
        }
        let access = provider.access_location().selector();
        let refresh = provider
            .refresh_location()
            .map(|location| location.selector());
        self.validate_selectors(&access, refresh.as_deref())?;
        self.providers.insert(provider.id().clone(), position);
        self.access.insert(access, position);
        self.access_order.push(position);
        if let Some(refresh) = refresh {
            self.refresh.insert(refresh, position);
        }
        Ok(())
    }

    fn validate_selectors(&self, access: &str, refresh: Option<&str>) -> Result<(), AuthError> {
        if self.access.contains_key(access) || self.refresh.contains_key(access) {
            return Err(AuthError::AmbiguousProvider(access.into()));
        }
        if let Some(refresh) = refresh
            && (self.access.contains_key(refresh) || self.refresh.contains_key(refresh))
        {
            return Err(AuthError::AmbiguousProvider(refresh.into()));
        }
        Ok(())
    }
}
