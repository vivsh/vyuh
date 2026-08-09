//! Immutable provider, selector, and login-method startup indexes.

use std::collections::BTreeMap;

use super::{
    AudienceId, LoginDefinitionInner, LoginMethodId, ProviderId, ProviderRuntime,
    contract::ProviderAudienceSet,
};
use crate::auth::AuthError;

#[derive(Clone, Default)]
pub(super) struct RuntimeIndexes {
    pub(super) providers: BTreeMap<ProviderId, usize>,
    pub(super) login_methods: BTreeMap<LoginMethodId, usize>,
    access: AccessIndex,
}

#[derive(Clone, Default)]
struct AccessIndex {
    access: BTreeMap<AudienceId, BTreeMap<String, usize>>,
    unrestricted_access: BTreeMap<String, usize>,
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
        self.providers.insert(provider.id().clone(), position);
        self.access.insert(provider.audiences(), access, position)?;
        Ok(())
    }

    /// Iterates only providers statically eligible for one local route audience.
    pub(super) fn access_for(&self, audience: &AudienceId) -> impl Iterator<Item = &usize> {
        self.access
            .access
            .get(audience)
            .into_iter()
            .flat_map(BTreeMap::values)
            .chain(self.access.unrestricted_access.values())
    }

    /// Returns the number of immutable audience-selector dispatch bindings.
    pub(super) fn access_binding_count(&self) -> usize {
        self.access.binding_count()
    }
}

impl AccessIndex {
    /// Adds one selector over finite or unrestricted audience coverage.
    fn insert(
        &mut self,
        audiences: &ProviderAudienceSet,
        selector: String,
        position: usize,
    ) -> Result<(), AuthError> {
        match audiences.restricted() {
            None => self.insert_unrestricted(selector, position),
            Some(audiences) => {
                for audience in audiences {
                    self.insert_restricted(audience.clone(), selector.clone(), position)?;
                }
                Ok(())
            }
        }
    }

    fn binding_count(&self) -> usize {
        self.unrestricted_access.len() + self.access.values().map(BTreeMap::len).sum::<usize>()
    }

    /// Adds a wildcard selector only when no exact or wildcard binding overlaps it.
    fn insert_unrestricted(&mut self, selector: String, position: usize) -> Result<(), AuthError> {
        if self.unrestricted_access.contains_key(&selector) {
            return Err(AuthError::AmbiguousProvider(format!(
                "{selector} for unrestricted audiences"
            )));
        }
        if let Some(audience) = self
            .access
            .iter()
            .find_map(|(audience, values)| values.contains_key(&selector).then_some(audience))
        {
            return Err(AuthError::AmbiguousProvider(format!(
                "{selector} for audience '{}'",
                audience.as_str()
            )));
        }
        self.unrestricted_access.insert(selector, position);
        Ok(())
    }

    /// Adds one exact audience-selector binding while rejecting wildcard overlap.
    fn insert_restricted(
        &mut self,
        audience: AudienceId,
        selector: String,
        position: usize,
    ) -> Result<(), AuthError> {
        let audience_name = audience.as_str().to_owned();
        if self.unrestricted_access.contains_key(&selector) {
            return Err(AuthError::AmbiguousProvider(format!(
                "{selector} for audience '{audience_name}'"
            )));
        }
        let values = self.access.entry(audience).or_default();
        if values.contains_key(&selector) {
            return Err(AuthError::AmbiguousProvider(format!(
                "{selector} for audience '{audience_name}'"
            )));
        }
        values.insert(selector, position);
        Ok(())
    }
}

/// Validates descriptor-level selector scopes before network providers initialize.
pub(super) fn validate_access_selectors(
    values: impl IntoIterator<Item = (String, ProviderAudienceSet)>,
) -> Result<(), AuthError> {
    let mut index = AccessIndex::default();
    for (position, (selector, audiences)) in values.into_iter().enumerate() {
        index.insert(&audiences, selector, position)?;
    }
    Ok(())
}
