//! Source-owned cache provider configuration.

use std::{collections::BTreeSet, fmt, sync::Arc};

use super::{CacheError, CacheProvider, MemoryCache, key::validate_provider_name};

/// A reusable name for one configured cache provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CacheName(&'static str);

impl CacheName {
    /// Declares a cache provider name. It is validated when the site is built.
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// Returns the declared provider name.
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

/// The bounded in-process cache selected by [`CacheConf::default`].
pub const DEFAULT_CACHE: CacheName = CacheName::new("default");

#[derive(Clone)]
pub(crate) struct CacheDefinition {
    pub(crate) name: CacheName,
    pub(crate) provider: Arc<dyn CacheProvider>,
}

/// Source-owned configuration for the site's immutable cache registry.
#[derive(Clone)]
pub struct CacheConf {
    providers: Vec<CacheDefinition>,
    default_provider: Option<CacheName>,
}

impl Default for CacheConf {
    fn default() -> Self {
        Self::empty()
            .provider(DEFAULT_CACHE, MemoryCache::default())
            .default_provider(DEFAULT_CACHE)
    }
}

impl fmt::Debug for CacheConf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = self
            .providers
            .iter()
            .map(|definition| definition.name.as_str())
            .collect::<Vec<_>>();
        formatter
            .debug_struct("CacheConf")
            .field("providers", &names)
            .field(
                "default_provider",
                &self.default_provider.map(CacheName::as_str),
            )
            .finish()
    }
}

impl CacheConf {
    /// Creates an empty configuration that requires provider and default selection.
    pub fn empty() -> Self {
        Self {
            providers: Vec::new(),
            default_provider: None,
        }
    }

    /// Registers one named asynchronous cache provider.
    pub fn provider<P>(mut self, name: CacheName, provider: P) -> Self
    where
        P: CacheProvider,
    {
        self.providers.push(CacheDefinition {
            name,
            provider: Arc::new(provider),
        });
        self
    }

    /// Selects the provider used by [`crate::Site::cache`].
    pub fn default_provider(mut self, name: CacheName) -> Self {
        self.default_provider = Some(name);
        self
    }

    pub(crate) fn definitions(&self) -> &[CacheDefinition] {
        &self.providers
    }

    pub(crate) fn default_name(&self) -> Option<CacheName> {
        self.default_provider
    }

    /// Validates provider names, uniqueness, provider state, and default selection.
    pub(crate) fn validate(&self) -> Result<(), CacheError> {
        let mut names = BTreeSet::new();
        for definition in &self.providers {
            validate_provider_name(definition.name.as_str())?;
            if !names.insert(definition.name.as_str()) {
                return Err(CacheError::DuplicateProvider);
            }
            definition.provider.validate()?;
        }
        let default = self
            .default_provider
            .ok_or(CacheError::MissingDefaultProvider)?;
        names
            .contains(default.as_str())
            .then_some(())
            .ok_or(CacheError::InvalidDefaultProvider)
    }
}
