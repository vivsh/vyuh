//! Canonical private cache keys and validated provider scopes.

use std::sync::Arc;

use super::{CacheError, CacheName};

pub(crate) const MAX_KEY_BYTES: usize = 512;
pub(crate) const MAX_NAMESPACE_BYTES: usize = 128;
pub(crate) const MAX_NAMESPACE_DEPTH: usize = 8;
pub(crate) const MAX_BULK_ITEMS: usize = 256;

/// An opaque canonical key supplied to a cache provider.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CacheKey(Arc<str>);

impl CacheKey {
    /// Returns the canonical backend key after Vyuh has applied its scope.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An opaque provider-wide key scope used only for `clear`.
#[derive(Clone, Debug)]
pub struct CacheScope(Arc<str>);

impl CacheScope {
    /// Returns the provider-wide canonical prefix that may be cleared.
    pub fn prefix(&self) -> &str {
        &self.0
    }
}

pub(crate) fn validate_provider_name(value: &str) -> Result<(), CacheError> {
    let valid = !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'));
    valid.then_some(()).ok_or(CacheError::InvalidProviderName)
}

pub(crate) fn cache_key(
    provider: CacheName,
    namespaces: &[String],
    key: &str,
) -> Result<CacheKey, CacheError> {
    validate_key(key, MAX_KEY_BYTES)?;
    if namespaces.len() > MAX_NAMESPACE_DEPTH {
        return Err(CacheError::InvalidKey);
    }
    let mut output = provider_scope(provider);
    output.push_str("ns:");
    output.push_str(&namespaces.len().to_string());
    output.push(':');
    for namespace in namespaces {
        validate_key(namespace, MAX_NAMESPACE_BYTES)?;
        append_part(&mut output, namespace);
    }
    output.push_str("key:");
    append_part(&mut output, key);
    Ok(CacheKey(Arc::from(output)))
}

pub(crate) fn cache_scope(provider: CacheName) -> CacheScope {
    CacheScope(Arc::from(provider_scope(provider)))
}

fn provider_scope(provider: CacheName) -> String {
    let mut output = String::from("vyuh-cache:v1:provider:");
    append_part(&mut output, provider.as_str());
    output.push(':');
    output
}

fn append_part(output: &mut String, value: &str) {
    output.push_str(&value.len().to_string());
    output.push(':');
    output.push_str(value);
    output.push(':');
}

fn validate_key(value: &str, limit: usize) -> Result<(), CacheError> {
    let valid = !value.is_empty()
        && value.len() <= limit
        && !value.bytes().any(|byte| byte.is_ascii_control());
    valid.then_some(()).ok_or(CacheError::InvalidKey)
}
