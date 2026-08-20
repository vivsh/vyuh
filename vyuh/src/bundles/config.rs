//! Declarative configuration contributed by one bundle.

use std::borrow::Cow;

use crate::{
    auth::{Audience, AuthProvider, ProviderDefinition, ProviderDefinitionInner},
    tasks::TaskLaneConf,
};

#[cfg(feature = "mcp")]
use super::McpConf;
use super::OpenApiConf;

/// Returns an empty bundle configuration builder.
pub fn conf() -> BundleConf {
    BundleConf::default()
}

/// Configuration that travels with a bundle and is finalized during site construction.
#[derive(Clone, Default)]
pub struct BundleConf {
    pub(crate) audience: Option<Audience>,
    pub(crate) tags: Vec<Cow<'static, str>>,
    pub(crate) task_lanes: Vec<TaskLaneConf>,
    pub(crate) providers: Vec<ProviderDefinitionInner>,
    pub(crate) openapi: Vec<OpenApiConf>,
    #[cfg(feature = "mcp")]
    pub(crate) mcp: Option<McpConf>,
    pub(crate) errors: Vec<String>,
}

impl BundleConf {
    /// Declares the audience inherited by authenticated operations in this bundle tree.
    pub fn audience(mut self, audience: Audience) -> Self {
        if self.audience.replace(audience).is_some() {
            self.errors
                .push("a bundle configuration may declare one audience".into());
        }
        self
    }

    /// Adds documentation tags to operations in this bundle tree.
    pub fn tags(mut self, tags: impl IntoIterator<Item = impl Into<Cow<'static, str>>>) -> Self {
        self.tags.extend(tags.into_iter().map(Into::into));
        self
    }

    /// Contributes one named non-default task-lane configuration.
    pub fn task_lane(mut self, lane: TaskLaneConf) -> Self {
        self.task_lanes.push(lane);
        self
    }

    /// Contributes one audience-scoped provider to the central authentication registry.
    pub fn auth<D: ProviderDefinition>(mut self, name: AuthProvider, provider: D) -> Self {
        let mut definition = provider.define();
        definition.name = name;
        self.providers.push(definition);
        self
    }

    /// Registers one OpenAPI aggregate rooted at this bundle.
    pub fn openapi(mut self, conf: OpenApiConf) -> Self {
        self.openapi.push(conf);
        self
    }

    /// Registers the MCP aggregate rooted at this bundle.
    #[cfg(feature = "mcp")]
    pub fn mcp(mut self, conf: McpConf) -> Self {
        if self.mcp.replace(conf).is_some() {
            self.errors
                .push("a bundle configuration may declare one MCP endpoint".into());
        }
        self
    }

    pub(crate) fn merge(&mut self, mut other: Self) {
        if let Some(audience) = other.audience.take()
            && self.audience.replace(audience).is_some()
        {
            self.errors
                .push("a bundle configuration may declare one audience".into());
        }
        self.tags.append(&mut other.tags);
        self.task_lanes.append(&mut other.task_lanes);
        self.providers.append(&mut other.providers);
        self.openapi.append(&mut other.openapi);
        #[cfg(feature = "mcp")]
        if let Some(mcp) = other.mcp.take() {
            if self.mcp.replace(mcp).is_some() {
                self.errors
                    .push("a bundle configuration may declare one MCP endpoint".into());
            }
        }
        self.errors.append(&mut other.errors);
    }

    pub(crate) fn take_openapi(&mut self) -> Vec<OpenApiConf> {
        std::mem::take(&mut self.openapi)
    }

    #[cfg(feature = "mcp")]
    pub(crate) fn take_mcp(&mut self) -> Option<McpConf> {
        self.mcp.take()
    }
}
