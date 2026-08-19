//! Bundle-owned MCP tool registrations and single-service claims.

use std::collections::{BTreeMap, BTreeSet};

use crate::{Error, OperationId, callables::Callable};

use super::{McpError, McpResource, McpToolConf, McpToolContext, resources::ResourceDefinition};

/// Direct MCP invocation target retained independently from service nodes.
#[derive(Clone)]
pub(crate) struct McpToolTarget {
    pub(crate) callable: Callable<McpToolContext, Error>,
    pub(crate) conf: McpToolConf,
}

/// Direct callable plus the operation metadata inserted into the bundle.
pub(crate) struct McpDirectRegistration {
    pub(crate) operation: crate::Operation,
    pub(crate) callable: Callable<McpToolContext, Error>,
    pub(crate) conf: McpToolConf,
}

/// MCP registrations collected while bundle parts are composed.
pub(crate) struct McpToolRegistry {
    targets: BTreeMap<OperationId, McpToolTarget>,
    claimed: BTreeSet<OperationId>,
}

impl McpToolRegistry {
    /// Creates an empty registry for one bundle tree.
    pub(crate) fn new() -> Self {
        Self {
            targets: BTreeMap::new(),
            claimed: BTreeSet::new(),
        }
    }

    /// Registers one direct callable target.
    pub(crate) fn register_direct(
        &mut self,
        id: OperationId,
        callable: Callable<McpToolContext, Error>,
        conf: McpToolConf,
    ) {
        self.targets.insert(id, McpToolTarget { callable, conf });
    }

    /// Absorbs registrations and existing service claims from a child bundle.
    pub(crate) fn merge(&mut self, other: Self) {
        self.targets.extend(other.targets);
        self.claimed.extend(other.claimed);
    }

    /// Clears finalized ownership before rebuilding a composed site.
    pub(crate) fn clear_claims(&mut self) {
        self.claimed.clear();
    }

    /// Records the finalized service ownership of direct tool registrations.
    pub(crate) fn claim(&mut self, ids: impl IntoIterator<Item = OperationId>) {
        self.claimed.extend(ids);
    }

    /// Returns the target for a claimed operation.
    pub(crate) fn target(&self, id: OperationId) -> Option<&McpToolTarget> {
        self.targets.get(&id)
    }

    /// Returns registrations that have not been assigned to a service.
    pub(crate) fn unclaimed(&self) -> impl Iterator<Item = OperationId> + '_ {
        self.targets
            .keys()
            .filter(|id| !self.claimed.contains(id))
            .copied()
    }
}

/// Bundle-owned static MCP resource registrations and single-service claims.
pub(crate) struct McpResourceRegistry {
    resources: BTreeMap<String, RegisteredResource>,
    claimed: BTreeSet<String>,
}

struct RegisteredResource {
    name: String,
    owner: uuid::Uuid,
    resource: McpResource,
}

impl McpResourceRegistry {
    /// Creates an empty registry for one bundle tree.
    pub(crate) fn new() -> Self {
        Self {
            resources: BTreeMap::new(),
            claimed: BTreeSet::new(),
        }
    }

    /// Registers one immutable resource owned by a bundle.
    pub(crate) fn register(
        &mut self,
        name: String,
        resource: McpResource,
        owner: uuid::Uuid,
    ) -> Result<(), McpError> {
        if name.trim().is_empty() {
            return Err(McpError::Config(
                "MCP resource names cannot be empty".to_string(),
            ));
        }
        resource.validate()?;
        let uri = resource.uri().to_string();
        if self.resources.contains_key(&uri) {
            return Err(McpError::Config(format!(
                "duplicate MCP resource URI '{uri}'"
            )));
        }
        self.resources.insert(
            uri,
            RegisteredResource {
                name,
                owner,
                resource,
            },
        );
        Ok(())
    }

    /// Absorbs child registrations while preserving global URI uniqueness.
    pub(crate) fn merge(&mut self, other: Self) -> Result<(), McpError> {
        for (_, resource) in other.resources {
            self.register(resource.name, resource.resource, resource.owner)?;
        }
        Ok(())
    }

    /// Clears finalized ownership before rebuilding a composed site.
    pub(crate) fn clear_claims(&mut self) {
        self.claimed.clear();
    }

    /// Lists resource URI and bundle ownership for service assignment.
    pub(crate) fn owners(&self) -> impl Iterator<Item = (&str, uuid::Uuid)> {
        self.resources
            .iter()
            .map(|(uri, resource)| (uri.as_str(), resource.owner))
    }

    /// Records finalized ownership for one service's resources.
    pub(crate) fn claim(&mut self, uris: impl IntoIterator<Item = String>) {
        self.claimed.extend(uris);
    }

    /// Resolves static definitions for a single service.
    pub(crate) fn definitions(
        &self,
        uris: &[String],
    ) -> Result<BTreeMap<String, ResourceDefinition>, McpError> {
        let mut definitions = BTreeMap::new();
        for uri in uris {
            let resource = self.resources.get(uri).ok_or_else(|| {
                McpError::Config(format!("MCP resource '{uri}' is missing from the registry"))
            })?;
            definitions.insert(
                uri.clone(),
                resource.resource.definition(resource.name.clone()),
            );
        }
        Ok(definitions)
    }

    /// Returns registrations that have not been assigned to a service.
    pub(crate) fn unclaimed(&self) -> impl Iterator<Item = &str> {
        self.resources
            .keys()
            .filter(|uri| !self.claimed.contains(*uri))
            .map(String::as_str)
    }
}
