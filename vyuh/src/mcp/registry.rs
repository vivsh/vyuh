//! Bundle-owned MCP tool registrations and single-service claims.

use std::collections::{BTreeMap, BTreeSet};

use crate::{Error, OperationId, callables::Callable};

use super::McpToolContext;

/// Invocation target retained independently from protocol service nodes.
#[derive(Clone)]
pub(crate) enum McpToolTarget {
    Route(OperationId),
    Direct(Callable<McpToolContext, Error>),
}

/// Direct callable plus the operation metadata inserted into the bundle.
pub(crate) struct McpDirectRegistration {
    pub(crate) operation: crate::Operation,
    pub(crate) callable: Callable<McpToolContext, Error>,
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

    /// Registers one opted-in HTTP route.
    pub(crate) fn register_route(&mut self, id: OperationId) {
        self.targets.insert(id, McpToolTarget::Route(id));
    }

    /// Registers one direct callable target.
    pub(crate) fn register_direct(
        &mut self,
        id: OperationId,
        callable: Callable<McpToolContext, Error>,
    ) {
        self.targets.insert(id, McpToolTarget::Direct(callable));
    }

    /// Absorbs registrations and existing service claims from a child bundle.
    pub(crate) fn merge(&mut self, other: Self) {
        self.targets.extend(other.targets);
        self.claimed.extend(other.claimed);
    }

    /// Claims every registration not already owned by another MCP service.
    pub(crate) fn claim_unclaimed(&mut self) -> Vec<OperationId> {
        let ids = self
            .targets
            .keys()
            .filter(|id| !self.claimed.contains(id))
            .copied()
            .collect::<Vec<_>>();
        self.claimed.extend(ids.iter().copied());
        ids
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
