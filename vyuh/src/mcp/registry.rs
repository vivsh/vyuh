//! Bundle-owned MCP tool registrations and single-service claims.

use std::collections::{BTreeMap, BTreeSet};

use crate::{Error, OperationId, callables::Callable};

use super::{McpToolConf, McpToolContext};

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
