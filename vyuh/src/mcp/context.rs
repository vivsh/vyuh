//! Direct MCP tool execution context and supported context extractors.

use crate::{
    Site,
    auth::{AuthUser, Permit, ScopeRule},
    callables::{CallError, DataBox, FromContextParts, HasSite, IntoDataBox},
};

/// Runtime context supplied to direct MCP tool callables.
///
/// It exposes the built site and authenticated application identity while the
/// semantic JSON payload remains the callable's final `Data<T>` argument.
pub struct McpToolContext {
    site: Site,
    payload: DataBox,
    user: Option<AuthUser>,
}

impl McpToolContext {
    /// Creates a context after transport authentication and payload decoding.
    pub(crate) fn new(site: Site, payload: DataBox, user: Option<AuthUser>) -> Self {
        Self {
            site,
            payload,
            user,
        }
    }
}

impl HasSite for McpToolContext {
    fn site(&self) -> &Site {
        &self.site
    }
}

impl IntoDataBox for McpToolContext {
    fn into_data_box(self) -> DataBox {
        self.payload
    }
}

impl FromContextParts<McpToolContext> for AuthUser {
    fn from_context_parts(context: &McpToolContext) -> Result<Self, CallError> {
        context.user.clone().ok_or(CallError::Unauthorized)
    }
}

impl<R: ScopeRule> FromContextParts<McpToolContext> for Permit<R> {
    fn from_context_parts(context: &McpToolContext) -> Result<Self, CallError> {
        let user = AuthUser::from_context_parts(context)?;
        if !R::EXPR.allows(&user) {
            return Err(CallError::Forbidden);
        }
        Ok(Permit::new(user))
    }
}
