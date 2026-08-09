use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{borrow::Cow, collections::BTreeMap, fmt, str::FromStr};

use crate::auth::AudienceId;
use crate::middlewares::SlashPolicy;
use crate::routes::Methods;

use super::{ArgPart, ArgSpec, CallSpec, Callable, IntoArgPart, LayerSpec, ReturnSpec};

/// Canonical runtime identity for one registered Vyuh operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperationId(uuid::Uuid);

impl OperationId {
    pub(crate) fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for OperationId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

impl axum::extract::FromRequestParts<crate::Site> for OperationId {
    type Rejection = axum::http::StatusCode;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &crate::Site,
    ) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<Self>()
            .copied()
            .ok_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
    }
}

impl IntoArgPart for OperationId {
    fn into_arg_part() -> ArgPart {
        ArgPart::Ignore
    }
}

impl IntoArgPart for axum::Extension<OperationId> {
    fn into_arg_part() -> ArgPart {
        ArgPart::Ignore
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]

pub enum OperationKind {
    Cron,
    Periodic,
    PgNotify,
    Signal,
    Task,
    Command,
    Route,
    ApiDoc,
    Service,
    #[cfg(feature = "mcp")]
    McpTool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Operation {
    /// Canonical runtime identity assigned when the operation is created.
    pub id: OperationId,
    pub name: String,
    pub description: Option<String>,
    pub summary: Option<String>,
    /// Configurable string identifier emitted into OpenAPI documents.
    pub openapi_id: Option<String>,
    pub deprecated: bool,
    pub path: String,
    pub kind: OperationKind,
    pub methods: Methods,
    pub args: Vec<ArgSpec>,
    pub layers: Vec<LayerSpec>,
    pub returns: Vec<ReturnSpec>,
    pub tags: Vec<Cow<'static, str>>,
    pub conf: Option<serde_json::Value>,
    pub owner: Option<String>,
    pub hidden: bool,
    /// MCP exposure metadata for explicitly registered direct or route tools.
    #[cfg(feature = "mcp")]
    #[serde(skip)]
    pub(crate) mcp: Option<crate::mcp::McpToolConf>,
    /// Effective audience inherited from the owning bundle tree.
    pub(crate) audience: Option<AudienceId>,
    pub(crate) bundle_id: Option<uuid::Uuid>,
    pub(crate) slash_policy: Option<SlashPolicy>,
}

impl Operation {
    /// Validates and freezes deterministic scope metadata before runtime routing starts.
    pub(crate) fn normalize_authorization(&mut self) -> Result<(), String> {
        let mut count = 0usize;
        for argument in &mut self.args {
            count += normalize_part(&mut argument.part)?;
        }
        for layer in &mut self.layers {
            for part in &mut layer.parts {
                count += normalize_part(part)?;
            }
        }
        if count > 1 {
            return Err("an operation may declare only one Permit scope rule".to_string());
        }
        Ok(())
    }

    /// Returns normalized application authorization metadata for this operation.
    pub(crate) fn scope_requirement(&self) -> Option<super::specs::ScopeRequirement<'_>> {
        self.args
            .iter()
            .find_map(|argument| argument.part.authorization())
            .or_else(|| {
                self.layers
                    .iter()
                    .flat_map(|layer| layer.parts.iter())
                    .find_map(ArgPart::authorization)
            })
    }

    pub(crate) fn requires_auth(&self) -> bool {
        self.args
            .iter()
            .any(|argument| argument.part.requires_auth())
            || self
                .layers
                .iter()
                .flat_map(|layer| layer.parts.iter())
                .any(crate::callables::ArgPart::requires_auth)
    }

    /// Returns whether ordinary HTTP audience inheritance applies.
    pub(crate) fn requires_bundle_audience(&self) -> bool {
        #[cfg(feature = "mcp")]
        if self.kind == OperationKind::McpTool {
            return false;
        }
        self.requires_auth()
    }
    pub(crate) fn assign_bundle_id(&mut self, id: uuid::Uuid) {
        if self.bundle_id.is_none() {
            self.bundle_id = Some(id);
        }
    }

    /// Returns the audience required by this operation, when it is authenticated.
    pub fn audience(&self) -> Option<&str> {
        self.audience.as_ref().map(AudienceId::as_str)
    }

    pub(crate) fn audience_id(&self) -> Option<&AudienceId> {
        self.audience.as_ref()
    }

    pub(crate) fn nest(&mut self, path: &str) {
        self.path = format!("{}{}", path.trim_end_matches('/'), self.path);
    }

    pub fn with_owner<T: Into<String>>(mut self, owner: T) -> Self {
        self.owner = Some(owner.into());
        self
    }

    pub fn with_conf<T: Serialize>(mut self, conf: &T) -> Self {
        self.conf = serde_json::to_value(conf).ok();
        self
    }

    /// Extract individual HTTP methods from the Methods.
    /// Returns a list of method strings like "GET", "POST", etc.
    /// Handles combined filters (e.g., GET | POST).
    pub fn http_methods(&self) -> Vec<&'static str> {
        self.methods.to_vec()
    }

    pub fn from_api_doc(name: &str, path: &str) -> Self {
        Operation {
            id: OperationId::new(),
            name: name.to_string(),
            description: None,
            summary: None,
            openapi_id: None,
            deprecated: false,
            path: path.to_string(),
            methods: Methods::GET,
            kind: OperationKind::ApiDoc,
            args: Vec::new(),
            layers: Vec::new(),
            returns: Vec::new(),
            tags: Vec::new(),
            conf: None,
            owner: None,
            hidden: true,
            #[cfg(feature = "mcp")]
            mcp: None,
            audience: None,
            bundle_id: None,
            slash_policy: None,
        }
    }

    pub fn from_specs(kind: OperationKind, specs: &CallSpec) -> Self {
        let (summary, description) =
            Self::split_str_into_summary_description(specs.description.as_deref());
        Operation {
            id: OperationId::new(),
            name: specs.name.clone(),
            description,
            summary,
            openapi_id: Some(specs.name.clone()),
            deprecated: false,
            path: String::new(),
            methods: Methods::POST,
            kind,
            args: specs.args.clone(),
            layers: Vec::new(),
            returns: specs.returns.clone(),
            tags: Vec::new(),
            conf: None,
            owner: None,
            hidden: false,
            #[cfg(feature = "mcp")]
            mcp: None,
            audience: None,
            bundle_id: None,
            slash_policy: None,
        }
    }

    pub fn from_callable<T: Send + Sized>(kind: OperationKind, callable: Callable<T>) -> Self {
        Self::from_specs(kind, callable.inspect())
    }

    fn split_str_into_summary_description(
        content: Option<&str>,
    ) -> (Option<String>, Option<String>) {
        let s = match content {
            Some(s) => s,
            None => return (None, None),
        };
        let parts: Vec<&str> = s.splitn(2, "\n\n").collect();
        let summary = parts
            .first()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let description = if parts.len() > 1 {
            Some(parts[1].trim().to_string())
        } else {
            None
        };
        (summary, description)
    }
}

fn normalize_part(part: &mut ArgPart) -> Result<usize, String> {
    part.normalize_authorization()
        .map_err(|error| error.to_string())
}

/// Read-only access to the operations registered on one built site.
pub struct Operations<'a> {
    operations: &'a BTreeMap<OperationId, Operation>,
}

impl<'a> Operations<'a> {
    pub(crate) const fn new(operations: &'a BTreeMap<OperationId, Operation>) -> Self {
        Self { operations }
    }

    /// Lists every operation, including hidden framework operations.
    pub fn list(&self) -> impl ExactSizeIterator<Item = &'a Operation> + 'a {
        self.operations.values()
    }

    /// Finds one operation by its canonical runtime identity.
    pub fn find(&self, id: OperationId) -> Option<&'a Operation> {
        self.operations.get(&id)
    }
}
