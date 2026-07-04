use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

use crate::middlewares::SlashPolicy;
use crate::routes::Methods;

use super::{ArgSpec, CallSpec, Callable, LayerSpec, ReturnSpec};

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
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Operation {
    pub id: uuid::Uuid,
    pub name: String,
    pub description: Option<String>,
    pub summary: Option<String>,
    pub operation_id: Option<String>,
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
    pub(crate) bundle_id: Option<uuid::Uuid>,
    pub(crate) slash_policy: Option<SlashPolicy>,
}

impl Operation {
    pub(crate) fn assign_bundle_id(&mut self, id: uuid::Uuid) {
        if self.bundle_id.is_none() {
            self.bundle_id = Some(id);
        }
    }

    pub(crate) fn set_bundle_id(&mut self, id: uuid::Uuid) {
        self.bundle_id = Some(id);
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
        return self.methods.to_vec();
    }

    pub fn from_api_doc(name: &str, path: &str) -> Self {
        Operation {
            id: uuid::Uuid::new_v4(),
            name: name.to_string(),
            description: None,
            summary: None,
            operation_id: None,
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
            bundle_id: None,
            slash_policy: None,
        }
    }

    pub fn from_specs(kind: OperationKind, specs: &CallSpec) -> Self {
        let (summary, description) =
            Self::split_str_into_summary_description(specs.description.as_deref());
        Operation {
            id: uuid::Uuid::new_v4(),
            name: specs.name.clone(),
            description: description,
            summary: summary,
            operation_id: Some(specs.name.clone()),
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
            bundle_id: None,
            slash_policy: None,
        }
    }

    pub fn from_callable<T: Send>(kind: OperationKind, callable: Callable<T>) -> Self
    where
        T: Sized,
    {
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
            .get(0)
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
