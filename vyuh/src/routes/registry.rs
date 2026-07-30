//! Immutable route reversal and resolution for a built site.

use std::collections::{BTreeMap, HashMap};

use axum::http::{Method, Uri};
use matchit::Router;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};

use crate::{Operation, OperationId, OperationKind};

#[derive(Clone)]
struct RouteEntry {
    path: String,
}

/// Immutable indexes used to reverse and resolve registered routes.
pub(crate) struct RouteRegistry {
    names: BTreeMap<String, RouteEntry>,
    methods: HashMap<Method, Router<OperationId>>,
}

impl RouteRegistry {
    /// Builds route indexes from finalized operation metadata.
    pub(crate) fn build<'a>(
        operations: impl Iterator<Item = &'a Operation>,
        default_slash: crate::middlewares::SlashPolicy,
    ) -> Result<Self, String> {
        let mut registry = Self {
            names: BTreeMap::new(),
            methods: HashMap::new(),
        };
        for operation in operations.filter(|value| value.kind == OperationKind::Route) {
            registry.insert(operation, default_slash)?;
        }
        Ok(registry)
    }

    fn insert(
        &mut self,
        operation: &Operation,
        default_slash: crate::middlewares::SlashPolicy,
    ) -> Result<(), String> {
        self.names.insert(
            operation.name.clone(),
            RouteEntry {
                path: operation.path.clone(),
            },
        );
        for (method_name, _) in operation.methods.iter() {
            let method = Method::from_bytes(method_name.as_bytes())
                .map_err(|_| format!("invalid route method {method_name}"))?;
            self.insert_path(&method, &operation.path, operation.id)?;
            if let Some(alias) = slash_alias(operation, default_slash) {
                self.insert_path(&method, &alias, operation.id)?;
            }
        }
        Ok(())
    }

    fn insert_path(&mut self, method: &Method, path: &str, id: OperationId) -> Result<(), String> {
        self.methods
            .entry(method.clone())
            .or_default()
            .insert(path, id)
            .map(|_| ())
            .map_err(|error| format!("conflicting route {method} {path}: {error}"))
    }

    fn reverse_url(&self, name: &str, args: &[(&str, &str)]) -> Option<String> {
        let entry = self.names.get(name)?;
        let mut path = entry.path.clone();
        for (name, value) in args {
            let placeholder = format!("{{{name}}}");
            if path.contains(&placeholder) {
                let encoded = utf8_percent_encode(value, NON_ALPHANUMERIC).to_string();
                path = path.replace(&placeholder, &encoded);
            }
        }
        (!path.contains('{') && !path.contains('}')).then_some(path)
    }

    fn resolve_url(&self, method: &Method, url: &str) -> Option<OperationId> {
        let without_fragment = url.split('#').next()?;
        let uri = without_fragment.parse::<Uri>().ok()?;
        self.methods
            .get(method)?
            .at(uri.path())
            .ok()
            .map(|matched| *matched.value)
    }
}

fn slash_alias(
    operation: &Operation,
    default_slash: crate::middlewares::SlashPolicy,
) -> Option<String> {
    if operation.path == "/" || operation.path.contains("{*") {
        return None;
    }
    let policy = crate::middlewares::effective_slash(
        operation.slash_policy.unwrap_or(default_slash),
        operation,
    );
    match policy {
        crate::middlewares::SlashPolicy::Exact => None,
        _ => operation
            .path
            .strip_suffix('/')
            .map(str::to_owned)
            .or_else(|| Some(format!("{}/", operation.path))),
    }
}

/// Read-only route reversal and resolution for one built site.
pub struct Routes<'a> {
    registry: &'a RouteRegistry,
}

impl<'a> Routes<'a> {
    pub(crate) const fn new(registry: &'a RouteRegistry) -> Self {
        Self { registry }
    }

    /// Reverses a named route and percent-encodes supplied path arguments.
    pub fn reverse_url(&self, name: &str, args: &[(&str, &str)]) -> Option<String> {
        self.registry.reverse_url(name, args)
    }

    /// Resolves the operation dispatched for one HTTP method and URL.
    pub fn resolve_url(&self, method: Method, url: &str) -> Option<OperationId> {
        self.registry.resolve_url(&method, url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::Methods;

    fn route(name: &str, path: &str, methods: Methods) -> Operation {
        let mut operation = Operation::from_api_doc(name, path);
        operation.kind = OperationKind::Route;
        operation.name = name.to_string();
        operation.methods = methods;
        operation.hidden = false;
        operation
    }

    /// Verifies reversal encodes values, ignores extras, and requires every placeholder.
    #[test]
    fn reverse_url_validates_arguments() -> Result<(), String> {
        let operations = [route("detail", "/items/{id}", Methods::GET)];
        let registry =
            RouteRegistry::build(operations.iter(), crate::middlewares::SlashPolicy::Exact)?;
        assert_eq!(
            registry.reverse_url("detail", &[("id", "a/b c"), ("extra", "ignored")]),
            Some("/items/a%2Fb%20c".to_string())
        );
        assert_eq!(registry.reverse_url("detail", &[]), None);
        assert_eq!(registry.reverse_url("missing", &[]), None);
        Ok(())
    }

    /// Verifies resolution follows method, query, fragment, and slash-alias policy.
    #[test]
    fn resolve_url_matches_runtime_route() -> Result<(), String> {
        let operations = [route("detail", "/items/{id}", Methods::GET)];
        let registry =
            RouteRegistry::build(operations.iter(), crate::middlewares::SlashPolicy::Trim)?;
        let expected = operations.first().map(|operation| operation.id);
        assert_eq!(
            registry.resolve_url(&Method::GET, "/items/42/?view=full#section"),
            expected
        );
        assert_eq!(registry.resolve_url(&Method::POST, "/items/42"), None);
        assert_eq!(registry.resolve_url(&Method::GET, "not a URL"), None);
        Ok(())
    }

    /// Verifies conflicting patterns for one method fail during registry construction.
    #[test]
    fn conflicting_patterns_are_rejected() {
        let operations = [
            route("first", "/items/{id}", Methods::GET),
            route("second", "/items/{name}", Methods::GET),
        ];
        let result =
            RouteRegistry::build(operations.iter(), crate::middlewares::SlashPolicy::Exact);
        assert!(result.is_err());
    }
}
