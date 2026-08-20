use schemars::JsonSchema;
use serde::Serialize;

use crate::{
    Operation, OperationKind, Site,
    callables::{ArgPart, LayerSpec},
    middlewares::HttpConf,
};

#[derive(Debug, Clone)]
pub(crate) struct MiddlewareInfo {
    pub name: String,
    pub scope: &'static str,
    pub description: Option<String>,
    pub request_parts: Vec<MiddlewarePart>,
    pub settings: Vec<MiddlewareSetting>,
}

#[derive(Debug, Clone)]
pub(crate) struct MiddlewarePart {
    pub name: String,
    pub description: Option<String>,
    pub part: ArgPart,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub(crate) struct MiddlewareSetting {
    pub key: String,
    pub value: String,
}

pub(crate) fn operation_middleware(site: &Site, op: &Operation) -> Vec<MiddlewareInfo> {
    let mut middleware = Vec::with_capacity(op.layers.len() + 8);
    middleware.extend(op.layers.iter().map(layer_info));
    if op.kind == OperationKind::Route {
        middleware.extend(site_policies(&site.conf().http));
    }
    middleware
}

fn layer_info(layer: &LayerSpec) -> MiddlewareInfo {
    MiddlewareInfo {
        name: layer.name.clone(),
        scope: "operation",
        description: layer.description.clone(),
        request_parts: layer
            .parts
            .iter()
            .map(|part| layer_part(layer, part))
            .collect(),
        settings: Vec::new(),
    }
}

fn layer_part(layer: &LayerSpec, part: &ArgPart) -> MiddlewarePart {
    MiddlewarePart {
        name: layer.name.clone(),
        description: layer.description.clone(),
        part: part.clone(),
    }
}

fn site_policies(http: &HttpConf) -> Vec<MiddlewareInfo> {
    let mut policies = Vec::with_capacity(8);
    if http.catch_panic.enabled {
        policies.push(site_policy(
            "catch_panic",
            "Converts panics into framework errors.",
            [],
        ));
    }
    if http.request_id.enabled {
        policies.push(site_policy(
            "request_id",
            "Reads or creates a request id and writes it to the response.",
            [setting("header", http.request_id.header.as_str())],
        ));
    }
    optional_policies(http, &mut policies);
    policies
}

fn optional_policies(http: &HttpConf, policies: &mut Vec<MiddlewareInfo>) {
    if http.trace.enabled {
        policies.push(site_policy("trace", "Emits HTTP request tracing.", []));
    }
    if http.compression.enabled {
        policies.push(site_policy(
            "compression",
            "Compresses supported responses.",
            [],
        ));
    }
    if http.cors.enabled {
        policies.push(site_policy(
            "cors",
            "Applies Cross-Origin Resource Sharing policy.",
            [setting("permissive", bool_text(http.cors.permissive))],
        ));
    }
    limit_policies(http, policies);
}

fn limit_policies(http: &HttpConf, policies: &mut Vec<MiddlewareInfo>) {
    if http.timeout.enabled {
        policies.push(site_policy(
            "timeout",
            "Limits request processing time.",
            [setting("timeout_ms", http.timeout.timeout_ms.to_string())],
        ));
    }
    if http.body_limit.enabled {
        policies.push(site_policy(
            "body_limit",
            "Limits request body size.",
            [setting("max_bytes", http.body_limit.max_bytes.to_string())],
        ));
    }
    if http.security_headers.enabled {
        policies.push(security_headers(http));
    }
}

fn security_headers(http: &HttpConf) -> MiddlewareInfo {
    site_policy(
        "security_headers",
        "Adds configured security response headers.",
        [
            setting(
                "x_content_type_options",
                bool_text(http.security_headers.x_content_type_options),
            ),
            setting(
                "x_frame_options",
                option_text(http.security_headers.x_frame_options.as_deref()),
            ),
            setting(
                "referrer_policy",
                option_text(http.security_headers.referrer_policy.as_deref()),
            ),
        ],
    )
}

fn site_policy<const N: usize>(
    name: &str,
    description: &str,
    settings: [MiddlewareSetting; N],
) -> MiddlewareInfo {
    MiddlewareInfo {
        name: name.to_string(),
        scope: "site",
        description: Some(description.to_string()),
        request_parts: Vec::new(),
        settings: settings.into_iter().collect(),
    }
}

fn setting(key: &str, value: impl Into<String>) -> MiddlewareSetting {
    MiddlewareSetting {
        key: key.to_string(),
        value: value.into(),
    }
}

fn bool_text(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

fn option_text(value: Option<&str>) -> &str {
    value.unwrap_or("-")
}
