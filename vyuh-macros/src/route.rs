//! Route macro implementation using bundlepart infrastructure.
//!
//! Provides #[route] attribute macro for HTTP route handlers.
//! Delegates to bundlepart.rs for consistent code generation.

use darling::FromMeta;
use proc_macro::TokenStream;
use quote::quote;

use crate::bundlepart::{self, FnSpec};

/// Route configuration metadata mapped to `vyuh::routes::RouteConf`.
#[derive(Debug, FromMeta, Default)]
struct RouteConfMeta {
    name: Option<String>,

    /// HTTP method(s); default: GET
    #[darling(default, multiple, rename = "method")]
    methods: Vec<String>,

    /// URL path.
    #[darling(rename = "path")]
    path: String,

    /// Optional slash policy: exact, trim, redirect_append, redirect_remove, auto.
    slash: Option<String>,
}

/// Entry point for #[route] macro.
///
/// Handles both free functions and methods in impl blocks.
pub(crate) fn parse_route(attr: TokenStream, item: TokenStream) -> TokenStream {
    bundlepart::generate_bundle_part::<RouteConfMeta>(attr, item, "route", build_route_conf)
}

/// Build RouteConf from parsed metadata and function spec.
///
/// Validates and normalizes configuration, generates RouteConf construction.
fn build_route_conf(
    conf: &RouteConfMeta,
    spec: &FnSpec,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let path = &conf.path;
    validate_path(path)?;

    let methods = normalize_methods(&conf.methods);
    for method in &methods {
        validate_method(method)?;
    }

    let method_filter = build_method_filter(&methods);
    let name = &conf.name.as_deref().unwrap_or(&spec.name);
    let slash = build_slash_policy(conf.slash.as_deref())?;

    Ok(quote! {
        ::vyuh::routes::RouteConf {
            name: ::std::borrow::Cow::Borrowed(#name),
            methods: #method_filter,
            path: ::std::borrow::Cow::Borrowed(#path),
            slash: #slash,
        }
    })
}

fn build_slash_policy(value: Option<&str>) -> Result<proc_macro2::TokenStream, syn::Error> {
    let Some(value) = value else {
        return Ok(quote! { None });
    };
    match value {
        "exact" => Ok(quote! { Some(::vyuh::middlewares::SlashPolicy::Exact) }),
        "trim" => Ok(quote! { Some(::vyuh::middlewares::SlashPolicy::Trim) }),
        "redirect_append" => Ok(quote! { Some(::vyuh::middlewares::SlashPolicy::RedirectAppend) }),
        "redirect_remove" => Ok(quote! { Some(::vyuh::middlewares::SlashPolicy::RedirectRemove) }),
        "auto" => Ok(quote! { Some(::vyuh::middlewares::SlashPolicy::Auto) }),
        other => Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            format!(
                "Invalid slash policy '{}'. Supported: exact, trim, redirect_append, redirect_remove, auto",
                other
            ),
        )),
    }
}

/// Normalize method list (uppercase, default to GET).
fn normalize_methods(methods: &[String]) -> Vec<String> {
    if methods.is_empty() {
        vec!["GET".to_string()]
    } else {
        methods.iter().map(|m| m.to_uppercase()).collect()
    }
}

/// Validate URL path format.
fn validate_path(path: &str) -> Result<(), syn::Error> {
    if path.is_empty() {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "Route path cannot be empty",
        ));
    }

    if !path.starts_with('/') {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("Route path must start with '/'. Found: '{}'", path),
        ));
    }

    if path.contains("//") {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("Route path contains double slashes: '{}'", path),
        ));
    }

    Ok(())
}

/// Validate HTTP method.
fn validate_method(method: &str) -> Result<(), syn::Error> {
    const VALID: &[&str] = &[
        "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "TRACE", "CONNECT",
    ];

    if !VALID.contains(&method) {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            format!(
                "Invalid HTTP method '{}'. Supported: {}",
                method,
                VALID.join(", ")
            ),
        ));
    }

    Ok(())
}

/// Build Methods filter expression from method list.
fn build_method_filter(methods: &[String]) -> proc_macro2::TokenStream {
    let filters: Vec<_> = methods
        .iter()
        .map(|m| method_to_const(m.as_str()))
        .collect();

    if filters.len() == 1 {
        filters[0].clone()
    } else {
        let first = &filters[0];
        let rest = &filters[1..];
        quote! { #first #(| #rest)* }
    }
}

/// Convert method string to Methods constant.
fn method_to_const(method: &str) -> proc_macro2::TokenStream {
    match method {
        "GET" => quote! { ::vyuh::routes::Methods::GET },
        "POST" => quote! { ::vyuh::routes::Methods::POST },
        "PUT" => quote! { ::vyuh::routes::Methods::PUT },
        "DELETE" => quote! { ::vyuh::routes::Methods::DELETE },
        "PATCH" => quote! { ::vyuh::routes::Methods::PATCH },
        "OPTIONS" => quote! { ::vyuh::routes::Methods::OPTIONS },
        "HEAD" => quote! { ::vyuh::routes::Methods::HEAD },
        "TRACE" => quote! { ::vyuh::routes::Methods::TRACE },
        "CONNECT" => quote! { ::vyuh::routes::Methods::CONNECT },
        _ => quote! { compile_error!("invalid HTTP method") },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    /// Verifies route metadata rejects the removed MCP conversion annotation.
    #[test]
    fn rejects_mcp_annotation() {
        let result = darling::ast::NestedMeta::parse_meta_list(quote! {
            path = "/notes", mcp
        });
        assert!(matches!(
            &result,
            Ok(items) if RouteConfMeta::from_list(items).is_err()
        ));
    }

    #[test]
    fn normalize_methods_defaults_to_get() {
        assert_eq!(normalize_methods(&[]), vec!["GET"]);
    }

    #[test]
    fn normalize_methods_is_case_insensitive() {
        let methods = vec!["get".to_string(), "pOsT".to_string()];
        assert_eq!(normalize_methods(&methods), vec!["GET", "POST"]);
    }

    #[test]
    fn validate_method_accepts_all_runtime_methods() {
        for method in [
            "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "TRACE", "CONNECT",
        ] {
            validate_method(method).unwrap();
        }
    }

    #[test]
    fn validate_method_rejects_unknown_methods() {
        let err = validate_method("BREW").unwrap_err();
        assert!(err.to_string().contains("Invalid HTTP method"));
    }

    #[test]
    fn validate_path_requires_absolute_path() {
        let err = validate_path("notes").unwrap_err();
        assert!(err.to_string().contains("must start with '/'"));
    }

    #[test]
    fn validate_path_rejects_double_slashes() {
        let err = validate_path("/api//notes").unwrap_err();
        assert!(err.to_string().contains("double slashes"));
    }
}
