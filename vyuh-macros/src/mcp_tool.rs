//! Direct semantic MCP tool macro.

use darling::FromMeta;
use proc_macro::TokenStream;
use quote::quote;

use crate::bundlepart::{self, FnSpec};

#[derive(Debug, Default, FromMeta)]
struct McpToolMeta {
    read_only: Option<bool>,
    destructive: Option<bool>,
    idempotent: Option<bool>,
    open_world: Option<bool>,
}

pub(crate) fn parse_mcp_tool(attr: TokenStream, item: TokenStream) -> TokenStream {
    bundlepart::generate_named_bundle_part::<McpToolMeta>(attr, item, "mcp_tool", build_conf)
}

fn build_conf(conf: &McpToolMeta, _spec: &FnSpec) -> Result<proc_macro2::TokenStream, syn::Error> {
    let annotations = annotation_calls(
        conf.read_only,
        conf.destructive,
        conf.idempotent,
        conf.open_world,
    );
    Ok(quote! {
        ::vyuh::mcp::McpToolConf::default() #annotations
    })
}

fn annotation_calls(
    read_only: Option<bool>,
    destructive: Option<bool>,
    idempotent: Option<bool>,
    open_world: Option<bool>,
) -> proc_macro2::TokenStream {
    let read_only = read_only.map(|value| quote! { .read_only(#value) });
    let destructive = destructive.map(|value| quote! { .destructive(#value) });
    let idempotent = idempotent.map(|value| quote! { .idempotent(#value) });
    let open_world = open_world.map(|value| quote! { .open_world(#value) });
    quote! { #read_only #destructive #idempotent #open_world }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    /// Verifies every supported annotation is parsed and emitted as builder sugar.
    #[test]
    fn emits_supported_annotations() {
        let items = darling::ast::NestedMeta::parse_meta_list(quote! {
            read_only = true,
            destructive = false,
            idempotent = true,
            open_world = false
        })
        .unwrap_or_default();
        let meta = McpToolMeta::from_list(&items).unwrap_or_default();
        let output = annotation_calls(
            meta.read_only,
            meta.destructive,
            meta.idempotent,
            meta.open_world,
        )
        .to_string();
        assert!(output.contains("read_only"));
        assert!(output.contains("destructive"));
        assert!(output.contains("idempotent"));
        assert!(output.contains("open_world"));
    }

    /// Verifies unknown tool annotations fail macro metadata parsing.
    #[test]
    fn rejects_unknown_annotation() {
        let items = darling::ast::NestedMeta::parse_meta_list(quote! { role = "admin" })
            .unwrap_or_default();
        assert!(McpToolMeta::from_list(&items).is_err());
    }
}
