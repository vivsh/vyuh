//! Direct semantic MCP tool macro.

use darling::FromMeta;
use proc_macro::TokenStream;
use quote::quote;

use crate::bundlepart::{self, FnSpec};

#[derive(Debug, Default, FromMeta)]
struct McpToolMeta {
    ui_resource_uri: Option<String>,
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
        conf.ui_resource_uri.as_deref(),
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
    ui_resource_uri: Option<&str>,
    read_only: Option<bool>,
    destructive: Option<bool>,
    idempotent: Option<bool>,
    open_world: Option<bool>,
) -> proc_macro2::TokenStream {
    let ui_resource_uri = ui_resource_uri.map(|value| quote! { .ui_resource_uri(#value) });
    let read_only = read_only.map(|value| quote! { .read_only(#value) });
    let destructive = destructive.map(|value| quote! { .destructive(#value) });
    let idempotent = idempotent.map(|value| quote! { .idempotent(#value) });
    let open_world = open_world.map(|value| quote! { .open_world(#value) });
    quote! { #ui_resource_uri #read_only #destructive #idempotent #open_world }
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
        .expect("valid test metadata");
        let meta = McpToolMeta::from_list(&items).expect("supported test metadata");
        let output = annotation_calls(
            meta.ui_resource_uri.as_deref(),
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

    /// Verifies UI resource annotations use the typed tool configuration builder.
    #[test]
    fn emits_ui_resource_uri() {
        let items = darling::ast::NestedMeta::parse_meta_list(quote! {
            ui_resource_uri = "ui://widget/member-card.html"
        })
        .expect("valid test metadata");
        let meta = McpToolMeta::from_list(&items).expect("supported test metadata");
        let output =
            annotation_calls(meta.ui_resource_uri.as_deref(), None, None, None, None).to_string();
        assert!(output.contains("ui_resource_uri"));
    }

    /// Verifies unknown tool annotations fail macro metadata parsing.
    #[test]
    fn rejects_unknown_annotation() {
        let items = darling::ast::NestedMeta::parse_meta_list(quote! { role = "admin" })
            .expect("valid test metadata");
        assert!(McpToolMeta::from_list(&items).is_err());
    }
}
