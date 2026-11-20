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
    allow_anonymous: Option<bool>,
    oauth_security: Option<bool>,
    openai_profile: Option<bool>,
}

pub(crate) fn parse_mcp_tool(attr: TokenStream, item: TokenStream) -> TokenStream {
    bundlepart::generate_named_bundle_part::<McpToolMeta>(attr, item, "mcp_tool", build_conf)
}

fn build_conf(conf: &McpToolMeta, _spec: &FnSpec) -> Result<proc_macro2::TokenStream, syn::Error> {
    Ok(tool_config(conf))
}

/// Emits the same typed configuration used by direct registrations.
fn tool_config(conf: &McpToolMeta) -> proc_macro2::TokenStream {
    let ui = conf
        .ui_resource_uri
        .as_deref()
        .map(|value| quote! { .ui_resource_uri(#value) });
    let read_only = conf.read_only.map(|value| quote! { .read_only(#value) });
    let destructive = conf
        .destructive
        .map(|value| quote! { .destructive(#value) });
    let idempotent = conf.idempotent.map(|value| quote! { .idempotent(#value) });
    let open_world = conf.open_world.map(|value| quote! { .open_world(#value) });
    let anonymous = (conf.allow_anonymous == Some(true)).then(|| quote! { .allow_anonymous() });
    let oauth = (conf.oauth_security == Some(true)).then(|| quote! { .oauth_security() });
    let profile = (conf.openai_profile == Some(true)).then(|| quote! { .openai_profile() });
    quote! {
        ::vyuh::mcp::McpToolConf::default()
        #ui #read_only #destructive #idempotent #open_world #anonymous #oauth #profile
    }
}

#[cfg(test)]
#[path = "tests/mcp_tool.rs"]
mod tests;
