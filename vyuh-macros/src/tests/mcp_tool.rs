use darling::{FromMeta, ast::NestedMeta};
use proc_macro2::TokenStream;
use quote::quote;

use super::{McpToolMeta, tool_config};

fn parse(attrs: TokenStream) -> darling::Result<McpToolMeta> {
    McpToolMeta::from_list(&NestedMeta::parse_meta_list(attrs)?)
}

/// Expands annotations, the UI URI, and access options into the direct builder calls.
#[test]
fn emits_typed_tool_config() -> darling::Result<()> {
    let attrs = parse(quote! {
        ui_resource_uri = "ui://widget/member-card.html",
        read_only = true,
        destructive = false,
        idempotent = true,
        open_world = false,
        oauth_security,
        openai_profile = true
    })?;
    let expected = quote! {
        ::vyuh::mcp::McpToolConf::default()
            .ui_resource_uri("ui://widget/member-card.html")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false)
            .oauth_security()
            .openai_profile()
    };
    assert_eq!(tool_config(&attrs).to_string(), expected.to_string());
    let anonymous = parse(quote! { allow_anonymous = true })?;
    let expected = quote! { ::vyuh::mcp::McpToolConf::default().allow_anonymous() };
    assert_eq!(tool_config(&anonymous).to_string(), expected.to_string());
    Ok(())
}

/// Keeps access opt-ins disabled when absent or explicitly false.
#[test]
fn omits_disabled_access_options() -> darling::Result<()> {
    let disabled = parse(quote! {
        allow_anonymous = false, oauth_security = false, openai_profile = false
    })?;
    let default = parse(quote! {})?;
    let expected = quote! { ::vyuh::mcp::McpToolConf::default() };
    assert_eq!(tool_config(&disabled).to_string(), expected.to_string());
    assert_eq!(tool_config(&default).to_string(), expected.to_string());
    Ok(())
}

/// Rejects unknown, non-boolean, and repeated option syntax during macro parsing.
#[test]
fn rejects_bad_tool_attributes() {
    for attrs in [
        quote! { role = "admin" },
        quote! { allow_anonymous = 1 },
        quote! { oauth_security = "yes" },
        quote! { openai_profile(yes) },
        quote! { allow_anonymous, allow_anonymous = false },
        quote! { oauth_security = true, oauth_security = false },
        quote! { openai_profile = false, openai_profile },
    ] {
        assert!(parse(attrs.clone()).is_err(), "accepted {attrs}");
    }
}
