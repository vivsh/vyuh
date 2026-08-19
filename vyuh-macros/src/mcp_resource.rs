use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, parse_macro_input};

/// Expands an MCP resource factory into a bundle part constructor.
pub fn parse_mcp_resource(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let function = parse_macro_input!(item as ItemFn);
    let name = &function.sig.ident;
    let part = format_ident!("__bundle_part_{name}");
    let resource_name = name.to_string();
    quote! {
        #function

        #[doc(hidden)]
        fn #part() -> ::vyuh::bundles::BundlePart {
            ::vyuh::bundles::mcp_resource(#resource_name, #name())
        }
    }
    .into()
}
