use proc_macro::TokenStream;
use quote::quote;
use syn::spanned::Spanned;
use syn::{Ident, Path, Token, parse_macro_input};

pub(crate) fn parse_bundle(input: TokenStream) -> TokenStream {
    let input_parsed = parse_macro_input!(input as BundleInput);

    let bundle_part_setup = match input_parsed
        .handlers
        .iter()
        .map(bundle_part_setup)
        .collect::<syn::Result<Vec<_>>>()
    {
        Ok(items) => items,
        Err(err) => return err.to_compile_error().into(),
    };

    let expanded = quote! {
        {
            ::vyuh::bundles::bundle([
                #(#bundle_part_setup)*
            ])
        }
    };

    expanded.into()
}

fn bundle_part_setup(handler: &Path) -> syn::Result<proc_macro2::TokenStream> {
    let Some(segment) = handler.segments.last() else {
        return Err(syn::Error::new(handler.span(), "expected bundle item path"));
    };
    let fn_name = segment.ident.to_string();
    let bundle_part_fn = Ident::new(&format!("__bundle_part_{}", fn_name), handler.span());
    Ok(quote! {
        #bundle_part_fn(),
    })
}

struct BundleInput {
    handlers: Vec<Path>,
}

impl syn::parse::Parse for BundleInput {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let mut handlers = Vec::new();

        // Parse handlers
        while !input.is_empty() {
            handlers.push(input.parse::<Path>()?);

            // Optional trailing comma
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(Self { handlers })
    }
}
