use proc_macro::TokenStream;
use quote::quote;
use syn::{LitStr, parse_macro_input};

#[derive(Default)]
struct SchemaAttr {
    namespace: Option<LitStr>,
}

impl syn::parse::Parse for SchemaAttr {
    fn parse(input: syn::parse::ParseStream<'_>) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(Self::default());
        }
        let key: syn::Ident = input.parse()?;
        if key != "namespace" {
            return Err(syn::Error::new_spanned(
                key,
                "expected `namespace = \"...\"`",
            ));
        }
        input.parse::<syn::Token![=]>()?;
        let namespace: LitStr = input.parse()?;
        Ok(Self {
            namespace: Some(namespace),
        })
    }
}

pub(crate) fn parse_schema(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = parse_macro_input!(attr as SchemaAttr);
    let input = parse_macro_input!(item as syn::ItemFn);
    let fn_name = &input.sig.ident;
    let bundle_part_fn_name =
        syn::Ident::new(&format!("__bundle_part_{}", fn_name), fn_name.span());
    let source = match attr.namespace {
        Some(namespace) => quote! { ::vyuh::db::crate_schema(#namespace, #fn_name) },
        None => quote! { ::vyuh::db::root_schema(#fn_name) },
    };

    quote! {
        #input

        #[allow(non_snake_case)]
        #[doc(hidden)]
        fn #bundle_part_fn_name() -> ::vyuh::bundles::BundlePart {
            ::vyuh::bundles::schema(#source)
        }
    }
    .into()
}
