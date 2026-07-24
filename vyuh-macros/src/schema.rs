use darling::FromMeta;
use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;

#[derive(Default, darling::FromMeta)]
struct SchemaAttr {
    #[darling(default)]
    namespace: Option<String>,
}

pub(crate) fn parse_schema(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = match parse_attr(attr) {
        Ok(attr) => attr,
        Err(err) => return err.write_errors().into(),
    };
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

fn parse_attr(attr: TokenStream) -> darling::Result<SchemaAttr> {
    if attr.is_empty() {
        return Ok(SchemaAttr::default());
    }
    let nested = darling::ast::NestedMeta::parse_meta_list(attr.into())?;
    SchemaAttr::from_list(&nested)
}
