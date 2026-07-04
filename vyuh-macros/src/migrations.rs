use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;

pub(crate) fn parse_migrations(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as syn::ItemFn);

    let fn_name = &input.sig.ident;
    let bundle_part_fn_name =
        syn::Ident::new(&format!("__bundle_part_{}", fn_name), fn_name.span());

    quote! {
        #input

        #[allow(non_snake_case)]
        #[doc(hidden)]
        fn #bundle_part_fn_name() -> ::vyuh::bundles::BundlePart {
            ::vyuh::bundles::migrations(#fn_name())
        }
    }
    .into()
}
