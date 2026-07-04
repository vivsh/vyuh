use proc_macro::TokenStream;
use quote::quote;
use syn::{ImplItemFn, ItemFn};

pub(crate) fn parse_url_info(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let (original, fn_ident, is_method) = if let Ok(func) = syn::parse::<ItemFn>(item.clone()) {
        let ident = func.sig.ident.clone();
        (quote! { #func }, ident, false)
    } else if let Ok(method) = syn::parse::<ImplItemFn>(item.clone()) {
        let ident = method.sig.ident.clone();
        (quote! { #method }, ident, true)
    } else {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "#[url_info] can only be applied to functions or methods",
        )
        .to_compile_error()
        .into();
    };

    let fn_name = fn_ident.to_string();
    let bundle_part_fn_name =
        syn::Ident::new(&format!("__bundle_part_{}", fn_name), fn_ident.span());
    let call_expr = if is_method {
        quote! { Self::#fn_ident }
    } else {
        quote! { #fn_ident }
    };

    quote! {
        #original

        #[allow(non_snake_case)]
        #[doc(hidden)]
        fn #bundle_part_fn_name() -> ::vyuh::bundles::BundlePart {
            ::vyuh::bundles::url_info(#call_expr)
        }
    }
    .into()
}
