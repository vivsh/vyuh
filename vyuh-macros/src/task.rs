use darling::FromMeta;
use proc_macro::TokenStream;
use quote::quote;
use syn::{Expr, ImplItemFn, ItemFn};

#[derive(Debug, Default, FromMeta)]
struct TaskArgs {
    /// Optional task name
    #[darling(default)]
    name: Option<String>,
    /// Optional static execution lane.
    #[darling(default)]
    lane: Option<Expr>,
    /// Optional static idempotency policy.
    #[darling(default)]
    idempotency: Option<Expr>,
}

/// Unified implementation for both free functions and methods
pub(crate) fn parse_task(attr: TokenStream, item: TokenStream) -> TokenStream {
    parse_task_as(attr, item, false)
}

/// Registers a value-only local batch handler.
pub(crate) fn parse_task_batch(attr: TokenStream, item: TokenStream) -> TokenStream {
    parse_task_as(attr, item, true)
}

fn parse_task_as(attr: TokenStream, item: TokenStream, batch: bool) -> TokenStream {
    let args = if attr.is_empty() {
        TaskArgs::default()
    } else {
        match darling::ast::NestedMeta::parse_meta_list(attr.into()) {
            Ok(v) => match TaskArgs::from_list(&v) {
                Ok(args) => args,
                Err(e) => return e.write_errors().into(),
            },
            Err(e) => return e.into_compile_error().into(),
        }
    };

    let (original, fn_ident, is_method) = if let Ok(func) = syn::parse::<ItemFn>(item.clone()) {
        let ident = func.sig.ident.clone();
        (quote! { #func }, ident, false)
    } else if let Ok(method) = syn::parse::<ImplItemFn>(item.clone()) {
        let ident = method.sig.ident.clone();
        (quote! { #method }, ident, true)
    } else {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "task attributes can only be applied to functions or methods",
        )
        .to_compile_error()
        .into();
    };

    let fn_name = fn_ident.to_string();
    let task_name = args.name.as_ref().unwrap_or(&fn_name);

    let bundle_part_fn_name =
        syn::Ident::new(&format!("__bundle_part_{}", fn_name), fn_ident.span());

    let call_expr = if is_method {
        quote! { Self::#fn_ident }
    } else {
        quote! { #fn_ident }
    };

    let lane = args
        .lane
        .map(|lane| quote! { .lane(#lane) })
        .unwrap_or_default();
    let idempotency = args
        .idempotency
        .map(|policy| quote! { .idempotency(#policy) })
        .unwrap_or_default();
    let register = if batch {
        quote! { ::vyuh::bundles::task_batch }
    } else {
        quote! { ::vyuh::bundles::task }
    };

    let expanded = quote! {
        #original

        #[allow(non_snake_case)]
        fn #bundle_part_fn_name() -> ::vyuh::bundles::BundlePart {
            #register(
                #call_expr,
                ::vyuh::tasks::TaskDefinition::new(#task_name)
                    #lane
                    #idempotency,
            )
        }
    };

    expanded.into()
}
