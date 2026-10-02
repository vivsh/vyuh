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
    /// Explicit shared Pravah routing policy, valid only for Flow factories.
    #[darling(default)]
    dispatch: Option<syn::Path>,
}

/// Unified implementation for both free functions and methods
pub(crate) fn parse_work(attr: TokenStream, item: TokenStream) -> TokenStream {
    parse_task_as(attr, item, quote! { ::vyuh::bundles::work }, false)
}

/// Registers a value-only local batch handler.
pub(crate) fn parse_work_batch(attr: TokenStream, item: TokenStream) -> TokenStream {
    parse_task_as(attr, item, quote! { ::vyuh::bundles::work_batch }, false)
}

/// Registers synchronous orchestration; Rust bounds validate its signature.
pub(crate) fn parse_flow(attr: TokenStream, item: TokenStream) -> TokenStream {
    parse_task_as(attr, item, quote! { ::vyuh::bundles::flow }, true)
}

fn parse_task_as(
    attr: TokenStream,
    item: TokenStream,
    register: proc_macro2::TokenStream,
    is_flow: bool,
) -> TokenStream {
    let args = match parse_args(attr) {
        Ok(args) => args,
        Err(error) => return error,
    };

    if !is_flow && args.dispatch.is_some() {
        return syn::Error::new(
            proc_macro2::Span::call_site(),
            "dispatch is only supported on Flow factories",
        )
        .into_compile_error()
        .into();
    }

    let (original, fn_ident, is_method) = match parse_function(item) {
        Ok(parts) => parts,
        Err(error) => return error.into_compile_error().into(),
    };
    let name = fn_ident.to_string();
    let task_name = args.name.as_deref().unwrap_or(&name);
    let conf = configuration(&args, task_name, is_flow);
    let bundle_part = syn::Ident::new(&format!("__bundle_part_{name}"), fn_ident.span());
    let call = if is_method {
        quote! { Self::#fn_ident }
    } else {
        quote! { #fn_ident }
    };
    quote! {
        #original
        #[allow(non_snake_case)]
        fn #bundle_part() -> ::vyuh::bundles::BundlePart {
            #register(#call, #conf)
        }
    }
    .into()
}

/// Parses explicit registration configuration, never Rust argument/return type spelling.
fn parse_args(attr: TokenStream) -> Result<TaskArgs, TokenStream> {
    if attr.is_empty() {
        return Ok(TaskArgs::default());
    }
    let values = darling::ast::NestedMeta::parse_meta_list(attr.into())
        .map_err(|error| TokenStream::from(error.into_compile_error()))?;
    TaskArgs::from_list(&values).map_err(|error| error.write_errors().into())
}

/// Preserves the original declaration and supported free/associated registration target.
fn parse_function(item: TokenStream) -> syn::Result<(proc_macro2::TokenStream, syn::Ident, bool)> {
    if let Ok(func) = syn::parse::<ItemFn>(item.clone()) {
        let ident = func.sig.ident.clone();
        Ok((quote! { #func }, ident, false))
    } else if let Ok(method) = syn::parse::<ImplItemFn>(item.clone()) {
        let ident = method.sig.ident.clone();
        Ok((quote! { #method }, ident, true))
    } else {
        Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "task attributes can only be applied to functions or methods",
        ))
    }
}

/// Emits only calls to the equivalent direct configuration API.
fn configuration(args: &TaskArgs, name: &str, is_flow: bool) -> proc_macro2::TokenStream {
    let lane = args
        .lane
        .as_ref()
        .map(|lane| quote! { .lane(#lane) })
        .unwrap_or_default();
    let idempotency = args
        .idempotency
        .as_ref()
        .map(|policy| quote! { .idempotency(#policy) })
        .unwrap_or_default();
    let conf = if is_flow {
        quote! { ::vyuh::tasks::FlowConf }
    } else {
        quote! { ::vyuh::tasks::TaskDefinition }
    };
    let dispatch = args
        .dispatch
        .as_ref()
        .map(|ty| quote! { .dispatch::<#ty>() })
        .unwrap_or_default();

    quote! { #conf::new(#name) #lane #idempotency #dispatch }
}
