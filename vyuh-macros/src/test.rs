//! Expansion for `#[vyuh::test]` integration fixtures.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{
    Expr, FnArg, GenericArgument, ItemFn, Meta, Pat, PathArguments, ReturnType, Token, Type,
    parse::Parser, punctuated::Punctuated,
};

#[derive(Default)]
struct TestArgs {
    conf: Option<Expr>,
    bundle: Option<Expr>,
    migrations: Option<bool>,
}

/// Parses the fixture arguments and expands a Tokio test around a `TestSite` body function.
pub(crate) fn parse_test(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = match parse_args(attr.into()) {
        Ok(args) => args,
        Err(error) => return error.into_compile_error().into(),
    };
    let function = match syn::parse::<ItemFn>(item) {
        Ok(function) => function,
        Err(error) => return error.into_compile_error().into(),
    };
    match expand_test(args, function) {
        Ok(expanded) => expanded.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

/// Parses the deliberately small attribute surface and rejects unknown or repeated options.
fn parse_args(attr: TokenStream2) -> syn::Result<TestArgs> {
    let parser = Punctuated::<Meta, Token![,]>::parse_terminated;
    let metas = parser.parse2(attr)?;
    let mut args = TestArgs::default();
    for meta in metas {
        parse_arg(&mut args, meta)?;
    }
    Ok(args)
}

/// Applies one `conf`, `bundle`, or `migrations` option to the parsed test configuration.
fn parse_arg(args: &mut TestArgs, meta: Meta) -> syn::Result<()> {
    let Meta::NameValue(value) = meta else {
        return Err(syn::Error::new_spanned(
            meta,
            "expected `conf = ...`, `bundle = ...`, or `migrations = true|false`",
        ));
    };
    let Some(name) = value.path.get_ident() else {
        return Err(syn::Error::new_spanned(
            value.path,
            "unsupported test option",
        ));
    };
    match name.to_string().as_str() {
        "conf" => set_expr(&mut args.conf, value.value, "conf"),
        "bundle" => set_expr(&mut args.bundle, value.value, "bundle"),
        "migrations" => set_migrations(args, value.value),
        _ => Err(syn::Error::new_spanned(name, "unsupported test option")),
    }
}

/// Stores a configuration expression while diagnosing a duplicate option at its use site.
fn set_expr(slot: &mut Option<Expr>, value: Expr, name: &str) -> syn::Result<()> {
    if slot.is_some() {
        return Err(syn::Error::new_spanned(
            value,
            format!("duplicate `{name}` option"),
        ));
    }
    *slot = Some(value);
    Ok(())
}

/// Parses the one boolean option instead of accepting arbitrary runtime expressions for it.
fn set_migrations(args: &mut TestArgs, value: Expr) -> syn::Result<()> {
    if args.migrations.is_some() {
        return Err(syn::Error::new_spanned(
            value,
            "duplicate `migrations` option",
        ));
    }
    let Expr::Lit(literal) = value else {
        return Err(syn::Error::new_spanned(
            value,
            "`migrations` must be `true` or `false`",
        ));
    };
    let syn::Lit::Bool(value) = literal.lit else {
        return Err(syn::Error::new_spanned(
            literal,
            "`migrations` must be `true` or `false`",
        ));
    };
    args.migrations = Some(value.value);
    Ok(())
}

/// Validates the function contract and constructs the test wrapper and hidden body function.
fn expand_test(args: TestArgs, function: ItemFn) -> syn::Result<TokenStream2> {
    validate_function(&function)?;
    let error = result_error_type(&function.sig.output)?;
    let wrapper = &function.sig.ident;
    let body = format_ident!("__vyuh_test_{}_body", wrapper);
    let attrs = &function.attrs;
    let vis = &function.vis;
    let body_sig = body_signature(&function, &body);
    let block = &function.block;
    let setup = setup_tokens(&args);
    Ok(quote! {
        #(#attrs)*
        #[::vyuh::testing::tokio::test]
        #vis async fn #wrapper() -> ::std::result::Result<(), ::vyuh::testing::TestRunError<#error>> {
            #setup
            let __vyuh_body = #body(&__vyuh_site).await;
            let __vyuh_cleanup = __vyuh_site.teardown().await;
            ::vyuh::testing::finish_test(__vyuh_body, __vyuh_cleanup)
        }

        #body_sig #block
    })
}

/// Builds the hidden original function while preserving its input and result signature.
fn body_signature(function: &ItemFn, body: &syn::Ident) -> TokenStream2 {
    let signature = &function.sig;
    let inputs = &signature.inputs;
    let output = &signature.output;
    quote! { async fn #body(#inputs) #output }
}

/// Validates restrictions that make the generated no-argument wrapper unambiguous.
fn validate_function(function: &ItemFn) -> syn::Result<()> {
    if function.sig.asyncness.is_none() {
        return Err(syn::Error::new_spanned(
            &function.sig.fn_token,
            "`#[vyuh::test]` requires an async function",
        ));
    }
    if !function.sig.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &function.sig.generics,
            "`#[vyuh::test]` does not support generic test functions",
        ));
    }
    validate_site_argument(function)
}

/// Ensures the original body receives exactly the immutable `site: &TestSite` fixture argument.
fn validate_site_argument(function: &ItemFn) -> syn::Result<()> {
    if function.sig.inputs.len() != 1 {
        return Err(syn::Error::new_spanned(
            &function.sig.inputs,
            "`#[vyuh::test]` requires `site: &TestSite` as its only argument",
        ));
    }
    let Some(FnArg::Typed(argument)) = function.sig.inputs.first() else {
        return Err(syn::Error::new_spanned(
            &function.sig.inputs,
            "`#[vyuh::test]` requires `site: &TestSite`",
        ));
    };
    let Pat::Ident(pattern) = argument.pat.as_ref() else {
        return Err(syn::Error::new_spanned(
            &argument.pat,
            "test fixture must be named `site`",
        ));
    };
    if pattern.ident != "site" || !is_test_site_ref(argument.ty.as_ref()) {
        return Err(syn::Error::new_spanned(
            argument,
            "`#[vyuh::test]` requires `site: &TestSite`",
        ));
    }
    Ok(())
}

/// Checks the final path segment so imported and fully-qualified `TestSite` paths both work.
fn is_test_site_ref(ty: &Type) -> bool {
    let Type::Reference(reference) = ty else {
        return false;
    };
    reference.mutability.is_none()
        && matches!(reference.elem.as_ref(), Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "TestSite"))
}

/// Extracts the user error type so the generated wrapper can retain every failure source.
fn result_error_type(output: &ReturnType) -> syn::Result<&Type> {
    let ReturnType::Type(_, ty) = output else {
        return Err(syn::Error::new_spanned(
            output,
            "`#[vyuh::test]` requires a `Result<T, E>` return type",
        ));
    };
    let Type::Path(path) = ty.as_ref() else {
        return Err(syn::Error::new_spanned(
            ty,
            "`#[vyuh::test]` requires a `Result<T, E>` return type",
        ));
    };
    let Some(segment) = path.path.segments.last() else {
        return Err(syn::Error::new_spanned(
            path,
            "`#[vyuh::test]` requires a `Result<T, E>` return type",
        ));
    };
    if segment.ident != "Result" {
        return Err(syn::Error::new_spanned(
            segment,
            "`#[vyuh::test]` requires a `Result<T, E>` return type",
        ));
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(syn::Error::new_spanned(
            segment,
            "`#[vyuh::test]` requires a `Result<T, E>` return type",
        ));
    };
    let Some(GenericArgument::Type(error)) = arguments.args.iter().nth(1) else {
        return Err(syn::Error::new_spanned(
            arguments,
            "`#[vyuh::test]` requires a `Result<T, E>` return type",
        ));
    };
    Ok(error)
}

/// Produces setup statements whose `conf` and `bundle` expressions are evaluated exactly once.
fn setup_tokens(args: &TestArgs) -> TokenStream2 {
    let conf = source_tokens(args.conf.as_ref(), quote!(::vyuh::SiteConf::default()));
    let bundle = source_tokens(
        args.bundle.as_ref(),
        quote!(::vyuh::bundles::Bundle::default()),
    );
    let site = if args.migrations.unwrap_or(true) {
        quote!(::vyuh::testing::test_site(__vyuh_conf, __vyuh_bundle))
    } else {
        quote!(
            ::vyuh::testing::TestSite::builder(__vyuh_conf, __vyuh_bundle)
                .without_migrations()
                .build()
        )
    };
    quote! {
        let __vyuh_conf = ::vyuh::testing::TestConfSource::into_test_conf(#conf)
            .map_err(::vyuh::testing::TestRunError::Configuration)?;
        let __vyuh_bundle = #bundle;
        let __vyuh_site = #site.await.map_err(::vyuh::testing::TestRunError::Setup)?;
    }
}

/// Treats a bare path as a zero-argument fixture factory while preserving arbitrary expressions.
fn source_tokens(value: Option<&Expr>, default: TokenStream2) -> TokenStream2 {
    match value {
        Some(Expr::Path(path)) => quote! { (#path)() },
        Some(value) => quote! { #value },
        None => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;

    /// Accepts all supported options together.
    #[test]
    fn parses_supported_options() -> syn::Result<()> {
        let args = parse_args(quote!(
            conf = test_conf,
            bundle = app_bundle,
            migrations = false
        ))?;
        assert!(args.conf.is_some());
        assert!(args.bundle.is_some());
        assert_eq!(args.migrations, Some(false));
        Ok(())
    }

    /// Rejects options outside the documented surface.
    #[test]
    fn rejects_unknown_option() {
        assert!(parse_args(quote!(database = test_db)).is_err());
    }

    /// Rejects a missing fixture argument before generating code.
    #[test]
    fn rejects_missing_site_argument() -> syn::Result<()> {
        let function: ItemFn = syn::parse2(quote!(
            async fn test() -> Result<(), ()> {
                Ok(())
            }
        ))?;
        assert!(validate_function(&function).is_err());
        Ok(())
    }
}
