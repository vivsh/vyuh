//! `#[beacon]` route-registration macro implementation.

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Expr, ExprArray, ExprLit, ExprPath, ItemFn, Lit, Meta, Token, parse::Parser,
    punctuated::Punctuated,
};

/// Expands one declarative Beacon factory into a bundle part factory.
pub(crate) fn parse_beacon(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = proc_macro2::TokenStream::from(attr);
    let item = proc_macro2::TokenStream::from(item);
    match expand(attr, item) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn expand(
    attr: proc_macro2::TokenStream,
    item: proc_macro2::TokenStream,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let function = syn::parse2::<ItemFn>(item.clone())?;
    if !function.sig.inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &function.sig.inputs,
            "Beacon factories must not take handler arguments",
        ));
    }
    let conf = BeaconMeta::parse(attr)?;
    let path = conf.path;
    let name = conf.name.unwrap_or_else(|| function.sig.ident.to_string());
    let modes = modes_tokens(conf.modes)?;
    let slash = slash_tokens(conf.slash.as_deref())?;
    let function_name = &function.sig.ident;
    let wrapper = syn::Ident::new(
        &format!("__bundle_part_{}", function_name),
        function_name.span(),
    );
    Ok(quote! {
        #item

        #[doc(hidden)]
        fn #wrapper() -> ::vyuh::bundles::BundlePart {
            ::vyuh::bundles::beacon(
                #function_name(),
                ::vyuh::channels::BeaconConf::new(#name, #path)
                    .modes(#modes)
                    #slash,
            )
        }
    })
}

struct BeaconMeta {
    path: String,
    name: Option<String>,
    modes: Vec<String>,
    slash: Option<String>,
}

impl BeaconMeta {
    fn parse(tokens: proc_macro2::TokenStream) -> Result<Self, syn::Error> {
        let values = Punctuated::<Meta, Token![,]>::parse_terminated.parse2(tokens)?;
        let mut path = None;
        let mut name = None;
        let mut modes = Vec::new();
        let mut slash = None;
        for value in values {
            let Meta::NameValue(value) = value else {
                return Err(syn::Error::new_spanned(
                    value,
                    "Beacon attributes must use name = value",
                ));
            };
            if value.path.is_ident("path") {
                path = Some(string_value(&value.value, "path")?);
            } else if value.path.is_ident("name") {
                name = Some(string_value(&value.value, "name")?);
            } else if value.path.is_ident("modes") {
                modes = mode_values(&value.value)?;
            } else if value.path.is_ident("slash") {
                slash = Some(string_value(&value.value, "slash")?);
            } else {
                return Err(syn::Error::new_spanned(
                    value.path,
                    "unsupported Beacon attribute",
                ));
            }
        }
        let path = path.ok_or_else(|| {
            syn::Error::new(
                proc_macro2::Span::call_site(),
                "Beacon requires path = \"/...\"",
            )
        })?;
        Ok(Self {
            path,
            name,
            modes,
            slash,
        })
    }
}

fn string_value(value: &Expr, name: &str) -> Result<String, syn::Error> {
    match value {
        Expr::Lit(ExprLit {
            lit: Lit::Str(value),
            ..
        }) => Ok(value.value()),
        _ => Err(syn::Error::new_spanned(
            value,
            format!("Beacon {name} must be a string"),
        )),
    }
}

fn mode_values(value: &Expr) -> Result<Vec<String>, syn::Error> {
    let Expr::Array(ExprArray { elems, .. }) = value else {
        return Err(syn::Error::new_spanned(
            value,
            "Beacon modes must be [ws, sse, poll]",
        ));
    };
    elems.iter().map(mode_value).collect()
}

fn mode_value(value: &Expr) -> Result<String, syn::Error> {
    match value {
        Expr::Path(ExprPath { path, .. }) => path
            .get_ident()
            .map(ToString::to_string)
            .ok_or_else(|| syn::Error::new_spanned(path, "Beacon mode must be ws, sse, or poll")),
        Expr::Lit(ExprLit {
            lit: Lit::Str(value),
            ..
        }) => Ok(value.value()),
        _ => Err(syn::Error::new_spanned(
            value,
            "Beacon mode must be ws, sse, or poll",
        )),
    }
}

fn modes_tokens(values: Vec<String>) -> Result<proc_macro2::TokenStream, syn::Error> {
    if values.is_empty() {
        return Ok(quote!(::vyuh::channels::ALL_TRANSPORTS));
    }
    let mut output = quote!(0u8);
    for value in values {
        let mode = match value.as_str() {
            "ws" => quote!(::vyuh::channels::WS),
            "sse" => quote!(::vyuh::channels::SSE),
            "poll" => quote!(::vyuh::channels::POLL),
            _ => {
                return Err(syn::Error::new(
                    proc_macro2::Span::call_site(),
                    "Beacon modes support ws, sse, and poll",
                ));
            }
        };
        output = quote!(#output | #mode);
    }
    Ok(output)
}

fn slash_tokens(value: Option<&str>) -> Result<proc_macro2::TokenStream, syn::Error> {
    let Some(value) = value else {
        return Ok(quote!());
    };
    let policy = match value {
        "exact" => quote!(::vyuh::middlewares::SlashPolicy::Exact),
        "trim" => quote!(::vyuh::middlewares::SlashPolicy::Trim),
        "redirect_append" => quote!(::vyuh::middlewares::SlashPolicy::RedirectAppend),
        "redirect_remove" => quote!(::vyuh::middlewares::SlashPolicy::RedirectRemove),
        "auto" => quote!(::vyuh::middlewares::SlashPolicy::Auto),
        _ => {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "invalid Beacon slash policy",
            ));
        }
    };
    Ok(quote!(.slash(#policy)))
}
