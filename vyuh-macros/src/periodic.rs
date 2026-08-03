//! Periodic macro implementation using bundlepart infrastructure.
//!
//! Provides #[periodic] attribute macro for periodic task handlers.
//! Delegates to bundlepart.rs for consistent code generation.

use darling::FromMeta;
use proc_macro::TokenStream;
use quote::quote;

use crate::bundlepart::{self, FnSpec};

/// Periodic configuration metadata.
///
/// Maps to vyuh::emitters::PeriodicConf runtime structure.
#[derive(Debug, FromMeta, Default)]
struct PeriodicConfMeta {
    /// Duration in seconds
    #[darling(default)]
    secs: Option<u64>,

    /// Duration in milliseconds
    #[darling(default)]
    millis: Option<u64>,
}

/// Entry point for #[periodic] macro.
///
/// Handles both free functions and methods in impl blocks.
pub(crate) fn parse_periodic(attr: TokenStream, item: TokenStream) -> TokenStream {
    bundlepart::generate_bundle_part::<PeriodicConfMeta>(
        attr,
        item,
        "periodic",
        build_periodic_conf,
    )
}

/// Build PeriodicConf from parsed metadata and function spec.
///
/// Validates duration parameters and generates PeriodicConf construction.
fn build_periodic_conf(
    conf: &PeriodicConfMeta,
    _spec: &FnSpec,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let interval = match (conf.secs, conf.millis) {
        (Some(s), None) => quote! { ::tokio::time::Duration::from_secs(#s) },
        (None, Some(m)) => quote! { ::tokio::time::Duration::from_millis(#m) },
        (Some(s), Some(m)) => {
            quote! { ::tokio::time::Duration::from_secs(#s) + ::tokio::time::Duration::from_millis(#m) }
        }
        (None, None) => {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "Periodic requires at least one of: secs, millis. Use: #[periodic(secs = 60)] or #[periodic(millis = 1000)]",
            ));
        }
    };

    Ok(quote! {
        ::vyuh::emitters::PeriodicConf {
            interval: #interval,
        }
    })
}
