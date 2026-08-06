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

    /// Target executor: `signal` or `task`.
    #[darling(default)]
    executor: Option<String>,

    /// Stable durable task schedule name.
    #[darling(default)]
    schedule: Option<String>,

    /// Durable task first-start policy: `next` or `immediately`.
    #[darling(default)]
    start: Option<String>,
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

    let executor = executor_tokens(conf.executor.as_deref())?;
    let schedule = conf
        .schedule
        .as_ref()
        .map(|value| quote! { .schedule(#value) });
    let start = start_tokens(conf.start.as_deref())?;
    Ok(quote! {
        ::vyuh::emitters::PeriodicConf::new(#interval)
            .executor(#executor)
            #schedule
            #start
    })
}

/// Produces the public executor value from the concise macro spelling.
fn executor_tokens(value: Option<&str>) -> Result<proc_macro2::TokenStream, syn::Error> {
    match value.unwrap_or("signal") {
        "signal" => Ok(quote! { ::vyuh::emitters::EmitterExecutor::Signal }),
        "task" => Ok(quote! { ::vyuh::emitters::EmitterExecutor::Task }),
        value => Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("unsupported periodic executor '{value}'; use 'signal' or 'task'"),
        )),
    }
}

/// Produces the durable-task start policy while retaining signal compatibility.
fn start_tokens(value: Option<&str>) -> Result<proc_macro2::TokenStream, syn::Error> {
    match value.unwrap_or("next") {
        "next" => Ok(quote! { .on_start(::vyuh::emitters::ScheduleStart::Next) }),
        "immediately" => Ok(quote! { .on_start(::vyuh::emitters::ScheduleStart::Immediately) }),
        value => Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("unsupported periodic start policy '{value}'; use 'next' or 'immediately'"),
        )),
    }
}
