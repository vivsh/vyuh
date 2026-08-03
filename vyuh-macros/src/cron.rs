//! Cron macro implementation using bundlepart infrastructure.
//!
//! Provides #[cron] attribute macro for scheduled task handlers.
//! Delegates to bundlepart.rs for consistent code generation.

use darling::FromMeta;
use proc_macro::TokenStream;
use quote::quote;

use crate::bundlepart::{self, FnSpec};

/// Cron configuration metadata.
///
/// Maps to vyuh::emitters::CronConf runtime structure.
#[derive(Debug, FromMeta, Default)]
struct CronConfMeta {
    /// Cron expression (e.g., "0 0 * * *")
    #[darling(default)]
    expr: Option<String>,
}

/// Entry point for #[cron] macro.
///
/// Handles both free functions and methods in impl blocks.
pub(crate) fn parse_cron(attr: TokenStream, item: TokenStream) -> TokenStream {
    bundlepart::generate_bundle_part::<CronConfMeta>(attr, item, "cron", build_cron_conf)
}

/// Build CronConf from parsed metadata and function spec.
///
/// Validates cron expression and generates CronConf construction.
fn build_cron_conf(
    conf: &CronConfMeta,
    _spec: &FnSpec,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let expr = conf.expr.as_ref().ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "Cron expression is required. Use: #[cron(expr = \"0 0 * * *\")]",
        )
    })?;

    validate_cron_expr(expr)?;

    Ok(quote! {
        ::vyuh::emitters::CronConf {
            expr: #expr.to_string(),
        }
    })
}

/// Validate cron expression by parsing it.
///
/// Uses the actual cron parser to catch invalid expressions at compile time.
fn validate_cron_expr(expr: &str) -> Result<(), syn::Error> {
    if expr.trim().is_empty() {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "Cron expression cannot be empty",
        ));
    }

    if let Err(e) = expr.parse::<cron::Schedule>() {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("Invalid cron expression '{}': {}", expr, e),
        ));
    }

    Ok(())
}
