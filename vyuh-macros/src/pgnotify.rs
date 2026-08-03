//! PgNotify macro implementation using bundlepart infrastructure.
//!
//! Provides #[pgnotify] attribute macro for PostgreSQL NOTIFY handlers.
//! Delegates to bundlepart.rs for consistent code generation.

use darling::FromMeta;
use proc_macro::TokenStream;
use quote::quote;

use crate::bundlepart::{self, FnSpec};

/// PgNotify configuration metadata.
///
/// Maps to vyuh::emitters::PgNotifyConf runtime structure.
#[derive(Debug, FromMeta, Default)]
struct PgNotifyConfMeta {
    /// PostgreSQL LISTEN channel name
    #[darling(default)]
    channel: Option<String>,

    #[darling(default)]
    debounce: Option<String>,

    #[darling(default)]
    debounce_millis: Option<u64>,

    #[darling(default)]
    debounce_secs: Option<u64>,
}

/// Entry point for #[pgnotify] macro.
///
/// Handles both free functions and methods in impl blocks.
pub(crate) fn parse_pgnotify(attr: TokenStream, item: TokenStream) -> TokenStream {
    bundlepart::generate_bundle_part::<PgNotifyConfMeta>(
        attr,
        item,
        "pgnotify",
        build_pgnotify_conf,
    )
}

/// Build PgNotifyConf from parsed metadata and function spec.
///
/// Validates channel name and generates PgNotifyConf construction.
fn build_pgnotify_conf(
    conf: &PgNotifyConfMeta,
    _spec: &FnSpec,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let channel = conf.channel.as_ref().ok_or_else(|| {
        syn::Error::new(
            proc_macro2::Span::call_site(),
            "PostgreSQL LISTEN channel is required. Use: #[pgnotify(channel = \"my_channel\")]",
        )
    })?;

    validate_channel_name(channel)?;

    let debounce = build_debounce(conf)?;

    Ok(quote! {
        ::vyuh::emitters::PgNotifyConf {
            channel: #channel.to_string(),
            debounce: #debounce,
        }
    })
}

fn build_debounce(conf: &PgNotifyConfMeta) -> Result<proc_macro2::TokenStream, syn::Error> {
    let has_duration = conf.debounce_millis.is_some() || conf.debounce_secs.is_some();
    if conf.debounce.is_some() && !has_duration {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "debounce mode requires debounce_millis or debounce_secs",
        ));
    }

    let millis = match (conf.debounce_millis, conf.debounce_secs) {
        (Some(_), Some(_)) => {
            return Err(syn::Error::new(
                proc_macro2::Span::call_site(),
                "use only one of debounce_millis or debounce_secs",
            ));
        }
        (Some(millis), None) => millis,
        (None, Some(secs)) => secs.checked_mul(1000).ok_or_else(|| {
            syn::Error::new(proc_macro2::Span::call_site(), "debounce_secs is too large")
        })?,
        (None, None) => return Ok(quote! { None }),
    };

    if millis == 0 {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "debounce duration must be greater than zero",
        ));
    }

    let mode = conf.debounce.as_deref().unwrap_or("trailing");
    validate_debounce_mode(mode)?;

    Ok(quote! {
        Some(::vyuh::emitters::DebounceConf {
            window: ::std::time::Duration::from_millis(#millis),
            mode: #mode.parse::<::vyuh::emitters::DebounceMode>().unwrap(),
        })
    })
}

fn validate_debounce_mode(mode: &str) -> Result<(), syn::Error> {
    match mode.to_ascii_lowercase().as_str() {
        "leading" | "trailing" | "leading_trailing" => Ok(()),
        other => Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            format!(
                "Invalid debounce mode '{}'. Supported modes: \"leading\", \"trailing\", \"leading_trailing\"",
                other
            ),
        )),
    }
}

/// Validate PostgreSQL channel name.
///
/// Ensures channel name is not empty and follows basic constraints.
fn validate_channel_name(channel: &str) -> Result<(), syn::Error> {
    if channel.trim().is_empty() {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            "PostgreSQL channel name cannot be empty",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debounce_duration_defaults_to_trailing() {
        let conf = PgNotifyConfMeta {
            channel: Some("events".to_string()),
            debounce_millis: Some(250),
            ..PgNotifyConfMeta::default()
        };

        let tokens = build_debounce(&conf).unwrap().to_string();
        assert!(tokens.contains("from_millis"));
        assert!(tokens.contains("250"));
        assert!(tokens.contains("\"trailing\""));
    }

    #[test]
    fn debounce_secs_is_supported() {
        let conf = PgNotifyConfMeta {
            channel: Some("events".to_string()),
            debounce_secs: Some(2),
            debounce: Some("leading_trailing".to_string()),
            ..PgNotifyConfMeta::default()
        };

        let tokens = build_debounce(&conf).unwrap().to_string();
        assert!(tokens.contains("2000"));
        assert!(tokens.contains("\"leading_trailing\""));
    }

    #[test]
    fn debounce_mode_requires_duration() {
        let conf = PgNotifyConfMeta {
            channel: Some("events".to_string()),
            debounce: Some("trailing".to_string()),
            ..PgNotifyConfMeta::default()
        };

        assert!(build_debounce(&conf).is_err());
    }

    #[test]
    fn debounce_rejects_invalid_mode() {
        let conf = PgNotifyConfMeta {
            channel: Some("events".to_string()),
            debounce_millis: Some(250),
            debounce: Some("later".to_string()),
            ..PgNotifyConfMeta::default()
        };

        assert!(build_debounce(&conf).is_err());
    }

    #[test]
    fn debounce_rejects_zero_duration() {
        let conf = PgNotifyConfMeta {
            channel: Some("events".to_string()),
            debounce_millis: Some(0),
            ..PgNotifyConfMeta::default()
        };

        assert!(build_debounce(&conf).is_err());
    }
}
