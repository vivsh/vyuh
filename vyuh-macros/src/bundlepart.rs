//! Bundle part code generation with spec overrides.
//!
//! Provides infrastructure for parsing macro attributes that combine
//! configuration (route paths, cron expressions, etc.) with spec overrides
//! (argument names, type overrides, return types). All bundle-part macros
//! use this module to generate consistent patch operations.

use darling::FromMeta;
use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;

// ============================================================================
// Core specification types (merged with overrides)
// ============================================================================

/// Function argument with complete metadata.
///
/// Includes both parsed signature info and any override metadata.
#[derive(Debug, Clone)]
pub struct FnArg {
    pub pos: usize,
    pub name: String,
    /// Type override. None means use runtime reflection from signature.
    pub ty: Option<syn::Type>,
    pub description: Option<String>,
}

impl FnArg {
    /// Get type as string for code generation, or "_" if not specified.
    #[allow(dead_code)]
    pub fn type_name(&self) -> Option<String> {
        self.ty.as_ref().map(type_to_string)
    }
}

/// Return type with complete metadata.
///
/// Includes both parsed return type and any override metadata.
#[derive(Debug, Clone)]
pub struct FnReturn {
    /// Return type. Must be Some for appended returns from macro attributes.
    pub ty: Option<syn::Type>,
    pub status: Option<u16>,
    pub description: Option<String>,
}

impl FnReturn {
    /// Get type as string for code generation, or "_" if not specified.
    #[allow(dead_code)]
    pub fn type_name(&self) -> Option<String> {
        self.ty.as_ref().map(type_to_string)
    }
}

/// Complete function specification.
///
/// Single source of truth for all function metadata, including signature,
/// documentation, and any overrides from macro attributes.
#[derive(Debug)]
pub struct FnSpec {
    pub name: String,
    pub args: Vec<FnArg>,
    pub returns: Vec<FnReturn>,
    pub description: Option<String>,
    pub operation_id: Option<String>,
    pub deprecated: bool,
    pub method: bool,
    pub receiver: Option<ReceiverSpec>,
}

/// Method receiver specification.
///
/// Represents different self receiver patterns in method signatures.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ReceiverSpec {
    /// Value receiver: `self` or `mut self`
    Value { mut_self: bool },
    /// Reference receiver: `&self` or `&mut self`
    Ref { mut_ref: bool },
    /// Typed receiver: `self: Type` (rare but valid)
    Typed { ty: String },
}

/// Extract receiver specification from syn::Receiver.
///
/// Identifies the self receiver pattern (value, reference, or typed)
/// and captures mutability information.
fn extract_receiver(rcv: &syn::Receiver) -> ReceiverSpec {
    if rcv.colon_token.is_some() {
        let ty = &rcv.ty;
        return ReceiverSpec::Typed {
            ty: quote!(#ty).to_string(),
        };
    }

    let mut_self = rcv.mutability.is_some();
    if rcv.reference.is_some() {
        ReceiverSpec::Ref { mut_ref: mut_self }
    } else {
        ReceiverSpec::Value { mut_self }
    }
}

/// Extract doc comments from attributes and merge into single string.
///
/// Collects all `#[doc = "..."]` attributes, preserves empty lines and leading
/// whitespace for proper markdown formatting (code blocks, paragraphs, etc.).
/// Strips the conventional single space after `///` but preserves additional indentation.
fn extract_docs(attrs: &[syn::Attribute]) -> Option<String> {
    let docs: Vec<String> = attrs
        .iter()
        .filter(|attr| attr.path().is_ident("doc"))
        .filter_map(|attr| {
            if let syn::Meta::NameValue(meta) = &attr.meta
                && let syn::Expr::Lit(expr_lit) = &meta.value
                && let syn::Lit::Str(lit_str) = &expr_lit.lit
            {
                let line = lit_str.value();
                // Trim trailing whitespace
                let line = line.trim_end();
                // Strip conventional single leading space from `/// ` but keep extra indentation
                let line = line.strip_prefix(' ').unwrap_or(line);
                return Some(line.to_string());
            }
            None
        })
        .collect();

    if docs.is_empty() {
        None
    } else {
        Some(docs.join("\n"))
    }
}

/// Convert syn::Type to string representation for code generation.
///
/// Handles type paths with or without generic arguments, falling back to
/// quote! for complex types. Returns "_" for empty paths.
fn type_to_string(ty: &syn::Type) -> String {
    match ty {
        syn::Type::Path(type_path) => type_path.path.segments.last().map_or_else(
            || "_".to_string(),
            |segment| {
                let type_ident = &segment.ident;
                let type_args = &segment.arguments;
                if type_args.is_empty() {
                    type_ident.to_string()
                } else {
                    format!("{}{}", type_ident, quote! {#type_args})
                }
            },
        ),
        _ => quote! {#ty}.to_string(),
    }
}

/// Extract function arguments from signature.
///
/// Converts syn function inputs to FnArg with positions, excluding self receivers.
/// Sets ty to None - runtime reflection will handle type extraction.
fn extract_args(sig: &syn::Signature) -> Vec<FnArg> {
    sig.inputs
        .iter()
        .filter_map(|arg| match arg {
            syn::FnArg::Receiver(_) => None, // Self goes in receiver, not args
            syn::FnArg::Typed(pat_type) => Some(extract_param_name(&pat_type.pat)),
        })
        .enumerate()
        .map(|(pos, name)| FnArg {
            pos,
            name,
            ty: None, // Runtime reflection handles type extraction
            description: None,
        })
        .collect()
}

/// Extract return types from signature.
fn extract_returns(_sig: &syn::Signature) -> Vec<FnReturn> {
    vec![] // Runtime reflection handles primary return type
}

/// Build FnSpec from syn::Signature and documentation.
///
/// Extracts all function metadata into a unified structure for code generation.
/// The `method` field is set to false by default and should be updated by caller.
fn build_spec_from_signature(sig: &syn::Signature, description: Option<String>) -> FnSpec {
    let receiver = sig.inputs.first().and_then(|arg| match arg {
        syn::FnArg::Receiver(rcv) => Some(extract_receiver(rcv)),
        _ => None,
    });

    FnSpec {
        name: sig.ident.to_string(),
        args: extract_args(sig),
        returns: extract_returns(sig),
        description,
        operation_id: None,
        deprecated: false,
        method: false,
        receiver,
    }
}

/// Extract parameter name from pattern.
///
/// Recursively extracts identifier from nested patterns (type annotations,
/// references, tuples, etc.). Returns "_" for wildcard or unsupported patterns.
fn extract_param_name(pat: &syn::Pat) -> String {
    match pat {
        syn::Pat::Ident(ident) => ident.ident.to_string(),
        syn::Pat::Type(pat_type) => extract_param_name(&pat_type.pat),
        syn::Pat::Reference(pat_ref) => extract_param_name(&pat_ref.pat),
        syn::Pat::Paren(paren) => extract_param_name(&paren.pat),
        syn::Pat::TupleStruct(ts) => ts.elems.first().map_or("_".to_string(), extract_param_name),
        syn::Pat::Tuple(tuple) => tuple
            .elems
            .first()
            .map_or("_".to_string(), extract_param_name),
        syn::Pat::Wild(_) => "_".to_string(),
        _ => "_".to_string(),
    }
}

/// Extract function specification from TokenStream.
///
/// Parses the token stream as either a standalone function (ItemFn) or a method
/// (ImplItemFn), extracting signature and attributes. Returns an error if the
/// item is neither a function nor a method.
///
/// # Parameters
/// * `item` - TokenStream containing the function or method
/// * `attr_name` - Name of the attribute macro (for error messages)
///
/// # Returns
/// * `Ok(FnSpec)` - Successfully extracted function specification
/// * `Err(syn::Error)` - Item is not a function or method
pub(crate) fn extract_func_spec(
    item: &proc_macro2::TokenStream,
    attr_name: &str,
) -> Result<FnSpec, syn::Error> {
    let item = item.clone();
    let (sig, attrs) = if let Ok(func) = syn::parse2::<syn::ItemFn>(item.clone()) {
        (func.sig, func.attrs)
    } else if let Ok(method) = syn::parse2::<syn::ImplItemFn>(item.clone()) {
        (method.sig, method.attrs)
    } else {
        return Err(syn::Error::new(
            proc_macro2::Span::call_site(),
            format!("#[{attr_name}] can only be applied to functions or methods"),
        ));
    };

    let docs = extract_docs(&attrs);

    let mut fn_spec = build_spec_from_signature(&sig, docs);
    // A function is a method if it has a receiver (self parameter)
    fn_spec.method = fn_spec.receiver.is_some();

    Ok(fn_spec)
}

// ============================================================================
// Attribute parsing and application
// ============================================================================

/// Argument override from macro attributes.
#[derive(Debug, Clone, FromMeta)]
struct ArgOverride {
    /// Argument position (0-based)
    #[darling(default)]
    pos: Option<usize>,
    /// Argument name to match
    name: String,
    /// Optional type override  
    #[darling(default)]
    ty: Option<syn::Type>,
    /// Optional description
    #[darling(default)]
    description: Option<String>,
}

/// Return type override from macro attributes.
#[derive(Debug, Clone, FromMeta)]
struct ReturnOverride {
    /// Return type name
    ty: Option<syn::Type>,
    /// Optional HTTP status code
    #[darling(default)]
    status: Option<u16>,
    /// Optional description
    #[darling(default)]
    description: Option<String>,
}

/// Parse macro attributes and apply them to FnSpec.
///
/// Parses arg(...), returns(...), and description overrides from macro
/// attributes and applies them directly to the FnSpec.
pub(crate) fn parse_and_apply_overrides<T: FromMeta + Default>(
    items: &[darling::ast::NestedMeta],
    spec: &mut FnSpec,
) -> darling::Result<T> {
    let mut conf_items = Vec::with_capacity(items.len());
    let mut arg_overrides = Vec::new();
    let mut return_overrides = Vec::new();
    let mut description = None;
    let mut operation_id = None;
    let mut deprecated = false;

    for item in items {
        match item {
            darling::ast::NestedMeta::Meta(syn::Meta::List(list)) if list.path.is_ident("arg") => {
                let nested = darling::ast::NestedMeta::parse_meta_list(list.tokens.clone())
                    .map_err(|e| darling::Error::custom(format!("Failed to parse arg: {}", e)))?;
                let arg_ovr = ArgOverride::from_list(&nested)
                    .map_err(|e| darling::Error::custom(format!("Invalid arg syntax: {}", e)))?;
                arg_overrides.push(arg_ovr);
            }
            darling::ast::NestedMeta::Meta(syn::Meta::List(list))
                if list.path.is_ident("returns") =>
            {
                let nested = darling::ast::NestedMeta::parse_meta_list(list.tokens.clone())
                    .map_err(|e| {
                        darling::Error::custom(format!("Failed to parse returns: {}", e))
                    })?;
                let ret_ovr = ReturnOverride::from_list(&nested).map_err(|e| {
                    darling::Error::custom(format!("Invalid returns syntax: {}", e))
                })?;
                return_overrides.push(ret_ovr);
            }
            darling::ast::NestedMeta::Meta(syn::Meta::NameValue(nv))
                if nv.path.is_ident("description") =>
            {
                if let syn::Expr::Lit(expr_lit) = &nv.value
                    && let syn::Lit::Str(lit_str) = &expr_lit.lit
                {
                    description = Some(lit_str.value());
                }
            }
            darling::ast::NestedMeta::Meta(syn::Meta::NameValue(nv))
                if nv.path.is_ident("operation_id") =>
            {
                if let syn::Expr::Lit(expr_lit) = &nv.value
                    && let syn::Lit::Str(lit_str) = &expr_lit.lit
                {
                    operation_id = Some(lit_str.value());
                }
            }
            darling::ast::NestedMeta::Meta(syn::Meta::Path(path))
                if path.is_ident("deprecated") =>
            {
                deprecated = true;
            }
            _ => conf_items.push(item.clone()),
        }
    }

    // Apply overrides to spec
    apply_arg_overrides(spec, &arg_overrides)?;
    apply_return_overrides(spec, &return_overrides)?;

    if let Some(desc) = description {
        spec.description = Some(desc);
    }
    if let Some(id) = operation_id {
        spec.operation_id = Some(id);
    }
    if deprecated {
        spec.deprecated = true;
    }

    let conf = if conf_items.is_empty() {
        T::default()
    } else {
        T::from_list(&conf_items)?
    };

    Ok(conf)
}

/// Apply argument overrides to spec.
fn apply_arg_overrides(spec: &mut FnSpec, overrides: &[ArgOverride]) -> darling::Result<()> {
    for ovr in overrides {
        let arg = if let Some(pos) = ovr.pos {
            spec.args.get_mut(pos).ok_or_else(|| {
                darling::Error::custom(format!("Argument position {} out of range", pos))
            })?
        } else {
            spec.args
                .iter_mut()
                .find(|a| a.name == ovr.name)
                .ok_or_else(|| {
                    darling::Error::custom(format!("Argument '{}' not found", ovr.name))
                })?
        };

        // Validate name matches if both pos and name specified
        if ovr.pos.is_some() && arg.name != ovr.name {
            return Err(darling::Error::custom(format!(
                "Position and name mismatch: expected '{}', got '{}'",
                arg.name, ovr.name
            )));
        }

        // Apply overrides
        if let Some(ty) = &ovr.ty {
            arg.ty = Some(ty.clone());
        }
        if let Some(ref desc) = ovr.description {
            arg.description = Some(desc.clone());
        }
    }
    Ok(())
}

/// Apply return overrides to spec.
///
/// Validates:
/// - Only one returns() without ty (main return metadata)
/// - Each status code used at most once
fn apply_return_overrides(spec: &mut FnSpec, overrides: &[ReturnOverride]) -> darling::Result<()> {
    let mut main_override_seen = false;
    let mut status_codes = std::collections::HashSet::new();

    for ovr in overrides {
        if ovr.ty.is_none() {
            // No ty = modify signature return metadata
            if main_override_seen {
                return Err(darling::Error::custom(
                    "Multiple returns() without ty field - only one can modify the signature return",
                ));
            }
            main_override_seen = true;

            // Create entry for signature return with just metadata
            spec.returns.push(FnReturn {
                ty: None, // Runtime reflection extracts actual type from signature
                status: ovr.status,
                description: ovr.description.clone(),
            });
        } else {
            // Has ty = append additional return

            // Validation: each status code used at most once
            if let Some(status) = ovr.status
                && !status_codes.insert(status)
            {
                return Err(darling::Error::custom(format!(
                    "Duplicate status code {} - each status can only have one return type",
                    status
                )));
            }

            spec.returns.push(FnReturn {
                ty: ovr.ty.clone(),
                status: ovr.status,
                description: ovr.description.clone(),
            });
        }
    }
    Ok(())
}

/// Generate bundle part with configuration and patch operations.
///
/// Main entry point. Parses, validates, generates bundle registration.
pub fn generate_bundle_part<T: darling::FromMeta + Default>(
    attr: TokenStream,
    item: TokenStream,
    attr_name: &str,
    conf_builder: fn(&T, &FnSpec) -> Result<proc_macro2::TokenStream, syn::Error>,
) -> TokenStream {
    let attr2: proc_macro2::TokenStream = attr.into();
    let item2: proc_macro2::TokenStream = item.into();

    match generate_impl(attr2, item2, attr_name, conf_builder) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// Implementation of bundle part generation.
pub(crate) fn generate_impl<T: darling::FromMeta + Default>(
    attr: proc_macro2::TokenStream,
    item: proc_macro2::TokenStream,
    attr_name: &str,
    conf_builder: fn(&T, &FnSpec) -> Result<proc_macro2::TokenStream, syn::Error>,
) -> Result<proc_macro2::TokenStream, syn::Error> {
    let mut spec = extract_func_spec(&item, attr_name)?;
    let conf = parse_and_apply_metadata::<T>(attr, &mut spec)?;
    let conf_tokens = conf_builder(&conf, &spec)?;
    let patch_chain = build_patch_chain(&spec);

    Ok(emit_registration(
        &spec,
        attr_name,
        &item,
        &conf_tokens,
        &patch_chain,
    ))
}

/// Parse metadata from attribute tokens and apply to spec.
fn parse_and_apply_metadata<T: darling::FromMeta + Default>(
    attr: proc_macro2::TokenStream,
    spec: &mut FnSpec,
) -> Result<T, syn::Error> {
    let nested_meta = darling::ast::NestedMeta::parse_meta_list(attr)
        .map_err(|e| syn::Error::new(e.span(), e))?;

    if nested_meta.is_empty() {
        return Ok(T::default());
    }

    parse_and_apply_overrides::<T>(&nested_meta, spec)
        .map_err(|e| syn::Error::new(Span::call_site(), format!("{}", e)))
}

/// Emit registration function wrapping handler and conf.
fn emit_registration(
    spec: &FnSpec,
    attr_name: &str,
    item: &proc_macro2::TokenStream,
    conf_tokens: &proc_macro2::TokenStream,
    patch_chain: &proc_macro2::TokenStream,
) -> proc_macro2::TokenStream {
    let handler_fn = if spec.method {
        // Method inside impl block
        let method_name = format!("Self::{}", spec.name);
        syn::Ident::new(&method_name, Span::call_site())
    } else {
        // Free function
        syn::Ident::new(&spec.name, Span::call_site())
    };
    let wrapper_fn = syn::Ident::new(&format!("__bundle_part_{}", spec.name), Span::call_site());
    let factory = syn::Ident::new(attr_name, Span::call_site());

    quote! {
        #item

        #[allow(non_snake_case)]
        #[doc(hidden)]
        fn #wrapper_fn() -> ::vyuh::bundles::BundlePart {
            ::vyuh::bundles::#factory(#handler_fn, #conf_tokens) #patch_chain
        }
    }
}

/// Build PatchOp chain from function spec.
///
/// Generates fluent API calls for args and returns with all metadata.
pub(crate) fn build_patch_chain(spec: &FnSpec) -> proc_macro2::TokenStream {
    let arg_patches = spec.args.iter().map(build_arg_patch);

    let return_patches = spec.returns.iter().map(build_return_patch);

    let desc_patch = if let Some(desc) = &spec.description {
        quote! { .description(#desc) }
    } else {
        quote! {}
    };
    let id_patch = if let Some(operation_id) = &spec.operation_id {
        quote! { .operation_id(#operation_id) }
    } else {
        quote! {}
    };
    let deprecated_patch = if spec.deprecated {
        quote! { .deprecated(true) }
    } else {
        quote! {}
    };

    quote! {
        .patch(::vyuh::callables::PatchOp::new()
            #desc_patch
            #id_patch
            #deprecated_patch
            #(#arg_patches)*
            #(#return_patches)*
        )
    }
}

/// Build patch for single argument (uses FnArg.pos field).
pub(crate) fn build_arg_patch(arg: &FnArg) -> proc_macro2::TokenStream {
    let pos = arg.pos;
    let name = &arg.name;
    let idx_lit = syn::LitInt::new(&pos.to_string(), Span::call_site());

    let mut patch = quote! { .arg(#idx_lit).name(#name) };

    // Only add type override if explicitly provided
    if let Some(ty) = &arg.ty {
        patch = quote! { #patch.typed::<#ty>() };
    }

    if let Some(desc) = &arg.description {
        patch = quote! { #patch.doc(#desc) };
    }

    quote! { #patch.done() }
}

/// Build patch for return type.
///
/// - If ty is None: modifies signature return with .ret() (status/doc only)
/// - If ty is Some: appends additional return with .append().typed<T>()
pub(crate) fn build_return_patch(ret: &FnReturn) -> proc_macro2::TokenStream {
    let mut patch = if let Some(ty) = &ret.ty {
        // Append additional return with explicit type
        quote! { .append().typed::<#ty>() }
    } else {
        // Modify signature return (no type needed - runtime extracts it)
        quote! { .ret() }
    };

    if let Some(status) = ret.status {
        let status_lit = syn::LitInt::new(&status.to_string(), Span::call_site());
        patch = quote! { #patch.status(#status_lit) };
    }

    if let Some(desc) = &ret.description {
        patch = quote! { #patch.doc(#desc) };
    }

    quote! { #patch.done() }
}

#[cfg(test)]
#[path = "bundlepart_tests.rs"]
mod bundlepart_tests;
