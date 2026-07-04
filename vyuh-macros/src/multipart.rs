use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Data, DeriveInput, Expr, Fields, GenericArgument, Lit, PathArguments, Type, parse_macro_input,
};

pub fn derive_multipart_data(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let ident = input.ident;
    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    ident,
                    "MultipartData can only be derived for structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                ident,
                "MultipartData can only be derived for structs",
            ));
        }
    };

    let mut spec_parts = Vec::new();
    let mut initializers = Vec::new();

    for field in fields {
        let Some(field_ident) = field.ident.clone() else {
            return Err(syn::Error::new_spanned(
                field,
                "MultipartData can only be derived for structs with named fields",
            ));
        };
        let field_name = field_ident.to_string();
        let upload = UploadAttrs::parse(&field)?;

        match field_kind(&field.ty) {
            FieldKind::File { optional } => {
                spec_parts.push(file_spec(
                    &field_name,
                    upload,
                    optional,
                    false,
                    &field_ident,
                )?);
                initializers.push(file_init(&field_ident, &field_name, optional));
            }
            FieldKind::FileVec => {
                spec_parts.push(file_spec(&field_name, upload, true, true, &field_ident)?);
                initializers.push(file_vec_init(&field_ident, &field_name));
            }
            FieldKind::String { optional } => {
                spec_parts.push(text_spec(&field_name, optional));
                initializers.push(string_init(&field_ident, &field_name, optional));
            }
            FieldKind::Bool => {
                spec_parts.push(text_spec(&field_name, true));
                initializers.push(bool_init(&field_ident, &field_name));
            }
            FieldKind::I64 { optional } => {
                spec_parts.push(text_spec(&field_name, optional));
                initializers.push(i64_init(&field_ident, &field_name, optional));
            }
            FieldKind::Unsupported => {
                return Err(syn::Error::new_spanned(
                    field.ty,
                    "MultipartData derive supports String, Option<String>, bool, i64, Option<i64>, UploadedFile, Option<UploadedFile>, and Vec<UploadedFile>",
                ));
            }
        }
    }

    Ok(quote! {
        impl ::vyuh::routes::multipart::MultipartData for #ident {
            fn multipart_spec() -> ::vyuh::routes::multipart::MultipartSpec {
                let mut spec = ::vyuh::routes::multipart::MultipartSpec::new();
                #(#spec_parts)*
                spec
            }

            fn from_multipart(
                map: ::vyuh::routes::multipart::MultipartMap
            ) -> ::std::result::Result<Self, ::vyuh::routes::multipart::MultipartError> {
                Ok(Self {
                    #(#initializers),*
                })
            }
        }
    })
}

enum FieldKind {
    File { optional: bool },
    FileVec,
    String { optional: bool },
    Bool,
    I64 { optional: bool },
    Unsupported,
}

fn field_kind(ty: &Type) -> FieldKind {
    if type_ends_with(ty, "UploadedFile") {
        return FieldKind::File { optional: false };
    }
    if type_ends_with(ty, "String") {
        return FieldKind::String { optional: false };
    }
    if type_ends_with(ty, "bool") {
        return FieldKind::Bool;
    }
    if type_ends_with(ty, "i64") {
        return FieldKind::I64 { optional: false };
    }
    if let Some(inner) = vec_inner_type(ty) {
        if type_ends_with(inner, "UploadedFile") {
            return FieldKind::FileVec;
        }
    }
    match option_inner_type(ty) {
        Some(inner) if type_ends_with(inner, "UploadedFile") => FieldKind::File { optional: true },
        Some(inner) if type_ends_with(inner, "String") => FieldKind::String { optional: true },
        Some(inner) if type_ends_with(inner, "i64") => FieldKind::I64 { optional: true },
        _ => FieldKind::Unsupported,
    }
}

fn file_spec(
    field_name: &str,
    upload: UploadAttrs,
    optional: bool,
    multiple: bool,
    field_ident: &syn::Ident,
) -> syn::Result<proc_macro2::TokenStream> {
    let content_types = upload.content_types;
    let extensions = upload.extensions;
    let max_size = upload.max_size;
    let sniff = upload.sniff;
    let mut rule = quote! { ::vyuh::routes::multipart::FileRule::new() };
    if !optional {
        rule = quote! { #rule.required() };
    }
    if multiple {
        rule = quote! { #rule.multiple() };
    }
    if !content_types.is_empty() {
        rule = quote! { #rule.content_types([#(#content_types),*]) };
    }
    if !extensions.is_empty() {
        rule = quote! { #rule.extensions([#(#extensions),*]) };
    }
    if let Some(max_size) = max_size {
        rule = quote! { #rule.max_size(#max_size) };
    }
    if let Some(sniff) = sniff {
        if sniff == "image" {
            rule = quote! { #rule.sniff_image() };
        } else {
            return Err(syn::Error::new_spanned(
                field_ident,
                "unsupported upload sniff rule; expected sniff = \"image\"",
            ));
        }
    }
    Ok(quote! {
        spec = spec.file(#field_name, #rule);
    })
}

fn file_init(
    field_ident: &syn::Ident,
    field_name: &str,
    optional: bool,
) -> proc_macro2::TokenStream {
    if optional {
        quote! {
            #field_ident: map.file_opt(#field_name).cloned()
        }
    } else {
        quote! {
            #field_ident: map.file(#field_name)?.clone()
        }
    }
}

fn file_vec_init(field_ident: &syn::Ident, field_name: &str) -> proc_macro2::TokenStream {
    quote! {
        #field_ident: map.files(#field_name).to_vec()
    }
}

fn text_spec(field_name: &str, optional: bool) -> proc_macro2::TokenStream {
    let rule = if optional {
        quote! { ::vyuh::routes::multipart::FieldRule::new() }
    } else {
        quote! { ::vyuh::routes::multipart::FieldRule::new().required() }
    };
    quote! {
        spec = spec.text(#field_name, #rule);
    }
}

fn string_init(
    field_ident: &syn::Ident,
    field_name: &str,
    optional: bool,
) -> proc_macro2::TokenStream {
    if optional {
        quote! {
            #field_ident: map.text_opt(#field_name)
                .filter(|value| !value.is_empty())
                .map(::std::string::ToString::to_string)
        }
    } else {
        quote! {
            #field_ident: map.text(#field_name)?.to_string()
        }
    }
}

fn bool_init(field_ident: &syn::Ident, field_name: &str) -> proc_macro2::TokenStream {
    quote! {
        #field_ident: match map.text_opt(#field_name) {
            None => false,
            Some("true" | "on" | "1") => true,
            Some("false" | "off" | "0") => false,
            Some(value) => {
                return Err(::vyuh::routes::multipart::MultipartError::invalid_field(
                    #field_name,
                    format!("expected boolean value, got '{value}'"),
                ));
            }
        }
    }
}

fn i64_init(
    field_ident: &syn::Ident,
    field_name: &str,
    optional: bool,
) -> proc_macro2::TokenStream {
    if optional {
        quote! {
            #field_ident: match map.text_opt(#field_name).filter(|value| !value.is_empty()) {
                Some(value) => Some(value.parse::<i64>().map_err(|err| {
                    ::vyuh::routes::multipart::MultipartError::invalid_field(#field_name, err.to_string())
                })?),
                None => None,
            }
        }
    } else {
        quote! {
            #field_ident: map.text(#field_name)?.parse::<i64>().map_err(|err| {
                ::vyuh::routes::multipart::MultipartError::invalid_field(#field_name, err.to_string())
            })?
        }
    }
}

#[derive(Default)]
struct UploadAttrs {
    content_types: Vec<String>,
    extensions: Vec<String>,
    sniff: Option<String>,
    max_size: Option<u64>,
}

impl UploadAttrs {
    fn parse(field: &syn::Field) -> syn::Result<Self> {
        let mut attrs = Self::default();
        for attr in &field.attrs {
            if !attr.path().is_ident("upload") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("content_types") {
                    attrs.content_types = parse_string_values(meta.value()?.parse()?)?;
                } else if meta.path.is_ident("extensions") {
                    attrs.extensions = parse_string_values(meta.value()?.parse()?)?;
                } else if meta.path.is_ident("sniff") {
                    let value: Lit = meta.value()?.parse()?;
                    attrs.sniff = Some(match value {
                        Lit::Str(value) => value.value(),
                        other => {
                            return Err(syn::Error::new_spanned(
                                other,
                                "sniff must be a string literal",
                            ));
                        }
                    });
                } else if meta.path.is_ident("max_size") {
                    let value: Lit = meta.value()?.parse()?;
                    attrs.max_size = Some(match value {
                        Lit::Int(value) => value.base10_parse()?,
                        other => {
                            return Err(syn::Error::new_spanned(
                                other,
                                "max_size must be an integer literal",
                            ));
                        }
                    });
                } else {
                    return Err(meta.error("unsupported upload attribute"));
                }
                Ok(())
            })?;
        }
        Ok(attrs)
    }
}

fn parse_string_values(expr: Expr) -> syn::Result<Vec<String>> {
    match expr {
        Expr::Array(array) => array
            .elems
            .into_iter()
            .map(|expr| match expr {
                Expr::Lit(expr) => match expr.lit {
                    Lit::Str(value) => Ok(value.value()),
                    other => Err(syn::Error::new_spanned(other, "expected string literal")),
                },
                other => Err(syn::Error::new_spanned(other, "expected string literal")),
            })
            .collect(),
        Expr::Lit(expr) => match expr.lit {
            Lit::Str(value) => Ok(vec![value.value()]),
            other => Err(syn::Error::new_spanned(other, "expected string literal")),
        },
        other => Err(syn::Error::new_spanned(
            other,
            "expected string literal or array of string literals",
        )),
    }
}

fn type_ends_with(ty: &Type, ident: &str) -> bool {
    match ty {
        Type::Path(path) => path
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == ident),
        _ => false,
    }
}

fn option_inner_type(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    let GenericArgument::Type(inner) = args.args.first()? else {
        return None;
    };
    Some(inner)
}

fn vec_inner_type(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Vec" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    let GenericArgument::Type(inner) = args.args.first()? else {
        return None;
    };
    Some(inner)
}
