use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Error, GenericArgument, PathArguments, Type, spanned::Spanned};

#[derive(Clone, Copy, PartialEq, Eq)]
enum FilterOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    Like,
    ILike,
    In,
}

impl FilterOp {
    fn path(self, crate_path: &proc_macro2::TokenStream) -> proc_macro2::TokenStream {
        match self {
            FilterOp::Eq => quote! { #crate_path::db::FilterOp::Eq },
            FilterOp::Ne => quote! { #crate_path::db::FilterOp::Ne },
            FilterOp::Lt => quote! { #crate_path::db::FilterOp::Lt },
            FilterOp::Lte => quote! { #crate_path::db::FilterOp::Lte },
            FilterOp::Gt => quote! { #crate_path::db::FilterOp::Gt },
            FilterOp::Gte => quote! { #crate_path::db::FilterOp::Gte },
            FilterOp::Like => quote! { #crate_path::db::FilterOp::Like },
            FilterOp::ILike => quote! { #crate_path::db::FilterOp::ILike },
            FilterOp::In => quote! { #crate_path::db::FilterOp::In },
        }
    }
}

struct FilterAttr {
    op: FilterOp,
    column: Option<String>,
}

pub fn derive_filterable(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as DeriveInput);
    derive_filterable_impl(&input).into()
}

fn derive_filterable_impl(input: &DeriveInput) -> proc_macro2::TokenStream {
    let ident = &input.ident;
    let mut generics = input.generics.clone();
    let crate_path = get_crate_path();

    let fields = match &input.data {
        syn::Data::Struct(data) => match &data.fields {
            syn::Fields::Named(fields) => &fields.named,
            _ => {
                return Error::new_spanned(ident, "Filterable supports named structs only")
                    .to_compile_error();
            }
        },
        _ => {
            return Error::new_spanned(ident, "Filterable supports structs only")
                .to_compile_error();
        }
    };

    let mut filter_stmts = Vec::new();
    let mut where_predicates = Vec::new();

    for field in fields {
        let Some(field_ident) = field.ident.as_ref() else {
            continue;
        };
        let Some(attr) = parse_filter_attr(field) else {
            continue;
        };
        let attr = match attr {
            Ok(attr) => attr,
            Err(e) => return e.to_compile_error(),
        };
        let column = attr.column.unwrap_or_else(|| field_ident.to_string());
        let op = attr.op.path(&crate_path);
        let ty = &field.ty;

        if let Some(inner) = option_inner_type(ty) {
            if attr.op == FilterOp::In {
                if let Some(vec_inner) = vec_inner_type(inner) {
                    where_predicates.push(quote! {
                        #vec_inner: ::core::clone::Clone
                            + for<'q> ::sqlx::Encode<'q, #crate_path::db::Database>
                            + ::sqlx::Type<#crate_path::db::Database>
                            + ::core::marker::Send
                            + ::core::marker::Sync
                            + 'static
                    });
                    filter_stmts.push(quote! {
                        if let Some(values) = &self.#field_ident {
                            if !values.is_empty() {
                                filters.push(#crate_path::db::FilterPredicate {
                                    column: #column,
                                    op: #op,
                                    value: #crate_path::db::FilterValue::Many(
                                        values.iter().cloned().map(#crate_path::db::ArgValue::new).collect()
                                    ),
                                });
                            }
                        }
                    });
                } else {
                    where_predicates.push(quote! {
                        #inner: ::core::clone::Clone
                            + for<'q> ::sqlx::Encode<'q, #crate_path::db::Database>
                            + ::sqlx::Type<#crate_path::db::Database>
                            + ::core::marker::Send
                            + ::core::marker::Sync
                            + 'static
                    });
                    filter_stmts.push(quote! {
                        if let Some(value) = &self.#field_ident {
                            filters.push(#crate_path::db::FilterPredicate {
                                column: #column,
                                op: #op,
                                value: #crate_path::db::FilterValue::One(#crate_path::db::ArgValue::new(value.clone())),
                            });
                        }
                    });
                }
            } else {
                where_predicates.push(quote! {
                    #inner: ::core::clone::Clone
                        + for<'q> ::sqlx::Encode<'q, #crate_path::db::Database>
                        + ::sqlx::Type<#crate_path::db::Database>
                        + ::core::marker::Send
                        + ::core::marker::Sync
                        + 'static
                });
                filter_stmts.push(quote! {
                    if let Some(value) = &self.#field_ident {
                        filters.push(#crate_path::db::FilterPredicate {
                            column: #column,
                            op: #op,
                            value: #crate_path::db::FilterValue::One(#crate_path::db::ArgValue::new(value.clone())),
                        });
                    }
                });
            }
        } else if attr.op == FilterOp::In {
            if let Some(vec_inner) = vec_inner_type(ty) {
                where_predicates.push(quote! {
                    #vec_inner: ::core::clone::Clone
                        + for<'q> ::sqlx::Encode<'q, #crate_path::db::Database>
                        + ::sqlx::Type<#crate_path::db::Database>
                        + ::core::marker::Send
                        + ::core::marker::Sync
                        + 'static
                });
                filter_stmts.push(quote! {
                    if !self.#field_ident.is_empty() {
                        filters.push(#crate_path::db::FilterPredicate {
                            column: #column,
                            op: #op,
                            value: #crate_path::db::FilterValue::Many(
                                self.#field_ident.iter().cloned().map(#crate_path::db::ArgValue::new).collect()
                            ),
                        });
                    }
                });
            } else {
                return Error::new(
                    field.span(),
                    "the in filter operator requires Vec<T> or Option<Vec<T>>",
                )
                .to_compile_error();
            }
        } else {
            where_predicates.push(quote! {
                #ty: ::core::clone::Clone
                    + for<'q> ::sqlx::Encode<'q, #crate_path::db::Database>
                    + ::sqlx::Type<#crate_path::db::Database>
                    + ::core::marker::Send
                    + ::core::marker::Sync
                    + 'static
            });
            filter_stmts.push(quote! {
                filters.push(#crate_path::db::FilterPredicate {
                    column: #column,
                    op: #op,
                    value: #crate_path::db::FilterValue::One(#crate_path::db::ArgValue::new(self.#field_ident.clone())),
                });
            });
        }
    }

    if !where_predicates.is_empty() {
        let wc = generics.where_clause.get_or_insert(syn::WhereClause {
            where_token: <syn::Token![where]>::default(),
            predicates: syn::punctuated::Punctuated::new(),
        });
        for predicate in where_predicates {
            wc.predicates.push(syn::parse_quote! { #predicate });
        }
    }

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    quote! {
        impl #impl_generics #crate_path::db::Filterable for #ident #ty_generics #where_clause {
            fn filters(&self) -> Vec<#crate_path::db::FilterPredicate> {
                let mut filters = Vec::new();
                #(#filter_stmts)*
                filters
            }
        }
    }
}

fn get_crate_path() -> proc_macro2::TokenStream {
    if std::env::var("CARGO_CRATE_NAME").as_deref() == Ok("vyuh") {
        quote! { crate }
    } else {
        quote! { ::vyuh }
    }
}

fn parse_filter_attr(field: &syn::Field) -> Option<Result<FilterAttr, Error>> {
    let attrs: Vec<_> = field
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("filter"))
        .collect();

    if attrs.is_empty() {
        return None;
    }
    if attrs.len() > 1 {
        return Some(Err(Error::new(
            attrs[1].span(),
            "only one #[filter(...)] attribute is supported per field",
        )));
    }

    let mut op = None;
    let mut column = None;
    let attr = attrs[0];

    let parsed = attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("column") {
            let value: syn::LitStr = meta.value()?.parse()?;
            column = Some(value.value());
            return Ok(());
        }

        let next_op = if meta.path.is_ident("eq") {
            FilterOp::Eq
        } else if meta.path.is_ident("ne") {
            FilterOp::Ne
        } else if meta.path.is_ident("lt") {
            FilterOp::Lt
        } else if meta.path.is_ident("lte") {
            FilterOp::Lte
        } else if meta.path.is_ident("gt") {
            FilterOp::Gt
        } else if meta.path.is_ident("gte") {
            FilterOp::Gte
        } else if meta.path.is_ident("like") {
            FilterOp::Like
        } else if meta.path.is_ident("ilike") {
            FilterOp::ILike
        } else if meta.path.is_ident("in") {
            FilterOp::In
        } else {
            return Err(meta.error("unknown filter option"));
        };

        if op.replace(next_op).is_some() {
            return Err(meta.error("only one filter operator is supported per field"));
        }
        Ok(())
    });

    match parsed {
        Ok(()) => Some(Ok(FilterAttr {
            op: op.unwrap_or(FilterOp::Eq),
            column,
        })),
        Err(e) => Some(Err(e)),
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
    let Some(GenericArgument::Type(inner)) = args.args.first() else {
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
    let Some(GenericArgument::Type(inner)) = args.args.first() else {
        return None;
    };
    Some(inner)
}
