use proc_macro::TokenStream;
use quote::quote;
use syn::DeriveInput;

use crate::schemable::{FieldMeta, ParsedStruct};

pub fn derive_model(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as DeriveInput);
    derive_model_impl(&input).into()
}

fn derive_model_impl(input: &DeriveInput) -> proc_macro2::TokenStream {
    let parsed = match ParsedStruct::from_derive_input(input.clone()) {
        Ok(parsed) => parsed,
        Err(err) => return err.to_compile_error(),
    };

    let ident = &parsed.ident;
    let mut generics = parsed.generics.clone();
    let pk_fields = match primary_key_fields(&parsed) {
        Ok(fields) => fields,
        Err(err) => return err.to_compile_error(),
    };
    if pk_fields.is_empty() {
        return syn::Error::new_spanned(
            ident,
            "Model requires a primary key field or a field named `id`",
        )
        .to_compile_error();
    };

    let pk_idents = pk_fields
        .iter()
        .filter_map(|field| field.ident.as_ref())
        .collect::<Vec<_>>();
    let pk_types = pk_fields.iter().map(|field| &field.ty).collect::<Vec<_>>();
    let pk_columns = pk_fields
        .iter()
        .map(|field| column_name(field))
        .collect::<Vec<_>>();
    let pk_type = match pk_types.as_slice() {
        [ty] => quote! { #ty },
        _ => quote! { (#(#pk_types),*) },
    };
    let pk_value = match pk_idents.as_slice() {
        [ident] => quote! { self.#ident.clone() },
        _ => quote! { (#(self.#pk_idents.clone()),*) },
    };

    let record = crate::scannable::derive_record_impl(input);
    let crate_path = get_crate_path();
    let wc = generics.where_clause.get_or_insert(syn::WhereClause {
        where_token: <syn::Token![where]>::default(),
        predicates: syn::punctuated::Punctuated::new(),
    });
    wc.predicates.push(syn::parse_quote! {
        #pk_type: ::core::clone::Clone + ::core::hash::Hash + ::core::cmp::Eq
    });
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let into_table = gen_into_table(&parsed);

    quote! {
        #record
        #into_table

        impl #impl_generics #crate_path::db::Model for #ident #ty_generics #where_clause {
            type PrimaryKey = #pk_type;

            fn model_schema() -> #crate_path::db::ModelSchema<Self> {
                #crate_path::db::ModelSchema::new(
                    <Self as #crate_path::db::Record>::record_schema(),
                    &[#(#pk_columns),*],
                )
            }

            fn primary_key(&self) -> Self::PrimaryKey {
                #pk_value
            }
        }
    }
}

fn primary_key_fields(parsed: &ParsedStruct) -> syn::Result<Vec<&FieldMeta>> {
    if let Some(primary_key) = &parsed.container.primary_key {
        let mut out = Vec::with_capacity(primary_key.columns.len());
        for column in &primary_key.columns {
            let value = column.value();
            let Some(field) = parsed
                .fields
                .iter()
                .find(|field| column_name(field) == value)
            else {
                return Err(syn::Error::new_spanned(
                    column,
                    format!("table primary_key references unknown column '{value}'"),
                ));
            };
            out.push(field);
        }
        return Ok(out);
    }
    let flagged = parsed
        .fields
        .iter()
        .filter(|field| field.column.primary_key)
        .collect::<Vec<_>>();
    if !flagged.is_empty() {
        return Ok(flagged);
    }
    Ok(parsed
        .fields
        .iter()
        .find(|field| field.ident.as_ref().is_some_and(|ident| ident == "id"))
        .into_iter()
        .collect())
}

fn gen_into_table(parsed: &ParsedStruct) -> proc_macro2::TokenStream {
    let ident = &parsed.ident;
    let crate_path = get_crate_path();
    let generics = parsed.generics.clone();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let table_name = parsed
        .container
        .name
        .as_ref()
        .or(parsed.container.table.as_ref())
        .map(|lit| lit.value())
        .unwrap_or_else(|| crate::schemable::to_snake_case(&ident.to_string()));
    let schema_tokens = gen_schema_call(parsed.container.schema.as_ref().map(|lit| lit.value()));
    let columns = parsed
        .fields
        .iter()
        .filter(|field| !field.column.skip && !field.column.flatten)
        .filter_map(gen_column);
    let indexes = parsed
        .fields
        .iter()
        .filter(|field| !field.column.skip && !field.column.flatten)
        .filter_map(|field| gen_index(field));
    let constraints = parsed
        .fields
        .iter()
        .filter(|field| !field.column.skip && !field.column.flatten)
        .filter_map(|field| gen_unique_constraint(field));
    let primary_key = gen_primary_key(parsed);
    let foreign_keys = parsed.container.foreign_keys.iter().map(gen_foreign_key);

    quote! {
        impl #impl_generics #crate_path::db::IntoTable for #ident #ty_generics #where_clause {
            fn into_table(
                dialect: &#crate_path::db::Dialect,
            ) -> #crate_path::db::Table {
                let mut table = #crate_path::db::TableBuilder::new(#table_name);
                #schema_tokens
                #(#columns)*
                #primary_key
                #(#indexes)*
                #(#constraints)*
                #(#foreign_keys)*
                table.build()
            }
        }
    }
}

fn gen_schema_call(schema: Option<String>) -> Option<proc_macro2::TokenStream> {
    schema.map(|value| quote! { table = table.schema(#value); })
}

fn gen_column(field: &crate::schemable::FieldMeta) -> Option<proc_macro2::TokenStream> {
    let name = column_name(field);
    let ty = &field.ty;
    let nullable_tokens = gen_nullable(
        field,
        field.column.serial || field.column.sql_type.is_some(),
    );
    let pk = field
        .column
        .primary_key
        .then(|| quote! { let c = c.primary_key(); });
    let default = field.column.default.as_ref().map(|lit| {
        let value = lit.value();
        quote! { let c = c.default(#value); }
    });
    let check = field.column.check.as_ref().map(|lit| {
        let value = lit.value();
        quote! { let c = c.check(#value); }
    });
    let references = gen_reference(field);
    let body = quote! {
        #nullable_tokens
        #pk
        #default
        #check
        #references
        c
    };

    if field.column.serial {
        return Some(quote! {
            table = table.column(#name, "bigserial", |c| {
                #body
            });
        });
    }
    if let Some(sql_type) = field.column.sql_type.as_ref().map(|lit| lit.value()) {
        return Some(quote! {
            table = table.column(#name, #sql_type, |c| {
                #body
            });
        });
    }
    Some(quote! {
        table = table.column_from_type::<#ty>(dialect, #name, |c| {
            #body
        });
    })
}

fn gen_nullable(field: &FieldMeta, explicit_type: bool) -> Option<proc_macro2::TokenStream> {
    match field.column.nullable {
        Some(true) => Some(quote! { let c = c.nullable(); }),
        Some(false) => Some(quote! { let c = c.not_null(); }),
        None if explicit_type && is_option_type(&field.ty) => {
            Some(quote! { let c = c.nullable(); })
        }
        None if explicit_type => Some(quote! { let c = c.not_null(); }),
        None => None,
    }
}

fn gen_reference(field: &crate::schemable::FieldMeta) -> Option<proc_macro2::TokenStream> {
    let references = field.column.references.as_ref()?.value();
    let (table, column) = references.rsplit_once('.')?;
    let name = field.column.references_name.as_ref().map(|lit| lit.value());
    Some(match name {
        Some(name) => quote! {
            let c = c.references_named(#name, #table, #column);
        },
        None => quote! {
            let c = c.references(#table, #column);
        },
    })
}

fn gen_index(field: &crate::schemable::FieldMeta) -> Option<proc_macro2::TokenStream> {
    if !field.column.index && field.column.index_name.is_none() {
        return None;
    }
    let name = column_name(field);
    Some(
        match field.column.index_name.as_ref().map(|lit| lit.value()) {
            Some(index_name) => quote! { table = table.index(#index_name, &[#name]); },
            None => quote! { table = table.index_columns(&[#name]); },
        },
    )
}

fn gen_unique_constraint(field: &crate::schemable::FieldMeta) -> Option<proc_macro2::TokenStream> {
    if !field.column.unique && field.column.unique_name.is_none() {
        return None;
    }
    let name = column_name(field);
    Some(
        match field.column.unique_name.as_ref().map(|lit| lit.value()) {
            Some(unique_name) => quote! { table = table.unique(#unique_name, &[#name]); },
            None => quote! { table = table.unique_columns(&[#name]); },
        },
    )
}

fn gen_primary_key(parsed: &ParsedStruct) -> Option<proc_macro2::TokenStream> {
    let primary_key = parsed.container.primary_key.as_ref()?;
    let columns = primary_key.columns.iter().collect::<Vec<_>>();
    Some(match primary_key.name.as_ref() {
        Some(name) => quote! { table = table.primary_key(#name, &[#(#columns),*]); },
        None => quote! { table = table.primary_key_columns(&[#(#columns),*]); },
    })
}

fn gen_foreign_key(foreign_key: &crate::schemable::ForeignKeySpec) -> proc_macro2::TokenStream {
    let columns = foreign_key.columns.iter().collect::<Vec<_>>();
    let target_table = &foreign_key.references.table;
    let target_columns = foreign_key.references.columns.iter().collect::<Vec<_>>();
    match foreign_key.name.as_ref() {
        Some(name) => {
            quote! { table = table.foreign_key_named_columns(#name, &[#(#columns),*], #target_table, &[#(#target_columns),*]); }
        }
        None => {
            quote! { table = table.foreign_key_columns(&[#(#columns),*], #target_table, &[#(#target_columns),*]); }
        }
    }
}

fn column_name(field: &FieldMeta) -> String {
    field
        .column
        .name
        .as_ref()
        .map(|lit| lit.value())
        .or_else(|| field.ident.as_ref().map(|ident| ident.to_string()))
        .unwrap_or_default()
}

fn is_option_type(ty: &syn::Type) -> bool {
    option_inner(&quote!(#ty).to_string().replace(' ', "")).is_some()
}

fn option_inner(text: &str) -> Option<&str> {
    if let Some(inner) = text
        .strip_prefix("Option<")
        .and_then(|s| s.strip_suffix('>'))
    {
        return Some(inner);
    }
    text.strip_prefix("std::option::Option<")
        .and_then(|s| s.strip_suffix('>'))
}

fn get_crate_path() -> proc_macro2::TokenStream {
    if std::env::var("CARGO_CRATE_NAME").as_deref() == Ok("vyuh") {
        quote! { crate }
    } else {
        quote! { ::vyuh }
    }
}
