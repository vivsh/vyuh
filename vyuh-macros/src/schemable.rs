use darling::FromMeta;
use syn::punctuated::Punctuated;
use syn::{
    Attribute, DeriveInput, Error, Expr, ExprArray, Fields, Lit, LitInt, LitStr, Meta, Result,
    Token, Type, spanned::Spanned,
};

// -------------------------------------------------------------------------------------
// Namespace key allowlists (used only for better error messages; does not change acceptance)
// -------------------------------------------------------------------------------------

static VALIDATE_KEYS: &[&str] = &[
    "enum_values",
    "min_length",
    "max_length",
    "exact_length",
    "pattern",
    "email",
    "url",
    "uuid",
    "phone_e164",
    "ipv4",
    "ipv6",
    "date",
    "datetime",
    "min",
    "max",
    "exclusive_min",
    "exclusive_max",
    "multiple_of",
    "min_items",
    "max_items",
    "unique_items",
    "custom",
    "custom_schema",
    "delegate",
];

static COLUMN_KEYS: &[&str] = &[
    "name",
    "type",
    "nullable",
    "primary_key",
    "serial",
    "skip",
    "flatten",
    "json",
    "reference",
    "backref",
    "prefetch",
    "read_only",
    "skip_bind",
    "selectable",
    "insertable",
    "updatable",
    "default",
    "references",
    "references_name",
    "check",
    "index",
    "index_name",
    "index_type",
    "unique",
    "unique_name",
    "unique_group",
];

// -------------------------------------------------------------------------------------
// Small helpers
// -------------------------------------------------------------------------------------

fn parse_ns_list(attr: &Attribute, ns: &str) -> Result<Option<Vec<darling::ast::NestedMeta>>> {
    if !attr.path().is_ident(ns) {
        return Ok(None);
    }

    let nested = match &attr.meta {
        syn::Meta::List(list) => {
            let tokens = normalize_attr_keywords(list.tokens.clone());
            darling::ast::NestedMeta::parse_meta_list(tokens).map_err(|e| {
                augment_darling_error(e.into(), &format!("Error parsing #[{ns}(...)]"))
            })? // preserves spans
        }
        syn::Meta::Path(_) => Vec::new(),
        _ => {
            return Err(Error::new(
                attr.span(),
                format!("expected #[{ns}] or #[{ns}(...)]"),
            ));
        }
    };

    Ok(Some(nested))
}

fn normalize_attr_keywords(tokens: proc_macro2::TokenStream) -> proc_macro2::TokenStream {
    use proc_macro2::{Group, Ident, TokenTree};

    tokens
        .into_iter()
        .map(|token| match token {
            TokenTree::Ident(ident) if ident == "type" => {
                TokenTree::Ident(Ident::new_raw("type", ident.span()))
            }
            TokenTree::Group(group) => {
                let mut out =
                    Group::new(group.delimiter(), normalize_attr_keywords(group.stream()));
                out.set_span(group.span());
                TokenTree::Group(out)
            }
            other => other,
        })
        .collect()
}

fn top_level_key(nm: &darling::ast::NestedMeta) -> Option<&syn::Path> {
    use darling::ast::NestedMeta;
    match nm {
        NestedMeta::Meta(syn::Meta::Path(p)) => Some(p),
        NestedMeta::Meta(syn::Meta::NameValue(nv)) => Some(&nv.path),
        NestedMeta::Meta(syn::Meta::List(ml)) => Some(&ml.path),
        NestedMeta::Lit(_) => None,
    }
}

fn enforce_namespace(
    ns: &str,
    nested: &[darling::ast::NestedMeta],
    allowed: &[&str],
) -> Result<()> {
    for nm in nested {
        let Some(path) = top_level_key(nm) else {
            continue;
        };
        let Some(ident) = path.get_ident() else {
            continue;
        };
        let key = ident.to_string().trim_start_matches("r#").to_string();

        if !allowed.iter().any(|a| *a == key) {
            // "belongs to ..." hint (best-effort)
            let hint = if VALIDATE_KEYS.contains(&key.as_str()) {
                " (belongs to #[validate(...)] )"
            } else if COLUMN_KEYS.contains(&key.as_str()) {
                " (belongs to #[column(...)] )"
            } else {
                ""
            };

            return Err(Error::new(
                ident.span(),
                format!("Unknown option '{key}' in #[{ns}(...)]{hint}"),
            ));
        }
    }
    Ok(())
}

fn parse_enum_attribute(meta: &Meta) -> darling::Result<Vec<Lit>> {
    match meta {
        Meta::List(list) => {
            let nested = list
                .parse_args_with(Punctuated::<Lit, Token![,]>::parse_terminated)
                .map_err(darling::Error::from)?;
            Ok(nested.into_iter().collect())
        }
        _ => Err(darling::Error::custom("Expected list or array for enum").with_span(meta)),
    }
}

#[derive(Debug, Clone, Default)]
pub struct EnumWrapper(pub Vec<Lit>);

impl FromMeta for EnumWrapper {
    fn from_meta(item: &Meta) -> darling::Result<Self> {
        parse_enum_attribute(item).map(EnumWrapper)
    }
}

/// Reference specification for foreign key relationships
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct ReferenceSpec {
    pub from: Option<String>,
    pub to: Option<String>,
    pub join: Option<String>,
    pub on: Vec<JoinOnSpec>,
    pub relation: Option<syn::Path>,
}

#[derive(Debug, Clone, FromMeta)]
#[allow(dead_code)]
pub struct JoinOnSpec {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MarkerSpec {
    pub path: syn::Path,
}

impl FromMeta for MarkerSpec {
    fn from_meta(item: &Meta) -> darling::Result<Self> {
        match item {
            Meta::Path(path) => Ok(Self { path: path.clone() }),
            Meta::List(list) => Self::from_list(
                &darling::ast::NestedMeta::parse_meta_list(list.tokens.clone())
                    .map_err(darling::Error::from)?,
            ),
            Meta::NameValue(name_value) => Self::from_expr(&name_value.value),
        }
    }

    fn from_expr(expr: &Expr) -> darling::Result<Self> {
        match expr {
            Expr::Path(path) => Ok(Self {
                path: path.path.clone(),
            }),
            _ => Err(darling::Error::custom("expected marker type").with_span(expr)),
        }
    }

    fn from_list(items: &[darling::ast::NestedMeta]) -> darling::Result<Self> {
        if items.len() != 1 {
            return Err(darling::Error::custom("expected a single marker type"));
        }
        let Some(item) = items.first() else {
            return Err(darling::Error::custom("expected a marker type"));
        };
        match item {
            darling::ast::NestedMeta::Meta(Meta::Path(path)) => Ok(Self { path: path.clone() }),
            darling::ast::NestedMeta::Meta(meta) => {
                Err(darling::Error::custom("expected marker type").with_span(meta))
            }
            darling::ast::NestedMeta::Lit(lit) => {
                Err(darling::Error::custom("expected marker type").with_span(lit))
            }
        }
    }
}

impl FromMeta for ReferenceSpec {
    fn from_word() -> darling::Result<Self> {
        Ok(ReferenceSpec::default())
    }

    fn from_list(items: &[darling::ast::NestedMeta]) -> darling::Result<Self> {
        let mut spec = ReferenceSpec::default();
        for item in items {
            match item {
                darling::ast::NestedMeta::Meta(meta) => parse_reference_meta(meta, &mut spec)?,
                darling::ast::NestedMeta::Lit(lit) => {
                    return Err(
                        darling::Error::custom("unsupported reference literal").with_span(lit)
                    );
                }
            }
        }
        Ok(spec)
    }
}

fn parse_reference_meta(meta: &Meta, spec: &mut ReferenceSpec) -> darling::Result<()> {
    match meta {
        Meta::Path(path) => {
            spec.relation = Some(path.clone());
            Ok(())
        }
        Meta::NameValue(name_value) => parse_reference_name_value(name_value, spec),
        Meta::List(list) if list.path.is_ident("on") => parse_reference_on(list, spec),
        Meta::List(list) => {
            Err(darling::Error::custom("unsupported reference list").with_span(list))
        }
    }
}

fn parse_reference_name_value(
    name_value: &syn::MetaNameValue,
    spec: &mut ReferenceSpec,
) -> darling::Result<()> {
    let value = lit_str_value(&name_value.value)?;
    if name_value.path.is_ident("from") {
        spec.from = Some(value);
        return Ok(());
    }
    if name_value.path.is_ident("to") {
        spec.to = Some(value);
        return Ok(());
    }
    if name_value.path.is_ident("join") {
        spec.join = Some(value);
        return Ok(());
    }
    Err(darling::Error::custom("unknown reference argument").with_span(name_value))
}

fn lit_str_value(expr: &Expr) -> darling::Result<String> {
    match expr {
        Expr::Lit(lit) => match &lit.lit {
            Lit::Str(value) => Ok(value.value()),
            _ => Err(darling::Error::custom("expected string literal").with_span(expr)),
        },
        _ => Err(darling::Error::custom("expected string literal").with_span(expr)),
    }
}

fn parse_reference_on(list: &syn::MetaList, spec: &mut ReferenceSpec) -> darling::Result<()> {
    let nested = darling::ast::NestedMeta::parse_meta_list(list.tokens.clone())
        .map_err(darling::Error::from)?;
    let on = JoinOnSpec::from_list(&nested)?;
    spec.on.push(on);
    Ok(())
}

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct StringList(pub Vec<String>);

impl StringList {
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[allow(dead_code)]
    pub fn iter(&self) -> impl Iterator<Item = &String> {
        self.0.iter()
    }
}

impl FromMeta for StringList {
    fn from_expr(expr: &Expr) -> darling::Result<Self> {
        match expr {
            Expr::Array(array) => parse_string_array(array).map(Self),
            Expr::Lit(lit) => parse_string_lit(&lit.lit).map(|value| Self(vec![value])),
            _ => Err(darling::Error::custom("expected string or string array").with_span(expr)),
        }
    }
}

fn parse_string_array(array: &ExprArray) -> darling::Result<Vec<String>> {
    array
        .elems
        .iter()
        .map(|expr| match expr {
            Expr::Lit(lit) => parse_string_lit(&lit.lit),
            _ => Err(darling::Error::custom("expected string literal").with_span(expr)),
        })
        .collect()
}

fn parse_string_lit(lit: &Lit) -> darling::Result<String> {
    match lit {
        Lit::Str(value) => Ok(value.value()),
        _ => Err(darling::Error::custom("expected string literal").with_span(lit)),
    }
}

/// Table-level primary key metadata from `#[table(primary_key(...))]`.
#[derive(Debug, Clone, Default, FromMeta)]
#[allow(dead_code)]
pub struct PrimaryKeySpec {
    #[darling(default)]
    pub name: Option<String>,
    #[darling(default)]
    pub columns: StringList,
}

/// Target side of a table-level composite foreign key.
#[derive(Debug, Clone, FromMeta)]
#[allow(dead_code)]
pub struct ForeignKeyTargetSpec {
    pub table: String,
    pub columns: StringList,
}

/// Table-level foreign key metadata from `#[table(foreign_key(...))]`.
#[derive(Debug, Clone, FromMeta)]
#[allow(dead_code)]
pub struct ForeignKeySpec {
    #[darling(default)]
    pub name: Option<String>,
    pub columns: StringList,
    pub references: ForeignKeyTargetSpec,
}

// -------------------------------------------------------------------------------------
// Attributes
// -------------------------------------------------------------------------------------

/// Container-level schema attributes
#[derive(Debug, Default, Clone, FromMeta)]
#[allow(dead_code)]
pub struct ContainerAttrs {
    #[darling(default)]
    pub name: Option<LitStr>,
    #[darling(default)]
    pub table: Option<LitStr>,
    #[darling(default)]
    pub schema: Option<LitStr>,
    #[darling(default)]
    pub primary_key: Option<PrimaryKeySpec>,
    #[darling(default, multiple, rename = "foreign_key")]
    pub foreign_keys: Vec<ForeignKeySpec>,
}

/// Field-level validation attributes from #[validate(...)]
#[derive(Debug, Default, Clone, FromMeta)]
#[allow(dead_code)]
pub struct ValidateAttrs {
    #[darling(default, rename = "enum_values")]
    pub enumeration: EnumWrapper,

    #[darling(default)]
    pub min_length: Option<LitInt>,
    #[darling(default)]
    pub max_length: Option<LitInt>,
    #[darling(default)]
    pub exact_length: Option<LitInt>,

    #[darling(default)]
    pub pattern: Option<LitStr>,

    #[darling(default)]
    pub email: bool,
    #[darling(default)]
    pub url: bool,
    #[darling(default)]
    pub uuid: bool,
    #[darling(default)]
    pub phone_e164: bool,
    #[darling(default)]
    pub ipv4: bool,
    #[darling(default)]
    pub ipv6: bool,
    #[darling(default)]
    pub date: bool,
    #[darling(default)]
    pub datetime: bool,

    #[darling(default)]
    pub min: Option<LitInt>,
    #[darling(default)]
    pub max: Option<LitInt>,
    #[darling(default)]
    pub exclusive_min: bool,
    #[darling(default)]
    pub exclusive_max: bool,
    #[darling(default)]
    pub multiple_of: Option<LitInt>,

    #[darling(default)]
    pub min_items: Option<LitInt>,
    #[darling(default)]
    pub max_items: Option<LitInt>,
    #[darling(default)]
    pub unique_items: bool,

    #[darling(default)]
    pub custom: Option<syn::Path>,
    #[darling(default)]
    pub custom_schema: Option<LitStr>,
    #[darling(default)]
    pub delegate: bool,
}

impl ValidateAttrs {
    pub fn validate(&self) -> Result<()> {
        // Enforce mutually exclusive delegate and custom
        if self.delegate && self.custom.is_some() {
            return Err(Error::new(
                proc_macro2::Span::call_site(),
                "Cannot use both delegate and custom on the same field",
            ));
        }

        if self.custom_schema.is_some() && self.custom.is_none() {
            return Err(Error::new(
                proc_macro2::Span::call_site(),
                "custom_schema requires custom to be set",
            ));
        }

        // Enforce mutually exclusive format flags
        let format_flags = [
            ("email", self.email),
            ("url", self.url),
            ("uuid", self.uuid),
            ("phone_e164", self.phone_e164),
            ("ipv4", self.ipv4),
            ("ipv6", self.ipv6),
            ("date", self.date),
            ("datetime", self.datetime),
        ];
        let active: Vec<&str> = format_flags
            .iter()
            .filter(|(_, v)| *v)
            .map(|(n, _)| *n)
            .collect();
        if active.len() > 1 {
            return Err(Error::new(
                proc_macro2::Span::call_site(),
                format!("Cannot mix format validators: found {}", active.join(", ")),
            ));
        }

        // Enforce exclusive_min requires min
        if self.exclusive_min && self.min.is_none() {
            return Err(Error::new(
                proc_macro2::Span::call_site(),
                "exclusive_min requires min to be set",
            ));
        }

        // Enforce exclusive_max requires max
        if self.exclusive_max && self.max.is_none() {
            return Err(Error::new(
                proc_macro2::Span::call_site(),
                "exclusive_max requires max to be set",
            ));
        }

        // Enforce exact_length is exclusive with min/max_length
        if self.exact_length.is_some() && (self.min_length.is_some() || self.max_length.is_some()) {
            return Err(Error::new(
                proc_macro2::Span::call_site(),
                "exact_length cannot be used with min_length or max_length",
            ));
        }

        if let Some(multiple_of) = &self.multiple_of
            && multiple_of.base10_parse::<i128>()? == 0
        {
            return Err(Error::new(
                multiple_of.span(),
                "multiple_of must not be zero",
            ));
        }

        Ok(())
    }
}

/// Database column metadata from #[column(...)]
#[derive(Debug, Default, Clone, FromMeta)]
#[allow(dead_code)]
pub struct ColumnAttrs {
    #[darling(default)]
    pub name: Option<LitStr>,

    #[darling(default, rename = "r#type")]
    pub sql_type: Option<LitStr>,
    #[darling(default)]
    pub nullable: Option<bool>,

    #[darling(default)]
    pub primary_key: bool,
    #[darling(default)]
    pub serial: bool,

    #[darling(default)]
    pub skip: bool,
    #[darling(default)]
    pub flatten: bool,
    #[darling(default)]
    pub json: bool,
    #[darling(default)]
    pub reference: Option<ReferenceSpec>,
    #[darling(default)]
    pub backref: Option<MarkerSpec>,
    #[darling(default)]
    pub prefetch: Option<MarkerSpec>,

    #[darling(default)]
    pub read_only: bool,
    #[darling(default)]
    pub skip_bind: bool,

    #[darling(default)]
    pub selectable: Option<bool>,
    #[darling(default)]
    pub insertable: Option<bool>,
    #[darling(default)]
    pub updatable: Option<bool>,

    #[darling(default)]
    pub default: Option<LitStr>,
    #[darling(default)]
    pub references: Option<LitStr>,
    #[darling(default)]
    pub references_name: Option<LitStr>,
    #[darling(default)]
    pub check: Option<LitStr>,

    #[darling(default)]
    pub index: bool,
    #[darling(default)]
    pub index_name: Option<LitStr>,
    #[darling(default)]
    pub index_type: Option<LitStr>,

    #[darling(default)]
    pub unique: bool,
    #[darling(default)]
    pub unique_name: Option<LitStr>,

    #[darling(default, multiple, rename = "unique_group")]
    pub unique_groups: Vec<LitStr>,
}

/// Combined field attributes across all namespaces
#[derive(Debug, Clone)]
pub struct FieldMeta {
    pub ident: Option<syn::Ident>,
    pub ty: Type,
    pub validate: ValidateAttrs,
    #[allow(dead_code)]
    pub column: ColumnAttrs,
}

fn augment_darling_error(e: darling::Error, context: &str) -> syn::Error {
    let mut combined_err: Option<syn::Error> = None;

    for single_err in e {
        let syn_err: syn::Error = single_err.into();
        let msg = format!("{}: {}", context, syn_err);
        let enriched = Error::new(syn_err.span(), msg);
        match combined_err {
            Some(ref mut existing) => existing.combine(enriched),
            None => combined_err = Some(enriched),
        }
    }

    combined_err.unwrap_or_else(|| {
        Error::new(
            proc_macro2::Span::call_site(),
            format!("{}: Unknown error", context),
        )
    })
}

impl FieldMeta {
    pub fn from_field(field: &syn::Field) -> Result<Self> {
        let mut validate = ValidateAttrs::default();
        let mut column = ColumnAttrs::default();

        let field_name = field
            .ident
            .as_ref()
            .map(|i| i.to_string())
            .unwrap_or_else(|| "<unnamed>".to_string());

        for attr in &field.attrs {
            if let Some(nested) = parse_ns_list(attr, "validate")? {
                enforce_namespace("validate", &nested, VALIDATE_KEYS)?;
                validate = ValidateAttrs::from_list(&nested).map_err(|e| {
                    augment_darling_error(
                        e,
                        &format!("Error decoding #[validate] on field '{field_name}'"),
                    )
                })?;
                validate
                    .validate()
                    .map_err(|e| Error::new(attr.span(), e.to_string()))?;
            }

            if let Some(nested) = parse_ns_list(attr, "column")? {
                enforce_namespace("column", &nested, COLUMN_KEYS)?;
                column = ColumnAttrs::from_list(&nested).map_err(|e| {
                    augment_darling_error(
                        e,
                        &format!("Error decoding #[column] on field '{field_name}'"),
                    )
                })?;
            }
        }

        Ok(Self {
            ident: field.ident.clone(),
            ty: field.ty.clone(),
            validate,
            column,
        })
    }
}

impl ContainerAttrs {
    pub fn from_attrs(attrs: &[Attribute]) -> Result<Self> {
        let mut out = Self::default();
        for attr in attrs {
            if let Some(nested) = parse_ns_list(attr, "table")? {
                out = ContainerAttrs::from_list(&nested)
                    .map_err(|e| augment_darling_error(e, "Error decoding #[table]"))?;
            }
            if let Some(nested) = parse_ns_list(attr, "schema")? {
                let legacy = ContainerAttrs::from_list(&nested)
                    .map_err(|e| augment_darling_error(e, "Error decoding #[schema]"));
                let legacy = legacy?;
                if out.name.is_none() {
                    out.name = legacy.name.or(legacy.table);
                }
                if out.schema.is_none() {
                    out.schema = legacy.schema;
                }
            }
        }
        Ok(out)
    }
}

#[allow(dead_code)]
pub fn to_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i != 0 {
            out.push('_');
        }
        out.push(ch.to_lowercase().next().unwrap_or(ch));
    }
    out
}

/// Parsed struct information ready for codegen
#[derive(Debug)]
#[allow(dead_code)]
pub struct ParsedStruct {
    pub ident: syn::Ident,
    pub generics: syn::Generics,
    pub container: ContainerAttrs,
    pub fields: Vec<FieldMeta>,
}

impl ParsedStruct {
    pub fn from_derive_input(input: DeriveInput) -> Result<Self> {
        let container = ContainerAttrs::from_attrs(&input.attrs)?;
        let ident = input.ident;
        let generics = input.generics;

        let fields = match input.data {
            syn::Data::Struct(data) => match data.fields {
                Fields::Named(fields) => fields
                    .named
                    .iter()
                    .map(FieldMeta::from_field)
                    .collect::<Result<Vec<_>>>()?,
                _ => {
                    return Err(Error::new_spanned(
                        ident,
                        "only structs with named fields are supported",
                    ));
                }
            },
            _ => return Err(Error::new_spanned(ident, "only structs are supported")),
        };

        Ok(Self {
            ident,
            generics,
            container,
            fields,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use syn::parse_quote;

    #[test]
    fn validate_custom_schema_requires_custom() {
        let input: DeriveInput = parse_quote! {
            struct Test {
                #[validate(custom_schema = "slug")]
                field: String,
            }
        };
        let result = ParsedStruct::from_derive_input(input);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "custom_schema requires custom to be set"
        );
    }

    #[test]
    fn validate_custom_schema_with_custom() {
        let input: DeriveInput = parse_quote! {
            struct Test {
                #[validate(custom = "validate_field", custom_schema = "slug")]
                field: String,
            }
        };
        let parsed = ParsedStruct::from_derive_input(input).unwrap();
        let field = &parsed.fields[0];
        assert_eq!(
            field.validate.custom_schema.as_ref().unwrap().value(),
            "slug"
        );
    }

    #[test]
    fn validate_multiple_of_zero_fails() {
        let input: DeriveInput = parse_quote! {
            struct Test {
                #[validate(multiple_of = 0)]
                field: i32,
            }
        };
        let result = ParsedStruct::from_derive_input(input);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "multiple_of must not be zero"
        );
    }
}
