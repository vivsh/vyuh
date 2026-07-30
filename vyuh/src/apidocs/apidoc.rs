//! OpenAPI 3.0 documentation generator using schemars and openapiv3.
//!
//! This module generates OpenAPI 3.0 specifications from view metadata,
//! leveraging schemars for JSON Schema generation.

use axum::{http::header, response::Html};
use indexmap::IndexMap;
use openapiv3::{
    Components, Encoding, Info, MediaType, OpenAPI, Operation, Parameter, ParameterData,
    ParameterSchemaOrContent, PathItem, Paths, ReferenceOr, RequestBody, Response, Responses,
    StatusCode, Tag,
};

use crate::{
    apidocs::schema::{ComponentRegistry, SchemaConversionError},
    auth::{AuthConf, CredentialType, ProviderDoc, ProviderDocLocation},
    callables::{
        ArgPart, ArgSpec, LayerSpec, MultipartApiField, MultipartApiFieldKind, MultipartApiSpec,
        ReturnPart, ReturnSpec, TypeSchema,
    },
};

/// Available API documentation viewers.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocViewer {
    Swagger,
    Redoc,
    Rapidoc,
}

/// OpenAPI document version emitted by the generator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenApiVersion {
    /// OpenAPI 3.0.3 for broad client-generator compatibility.
    V30,
    /// OpenAPI 3.1.0 for newer tooling.
    V31,
}

impl OpenApiVersion {
    fn as_str(self) -> &'static str {
        match self {
            Self::V30 => "3.0.3",
            Self::V31 => "3.1.0",
        }
    }
}

/// Metadata for API documentation.
#[derive(Debug, Clone)]
pub struct ApiMeta {
    pub version: String,
    pub title: String,
    pub description: Option<String>,
    pub tags: Vec<TagInfo>,
}

impl Default for ApiMeta {
    fn default() -> Self {
        Self {
            version: "0.0.1".to_string(),
            title: "API".to_string(),
            description: None,
            tags: Vec::new(),
        }
    }
}

impl ApiMeta {
    /// Add tags to the API metadata.
    pub fn with_tags(mut self, tags: Vec<TagInfo>) -> Self {
        self.tags = tags;
        self
    }
}

/// Tag information for organizing API endpoints.
#[derive(Debug, Clone)]
pub struct TagInfo {
    pub name: String,
    pub description: Option<String>,
}

/// Generates OpenAPI 3.0 documentation from view metadata.
/// Uses openapiv3 crate and schemars for JSON Schema generation.
#[derive(Debug, Clone)]
pub struct ApiDocGenerator {
    pub meta: ApiMeta,
    auth: Option<AuthConf>,
    version: OpenApiVersion,
}

impl Default for ApiDocGenerator {
    fn default() -> Self {
        Self {
            meta: ApiMeta::default(),
            auth: None,
            version: OpenApiVersion::V30,
        }
    }
}

impl ApiDocGenerator {
    /// Create a new ApiDocGenerator with the given API metadata.
    pub fn new(meta: ApiMeta) -> Self {
        Self {
            meta,
            auth: None,
            version: OpenApiVersion::V30,
        }
    }

    /// Use site authentication config when building security schemes.
    pub fn with_auth(mut self, auth: AuthConf) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Select the OpenAPI document version to emit.
    pub fn with_version(mut self, version: OpenApiVersion) -> Self {
        self.version = version;
        self
    }

    /// Generate OpenAPI 3.0 specification from view metadata.
    ///
    /// # Errors
    /// Returns an error if any schema conversion fails.
    pub fn generate(
        &self,
        views: &[&crate::callables::Operation],
    ) -> Result<OpenAPI, SchemaConversionError> {
        // Create registry for schema components
        let mut registry = ComponentRegistry::new();

        // Build paths from views
        let mut paths_map: IndexMap<String, ReferenceOr<PathItem>> = IndexMap::new();

        let mut sorted_views = views.to_vec();
        sorted_views.sort_by(|a, b| a.path.cmp(&b.path).then_with(|| a.name.cmp(&b.name)));

        for view in sorted_views {
            add_view_to_paths(&mut paths_map, view, &mut registry, self.auth.as_ref())?;
        }

        // Build tags
        let mut sorted_tags = self.meta.tags.iter().collect::<Vec<_>>();
        sorted_tags.sort_by(|a, b| a.name.cmp(&b.name));
        let tags: Vec<Tag> = sorted_tags
            .into_iter()
            .map(|t| Tag {
                name: t.name.clone(),
                description: t.description.clone(),
                external_docs: None,
                extensions: IndexMap::new(),
            })
            .collect();

        // Extract security scheme names before consuming registry
        let security_scheme_names = registry.get_security_scheme_names();

        // Get component schemas from registry
        let components_schemas = registry.into_components_schemars()?;

        // Build security schemes from registered scheme names
        let security_schemes = build_security_schemes(&security_scheme_names, self.auth.as_ref());

        let components = if components_schemas.is_empty() && security_schemes.is_empty() {
            None
        } else {
            Some(Components {
                schemas: components_schemas,
                security_schemes,
                ..Default::default()
            })
        };

        Ok(OpenAPI {
            openapi: self.version.as_str().to_string(),
            info: Info {
                title: self.meta.title.clone(),
                description: self.meta.description.clone(),
                terms_of_service: None,
                contact: None,
                license: None,
                version: self.meta.version.clone(),
                extensions: IndexMap::new(),
            },
            servers: vec![],
            paths: Paths {
                paths: paths_map,
                extensions: IndexMap::new(),
            },
            components,
            security: None,
            tags: if tags.is_empty() { vec![] } else { tags },
            external_docs: None,
            extensions: IndexMap::new(),
        })
    }

    /// Serve API documentation viewer HTML.
    ///
    /// Hidden for v0 because built-in documentation viewers are not
    /// release-ready. OpenAPI JSON spec generation is the supported public
    /// surface.
    #[doc(hidden)]
    pub fn serve_doc(path: &str, viewer: DocViewer) -> Html<String> {
        match viewer {
            DocViewer::Swagger => Self::serve_swagger(path),
            DocViewer::Redoc => Self::serve_redoc(path),
            DocViewer::Rapidoc => Self::serve_rapidoc(path),
        }
    }

    fn serve_rapidoc(path: &str) -> Html<String> {
        let html = include_str!("templates/rapidoc.html").replace("###__PATH__###", path);
        Html(html)
    }

    fn serve_redoc(path: &str) -> Html<String> {
        let html = include_str!("templates/redoc.html").replace("###__PATH__###", path);
        Html(html)
    }

    fn serve_swagger(path: &str) -> Html<String> {
        let html = include_str!("templates/swagger.html").replace("###__PATH__###", path);
        Html(html)
    }

    /// Create a router serving OpenAPI docs with Swagger, Redoc, and RapiDoc viewers.
    ///
    /// Hidden for v0 because built-in documentation viewers are not
    /// release-ready. OpenAPI JSON spec generation is the supported public
    /// surface.
    ///
    /// # Errors
    /// Returns an error if the OpenAPI spec cannot be generated or serialized.
    #[doc(hidden)]
    pub fn views(
        &self,
        doc_url: &str,
        api_url: &str,
        views: &[&crate::callables::Operation],
    ) -> Result<axum::Router<crate::Site>, ApiDocError> {
        use axum::http::StatusCode;

        let openapi_doc = self.generate(views)?;
        let openapi_json =
            serde_json::to_string(&openapi_doc).map_err(ApiDocError::JsonSerialization)?;

        let doc_url_owned = doc_url.to_string();
        let api_url_owned = api_url.to_string();

        Ok(axum::Router::new()
            .route(
                &api_url_owned,
                axum::routing::get(move || async move {
                    (
                        StatusCode::OK,
                        [("content-type", "application/json")],
                        openapi_json.clone(),
                    )
                }),
            )
            .route(
                &format!("{}/swagger", doc_url_owned),
                axum::routing::get({
                    let api_url = api_url_owned.clone();
                    move || async move { Self::serve_swagger(&api_url) }
                }),
            )
            .route(
                &format!("{}/redoc", doc_url_owned),
                axum::routing::get({
                    let api_url = api_url_owned.clone();
                    move || async move { Self::serve_redoc(&api_url) }
                }),
            )
            .route(
                &format!("{}/rapidoc", doc_url_owned),
                axum::routing::get({
                    let api_url = api_url_owned.clone();
                    move || async move { Self::serve_rapidoc(&api_url) }
                }),
            ))
    }
}

/// Errors that can occur when building API documentation.
#[derive(Debug, thiserror::Error)]
pub enum ApiDocError {
    #[error("schema conversion failed: {0}")]
    SchemaConversion(#[from] SchemaConversionError),
    #[error("failed to serialize OpenAPI spec: {0}")]
    JsonSerialization(#[source] serde_json::Error),
}

/// Convert TypeSchema to OpenAPI schema via JSON serialization.
fn type_schema_to_openapi(
    schema: &TypeSchema,
    registry: &mut ComponentRegistry,
) -> Result<ReferenceOr<openapiv3::Schema>, SchemaConversionError> {
    let schemars_schema = schema.schema(registry.generator_mut());

    let json_value = serde_json::to_value(&schemars_schema).map_err(|e| {
        SchemaConversionError::Serialization {
            name: "<inline>".to_string(),
            source: e,
        }
    })?;

    convert_json_value_to_openapi(json_value, "<inline>")
}

fn type_schema_to_openapi_with_multipart(
    schema: &TypeSchema,
    multipart: &MultipartApiSpec,
    registry: &mut ComponentRegistry,
) -> Result<ReferenceOr<openapiv3::Schema>, SchemaConversionError> {
    let generator = registry.generator_mut();
    let schemars_schema = schema.schema(generator);
    let mut json_value = serde_json::to_value(&schemars_schema).map_err(|e| {
        SchemaConversionError::Serialization {
            name: "<inline>".to_string(),
            source: e,
        }
    })?;

    if let Some(name) = component_ref_name(&json_value) {
        if let Some(definition) = generator.definitions_mut().get_mut(name) {
            apply_multipart_to_schema(definition, multipart);
        }
    } else {
        apply_multipart_to_schema(&mut json_value, multipart);
    }

    convert_json_value_to_openapi(json_value, "<inline>")
}

fn component_ref_name(value: &serde_json::Value) -> Option<&str> {
    value
        .get("$ref")
        .and_then(serde_json::Value::as_str)
        .and_then(|reference| {
            reference
                .strip_prefix("#/$defs/")
                .or_else(|| reference.strip_prefix("#/definitions/"))
                .or_else(|| reference.strip_prefix("#/components/schemas/"))
        })
}

fn apply_multipart_to_schema(value: &mut serde_json::Value, multipart: &MultipartApiSpec) {
    let Some(map) = value.as_object_mut() else {
        return;
    };
    apply_required_fields(map, multipart);
    let Some(properties) = map
        .entry("properties")
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
    else {
        return;
    };
    for field in &multipart.fields {
        apply_multipart_field(properties, field);
    }
}

fn apply_required_fields(
    map: &mut serde_json::Map<String, serde_json::Value>,
    multipart: &MultipartApiSpec,
) {
    let required = multipart
        .fields
        .iter()
        .filter(|field| field.required)
        .map(|field| serde_json::Value::String(field.name.clone()))
        .collect::<Vec<_>>();
    if required.is_empty() {
        map.remove("required");
    } else {
        map.insert("required".to_string(), serde_json::Value::Array(required));
    }
}

fn apply_multipart_field(
    properties: &mut serde_json::Map<String, serde_json::Value>,
    field: &MultipartApiField,
) {
    let mut schema = match field.kind {
        MultipartApiFieldKind::File if field.multiple => serde_json::json!({
            "type": "array",
            "items": { "type": "string", "format": "binary" }
        }),
        MultipartApiFieldKind::File => serde_json::json!({
            "type": "string",
            "format": "binary"
        }),
        MultipartApiFieldKind::Text => properties
            .get(&field.name)
            .cloned()
            .unwrap_or_else(|| serde_json::json!({ "type": "string" })),
    };
    apply_multipart_field_extensions(&mut schema, field);
    properties.insert(field.name.clone(), schema);
}

fn apply_multipart_field_extensions(schema: &mut serde_json::Value, field: &MultipartApiField) {
    let Some(map) = schema.as_object_mut() else {
        return;
    };
    if let Some(max_length) = field.max_length {
        map.insert("maxLength".to_string(), serde_json::Value::from(max_length));
    }
    if let Some(max_bytes) = field.max_bytes {
        map.insert(
            "x-vyuh-upload-max-bytes".to_string(),
            serde_json::Value::from(max_bytes),
        );
    }
    insert_string_array(map, "x-vyuh-upload-content-types", &field.content_types);
    insert_string_array(map, "x-vyuh-upload-extensions", &field.extensions);
    if let Some(sniff) = &field.sniff {
        map.insert(
            "x-vyuh-upload-sniff".to_string(),
            serde_json::Value::String(sniff.clone()),
        );
    }
    if field.multiple {
        map.insert(
            "x-vyuh-upload-multiple".to_string(),
            serde_json::Value::Bool(true),
        );
    }
}

fn insert_string_array(
    map: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    values: &[String],
) {
    if !values.is_empty() {
        map.insert(key.to_string(), serde_json::json!(values));
    }
}

/// Convert JSON value (from schemars) to OpenAPI schema.
fn convert_json_value_to_openapi(
    mut json_value: serde_json::Value,
    name: &str,
) -> Result<ReferenceOr<openapiv3::Schema>, SchemaConversionError> {
    if let Some(ref_str) = json_value.get("$ref").and_then(|v| v.as_str()) {
        let openapi_ref = ref_str
            .replace("#/$defs/", "#/components/schemas/")
            .replace("#/definitions/", "#/components/schemas/");
        return Ok(ReferenceOr::Reference {
            reference: openapi_ref,
        });
    }

    transform_for_openapi(&mut json_value);

    let schema = serde_json::from_value::<openapiv3::Schema>(json_value).map_err(|e| {
        SchemaConversionError::Deserialization {
            name: name.to_string(),
            source: e,
        }
    })?;

    Ok(ReferenceOr::Item(schema))
}

/// Transform JSON Schema to OpenAPI 3.0 in-place.
fn transform_for_openapi(val: &mut serde_json::Value) {
    if val.as_bool() == Some(true) {
        *val = serde_json::json!({});
        return;
    }

    if val.as_bool() == Some(false) {
        *val = serde_json::json!({"not": {}});
        return;
    }

    if let serde_json::Value::Object(map) = val {
        rewrite_schema_ref(map);

        if let Some(type_val) = map.get("type").and_then(|v| v.as_array()).cloned() {
            transform_type_array(map, &type_val);
        }

        if let Some(serde_json::Value::Object(props)) = map.get_mut("properties") {
            for (_prop_name, prop_schema) in props.iter_mut() {
                transform_for_openapi(prop_schema);
            }
        }

        for key in [
            "items",
            "additionalProperties",
            "not",
            "$defs",
            "definitions",
        ] {
            if let Some(nested) = map.get_mut(key) {
                transform_for_openapi(nested);
            }
        }

        for key in ["allOf", "anyOf", "oneOf"] {
            if let Some(serde_json::Value::Array(schemas)) = map.get_mut(key) {
                for schema in schemas {
                    transform_for_openapi(schema);
                }
            }
        }
    } else if let serde_json::Value::Array(arr) = val {
        for item in arr {
            transform_for_openapi(item);
        }
    }
}

fn rewrite_schema_ref(map: &mut serde_json::Map<String, serde_json::Value>) {
    let Some(reference) = map.get_mut("$ref").and_then(|value| value.as_str()) else {
        return;
    };
    let openapi_ref = reference
        .replace("#/$defs/", "#/components/schemas/")
        .replace("#/definitions/", "#/components/schemas/");
    map.insert("$ref".to_string(), serde_json::Value::String(openapi_ref));
}

/// Transform type array to OpenAPI nullable format.
fn transform_type_array(
    map: &mut serde_json::Map<String, serde_json::Value>,
    types: &[serde_json::Value],
) {
    let (has_null, non_null): (Vec<_>, Vec<_>) =
        types.iter().partition(|v| v.as_str() == Some("null"));

    match non_null.len() {
        0 => {}
        1 => {
            map.insert("type".to_string(), non_null[0].clone());
            if !has_null.is_empty() {
                map.insert("nullable".to_string(), serde_json::Value::Bool(true));
            }
        }
        _ => {
            let any_of: Vec<_> = non_null
                .iter()
                .map(|t| serde_json::json!({"type": t}))
                .collect();
            map.remove("type");
            map.insert("anyOf".to_string(), serde_json::Value::Array(any_of));
            if !has_null.is_empty() {
                map.insert("nullable".to_string(), serde_json::Value::Bool(true));
            }
        }
    }
}

/// Add a view to the OpenAPI paths collection.
fn add_view_to_paths(
    paths: &mut IndexMap<String, ReferenceOr<PathItem>>,
    view: &crate::callables::Operation,
    registry: &mut ComponentRegistry,
    auth: Option<&AuthConf>,
) -> Result<(), SchemaConversionError> {
    let path_key = view.path.to_string();

    let path_item = paths
        .entry(path_key)
        .or_insert_with(|| ReferenceOr::Item(PathItem::default()));

    let operation = build_operation(view, registry, auth)?;
    let method_names = view.http_methods();

    if let ReferenceOr::Item(item) = path_item {
        set_operations_for_methods(item, &method_names, &view.path, operation);
    }

    Ok(())
}

#[derive(Clone, Copy)]
struct FlatArgPart<'a> {
    part: &'a ArgPart,
    optional: bool,
    fallible: bool,
}

impl FlatArgPart<'_> {
    fn suppresses_responses(self) -> bool {
        self.optional || self.fallible
    }
}

fn flatten_arg_part(part: &ArgPart) -> Vec<FlatArgPart<'_>> {
    let mut output = Vec::new();
    push_flattened_arg_part(part, false, false, &mut output);
    output
}

fn push_flattened_arg_part<'a>(
    part: &'a ArgPart,
    optional: bool,
    fallible: bool,
    output: &mut Vec<FlatArgPart<'a>>,
) {
    match part {
        ArgPart::Composite(parts) => {
            for nested in parts {
                push_flattened_arg_part(nested, optional, fallible, output);
            }
        }
        ArgPart::Optional(nested) => {
            push_flattened_arg_part(nested, true, fallible, output);
        }
        ArgPart::Fallible(nested) => {
            push_flattened_arg_part(nested, optional, true, output);
        }
        other => output.push(FlatArgPart {
            part: other,
            optional,
            fallible,
        }),
    }
}

/// Set operation for all HTTP methods in the MethodFilter.
fn set_operations_for_methods(
    item: &mut PathItem,
    method_names: &[&str],
    path: &str,
    operation: Operation,
) {
    let is_multiple = method_names.len() > 1;
    for method in method_names {
        let mut op = operation.clone();
        fill_operation_docs(&mut op, method, path);
        if is_multiple {
            op.operation_id = Some(format!(
                "{}_{}",
                operation.operation_id.clone().unwrap_or_default(),
                method.to_lowercase()
            ));
        }
        match *method {
            "GET" => item.get = Some(op),
            "POST" => item.post = Some(op),
            "PUT" => item.put = Some(op),
            "DELETE" => item.delete = Some(op),
            "PATCH" => item.patch = Some(op),
            "HEAD" => item.head = Some(op),
            "OPTIONS" => item.options = Some(op),
            "TRACE" => item.trace = Some(op),
            _ => {}
        }
    }
}

fn fill_operation_docs(op: &mut Operation, method: &str, path: &str) {
    if matches!(method, "GET" | "HEAD" | "OPTIONS" | "TRACE") {
        op.extensions.shift_remove("x-vyuh-csrf-header");
    }
    if op.summary.is_none() {
        op.summary = op
            .operation_id
            .as_deref()
            .map(summary_from_id)
            .or_else(|| Some(route_label(method, path)));
    }
    if op.description.is_none() {
        op.description = Some(route_description(method, path));
    }
}

fn route_label(method: &str, path: &str) -> String {
    format!("{method} {path}")
}

fn route_description(method: &str, path: &str) -> String {
    format!("Application route registered for `{method} {path}`.")
}

fn summary_from_id(id: &str) -> String {
    id.trim_end_matches("_get")
        .trim_end_matches("_post")
        .trim_end_matches("_put")
        .trim_end_matches("_delete")
        .trim_end_matches("_patch")
        .replace(['_', '-'], " ")
        .split_whitespace()
        .map(capitalize)
        .collect::<Vec<_>>()
        .join(" ")
}

fn capitalize(word: &str) -> String {
    let mut chars = word.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    first.to_uppercase().chain(chars).collect()
}

fn register_security(
    registry: &mut ComponentRegistry,
    scheme: &str,
    scopes: &[String],
    join_all: bool,
    auth: Option<&AuthConf>,
    optional: bool,
) {
    if scheme == "vyuhAuth" {
        let Some(auth) = auth else {
            registry.register_security(scheme.to_string(), scopes, false, optional);
            return;
        };
        for provider in auth.provider_docs() {
            registry.register_security(provider_scheme_name(&provider), scopes, false, optional);
        }
        return;
    }
    if scheme == "bearerAuth" {
        registry.register_security(scheme.to_string(), scopes, false, optional);
        return;
    }
    registry.register_security(scheme.to_string(), scopes, join_all, optional);
}

fn build_operation_security(
    registry: &mut ComponentRegistry,
) -> Option<Vec<IndexMap<String, Vec<String>>>> {
    let requirements = registry.drain_operation_security();
    if requirements.is_empty() {
        return None;
    }

    let mut joined = IndexMap::new();
    let mut output = Vec::with_capacity(requirements.len());
    let all_optional = requirements.iter().all(|req| req.optional);
    if all_optional {
        output.push(IndexMap::new());
    }
    for req in requirements {
        if req.join_all {
            joined.insert(req.scheme, req.scopes);
        } else {
            let mut item = IndexMap::new();
            item.insert(req.scheme, req.scopes);
            output.push(item);
        }
    }
    if !joined.is_empty() {
        output.push(joined);
    }
    Some(output)
}

/// Build operation from view metadata.
fn build_operation(
    view: &crate::callables::Operation,
    registry: &mut ComponentRegistry,
    auth: Option<&AuthConf>,
) -> Result<Operation, SchemaConversionError> {
    // Build parameters from both args and layer specs
    let mut parameters = build_params(&view.args, registry, auth)?;

    // Process layer specs - they may contribute parameters (e.g., auth headers)
    for layer in &view.layers {
        for part in &layer.parts {
            for flat in flatten_arg_part(part) {
                if let Some(param) = build_layer_param(layer, flat, registry, auth)? {
                    parameters.push(ReferenceOr::Item(param));
                }
            }
        }
    }

    let request_body = build_request_body(&view.args, registry)?;
    let responses = build_responses(&view.args, &view.layers, &view.returns, registry)?;
    let tags: Vec<String> = view.tags.iter().map(|s| s.to_string()).collect();

    let security = build_operation_security(registry);

    let mut extensions = IndexMap::new();
    if let Some(audience) = view.audience() {
        extensions.insert(
            "x-vyuh-audience".to_owned(),
            serde_json::Value::String(audience.to_owned()),
        );
        if let Some(auth) = auth.filter(|_| operation_has_unsafe_method(view)) {
            let headers = auth
                .provider_docs()
                .into_iter()
                .filter_map(|provider| provider.csrf_header)
                .collect::<std::collections::BTreeSet<_>>();
            if !headers.is_empty() {
                extensions.insert(
                    "x-vyuh-csrf-header".to_owned(),
                    serde_json::Value::Array(headers.into_iter().map(Into::into).collect()),
                );
            }
        }
    }
    Ok(Operation {
        tags,
        summary: view.summary.as_ref().map(|s| s.to_string()),
        description: view.description.as_ref().map(|s| s.to_string()),
        external_docs: None,
        operation_id: Some(view.openapi_id.as_deref().unwrap_or(&view.name).to_string()),
        parameters,
        request_body,
        responses,
        callbacks: IndexMap::new(),
        deprecated: view.deprecated,
        security,
        servers: vec![],
        extensions,
    })
}

fn operation_has_unsafe_method(operation: &crate::callables::Operation) -> bool {
    operation
        .methods
        .to_vec()
        .iter()
        .any(|method| !matches!(*method, "GET" | "HEAD" | "OPTIONS" | "TRACE"))
}

/// Build parameters from argument specifications.
fn build_params(
    args: &[ArgSpec],
    registry: &mut ComponentRegistry,
    auth: Option<&AuthConf>,
) -> Result<Vec<ReferenceOr<Parameter>>, SchemaConversionError> {
    let mut result = Vec::new();

    for arg in args {
        for part in flatten_arg_part(&arg.part) {
            if let Some(param) = build_param(arg, part, registry, auth)? {
                result.push(ReferenceOr::Item(param));
            }
        }
    }

    Ok(result)
}

/// Build parameter from layer specification.
fn build_layer_param(
    layer: &crate::callables::LayerSpec,
    flat: FlatArgPart<'_>,
    registry: &mut ComponentRegistry,
    auth: Option<&AuthConf>,
) -> Result<Option<Parameter>, SchemaConversionError> {
    let (schema, location, required) = match flat.part {
        ArgPart::Cookie(st) => (st, "cookie", false),
        ArgPart::Header(st) => (st, "header", false),
        ArgPart::Path(st) => (st, "path", true),
        ArgPart::Query(st) => (st, "query", false),
        ArgPart::Body(_, _)
        | ArgPart::BodyWith { .. }
        | ArgPart::Response(_)
        | ArgPart::Authentication => return Ok(None),
        ArgPart::Composite(_) | ArgPart::Optional(_) | ArgPart::Fallible(_) => return Ok(None),
        ArgPart::Security {
            scheme,
            scopes,
            join_all,
        } => {
            let scopes_str: Vec<String> = scopes.iter().map(|s| s.to_string()).collect();
            register_security(
                registry,
                scheme.as_ref(),
                &scopes_str,
                *join_all,
                auth,
                flat.optional,
            );
            return Ok(None);
        }
        ArgPart::Zone | ArgPart::Ignore => return Ok(None),
    };

    let openapi_schema = type_schema_to_openapi(schema, registry)?;

    let parameter_data = ParameterData {
        name: layer.name.clone(),
        description: layer.description.clone(),
        required,
        deprecated: None,
        format: ParameterSchemaOrContent::Schema(openapi_schema),
        example: None,
        examples: IndexMap::new(),
        explode: None,
        extensions: IndexMap::new(),
    };

    let param = match location {
        "query" => Parameter::Query {
            parameter_data,
            allow_reserved: false,
            style: openapiv3::QueryStyle::Form,
            allow_empty_value: None,
        },
        "path" => Parameter::Path {
            parameter_data,
            style: openapiv3::PathStyle::Simple,
        },
        "header" => Parameter::Header {
            parameter_data,
            style: openapiv3::HeaderStyle::Simple,
        },
        "cookie" => Parameter::Cookie {
            parameter_data,
            style: openapiv3::CookieStyle::Form,
        },
        _ => return Ok(None),
    };

    Ok(Some(param))
}

/// Build a single parameter from argument specification.
fn build_param(
    arg: &ArgSpec,
    flat: FlatArgPart<'_>,
    registry: &mut ComponentRegistry,
    auth: Option<&AuthConf>,
) -> Result<Option<Parameter>, SchemaConversionError> {
    let (schema, location, required) = match flat.part {
        ArgPart::Cookie(st) => (st, "cookie", false),
        ArgPart::Header(st) => (st, "header", false),
        ArgPart::Path(st) => (st, "path", true),
        ArgPart::Query(st) => (st, "query", false),
        ArgPart::Body(_, _)
        | ArgPart::BodyWith { .. }
        | ArgPart::Response(_)
        | ArgPart::Authentication => {
            return Ok(None);
        }
        ArgPart::Composite(_) | ArgPart::Optional(_) | ArgPart::Fallible(_) => return Ok(None),
        ArgPart::Security {
            scheme,
            scopes,
            join_all,
        } => {
            let scopes_str: Vec<String> = scopes.iter().map(|s| s.to_string()).collect();
            register_security(
                registry,
                scheme.as_ref(),
                &scopes_str,
                *join_all,
                auth,
                flat.optional,
            );
            return Ok(None);
        }
        ArgPart::Zone | ArgPart::Ignore => return Ok(None),
    };

    let openapi_schema = type_schema_to_openapi(schema, registry)?;

    let parameter_data = ParameterData {
        name: arg.name.clone(),
        description: arg.description.clone(),
        required,
        deprecated: None,
        format: ParameterSchemaOrContent::Schema(openapi_schema),
        example: None,
        examples: IndexMap::new(),
        explode: None,
        extensions: IndexMap::new(),
    };

    let param = match location {
        "query" => Parameter::Query {
            parameter_data,
            allow_reserved: false,
            style: openapiv3::QueryStyle::Form,
            allow_empty_value: None,
        },
        "path" => Parameter::Path {
            parameter_data,
            style: openapiv3::PathStyle::Simple,
        },
        "header" => Parameter::Header {
            parameter_data,
            style: openapiv3::HeaderStyle::Simple,
        },
        "cookie" => Parameter::Cookie {
            parameter_data,
            style: openapiv3::CookieStyle::Form,
        },
        _ => return Ok(None),
    };

    Ok(Some(param))
}

/// Build request body from arguments if any body part exists.
fn build_request_body(
    args: &[ArgSpec],
    registry: &mut ComponentRegistry,
) -> Result<Option<ReferenceOr<RequestBody>>, SchemaConversionError> {
    for arg in args {
        for flat in flatten_arg_part(&arg.part) {
            if let Some((schema, content_type, multipart)) = body_part(flat.part) {
                let media_type = build_body_media_type(schema, multipart, registry)?;
                return Ok(Some(ReferenceOr::Item(RequestBody {
                    description: arg.description.clone(),
                    content: request_body_content(content_type, media_type),
                    required: !flat.optional,
                    extensions: IndexMap::new(),
                })));
            }
        }
    }
    Ok(None)
}

fn body_part(part: &ArgPart) -> Option<(&TypeSchema, &str, Option<&MultipartApiSpec>)> {
    match part {
        ArgPart::Body(schema, content_type) => Some((schema, content_type.as_ref(), None)),
        ArgPart::BodyWith {
            schema,
            content_type,
            multipart,
        } => Some((schema, content_type.as_ref(), multipart.as_ref())),
        _ => None,
    }
}

fn build_body_media_type(
    schema: &TypeSchema,
    multipart: Option<&MultipartApiSpec>,
    registry: &mut ComponentRegistry,
) -> Result<MediaType, SchemaConversionError> {
    let openapi_schema = match multipart {
        Some(spec) => type_schema_to_openapi_with_multipart(schema, spec, registry)?,
        None => type_schema_to_openapi(schema, registry)?,
    };
    Ok(MediaType {
        schema: Some(openapi_schema),
        example: None,
        examples: IndexMap::new(),
        encoding: multipart.map(multipart_encoding).unwrap_or_default(),
        extensions: IndexMap::new(),
    })
}

fn request_body_content(content_type: &str, media_type: MediaType) -> IndexMap<String, MediaType> {
    let mut content = IndexMap::new();
    content.insert(content_type.to_string(), media_type);
    content
}

fn multipart_encoding(multipart: &MultipartApiSpec) -> IndexMap<String, Encoding> {
    multipart
        .fields
        .iter()
        .filter(|field| {
            field.kind == MultipartApiFieldKind::File && !field.content_types.is_empty()
        })
        .map(|field| {
            (
                field.name.clone(),
                Encoding {
                    content_type: Some(field.content_types.join(", ")),
                    headers: IndexMap::new(),
                    style: None,
                    explode: false,
                    allow_reserved: false,
                    extensions: IndexMap::new(),
                },
            )
        })
        .collect()
}

/// Build responses from return specifications.
fn build_responses(
    args: &[ArgSpec],
    layers: &[LayerSpec],
    returns: &[ReturnSpec],
    registry: &mut ComponentRegistry,
) -> Result<Responses, SchemaConversionError> {
    let mut responses_map: IndexMap<StatusCode, ReferenceOr<Response>> = IndexMap::new();
    let mut has_responses = false;

    for ret in returns {
        let status_code = ret
            .status_code
            .unwrap_or_else(|| default_status_for_part(&ret.part));
        let status_key = StatusCode::Code(status_code);

        match &ret.part {
            ReturnPart::Unknown => {
                has_responses = true;
                responses_map.insert(
                    status_key,
                    ReferenceOr::Item(Response {
                        description: ret
                            .description
                            .clone()
                            .unwrap_or_else(|| "Unknown response".to_string()),
                        headers: IndexMap::new(),
                        content: IndexMap::new(),
                        links: IndexMap::new(),
                        extensions: IndexMap::new(),
                    }),
                );
            }
            ReturnPart::Body(schema, content_type) => {
                has_responses = true;
                add_body_to_response(
                    &mut responses_map,
                    status_key,
                    ret,
                    status_code,
                    schema,
                    content_type,
                    registry,
                )?;
            }
            ReturnPart::Created(schema, content_type) => {
                has_responses = true;
                add_body_to_response(
                    &mut responses_map,
                    status_key,
                    ret,
                    status_code,
                    schema,
                    content_type,
                    registry,
                )?;
            }
            ReturnPart::Accepted(schema, content_type) => {
                has_responses = true;
                add_body_to_response(
                    &mut responses_map,
                    status_key,
                    ret,
                    status_code,
                    schema,
                    content_type,
                    registry,
                )?;
            }
            ReturnPart::Header(schema) => {
                has_responses = true;
                add_header_to_response(
                    &mut responses_map,
                    status_key,
                    ret,
                    status_code,
                    schema,
                    registry,
                )?;
            }
            ReturnPart::Empty => {
                has_responses = true;
                responses_map
                    .entry(status_key)
                    .or_insert_with(|| create_response(ret, status_code));
            }
            ReturnPart::Redirect { status_code } => {
                has_responses = true;
                add_redirect_response(&mut responses_map, ret, *status_code, registry)?;
            }
            ReturnPart::Binary(content_type) => {
                has_responses = true;
                add_binary_response(&mut responses_map, ret, status_code, content_type, registry)?;
            }
        }
    }

    add_implied_responses(args, layers, &mut responses_map, registry)?;

    if !has_responses {
        responses_map.insert(
            StatusCode::Code(200),
            ReferenceOr::Item(Response {
                description: "Success".to_string(),
                headers: IndexMap::new(),
                content: IndexMap::new(),
                links: IndexMap::new(),
                extensions: IndexMap::new(),
            }),
        );
    }

    Ok(Responses {
        default: None,
        responses: responses_map,
        extensions: IndexMap::new(),
    })
}

/// Get default status code for return part type.
fn default_status_for_part(part: &ReturnPart) -> u16 {
    match part {
        ReturnPart::Empty => 204,
        ReturnPart::Created(..) => 201,
        ReturnPart::Accepted(..) => 202,
        ReturnPart::Redirect { status_code } => *status_code,
        _ => 200,
    }
}

/// Add body content to response.
fn add_body_to_response(
    responses_map: &mut IndexMap<StatusCode, ReferenceOr<Response>>,
    status_key: StatusCode,
    ret: &ReturnSpec,
    status_code: u16,
    schema: &crate::callables::TypeSchema,
    content_type: &str,
    registry: &mut ComponentRegistry,
) -> Result<(), SchemaConversionError> {
    let openapi_schema = type_schema_to_openapi(schema, registry)?;

    let response = responses_map
        .entry(status_key)
        .or_insert_with(|| create_response(ret, status_code));

    if let ReferenceOr::Item(resp) = response {
        resp.content.insert(
            content_type.to_string(),
            MediaType {
                schema: Some(openapi_schema),
                example: ret.examples.first().map(|example| example.value.clone()),
                examples: IndexMap::new(),
                encoding: IndexMap::new(),
                extensions: IndexMap::new(),
            },
        );
        add_documented_headers(resp, ret, registry)?;
    }

    Ok(())
}

/// Add header to response.
fn add_header_to_response(
    responses_map: &mut IndexMap<StatusCode, ReferenceOr<Response>>,
    status_key: StatusCode,
    ret: &ReturnSpec,
    status_code: u16,
    schema: &crate::callables::TypeSchema,
    registry: &mut ComponentRegistry,
) -> Result<(), SchemaConversionError> {
    let openapi_schema = type_schema_to_openapi(schema, registry)?;

    let response = responses_map
        .entry(status_key)
        .or_insert_with(|| create_response(ret, status_code));

    if let ReferenceOr::Item(resp) = response {
        let header_name = ret
            .description
            .clone()
            .unwrap_or_else(|| "X-Custom-Header".to_string());
        resp.headers.insert(
            header_name,
            ReferenceOr::Item(openapiv3::Header {
                description: None,
                style: openapiv3::HeaderStyle::Simple,
                required: false,
                deprecated: None,
                format: ParameterSchemaOrContent::Schema(openapi_schema),
                example: None,
                examples: IndexMap::new(),
                extensions: IndexMap::new(),
            }),
        );
        add_documented_headers(resp, ret, registry)?;
    }

    Ok(())
}

fn add_redirect_response(
    responses_map: &mut IndexMap<StatusCode, ReferenceOr<Response>>,
    ret: &ReturnSpec,
    status_code: u16,
    registry: &mut ComponentRegistry,
) -> Result<(), SchemaConversionError> {
    let status_key = StatusCode::Code(status_code);
    let response = responses_map
        .entry(status_key)
        .or_insert_with(|| create_response(ret, status_code));

    if let ReferenceOr::Item(resp) = response {
        let schema = type_schema_to_openapi(&TypeSchema::wrap::<String>(), registry)?;
        resp.headers.insert(
            "Location".to_string(),
            ReferenceOr::Item(openapiv3::Header {
                description: Some("Redirect target.".to_string()),
                style: openapiv3::HeaderStyle::Simple,
                required: true,
                deprecated: None,
                format: ParameterSchemaOrContent::Schema(schema),
                example: None,
                examples: IndexMap::new(),
                extensions: IndexMap::new(),
            }),
        );
        add_documented_headers(resp, ret, registry)?;
    }
    Ok(())
}

fn add_binary_response(
    responses_map: &mut IndexMap<StatusCode, ReferenceOr<Response>>,
    ret: &ReturnSpec,
    status_code: u16,
    content_type: &str,
    registry: &mut ComponentRegistry,
) -> Result<(), SchemaConversionError> {
    let status_key = StatusCode::Code(status_code);
    let schema = type_schema_to_openapi(&TypeSchema::binary_body(), registry)?;
    let response = responses_map
        .entry(status_key)
        .or_insert_with(|| create_response(ret, status_code));

    if let ReferenceOr::Item(resp) = response {
        resp.content.insert(
            content_type.to_string(),
            MediaType {
                schema: Some(schema),
                example: ret.examples.first().map(|example| example.value.clone()),
                examples: IndexMap::new(),
                encoding: IndexMap::new(),
                extensions: IndexMap::new(),
            },
        );
        add_documented_headers(resp, ret, registry)?;
    }
    Ok(())
}

fn add_documented_headers(
    response: &mut Response,
    ret: &ReturnSpec,
    registry: &mut ComponentRegistry,
) -> Result<(), SchemaConversionError> {
    for header in &ret.headers {
        let schema = type_schema_to_openapi(&header.schema, registry)?;
        response.headers.insert(
            header.name.to_string(),
            ReferenceOr::Item(openapiv3::Header {
                description: header.description.as_ref().map(ToString::to_string),
                style: openapiv3::HeaderStyle::Simple,
                required: header.required,
                deprecated: None,
                format: ParameterSchemaOrContent::Schema(schema),
                example: None,
                examples: IndexMap::new(),
                extensions: IndexMap::new(),
            }),
        );
    }
    Ok(())
}

fn add_implied_responses(
    args: &[ArgSpec],
    layers: &[LayerSpec],
    responses_map: &mut IndexMap<StatusCode, ReferenceOr<Response>>,
    registry: &mut ComponentRegistry,
) -> Result<(), SchemaConversionError> {
    for arg in args {
        for part in flatten_arg_part(&arg.part) {
            if !part.suppresses_responses() {
                add_part_responses(part.part, responses_map, registry)?;
            }
        }
    }
    for layer in layers {
        for part in &layer.parts {
            for nested in flatten_arg_part(part) {
                if !nested.suppresses_responses() {
                    add_part_responses(nested.part, responses_map, registry)?;
                }
            }
        }
    }
    Ok(())
}

fn add_part_responses(
    part: &ArgPart,
    responses_map: &mut IndexMap<StatusCode, ReferenceOr<Response>>,
    registry: &mut ComponentRegistry,
) -> Result<(), SchemaConversionError> {
    if let ArgPart::Response(returns) = part {
        for ret in returns {
            add_return_response(responses_map, ret, registry)?;
        }
    }
    Ok(())
}

fn add_return_response(
    responses_map: &mut IndexMap<StatusCode, ReferenceOr<Response>>,
    ret: &ReturnSpec,
    registry: &mut ComponentRegistry,
) -> Result<(), SchemaConversionError> {
    let status_code = ret
        .status_code
        .unwrap_or_else(|| default_status_for_part(&ret.part));
    let key = StatusCode::Code(status_code);
    if responses_map.contains_key(&key) {
        return Ok(());
    }
    match &ret.part {
        ReturnPart::Body(schema, content_type)
        | ReturnPart::Created(schema, content_type)
        | ReturnPart::Accepted(schema, content_type) => {
            add_body_to_response(
                responses_map,
                key,
                ret,
                status_code,
                schema,
                content_type,
                registry,
            )?;
        }
        ReturnPart::Empty => {
            responses_map.insert(key, create_response(ret, status_code));
        }
        ReturnPart::Redirect { status_code } => {
            add_redirect_response(responses_map, ret, *status_code, registry)?;
        }
        ReturnPart::Binary(content_type) => {
            add_binary_response(responses_map, ret, status_code, content_type, registry)?;
        }
        ReturnPart::Header(schema) => {
            add_header_to_response(responses_map, key, ret, status_code, schema, registry)?;
        }
        ReturnPart::Unknown => {
            responses_map.insert(key, create_response(ret, status_code));
        }
    }
    Ok(())
}

/// Create a response with proper description.
fn create_response(ret: &ReturnSpec, status_code: u16) -> ReferenceOr<Response> {
    ReferenceOr::Item(Response {
        description: ret
            .description
            .clone()
            .unwrap_or_else(|| status_description(status_code).to_string()),
        headers: IndexMap::new(),
        content: IndexMap::new(),
        links: IndexMap::new(),
        extensions: IndexMap::new(),
    })
}

/// Build security schemes from registered scheme names.
fn build_security_schemes(
    scheme_names: &[String],
    auth: Option<&AuthConf>,
) -> IndexMap<String, ReferenceOr<openapiv3::SecurityScheme>> {
    let mut schemes = IndexMap::new();

    for name in scheme_names {
        let scheme = create_security_scheme(name, auth);
        schemes.insert(name.clone(), ReferenceOr::Item(scheme));
    }

    schemes
}

/// Create a security scheme based on naming convention.
fn create_security_scheme(name: &str, auth: Option<&AuthConf>) -> openapiv3::SecurityScheme {
    if let Some(provider) = auth.and_then(|conf| {
        conf.provider_docs()
            .into_iter()
            .find(|provider| provider_scheme_name(provider) == name)
    }) {
        return provider_security_scheme(&provider);
    }
    let lower = name.to_lowercase();

    if name == "cookieAuth" {
        return cookie_security(auth);
    }
    if name == "basicAuth" {
        return openapiv3::SecurityScheme::HTTP {
            scheme: "basic".to_string(),
            bearer_format: None,
            description: Some(
                "HTTP Basic credentials exchanged by an application login route.".into(),
            ),
            extensions: IndexMap::new(),
        };
    }
    if lower.contains("bearer") || lower.contains("jwt") {
        openapiv3::SecurityScheme::HTTP {
            scheme: "bearer".to_string(),
            bearer_format: Some("JWT".to_string()),
            description: Some(format!("JWT Bearer token for {}", name)),
            extensions: IndexMap::new(),
        }
    } else if lower.contains("apikey") || lower.contains("api_key") {
        api_key_security(name, auth)
    } else if lower.contains("oauth") {
        openapiv3::SecurityScheme::OAuth2 {
            flows: openapiv3::OAuth2Flows::default(),
            description: Some(format!("OAuth2 authentication for {}", name)),
            extensions: IndexMap::new(),
        }
    } else {
        // Default to bearer auth for unknown schemes
        openapiv3::SecurityScheme::HTTP {
            scheme: "bearer".to_string(),
            bearer_format: None,
            description: Some(format!("Authentication for {}", name)),
            extensions: IndexMap::new(),
        }
    }
}

fn provider_scheme_name(provider: &ProviderDoc) -> String {
    format!("vyuh_{}", provider.id)
}

fn provider_security_scheme(provider: &ProviderDoc) -> openapiv3::SecurityScheme {
    match (&provider.credential_type, &provider.location) {
        (
            CredentialType::Token(format),
            ProviderDocLocation::Header {
                name,
                scheme: Some(scheme),
            },
        ) if name.eq_ignore_ascii_case(header::AUTHORIZATION.as_str())
            && scheme.eq_ignore_ascii_case("bearer") =>
        {
            openapiv3::SecurityScheme::HTTP {
                scheme: "bearer".to_owned(),
                bearer_format: format.clone(),
                description: Some(format!("Token provider '{}'", provider.id)),
                extensions: IndexMap::new(),
            }
        }
        (_, ProviderDocLocation::Header { name, .. }) => openapiv3::SecurityScheme::APIKey {
            location: openapiv3::APIKeyLocation::Header,
            name: name.clone(),
            description: Some(format!("Credential provider '{}'", provider.id)),
            extensions: IndexMap::new(),
        },
        (_, ProviderDocLocation::Cookie(name)) => openapiv3::SecurityScheme::APIKey {
            location: openapiv3::APIKeyLocation::Cookie,
            name: name.clone(),
            description: Some(format!("Credential provider '{}'", provider.id)),
            extensions: IndexMap::new(),
        },
        (_, ProviderDocLocation::Query(name)) => openapiv3::SecurityScheme::APIKey {
            location: openapiv3::APIKeyLocation::Query,
            name: name.clone(),
            description: Some(format!("Credential provider '{}'", provider.id)),
            extensions: IndexMap::new(),
        },
    }
}

fn cookie_security(auth: Option<&AuthConf>) -> openapiv3::SecurityScheme {
    let name = auth
        .and_then(|conf| {
            conf.provider_docs()
                .into_iter()
                .find_map(|provider| match provider.location {
                    ProviderDocLocation::Cookie(name) => Some(name),
                    _ => None,
                })
        })
        .unwrap_or_else(|| "access_token".to_string());
    openapiv3::SecurityScheme::APIKey {
        location: openapiv3::APIKeyLocation::Cookie,
        name,
        description: Some("JWT access token cookie.".to_string()),
        extensions: IndexMap::new(),
    }
}

fn api_key_security(name: &str, auth: Option<&AuthConf>) -> openapiv3::SecurityScheme {
    let _ = auth;
    api_key_header(name, "X-API-Key")
}

fn api_key_header(name: &str, header: &str) -> openapiv3::SecurityScheme {
    openapiv3::SecurityScheme::APIKey {
        location: openapiv3::APIKeyLocation::Header,
        name: header.to_string(),
        description: Some(format!("API key for {name}")),
        extensions: IndexMap::new(),
    }
}

/// Get standard description for HTTP status code.
fn status_description(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        422 => "Unprocessable Entity",
        500 => "Internal Server Error",
        _ => "Response",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::UploadedFile;
    use crate::{
        callables::{
            ArgPart, ArgSpec, IntoArgPart, Operation as VyuhOperation, OperationKind, ReturnPart,
            ReturnSpec, TypeSchema,
        },
        routes::{Methods, MultipartForm},
    };
    use schemars::JsonSchema;
    use serde::{Deserialize, Serialize};
    use serde_json::Value;
    use std::borrow::Cow;
    use std::process::Command;

    fn route_op(name: &str, path: &str, methods: Methods) -> VyuhOperation {
        VyuhOperation {
            id: crate::OperationId::new(),
            name: name.to_string(),
            description: None,
            summary: None,
            openapi_id: None,
            deprecated: false,
            path: path.to_string(),
            kind: OperationKind::Route,
            methods,
            args: Vec::new(),
            layers: Vec::new(),
            returns: vec![ReturnSpec::new(ReturnPart::Empty)],
            tags: Vec::new(),
            conf: None,
            owner: None,
            hidden: false,
            audience: None,
            bundle_id: None,
            slash_policy: None,
        }
    }

    fn cookie_auth(name: &str) -> AuthConf {
        AuthConf::empty().provider(
            crate::auth::DEFAULT_AUTH_PROVIDER,
            crate::auth::TokenProvider::new(crate::auth::Jwt::hs256_site_secret())
                .access(crate::auth::TokenConf::cookie(name)),
        )
    }

    #[test]
    fn generates_operations_for_multiple_methods() {
        let op = route_op("notes", "/notes", Methods::GET | Methods::HEAD);
        let api = ApiDocGenerator::default().generate(&[&op]).unwrap();
        let path = api.paths.paths.get("/notes").unwrap();

        let ReferenceOr::Item(item) = path else {
            panic!("expected inline path item");
        };

        assert_eq!(
            item.get.as_ref().unwrap().operation_id.as_deref(),
            Some("notes_get")
        );
        assert_eq!(
            item.head.as_ref().unwrap().operation_id.as_deref(),
            Some("notes_head")
        );
    }

    #[test]
    fn fills_missing_docs() {
        let op = route_op("notes", "/notes", Methods::GET);
        let api = ApiDocGenerator::default().generate(&[&op]).unwrap();
        let ReferenceOr::Item(item) = api.paths.paths.get("/notes").unwrap() else {
            panic!("expected inline path item");
        };
        let get = item.get.as_ref().unwrap();

        assert_eq!(get.summary.as_deref(), Some("Notes"));
        assert_eq!(
            get.description.as_deref(),
            Some("Application route registered for `GET /notes`.")
        );
    }

    #[test]
    fn preserves_summary_and_description() {
        let mut op = route_op("notes", "/notes", Methods::GET);
        op.summary = Some("List notes".to_string());
        op.description = Some("Returns all notes visible to the caller.".to_string());

        let api = ApiDocGenerator::default().generate(&[&op]).unwrap();
        let ReferenceOr::Item(item) = api.paths.paths.get("/notes").unwrap() else {
            panic!("expected inline path item");
        };
        let get = item.get.as_ref().unwrap();

        assert_eq!(get.summary.as_deref(), Some("List notes"));
        assert_eq!(
            get.description.as_deref(),
            Some("Returns all notes visible to the caller.")
        );
    }

    #[test]
    fn emits_redirect_responses_with_location_header() {
        let mut op = route_op("login", "/login", Methods::POST);
        op.returns = vec![ReturnSpec {
            description: Some("Redirects after login.".to_string()),
            status_code: None,
            part: ReturnPart::Redirect { status_code: 303 },
            headers: Vec::new(),
            examples: Vec::new(),
            schema_name: None,
        }];

        let api = ApiDocGenerator::default().generate(&[&op]).unwrap();
        let json = serde_json::to_value(api).unwrap();

        assert_eq!(
            json["paths"]["/login"]["post"]["responses"]["303"]["description"],
            "Redirects after login."
        );
        assert_eq!(
            json["paths"]["/login"]["post"]["responses"]["303"]["headers"]["Location"]["required"],
            true
        );
    }

    #[derive(JsonSchema)]
    #[allow(dead_code)]
    struct ValidatedNote {
        title: String,
    }

    impl crate::validation::ValidationSchema for ValidatedNote {
        fn apply_validation_schema(
            _schema: &mut serde_json::Value,
            _definitions: &mut serde_json::Map<String, serde_json::Value>,
        ) {
        }
    }

    #[test]
    fn adds_implied_error_responses_from_argument_metadata() {
        let mut op = route_op("create_note", "/notes", Methods::POST);
        op.args.push(ArgSpec {
            name: "body".to_string(),
            description: None,
            position: 0,
            part: ArgPart::Composite(vec![
                ArgPart::Body(
                    TypeSchema::wrap_valid::<ValidatedNote>(),
                    "application/json".into(),
                ),
                ArgPart::Response(vec![
                    ReturnSpec::error(400, "Bad request."),
                    ReturnSpec::error(422, "Validation failed."),
                ]),
            ]),
        });

        let api = ApiDocGenerator::default().generate(&[&op]).unwrap();
        let json = serde_json::to_value(api).unwrap();
        let responses = &json["paths"]["/notes"]["post"]["responses"];

        assert!(responses.get("400").is_some());
        assert!(responses.get("422").is_some());
        assert!(responses.get("500").is_none());
        assert_eq!(
            responses["422"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ErrorReport"
        );
    }

    /// Verifies optional body extractors keep request schema but suppress extractor errors.
    #[test]
    fn optional_json_body_is_optional_without_implied_errors() {
        let mut op = route_op("create_note", "/notes", Methods::POST);
        op.args.push(ArgSpec {
            name: "body".to_string(),
            description: None,
            position: 0,
            part: ArgPart::Optional(Box::new(ArgPart::Composite(vec![
                ArgPart::Body(
                    TypeSchema::wrap_valid::<ValidatedNote>(),
                    "application/json".into(),
                ),
                ArgPart::Response(vec![
                    ReturnSpec::error(400, "Bad request."),
                    ReturnSpec::error(422, "Validation failed."),
                ]),
            ]))),
        });

        let api = ApiDocGenerator::default().generate(&[&op]).unwrap();
        let json = serde_json::to_value(api).unwrap();
        let post = &json["paths"]["/notes"]["post"];

        assert!(post["requestBody"].get("required").is_none());
        assert!(post["responses"].get("400").is_none());
        assert!(post["responses"].get("422").is_none());
    }

    /// Verifies fallible body extractors keep required body but suppress extractor errors.
    #[test]
    fn fallible_json_body_is_required_without_implied_errors() {
        let mut op = route_op("create_note", "/notes", Methods::POST);
        op.args.push(ArgSpec {
            name: "body".to_string(),
            description: None,
            position: 0,
            part: ArgPart::Fallible(Box::new(ArgPart::Composite(vec![
                ArgPart::Body(
                    TypeSchema::wrap_valid::<ValidatedNote>(),
                    "application/json".into(),
                ),
                ArgPart::Response(vec![
                    ReturnSpec::error(400, "Bad request."),
                    ReturnSpec::error(422, "Validation failed."),
                ]),
            ]))),
        });

        let api = ApiDocGenerator::default().generate(&[&op]).unwrap();
        let json = serde_json::to_value(api).unwrap();
        let post = &json["paths"]["/notes"]["post"];

        assert_eq!(post["requestBody"]["required"], Value::Bool(true));
        assert!(post["responses"].get("400").is_none());
        assert!(post["responses"].get("422").is_none());
    }

    #[test]
    fn emits_temporary_and_permanent_redirect_responses() {
        let mut op = route_op("redirects", "/redirects", Methods::GET);
        op.returns = vec![
            ReturnSpec {
                description: None,
                status_code: None,
                part: ReturnPart::Redirect { status_code: 307 },
                headers: Vec::new(),
                examples: Vec::new(),
                schema_name: None,
            },
            ReturnSpec {
                description: None,
                status_code: None,
                part: ReturnPart::Redirect { status_code: 308 },
                headers: Vec::new(),
                examples: Vec::new(),
                schema_name: None,
            },
        ];

        let api = ApiDocGenerator::default().generate(&[&op]).unwrap();
        let json = serde_json::to_value(api).unwrap();
        let responses = &json["paths"]["/redirects"]["get"]["responses"];

        assert!(responses.get("307").is_some());
        assert!(responses.get("308").is_some());
        assert!(responses["307"]["headers"].get("Location").is_some());
        assert!(responses["308"]["headers"].get("Location").is_some());
    }

    #[test]
    fn emits_security_metadata_from_arguments() {
        let mut op = route_op("notes", "/notes", Methods::GET);
        op.args.push(ArgSpec {
            name: "auth".to_string(),
            description: None,
            position: 0,
            part: ArgPart::Security {
                scheme: Cow::Borrowed("bearerAuth"),
                scopes: Vec::new(),
                join_all: true,
            },
        });

        let api = ApiDocGenerator::default().generate(&[&op]).unwrap();
        let ReferenceOr::Item(item) = api.paths.paths.get("/notes").unwrap() else {
            panic!("expected inline path item");
        };
        let get = item.get.as_ref().unwrap();

        assert!(get.security.as_ref().unwrap()[0].contains_key("bearerAuth"));
        assert!(
            api.components
                .as_ref()
                .unwrap()
                .security_schemes
                .contains_key("bearerAuth")
        );
    }

    /// Verifies optional auth documents optional security and suppresses 401.
    #[test]
    fn optional_auth_emits_optional_security_without_unauthorized_response() {
        let mut op = route_op("notes", "/notes", Methods::GET);
        op.args.push(ArgSpec {
            name: "auth".to_string(),
            description: None,
            position: 0,
            part: ArgPart::Optional(Box::new(ArgPart::Composite(vec![
                ArgPart::Security {
                    scheme: Cow::Borrowed("bearerAuth"),
                    scopes: Vec::new(),
                    join_all: true,
                },
                ArgPart::Response(vec![ReturnSpec::error(401, "Unauthorized.")]),
            ]))),
        });

        let api = ApiDocGenerator::default().generate(&[&op]).unwrap();
        let json = serde_json::to_value(api).unwrap();
        let get = &json["paths"]["/notes"]["get"];

        assert_eq!(get["security"][0], serde_json::json!({}));
        assert_eq!(get["security"][1]["bearerAuth"], serde_json::json!([]));
        assert!(get["responses"].get("401").is_none());
    }

    #[test]
    fn emits_cookie_security_from_auth_config() {
        let mut op = route_op("profile", "/profile", Methods::GET);
        op.args.push(ArgSpec {
            name: "auth".to_string(),
            description: None,
            position: 0,
            part: ArgPart::Security {
                scheme: Cow::Borrowed("vyuhAuth"),
                scopes: Vec::new(),
                join_all: true,
            },
        });

        let api = ApiDocGenerator::default()
            .with_auth(cookie_auth("blog_access"))
            .generate(&[&op])
            .unwrap();
        let json = serde_json::to_value(api).unwrap();
        let security = &json["paths"]["/profile"]["get"]["security"];

        assert_eq!(security[0]["vyuh_default"], serde_json::json!([]));
        assert_eq!(
            json["components"]["securitySchemes"]["vyuh_default"]["in"],
            "cookie"
        );
        assert_eq!(
            json["components"]["securitySchemes"]["vyuh_default"]["name"],
            "blog_access"
        );
    }

    /// Verifies unsafe cookie-authenticated operations expose their CSRF header contract.
    #[test]
    fn emits_csrf_metadata_only_for_unsafe_methods() {
        let mut op = route_op("profile", "/profile", Methods::GET | Methods::POST);
        op.audience = Some(crate::auth::AudienceId::new("api").unwrap());
        op.args.push(ArgSpec {
            name: "auth".to_string(),
            description: None,
            position: 0,
            part: ArgPart::Security {
                scheme: Cow::Borrowed("vyuhAuth"),
                scopes: Vec::new(),
                join_all: true,
            },
        });
        let auth = cookie_auth("access");
        let api = ApiDocGenerator::default()
            .with_auth(auth)
            .generate(&[&op])
            .unwrap();
        let json = serde_json::to_value(api).unwrap();
        assert!(
            json["paths"]["/profile"]["get"]
                .get("x-vyuh-csrf-header")
                .is_none()
        );
        assert_eq!(
            json["paths"]["/profile"]["post"]["x-vyuh-csrf-header"],
            serde_json::json!(["x-csrf-token"])
        );
    }

    #[test]
    fn sorts_paths_deterministically() {
        let z = route_op("zebra", "/zebra", Methods::GET);
        let a = route_op("alpha", "/alpha", Methods::GET);

        let api = ApiDocGenerator::default().generate(&[&z, &a]).unwrap();
        let keys = api.paths.paths.keys().cloned().collect::<Vec<_>>();

        assert_eq!(keys, vec!["/alpha".to_string(), "/zebra".to_string()]);
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    struct ProductionPostOut {
        id: i64,
        title: String,
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    struct ProductionCreatePostIn {
        title: String,
        body: String,
    }

    impl crate::ValidationSchema for ProductionCreatePostIn {
        fn apply_validation_schema(
            schema: &mut Value,
            _definitions: &mut serde_json::Map<String, Value>,
        ) {
            if let Some(title) = schema
                .get_mut("properties")
                .and_then(Value::as_object_mut)
                .and_then(|props| props.get_mut("title"))
                .and_then(Value::as_object_mut)
            {
                title.insert("minLength".to_string(), Value::from(3));
            }
        }
    }

    #[derive(Debug, JsonSchema, crate::MultipartData)]
    #[allow(dead_code)]
    struct ProductionUploadImageIn {
        #[upload(
            content_types = ["image/png", "image/jpeg"],
            extensions = ["png", "jpg", "jpeg"],
            max_size = 2_000_000,
            sniff = "image"
        )]
        image: UploadedFile,
        gallery: Vec<UploadedFile>,
        optional: Option<UploadedFile>,
        caption: Option<String>,
    }

    #[derive(Debug, Serialize, Deserialize, JsonSchema)]
    struct ProductionImageOut {
        url: String,
    }

    /// Verifies generated OpenAPI has enough metadata for client generation.
    #[test]
    fn production_grade_openapi_has_precise_contract_metadata() {
        let spec = production_spec_json();
        assert_no_unknown_responses(&spec);
        assert_operation_ids(&spec);
        assert_json_responses_have_schemas(&spec);
        assert_security_contract(&spec);
        assert_implied_error_contract(&spec);
        assert_redirect_contract(&spec);
        assert_multipart_binary_contract(&spec);
        assert_no_json_schema_definition_refs(&spec);
        assert!(
            spec["paths"]["/posts"]["get"]["responses"]
                .get("500")
                .is_none()
        );
        assert!(spec["components"]["schemas"].get("ErrorReport").is_some());
    }

    /// Validates the generated spec with Redocly or Swagger CLI when available.
    #[test]
    fn production_grade_openapi_validates_with_available_tooling() {
        let spec = production_spec_json();
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), serde_json::to_vec_pretty(&spec).unwrap()).unwrap();
        validate_with_external_tool(file.path());
    }

    fn production_spec_json() -> Value {
        let operations = production_operations();
        let refs = operations.iter().collect::<Vec<_>>();
        let generator = ApiDocGenerator::new(ApiMeta {
            title: "Production API".to_string(),
            version: "1.0.0".to_string(),
            description: None,
            tags: Vec::new(),
        })
        .with_auth(cookie_auth("vyuh_access"));
        serde_json::to_value(generator.generate(&refs).unwrap()).unwrap()
    }

    fn production_operations() -> Vec<VyuhOperation> {
        vec![
            production_list_route(),
            production_create_route(),
            production_admin_route(),
            production_redirect_route(),
            production_upload_route(),
        ]
    }

    fn production_list_route() -> VyuhOperation {
        let mut op = route_op("list_posts", "/posts", Methods::GET);
        op.openapi_id = Some("list_posts".to_string());
        op.returns = vec![json_return::<Vec<ProductionPostOut>>()];
        op
    }

    fn production_create_route() -> VyuhOperation {
        let mut op = route_op("create_post", "/posts", Methods::POST);
        op.openapi_id = Some("create_post".to_string());
        op.args.push(ArgSpec {
            name: "body".to_string(),
            description: Some("Post creation payload.".to_string()),
            position: 0,
            part: ArgPart::Composite(vec![
                ArgPart::Body(
                    TypeSchema::wrap_valid::<ProductionCreatePostIn>(),
                    Cow::Borrowed("application/json"),
                ),
                ArgPart::Response(vec![
                    ReturnSpec::error(400, "Bad request."),
                    ReturnSpec::error(422, "Validation failed."),
                ]),
            ]),
        });
        op.returns = vec![created_return::<ProductionPostOut>()];
        op
    }

    fn production_admin_route() -> VyuhOperation {
        let mut op = route_op("admin_posts", "/admin/posts", Methods::GET);
        op.openapi_id = Some("admin_posts".to_string());
        op.args.push(ArgSpec {
            name: "auth".to_string(),
            description: Some("Authenticated user.".to_string()),
            position: 0,
            part: ArgPart::Composite(vec![
                ArgPart::Security {
                    scheme: Cow::Borrowed("vyuhAuth"),
                    scopes: Vec::new(),
                    join_all: true,
                },
                ArgPart::Response(vec![ReturnSpec::error(401, "Unauthorized.")]),
            ]),
        });
        op.returns = vec![json_return::<Vec<ProductionPostOut>>()];
        op
    }

    fn production_redirect_route() -> VyuhOperation {
        let mut op = route_op("login_redirect", "/login", Methods::POST);
        op.openapi_id = Some("login_redirect".to_string());
        op.returns = vec![ReturnSpec::new(ReturnPart::Redirect { status_code: 307 })];
        op
    }

    fn production_upload_route() -> VyuhOperation {
        let mut op = route_op("upload_post_image", "/posts/{id}/image", Methods::POST);
        op.openapi_id = Some("upload_post_image".to_string());
        op.args.push(ArgSpec {
            name: "id".to_string(),
            description: Some("Post id.".to_string()),
            position: 0,
            part: ArgPart::Composite(vec![
                ArgPart::Path(TypeSchema::wrap_unvalidated::<i64>()),
                ArgPart::Response(vec![ReturnSpec::error(400, "Bad request.")]),
            ]),
        });
        op.args.push(ArgSpec {
            name: "body".to_string(),
            description: Some("Image upload form.".to_string()),
            position: 1,
            part: <MultipartForm<ProductionUploadImageIn> as IntoArgPart>::into_arg_part(),
        });
        op.returns = vec![created_return::<ProductionImageOut>()];
        op
    }

    fn json_return<T: JsonSchema + 'static>() -> ReturnSpec {
        ReturnSpec::new(ReturnPart::Body(
            TypeSchema::wrap::<T>(),
            Cow::Borrowed("application/json"),
        ))
    }

    fn created_return<T: JsonSchema + 'static>() -> ReturnSpec {
        ReturnSpec::new(ReturnPart::Created(
            TypeSchema::wrap::<T>(),
            Cow::Borrowed("application/json"),
        ))
    }

    fn assert_no_unknown_responses(spec: &Value) {
        let text = serde_json::to_string(spec).unwrap();
        assert!(!text.contains("Unknown response"));
    }

    fn assert_operation_ids(spec: &Value) {
        for operation in all_operations(spec) {
            assert!(
                operation
                    .get("operationId")
                    .and_then(Value::as_str)
                    .is_some()
            );
        }
    }

    fn assert_json_responses_have_schemas(spec: &Value) {
        for operation in all_operations(spec) {
            let Some(responses) = operation.get("responses").and_then(Value::as_object) else {
                continue;
            };
            for response in responses.values() {
                let Some(json) = response
                    .get("content")
                    .and_then(|content| content.get("application/json"))
                else {
                    continue;
                };
                assert!(json.get("schema").is_some());
            }
        }
    }

    fn assert_security_contract(spec: &Value) {
        assert!(spec["paths"]["/posts"]["get"].get("security").is_none());
        assert_eq!(
            spec["paths"]["/admin/posts"]["get"]["security"][0]["vyuh_default"],
            serde_json::json!([])
        );
        assert!(
            spec["components"]["securitySchemes"]
                .get("vyuh_default")
                .is_some()
        );
    }

    fn assert_implied_error_contract(spec: &Value) {
        let create_responses = &spec["paths"]["/posts"]["post"]["responses"];
        assert!(create_responses.get("400").is_some());
        assert!(create_responses.get("422").is_some());
        assert_eq!(
            create_responses["422"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ErrorReport"
        );
        assert!(
            spec["paths"]["/admin/posts"]["get"]["responses"]
                .get("401")
                .is_some()
        );
    }

    fn assert_redirect_contract(spec: &Value) {
        let response = &spec["paths"]["/login"]["post"]["responses"]["307"];
        assert_eq!(response["headers"]["Location"]["required"], true);
    }

    fn assert_multipart_binary_contract(spec: &Value) {
        let media_type = &spec["paths"]["/posts/{id}/image"]["post"]["requestBody"]["content"]["multipart/form-data"];
        let schema = &media_type["schema"];
        let schema = resolve_component_schema(spec, schema);
        let image = &schema["properties"]["image"];
        assert_eq!(image["type"], "string");
        assert_eq!(image["format"], "binary");
        assert_eq!(schema["required"], serde_json::json!(["image"]));
        assert_eq!(
            media_type["encoding"]["image"]["contentType"],
            "image/jpeg, image/png"
        );
        assert_eq!(
            image["x-vyuh-upload-content-types"],
            serde_json::json!(["image/jpeg", "image/png"])
        );
        assert_eq!(
            image["x-vyuh-upload-extensions"],
            serde_json::json!(["jpeg", "jpg", "png"])
        );
        assert_eq!(image["x-vyuh-upload-max-bytes"], 2_000_000);
        assert_eq!(image["x-vyuh-upload-sniff"], "image");

        let gallery = &schema["properties"]["gallery"];
        assert_eq!(gallery["type"], "array");
        assert_eq!(gallery["items"]["type"], "string");
        assert_eq!(gallery["items"]["format"], "binary");
        assert_eq!(gallery["x-vyuh-upload-multiple"], true);

        let optional = &schema["properties"]["optional"];
        assert_eq!(optional["type"], "string");
        assert_eq!(optional["format"], "binary");
    }

    fn assert_no_json_schema_definition_refs(spec: &Value) {
        let text = serde_json::to_string(spec).unwrap();
        assert!(!text.contains("#/$defs/"));
        assert!(!text.contains("#/definitions/"));
    }

    fn resolve_component_schema<'a>(spec: &'a Value, schema: &'a Value) -> &'a Value {
        let Some(reference) = schema.get("$ref").and_then(Value::as_str) else {
            return schema;
        };
        let Some(name) = reference.strip_prefix("#/components/schemas/") else {
            return schema;
        };
        &spec["components"]["schemas"][name]
    }

    fn all_operations(spec: &Value) -> Vec<&Value> {
        let mut operations = Vec::new();
        let methods = ["get", "post", "put", "patch", "delete"];
        for path in spec["paths"].as_object().into_iter().flatten() {
            for method in methods {
                if let Some(operation) = path.1.get(method) {
                    operations.push(operation);
                }
            }
        }
        operations
    }

    fn validate_with_external_tool(path: &std::path::Path) {
        if run_tool("redocly", &["lint", path.to_str().unwrap()]) {
            return;
        }
        if run_tool("swagger-cli", &["validate", path.to_str().unwrap()]) {
            return;
        }
        eprintln!("skipping external OpenAPI validation; redocly/swagger-cli not installed");
    }

    fn run_tool(command: &str, args: &[&str]) -> bool {
        let Ok(output) = Command::new(command).args(args).output() else {
            return false;
        };
        assert!(
            output.status.success(),
            "{command} failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        true
    }
}
