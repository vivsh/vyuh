use schemars::generate::SchemaSettings;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::{
    Operation, OperationKind,
    callables::{ArgPart, ArgSpec, ReturnPart, ReturnSpec, TypeSchema},
    console::middleware::{MiddlewareInfo, MiddlewareSetting, operation_middleware},
};

#[derive(Debug, Serialize)]
pub(crate) struct OperationView {
    pub id: String,
    pub name: String,
    pub kind: OperationKind,
    pub summary: Option<String>,
    pub description: Option<String>,
    pub path: String,
    pub methods: Vec<&'static str>,
    pub tags: Vec<String>,
    pub owner: Option<String>,
    pub hidden: bool,
    pub args: Vec<SchemaView>,
    pub middleware: Vec<MiddlewareView>,
    pub returns: Vec<SchemaView>,
}

impl OperationView {
    pub(crate) fn from_operation(op: &Operation, site: &crate::Site) -> Self {
        Self {
            id: op.id.to_string(),
            name: op.name.clone(),
            kind: op.kind.clone(),
            summary: op.summary.clone(),
            description: op.description.clone(),
            path: op.path.clone(),
            methods: op.http_methods(),
            tags: op.tags.iter().map(|tag| tag.to_string()).collect(),
            owner: op.owner.clone(),
            hidden: op.hidden,
            args: op.args.iter().flat_map(SchemaView::from_arg).collect(),
            middleware: operation_middleware(site, op)
                .iter()
                .map(MiddlewareView::from_info)
                .collect(),
            returns: operation_returns(op),
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct MiddlewareView {
    pub name: String,
    pub scope: String,
    pub description: Option<String>,
    pub request_parts: Vec<SchemaView>,
    pub settings: Vec<MiddlewareSetting>,
}

impl MiddlewareView {
    fn from_info(info: &MiddlewareInfo) -> Self {
        Self {
            name: info.name.clone(),
            scope: info.scope.to_string(),
            description: info.description.clone(),
            request_parts: info
                .request_parts
                .iter()
                .flat_map(|part| {
                    SchemaView::from_part(&part.name, part.description.clone(), &part.part)
                })
                .collect(),
            settings: info.settings.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct SchemaView {
    pub name: String,
    pub location: String,
    pub description: Option<String>,
    pub status_code: Option<u16>,
    pub content_type: Option<String>,
    pub display_name: String,
    pub schema_type: String,
    pub ref_name: Option<String>,
    pub properties: Vec<PropertyView>,
    pub raw_schema: Option<String>,
    pub unresolved_ref: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct PropertyView {
    pub name: String,
    pub type_label: String,
    pub required: bool,
    pub description: Option<String>,
}

impl SchemaView {
    fn from_arg(arg: &ArgSpec) -> Vec<Self> {
        flatten_arg_part(&arg.part)
            .into_iter()
            .filter_map(|flat| {
                if matches!(flat.part, ArgPart::Response(_)) {
                    return None;
                }
                let (location, schema, content_type) = arg_part(flat.part);
                Some(Self::from_schema(
                    arg.name.clone(),
                    location_label(location, flat),
                    arg.description.clone(),
                    None,
                    content_type,
                    schema,
                ))
            })
            .collect()
    }

    fn from_part(name: &str, description: Option<String>, part: &ArgPart) -> Vec<Self> {
        flatten_arg_part(part)
            .into_iter()
            .filter_map(|flat| {
                if matches!(flat.part, ArgPart::Response(_)) {
                    return None;
                }
                let (location, schema, content_type) = arg_part(flat.part);
                Some(Self::from_schema(
                    name.to_string(),
                    location_label(location, flat),
                    description.clone(),
                    None,
                    content_type,
                    schema,
                ))
            })
            .collect()
    }

    fn from_return(ret: &ReturnSpec) -> Self {
        let (location, schema, content_type) = return_part(&ret.part);
        Self::from_schema(
            "response".to_string(),
            location,
            ret.description.clone(),
            ret.status_code,
            content_type,
            schema,
        )
    }

    fn from_schema(
        name: String,
        location: String,
        description: Option<String>,
        status_code: Option<u16>,
        content_type: Option<String>,
        schema: Option<&TypeSchema>,
    ) -> Self {
        let Some(schema) = schema else {
            return Self::empty(name, location, description, status_code, content_type);
        };
        let resolved = resolve_schema(schema);
        let display_name = display_name(&name, &resolved.schema, resolved.ref_name.as_deref());
        let schema_type = type_label(&resolved.schema);
        let properties = property_views(&resolved.schema);
        let raw_schema = serde_json::to_string_pretty(&resolved.schema).ok();
        Self {
            name,
            location,
            description,
            status_code,
            content_type,
            display_name,
            schema_type,
            ref_name: resolved.ref_name,
            properties,
            raw_schema,
            unresolved_ref: resolved.unresolved_ref,
        }
    }

    fn empty(
        name: String,
        location: String,
        description: Option<String>,
        status_code: Option<u16>,
        content_type: Option<String>,
    ) -> Self {
        Self {
            display_name: name.clone(),
            name,
            location,
            description,
            status_code,
            content_type,
            schema_type: "unknown".to_string(),
            ref_name: None,
            properties: Vec::new(),
            raw_schema: None,
            unresolved_ref: false,
        }
    }
}

struct ResolvedSchema {
    schema: Value,
    ref_name: Option<String>,
    unresolved_ref: bool,
}

fn arg_part(part: &ArgPart) -> (String, Option<&TypeSchema>, Option<String>) {
    match part {
        ArgPart::Header(schema) => ("header".into(), Some(schema), None),
        ArgPart::Cookie(schema) => ("cookie".into(), Some(schema), None),
        ArgPart::Query(schema) => ("query".into(), Some(schema), None),
        ArgPart::Path(schema) => ("path".into(), Some(schema), None),
        ArgPart::Body(schema, content_type) => {
            ("body".into(), Some(schema), Some(content_type.to_string()))
        }
        ArgPart::BodyWith {
            schema,
            content_type,
            ..
        } => ("body".into(), Some(schema), Some(content_type.to_string())),
        ArgPart::Security { scheme, .. } => (format!("security: {scheme}"), None, None),
        ArgPart::Response(_) => ("response".into(), None, None),
        ArgPart::Composite(_) => ("composite".into(), None, None),
        ArgPart::Optional(_) => ("optional".into(), None, None),
        ArgPart::Fallible(_) => ("fallible".into(), None, None),
        ArgPart::Zone => ("zone".into(), None, None),
        ArgPart::Ignore | ArgPart::Authentication => ("runtime".into(), None, None),
    }
}

fn return_part(part: &ReturnPart) -> (String, Option<&TypeSchema>, Option<String>) {
    match part {
        ReturnPart::Header(schema) => ("header".into(), Some(schema), None),
        ReturnPart::Body(schema, content_type) => {
            ("body".into(), Some(schema), Some(content_type.to_string()))
        }
        ReturnPart::Created(schema, content_type) => (
            "created".into(),
            Some(schema),
            Some(content_type.to_string()),
        ),
        ReturnPart::Accepted(schema, content_type) => (
            "accepted".into(),
            Some(schema),
            Some(content_type.to_string()),
        ),
        ReturnPart::Empty => ("empty".into(), None, None),
        ReturnPart::Redirect { status_code } => (
            format!("redirect {status_code}"),
            None,
            Some("Location".into()),
        ),
        ReturnPart::Binary(content_type) => ("binary".into(), None, Some(content_type.to_string())),
        ReturnPart::Unknown => ("unknown".into(), None, None),
    }
}

fn operation_returns(op: &Operation) -> Vec<SchemaView> {
    let mut returns = op
        .returns
        .iter()
        .map(SchemaView::from_return)
        .collect::<Vec<_>>();
    for arg in &op.args {
        for part in flatten_arg_part(&arg.part) {
            if part.suppresses_responses() {
                continue;
            }
            if let ArgPart::Response(specs) = part.part {
                returns.extend(specs.iter().map(SchemaView::from_return));
            }
        }
    }
    returns
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
        ArgPart::Optional(nested) => push_flattened_arg_part(nested, true, fallible, output),
        ArgPart::Fallible(nested) => push_flattened_arg_part(nested, optional, true, output),
        other => output.push(FlatArgPart {
            part: other,
            optional,
            fallible,
        }),
    }
}

fn location_label(location: String, part: FlatArgPart<'_>) -> String {
    match (part.optional, part.fallible) {
        (true, true) => format!("optional fallible {location}"),
        (true, false) => format!("optional {location}"),
        (false, true) => format!("fallible {location}"),
        (false, false) => location,
    }
}

fn resolve_schema(schema: &TypeSchema) -> ResolvedSchema {
    let settings = SchemaSettings::draft07();
    let mut generator = schemars::SchemaGenerator::new(settings);
    let root = schema.schema(&mut generator).to_value();
    let definitions = generator.definitions_mut().clone();
    resolve_value(root, &definitions)
}

fn resolve_value(root: Value, definitions: &Map<String, Value>) -> ResolvedSchema {
    let Some(reference) = root.get("$ref").and_then(Value::as_str) else {
        return ResolvedSchema {
            schema: root,
            ref_name: None,
            unresolved_ref: false,
        };
    };
    let name = ref_name(reference).map(str::to_string);
    let schema = name
        .as_deref()
        .and_then(|key| definitions.get(key).cloned())
        .or_else(|| defs_lookup(&root, name.as_deref()));
    match schema {
        Some(schema) => ResolvedSchema {
            schema,
            ref_name: name,
            unresolved_ref: false,
        },
        None => ResolvedSchema {
            schema: root,
            ref_name: name,
            unresolved_ref: true,
        },
    }
}

fn defs_lookup(root: &Value, name: Option<&str>) -> Option<Value> {
    let name = name?;
    root.get("definitions")
        .or_else(|| root.get("$defs"))
        .and_then(Value::as_object)
        .and_then(|defs| defs.get(name))
        .cloned()
}

fn ref_name(reference: &str) -> Option<&str> {
    reference
        .strip_prefix("#/definitions/")
        .or_else(|| reference.strip_prefix("#/$defs/"))
}

fn display_name(fallback: &str, schema: &Value, ref_name: Option<&str>) -> String {
    schema
        .get("title")
        .and_then(Value::as_str)
        .or(ref_name)
        .unwrap_or(fallback)
        .to_string()
}

fn property_views(schema: &Value) -> Vec<PropertyView> {
    let required = required_fields(schema);
    schema
        .get("properties")
        .and_then(Value::as_object)
        .map(|props| properties_from_map(props, &required))
        .unwrap_or_default()
}

fn required_fields(schema: &Value) -> Vec<&str> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}

fn properties_from_map(props: &Map<String, Value>, required: &[&str]) -> Vec<PropertyView> {
    props
        .iter()
        .map(|(name, schema)| PropertyView {
            name: name.clone(),
            type_label: type_label(schema),
            required: required.contains(&name.as_str()),
            description: schema
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
        .collect()
}

fn type_label(schema: &Value) -> String {
    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        return ref_name(reference).unwrap_or(reference).to_string();
    }
    if let Some(kind) = schema.get("type").and_then(Value::as_str) {
        return formatted_type(kind, schema);
    }
    if let Some(items) = schema.get("type").and_then(Value::as_array) {
        return items
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(" | ");
    }
    composite_type(schema).unwrap_or_else(|| "schema".to_string())
}

fn formatted_type(kind: &str, schema: &Value) -> String {
    match (kind, schema.get("items")) {
        ("array", Some(items)) => format!("array<{}>", type_label(items)),
        _ => schema
            .get("format")
            .and_then(Value::as_str)
            .map(|format| format!("{kind}:{format}"))
            .unwrap_or_else(|| kind.to_string()),
    }
}

fn composite_type(schema: &Value) -> Option<String> {
    ["oneOf", "anyOf", "allOf"]
        .iter()
        .find(|key| schema.get(**key).is_some())
        .map(|key| (*key).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn inline_schema_lists_properties() {
        let view = resolve_value(
            json!({
                "title": "InvoiceSignal",
                "type": "object",
                "required": ["invoice_id"],
                "properties": {
                    "invoice_id": { "type": "string" },
                    "amount": { "type": "number", "description": "Total amount" }
                }
            }),
            &Map::new(),
        );
        let properties = property_views(&view.schema);
        assert_eq!(
            display_name("response", &view.schema, None),
            "InvoiceSignal"
        );
        assert_eq!(properties.len(), 2);
        assert!(
            properties
                .iter()
                .any(|prop| prop.name == "invoice_id" && prop.required)
        );
    }

    #[test]
    fn definitions_ref_resolves() {
        let mut definitions = Map::new();
        definitions.insert(
            "InvoiceSignal".to_string(),
            json!({
                "type": "object",
                "properties": { "invoice_id": { "type": "string" } }
            }),
        );
        let view = resolve_value(
            json!({ "$ref": "#/definitions/InvoiceSignal" }),
            &definitions,
        );
        assert_eq!(view.ref_name.as_deref(), Some("InvoiceSignal"));
        assert!(!view.unresolved_ref);
        assert_eq!(property_views(&view.schema).len(), 1);
    }

    #[test]
    fn defs_ref_resolves() {
        let view = resolve_value(
            json!({
                "$ref": "#/$defs/InvoiceSignal",
                "$defs": {
                    "InvoiceSignal": {
                        "type": "object",
                        "properties": { "invoice_id": { "type": "string" } }
                    }
                }
            }),
            &Map::new(),
        );
        assert_eq!(view.ref_name.as_deref(), Some("InvoiceSignal"));
        assert!(!view.unresolved_ref);
        assert_eq!(property_views(&view.schema).len(), 1);
    }

    #[test]
    fn unresolved_ref_keeps_raw_ref() {
        let view = resolve_value(json!({ "$ref": "#/definitions/Missing" }), &Map::new());
        assert_eq!(view.ref_name.as_deref(), Some("Missing"));
        assert!(view.unresolved_ref);
        assert_eq!(type_label(&view.schema), "Missing");
    }
}
