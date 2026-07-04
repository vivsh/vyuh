use std::collections::BTreeSet;

use indexmap::IndexMap;
use openapiv3::ReferenceOr;
use thiserror::Error;

/// Errors that can occur during schema conversion.
#[derive(Debug, Error)]
pub enum SchemaConversionError {
    #[error("failed to serialize schema '{name}': {source}")]
    Serialization {
        name: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to deserialize schema '{name}': {source}")]
    Deserialization {
        name: String,
        #[source]
        source: serde_json::Error,
    },
}

/// Registry for OpenAPI schema components to enable deduplication via refs.
/// Uses schemars::SchemaGenerator internally to collect all definitions.
#[derive(Default)]
pub struct ComponentRegistry {
    components: IndexMap<String, openapiv3::Schema>,
    generator: schemars::SchemaGenerator,
    security_schemes: BTreeSet<String>,
    operation_security: Vec<SecurityRequirement>,
    pub(crate) tags: BTreeSet<String>,
}

/// Security requirement collected while building one operation.
#[derive(Debug, Clone)]
pub struct SecurityRequirement {
    pub scheme: String,
    pub scopes: Vec<String>,
    pub join_all: bool,
    pub optional: bool,
}

impl std::fmt::Debug for ComponentRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ComponentRegistry")
            .field("components", &self.components)
            .field("security_schemes", &self.security_schemes)
            .field("operation_security", &self.operation_security)
            .field("tags", &self.tags)
            .finish_non_exhaustive()
    }
}

impl ComponentRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            components: IndexMap::new(),
            generator: schemars::SchemaGenerator::default(),
            security_schemes: BTreeSet::new(),
            operation_security: Vec::new(),
            tags: BTreeSet::new(),
        }
    }

    /// Register a schema with a given name. Returns the reference path.
    pub fn register(&mut self, name: String, schema: openapiv3::Schema) -> String {
        let ref_path = format!("#/components/schemas/{}", name);
        self.components.insert(name, schema);
        ref_path
    }

    pub fn register_security(
        &mut self,
        name: String,
        scopes: &[String],
        join_all: bool,
        optional: bool,
    ) {
        self.security_schemes.insert(name.clone());
        self.operation_security.push(SecurityRequirement {
            scheme: name,
            scopes: scopes.to_vec(),
            join_all,
            optional,
        });
    }

    pub fn has_security_schemes(&self) -> bool {
        !self.security_schemes.is_empty()
    }

    pub fn get_security_scheme_names(&self) -> Vec<String> {
        self.security_schemes.iter().cloned().collect()
    }

    pub fn drain_operation_security(&mut self) -> Vec<SecurityRequirement> {
        std::mem::take(&mut self.operation_security)
    }

    pub fn has_operation_security(&self) -> bool {
        !self.operation_security.is_empty()
    }

    /// Check if a schema name is already registered.
    pub fn contains(&self, name: &str) -> bool {
        self.components.contains_key(name)
    }

    /// Consume the registry and return the component schemas map.
    pub fn into_components(self) -> IndexMap<String, openapiv3::ReferenceOr<openapiv3::Schema>> {
        self.components
            .into_iter()
            .map(|(k, v)| (k, openapiv3::ReferenceOr::Item(v)))
            .collect()
    }

    /// Consume the registry and return the component schemas map for OpenAPI 3.0.
    /// Extracts all definitions from the schemars generator and converts them.
    pub fn into_components_schemars(
        mut self,
    ) -> Result<IndexMap<String, openapiv3::ReferenceOr<openapiv3::Schema>>, SchemaConversionError>
    {
        let definitions = std::mem::take(self.generator.definitions_mut());
        let mut result = IndexMap::with_capacity(definitions.len());

        for (name, json_schema) in definitions {
            // definitions_mut returns Map<String, serde_json::Value>
            let openapi_schema = convert_json_value_to_openapi(json_schema, &name)?;
            result.insert(name, openapi_schema);
        }

        Ok(result)
    }

    /// Get mutable reference to the schemars generator.
    pub fn generator_mut(&mut self) -> &mut schemars::SchemaGenerator {
        &mut self.generator
    }
}

/// Convert JSON value (from schemars) to OpenAPI Schema
fn convert_json_value_to_openapi(
    mut json_value: serde_json::Value,
    name: &str,
) -> Result<ReferenceOr<openapiv3::Schema>, SchemaConversionError> {
    // Handle $ref before transformation
    if let Some(ref_str) = json_value.get("$ref").and_then(|v| v.as_str()) {
        let openapi_ref = ref_str
            .replace("#/$defs/", "#/components/schemas/")
            .replace("#/definitions/", "#/components/schemas/");
        return Ok(ReferenceOr::Reference {
            reference: openapi_ref,
        });
    }

    // Transform in-place for efficiency
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
/// Main conversion: type arrays like `["integer", "null"]` → `"type": "integer", "nullable": true`
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

        // Transform type arrays to nullable
        if let Some(type_val) = map.get("type").and_then(|v| v.as_array()).cloned() {
            transform_type_array(map, &type_val);
        }

        // Recurse into all nested values
        // Special handling for properties (object of schemas)
        if let Some(serde_json::Value::Object(props)) = map.get_mut("properties") {
            for (_prop_name, prop_schema) in props.iter_mut() {
                transform_for_openapi(prop_schema);
            }
        }

        // Handle other schema-containing fields
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

        // Handle composition arrays (allOf, anyOf, oneOf)
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

/// Transform type array to OpenAPI nullable format
fn transform_type_array(
    map: &mut serde_json::Map<String, serde_json::Value>,
    types: &[serde_json::Value],
) {
    let (has_null, non_null): (Vec<_>, Vec<_>) =
        types.iter().partition(|v| v.as_str() == Some("null"));

    match non_null.len() {
        0 => {} // Keep as-is if only null
        1 => {
            // Single type + optional null → type with nullable
            map.insert("type".to_string(), non_null[0].clone());
            if !has_null.is_empty() {
                map.insert("nullable".to_string(), serde_json::Value::Bool(true));
            }
        }
        _ => {
            // Multiple non-null types → anyOf
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
