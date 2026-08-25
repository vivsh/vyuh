use indexmap::IndexMap;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use super::error::CommandError;

// ── arg types ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) enum CommandArgType {
    String,
    Number,
    Integer,
    Boolean,
    Array(Box<CommandArgType>),
}

impl CommandArgType {
    pub(crate) fn type_name(&self) -> &'static str {
        match self {
            CommandArgType::String => "string",
            CommandArgType::Number => "number",
            CommandArgType::Integer => "integer",
            CommandArgType::Boolean => "boolean",
            CommandArgType::Array(inner) => match **inner {
                CommandArgType::String => "string[]",
                CommandArgType::Number => "number[]",
                CommandArgType::Integer => "integer[]",
                CommandArgType::Boolean => "boolean[]",
                _ => "array",
            },
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CommandArg {
    pub(crate) name: String,
    pub(crate) flag: String,
    pub(crate) arg_type: CommandArgType,
    pub(crate) required: bool,
    pub(crate) description: Option<String>,
    pub(crate) hints: Vec<String>,
}

impl CommandArg {
    pub(crate) fn flag_name(&self) -> &str {
        &self.flag
    }
}

// ── schema parsing ────────────────────────────────────────────────────────────

pub(super) fn parse_schema_to_args(
    schema: &schemars::Schema,
) -> Result<Vec<CommandArg>, CommandError> {
    let root_obj = schema
        .as_object()
        .ok_or_else(|| CommandError::UnsupportedSchema("schema is not an object".into()))?;
    let schema_obj = resolve_root_schema_object(root_obj)?;

    let Some(properties) = schema_obj.get("properties").and_then(|p| p.as_object()) else {
        if schema_obj
            .get("type")
            .and_then(|value| value.as_str())
            .is_some_and(|value| value == "object")
        {
            return Ok(Vec::new());
        }
        return Err(CommandError::UnsupportedSchema(
            "Command argument schema must be an object with named fields".into(),
        ));
    };

    let required_fields: Vec<String> = schema_obj
        .get("required")
        .and_then(|r| r.as_array())
        .map_or(Vec::new(), |arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        });

    let mut args = Vec::with_capacity(properties.len());
    for (prop, prop_schema) in properties {
        let prop_obj = prop_schema.as_object().ok_or_else(|| {
            CommandError::UnsupportedSchema("property schema is not an object".into())
        })?;

        let arg_type = parse_arg_type(prop_obj)?;
        let required = required_fields.contains(prop);
        let description = prop_obj
            .get("description")
            .and_then(|d| d.as_str())
            .map(|s| s.to_string());
        let hints = collect_hints(prop_obj);

        args.push(CommandArg {
            name: prop.to_string(),
            flag: to_flag_name(prop),
            arg_type,
            required,
            description,
            hints,
        });
    }
    Ok(args)
}

fn collect_hints(prop_obj: &Map<String, Value>) -> Vec<String> {
    let mut hints = Vec::new();

    if let Some(min) = prop_obj.get("minLength").and_then(Value::as_u64) {
        hints.push(format!("min length: {min}"));
    }
    if let Some(max) = prop_obj.get("maxLength").and_then(Value::as_u64) {
        hints.push(format!("max length: {max}"));
    }
    if let Some(format) = prop_obj.get("format").and_then(Value::as_str) {
        hints.push(format!("format: {format}"));
    }
    if let Some(min) = prop_obj.get("minimum") {
        let prefix = if prop_obj
            .get("exclusiveMinimum")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            ">"
        } else {
            ">="
        };
        hints.push(format!("value: {prefix} {min}"));
    }
    if let Some(max) = prop_obj.get("maximum") {
        let prefix = if prop_obj
            .get("exclusiveMaximum")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            "<"
        } else {
            "<="
        };
        hints.push(format!("value: {prefix} {max}"));
    }
    if let Some(multiple) = prop_obj.get("multipleOf") {
        hints.push(format!("multiple of: {multiple}"));
    }
    if let Some(min) = prop_obj.get("minItems").and_then(Value::as_u64) {
        hints.push(format!("min items: {min}"));
    }
    if let Some(max) = prop_obj.get("maxItems").and_then(Value::as_u64) {
        hints.push(format!("max items: {max}"));
    }
    if prop_obj
        .get("uniqueItems")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        hints.push("unique items".to_string());
    }
    if let Some(values) = prop_obj.get("enum").and_then(Value::as_array) {
        let choices = values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| value.to_string())
            })
            .collect::<Vec<_>>();
        if !choices.is_empty() {
            hints.push(format!("choices: {}", choices.join(", ")));
        }
    }
    if let Some(validators) = prop_obj.get("x-vyuh-validators").and_then(Value::as_array) {
        let names = validators
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>();
        if !names.is_empty() {
            hints.push(format!("validators: {}", names.join(", ")));
        }
    }
    if let Some(default) = prop_obj.get("default") {
        hints.push(format!("default: {}", format_default(default)));
    }

    hints
}

fn format_default(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| value.to_string())
}

fn resolve_root_schema_object(
    root_obj: &Map<String, Value>,
) -> Result<Map<String, Value>, CommandError> {
    let Some(reference) = root_obj.get("$ref").and_then(|v| v.as_str()) else {
        return Ok(root_obj.clone());
    };
    let path = reference.strip_prefix("#/").ok_or_else(|| {
        CommandError::UnsupportedSchema(format!("unsupported schema reference: {reference}"))
    })?;
    let mut value = Value::Object(root_obj.clone());
    for segment in path.split('/') {
        let decoded = segment.replace("~1", "/").replace("~0", "~");
        value = value
            .as_object()
            .and_then(|obj| obj.get(&decoded))
            .cloned()
            .ok_or_else(|| {
                CommandError::UnsupportedSchema(format!("schema reference not found: {reference}"))
            })?;
    }
    value.as_object().cloned().ok_or_else(|| {
        CommandError::UnsupportedSchema(format!("schema reference is not an object: {reference}"))
    })
}

fn extract_type_from_schema(schema_obj: &Map<String, Value>) -> Result<&str, CommandError> {
    if let Some(any_of) = schema_obj.get("anyOf") {
        // anyOf: [{type: "X"}, {type: "null"}]  — produced by schemars 0.8.x for Option<T>
        any_of
            .as_array()
            .and_then(|arr| {
                arr.iter().find_map(|v| {
                    v.as_object()
                        .and_then(|o| o.get("type"))
                        .and_then(|t| t.as_str())
                        .filter(|&s| s != "null")
                })
            })
            .ok_or_else(|| {
                CommandError::UnsupportedSchema("anyOf schema type not found or unsupported".into())
            })
    } else if let Some(type_val) = schema_obj.get("type") {
        match type_val {
            // "type": "string"  — scalar, most common case
            Value::String(s) => Ok(s.as_str()),
            // "type": ["string", "null"]  — produced by schemars 1.x for Option<T>
            Value::Array(arr) => arr
                .iter()
                .find_map(|v| v.as_str().filter(|&s| s != "null"))
                .ok_or_else(|| {
                    CommandError::UnsupportedSchema("type array has no non-null entry".into())
                }),
            _ => Err(CommandError::UnsupportedSchema(
                "Property type is not a string or array".into(),
            )),
        }
    } else {
        Err(CommandError::UnsupportedSchema(
            "Property type missing".into(),
        ))
    }
}

fn parse_array_type(prop_obj: &Map<String, Value>) -> Result<CommandArgType, CommandError> {
    let items_obj = prop_obj
        .get("items")
        .ok_or_else(|| CommandError::UnsupportedSchema("Array items schema missing".into()))?
        .as_object()
        .ok_or_else(|| {
            CommandError::UnsupportedSchema("Array item schema is not an object".into())
        })?;
    let item_type_str = extract_type_from_schema(items_obj)?;
    let item_arg_type = match item_type_str {
        "string" => CommandArgType::String,
        "number" => CommandArgType::Number,
        "integer" => CommandArgType::Integer,
        "boolean" => CommandArgType::Boolean,
        _ => return Err(CommandError::UnsupportedType(item_type_str.to_string())),
    };
    Ok(CommandArgType::Array(Box::new(item_arg_type)))
}

fn parse_arg_type(prop_obj: &Map<String, Value>) -> Result<CommandArgType, CommandError> {
    let type_str = extract_type_from_schema(prop_obj)?;
    match type_str {
        "string" => Ok(CommandArgType::String),
        "number" => Ok(CommandArgType::Number),
        "integer" => Ok(CommandArgType::Integer),
        "boolean" => Ok(CommandArgType::Boolean),
        "array" => parse_array_type(prop_obj),
        _ => Err(CommandError::UnsupportedType(type_str.to_string())),
    }
}

// ── cli arg parsing ───────────────────────────────────────────────────────────

pub(super) fn parse_args<T: DeserializeOwned + 'static>(
    command_name: &str,
    args: &[&str],
    arg_defs: &[CommandArg],
) -> Result<T, CommandError> {
    let arg_map = build_arg_map(arg_defs);
    let store = parse_flag_args(command_name, args, &arg_map)?;

    let mut obj = Map::new();
    for (key, values) in &store {
        let arg_def = arg_map
            .get(key.as_str())
            .ok_or_else(|| CommandError::UnknownFlag {
                command: command_name.to_string(),
                flag: key.clone(),
            })?;
        validate_arg_values(arg_def.flag_name(), values, &arg_def.arg_type)?;
        let json_value = convert_value(arg_def.flag_name(), values, &arg_def.arg_type)?;
        obj.insert(arg_def.name.clone(), json_value);
    }

    check_required_args(arg_defs, &obj)?;

    // Inject false for any boolean flag not supplied on the command line so that
    // serde can deserialize structs whose `bool` fields have no serde default.
    for arg_def in arg_defs {
        if matches!(arg_def.arg_type, CommandArgType::Boolean) && !obj.contains_key(&arg_def.name) {
            obj.insert(arg_def.name.clone(), Value::Bool(false));
        }
    }

    serde_json::from_value(Value::Object(obj))
        .map_err(|e| CommandError::DeserializeError(e.to_string()))
}

fn build_arg_map(arg_defs: &[CommandArg]) -> IndexMap<&str, &CommandArg> {
    let mut arg_map = IndexMap::new();
    for arg in arg_defs {
        arg_map.insert(arg.name.as_str(), arg);
        arg_map.insert(arg.flag.as_str(), arg);
    }
    arg_map
}

fn parse_flag_args(
    command_name: &str,
    args: &[&str],
    arg_map: &IndexMap<&str, &CommandArg>,
) -> Result<IndexMap<String, Vec<String>>, CommandError> {
    let mut store = IndexMap::new();
    let mut current_key: Option<String> = None;
    let mut i = 0;
    while i < args.len() {
        let arg = args[i];
        if arg.starts_with("--") {
            let (next_i, next_key) = handle_flag(command_name, arg, args, i, &mut store, arg_map)?;
            current_key = next_key;
            i = next_i;
        } else if let Some(key) = current_key.as_deref() {
            store
                .entry(key.to_string())
                .or_default()
                .push(arg.to_string());
            i += 1;
        } else {
            return Err(CommandError::UnexpectedArgument {
                command: command_name.to_string(),
                argument: arg.to_string(),
            });
        }
    }
    Ok(store)
}

fn handle_flag(
    command_name: &str,
    flag: &str,
    args: &[&str],
    i: usize,
    store: &mut IndexMap<String, Vec<String>>,
    arg_map: &IndexMap<&str, &CommandArg>,
) -> Result<(usize, Option<String>), CommandError> {
    let raw_key = flag.trim_start_matches("--");
    let (key, inline_value) = match raw_key.split_once('=') {
        Some((key, value)) => (key, Some(value)),
        None => (raw_key, None),
    };

    if let Some(stripped) = key.strip_prefix("no-") {
        if let Some(arg_def) = arg_map.get(stripped)
            && matches!(arg_def.arg_type, CommandArgType::Boolean)
        {
            insert_flag_value(command_name, store, arg_def, "false".to_string())?;
            return Ok((i + 1, None));
        }
        return Err(CommandError::UnknownFlag {
            command: command_name.to_string(),
            flag: key.to_string(),
        });
    }

    if let Some(arg_def) = arg_map.get(key) {
        if let Some(value) = inline_value {
            insert_flag_value(command_name, store, arg_def, value.to_string())?;
            return Ok((i + 1, None));
        }
        if matches!(arg_def.arg_type, CommandArgType::Boolean) {
            return handle_bool_flag(command_name, arg_def, args, i, store);
        }
    } else {
        return Err(CommandError::UnknownFlag {
            command: command_name.to_string(),
            flag: key.to_string(),
        });
    }

    let arg_def = arg_map.get(key).ok_or_else(|| CommandError::UnknownFlag {
        command: command_name.to_string(),
        flag: key.to_string(),
    })?;
    ensure_flag_can_start(command_name, store, arg_def)?;
    store.entry(arg_def.name.clone()).or_default();
    Ok((i + 1, Some(arg_def.name.clone())))
}

fn handle_bool_flag(
    command_name: &str,
    arg_def: &CommandArg,
    args: &[&str],
    i: usize,
    store: &mut IndexMap<String, Vec<String>>,
) -> Result<(usize, Option<String>), CommandError> {
    let next_is_bool_value = args
        .get(i + 1)
        .map(|next| !next.starts_with("--") && (*next == "true" || *next == "false"))
        .unwrap_or(false);

    if next_is_bool_value {
        if let Some(next_val) = args.get(i + 1) {
            insert_flag_value(command_name, store, arg_def, next_val.to_string())?;
            Ok((i + 2, None))
        } else {
            insert_flag_value(command_name, store, arg_def, "true".to_string())?;
            Ok((i + 1, None))
        }
    } else {
        insert_flag_value(command_name, store, arg_def, "true".to_string())?;
        Ok((i + 1, None))
    }
}

fn insert_flag_value(
    command_name: &str,
    store: &mut IndexMap<String, Vec<String>>,
    arg_def: &CommandArg,
    value: String,
) -> Result<(), CommandError> {
    ensure_flag_can_start(command_name, store, arg_def)?;
    store.entry(arg_def.name.clone()).or_default().push(value);
    Ok(())
}

fn ensure_flag_can_start(
    command_name: &str,
    store: &IndexMap<String, Vec<String>>,
    arg_def: &CommandArg,
) -> Result<(), CommandError> {
    if !matches!(arg_def.arg_type, CommandArgType::Array(_)) && store.contains_key(&arg_def.name) {
        return Err(CommandError::DuplicateFlag {
            command: command_name.to_string(),
            flag: arg_def.flag.clone(),
        });
    }
    Ok(())
}

fn validate_arg_values(
    key: &str,
    values: &[String],
    arg_type: &CommandArgType,
) -> Result<(), CommandError> {
    match arg_type {
        CommandArgType::Array(_) => {
            if values.is_empty() {
                return Err(CommandError::MissingValue {
                    flag: key.to_string(),
                });
            }
        }
        CommandArgType::Boolean => {
            if values.len() != 1 {
                return Err(CommandError::TooManyValues {
                    flag: key.to_string(),
                    count: values.len(),
                });
            }
        }
        _ => {
            if values.is_empty() {
                return Err(CommandError::MissingValue {
                    flag: key.to_string(),
                });
            }
            if values.len() > 1 {
                return Err(CommandError::TooManyValues {
                    flag: key.to_string(),
                    count: values.len(),
                });
            }
        }
    }
    Ok(())
}

fn check_required_args(
    arg_defs: &[CommandArg],
    obj: &Map<String, Value>,
) -> Result<(), CommandError> {
    for arg_def in arg_defs {
        // Boolean flags are never "required" from a CLI perspective: absence means false.
        if matches!(arg_def.arg_type, CommandArgType::Boolean) {
            continue;
        }
        if arg_def.required && !obj.contains_key(&arg_def.name) {
            return Err(CommandError::MissingRequired {
                flag: arg_def.flag.clone(),
            });
        }
    }
    Ok(())
}

fn convert_value(
    key: &str,
    values: &[String],
    arg_type: &CommandArgType,
) -> Result<Value, CommandError> {
    match arg_type {
        CommandArgType::Array(item_type) => {
            let mut arr = Vec::with_capacity(values.len());
            for val in values {
                arr.push(convert_single_value(key, val, item_type)?);
            }
            Ok(Value::Array(arr))
        }
        _ => {
            let val = values.first().ok_or_else(|| CommandError::MissingValue {
                flag: key.to_string(),
            })?;
            convert_single_value(key, val, arg_type)
        }
    }
}

fn convert_single_value(
    key: &str,
    value: &str,
    arg_type: &CommandArgType,
) -> Result<Value, CommandError> {
    match arg_type {
        CommandArgType::String => Ok(Value::String(value.to_string())),
        CommandArgType::Number => {
            let num: f64 =
                value
                    .parse()
                    .map_err(|e: std::num::ParseFloatError| CommandError::ParseError {
                        flag: key.to_string(),
                        value: value.to_string(),
                        expected_type: "number".to_string(),
                        error: e.to_string(),
                    })?;
            Ok(Value::Number(
                serde_json::Number::from_f64(num).ok_or_else(|| CommandError::ParseError {
                    flag: key.to_string(),
                    value: value.to_string(),
                    expected_type: "number".to_string(),
                    error: "invalid floating point value".to_string(),
                })?,
            ))
        }
        CommandArgType::Integer => {
            let num: i64 =
                value
                    .parse()
                    .map_err(|e: std::num::ParseIntError| CommandError::ParseError {
                        flag: key.to_string(),
                        value: value.to_string(),
                        expected_type: "integer".to_string(),
                        error: e.to_string(),
                    })?;
            Ok(Value::Number(serde_json::Number::from(num)))
        }
        CommandArgType::Boolean => {
            let b: bool =
                value
                    .parse()
                    .map_err(|e: std::str::ParseBoolError| CommandError::ParseError {
                        flag: key.to_string(),
                        value: value.to_string(),
                        expected_type: "boolean".to_string(),
                        error: e.to_string(),
                    })?;
            Ok(Value::Bool(b))
        }
        CommandArgType::Array(_) => Err(CommandError::UnsupportedSchema(
            "cannot convert single value to array".into(),
        )),
    }
}

fn to_flag_name(name: &str) -> String {
    name.replace('_', "-")
}
