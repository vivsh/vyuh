use schemars::JsonSchema;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::any::TypeId;
use std::borrow::Cow;
use std::future::Future;

use crate::validation::ValidationReport;

// Custom tuple types to represent handler arguments internally.
// These prevent users from accidentally using std tuples as argument types.
pub struct Tuple1<T1>(pub(crate) T1);
pub struct Tuple2<T1, T2>(pub(crate) T1, pub(crate) T2);
pub struct Tuple3<T1, T2, T3>(pub(crate) T1, pub(crate) T2, pub(crate) T3);
pub struct Tuple4<T1, T2, T3, T4>(pub(crate) T1, pub(crate) T2, pub(crate) T3, pub(crate) T4);
pub struct Tuple5<T1, T2, T3, T4, T5>(
    pub(crate) T1,
    pub(crate) T2,
    pub(crate) T3,
    pub(crate) T4,
    pub(crate) T5,
);
pub struct Tuple6<T1, T2, T3, T4, T5, T6>(
    pub(crate) T1,
    pub(crate) T2,
    pub(crate) T3,
    pub(crate) T4,
    pub(crate) T5,
    pub(crate) T6,
);

/// Handler data type constraint: serde + JSON schema + thread-safe.
///
/// Automatically implemented for all types satisfying the bounds.
/// DataValue ensures that Arc wrapped value is always Send
pub trait DataValue: JsonSchema + Serialize + DeserializeOwned + Send + Sync + 'static {}

impl<T> DataValue for T where T: JsonSchema + Serialize + DeserializeOwned + Send + Sync + 'static {}
/// Errors that can occur during handler execution, extraction, and deserialization.
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum CallError {
    /// Failed to deserialize payload from JSON
    #[error("Failed to deserialize payload")]
    DeserializeFailed,

    #[error("Failed to serialize payload")]
    SerializeFailed,

    /// Data type mismatch during extraction
    #[error("Data type mismatch")]
    TypeMismatch,

    /// Context extraction failed
    #[error("Extraction failed: {0}")]
    ExtractionFailed(Cow<'static, str>),

    /// Required field is missing
    #[error("Missing required field: {0}")]
    MissingField(Cow<'static, str>),

    /// Invalid argument provided
    #[error("Invalid argument: {0}")]
    InvalidArgument(Cow<'static, str>),

    /// Input parsed successfully but failed validation.
    #[error("Validation failed")]
    Validation(ValidationReport),

    /// Unauthorized access
    #[error("Unauthorized")]
    Unauthorized,

    /// Authenticated identity lacks the required application permission.
    #[error("Forbidden")]
    Forbidden,

    /// Resource not found
    #[error("Not found: {0}")]
    NotFound(Cow<'static, str>),

    /// Catch-all for any other error type
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl From<std::convert::Infallible> for CallError {
    fn from(e: std::convert::Infallible) -> Self {
        match e {}
    }
}

/// Compile-time type metadata for JSON schema generation and runtime identity.
#[derive(Clone)]
pub struct TypeSchema {
    pub(crate) type_schema: fn(&mut schemars::SchemaGenerator) -> schemars::Schema,

    #[cfg(feature = "mcp")]
    pub(crate) root_schema: fn() -> schemars::Schema,

    pub(crate) type_id: fn() -> TypeId,

    pub(crate) type_name: fn() -> &'static str,

    pub(crate) validated: bool,
}

impl std::fmt::Debug for TypeSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct((self.type_name)()).finish()
    }
}

impl serde::Serialize for TypeSchema {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use schemars::generate::SchemaSettings;
        let settings = SchemaSettings::draft07();
        let mut generator = schemars::SchemaGenerator::new(settings);
        let schema = (self.type_schema)(&mut generator);
        schema.serialize(serializer)
    }
}

impl TypeSchema {
    /// Captures compile-time type metadata for `T`.
    pub fn wrap<T: JsonSchema + 'static>() -> Self {
        fn converter<T: JsonSchema>(genr: &mut schemars::SchemaGenerator) -> schemars::Schema {
            genr.subschema_for::<T>()
        }
        #[cfg(feature = "mcp")]
        fn root<T: JsonSchema>() -> schemars::Schema {
            use schemars::generate::SchemaSettings;
            schemars::SchemaGenerator::new(SchemaSettings::draft07()).into_root_schema_for::<T>()
        }
        Self {
            type_schema: converter::<T>,
            #[cfg(feature = "mcp")]
            root_schema: root::<T>,
            type_id: || TypeId::of::<T>(),
            type_name: || std::any::type_name::<T>(),
            validated: false,
        }
    }

    /// Captures an opaque binary HTTP body schema.
    pub fn binary_body() -> Self {
        fn converter(_genr: &mut schemars::SchemaGenerator) -> schemars::Schema {
            schemars::json_schema!({
                "type": "string",
                "format": "binary"
            })
        }
        #[cfg(feature = "mcp")]
        fn root() -> schemars::Schema {
            schemars::json_schema!({
                "type": "string",
                "format": "binary"
            })
        }
        Self {
            type_schema: converter,
            #[cfg(feature = "mcp")]
            root_schema: root,
            type_id: || TypeId::of::<axum::body::Bytes>(),
            type_name: || "axum::body::Bytes",
            validated: false,
        }
    }

    /// Captures schema metadata for parse-only route input.
    pub fn wrap_unvalidated<T: JsonSchema + 'static>() -> Self {
        fn converter<T: JsonSchema>(genr: &mut schemars::SchemaGenerator) -> schemars::Schema {
            let schema = genr.subschema_for::<T>();
            let mut value = schema.to_value();
            strip_validation_keywords(&mut value);
            for definition in genr.definitions_mut().values_mut() {
                strip_validation_keywords(definition);
            }
            schemars::Schema::try_from(value).unwrap_or_default()
        }
        #[cfg(feature = "mcp")]
        fn root<T: JsonSchema>() -> schemars::Schema {
            use schemars::generate::SchemaSettings;
            let schema = schemars::SchemaGenerator::new(SchemaSettings::draft07())
                .into_root_schema_for::<T>();
            let mut value = schema.to_value();
            strip_validation_keywords(&mut value);
            schemars::Schema::try_from(value).unwrap_or_default()
        }
        Self {
            type_schema: converter::<T>,
            #[cfg(feature = "mcp")]
            root_schema: root::<T>,
            type_id: || TypeId::of::<T>(),
            type_name: || std::any::type_name::<T>(),
            validated: false,
        }
    }

    /// Captures schema metadata for a validated route input.
    pub fn wrap_valid<T>() -> Self
    where
        T: JsonSchema + crate::validation::ValidationSchema + 'static,
    {
        fn converter<T>(_genr: &mut schemars::SchemaGenerator) -> schemars::Schema
        where
            T: JsonSchema + crate::validation::ValidationSchema,
        {
            let mut settings = schemars::generate::SchemaSettings::default();
            settings.inline_subschemas = true;
            let mut genr = schemars::SchemaGenerator::new(settings);
            let schema = genr.subschema_for::<T>();
            let mut value = schema.to_value();
            T::apply_validation_schema(&mut value, genr.definitions_mut());
            schemars::Schema::try_from(value).unwrap_or_default()
        }
        #[cfg(feature = "mcp")]
        fn root<T>() -> schemars::Schema
        where
            T: JsonSchema + crate::validation::ValidationSchema,
        {
            converter::<T>(&mut schemars::SchemaGenerator::default())
        }
        Self {
            type_schema: converter::<T>,
            #[cfg(feature = "mcp")]
            root_schema: root::<T>,
            type_id: || TypeId::of::<T>(),
            type_name: || std::any::type_name::<T>(),
            validated: true,
        }
    }

    /// Generates JSON schema using provided generator.
    pub fn schema(&self, genr: &mut schemars::SchemaGenerator) -> schemars::Schema {
        (self.type_schema)(genr)
    }

    /// Generates a self-contained root schema for MCP transport contracts.
    #[cfg(feature = "mcp")]
    pub(crate) fn root_schema(&self) -> schemars::Schema {
        (self.root_schema)()
    }

    /// Returns runtime `TypeId` for type checking.
    pub fn type_id(&self) -> TypeId {
        (self.type_id)()
    }

    /// Returns true when this schema came from a validated extractor.
    pub fn is_validated(&self) -> bool {
        self.validated
    }
}

fn strip_validation_keywords(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for key in [
                "minLength",
                "maxLength",
                "pattern",
                "minimum",
                "maximum",
                "exclusiveMinimum",
                "exclusiveMaximum",
                "multipleOf",
                "minItems",
                "maxItems",
                "uniqueItems",
            ] {
                map.remove(key);
            }
            if map.get("format").and_then(|value| value.as_str()) != Some("binary") {
                map.remove("format");
            }

            for nested in map.values_mut() {
                strip_validation_keywords(nested);
            }
        }
        serde_json::Value::Array(values) => {
            for nested in values {
                strip_validation_keywords(nested);
            }
        }
        _ => {}
    }
}

/// Describes how a handler argument is extracted from requests.
#[derive(Debug, Clone, Serialize)]
pub enum ArgPart {
    /// Runtime-injected argument (e.g. `Site`, `State<T>`); not visible in OpenAPI.
    Ignore,
    /// Raw HTTP request access, tracked separately for MCP route eligibility.
    #[cfg(feature = "mcp")]
    RawRequest,
    /// Extracted from HTTP headers
    Header(TypeSchema),

    /// Extracted from HTTP cookies
    Cookie(TypeSchema),

    /// Extracted from query string parameters
    Query(TypeSchema),

    /// Extracted from URL path parameters
    Path(TypeSchema),

    /// Extracted from request body with specified content type
    Body(TypeSchema, Cow<'static, str>),

    /// Extracted from a request body with additional body-specific metadata.
    BodyWith {
        schema: TypeSchema,
        content_type: Cow<'static, str>,
        multipart: Option<MultipartApiSpec>,
    },

    /// Security credentials (API key, OAuth token, etc.)
    Security {
        scheme: Cow<'static, str>,
        scopes: Vec<Cow<'static, str>>,
        join_all: bool,
    },

    /// Application role requirement retained for MCP discovery authorization.
    #[cfg(feature = "mcp")]
    Authorization {
        mask: crate::auth::RoleType,
        join_all: bool,
    },

    /// Internal marker used to validate that authenticated Vyuh routes belong
    /// to an audience-bearing bundle.
    Authentication,

    /// Response metadata implied by an argument extractor or wrapper.
    Response(Vec<ReturnSpec>),

    /// Multiple metadata parts contributed by one argument type.
    Composite(Vec<ArgPart>),

    /// Optional wrapper around an argument part.
    Optional(Box<ArgPart>),

    /// Fallible wrapper around an argument part.
    Fallible(Box<ArgPart>),

    /// Multi-tenancy zone identifier
    Zone,
}

impl ArgPart {
    pub(crate) fn requires_auth(&self) -> bool {
        match self {
            Self::Authentication => true,
            Self::Composite(parts) => parts.iter().any(Self::requires_auth),
            Self::Optional(part) | Self::Fallible(part) => part.requires_auth(),
            _ => false,
        }
    }

    /// Returns the first request-body schema carried by this part.
    pub fn body_schema(&self) -> Option<&TypeSchema> {
        match self {
            Self::Body(schema, _) => Some(schema),
            Self::BodyWith { schema, .. } => Some(schema),
            Self::Composite(parts) => parts.iter().find_map(Self::body_schema),
            Self::Optional(part) | Self::Fallible(part) => part.body_schema(),
            _ => None,
        }
    }
}

/// OpenAPI-safe metadata derived from a typed multipart upload contract.
#[derive(Debug, Clone, Serialize)]
pub struct MultipartApiSpec {
    /// Multipart fields known to the typed parser.
    pub fields: Vec<MultipartApiField>,
    /// Whether fields outside the declared contract are accepted.
    pub allow_unknown: bool,
}

/// Multipart field kind used for request-body documentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MultipartApiFieldKind {
    Text,
    File,
}

/// OpenAPI-safe metadata for one multipart field.
#[derive(Debug, Clone, Serialize)]
pub struct MultipartApiField {
    /// Multipart field name.
    pub name: String,
    /// Whether this is a text or file field.
    pub kind: MultipartApiFieldKind,
    /// Whether at least one value is required.
    pub required: bool,
    /// Whether repeated values are accepted.
    pub multiple: bool,
    /// Maximum text length in Unicode scalar values.
    pub max_length: Option<usize>,
    /// Maximum accepted field or file size in bytes.
    pub max_bytes: Option<u64>,
    /// Allowed declared file content types.
    pub content_types: Vec<String>,
    /// Allowed file name extensions without leading dots.
    pub extensions: Vec<String>,
    /// Optional sniffing rule name.
    pub sniff: Option<String>,
}

/// Describes how a handler return value is serialized into responses.
#[derive(Debug, Clone, Serialize)]
pub enum ReturnPart {
    /// Written to HTTP response headers
    Header(TypeSchema),

    /// Serialized to response body with specified content type
    Body(TypeSchema, Cow<'static, str>),

    /// Response body for a 201 Created reply; defaults the status code to 201 without needing an explicit `ReturnSpec::status_code`.
    Created(TypeSchema, Cow<'static, str>),

    /// Response body for a 202 Accepted reply.
    Accepted(TypeSchema, Cow<'static, str>),

    /// No content (e.g., 204 No Content)
    Empty,

    /// Redirect response with a `Location` header.
    Redirect { status_code: u16 },

    /// Binary or streaming response body with the given content type.
    Binary(Cow<'static, str>),

    /// Return type could not be statically described (e.g. raw `axum::response::Response`).
    Unknown,
}

/// Provides compile-time extraction metadata for handler arguments.
pub trait IntoArgPart {
    fn into_arg_part() -> ArgPart;
}

/// Provides compile-time extraction metadata for middleware layer or decorator arguments.
pub trait IntoLayerParts {
    fn into_layer_parts() -> Vec<ArgPart>;
}

/// Provides compile-time serialization metadata for handler returns.
pub trait IntoReturnPart: Send {
    fn into_return_part() -> ReturnPart;
}

/// Marker trait indicating handler arguments contain `Data<T>`.
pub trait HasData<T: DataValue> {}

/// Provides argument specifications for handler signature introspection.
pub trait IntoArgSpecs {
    fn into_arg_specs() -> Vec<ArgSpec>;
}

/// Async handler with typed arguments and output.
///
/// Automatically implemented for functions and closures via macro.
pub trait Specable<Args: IntoArgSpecs>: Send {
    type Output: IntoReturnPart;
    type Future: Future<Output = Self::Output> + Send;

    fn call(&self, args: Args) -> Self::Future;
}

/// Provides enhanced handler specification with real parameter names.
///
/// Primarily for proc macros to override auto-generated specs with names
/// and descriptions extracted from source code.
pub trait IntoHandlerSpec {
    fn into_spec() -> CallSpec;
}

impl IntoArgSpecs for () {
    fn into_arg_specs() -> Vec<ArgSpec> {
        vec![]
    }
}

/// Implementations of IntoArgSpecs for tuple types
macro_rules! impl_argspec {
    (
        [$($ty:ident),*], $last:ident, $tuple:ident
    ) => {
        impl<$($ty: IntoArgPart,)* $last: IntoArgPart> IntoArgSpecs for $tuple<$($ty,)* $last> {
            fn into_arg_specs() -> Vec<ArgSpec> {
                let mut args = Vec::new();
                #[allow(unused_mut)]
                let mut position = 0;
                $(
                    args.push(ArgSpec {
                        name: format!("arg{}", position),
                        description: None,
                        position,
                        part: $ty::into_arg_part(),
                    });
                    position += 1;
                )*
                args.push(ArgSpec {
                    name: format!("arg{}", position),
                    description: None,
                    position,
                    part: $last::into_arg_part(),
                });
                args
            }

        }
    };
}

impl_argspec!([], T1, Tuple1);
impl_argspec!([T1], T2, Tuple2);
impl_argspec!([T1, T2], T3, Tuple3);
impl_argspec!([T1, T2, T3], T4, Tuple4);
impl_argspec!([T1, T2, T3, T4], T5, Tuple5);
impl_argspec!([T1, T2, T3, T4, T5], T6, Tuple6);

/// Middleware layer argument metadata.
#[derive(Debug, Clone, Serialize)]
pub struct LayerSpec {
    /// Argument name from function signature.
    pub name: String,

    /// Optional documentation string.
    pub description: Option<String>,

    /// Extraction specification.
    pub parts: Vec<ArgPart>,
}

impl LayerSpec {
    /// Creates argument spec with type, position, name, and documentation.
    pub fn from_type<T: IntoLayerParts>(name: &str, doc: &str) -> Self {
        Self {
            name: name.to_string(),
            description: Some(doc.to_string()),
            parts: T::into_layer_parts(),
        }
    }
}

/// Single handler argument metadata.
#[derive(Debug, Clone, Serialize)]
pub struct ArgSpec {
    /// Argument name from function signature.
    pub name: String,

    /// Optional documentation string.
    pub description: Option<String>,

    /// Position in function signature (0-based).
    pub position: usize,

    /// Extraction specification.
    pub part: ArgPart,
}

impl ArgSpec {
    /// Creates argument spec with type, position, name, and documentation.
    pub fn from_type<T: IntoArgPart>(position: usize, name: &str, doc: &str) -> Self {
        Self {
            name: name.to_string(),
            description: Some(doc.to_string()),
            position,
            part: T::into_arg_part(),
        }
    }
}

/// Handler return value metadata.
#[derive(Debug, Clone, Serialize)]
pub struct ReturnSpec {
    /// Optional documentation string.
    pub description: Option<String>,
    /// HTTP status code.
    pub status_code: Option<u16>,
    /// Response specification.
    pub part: ReturnPart,
    /// Response headers documented for this response.
    pub headers: Vec<ReturnHeaderSpec>,
    /// Example payloads for this response.
    pub examples: Vec<ReturnExample>,
    /// Stable component-name hint for generated schemas.
    pub schema_name: Option<Cow<'static, str>>,
}

/// Header metadata attached to a response.
#[derive(Debug, Clone, Serialize)]
pub struct ReturnHeaderSpec {
    /// Header name.
    pub name: Cow<'static, str>,
    /// Optional header description.
    pub description: Option<Cow<'static, str>>,
    /// Header value schema.
    pub schema: TypeSchema,
    /// Whether the header is always present for this response.
    pub required: bool,
}

/// Example payload metadata attached to a response.
#[derive(Debug, Clone, Serialize)]
pub struct ReturnExample {
    /// Example name.
    pub name: Cow<'static, str>,
    /// Optional example summary.
    pub summary: Option<Cow<'static, str>>,
    /// Example JSON value.
    pub value: serde_json::Value,
}

impl ReturnSpec {
    /// Creates return spec from a response part.
    pub fn new(part: ReturnPart) -> Self {
        Self {
            description: None,
            status_code: None,
            part,
            headers: Vec::new(),
            examples: Vec::new(),
            schema_name: None,
        }
    }

    /// Creates return spec with type, optional documentation, and status code.
    pub fn from_type<T: IntoReturnPart>(doc: Option<String>, status_code: Option<u16>) -> Self {
        Self {
            description: doc,
            status_code,
            part: T::into_return_part(),
            headers: Vec::new(),
            examples: Vec::new(),
            schema_name: None,
        }
    }

    /// Creates a JSON error response using Vyuh's public error body.
    pub fn error(status_code: u16, description: impl Into<String>) -> Self {
        Self {
            description: Some(description.into()),
            status_code: Some(status_code),
            part: ReturnPart::Body(
                TypeSchema::wrap::<crate::errors::ErrorReport>(),
                Cow::Borrowed("application/json"),
            ),
            headers: Vec::new(),
            examples: Vec::new(),
            schema_name: Some(Cow::Borrowed("ErrorReport")),
        }
    }

    /// Adds a response header to this spec.
    pub fn with_header(mut self, header: ReturnHeaderSpec) -> Self {
        self.headers.push(header);
        self
    }

    /// Adds a response example to this spec.
    pub fn with_example(mut self, example: ReturnExample) -> Self {
        self.examples.push(example);
        self
    }
}

/// Method receiver kind: `self`, `&self`, `&mut self`, etc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReceiverSpec {
    Ref,
    MutRef,
    Value,
    Box,
    Arc,
    Unknown(&'static str), // To be filled by macros for any other receiver types
}

/// Complete handler signature specification.
///
/// Includes arguments, returns, and metadata for API documentation.
#[derive(Debug, Default, Clone)]
pub struct CallSpec {
    pub description: Option<String>,
    pub name: String,
    pub is_method: bool,
    pub receiver: Option<ReceiverSpec>,
    pub args: Vec<ArgSpec>,
    pub returns: Vec<ReturnSpec>,
}

impl CallSpec {
    /// Extracts compile-time specification from handler.
    pub fn new<Args, H>(handler: &H) -> Self
    where
        H: Specable<Args>,
        Args: IntoArgSpecs,
    {
        let _ = handler; // Use the handler to infer types but don't actually need it
        Self {
            description: None,
            name: std::any::type_name::<H>().to_string(),
            is_method: false,
            receiver: None,
            args: Args::into_arg_specs(),
            returns: vec![ReturnSpec {
                description: None,
                status_code: None,
                part: H::Output::into_return_part(),
                headers: Vec::new(),
                examples: Vec::new(),
                schema_name: None,
            }],
        }
    }

    /// Returns number of handler arguments.
    pub fn arity(&self) -> usize {
        self.args.len()
    }

    /// Returns `TypeId` of body argument, if present.
    pub fn payload_type(&self) -> Option<TypeId> {
        self.args
            .iter()
            .rev()
            .find_map(|arg| arg.part.body_schema().map(TypeSchema::type_id))
    }

    /// Updates argument at position. Used by proc macros.
    #[allow(dead_code)]
    pub(crate) fn set_arg<T: IntoArgPart>(&mut self, position: usize, name: &str, doc: &str) {
        self.args.retain(|a| a.position != position);
        self.args.push(ArgSpec {
            name: name.to_string(),
            description: Some(doc.to_string()),
            position,
            part: T::into_arg_part(),
        });
        self.args.sort_by_key(|a| a.position);
    }

    /// Replaces all return specs. Used by proc macros.
    #[allow(dead_code)]
    pub(crate) fn set_returns(&mut self, output: Vec<ReturnSpec>) {
        self.returns = output;
    }

    /// Appends additional return spec. Used by proc macros.
    #[allow(dead_code)]
    pub(crate) fn append_return(&mut self, ret: ReturnSpec) {
        self.returns.push(ret);
    }
}

macro_rules! impl_handler {
    (
        [$($ty:ident),*], $last:ident, $tuple:ident
    ) => {
        #[allow(non_snake_case, unused_mut, unused_variables)]
        impl<F, Fut, R, $($ty,)* $last> Specable<$tuple<$($ty,)* $last>> for F
        where
            F: Fn($($ty,)* $last) -> Fut + Send + Sync,
            Fut: Future<Output = R> + Send,
            R: IntoReturnPart,
            $($ty: IntoArgPart,)*
            $last: IntoArgPart,
        {
            type Output = R;
            type Future = Fut;

            fn call(&self, $tuple($($ty,)* $last): $tuple<$($ty,)* $last>) -> Self::Future {
                (self)($($ty,)* $last)
            }
        }
    };
}

impl<F, Fut, R: IntoReturnPart> Specable<()> for F
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = R> + Send,
{
    type Output = R;
    type Future = Fut;

    fn call(&self, _args: ()) -> Self::Future {
        (self)()
    }
}

impl_handler!([], T1, Tuple1);
impl_handler!([T1], T2, Tuple2);
impl_handler!([T1, T2], T3, Tuple3);
impl_handler!([T1, T2, T3], T4, Tuple4);
impl_handler!([T1, T2, T3, T4], T5, Tuple5);
impl_handler!([T1, T2, T3, T4, T5], T6, Tuple6);

// HasData implementations for custom tuples containing Data<T>
impl<T: crate::callables::DataValue> HasData<T> for Tuple1<crate::callables::Data<T>> {}
impl<T: crate::callables::DataValue, A1> HasData<T> for Tuple2<A1, crate::callables::Data<T>> {}
impl<T: crate::callables::DataValue, A1, A2> HasData<T>
    for Tuple3<A1, A2, crate::callables::Data<T>>
{
}
impl<T: crate::callables::DataValue, A1, A2, A3> HasData<T>
    for Tuple4<A1, A2, A3, crate::callables::Data<T>>
{
}
impl<T: crate::callables::DataValue, A1, A2, A3, A4> HasData<T>
    for Tuple5<A1, A2, A3, A4, crate::callables::Data<T>>
{
}
impl<T: crate::callables::DataValue, A1, A2, A3, A4, A5> HasData<T>
    for Tuple6<A1, A2, A3, A4, A5, crate::callables::Data<T>>
{
}

// Valid<Data<T>> is the validated form of handler data and still counts as
// the handler's data payload.
impl<T: crate::callables::DataValue + crate::validation::Validate> HasData<T>
    for Tuple1<crate::validation::Valid<crate::callables::Data<T>>>
{
}
impl<T: crate::callables::DataValue + crate::validation::Validate, A1> HasData<T>
    for Tuple2<A1, crate::validation::Valid<crate::callables::Data<T>>>
{
}
impl<T: crate::callables::DataValue + crate::validation::Validate, A1, A2> HasData<T>
    for Tuple3<A1, A2, crate::validation::Valid<crate::callables::Data<T>>>
{
}
impl<T: crate::callables::DataValue + crate::validation::Validate, A1, A2, A3> HasData<T>
    for Tuple4<A1, A2, A3, crate::validation::Valid<crate::callables::Data<T>>>
{
}
impl<T: crate::callables::DataValue + crate::validation::Validate, A1, A2, A3, A4> HasData<T>
    for Tuple5<A1, A2, A3, A4, crate::validation::Valid<crate::callables::Data<T>>>
{
}
impl<T: crate::callables::DataValue + crate::validation::Validate, A1, A2, A3, A4, A5> HasData<T>
    for Tuple6<A1, A2, A3, A4, A5, crate::validation::Valid<crate::callables::Data<T>>>
{
}
