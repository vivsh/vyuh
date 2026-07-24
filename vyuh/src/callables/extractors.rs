//! Extractors for extracting data from context in callable handlers.

use super::callables::{DataBox, FromContext, FromContextParts, IntoDataBox, IntoOutput};
use super::specs::{
    ArgPart, CallError, DataValue, IntoArgPart, IntoReturnPart, ReturnPart, ReturnSpec, TypeSchema,
};
use crate::routes::{
    Accepted, BodyBytes, CookieJson, Created, FileResponse, Form, Json, JsonStr, OkJson, OkOut,
    Path, PermanentRedirect, Query, StreamResponse, TemporaryRedirect,
};
use crate::validation::{Valid, Validate, ValidationSchema};
use crate::{
    Site,
    auth::{ApiKey, AuthUser},
    site,
};
use schemars::JsonSchema;
use std::borrow::Cow;
use std::sync::Arc;

fn bad_request_response() -> ArgPart {
    ArgPart::Response(vec![ReturnSpec::error(400, "Bad request.")])
}

fn unauthorized_response() -> ArgPart {
    ArgPart::Response(vec![ReturnSpec::error(401, "Unauthorized.")])
}

fn validation_response() -> ArgPart {
    ArgPart::Response(vec![ReturnSpec::error(422, "Validation failed.")])
}

fn request_part_with_bad_request(part: ArgPart) -> ArgPart {
    ArgPart::Composite(vec![part, bad_request_response()])
}

fn validated_request_part(part: ArgPart) -> ArgPart {
    ArgPart::Composite(vec![part, bad_request_response(), validation_response()])
}

/// Trait for context types that provide access to a `Site` instance.
pub trait HasSite {
    fn site(&self) -> &Site;
}

/// Trait for types that can be extracted from a `Site`.
pub trait FromSite: Sized + Send {
    fn from_site(site: &Site) -> Result<Self, CallError>;
}

/// Generic implementation: any `T: FromSite` can be extracted from any `C: HasSite`.
impl<C: HasSite, T: FromSite> FromContextParts<C> for T {
    fn from_context_parts(ctx: &C) -> Result<Self, CallError> {
        T::from_site(ctx.site())
    }
}

/// Uniform typed application data flowing through Vyuh handlers.
///
/// `Data<T>` uses `Arc<T>` internally so the same value can be cheaply shared
/// across signal fanout, commands, emitters, tasks, and route handlers.
#[derive(Debug)]
pub struct Data<T: DataValue>(pub Arc<T>);

impl<T: DataValue> Clone for Data<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T: DataValue> Data<T> {
    pub fn new(inner: T) -> Data<T> {
        Data(Arc::new(inner))
    }

    pub fn from_arc(inner: Arc<T>) -> Data<T> {
        Data(inner)
    }

    pub fn into_inner(self) -> Arc<T> {
        self.0
    }
}

impl<T: DataValue> std::ops::Deref for Data<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: DataValue> AsRef<T> for Data<T> {
    fn as_ref(&self) -> &T {
        &self.0
    }
}

impl<T: DataValue> From<Arc<T>> for Data<T> {
    fn from(value: Arc<T>) -> Self {
        Data::from_arc(value)
    }
}

impl<T: DataValue> From<T> for Data<T> {
    fn from(value: T) -> Self {
        Data::new(value)
    }
}

/// `Data<T>` implements `FromContext` directly (not `FromContextParts`).
/// This ensures it can only appear as the last argument in a handler signature,
/// since `impl_context!` requires all but the last arg to impl `FromContextParts`.
impl<C, T> FromContext<C> for Data<T>
where
    C: IntoDataBox + Send + 'static,
    T: DataValue,
{
    fn from_context(ctx: C) -> Result<Self, CallError> {
        // Extract type-erased data from context and downcast to T
        let payload_data = ctx.into_data_box();
        let value = payload_data
            .downcast_arc::<T>()
            .ok_or_else(|| CallError::TypeMismatch)?;
        Ok(Data::from(value))
    }

    /// Returns a deserializer function for JSON data.
    /// This is used by task queues to deserialize string data into typed values.
    fn deserializer() -> Option<fn(&str) -> Result<DataBox, CallError>> {
        Some(|s: &str| {
            let value: T = serde_json::from_str(s).map_err(|_| CallError::DeserializeFailed)?;
            Ok(DataBox::new_data(value))
        })
    }
}

impl<T: DataValue> IntoArgPart for Data<T> {
    fn into_arg_part() -> ArgPart {
        request_part_with_bad_request(ArgPart::Body(
            TypeSchema::wrap_unvalidated::<T>(),
            "application/json".into(),
        ))
    }
}

impl<T: DataValue> IntoReturnPart for Data<T> {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Body(TypeSchema::wrap::<T>(), "application/json".into())
    }
}

impl<C, E, T> FromContext<C> for Valid<E>
where
    E: FromContext<C> + std::ops::Deref<Target = T>,
    T: Validate,
{
    fn from_context(ctx: C) -> Result<Self, CallError> {
        let extracted = E::from_context(ctx)?;
        extracted
            .validate()
            .map(|()| Valid(extracted))
            .map_err(CallError::Validation)
    }

    fn deserializer() -> Option<fn(&str) -> Result<DataBox, CallError>> {
        E::deserializer()
    }
}

impl<T> super::specs::HasData<T> for Valid<Data<T>> where T: DataValue {}

/// `Data<T>` can be returned from handlers as an infallible output.
impl<T: DataValue, E> IntoOutput<E> for Data<T> {
    fn into_output(self) -> Result<DataBox, E> {
        Ok(DataBox::from_data_arc(self.0))
    }
}

impl<T> axum::extract::FromRequest<crate::Site> for Data<T>
where
    T: DataValue,
{
    type Rejection = crate::errors::ErrorReport;

    async fn from_request(
        req: axum::extract::Request,
        state: &crate::Site,
    ) -> Result<Self, Self::Rejection> {
        Json::<T>::from_request(req, state)
            .await
            .map(|Json(value)| Data::new(value))
    }
}

impl<T> axum::response::IntoResponse for Data<T>
where
    T: DataValue,
{
    fn into_response(self) -> axum::response::Response {
        axum::Json(self.0).into_response()
    }
}

// Common axum extractor implementations

impl<T: JsonSchema + Send + 'static> IntoArgPart for Path<T> {
    fn into_arg_part() -> ArgPart {
        request_part_with_bad_request(ArgPart::Path(TypeSchema::wrap_unvalidated::<T>()))
    }
}

impl<T: JsonSchema + Send + 'static> IntoArgPart for Query<T> {
    fn into_arg_part() -> ArgPart {
        request_part_with_bad_request(ArgPart::Query(TypeSchema::wrap_unvalidated::<T>()))
    }
}

impl<T: JsonSchema + Send + 'static> IntoArgPart for Form<T> {
    fn into_arg_part() -> ArgPart {
        request_part_with_bad_request(ArgPart::Body(
            TypeSchema::wrap_unvalidated::<T>(),
            Cow::Borrowed("application/x-www-form-urlencoded"),
        ))
    }
}

impl<T: JsonSchema + Send + 'static> IntoArgPart for Json<T> {
    fn into_arg_part() -> ArgPart {
        request_part_with_bad_request(ArgPart::Body(
            TypeSchema::wrap_unvalidated::<T>(),
            Cow::Borrowed("application/json"),
        ))
    }
}

impl IntoArgPart for BodyBytes {
    fn into_arg_part() -> ArgPart {
        request_part_with_bad_request(ArgPart::Body(
            TypeSchema::binary_body(),
            Cow::Borrowed("application/octet-stream"),
        ))
    }
}

impl<T> IntoArgPart for Valid<Path<T>>
where
    T: JsonSchema + Validate + ValidationSchema + Send + 'static,
{
    fn into_arg_part() -> ArgPart {
        validated_request_part(ArgPart::Path(TypeSchema::wrap_valid::<T>()))
    }
}

impl<T> IntoArgPart for Valid<Query<T>>
where
    T: JsonSchema + Validate + ValidationSchema + Send + 'static,
{
    fn into_arg_part() -> ArgPart {
        validated_request_part(ArgPart::Query(TypeSchema::wrap_valid::<T>()))
    }
}

impl<T> IntoArgPart for Valid<Form<T>>
where
    T: JsonSchema + Validate + ValidationSchema + Send + 'static,
{
    fn into_arg_part() -> ArgPart {
        validated_request_part(ArgPart::Body(
            TypeSchema::wrap_valid::<T>(),
            Cow::Borrowed("application/x-www-form-urlencoded"),
        ))
    }
}

impl<T> IntoArgPart for Valid<Json<T>>
where
    T: JsonSchema + Validate + ValidationSchema + Send + 'static,
{
    fn into_arg_part() -> ArgPart {
        validated_request_part(ArgPart::Body(
            TypeSchema::wrap_valid::<T>(),
            Cow::Borrowed("application/json"),
        ))
    }
}

impl<T> IntoArgPart for Valid<Data<T>>
where
    T: DataValue + Validate + ValidationSchema,
{
    fn into_arg_part() -> ArgPart {
        validated_request_part(ArgPart::Body(
            TypeSchema::wrap_valid::<T>(),
            Cow::Borrowed("application/json"),
        ))
    }
}

impl<T: JsonSchema + Send + 'static> IntoArgPart for axum::extract::Path<T> {
    fn into_arg_part() -> ArgPart {
        request_part_with_bad_request(ArgPart::Path(TypeSchema::wrap::<T>()))
    }
}

impl<T: JsonSchema + Send + 'static> IntoArgPart for axum::extract::Query<T> {
    fn into_arg_part() -> ArgPart {
        request_part_with_bad_request(ArgPart::Query(TypeSchema::wrap::<T>()))
    }
}

impl<T: JsonSchema + Send + 'static> IntoArgPart for axum_extra::extract::Query<T> {
    fn into_arg_part() -> ArgPart {
        request_part_with_bad_request(ArgPart::Query(TypeSchema::wrap::<T>()))
    }
}

impl<T: JsonSchema + Send + 'static> IntoArgPart for axum::extract::Json<T> {
    fn into_arg_part() -> ArgPart {
        request_part_with_bad_request(ArgPart::Body(
            TypeSchema::wrap::<T>(),
            Cow::Borrowed("application/json"),
        ))
    }
}

impl<T: Clone + Send + Sync + 'static> IntoArgPart for axum::extract::State<T> {
    fn into_arg_part() -> ArgPart {
        ArgPart::Zone // State is not documented in OpenAPI
    }
}

impl IntoReturnPart for axum::response::Response {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Unknown
    }
}

impl<T: JsonSchema + Send + 'static> IntoReturnPart for axum::extract::Json<T> {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Body(TypeSchema::wrap::<T>(), Cow::Borrowed("application/json"))
    }
}

impl<T: JsonSchema + Send + 'static> IntoReturnPart for Json<T> {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Body(TypeSchema::wrap::<T>(), Cow::Borrowed("application/json"))
    }
}

impl<T: JsonSchema + Send + 'static> IntoReturnPart for CookieJson<T> {
    fn into_return_part() -> ReturnPart {
        <Json<T> as IntoReturnPart>::into_return_part()
    }
}

impl IntoReturnPart for OkJson {
    fn into_return_part() -> ReturnPart {
        <Json<OkOut> as IntoReturnPart>::into_return_part()
    }
}

impl<T: JsonSchema + Send + 'static> IntoReturnPart for Created<T> {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Created(TypeSchema::wrap::<T>(), Cow::Borrowed("application/json"))
    }
}

impl<T: JsonSchema + Send + 'static> IntoReturnPart for Accepted<T> {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Accepted(TypeSchema::wrap::<T>(), Cow::Borrowed("application/json"))
    }
}

impl IntoReturnPart for axum::response::NoContent {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Empty
    }
}

impl IntoReturnPart for TemporaryRedirect {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Redirect { status_code: 307 }
    }
}

impl IntoReturnPart for PermanentRedirect {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Redirect { status_code: 308 }
    }
}

impl IntoReturnPart for FileResponse {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Binary(Cow::Borrowed("application/octet-stream"))
    }
}

impl IntoReturnPart for StreamResponse {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Binary(Cow::Borrowed("application/octet-stream"))
    }
}

impl IntoReturnPart for axum::http::StatusCode {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Empty
    }
}

impl<T: IntoReturnPart> IntoReturnPart for (axum::http::StatusCode, T) {
    fn into_return_part() -> ReturnPart {
        T::into_return_part()
    }
}

impl<T: IntoReturnPart> IntoReturnPart for (axum_extra::extract::CookieJar, T) {
    fn into_return_part() -> ReturnPart {
        T::into_return_part()
    }
}

impl IntoReturnPart for axum::response::Html<String> {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Body(TypeSchema::wrap::<String>(), "text/html".into())
    }
}

impl IntoReturnPart for () {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Empty
    }
}

impl<T, E> IntoReturnPart for Result<T, E>
where
    T: IntoReturnPart,
    E: Send,
{
    fn into_return_part() -> ReturnPart {
        T::into_return_part()
    }
}

impl<T> IntoReturnPart for Option<T>
where
    T: IntoReturnPart,
{
    fn into_return_part() -> ReturnPart {
        T::into_return_part()
    }
}

impl<'a> IntoReturnPart for JsonStr {
    fn into_return_part() -> ReturnPart {
        ReturnPart::Body(TypeSchema::wrap::<String>(), "application/json".into())
    }
}

impl IntoArgPart for AuthUser {
    fn into_arg_part() -> ArgPart {
        ArgPart::Composite(vec![
            ArgPart::Security {
                scheme: "bearerAuth".into(),
                join_all: true,
                scopes: Vec::new(),
            },
            unauthorized_response(),
        ])
    }
}

impl IntoArgPart for ApiKey {
    fn into_arg_part() -> ArgPart {
        ArgPart::Composite(vec![
            ArgPart::Security {
                scheme: "apiKeyAuth".into(),
                join_all: true,
                scopes: Vec::new(),
            },
            unauthorized_response(),
        ])
    }
}

impl<T> IntoArgPart for Option<T>
where
    T: IntoArgPart,
{
    fn into_arg_part() -> ArgPart {
        ArgPart::Optional(Box::new(T::into_arg_part()))
    }
}

impl<T, E> IntoArgPart for Result<T, E>
where
    T: IntoArgPart,
{
    fn into_arg_part() -> ArgPart {
        ArgPart::Fallible(Box::new(T::into_arg_part()))
    }
}

impl IntoArgPart for site::Site {
    fn into_arg_part() -> ArgPart {
        ArgPart::Ignore
    }
}

impl FromSite for site::Site {
    fn from_site(site: &site::Site) -> Result<Self, CallError> {
        Ok(site.clone())
    }
}

impl<C: HasSite> FromContext<C> for site::Site {
    fn from_context(ctx: C) -> Result<Self, CallError> {
        Ok(ctx.site().clone())
    }
}
