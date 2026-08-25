use std::any::TypeId;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::specs::{
    CallError, CallSpec, IntoArgSpecs, IntoReturnPart, Specable, Tuple1, Tuple2, Tuple3, Tuple4,
    Tuple5, Tuple6,
};

type HandlerFuture<E> = Pin<Box<dyn Future<Output = Result<DataBox, E>> + Send>>;
type HandlerFn<C, E> = dyn Fn(C) -> HandlerFuture<E> + Send + Sync;

/// Deserializes a handler's textual payload into its erased runtime value.
pub type DataDeserializer = fn(&str) -> Result<DataBox, CallError>;

/// Extracts typed arguments from context, excluding data extractors.
///
/// Types implementing this can appear at any position in a handler signature.
/// Data extractors implement only `FromContext`.
pub trait FromContextParts<C>: Sized + Send {
    fn from_context_parts(ctx: &C) -> Result<Self, CallError>;
}

/// Extracts typed arguments from context for the last argument position.
///
/// Data extractors (like `Data<T>`) implement only this trait.
/// Other extractors implement both this and `FromContextParts`.
///
/// This enforces data-must-be-last: handlers can only extract data
/// as the final argument.
pub trait FromContext<C>: Sized + Send {
    fn from_context(ctx: C) -> Result<Self, CallError>;

    fn deserializer() -> Option<DataDeserializer> {
        None
    }
}

/// Provides type-erased data from context.
/// Implement for context types that carry handler data, such as `SignalContext`
/// and `TaskContext`.
pub trait IntoDataBox {
    fn into_data_box(self) -> DataBox;
}

/// Converts handler output into `Result<DataBox, E>`.
///
/// Supported return types: `Data<T>`, `()`, `Result<Data<T>, E>`, `Result<(), E>`.
/// For truly infallible handlers, use `std::convert::Infallible` as error type.
pub trait IntoOutput<E> {
    fn into_output(self) -> Result<DataBox, E>;
}

impl<T, E> IntoOutput<E> for Result<T, E>
where
    T: IntoOutput<E>,
{
    fn into_output(self) -> Result<DataBox, E> {
        self.and_then(T::into_output)
    }
}

impl<E> IntoOutput<E> for () {
    fn into_output(self) -> Result<DataBox, E> {
        Ok(DataBox::new(()))
    }
}

// Unit type represents handlers with no arguments.
impl<C> FromContextParts<C> for () {
    fn from_context_parts(_ctx: &C) -> Result<Self, CallError> {
        Ok(())
    }
}

impl<C> FromContext<C> for () {
    fn from_context(_ctx: C) -> Result<Self, CallError> {
        Ok(())
    }
}

macro_rules! impl_context {
    ([$($ty:ident),*], $last:ident, $tuple:ident) => {
        #[allow(non_snake_case, unused_mut, unused_variables)]
        impl<C, $($ty,)* $last> FromContextParts<C> for $tuple<$($ty,)* $last>
        where
            $($ty: FromContextParts<C>,)*
            $last: FromContextParts<C>,
        {
            fn from_context_parts(ctx: &C) -> Result<Self, CallError> {
                $(let $ty = $ty::from_context_parts(ctx)?;)*
                let $last = $last::from_context_parts(ctx)?;
                Ok($tuple($($ty,)* $last))
            }
        }

        #[allow(non_snake_case, unused_mut, unused_variables)]
        impl<C, $($ty,)* $last> FromContext<C> for $tuple<$($ty,)* $last>
        where
            $($ty: FromContextParts<C>,)*
            $last: FromContext<C>,
        {
            fn from_context(ctx: C) -> Result<Self, CallError> {
                $(let $ty = $ty::from_context_parts(&ctx)?;)*
                let $last = $last::from_context(ctx)?;
                Ok($tuple($($ty,)* $last))
            }

            fn deserializer() -> Option<fn(&str) -> Result<DataBox, CallError>> {
                $last::deserializer()
            }
        }
    };
}

impl_context!([], T1, Tuple1);
impl_context!([T1], T2, Tuple2);
impl_context!([T1, T2], T3, Tuple3);
impl_context!([T1, T2, T3], T4, Tuple4);
impl_context!([T1, T2, T3, T4], T5, Tuple5);
impl_context!([T1, T2, T3, T4, T5], T6, Tuple6);

type JsonSerializer = fn(&dyn std::any::Any) -> Result<serde_json::Value, String>;
type SchemaName = fn() -> String;

/// Type-erased data container wrapping `Arc<dyn Any>`.
///
/// `DataBox` preserves downcasting for callable dispatch. When constructed from
/// `Data<T>`, it also preserves JSON serialization and schema-name metadata so
/// erased signal payloads can still feed channel delivery.
#[derive(Clone)]
pub struct DataBox {
    type_id: TypeId,
    inner: Arc<dyn std::any::Any + Send + Sync>,
    json_serializer: Option<JsonSerializer>,
    schema_name: Option<SchemaName>,
}

impl std::fmt::Debug for DataBox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataBox")
            .field("type_id", &self.type_id)
            .field("serializable", &self.json_serializer.is_some())
            .finish()
    }
}

impl DataBox {
    /// Wraps existing `Arc<T>` in type-erased container.
    pub fn from_arc<T: Send + Sync + 'static>(inner: Arc<T>) -> Self {
        Self {
            inner,
            type_id: TypeId::of::<T>(),
            json_serializer: None,
            schema_name: None,
        }
    }

    /// Wraps existing `Arc<T>` while preserving `Data<T>` wire metadata.
    pub fn from_data_arc<T: super::specs::DataValue>(inner: Arc<T>) -> Self {
        Self {
            inner,
            type_id: TypeId::of::<T>(),
            json_serializer: Some(serialize_json::<T>),
            schema_name: Some(schema_name::<T>),
        }
    }

    /// Wraps a value in a type-erased container without wire metadata.
    pub fn new<T: Send + Sync + 'static>(value: T) -> Self {
        Self {
            inner: Arc::new(value),
            type_id: TypeId::of::<T>(),
            json_serializer: None,
            schema_name: None,
        }
    }

    /// Wraps a `DataValue` while preserving JSON and schema metadata.
    pub fn new_data<T: super::specs::DataValue>(value: T) -> Self {
        Self::from_data_arc(Arc::new(value))
    }

    pub(crate) fn into_any_arc(self) -> Arc<dyn std::any::Any + Send + Sync> {
        self.inner
    }

    /// Returns the runtime `TypeId` of the wrapped value.
    #[inline]
    pub fn payload_type_id(&self) -> TypeId {
        self.type_id
    }

    /// Returns the erased payload for predicate checks and downcasting.
    pub fn as_any(&self) -> &dyn std::any::Any {
        self.inner.as_ref()
    }

    /// Serializes the payload when this box was created from `Data<T>`.
    ///
    /// Returns `None` for boxes without preserved wire metadata.
    pub fn to_json(&self) -> Option<Result<serde_json::Value, String>> {
        self.json_serializer
            .map(|serializer| serializer(self.inner.as_ref()))
    }

    /// Returns the schema name when this box was created from `Data<T>`.
    pub fn schema_name(&self) -> Option<String> {
        self.schema_name.map(|schema_name| schema_name())
    }

    /// Downcasts to `&T`, returning `None` on type mismatch.
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.inner.downcast_ref::<T>()
    }

    /// Downcasts into the original `Arc<T>`, returning `None` on type mismatch.
    pub fn downcast_arc<T: 'static + Send + Sync>(self) -> Option<Arc<T>> {
        self.inner.downcast::<T>().ok()
    }
}

fn serialize_json<T: serde::Serialize + 'static>(
    value: &dyn std::any::Any,
) -> Result<serde_json::Value, String> {
    let Some(value) = value.downcast_ref::<T>() else {
        return Err("data type mismatch".to_string());
    };
    serde_json::to_value(value).map_err(|err| err.to_string())
}

fn schema_name<T: schemars::JsonSchema>() -> String {
    <T as schemars::JsonSchema>::schema_name().into_owned()
}

/// Type-erased async handler storing context and error types only.
///
/// Stores handlers with different data/output types uniformly via `DataBox`.
/// Generic over context `C` and error `E`; input/output types erased at runtime.
///
/// Data-must-be-last constraint enforced via `FromContext`/`FromContextParts` split.
pub struct Callable<C, E = CallError>
where
    C: Send + 'static,
    E: Send + 'static,
{
    spec: Arc<CallSpec>,
    pub(crate) type_id: TypeId,
    deserializer: Option<DataDeserializer>,
    // serializer: Option<fn(DataBox) -> Result<String, CallError>>,
    inner: Arc<HandlerFn<C, E>>,
}

impl<C, E> Clone for Callable<C, E>
where
    C: Send + 'static,
    C: Send + 'static,
    E: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            spec: Arc::clone(&self.spec),
            type_id: self.type_id,
            deserializer: self.deserializer,
            // serializer: self.serializer,
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<C, E> Callable<C, E>
where
    C: Send + 'static,
    E: Send + 'static,
{
    /// Returns handler metadata for introspection and API documentation.
    #[inline]
    pub fn inspect(&self) -> &CallSpec {
        &self.spec
    }

    /// Deserializes JSON string into type-erased data.
    /// Returns error if no deserializer registered or parsing fails.
    #[inline]
    pub fn deserialize(&self, data: &str) -> Result<DataBox, CallError> {
        if let Some(deser) = self.deserializer {
            deser(data)
        } else {
            Err(CallError::TypeMismatch)
        }
    }
}

impl<C, E> Callable<C, E>
where
    C: Send + 'static,
    E: From<CallError> + Send + 'static,
{
    /// Wraps typed handler into type-erased `Callable`.
    ///
    /// Captures spec, optional JSON deserializer, and type-erases input/output.
    /// Data-must-be-last enforced via `FromContext` bound on `Args`.
    pub fn new<H, O, Args>(handler: H) -> Self
    where
        O: IntoOutput<E> + IntoReturnPart + Send + 'static,
        H: Specable<Args, Output = O> + Send + Sync + 'static,
        Args: FromContext<C> + IntoArgSpecs,
    {
        let spec = Arc::new(CallSpec::new(&handler));
        let type_id = spec.payload_type().unwrap_or(TypeId::of::<()>());
        let handler = Arc::new(handler);
        let inner = Arc::new(
            move |ctx: C| -> Pin<Box<dyn Future<Output = Result<DataBox, E>> + Send>> {
                let handler = Arc::clone(&handler);
                Box::pin(async move {
                    let args = Args::from_context(ctx).map_err(E::from)?;
                    let result = handler.call(args).await;
                    result.into_output()
                })
            },
        );

        // let serializer = |p: DataBox|{
        //     let value = p.downcast_ref::<O>().ok_or(CallError::SerializeFailed)?;
        //     serde_json::to_string(value).map_err(|_| CallError::SerializeFailed)
        // };

        let deserializer = Args::deserializer();

        Callable {
            spec,
            type_id,
            deserializer,
            // serializer: Some(serializer),
            inner,
        }
    }

    /// Invokes handler with context, returning type-erased output.
    #[inline]
    pub fn call(&self, ctx: C) -> impl Future<Output = Result<DataBox, E>> + Send + '_ {
        (self.inner)(ctx)
    }

    pub fn deserialize_input(&self, data: &str) -> Result<DataBox, CallError> {
        if let Some(deser) = self.deserializer {
            deser(data)
        } else {
            Err(CallError::DeserializeFailed)
        }
    }

    // pub fn serialize_output<T: serde::Serialize>(&self, data: DataBox) -> Result<String, CallError> {
    //     if let Some(ser) = self.serializer {
    //         ser(data)
    //     } else {
    //         Err(CallError::SerializeFailed)
    //     }

    // }
}
