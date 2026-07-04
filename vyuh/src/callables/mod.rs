mod callables;
mod extractors;
mod operations;
mod patch;
pub(crate) mod specs;

#[cfg(test)]
mod tests;

// Re-export all public types and traits
pub use specs::{
    ArgPart,
    // Spec types
    ArgSpec,
    // Error type
    CallError,

    // Main types
    CallSpec,
    // Core traits
    DataValue,
    HasData,

    IntoArgPart,
    IntoArgSpecs,
    IntoHandlerSpec,
    IntoLayerParts,
    IntoReturnPart,
    LayerSpec,
    MultipartApiField,
    MultipartApiFieldKind,
    MultipartApiSpec,
    ReceiverSpec,
    ReturnPart,
    ReturnSpec,
    Specable,
    TypeSchema,
};

pub use callables::{
    // Runtime types
    Callable,
    DataBox,
    // Extraction traits
    FromContext,
    FromContextParts,
    IntoDataBox,
    IntoOutput,
};

pub use patch::{ArgPatch, PatchOp, ReturnPatch};

pub use extractors::{Data, FromSite, HasSite};

pub use operations::{Operation, OperationKind};
