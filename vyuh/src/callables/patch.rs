use super::{ArgPart, IntoArgPart, IntoReturnPart, Operation, ReturnPart, ReturnSpec};
use std::borrow::Cow;

/// Specification patch for modifying Operation metadata.
/// Can be constructed fluently and applied to an Operation.
/// Used by proc macros and user code to override auto-generated specs.
#[derive(Debug, Default, Clone)]
pub struct PatchOp {
    name: Option<String>,
    description: Option<String>,
    operation_id: Option<String>,
    deprecated: Option<bool>,
    arg_patches: Vec<ArgPatchData>,
    return_patch: Option<ReturnPatchData>,
    append_returns: Vec<ReturnPatchData>,
    tags: Option<Vec<Cow<'static, str>>>,
}

#[derive(Debug, Clone)]
struct ArgPatchData {
    position: usize,
    name: Option<String>,
    description: Option<String>,
    part: Option<ArgPart>,
}

impl ArgPatchData {
    fn apply_to_op(self, op: &mut Operation) {
        if let Some(arg) = op.args.get_mut(self.position) {
            if let Some(name) = self.name {
                arg.name = name;
            }
            if let Some(description) = self.description {
                arg.description = Some(description);
            }
            if let Some(part) = self.part {
                arg.part = part;
            }
        }
    }
}

#[derive(Debug, Clone)]
struct ReturnPatchData {
    description: Option<String>,
    status_code: Option<u16>,
    part: Option<ReturnPart>,
}

impl ReturnPatchData {
    fn apply(self, ret: &mut ReturnSpec) {
        if let Some(description) = self.description {
            ret.description = Some(description);
        }
        if let Some(status_code) = self.status_code {
            ret.status_code = Some(status_code);
        }
        if let Some(part) = self.part {
            ret.part = part;
        }
    }

    fn to_return_spec(&self) -> ReturnSpec {
        ReturnSpec {
            description: self.description.clone(),
            status_code: self.status_code,
            part: self.part.clone().unwrap_or_else(|| {
                ReturnPart::Body(super::TypeSchema::wrap::<()>(), "application/json".into())
            }),
            headers: Vec::new(),
            examples: Vec::new(),
            schema_name: None,
        }
    }
}

impl PatchOp {
    /// Creates a new empty patch specification.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the handler name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the handler description (documentation).
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Sets a stable OpenAPI operation id.
    pub fn operation_id(mut self, operation_id: impl Into<String>) -> Self {
        self.operation_id = Some(operation_id.into());
        self
    }

    /// Marks the operation as deprecated or active.
    pub fn deprecated(mut self, deprecated: bool) -> Self {
        self.deprecated = Some(deprecated);
        self
    }

    /// Sets the operation tags.
    pub fn tags(mut self, tags: Vec<Cow<'static, str>>) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Appends a tag to the operation.
    pub fn tag(mut self, tag: impl Into<Cow<'static, str>>) -> Self {
        let tag_value = tag.into();
        if let Some(ref mut tags) = self.tags {
            tags.push(tag_value);
        } else {
            self.tags = Some(vec![tag_value]);
        }
        self
    }

    /// Begins patching an argument at the given position.
    /// Returns ArgPatch for chaining argument modifications.
    pub fn arg(self, position: usize) -> ArgPatch {
        ArgPatch {
            spec: self,
            position,
            name: None,
            description: None,
            part: None,
        }
    }

    /// Modifies the last return specification in the list.
    /// Returns ReturnPatch for chaining return modifications.
    pub fn ret(self) -> ReturnPatch {
        ReturnPatch {
            spec: self,
            is_append: false,
            description: None,
            status_code: None,
            part: None,
        }
    }

    /// Appends a new return specification (e.g., for error responses).
    /// Returns ReturnPatch for chaining return modifications.
    pub fn append(self) -> ReturnPatch {
        ReturnPatch {
            spec: self,
            is_append: true,
            description: None,
            status_code: None,
            part: None,
        }
    }

    /// Applies this patch to an Operation, modifying it in place.
    /// Consumes self to avoid unnecessary clones.
    pub fn apply(self, op: &mut Operation) {
        if let Some(name) = self.name {
            op.name = name;
        }
        if let Some(description) = self.description {
            op.description = Some(description);
        }
        if let Some(operation_id) = self.operation_id {
            op.openapi_id = Some(operation_id);
        }
        if let Some(deprecated) = self.deprecated {
            op.deprecated = deprecated;
        }
        if let Some(tags) = self.tags {
            op.tags = tags;
        }
        for arg_patch in self.arg_patches {
            arg_patch.apply_to_op(op);
        }
        // Modify last return if specified
        if let Some(return_patch) = self.return_patch {
            if let Some(last_ret) = op.returns.last_mut() {
                return_patch.apply(last_ret);
            }
        }
        // Append new returns
        for append_patch in self.append_returns {
            op.returns.push(append_patch.to_return_spec());
        }
    }
}

/// Chainable builder for patching individual argument metadata.
/// Created by PatchSpec::arg(), chains back to PatchSpec methods.
pub struct ArgPatch {
    spec: PatchOp,
    position: usize,
    name: Option<String>,
    description: Option<String>,
    part: Option<ArgPart>,
}

impl ArgPatch {
    /// Sets the argument name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the argument documentation.
    pub fn doc(mut self, doc: impl Into<String>) -> Self {
        self.description = Some(doc.into());
        self
    }

    /// Sets the argument type using a type that implements IntoArgPart.
    /// Allows overriding auto-detected types.
    pub fn typed<T: IntoArgPart>(mut self) -> Self {
        self.part = Some(T::into_arg_part());
        self
    }

    /// Finalizes current arg patch and starts patching another argument.
    pub fn arg(self, position: usize) -> ArgPatch {
        let arg_patch_data = ArgPatchData {
            position: self.position,
            name: self.name,
            description: self.description,
            part: self.part,
        };
        let mut spec = self.spec;
        spec.arg_patches.push(arg_patch_data);

        ArgPatch {
            spec,
            position,
            name: None,
            description: None,
            part: None,
        }
    }

    /// Finalizes current arg patch and continues with PatchSpec methods.
    fn finalize(self) -> PatchOp {
        let arg_patch_data = ArgPatchData {
            position: self.position,
            name: self.name,
            description: self.description,
            part: self.part,
        };
        let mut spec = self.spec;
        spec.arg_patches.push(arg_patch_data);
        spec
    }

    /// Finalizes argument patch and returns to PatchSpec for further chaining.
    pub fn done(self) -> PatchOp {
        self.finalize()
    }

    /// Finalizes arg patch and modifies the last return.
    pub fn ret(self) -> ReturnPatch {
        self.finalize().ret()
    }

    /// Finalizes arg patch and appends a new return.
    pub fn append(self) -> ReturnPatch {
        self.finalize().append()
    }

    /// Applies the patch to an Operation.
    pub fn apply_to_operation(self, op: &mut Operation) {
        self.finalize().apply(op);
    }
}

/// Chainable builder for patching return metadata.
/// Created by PatchSpec::ret() or PatchSpec::append(), chains back to PatchSpec methods.
pub struct ReturnPatch {
    spec: PatchOp,
    is_append: bool,
    description: Option<String>,
    status_code: Option<u16>,
    part: Option<ReturnPart>,
}

impl ReturnPatch {
    /// Sets the return documentation.
    pub fn doc(mut self, doc: impl Into<String>) -> Self {
        self.description = Some(doc.into());
        self
    }

    /// Sets the HTTP status code for this return.
    pub fn status(mut self, status_code: u16) -> Self {
        self.status_code = Some(status_code);
        self
    }

    /// Sets the return type using a type that implements IntoReturnPart.
    /// Allows overriding auto-detected types.
    pub fn typed<T: IntoReturnPart>(mut self) -> Self {
        self.part = Some(T::into_return_part());
        self
    }

    /// Finalizes current return patch and starts patching an argument.
    pub fn arg(self, position: usize) -> ArgPatch {
        self.finalize().arg(position)
    }

    /// Finalizes current return patch and modifies the last return.
    pub fn ret(self) -> ReturnPatch {
        self.finalize().ret()
    }

    /// Finalizes current return patch and appends a new return.
    pub fn append(self) -> ReturnPatch {
        self.finalize().append()
    }

    /// Finalizes current return patch and continues with PatchSpec methods.
    fn finalize(self) -> PatchOp {
        let return_patch_data = ReturnPatchData {
            description: self.description,
            status_code: self.status_code,
            part: self.part,
        };
        let mut spec = self.spec;

        if self.is_append {
            spec.append_returns.push(return_patch_data);
        } else {
            spec.return_patch = Some(return_patch_data);
        }

        spec
    }

    /// Finalizes return patch and returns to PatchSpec for further chaining.
    pub fn done(self) -> PatchOp {
        self.finalize()
    }

    /// Applies the patch to an Operation.
    pub fn apply_to_operation(self, op: &mut Operation) {
        self.finalize().apply(op);
    }
}
