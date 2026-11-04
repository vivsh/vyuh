//! Typed heterogeneous child groups and invocation-local all-settled result views.

use serde::{Deserialize, de::DeserializeOwned};
use serde_json::value::RawValue;

use crate::callables::{DataBox, DataValue};
use crate::tasks::{TaskFailure, TaskRuntimeError};

/// Ordered child inputs for [`super::FlowState::all`], preserving each registration.
///
/// Insertion retains the concrete input type; the existing registry later resolves
/// its handler, lane, and policies. Constructing this collection submits no work.
/// Vectors and tuples of one through sixteen inputs also convert into this type.
#[derive(Default)]
pub struct FlowAllIn {
    pub(super) inputs: Vec<DataBox>,
}

impl FlowAllIn {
    /// Creates an empty group, which resumes with an empty result array.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends one typed child input and returns the group for builder construction.
    /// Registration and payload validation occur during normal outcome preparation.
    pub fn with<I: DataValue>(mut self, input: I) -> Self {
        self.push(input);
        self
    }

    /// Appends one child without submitting it or changing its registered lane.
    /// Each call may supply a different input type; nested tuples are not flattened.
    pub fn push<I: DataValue>(&mut self, input: I) {
        self.inputs.push(DataBox::new_data(input));
    }

    /// Returns the number of children, including repeated input types.
    pub fn len(&self) -> usize {
        self.inputs.len()
    }

    /// Returns whether this group contains no child inputs.
    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty()
    }
}

impl<I: DataValue> From<Vec<I>> for FlowAllIn {
    fn from(inputs: Vec<I>) -> Self {
        Self {
            inputs: inputs.into_iter().map(DataBox::new_data).collect(),
        }
    }
}

macro_rules! tuple_inputs {
    ($($type:ident : $value:ident),+) => {
        impl<$($type: DataValue),+> From<($($type,)+)> for FlowAllIn {
            fn from(($($value,)+): ($($type,)+)) -> Self {
                Self { inputs: vec![$(DataBox::new_data($value)),+] }
            }
        }
    };
}

tuple_inputs!(A: a);
tuple_inputs!(A: a, B: b);
tuple_inputs!(A: a, B: b, C: c);
tuple_inputs!(A: a, B: b, C: c, D: d);
tuple_inputs!(A: a, B: b, C: c, D: d, E: e);
tuple_inputs!(A: a, B: b, C: c, D: d, E: e, F: f);
tuple_inputs!(A: a, B: b, C: c, D: d, E: e, F: f, G: g);
tuple_inputs!(A: a, B: b, C: c, D: d, E: e, F: f, G: g, H: h);
tuple_inputs!(A: a, B: b, C: c, D: d, E: e, F: f, G: g, H: h, I: i);
tuple_inputs!(A: a, B: b, C: c, D: d, E: e, F: f, G: g, H: h, I: i, J: j);
tuple_inputs!(A: a, B: b, C: c, D: d, E: e, F: f, G: g, H: h, I: i, J: j, K: k);
tuple_inputs!(A: a, B: b, C: c, D: d, E: e, F: f, G: g, H: h, I: i, J: j, K: k, L: l);
tuple_inputs!(A: a, B: b, C: c, D: d, E: e, F: f, G: g, H: h, I: i, J: j, K: k, L: l, M: m);
tuple_inputs!(A: a, B: b, C: c, D: d, E: e, F: f, G: g, H: h, I: i, J: j, K: k, L: l, M: m, N: n);
tuple_inputs!(A: a, B: b, C: c, D: d, E: e, F: f, G: g, H: h, I: i, J: j, K: k, L: l, M: m, N: n, O: o);
tuple_inputs!(A: a, B: b, C: c, D: d, E: e, F: f, G: g, H: h, I: i, J: j, K: k, L: l, M: m, N: n, O: o, P: p);

/// Ordered, heterogeneous results extracted through an ordinary continuation.
///
/// Use this as [`super::Flow::Resume`] for dynamic groups. The existing outer
/// continuation result reports join assembly failures; this view retains each
/// child's success or failure. Successful JSON is owned but decoded only when
/// requested, without a JSON-value tree or a change to the persisted envelope.
#[derive(Deserialize)]
#[serde(transparent)]
pub struct FlowAllOut {
    results: Vec<Result<Box<RawValue>, TaskFailure>>,
}

impl FlowAllOut {
    /// Returns the number of child results in original input order.
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Returns whether this is the result of an empty group.
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    /// Decodes one successful value or returns its original child failure.
    ///
    /// The outer error reports an invalid index or incompatible successful JSON;
    /// the inner error is the persisted child failure, including its task ID.
    /// This does not consume results or change task state. Repeated calls repeat
    /// decoding; fixed-shape groups may instead use a typed tuple resume directly.
    pub fn decode<T: DeserializeOwned>(
        &self,
        index: usize,
    ) -> Result<Result<T, TaskFailure>, TaskRuntimeError> {
        let result = self
            .results
            .get(index)
            .ok_or(TaskRuntimeError::FlowAllIndex {
                index,
                len: self.len(),
            })?;
        match result {
            Ok(value) => serde_json::from_str(value.get())
                .map(Ok)
                .map_err(|source| TaskRuntimeError::FlowAllDecode { index, source }),
            Err(failure) => Ok(Err(failure.clone())),
        }
    }
}

#[cfg(test)]
#[path = "tests/all.rs"]
mod tests;
