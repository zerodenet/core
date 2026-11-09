//! Concrete existing frontend JSON ownership, on the same control byte book.
use serde::Deserialize;
use serde_json::{value::RawValue, Value};
use std::{io, sync::Arc};
use zero_runtime::configuration::{ControlBudget, ControlBudgetPermit, ControlCharge};
mod body;
mod estimate;
pub(crate) use body::http as http_storage;
mod ipc;
mod owned;
mod validate;
pub(crate) struct Decoded<T> {
    value: T,
    storage: ControlBudgetPermit,
}
impl<T: std::fmt::Debug> std::fmt::Debug for Decoded<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}
impl<T> Decoded<T> {
    pub(crate) fn into_parts(self) -> (T, ControlBudgetPermit) {
        (self.value, self.storage)
    }
}
impl<T> std::ops::Deref for Decoded<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.value
    }
}
pub(crate) enum DecodeError {
    Capacity(io::Error),
    Invalid {
        source: serde_json::Error,
        _workspace: ControlBudgetPermit,
    },
}
impl DecodeError {
    pub(crate) fn into_capacity(self) -> Option<io::Error> {
        match self {
            Self::Capacity(source) => Some(source),
            Self::Invalid { .. } => None,
        }
    }
}
impl std::fmt::Debug for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Capacity(e) => f.debug_tuple("InputCapacity").field(e).finish(),
            Self::Invalid { source, .. } => f.debug_tuple("InputJson").field(source).finish(),
        }
    }
}
fn capacity() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "control decoded storage envelope exceeds byte capacity",
    )
}
#[derive(Debug)]
enum Failure {
    Capacity(io::Error),
    Invalid(serde_json::Error),
}
impl From<io::Error> for Failure {
    fn from(e: io::Error) -> Self {
        Self::Capacity(e)
    }
}
impl From<serde_json::Error> for Failure {
    fn from(e: serde_json::Error) -> Self {
        Self::Invalid(e)
    }
}
fn sum(a: usize, b: usize) -> io::Result<usize> {
    a.checked_add(b).ok_or_else(capacity)
}
fn workspace(raw: &[u8], book: &Arc<ControlBudget>) -> io::Result<ControlBudgetPermit> {
    // A serde data error can quote an offending original string. Pay its
    // bounded text as well as decoder scratch before constructing the error.
    let bytes = sum(sum(raw.len(), 4096)?, estimate::scratch(raw)?)?;
    book.reserve(ControlCharge::RequestBytes(bytes))
}
fn value(
    raw: &RawValue,
    book: &Arc<ControlBudget>,
    storage: &mut ControlBudgetPermit,
) -> Result<Value, Failure> {
    storage.grow(estimate::value(raw.get().as_bytes(), book)?)?;
    Ok(serde_json::from_str(raw.get())?)
}
fn finish<T>(
    result: Result<T, Failure>,
    storage: ControlBudgetPermit,
    work: ControlBudgetPermit,
) -> Result<Decoded<T>, DecodeError> {
    match result {
        Ok(value) => {
            drop(work);
            Ok(Decoded { value, storage })
        }
        Err(Failure::Capacity(source)) => Err(DecodeError::Capacity(source)),
        Err(Failure::Invalid(source)) => Err(DecodeError::Invalid {
            source,
            _workspace: work,
        }),
    }
}
pub(crate) fn decode_ipc(
    raw: &str,
    book: &Arc<ControlBudget>,
) -> Result<Decoded<crate::ipc::protocol::IpcRequest>, DecodeError> {
    ipc::decode(raw, book)
}
pub(crate) fn grow_command(
    method: &str,
    params: &Value,
    storage: &mut ControlBudgetPermit,
    book: &Arc<ControlBudget>,
) -> io::Result<()> {
    let mut bytes = owned::command(method, params, book)?;
    // The existing from_value error may retain the offending method/field.
    bytes = sum(bytes, sum(method.len(), 4096)?)?;
    storage.grow(bytes)
}
pub(crate) fn clone_id(id: &Option<Value>) -> io::Result<usize> {
    match id {
        Some(value) => owned::cloned(value),
        None => Ok(0),
    }
}
/// Prepay Value/Content/concrete rows from the immutable wire, then run the
/// original decoder, including its first-key/duplicate/raw_value semantics.
pub(crate) fn decode_query(
    raw: &[u8],
    book: &Arc<ControlBudget>,
) -> Result<Decoded<zero_api::QueryRequest>, DecodeError> {
    let work = workspace(raw, book).map_err(DecodeError::Capacity)?;
    let mut storage = book
        .reserve(ControlCharge::RequestBytes(0))
        .map_err(DecodeError::Capacity)?;
    let result = (|| -> Result<zero_api::QueryRequest, Failure> {
        storage.grow(body::bytes(raw, book, true)?)?;
        Ok(serde_json::from_slice(raw)?)
    })();
    finish(result, storage, work)
}

#[cfg(test)]
#[path = "tests/request_json.rs"]
mod tests;
