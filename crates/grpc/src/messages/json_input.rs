//! Same original accepted stream/work/Scope request owner, no second record.
use super::registry::Record;
use std::{io, sync::Arc};
use zero_runtime::configuration::{
    control_json_decoder_workspace, control_json_map_bound, control_json_storage_bound,
    control_json_typed_array_bound, ControlBudget, ControlCharge, ControlJsonStorage,
};
pub(super) fn capacity() -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, "gRPC JSON storage overflow")
}
pub(super) fn sum(a: usize, b: usize) -> io::Result<usize> {
    a.checked_add(b).ok_or_else(capacity)
}
pub(super) mod estimate {
    use super::*;
    pub(in crate::messages) fn value(raw: &[u8], book: &Arc<ControlBudget>) -> io::Result<usize> {
        control_json_storage_bound(raw, book, ControlJsonStorage::Value)
    }
    pub(in crate::messages) fn typed_array(n: usize, size: usize) -> io::Result<usize> {
        control_json_typed_array_bound(n, size)
    }
    pub(in crate::messages) fn map(n: usize) -> io::Result<usize> {
        control_json_map_bound(n)
    }
    pub(in crate::messages) fn scratch(raw: &[u8]) -> io::Result<usize> {
        control_json_decoder_workspace(raw)
    }
}
pub(super) fn prepay(original: &Record, raw: &[u8], query: bool) -> io::Result<()> {
    let work_bytes = sum(sum(raw.len(), 4096)?, control_json_decoder_workspace(raw)?)?;
    let _workspace = original
        .budget
        .reserve(ControlCharge::RequestBytes(work_bytes))?;
    let value_bytes = estimate::value(raw, &original.budget)?;
    let _probe = original
        .budget
        .reserve(ControlCharge::RequestBytes(value_bytes))?;
    let extra = match serde_json::from_slice::<serde_json::Value>(raw) {
        Ok(value) => {
            if query {
                super::json_shape::query(&value)?
            } else {
                match (
                    value.get("method").and_then(serde_json::Value::as_str),
                    value.get("params"),
                ) {
                    (Some(method), Some(params)) => {
                        super::json_shape::command(method, params, &original.budget)?
                    }
                    _ => 0,
                }
            }
        }
        Err(_) if !query => control_json_storage_bound(
            raw,
            &original.budget,
            ControlJsonStorage::TypedRows {
                element_size: std::mem::size_of::<zero_api::StatsResetTarget>()
                    .max(std::mem::size_of::<String>()),
            },
        )?,
        Err(_) => 0,
    };
    let content = if query {
        0
    } else {
        control_json_storage_bound(raw, &original.budget, ControlJsonStorage::Content)?
    };
    let bytes = sum(sum(sum(value_bytes, extra)?, content)?, work_bytes)?;
    drop((_probe, _workspace));
    // Measurement Values/errors have gone before this lock/grow, and all
    // transient guards return outside the Record lock before actual decoding.
    original
        .input
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_mut()
        .ok_or_else(|| io::Error::other("original gRPC input not installed"))?
        .grow(bytes)
}
