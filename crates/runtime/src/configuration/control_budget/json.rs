//! Existing control JSON capacity only. No decoder result, callback, IO,
//! task/Scope/close authority, allocator or replacement control book.
use super::*;
mod estimate;
#[derive(Clone, Copy)]
pub enum ControlJsonStorage {
    Value,
    Content,
    TypedRows { element_size: usize },
}
/// Checked storage envelope for the same immutable input. Temporary measurement
/// scratch is prepaid from this exact original ControlBudget before parsing.
/// Malformed or magic-token strings keep prefix bounds; the actual frontend's
/// original serde decoder still decides its result/error and semantics.
pub fn control_json_storage_bound(
    raw: &[u8],
    book: &Arc<ControlBudget>,
    kind: ControlJsonStorage,
) -> io::Result<usize> {
    estimate::measure(raw, book, kind, false)
}
pub fn control_json_decoder_workspace(raw: &[u8]) -> io::Result<usize> {
    estimate::scratch(raw)
}
pub fn control_json_typed_array_bound(rows: usize, element_size: usize) -> io::Result<usize> {
    estimate::typed_array(rows, element_size)
}
pub fn control_json_map_bound(entries: usize) -> io::Result<usize> {
    estimate::map(entries)
}

#[cfg(test)]
#[path = "json_tests.rs"]
mod tests;

/// Same pure measurement for an actual response owner. The temporary parser
/// workspace is prepaid only from this book's ResponseBytes dimension.
pub fn control_json_response_storage_bound(
    raw: &[u8],
    book: &Arc<ControlBudget>,
    kind: ControlJsonStorage,
) -> io::Result<usize> {
    estimate::measure(raw, book, kind, true)
}
