use super::*;
pub(super) use zero_runtime::configuration::{
    control_json_decoder_workspace as scratch, control_json_map_bound as map,
    control_json_typed_array_bound as typed_array,
};
use zero_runtime::configuration::{control_json_storage_bound, ControlJsonStorage};
pub(super) fn value(raw: &[u8], book: &Arc<ControlBudget>) -> io::Result<usize> {
    control_json_storage_bound(raw, book, ControlJsonStorage::Value)
}
pub(super) fn content(raw: &[u8], book: &Arc<ControlBudget>) -> io::Result<usize> {
    control_json_storage_bound(raw, book, ControlJsonStorage::Content)
}
pub(super) fn rows(
    raw: &[u8],
    book: &Arc<ControlBudget>,
    element_size: usize,
) -> io::Result<usize> {
    control_json_storage_bound(raw, book, ControlJsonStorage::TypedRows { element_size })
}
