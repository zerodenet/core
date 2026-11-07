//! Original filter and registration-vector heap storage, measured without copy.
use std::{io, mem::size_of};
use zero_api::EventFilter;
pub(super) fn overflow() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "subscriber storage size overflow",
    )
}
pub(super) fn filter_bytes(filter: &EventFilter) -> io::Result<usize> {
    let mut bytes = 0usize;
    for values in [
        &filter.event_types,
        &filter.principal_keys,
        &filter.inbound_tags,
    ] {
        bytes = bytes
            .checked_add(
                values
                    .capacity()
                    .checked_mul(size_of::<String>())
                    .ok_or_else(overflow)?,
            )
            .ok_or_else(overflow)?;
        for value in values {
            bytes = bytes.checked_add(value.capacity()).ok_or_else(overflow)?;
        }
    }
    Ok(bytes)
}
pub(super) fn vector_bytes<T>(capacity: usize) -> io::Result<usize> {
    capacity.checked_mul(size_of::<T>()).ok_or_else(overflow)
}

/// The pinned std array channel uses one usize stamp and one message slot.
/// This covers its actual preallocated array payload, not allocator/RSS or its
/// fixed channel control/one blocking-reader structures (registration unit).
#[repr(C)]
struct QueueSlotEnvelope {
    stamp: usize,
    event: std::mem::MaybeUninit<super::book::QueuedEvent>,
}
pub(super) fn queue_array_bytes(slots: usize) -> io::Result<usize> {
    slots
        .checked_mul(size_of::<QueueSlotEnvelope>())
        .ok_or_else(overflow)
}
