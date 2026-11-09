//! Measure storage only. serde_json remains the sole syntax/schema authority.
use crate::packet_memory::{PacketMemoryError, PacketMetadataLayout};
pub(super) fn layout(bytes: &[u8]) -> Result<Option<PacketMetadataLayout>, PacketMemoryError> {
    let mut cursor = 0;
    let mut largest = 0;
    while cursor < bytes.len() {
        if bytes[cursor] != b'"' {
            cursor += 1;
            continue;
        }
        cursor += 1;
        let start = cursor;
        let mut escaped = false;
        while cursor < bytes.len() {
            match bytes[cursor] {
                b'"' => break,
                b'\\' => {
                    escaped = true;
                    cursor += 1;
                    if cursor < bytes.len() {
                        cursor += 1;
                    }
                }
                _ => cursor += 1,
            }
        }
        if escaped {
            largest = largest.max(cursor - start);
        }
        if cursor < bytes.len() {
            cursor += 1;
        }
    }
    if largest == 0 {
        return Ok(None);
    }
    // Pinned serde_json SliceRead uses one reused Vec<u8>, extending literal
    // spans, pushing escaped bytes and reserve(4) for Unicode. Each decoded
    // prefix/reservation is bounded by this raw escaped token's length.
    // Rust's Vec<u8> minimum allocation is8; each later amortized growth has
    // old capacity < required <= largest and new capacity <= 2*largest.
    // Reserve the simultaneous old+new peak separately from raw/output bytes.
    let (allocations, peak) = if largest <= 8 {
        (1, 8)
    } else {
        (
            2,
            largest.checked_mul(3).ok_or(PacketMemoryError::Overflow)?,
        )
    };
    Ok(Some(PacketMetadataLayout::new(allocations, peak)))
}
