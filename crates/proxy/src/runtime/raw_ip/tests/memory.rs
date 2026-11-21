//! Explicit allocation grant of the original software Source in these fixtures.
use super::RawIpStackOptions;
use std::sync::{Arc, OnceLock};

pub(crate) fn source(options: RawIpStackOptions, mtu: usize) -> RawIpStackOptions {
    let read = options.packet_return_totals.native_grant(mtu).unwrap();
    let cell = Arc::new(OnceLock::new());
    assert!(cell.set(read.memory_view().unwrap()).is_ok());
    options
        .with_packet_wire_memory(zero_stack::packet_output::memory::PacketWireMemory::Native)
        .with_packet_memory_cell(cell)
}
