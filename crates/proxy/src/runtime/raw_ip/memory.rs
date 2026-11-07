//! Byte-layout projection only; the actual Source supplies this immutable fact.
pub(crate) fn wire_layout(
    layout: zero_platform_tokio::contract::packet::PacketMemoryLayout,
) -> zero_stack::packet_output::memory::PacketWireMemory {
    use zero_platform_tokio::contract::packet::PacketMemoryLayout;
    use zero_stack::packet_output::memory::PacketWireMemory;
    match layout {
        PacketMemoryLayout::Native => PacketWireMemory::Native,
        PacketMemoryLayout::Padded { multiple, overhead } => PacketWireMemory::PaddedEnvelope {
            multiple: multiple.get(),
            overhead,
        },
    }
}

pub(crate) fn source_layout(
    layout: zero_stack::packet_output::memory::PacketWireMemory,
) -> Result<zero_platform_tokio::contract::packet::PacketMemoryLayout, zero_traits::PacketDeriveError>
{
    use zero_platform_tokio::contract::packet::PacketMemoryLayout;
    use zero_stack::packet_output::memory::PacketWireMemory;
    Ok(match layout {
        PacketWireMemory::Native => PacketMemoryLayout::Native,
        PacketWireMemory::PaddedEnvelope { multiple, overhead } => PacketMemoryLayout::Padded {
            multiple: std::num::NonZeroU16::new(multiple)
                .ok_or(zero_traits::PacketDeriveError::LayoutMismatch)?,
            overhead,
        },
    })
}
