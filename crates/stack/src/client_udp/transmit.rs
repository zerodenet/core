//! One actual UDP send pays its entire IP/fragment/codec storage before output.
use crate::{packet, packet_output::PacketSender};
use zero_traits::{PacketBuffer, PacketDeriveError};

pub(crate) async fn build(
    sender: &PacketSender,
    spec: packet::UdpPacketSpec<'_>,
    mtu: usize,
    identification: u32,
) -> Result<Vec<PacketBuffer>, PacketDeriveError> {
    let length = spec.length()?;
    let fragments = spec.fragment_lengths(mtu)?;
    let wire = sender.wire_memory().ok_or(PacketDeriveError::Unreserved)?;
    let mut groups = Vec::new();
    if !fragments.is_empty() {
        groups.push(fragments.clone());
        for fragment in &fragments {
            groups.push(wire.workspace_lengths(*fragment)?);
        }
    } else {
        groups.push(wire.workspace_lengths(length)?);
    }
    let mut original = sender.allocate_transmit_groups(length, groups)?;
    spec.write_into(&mut original)?;
    if fragments.is_empty() {
        return Ok(vec![original]);
    }
    // This builder created the temporary IP image from the still-borrowed
    // caller input. Once the exact fragments are filled, nothing uses that
    // image again. Each child retains the same envelope's entire original,
    // fragment and wire promise until its last actual user returns; no extra
    // Arc<original> or erased Derived wrapper is needed here. The explicit
    // general library fragmentation API keeps its original custody contract.
    let mut outputs = original.try_derive_zeroed(&fragments)?;
    packet::write_packet_fragments(&original, mtu, identification, &mut outputs)?;
    Ok(outputs)
}
