//! One actual UDP send pays its entire IP/fragment/codec storage before output.
use crate::{packet, packet_output::PacketSender};
use zero_traits::{PacketBuffer, PacketDeriveError};

pub(super) async fn build(
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
    packet::fragment_ip_packet_accounted(original, mtu, identification)
        .await
        .map_err(|source| source.cause)
}
