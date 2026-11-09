//! Same-source rejection feedback without a temporary unreserved IP image.
use super::*;
impl UserUdpStack {
    pub(super) fn send_unreachable_owned(
        &self,
        payload: &[u8],
        source: SocketAddress,
        destination: SocketAddress,
    ) -> bool {
        let spec = packet::UdpPacketSpec {
            source: sockaddr_to_ipaddr(&source),
            destination: sockaddr_to_ipaddr(&destination),
            source_port: source.port,
            destination_port: destination.port,
            payload,
        };
        let Ok(length) = spec.length() else {
            return false;
        };
        let header: usize = if spec.source.is_ipv4() { 20 } else { 40 };
        let quote = length.min(self.mtu.saturating_sub(header + 8));
        let quote = if spec.source.is_ipv4() {
            quote.max(header.min(length))
        } else {
            quote
        };
        let Some(response_length) = (header + 8).checked_add(quote) else {
            return false;
        };
        let Some(wire) = self.outbound.wire_memory() else {
            return false;
        };
        let Ok(encoded) = wire.workspace_lengths(response_length) else {
            return false;
        };
        let Ok(mut original) = self
            .outbound
            .allocate_transmit_groups(length, vec![vec![response_length], encoded])
        else {
            return false;
        };
        if spec.write_into(&mut original).is_err() {
            return false;
        }
        let Some(response) = packet::IcmpErrorSpec::for_packet(&original, self.mtu, true) else {
            return false;
        };
        if response.length() != response_length {
            return false;
        }
        let Ok(mut packets) = original.try_derive_owned(&[response_length]) else {
            return false;
        };
        let Some(mut packet) = packets.pop() else {
            return false;
        };
        if response.write_into(&mut packet).is_err() {
            return false;
        }
        self.outbound
            .try_send_packet(crate::packet_output::ObservedPacket {
                packet,
                observer: self.outbound.observer(),
            })
            .is_ok()
    }
}
