use crate::runtime::udp_dispatch::packet_path_operation::PreparedUdpPacketPathOperation;
use crate::runtime::udp_flow::packet_path::{PacketPathFlowBinding, UdpPacketRef};
use crate::runtime::udp_flow::packet_path_chain::{
    PacketPathCarrierRequest, PacketPathStartRequest,
};

pub(super) fn build_udp_packet_path_pair<'a>(
    session_id: u64,
    carrier_operation: Box<dyn PreparedUdpPacketPathOperation>,
    datagram_operation: Box<dyn PreparedUdpPacketPathOperation>,
    packet: UdpPacketRef<'a>,
) -> Option<(PacketPathFlowBinding, PacketPathStartRequest<'a>)> {
    let carrier_desc = carrier_operation.carrier_descriptor()?;
    let datagram = datagram_operation.datagram_source()?;
    let flow_binding = PacketPathFlowBinding::new(datagram.clone(), &carrier_desc);

    Some((
        flow_binding,
        PacketPathStartRequest {
            session_id,
            carrier: PacketPathCarrierRequest {
                descriptor: carrier_desc,
                build_operation: carrier_operation,
            },
            datagram,
            packet,
        },
    ))
}
