//! Mieru inbound preparation and protocol handshake handoff.

use ::mieru::transport::MieruInboundListenerRequest;

use crate::runtime::inbound_operation::{InboundConnectionContext, TcpInboundListenerOperation};
use crate::transport::{MeteredStream, TcpRelayStream};

pub(super) fn prepare(
    profile: MieruInboundListenerRequest,
    udp: bool,
) -> Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation> {
    if udp {
        return Box::new(
            crate::runtime::inbound_operation::PeerRouteInboundListenerOperation {
                request: profile,
                max_packet_size: MieruInboundListenerRequest::MAX_PACKET_SIZE,
                pending_packets: MieruInboundListenerRequest::PACKET_QUEUE_CAPACITY,
                dispatch: |profile: MieruInboundListenerRequest,
                           socket,
                           peer,
                           packets,
                           context: InboundConnectionContext| async move {
                    let connection = profile.accept_packet_peer(socket, peer, packets).await?;
                    context.run_route_multiplexer(connection, "mieru_udp").await
                },
            },
        );
    }
    Box::new(TcpInboundListenerOperation {
        protocol_name: "mieru",
        error_protocol_name: "mieru",
        request: profile,
        dispatch: |profile: MieruInboundListenerRequest,
                   socket,
                   context: InboundConnectionContext| async move {
            let connection = profile
                .accept_multiplexer(MeteredStream::new(TcpRelayStream::from(socket)))
                .await?;
            context.run_route_multiplexer(connection, "mieru_udp").await
        },
    })
}
