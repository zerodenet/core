//! Mieru inbound preparation and protocol handshake handoff.

use ::mieru::transport::MieruInboundListenerRequest;

use crate::runtime::inbound_operation::{InboundConnectionContext, TcpInboundListenerOperation};
use crate::transport::{MeteredStream, TcpRelayStream};

pub(super) fn prepare(
    profile: MieruInboundListenerRequest,
) -> Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation> {
    Box::new(TcpInboundListenerOperation {
        protocol_name: "mieru",
        error_protocol_name: "mieru",
        request: profile,
        dispatch: |profile: MieruInboundListenerRequest,
                   socket,
                   context: InboundConnectionContext| async move {
            let response = profile.response_protocol();
            let route = profile
                .accept_client(MeteredStream::new(TcpRelayStream::from(socket)))
                .await?;
            context
                .dispatch_stream_route_with_client_response(route, response, "mieru_udp")
                .await
        },
    })
}
