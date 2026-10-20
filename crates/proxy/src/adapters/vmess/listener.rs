use ::vmess::transport::{VmessInboundAcceptedRoute, VmessInboundListenerRequest};

use crate::runtime::inbound_operation::TcpInboundListenerOperation;
use crate::runtime::inbound_route::NoClientMuxRouteDefaults;

pub(super) fn prepare(
    request: VmessInboundListenerRequest,
) -> Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation> {
    Box::new(TcpInboundListenerOperation {
        protocol_name: request.protocol_name(),
        error_protocol_name: request.error_protocol_name(),
        request,
        dispatch:
            |request: VmessInboundListenerRequest,
             socket,
             context: crate::runtime::inbound_operation::InboundConnectionContext| async move {
                let defaults = NoClientMuxRouteDefaults {
                    udp_protocol: VmessInboundListenerRequest::UDP_PROTOCOL,
                    mux_protocol: VmessInboundListenerRequest::MUX_PROTOCOL,
                    panic_message: VmessInboundListenerRequest::PANIC_MESSAGE,
                    abort_on_end: VmessInboundListenerRequest::ABORT_ON_END,
                    read_error_log: VmessInboundListenerRequest::READ_ERROR_LOG,
                };
                match request.accept_standard_route(socket).await? {
                    VmessInboundAcceptedRoute::Tcp {
                        destination,
                        auth,
                        stream,
                    } => {
                        context
                            .serve_standard_stream(
                                ::vmess::inbound::VmessInbound.protocol(),
                                destination,
                                Some(auth),
                                None,
                                move |facts| {
                                    ::vmess::component::inbound::TcpReply::new(*stream, facts)
                                        .map(zero_platform_tokio::contract::inbound::accept_stream)
                                },
                            )
                            .await
                    }
                    VmessInboundAcceptedRoute::Legacy(route) => {
                        context.dispatch_no_client_mux_route(*route, defaults).await
                    }
                }
            },
    })
}
