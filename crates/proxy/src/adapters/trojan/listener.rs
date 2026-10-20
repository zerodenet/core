use ::trojan::transport::{TrojanInboundAcceptedRoute, TrojanInboundListenerRequest};

use crate::runtime::inbound_operation::TcpInboundListenerOperation;
use crate::runtime::inbound_route::NoClientMuxRouteDefaults;

pub(super) fn prepare(
    request: TrojanInboundListenerRequest,
) -> Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation> {
    Box::new(TcpInboundListenerOperation {
        protocol_name: request.protocol_name(),
        error_protocol_name: request.error_protocol_name(),
        request,
        dispatch:
            |request: TrojanInboundListenerRequest,
             socket,
             context: crate::runtime::inbound_operation::InboundConnectionContext| async move {
                let defaults = NoClientMuxRouteDefaults {
                    udp_protocol: TrojanInboundListenerRequest::UDP_PROTOCOL,
                    mux_protocol: TrojanInboundListenerRequest::MUX_PROTOCOL,
                    panic_message: TrojanInboundListenerRequest::PANIC_MESSAGE,
                    abort_on_end: TrojanInboundListenerRequest::ABORT_ON_END,
                    read_error_log: TrojanInboundListenerRequest::READ_ERROR_LOG,
                };
                match request.accept_standard_route(socket).await? {
                    TrojanInboundAcceptedRoute::Tcp {
                        destination,
                        auth,
                        stream,
                    } => {
                        context
                            .serve_standard_stream(
                                ::trojan::inbound::TrojanInbound.protocol(),
                                destination,
                                Some(auth),
                                None,
                                move |facts| {
                                    ::trojan::component::inbound::TcpReply::new(*stream, facts)
                                        .map(zero_platform_tokio::contract::inbound::accept_stream)
                                },
                            )
                            .await
                    }
                    TrojanInboundAcceptedRoute::Legacy(route) => {
                        context.dispatch_no_client_mux_route(*route, defaults).await
                    }
                }
            },
    })
}
