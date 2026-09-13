use ::vless::transport::VlessInboundListenerRequest;
use zero_engine::EngineError;

use crate::runtime::inbound_operation::{
    InboundConnectionContext, TcpOrQuicInboundListenerOperation,
};
use crate::runtime::inbound_route::RecordedProtocolMuxRouteDefaults;
use crate::runtime::tcp_ingress::ClientResponseInboundProtocol;
use crate::runtime::{
    prepare_inbound_route_accept, InboundFallbackTarget, PreparedInboundRouteAccept,
};

#[derive(Clone)]
struct PreparedVlessInboundRequest {
    protocol: VlessInboundListenerRequest,
    fallback: Option<InboundFallbackTarget>,
}

pub(super) fn prepare(
    request: VlessInboundListenerRequest,
    fallback: Option<InboundFallbackTarget>,
    http3: bool,
) -> Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation> {
    let request = PreparedVlessInboundRequest {
        protocol: request,
        fallback,
    };
    if request.protocol.uses_mkcp() {
        return Box::new(
            crate::runtime::inbound_operation::PeerRouteInboundListenerOperation {
                request,
                max_packet_size: 65507,
                pending_packets: 128,
                dispatch: |request: PreparedVlessInboundRequest,
                           socket,
                           peer,
                           packets,
                           context: InboundConnectionContext| async move {
                    let streams = request.protocol.accept_mkcp_peer(socket, peer, packets)?;
                    context
                        .run_transport_streams(streams, move |stream, context| {
                            let request = request.clone();
                            async move {
                                let protocol = ClientResponseInboundProtocol::new(
                                    request.protocol.response_protocol(),
                                );
                                let accepted = tokio::time::timeout(
                                    std::time::Duration::from_secs(30),
                                    request.protocol.accept_recorded_mkcp_route(stream),
                                )
                                .await
                                .map_err(|_| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::TimedOut,
                                        "mKCP inbound handshake timed out",
                                    )
                                })??;
                                let accepted =
                                    prepare_inbound_route_accept(accepted, request.fallback)?;
                                context
                                    .dispatch_recorded_mux_stream_route(
                                        Ok(accepted),
                                        protocol,
                                        recorded_route_defaults(),
                                    )
                                    .await
                            }
                        })
                        .await
                },
            },
        );
    }
    if http3 || request.protocol.uses_hysteria() {
        return Box::new(
            crate::runtime::inbound_operation::QuicConnectionInboundListenerOperation {
                protocol_name: request.protocol.protocol_name(),
                request,
                dispatch: |request: PreparedVlessInboundRequest,
                           connection,
                           local,
                           context: InboundConnectionContext| async move {
                    let source = request.protocol.accept_http3_streams(connection, local)?;
                    dispatch_source(request, source, context).await
                },
            },
        );
    }
    Box::new(TcpOrQuicInboundListenerOperation {
        accept_proxy_protocol: request.protocol.accepts_proxy_protocol(),
        protocol_name: request.protocol.protocol_name(),
        error_protocol_name: request.protocol.error_protocol_name(),
        request,
        dispatch_tcp: |request: PreparedVlessInboundRequest,
                       socket,
                       context: InboundConnectionContext| async move {
            let request = PreparedVlessInboundRequest {
                protocol: request
                    .protocol
                    .with_handshake_target_connector(context.handshake_target_connector()),
                fallback: request.fallback,
            };
            if request.protocol.has_multiplexed_transport() {
                return dispatch_transport(request, socket, context).await;
            }
            let protocol = ClientResponseInboundProtocol::new(request.protocol.response_protocol());
            let defaults = recorded_route_defaults();
            let fallback = request.fallback;
            let accept = request
                .protocol
                .accept_recorded_tcp_route(socket)
                .await
                .map_err(EngineError::from)?;
            let accept: Option<PreparedInboundRouteAccept<_, _>> = accept
                .map(|result| prepare_inbound_route_accept(result, fallback))
                .transpose()?;
            context
                .dispatch_recorded_mux_tcp_route(Ok(accept), protocol, defaults)
                .await
        },
        dispatch_quic: |request: PreparedVlessInboundRequest,
                        stream,
                        context: InboundConnectionContext| async move {
            let protocol = ClientResponseInboundProtocol::new(request.protocol.response_protocol());
            let defaults = recorded_route_defaults();
            let fallback = request.fallback;
            let accept = request
                .protocol
                .accept_recorded_quic_route(stream)
                .await
                .map_err(EngineError::from)?;
            let accept = prepare_inbound_route_accept(accept, fallback)?;
            context
                .dispatch_recorded_mux_stream_route(Ok(accept), protocol, defaults)
                .await
        },
    })
}

fn recorded_route_defaults() -> RecordedProtocolMuxRouteDefaults {
    RecordedProtocolMuxRouteDefaults {
        udp_protocol: VlessInboundListenerRequest::UDP_PROTOCOL,
        mux_protocol: VlessInboundListenerRequest::MUX_PROTOCOL,
        panic_message: VlessInboundListenerRequest::PANIC_MESSAGE,
        abort_on_end: VlessInboundListenerRequest::ABORT_ON_END,
        udp_accept_log_message: Some("MUX stream accepted"),
    }
}

async fn dispatch_transport(
    request: PreparedVlessInboundRequest,
    socket: zero_platform_tokio::TokioSocket,
    context: InboundConnectionContext,
) -> Result<(), EngineError> {
    match request.protocol.accept_transport_streams(socket).await? {
        zero_core::InboundRouteAccept::Control(control) => {
            context.serve_control_session(control).await
        }
        zero_core::InboundRouteAccept::Route(source) => {
            dispatch_source(request, source, context).await
        }
        zero_core::InboundRouteAccept::Fallback(replay) => {
            type Route = vless::inbound::VlessAcceptedClientRoute<
                zero_transport::MeteredStream<
                    zero_transport::RecordingStream<zero_platform_tokio::TcpRelayStream>,
                >,
            >;
            let accept = prepare_inbound_route_accept(
                zero_core::InboundRouteAccept::<Route, _>::Fallback(replay),
                request.fallback,
            )?;
            let protocol = ClientResponseInboundProtocol::new(request.protocol.response_protocol());
            context
                .dispatch_recorded_mux_tcp_route(
                    Ok(Some(accept)),
                    protocol,
                    recorded_route_defaults(),
                )
                .await
        }
    }
}

async fn dispatch_source(
    request: PreparedVlessInboundRequest,
    source: vless::transport::VlessInboundTransportStreams,
    context: InboundConnectionContext,
) -> Result<(), EngineError> {
    context
        .run_transport_streams(source, move |incoming, context| {
            let request = request.clone();
            async move {
                let protocol =
                    ClientResponseInboundProtocol::new(request.protocol.response_protocol());
                let accept = tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    request.protocol.accept_recorded_transport_stream(incoming),
                )
                .await
                .map_err(|_| {
                    EngineError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "inbound stream handshake timed out",
                    ))
                })??;
                let accept = prepare_inbound_route_accept(accept, request.fallback)?;
                context
                    .dispatch_recorded_mux_stream_route(
                        Ok(accept),
                        protocol,
                        recorded_route_defaults(),
                    )
                    .await
            }
        })
        .await
}
