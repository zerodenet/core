//! Shadowsocks inbound preparation and protocol handshake handoff.

use ::shadowsocks::transport::ShadowsocksInboundBindings;

use crate::runtime::inbound_operation::{
    InboundConnectionContext, TcpAndDatagramInboundListenerOperation,
};
use crate::transport::{MeteredStream, TcpRelayStream};

pub(super) fn prepare(
    listen_address: String,
    listen_port: u16,
    mut bindings: ShadowsocksInboundBindings,
) -> Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation> {
    let carrier = bindings.take_carrier_plan();
    let (acceptor, udp_relay) = bindings.into_parts();
    Box::new(TcpAndDatagramInboundListenerOperation {
        host: None,
        carrier,
        protocol_name: "shadowsocks",
        error_protocol_name: "shadowsocks",
        listen_address,
        listen_port,
        tcp_request: acceptor,
        tcp_dispatch: |acceptor: ::shadowsocks::transport::ShadowsocksInboundTcpAcceptor,
                       socket,
                       context: InboundConnectionContext| async move {
            let (destination, auth, client) = acceptor
                .accept_stream_facts(MeteredStream::new(TcpRelayStream::from(socket)))
                .await?;
            context
                .serve_standard_stream(
                    ::shadowsocks::ShadowsocksInbound.protocol(),
                    destination,
                    Some(auth),
                    None,
                    move |facts| {
                        ::shadowsocks::component::inbound::TcpReply::new(client, facts)
                            .map(zero_platform_tokio::contract::inbound::accept_stream)
                    },
                )
                .await
        },
        udp_relay,
    })
}
