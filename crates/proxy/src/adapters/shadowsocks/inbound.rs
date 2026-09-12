//! Shadowsocks inbound preparation and protocol handshake handoff.

use ::shadowsocks::transport::ShadowsocksInboundBindings;

use crate::runtime::inbound_operation::{
    InboundConnectionContext, TcpAndDatagramInboundListenerOperation,
};
use crate::runtime::tcp_ingress::NoClientResponseStreamProtocol;
use crate::transport::{MeteredStream, TcpRelayStream};

pub(super) fn prepare(
    listen_address: String,
    listen_port: u16,
    mut bindings: ShadowsocksInboundBindings,
) -> Box<dyn crate::runtime::inbound_operation::PreparedInboundListenerOperation> {
    let carrier = bindings.take_carrier_plan();
    let (acceptor, udp_relay) = bindings.into_parts();
    Box::new(TcpAndDatagramInboundListenerOperation {
        carrier,
        protocol_name: "shadowsocks",
        error_protocol_name: "shadowsocks",
        listen_address,
        listen_port,
        tcp_request: acceptor,
        tcp_dispatch: |acceptor: ::shadowsocks::transport::ShadowsocksInboundTcpAcceptor,
                       socket,
                       context: InboundConnectionContext| async move {
            let (session, client) = acceptor
                .accept_stream(MeteredStream::new(TcpRelayStream::from(socket)))
                .await?;
            context
                .serve(session, client, NoClientResponseStreamProtocol::new())
                .await
        },
        udp_relay,
    })
}
