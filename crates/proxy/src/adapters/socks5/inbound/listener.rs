//! SOCKS5 protocol handshake and accepted-route handoff.

use ::socks5::transport::{setup_inbound_udp_association, Socks5InboundAcceptor};
use zero_engine::EngineError;

use crate::runtime::inbound_operation::InboundConnectionContext;
use crate::transport::{MeteredStream, TcpRelayStream};

pub(crate) async fn handle_socks5_connection(
    context: InboundConnectionContext,
    metered: MeteredStream<TcpRelayStream>,
    acceptor: &Socks5InboundAcceptor,
) -> Result<(), EngineError> {
    let tcp_context = context.clone();
    let udp_context = context;
    let mut metered = metered;
    let facts = acceptor.accept_command_facts(&mut metered).await?;
    match facts.command {
        ::socks5::Socks5Command::Connect(destination) => {
            tcp_context
                .serve_standard_stream(
                    ::socks5::Socks5Inbound.protocol(),
                    destination,
                    facts.auth,
                    None,
                    move |facts| {
                        ::socks5::component::inbound::ConnectReply::new(metered, facts)
                            .map(zero_platform_tokio::contract::inbound::accept_stream)
                    },
                )
                .await
        }
        ::socks5::Socks5Command::UdpAssociate(request) => {
            let setup = setup_inbound_udp_association(&mut metered, request).await?;
            udp_context
                .run_udp_association(
                    metered,
                    setup.relay,
                    setup.pending_control_traffic,
                    setup.handler,
                )
                .await
        }
    }
}
