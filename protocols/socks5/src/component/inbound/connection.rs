//! Authentication and wire replies only. The host supplies policy and originals.
use crate::transport::Socks5InboundAcceptor;
use std::sync::Arc;
use zero_platform_tokio::{
    contract::{
        component::ResourceAccess,
        context::{HandshakeContext, InboundConnectionFacts},
        inbound::{accept_datagram, accept_stream, AcceptedRequest, InboundHandshake},
        ChannelError, Impact, Reason, Stage,
    },
    TcpRelayStream,
};
use zero_transport::MeteredStream;

pub struct Connection {
    io: MeteredStream<TcpRelayStream>,
    facts: InboundConnectionFacts,
    acceptor: Socks5InboundAcceptor,
    resources: Arc<dyn ResourceAccess>,
}
impl Connection {
    pub fn new(
        io: TcpRelayStream,
        facts: InboundConnectionFacts,
        acceptor: Socks5InboundAcceptor,
        resources: Arc<dyn ResourceAccess>,
    ) -> Self {
        Self {
            io: MeteredStream::new(io),
            facts,
            acceptor,
            resources,
        }
    }
}
impl InboundHandshake for Connection {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        let mut socket = crate::component::socket::Socket::new(self.io);
        let command = match self.acceptor.accept_command_facts(&mut socket).await {
            Ok(command) => command,
            Err(wire) => return Err(wire_failure(wire, socket.take_source())),
        };
        let io = socket.into_inner();
        match command.command {
            crate::Socks5Command::Connect(destination) => {
                let facts = self.facts.request(
                    &context,
                    crate::Socks5Inbound.protocol(),
                    Some(destination),
                    command.auth,
                )?;
                Ok(AcceptedRequest::Stream(accept_stream(
                    super::ConnectReply::new(io, facts)?,
                )))
            }
            crate::Socks5Command::UdpAssociate(request) => {
                let caller = context.accepted.clone().ok_or_else(|| {
                    ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Session)
                })?;
                // UDP ASSOCIATE is a recognized target-free request, not a
                // fabricated TCP destination or an empty Control callback.
                let mut context = context;
                context.accepted = Some(caller.clone());
                let facts = self.facts.request(
                    &context,
                    crate::Socks5Inbound.protocol(),
                    None,
                    command.auth,
                )?;
                Ok(AcceptedRequest::Datagram(accept_datagram(
                    super::datagram::Reply {
                        io,
                        facts,
                        caller,
                        resources: self.resources,
                        session: crate::Socks5Inbound.accept_udp_association(request),
                    },
                )))
            }
        }
    }
}
pub(super) fn wire_failure(wire: zero_core::Error, source: Option<std::io::Error>) -> ChannelError {
    let reason = if source.is_some() {
        Reason::Io
    } else {
        Reason::Protocol
    };
    ChannelError::new(Stage::Handshake, reason, Impact::Session)
        .with_source(crate::component::error::HandshakeError { wire, source })
}
pub(super) fn io_failure(stage: Stage, source: std::io::Error) -> ChannelError {
    ChannelError::new(stage, Reason::Io, Impact::Session).with_source(source)
}
