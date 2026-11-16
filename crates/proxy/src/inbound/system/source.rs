//! System TCP keeps its existing local-destination semantics on real TCP originals.
use std::{num::NonZeroUsize, sync::Arc};
use zero_core::{
    request::{Destination, RequestFacts},
    Address, ProtocolType,
};
use zero_platform_tokio::{
    contract::{
        component::*,
        context::{HandshakeContext, InboundConnectionFacts},
        inbound::{
            accept_stream, erase_inbound, tcp, AcceptedRequest, InboundHandshake, ReplyOutcome,
            StreamReply,
        },
        lifecycle::{erase_lifecycle, BuildError},
        ChannelError, Impact, Reason, Stage,
    },
    TcpRelayStream, TokioSocket,
};
#[derive(Clone)]
pub(crate) struct Options {
    pub listen: std::net::SocketAddr,
    pub accepted: AcceptedTcpLimits,
}
pub(crate) struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Inbound],
            dependencies: vec![],
            static_identity: format!("system-tcp:{:?}:{:?}", options.listen, options.accepted)
                .into_bytes()
                .into(),
            resources: vec![ResourceDeclaration::TcpListener {
                destination: destination(options.listen),
                accepted: Some(options.accepted),
                max_bind_attempts: NonZeroUsize::new(1).unwrap(),
            }],
            task_limit: None,
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let (owner, source) = tcp::source(
            destination(options.listen),
            context.resources,
            context.cancel,
            {
                let ip = options.listen.ip();
                move |socket| Connection::new(socket, ip)
            },
        );
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(owner),
            roles: ComponentRoles {
                inbound: Some(erase_inbound(source)),
                ..Default::default()
            },
        })
    }
}
fn destination(address: std::net::SocketAddr) -> Destination {
    Destination::new(
        match address.ip() {
            std::net::IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
            std::net::IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
        },
        address.port(),
    )
}
struct Connection {
    io: TcpRelayStream,
    facts: InboundConnectionFacts,
    target: std::io::Result<Destination>,
}
impl Connection {
    fn new(socket: TokioSocket, listen_ip: std::net::IpAddr) -> Self {
        // SystemTcpStack historically selected the listener's local address.
        // Keep that fact; do not invent transparent original-destination lookup.
        Self {
            target: socket
                .local_addr()
                .map(|actual| destination(std::net::SocketAddr::new(listen_ip, actual.port()))),
            facts: InboundConnectionFacts::from_stream(&socket),
            io: socket.into(),
        }
    }
}
impl InboundHandshake for Connection {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        let target = self.target.map_err(|source| {
            ChannelError::new(Stage::Handshake, Reason::Io, Impact::Session).with_source(source)
        })?;
        let facts = self
            .facts
            .request(&context, ProtocolType::UNKNOWN, Some(target), None)?;
        Ok(AcceptedRequest::Stream(accept_stream(Reply {
            io: self.io,
            facts,
        })))
    }
}
struct Reply {
    io: TcpRelayStream,
    facts: RequestFacts,
}
impl StreamReply for Reply {
    type Stream = TcpRelayStream;
    fn facts(&self) -> &RequestFacts {
        &self.facts
    }
    async fn respond(self, outcome: ReplyOutcome) -> Result<Option<Self::Stream>, ChannelError> {
        Ok(matches!(outcome, ReplyOutcome::LocalChannelReady).then_some(self.io))
    }
}
