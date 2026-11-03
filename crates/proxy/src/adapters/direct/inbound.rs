//! Built-in fixed-target Stream source. Policy, native sockets and tasks stay hosted.
use std::{num::NonZeroUsize, sync::Arc};
use zero_core::{
    request::{Destination, RequestFacts},
    ProtocolType,
};
use zero_platform_tokio::{
    contract::{
        component::{
            AcceptedTcpLimits, BuildContext, ComponentFactory, ComponentInstance, ComponentRoles,
            ComponentSpec, ResourceDeclaration, ResourceOwner, RoleKind,
        },
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

pub(crate) struct Options {
    listen: Destination,
    target: Option<Destination>,
    udp: bool,
}
pub(crate) fn options(
    inbound: &zero_config::InboundConfig,
) -> Result<Options, zero_config::ConfigError> {
    let zero_config::InboundProtocolConfig::Direct { target, port } = &inbound.protocol else {
        return Err(zero_config::ConfigError::InvalidInbound(
            "Direct factory received another protocol".into(),
        ));
    };
    if inbound.udp.enabled && !cfg!(feature = "managed-datagram-runtime") {
        return Err(zero_config::ConfigError::InvalidInbound(
            "Direct UDP requires managed-datagram-runtime".into(),
        ));
    }
    Ok(Options {
        udp: inbound.udp.enabled,
        listen: match zero_core::address::socket_addr(&inbound.listen.address, inbound.listen.port)
        {
            Some(address) => destination(&address.ip().to_string(), address.port()),
            None => Destination::new(
                zero_core::Address::Domain(inbound.listen.address.clone()),
                inbound.listen.port,
            ),
        },
        target: target
            .as_ref()
            .map(|target| destination(target, port.unwrap_or(443))),
    })
}
fn destination(host: &str, port: u16) -> Destination {
    let address = match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(ip)) => zero_core::Address::Ipv4(ip.octets()),
        Ok(std::net::IpAddr::V6(ip)) => zero_core::Address::Ipv6(ip.octets()),
        Err(_) => zero_core::Address::Domain(host.to_owned()),
    };
    Destination::new(address, port)
}
pub(crate) struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Inbound],
            dependencies: vec![],
            static_identity: format!(
                "direct-inbound:{:?}:{:?}:{}",
                options.listen, options.target, options.udp
            )
            .into_bytes()
            .into(),
            resources: {
                let mut resources = vec![ResourceDeclaration::TcpListener {
                    destination: options.listen.clone(),
                    accepted: Some(AcceptedTcpLimits {
                        max_sockets: NonZeroUsize::new(4096).unwrap(),
                        max_tasks_per_socket: NonZeroUsize::new(1).unwrap(),
                        max_resources_per_socket: 0,
                    }),
                    max_bind_attempts: NonZeroUsize::new(50).unwrap(),
                }];
                #[cfg(feature = "managed-datagram-runtime")]
                if options.udp {
                    resources.push(ResourceDeclaration::DatagramListener {
                        accepted: Some(peer_limits()),
                        destination: options.listen.clone(),
                        max_bind_attempts: NonZeroUsize::new(50).unwrap(),
                        max_pending_sends: NonZeroUsize::new(32).unwrap(),
                    });
                }
                resources
            },
            task_limit: None,
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let (owner, source) = tcp::source(
            options.listen.clone(),
            context.resources.clone(),
            context.cancel.clone(),
            {
                let target = options.target.clone();
                move |socket| Connection::new(socket, target.clone())
            },
        );
        #[cfg(feature = "managed-datagram-runtime")]
        if options.udp {
            let (udp_owner, udp) = zero_platform_tokio::contract::inbound::udp::source(
                options.listen.clone(),
                context.resources,
                context.cancel,
                peer_limits(),
                std::time::Duration::from_secs(120),
                move |peer| datagram::Connection::new(peer, options.target.clone()),
            );
            return Ok(ComponentInstance {
                lifecycle: erase_lifecycle(PairOwner {
                    tcp: owner,
                    udp: udp_owner,
                }),
                roles: ComponentRoles {
                    inbound: Some(erase_inbound(PairSource { tcp: source, udp })),
                    ..Default::default()
                },
            });
        }
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(owner),
            roles: ComponentRoles {
                inbound: Some(erase_inbound(source)),
                ..Default::default()
            },
        })
    }
}
struct Connection {
    io: TcpRelayStream,
    facts: InboundConnectionFacts,
    target: Option<Destination>,
}
impl Connection {
    fn new(socket: TokioSocket, target: Option<Destination>) -> Self {
        Self {
            facts: InboundConnectionFacts::from_stream(&socket),
            io: socket.into(),
            target,
        }
    }
}
impl InboundHandshake for Connection {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        // A missing fixed target historically closes quietly. Mint no business
        // identity or fake destination for that empty connection.
        let target = self
            .target
            .ok_or_else(|| ChannelError::new(Stage::Handshake, Reason::Revoked, Impact::Session))?;
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

#[cfg(feature = "managed-datagram-runtime")]
mod datagram;
#[cfg(feature = "managed-datagram-runtime")]
fn peer_limits() -> zero_platform_tokio::contract::component::DatagramPeerLimits {
    zero_platform_tokio::contract::component::DatagramPeerLimits {
        max_tasks_per_peer: std::num::NonZeroUsize::new(1).unwrap(),
        max_resources_per_peer: 0,
        max_peers: NonZeroUsize::new(4096).unwrap(),
        packets_per_peer: NonZeroUsize::new(32).unwrap(),
    }
}
#[cfg(feature = "managed-datagram-runtime")]
struct PairSource<A, B> {
    tcp: A,
    udp: B,
}
#[cfg(feature = "managed-datagram-runtime")]
impl<A, B> zero_platform_tokio::contract::inbound::InboundSource for PairSource<A, B>
where
    A: zero_platform_tokio::contract::inbound::InboundSource<
        Handshake = zero_platform_tokio::contract::inbound::PendingInbound,
    >,
    B: zero_platform_tokio::contract::inbound::InboundSource<
        Handshake = zero_platform_tokio::contract::inbound::PendingInbound,
    >,
{
    type Handshake = zero_platform_tokio::contract::inbound::PendingInbound;
    async fn next(
        &mut self,
        context: zero_platform_tokio::contract::context::AcceptContext,
    ) -> Result<Option<Self::Handshake>, ChannelError> {
        tokio::select! { result = self.tcp.next(context.clone()) => result, result = self.udp.next(context) => result }
    }
}
#[cfg(feature = "managed-datagram-runtime")]
struct PairOwner {
    tcp: zero_platform_tokio::contract::inbound::tcp::TcpSourceLifecycle,
    udp: zero_platform_tokio::contract::inbound::udp::UdpSourceLifecycle,
}
#[cfg(feature = "managed-datagram-runtime")]
impl zero_platform_tokio::contract::lifecycle::ResourceLifecycle for PairOwner {
    async fn start(&mut self, deadline: tokio::time::Instant) -> Result<(), BuildError> {
        self.tcp.start(deadline).await?;
        self.udp.start(deadline).await
    }
    fn revoke(&self, reason: zero_platform_tokio::contract::CloseReason) {
        self.tcp.revoke(reason);
        self.udp.revoke(reason);
    }
    async fn shutdown(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<zero_platform_tokio::contract::lifecycle::CloseReport, BuildError> {
        let mut report = self.udp.shutdown(deadline).await?;
        report.append(self.tcp.shutdown(deadline).await?);
        Ok(report)
    }
}
