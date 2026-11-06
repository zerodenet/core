//! Native TCP/UDP source over one listener profile, replay authority and host grants.
use crate::inbound::{PreparedInboundProfile, ShadowsocksInboundTcpAcceptor};
use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash, Hasher},
    num::NonZeroUsize,
    sync::{Arc, Mutex, OnceLock},
};
use zero_core::{request::Destination, ProtocolType};
use zero_platform_tokio::{
    contract::{
        component::*,
        context::{HandshakeContext, InboundConnectionFacts},
        inbound::*,
        lifecycle::{erase_lifecycle, BuildError},
        ChannelError, Impact, Reason, Stage,
    },
    TcpRelayStream, TokioSocket,
};
mod datagram;
mod plugin;
pub struct Options {
    listen: Destination,
    profile: PreparedInboundProfile,
    plugin: Option<crate::validation::PluginConfig>,
    identity: Arc<[u8]>,
}
impl Options {
    pub fn new(
        listen: Destination,
        profile: PreparedInboundProfile,
        private_identity: &[u8],
    ) -> Self {
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = Vec::new();
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut hash = key.build_hasher();
            listen.hash(&mut hash);
            private_identity.hash(&mut hash);
            profile.legacy_replay_budget_identity().hash(&mut hash);
            #[cfg(feature = "blake3")]
            {
                profile.replay_total().hash(&mut hash);
                profile.udp_budget_identity().hash(&mut hash);
            }
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        Self {
            listen,
            profile,
            plugin: None,
            identity: identity.into(),
        }
    }
    pub fn with_plugin(mut self, plugin: Option<crate::validation::PluginConfig>) -> Self {
        self.plugin = plugin;
        self
    }
}
pub struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let mut spec = ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Inbound],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            task_limit: NonZeroUsize::new(usize::from(options.profile.needs_replay_task())),
            resources: vec![
                ResourceDeclaration::TcpListener {
                    destination: options.listen.clone(),
                    max_bind_attempts: NonZeroUsize::new(50).unwrap(),
                    accepted: Some(AcceptedTcpLimits {
                        max_sockets: NonZeroUsize::new(4096).unwrap(),
                        max_tasks_per_socket: NonZeroUsize::new(1).unwrap(),
                        max_resources_per_socket: 0,
                    }),
                },
                ResourceDeclaration::DatagramListener {
                    destination: options.listen.clone(),
                    max_bind_attempts: NonZeroUsize::new(50).unwrap(),
                    max_pending_sends: NonZeroUsize::new(32).unwrap(),
                    accepted: Some(peer_limits()),
                },
            ],
        };
        if let Some(plugin) = &options.plugin {
            spec.task_limit =
                NonZeroUsize::new(1 + usize::from(options.profile.needs_replay_task()));
            spec.resources.push(ResourceDeclaration::ListenerHandoff {
                stream: plugin.mode.tcp(),
                datagram: plugin.mode.udp(),
                max_probe_attempts: NonZeroUsize::new(1000).unwrap(),
            });
            spec.resources.push(ResourceDeclaration::Process {
                program: plugin.command.clone().into(),
                max_starts: NonZeroUsize::new(1).unwrap(),
            });
        }
        spec
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        // Each Build receives the frozen complete credentials; failed starts
        // and rollback never reuse a consumed or hot-mutated cold recipe.
        let profile = options.profile.clone().activate();
        let plugin = options.plugin.clone().map(|config| {
            plugin::Owner::new(
                config,
                context.resources.clone(),
                context.tasks.clone(),
                context.cancel.clone(),
            )
        });
        let idle = profile.state_limits().udp_idle();
        let acceptor = ShadowsocksInboundTcpAcceptor::new(profile.clone());
        #[cfg(feature = "blake3")]
        let replay = acceptor.replay_pool().map(|pool| {
            crate::component::replay::Owner::in_scope(
                pool,
                context.instance(),
                context.tasks.clone(),
                context.cancel.clone(),
            )
        });
        let security = profile.legacy_replay();
        let responder = Arc::new(Mutex::new(
            crate::udp::ShadowsocksInboundUdpResponder::from_profile(profile),
        ));
        let (tcp_owner, tcp) = tcp::source(
            options.listen.clone(),
            context.resources.clone(),
            context.cancel.clone(),
            move |socket| Connection {
                socket,
                acceptor: acceptor.clone(),
            },
        );
        let (udp_owner, udp) = udp::source_events(
            options.listen.clone(),
            context.resources,
            context.cancel,
            peer_limits(),
            idle,
            {
                let responder = responder.clone();
                move |peer| datagram::Connection::new(peer, responder.clone())
            },
            move |packet, sender| {
                let (dispatch, authentication) = responder
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .decode_authenticated_packet(packet, sender)
                    .map_err(|source| {
                        ChannelError::new(Stage::Receive, Reason::Protocol, Impact::Datagram)
                            .with_source(source)
                    })?;
                Ok(Some(InboundDatagramEvent::AuthenticatedPacket {
                    dispatch,
                    sender,
                    // Compatibility ingress accounting already charges the decoded payload.
                    protocol_bytes: 0,
                    authentication,
                }))
            },
        );
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(PairOwner {
                tcp: tcp_owner,
                udp: udp_owner,
                plugin,
                #[cfg(feature = "blake3")]
                replay,
                security,
            }),
            roles: ComponentRoles {
                inbound: Some(erase_inbound(PairSource { tcp, udp })),
                ..Default::default()
            },
        })
    }
}
fn peer_limits() -> DatagramPeerLimits {
    DatagramPeerLimits {
        max_tasks_per_peer: std::num::NonZeroUsize::new(1).unwrap(),
        max_resources_per_peer: 0,
        max_peers: NonZeroUsize::new(4096).unwrap(),
        packets_per_peer: NonZeroUsize::new(32).unwrap(),
    }
}
struct Connection {
    socket: TokioSocket,
    acceptor: ShadowsocksInboundTcpAcceptor,
}
impl InboundHandshake for Connection {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        let connection = InboundConnectionFacts::from_stream(&self.socket);
        let (destination, auth, client) = self
            .acceptor
            .accept_stream_facts(zero_transport::MeteredStream::new(TcpRelayStream::from(
                self.socket,
            )))
            .await
            .map_err(|source| {
                ChannelError::new(Stage::Handshake, Reason::Protocol, Impact::Session)
                    .with_source(source)
            })?;
        let facts = connection.request(
            &context,
            ProtocolType::new("shadowsocks"),
            Some(destination),
            Some(auth),
        )?;
        Ok(AcceptedRequest::Stream(accept_stream(
            super::TcpReply::new(client, facts)?,
        )))
    }
}
struct PairSource<A, B> {
    tcp: A,
    udp: B,
}
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
struct PairOwner {
    security: crate::shared::legacy_replay::LegacyReplay,
    #[cfg(feature = "blake3")]
    replay: Option<crate::component::replay::Owner>,
    plugin: Option<plugin::Owner>,
    tcp: zero_platform_tokio::contract::inbound::tcp::TcpSourceLifecycle,
    udp: zero_platform_tokio::contract::inbound::udp::UdpSourceLifecycle,
}
impl zero_platform_tokio::contract::lifecycle::ResourceLifecycle for PairOwner {
    async fn start(&mut self, deadline: tokio::time::Instant) -> Result<(), BuildError> {
        self.security
            .activate_security_budgets()
            .map_err(|source| {
                ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component)
                    .with_source(source)
            })?;
        #[cfg(feature = "blake3")]
        if let Some(replay) = &mut self.replay {
            replay.start(deadline).await?;
        }
        self.tcp.start(deadline).await?;
        self.udp.start(deadline).await?;
        if let Some(plugin) = &mut self.plugin {
            plugin.start(deadline).await?;
        }
        Ok(())
    }
    fn revoke(&self, reason: zero_platform_tokio::contract::CloseReason) {
        if let Some(plugin) = &self.plugin {
            plugin.revoke(reason);
        }
        self.tcp.revoke(reason);
        self.udp.revoke(reason);
        #[cfg(feature = "blake3")]
        if let Some(replay) = &self.replay {
            replay.revoke(reason);
        }
    }
    async fn shutdown(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<zero_platform_tokio::contract::lifecycle::CloseReport, BuildError> {
        let mut report = if let Some(plugin) = &mut self.plugin {
            plugin.shutdown(deadline).await?
        } else {
            Default::default()
        };
        report.append(self.udp.shutdown(deadline).await?);
        report.append(self.tcp.shutdown(deadline).await?);
        #[cfg(feature = "blake3")]
        if let Some(replay) = &mut self.replay {
            report.append(replay.shutdown(deadline).await?);
        }
        Ok(report)
    }
}
