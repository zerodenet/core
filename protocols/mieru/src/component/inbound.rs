//! Standard Mieru TCP/UDP underlays: original cipher driver and real logical roles.
mod datagram;
mod request;
use crate::inbound::MieruInboundProfile;
use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash, Hasher},
    num::NonZeroUsize,
    sync::{Arc, OnceLock},
};
use zero_core::request::Destination;
use zero_platform_tokio::{
    contract::{
        component::*,
        context::{HandshakeContext, InboundConnectionFacts},
        inbound::*,
        lifecycle::{erase_lifecycle, BuildError},
        scope::TaskClass,
        CancellationToken, ChannelError, Impact, Reason, Stage,
    },
    TcpRelayStream, TokioSocket,
};
pub struct Options {
    source_limits: InboundSourceLimits,

    udp: bool,
    listen: Destination,
    profile: MieruInboundProfile,
    identity: Arc<[u8]>,
    work_tasks: NonZeroUsize,
    work_resources: usize,
}
impl Options {
    pub fn source_limits(&self) -> InboundSourceLimits {
        self.source_limits
    }
    pub fn with_inbound_source_limits(
        mut self,
        limits: InboundSourceLimits,
    ) -> Result<Self, zero_platform_tokio::contract::ChannelError> {
        let limits = limits.validate()?;
        let used = if self.udp {
            vec![
                limits.bind_attempts,
                limits.pending_sends,
                limits.datagram_peers,
                limits.packets_per_peer,
            ]
        } else {
            vec![limits.accepted_sockets, limits.bind_attempts]
        };
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"actual-source-limits-v1");
        for value in used {
            identity.extend_from_slice(&value.get().to_le_bytes());
        }
        self.identity = identity.into();
        self.source_limits = limits;
        Ok(self)
    }

    fn peer_limits(&self) -> DatagramPeerLimits {
        let mut limits = peer_limits();
        limits.max_peers = self.source_limits.datagram_peers;
        limits.packets_per_peer = self.source_limits.packets_per_peer;
        limits.max_tasks_per_peer = self.work_tasks;
        limits.max_resources_per_peer = self.work_resources;
        limits
    }

    /// Cold limits on this same original accepted/peer graph. No work starts.
    pub fn with_inbound_work_limits(
        mut self,
        tasks: NonZeroUsize,
        resources: usize,
        requests: NonZeroUsize,
    ) -> Result<Self, zero_platform_tokio::contract::ChannelError> {
        if tasks.get() > 4096 || resources > 65536 || requests.get() > 65536 {
            return Err(zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::InvalidConfig,
                zero_platform_tokio::contract::Impact::Component,
            ));
        }
        let mut identity = self.identity.to_vec();
        identity.extend_from_slice(b"actual-inbound-work-limits-v1");
        for n in [tasks.get(), resources, requests.get()] {
            identity.extend_from_slice(&n.to_le_bytes());
        }
        self.identity = identity.into();
        self.work_tasks = tasks;
        self.work_resources = resources;
        Ok(self)
    }

    pub fn new(
        listen: Destination,
        udp: bool,
        profile: MieruInboundProfile,
        private_identity: &[u8],
    ) -> Result<Self, ChannelError> {
        profile.validate_options().map_err(error)?;
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = Vec::new();
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut hash = key.build_hasher();
            listen.hash(&mut hash);
            private_identity.hash(&mut hash);
            identity.extend_from_slice(&hash.finish().to_le_bytes());
        }
        Ok(Self {
            udp,
            listen,
            profile,
            identity: identity.into(),
            work_tasks: NonZeroUsize::new(514).unwrap(),
            work_resources: 257,
            source_limits: InboundSourceLimits::default(),
        })
    }
}
pub struct Factory;
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Inbound],
            dependencies: vec![],
            static_identity: options.identity.clone(),
            task_limit: None,
            resources: vec![if options.udp {
                ResourceDeclaration::DatagramListener {
                    destination: options.listen.clone(),
                    max_bind_attempts: options.source_limits.bind_attempts,
                    max_pending_sends: options.source_limits.pending_sends,
                    accepted: Some(options.peer_limits()),
                }
            } else {
                ResourceDeclaration::TcpListener {
                    destination: options.listen.clone(),
                    max_bind_attempts: options.source_limits.bind_attempts,
                    accepted: Some(AcceptedTcpLimits {
                        max_sockets: options.source_limits.accepted_sockets,
                        max_tasks_per_socket: options.work_tasks,
                        max_resources_per_socket: options.work_resources,
                    }),
                }
            }],
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        if options.udp {
            let (lifecycle, source) = udp::source_events(
                options.listen.clone(),
                context.resources,
                context.cancel,
                options.peer_limits(),
                std::time::Duration::from_secs(300),
                move |peer| PacketConnection {
                    peer,
                    profile: options.profile.clone(),
                },
                |packet: &[u8], _| {
                    if packet.len() > crate::transport::MieruInboundListenerRequest::MAX_PACKET_SIZE
                    {
                        return Err(ChannelError::new(
                            Stage::Receive,
                            Reason::Protocol,
                            Impact::Datagram,
                        ));
                    }
                    Ok(None)
                },
            );
            Ok(ComponentInstance {
                lifecycle: erase_lifecycle(lifecycle),
                roles: ComponentRoles {
                    inbound: Some(erase_inbound(source)),
                    ..Default::default()
                },
            })
        } else {
            let (lifecycle, source) = tcp::source(
                options.listen.clone(),
                context.resources,
                context.cancel,
                move |socket| Connection {
                    socket,
                    profile: options.profile.clone(),
                },
            );
            Ok(ComponentInstance {
                lifecycle: erase_lifecycle(lifecycle),
                roles: ComponentRoles {
                    inbound: Some(erase_inbound(source)),
                    ..Default::default()
                },
            })
        }
    }
}
struct Connection {
    socket: TokioSocket,
    profile: MieruInboundProfile,
}
impl InboundHandshake for Connection {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        let caller = context
            .accepted
            .as_ref()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Session))?;
        let facts = InboundConnectionFacts::from_stream(&self.socket);
        let (connection, driver) = self
            .profile
            .prepare_multiplexer(zero_transport::MeteredStream::new(TcpRelayStream::from(
                self.socket,
            )))
            .await
            .map_err(error)?;
        let stop = context.cancel.child_token();
        let stopping = stop.clone();
        caller.spawn(TaskClass::Auxiliary, move |_| async move {
            let _stop_on_exit=stopping.clone().drop_guard();
            tokio::select! { biased; _=stopping.cancelled()=>Ok(()), result=driver=>result.map_err(error) }
        })?;
        Ok(AcceptedRequest::Multiplex(Box::new(request::Source::new(
            connection, facts, stop,
        ))))
    }
}
pub(super) fn error(source: impl std::error::Error + Send + Sync + 'static) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session).with_source(source)
}

fn peer_limits() -> DatagramPeerLimits {
    DatagramPeerLimits {
        max_peers: NonZeroUsize::new(128).unwrap(),
        packets_per_peer: NonZeroUsize::new(256).unwrap(),
        max_tasks_per_peer: NonZeroUsize::new(514).unwrap(),
        max_resources_per_peer: 257,
    }
}
struct PacketConnection {
    peer: udp::DatagramPeer,
    profile: MieruInboundProfile,
}
impl InboundHandshake for PacketConnection {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        let caller = context
            .accepted
            .as_ref()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Session))?;
        if caller.origin()? != zero_platform_tokio::AcceptedOrigin::Datagram {
            return Err(ChannelError::new(
                Stage::Prepare,
                Reason::StalePlan,
                Impact::Session,
            ));
        }
        let facts = self.peer.connection_facts().map_err(error)?;
        let (connection, driver) = self
            .profile
            .prepare_packet_peer(self.peer)
            .await
            .map_err(error)?;
        let stop = context.cancel.child_token();
        let stopping = stop.clone();
        caller.spawn(TaskClass::Auxiliary, move |_| async move {
            let _stop_on_exit = stopping.clone().drop_guard();
            tokio::select! { biased; _=stopping.cancelled()=>Ok(()), result=driver=>result.map_err(error) }
        })?;
        Ok(AcceptedRequest::Multiplex(Box::new(request::Source::new(
            connection, facts, stop,
        ))))
    }
}
