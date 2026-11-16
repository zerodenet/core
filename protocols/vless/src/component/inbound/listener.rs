//! Standard sources over original host-owned carriers and the same cache owner.
//! Cold credentials and task ownership remain inert until confirmed StopOld,
//! host Build and the installed lifecycle's bounded start phase.
#[path = "listener/build.rs"]
mod build;
#[path = "listener/maintenance.rs"]
mod maintenance;
#[path = "quic.rs"]
mod quic;
#[path = "listener/source_limits.rs"]
mod source_limits;
#[path = "listener/work.rs"]
mod work;
use crate::transport::PreparedVlessInboundListenerRequest;
use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hash, Hasher},
    num::NonZeroUsize,
    sync::{Arc, Mutex, OnceLock},
};
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    component::*,
    context::HandshakeContext,
    inbound::*,
    lifecycle::{erase_lifecycle, BuildError},
    ChannelError, Impact, Reason, Stage,
};
#[path = "listener/limits.rs"]
mod limits;
pub struct Options {
    source_limits: InboundSourceLimits,
    tls_material: Vec<Arc<zero_transport::tls::source::PreparedServer>>,

    listen: Destination,
    udp: bool,
    decryption_maintenance: bool,
    quic: Option<zero_transport::quic::QuicServerConfig>,
    packet_masks: Vec<zero_transport::finalmask::udp::Mask>,
    targets: Vec<zero_traits::FallbackEndpoint>,
    file_root: Option<std::path::PathBuf>,
    prepared: Mutex<Option<PreparedVlessInboundListenerRequest>>,
    identity: Arc<[u8]>,
    work_tasks: NonZeroUsize,
    work_resources: usize,
    metadata_probes: NonZeroUsize,
    metadata_attempts: NonZeroUsize,
    metadata_sends: NonZeroUsize,
    quic_connections: NonZeroUsize,
    quic_users: NonZeroUsize,
}
impl Options {
    pub fn new(
        listen: Destination,
        prepared: PreparedVlessInboundListenerRequest,
        private_identity: &[u8],
    ) -> Self {
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = Vec::new();
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut hasher = key.build_hasher();
            listen.hash(&mut hasher);
            prepared.uses_mkcp().hash(&mut hasher);
            private_identity.hash(&mut hasher);
            identity.extend_from_slice(&hasher.finish().to_le_bytes());
        }
        let work_tasks = NonZeroUsize::new(520 + prepared.session_tasks()).unwrap();
        let tls_material = prepared.tls_material();
        Self {
            tls_material,
            targets: prepared.target_endpoints(),
            file_root: prepared.file_root(),
            udp: prepared.uses_mkcp(),
            decryption_maintenance: prepared.requires_decryption_maintenance(),
            quic: None,
            packet_masks: if prepared.uses_mkcp() {
                prepared.masks()
            } else {
                Vec::new()
            },
            listen,
            prepared: Mutex::new(Some(prepared)),
            identity: identity.into(),
            work_tasks,
            work_resources: 257,
            source_limits: InboundSourceLimits::default(),
            metadata_probes: NonZeroUsize::new(2).unwrap(),
            metadata_attempts: NonZeroUsize::new(8).unwrap(),
            metadata_sends: NonZeroUsize::new(2).unwrap(),
            quic_connections: NonZeroUsize::new(8192).unwrap(),
            quic_users: NonZeroUsize::new(65536).unwrap(),
        }
    }
    pub fn with_quic(
        mut self,
        plan: crate::transport::VlessInboundBindPlan,
    ) -> Result<Self, zero_transport::RuntimeError> {
        if matches!(self.listen.address, zero_core::Address::Domain(_)) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "QUIC listener requires a literal endpoint",
            )
            .into());
        }
        self.prepared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .ok_or_else(|| std::io::Error::other("source already activated"))?
            .validate_standard_quic_source()
            .map_err(std::io::Error::other)?;
        self.packet_masks = plan.masks();
        zero_transport::finalmask::owned::validate(&self.packet_masks)?;
        let (server, material) = plan.server_config()?;
        self.tls_material.push(material);
        self.quic = Some(server);
        Ok(self)
    }
    fn masked_mkcp(&self) -> bool {
        self.udp
            && self.packet_masks.iter().any(|m| {
                matches!(
                    m,
                    zero_transport::finalmask::udp::Mask::Xdns { .. }
                        | zero_transport::finalmask::udp::Mask::Xicmp { .. }
                )
            })
    }
    fn internal_tasks(&self) -> Option<NonZeroUsize> {
        let carriers = if self.quic.is_some() {
            self.quic_drivers()
        } else if self.masked_mkcp() {
            zero_transport::finalmask::owned::driver_count(&self.packet_masks).max(1)
        } else {
            0
        };
        NonZeroUsize::new(carriers + usize::from(self.decryption_maintenance))
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
            static_identity: options.metadata_identity(),
            task_limit: options.internal_tasks(),
            resources: {
                let mut resources = vec![
                    if options.quic.is_some() || options.masked_mkcp() {
                        ResourceDeclaration::MetadataDatagram {
                            source: quic::endpoint(&options.listen),
                            limits: options.metadata_limits(),
                            max_bind_attempts: NonZeroUsize::new(1).unwrap(),
                        }
                    } else if options.udp {
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
                    },
                    ResourceDeclaration::DatagramAssociations {
                        max_associations: options.source_limits.datagram_associations,
                        max_tasks_per_association: options.work_tasks,
                        max_resources_per_association: options.work_resources,
                    },
                ];
                if options
                    .packet_masks
                    .iter()
                    .any(|m| matches!(m, zero_transport::finalmask::udp::Mask::Xicmp { .. }))
                {
                    resources.push(ResourceDeclaration::NativeIcmp {
                        max_sockets: NonZeroUsize::new(1).unwrap(),
                    });
                }
                if let Some(root) = &options.file_root {
                    resources.push(ResourceDeclaration::FileTree {
                        root: root.clone(),
                        max_sessions: options.source_limits.file_sessions,
                    });
                }
                if !options.targets.is_empty() {
                    resources.push(ResourceDeclaration::InboundTargets {
                        endpoints: options.targets.clone(),
                        max_sockets: options.source_limits.target_sockets,
                    });
                }
                resources
            },
        };
        zero_transport::tls::source::declare(&mut spec, &options.tls_material);
        spec
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        build::source(options, context)
    }
}
struct Connection {
    request: crate::transport::VlessInboundListenerRequest,
    socket: zero_platform_tokio::TokioSocket,
    resources: Arc<dyn ResourceAccess>,
}
impl InboundHandshake for Connection {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        let caller = context
            .accepted
            .clone()
            .ok_or_else(|| ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Session))?;
        let resources = self.resources;
        self.request
            .with_handshake_target_connector(zero_transport::handshake_target::Connector::new(
                move |endpoint| {
                    let resources = resources.clone();
                    let caller = caller.clone();
                    Box::pin(async move {
                        resources
                            .open_inbound_stream(caller, endpoint)
                            .await
                            .map_err(std::io::Error::other)
                    })
                },
            ))
            .accept_standard_request(self.socket, context)
            .await
    }
}

struct PacketConnection {
    request: crate::transport::VlessInboundListenerRequest,
    peer: udp::DatagramPeer,
}
impl InboundHandshake for PacketConnection {
    async fn complete(self, context: HandshakeContext) -> Result<AcceptedRequest, ChannelError> {
        self.request.accept_standard_mkcp_peer(self.peer, context)
    }
}

mod file_limit;
