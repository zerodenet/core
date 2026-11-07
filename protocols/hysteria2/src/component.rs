//! Protocol-owned standard inbound assembly; resource acquisition stays hosted.
use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hasher},
    num::NonZeroUsize,
    sync::{Arc, Mutex, OnceLock},
};
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    component::*,
    inbound::*,
    lifecycle::{erase_lifecycle, BuildError},
};
mod connection;
mod datagram;
mod source;
mod web;
mod website;
use crate::transport::{Hysteria2AuthenticatedInboundProfile, Hysteria2InboundBindPlan};
#[path = "component/limits.rs"]
mod limits;
pub struct Options {
    listen: std::net::SocketAddr,
    server: quinn::ServerConfig,
    masks: Vec<zero_transport::finalmask::udp::Mask>,
    profile: Hysteria2AuthenticatedInboundProfile,
    store: crate::inbound::Hysteria2InboundProfileStore,
    tag: String,
    sites: Vec<(Destination, crate::transport::website::Hysteria2Website)>,
    identity: Arc<[u8]>,
    metadata_probes: NonZeroUsize,
    metadata_attempts: NonZeroUsize,
    metadata_sends: NonZeroUsize,
    quic_connections: NonZeroUsize,
    quic_users: NonZeroUsize,
}
impl Options {
    #[expect(
        clippy::too_many_arguments,
        reason = "existing cold options keep native bind, authentication, website and immutable identity explicit"
    )]
    pub fn new(
        listen: std::net::SocketAddr,
        bind: Hysteria2InboundBindPlan,
        profile: Hysteria2AuthenticatedInboundProfile,
        store: crate::inbound::Hysteria2InboundProfileStore,
        tag: &str,
        http: Option<Destination>,
        https: Option<Destination>,
        force_https: bool,
        private_identity: &[u8],
    ) -> Result<Self, zero_transport::RuntimeError> {
        static KEYS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let mut identity = Vec::new();
        for key in KEYS.get_or_init(|| [RandomState::new(), RandomState::new()]) {
            let mut h = key.build_hasher();
            h.write(private_identity);
            identity.extend(h.finish().to_le_bytes());
        }
        let website = crate::transport::website::Hysteria2Website::new(
            profile.masquerade.clone(),
            listen.port(),
            if force_https {
                Some(
                    https
                        .as_ref()
                        .ok_or_else(|| std::io::Error::other("HTTPS redirect listener missing"))?
                        .port,
                )
            } else {
                None
            },
        );
        let mut sites = Vec::new();
        if let Some(http) = http {
            sites.push((http, website.clone()));
        }
        if let Some(https) = https {
            sites.push((https, website.with_tls(&bind)?));
        }
        Ok(Self {
            listen,
            server: bind.server_config()?,
            masks: bind.masks(),
            profile,
            store,
            tag: tag.into(),
            sites,
            identity: identity.into(),
            metadata_probes: NonZeroUsize::new(2).unwrap(),
            metadata_attempts: NonZeroUsize::new(8).unwrap(),
            metadata_sends: NonZeroUsize::new(2).unwrap(),
            quic_connections: NonZeroUsize::new(8192).unwrap(),
            quic_users: NonZeroUsize::new(65536).unwrap(),
        })
    }
}
pub struct Factory;
fn n(v: usize) -> NonZeroUsize {
    NonZeroUsize::new(v).unwrap()
}
impl ComponentFactory for Factory {
    type Options = Options;
    fn describe(&self, options: &Options) -> ComponentSpec {
        let profile = &options.profile;
        let mut resources = vec![
            ResourceDeclaration::MetadataDatagram {
                source: options.listen,
                limits: options.metadata_limits(),
                max_bind_attempts: n(1),
            },
            ResourceDeclaration::DatagramAssociations {
                max_associations: n(4096),
                max_tasks_per_association: n(520),
                max_resources_per_association: 257,
            },
        ];
        for (destination, _) in &options.sites {
            resources.push(ResourceDeclaration::TcpListener {
                destination: destination.clone(),
                accepted: Some(AcceptedTcpLimits {
                    max_sockets: n(4096),
                    max_tasks_per_socket: n(520),
                    max_resources_per_socket: 257,
                }),
                max_bind_attempts: n(50),
            });
        }
        if let Some(root) = profile.masquerade.file_root() {
            resources.push(ResourceDeclaration::FileTree {
                root,
                max_sessions: n(16),
            });
        }
        if let Some(endpoint) = profile.masquerade.endpoint() {
            resources.push(ResourceDeclaration::InboundTargets {
                endpoints: vec![endpoint],
                max_sockets: n(4096),
            });
        }
        ComponentSpec {
            owner: ResourceOwner::Component,
            roles: vec![RoleKind::Inbound],
            dependencies: vec![],
            static_identity: options.metadata_identity(),
            task_limit: Some(n(options.quic_drivers())),
            resources,
        }
    }
    fn build(
        &self,
        options: Arc<Options>,
        context: BuildContext,
    ) -> Result<ComponentInstance, BuildError> {
        let mut profile = options.profile.clone();
        profile.protocol = options.store.publish(&options.tag, &profile.protocol);
        let resources = context.resources.clone();
        let (owner, source) = zero_transport::quic::source::source(
            MetadataDatagramRequest {
                ordinal: 0,
                source: options.listen,
                limits: options.metadata_limits(),
            },
            options.server.clone(),
            options.masks.clone(),
            zero_transport::quic::owned::OwnedEndpointLimits {
                drivers: n(options.quic_drivers()),
                users: options.quic_users,
            },
            context.resources.clone(),
            context.tasks,
            context.cancel.clone(),
            move |incoming| connection::Incoming {
                incoming,
                profile: profile.clone(),
                resources: resources.clone(),
            },
        );
        let mut owners: Vec<
            Box<dyn zero_platform_tokio::contract::lifecycle::DynResourceLifecycle>,
        > = vec![erase_lifecycle(owner)];
        let mut sources = vec![erase_inbound(source)];
        for (destination, site) in &options.sites {
            let site = site.clone();
            let resources = context.resources.clone();
            let (owner, source) = tcp::source(
                destination.clone(),
                context.resources.clone(),
                context.cancel.clone(),
                move |socket| website::Incoming {
                    socket,
                    site: site.clone(),
                    resources: resources.clone(),
                },
            );
            owners.push(erase_lifecycle(owner));
            sources.push(erase_inbound(source));
        }
        Ok(ComponentInstance {
            lifecycle: erase_lifecycle(source::Lifecycle { owners }),
            roles: ComponentRoles {
                inbound: Some(erase_inbound(source::Group { sources })),
                ..Default::default()
            },
        })
    }
}
