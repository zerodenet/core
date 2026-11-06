//! Standard factory build over the same preinstalled native owner and Scope.
use super::*;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
};
use zero_core::{
    component::{ComponentRoleIds, InboundId},
    request::Destination,
};
use zero_platform_tokio::{
    ControlledTcpListener,
    contract::{
        BoxFuture, ChannelError, CloseReason, Impact, Reason, Stage,
        component::{
            BuildContext, DatagramPeerLimits, GrantedDatagramListener, GrantedMetadataListener,
            MetadataDatagramRequest, ResourceAccess, ResourceDeclaration,
        },
        inbound::DynInboundSource,
        lifecycle::{BuildError, CloseReport, ResourceLifecycle, erase_lifecycle},
        packet::BoxPacketSession,
    },
};

#[cfg(feature = "managed-datagram-runtime")]
mod datagram;
mod files;
mod targets;
#[cfg(feature = "managed-datagram-runtime")]
pub(in crate::runtime) use datagram::PeerSourceContext;
#[cfg(not(feature = "managed-datagram-runtime"))]
#[derive(Clone)]
pub(in crate::runtime) struct PeerSourceContext;

mod resources;
use resources::{BoundResources, Lifecycle};
impl InboundInstallContext {
    pub(in crate::runtime) fn build_inbound(
        &self,
        recipe: crate::component::ComponentRecipe,
        view: NativeListenerView,
        upstream: Option<crate::protocol_registry::UpstreamConnectServices>,
    ) -> Result<(Box<dyn DynInboundSource>, Option<PeerSourceContext>), EngineError> {
        let mut metadata = None;
        let mut handoff = None;
        let mut extra_tcp = Vec::new();
        let (listener, primary_udp): (
            Option<ControlledTcpListener>,
            Option<zero_platform_tokio::contract::component::GrantedDatagramListener>,
        ) = match view {
            NativeListenerView::Metadata(views) => {
                metadata = Some(views.resources);
                extra_tcp = views.tcp;
                (None, None)
            }
            NativeListenerView::Tcp(listener) => (Some(listener), None),
            NativeListenerView::Handoff { listener, grant } => {
                handoff = Some(grant);
                (Some(listener), None)
            }
            #[cfg(feature = "datagram-route-runtime")]
            NativeListenerView::Datagram(view) => (None, Some(view)),
        };
        let metadata_destination;
        let (destination, auxiliary) = match recipe.spec().resources.as_slice() {
            [
                ResourceDeclaration::MetadataDatagram { source, .. },
                auxiliary @ ..,
            ] if metadata.is_some() => {
                metadata_destination = Destination::new(
                    match source.ip() {
                        std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
                        std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
                    },
                    source.port(),
                );
                (&metadata_destination, auxiliary)
            }
            [
                ResourceDeclaration::TcpListener { destination, .. },
                auxiliary @ ..,
            ] => (destination, auxiliary),
            [
                ResourceDeclaration::DatagramListener { destination, .. },
                auxiliary @ ..,
            ] if primary_udp.is_some() => (destination, auxiliary),
            _ => return Err(std::io::Error::other(denied()).into()),
        };
        #[cfg(feature = "managed-datagram-runtime")]
        if let Some(ResourceDeclaration::DatagramAssociations {
            max_associations,
            max_tasks_per_association,
            max_resources_per_association,
        }) = auxiliary
            .iter()
            .find(|resource| matches!(resource, ResourceDeclaration::DatagramAssociations { .. }))
        {
            self.entry.install_datagram_continuity(
                self.protocols.clone(),
                *max_associations,
                *max_tasks_per_association,
                *max_resources_per_association,
            )?;
        }
        #[cfg(feature = "raw-ip-runtime")]
        if matches!(
            recipe.spec().resources.first(),
            Some(ResourceDeclaration::DatagramListener { accepted: None, .. })
        ) {
            let original = primary_udp
                .as_ref()
                .ok_or_else(|| std::io::Error::other(denied()))?
                .tx
                .identity();
            *self.entry.packet_source.lock().unwrap() = Some(
                crate::runtime::inbound_operation::raw_ip::PacketSourceContext::install(
                    self, original,
                )?,
            );
        }
        let identity = self.entry.identity();
        #[cfg(feature = "managed-datagram-runtime")]
        let udp = match auxiliary.iter().find(|r| {
            matches!(
                r,
                ResourceDeclaration::DatagramListener {
                    accepted: Some(_),
                    ..
                }
            )
        }) {
            Some(declaration) => Some(datagram::DatagramResource::new(
                identity.component,
                declaration,
                match &handoff {
                    Some(grant) => {
                        grant
                            .endpoints
                            .get()
                            .ok_or_else(|| std::io::Error::other(denied()))?
                            .datagram
                    }
                    None => listener
                        .as_ref()
                        .ok_or_else(|| std::io::Error::other(denied()))?
                        .local_addr()?,
                },
            )?),
            None => None,
        };
        let carrier = metadata
            .as_ref()
            .map(|_| Arc::new(std::sync::OnceLock::new()));
        #[cfg(feature = "managed-datagram-runtime")]
        let peer = if metadata.is_some() {
            let Some(ResourceDeclaration::DatagramAssociations {
                max_associations,
                max_tasks_per_association,
                max_resources_per_association,
            }) = auxiliary
                .iter()
                .find(|r| matches!(r, ResourceDeclaration::DatagramAssociations { .. }))
            else {
                return Err(std::io::Error::other(denied()).into());
            };
            Some(PeerSourceContext {
                identity: Arc::default(),
                limits: DatagramPeerLimits {
                    max_peers: *max_associations,
                    packets_per_peer: std::num::NonZeroUsize::new(1).unwrap(),
                    max_tasks_per_peer: *max_tasks_per_association,
                    max_resources_per_peer: *max_resources_per_association,
                },
                peers: self
                    .entry
                    .datagram_continuity()
                    .expect("declared association graph")
                    .group(),
                carrier: carrier.clone(),
            })
        } else if matches!(
            recipe.spec().resources.first(),
            Some(ResourceDeclaration::DatagramListener { accepted: None, .. })
        ) {
            None
        } else if let Some(primary) = &primary_udp {
            let ResourceDeclaration::DatagramListener {
                accepted: Some(limits),
                ..
            } = &recipe.spec().resources[0]
            else {
                return Err(std::io::Error::other(denied()).into());
            };
            let identity = Arc::new(std::sync::OnceLock::new());
            let _ = identity.set(primary.tx.identity());
            Some(PeerSourceContext {
                carrier: None,
                identity,
                limits: *limits,
                peers: self
                    .entry
                    .install_child_group(self.protocols.clone(), limits.max_peers)?,
            })
        } else {
            match (auxiliary, udp.as_ref()) {
                (
                    [
                        ResourceDeclaration::DatagramListener {
                            accepted: Some(limits),
                            ..
                        },
                        ..,
                    ],
                    Some(udp),
                ) => Some(PeerSourceContext {
                    carrier: None,
                    identity: udp.identity.clone(),
                    limits: *limits,
                    peers: self
                        .entry
                        .install_child_group(self.protocols.clone(), limits.max_peers)?,
                }),
                _ => None,
            }
        };
        #[cfg(not(feature = "managed-datagram-runtime"))]
        let peer = None;
        let target_declaration = auxiliary
            .iter()
            .find(|resource| matches!(resource, ResourceDeclaration::InboundTargets { .. }));
        let targets = target_declaration
            .map(|declaration| {
                targets::Targets::new(
                    declaration,
                    upstream
                        .ok_or_else(|| std::io::Error::other("inbound target services absent"))?,
                    self.entry.accepted_registrar().scope_identity(),
                    self.entry.task_access().cancellation(),
                )
            })
            .transpose()?;
        let tasks = if metadata.is_some() || recipe.spec().task_limit.is_some() {
            self.entry.accepted_registrar().clone()
        } else {
            self.entry.accepted_registrar().without_tasks()
        };
        let files = auxiliary
            .iter()
            .find(|r| matches!(r, ResourceDeclaration::FileTree { .. }))
            .map(|declaration| {
                files::FileTrees::new(
                    declaration,
                    self.entry.accepted_registrar().scope_identity(),
                    self.entry.task_access().cancellation(),
                )
            })
            .transpose()?;
        let process_declarations: Vec<_> = auxiliary
            .iter()
            .filter(|r| matches!(r, ResourceDeclaration::Process { .. }))
            .cloned()
            .collect();
        let process = if process_declarations.is_empty() {
            None
        } else {
            Some(
                crate::component::instance::HostResources::new_hosted(
                    identity,
                    &process_declarations,
                    self.entry.task_access().cancellation(),
                    self.entry.accepted_registrar().downgrade(),
                    self.protocols.runtime_host().original_budgets(),
                )
                .map_err(std::io::Error::other)?,
            )
        };
        let resources = Arc::new(BoundResources {
            #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
            packet_native: self.entry.packet_native.get().cloned(),
            handoff,
            process,
            extra_tcp: Mutex::new(extra_tcp),
            files,
            icmp_carrier: metadata.is_some()
                && auxiliary
                    .iter()
                    .any(|r| matches!(r, ResourceDeclaration::NativeIcmp { .. })),
            metadata,
            carrier,
            targets,
            #[cfg(feature = "managed-datagram-runtime")]
            udp,
            address: destination.clone(),
            listener: Mutex::new(listener),
            primary_udp: Mutex::new(primary_udp),
            starting: AtomicBool::new(false),
            cancel: self.entry.task_access().cancellation(),
            datagram: match auxiliary {
                [] => None,
                resources
                    if resources.iter().all(|resource| {
                        matches!(
                            resource,
                            ResourceDeclaration::DatagramAssociations { .. }
                                | ResourceDeclaration::InboundTargets { .. }
                                | ResourceDeclaration::NativeIcmp { .. }
                                | ResourceDeclaration::FileTree { .. }
                                | ResourceDeclaration::TcpListener { .. }
                        )
                    }) =>
                {
                    None
                }
                #[cfg(feature = "managed-datagram-runtime")]
                [
                    ResourceDeclaration::DatagramListener {
                        accepted: Some(_), ..
                    },
                    ..,
                ] => None,
                [declaration @ ResourceDeclaration::AcceptedDatagram { .. }] => Some(
                    zero_platform_tokio::contract::component::AcceptedDatagramAuthority::new(
                        declaration,
                        self.entry.accepted_registrar(),
                        self.entry.task_access().cancellation(),
                    )
                    .map_err(std::io::Error::other)?,
                ),
                _ => return Err(std::io::Error::other(denied()).into()),
            },
        });
        let erased: Arc<dyn ResourceAccess> = resources.clone();
        *self.entry.inbound_resources.lock().unwrap() = Some(Arc::downgrade(&erased));
        let mut instance = recipe
            .build(BuildContext {
                component: identity.component,
                generation: identity.generation,
                dependencies: Arc::from([]),
                roles: ComponentRoleIds {
                    inbound: Some(InboundId(identity.component.0)),
                    ..Default::default()
                },
                resources: resources.clone(),
                // It is the same actual Scope, with no protocol-internal task grant.
                tasks,
                cancel: self.entry.task_access().cancellation(),
                background_carrier: None,
            })
            .map_err(std::io::Error::other)?;
        let source = instance
            .roles
            .inbound
            .take()
            .ok_or_else(|| std::io::Error::other(denied()))?;
        // Protocol owner enters the retained original before any lifecycle start.
        self.entry.install_resource(
            identity,
            erase_lifecycle(Lifecycle {
                protocol: instance.lifecycle,
                resources,
            }),
        )?;
        Ok((source, peer))
    }
}
fn denied() -> ChannelError {
    ChannelError::new(Stage::Prepare, Reason::Unsupported, Impact::Component)
}

impl InboundOwner {
    pub(in crate::runtime) fn inbound_resources(
        &self,
    ) -> Option<std::sync::Weak<dyn ResourceAccess>> {
        self.inbound_resources.lock().unwrap().clone()
    }
}
