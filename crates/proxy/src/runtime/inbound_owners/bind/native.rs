//! One native holder for the canonical listener declaration, never two budgets.
use super::*;
use zero_platform_tokio::{
    ControlledTcpListener,
    contract::{
        ChannelError,
        component::ResourceDeclaration,
        lifecycle::{DynResourceLifecycle, erase_lifecycle},
    },
};
pub(in crate::runtime::inbound_owners) mod handoff;
mod metadata;
mod tcp_group;
pub(in crate::runtime::inbound_owners) use tcp_group::MetadataViews;
#[cfg(feature = "datagram-route-runtime")]
pub(in crate::runtime::inbound_owners) mod udp;

pub(crate) enum NativeListenerView {
    Tcp(ControlledTcpListener),
    Handoff {
        listener: ControlledTcpListener,
        grant: Arc<handoff::Grant>,
    },
    Metadata(MetadataViews),
    #[cfg(feature = "datagram-route-runtime")]
    Datagram(zero_platform_tokio::contract::component::GrantedDatagramListener),
}
impl NativeListenerView {
    #[cfg(test)]
    pub(super) fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        match self {
            Self::Tcp(view) => view.local_addr(),
            Self::Handoff { listener, .. } => listener.local_addr(),
            Self::Metadata(_) => Err(std::io::Error::other(
                "metadata address is published by the actual source",
            )),
            #[cfg(feature = "datagram-route-runtime")]
            Self::Datagram(view) => view.rx.local_addr(),
        }
    }
}

pub(super) enum NativeBindGrant {
    Tcp(Arc<grant::NativeTcpGrant>),
    Handoff(Arc<handoff::Grant>),
    Metadata {
        resources: Arc<crate::component::instance::HostResources>,
        tcp: Vec<(zero_core::request::Destination, Arc<grant::NativeTcpGrant>)>,
    },
    #[cfg(feature = "datagram-route-runtime")]
    Datagram(Arc<udp::NativeDatagramGrant>),
}
impl NativeBindGrant {
    pub(super) fn new(
        identity: ComponentInstanceId,
        declarations: &[ResourceDeclaration],
        address: Option<std::net::SocketAddr>,
        attempts: NonZeroUsize,
        cancel: CancellationToken,
        registrar: &zero_platform_tokio::contract::scope::TaskRegistrar,
        originals: Arc<zero_runtime::HostOriginalBudgets>,
        owner: Weak<InboundOwner>,
    ) -> Result<Self, ChannelError> {
        let declaration = declarations.first().ok_or_else(|| {
            ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::InvalidConfig,
                zero_platform_tokio::contract::Impact::Component,
            )
        })?;
        if declarations
            .iter()
            .any(|d| matches!(d, ResourceDeclaration::ListenerHandoff { .. }))
        {
            return handoff::Grant::new(
                identity,
                declarations,
                address,
                attempts,
                cancel,
                registrar.clone(),
                owner,
            )
            .map(Arc::new)
            .map(Self::Handoff);
        }
        match declaration {
            ResourceDeclaration::MetadataDatagram { .. } => {
                let resources = crate::component::instance::HostResources::new_hosted(
                    identity,
                    &declarations
                        .iter()
                        .filter(|d| {
                            matches!(
                                d,
                                ResourceDeclaration::MetadataDatagram { .. }
                                    | ResourceDeclaration::NativeIcmp { .. }
                            )
                        })
                        .cloned()
                        .collect::<Vec<_>>(),
                    cancel.clone(),
                    registrar.downgrade(),
                    originals.clone(),
                )?;
                let tcp = tcp_group::prepare(identity, declarations, attempts, cancel, registrar)?;
                Ok(Self::Metadata { resources, tcp })
            }
            ResourceDeclaration::TcpListener { .. } => grant::NativeTcpGrant::new(
                identity,
                declaration,
                address,
                attempts,
                cancel,
                registrar,
            )
            .map(Arc::new)
            .map(Self::Tcp),
            #[cfg(feature = "datagram-route-runtime")]
            ResourceDeclaration::DatagramListener { .. } => udp::NativeDatagramGrant::new_scoped(
                identity,
                declaration,
                address,
                attempts,
                cancel,
                registrar.clone(),
            )
            .and_then(|grant| {
                grant
                    .with_original_budgets(originals)
                    .with_packet_reads(declarations)
            })
            .map(Arc::new)
            .map(Self::Datagram),
            _ => Err(ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::Unsupported,
                zero_platform_tokio::contract::Impact::Component,
            )),
        }
    }
    pub(super) fn tcp_resolutions(&self) -> Vec<&Arc<grant::NativeTcpGrant>> {
        match self {
            Self::Tcp(grant) if grant.needs_resolution() => vec![grant],
            Self::Handoff(grant) if grant.outer.needs_resolution() => vec![&grant.outer],
            Self::Metadata { tcp, .. } => tcp
                .iter()
                .filter_map(|(_, grant)| grant.needs_resolution().then_some(grant))
                .collect(),
            _ => vec![],
        }
    }
    pub(super) fn accepted_task_receiver(
        &self,
    ) -> Vec<zero_platform_tokio::AcceptedTcpTaskReceiver> {
        match self {
            Self::Tcp(grant) => grant.accepted_task_receiver().into_iter().collect(),
            Self::Handoff(grant) => grant.outer.accepted_task_receiver().into_iter().collect(),
            Self::Metadata { tcp, .. } => tcp
                .iter()
                .filter_map(|(_, grant)| grant.accepted_task_receiver())
                .collect(),
            #[cfg(feature = "datagram-route-runtime")]
            Self::Datagram(_) => vec![],
        }
    }
    pub(super) fn extra_resources(&self) -> Vec<Box<dyn DynResourceLifecycle>> {
        match self {
            Self::Metadata { tcp, .. } => tcp
                .iter()
                .map(|(_, grant)| erase_lifecycle(grant::TcpResource(grant.clone())))
                .collect(),
            _ => vec![],
        }
    }
    pub(super) fn resource(&self) -> Box<dyn DynResourceLifecycle> {
        match self {
            Self::Tcp(grant) => erase_lifecycle(grant::TcpResource(grant.clone())),
            Self::Handoff(grant) => erase_lifecycle(handoff::Owner(grant.clone())),
            Self::Metadata { resources, .. } => {
                erase_lifecycle(metadata::Resource(resources.clone()))
            }
            #[cfg(feature = "datagram-route-runtime")]
            Self::Datagram(grant) => erase_lifecycle(udp::DatagramResource(grant.clone())),
        }
    }
    pub(super) fn needs_packet_reads(&self) -> bool {
        #[cfg(feature = "datagram-route-runtime")]
        if let Self::Datagram(grant) = self {
            return grant.needs_packet_reads();
        }
        false
    }
    pub(super) fn bind(&self) -> Result<NativeListenerView, EngineError> {
        match self {
            Self::Tcp(grant) => grant.bind().map(NativeListenerView::Tcp),
            Self::Handoff(grant) => grant.bind(),
            Self::Metadata { resources, tcp } => {
                tcp_group::bind(resources.clone(), tcp).map(NativeListenerView::Metadata)
            }
            #[cfg(feature = "datagram-route-runtime")]
            Self::Datagram(grant) => grant.bind().map(NativeListenerView::Datagram),
        }
    }
    #[cfg(test)]
    pub(super) fn bind_tcp(&self) -> Result<ControlledTcpListener, EngineError> {
        match self {
            Self::Tcp(grant) => grant.bind(),
            Self::Handoff(_) => Err(io("TCP test promotion cannot discard handoff ownership")),
            Self::Metadata { .. } => Err(io("TCP bind requested from a metadata declaration")),
            #[cfg(feature = "datagram-route-runtime")]
            Self::Datagram(_) => Err(io("TCP bind requested from a datagram declaration")),
        }
    }
}
