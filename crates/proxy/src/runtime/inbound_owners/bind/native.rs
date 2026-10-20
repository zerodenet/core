//! One native holder for the canonical listener declaration, never two budgets.
use super::*;
use zero_platform_tokio::{
    contract::{
        component::ResourceDeclaration,
        lifecycle::{erase_lifecycle, DynResourceLifecycle},
        ChannelError,
    },
    ControlledTcpListener,
};
#[cfg(feature = "datagram-route-runtime")]
mod udp;

pub(crate) enum NativeListenerView {
    Tcp(ControlledTcpListener),
    #[cfg(feature = "datagram-route-runtime")]
    Datagram(zero_platform_tokio::contract::component::GrantedDatagramListener),
}
impl NativeListenerView {
    pub(crate) fn into_bound(self) -> Result<crate::protocol_registry::BoundInbound, ChannelError> {
        use crate::protocol_registry::BoundInbound;
        match self {
            Self::Tcp(view) => {
                zero_platform_tokio::TokioListener::from_controlled(view).map(BoundInbound::Tcp)
            }
            #[cfg(feature = "datagram-route-runtime")]
            Self::Datagram(view) => Ok(BoundInbound::ControlledDatagram(view)),
        }
    }
    #[cfg(test)]
    pub(super) fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        match self {
            Self::Tcp(view) => view.local_addr(),
            #[cfg(feature = "datagram-route-runtime")]
            Self::Datagram(view) => view.rx.local_addr(),
        }
    }
}

pub(super) enum NativeBindGrant {
    Tcp(Arc<grant::NativeTcpGrant>),
    #[cfg(feature = "datagram-route-runtime")]
    Datagram(Arc<udp::NativeDatagramGrant>),
}
impl NativeBindGrant {
    pub(super) fn new(
        identity: ComponentInstanceId,
        declaration: &ResourceDeclaration,
        address: std::net::SocketAddr,
        attempts: NonZeroUsize,
        cancel: CancellationToken,
    ) -> Result<Self, ChannelError> {
        match declaration {
            ResourceDeclaration::TcpListener { .. } => {
                grant::NativeTcpGrant::new(identity, declaration, address, attempts, cancel)
                    .map(Arc::new)
                    .map(Self::Tcp)
            }
            #[cfg(feature = "datagram-route-runtime")]
            ResourceDeclaration::DatagramListener { .. } => {
                udp::NativeDatagramGrant::new(identity, declaration, address, attempts, cancel)
                    .map(Arc::new)
                    .map(Self::Datagram)
            }
            _ => Err(ChannelError::new(
                zero_platform_tokio::contract::Stage::Prepare,
                zero_platform_tokio::contract::Reason::Unsupported,
                zero_platform_tokio::contract::Impact::Component,
            )),
        }
    }
    pub(super) fn resource(&self) -> Box<dyn DynResourceLifecycle> {
        match self {
            Self::Tcp(grant) => erase_lifecycle(grant::TcpResource(grant.clone())),
            #[cfg(feature = "datagram-route-runtime")]
            Self::Datagram(grant) => erase_lifecycle(udp::DatagramResource(grant.clone())),
        }
    }
    pub(super) fn bind(&self) -> Result<NativeListenerView, EngineError> {
        match self {
            Self::Tcp(grant) => grant.bind().map(NativeListenerView::Tcp),
            #[cfg(feature = "datagram-route-runtime")]
            Self::Datagram(grant) => grant.bind().map(NativeListenerView::Datagram),
        }
    }
    #[cfg(test)]
    pub(super) fn bind_tcp(&self) -> Result<ControlledTcpListener, EngineError> {
        match self {
            Self::Tcp(grant) => grant.bind(),
            #[cfg(feature = "datagram-route-runtime")]
            Self::Datagram(_) => Err(io("TCP bind requested from a datagram declaration")),
        }
    }
}
