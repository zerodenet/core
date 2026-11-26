//! Closed Rust variants name actual installed owners, never erased payloads.
use super::*;
type Xhttp = super::super::mixed::factory::Shared;
type Browser = crate::component::browser::factory::Shared;
#[derive(Clone)]
pub(super) enum WeakOwner {
    Packet(PacketMetadataWeak<entry::Generation>),
    Xhttp(PacketMetadataWeak<Xhttp>),
    Browser(PacketMetadataWeak<Browser>),
}
pub(super) enum Owner {
    Packet(PacketMetadataArc<entry::Generation>),
    Xhttp(PacketMetadataArc<Xhttp>),
    Browser(PacketMetadataArc<Browser>),
}
impl WeakOwner {
    pub fn same_target(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Packet(a), Self::Packet(b)) => PacketMetadataWeak::ptr_eq(a, b),
            (Self::Xhttp(a), Self::Xhttp(b)) => PacketMetadataWeak::ptr_eq(a, b),
            (Self::Browser(a), Self::Browser(b)) => PacketMetadataWeak::ptr_eq(a, b),
            _ => false,
        }
    }
    pub fn upgrade(&self) -> Option<Owner> {
        match self {
            Self::Packet(g) => g.upgrade().map(Owner::Packet),
            Self::Xhttp(g) => g.upgrade().map(Owner::Xhttp),
            Self::Browser(g) => g.upgrade().map(Owner::Browser),
        }
    }
}
impl Owner {
    pub fn downgrade(&self) -> WeakOwner {
        match self {
            Self::Packet(g) => WeakOwner::Packet(PacketMetadataArc::downgrade(g)),
            Self::Xhttp(g) => WeakOwner::Xhttp(PacketMetadataArc::downgrade(g)),
            Self::Browser(g) => WeakOwner::Browser(PacketMetadataArc::downgrade(g)),
        }
    }
    pub fn instance(&self) -> ComponentInstanceId {
        match self {
            Self::Packet(g) => g.instance,
            Self::Xhttp(g) => g.instance,
            Self::Browser(g) => g.handle.instance(),
        }
    }
    pub fn closed_marker(&self) -> PacketMetadataArc<std::sync::atomic::AtomicBool> {
        match self {
            Self::Packet(g) => g.closed.clone(),
            Self::Xhttp(g) => g.closed.clone(),
            Self::Browser(g) => g.handle.closed_marker().clone(),
        }
    }
    pub fn closed(&self) -> bool {
        let marker = match self {
            Self::Packet(g) => &g.closed,
            Self::Xhttp(g) => &g.closed,
            Self::Browser(g) => g.handle.closed_marker(),
        };
        marker.load(std::sync::atomic::Ordering::Acquire)
    }
    pub fn cancelled(&self) -> bool {
        match self {
            Self::Packet(g) => g.cancel.is_cancelled(),
            Self::Xhttp(g) => g.cancel.is_cancelled(),
            Self::Browser(g) => g.handle.is_cancelled(),
        }
    }
    pub fn upstream(&self) -> &zero_platform_tokio::contract::network::UpstreamTransport {
        match self {
            Self::Packet(g) => &g.upstream,
            Self::Xhttp(g) => g.carrier_upstream(),
            Self::Browser(g) => g.handle.upstream(),
        }
    }
    pub fn memory(
        &self,
        layout: zero_traits::packet_metadata::PacketMetadataLayout,
    ) -> Result<zero_traits::packet_metadata::PacketMetadataCredit, ChannelError> {
        match self {
            Self::Packet(g) => g.memory(layout),
            Self::Xhttp(g) => g
                .memory
                .reserve(layout)
                .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source)),
            Self::Browser(g) => g
                .handle
                .memory()
                .reserve(layout)
                .map_err(|source| error(Stage::Prepare, Reason::Capacity).with_source(source)),
        }
    }
    pub async fn open(
        &self,
        context: &OpenContext,
    ) -> Result<crate::component::preconnect::Opened, ChannelError> {
        match self {
            Self::Packet(g) => TransportCarrier::open_original(g, context).await,
            Self::Xhttp(g) => g.acquire_transport(context).await,
            Self::Browser(g) => Browser::acquire_transport(g, context).await,
        }
    }
}
impl TransportCarrier {
    pub(in crate::component) fn install_xhttp(
        &self,
        owner: &PacketMetadataArc<Xhttp>,
    ) -> Result<(), ChannelError> {
        self.install_owner(Owner::Xhttp(owner.clone()))
    }
    pub(in crate::component) fn install_browser(
        &self,
        owner: &PacketMetadataArc<Browser>,
    ) -> Result<(), ChannelError> {
        self.install_owner(Owner::Browser(owner.clone()))
    }
}
