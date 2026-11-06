//! Concrete native Packet-view transfer. No factory callbacks or close minting.
use crate::contract::{scope::TaskScopeIdentity, stream::StreamIo};
use std::{pin::Pin, sync::Arc};
use zero_core::component::ComponentInstanceId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PacketDeviceBinding {
    /// Duplicate this borrowed descriptor; never configure/close the supplied FD.
    Descriptor { fd: i32, interface: Arc<str> },
    /// Own the original created device before configuring its local address.
    Create {
        name: Option<Arc<str>>,
        addresses: Vec<(std::net::IpAddr, std::net::IpAddr)>,
        mtu: u16,
    },
    /// Borrow one actual existing Wintun adapter/session; never create on miss.
    ExistingWindows {
        interface: Arc<str>,
        addresses: Vec<std::net::IpAddr>,
        mtu: u16,
    },
}

pub type PacketDeviceIo = Pin<Box<dyn StreamIo + Sync>>;

/// Identity metadata only. Matching it cannot manufacture a native close ACK.
#[derive(Clone, Debug)]
pub struct PacketDeviceIdentity {
    instance: ComponentInstanceId,
    scope: TaskScopeIdentity,
    original: Arc<()>,
}
impl PacketDeviceIdentity {
    /// Concrete host provider construction, before any native acquisition.
    pub fn new(instance: ComponentInstanceId, scope: TaskScopeIdentity) -> Self {
        Self {
            instance,
            scope,
            original: Arc::new(()),
        }
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    pub fn scope(&self) -> &TaskScopeIdentity {
        &self.scope
    }
    pub fn same_provider(&self, other: &Self) -> bool {
        self.instance == other.instance
            && self.scope.same_scope(&other.scope)
            && Arc::ptr_eq(&self.original, &other.original)
    }
}
pub struct GrantedPacketDevice {
    pub io: PacketDeviceIo,
    pub identity: PacketDeviceIdentity,
    pub original: crate::controlled_carrier::PacketProviderRegistration,
    pub interface: Arc<str>,
    #[cfg(feature = "native-packet-origin")]
    pub native_read: zero_tun::packet_memory::NativeReadGrant,
}
