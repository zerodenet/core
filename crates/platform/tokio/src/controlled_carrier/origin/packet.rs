//! Concrete builtin Packet original; metadata alone has no constructor.
//! The optional native backend introduces no configurable device/origin SPI.
use super::*;
use crate::contract::{CancellationToken, component::PacketDeviceIdentity};
struct ActualPacket {
    device: PacketDeviceIdentity,
    native: NativePacket,
    driver: TaskJoinWitness,
    cancel: CancellationToken,
}
// Keep the public variants stable under dependency feature unification.
// Without the optional native backend this storage cannot be instantiated.
#[cfg(feature = "native-packet-origin")]
type NativePacket = zero_tun::OwnedTunIdentity;
#[cfg(not(feature = "native-packet-origin"))]
type NativePacket = std::convert::Infallible;
#[cfg(feature = "native-packet-origin")]
type NativeClosed = zero_tun::OwnedTunClosed;
#[cfg(not(feature = "native-packet-origin"))]
type NativeClosed = std::convert::Infallible;
#[derive(Clone)]
pub struct PacketProviderRegistration(Arc<ActualPacket>);
#[derive(Clone)]
pub struct PacketProviderClosed {
    owner: Weak<ActualPacket>,
    native: NativeClosed,
}
impl PacketProviderRegistration {
    #[cfg(feature = "native-packet-origin")]
    pub fn from_acquired(
        device: PacketDeviceIdentity,
        native: zero_tun::OwnedTunIdentity,
        driver: TaskJoinWitness,
        cancel: CancellationToken,
    ) -> Result<Self, ChannelError> {
        // No other owner callback is made here; the actual native host checks
        // live Scope/admission before acquire and matches this sealed object.
        if !device.scope().same_scope(&driver.scope_identity()) {
            return Err(error(
                device.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        Ok(Self(Arc::new(ActualPacket {
            device,
            native,
            driver,
            cancel,
        })))
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.0.device.instance()
    }
    pub fn device(&self) -> &PacketDeviceIdentity {
        &self.0.device
    }
    pub fn driver(&self) -> &TaskJoinWitness {
        &self.0.driver
    }
    pub fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn limit(&self) -> std::num::NonZeroUsize {
        std::num::NonZeroUsize::new(1).unwrap()
    }
    #[cfg(feature = "native-packet-origin")]
    pub fn native(&self) -> &zero_tun::OwnedTunIdentity {
        &self.0.native
    }
    pub fn closed(&self) -> Option<PacketProviderClosed> {
        #[cfg(feature = "native-packet-origin")]
        {
            Some(PacketProviderClosed {
                owner: Arc::downgrade(&self.0),
                native: self.0.native.closed()?,
            })
        }
        #[cfg(not(feature = "native-packet-origin"))]
        {
            match self.0.native {}
        }
    }
    pub fn is_retired(&self) -> bool {
        self.0.cancel.is_cancelled()
            || !self.0.device.scope().resource_open()
            || self.0.driver.is_joined()
            || self.closed().is_some()
    }
    pub async fn retired(&self) {
        if self.is_retired() {
            return;
        }
        tokio::select! { _ = self.0.cancel.cancelled() => {}, _ = self.0.driver.wait() => {} }
    }
}
impl PacketProviderClosed {
    pub fn matches(&self, owner: &PacketProviderRegistration) -> bool {
        if !Weak::ptr_eq(&self.owner, &Arc::downgrade(&owner.0)) {
            return false;
        }
        #[cfg(feature = "native-packet-origin")]
        {
            self.native.matches(&owner.0.native)
        }
        #[cfg(not(feature = "native-packet-origin"))]
        {
            match self.native {}
        }
    }
}
