//! Typed observation of the actual provider; no common fabricated registration.
use super::*;
use crate::controlled_datagram::metadata_pool::{
    MetadataPoolOwnerClosed, MetadataPoolRegistration,
};

#[derive(Clone, Copy)]
pub enum CarrierOriginOwner<'a> {
    Carrier(&'a CarrierOwnerRegistration),
    Packet(&'a PacketProviderRegistration),
    LogicalPacket(&'a LogicalPacketProviderRegistration),
    Listener(&'a crate::controlled_datagram::listener::origin::ListenerProviderRegistration),
    Metadata(&'a MetadataPoolRegistration),
}
#[derive(Clone)]
pub struct CarrierOriginOwnerClosed(OwnerClosed);
#[derive(Clone)]
enum OwnerClosed {
    Carrier(super::super::CarrierOwnerClosed),
    Packet(PacketProviderClosed),
    Listener(crate::controlled_datagram::listener::origin::ListenerProviderClosed),
    Metadata(MetadataPoolOwnerClosed),
}
#[derive(Clone)]
pub struct CarrierOriginUsersClosed(super::super::CarrierOwnerUsersClosed);
impl<'a> From<&'a CarrierOwnerRegistration> for CarrierOriginOwner<'a> {
    fn from(owner: &'a CarrierOwnerRegistration) -> Self {
        Self::Carrier(owner)
    }
}
impl<'a> From<&'a MetadataPoolRegistration> for CarrierOriginOwner<'a> {
    fn from(owner: &'a MetadataPoolRegistration) -> Self {
        Self::Metadata(owner)
    }
}
impl<'a> From<&'a crate::controlled_datagram::listener::origin::ListenerProviderRegistration>
    for CarrierOriginOwner<'a>
{
    fn from(
        owner: &'a crate::controlled_datagram::listener::origin::ListenerProviderRegistration,
    ) -> Self {
        Self::Listener(owner)
    }
}
impl<'a> From<&'a PacketProviderRegistration> for CarrierOriginOwner<'a> {
    fn from(owner: &'a PacketProviderRegistration) -> Self {
        Self::Packet(owner)
    }
}
impl<'a> From<&'a LogicalPacketProviderRegistration> for CarrierOriginOwner<'a> {
    fn from(owner: &'a LogicalPacketProviderRegistration) -> Self {
        Self::LogicalPacket(owner)
    }
}
impl<'a> CarrierOriginOwner<'a> {
    pub fn instance(self) -> ComponentInstanceId {
        match self {
            Self::Carrier(owner) => owner.instance(),
            Self::Packet(owner) => owner.instance(),
            Self::LogicalPacket(owner) => owner.instance(),
            Self::Listener(owner) => owner.instance(),
            Self::Metadata(owner) => owner.instance(),
        }
    }
    pub fn limit(self) -> std::num::NonZeroUsize {
        match self {
            Self::Carrier(owner) => owner.limit(),
            Self::Packet(owner) => owner.limit(),
            Self::LogicalPacket(owner) => owner.limit(),
            Self::Listener(owner) => owner.limit(),
            Self::Metadata(owner) => owner.limit(),
        }
    }
    pub fn same_owner(self, other: impl Into<Self>) -> bool {
        match (self, other.into()) {
            (Self::Carrier(a), Self::Carrier(b)) => a.same_owner(b),
            (Self::Packet(a), Self::Packet(b)) => a.same_owner(b),
            (Self::LogicalPacket(a), Self::LogicalPacket(b)) => a.same_owner(b),
            (Self::Listener(a), Self::Listener(b)) => a.same_owner(b),
            (Self::Metadata(a), Self::Metadata(b)) => a.same_owner(b),
            _ => false,
        }
    }
    pub fn closed(self) -> Option<CarrierOriginOwnerClosed> {
        match self {
            Self::LogicalPacket(_) => None,
            Self::Packet(owner) => owner
                .closed()
                .map(|closed| CarrierOriginOwnerClosed(OwnerClosed::Packet(closed))),
            Self::Listener(owner) => owner
                .closed()
                .map(|closed| CarrierOriginOwnerClosed(OwnerClosed::Listener(closed))),
            Self::Carrier(owner) => owner
                .closed()
                .map(|closed| CarrierOriginOwnerClosed(OwnerClosed::Carrier(closed))),
            Self::Metadata(owner) => owner
                .closed()
                .map(|closed| CarrierOriginOwnerClosed(OwnerClosed::Metadata(closed))),
        }
    }
    pub fn users_closed(self) -> Option<CarrierOriginUsersClosed> {
        match self {
            Self::Carrier(owner) => owner.users_closed().map(CarrierOriginUsersClosed),
            Self::Packet(_) | Self::LogicalPacket(_) | Self::Listener(_) | Self::Metadata(_) => {
                None
            }
        }
    }
}
impl CarrierOriginOwnerClosed {
    pub fn matches<'a>(&self, owner: impl Into<CarrierOriginOwner<'a>>) -> bool {
        match (&self.0, owner.into()) {
            (OwnerClosed::Packet(closed), CarrierOriginOwner::Packet(owner)) => {
                closed.matches(owner)
            }
            (OwnerClosed::Listener(closed), CarrierOriginOwner::Listener(owner)) => {
                closed.matches(owner)
            }
            (OwnerClosed::Carrier(closed), CarrierOriginOwner::Carrier(owner)) => {
                closed.matches(owner)
            }
            (OwnerClosed::Metadata(closed), CarrierOriginOwner::Metadata(owner)) => {
                closed.matches(owner)
            }
            _ => false,
        }
    }
}
impl CarrierOriginUsersClosed {
    pub fn matches<'a>(&self, owner: impl Into<CarrierOriginOwner<'a>>) -> bool {
        matches!(owner.into(), CarrierOriginOwner::Carrier(owner) if self.0.matches(owner))
    }
    pub fn first_caller_failure(&self) -> Option<crate::contract::scope::TaskJoined> {
        self.0.first_caller_failure()
    }
}
