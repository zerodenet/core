//! Typed observation of the actual provider; no common fabricated registration.
use super::*;
use crate::controlled_datagram::metadata_pool::{
    MetadataPoolOwnerClosed, MetadataPoolRegistration,
};

#[derive(Clone, Copy)]
pub enum CarrierOriginOwner<'a> {
    Carrier(&'a CarrierOwnerRegistration),
    Metadata(&'a MetadataPoolRegistration),
}
#[derive(Clone)]
pub struct CarrierOriginOwnerClosed(OwnerClosed);
#[derive(Clone)]
enum OwnerClosed {
    Carrier(super::super::CarrierOwnerClosed),
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
impl<'a> CarrierOriginOwner<'a> {
    pub fn instance(self) -> ComponentInstanceId {
        match self {
            Self::Carrier(owner) => owner.instance(),
            Self::Metadata(owner) => owner.instance(),
        }
    }
    pub fn limit(self) -> std::num::NonZeroUsize {
        match self {
            Self::Carrier(owner) => owner.limit(),
            Self::Metadata(owner) => owner.limit(),
        }
    }
    pub fn same_owner(self, other: impl Into<Self>) -> bool {
        match (self, other.into()) {
            (Self::Carrier(a), Self::Carrier(b)) => a.same_owner(b),
            (Self::Metadata(a), Self::Metadata(b)) => a.same_owner(b),
            _ => false,
        }
    }
    pub fn closed(self) -> Option<CarrierOriginOwnerClosed> {
        match self {
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
            Self::Metadata(_) => None,
        }
    }
}
impl CarrierOriginOwnerClosed {
    pub fn matches<'a>(&self, owner: impl Into<CarrierOriginOwner<'a>>) -> bool {
        match (&self.0, owner.into()) {
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
