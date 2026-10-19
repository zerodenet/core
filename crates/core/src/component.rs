//! Runtime-neutral identities. Resource owners and callable roles are distinct.
use alloc::sync::Arc;

macro_rules! identity {
    ($($name:ident),+ $(,)?) => {$ (
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub u64);
    )+};
}
identity!(
    SessionId,
    ComponentId,
    InboundId,
    OutboundId,
    EndpointId,
    AssociationId,
    TaskId
);

macro_rules! generation {
    ($($name:ident),+ $(,)?) => {$ (
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub u64);
        impl $name {
            /// Exhaustion is an error, never a wrap into a previously used identity.
            pub const fn checked_next(self) -> Option<Self> {
                match self.0.checked_add(1) {
                    Some(value) => Some(Self(value)),
                    None => None,
                }
            }
        }
    )+};
}
generation!(
    ConfigRevision,
    ResourceGeneration,
    NetworkGeneration,
    PolicyRevision
);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RoleId {
    Inbound(InboundId),
    Outbound(OutboundId),
    Endpoint(EndpointId),
}

/// One callable role in one concrete resource generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RoleInstanceId {
    pub component: ComponentId,
    pub role: RoleId,
    pub generation: ResourceGeneration,
}

/// IDs assigned by assembly after the pure descriptor has been validated.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ComponentRoleIds {
    pub inbound: Option<InboundId>,
    pub outbound: Option<OutboundId>,
    pub endpoint: Option<EndpointId>,
}

/// Opaque protocol-authenticated identity; never a password or key material.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct PrincipalId(pub Arc<str>);

impl core::fmt::Debug for PrincipalId {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str("PrincipalId(<redacted>)")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PeerId(pub Arc<str>);

/// Identity facts only. Mutable quotas, revocation and rates belong to policy.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AuthSubject {
    pub owner: ComponentId,
    pub principal: PrincipalId,
}
