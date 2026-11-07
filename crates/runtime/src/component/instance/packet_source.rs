//! Concrete source lineage projected by the actual inbound host. Read-only
//! registrations never transfer native ownership or invent another Scope.
use super::*;
use zero_platform_tokio::{
    contract::component::{ResourceDeclaration, RoleKind},
    controlled_carrier::CarrierOrigin,
    controlled_datagram::provider::NativeDatagramOrigin,
};
impl ComponentSlot {
    pub(in crate::component) fn bind_packet_source(
        &self,
        source: NativeDatagramOrigin,
    ) -> Result<(), ChannelError> {
        self.bind_packet_carrier_source(CarrierOrigin::from_listener(source))
    }
    pub(in crate::component) fn bind_packet_carrier_source(
        &self,
        source: CarrierOrigin,
    ) -> Result<(), ChannelError> {
        if source.packet_source_admission().is_none() {
            return Err(self.failure(
                Reason::StalePlan,
                "logical Packet source lacks its actual Source admission lineage",
            ));
        }
        let spec = self.recipe.spec();
        if !spec.resources.is_empty()
            || ![RoleKind::Packet, RoleKind::Stream, RoleKind::Datagram]
                .iter()
                .all(|role| spec.roles.contains(role))
            || self.cancel.is_cancelled()
            || source.is_retired()
        {
            return Err(self.failure(
                Reason::StalePlan,
                "source is not the actual virtual Packet carrier dependency",
            ));
        }
        if let Some(actual) = self.packet_source.get() {
            return if actual.same_original(&source) {
                Ok(())
            } else {
                Err(self.failure(
                    Reason::StalePlan,
                    "Packet source replacement requires a fresh component generation",
                ))
            };
        }
        self.packet_source.set(source).map_err(|_| {
            self.failure(
                Reason::StalePlan,
                "Packet source was concurrently installed",
            )
        })
    }
    pub(in crate::component) fn matches_packet_source(&self, origin: &CarrierOrigin) -> bool {
        self.packet_source
            .get()
            .is_some_and(|source| source.same_original(origin) && !source.is_retired())
    }
    pub(in crate::component) fn matches_bound_packet_origin(
        &self,
        ordinal: usize,
        origin: &CarrierOrigin,
    ) -> bool {
        origin
            .listener()
            .is_some_and(|original| match self.live.get() {
                Some(Ok(live)) => live.resources.owns_bound_datagram_origin(ordinal, original),
                _ => false,
            })
    }
    /// Actual successful start and concrete native dependency, never a role
    /// marker or a promise that a deferred device may eventually start.
    pub(in crate::component) fn packet_provider_ready(&self) -> bool {
        self.provider_ready(false)
    }
    pub(in crate::component) fn device_carrier_ready(&self) -> bool {
        self.provider_ready(true)
    }
    fn provider_ready(&self, flow_roles: bool) -> bool {
        if self.cancel.is_cancelled() {
            return false;
        }
        let Some(Ok(live)) = self.live.get() else {
            return false;
        };
        if !matches!(live.started.get(), Some(Ok(())))
            || live.packet.is_none()
            || (flow_roles && (live.stream.is_none() || live.datagram.is_none()))
        {
            return false;
        }
        if self
            .packet_source
            .get()
            .is_some_and(|source| !source.is_retired())
        {
            return true;
        }
        self.recipe
            .spec()
            .resources
            .iter()
            .enumerate()
            .any(|(ordinal, resource)| match resource {
                ResourceDeclaration::ControlledPacketDevice { .. } => live
                    .resources
                    .original_packet_owner(ordinal)
                    .is_some_and(|owner| !owner.is_retired()),
                ResourceDeclaration::BoundOutboundDatagram { .. } => {
                    live.resources.bound_datagram_origin_available(ordinal)
                }
                _ => false,
            })
    }
}

impl ComponentSlot {
    /// Exact already-installed Packet provider observations. This does not
    /// start a dependency or issue IO, tasks, Source readiness or close proof.
    pub(in crate::component) fn packet_provider_originals(&self) -> Vec<CarrierOrigin> {
        if !self.packet_provider_ready() {
            return Vec::new();
        }
        let mut originals = Vec::new();
        if let Some(actual) = self.packet_source.get() {
            originals.push(actual.clone());
        }
        if let Some(Ok(live)) = self.live.get() {
            for (ordinal, resource) in self.recipe.spec().resources.iter().enumerate() {
                match resource {
                    ResourceDeclaration::ControlledPacketDevice { .. } => {
                        if let Some(actual) = live.resources.original_packet_owner(ordinal) {
                            originals.push(CarrierOrigin::from_packet(actual));
                        }
                    }
                    ResourceDeclaration::BoundOutboundDatagram { .. } => {
                        originals.extend(live.resources.bound_packet_originals(ordinal));
                    }
                    _ => {}
                }
            }
        }
        originals.retain(|original| !original.is_retired());
        originals
    }
}
