//! Typed Host accounting facts, separate from the protocol's secret-free identity.
use super::*;
use zero_platform_tokio::contract::component::{ComponentSpec, ResourceDeclaration as R};
/// Only providers whose actual originals are bound to this Host custody root.
/// This is a cold replacement fact, never an I/O or lifecycle readiness grant.
pub(in crate::component) fn owns_carrier_custody(spec: &ComponentSpec) -> bool {
    spec.resources.iter().any(|resource| {
        matches!(
            resource,
            R::ControlledCarrierPool { .. }
                | R::ControlledServiceStream { .. }
                | R::ControlledAssociationPool { .. }
                | R::ControlledDatagramCarrier { .. }
                | R::ControlledOutboundStream { .. }
                | R::ControlledDirectStream { .. }
                | R::ControlledNodeStream { .. }
                | R::ControlledNodeCarrier { .. }
                | R::ControlledMetadataDatagram { .. }
                | R::ControlledMetadataCarrier { .. }
                | R::BoundOutboundDatagram { .. }
        )
    })
}
impl ComponentRecipe {
    pub(in crate::component) fn with_carrier_total_limit(mut self, limit: Option<usize>) -> Self {
        self.carrier_total = limit;
        self
    }
    pub(in crate::component) fn carrier_total_limit(&self) -> Option<usize> {
        self.carrier_total
    }
}
