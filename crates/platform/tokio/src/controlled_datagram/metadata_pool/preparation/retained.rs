//! Observation only after the same original has its actual driver receiver.
use super::*;

impl MetadataPoolOrigin {
    /// Observe an already handed-off original through its same live native
    /// driver graph. This grants no IO or new receiver and cannot promote an
    /// unobserved initializer merely because its endpoint happens to match.
    pub fn retained_protocol_original(
        &self,
        upstream: &crate::contract::network::UpstreamTransport,
        observer: crate::contract::network::PendingCarrierObserver,
        endpoint: &zero_core::request::Destination,
        retirement: &crate::controlled_carrier::CarrierProtocolRetirement,
    ) -> Result<crate::controlled_carrier::ProtocolOriginal, ChannelError> {
        let slot = self
            .slot
            .upgrade()
            .ok_or_else(|| error(Reason::Revoked, "retained metadata original disappeared"))?;
        let owner = slot
            .owner
            .upgrade()
            .ok_or_else(|| error(Reason::Revoked, "retained metadata owner disappeared"))?;
        let bound = owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .bound
            .clone()
            .ok_or_else(|| error(Reason::Revoked, "retained metadata authority disappeared"))?;
        let phase = slot
            .protocol
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| error(Reason::StalePlan, "retained original has no protocol owner"))?;
        let receiver = self.retained_driver().ok_or_else(|| {
            error(
                Reason::StalePlan,
                "metadata original has no actual driver receiver",
            )
        })?;
        let binding = upstream.binding();
        let declared = matches!(&bound.declaration, ResourceDeclaration::ControlledMetadataCarrier { endpoint: actual, dial, .. }
            if actual == endpoint && dial == &binding.dial);
        if !slot.root_bound
            || self.is_retired()
            || self.closed().is_some()
            || retirement.retirement_requested()
            || !retirement.matches_phase(&phase)
            || !binding.prefix.is_empty()
            || !declared
            || !self
                .binding_snapshot()
                .is_some_and(|(actual, selected, dns)| {
                    &actual == endpoint
                        && &selected == binding
                        && dns == upstream.carrier_dns_generation()
                })
            || !bound.scope.resource_open()
            || bound.host_cancel.is_cancelled()
            || owner.cancel.is_cancelled()
            || !receiver.scope_identity().same_scope(&bound.scope)
            || receiver.protocol_driver().is_joined()
            || receiver.packet_driver().is_joined()
            || !receiver.wire_is_live()
            || !upstream
                .protocol_original_observer()
                .as_ref()
                .is_some_and(|actual| actual.same_observer(&observer))
        {
            return Err(error(
                Reason::StalePlan,
                "retained metadata original differs from actual live owner/driver",
            ));
        }
        Ok(
            crate::controlled_carrier::ProtocolOriginal::metadata_original(
                self.clone(),
                bound.scope.clone(),
                upstream.original_binding(),
                observer,
                retirement.clone(),
            ),
        )
    }
}
