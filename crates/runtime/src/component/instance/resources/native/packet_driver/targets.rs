use super::*;
use zero_platform_tokio::contract::component::{PacketDriverTarget, RoleKind};
impl HostResources {
    pub(super) fn packet_driver_targets(
        &self,
        network: zero_core::component::NetworkGeneration,
    ) -> Result<Arc<[PacketDriverTarget]>, ChannelError> {
        let mut pending = self
            .dependency_slots
            .get()
            .ok_or_else(|| self.denied())?
            .to_vec();
        let mut seen = std::collections::BTreeSet::new();
        let mut targets = Vec::new();
        while let Some(weak) = pending.pop() {
            let slot = weak.upgrade().ok_or_else(|| self.denied())?;
            if !seen.insert(slot.instance_id()) {
                continue;
            }
            pending.extend(
                slot.dependency_slots
                    .get()
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    .cloned(),
            );
            if !slot
                .recipe
                .spec()
                .roles
                .iter()
                .any(|role| matches!(role, RoleKind::Stream | RoleKind::Datagram))
            {
                continue;
            }
            let mut graph = vec![slot.clone()];
            let mut originals = Vec::new();
            let mut packet_originals: Vec<zero_platform_tokio::controlled_carrier::CarrierOrigin> =
                Vec::new();
            let mut providers = std::collections::BTreeSet::new();
            while let Some(provider) = graph.pop() {
                if !providers.insert(provider.instance_id()) {
                    continue;
                }
                for original in provider.packet_provider_originals() {
                    if !packet_originals
                        .iter()
                        .any(|actual| actual.same_original(&original))
                    {
                        packet_originals.push(original);
                    }
                }
                for ordinal in 0..provider.recipe.spec().resources.len() {
                    if let Some(owner) = provider.original_carrier_owner(ordinal) {
                        if !originals.iter().any(|actual: &zero_platform_tokio::controlled_carrier::CarrierOwnerRegistration| actual.same_owner(&owner)) { originals.push(owner); }
                    }
                }
                for weak in provider
                    .dependency_slots
                    .get()
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                {
                    graph.push(weak.upgrade().ok_or_else(|| self.denied())?);
                }
            }
            let pending_capacity = originals
                .iter()
                .try_fold(0usize, |sum, owner| sum.checked_add(owner.limit().get()))
                .ok_or_else(|| self.denied())?;
            targets.push(PacketDriverTarget {
                instance: slot.instance_id(),
                role: slot.role_id(),
                datagram: slot.recipe.spec().roles.contains(&RoleKind::Datagram),
                binding: UpstreamBinding {
                    prefix: Vec::new(),
                    dial: slot.dial.clone(),
                    network_generation: network,
                },
                originals: originals.into(),
                packet_originals: packet_originals.into(),
                pending_capacity,
            });
        }
        if targets.is_empty() {
            return Err(self.denied());
        }
        Ok(targets.into())
    }
}
