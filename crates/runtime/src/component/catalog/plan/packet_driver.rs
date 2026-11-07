//! Exact targets of an already prepared Packet driver's real dependency graph.
use super::*;
use zero_core::component::{ComponentInstanceId, NetworkGeneration};
use zero_platform_tokio::contract::{
    component::{PacketDriverTarget, RoleKind},
    network::UpstreamBinding,
};
impl ComponentPlan {
    /// This does not decode, start, select IO or grant a Source outside order.
    /// Source callers pass the already verified input's actual dependency set;
    /// standalone callers pass their same Slot's actual dependency instances.
    pub fn packet_driver_targets(
        &self,
        dependencies: &[ComponentInstanceId],
        network: NetworkGeneration,
    ) -> Result<Arc<[PacketDriverTarget]>, ChannelError> {
        let mut pending = dependencies.to_vec();
        let mut seen = std::collections::BTreeSet::new();
        let mut targets = Vec::new();
        while let Some(instance) = pending.pop() {
            if !seen.insert(instance) {
                continue;
            }
            let slot = self
                .order
                .iter()
                .find(|slot| slot.instance_id() == instance)
                .ok_or_else(|| {
                    invalid(
                        Reason::StalePlan,
                        "Packet driver dependency generation changed",
                    )
                })?;
            pending.extend(self.retirement_dependencies(slot));
            if !slot
                .recipe
                .spec()
                .roles
                .iter()
                .any(|role| matches!(role, RoleKind::Stream | RoleKind::Datagram))
            {
                continue;
            }
            let mut originals = Vec::new();
            let mut packet_originals: Vec<zero_platform_tokio::controlled_carrier::CarrierOrigin> =
                Vec::new();
            let mut graph = vec![instance];
            let mut visited = std::collections::BTreeSet::new();
            while let Some(id) = graph.pop() {
                if !visited.insert(id) {
                    continue;
                }
                let provider = self
                    .order
                    .iter()
                    .find(|slot| slot.instance_id() == id)
                    .ok_or_else(|| {
                        invalid(
                            Reason::StalePlan,
                            "Packet driver provider generation changed",
                        )
                    })?;
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
                graph.extend(self.retirement_dependencies(provider));
            }
            let pending_capacity = originals
                .iter()
                .try_fold(0usize, |sum, owner| sum.checked_add(owner.limit().get()))
                .ok_or_else(|| {
                    invalid(Reason::Capacity, "Packet driver pending capacity overflow")
                })?;
            targets.push(PacketDriverTarget {
                instance,
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
            return Err(invalid(
                Reason::InvalidConfig,
                "Packet driver has no actual Datagram dependency",
            ));
        }
        Ok(targets.into())
    }
}
