//! Select only a real retained Packet owner; never prepare/decode a candidate.
use super::*;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::contract::component::GrantedPacketServiceCaller;
impl ComponentCatalog {
    pub fn packet_driver_candidate(
        &self,
        grant: &GrantedPacketServiceCaller,
    ) -> Result<(Arc<RuntimeConfig>, Arc<ComponentPlan>), ChannelError> {
        let entries = {
            let state = self.state.lock().expect("component catalog poisoned");
            state
                .cold
                .iter()
                .chain(state.staged.iter())
                .chain(state.current.iter())
                .filter_map(|entry| {
                    entry
                        .plan
                        .as_ref()
                        .ok()
                        .map(|plan| (entry.config.clone(), plan.clone()))
                })
                .collect::<Vec<_>>()
        };
        for (config, plan) in entries {
            if let Some(slot) = plan
                .order
                .iter()
                .find(|slot| slot.instance_id() == grant.original().instance())
            {
                if slot.matches_packet_driver_grant(grant) {
                    return Ok((config, plan));
                }
            }
        }
        Err(invalid(
            Reason::Revoked,
            "Packet caller has no same registered current/start owner",
        ))
    }
    /// External Sources are matched by their actual owner separately. This is
    /// only exact already assigned input/dependency metadata, not a virtual Slot.
    pub fn packet_source_driver_candidate(
        &self,
        tag: &str,
        input: &[u8; 32],
        dependencies: &[ComponentInstanceId],
    ) -> Result<(Arc<RuntimeConfig>, Arc<ComponentPlan>), ChannelError> {
        let entries = {
            let state = self.state.lock().expect("component catalog poisoned");
            state
                .cold
                .iter()
                .chain(state.staged.iter())
                .chain(state.current.iter())
                .filter_map(|entry| {
                    entry
                        .plan
                        .as_ref()
                        .ok()
                        .map(|plan| (entry.config.clone(), plan.clone()))
                })
                .collect::<Vec<_>>()
        };
        for (config, plan) in entries {
            if plan
                .source_dependency_instances(tag, input)
                .is_ok_and(|actual| actual == dependencies)
            {
                return Ok((config, plan));
            }
        }
        Err(invalid(
            Reason::StalePlan,
            "Packet Source input/dependency instances have no actual retained candidate",
        ))
    }
}
