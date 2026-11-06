//! Only this real Host plan can select a derived Process dependency.
use super::*;
use zero_platform_tokio::owned_process::ProcessEndpointUse;
impl ComponentPlan {
    /// No acquisition, Session admission or selected-address permission is
    /// inferred from an ID. Both real installed Slots and the original physical
    /// reservation's registered authority are checked before making this permit.
    pub fn process_endpoint_use(
        &self,
        demand: zero_platform_tokio::owned_process::ProcessEndpointDemand<'_>,
    ) -> Result<ProcessEndpointUse, ChannelError> {
        let zero_platform_tokio::owned_process::ProcessEndpointDemand {
            consumer,
            process,
            process_ordinal,
            endpoint,
            binding,
            class,
            grant,
        } = demand;
        let fail = || {
            invalid(
                Reason::StalePlan,
                "no actual current Process endpoint dependency",
            )
        };
        let selected = self
            .order
            .iter()
            .find(|slot| slot.instance_id() == consumer)
            .ok_or_else(fail)?;
        if !selected.dependency_instances().contains(&process) {
            return Err(fail());
        }
        let dependency = self
            .order
            .iter()
            .find(|slot| slot.instance_id() == process)
            .ok_or_else(fail)?;
        let handoff = dependency.ready_process_endpoint(process_ordinal)?;
        if handoff.instance() != process
            || handoff.spec().network_generation != binding.network_generation
        {
            return Err(fail());
        }
        selected.endpoint_consumer_use(&handoff, endpoint, binding, class, grant)
    }
}
