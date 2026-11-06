//! Only this real Host plan can select a derived Process dependency.
use super::*;
use zero_platform_tokio::owned_process::ProcessEndpointUse;
impl ComponentPlan {
    /// Preflight only. The actual Host retains/drives the returned Slot's one
    /// start operation; a borrower must not own/drop its initializer future.
    pub(in crate::component) fn preflight_process_endpoint(
        &self,
        demand: &zero_platform_tokio::owned_process::ProcessEndpointDemand<'_>,
    ) -> Result<Arc<ComponentSlot>, ChannelError> {
        let fail = || {
            invalid(
                Reason::StalePlan,
                "no actual direct deferred Process dependency",
            )
        };
        let selected = self
            .order
            .iter()
            .find(|slot| slot.instance_id() == demand.consumer)
            .ok_or_else(fail)?;
        if !selected.dependency_instances().contains(&demand.process) {
            return Err(fail());
        }
        selected.check_process_endpoint_consumer(
            demand.endpoint,
            demand.binding,
            demand.class,
            demand.grant,
        )?;
        let dependency = self
            .order
            .iter()
            .find(|slot| slot.instance_id() == demand.process)
            .ok_or_else(fail)?;
        if !dependency.recipe.spec().roles.is_empty() || dependency.recipe.spec().task_limit.is_none() || !dependency.recipe.spec().resources.iter().any(|resource|matches!(resource,zero_platform_tokio::contract::component::ResourceDeclaration::ProcessEndpoint{process_ordinal,spec} if *process_ordinal==demand.process_ordinal && spec.network_generation==demand.binding.network_generation)) || !matches!(dependency.recipe.spec().resources.get(demand.process_ordinal),Some(zero_platform_tokio::contract::component::ResourceDeclaration::Process{max_starts,..}) if max_starts.get()==1){return Err(fail());}
        Ok(dependency.clone())
    }
    /// Actual Host-owned initialization; this borrower can cancel independently.
    /// The immutable first operation deadline is never extended by another user.
    pub async fn ensure_process_endpoint_use(
        &self,
        demand: zero_platform_tokio::owned_process::ProcessEndpointDemand<'_>,
        deadline: tokio::time::Instant,
        cancel: &zero_platform_tokio::contract::CancellationToken,
    ) -> Result<ProcessEndpointUse, ChannelError> {
        if cancel.is_cancelled() {
            return Err(invalid(
                Reason::Cancelled,
                "Process endpoint caller stopped",
            ));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(invalid(
                Reason::Timeout,
                "Process endpoint caller deadline elapsed",
            ));
        }
        let dependency = self.preflight_process_endpoint(&demand)?;
        let operation = dependency.start_owned(deadline);
        tokio::select! { biased;
            _ = cancel.cancelled() => return Err(invalid(Reason::Cancelled, "Process endpoint caller stopped")),
            outcome = tokio::time::timeout_at(deadline, operation) => {
                outcome.map_err(|_| invalid(Reason::Timeout, "original Process endpoint initialization is pending"))?
                    .map_err(crate::component::instance::retained)?;
            }
        }
        self.process_endpoint_use(demand)
    }
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
