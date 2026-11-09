//! Native candidates consume the selected role's original grant, never DNS grants.
use super::*;
use crate::component::stream_bridge::network::NodeOpener;
use zero_platform_tokio::controlled_carrier::{CarrierAdoption, CarrierCandidates};
impl Root {
    pub(super) fn with_native_datagram(
        mut self,
        instance: zero_core::component::ComponentInstanceId,
        ordinal: usize,
    ) -> Self {
        self.native_datagram = Some((instance, ordinal));
        self
    }
    fn native_datagram_opener(
        &self,
        binding: &UpstreamBinding,
        call: &CarrierContext,
        parent: Option<&CarrierAdoption>,
    ) -> Result<NodeOpener, ChannelError> {
        let fail = || {
            crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::StalePlan,
                "native Datagram differs from the actual selected Query/role/declaration",
            )
        };
        let (instance, ordinal) = self.native_datagram.ok_or_else(fail)?;
        if self.native.is_none()
            || binding != &self.binding
            || !binding.prefix.is_empty()
            || binding.network_generation.0 != self.egress.generation()
            || call.session.is_some()
            || self.invocation.session.is_some()
            || call.deadline != self.invocation.deadline
            || call.cancel.is_cancelled()
            || self.cancel.is_cancelled()
            || self.caller.is_joined()
            || !zero_dns::OwnedDnsQueryTasks::matches_current_caller(&self.caller)
        {
            return Err(fail());
        }
        let source = self.current.upgrade().ok_or_else(fail)?;
        let services = source.runtime_services().map_err(|_| fail())?;
        let execution = services.execution();
        let plan = execution.components.as_ref().map_err(|_| fail())?;
        let slot = self
            .roles
            .iter()
            .filter_map(|role| plan.role_slot(*role))
            .find(|slot| slot.instance_id() == instance)
            .ok_or_else(fail)?;
        let registration = slot.original_carrier_owner(ordinal).ok_or_else(fail)?;
        let bound = registration.bound().ok_or_else(fail)?;
        if !slot.matches_live_scope(&bound.scope_identity()) {
            return Err(fail());
        }
        // This pure resolver does not acquire a socket. The actual candidate
        // later consumes this selected owner's already installed sealed grant.
        // Require its original in this same Query before even Node resolution.
        let retained = self
            .pending
            .lock()
            .expect("DNS pending originals poisoned")
            .iter()
            .any(|entry| {
                entry.task.same_registration(&self.caller)
                    && registration.matches_adoption(&entry.original)
                    && parent.is_none_or(|parent| entry.original.same_slot(parent))
            });
        if !retained {
            return Err(fail());
        }
        if let Some(parent) = parent {
            self.check_pending(binding, parent)?;
        }
        let waiting = self.waiting.get().cloned().ok_or_else(fail)?;
        Ok(NodeOpener::new(
            execution.upstream().with_observer(self.observer.clone()),
            self.endpoint.clone(),
        )
        .with_plan(Some(Arc::downgrade(plan)))
        .with_carrier_owner(instance)
        .with_service_call(self.invocation.clone())
        .with_pending(binding.clone(), Some(waiting)))
    }
    pub(super) fn resolve_native_datagram(
        &self,
        binding: UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<CarrierCandidates, ChannelError>> {
        Box::pin(async move {
            if endpoint != self.endpoint {
                return Err(crate::component::instance::invalid(
                    zero_platform_tokio::contract::Reason::StalePlan,
                    "native Datagram endpoint differs from the selected declaration",
                ));
            }
            self.native_datagram_opener(&binding, &call, None)?
                .resolve_controlled_carrier(&binding, endpoint, call)
                .await
        })
    }
    pub(super) fn resolve_related_native_datagram(
        &self,
        binding: UpstreamBinding,
        parent: CarrierAdoption,
        endpoint: Destination,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<CarrierCandidates, ChannelError>> {
        Box::pin(async move {
            // The existing related resolver validates the actual parent TCP
            // endpoint, same binding and derived UDP authority independently.
            self.native_datagram_opener(&binding, &call, Some(&parent))?
                .resolve_related_datagram(&binding, parent, endpoint, call)
                .await
        })
    }
}
