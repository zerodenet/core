//! Direct lends from its actual selected owner, never the DNS host's native grant.
use super::*;
impl Root {
    pub(in crate::component::native_dns::provider) fn with_direct_endpoint(
        mut self,
        endpoint: Option<Destination>,
    ) -> Self {
        self.direct_endpoint = endpoint;
        self
    }
    pub(super) fn direct_transport(
        &self,
        binding: &UpstreamBinding,
    ) -> Result<UpstreamTransport, ChannelError> {
        let failure = || {
            crate::component::instance::invalid(
                zero_platform_tokio::contract::Reason::StalePlan,
                "Direct DNS invocation differs from its current actual owner",
            )
        };
        if binding != &self.binding
            || self.cancel.is_cancelled()
            || self.invocation.session.is_some()
            || self.invocation.cancel.is_cancelled()
            || self.caller.is_joined()
            || !self.caller.scope_identity().resource_open()
            || self.egress.generation() != binding.network_generation.0
            || self
                .grants
                .registration()
                .bound()
                .is_none_or(|bound| bound.host_cancelled())
        {
            return Err(failure());
        }
        let source = self.current.upgrade().ok_or_else(failure)?;
        let services = source.runtime_services().map_err(|_| failure())?;
        let execution = services.execution();
        let plan = execution.components.as_ref().map_err(|_| failure())?;
        let selected = self.roles.first().ok_or_else(failure)?;
        let slot = plan.role_slot(*selected).ok_or_else(failure)?;
        let waiting = self.waiting.get().cloned().ok_or_else(failure)?;
        crate::component::direct_bridge::service_transport(
            &slot,
            Arc::downgrade(plan),
            execution.upstream().with_observer(self.observer.clone()),
            self.direct_endpoint
                .as_ref()
                .unwrap_or(&self.endpoint)
                .clone(),
            binding.clone(),
            self.invocation.clone(),
            Some(waiting),
        )
    }
}
