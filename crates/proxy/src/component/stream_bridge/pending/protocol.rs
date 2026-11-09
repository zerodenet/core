//! Canonical actual caller anchor and readonly selected-role validation.
use super::*;
use zero_platform_tokio::controlled_carrier::ProtocolOriginal;
impl Observer {
    pub(super) fn receive_protocol(
        &self,
        binding: &UpstreamBinding,
        original: ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        let fail = || {
            invalid(
                Reason::StalePlan,
                "protocol original differs from actual invocation/role/Scope/prefix",
            )
        };
        let anchor = self.anchor.get().ok_or_else(fail)?;
        if binding != anchor.binding() || !original.matches_observer(anchor) {
            return Err(fail());
        }
        self.caller.check_open()?;
        if let Some(admitted) = &self.admitted {
            admitted.check_active().map_err(|source| {
                invalid(Reason::Revoked, "pending invocation admission ended").with_source(source)
            })?;
        }
        let source = self.source.upgrade().ok_or_else(fail)?;
        let services = source
            .runtime_services()
            .map_err(|source| fail().with_source(source))?;
        let execution = services.execution();
        let plan = execution
            .components
            .as_ref()
            .map_err(|source| fail().with_source(source.clone()))?;
        if original.binding().network_generation != binding.network_generation
            || services.upstream().network_generation() != binding.network_generation.0
            || !self.roles.iter().enumerate().any(|(index, role)| {
                original.binding().prefix == self.roles[..index]
                    && plan.validate_protocol_original(*role, &original).is_ok()
            })
        {
            return Err(fail());
        }
        if let Caller::Probe(call) = &self.caller {
            for role in &self.roles {
                plan.role_slot(*role)
                    .ok_or_else(fail)?
                    .check_probe_caller(call)?;
            }
        }
        // Caller-owned ledgers independently check the exact real native task
        // registration. Readonly plan validation grants no receiver or IO.
        self.caller.retain_protocol(original)
    }
}
