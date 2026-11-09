//! Read the same provider's actual host graph; never retain or authorize I/O.
use super::*;
impl BackgroundCarrier {
    fn pending_services(
        &self,
        binding: &UpstreamBinding,
    ) -> Result<crate::protocol_registry::TcpRuntimeServices, ChannelError> {
        if binding.dial != self.initial.dial
            || binding.network_generation.0 != self.network.egress.generation()
        {
            return Err(invalid(
                Reason::StalePlan,
                "background pending binding changed",
            ));
        }
        let source = self.network.current_services().map_err(|source| {
            invalid(Reason::Revoked, "background pending host retired").with_source(source)
        })?;
        let services = source.runtime_services().map_err(|source| {
            invalid(Reason::Revoked, "background pending services retired").with_source(source)
        })?;
        services
            .components
            .as_ref()
            .map_err(|source| {
                invalid(Reason::StalePlan, "background pending plan retired")
                    .with_source(source.clone())
            })?
            .authorize_carrier_prefix(self.instance, &binding.prefix)?;
        Ok(services)
    }
    pub(super) fn declared_pending_capacity(
        &self,
        binding: &UpstreamBinding,
    ) -> Result<usize, ChannelError> {
        let services = self.pending_services(binding)?;
        services
            .components
            .as_ref()
            .map_err(|source| {
                invalid(Reason::StalePlan, "background pending plan retired")
                    .with_source(source.clone())
            })?
            .pending_carrier_capacity(&binding.prefix)
    }
    pub(crate) fn declared_pending_capacity_with_dependency(
        &self,
        binding: &UpstreamBinding,
        dependency: ComponentInstanceId,
    ) -> Result<usize, ChannelError> {
        let services = self.pending_services(binding)?;
        services
            .components
            .as_ref()
            .map_err(|source| {
                invalid(Reason::StalePlan, "background plan retired").with_source(source.clone())
            })?
            .pending_carrier_capacity_with_dependency(self.instance, dependency, &binding.prefix)
    }
    pub(super) fn validate_pending_original(
        &self,
        binding: &UpstreamBinding,
        adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        if binding.dial != self.initial.dial {
            return Err(invalid(Reason::StalePlan, "original pending dial changed"));
        }
        let source = self.network.current_services().map_err(|source| {
            invalid(Reason::Revoked, "original host retired").with_source(source)
        })?;
        if source
            .validate_owned_carrier_instance(self.instance, adoption)
            .is_err()
            && !binding.prefix.iter().any(|role| {
                source
                    .validate_owned_carrier_adoption(*role, adoption)
                    .is_ok()
            })
        {
            return Err(invalid(
                Reason::StalePlan,
                "pending original differs from the actual retained provider graph",
            ));
        }
        Ok(())
    }
    pub(super) fn validate_pending_protocol(
        &self,
        binding: &UpstreamBinding,
        original: &zero_platform_tokio::controlled_carrier::ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        if binding.dial != self.initial.dial
            || binding.network_generation.0 != self.network.egress.generation()
            || original.binding().dial != binding.dial
            || original.binding().network_generation != binding.network_generation
        {
            return Err(invalid(
                Reason::StalePlan,
                "protocol pending binding changed",
            ));
        }
        let source = self.network.current_services().map_err(|source| {
            invalid(Reason::Revoked, "protocol pending host retired").with_source(source)
        })?;
        let services = source.runtime_services().map_err(|source| {
            invalid(Reason::Revoked, "protocol pending services retired").with_source(source)
        })?;
        let plan = services.components.as_ref().map_err(|source| {
            invalid(Reason::StalePlan, "protocol pending plan retired").with_source(source.clone())
        })?;
        if binding.prefix.iter().enumerate().any(|(index, role)| {
            original.binding().prefix == binding.prefix[..index]
                && plan.validate_protocol_original(*role, original).is_ok()
        }) {
            Ok(())
        } else {
            Err(invalid(
                Reason::StalePlan,
                "protocol original outside the actual ordered owning graph",
            ))
        }
    }
}
