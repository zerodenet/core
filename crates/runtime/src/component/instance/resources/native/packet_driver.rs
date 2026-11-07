//! Concrete software Packet actors over this Host's exact declared dependencies.
use super::*;
use zero_platform_tokio::{
    contract::{
        component::{GrantedPacketServiceCaller, PacketDriverCalls},
        network::{CarrierContext, UpstreamBinding},
        scope::TaskJoinWitness,
    },
    controlled_carrier::{CarrierOrigin, LogicalPacketProviderRegistration},
};
mod targets;
impl HostResources {
    pub(in crate::component) fn packet_driver_start_open(&self) -> bool {
        self.require_start().is_ok()
    }

    pub(in crate::component) fn matches_packet_driver_grant(
        &self,
        grant: &GrantedPacketServiceCaller,
    ) -> bool {
        !self.cancel.is_cancelled()
            && self
                .packet_calls
                .get()
                .is_some_and(|calls| grant.matches_calls(calls))
            && self.instance == Some(grant.original().instance())
            && self.original_tasks.as_ref().is_some_and(|tasks| {
                tasks
                    .scope_identity()
                    .same_scope(&grant.caller().scope_identity())
            })
            && (self
                .packet_source
                .get()
                .is_some_and(|actual| actual.same_original(grant.original()))
                || self.require_start().is_ok())
    }

    pub(super) fn prepare_packet_driver_calls(
        &self,
        source: &LogicalPacketProviderRegistration,
    ) -> Result<(), ChannelError> {
        self.require_start()?;
        let origin = CarrierOrigin::from_logical_packet(source.clone());
        if let Some(calls) = self.packet_calls.get() {
            return calls
                .matches_original(&origin)
                .then_some(())
                .ok_or_else(|| self.denied());
        }
        let calls = Arc::new(PacketDriverCalls::from_registered(
            origin.clone(),
            self.original_tasks.as_ref().ok_or_else(|| self.denied())?,
        )?);
        if self.packet_calls.set(calls).is_err()
            && !self
                .packet_calls
                .get()
                .is_some_and(|calls| calls.matches_original(&origin))
        {
            return Err(self.denied());
        }
        Ok(())
    }
    pub(super) fn issue_packet_driver_caller(
        &self,
        original: &CarrierOrigin,
        driver: &TaskJoinWitness,
        binding: UpstreamBinding,
        call: CarrierContext,
    ) -> Result<GrantedPacketServiceCaller, ChannelError> {
        if self.cancel.is_cancelled()
            || self.instance != Some(original.instance())
            || self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .closed
            || !original
                .logical_packet()
                .is_some_and(|source| source.has_original_total(&self.originals.carrier))
        {
            return Err(self.denied());
        }
        let calls = self
            .packet_calls
            .get()
            .filter(|calls| calls.matches_original(original))
            .ok_or_else(|| self.denied())?;
        let targets = self.packet_driver_targets(binding.network_generation)?;
        GrantedPacketServiceCaller::from_registered(
            calls.clone(),
            original,
            driver,
            binding,
            call,
            targets,
        )
    }
}
