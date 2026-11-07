use super::*;
use zero_platform_tokio::{
    contract::{
        component::{GrantedPacketServiceCaller, PacketDriverCalls, PacketDriverTarget},
        network::{CarrierContext, UpstreamBinding},
        scope::TaskJoinWitness,
    },
    controlled_carrier::CarrierOrigin,
};
impl NativeDatagramGrant {
    pub(in crate::runtime::inbound_owners) fn matches_retained_packet_driver(
        &self,
        grant: &GrantedPacketServiceCaller,
    ) -> bool {
        self.identity == grant.original().instance()
            && self
                .packet_calls
                .get()
                .is_some_and(|same| grant.matches_calls(same))
            && self
                .packet_source_origin()
                .is_some_and(|same| same.same_original(grant.original()))
            && self.scope.as_ref().is_some_and(|tasks| {
                tasks
                    .scope_identity()
                    .same_scope(&grant.caller().scope_identity())
            })
    }

    pub(in crate::runtime::inbound_owners) fn packet_driver_caller(
        &self,
        original: &CarrierOrigin,
        driver: &TaskJoinWitness,
        binding: UpstreamBinding,
        call: CarrierContext,
        targets: Arc<[PacketDriverTarget]>,
    ) -> Result<GrantedPacketServiceCaller, ChannelError> {
        let denied = || ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component);
        let tasks = self.scope.as_ref().ok_or_else(denied)?;
        let actual = self.packet_source_origin().ok_or_else(denied)?;
        if self.cancel.is_cancelled()
            || !actual.same_original(original)
            || !tasks.scope_identity().same_scope(&driver.scope_identity())
        {
            return Err(denied());
        }
        let calls = match self.packet_calls.get() {
            Some(calls) => calls.clone(),
            None => {
                let copy = self
                    .packet_reads
                    .as_ref()
                    .ok_or_else(denied)?
                    .send_copy_workspace()?
                    .ok_or_else(denied)?;
                let target = self
                    .packet_reads
                    .as_ref()
                    .ok_or_else(denied)?
                    .send_target_workspace()?
                    .ok_or_else(denied)?;
                let wire = actual.driver().cloned().ok_or_else(denied)?;
                let calls = Arc::new(PacketDriverCalls::from_registered(
                    actual.clone(),
                    &tasks.downgrade(),
                )?);
                calls.install_send_workspaces(&actual, vec![(wire, copy, target)])?;
                let _ = self.packet_calls.set(calls);
                self.packet_calls.get().ok_or_else(denied)?.clone()
            }
        };
        GrantedPacketServiceCaller::from_registered(calls, original, driver, binding, call, targets)
    }
    pub(in crate::runtime::inbound_owners) fn matches_packet_driver(
        &self,
        grant: &GrantedPacketServiceCaller,
    ) -> bool {
        !self.cancel.is_cancelled()
            && self
                .packet_calls
                .get()
                .is_some_and(|calls| grant.matches_calls(calls))
            && self
                .packet_source_origin()
                .is_some_and(|actual| actual.same_original(grant.original()))
            && self.scope.as_ref().is_some_and(|tasks| {
                tasks
                    .scope_identity()
                    .same_scope(&grant.caller().scope_identity())
            })
    }
}
