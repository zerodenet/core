use super::*;
use std::net::IpAddr;
use zero_platform_tokio::{
    controlled_icmp::{ControlledIcmpSocket, IcmpAcquisitionOutcome, IcmpCloseReceipt},
    EgressInterface,
};
impl HostIcmpProbeGrant {
    fn shared(&self) -> Result<Arc<Shared>, ChannelError> {
        let shared = self.shared.upgrade().ok_or_else(|| {
            error(
                self.instance,
                Stage::Prepare,
                Reason::Revoked,
                "ICMP host owner is gone",
            )
        })?;
        if shared.instance != self.instance {
            return Err(error(
                self.instance,
                Stage::Prepare,
                Reason::StalePlan,
                "ICMP grant belongs to another instance",
            ));
        }
        Ok(shared)
    }
    /// Host routing supplies the actual bind family/egress; this capability is
    /// owner-specific. Inbound ResourceAccess delegates only during the actual
    /// finite lifecycle start phase; cleanup authority stays at this provider.
    pub fn bind(
        &self,
        bind: IpAddr,
        egress: Option<&EgressInterface>,
    ) -> Result<ControlledIcmpSocket, ChannelError> {
        self.bind_inner(bind, egress, None)
    }
    /// Each live echo uses its own actual registered caller; refund does not
    /// wait for the containing long-lived Source's whole Scope to shut down.
    pub fn bind_for_task(
        &self,
        bind: IpAddr,
        egress: Option<&EgressInterface>,
        caller: &zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) -> Result<ControlledIcmpSocket, ChannelError> {
        self.bind_inner(bind, egress, Some(caller))
    }
    pub fn bind_authorized(
        &self,
        authorization:zero_platform_tokio::controlled_datagram::metadata_pool::MetadataIcmpAuthorization,
    ) -> Result<ControlledIcmpSocket, ChannelError> {
        let observation = authorization.observation();
        let _observed = observation.defer_notifications();
        let shared = self.shared()?;
        let _callbacks;
        let mut state = shared.try_state()?;
        _callbacks = state.native.defer_notifications();
        if shared.cancel.is_cancelled() || state.phase != Phase::Live {
            return Err(error(
                self.instance,
                Stage::Prepare,
                Reason::Revoked,
                "metadata ICMP actual owner unavailable",
            ));
        }
        let result = authorization.bind(&mut state.native);
        state.harvest();
        result
    }
    fn bind_inner(
        &self,
        bind: IpAddr,
        egress: Option<&EgressInterface>,
        caller: Option<&zero_platform_tokio::contract::scope::TaskJoinWitness>,
    ) -> Result<ControlledIcmpSocket, ChannelError> {
        let shared = self.shared()?;
        if shared.cancel.is_cancelled() {
            return Err(error(
                self.instance,
                Stage::Prepare,
                Reason::Revoked,
                "ICMP host owner is revoked",
            ));
        }
        let _callbacks;
        let mut state = shared.try_state()?;
        _callbacks = state.native.defer_notifications();
        if shared.cancel.is_cancelled() || state.phase == Phase::Revoked {
            return Err(error(
                self.instance,
                Stage::Prepare,
                Reason::Revoked,
                "ICMP host owner is revoked",
            ));
        }
        if state.phase != Phase::Live {
            return Err(error(
                self.instance,
                Stage::Prepare,
                Reason::InvalidConfig,
                "ICMP grant requires the installed live host owner",
            ));
        }
        let result = match caller {
            Some(caller) => state.native.bind_for_task(bind, egress, caller),
            None => state.native.bind(bind, egress),
        };
        state.harvest();
        match result {
            Ok(view) => Ok(view),
            Err(failure) => {
                let unknown = matches!(failure.outcome, IcmpAcquisitionOutcome::Unknown(_));
                let failure = native_error(self.instance, Stage::Prepare, failure.source, unknown);
                // Expected per-probe failures with a proven disposition remain
                // caller errors, matching legacy unreachable behavior. Unknown
                // is a persistent ownership fault and blocks new native binds.
                Err(if unknown {
                    state.remember(failure)
                } else {
                    failure
                })
            }
        }
    }
    /// Retirement never revokes cleanup authority for this same actual pool.
    pub fn close(&self, view: &ControlledIcmpSocket) -> Result<IcmpCloseReceipt, ChannelError> {
        let shared = self.shared()?;
        let _callbacks;
        let mut state = shared.try_state()?;
        _callbacks = state.native.defer_notifications();
        let result = state.native.close(view);
        state.harvest();
        result.map_err(|source| {
            let unknown = !state.status.unconfirmed.is_empty();
            let failure = native_error(self.instance, Stage::Close, source, unknown);
            if unknown {
                state.remember(failure)
            } else {
                failure
            }
        })
    }
}
