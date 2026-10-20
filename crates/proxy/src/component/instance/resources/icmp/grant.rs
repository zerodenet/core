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
    /// crate-private and is never given to arbitrary protocol ResourceAccess.
    pub(crate) fn bind(
        &self,
        bind: IpAddr,
        egress: Option<&EgressInterface>,
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
        let mut state = shared.try_state()?;
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
        let result = state.native.bind(bind, egress);
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
    pub(crate) fn close(
        &self,
        view: &ControlledIcmpSocket,
    ) -> Result<IcmpCloseReceipt, ChannelError> {
        let shared = self.shared()?;
        let mut state = shared.try_state()?;
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
