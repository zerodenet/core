use super::*;
use zero_platform_tokio::contract::{
    lifecycle::{ResourceLifecycle, Unfinished},
    CloseReason,
};
impl ResourceLifecycle for HostIcmpProbeOwner {
    async fn start(&mut self, deadline: Instant) -> Result<(), ChannelError> {
        if self.shared.cancel.is_cancelled() {
            return Err(error(
                self.shared.instance,
                Stage::Prepare,
                Reason::Revoked,
                "ICMP host owner is revoked",
            ));
        }
        if Instant::now() >= deadline {
            return Err(error(
                self.shared.instance,
                Stage::Prepare,
                Reason::Timeout,
                "ICMP start deadline elapsed",
            ));
        }
        let mut state = self.shared.state();
        if self.shared.cancel.is_cancelled() || state.phase == Phase::Revoked {
            return Err(error(
                self.shared.instance,
                Stage::Prepare,
                Reason::Revoked,
                "ICMP host owner is revoked",
            ));
        }
        state.phase = Phase::Live;
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        // Actual cancellation wakes outside the state mutex. Phase writes can
        // be retried by shutdown; the token already fences every bind.
        self.shared.cancel.cancel();
        if let Ok(mut state) = self.shared.state.try_lock() {
            state.phase = Phase::Revoked;
            state.native.revoke();
        }
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.revoke(CloseReason::Shutdown);
        let mut state = self.shared.state();
        state.phase = Phase::Revoked;
        if Instant::now() < deadline {
            state.status = state.native.close_all();
            let confirmed = state.status.confirmed_closed;
            state.closed_total += confirmed;
        } else {
            state.harvest();
        }
        if let Some(unknown) = state.status.unconfirmed.first() {
            let source = unknown.source.clone();
            let failure = native_error(self.shared.instance, Stage::Close, source, true);
            return Err(state.remember(failure));
        }
        let complete = state.status.is_complete();
        let mut report = state.take_closed_io(self.shared.instance);
        if !complete {
            report
                .unfinished
                .push(Unfinished::Io("ICMP originals remain owned".into()));
        }
        Ok(report)
    }
}
impl Drop for HostIcmpProbeOwner {
    fn drop(&mut self) {
        self.shared.cancel.cancel();
    }
}
