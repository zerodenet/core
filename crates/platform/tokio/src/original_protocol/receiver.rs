//! Query pending ownership may return only to a real sealed cached receiver.
//! Actual Root retirement and strict global settlement remain separate.
use super::*;
use crate::controlled_carrier::ProtocolOriginalReceiver;
impl Phase {
    pub(crate) fn retain_receiver(
        &self,
        receiver: ProtocolOriginalReceiver,
    ) -> Result<(), ChannelError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !self.root_bound
            || state.complete
            || state.retirement_started
            || state.receiver.is_some()
            || receiver.instance() != self.instance
        {
            return Err(ChannelError::new(
                crate::contract::Stage::Prepare,
                crate::contract::Reason::Revoked,
                crate::contract::Impact::Component,
            ));
        }
        state.receiver = Some(receiver);
        drop(state);
        self.wake();
        Ok(())
    }
    pub(crate) fn receiver(&self) -> Option<ProtocolOriginalReceiver> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .receiver
            .clone()
    }
    fn request_pending(&self) {
        let needed = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.receiver.is_some() {
                false
            } else {
                state.retirement_started = true;
                true
            }
        };
        if needed {
            self.requested.cancel();
            self.wake();
        }
    }
}
impl CarrierProtocolRetirement {
    pub(crate) fn original_phase(&self) -> Option<Arc<Phase>> {
        self.phase.upgrade()
    }
    pub(crate) fn receiver(&self) -> Option<ProtocolOriginalReceiver> {
        self.original_receiver()
    }
    /// Exact previously published owning receiver; does not grant IO or closure.
    pub fn original_receiver(&self) -> Option<ProtocolOriginalReceiver> {
        self.phase.upgrade()?.receiver()
    }
    pub(crate) fn request_pre_delivery_retirement(&self) {
        if let Some(phase) = self.phase.upgrade() {
            phase.request_pending();
        }
    }
    pub(crate) async fn wait_returned_original(&self) -> Result<(), ChannelError> {
        if self.is_settled() {
            return Ok(());
        }
        let phase = self.phase.upgrade().ok_or_else(|| {
            ChannelError::new(
                crate::contract::Stage::Close,
                crate::contract::Reason::Revoked,
                crate::contract::Impact::Component,
            )
        })?;
        phase.request_pending();
        loop {
            let changed = phase.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_settled() || phase.receiver().is_some() {
                return Ok(());
            }
            changed.await;
        }
    }
}
