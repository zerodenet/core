//! Exact original observation: protocol completion is not global credit return.
use super::*;
impl CarrierProtocolRetirement {
    pub fn same_original(&self, other: &Self) -> bool {
        Weak::ptr_eq(&self.phase, &other.phase) && Arc::ptr_eq(&self.settled, &other.settled)
    }
    pub(crate) fn matches_phase(&self, phase: &Arc<Phase>) -> bool {
        Weak::ptr_eq(&self.phase, &Arc::downgrade(phase))
            && Arc::ptr_eq(&self.settled, &phase.settled)
    }
    pub async fn wait_settled_original(&self) -> Result<(), ChannelError> {
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
        phase.request();
        loop {
            let changed = phase.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_settled() {
                return Ok(());
            }
            changed.await;
        }
    }
    pub async fn wait_protocol_original(&self) -> Result<(), ChannelError> {
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
        phase.request();
        loop {
            let changed = phase.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if phase.complete() {
                return Ok(());
            }
            changed.await;
        }
    }

    /// The actual Root (or the genuine standalone original's sole book) removed
    /// its charged reservation. A Root-bound local owner cannot publish this.
    /// Native/users and original registration ACKs remain independent facts.
    /// Same original Root removed its charged reservation only after the real
    /// native/users receipts and every actual registration ACK.
    pub fn is_settled(&self) -> bool {
        self.settled.load(Ordering::Acquire)
    }
    /// Pure actual request observation, not protocol/native/Host settlement.
    pub fn retirement_requested(&self) -> bool {
        self.phase.upgrade().is_some_and(|phase| phase.requested())
    }
    pub fn request_retirement(&self) {
        if let Some(phase) = self.phase.upgrade() {
            phase.request();
        }
    }
    /// Wake only the existing Root continuation after an actual view/cleanup
    /// state change. This carries neither progress nor closure evidence.
    pub fn notify_progress(&self) {
        if let Some(phase) = self.phase.upgrade() {
            phase.wake();
        }
    }
}
