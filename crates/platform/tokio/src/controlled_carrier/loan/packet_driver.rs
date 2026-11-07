//! Concrete same-LoanOwner custody for an actual Packet-driver attempt.
use super::*;
use crate::contract::{
    lifecycle::{CloseReport, DynChannelControl, Unfinished},
    BoxFuture, ChannelError, CloseReason,
};
use std::sync::Weak;
use tokio::{sync::Mutex, time::Instant};
struct Control {
    owner: Weak<Mutex<LoanOwner>>,
    call: crate::contract::component::packet_driver::streams::StreamCancellation,
}
impl LoanOwner {
    pub(crate) fn packet_driver_view(
        &self,
        owner: Weak<Mutex<LoanOwner>>,
        call: crate::contract::component::packet_driver::streams::StreamCancellation,
    ) -> Self {
        let mut view = Self::new(
            Box::new(Control { owner, call }),
            self.original().clone(),
            self.packet_driver_state(),
        );
        view.native_control_only = self.native_control_only;
        if let Some(receiver) = self.receiver.get() {
            view.receiver
                .set(receiver.clone())
                .unwrap_or_else(|_| unreachable!("fresh same-loan delegate"));
        }
        view
    }
}
fn pending() -> CloseReport {
    CloseReport {
        unfinished: vec![Unfinished::Resource(
            "Packet driver retains the original logical Stream control".into(),
        )],
        ..Default::default()
    }
}
impl DynChannelControl for Control {
    fn cancel(&self, reason: CloseReason) {
        // Cancellation is lock-free and addresses the same loan State. It does
        // not cancel the parent wire or invent native/Scope completion.
        self.call.cancel(reason);
    }
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        let Some(owner) = self.owner.upgrade() else {
            return Ok(pending());
        };
        let result = match owner.try_lock() {
            Ok(mut owner) => owner.try_close(),
            Err(_) => Ok(pending()),
        };
        result
    }
    fn wait_closed(&mut self, end: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        Box::pin(async move {
            self.cancel(CloseReason::Shutdown);
            let Some(owner) = self.owner.upgrade() else {
                return Ok(pending());
            };
            let Ok(mut owner) = tokio::time::timeout_at(end, owner.lock()).await else {
                return Ok(pending());
            };
            owner.close(end).await
        })
    }
}

#[derive(Clone)]
pub(crate) struct OriginalCancellation(Arc<State>);
impl OriginalCancellation {
    pub(super) fn new(state: Arc<State>) -> Self {
        Self(state)
    }
    pub(crate) fn cancel(&self) {
        self.0.cancel.cancel();
    }
}
