use super::*;
use crate::contract::{
    lifecycle::{CloseReport, Unfinished},
    ChannelError, CloseReason,
};
use std::sync::atomic::Ordering;
use tokio::time::Instant;

pub struct LoanOwner {
    control: Box<dyn DynChannelControl>,
    original: CarrierOrigin,
    state: Arc<State>,
    closed: bool,
}
/// Same logical loan only. No native-close or task-exit claim is made here.
#[derive(Clone)]
pub struct LoanClosed {
    state: Arc<State>,
    original: CarrierOrigin,
}
impl LoanOwner {
    pub(super) fn new(
        control: Box<dyn DynChannelControl>,
        original: CarrierOrigin,
        state: Arc<State>,
    ) -> Self {
        Self {
            control,
            original,
            state,
            closed: false,
        }
    }
    pub fn original(&self) -> &CarrierOrigin {
        &self.original
    }
    pub fn cancel(&self, reason: CloseReason) {
        self.state.cancel.cancel();
        self.control.cancel(reason);
    }
    pub fn closed(&self) -> Option<LoanClosed> {
        self.closed.then(|| LoanClosed {
            state: self.state.clone(),
            original: self.original.clone(),
        })
    }
    pub async fn close(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        if self.closed {
            return Ok(CloseReport::default());
        }
        let returned = async {
            loop {
                let changed = self.state.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.state.returned.load(Ordering::Acquire) {
                    return;
                }
                changed.await;
            }
        };
        if tokio::time::timeout_at(deadline, returned).await.is_err() {
            return Ok(CloseReport {
                unfinished: vec![Unfinished::Resource("carrier loan view retained".into())],
                ..Default::default()
            });
        }
        let report = self.control.wait_closed(deadline).await?;
        if report.is_complete() {
            self.closed = true;
        }
        Ok(report)
    }
}
impl LoanClosed {
    pub fn matches(&self, owner: &LoanOwner) -> bool {
        Arc::ptr_eq(&self.state, &owner.state) && self.original.same_original(owner.original())
    }
    pub fn original(&self) -> &CarrierOrigin {
        &self.original
    }
}
impl crate::contract::lifecycle::ChannelControl for LoanOwner {
    fn cancel(&self, reason: CloseReason) {
        LoanOwner::cancel(self, reason);
    }
    async fn wait_closed(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.close(deadline).await
    }
}
impl Drop for LoanOwner {
    fn drop(&mut self) {
        self.cancel(CloseReason::Shutdown);
    }
}
