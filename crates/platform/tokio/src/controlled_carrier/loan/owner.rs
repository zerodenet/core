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
    pub(super) related: Option<Box<super::related::Cleanup>>,
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
            related: None,
        }
    }
    pub fn original(&self) -> &CarrierOrigin {
        &self.original
    }
    pub fn cancel(&self, reason: CloseReason) {
        self.state.cancel.cancel();
        self.control.cancel(reason);
        if let Some(related) = &self.related {
            related.token.parent.request_retirement();
        }
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
        if let Some(related) = &mut self.related {
            if !related.protocol_closed {
                let report = self.control.wait_closed(deadline).await?;
                if !report.is_complete() {
                    return Ok(report);
                }
                // Persist the same child receipts before the parent await.
                related.pending.append(report);
                related.protocol_closed = true;
            }
            let report = Box::pin(related.token.parent.retire(deadline)).await?;
            if !report.is_complete() {
                return Ok(report);
            }
            if !related
                .token
                .parent
                .closed()
                .is_some_and(|receipt| receipt.matches(&related.token.parent))
                || !related
                    .token
                    .parent
                    .users_closed()
                    .is_some_and(|receipt| receipt.matches(&related.token.parent))
            {
                return Err(crate::controlled_carrier::error(
                    related.token.parent.instance(),
                    crate::contract::Stage::Close,
                    crate::contract::Reason::Internal,
                    crate::contract::DeliveryState::Unknown,
                ));
            }
            related.pending.append(report);
            self.closed = true;
            return Ok(std::mem::take(&mut related.pending));
        }
        let report = self.control.wait_closed(deadline).await?;
        self.closed = report.is_complete();
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
