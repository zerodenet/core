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
    first_close_failure: Option<Arc<ChannelError>>,
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
            first_close_failure: None,
            related: None,
        }
    }
    pub fn original(&self) -> &CarrierOrigin {
        &self.original
    }
    /// The actual logical owner retains this stable fault record independently
    /// of a cancelled close waiter and of positive native/Scope observations.
    pub fn first_close_failure(&self) -> Option<&Arc<ChannelError>> {
        self.first_close_failure.as_ref()
    }
    fn receive_close_result(
        &mut self,
        result: Result<CloseReport, ChannelError>,
    ) -> Result<CloseReport, ChannelError> {
        let Err(source) = result else {
            return result;
        };
        let Some(receipt) = source.cleanup_failure_receipt().cloned() else {
            return Err(source);
        };
        let actual = Arc::new(source);
        let retained = self
            .first_close_failure
            .get_or_insert_with(|| actual.clone());
        if retained
            .cleanup_failure_receipt()
            .is_some_and(|old| old.same_receipt(&receipt))
        {
            // This same owner now strongly retains the returned error and its
            // stable fault payload, including late secondary causes. The ACK
            // changes no native, view, task or registration-close requirement.
            receipt.acknowledge_retained(retained);
            return Err(ChannelError::retained(retained.clone()));
        }
        // A foreign receipt cannot overwrite this actual owner's first fault
        // record or claim local delivery. Its producer keeps that obligation.
        Err(ChannelError::retained(actual))
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
    /// Cleanup only work that has really returned; retain the same control on
    /// Pending or error. Related control custody still closes child before parent.
    pub fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        self.cancel(CloseReason::Shutdown);
        if self.closed {
            return Ok(Default::default());
        }
        if !self.state.returned.load(Ordering::Acquire) {
            return Ok(CloseReport {
                unfinished: vec![Unfinished::Resource("carrier loan view retained".into())],
                ..Default::default()
            });
        }
        if self
            .related
            .as_ref()
            .is_some_and(|related| !related.protocol_closed)
        {
            let result = self.control.try_close();
            let report = self.receive_close_result(result)?;
            if !report.is_complete() {
                return Ok(report);
            }
            let related = self.related.as_mut().unwrap();
            related.pending.append(report);
            related.protocol_closed = true;
        }
        if let Some(related) = &mut self.related {
            let report = related.token.parent.try_retire()?;
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
        let result = self.control.try_close();
        let report = self.receive_close_result(result)?;
        self.closed = report.is_complete();
        Ok(report)
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
        if self
            .related
            .as_ref()
            .is_some_and(|related| !related.protocol_closed)
        {
            let result = self.control.wait_closed(deadline).await;
            let report = self.receive_close_result(result)?;
            if !report.is_complete() {
                return Ok(report);
            }
            let related = self.related.as_mut().unwrap();
            // Persist the same child receipts before the parent await.
            related.pending.append(report);
            related.protocol_closed = true;
        }
        if let Some(related) = &mut self.related {
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
        let result = self.control.wait_closed(deadline).await;
        let report = self.receive_close_result(result)?;
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
    fn try_close(&mut self) -> Result<CloseReport, ChannelError> {
        LoanOwner::try_close(self)
    }

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
