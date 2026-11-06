//! Poll the cancelled original in its existing host supervisor. No task, Scope,
//! native provider or admission queue is created by this cleanup continuation.
use super::{owner::close_slot, CarrierOwnerRegistration};
use crate::contract::{lifecycle::CloseReport, BoxFuture, ChannelError};
use std::{
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::time::Instant;

#[derive(Default)]
pub(super) struct Retirement {
    deadline: Option<Instant>,
    polling: bool,
    work: Option<BoxFuture<'static, Result<CloseReport, ChannelError>>>,
    first_fault: Option<Arc<ChannelError>>,
}
impl Retirement {
    pub(super) fn take_closed_work(
        &mut self,
    ) -> Option<BoxFuture<'static, Result<CloseReport, ChannelError>>> {
        self.work.take()
    }
}
impl CarrierOwnerRegistration {
    /// Continue only already-cancelled slots. This cannot cancel a live sibling,
    /// grant I/O, accept a task on behalf of its Scope, or infer closure from Drop.
    /// The first cleanup deadline is immutable across polls and later attempts.
    pub fn poll_cancelled_retirements(
        &self,
        cx: &mut Context<'_>,
        budget: Duration,
    ) -> (bool, Option<Arc<ChannelError>>) {
        let Some(owner) = self.owner.upgrade() else {
            return (false, None);
        };
        let slots = owner.state().slots.clone();
        let mut progressed = false;
        let mut first_fault = None;
        for slot in slots {
            if !slot.cancel.is_cancelled() {
                continue;
            }
            let work = {
                let mut state = lock(&slot.retirement);
                if state.polling {
                    continue;
                }
                state.polling = true;
                let deadline = *state
                    .deadline
                    .get_or_insert_with(|| Instant::now() + budget);
                state.work.take().unwrap_or_else(|| {
                    let original = slot.clone();
                    Box::pin(async move { close_slot(&original, deadline).await })
                })
            };
            let mut work = work;
            let result = work.as_mut().poll(cx);
            let mut retired_work = None;
            let mut complete = None;
            {
                let mut state = lock(&slot.retirement);
                state.polling = false;
                match result {
                    Poll::Pending => state.work = Some(work),
                    Poll::Ready(Ok(report)) => {
                        retired_work = Some(work);
                        if report.is_complete() {
                            complete = Some(report)
                        }
                        // A failed deadline keeps the same original and fixed
                        // deadline. A later real join/return may permit a new
                        // nonwaiting positive settlement; it never grants IO.
                    }
                    Poll::Ready(Err(error)) => {
                        retired_work = Some(work);
                        state.first_fault.get_or_insert_with(|| Arc::new(error));
                    }
                }
                if first_fault.is_none() {
                    first_fault = state.first_fault.clone();
                }
            }
            drop(retired_work);
            if let Some(report) = complete {
                owner.state().confirmed.append(report);
                progressed = true;
            }
        }
        (progressed, first_fault)
    }
}
fn lock<T>(value: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    value
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
