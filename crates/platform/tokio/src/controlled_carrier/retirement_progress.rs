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
    removed: bool,
    work: Option<BoxFuture<'static, Result<CloseReport, ChannelError>>>,
    first_fault: Option<Arc<ChannelError>>,
    // Same original slot's one read-only credit waiter survives its immutable
    // cleanup deadline. A late real ACK still wakes the existing Host driver.
    credit: Option<BoxFuture<'static, crate::contract::scope::TaskJoined>>,
}
impl Retirement {
    pub(super) fn take_closed_work(
        &mut self,
    ) -> (
        Option<BoxFuture<'static, Result<CloseReport, ChannelError>>>,
        Option<BoxFuture<'static, crate::contract::scope::TaskJoined>>,
    ) {
        self.removed = true;
        (self.work.take(), self.credit.take())
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
            let caller = super::caller_credit::caller(&slot);
            let (work, credit) = {
                let mut state = lock(&slot.retirement);
                if state.polling || state.removed {
                    continue;
                }
                state.polling = true;
                let deadline = *state
                    .deadline
                    .get_or_insert_with(|| Instant::now() + budget);
                let work = state.work.take().unwrap_or_else(|| {
                    let original = slot.clone();
                    Box::pin(async move { close_slot(&original, deadline).await })
                });
                let credit = state.credit.take().or_else(|| {
                    caller.map(|caller| {
                        Box::pin(async move { caller.wait_registration_released().await })
                            as BoxFuture<'static, crate::contract::scope::TaskJoined>
                    })
                });
                (work, credit)
            };
            let mut credit = credit;
            let credit_released = credit
                .as_mut()
                .is_none_or(|credit| credit.as_mut().poll(cx).is_ready());
            let mut work = work;
            let result = work.as_mut().poll(cx);
            let mut retired_work = None;
            let mut retired_credit = None;
            let mut complete = None;
            {
                let mut state = lock(&slot.retirement);
                state.polling = false;
                if !credit_released && !state.removed {
                    state.credit = credit.take();
                }
                match result {
                    // A concurrent explicit positive removal can occur while
                    // this continuation is polling outside the lock. Never
                    // reinstall its self-retaining future after that removal.
                    Poll::Pending if state.removed => retired_work = Some(work),
                    Poll::Pending => state.work = Some(work),
                    Poll::Ready(Ok(report)) => {
                        retired_work = Some(work);
                        if report.is_complete() {
                            retired_credit = state.credit.take();
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
            drop(retired_credit);
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
