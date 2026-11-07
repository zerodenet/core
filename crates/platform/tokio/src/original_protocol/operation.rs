//! One persistent original operation. Observer deadlines do not own its future.
use super::*;
use crate::contract::{lifecycle::DynChannelControl, BoxFuture};
use std::task::{Context, Poll, Wake};
#[derive(Default)]
pub(super) struct Operation {
    pub(super) polling: bool,
    pub(super) woken: bool,
    work: Option<BoxFuture<'static, Result<CloseReport, ChannelError>>>,
    first_end: Option<Instant>,
}
pub(super) struct OriginalWake(pub(super) Weak<Phase>);
impl Wake for OriginalWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if let Some(phase) = self.0.upgrade() {
            phase.operation_wake();
        }
    }
}
impl Phase {
    fn operation_wake(&self) {
        let deferred = {
            let mut state = self
                .operation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.polling {
                state.woken = true;
                true
            } else {
                false
            }
        };
        if !deferred {
            self.wake_now();
        }
    }

    /// Only Root callers register their standing host waker separately. The
    /// original operation always polls with a weak proxy to that same Root,
    /// so dropping an observer cannot unregister its cleanup progress.
    pub(crate) fn poll_operation(
        self: &Arc<Self>,
        _cx: &mut Context<'_>,
        end: Instant,
    ) -> Poll<bool> {
        if self.complete() {
            return Poll::Ready(true);
        }
        let mut work = {
            let mut state = self
                .operation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.polling {
                return Poll::Pending;
            }
            state.polling = true;
            let end = *state.first_end.get_or_insert(end);
            state.work.take().unwrap_or_else(|| {
                let original = self.control.clone();
                Box::pin(async move {
                    let mut original = original.lock_owned().await;
                    let control = original.as_mut().expect("unsettled original controller");
                    control.cancel(CloseReason::Shutdown);
                    control.wait_closed(end).await
                })
            })
        };
        let mut reset = polling::PollingReset::new(self);
        let mut context = Context::from_waker(&self.operation_waker);
        // No original/state/Root lock crosses user polling or waker callbacks.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            work.as_mut().poll(&mut context)
        }));
        let (pending_work, complete) = match result {
            Err(payload) => {
                // The exact thrown payload is retained before dropping the
                // poisoned future. Its same unique controller remains in Root;
                // recovery may resume actual state, never repoll this future.
                self.receive(Err(ChannelError::new(
                    crate::contract::Stage::Close,
                    crate::contract::Reason::Internal,
                    crate::contract::Impact::Component,
                )
                .with_source(OriginalPollPanic(Mutex::new(payload)))));
                drop(work);
                (None, Some(false))
            }
            Ok(Poll::Pending) => (Some(work), None),
            Ok(Poll::Ready(result)) => {
                // Drop the completed future/owned control guard first, then
                // persist its exact result before releasing the polling gate.
                drop(work);
                (None, Some(self.receive(result)))
            }
        };
        let woken = {
            let mut state = self
                .operation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.work = pending_work;
            state.polling = false;
            std::mem::take(&mut state.woken)
        };
        reset.finish();
        if woken {
            self.wake_now();
        }
        match complete {
            Some(complete) => {
                if complete {
                    self.changed.notify_waiters();
                }
                Poll::Ready(complete)
            }
            None => Poll::Pending,
        }
    }
    pub(crate) async fn wait(self: &Arc<Self>, end: Instant) -> bool {
        self.request();
        let waiting = async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let result =
                    std::future::poll_fn(|cx| Poll::Ready(self.poll_operation(cx, end))).await;
                if matches!(result, Poll::Ready(true)) {
                    return true;
                }
                changed.await;
            }
        };
        match tokio::time::timeout_at(end, waiting).await {
            Ok(result) => result,
            // This observer owns no protocol fault. The same actual operation
            // remains in Root; a later real error must retain its own source.
            Err(_) => false,
        }
    }
}
