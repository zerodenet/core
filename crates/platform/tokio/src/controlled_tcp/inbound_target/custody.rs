//! The same existing Host pump retains this concrete graph if its waiter goes.
use super::*;
use std::task::Context;
impl SourceCustody for Original {
    fn instance(&self) -> Option<zero_core::component::ComponentInstanceId> {
        Some(self.core.pool.instance)
    }
    fn released(&self) -> bool {
        if !self.core.closed.load(Ordering::Acquire) || !self.core.acked() {
            return false;
        }
        let Ok(mut state) = self.core.state.try_lock() else {
            return false;
        };
        if state.first_fault.is_some() && !self.core.reported_fault.load(Ordering::Acquire) {
            return false;
        }
        let permits = std::mem::take(&mut state.idle_permits);
        drop(state);
        // Returning a semaphore permit wakes external waiters, never under
        // this original's state lock or the Root bookkeeping lock.
        drop(permits);
        true
    }
    fn poll_retirement(&self, cx: &mut Context<'_>) -> bool {
        if !self.core.stop.is_cancelled() {
            return false;
        }
        let Some(_polling) =
            crate::original_poll::OriginalPoll::enter(&self.polling, &self.repoll, cx.waker())
        else {
            return false;
        };
        let mut future = self
            .waiting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| {
                let core = self.core.clone();
                Box::pin(async move {
                    core.clone().progress().await;
                    if let Some(driver) = core.driver.get() {
                        driver.wait_registration_released().await;
                    }
                })
            });
        let ready = future.as_mut().poll(cx).is_ready();
        if !ready {
            *self
                .waiting
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(future);
        }
        ready && self.released()
    }
    fn failure(&self) -> Option<Arc<ChannelError>> {
        self.core
            .state
            .try_lock()
            .ok()
            .and_then(|state| state.first_fault.clone())
    }
}
