//! Passive same-Scope observation; the journal never joins its own caller.
use super::*;
use crate::contract::{BoxFuture, DeliveryState, Impact, Reason, Stage};
use crate::controlled_carrier::SourceCustody;
use std::{
    sync::atomic::{AtomicBool, Ordering},
    task::Context,
};
#[derive(Default)]
pub(super) struct Waiting {
    future: Mutex<(
        Option<BoxFuture<'static, ()>>,
        Option<BoxFuture<'static, ()>>,
    )>,
    polling: AtomicBool,
    scope_done: AtomicBool,
}
struct Polling<'a>(&'a AtomicBool);
impl Drop for Polling<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}
impl Waiting {
    fn poll(
        &self,
        scope: &ScopeRetirementObservation,
        changed: &Arc<tokio::sync::Notify>,
        cx: &mut Context<'_>,
    ) {
        if self.polling.swap(true, Ordering::AcqRel) {
            return;
        }
        let _polling = Polling(&self.polling);
        let (mut waiting_scope, mut waiting_native) = std::mem::take(
            &mut *self
                .future
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        // Register native-call return before reading its original census. It
        // remains independent of an already drained Scope (including late IO).
        for _ in 0..2 {
            let future = waiting_native.get_or_insert_with(|| {
                let changed = changed.clone();
                Box::pin(async move { changed.notified_owned().await })
            });
            if future.as_mut().poll(cx).is_pending() {
                break;
            }
            waiting_native = None;
        }
        if !self.scope_done.load(Ordering::Acquire) {
            let future = waiting_scope.get_or_insert_with(|| {
                let scope = scope.clone();
                Box::pin(async move { scope.wait_drained().await })
            });
            if future.as_mut().poll(cx).is_ready() {
                self.scope_done.store(true, Ordering::Release);
                waiting_scope = None;
            }
        }
        // Waker clone/wake ran without provider/Scope/Root locks. Panic drops
        // only these passive readers; RAII resets election for an exact retry.
        *self
            .future
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = (waiting_scope, waiting_native);
    }
}
impl SourceCustody for Core {
    fn released(&self) -> bool {
        let closed = match self.state.try_lock() {
            Ok(state) => state.closed.is_some(),
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner().closed.is_some(),
            Err(std::sync::TryLockError::WouldBlock) => false,
        };
        closed
            && self
                .hosted
                .as_ref()
                .is_some_and(|hosted| hosted.scope.is_drained())
    }
    fn poll_retirement(&self, cx: &mut Context<'_>) -> bool {
        let Some(hosted) = &self.hosted else {
            return false;
        };
        hosted.waiting.poll(&hosted.scope, &self.changed, cx);
        if hosted.stop.is_cancelled() || hosted.scope.is_drained() {
            // An active synchronous read/encode keeps State locked. Never
            // close or infer NoNative while its concrete call remains active.
            let _ = self.try_close();
        }
        if self.released() {
            return true;
        }
        self.released()
    }
    fn failure(&self) -> Option<Arc<ChannelError>> {
        let source = {
            let state = match self.state.try_lock() {
                Ok(state) => state,
                Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => return None,
            };
            [&state.lock, &state.read, &state.append, &state.temporary]
                .iter()
                .find_map(|slot| slot.close_failure())
                .or_else(|| state.path_failure.clone())
        }?;
        let mut recorded = self
            .native_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Some(
            recorded
                .get_or_insert_with(|| {
                    let mut error = ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                        .with_source(source);
                    error.component = self.hosted.as_ref().map(|hosted| hosted.instance.component);
                    error.delivery = DeliveryState::Unknown;
                    Arc::new(error)
                })
                .clone(),
        )
    }
}
