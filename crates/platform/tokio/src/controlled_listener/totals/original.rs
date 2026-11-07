//! Existing Host polls this exact Source native and publication continuation.
use super::*;
impl SourceCustody for Original {
    fn instance(&self) -> Option<zero_core::component::ComponentInstanceId> {
        self.owner.originals.get().map(|binding| binding.instance)
    }
    fn released(&self) -> bool {
        if !self.attempt.native_complete() {
            return false;
        }
        if self.attempt.failed_return.load(Ordering::Acquire)
            && !self.attempt.published.load(Ordering::Acquire)
        {
            return true;
        }
        (!self.attempt.published.load(Ordering::Acquire)
            || self.attempt.view_returned.load(Ordering::Acquire))
            && self.scope.is_drained()
    }
    fn poll_retirement(&self, cx: &mut Context<'_>) -> bool {
        let Some(_polling) =
            crate::original_poll::OriginalPoll::enter(&self.polling, &self.repoll, cx.waker())
        else {
            return false;
        };
        let mut progress = false;
        if self.owner.cancel.is_cancelled()
            && !self.attempt.native_complete()
            && !self.native_finished.load(Ordering::Acquire)
        {
            // Same actual native controller/Shared, polled by the existing Host,
            // not another task or owner. Keep its one bounded close continuation.
            let end = *self
                .deadline
                .get_or_init(|| tokio::time::Instant::now() + std::time::Duration::from_secs(5));
            let mut native = self
                .native
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .unwrap_or_else(|| {
                    let shared = self.owner.clone();
                    Box::pin(async move {
                        let mut control = super::control::ControlledTcpListenerControl {
                            shared,
                            reported: false,
                        };
                        crate::contract::lifecycle::ChannelControl::wait_closed(&mut control, end)
                            .await
                    })
                });
            match native.as_mut().poll(cx) {
                Poll::Pending => {
                    *self
                        .native
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(native);
                }
                Poll::Ready(result) => {
                    self.native_finished.store(true, Ordering::Release);
                    progress = true;
                    if let Err(source) = result {
                        self.first_fault
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .get_or_insert_with(|| Arc::new(source));
                    }
                }
            }
        }
        if self.users_finished.load(Ordering::Acquire) {
            return progress;
        }
        let mut waiting = self
            .waiting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| {
                let scope = self.scope.clone();
                let attempt = self.attempt.clone();
                Box::pin(async move {
                    loop {
                        let changed = attempt.changed.notified();
                        tokio::pin!(changed);
                        changed.as_mut().enable();
                        if !attempt.published.load(Ordering::Acquire)
                            || attempt.view_returned.load(Ordering::Acquire)
                        {
                            break;
                        }
                        changed.await;
                    }
                    scope.wait_drained().await;
                })
            });
        let ready = waiting.as_mut().poll(cx).is_ready();
        if !ready {
            *self
                .waiting
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(waiting);
        }
        if ready {
            self.users_finished.store(true, Ordering::Release);
        }
        ready || progress
    }
    fn failure(&self) -> Option<Arc<ChannelError>> {
        let source = self
            .owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .error
            .clone();
        if let Some(source) = source {
            self.first_fault
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_or_insert_with(|| {
                    let mut failure =
                        ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                            .with_source(source);
                    failure.component = Some(
                        self.owner
                            .originals
                            .get()
                            .expect("bound original")
                            .instance
                            .component,
                    );
                    failure.delivery = DeliveryState::Unknown;
                    Arc::new(failure)
                });
        }
        self.first_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}
