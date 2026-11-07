//! Store this original async work before awaiting it. The existing Host receipt
//! monitor owns continuation on caller loss; no frontend execution task is added.
use super::*;
use futures_util::FutureExt;
impl FrontendCommand {
    pub async fn run_async<T, F, C, Fut>(&self, work: F, complete: C) -> Result<T, ChannelError>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
        C: FnOnce(ApplyObservation) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), ChannelError>> + Send + 'static,
    {
        let owner = self
            .0
            .owner
            .upgrade()
            .ok_or_else(|| fault(Reason::Revoked))?;
        let book = owner
            .book
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| fault(Reason::Revoked))?;
        if !self.0.matches_current(&book) {
            return Err(fault(Reason::StalePlan));
        }
        {
            let state = lock(&owner.state);
            if state.closing || state.faulted || state.pause.is_some() {
                return Err(fault(Reason::Revoked));
            }
            if self.0.raw.get().is_some()
                || self.0.async_work.load(Ordering::Acquire)
                || self.0.creating.swap(true, Ordering::AcqRel)
            {
                return Err(fault(Reason::InvalidConfig));
            }
        }
        struct Acquisition(Arc<Original>);
        impl Drop for Acquisition {
            fn drop(&mut self) {
                self.0.creating.store(false, Ordering::Release);
                self.0.notify();
            }
        }
        let acquisition = Acquisition(self.0.clone());
        owner
            .ensure(book.limits().messages.pending_commands)
            .await?;
        let entry = self.0.clone();
        let stop = ApplyObservation::caller_stop()
            .map(|parent| parent.child_token())
            .unwrap_or_else(CancellationToken::new);
        let _ = entry.stop.set(stop.clone());
        struct Caller(CancellationToken);
        impl Drop for Caller {
            fn drop(&mut self) {
                self.0.cancel();
            }
        }
        let _caller = Caller(stop.clone());
        let (reply, response) = oneshot::channel();
        let producer = entry.observation.owned_work_producer();
        let original = entry.clone();
        let cleanup = async move {
            let value = std::panic::AssertUnwindSafe(async {
                let _producer = producer;
                original
                    .observation
                    .scope_with_stop(stop.clone(), work)
                    .await
            })
            .catch_unwind()
            .await;
            // Move the actual payload into a typed source before any cleanup
            // await. It is not a made-up blocking JoinError or TaskFailure.
            let (value, mut failure) = match value {
                Ok(value) => (Some(value), None),
                Err(payload) => {
                    let source = super::super::async_panic::caught(payload);
                    stop.cancel();
                    original.raw_failure.get_or_init(|| source.clone());
                    original.ownership_fault_published();
                    (None, Some(source))
                }
            };
            let cleaned = std::panic::AssertUnwindSafe(async {
                complete(original.observation.clone()).await
            })
            .catch_unwind()
            .await;
            match cleaned {
                Ok(result) => {
                    if let Err(source) = result {
                        let source = Arc::new(source);
                        failure = Some(match failure {
                            Some(work) => Arc::new(fault(Reason::Internal).with_source(
                                super::super::run::CommandTaskFaults {
                                    raw: Some(work),
                                    cleanup: Some(source),
                                },
                            )),
                            None => source,
                        });
                    }
                    original.cleanup_complete.store(true, Ordering::Release);
                }
                Err(payload) => {
                    let source = super::super::async_panic::caught(payload);
                    failure = Some(match failure {
                        Some(work) => Arc::new(fault(Reason::Internal).with_source(
                            super::super::run::CommandTaskFaults {
                                raw: Some(work),
                                cleanup: Some(source),
                            },
                        )),
                        None => source,
                    });
                    // Unknown cleanup remains owned. Never repoll a panicked
                    // cleanup or refund solely because its body unwound.
                    original.poisoned.store(true, Ordering::Release);
                }
            }
            if let Some(source) = &failure {
                if !original
                    .raw_failure
                    .get()
                    .is_some_and(|raw| Arc::ptr_eq(raw, source))
                {
                    original.failure.get_or_init(|| source.clone());
                }
                original.ownership_fault_published();
            }
            let result = match (value, failure) {
                (Some(value), None) => Ok(value),
                (_, Some(source)) => Err(retained(source)),
                _ => Err(fault(Reason::Internal)),
            };
            let _ = reply.send(result);
            original.notify();
            Ok(())
        };
        *lock(&entry.cleanup) = Some(Box::pin(cleanup));
        entry.async_work.store(true, Ordering::Release);
        drop(acquisition);
        entry.observation.handoff_current_producer();
        owner.changed.notify_waiters();
        response
            .await
            .map_err(|source| fault(Reason::Internal).with_source(source))?
    }
}
