use super::*;
impl FrontendCommand {
    /// Same original frontend Work, no added execution wrapper. Only the
    /// already-existing raw blocking child is registered in the Host Scope.
    pub async fn run_blocking<T, F, C, Fut>(&self, work: F, complete: C) -> Result<T, ChannelError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
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
            // Same concrete pause mutex: a new acquisition cannot become
            // creating after the candidate fenced this raw Scope. Otherwise
            // retirement could await creating while ensure awaits its serial.
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
        let _acquisition = Acquisition(self.0.clone());
        let tasks = owner
            .ensure(book.limits().messages.pending_commands)
            .await?;
        let entry = self.0.clone();
        let stop = ApplyObservation::caller_stop().unwrap_or_else(CancellationToken::new);
        let _ = entry.stop.set(stop.clone());
        let observation = entry.observation.clone();
        let producer = observation.owned_work_producer();
        let (start, gate) = std::sync::mpsc::sync_channel(1);
        let (send, receive) = oneshot::channel();
        let (reply, response) = oneshot::channel();
        let running = entry.clone();
        let callbacks = tasks.defer_callbacks();
        let (_, raw) = tasks
            .spawn_blocking_tracked(TaskClass::Auxiliary, move || {
                let _ = running.raw_id.set(tokio::task::id());
                if gate.recv().is_err() {
                    running.raw_no_work.store(true, Ordering::Release);
                    running.notify();
                    return Ok(());
                }
                let value = tokio::runtime::Handle::current()
                    .block_on(observation.scope_with_stop(stop, async { producer.run_sync(work) }));
                let _ = send.send(value);
                Ok(())
            })
            .map_err(|e| fault(Reason::Capacity).with_source(e))?;
        entry
            .raw
            .set(raw.clone())
            .map_err(|_| fault(Reason::InvalidConfig))?;
        let actual = entry.clone();
        let cleanup = async move {
            let value = receive.await;
            let joined = raw.wait_registration_released().await;
            if let Some(id) = actual.raw_id.get().copied() {
                let failure = joined
                    .result()
                    .err()
                    .and_then(|e| super::super::run::raw_panic(e.as_ref()));
                actual
                    .observation
                    .scope(async {
                        let _ = ApplyObservation::record_blocking_outcome(id, failure);
                    })
                    .await;
            }
            let cleanup = complete(actual.observation.clone()).await;
            let failure = match (joined.result(), cleanup) {
                (Err(raw), Err(cleanup)) => Some(Arc::new(fault(Reason::Internal).with_source(
                    super::super::run::CommandTaskFaults {
                        raw: Some(raw),
                        cleanup: Some(Arc::new(cleanup)),
                    },
                ))),
                (Err(source), _) => Some(source),
                (_, Err(source)) => Some(Arc::new(source)),
                _ => None,
            };
            if let Some(source) = &failure {
                actual.failure.get_or_init(|| source.clone());
            }
            // Native cleanup is positive before this original future returns.
            actual.cleanup_complete.store(true, Ordering::Release);
            let result = match (value, failure) {
                (Ok(value), None) => Ok(value),
                (_, Some(source)) => Err(retained(source)),
                _ => Err(fault(Reason::Internal)),
            };
            let _ = reply.send(result);
            actual.notify();
            Ok(())
        };
        *lock(&entry.cleanup) = Some(Box::pin(cleanup));
        // Row, real witness, outcome receiver and cleanup continuation are all
        // retained by the Host before the original blocking gate opens.
        drop(callbacks);
        entry.observation.handoff_current_producer();
        let _ = start.send(());
        owner.changed.notify_waiters();
        response
            .await
            .map_err(|e| fault(Reason::Internal).with_source(e))?
    }
}
