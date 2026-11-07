use super::*;
impl EmbeddedCommandOwner {
    /// The same finite embedded duty executes native async diagnostics directly.
    /// Auxiliary work and its Critical cleanup retain their actual registrations;
    /// no parent blocking thread waits for a child in that same blocking pool.
    pub async fn run_async<T, F, C, Fut>(
        self: &Arc<Self>,
        budget: Arc<ControlBudget>,
        work: F,
        complete: C,
    ) -> Result<T, ChannelError>
    where
        T: Send + 'static,
        F: Future<Output = T> + Send + 'static,
        C: FnOnce(ApplyObservation) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), ChannelError>> + Send + 'static,
    {
        // The public metadata cell is an observer only. This actual Host row
        // owns the job even when that outer caller is dropped.
        let outer_observation = ApplyObservation::current();
        let caller_stop = ApplyObservation::caller_stop();
        if !Arc::ptr_eq(self, &budget.command_owner()) {
            return Err(fault(Reason::StalePlan));
        }
        let tasks = self
            .ensure(budget.limits().messages.pending_commands)
            .await?;
        let credit = budget
            .reserve(ControlCharge::PendingCommand)
            .map_err(|e| fault(Reason::Capacity).with_source(e))?;
        let observation = ApplyObservation::new();
        let entry = Arc::new(Entry {
            owner: Arc::downgrade(self),
            observation,
            stop: caller_stop
                .map(|parent| parent.child_token())
                .unwrap_or_else(CancellationToken::new),
            raw: OnceLock::new(),
            wrapper: OnceLock::new(),
            raw_id: OnceLock::new(),
            credit: Mutex::new(Some(credit)),
            failure: OnceLock::new(),
            raw_failure: OnceLock::new(),
            wrapper_rejected: std::sync::atomic::AtomicBool::new(false),
        });
        entry.observation.install_embedded(Arc::downgrade(&entry));
        struct Caller(CancellationToken);
        impl Drop for Caller {
            fn drop(&mut self) {
                self.0.cancel();
            }
        }
        let _caller = Caller(entry.stop.clone());
        let (start, gate) = oneshot::channel();
        let (output, receive) = oneshot::channel();
        let (reply, response) = oneshot::channel();
        // Install this exact observation before opening the actual async work gate.
        let observation = entry.observation.clone();
        let stop = entry.stop.clone();
        // The registration/entry gate is lexical: no std MutexGuard or
        // deferred-callback guard may enter the public Send future's await.
        {
            let callbacks = tasks.defer_callbacks();
            let mut state = lock(&self.state);
            if state.closing
                || state.faulted
                || state.pause.is_some()
                || state
                    .entries
                    .iter()
                    .any(|e| e.failure.get().is_some() || e.raw_failure.get().is_some())
            {
                return Err(fault(Reason::Revoked));
            }
            state.entries.push(entry.clone());
            let raw = tasks.spawn_tracked(TaskClass::Auxiliary, async move {
                if gate.await.is_err() {
                    return Ok(());
                }
                let value = observation.scope_with_stop(stop, work).await;
                let _ = output.send(value);
                Ok(())
            });
            let raw = match raw {
                Ok((_, witness)) => witness,
                Err(error) => {
                    state.entries.pop();
                    drop(state);
                    drop(callbacks);
                    return Err(fault(Reason::Capacity).with_source(error));
                }
            };
            let _ = entry.raw.set(raw.clone());
            let owner = self.clone();
            let actual = entry.clone();
            let (ready, begun) = oneshot::channel();
            let wrapper = tasks.spawn_tracked(TaskClass::Critical, async move {
                if begun.await.is_err() {
                    return Ok(());
                }
                let value = receive.await;
                let joined = raw.wait().await;
                // Async registration owns no blocking Tokio ID. Do not fabricate a
                // blocking outcome from this actual Auxiliary task witness.
                if let Some(id) = actual.raw_id.get().copied() {
                    let source = joined.result().err();
                    let panic = source
                        .as_ref()
                        .and_then(|e| super::run::raw_panic(e.as_ref()));
                    let observation = actual.observation.clone();
                    observation
                        .scope(async {
                            let _ = ApplyObservation::record_blocking_outcome(id, panic.clone());
                        })
                        .await;
                    if let Some(observer) = &outer_observation {
                        // Publish the same actual raw outcome; this does not issue
                        // frontend custody, an accepted Apply, or a TaskJoined.
                        observer
                            .scope(async {
                                let _ = ApplyObservation::record_blocking_outcome(id, panic);
                            })
                            .await;
                    }
                }
                let cleanup = complete(actual.observation.clone()).await;
                let result = match (joined.result(), cleanup) {
                    (Err(raw), Err(cleanup)) => Err(Arc::new(fault(Reason::Internal).with_source(
                        super::run::CommandTaskFaults {
                            raw: Some(raw),
                            cleanup: Some(Arc::new(cleanup)),
                        },
                    ))),
                    (Err(raw), _) => Err(raw),
                    (_, Err(cleanup)) => Err(Arc::new(cleanup)),
                    _ => Ok(()),
                };
                if let Err(source) = &result {
                    actual.failure.get_or_init(|| source.clone());
                }
                let response = match value {
                    Ok(value) if result.is_ok() => Ok(value),
                    _ => Err(result
                        .as_ref()
                        .err()
                        .map_or_else(|| fault(Reason::Internal), |e| retained(e.clone()))),
                };
                let _ = reply.send(response);
                owner.changed.notify_waiters();
                result.map_err(retained)
            });
            let wrapper = match wrapper {
                Ok((_, witness)) => witness,
                Err(error) => {
                    // The actual raw registration stays owned even though its gate
                    // never opens. External retirement still consumes its real exit.
                    entry
                        .failure
                        .get_or_init(|| Arc::new(fault(Reason::Capacity).with_source(error)));
                    entry
                        .wrapper_rejected
                        .store(true, std::sync::atomic::Ordering::Release);
                    drop(start);
                    drop(state);
                    drop(callbacks);
                    self.changed.notify_waiters();
                    return Err(fault(Reason::Capacity));
                }
            };
            let _ = entry.wrapper.set(wrapper);
            drop(state);
            drop(callbacks);
            let _ = ready.send(());
            let _ = start.send(());
        }
        response
            .await
            .map_err(|e| fault(Reason::Internal).with_source(e))?
    }
}
