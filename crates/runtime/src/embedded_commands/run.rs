use super::*;
impl EmbeddedCommandOwner {
    /// One exact embedded command. The result channel is not its ownership:
    /// dropping the caller only signals stop; the installed row and both real
    /// registrations survive through completion, native cleanup and Host ACK.
    pub async fn run<T, F, C, Fut>(
        self: &Arc<Self>,
        budget: Arc<ControlBudget>,
        work: F,
        complete: C,
    ) -> Result<T, ChannelError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
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
        let (start, gate) = std::sync::mpsc::sync_channel(1);
        let (output, receive) = oneshot::channel();
        let (reply, response) = oneshot::channel();
        // Capture the same original observation before handing it to blocking.
        let observation = entry.observation.clone();
        let stop = entry.stop.clone();
        let raw_entry = entry.clone();
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
            let raw = tasks.spawn_blocking_tracked(TaskClass::Auxiliary, move || {
                let _ = raw_entry.raw_id.set(tokio::task::id());
                if gate.recv().is_err() {
                    return Ok(());
                }
                let value = tokio::runtime::Handle::current().block_on(
                    observation.scope_with_stop(stop, async {
                        let guard =
                            ApplyObservation::capture().expect("same observed blocking invocation");
                        guard.run_sync(work)
                    }),
                );
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
                // Raw native exit and Scope identity stay separate from the Tokio
                // blocking ID; only a genuine JoinError carries a blocking failure.
                if let Some(id) = actual.raw_id.get().copied() {
                    let source = joined.result().err();
                    let panic = source.as_ref().and_then(|e| raw_panic(e.as_ref()));
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
                        CommandTaskFaults {
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
pub(super) fn raw_panic(
    mut source: &(dyn std::error::Error + 'static),
) -> Option<Arc<tokio::task::JoinError>> {
    loop {
        if let Some(actual) =
            source.downcast_ref::<zero_platform_tokio::contract::scope::TaskJoinError>()
        {
            return Some(actual.original().clone());
        }
        if let Some(actual) = source.downcast_ref::<Arc<tokio::task::JoinError>>() {
            return Some(actual.clone());
        }
        source = source.source()?;
    }
}
#[derive(Debug)]
pub struct CommandTaskFaults {
    pub(super) raw: Option<Arc<ChannelError>>,
    pub(super) cleanup: Option<Arc<ChannelError>>,
}
impl std::fmt::Display for CommandTaskFaults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "embedded raw/cleanup failed: {:?}; {:?}",
            self.raw, self.cleanup
        )
    }
}
impl std::error::Error for CommandTaskFaults {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.raw.as_ref().map(|e| e.as_ref() as _)
    }
}

impl CommandTaskFaults {
    pub fn raw(&self) -> Option<&Arc<ChannelError>> {
        self.raw.as_ref()
    }
    pub fn cleanup(&self) -> Option<&Arc<ChannelError>> {
        self.cleanup.as_ref()
    }
}
