//! Publish this same actual registration before its original worker can do IO.
use super::*;
impl<T: TaskOutput> ScopedSourceTasks<T> {
    pub(super) fn spawn(
        &self,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Result<TaskId, ChannelError> {
        self.spawn_with_witness(|_| future)
    }
    pub(super) fn spawn_with_witness<F, Fut>(&self, make: F) -> Result<TaskId, ChannelError>
    where
        F: FnOnce(TaskJoinWitness) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
    {
        self.reap_released();
        let scope = self
            .inner
            .scope
            .get()
            .ok_or_else(|| error(Reason::InvalidConfig))?;
        let host_stop = scope.cancellation().ok_or_else(|| error(Reason::Revoked))?;
        let record = Arc::new(Record {
            witness: OnceLock::new(),
            output: Mutex::new(None),
            delivered: std::sync::atomic::AtomicBool::new(false),
        });
        {
            let mut state = self.inner.state.lock().unwrap();
            if state.closing {
                return Err(error(Reason::Revoked));
            }
            if state.records.len() >= self.inner.limit {
                return Err(error(Reason::Capacity));
            }
            state.records.push(record.clone());
        }
        let output = record.clone();
        let owner = self.inner.clone();
        let registered = scope.spawn_tracked(self.inner.class, async move {
            // The actual body can be polled before spawn_tracked returns.
            // Its same record/witness must be persistent before native query
            // submission. This wait adds no worker or fabricated task receipt.
            let witness = loop {
                let changed = owner.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if let Some(witness) = output.witness.get() {
                    break witness.clone();
                }
                tokio::select! { biased;
                    _ = host_stop.cancelled() => return Ok(()),
                    _ = owner.stop.cancelled() => return Ok(()),
                    _ = changed => {},
                }
            };
            let mut result = tokio::select! {biased;
                _=host_stop.cancelled()=>return Ok(()),
                _=owner.stop.cancelled()=>return Ok(()),
                output=make(witness)=>output,
            };
            // Expected Auxiliary outputs belong to their original receiver.
            // Critical driver errors also reach the actual Scope supervisor.
            let failure = if owner.class == TaskClass::Critical {
                result.preserve_failure()
            } else {
                None
            };
            *output.output.lock().unwrap() = Some(result);
            owner.changed.notify_waiters();
            failure.map_or(Ok(()), |source| {
                Err(error(Reason::Io).with_source(Reported(source)))
            })
        });
        match registered {
            Ok((id, witness)) => {
                record
                    .witness
                    .set(witness)
                    .expect("one actual Source worker registration");
                self.inner.changed.notify_waiters();
                Ok(id)
            }
            Err(source) => {
                let reason = match source {
                    zero_platform_tokio::contract::scope::SpawnError::Capacity => Reason::Capacity,
                    _ => Reason::Revoked,
                };
                let mut state = self.inner.state.lock().unwrap();
                state.records.retain(|entry| !Arc::ptr_eq(entry, &record));
                // No task was accepted or polled. Capacity denial grants no
                // replacement credit and is not a sticky cleanup fault.
                if reason != Reason::Capacity {
                    state.closing = true;
                }
                drop(state);
                Err(ChannelError::new(Stage::Prepare, reason, Impact::Component)
                    .with_source(source))
            }
        }
    }
}
