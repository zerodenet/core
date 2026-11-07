use super::*;
struct ScopeCustody<'a> {
    owner: &'a EmbeddedCommandOwner,
    scope: Option<HostedScope>,
    notify: bool,
}
impl Drop for ScopeCustody<'_> {
    fn drop(&mut self) {
        if let Some(scope) = self.scope.take() {
            lock(&self.owner.state).scope = Some(scope);
        }
        if self.notify {
            self.owner.changed.notify_waiters();
        }
    }
}
impl EmbeddedCommandOwner {
    pub(super) async fn monitor(
        self: Arc<Self>,
        scope: HostedScope,
        stop: CancellationToken,
    ) -> Result<(), ChannelError> {
        let mut custody = ScopeCustody {
            owner: self.as_ref(),
            scope: Some(scope),
            notify: true,
        };
        loop {
            self.reclaim();
            let done = stop.is_cancelled() && {
                let state = lock(&self.state);
                if state.scope_retiring {
                    limits::raw_scope_quiescent(&state)
                } else {
                    state.entries.iter().all(|e| lock(&e.credit).is_none())
                        && state.frontends.iter().all(|e| !e.has_credit())
                }
            };
            if done {
                break;
            }
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            tokio::select! { biased;
                receipt = custody.scope.as_mut().unwrap().next_receipt() => {
                    if let Some(receipt) = receipt {
                        self.retain_receipt(&receipt);
                        #[cfg(test)] {
                            let gate = lock(&self.hold_ack).take();
                            if let Some(gate) = gate { let _ = gate.await; }
                        }
                        custody.scope.as_mut().unwrap().acknowledge(&receipt).map_err(|e| fault(Reason::Internal).with_source(e))?;
                    }
                }
                _ = std::future::poll_fn(|cx| self.poll_frontends(cx)) => {},
                _ = &mut changed => {}
                _ = stop.cancelled(), if !stop.is_cancelled() => {}
            }
        }
        // This signals actual Scope cancellation only after all operation rows
        // returned. It is not the monitor's own join/ACK.
        custody.scope.as_mut().unwrap().begin_shutdown();
        drop(custody);
        self.changed.notify_waiters();
        Ok(())
    }
    pub(crate) async fn observe_failure(&self) -> ChannelError {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(failure) = self.original_failure() {
                return failure;
            }
            let scope = {
                let mut state = lock(&self.state);
                if state.starting {
                    None
                } else {
                    state.scope.take()
                }
            };
            if let Some(scope) = scope {
                let original = scope.registrar().scope_identity();
                let mut custody = ScopeCustody {
                    owner: self,
                    scope: Some(scope),
                    notify: true,
                };
                #[cfg(test)]
                {
                    let gate = lock(&self.observation_gate).take();
                    if let Some((entered, gate)) = gate {
                        let _ = entered.send(());
                        let _ = gate.await;
                    }
                }
                if let Some(receipt) = custody.scope.as_mut().unwrap().next_receipt().await {
                    self.retain_receipt(&receipt);
                    if let Err(error) = custody.scope.as_mut().unwrap().acknowledge(&receipt) {
                        return fault(Reason::Internal).with_source(error);
                    }
                    custody.notify = true;
                    drop(custody);
                    continue;
                }
                // A parallel actual close consumer may have observed the
                // temporary lease, so returning even a drained Scope must wake
                // it. Do not immediately retake/notify this empty old Scope.
                drop(custody);
                let resumed = self.changed.notified();
                tokio::pin!(resumed);
                resumed.as_mut().enable();
                if let Some(failure) = self.original_failure() {
                    return failure;
                }
                let replaced = lock(&self.state)
                    .tasks
                    .as_ref()
                    .is_some_and(|tasks| !tasks.scope_identity().same_scope(&original));
                if !replaced {
                    resumed.await;
                }
                continue;
            }
            changed.await;
        }
    }
    fn retain_receipt(&self, receipt: &zero_platform_tokio::contract::scope::HostedExitReceipt) {
        let failure = receipt.result().err();
        if let Some(failure) = failure {
            self.ownership_fault_published();
            let frontend = lock(&self.state)
                .frontends
                .iter()
                .find(|entry| {
                    entry
                        .raw_witness()
                        .is_some_and(|w| w.matches_joined(receipt.joined()))
                })
                .cloned();
            if let Some(frontend) = frontend {
                frontend.record_raw_failure(failure.clone());
                self.changed.notify_waiters();
                return;
            }
            let entry = lock(&self.state)
                .entries
                .iter()
                .find(|entry| {
                    entry
                        .raw
                        .get()
                        .into_iter()
                        .chain(entry.wrapper.get())
                        .any(|w| w.matches_joined(receipt.joined()))
                })
                .cloned();
            if let Some(entry) = entry {
                if entry
                    .raw
                    .get()
                    .is_some_and(|w| w.matches_joined(receipt.joined()))
                {
                    entry.raw_failure.get_or_init(|| failure.clone());
                } else {
                    entry.failure.get_or_init(|| failure.clone());
                }
                self.changed.notify_waiters();
            } else {
                let mut state = lock(&self.state);
                if state
                    .monitor
                    .as_ref()
                    .is_some_and(|w| w.matches_joined(receipt.joined()))
                {
                    state.monitor_failure.get_or_insert(failure);
                }
            }
        }
    }
    fn reclaim(&self) {
        let mut returned = Vec::new();
        let removed = {
            let mut state = lock(&self.state);
            let mut removed = Vec::new();
            let mut index = 0;
            while index < state.entries.len() {
                let entry = &state.entries[index];
                let released = entry.raw.get().is_some_and(released)
                    && (entry.wrapper.get().is_some_and(released)
                        || entry
                            .wrapper_rejected
                            .load(std::sync::atomic::Ordering::Acquire));
                if released {
                    returned.extend(lock(&entry.credit).take());
                    if entry.failure.get().is_none() && entry.raw_failure.get().is_none() {
                        removed.push(state.entries.remove(index));
                        continue;
                    }
                }
                index += 1;
            }
            removed
        };
        // Permit notifications and original error/observation destruction happen
        // after releasing the owner lock, including failed registration paths.
        drop(removed);
        drop(returned);
        self.changed.notify_waiters();
    }
    pub(super) async fn finish_monitor(&self) -> Result<(), ChannelError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let scope = lock(&self.state).scope.take();
            if let Some(scope) = scope {
                let mut custody = ScopeCustody {
                    owner: self,
                    scope: Some(scope),
                    notify: true,
                };
                // Only an external owner consumes the monitor's real exit.
                while let Some(receipt) = custody.scope.as_mut().unwrap().next_receipt().await {
                    self.retain_receipt(&receipt);
                    custody
                        .scope
                        .as_mut()
                        .unwrap()
                        .acknowledge(&receipt)
                        .map_err(|e| fault(Reason::Internal).with_source(e))?;
                }
                drop(custody);
                self.reclaim();
                return Ok(());
            }
            if lock(&self.state).tasks.is_none() {
                return Ok(());
            }
            changed.await;
        }
    }
}
fn released(witness: &TaskJoinWitness) -> bool {
    witness
        .try_joined()
        .is_some_and(|j| j.registration_released())
}
