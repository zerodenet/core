//! Main polls this same Source owner while its original application is pinned.
use super::*;
use std::future::Future;

struct Polling<'a> {
    owner: &'a Execution,
    standard: Option<standard::Actor>,
    scoped: Option<scoped::Actor>,
    slot: Option<task::Slot>,
    standard_failure: Option<standard::FailureSlot>,
}
impl<'a> Polling<'a> {
    fn take(owner: &'a Execution) -> Option<Self> {
        let mut state = owner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.polling || state.creating {
            return None;
        }
        state.polling = true;
        Some(Self {
            owner,
            standard: state.standard.take(),
            scoped: state.scoped.take(),
            slot: state.slot.take(),
            standard_failure: state.standard_failure.take(),
        })
    }
}
impl Drop for Polling<'_> {
    fn drop(&mut self) {
        let mut state = self
            .owner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.standard = self.standard.take();
        state.scoped = self.scoped.take();
        state.slot = self.slot.take();
        state.standard_failure = self.standard_failure.take();
        state.polling = false;
    }
}
impl OwnedQuotaStorage {
    pub(super) fn poll_writer(&self, cx: &mut Context<'_>) -> io::Result<()> {
        let Some(mut current) = Polling::take(&self.execution) else {
            return Ok(());
        };
        let (tasks, producer, gate, stopping) = {
            let state = self
                .execution
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            (
                state.tasks.clone(),
                state.producer.clone(),
                state.gate.clone(),
                state.stopping,
            )
        };
        if let Some(worker) = current.standard.as_mut() {
            if tasks.is_some() || stopping {
                worker.signal.shutdown();
            }
            if !worker.is_finished() {
                return Ok(());
            }
            worker.collect(
                current
                    .standard_failure
                    .as_mut()
                    .expect("prepared std fault custody"),
                self.retained_lease_failure(),
            )?;
            // Actual std.join and matching originals, never a Scope watcher.
            if let Some(Err(original)) = &worker.joined {
                self.receive_worker_failure(original.clone(), false)?;
                let mut state = self
                    .execution
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                state.terminal.get_or_insert_with(|| original.clone());
                drop(state);
                self.execution.changed.notify_waiters();
            }
            self.confirm_closed()?;
            if let Some(source) = self.source.upgrade() {
                source.io.ensure_available()?;
            }
            if !worker.collect_signal() {
                return Ok(());
            }
            if tasks.is_none()
                || self
                    .execution
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .terminal
                    .is_some()
            {
                return Ok(());
            }
            let retired = current.standard.take();
            drop(retired);
            self.execution
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .final_done = false;
        }
        let (configured, terminal) = {
            let state = self
                .execution
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            (state.producer.is_some(), state.terminal.is_some())
        };
        if stopping
            && current.standard.is_none()
            && current.scoped.is_none()
            && (!configured || terminal)
        {
            // No task has native custody; cold failure/unused storage still
            // consumes the same actual lease close instead of leaking it.
            self.close_lease()?;
            return Ok(());
        }
        let Some(tasks) = tasks else {
            return Ok(());
        };
        let Some(producer) = producer else {
            return Ok(());
        };
        let gate = gate.ok_or_else(|| io::Error::other("quota controller gate missing"))?;
        if current.scoped.is_none() {
            let slot = current
                .slot
                .take()
                .ok_or_else(|| io::Error::other("quota task liability missing"))?;
            current.scoped = Some(scoped::Actor::new(tasks, slot)?);
        }
        let actor = current.scoped.as_mut().unwrap();
        actor.progress(self, cx)?;
        let (pending, final_done, terminal, ready_at) = {
            let state = self
                .execution
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            (
                state.pending,
                state.final_done,
                state.terminal.is_some(),
                state.ready_at,
            )
        };
        if terminal || (stopping && final_done && actor.idle()) {
            actor.seal();
            actor.progress(self, cx)?;
            return Ok(());
        }
        if !actor.idle() || (!pending && !stopping) {
            return Ok(());
        }
        // Preserve the genuine writer's bounded batching interval. Dirty
        // notification wakes supervision, not a new fsync for every packet.
        // Final flush never waits for ordinary debounce.
        if !stopping && ready_at.is_some_and(|at| std::time::Instant::now() < at) {
            return Ok(());
        }
        let Some(cell) = self.execution.take_write_cell(stopping)? else {
            if !stopping {
                self.execution
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .ready_at = Some(std::time::Instant::now() + Duration::from_millis(50));
            }
            return Ok(());
        };
        {
            let mut state = self
                .execution
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.pending = false;
            state.ready_at = None;
        }
        match actor.launch(self, producer, gate, cell, stopping) {
            Ok(()) => Ok(()),
            Err((error, cell)) => {
                let returned = {
                    let mut state = self
                        .execution
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    state.pending = true;
                    if !stopping {
                        state.ready_at =
                            Some(std::time::Instant::now() + Duration::from_millis(50));
                    }
                    let cells = if stopping {
                        &mut state.final_cells
                    } else {
                        &mut state.business
                    };
                    if let Some(slot) = cells.iter_mut().find(|slot| slot.is_none()) {
                        *slot = Some(cell);
                        None
                    } else {
                        Some(cell)
                    }
                };
                drop(returned);
                if error.kind() == io::ErrorKind::WouldBlock {
                    Ok(())
                } else {
                    Err(error)
                }
            }
        }
    }
}
impl ConfigurationSource {
    /// Inline progress only: this future never spawns a monitor or owns a
    /// second Scope/Host. It continues through startup/reload/shutdown awaits.
    pub async fn drive_quota_writer<T>(&self, operation: impl Future<Output = T>) -> T {
        tokio::pin!(operation);
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let storage = self
                .quota
                .current
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if let Some(owned::StorageEntry::Owned(storage)) = &storage {
                let result = std::future::poll_fn(|cx| {
                    let result = storage.poll_writer(cx);
                    Poll::Ready(result)
                })
                .await;
                if let Err(error) = result {
                    storage.execution.receive_driver_failure(error);
                }
            }
            let ready_at = match &storage {
                Some(owned::StorageEntry::Owned(storage)) => {
                    let state = storage
                        .execution
                        .state
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    if state.standard.is_none()
                        && state.scoped.as_ref().is_some_and(scoped::Actor::idle)
                        && state.pending
                        && !state.stopping
                        && state.terminal.is_none()
                    {
                        state.ready_at
                    } else {
                        None
                    }
                }
                _ => None,
            };
            tokio::select! { biased;
                _ = async {
                    match ready_at {
                        Some(at) => tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {},
                result = &mut operation => return result,
                _ = tick.tick() => {},
                _ = async {
                    match &storage {
                        Some(owned::StorageEntry::Owned(storage)) => storage.execution.changed.notified().await,
                        _ => std::future::pending::<()>().await,
                    }
                } => {},
            }
        }
    }
}
