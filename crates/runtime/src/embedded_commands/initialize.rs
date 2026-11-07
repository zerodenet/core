//! Lazy initialization of this one concrete Host control duty.
use super::*;
impl EmbeddedCommandOwner {
    pub(super) async fn ensure(self: &Arc<Self>, limit: usize) -> Result<TaskRegistrar, ChannelError> {
        let _serial = self.serial.lock().await;
        self.validate_limit(limit)
            .map_err(|e| fault(Reason::Capacity).with_source(e))?;
        let rollover = {
            let state = lock(&self.state);
            state.limit != 0 && state.limit != limit
        };
        if rollover {
            let stop = lock(&self.state).monitor_stop.clone();
            stop.cancel();
            self.changed.notify_waiters();
            self.finish_monitor().await?;
            let mut state = lock(&self.state);
            let old_scope = state.scope.take();
            let old_tasks = state.tasks.take();
            state.monitor = None;
            drop(state);
            drop(old_scope);
            drop(old_tasks);
        }
        if let Some(error) = self.original_failure() {
            return Err(error);
        }
        {
            let state = lock(&self.state);
            if state.closing || state.faulted || state.pause.is_some() {
                return Err(fault(Reason::Revoked));
            }
            if let Some(tasks) = &state.tasks {
                return Ok(tasks.clone());
            }
        }
        self.start_scope(limit)
    }
    pub(super) fn ensure_now(self: &Arc<Self>, limit: usize) -> Result<(), ChannelError> {
        let _serial = self
            .serial
            .try_lock()
            .map_err(|_| fault(Reason::Capacity))?;
        {
            let state = lock(&self.state);
            if state.closing || state.faulted || state.pause.is_some() {
                return Err(fault(Reason::Revoked));
            }
            if state.tasks.is_some() {
                return if state.limit == limit {
                    Ok(())
                } else {
                    Err(fault(Reason::Capacity))
                };
            }
        }
        self.start_scope(limit).map(|_| ())
    }
    fn start_scope(self: &Arc<Self>, limit: usize) -> Result<TaskRegistrar, ChannelError> {
        let declared = limit
            .checked_mul(2)
            .and_then(|n| n.checked_add(1))
            .and_then(NonZeroUsize::new)
            .ok_or_else(|| fault(Reason::Capacity))?;
        let runtime = match self.runtime.get() {
            Some(runtime) => runtime.clone(),
            None => {
                let runtime = tokio::runtime::Handle::try_current()
                    .map_err(|e| fault(Reason::Internal).with_source(e))?;
                let _ = self.runtime.set(runtime.clone());
                runtime
            }
        };
        let mut scope = TaskScope::suspended_hosted(runtime, Some(declared));
        scope
            .enable_registration()
            .map_err(|e| fault(Reason::Revoked).with_source(e))?;
        let tasks = scope.registrar();
        {
            let mut state = lock(&self.state);
            state.starting = true;
            state.scope = Some(scope);
            state.tasks = Some(tasks.clone());
            state.limit = limit;
        }
        let (start, begun) = oneshot::channel();
        let owner = self.clone();
        let stop = CancellationToken::new();
        let monitor_stop = stop.clone();
        let spawned = tasks.spawn_tracked(TaskClass::Critical, async move {
            let Ok(scope) = begun.await else {
                return Ok(());
            };
            owner.monitor(scope, monitor_stop).await
        });
        let (_, witness) = match spawned {
            Ok(actual) => actual,
            Err(error) => {
                let source = Arc::new(fault(Reason::Capacity).with_source(error));
                let mut scope = {
                    let mut state = lock(&self.state);
                    state.starting = false;
                    state.faulted = true;
                    state.monitor_failure.get_or_insert_with(|| source.clone());
                    state.scope.take().expect("same unpublished control Scope")
                };
                // The exact failed registration created no monitor. Its actual
                // empty Scope closes outside the owner lock, never by Drop.
                scope.begin_shutdown();
                lock(&self.state).scope = Some(scope);
                self.changed.notify_waiters();
                return Err(retained(source));
            }
        };
        {
            let mut state = lock(&self.state);
            state.tasks = Some(tasks.clone());
            state.monitor = Some(witness);
            state.monitor_stop = stop;
            state.limit = limit;
        }
        // Owner and real monitor witness are installed before it receives the Scope.
        let scope = lock(&self.state)
            .scope
            .take()
            .expect("same installed cold control Scope");
        if let Err(mut scope) = start.send(scope) {
            scope.begin_shutdown();
            let source = Arc::new(fault(Reason::Internal));
            let mut state = lock(&self.state);
            state.scope = Some(scope);
            state.starting = false;
            state.faulted = true;
            state.monitor_failure.get_or_insert_with(|| source.clone());
            drop(state);
            self.changed.notify_waiters();
            return Err(retained(source));
        }
        lock(&self.state).starting = false;
        self.changed.notify_waiters();
        Ok(tasks)
    }
}
