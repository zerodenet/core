//! The same finite per-connection command jobs retain all actual results.
use super::*;
use std::future::Future;
use tokio::task::JoinSet;
pub(super) struct ConcurrentCommandTasks {
    tasks: JoinSet<io::Result<()>>,
    stop: CancellationToken,
    failures: Vec<io::Error>,
    budget: Arc<ControlBudget>,
    retained: std::collections::HashMap<tokio::task::Id, CommandCredits>,
    local: zero_runtime::configuration::ControlConnectionBudget,
    #[cfg(test)]
    _original: Option<ControlBudgetPermit>,
}

pub(super) struct CommandCredits {
    _command: zero_runtime::configuration::ControlConnectionPermit,
    _bytes: Vec<ControlBudgetPermit>,
    pub(super) observation: zero_runtime::ApplyObservation,
}
impl CommandCredits {
    pub(super) fn new(
        command: zero_runtime::configuration::ControlConnectionPermit,
        bytes: Vec<ControlBudgetPermit>,
    ) -> Self {
        Self {
            _command: command,
            _bytes: bytes,
            observation: zero_runtime::ApplyObservation::new(),
        }
    }
    pub(super) async fn settle(&self) -> io::Result<()> {
        super::super::owner::wait_command_operation(&self.observation).await
    }
}

struct StopOnPanic(CancellationToken);
impl Drop for StopOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.cancel();
        }
    }
}
impl ConcurrentCommandTasks {
    #[cfg(test)]
    pub(super) fn new(stop: CancellationToken) -> Self {
        let budget = ControlBudget::new(ControlBudgetLimits::from_runtime(
            &zero_config::RuntimeOptionsConfig::default(),
        ));
        Self::test_with_budget(stop, budget)
    }
    #[cfg(test)]
    pub(super) fn test_with_budget(stop: CancellationToken, budget: Arc<ControlBudget>) -> Self {
        let original = budget.reserve(ControlCharge::Connection).unwrap();
        let mut commands = Self::with_budget(stop, budget, original.connection_budget().unwrap());
        commands._original = Some(original);
        commands
    }
    pub(super) fn with_budget(
        stop: CancellationToken,
        budget: Arc<ControlBudget>,
        local: zero_runtime::configuration::ControlConnectionBudget,
    ) -> Self {
        Self {
            tasks: JoinSet::new(),
            stop,
            failures: Vec::new(),
            budget,
            retained: Default::default(),
            local,
            #[cfg(test)]
            _original: None,
        }
    }
    fn receive(
        &mut self,
        result: Result<(tokio::task::Id, io::Result<()>), tokio::task::JoinError>,
    ) {
        let id = match &result {
            Ok((id, _)) => *id,
            Err(error) => error.id(),
        };
        if let Err(error) = result
            .map_err(io::Error::other)
            .and_then(|(_, result)| result)
        {
            self.failures.push(error);
            self.stop.cancel();
        }
        // The original JoinHandle result has been consumed and its typed fault
        // retained before this exact job's command/input capacity is returned.
        drop(self.retained.remove(&id));
    }

    pub(super) fn has_tasks(&self) -> bool {
        !self.tasks.is_empty()
    }
    pub(super) async fn receive_next(&mut self) {
        if let Some(result) = self.tasks.join_next_with_id().await {
            self.receive(result);
        }
    }
    fn reap_finished(&mut self) {
        while let Some(result) = self.tasks.try_join_next_with_id() {
            self.receive(result);
        }
    }
    #[cfg(test)]
    pub(super) fn try_spawn(
        &mut self,
        future: impl Future<Output = io::Result<()>> + Send + 'static,
    ) -> bool {
        self.try_spawn_owned(future, Vec::new())
    }

    pub(super) fn try_spawn_owned(
        &mut self,
        future: impl Future<Output = io::Result<()>> + Send + 'static,
        retained: Vec<ControlBudgetPermit>,
    ) -> bool {
        self.reap_finished();
        if self.stop.is_cancelled()
            || !self.failures.is_empty()
            || self.tasks.len() >= self.budget.limits().messages.ipc_pending_commands
        {
            return false;
        }
        let Ok(command) = self
            .local
            .reserve(zero_runtime::configuration::ControlConnectionCharge::PendingCommand)
        else {
            return false;
        };
        let stop = self.stop.clone();
        let credits = CommandCredits::new(command, retained);
        let observation = credits.observation.clone();
        let (start, started) = tokio::sync::oneshot::channel();
        let original = self.tasks.spawn(async move {
            if started.await.is_err() || stop.is_cancelled() {
                return Ok(());
            }
            let _panic = StopOnPanic(stop.clone());
            // Once this original command has started, its existing operation
            // owns completion/cleanup. A disconnected caller cannot turn
            // cancellation into ACK or discard its retained input capacity.
            let result =
                super::super::owner::catch_work(observation.scope_with_stop(stop.clone(), future))
                    .await;
            let joined = super::super::owner::wait_command_operation(&observation).await;
            let result = super::super::owner::finish_caught(result, [joined]);
            if result.is_err() {
                stop.cancel();
            }
            result
        });
        self.retained.insert(original.id(), credits);
        let _ = start.send(());
        true
    }

    pub(super) async fn cancel_all(&mut self) -> io::Result<()> {
        self.stop.cancel();
        while let Some(result) = self.tasks.join_next_with_id().await {
            self.receive(result);
        }
        super::super::owner::finish_work(std::mem::take(&mut self.failures).into_iter().map(Err))
    }
}

#[cfg(test)]
mod tests;
