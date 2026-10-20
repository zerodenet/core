//! Each original future enters the actual host Scope exactly once.
mod record;
use record::Record;
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use zero_core::component::TaskId;
use zero_platform_tokio::contract::CancellationToken;
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, ComponentError},
    scope::{SpawnError, TaskClass, TaskJoinWitness, TaskJoined, WeakTaskRegistrar},
    ChannelError, Impact, Reason, Stage,
};

pub(super) struct ScopedWorkers {
    registrar: WeakTaskRegistrar,
    cancel: CancellationToken,
    state: Mutex<State>,
}
#[derive(Default)]
struct State {
    records: Vec<Arc<Record>>,
    closing: bool,
    drained: bool,
    first: Option<Failure>,
    additional: usize,
}
#[derive(Clone)]
enum Failure {
    Task(TaskJoined),
    Admission(Arc<ChannelError>),
}
impl ScopedWorkers {
    pub(super) fn new(registrar: WeakTaskRegistrar) -> Self {
        Self {
            registrar,
            cancel: CancellationToken::new(),
            state: Mutex::new(State::default()),
        }
    }
    pub(super) fn spawn(
        &self,
        future: impl Future<Output = Result<(), ComponentError>> + Send + 'static,
    ) -> Result<(), ChannelError> {
        self.reap();
        let record = Record::new();
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closing {
                return Err(failure(&state).unwrap_or_else(|| {
                    ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component)
                }));
            }
            if state.records.len() >= super::WORKER_LIMIT {
                return Err(
                    ChannelError::new(Stage::Prepare, Reason::Capacity, Impact::Component)
                        .with_source(std::io::Error::new(
                            std::io::ErrorKind::WouldBlock,
                            "raw-IP original worker record limit exhausted",
                        )),
                );
            }
            state.records.push(record.clone());
        }
        let gate = record.clone();
        let cancelled = self.cancel.clone();
        // No resource lock spans the temporary Scope upgrade or callbacks.
        // If notification unwinds after acceptance, the Pending reservation is
        // retained as Unknown; it cannot refund budget or assert an empty join.
        let registered = self.registrar.spawn_tracked(TaskClass::Critical, async move {
            let running = async move {
                if gate.wait_registration().await.is_none() { return Ok(()); }
                future.await
            };
            tokio::select! { biased; result = running => result, _ = cancelled.cancelled() => Ok(()) }
        });
        match registered {
            Ok((id, witness)) => {
                record.install(id, witness);
                Ok(())
            }
            Err(source) => {
                record.reject(); // This Result guarantees no task was registered.
                let reason = match source {
                    SpawnError::Capacity => Reason::Capacity,
                    SpawnError::IdentityExhausted => Reason::Internal,
                    SpawnError::NotStarted | SpawnError::Closing => Reason::Revoked,
                };
                let error = Arc::new(
                    ChannelError::new(Stage::Prepare, reason, Impact::Component)
                        .with_source(source),
                );
                let (retired, first) = {
                    let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                    state.closing = true;
                    if state.first.is_none() {
                        state.first = Some(Failure::Admission(error.clone()));
                    } else {
                        state.additional = state.additional.saturating_add(1);
                    }
                    let position = state
                        .records
                        .iter()
                        .position(|candidate| Arc::ptr_eq(candidate, &record))
                        .expect("reserved original worker");
                    (
                        state.records.remove(position),
                        failure(&state).expect("retained first admission or task failure"),
                    )
                };
                self.cancel.cancel();
                drop(retired);
                Err(first)
            }
        }
    }
    pub(super) fn first_failure(&self) -> Option<ChannelError> {
        failure(&self.state.lock().unwrap_or_else(|error| error.into_inner()))
    }
    pub(super) fn revoke(&self) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .closing = true;
        self.cancel.cancel();
    }
    pub(super) fn is_drained(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .drained
    }
    fn retain(state: &mut State, joined: &TaskJoined) {
        if joined.result().is_err() {
            if state.first.is_none() {
                state.first = Some(Failure::Task(joined.clone()));
            } else {
                state.additional = state.additional.saturating_add(1);
            }
            state.closing = true;
        }
    }
    fn reap(&self) {
        let completed = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let mut completed = Vec::new();
            let mut index = 0;
            while index < state.records.len() {
                let registered = state.records[index].registered();
                if let Some((id, witness)) = registered {
                    if let Some(joined) = witness.try_joined() {
                        assert_eq!(id, joined.id(), "same original Scope task");
                        Self::retain(&mut state, &joined);
                        completed.push((state.records.remove(index), witness, joined));
                        continue;
                    }
                }
                index += 1;
            }
            completed
        };
        self.accept(completed);
    }
    fn accept(&self, completed: Vec<(Arc<Record>, TaskJoinWitness, TaskJoined)>) {
        for (_record, witness, joined) in completed {
            // The persistent first failure was saved before local delivery.
            // Host acknowledgement still belongs to its original receipt.
            if joined.result().is_err() {
                witness
                    .acknowledge_hosted_failure(&joined)
                    .expect("same original failed task receipt");
            }
        }
        let closing = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .closing;
        if closing {
            self.cancel.cancel();
        }
    }
    pub(super) async fn join(&self) -> Result<CloseReport, ChannelError> {
        self.revoke();
        let records = self
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .records
            .clone();
        let mut report = CloseReport::default();
        for record in records {
            if let Some((id, witness)) = record.wait_registration().await {
                let joined = witness.wait().await;
                assert_eq!(id, joined.id(), "same original Scope task");
                let completed = {
                    let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                    if let Some(position) = state
                        .records
                        .iter()
                        .position(|candidate| Arc::ptr_eq(candidate, &record))
                    {
                        Self::retain(&mut state, &joined);
                        Some((state.records.remove(position), witness, joined))
                    } else {
                        None
                    }
                };
                if let Some(completed) = completed {
                    report.exited_tasks.push(id);
                    self.accept(vec![completed]);
                }
            }
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.drained = state.records.is_empty();
        if let Some(error) = failure(&state) {
            return Err(error);
        }
        Ok(report)
    }
}
fn failure(state: &State) -> Option<ChannelError> {
    match &state.first {
        Some(Failure::Admission(error)) => Some(forward(error.clone())),
        Some(Failure::Task(joined)) => joined.result().err().map(forward),
        None => None,
    }
}
fn forward(source: Arc<ChannelError>) -> ChannelError {
    let mut error = ChannelError::new(source.stage, source.reason, source.impact);
    error.component = source.component;
    error.session = source.session;
    error.delivery = source.delivery;
    error.retry = source.retry;
    error.with_source(SharedFailure(source))
}
#[derive(Debug)]
struct SharedFailure(Arc<ChannelError>);
impl std::fmt::Display for SharedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
#[cfg(test)]
mod tests;
