//! The original Direct driver is one participant; no invented Source main.
use super::*;
use std::sync::{Mutex, OnceLock};
use zero_platform_tokio::contract::{
    scope::{TaskJoinWitness, TaskScopeIdentity},
    ChannelError, Impact, Reason, Stage,
};
pub(crate) struct NativeDevicePaused {
    original: Arc<ReadPause>,
    request: u64,
}
pub(super) struct ReadPause {
    scope: TaskScopeIdentity,
    actual: OnceLock<TaskJoinWitness>,
    state: Mutex<State>,
    changed: tokio::sync::Notify,
}
#[derive(Default)]
struct State {
    serial: u64,
    request: Option<u64>,
    armed: bool,
    checkpoint: bool,
    acquiring: usize,
    result: Option<Result<(), Arc<ChannelError>>>,
}
pub(super) struct Checkpoint {
    original: Arc<ReadPause>,
}
pub(super) struct Acquisition {
    original: Arc<ReadPause>,
}
pub(super) struct ReadCheckpoint {
    pub checkpoint: Checkpoint,
    #[cfg(windows)]
    pub sdk: Option<zero_tun::OwnedWintunReadControl>,
}
impl Drop for NativeDevicePaused {
    fn drop(&mut self) {
        let mut state = self.original.state.lock().unwrap();
        if state.request == Some(self.request) {
            state.request = None;
            state.armed = false;
            state.result = None;
        }
        drop(state);
        self.original.changed.notify_waiters();
    }
}
impl Drop for Acquisition {
    fn drop(&mut self) {
        self.original.state.lock().unwrap().acquiring -= 1;
        self.original.changed.notify_waiters();
    }
}
impl Drop for Checkpoint {
    fn drop(&mut self) {
        self.original.state.lock().unwrap().checkpoint = false;
        self.original.changed.notify_waiters();
    }
}
impl ReadPause {
    pub(super) fn new(scope: TaskScopeIdentity) -> (Arc<Self>, Checkpoint) {
        let original = Arc::new(Self {
            scope,
            actual: OnceLock::new(),
            state: Mutex::new(State {
                checkpoint: true,
                ..Default::default()
            }),
            changed: tokio::sync::Notify::new(),
        });
        (original.clone(), Checkpoint { original })
    }
    pub(super) fn bind(&self, actual: &TaskJoinWitness) -> Result<(), ChannelError> {
        if !actual.scope_identity().same_scope(&self.scope) {
            return Err(error(Reason::StalePlan));
        }
        self.actual
            .set(actual.clone())
            .map_err(|_| error(Reason::StalePlan))
    }
    fn live(&self, state: &State) -> bool {
        state.checkpoint
            && self.scope.resource_open()
            && self.actual.get().is_some_and(|actual| !actual.is_joined())
    }
    pub(super) fn try_enter(self: &Arc<Self>) -> Result<Acquisition, ChannelError> {
        let mut state = self.state.lock().unwrap();
        if !self.live(&state) {
            return Err(error(Reason::Revoked));
        }
        if state.request.is_some() {
            return Err(error(Reason::Capacity));
        }
        state.acquiring = state
            .acquiring
            .checked_add(1)
            .ok_or_else(|| error(Reason::Capacity))?;
        Ok(Acquisition {
            original: self.clone(),
        })
    }
    pub(super) async fn pause(
        self: &Arc<Self>,
        deadline: tokio::time::Instant,
    ) -> Result<NativeDevicePaused, ChannelError> {
        let request = {
            let mut state = self.state.lock().unwrap();
            if !self.live(&state) {
                return Err(error(Reason::Revoked));
            }
            if state.request.is_some() {
                return Err(error(Reason::Capacity));
            }
            state.serial = state
                .serial
                .checked_add(1)
                .ok_or_else(|| error(Reason::Capacity))?;
            state.request = Some(state.serial);
            state.armed = false;
            state.result = None;
            state.serial
        };
        let guard = NativeDevicePaused {
            original: self.clone(),
            request,
        };
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let wake = {
                let mut state = self.state.lock().unwrap();
                if !self.live(&state) || state.request != Some(request) {
                    return Err(error(Reason::Revoked));
                }
                if let Some(result) = &state.result {
                    return match result {
                        Ok(()) => Ok(guard),
                        Err(source) => {
                            Err(
                                ChannelError::new(source.stage, source.reason, source.impact)
                                    .with_source(SharedFailure(source.clone())),
                            )
                        }
                    };
                }
                let wake = state.acquiring == 0 && !state.armed;
                if wake {
                    state.armed = true;
                }
                wake
            };
            if wake {
                self.changed.notify_waiters();
                continue;
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|source| error(Reason::Timeout).with_source(source))?;
        }
    }
}
impl Checkpoint {
    pub(super) async fn requested(&self) -> u64 {
        loop {
            let changed = self.original.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.original.state.lock().unwrap();
                if state.armed {
                    if let Some(request) = state.request {
                        return request;
                    }
                }
            }
            changed.await;
        }
    }
    pub(super) fn acknowledge(&self, request: u64, result: Result<(), ChannelError>) {
        let mut state = self.original.state.lock().unwrap();
        if state.request == Some(request) {
            state.result = Some(result.map_err(Arc::new));
        }
        drop(state);
        self.original.changed.notify_waiters();
    }
    pub(super) async fn resumed(&self, request: u64) {
        loop {
            let changed = self.original.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.original.state.lock().unwrap().request != Some(request) {
                return;
            }
            changed.await;
        }
    }
}
impl NativeDevicePaused {
    pub(crate) fn validate(&self) -> Result<(), ChannelError> {
        let state = self.original.state.lock().unwrap();
        if state.request != Some(self.request)
            || !self.original.live(&state)
            || !matches!(state.result, Some(Ok(())))
            || state.acquiring != 0
        {
            return Err(error(Reason::Revoked));
        }
        Ok(())
    }
}
fn error(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Component)
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
impl HostPacketDevice {
    pub(crate) async fn pause_if_idle(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<NativeDevicePaused, ChannelError> {
        self.read_pause
            .as_ref()
            .ok_or_else(|| error(Reason::Unsupported))?
            .pause(deadline)
            .await
    }
}
#[cfg(test)]
mod tests;
