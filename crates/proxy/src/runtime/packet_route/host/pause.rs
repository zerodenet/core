//! The original native device/wire driver is one participant, never a fake Source main.
use super::*;
use std::sync::{Mutex, OnceLock};
use zero_platform_tokio::contract::{
    scope::{TaskJoinWitness, TaskScopeIdentity},
    ChannelError, Impact, Reason, Stage,
};
/// Admission is fenced, but the original wire has not been armed or paused.
pub(crate) struct NativeDeviceFenced {
    guard: RequestGuard,
}
pub(crate) struct NativeDevicePaused {
    guard: RequestGuard,
}
struct RequestGuard {
    original: Arc<ReadPause>,
    request: u64,
}
pub(crate) struct ReadPause {
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
pub(crate) struct Checkpoint {
    original: Arc<ReadPause>,
}
pub(crate) struct Acquisition {
    original: Arc<ReadPause>,
}
pub(super) struct ReadCheckpoint {
    pub checkpoint: Checkpoint,
    #[cfg(windows)]
    pub sdk: Option<zero_tun::OwnedWintunReadControl>,
}
impl Drop for RequestGuard {
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
    pub(crate) fn new(scope: TaskScopeIdentity) -> (Arc<Self>, Checkpoint) {
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
    pub(crate) fn bind(&self, actual: &TaskJoinWitness) -> Result<(), ChannelError> {
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
    pub(crate) fn try_enter(self: &Arc<Self>) -> Result<Acquisition, ChannelError> {
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
    pub(crate) async fn fence(
        self: &Arc<Self>,
        deadline: tokio::time::Instant,
    ) -> Result<NativeDeviceFenced, ChannelError> {
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
        let guard = RequestGuard {
            original: self.clone(),
            request,
        };
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.state.lock().unwrap();
                if !self.live(&state) || state.request != Some(request) {
                    return Err(error(Reason::Revoked));
                }
                // Existing acquisitions may finish while the original wire
                // continues running. Only finish may request its checkpoint.
                if state.acquiring == 0 {
                    return Ok(NativeDeviceFenced { guard });
                }
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|source| error(Reason::Timeout).with_source(source))?;
        }
    }
    pub(crate) async fn pause(
        self: &Arc<Self>,
        deadline: tokio::time::Instant,
    ) -> Result<NativeDevicePaused, ChannelError> {
        self.fence(deadline).await?.finish(deadline).await
    }
}
impl NativeDeviceFenced {
    pub(crate) async fn finish(
        self,
        deadline: tokio::time::Instant,
    ) -> Result<NativeDevicePaused, ChannelError> {
        let guard = self.guard;
        let original = guard.original.clone();
        let request = guard.request;
        {
            let mut state = original.state.lock().unwrap();
            if !original.live(&state)
                || state.request != Some(request)
                || state.acquiring != 0
                || state.armed
            {
                return Err(error(Reason::Revoked));
            }
            state.armed = true;
        }
        original.changed.notify_waiters();
        loop {
            let changed = original.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = original.state.lock().unwrap();
                if !original.live(&state) || state.request != Some(request) {
                    return Err(error(Reason::Revoked));
                }
                if let Some(result) = &state.result {
                    return match result {
                        Ok(()) => Ok(NativeDevicePaused { guard }),
                        Err(source) => {
                            Err(
                                ChannelError::new(source.stage, source.reason, source.impact)
                                    .with_source(SharedFailure(source.clone())),
                            )
                        }
                    };
                }
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|source| error(Reason::Timeout).with_source(source))?;
        }
    }
}

impl Checkpoint {
    pub(crate) fn matches_driver(&self, actual: &TaskJoinWitness) -> bool {
        self.original
            .actual
            .get()
            .is_some_and(|bound| bound.same_registration(actual))
    }
    pub(crate) async fn requested(&self) -> u64 {
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
    pub(crate) fn acknowledge(&self, request: u64, result: Result<(), ChannelError>) {
        let mut state = self.original.state.lock().unwrap();
        if state.armed && state.request == Some(request) {
            state.result = Some(result.map_err(Arc::new));
        }
        drop(state);
        self.original.changed.notify_waiters();
    }
    pub(crate) async fn resumed(&self, request: u64) {
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
        let state = self.guard.original.state.lock().unwrap();
        if state.request != Some(self.guard.request)
            || !self.guard.original.live(&state)
            || !state.armed
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
    pub(crate) async fn fence_if_idle(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<NativeDeviceFenced, ChannelError> {
        self.read_pause
            .as_ref()
            .ok_or_else(|| error(Reason::Unsupported))?
            .fence(deadline)
            .await
    }
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
