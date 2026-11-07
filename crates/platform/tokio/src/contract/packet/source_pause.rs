//! Reversible checkpoints of the same actual Packet source main and native wire.
//! These facts grant no task, IO, user return, native close or Scope-join proof.
use super::*;
use crate::contract::scope::{TaskJoinWitness, TaskScopeIdentity};
use crate::contract::{CancellationToken, Impact, Reason, Stage};
use std::sync::{Arc, Mutex};
use zero_core::component::ComponentInstanceId;
#[derive(Clone, Copy)]
pub enum PacketPauseParticipant {
    Main,
    Wire,
}
impl PacketPauseParticipant {
    fn index(self) -> usize {
        match self {
            Self::Main => 0,
            Self::Wire => 1,
        }
    }
}
pub struct PacketSourcePause {
    instance: ComponentInstanceId,
    scope: TaskScopeIdentity,
    cancel: CancellationToken,
    state: Mutex<PauseState>,
    changed: tokio::sync::Notify,
}
#[derive(Default)]
struct PauseState {
    serial: u64,
    request: Option<u64>,
    deadline: Option<tokio::time::Instant>,
    actual: [Option<TaskJoinWitness>; 2],
    present: [bool; 2],
    acknowledged: [Option<bool>; 2],
    fault: Option<Arc<ChannelError>>,
    acquiring: usize,
    checkpoints_requested: bool,
    wire_requested: bool,
}
mod admission;
pub use admission::PacketSourceAcquisition;
mod checkpoint;
pub use checkpoint::PacketSourceCheckpoint;
pub struct PacketSourcePaused {
    original: Arc<PacketSourcePause>,
    request: u64,
}
/// Same original request after only its actual Main has reached an idle
/// checkpoint. Native Wire and Source closure remain entirely unconfirmed.
pub struct PacketSourceMainPaused {
    paused: PacketSourcePaused,
}
impl PacketSourceMainPaused {
    pub fn scope(&self) -> &TaskScopeIdentity {
        &self.paused.original.scope
    }
    pub fn matches(&self, original: &Arc<PacketSourcePause>) -> bool {
        Arc::ptr_eq(&self.paused.original, original)
    }

    pub fn instance(&self) -> ComponentInstanceId {
        self.paused.original.instance
    }
    pub async fn finish(self) -> Result<PacketSourcePaused, ChannelError> {
        let original = &self.paused.original;
        let deadline = {
            let mut state = original.state.lock().unwrap();
            if state.request != Some(self.paused.request)
                || !original.live(&state)
                || state.acknowledged[0] != Some(true)
                || state.acquiring != 0
            {
                return Err(pause_error(Reason::Revoked));
            }
            state.wire_requested = true;
            state.deadline.ok_or_else(|| pause_error(Reason::Revoked))?
        };
        original.changed.notify_waiters();
        original
            .wait_checkpoint(self.paused.request, true, deadline)
            .await?;
        Ok(self.paused)
    }
}
impl PacketSourcePaused {
    pub fn validate(&self) -> Result<(), ChannelError> {
        let state = self.original.state.lock().unwrap();
        if state.request != Some(self.request)
            || !self.original.live(&state)
            || state.acknowledged != [Some(true), Some(true)]
            || state.acquiring != 0
            || !state.checkpoints_requested
            || !state.wire_requested
        {
            return Err(pause_error(Reason::Revoked));
        }
        Ok(())
    }
}
impl Drop for PacketSourcePaused {
    fn drop(&mut self) {
        let mut state = self.original.state.lock().unwrap();
        if state.request == Some(self.request) {
            state.request = None;
            state.deadline = None;
            state.acknowledged = [None, None];
            state.fault = None;
            state.checkpoints_requested = false;
            state.wire_requested = false;
        }
        drop(state);
        self.original.changed.notify_waiters();
    }
}
impl PacketSourcePause {
    /// Called by the original host with that source's real identity/Scope.
    /// Allocation is cold metadata only and does not restore registrar authority.
    pub fn new(
        instance: ComponentInstanceId,
        scope: TaskScopeIdentity,
        cancel: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            instance,
            scope,
            cancel,
            state: Mutex::new(PauseState::default()),
            changed: tokio::sync::Notify::new(),
        })
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    pub fn scope(&self) -> &TaskScopeIdentity {
        &self.scope
    }
    /// The host calls only at the real main/wire registration sites, retaining
    /// the existing TaskIds. Neither role can be substituted by a second task.
    pub fn bind(
        self: &Arc<Self>,
        participant: PacketPauseParticipant,
        actual: &TaskJoinWitness,
    ) -> Result<PacketSourceCheckpoint, ChannelError> {
        // Retain even an already-ended actual registration as observation;
        // pause() independently refuses it as a live checkpoint participant.
        if !actual.scope_identity().same_scope(&self.scope) {
            return Err(pause_error(Reason::Revoked));
        }
        let mut state = self.state.lock().unwrap();
        if state.request.is_some() {
            return Err(pause_error(Reason::StalePlan));
        }
        if state.actual[participant.index()].is_some() {
            return Err(pause_error(Reason::StalePlan));
        }
        // A source main cannot also stand in for its native wire driver.
        if state
            .actual
            .iter()
            .flatten()
            .any(|other| other.same_registration(actual))
        {
            return Err(pause_error(Reason::InvalidConfig));
        }
        state.actual[participant.index()] = Some(actual.clone());
        state.present[participant.index()] = true;
        Ok(PacketSourceCheckpoint {
            original: self.clone(),
            participant,
            witness: actual.clone(),
        })
    }
    fn live(&self, state: &PauseState) -> bool {
        !self.cancel.is_cancelled()
            && self.scope.resource_open()
            && state.present == [true, true]
            && state.actual.iter().all(|actual| {
                actual.as_ref().is_some_and(|witness| {
                    !witness.is_joined() && witness.scope_identity().same_scope(&self.scope)
                })
            })
    }
    pub async fn pause(
        self: &Arc<Self>,
        deadline: tokio::time::Instant,
    ) -> Result<PacketSourcePaused, ChannelError> {
        self.pause_main(deadline).await?.finish().await
    }
    /// Fence actual opens, settle in-flight acquisitions and request only Main.
    /// The coordinator must reject live users before revoking cached receivers.
    pub async fn pause_main(
        self: &Arc<Self>,
        deadline: tokio::time::Instant,
    ) -> Result<PacketSourceMainPaused, ChannelError> {
        let request = {
            let mut state = self.state.lock().unwrap();
            if !self.live(&state) {
                return Err(pause_error(Reason::Revoked));
            }
            if state.request.is_some() {
                return Err(pause_error(Reason::Capacity));
            }
            state.serial = state
                .serial
                .checked_add(1)
                .ok_or_else(|| pause_error(Reason::Capacity))?;
            state.request = Some(state.serial);
            state.deadline = Some(deadline);
            state.acknowledged = [None, None];
            state.fault = None;
            state.checkpoints_requested = false;
            state.wire_requested = false;
            state.serial
        };
        #[cfg(test)]
        eprintln!("actual Packet pause MAIN requested: instance={:?} request={request}", self.instance);
        let guard = PacketSourcePaused {
            original: self.clone(),
            request,
        };
        self.changed.notify_waiters();
        self.wait_checkpoint(request, false, deadline).await?;
        Ok(PacketSourceMainPaused { paused: guard })
    }
    async fn wait_checkpoint(
        &self,
        request: u64,
        wire: bool,
        deadline: tokio::time::Instant,
    ) -> Result<(), ChannelError> {
        #[cfg(test)]
        let mut observed = None;
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let wake_checkpoints;
            {
                let mut state = self.state.lock().unwrap();
                if state.request != Some(request) || !self.live(&state) {
                    return Err(pause_error(Reason::Revoked));
                }
                #[cfg(test)]
                {
                    let actual = (state.acquiring, state.acknowledged, state.checkpoints_requested, state.wire_requested);
                    if observed != Some(actual) {
                        eprintln!("actual Packet pause waiting: instance={:?} request={request} wire={wire} acquiring={} acknowledgements={:?} checkpoints_requested={} wire_requested={}", self.instance, actual.0, actual.1, actual.2, actual.3);
                        observed = Some(actual);
                    }
                }
                // The same mutex sealed new opens when this request was
                // installed. ACKs become observable only after the last real
                // in-flight acquisition installed custody or returned failure.
                wake_checkpoints = state.acquiring == 0 && !state.checkpoints_requested;
                if wake_checkpoints {
                    state.checkpoints_requested = true;
                }
                if let Some(source) = &state.fault {
                    return Err(
                        ChannelError::new(source.stage, source.reason, source.impact)
                            .with_source(PauseFault(source.clone())),
                    );
                }
                if state.acknowledged.contains(&Some(false)) {
                    return Err(
                        pause_error(Reason::Capacity).with_source(std::io::Error::new(
                            std::io::ErrorKind::WouldBlock,
                            "same original Packet source retains users, frames or cleanup debt",
                        )),
                    );
                }
                if state.acknowledged[0] == Some(true)
                    && (!wire || state.acknowledged[1] == Some(true))
                {
                    return Ok(());
                }
            }
            if wake_checkpoints {
                self.changed.notify_waiters();
                continue;
            }
            tokio::select! {
                _ = self.cancel.cancelled() => return Err(pause_error(Reason::Revoked)),
                result = tokio::time::timeout_at(deadline,changed) => if let Err(source)=result { return Err(pause_error(Reason::Timeout).with_source(source)); },
            }
        }
    }
    /// Polled by the original participant inside its existing native/main loop.
    async fn requested(&self) -> u64 {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.state.lock().unwrap();
                if state.checkpoints_requested {
                    if let Some(request) = state.request {
                        return request;
                    }
                }
            }
            changed.await;
        }
    }
    async fn resumed(&self, request: u64) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.state.lock().unwrap().request != Some(request) {
                return;
            }
            changed.await;
        }
    }
}
fn pause_error(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Prepare, reason, Impact::Component)
}

#[derive(Debug)]
struct PauseFault(Arc<ChannelError>);
impl std::fmt::Display for PauseFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for PauseFault {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[cfg(test)]
mod tests;
