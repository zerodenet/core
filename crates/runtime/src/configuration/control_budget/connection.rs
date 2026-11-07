//! Local holders belong to the connection reservation issued by this same Root.
use super::*;
#[derive(Clone, Copy, Debug)]
pub enum ControlConnectionCharge {
    PendingCommand,
    EventFrame,
    SseFrame,
    GrpcStream,
}
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Usage {
    commands: usize,
    events: usize,
    sse: usize,
    grpc: usize,
    closed: bool,
}
impl Usage {
    fn empty(self) -> bool {
        self.commands == 0 && self.events == 0 && self.grpc == 0 && self.sse == 0
    }
    fn fits(self, limits: ControlBudgetLimits) -> bool {
        self.commands <= limits.messages.ipc_pending_commands
            && self.events <= limits.messages.ipc_event_frames
            && self.sse <= limits.messages.sse_event_frames
            && self.grpc <= limits.messages.grpc_streams
    }
}
#[derive(Clone)]
pub struct ControlConnectionBudget {
    owner: Arc<ControlBudget>,
    id: u64,
}
pub struct ControlConnectionPermit {
    connection: ControlConnectionBudget,
    charge: ControlConnectionCharge,
}
impl ControlBudgetPermit {
    /// Only this actual Connection reservation can issue its shared local book.
    /// Repeated observations retain the same registration, not another allowance.
    pub fn connection_budget(&self) -> Option<ControlConnectionBudget> {
        self.connection.clone()
    }
}
impl ControlConnectionBudget {
    pub(super) fn issued(owner: Arc<ControlBudget>, id: u64) -> Self {
        Self { owner, id }
    }
    pub fn reserve(&self, charge: ControlConnectionCharge) -> io::Result<ControlConnectionPermit> {
        let mut state = self
            .owner
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "root control admission closed",
            ));
        }
        let Some(mut local) = state.connections.get(&self.id).copied() else {
            return Err(capacity());
        };
        if local.closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "original control connection retired",
            ));
        }
        let mut used = state.used;
        let counter = match charge {
            ControlConnectionCharge::PendingCommand => {
                used.pending_commands =
                    used.pending_commands.checked_add(1).ok_or_else(capacity)?;
                &mut local.commands
            }
            ControlConnectionCharge::EventFrame => &mut local.events,
            ControlConnectionCharge::SseFrame => &mut local.sse,
            ControlConnectionCharge::GrpcStream => {
                used.grpc_requests = used.grpc_requests.checked_add(1).ok_or_else(capacity)?;
                &mut local.grpc
            }
        };
        *counter = counter.checked_add(1).ok_or_else(capacity)?;
        let fits = |limits| super::fits(used, limits) && local.fits(limits);
        if !fits(state.limits)
            || state
                .pending
                .as_ref()
                .is_some_and(|pending| !fits(pending.limits))
        {
            return Err(capacity());
        }
        state.used = used;
        state.connections.insert(self.id, local);
        Ok(ControlConnectionPermit {
            connection: self.clone(),
            charge,
        })
    }
}
pub(super) fn fits_all(state: &State, limits: ControlBudgetLimits) -> bool {
    state.connections.values().all(|usage| usage.fits(limits))
}
pub(super) fn close(state: &mut State, connection: &ControlConnectionBudget) {
    let Some(local) = state.connections.get_mut(&connection.id) else {
        return;
    };
    local.closed = true;
    if local.empty() {
        state.connections.remove(&connection.id);
    }
}
impl Drop for ControlConnectionPermit {
    fn drop(&mut self) {
        let mut state = self
            .connection
            .owner
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let local = state
            .connections
            .get_mut(&self.connection.id)
            .expect("same connection retained until last local holder");
        let counter = match self.charge {
            ControlConnectionCharge::PendingCommand => &mut local.commands,
            ControlConnectionCharge::EventFrame => &mut local.events,
            ControlConnectionCharge::SseFrame => &mut local.sse,
            ControlConnectionCharge::GrpcStream => &mut local.grpc,
        };
        *counter = counter
            .checked_sub(1)
            .expect("same original local holder returned once");
        if local.closed && local.empty() {
            state.connections.remove(&self.connection.id);
        }
        let counter = match self.charge {
            ControlConnectionCharge::PendingCommand => Some(&mut state.used.pending_commands),
            ControlConnectionCharge::EventFrame | ControlConnectionCharge::SseFrame => None,
            ControlConnectionCharge::GrpcStream => Some(&mut state.used.grpc_requests),
        };
        if let Some(counter) = counter {
            *counter = counter
                .checked_sub(1)
                .expect("same Root command or RPC returned once");
        }
        drop(state);
        self.connection.owner.1.notify_waiters();
    }
}
