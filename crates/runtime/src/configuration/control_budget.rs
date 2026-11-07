//! Finite accounting for the actual root control servers. No worker, listener,
//! queue, replay store or subscriber is allocated by constructing this owner.
use std::{
    io,
    sync::{Arc, Mutex, Weak},
};

mod capacity;
mod connection;
pub use connection::{ControlConnectionBudget, ControlConnectionCharge, ControlConnectionPermit};
mod limits;
pub use limits::{ControlBudgetLimits, ControlMessageLimits};
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ControlBudgetUsage {
    pub connections: usize,
    pub subscriptions: usize,
    pub queued_event_bytes: usize,
    pub pipe_workspace_bytes: usize,
    pub request_bytes: usize,
    pub response_bytes: usize,
    pub pending_commands: usize,
    pub grpc_requests: usize,
}
#[derive(Clone, Copy)]
pub enum ControlCharge {
    Connection,
    Subscription,
    QueuedEventBytes(usize),
    PipeWorkspaceBytes(usize),
    RequestBytes(usize),
    ResponseBytes(usize),
    PendingCommand,
    GrpcRequest,
}
#[derive(Debug)]
struct Pending {
    identity: Arc<()>,
    limits: ControlBudgetLimits,
}
#[derive(Debug)]
struct State {
    limits: ControlBudgetLimits,
    used: ControlBudgetUsage,
    pending: Option<Pending>,
    closed: bool,
    connection_serial: u64,
    connections: std::collections::BTreeMap<u64, connection::Usage>,
}
#[derive(Debug)]
pub struct ControlBudget(Mutex<State>, Arc<tokio::sync::Notify>);
/// Each live permit is retained by the actual original, not by a waiter. Its
/// owner releases it only after matching native close / real worker and queue
/// custody return, as appropriate to that charge.
pub struct ControlBudgetPermit {
    owner: Arc<ControlBudget>,
    charge: ControlCharge,
    connection: Option<ControlConnectionBudget>,
}
/// Exact finite host attempt; Drop deliberately does not authorize publication
/// or retirement. The existing application attempt retains it across awaits.
pub struct ControlBudgetCandidate {
    owner: Weak<ControlBudget>,
    identity: Arc<()>,
    limits: ControlBudgetLimits,
}
fn capacity() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "root control owner capacity exhausted",
    )
}
fn fits(used: ControlBudgetUsage, limits: ControlBudgetLimits) -> bool {
    used.connections <= limits.connections
        && used.subscriptions <= limits.subscriptions
        && used.queued_event_bytes <= limits.queued_event_bytes
        && used.pipe_workspace_bytes <= limits.pipe_workspace_bytes
        && used.request_bytes <= limits.messages.request_bytes_total
        && used.response_bytes <= limits.messages.response_bytes_total
        && used.pending_commands <= limits.messages.pending_commands
        && used.grpc_requests <= limits.messages.grpc_requests
}
impl ControlBudget {
    pub fn new(limits: ControlBudgetLimits) -> Arc<Self> {
        Arc::new(Self(
            Mutex::new(State {
                limits,
                used: ControlBudgetUsage::default(),
                pending: None,
                closed: false,
                connection_serial: 0,
                connections: Default::default(),
            }),
            Arc::new(tokio::sync::Notify::new()),
        ))
    }
    pub fn limits(&self) -> ControlBudgetLimits {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .limits
    }
    pub fn usage(&self) -> ControlBudgetUsage {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .used
    }
    pub fn reserve(self: &Arc<Self>, charge: ControlCharge) -> io::Result<ControlBudgetPermit> {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "root control admission closed",
            ));
        }
        let mut used = state.used;
        let (counter, cost) = match charge {
            ControlCharge::Connection => (&mut used.connections, 1),
            ControlCharge::Subscription => (&mut used.subscriptions, 1),
            ControlCharge::QueuedEventBytes(bytes) => (&mut used.queued_event_bytes, bytes),
            ControlCharge::PipeWorkspaceBytes(bytes) => (&mut used.pipe_workspace_bytes, bytes),
            ControlCharge::RequestBytes(bytes) => (&mut used.request_bytes, bytes),
            ControlCharge::ResponseBytes(bytes) => (&mut used.response_bytes, bytes),
            ControlCharge::PendingCommand => (&mut used.pending_commands, 1),
            ControlCharge::GrpcRequest => (&mut used.grpc_requests, 1),
        };
        *counter = counter.checked_add(cost).ok_or_else(capacity)?;
        if !fits(used, state.limits)
            || state
                .pending
                .as_ref()
                .is_some_and(|pending| !fits(used, pending.limits))
        {
            return Err(capacity());
        }
        let connection = if matches!(charge, ControlCharge::Connection) {
            let id = state
                .connection_serial
                .checked_add(1)
                .ok_or_else(capacity)?;
            state.connection_serial = id;
            state.connections.insert(id, connection::Usage::default());
            Some(ControlConnectionBudget::issued(self.clone(), id))
        } else {
            None
        };
        state.used = used;
        Ok(ControlBudgetPermit {
            owner: self.clone(),
            charge,
            connection,
        })
    }
    pub fn validate(&self, limits: ControlBudgetLimits) -> io::Result<()> {
        let state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.pending.is_some() {
            return Err(io::Error::other("root control candidate still owned"));
        }
        if !fits(state.used, limits) || !connection::fits_all(&state, limits) {
            return Err(capacity());
        }
        Ok(())
    }
    pub fn prepare(
        self: &Arc<Self>,
        limits: ControlBudgetLimits,
    ) -> io::Result<ControlBudgetCandidate> {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed || state.pending.is_some() {
            return Err(io::Error::other(
                "root control candidate cannot replace an unresolved owner",
            ));
        }
        if !fits(state.used, limits) || !connection::fits_all(&state, limits) {
            return Err(capacity());
        }
        let identity = Arc::new(());
        state.pending = Some(Pending {
            identity: identity.clone(),
            limits,
        });
        Ok(ControlBudgetCandidate {
            owner: Arc::downgrade(self),
            identity,
            limits,
        })
    }
    pub fn close_admission(&self) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .closed = true;
        self.1.notify_waiters();
    }
    pub fn confirm_released(&self) -> io::Result<()> {
        let state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.closed || state.used != ControlBudgetUsage::default() || state.pending.is_some() {
            return Err(io::Error::other(
                "root control original charges remain retained",
            ));
        }
        Ok(())
    }
}
impl ControlBudgetCandidate {
    pub fn limits(&self) -> ControlBudgetLimits {
        self.limits
    }
    fn finish(&self, commit: bool) -> io::Result<()> {
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| io::Error::other("original root control owner expired"))?;
        let mut state = owner
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let pending = state
            .pending
            .as_ref()
            .ok_or_else(|| io::Error::other("root control candidate was already settled"))?;
        if !Arc::ptr_eq(&pending.identity, &self.identity) || pending.limits != self.limits {
            return Err(io::Error::other("foreign root control candidate"));
        }
        if commit {
            state.limits = self.limits;
        }
        state.pending = None;
        drop(state);
        owner.1.notify_waiters();
        Ok(())
    }
    pub fn commit(&self) -> io::Result<()> {
        self.finish(true)
    }
    pub fn retire(&self) -> io::Result<()> {
        self.finish(false)
    }
}
impl Drop for ControlBudgetPermit {
    fn drop(&mut self) {
        let mut state = self
            .owner
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (counter, cost) = match self.charge {
            ControlCharge::Connection => (&mut state.used.connections, 1),
            ControlCharge::Subscription => (&mut state.used.subscriptions, 1),
            ControlCharge::QueuedEventBytes(bytes) => (&mut state.used.queued_event_bytes, bytes),
            ControlCharge::PipeWorkspaceBytes(bytes) => {
                (&mut state.used.pipe_workspace_bytes, bytes)
            }
            ControlCharge::RequestBytes(bytes) => (&mut state.used.request_bytes, bytes),
            ControlCharge::ResponseBytes(bytes) => (&mut state.used.response_bytes, bytes),
            ControlCharge::PendingCommand => (&mut state.used.pending_commands, 1),
            ControlCharge::GrpcRequest => (&mut state.used.grpc_requests, 1),
        };
        *counter = counter
            .checked_sub(cost)
            .expect("same root control charge returned once");
        if let Some(connection) = &self.connection {
            connection::close(&mut state, connection);
        }
        drop(state);
        self.owner.1.notify_waiters();
    }
}
#[cfg(test)]
#[path = "../tests/control_budget.rs"]
mod tests;
