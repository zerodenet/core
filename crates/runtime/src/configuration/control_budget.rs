//! Finite accounting for the actual root control servers. No worker, listener,
//! queue, replay store or subscriber is allocated by constructing this owner.
use std::{
    io,
    sync::{Arc, Mutex, Weak},
};

mod candidate;
mod capacity;
mod custody;
pub use custody::ControlCommandCustody;
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
    scope_pause: Option<Arc<crate::embedded_commands::limits::CommandScopePause>>,
    committed: bool,
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
pub struct ControlBudget(
    Mutex<State>,
    Arc<tokio::sync::Notify>,
    Arc<crate::EmbeddedCommandOwner>,
);
/// Each live permit is retained by the actual original, not by a waiter. Its
/// owner releases it only after matching native close / real worker and queue
/// custody return, as appropriate to that charge.
pub struct ControlBudgetPermit {
    owner: Arc<ControlBudget>,
    charge: ControlCharge,
    connection: Option<ControlConnectionBudget>,
    command_identity: Option<Arc<()>>,
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
    pub fn command_owner(&self) -> Arc<crate::EmbeddedCommandOwner> {
        self.2.clone()
    }
    pub fn new(limits: ControlBudgetLimits) -> Arc<Self> {
        let commands = Arc::new(crate::EmbeddedCommandOwner::default());
        let owner = Arc::new(Self(
            Mutex::new(State {
                limits,
                used: ControlBudgetUsage::default(),
                pending: None,
                closed: false,
                connection_serial: 0,
                connections: Default::default(),
            }),
            Arc::new(tokio::sync::Notify::new()),
            commands.clone(),
        ));
        commands.bind_book(&owner);
        owner
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
            command_identity: matches!(charge, ControlCharge::PendingCommand).then(|| Arc::new(())),
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
        let scope_pause = self
            .2
            .pause_for_limit(limits.messages.pending_commands, &identity)?;
        state.pending = Some(Pending {
            identity: identity.clone(),
            limits,
            scope_pause,
            committed: false,
        });
        Ok(ControlBudgetCandidate {
            owner: Arc::downgrade(self),
            identity,
            limits,
        })
    }
    /// The application calls this only after its previous concrete servers were
    /// positively retired. Empty accounting is a separate prerequisite, never a
    /// replacement for their native/worker close receipts.
    pub fn reopen_after_retirement(&self) -> io::Result<()> {
        self.reopen_for_runtime(self.limits())
    }
    /// A real retired startup uses its actual Engine generation, including
    /// after an operation unwound between metadata commit and its wake duty.
    pub fn reopen_for_runtime(&self, limits: ControlBudgetLimits) -> io::Result<()> {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !state.closed {
            return if state.limits == limits {
                Ok(())
            } else {
                Err(io::Error::other(
                    "live control owner differs from startup generation",
                ))
            };
        }
        if state.used != ControlBudgetUsage::default()
            || state.pending.is_some()
            || !self.2.retirement_complete()
        {
            return Err(capacity());
        }
        state.limits = limits;
        state.closed = false;
        drop(state);
        self.1.notify_waiters();
        Ok(())
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

impl Drop for ControlBudget {
    fn drop(&mut self) {
        // Active originals retain this same book via their real permission.
        // An idle kernel Drop may stop its receipt driver but issues no join or
        // native-close receipt; graceful callers use the explicit shutdown.
        self.2.begin_shutdown();
    }
}

mod json;
pub use json::{
    control_json_decoder_workspace, control_json_map_bound, control_json_response_storage_bound,
    control_json_storage_bound, control_json_typed_array_bound, ControlJsonStorage,
};

pub(in crate::configuration) use json::initial_storage;
