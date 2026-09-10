use std::sync::{Arc, Mutex};

use crate::client::ClientConnection;

use super::{ClientPoolPolicy, ClientPoolSnapshot};

pub(super) struct Slot {
    state: Mutex<SlotState>,
    pub(super) changed: tokio::sync::watch::Sender<u64>,
}

#[derive(Default)]
struct SlotState {
    connections: Vec<Arc<ClientConnection>>,
    dialing: bool,
    retired: bool,
    next: usize,
}

pub(super) enum DialPlan {
    Start,
    Wait,
    Retry,
    Retired,
    Capacity,
}

impl Slot {
    pub(super) fn new() -> Self {
        let (changed, _) = tokio::sync::watch::channel(0);
        Self {
            state: Mutex::new(SlotState::default()),
            changed,
        }
    }

    pub(super) fn has_live_work(&self) -> bool {
        let mut state = self.state.lock().unwrap();
        state
            .connections
            .retain(|connection| !connection.is_closed());
        state.dialing || !state.connections.is_empty()
    }

    pub(super) fn retire(&self) {
        let mut state = self.state.lock().unwrap();
        state.retired = true;
        state.connections.clear();
        drop(state);
        self.signal();
    }

    pub(super) fn connections(
        &self,
        policy: &ClientPoolPolicy,
    ) -> Option<Vec<Arc<ClientConnection>>> {
        let mut state = self.state.lock().unwrap();
        if state.retired {
            return None;
        }
        state.connections.retain(|connection| {
            !connection.is_closed() && connection.age() < policy.max_connection_age
        });
        let count = state.connections.len();
        if count == 0 {
            return Some(Vec::new());
        }
        let start = state.next % count;
        state.next = (start + 1) % count;
        let mut connections = Vec::with_capacity(count);
        connections.extend(state.connections[start..].iter().cloned());
        connections.extend(state.connections[..start].iter().cloned());
        connections.sort_by_key(|connection| connection.load());
        Some(connections)
    }

    pub(super) fn plan_growth(&self, policy: &ClientPoolPolicy) -> DialPlan {
        let mut state = self.state.lock().unwrap();
        if state.retired {
            return DialPlan::Retired;
        }
        state.connections.retain(|connection| {
            !connection.is_closed() && connection.age() < policy.max_connection_age
        });
        if state.connections.is_empty()
            || state.connections.len() >= policy.max_connections_per_identity
            || state.dialing
            || state
                .connections
                .iter()
                .any(|connection| connection.load() < policy.scale_out_load(connection))
        {
            return DialPlan::Retry;
        }
        state.dialing = true;
        DialPlan::Start
    }

    pub(super) fn remove(&self, connection: &Arc<ClientConnection>) {
        let mut state = self.state.lock().unwrap();
        let previous = state.connections.len();
        state
            .connections
            .retain(|candidate| !Arc::ptr_eq(candidate, connection));
        let changed = previous != state.connections.len();
        drop(state);
        if changed {
            self.signal();
        }
    }

    pub(super) fn plan_dial(
        &self,
        attempted: &[Arc<ClientConnection>],
        policy: &ClientPoolPolicy,
    ) -> DialPlan {
        let mut state = self.state.lock().unwrap();
        if state.retired {
            return DialPlan::Retired;
        }
        state
            .connections
            .retain(|connection| !connection.is_closed());
        if state.connections.iter().any(|connection| {
            !attempted
                .iter()
                .any(|candidate| Arc::ptr_eq(candidate, connection))
        }) {
            return DialPlan::Retry;
        }
        if state.dialing {
            return DialPlan::Wait;
        }
        if state.connections.len() >= policy.max_connections_per_identity {
            return DialPlan::Capacity;
        }
        state.dialing = true;
        DialPlan::Start
    }

    fn finish_dial(&self, connection: Option<Arc<ClientConnection>>) {
        let mut state = self.state.lock().unwrap();
        state.dialing = false;
        if !state.retired {
            if let Some(connection) = connection {
                state.connections.push(connection);
            }
        }
        drop(state);
        self.signal();
    }

    pub(super) fn update_snapshot(
        &self,
        policy: &ClientPoolPolicy,
        snapshot: &mut ClientPoolSnapshot,
    ) {
        let state = self.state.lock().unwrap();
        snapshot.dialing_identities += usize::from(state.dialing);
        for connection in state.connections.iter().filter(|connection| {
            !connection.is_closed() && connection.age() < policy.max_connection_age
        }) {
            snapshot.connections += 1;
            snapshot.active_streams += connection.active_streams();
            snapshot.pending_opens += connection.pending_opens();
        }
    }

    fn signal(&self) {
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }
}

pub(super) struct DialGuard {
    slot: Arc<Slot>,
    active: bool,
}

impl DialGuard {
    pub(super) fn new(slot: Arc<Slot>) -> Self {
        Self { slot, active: true }
    }

    pub(super) fn finish(mut self, connection: Option<Arc<ClientConnection>>) {
        self.active = false;
        self.slot.finish_dial(connection);
    }
}

impl Drop for DialGuard {
    fn drop(&mut self) {
        if self.active {
            self.slot.finish_dial(None);
        }
    }
}
