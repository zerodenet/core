use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use super::plan::{TargetId, UrlTestSelection};

mod reload;
mod selection;
use selection::Signal;
pub use selection::{GroupSelectionBinding, GroupSelectionChanged, SelectionChange};

#[derive(Debug, Default)]
pub(crate) struct OutboundGroupStateStore {
    selector: Mutex<HashMap<TargetId, SelectorState>>,
    urltest: Mutex<HashMap<TargetId, UrlTestState>>,
    loadbalance: Mutex<HashMap<TargetId, u64>>,
}

#[derive(Debug)]
struct SelectorState {
    selected: TargetId,
    signal: Arc<Signal>,
}
#[derive(Debug)]
struct UrlTestState {
    state: UrlTestGroupState,
    signal: Arc<Signal>,
}
fn wake(waiters: Vec<std::task::Waker>) {
    for waiter in waiters {
        waiter.wake();
    }
}

impl OutboundGroupStateStore {
    pub(crate) fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn initialize_selector(&self, group_id: TargetId, selected: TargetId) {
        self.selector
            .lock()
            .expect("selector group state lock poisoned")
            .insert(
                group_id,
                SelectorState {
                    selected,
                    signal: Arc::default(),
                },
            );
    }
    pub(crate) fn update_selector(&self, group_id: TargetId, selected: TargetId) {
        let waiters = {
            let mut states = self
                .selector
                .lock()
                .expect("selector group state lock poisoned");
            match states.get_mut(&group_id) {
                Some(state) if state.selected != selected => {
                    state.selected = selected;
                    state.signal.advance()
                }
                Some(_) => Vec::new(),
                None => {
                    states.insert(
                        group_id,
                        SelectorState {
                            selected,
                            signal: Arc::default(),
                        },
                    );
                    Vec::new()
                }
            }
        };
        wake(waiters);
    }
    pub(crate) fn selector_selected_target(&self, group_id: TargetId) -> Option<TargetId> {
        self.selector
            .lock()
            .expect("selector group state lock poisoned")
            .get(&group_id)
            .map(|state| state.selected)
    }
    pub(crate) fn selection_binding(&self, group_id: TargetId) -> Option<GroupSelectionBinding> {
        if let Some(state) = self
            .selector
            .lock()
            .expect("selector group state lock poisoned")
            .get(&group_id)
        {
            return Some(state.signal.capture(state.selected));
        }
        self.urltest
            .lock()
            .expect("urltest group state lock poisoned")
            .get(&group_id)
            .map(|state| state.signal.capture(state.state.selected))
    }

    pub(crate) fn initialize_urltest(
        &self,
        group_id: TargetId,
        selected: TargetId,
        members: &[TargetId],
    ) {
        self.urltest
            .lock()
            .expect("urltest group state lock poisoned")
            .insert(
                group_id,
                UrlTestState {
                    signal: Arc::default(),
                    state: UrlTestGroupState {
                        selected,
                        latency_ms: None,
                        last_checked_unix_ms: None,
                        selection: None,
                        members: members
                            .iter()
                            .map(|member_id| UrlTestMemberState {
                                member_id: *member_id,
                                healthy: false,
                                latency_ms: None,
                                last_checked_unix_ms: None,
                                last_error: None,
                                effective_chains: Vec::new(),
                            })
                            .collect(),
                    },
                },
            );
    }

    pub(crate) fn update_urltest(
        &self,
        group_id: TargetId,
        selected: TargetId,
        latency_ms: Option<u64>,
        members: Vec<UrlTestMemberState>,
        selection: UrlTestSelection,
    ) {
        let next = UrlTestGroupState {
            selected,
            latency_ms,
            last_checked_unix_ms: Some(unix_timestamp_ms()),
            members,
            selection: Some(selection),
        };
        let waiters = {
            let mut states = self
                .urltest
                .lock()
                .expect("urltest group state lock poisoned");
            match states.get_mut(&group_id) {
                Some(state) => {
                    let waiters = if state.state.selected != selected {
                        state.signal.advance()
                    } else {
                        Vec::new()
                    };
                    state.state = next;
                    waiters
                }
                None => {
                    states.insert(
                        group_id,
                        UrlTestState {
                            state: next,
                            signal: Arc::default(),
                        },
                    );
                    Vec::new()
                }
            }
        };
        wake(waiters);
    }

    pub(crate) fn selected_target(&self, group_id: TargetId) -> Option<TargetId> {
        self.urltest_selected_target(group_id)
            .or_else(|| self.selector_selected_target(group_id))
    }

    /// Change selection without fabricating a probe or overwriting new results.
    pub(crate) fn switch_urltest(
        &self,
        group_id: TargetId,
        expected: &UrlTestGroupState,
        selection: UrlTestSelection,
    ) -> bool {
        let mut states = self
            .urltest
            .lock()
            .expect("urltest group state lock poisoned");
        let Some(state) = states.get_mut(&group_id) else {
            return false;
        };
        if &state.state != expected {
            return false;
        }
        let waiters = if state.state.selected != selection.selected {
            state.signal.advance()
        } else {
            Vec::new()
        };
        state.state.selected = selection.selected;
        state.state.latency_ms = selection.best_latency_ms;
        state.state.selection = Some(selection);
        drop(states);
        wake(waiters);
        true
    }

    pub(crate) fn urltest_state(&self, group_id: TargetId) -> Option<UrlTestGroupState> {
        self.urltest
            .lock()
            .expect("urltest group state lock poisoned")
            .get(&group_id)
            .map(|state| state.state.clone())
    }

    pub(crate) fn urltest_selected_target(&self, group_id: TargetId) -> Option<TargetId> {
        self.urltest
            .lock()
            .expect("urltest group state lock poisoned")
            .get(&group_id)
            .map(|state| state.state.selected)
    }

    pub(crate) fn initialize_loadbalance(&self, group_id: TargetId) {
        self.loadbalance
            .lock()
            .expect("loadbalance group state lock poisoned")
            .entry(group_id)
            .or_insert(0);
    }

    pub(crate) fn loadbalance_next_pick(&self, group_id: TargetId, member_count: usize) -> usize {
        let mut map = self
            .loadbalance
            .lock()
            .expect("loadbalance group state lock poisoned");
        let counter = map.entry(group_id).or_insert(0);
        let index = *counter as usize % member_count;
        *counter += 1;
        index
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UrlTestGroupState {
    pub selected: TargetId,
    pub latency_ms: Option<u64>,
    pub last_checked_unix_ms: Option<u64>,
    pub members: Vec<UrlTestMemberState>,
    pub selection: Option<UrlTestSelection>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UrlTestMemberState {
    pub member_id: TargetId,
    pub healthy: bool,
    pub latency_ms: Option<u64>,
    pub last_checked_unix_ms: Option<u64>,
    pub last_error: Option<String>,
    pub effective_chains: Vec<Vec<TargetId>>,
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_millis() as u64
}
