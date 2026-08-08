use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use super::plan::{TargetId, UrlTestSelection};

#[derive(Debug, Default)]
pub(crate) struct OutboundGroupStateStore {
    selector: Mutex<HashMap<TargetId, TargetId>>,
    urltest: Mutex<HashMap<TargetId, UrlTestGroupState>>,
    loadbalance: Mutex<HashMap<TargetId, u64>>,
}

impl OutboundGroupStateStore {
    pub(crate) fn shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn initialize_selector(&self, group_id: TargetId, selected: TargetId) {
        self.selector
            .lock()
            .expect("selector group state lock poisoned")
            .insert(group_id, selected);
    }

    pub(crate) fn update_selector(&self, group_id: TargetId, selected: TargetId) {
        self.selector
            .lock()
            .expect("selector group state lock poisoned")
            .insert(group_id, selected);
    }

    pub(crate) fn selector_selected_target(&self, group_id: TargetId) -> Option<TargetId> {
        self.selector
            .lock()
            .expect("selector group state lock poisoned")
            .get(&group_id)
            .copied()
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
                UrlTestGroupState {
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
        self.urltest
            .lock()
            .expect("urltest group state lock poisoned")
            .insert(
                group_id,
                UrlTestGroupState {
                    selected,
                    latency_ms,
                    last_checked_unix_ms: Some(unix_timestamp_ms()),
                    members,
                    selection: Some(selection),
                },
            );
    }

    pub(crate) fn selected_target(&self, group_id: TargetId) -> Option<TargetId> {
        self.urltest_selected_target(group_id)
            .or_else(|| self.selector_selected_target(group_id))
    }

    pub(crate) fn urltest_state(&self, group_id: TargetId) -> Option<UrlTestGroupState> {
        self.urltest
            .lock()
            .expect("urltest group state lock poisoned")
            .get(&group_id)
            .cloned()
    }

    pub(crate) fn urltest_selected_target(&self, group_id: TargetId) -> Option<TargetId> {
        self.urltest
            .lock()
            .expect("urltest group state lock poisoned")
            .get(&group_id)
            .map(|state| state.selected)
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
