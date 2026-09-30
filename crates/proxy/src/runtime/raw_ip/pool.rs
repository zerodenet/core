use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use tokio::sync::OnceCell;
use zero_api::OutboundDeviceHealthSnapshot;
use zero_engine::EngineError;

use super::device::SharedRawIpDevice;

pub(crate) const MAX_RAW_IP_DEVICES: usize = 128;
const RETIRE_GRACE: Duration = Duration::from_secs(30);

#[derive(Default)]
pub(crate) struct RawIpDevicePool {
    state: Mutex<PoolState>,
}

#[derive(Default)]
struct PoolState {
    entries: HashMap<(String, usize), CachedDevice>,
    unresolved_endpoints: HashMap<(String, usize), [u8; 32]>,
    retiring: VecDeque<(String, Weak<SharedRawIpDevice>)>,
    shutdown: bool,
}

#[derive(Clone)]
struct CachedDevice {
    identity: [u8; 32],
    egress_generation: u64,
    cell: Arc<OnceCell<Arc<SharedRawIpDevice>>>,
}

pub(crate) struct StagedRawIpDevices {
    entries: HashMap<(String, usize), CachedDevice>,
    new_devices: Vec<Arc<SharedRawIpDevice>>,
    published: bool,
}

impl RawIpDevicePool {
    pub(crate) fn health_snapshot(
        &self,
        tag: &str,
        peer_index: usize,
        identity: [u8; 32],
    ) -> Option<OutboundDeviceHealthSnapshot> {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let unresolved = state
            .unresolved_endpoints
            .get(&(tag.to_owned(), peer_index))
            .is_some_and(|failed_identity| *failed_identity == identity);
        state
            .entries
            .get(&(tag.to_owned(), peer_index))
            .filter(|entry| entry.identity == identity)
            .and_then(|entry| entry.cell.get())
            .map(|device| {
                let mut snapshot = device.health_snapshot(tag.to_owned(), peer_index);
                snapshot.endpoint_resolution_failed = unresolved;
                snapshot
            })
            .or_else(|| {
                unresolved.then(|| zero_api::OutboundDeviceHealthSnapshot {
                    tag: tag.to_owned(),
                    peer_index,
                    state: zero_api::OutboundDeviceHealthState::EndpointUnresolved,
                    endpoint_resolution_failed: true,
                    ..Default::default()
                })
            })
            .or_else(|| {
                state
                    .shutdown
                    .then(|| zero_api::OutboundDeviceHealthSnapshot {
                        tag: tag.to_owned(),
                        peer_index,
                        state: zero_api::OutboundDeviceHealthState::Stopped,
                        ..Default::default()
                    })
            })
    }

    pub(crate) fn mark_endpoint_unresolved(
        &self,
        tag: &str,
        peer_index: usize,
        identity: [u8; 32],
    ) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.unresolved_endpoints.len() >= MAX_RAW_IP_DEVICES
            && !state
                .unresolved_endpoints
                .contains_key(&(tag.to_owned(), peer_index))
        {
            state.unresolved_endpoints.clear();
        }
        state
            .unresolved_endpoints
            .insert((tag.to_owned(), peer_index), identity);
    }

    pub(crate) fn clear_endpoint_unresolved(
        &self,
        tag: &str,
        peer_index: usize,
        identity: [u8; 32],
    ) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let key = (tag.to_owned(), peer_index);
        if state
            .unresolved_endpoints
            .get(&key)
            .is_some_and(|failed_identity| *failed_identity == identity)
        {
            state.unresolved_endpoints.remove(&key);
        }
    }

    pub(crate) fn begin_stage(&self) -> StagedRawIpDevices {
        StagedRawIpDevices {
            entries: HashMap::new(),
            new_devices: Vec::new(),
            published: false,
        }
    }

    pub(crate) fn reusable_cell(
        &self,
        tag: &str,
        peer_index: usize,
        identity: [u8; 32],
        egress_generation: u64,
    ) -> Option<Arc<OnceCell<Arc<SharedRawIpDevice>>>> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entries
            .get(&(tag.to_owned(), peer_index))
            .filter(|entry| {
                entry.identity == identity
                    && entry.egress_generation == egress_generation
                    && entry.cell.get().is_some_and(|device| device.is_usable())
            })
            .map(|entry| entry.cell.clone())
    }

    pub(crate) fn cell_for(
        &self,
        tag: &str,
        peer_index: usize,
        identity: [u8; 32],
        egress_generation: u64,
    ) -> Result<Arc<OnceCell<Arc<SharedRawIpDevice>>>, EngineError> {
        self.reusable_cell(tag, peer_index, identity, egress_generation)
            .ok_or_else(|| invalid("raw-IP peer device is not active"))
    }

    pub(crate) fn is_current(
        &self,
        tag: &str,
        peer_index: usize,
        identity: [u8; 32],
        egress_generation: u64,
        device: &Arc<SharedRawIpDevice>,
    ) -> bool {
        self.reusable_cell(tag, peer_index, identity, egress_generation)
            .and_then(|cell| cell.get().cloned())
            .is_some_and(|current| Arc::ptr_eq(&current, device))
    }

    pub(crate) fn publish(&self, mut staged: StagedRawIpDevices) -> Vec<Arc<SharedRawIpDevice>> {
        let mut stopped = Vec::new();
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.shutdown = false;
        state.unresolved_endpoints.clear();
        let old = std::mem::replace(&mut state.entries, std::mem::take(&mut staged.entries));
        for (key, entry) in old {
            let reused = state
                .entries
                .get(&key)
                .is_some_and(|next| Arc::ptr_eq(&entry.cell, &next.cell));
            if !reused {
                if let Some(device) = entry.cell.get() {
                    if state.entries.keys().any(|(tag, _)| tag == &key.0) {
                        device.retire_after(RETIRE_GRACE);
                        state
                            .retiring
                            .push_back((key.0.clone(), Arc::downgrade(device)));
                    } else {
                        // Removing/administratively disabling a resource must
                        // not leave a detached carrier sending during grace.
                        device.close_now();
                        stopped.push(device.clone());
                    }
                }
            }
        }
        let active_tags = state
            .entries
            .keys()
            .map(|(tag, _)| tag.clone())
            .collect::<std::collections::HashSet<_>>();
        state.retiring.retain(|(tag, weak)| {
            let Some(device) = weak.upgrade() else {
                return false;
            };
            if !active_tags.contains(tag) {
                device.close_now();
                stopped.push(device);
                false
            } else {
                true
            }
        });
        while state.retiring.len() > MAX_RAW_IP_DEVICES {
            if let Some(device) = state
                .retiring
                .pop_front()
                .and_then(|(_, device)| device.upgrade())
            {
                device.close_now();
                stopped.push(device);
            }
        }
        staged.published = true;
        stopped
    }

    pub(crate) fn shutdown(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.shutdown = true;
        for entry in state.entries.values() {
            if let Some(device) = entry.cell.get() {
                device.close_now();
            }
        }
        for (_, device) in state.retiring.drain(..) {
            if let Some(device) = device.upgrade() {
                device.close_now();
            }
        }
        state.entries.clear();
    }
}

impl StagedRawIpDevices {
    pub(crate) fn insert(
        &mut self,
        tag: String,
        peer_index: usize,
        identity: [u8; 32],
        egress_generation: u64,
        cell: Arc<OnceCell<Arc<SharedRawIpDevice>>>,
        is_new: bool,
    ) -> Result<(), EngineError> {
        if self.entries.len() >= MAX_RAW_IP_DEVICES {
            return Err(invalid("raw-IP device limit exceeded"));
        }
        if is_new {
            let device = cell
                .get()
                .ok_or_else(|| invalid("raw-IP device not prepared"))?;
            self.new_devices.push(device.clone());
        }
        if self
            .entries
            .insert(
                (tag, peer_index),
                CachedDevice {
                    identity,
                    egress_generation,
                    cell,
                },
            )
            .is_some()
        {
            return Err(invalid("duplicate raw-IP peer device"));
        }
        Ok(())
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }
}

impl Drop for StagedRawIpDevices {
    fn drop(&mut self) {
        if !self.published {
            for device in &self.new_devices {
                device.close_now();
            }
        }
    }
}

fn invalid(message: &'static str) -> EngineError {
    EngineError::Io(std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        message,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::OnceCell;

    use super::{RawIpDevicePool, MAX_RAW_IP_DEVICES};

    #[test]
    fn shutdown_reports_stopped_instead_of_not_started() {
        let pool = RawIpDevicePool::default();
        assert!(pool.health_snapshot("wg", 0, [1; 32]).is_none());
        pool.shutdown();
        assert_eq!(
            pool.health_snapshot("wg", 0, [1; 32]).unwrap().state,
            zero_api::OutboundDeviceHealthState::Stopped,
        );
    }

    #[test]
    fn unresolved_endpoint_is_reported_only_for_matching_configuration() {
        let pool = RawIpDevicePool::default();
        pool.mark_endpoint_unresolved("wg", 0, [2; 32]);
        assert!(pool.health_snapshot("wg", 0, [1; 32]).is_none());
        let snapshot = pool.health_snapshot("wg", 0, [2; 32]).unwrap();
        assert_eq!(
            snapshot.state,
            zero_api::OutboundDeviceHealthState::EndpointUnresolved
        );
        assert!(snapshot.endpoint_resolution_failed);
        pool.clear_endpoint_unresolved("wg", 0, [1; 32]);
        assert!(pool.health_snapshot("wg", 0, [2; 32]).is_some());
        pool.clear_endpoint_unresolved("wg", 0, [2; 32]);
        assert!(pool.health_snapshot("wg", 0, [2; 32]).is_none());
    }

    #[test]
    fn staged_raw_ip_devices_reject_over_limit_without_publishing() {
        let pool = RawIpDevicePool::default();
        let mut staged = pool.begin_stage();
        for index in 0..MAX_RAW_IP_DEVICES {
            staged
                .insert(
                    "wg".to_owned(),
                    index,
                    [1; 32],
                    1,
                    Arc::new(OnceCell::new()),
                    false,
                )
                .unwrap();
        }
        assert!(staged
            .insert(
                "wg".to_owned(),
                MAX_RAW_IP_DEVICES,
                [1; 32],
                1,
                Arc::new(OnceCell::new()),
                false,
            )
            .is_err());
        assert_eq!(staged.len(), MAX_RAW_IP_DEVICES);
        assert!(pool.cell_for("wg", 0, [1; 32], 1).is_err());
    }
}
