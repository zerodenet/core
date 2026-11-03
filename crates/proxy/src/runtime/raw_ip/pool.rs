mod retirement;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, Weak},
};

use tokio::sync::OnceCell;
use zero_api::OutboundDeviceHealthSnapshot;
use zero_engine::EngineError;

use super::device::SharedRawIpDevice;

pub(crate) const MAX_RAW_IP_DEVICES: usize = 128;

#[derive(Default)]
pub(crate) struct RawIpDevicePool {
    state: Arc<Mutex<PoolState>>,
}

#[derive(Default)]
struct PoolState {
    entries: HashMap<(String, usize), CachedDevice>,
    unresolved_endpoints: HashMap<(String, usize), [u8; 32]>,
    retiring: VecDeque<Arc<SharedRawIpDevice>>,
    compatibility_retiring: VecDeque<(String, Weak<SharedRawIpDevice>)>,
    compatibility_stopping: Vec<Arc<SharedRawIpDevice>>,
    shutdown: bool,
}

#[derive(Clone)]
struct CachedDevice {
    compatibility: bool,
    identity: [u8; 32],
    egress_generation: u64,
    cell: Arc<OnceCell<Arc<SharedRawIpDevice>>>,
}

pub(crate) struct StagedRawIpDevices {
    owner: Arc<Mutex<PoolState>>,
    entries: HashMap<(String, usize), CachedDevice>,
    new_devices: Vec<(Arc<SharedRawIpDevice>, bool)>,
    published: bool,
}

impl RawIpDevicePool {
    pub(crate) fn incarnations(&self, tags: &[String]) -> Vec<u64> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut ids: Vec<_> = state
            .entries
            .iter()
            .filter(|((tag, _), _)| tags.contains(tag))
            .filter_map(|(_, entry)| entry.cell.get())
            .filter(|device| device.is_usable())
            .map(|device| device.incarnation)
            .collect();
        ids.sort_unstable();
        ids
    }

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
            owner: self.state.clone(),
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
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.shutdown = false;
        state.unresolved_endpoints.clear();
        // All changed originals were confirmed before BuildNew. Only reused
        // cells may remain here; unexpected publication cannot lose an owner.
        let old = std::mem::replace(&mut state.entries, std::mem::take(&mut staged.entries));
        let mut stopped = Vec::new();
        for (key, entry) in old {
            if state
                .entries
                .get(&key)
                .is_some_and(|next| Arc::ptr_eq(&entry.cell, &next.cell))
            {
                continue;
            }
            if let Some(device) = entry.cell.get() {
                if entry.compatibility {
                    // Explicitly retained existing outer_udp_proxy behavior.
                    // Grace expiry and worker joins are NOT native-close proof.
                    if state.entries.keys().any(|(tag, _)| tag == &key.0) {
                        device.retire_after(std::time::Duration::from_secs(30));
                        state
                            .compatibility_retiring
                            .push_back((key.0, Arc::downgrade(device)));
                    } else {
                        device.close_now();
                        stopped.push(device.clone());
                        state.compatibility_stopping.push(device.clone());
                    }
                } else {
                    device.close_now();
                    state.retiring.push_back(device.clone());
                    stopped.push(device.clone());
                }
            }
        }
        let active_tags = state
            .entries
            .keys()
            .map(|(tag, _)| tag.clone())
            .collect::<std::collections::HashSet<_>>();
        let mut compatibility = std::mem::take(&mut state.compatibility_retiring);
        compatibility.retain(|(tag, weak)| {
            let Some(device) = weak.upgrade() else {
                return false;
            };
            if !active_tags.contains(tag) {
                device.close_now();
                stopped.push(device.clone());
                state.compatibility_stopping.push(device);
                false
            } else {
                true
            }
        });
        while compatibility.len() > MAX_RAW_IP_DEVICES {
            if let Some(device) = compatibility
                .pop_front()
                .and_then(|(_, weak)| weak.upgrade())
            {
                device.close_now();
                stopped.push(device.clone());
                state.compatibility_stopping.push(device);
            }
        }
        state.compatibility_retiring = compatibility;
        staged.published = true;
        stopped
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
        compatibility: bool,
    ) -> Result<(), EngineError> {
        if self.entries.len() >= MAX_RAW_IP_DEVICES {
            return Err(invalid("raw-IP device limit exceeded"));
        }
        if is_new {
            let device = cell
                .get()
                .ok_or_else(|| invalid("raw-IP device not prepared"))?;
            self.new_devices.push((device.clone(), compatibility));
        }
        if self
            .entries
            .insert(
                (tag, peer_index),
                CachedDevice {
                    compatibility,
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
            let mut state = self.owner.lock().unwrap_or_else(|e| e.into_inner());
            for (device, compatibility) in &self.new_devices {
                device.close_now();
                if *compatibility {
                    state.compatibility_stopping.push(device.clone());
                } else {
                    state.retiring.push_back(device.clone());
                }
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
mod tests;
