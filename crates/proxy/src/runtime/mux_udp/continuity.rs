use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use zero_core::UdpContinuityKey;

use crate::runtime::udp_dispatch::UdpDispatch;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct MuxUdpContinuityScope {
    inbound_tag: String,
    protocol: &'static str,
    principal_key: Option<String>,
    continuity_key: UdpContinuityKey,
}

impl MuxUdpContinuityScope {
    pub(crate) fn new(
        inbound_tag: &str,
        protocol: &'static str,
        principal_key: Option<&str>,
        continuity_key: UdpContinuityKey,
    ) -> Self {
        Self {
            inbound_tag: inbound_tag.to_owned(),
            protocol,
            principal_key: principal_key.map(str::to_owned),
            continuity_key,
        }
    }
}

pub(crate) enum MuxUdpContinuityAttach<T = UdpDispatch> {
    New {
        generation: u64,
    },
    Reattached {
        generation: u64,
        dispatch: Option<T>,
    },
    Conflict {
        generation: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MuxUdpContinuitySnapshot {
    pub(crate) attached: usize,
    pub(crate) retained: usize,
}

pub(crate) struct MuxUdpContinuityPrune<T = UdpDispatch> {
    pub(crate) removed: usize,
    pub(crate) dispatches: Vec<T>,
}

pub(crate) enum MuxUdpDetachedCancellation {
    Retained,
    Cancelled(Box<UdpDispatch>),
    Gone,
}

struct ContinuityEntry<T> {
    generation: u64,
    attached: bool,
    expires_at: Instant,
    dispatch: Option<T>,
}

pub(crate) struct MuxUdpContinuityRegistry<T = UdpDispatch> {
    entries: Arc<Mutex<HashMap<MuxUdpContinuityScope, ContinuityEntry<T>>>>,
}

impl<T> Clone for MuxUdpContinuityRegistry<T> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
        }
    }
}

impl<T> Default for MuxUdpContinuityRegistry<T> {
    fn default() -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<T> MuxUdpContinuityRegistry<T> {
    pub(crate) fn attach(
        &self,
        scope: MuxUdpContinuityScope,
        retention: Duration,
    ) -> MuxUdpContinuityAttach<T> {
        let now = Instant::now();
        let mut entries = self.entries.lock().expect("continuity registry poisoned");

        match entries.get_mut(&scope) {
            Some(entry) if entry.attached => MuxUdpContinuityAttach::Conflict {
                generation: entry.generation,
            },
            Some(entry) => {
                entry.generation = entry.generation.saturating_add(1);
                entry.attached = true;
                entry.expires_at = now + retention;
                MuxUdpContinuityAttach::Reattached {
                    generation: entry.generation,
                    dispatch: entry.dispatch.take(),
                }
            }
            None => {
                entries.insert(
                    scope,
                    ContinuityEntry {
                        generation: 1,
                        attached: true,
                        expires_at: now + retention,
                        dispatch: None,
                    },
                );
                MuxUdpContinuityAttach::New { generation: 1 }
            }
        }
    }

    pub(crate) fn detach(
        &self,
        scope: &MuxUdpContinuityScope,
        generation: u64,
        retention: Duration,
        dispatch: Option<T>,
    ) -> Result<(), Option<T>> {
        let mut entries = self.entries.lock().expect("continuity registry poisoned");
        let Some(entry) = entries.get_mut(scope) else {
            return Err(dispatch);
        };
        if entry.generation != generation || !entry.attached {
            return Err(dispatch);
        }
        entry.attached = false;
        entry.expires_at = Instant::now() + retention;
        entry.dispatch = dispatch;
        Ok(())
    }

    pub(crate) fn finish(&self, scope: &MuxUdpContinuityScope, generation: u64) -> bool {
        let mut entries = self.entries.lock().expect("continuity registry poisoned");
        if entries
            .get(scope)
            .is_some_and(|entry| entry.generation == generation)
        {
            entries.remove(scope);
            return true;
        }
        false
    }

    pub(crate) fn expire(&self, scope: &MuxUdpContinuityScope, generation: u64) -> Option<T> {
        let mut entries = self.entries.lock().expect("continuity registry poisoned");
        let expired = entries.get(scope).is_some_and(|entry| {
            entry.generation == generation && !entry.attached && entry.expires_at <= Instant::now()
        });
        expired
            .then(|| entries.remove(scope))
            .flatten()
            .and_then(|entry| entry.dispatch)
    }

    pub(crate) fn prune_expired(&self) -> MuxUdpContinuityPrune<T> {
        let mut entries = self.entries.lock().expect("continuity registry poisoned");
        let now = Instant::now();
        let expired_scopes = entries
            .iter()
            .filter(|(_, entry)| !entry.attached && entry.expires_at <= now)
            .map(|(scope, _)| scope.clone())
            .collect::<Vec<_>>();
        let removed = expired_scopes.len();
        let dispatches = expired_scopes
            .into_iter()
            .filter_map(|scope| entries.remove(&scope).and_then(|entry| entry.dispatch))
            .collect();
        MuxUdpContinuityPrune {
            removed,
            dispatches,
        }
    }

    pub(crate) fn snapshot(&self) -> MuxUdpContinuitySnapshot {
        let entries = self.entries.lock().expect("continuity registry poisoned");
        MuxUdpContinuitySnapshot {
            attached: entries.values().filter(|entry| entry.attached).count(),
            retained: entries.values().filter(|entry| !entry.attached).count(),
        }
    }
}

impl MuxUdpContinuityRegistry<UdpDispatch> {
    pub(crate) fn poll_detached_cancellation(
        &self,
        scope: &MuxUdpContinuityScope,
        generation: u64,
    ) -> MuxUdpDetachedCancellation {
        let mut entries = self.entries.lock().expect("continuity registry poisoned");
        let cancelled = match entries.get_mut(scope) {
            Some(entry) if entry.generation == generation && !entry.attached => entry
                .dispatch
                .as_mut()
                .is_some_and(UdpDispatch::finish_pending_cancellations),
            Some(_) => return MuxUdpDetachedCancellation::Gone,
            None => return MuxUdpDetachedCancellation::Gone,
        };
        if !cancelled {
            return MuxUdpDetachedCancellation::Retained;
        }
        entries
            .remove(scope)
            .and_then(|entry| entry.dispatch)
            .map_or(MuxUdpDetachedCancellation::Gone, |dispatch| {
                MuxUdpDetachedCancellation::Cancelled(Box::new(dispatch))
            })
    }
}
