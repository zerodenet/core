//! Staged counter attachment. Candidate devices may emit traffic before commit;
//! their new identities become queryable only when the runtime publishes them.
use super::{counter::CounterSet, TrafficMeter, TrafficRegistry};
use std::{
    collections::BTreeMap,
    sync::atomic::Ordering,
    sync::{Arc, Weak},
};
use zero_api::TrafficScope;

pub struct PreparedEndpointTraffic {
    registry: Arc<TrafficRegistry>,
    id: String,
    version: u64,
    endpoint: TrafficMeter,
    peers: Vec<(String, TrafficMeter)>,
}
impl PreparedEndpointTraffic {
    pub fn endpoint(&self) -> TrafficMeter {
        self.endpoint.clone()
    }
    pub fn peers(&self) -> Vec<TrafficMeter> {
        self.peers.iter().map(|(_, meter)| meter.clone()).collect()
    }
    pub fn publish(&self) {
        let _guard = self
            .registry
            .management
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if self
            .registry
            .source_versions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&self.id)
            != Some(&self.version)
        {
            return;
        }
        let mut entries = self
            .registry
            .entries
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let scope = TrafficScope::Endpoint {
            endpoint_id: self.id.clone(),
        };
        if !entries
            .get(&scope)
            .is_some_and(|counter| Arc::ptr_eq(counter, &self.endpoint.0))
            && !self
                .registry
                .pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&scope)
                .and_then(Weak::upgrade)
                .is_some_and(|counter| Arc::ptr_eq(&counter, &self.endpoint.0))
        {
            return;
        }
        entries.insert(
            TrafficScope::Endpoint {
                endpoint_id: self.id.clone(),
            },
            self.endpoint.0.clone(),
        );
        for (id, meter) in &self.peers {
            entries.insert(
                TrafficScope::Peer {
                    endpoint_id: self.id.clone(),
                    peer_id: id.clone(),
                },
                meter.0.clone(),
            );
        }
        entries.retain(|scope, _| !matches!(scope, TrafficScope::Peer { endpoint_id, peer_id } if endpoint_id == &self.id && !self.peers.iter().any(|(id,_)| id == peer_id)));
        self.registry.pending.lock().unwrap_or_else(|e| e.into_inner()).retain(|scope, _| !matches!(scope, TrafficScope::Endpoint { endpoint_id } | TrafficScope::Peer { endpoint_id, .. } if endpoint_id == &self.id));
        self.registry.revision.fetch_add(1, Ordering::Relaxed);
    }
}
impl TrafficRegistry {
    pub fn prepare_endpoint(
        self: &Arc<Self>,
        id: &str,
        peers: &[String],
    ) -> PreparedEndpointTraffic {
        let _guard = self.management.lock().unwrap_or_else(|e| e.into_inner());
        let entries = self.entries.read().unwrap_or_else(|e| e.into_inner());
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.retain(|_, counter| counter.strong_count() != 0);
        let mut get = |scope: TrafficScope| {
            let counter = entries
                .get(&scope)
                .cloned()
                .or_else(|| pending.get(&scope).and_then(Weak::upgrade))
                .unwrap_or_else(|| Arc::new(CounterSet::new(None)));
            pending.insert(scope, Arc::downgrade(&counter));
            TrafficMeter(counter)
        };
        let endpoint = get(TrafficScope::Endpoint {
            endpoint_id: id.into(),
        });
        let generation = *endpoint
            .0
            .generation
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let peers = peers
            .iter()
            .map(|peer| {
                let meter = get(TrafficScope::Peer {
                    endpoint_id: id.into(),
                    peer_id: peer.clone(),
                });
                *meter.0.generation.lock().unwrap_or_else(|e| e.into_inner()) = generation;
                (peer.clone(), meter)
            })
            .collect();
        let version = self.next_source_version.fetch_add(1, Ordering::Relaxed);
        self.source_versions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id.into(), version);
        PreparedEndpointTraffic {
            registry: self.clone(),
            id: id.into(),
            version,
            endpoint,
            peers,
        }
    }
}
pub(super) type PendingSources = std::sync::Mutex<BTreeMap<TrafficScope, Weak<CounterSet>>>;
