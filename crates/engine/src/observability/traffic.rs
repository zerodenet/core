//! Current-scope registry and per-scope observation periods; quota state is separate.
mod counter;
mod host;
mod meter;
pub use host::HostInterfaceSample;
mod reset;
mod role;
pub use role::InboundTrafficRegistration;
mod source;
mod view;
pub use source::PreparedEndpointTraffic;

use counter::CounterSet;
pub(crate) use meter::TrafficFlowLease;
pub use meter::{TrafficMeter, TrafficRouteLease};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
};
use zero_api::{TrafficMetric, TrafficPlane, TrafficScope};

#[derive(Debug)]
pub(crate) struct TrafficRegistry {
    entries: RwLock<BTreeMap<TrafficScope, Arc<CounterSet>>>,
    pub(super) management: Mutex<()>,
    pub(super) revision: AtomicU64,
    pub(super) config_revision: AtomicU64,
    pending: source::PendingSources,
    transient: Mutex<BTreeMap<TrafficScope, usize>>,
    declared: Mutex<std::collections::BTreeSet<TrafficScope>>,
    source_versions: Mutex<BTreeMap<String, u64>>,
    next_source_version: AtomicU64,
    pub(crate) sample_offset: AtomicU64,
    host_sources: Mutex<BTreeMap<String, HostInterfaceSample>>,
    pub(crate) host_sample_offset: AtomicU64,
}
impl TrafficRegistry {
    pub fn new(up: Arc<AtomicU64>, down: Arc<AtomicU64>) -> Self {
        Self {
            pending: Default::default(),
            host_sources: Default::default(),
            host_sample_offset: AtomicU64::new(0),
            transient: Default::default(),
            declared: Default::default(),
            source_versions: Default::default(),
            next_source_version: AtomicU64::new(1),
            entries: RwLock::new(BTreeMap::from([(
                TrafficScope::Global,
                Arc::new(CounterSet::new(Some((up, down)))),
            )])),
            management: Mutex::new(()),
            revision: AtomicU64::new(1),
            config_revision: AtomicU64::new(1),
            sample_offset: AtomicU64::new(0),
        }
    }
    pub fn meter(&self, scope: &TrafficScope) -> Option<TrafficMeter> {
        let current = self
            .entries
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(scope)
            .cloned();
        current
            .or_else(|| {
                self.pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(scope)
                    .and_then(std::sync::Weak::upgrade)
            })
            .map(TrafficMeter)
    }
    pub fn ensure(&self, scope: TrafficScope) -> TrafficMeter {
        let mut entries = self.entries.write().unwrap_or_else(|e| e.into_inner());
        let meter = if let Some(entry) = entries.get(&scope) {
            entry.clone()
        } else {
            let entry = Arc::new(CounterSet::new(None));
            entries.insert(scope, entry.clone());
            self.revision.fetch_add(1, Ordering::Relaxed);
            entry
        };
        TrafficMeter(meter)
    }
    pub fn set_generation(&self, id: &str, generation: u64) {
        let _guard = self.management.lock().unwrap_or_else(|e| e.into_inner());
        let entries = self.entries.read().unwrap_or_else(|e| e.into_inner());
        for (scope, entry) in entries.iter() {
            if matches!(scope, TrafficScope::Endpoint { endpoint_id } | TrafficScope::Peer { endpoint_id, .. } if endpoint_id == id)
            {
                *entry.generation.lock().unwrap_or_else(|e| e.into_inner()) = Some(generation);
            }
        }
    }
    pub fn reconcile(&self, config: &zero_config::RuntimeConfig, revision: u64) {
        let _guard = self.management.lock().unwrap_or_else(|e| e.into_inner());
        self.config_revision.store(revision, Ordering::Relaxed);
        let mut current = std::collections::BTreeSet::from([
            TrafficScope::Global,
            TrafficScope::Outbound {
                tag: "direct".into(),
            },
        ]);
        current.extend(
            config
                .inbounds
                .iter()
                .map(|i| TrafficScope::Inbound { tag: i.tag.clone() }),
        );
        current.extend(
            config
                .outbounds
                .iter()
                .map(|o| TrafficScope::Outbound { tag: o.tag.clone() }),
        );
        current.extend(
            config
                .endpoint_bindings()
                .into_iter()
                .filter(|b| b.canonical)
                .map(|b| TrafficScope::Endpoint {
                    endpoint_id: b.endpoint_id,
                }),
        );
        if let Some(tun) = &config.runtime.tun {
            current.insert(TrafficScope::Inbound {
                tag: tun.tag.clone(),
            });
        }
        *self.declared.lock().unwrap_or_else(|e| e.into_inner()) = current.clone();
        current.extend(
            self.transient
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .keys()
                .cloned(),
        );
        for scope in &current {
            let meter = self.ensure(scope.clone());
            if matches!(
                scope,
                TrafficScope::Inbound { .. } | TrafficScope::Outbound { .. }
            ) {
                meter.enable(
                    TrafficPlane::Flow,
                    &[
                        TrafficMetric::RxBytes,
                        TrafficMetric::TxBytes,
                        TrafficMetric::Errors,
                    ],
                );
            }
        }
        let mut entries = self.entries.write().unwrap_or_else(|e| e.into_inner());
        let before = entries.len();
        entries.retain(|scope, _| current.contains(scope) || matches!(scope, TrafficScope::HostInterface { .. }) || matches!(scope, TrafficScope::Peer { endpoint_id, .. } if current.contains(&TrafficScope::Endpoint { endpoint_id: endpoint_id.clone() })));
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|scope, _| match scope {
                TrafficScope::Endpoint { endpoint_id } | TrafficScope::Peer { endpoint_id, .. } => {
                    current.contains(&TrafficScope::Endpoint {
                        endpoint_id: endpoint_id.clone(),
                    })
                }
                _ => current.contains(scope),
            });
        self.source_versions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|id, _| {
                current.contains(&TrafficScope::Endpoint {
                    endpoint_id: id.clone(),
                })
            });
        if entries.len() != before {
            self.revision.fetch_add(1, Ordering::Relaxed);
        }
    }
}
