//! Read-only host provider ingestion, isolated from business/endpoint accounting.
use super::{
    counter::{epoch, now, CounterSet},
    TrafficRegistry,
};
use std::{
    collections::BTreeSet,
    sync::{atomic::Ordering, Arc},
};
use zero_api::{TrafficPlane, TrafficScope};

/// One authoritative interface read. Slots follow TrafficMetric::ALL.
/// Missing metrics stay unavailable. Providers must supply a complete, bounded
/// inventory, not a partial page; failures must be submitted as None.
#[derive(Debug, Clone)]
pub struct HostInterfaceSample {
    pub name: String,
    pub index: u32,
    pub accounting_basis: &'static str,
    pub counters: [Option<u64>; 12],
    pub sampled_at_unix_ms: u64,
}
impl TrafficRegistry {
    pub(crate) fn host_scopes(&self) -> Vec<TrafficScope> {
        self.host_sources
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .map(|name| TrafficScope::HostInterface { name: name.clone() })
            .collect()
    }
    pub fn observe_host_interfaces(&self, samples: Option<&[HostInterfaceSample]>) {
        let _guard = self.management.lock().unwrap_or_else(|e| e.into_inner());
        let mut sources = self.host_sources.lock().unwrap_or_else(|e| e.into_inner());
        let mut entries = self.entries.write().unwrap_or_else(|e| e.into_inner());
        let samples = samples.filter(|samples| {
            samples.len() <= 256 && {
                let mut names = BTreeSet::new();
                samples.iter().all(|s| {
                    s.index != 0
                        && !s.name.is_empty()
                        && s.name.len() <= 128
                        && names.insert(&s.name)
                })
            }
        });
        let Some(samples) = samples else {
            for (scope, counter) in entries.iter() {
                if matches!(scope, TrafficScope::HostInterface { .. }) {
                    counter.planes[TrafficPlane::Host as usize].suspend_host();
                }
            }
            return;
        };
        let names: BTreeSet<_> = samples.iter().map(|s| &s.name).collect();
        let before = entries.len();
        entries.retain(|scope, _| !matches!(scope, TrafficScope::HostInterface { name } if !names.contains(name)));
        sources.retain(|name, _| names.contains(name));
        if entries.len() != before {
            self.revision.fetch_add(1, Ordering::Relaxed);
        }
        for sample in samples {
            let scope = TrafficScope::HostInterface {
                name: sample.name.clone(),
            };
            let previous = sources.get(&sample.name);
            // Reused indices, source counter reset/wrap, or provider schema changes
            // create a fresh source generation and epoch, never a negative rate.
            let replaced =
                previous.is_none_or(|old| {
                    old.index != sample.index
                        || old.accounting_basis != sample.accounting_basis
                        || old.counters.iter().zip(sample.counters).any(|(old, new)| {
                            match (old, new) {
                                (Some(old), Some(new)) => new < *old,
                                (None, None) => false,
                                _ => true,
                            }
                        })
                });
            if replaced {
                let counter = Arc::new(CounterSet::new(None));
                let generation = self.next_source_version.fetch_add(1, Ordering::Relaxed);
                *counter.generation.lock().unwrap_or_else(|e| e.into_inner()) = Some(generation);
                counter.planes[TrafficPlane::Host as usize]
                    .replace_host(&sample.counters, sample.accounting_basis);
                let mut period = counter.period.lock().unwrap_or_else(|e| e.into_inner());
                period.baseline = counter.capture();
                period.epoch = epoch();
                period.started = now();
                drop(period);
                entries.insert(scope.clone(), counter);
                self.revision.fetch_add(1, Ordering::Relaxed);
            } else if let Some(counter) = entries.get(&scope) {
                counter.planes[TrafficPlane::Host as usize]
                    .replace_host(&sample.counters, sample.accounting_basis);
            }
            if let Some(counter) = entries.get(&scope) {
                counter.source_sampled_monotonic.store(
                    counter.clock.elapsed().as_nanos().min(u64::MAX as u128) as u64,
                    Ordering::Relaxed,
                );
                counter
                    .source_sampled_at
                    .store(sample.sampled_at_unix_ms, Ordering::Relaxed);
            }
            sources.insert(sample.name.clone(), sample.clone());
        }
    }
}
