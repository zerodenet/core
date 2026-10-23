//! Generation-bound carrier observations without retaining historical config.
use std::sync::{Arc, RwLock, Weak};
use zero_config::RuntimeConfig;

use super::{reconcile_group_health, Engine, EngineRuntimeSnapshot};
use crate::{health::OutboundHealth, observability::EngineEventLog};

/// A bound carrier-health capability. It retains no historical configuration,
/// protocol pool, component registry, or business request.
#[derive(Clone)]
pub struct OutboundHealthObservation {
    current: Weak<RwLock<Arc<EngineRuntimeSnapshot>>>,
    generation: Weak<RuntimeConfig>,
    health: Weak<OutboundHealth>,
    events: Weak<EngineEventLog>,
    tag: Arc<str>,
}
impl Engine {
    pub fn bind_outbound_health(
        &self,
        snapshot: &EngineRuntimeSnapshot,
        tag: impl Into<Arc<str>>,
    ) -> OutboundHealthObservation {
        OutboundHealthObservation {
            current: Arc::downgrade(&self.runtime_snapshot),
            generation: Arc::downgrade(snapshot.config()),
            health: Arc::downgrade(&self.outbound_health),
            events: Arc::downgrade(&self.event_log),
            tag: tag.into(),
        }
    }
}
impl OutboundHealthObservation {
    fn with_current(
        &self,
        record: impl FnOnce(&EngineRuntimeSnapshot, &OutboundHealth, &EngineEventLog),
    ) {
        let (Some(state), Some(health), Some(events)) = (
            self.current.upgrade(),
            self.health.upgrade(),
            self.events.upgrade(),
        ) else {
            return;
        };
        let current = state.read().expect("runtime snapshot lock poisoned");
        if Weak::ptr_eq(&self.generation, &Arc::downgrade(current.config())) {
            record(&current, &health, &events);
        }
    }
    pub fn record_success(&self) {
        self.with_current(|_, health, _| health.record_success(&self.tag));
    }
    pub fn record_failure(&self) {
        self.with_current(|snapshot, health, events| {
            if health.record_failure(&self.tag) {
                for &group in snapshot.plan().urltest_groups() {
                    reconcile_group_health(snapshot, group, health, events);
                }
            }
        });
    }
}
