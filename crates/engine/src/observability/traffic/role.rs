//! Actual dynamic ingress operations retain a role independently of config.
use super::{TrafficMeter, TrafficRegistry};
use std::sync::{atomic::Ordering, Arc};
use zero_api::{TrafficMetric, TrafficPlane, TrafficScope};

/// Runtime-owned ingress lifetime. Dropping the last registration retires an
/// unconfigured identity; existing connection counters and usage are untouched.
#[derive(Debug)]
pub struct InboundTrafficRegistration {
    registry: Arc<TrafficRegistry>,
    scope: TrafficScope,
}
impl TrafficRegistry {
    pub fn register_inbound(self: &Arc<Self>, tag: &str) -> InboundTrafficRegistration {
        let _guard = self.management.lock().unwrap_or_else(|e| e.into_inner());
        let scope = TrafficScope::Inbound { tag: tag.into() };
        *self
            .transient
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(scope.clone())
            .or_default() += 1;
        let meter: TrafficMeter = self.ensure(scope.clone());
        meter.enable(
            TrafficPlane::Flow,
            &[
                TrafficMetric::RxBytes,
                TrafficMetric::TxBytes,
                TrafficMetric::Errors,
            ],
        );
        InboundTrafficRegistration {
            registry: self.clone(),
            scope,
        }
    }
}
impl Drop for InboundTrafficRegistration {
    fn drop(&mut self) {
        let _guard = self
            .registry
            .management
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let mut transient = self
            .registry
            .transient
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let Some(count) = transient.get_mut(&self.scope) else {
            return;
        };
        *count -= 1;
        if *count != 0 {
            return;
        }
        transient.remove(&self.scope);
        if !self
            .registry
            .declared
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&self.scope)
            && self
                .registry
                .entries
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&self.scope)
                .is_some()
        {
            self.registry.revision.fetch_add(1, Ordering::Relaxed);
        }
    }
}
