//! Prepared handles; hot path updates never touch the registry.
use super::counter::CounterSet;
use std::sync::{atomic::Ordering, Arc};
use zero_api::{TrafficMetric, TrafficPlane};

/// Prepared counter references. No protocol parsing, label allocation or registry lock on writes.
#[derive(Debug, Clone)]
pub struct TrafficMeter(pub(super) Arc<CounterSet>);
impl TrafficMeter {
    /// Declare an observed execution role when the source is prepared.
    pub fn observes_role(&self, plane: TrafficPlane, outbound: bool) {
        self.0.planes[plane as usize]
            .roles
            .fetch_or(1 << outbound as usize, Ordering::Release);
    }
    /// Set by the owning source at preparation, never on individual I/O calls.
    pub fn accounting_basis(&self, plane: TrafficPlane, basis: &'static str) {
        *self.0.planes[plane as usize]
            .basis
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(basis);
    }
    pub(crate) fn same_source(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn enable(&self, plane: TrafficPlane, metrics: &[TrafficMetric]) {
        self.0.planes[plane as usize].enable(metrics);
    }
    /// Permanently exclude metrics that cannot cover every carrier used in
    /// this resource lifetime (for example packet counts after stream I/O).
    pub fn mark_unobservable(&self, plane: TrafficPlane, metrics: &[TrafficMetric]) {
        let mask = metrics
            .iter()
            .fold(0, |mask, metric| mask | (1 << (*metric as usize)));
        self.0.planes[plane as usize]
            .unobservable
            .fetch_or(mask, Ordering::Release);
    }
    pub fn record(&self, plane: TrafficPlane, metric: TrafficMetric, amount: u64) {
        self.0.planes[plane as usize].add(metric, amount);
    }
    pub fn received(&self, plane: TrafficPlane, bytes: usize) {
        self.record(plane, TrafficMetric::RxBytes, bytes as u64);
        self.record(plane, TrafficMetric::RxPackets, 1);
    }
    pub fn sent(&self, plane: TrafficPlane, bytes: usize) {
        self.record(plane, TrafficMetric::TxBytes, bytes as u64);
        self.record(plane, TrafficMetric::TxPackets, 1);
    }
    pub fn dropped(&self, plane: TrafficPlane) {
        self.dropped_reason(plane, zero_api::TrafficDropReason::Unspecified);
    }
    pub fn dropped_reason(&self, plane: TrafficPlane, reason: zero_api::TrafficDropReason) {
        self.dropped_count(plane, reason, 1);
    }
    /// Aggregate protocol/runtime discard facts in constant time.
    pub fn dropped_count(
        &self,
        plane: TrafficPlane,
        reason: zero_api::TrafficDropReason,
        amount: u64,
    ) {
        self.0.planes[plane as usize].dropped_reason(reason, amount);
    }
    pub fn error(&self, plane: TrafficPlane) {
        self.record(plane, TrafficMetric::Errors, 1);
    }
    pub fn packet_route(&self) -> TrafficRouteLease {
        self.0.routes_available.store(1, Ordering::Release);
        self.0.active_routes.fetch_add(1, Ordering::Relaxed);
        TrafficRouteLease(self.clone())
    }
    pub(crate) fn flow(&self, network: zero_core::Network) -> TrafficFlowLease {
        self.0.flows_available.store(1, Ordering::Release);
        match network {
            zero_core::Network::Tcp => self.0.active_streams.fetch_add(1, Ordering::Relaxed),
            zero_core::Network::Udp => self.0.active_datagrams.fetch_add(1, Ordering::Relaxed),
        };
        TrafficFlowLease {
            meter: self.clone(),
            network,
        }
    }
}
#[derive(Debug)]
pub(crate) struct TrafficFlowLease {
    meter: TrafficMeter,
    network: zero_core::Network,
}
impl Drop for TrafficFlowLease {
    fn drop(&mut self) {
        match self.network {
            zero_core::Network::Tcp => self.meter.0.active_streams.fetch_sub(1, Ordering::Relaxed),
            zero_core::Network::Udp => self
                .meter
                .0
                .active_datagrams
                .fetch_sub(1, Ordering::Relaxed),
        };
    }
}
#[derive(Debug)]
pub struct TrafficRouteLease(TrafficMeter);
impl TrafficRouteLease {
    /// Prepared handles refer to the same underlying observation source.
    pub fn belongs_to(&self, meter: &TrafficMeter) -> bool {
        self.0.same_source(meter)
    }
}
impl Drop for TrafficRouteLease {
    fn drop(&mut self) {
        self.0 .0.active_routes.fetch_sub(1, Ordering::Relaxed);
    }
}
