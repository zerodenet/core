//! Fixed atomic slots shared by runtime meter handles and observation views.
use std::sync::{
    atomic::{AtomicU16, AtomicU64, Ordering},
    Arc, Mutex,
};
use zero_api::{TrafficCounters, TrafficMetric, TrafficPlane, TrafficPlaneSnapshot};

pub(super) const WIDTH: usize = 8;
pub(super) type Values = [[u64; WIDTH]; 3];

#[derive(Debug)]
pub(super) struct Plane {
    values: [Arc<AtomicU64>; WIDTH],
    available: AtomicU16,
    basis: Mutex<Option<&'static str>>,
    roles: AtomicU16,
}
impl Default for Plane {
    fn default() -> Self {
        Self {
            values: std::array::from_fn(|_| Arc::new(AtomicU64::new(0))),
            available: AtomicU16::new(0),
            basis: Mutex::new(None),
            roles: AtomicU16::new(0),
        }
    }
}
impl Plane {
    pub fn flow_global(up: Arc<AtomicU64>, down: Arc<AtomicU64>) -> Self {
        let mut plane = Self::default();
        plane.values[0] = up;
        plane.values[1] = down;
        plane.enable(&[
            TrafficMetric::BytesUp,
            TrafficMetric::BytesDown,
            TrafficMetric::Errors,
        ]);
        plane
    }
    pub fn enable(&self, metrics: &[TrafficMetric]) {
        let mask = metrics
            .iter()
            .fold(0, |mask, metric| mask | (1 << (*metric as usize)));
        self.available.fetch_or(mask, Ordering::Release);
    }
    pub fn add(&self, metric: TrafficMetric, value: u64) {
        if value != 0 {
            let _ = self.values[metric as usize].fetch_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |old| Some(old.saturating_add(value)),
            );
        }
    }
    pub fn capture(&self) -> [u64; WIDTH] {
        std::array::from_fn(|i| self.values[i].load(Ordering::Relaxed))
    }
    pub fn project(
        &self,
        plane: TrafficPlane,
        values: [u64; WIDTH],
        baseline: [u64; WIDTH],
    ) -> TrafficPlaneSnapshot {
        let mask = self.available.load(Ordering::Acquire);
        let get = |i: usize| (mask & (1 << i) != 0).then(|| values[i].saturating_sub(baseline[i]));
        let available = TrafficMetric::ALL
            .into_iter()
            .filter(|m| mask & (1 << (*m as usize)) != 0)
            .collect::<Vec<_>>();
        TrafficPlaneSnapshot {
            source_roles: [
                zero_api::TrafficRole::Inbound,
                zero_api::TrafficRole::Outbound,
            ]
            .into_iter()
            .enumerate()
            .filter(|(i, _)| self.roles.load(Ordering::Acquire) & (1 << i) != 0)
            .map(|(_, role)| role)
            .collect(),
            plane,
            accounting_basis: self
                .basis
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .unwrap_or(match plane {
                    TrafficPlane::Flow => "business_boundary_bytes",
                    TrafficPlane::Inner => "authenticated_ip_packets_before_reassembly",
                    TrafficPlane::Outer => "protocol_datagram_payload_excludes_host_ip_udp_headers",
                })
                .into(),
            counters: TrafficCounters {
                bytes_up: get(0),
                bytes_down: get(1),
                rx_bytes: get(2),
                tx_bytes: get(3),
                rx_packets: get(4),
                tx_packets: get(5),
                dropped_packets: get(6),
                errors: get(7),
            },
            resettable_metrics: available.clone(),
            available_metrics: available,
        }
    }
}
#[derive(Debug)]
pub(super) struct Period {
    pub epoch: String,
    pub started: u64,
    pub baseline: Values,
}
#[derive(Debug)]
pub(super) struct CounterSet {
    pub clock: std::time::Instant,
    pub planes: [Plane; 3],
    pub period: Mutex<Period>,
    pub generation: Mutex<Option<u64>>,
    pub active_routes: AtomicU64,
    pub routes_available: AtomicU16,
    pub active_streams: AtomicU64,
    pub active_datagrams: AtomicU64,
    pub flows_available: AtomicU16,
}
impl CounterSet {
    pub fn new(global: Option<(Arc<AtomicU64>, Arc<AtomicU64>)>) -> Self {
        let mut planes = std::array::from_fn(|_| Plane::default());
        if let Some((up, down)) = global {
            planes[0] = Plane::flow_global(up, down);
        }
        Self {
            clock: std::time::Instant::now(),
            planes,
            period: Mutex::new(Period {
                epoch: epoch(),
                started: now(),
                baseline: [[0; WIDTH]; 3],
            }),
            generation: Mutex::new(None),
            active_routes: AtomicU64::new(0),
            routes_available: AtomicU16::new(0),
            active_streams: AtomicU64::new(0),
            active_datagrams: AtomicU64::new(0),
            flows_available: AtomicU16::new(0),
        }
    }
    pub fn capture(&self) -> Values {
        std::array::from_fn(|i| self.planes[i].capture())
    }
}
pub(super) fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
pub(super) fn epoch() -> String {
    format!("{:032x}", rand::random::<u128>())
}

/// Prepared counter references. No protocol parsing, label allocation or registry lock on writes.
#[derive(Debug, Clone)]
pub struct TrafficMeter(pub(super) Arc<CounterSet>);
impl TrafficMeter {
    pub(crate) fn observes_role(&self, plane: TrafficPlane, outbound: bool) {
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
        self.record(plane, TrafficMetric::DroppedPackets, 1);
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
