//! Fixed atomic slots shared by runtime meter handles and observation views.
use std::sync::{
    atomic::{AtomicU16, AtomicU64, Ordering},
    Arc, Mutex,
};
use zero_api::{TrafficCounters, TrafficMetric, TrafficPlane, TrafficPlaneSnapshot};

pub(super) const WIDTH: usize = 22;
pub(super) const PLANES: usize = 4;
pub(super) type Values = [[u64; WIDTH]; PLANES];

#[derive(Debug)]
pub(super) struct Plane {
    pub(super) values: [AtomicU64; WIDTH],
    mirrored_business: Option<(Arc<AtomicU64>, Arc<AtomicU64>)>,
    pub(super) available: AtomicU16,
    pub(super) unobservable: AtomicU16,
    pub(super) basis: Mutex<Option<&'static str>>,
    pub(super) roles: AtomicU16,
    pub(super) reasons: AtomicU16,
}
impl Default for Plane {
    fn default() -> Self {
        Self {
            values: std::array::from_fn(|_| AtomicU64::new(0)),
            mirrored_business: None,
            available: AtomicU16::new(0),
            unobservable: AtomicU16::new(0),
            basis: Mutex::new(None),
            roles: AtomicU16::new(0),
            reasons: AtomicU16::new(0),
        }
    }
}
impl Plane {
    pub fn flow_global(up: Arc<AtomicU64>, down: Arc<AtomicU64>) -> Self {
        let plane = Self {
            mirrored_business: Some((up, down)),
            ..Self::default()
        };
        plane.enable(&[
            TrafficMetric::BytesUp,
            TrafficMetric::BytesDown,
            TrafficMetric::Errors,
        ]);
        plane
    }
    fn slot(&self, i: usize) -> &AtomicU64 {
        match (&self.mirrored_business, i) {
            (Some((up, _)), 0) => up,
            (Some((_, down)), 1) => down,
            _ => &self.values[i],
        }
    }
    pub fn enable(&self, metrics: &[TrafficMetric]) {
        let mask = metrics
            .iter()
            .fold(0, |mask, metric| mask | (1 << (*metric as usize)));
        self.available.fetch_or(mask, Ordering::Release);
    }
    pub fn add(&self, metric: TrafficMetric, value: u64) {
        if value != 0 {
            let _ = self.slot(metric as usize).try_update(
                Ordering::Relaxed,
                Ordering::Relaxed,
                |old| Some(old.saturating_add(value)),
            );
        }
    }
    pub(super) fn dropped_reason(&self, reason: zero_api::TrafficDropReason, amount: u64) {
        if amount == 0 {
            return;
        }
        self.add(TrafficMetric::DroppedPackets, amount);
        let _ = self.values[12 + reason as usize].try_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |old| Some(old.saturating_add(amount)),
        );
        self.reasons
            .fetch_or(1 << reason as usize, Ordering::Release);
    }
    pub(super) fn replace_host(&self, values: &[Option<u64>; 12], basis: &'static str) {
        let mut mask = 0;
        for (i, value) in values.iter().enumerate() {
            if let Some(value) = value {
                self.values[i].store(*value, Ordering::Relaxed);
                mask |= 1 << i;
            }
        }
        self.available.store(mask, Ordering::Release);
        *self.basis.lock().unwrap_or_else(|e| e.into_inner()) = Some(basis);
    }
    pub(super) fn suspend_host(&self) {
        self.available.store(0, Ordering::Release);
    }
    pub fn capture(&self) -> [u64; WIDTH] {
        std::array::from_fn(|i| self.slot(i).load(Ordering::Relaxed))
    }
    pub fn project(
        &self,
        plane: TrafficPlane,
        values: [u64; WIDTH],
        baseline: [u64; WIDTH],
    ) -> TrafficPlaneSnapshot {
        let mask =
            self.available.load(Ordering::Acquire) & !self.unobservable.load(Ordering::Acquire);
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
                    TrafficPlane::Host => "host_interface_provider",
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
                rx_dropped_packets: get(8),
                tx_dropped_packets: get(9),
                rx_errors: get(10),
                tx_errors: get(11),
            },
            drop_reasons_resettable: self.reasons.load(Ordering::Acquire) != 0,
            drop_reasons: zero_api::TrafficDropReason::ALL
                .into_iter()
                .enumerate()
                .filter(|(i, _)| self.reasons.load(Ordering::Acquire) & (1 << i) != 0)
                .map(|(i, reason)| zero_api::TrafficDropCounter {
                    reason,
                    packets: values[12 + i].saturating_sub(baseline[12 + i]),
                })
                .collect(),
            drop_coverage: (self.reasons.load(Ordering::Acquire) != 0
                || mask & (1 << TrafficMetric::DroppedPackets as usize) != 0)
                .then(|| "observed_local_boundary_discards_only".into()),
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
    pub planes: [Plane; PLANES],
    pub period: Mutex<Period>,
    pub generation: Mutex<Option<u64>>,
    pub active_routes: AtomicU64,
    pub routes_available: AtomicU16,
    pub active_streams: AtomicU64,
    pub active_datagrams: AtomicU64,
    pub flows_available: AtomicU16,
    pub source_sampled_at: AtomicU64,
    pub source_sampled_monotonic: AtomicU64,
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
                baseline: [[0; WIDTH]; PLANES],
            }),
            generation: Mutex::new(None),
            active_routes: AtomicU64::new(0),
            routes_available: AtomicU16::new(0),
            active_streams: AtomicU64::new(0),
            active_datagrams: AtomicU64::new(0),
            flows_available: AtomicU16::new(0),
            source_sampled_at: AtomicU64::new(0),
            source_sampled_monotonic: AtomicU64::new(0),
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
