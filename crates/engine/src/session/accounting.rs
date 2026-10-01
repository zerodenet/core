//! Per-session claims prevent mirrored roles double counting a shared resource.
use crate::TrafficMeter;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use zero_api::{TrafficMetric, TrafficPlane};

#[derive(Debug)]
pub(super) struct EndpointFlowMeter {
    pub meter: TrafficMeter,
    boundaries: [AtomicU64; 4],
    claimed: [AtomicU64; 2],
    _activity: Option<crate::observability::traffic::TrafficFlowLease>,
}
impl EndpointFlowMeter {
    pub fn new(meter: TrafficMeter) -> Arc<Self> {
        meter.enable(
            TrafficPlane::Flow,
            &[
                TrafficMetric::BytesUp,
                TrafficMetric::BytesDown,
                TrafficMetric::Errors,
            ],
        );
        Arc::new(Self {
            meter,
            boundaries: Default::default(),
            claimed: Default::default(),
            _activity: None,
        })
    }
    pub fn peer(meter: TrafficMeter, network: zero_core::Network) -> Arc<Self> {
        let activity = meter.flow(network);
        let mut flow = Self::new(meter);
        Arc::get_mut(&mut flow)
            .expect("fresh peer flow meter")
            ._activity = Some(activity);
        flow
    }
    pub fn record(&self, outbound: bool, rx: bool, bytes: u64) {
        let slot = outbound as usize * 2 + (!rx) as usize;
        self.boundaries[slot].fetch_add(bytes, Ordering::Relaxed);
        let direction = (outbound == rx) as usize;
        let (a, b) = if direction == 0 { (0, 3) } else { (1, 2) };
        let total = self.boundaries[a]
            .load(Ordering::Relaxed)
            .max(self.boundaries[b].load(Ordering::Relaxed));
        let previous = self.claimed[direction].fetch_max(total, Ordering::Relaxed);
        self.meter.record(
            TrafficPlane::Flow,
            if direction == 0 {
                TrafficMetric::BytesUp
            } else {
                TrafficMetric::BytesDown
            },
            total.saturating_sub(previous),
        );
    }
}
