//! Device-boundary accounting, independent of protocol framing and key material.
use zero_api::{TrafficMetric, TrafficPlane};
use zero_engine::TrafficMeter;

#[derive(Debug, Clone, Default)]
pub(crate) struct RawIpTraffic {
    endpoint: Option<TrafficMeter>,
    peer: Option<TrafficMeter>,
    global: Option<TrafficMeter>,
}
impl RawIpTraffic {
    pub(crate) fn new(endpoint: TrafficMeter, peer: Option<TrafficMeter>) -> Self {
        for meter in std::iter::once(&endpoint).chain(peer.as_ref()) {
            for plane in [TrafficPlane::Inner, TrafficPlane::Outer] {
                meter.enable(
                    plane,
                    &[
                        TrafficMetric::RxBytes,
                        TrafficMetric::TxBytes,
                        TrafficMetric::RxPackets,
                        TrafficMetric::TxPackets,
                        TrafficMetric::DroppedPackets,
                        TrafficMetric::Errors,
                    ],
                );
            }
        }
        Self {
            endpoint: Some(endpoint),
            peer,
            global: None,
        }
    }
    pub(crate) fn with_global(mut self, global: Option<TrafficMeter>) -> Self {
        if let Some(meter) = &global {
            meter.enable(
                TrafficPlane::Inner,
                &[
                    TrafficMetric::RxBytes,
                    TrafficMetric::TxBytes,
                    TrafficMetric::RxPackets,
                    TrafficMetric::TxPackets,
                    TrafficMetric::DroppedPackets,
                    TrafficMetric::Errors,
                ],
            );
            meter.accounting_basis(
                TrafficPlane::Inner,
                "raw_ip_device_boundary_totals_not_business_flow_or_endpoint_sum",
            );
        }
        self.global = global;
        self
    }
    pub(crate) fn rx(&self, plane: TrafficPlane, size: usize, identified: bool) {
        if plane == TrafficPlane::Inner {
            if let Some(meter) = &self.global {
                meter.received(plane, size);
            }
        }
        if let Some(meter) = &self.endpoint {
            meter.received(plane, size);
        }
        if identified {
            self.peer_rx(plane, size);
        }
    }
    pub(crate) fn peer_rx(&self, plane: TrafficPlane, size: usize) {
        if let Some(meter) = &self.peer {
            meter.received(plane, size);
        }
    }
    pub(crate) fn tx(&self, plane: TrafficPlane, size: usize) {
        if plane == TrafficPlane::Inner {
            if let Some(meter) = &self.global {
                meter.sent(plane, size);
            }
        }
        for meter in self.endpoint.iter().chain(self.peer.iter()) {
            meter.sent(plane, size);
        }
    }
    pub(crate) fn dropped_reason(
        &self,
        plane: TrafficPlane,
        identified: bool,
        reason: zero_api::TrafficDropReason,
    ) {
        self.dropped_count(plane, identified, reason, 1);
    }
    pub(crate) fn dropped_count(
        &self,
        plane: TrafficPlane,
        identified: bool,
        reason: zero_api::TrafficDropReason,
        amount: u64,
    ) {
        if plane == TrafficPlane::Inner {
            if let Some(meter) = &self.global {
                meter.dropped_count(plane, reason, amount);
            }
        }
        if let Some(meter) = &self.endpoint {
            meter.dropped_count(plane, reason, amount);
        }
        if identified {
            if let Some(meter) = &self.peer {
                meter.dropped_count(plane, reason, amount);
            }
        }
    }
    pub(crate) fn error(&self, plane: TrafficPlane, identified: bool) {
        if plane == TrafficPlane::Inner {
            if let Some(meter) = &self.global {
                meter.error(plane);
            }
        }
        if let Some(meter) = &self.endpoint {
            meter.error(plane);
        }
        if identified {
            if let Some(meter) = &self.peer {
                meter.error(plane);
            }
        }
    }
}

impl zero_traits::IoObserver for RawIpTraffic {
    fn received(&self, _bytes: usize) {}
    fn sent(&self, _bytes: usize) {}
    fn error(&self) {
        self.error(TrafficPlane::Inner, true);
    }
    fn dropped(&self) {
        self.dropped_reason(
            TrafficPlane::Inner,
            true,
            zero_api::TrafficDropReason::Unspecified,
        );
    }
    fn dropped_reason(&self, reason: zero_traits::PacketDropReason) {
        self.dropped_reason(
            TrafficPlane::Inner,
            true,
            crate::runtime::traffic_io::drop_reason(reason),
        );
    }
}
