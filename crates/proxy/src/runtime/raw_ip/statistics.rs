//! Device-boundary accounting, independent of protocol framing and key material.
use zero_api::{TrafficMetric, TrafficPlane};
use zero_engine::TrafficMeter;

#[derive(Clone, Default)]
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
    pub(crate) fn dropped(&self, plane: TrafficPlane, identified: bool) {
        if plane == TrafficPlane::Inner {
            if let Some(meter) = &self.global {
                meter.dropped(plane);
            }
        }
        if let Some(meter) = &self.endpoint {
            meter.dropped(plane);
        }
        if identified {
            if let Some(meter) = &self.peer {
                meter.dropped(plane);
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
