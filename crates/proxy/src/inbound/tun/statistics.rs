//! TUN observes successful host IP I/O; kernel roles and periods own the counters.
use std::sync::Arc;
use zero_api::{TrafficMetric, TrafficPlane, TrafficScope};
use zero_engine::{Engine, InboundTrafficRegistration, TrafficMeter};
use zero_traits::IoObserver;

#[derive(Debug)]
pub(super) struct TunTraffic {
    _registration: InboundTrafficRegistration,
    role: TrafficMeter,
    global: TrafficMeter,
}
impl TunTraffic {
    pub(super) fn prepare(engine: &Engine, tag: &str) -> Arc<Self> {
        let registration = engine.register_inbound_traffic(tag);
        let role = engine
            .traffic_meter(&TrafficScope::Inbound { tag: tag.into() })
            .expect("registered TUN role");
        let global = engine
            .traffic_meter(&TrafficScope::Global)
            .expect("global traffic source");
        for meter in [&role, &global] {
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
        }
        role.accounting_basis(
            TrafficPlane::Inner,
            "host_tun_successful_ip_io_excludes_platform_framing",
        );
        global.accounting_basis(
            TrafficPlane::Inner,
            "raw_ip_device_boundary_totals_not_business_flow_or_endpoint_sum",
        );
        Arc::new(Self {
            _registration: registration,
            role,
            global,
        })
    }
}
impl IoObserver for TunTraffic {
    fn received(&self, bytes: usize) {
        for meter in [&self.role, &self.global] {
            meter.received(TrafficPlane::Inner, bytes);
        }
    }
    fn sent(&self, bytes: usize) {
        for meter in [&self.role, &self.global] {
            meter.sent(TrafficPlane::Inner, bytes);
        }
    }
    fn error(&self) {
        for meter in [&self.role, &self.global] {
            meter.error(TrafficPlane::Inner);
        }
    }
    fn dropped(&self) {
        for meter in [&self.role, &self.global] {
            meter.dropped(TrafficPlane::Inner);
        }
    }
}

#[cfg(test)]
mod tests;
