//! Prepared neutral I/O meters. No registry work on the data path.
use std::sync::Arc;
use zero_api::{TrafficMetric as Metric, TrafficPlane, TrafficScope};
use zero_engine::{Engine, TrafficMeter};
use zero_traits::IoObserver;

#[derive(Debug)]
struct RoleIo {
    meter: TrafficMeter,
    plane: TrafficPlane,
    packets: bool,
}
impl IoObserver for RoleIo {
    fn receive_coverage_lost(&self) {
        self.meter
            .mark_unobservable(self.plane, &[Metric::RxBytes, Metric::RxPackets]);
    }
    fn stream_boundary(&self) {
        if self.plane == TrafficPlane::Outer {
            self.carrier_boundary();
            self.meter
                .mark_unobservable(self.plane, &[Metric::RxPackets, Metric::TxPackets]);
        }
    }
    fn datagram_boundary(&self) {
        if self.plane == TrafficPlane::Outer {
            self.carrier_boundary();
            self.meter
                .enable(self.plane, &[Metric::RxPackets, Metric::TxPackets]);
        }
    }
    fn received_datagram(&self, bytes: usize) {
        self.received(bytes);
        if !self.packets {
            self.meter.record(self.plane, Metric::RxPackets, 1);
        }
    }
    fn sent_datagram(&self, bytes: usize) {
        self.sent(bytes);
        if !self.packets {
            self.meter.record(self.plane, Metric::TxPackets, 1);
        }
    }

    fn received(&self, bytes: usize) {
        self.meter.record(self.plane, Metric::RxBytes, bytes as u64);
        if self.packets {
            self.meter.record(self.plane, Metric::RxPackets, 1);
        }
    }
    fn sent(&self, bytes: usize) {
        self.meter.record(self.plane, Metric::TxBytes, bytes as u64);
        if self.packets {
            self.meter.record(self.plane, Metric::TxPackets, 1);
        }
    }
    fn error(&self) {
        if self.plane == TrafficPlane::Outer {
            self.meter.observes_role(self.plane, true);
            self.meter.enable(self.plane, &[Metric::Errors]);
        }
        self.meter.error(self.plane);
    }
    fn dropped(&self) {
        self.dropped_reason(zero_traits::PacketDropReason::Unspecified);
    }
    fn dropped_reason(&self, reason: zero_traits::PacketDropReason) {
        self.meter.dropped_reason(self.plane, drop_reason(reason));
    }
}
pub(crate) fn outbound(
    engine: &Engine,
    tag: &str,
    plane: TrafficPlane,
) -> Option<Arc<dyn IoObserver>> {
    let meter = engine.traffic_meter(&TrafficScope::Outbound { tag: tag.into() })?;
    let packets = plane == TrafficPlane::Inner;
    if packets {
        meter.observes_role(plane, true);
        meter.enable(plane, &[Metric::RxBytes, Metric::TxBytes]);
        meter.enable(plane, &[Metric::RxPackets, Metric::TxPackets]);
        meter.accounting_basis(
            plane,
            "outbound_ip_role_tx_device_fragments_rx_correlated_after_reassembly",
        );
    } else {
        meter.accounting_basis(
            plane,
            "outbound_carrier_io_boundaries_excludes_host_headers",
        );
    }
    Some(Arc::new(RoleIo {
        meter,
        plane,
        packets,
    }))
}

impl RoleIo {
    fn carrier_boundary(&self) {
        self.meter.observes_role(self.plane, true);
        self.meter.enable(
            self.plane,
            &[Metric::RxBytes, Metric::TxBytes, Metric::Errors],
        );
    }
}
#[cfg(test)]
#[path = "traffic_io/tests.rs"]
mod tests;

/// Wire contract mapping; data-plane traits remain runtime and API independent.
pub(crate) fn drop_reason(reason: zero_traits::PacketDropReason) -> zero_api::TrafficDropReason {
    use zero_api::TrafficDropReason as A;
    use zero_traits::PacketDropReason as D;
    match reason {
        D::Unspecified => A::Unspecified,
        D::QueueFull => A::QueueFull,
        D::QueueClosed => A::QueueClosed,
        D::InvalidPacket => A::InvalidPacket,
        D::SourceRejected => A::SourceRejected,
        D::FragmentRejected => A::FragmentRejected,
        D::PolicyRejected => A::PolicyRejected,
        D::IoFailure => A::IoFailure,
        D::NoRoute => A::NoRoute,
        D::HopLimit => A::HopLimit,
    }
}
