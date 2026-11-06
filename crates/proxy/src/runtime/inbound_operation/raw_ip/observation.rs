//! Secret-free wire observations feed the same host meters as Packet routing.
use super::*;
use zero_api::{TrafficDropReason, TrafficPlane};
use zero_platform_tokio::contract::packet::{PacketEvent, PacketObserver, PacketPlane};
#[derive(Clone)]
pub(super) struct ObservedTraffic {
    observer: Arc<dyn PacketObserver>,
    peer: Option<usize>,
}
impl ObservedTraffic {
    fn event(&self, plane: TrafficPlane, event: PacketEvent) {
        self.observer.event(
            match plane {
                TrafficPlane::Inner => PacketPlane::Inner,
                _ => PacketPlane::Outer,
            },
            self.peer,
            event,
        );
    }
    pub fn rx(&self, plane: TrafficPlane, bytes: usize, identified: bool) {
        self.event(plane, PacketEvent::Received { bytes, identified });
    }
    pub fn peer_rx(&self, plane: TrafficPlane, bytes: usize) {
        self.event(plane, PacketEvent::PeerReceived { bytes });
    }
    pub fn tx(&self, plane: TrafficPlane, bytes: usize) {
        self.event(plane, PacketEvent::Sent { bytes });
    }
    pub fn error(&self, plane: TrafficPlane, identified: bool) {
        self.event(plane, PacketEvent::Error { identified });
    }
    pub fn dropped_reason(&self, plane: TrafficPlane, identified: bool, reason: TrafficDropReason) {
        self.dropped_count(plane, identified, reason, 1);
    }
    pub fn dropped_count(
        &self,
        plane: TrafficPlane,
        identified: bool,
        reason: TrafficDropReason,
        count: u64,
    ) {
        let reason = packet_drop_reason(reason);
        self.event(
            plane,
            PacketEvent::Dropped {
                identified,
                reason,
                count,
            },
        );
    }
}
pub(super) struct WireTraffic {
    pub aggregate: ObservedTraffic,
    identities: Vec<Option<Arc<str>>>,
    generation: Option<u64>,
}
impl WireTraffic {
    pub fn new(observer: Arc<dyn PacketObserver>) -> Self {
        Self {
            aggregate: ObservedTraffic {
                observer,
                peer: None,
            },
            identities: vec![],
            generation: None,
        }
    }
    pub fn peer(&self, peer: Option<usize>) -> ObservedTraffic {
        ObservedTraffic {
            observer: self.aggregate.observer.clone(),
            peer,
        }
    }
    pub fn identity(&self, peer: Option<usize>) -> Option<Arc<str>> {
        peer.and_then(|p| self.identities.get(p)).cloned().flatten()
    }
    pub fn generation(&self) -> u64 {
        self.generation.unwrap_or(0)
    }
    pub fn respond_packet(&self, bytes: usize) {
        self.aggregate
            .observer
            .event(PacketPlane::Inner, None, PacketEvent::Responded { bytes });
    }
    pub fn allowed(&self) -> bool {
        self.aggregate.observer.ingress_allowed()
    }
    pub fn refresh(&mut self, device: &dyn RawIpInboundDevice) {
        if self.generation == Some(device.generation()) {
            return;
        }
        let identities = (0..device.peer_count())
            .map(|i| device.peer_identity(i))
            .collect::<Vec<_>>();
        self.aggregate.observer.peers(&identities);
        self.identities = identities
            .into_iter()
            .map(|id| id.map(Into::into))
            .collect();
        self.generation = Some(device.generation());
    }
}
pub(super) struct HostObserver {
    pub route: crate::runtime::route_runtime::InboundRouteRuntimeFactory,
    pub traffic: Mutex<Arc<super::statistics::IngressTraffic>>,
    pub ready: Option<crate::runtime::inbound_owners::InboundReady>,
    pub native_io: Option<Arc<dyn zero_traits::IoObserver>>,
}
impl PacketObserver for HostObserver {
    fn egress(&self) -> zero_platform_tokio::EgressInterfaceControl {
        self.route.datagram_egress()
    }
    fn ready(&self) {
        if let Some(ready) = &self.ready {
            ready.confirm_polled();
        }
    }
    fn peers(&self, identities: &[Option<String>]) {
        *self.traffic.lock().unwrap() = Arc::new(
            super::statistics::IngressTraffic::from_identities(identities.to_vec(), &self.route, self.native_io.clone()),
        );
    }
    fn ingress_allowed(&self) -> bool {
        self.route.endpoint_inbound_allowed()
    }
    fn event(&self, plane: PacketPlane, peer: Option<usize>, event: PacketEvent) {
        let plane = match plane {
            PacketPlane::Inner => TrafficPlane::Inner,
            PacketPlane::Outer => TrafficPlane::Outer,
        };
        if plane == TrafficPlane::Inner {
            if let Some(io) = &self.native_io {
                match event {
                    PacketEvent::Received { bytes, .. } => io.received(bytes),
                    PacketEvent::Sent { bytes } => io.sent(bytes),
                    PacketEvent::Error { .. } => io.error(),
                    PacketEvent::Dropped { reason, count, .. } => {
                        for _ in 0..count { io.dropped_reason(reason); }
                    }
                    // TUN has no authenticated wire peer; accepted native
                    // writes already provide the real sent-byte observation.
                    PacketEvent::Responded { .. } | PacketEvent::PeerReceived { .. } => {},
                }
                return;
            }
        }
        let guard = self.traffic.lock().unwrap();
        let meter = guard.peer(peer);
        match event {
            PacketEvent::Responded { bytes } => guard.respond_packet(bytes),
            PacketEvent::Received { identified, bytes } => meter.rx(plane, bytes, identified),
            PacketEvent::PeerReceived { bytes } => meter.peer_rx(plane, bytes),
            PacketEvent::Sent { bytes } => meter.tx(plane, bytes),
            PacketEvent::Error { identified } => meter.error(plane, identified),
            PacketEvent::Dropped {
                identified,
                reason,
                count,
            } => meter.dropped_count(
                plane,
                identified,
                crate::runtime::traffic_io::drop_reason(reason),
                count,
            ),
        }
    }
}

pub(super) fn packet_drop_reason(reason: TrafficDropReason) -> zero_traits::PacketDropReason {
    use zero_traits::PacketDropReason as D;
    match reason {
            TrafficDropReason::QueueFull => D::QueueFull,
            TrafficDropReason::QueueClosed => D::QueueClosed,
            TrafficDropReason::InvalidPacket => D::InvalidPacket,
            TrafficDropReason::SourceRejected => D::SourceRejected,
            TrafficDropReason::FragmentRejected => D::FragmentRejected,
            TrafficDropReason::PolicyRejected => D::PolicyRejected,
            TrafficDropReason::IoFailure => D::IoFailure,
            TrafficDropReason::NoRoute => D::NoRoute,
            TrafficDropReason::HopLimit => D::HopLimit,
            _ => D::Unspecified,
    }

}
