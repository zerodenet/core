//! The protocol supplies identities; runtime binds and updates neutral meters.
use crate::runtime::{raw_ip::RawIpTraffic, route_runtime::InboundRouteRuntimeFactory};

pub(super) struct IngressTraffic {
    pub aggregate: RawIpTraffic,
    peers: Vec<RawIpTraffic>,
    identities: Vec<Option<std::sync::Arc<str>>>,
    role: Option<zero_engine::TrafficMeter>,
    native_io: Option<std::sync::Arc<dyn zero_traits::IoObserver>>,
}
impl IngressTraffic {
    pub fn pending() -> Self {
        Self {
            aggregate: RawIpTraffic::default(),
            peers: vec![],
            identities: vec![],
            role: None,
            native_io: None,
        }
    }

    pub fn from_identities(
        identities: Vec<Option<String>>,
        runtime: &InboundRouteRuntimeFactory,
        native_io: Option<std::sync::Arc<dyn zero_traits::IoObserver>>,
    ) -> Self {
        // System TUN counts the actual native IP boundary, including real
        // fragments. Post-reassembly routing must not count it a second time.
        let global = native_io.is_none().then(|| runtime.global_packet_traffic()).flatten();
        let role = native_io.is_none().then(|| runtime.inbound_packet_traffic()).flatten();
        if let Some(meter) = &role {
            meter.enable(
                zero_api::TrafficPlane::Inner,
                &[
                    zero_api::TrafficMetric::RxBytes,
                    zero_api::TrafficMetric::TxBytes,
                    zero_api::TrafficMetric::RxPackets,
                    zero_api::TrafficMetric::TxPackets,
                ],
            );
            meter.accounting_basis(
                zero_api::TrafficPlane::Inner,
                "inbound_ip_role_rx_after_reassembly_tx_accepted_response_fragments",
            );
        }
        let ids = identities.iter().flatten().cloned().collect::<Vec<_>>();
        let mut peers = Vec::new();
        let aggregate = if let Some(prepared) = runtime.endpoint_traffic(&ids) {
            let meters = prepared.peers();
            peers = identities
                .iter()
                .map(|id| {
                    RawIpTraffic::new(
                        prepared.endpoint(),
                        id.as_ref()
                            .and_then(|id| ids.iter().position(|p| p == id))
                            .and_then(|i| meters.get(i).cloned()),
                    )
                    .with_global(global.clone())
                })
                .collect();
            let aggregate =
                RawIpTraffic::new(prepared.endpoint(), None).with_global(global.clone());
            prepared.publish();
            aggregate
        } else {
            RawIpTraffic::default().with_global(global)
        };
        Self {
            aggregate,
            peers,
            identities: identities
                .into_iter()
                .map(|id| id.map(Into::into))
                .collect(),
            role,
            native_io,
        }
    }
    pub fn drop_inner(&self, identity: Option<&str>, reason: zero_api::TrafficDropReason) {
        if let Some(io) = &self.native_io {
            io.dropped_reason(super::observation::packet_drop_reason(reason));
            return;
        }
        let peer = identity.and_then(|id| {
            self.identities
                .iter()
                .position(|known| known.as_deref() == Some(id))
        });
        self.peer(peer)
            .dropped_reason(zero_api::TrafficPlane::Inner, peer.is_some(), reason);
        if let Some(role) = &self.role {
            role.dropped_reason(zero_api::TrafficPlane::Inner, reason);
        }
    }
    pub fn send_response(
        &self,
        responses: &zero_stack::packet_output::PacketSender,
        packet: Vec<u8>,
        identity: Option<&str>,
    ) {
        if let Err(error) = responses.try_send(packet) {
            self.drop_inner(
                identity,
                match error {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => {
                        zero_api::TrafficDropReason::QueueFull
                    }
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                        zero_api::TrafficDropReason::QueueClosed
                    }
                },
            );
        }
    }
    pub fn admit_packet(&self, size: usize) {
        if let Some(meter) = &self.role {
            meter.received(zero_api::TrafficPlane::Inner, size);
        }
    }
    pub fn respond_packet(&self, size: usize) {
        if let Some(meter) = &self.role {
            meter.sent(zero_api::TrafficPlane::Inner, size);
        }
    }
    pub fn peer(&self, peer: Option<usize>) -> &RawIpTraffic {
        peer.and_then(|i| self.peers.get(i))
            .unwrap_or(&self.aggregate)
    }
}
