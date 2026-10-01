//! The protocol supplies identities; runtime binds and updates neutral meters.
use super::RawIpInboundDevice;
use crate::runtime::{raw_ip::RawIpTraffic, route_runtime::InboundRouteRuntimeFactory};

pub(super) struct IngressTraffic {
    pub aggregate: RawIpTraffic,
    peers: Vec<RawIpTraffic>,
    identities: Vec<Option<std::sync::Arc<str>>>,
    generation: u64,
    role: Option<zero_engine::TrafficMeter>,
}
impl IngressTraffic {
    pub fn bind(device: &dyn RawIpInboundDevice, runtime: &InboundRouteRuntimeFactory) -> Self {
        let global = runtime.global_packet_traffic();
        let role = runtime.inbound_packet_traffic();
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
        let identities = (0..device.peer_count())
            .map(|i| device.peer_identity(i))
            .collect::<Vec<_>>();
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
            generation: device.generation(),
            role,
        }
    }
    pub fn identity(&self, peer: Option<usize>) -> Option<std::sync::Arc<str>> {
        peer.and_then(|i| self.identities.get(i)).cloned().flatten()
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
    pub fn refresh(
        &mut self,
        device: &dyn RawIpInboundDevice,
        runtime: &InboundRouteRuntimeFactory,
    ) {
        if self.generation != device.generation() {
            *self = Self::bind(device, runtime);
        }
    }
    pub fn peer(&self, peer: Option<usize>) -> &RawIpTraffic {
        peer.and_then(|i| self.peers.get(i))
            .unwrap_or(&self.aggregate)
    }
}
