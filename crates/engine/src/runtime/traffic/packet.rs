//! Statistics identity resolution for existing bounded Packet conversation pins.
use super::super::Engine;
use crate::TrafficMeter;
use std::collections::BTreeSet;
use zero_api::TrafficScope;

impl Engine {
    /// Role/resource membership is resolved once when a conversation pin starts.
    /// This does not copy Flow bytes onto hops or create statistical identities.
    pub fn packet_route_traffic_meters(
        &self,
        inbound: &str,
        outbound: &str,
        inbound_peer: Option<&str>,
        outbound_peer: Option<&str>,
    ) -> Vec<TrafficMeter> {
        let mut scopes = BTreeSet::from([
            TrafficScope::Global,
            TrafficScope::Inbound {
                tag: inbound.into(),
            },
            TrafficScope::Outbound {
                tag: outbound.into(),
            },
        ]);
        for binding in self
            .config()
            .endpoint_bindings()
            .into_iter()
            .filter(|b| b.canonical)
        {
            let input = binding.inbound_tags.iter().any(|tag| tag == inbound);
            let output = binding.outbound_tags.iter().any(|tag| tag == outbound);
            if input || output {
                scopes.insert(TrafficScope::Endpoint {
                    endpoint_id: binding.endpoint_id.clone(),
                });
            }
            for peer in [
                input.then_some(inbound_peer).flatten(),
                output.then_some(outbound_peer).flatten(),
            ]
            .into_iter()
            .flatten()
            {
                scopes.insert(TrafficScope::Peer {
                    endpoint_id: binding.endpoint_id.clone(),
                    peer_id: peer.into(),
                });
            }
        }
        scopes
            .iter()
            .filter_map(|scope| self.traffic.meter(scope))
            .collect()
    }
}
