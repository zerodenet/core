use super::Engine;
use zero_api::EndpointDirections;
use zero_config::EndpointBindingConfig;
use zero_core::Network;

fn belongs_to_endpoint(
    flow: &crate::session::ActiveSession,
    binding: &EndpointBindingConfig,
    directions: EndpointDirections,
) -> bool {
    (directions.inbound
        && flow
            .inbound_tag
            .as_ref()
            .is_some_and(|tag| binding.inbound_tags.contains(tag)))
        || (directions.outbound
            && (flow
                .outbound_tag
                .as_ref()
                .is_some_and(|tag| binding.outbound_tags.contains(tag))
                || flow
                    .path
                    .relay_chain
                    .iter()
                    .any(|(tag, _)| binding.outbound_tags.contains(tag))))
}

impl crate::EngineRuntimeSnapshot {
    pub fn revoked_endpoint_directions(
        &self,
        previous: &Self,
    ) -> Vec<(EndpointBindingConfig, EndpointDirections)> {
        previous
            .endpoint_intents
            .entries
            .iter()
            .filter_map(|(id, old)| {
                let old_allowed = if old.enabled() {
                    old.directions()
                } else {
                    EndpointDirections::default()
                };
                let allowed = self
                    .endpoint_intents
                    .entries
                    .get(id)
                    .filter(|next| next.enabled())
                    .map_or_else(EndpointDirections::default, |next| next.directions());
                let revoked = EndpointDirections {
                    inbound: old_allowed.inbound && !allowed.inbound,
                    outbound: old_allowed.outbound && !allowed.outbound,
                };
                (revoked.inbound || revoked.outbound).then(|| (old.binding.clone(), revoked))
            })
            .collect()
    }
}

impl Engine {
    /// Includes concrete relay hops, so revoking a resource also closes flows
    /// whose final outbound tag names another hop.
    pub fn endpoint_flow_ids(
        &self,
        binding: &EndpointBindingConfig,
        directions: EndpointDirections,
    ) -> Vec<u64> {
        self.session_registry
            .snapshot()
            .into_iter()
            .filter(|flow| belongs_to_endpoint(flow, binding, directions))
            .map(|flow| flow.id)
            .collect()
    }

    /// Count each active logical Flow once even when it enters and exits the
    /// same endpoint or reaches it through a relay. Packet paths remain
    /// unavailable until a separate Packet registry supplies that fact.
    pub fn endpoint_active_flow_counts(&self, binding: &EndpointBindingConfig) -> (u64, u64) {
        let mut stream = 0_u64;
        let mut datagram = 0_u64;
        for flow in self.session_registry.snapshot() {
            if !belongs_to_endpoint(
                &flow,
                binding,
                EndpointDirections {
                    inbound: true,
                    outbound: true,
                },
            ) {
                continue;
            }
            match flow.network {
                Network::Tcp => stream = stream.saturating_add(1),
                Network::Udp => datagram = datagram.saturating_add(1),
            }
        }
        (stream, datagram)
    }

    pub fn cancel_endpoint_flows(
        &self,
        binding: &EndpointBindingConfig,
        directions: EndpointDirections,
    ) {
        for id in self.endpoint_flow_ids(binding, directions) {
            self.session_registry.cancel(id, "endpoint_revoked");
        }
    }
}
