//! Period views over the existing business meters and prepared Packet meters.
use super::Engine;
use std::collections::BTreeMap;
use zero_api::{
    ApiEvent, StatsResetCommand, StatsResetSnapshot, TrafficActivity, TrafficGetQuery,
    TrafficListQuery, TrafficListSnapshot, TrafficScope, TrafficSnapshot,
};

mod packet;

impl Engine {
    /// Bind a protocol-supplied stable peer identity, never an outer source address.
    pub fn bind_session_peer(&self, id: u64, role_tag: &str, outbound: bool, peer_id: &str) {
        let binding = self
            .config()
            .endpoint_bindings()
            .into_iter()
            .find(|binding| {
                binding.canonical
                    && if outbound {
                        binding.outbound_tags.iter().any(|tag| tag == role_tag)
                    } else {
                        binding.inbound_tags.iter().any(|tag| tag == role_tag)
                    }
            });
        if let Some(binding) = binding {
            if let Some(meter) = self.traffic.meter(&TrafficScope::Peer {
                endpoint_id: binding.endpoint_id,
                peer_id: peer_id.into(),
            }) {
                self.session_registry.bind_peer_traffic(id, outbound, meter);
            }
        }
    }
    /// Retain an actual dynamically started ingress role for its runtime lifetime.
    pub fn register_inbound_traffic(&self, tag: &str) -> crate::InboundTrafficRegistration {
        self.traffic.register_inbound(tag)
    }
    pub(super) fn project_endpoint_traffic(&self, endpoint: &mut zero_api::EndpointSnapshot) {
        if let Ok(snapshot) = self.traffic.snapshot(
            &TrafficScope::Endpoint {
                endpoint_id: endpoint.endpoint_id.clone(),
            },
            self.core_instance_id(),
            endpoint.config_revision,
        ) {
            endpoint.stats_epoch = Some(snapshot.stats_epoch);
            endpoint.stats_epoch_started_at_unix_ms = Some(snapshot.epoch_started_at_unix_ms);
            let inner = &snapshot.planes[1].counters;
            let outer = &snapshot.planes[2].counters;
            endpoint.counters.inner_rx_bytes = inner.rx_bytes;
            endpoint.counters.inner_tx_bytes = inner.tx_bytes;
            endpoint.counters.inner_rx_packets = inner.rx_packets;
            endpoint.counters.inner_tx_packets = inner.tx_packets;
            endpoint.counters.outer_rx_bytes = outer.rx_bytes;
            endpoint.counters.outer_tx_bytes = outer.tx_bytes;
            endpoint.counters.outer_rx_packets = outer.rx_packets;
            endpoint.counters.outer_tx_packets = outer.tx_packets;
            endpoint.counters.dropped_packets = inner.dropped_packets;
            endpoint.counters.active_packet_routes = snapshot.activity.active_packet_routes;
        }
    }
    pub(super) fn endpoint_role_meter(
        &self,
        tag: &str,
        outbound: bool,
    ) -> Option<crate::TrafficMeter> {
        let binding = self.config().endpoint_bindings().into_iter().find(|b| {
            b.canonical
                && if outbound {
                    b.outbound_tags.iter().any(|t| t == tag)
                } else {
                    b.inbound_tags.iter().any(|t| t == tag)
                }
        })?;
        self.traffic.meter(&TrafficScope::Endpoint {
            endpoint_id: binding.endpoint_id,
        })
    }
    pub fn traffic_meter(&self, scope: &TrafficScope) -> Option<crate::TrafficMeter> {
        self.traffic.meter(scope)
    }

    /// Register only protocol-supplied stable identities for an explicit resource.
    /// Protocol adapters call this after validating the peer inventory. Replacing
    /// the inventory retires removed labels; existing handles never resurrect them.
    pub fn prepare_endpoint_traffic(
        &self,
        binding: &zero_config::EndpointBindingConfig,
        peers: &[String],
    ) -> Option<crate::PreparedEndpointTraffic> {
        if !binding.canonical
            || !self
                .config()
                .endpoint_bindings()
                .iter()
                .any(|known| known.canonical && known.endpoint_id == binding.endpoint_id)
        {
            return None;
        }
        Some(self.traffic.prepare_endpoint(&binding.endpoint_id, peers))
    }

    pub fn traffic_snapshot(
        &self,
        query: &TrafficGetQuery,
    ) -> zero_api::ApiResult<TrafficSnapshot> {
        let mut result = self.traffic.snapshot(
            &query.scope,
            self.core_instance_id(),
            self.config_revision(),
        )?;
        self.attach_traffic_activity(std::slice::from_mut(&mut result));
        Ok(result)
    }
    pub fn traffic_snapshots(
        &self,
        query: &TrafficListQuery,
    ) -> zero_api::ApiResult<TrafficListSnapshot> {
        let mut result =
            self.traffic
                .list(query, self.core_instance_id(), self.config_revision())?;
        self.attach_traffic_activity(&mut result.scopes);
        Ok(result)
    }
    pub fn reset_traffic(
        &self,
        request: &StatsResetCommand,
    ) -> zero_api::ApiResult<StatsResetSnapshot> {
        let result = self.traffic.reset(
            request,
            self.core_instance_id(),
            self.config_revision(),
            self.operation_id(request.operation_id.as_deref()),
            |result| {
                self.attach_traffic_activity(&mut result.snapshots);
                let revision = result.snapshots.first().map(|s| s.config_revision);
                let mut event = ApiEvent::new(
                    format!(
                        "stats-reset-{}-{}",
                        result.operation_id, result.snapshots[0].stats_epoch
                    ),
                    zero_api::event_type::STATS_RESET,
                    super::started_at_unix_ms(),
                    serde_json::to_value(result).expect("statistics reset serialization"),
                );
                event.config_revision = revision;
                self.event_log.push_generated(event);
            },
        )?;
        Ok(result)
    }
    pub fn push_traffic_stats_sampled(&self) {
        let offset = self
            .traffic
            .sample_offset
            .load(std::sync::atomic::Ordering::Relaxed) as usize;
        if let Ok(page) = self.traffic_snapshots(&TrafficListQuery {
            offset,
            limit: Some(64),
            ..Default::default()
        }) {
            self.traffic.sample_offset.store(
                page.next_offset.unwrap_or(0) as u64,
                std::sync::atomic::Ordering::Relaxed,
            );
            if !page.scopes.is_empty() {
                let revision = page.config_revision;
                let mut event = ApiEvent::new(
                    format!(
                        "traffic-{}-{offset}-{}",
                        page.sampled_at_unix_ms,
                        self.operation_id(None)
                    ),
                    zero_api::event_type::STATS_SCOPES_SAMPLED,
                    page.sampled_at_unix_ms,
                    serde_json::to_value(page).expect("statistics sample serialization"),
                );
                event.config_revision = Some(revision);
                self.event_log.push_generated(event);
            }
        }
    }
    pub(super) fn traffic_activity_index(&self) -> BTreeMap<TrafficScope, (u64, u64)> {
        // Index roles once, rather than scanning every flow for every resource.
        let mut counts: BTreeMap<TrafficScope, (u64, u64)> = BTreeMap::new();
        let bindings = self.config().endpoint_bindings();
        let mut endpoints_by_role: BTreeMap<TrafficScope, Vec<String>> = BTreeMap::new();
        for binding in bindings {
            for tag in binding.inbound_tags {
                endpoints_by_role
                    .entry(TrafficScope::Inbound { tag })
                    .or_default()
                    .push(binding.endpoint_id.clone());
            }
            for tag in binding.outbound_tags {
                endpoints_by_role
                    .entry(TrafficScope::Outbound { tag })
                    .or_default()
                    .push(binding.endpoint_id.clone());
            }
        }
        for flow in self.active_sessions() {
            let mut roles = std::collections::BTreeSet::from([TrafficScope::Global]);
            if let Some(tag) = flow.inbound_tag {
                roles.insert(TrafficScope::Inbound { tag });
            }
            if let Some(tag) = flow.outbound_tag {
                roles.insert(TrafficScope::Outbound { tag });
            }
            for (tag, _) in flow.path.relay_chain {
                roles.insert(TrafficScope::Outbound { tag });
            }
            let resources = roles
                .iter()
                .filter_map(|r| endpoints_by_role.get(r))
                .flatten()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>();
            roles.extend(
                resources
                    .into_iter()
                    .map(|endpoint_id| TrafficScope::Endpoint { endpoint_id }),
            );
            for role in roles {
                let count = counts.entry(role).or_default();
                match flow.network {
                    zero_core::Network::Tcp => count.0 += 1,
                    zero_core::Network::Udp => count.1 += 1,
                }
            }
        }
        counts
    }
    pub(super) fn attach_traffic_activity(&self, snapshots: &mut [TrafficSnapshot]) {
        let counts = self.traffic_activity_index();
        for snapshot in snapshots {
            if !matches!(snapshot.scope, TrafficScope::Peer { .. }) {
                let (stream, datagram) = counts.get(&snapshot.scope).copied().unwrap_or_default();
                snapshot.activity = TrafficActivity {
                    active_stream_flows: Some(stream),
                    active_datagram_flows: Some(datagram),
                    active_packet_routes: snapshot.activity.active_packet_routes,
                };
            }
        }
    }
}
