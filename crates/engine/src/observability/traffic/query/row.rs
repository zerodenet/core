//! Fixed numeric/mask capture; serialization never allocates metric/reason DTOs.
use super::*;
use serde::{
    ser::{SerializeSeq, SerializeStruct},
    Serialize, Serializer,
};
use zero_api::{
    TrafficCounters, TrafficDropCounter, TrafficDropReason, TrafficMetric, TrafficPlane,
    TrafficRole,
};
#[derive(Clone, Copy)]
pub(super) struct FrozenPlane {
    plane: TrafficPlane,
    basis: &'static str,
    values: [u64; 22],
    available: u16,
    roles: u16,
    reasons: u16,
}
impl FrozenPlane {
    pub(super) fn new(
        i: usize,
        original: &super::super::counter::Plane,
        values: [u64; 22],
        baseline: [u64; 22],
        scope: &TrafficScope,
    ) -> Self {
        let plane = [
            TrafficPlane::Flow,
            TrafficPlane::Inner,
            TrafficPlane::Outer,
            TrafficPlane::Host,
        ][i];
        let available = original.available.load(Ordering::Acquire)
            & !original.unobservable.load(Ordering::Acquire);
        let mut basis = original
            .basis
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .unwrap_or(match plane {
                TrafficPlane::Flow => "business_boundary_bytes",
                TrafficPlane::Inner => "authenticated_ip_packets_before_reassembly",
                TrafficPlane::Outer => "protocol_datagram_payload_excludes_host_ip_udp_headers",
                TrafficPlane::Host => "host_interface_provider",
            });
        if i == 0 {
            basis = match scope {
                TrafficScope::HostInterface { .. } => basis,
                TrafficScope::Global => "business_direction_max_mirrored_boundaries",
                TrafficScope::Inbound { .. } => "inbound_business_boundary",
                TrafficScope::Outbound { .. } => "final_outbound_flow_observation_not_hop_carrier",
                TrafficScope::Endpoint { .. } => "business_direction_deduplicated_endpoint_roles",
                TrafficScope::Peer { .. } if available == 0 => "unavailable",
                TrafficScope::Peer { .. } => {
                    "business_direction_deduplicated_protocol_peer_flow_bindings"
                }
            };
        }
        Self {
            plane,
            basis,
            values: std::array::from_fn(|i| values[i].saturating_sub(baseline[i])),
            available,
            roles: original.roles.load(Ordering::Acquire),
            reasons: original.reasons.load(Ordering::Acquire),
        }
    }
}
struct Metrics(u16);
impl Serialize for Metrics {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(None)?;
        for metric in TrafficMetric::ALL {
            if self.0 & (1 << (metric as usize)) != 0 {
                seq.serialize_element(&metric)?;
            }
        }
        seq.end()
    }
}
struct Roles(u16);
impl Serialize for Roles {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(None)?;
        for (i, role) in [TrafficRole::Inbound, TrafficRole::Outbound]
            .into_iter()
            .enumerate()
        {
            if self.0 & (1 << i) != 0 {
                seq.serialize_element(&role)?;
            }
        }
        seq.end()
    }
}
struct Reasons<'a>(&'a FrozenPlane);
impl Serialize for Reasons<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(None)?;
        for (i, reason) in TrafficDropReason::ALL.into_iter().enumerate() {
            if self.0.reasons & (1 << i) != 0 {
                seq.serialize_element(&TrafficDropCounter {
                    reason,
                    packets: self.0.values[12 + i],
                })?;
            }
        }
        seq.end()
    }
}
impl Serialize for FrozenPlane {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let v = self.values;
        let get = |i: usize| (self.available & (1 << i) != 0).then_some(v[i]);
        let counters = TrafficCounters {
            bytes_up: get(0),
            bytes_down: get(1),
            rx_bytes: get(2),
            tx_bytes: get(3),
            rx_packets: get(4),
            tx_packets: get(5),
            dropped_packets: get(6),
            errors: get(7),
            rx_dropped_packets: get(8),
            tx_dropped_packets: get(9),
            rx_errors: get(10),
            tx_errors: get(11),
        };
        let coverage = (self.reasons != 0
            || self.available & (1 << (TrafficMetric::DroppedPackets as usize)) != 0)
            .then_some("observed_local_boundary_discards_only");
        let mut out = s.serialize_struct("TrafficPlaneSnapshot", 9)?;
        out.serialize_field("plane", &self.plane)?;
        out.serialize_field("accounting_basis", self.basis)?;
        out.serialize_field("counters", &counters)?;
        out.serialize_field("available_metrics", &Metrics(self.available))?;
        out.serialize_field("resettable_metrics", &Metrics(self.available))?;
        out.serialize_field("source_roles", &Roles(self.roles))?;
        out.serialize_field("drop_reasons", &Reasons(self))?;
        out.serialize_field("drop_reasons_resettable", &(self.reasons != 0))?;
        out.serialize_field("drop_coverage", &coverage)?;
        out.end()
    }
}
