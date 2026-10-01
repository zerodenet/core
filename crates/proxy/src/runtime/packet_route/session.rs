//! Pin an ingress IP conversation to one data plane across route reloads.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use zero_stack::packet;

const MAX_PINNED_ROUTES: usize = 4_096;
const IDLE_TIMEOUT: Duration = Duration::from_secs(600);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum PacketPlane {
    Packet(String),
    TranslatedPacket(String),
    Flow,
    DirectEcho,
}

type RouteKey = (packet::PacketConversationKey, Option<std::sync::Arc<str>>);

type MeterProvider = std::sync::Arc<
    dyn Fn(&str, Option<&str>, Option<&str>) -> Vec<zero_engine::TrafficMeter> + Send + Sync,
>;

struct RoutePin {
    _traffic: Vec<zero_engine::TrafficRouteLease>,
    plane: PacketPlane,
    outbound_peer: Option<std::sync::Arc<str>>,
    touched: Instant,
}

#[derive(Default)]
pub(crate) struct PacketSessionPins {
    entries: HashMap<RouteKey, RoutePin>,
    meters: Option<MeterProvider>,
}

impl PacketSessionPins {
    pub(crate) fn with_meters(meters: MeterProvider) -> Self {
        Self {
            entries: Default::default(),
            meters: Some(meters),
        }
    }
    pub(crate) fn expire(&mut self) {
        let now = Instant::now();
        self.entries
            .retain(|_, pin| now.duration_since(pin.touched) < IDLE_TIMEOUT);
    }
    pub(crate) fn permits(&mut self, packet: &[u8], plane: &PacketPlane) -> bool {
        self.permits_peer(packet, plane, None)
    }
    pub(crate) fn permits_peer(
        &mut self,
        packet: &[u8],
        plane: &PacketPlane,
        peer: Option<std::sync::Arc<str>>,
    ) -> bool {
        let Some(key) = key(packet, peer) else {
            return false;
        };
        let now = Instant::now();
        self.entries
            .retain(|_, pin| now.duration_since(pin.touched) < IDLE_TIMEOUT);
        match self.entries.get(&key) {
            Some(pin) => &pin.plane == plane,
            None => self.entries.len() < MAX_PINNED_ROUTES,
        }
    }

    pub(crate) fn record(&mut self, packet: &[u8], plane: PacketPlane) -> bool {
        self.record_peers(packet, plane, None, None)
    }
    pub(crate) fn record_peers(
        &mut self,
        packet: &[u8],
        plane: PacketPlane,
        inbound_peer: Option<std::sync::Arc<str>>,
        outbound_peer: Option<std::sync::Arc<str>>,
    ) -> bool {
        let Some(key) = key(packet, inbound_peer.clone()) else {
            return false;
        };
        if let Some(pin) = self.entries.get_mut(&key) {
            if pin.plane != plane {
                return false;
            }
            if pin.outbound_peer != outbound_peer {
                pin._traffic = leases(
                    self.meters.as_ref(),
                    &plane,
                    inbound_peer.as_deref(),
                    outbound_peer.as_deref(),
                    std::mem::take(&mut pin._traffic),
                );
                pin.outbound_peer = outbound_peer;
            }
            pin.touched = Instant::now();
            return true;
        }
        if self.entries.len() >= MAX_PINNED_ROUTES {
            return false;
        }
        self.entries.insert(
            key,
            RoutePin {
                _traffic: leases(
                    self.meters.as_ref(),
                    &plane,
                    inbound_peer.as_deref(),
                    outbound_peer.as_deref(),
                    Vec::new(),
                ),
                outbound_peer,
                plane,
                touched: Instant::now(),
            },
        );
        true
    }
}

fn leases(
    provider: Option<&MeterProvider>,
    plane: &PacketPlane,
    input: Option<&str>,
    output: Option<&str>,
    mut previous: Vec<zero_engine::TrafficRouteLease>,
) -> Vec<zero_engine::TrafficRouteLease> {
    match (provider, plane) {
        (Some(provider), PacketPlane::Packet(tag) | PacketPlane::TranslatedPacket(tag)) => {
            provider(tag, input, output)
                .into_iter()
                .map(
                    |meter| match previous.iter().position(|lease| lease.belongs_to(&meter)) {
                        Some(index) => previous.swap_remove(index),
                        None => meter.packet_route(),
                    },
                )
                .collect()
        }
        _ => Vec::new(),
    }
}

fn key(packet: &[u8], peer: Option<std::sync::Arc<str>>) -> Option<RouteKey> {
    packet::packet_conversation_key(packet).map(|key| (key, peer))
}

#[cfg(test)]
#[path = "session/tests.rs"]
mod tests;
