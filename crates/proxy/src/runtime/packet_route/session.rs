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
    observer: Option<std::sync::Arc<dyn zero_traits::IoObserver>>,
    plane: PacketPlane,
    outbound_peer: Option<std::sync::Arc<str>>,
    touched: Instant,
    accepted: bool,
    managed: Option<zero_engine::PacketRouteLease>,
    control: Option<zero_engine::PacketRouteControl>,
    replies: Option<tokio::sync::mpsc::Sender<Vec<u8>>>,
}

#[derive(Default)]
pub(crate) struct PacketSessionPins {
    entries: HashMap<RouteKey, RoutePin>,
    meters: Option<MeterProvider>,
    management: Option<(zero_engine::Engine, String)>,
    management_changed: std::sync::Arc<tokio::sync::Notify>,
}

mod management;

impl PacketSessionPins {
    /// Reuse the observation handle for an admitted conversation. New paths
    /// prepare once; ordinary packets do not visit the statistics registry.
    pub(crate) fn inner_io(
        &self,
        packet: &[u8],
        plane: &PacketPlane,
        peer: Option<std::sync::Arc<str>>,
        prepare: impl FnOnce() -> Option<std::sync::Arc<dyn zero_traits::IoObserver>>,
    ) -> Option<std::sync::Arc<dyn zero_traits::IoObserver>> {
        if let Some(pin) = key(packet, peer).and_then(|key| self.entries.get(&key)) {
            if &pin.plane == plane {
                return pin.observer.clone();
            }
        }
        prepare()
    }

    pub(crate) fn record_observed_peers(
        &mut self,
        packet: &[u8],
        plane: PacketPlane,
        inbound_peer: Option<std::sync::Arc<str>>,
        outbound_peer: Option<std::sync::Arc<str>>,
        observer: Option<std::sync::Arc<dyn zero_traits::IoObserver>>,
    ) -> bool {
        if !self.record_peers(packet, plane, inbound_peer.clone(), outbound_peer) {
            return false;
        }
        if let Some(pin) = key(packet, inbound_peer).and_then(|key| self.entries.get_mut(&key)) {
            pin.observer = observer;
        }
        true
    }

    pub(crate) fn with_meters(meters: MeterProvider) -> Self {
        Self {
            entries: Default::default(),
            meters: Some(meters),
            management: None,
            management_changed: Default::default(),
        }
    }
    pub(crate) fn expire(&mut self) {
        self.retire_closed();
        let now = Instant::now();
        self.entries
            .retain(|_, pin| now.duration_since(pin.touched) < IDLE_TIMEOUT);
    }

    pub(crate) fn retain_admitted(&mut self, mut admitted: impl FnMut(&PacketPlane) -> bool) {
        self.entries.retain(|_, pin| admitted(&pin.plane));
        self.expire();
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
            Some(pin) => {
                &pin.plane == plane && !pin.control.as_ref().is_some_and(|c| c.is_closed())
            }
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
            pin.accepted = true;
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
                observer: None,
                plane,
                touched: Instant::now(),
                accepted: true,
                managed: None,
                control: None,
                replies: None,
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
