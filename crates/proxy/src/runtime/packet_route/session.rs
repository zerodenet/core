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

type RouteKey = packet::PacketConversationKey;

struct RoutePin {
    plane: PacketPlane,
    touched: Instant,
}

#[derive(Default)]
pub(crate) struct PacketSessionPins {
    entries: HashMap<RouteKey, RoutePin>,
}

impl PacketSessionPins {
    pub(crate) fn permits(&mut self, packet: &[u8], plane: &PacketPlane) -> bool {
        let Some(key) = key(packet) else {
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
        let Some(key) = key(packet) else {
            return false;
        };
        if let Some(pin) = self.entries.get_mut(&key) {
            if pin.plane != plane {
                return false;
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
                plane,
                touched: Instant::now(),
            },
        );
        true
    }
}

fn key(packet: &[u8]) -> Option<RouteKey> {
    packet::packet_conversation_key(packet)
}

#[cfg(test)]
#[path = "session/tests.rs"]
mod tests;
