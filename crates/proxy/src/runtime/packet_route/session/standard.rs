//! Standard Packet routes live in the original pins; the source polls close
//! signals inline and forwards the unchanged opaque cookie to its reply sink.
use super::*;
use std::{
    future::{poll_fn, Future},
    pin::Pin,
    task::{Context, Poll},
};
use zero_core::packet::{IpPacket, ReturnAssociation};

pub(super) struct StandardRoute {
    association: ReturnAssociation,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    control: zero_engine::PacketRouteControl,
}
impl Drop for StandardRoute {
    fn drop(&mut self) {
        // The main has now removed this actual return mapping; there is no
        // independently running reply pump or fabricated task completion.
        self.control.responses_stopped();
    }
}
impl PacketSessionPins {
    pub(crate) fn admit_standard(
        &mut self,
        packet: &IpPacket,
        plane: &PacketPlane,
        peer: Option<std::sync::Arc<str>>,
    ) -> bool {
        let Some(key) = key(packet.payload(), peer.clone()) else {
            return false;
        };
        if let Some(pin) = self.entries.get(&key) {
            return pin
                .standard
                .as_ref()
                .is_some_and(|bound| bound.association == packet.return_association)
                && &pin.plane == plane
                && !pin.control.as_ref().is_some_and(|c| c.is_closed());
        }
        let Some((engine, inbound)) = self.management.clone() else {
            return false;
        };
        if !self.record_peers(packet.payload(), plane.clone(), peer, None) {
            return false;
        }
        let (tag, translated) = match plane {
            PacketPlane::Packet(tag) => (tag, false),
            PacketPlane::TranslatedPacket(tag) => (tag, true),
            _ => return false,
        };
        let lease = match engine.register_packet_route(zero_api::PacketRouteSnapshot {
            route_id: String::new(),
            core_instance_id: String::new(),
            config_revision: 0,
            endpoints: Vec::new(),
            inbound_tag: inbound,
            outbound_tag: tag.clone(),
            source: key.0.source.to_string(),
            destination: key.0.destination.to_string(),
            ip_protocol: key.0.protocol,
            translated,
            started_at_unix_ms: 0,
            state: zero_api::PacketRouteState::Active,
        }) {
            Ok(lease) => lease,
            Err(_) => {
                self.entries.remove(&key);
                return false;
            }
        };
        let control = lease.control();
        control.responses_started();
        let cancelled = control.clone();
        let pin = self
            .entries
            .get_mut(&key)
            .expect("same admitted actual pin");
        pin.accepted = false;
        pin.control = Some(control.clone());
        pin.standard = Some(StandardRoute {
            association: packet.return_association.clone(),
            cancelled: Box::pin(async move { cancelled.cancelled().await }),
            control,
        });
        pin.managed = Some(lease);
        true
    }
    pub(crate) fn permits_standard_return(&self, packet: &IpPacket) -> bool {
        let key = packet::packet_return_key(packet.payload());
        self.entries.iter().any(|((conversation, _), pin)| {
            key == Some(*conversation)
                && pin.accepted
                && !pin.control.as_ref().is_some_and(|c| c.is_closed())
                && pin
                    .standard
                    .as_ref()
                    .is_some_and(|bound| bound.association == packet.return_association)
        })
    }
    pub(crate) async fn standard_management_changed(&mut self) {
        poll_fn(|cx| self.poll_standard_closed(cx)).await
    }
    fn poll_standard_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let closed = self.entries.values_mut().any(|pin| {
            pin.standard
                .as_mut()
                .is_some_and(|bound| bound.cancelled.as_mut().poll(cx).is_ready())
        });
        if closed {
            // Remove the actual mapping before reporting the taskless response
            // custody released. Unaccepted frames never create a route fact.
            self.entries.retain(|_, pin| {
                !(pin.standard.is_some() && pin.control.as_ref().is_some_and(|c| c.is_closed()))
            });
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
