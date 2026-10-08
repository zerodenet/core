use super::*;
impl PacketSessionPins {
    pub(crate) fn managed(mut self, engine: zero_engine::Engine, inbound: String) -> Self {
        self.management = Some((engine, inbound));
        self
    }
    pub(crate) async fn management_changed(&self) {
        self.management_changed.notified().await;
    }
    pub(crate) fn replies_for(
        &mut self,
        packet: &[u8],
        plane: &PacketPlane,
        peer: Option<std::sync::Arc<str>>,
        destination: zero_stack::packet_output::PacketSender,
    ) -> Option<zero_stack::packet_output::PacketSender> {
        if self.management.is_none() {
            return Some(destination);
        }
        let (tag, translated) = match plane {
            PacketPlane::Packet(tag) => (tag, false),
            PacketPlane::TranslatedPacket(tag) => (tag, true),
            _ => return Some(destination),
        };
        let route_key = key(packet, peer.clone())?;
        if let Some(pin) = self.entries.get(&route_key) {
            return pin.replies.clone();
        }
        let (engine, inbound) = self.management.clone()?;
        let conversation = route_key.0;
        let lease = engine
            .register_packet_route(zero_api::PacketRouteSnapshot {
                route_id: String::new(),
                core_instance_id: String::new(),
                config_revision: 0,
                endpoints: Vec::new(),
                inbound_tag: inbound,
                outbound_tag: tag.clone(),
                source: conversation.source.to_string(),
                destination: conversation.destination.to_string(),
                ip_protocol: conversation.protocol,
                translated,
                started_at_unix_ms: 0,
                state: zero_api::PacketRouteState::Active,
            })
            .ok()?;
        let control = lease.control();
        control.responses_started();
        let cancelled = control.clone();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<zero_traits::PacketBuffer>(1);
        let response_control = control.clone();
        struct Receipt(
            zero_engine::PacketRouteControl,
            std::sync::Arc<tokio::sync::Notify>,
        );
        impl Drop for Receipt {
            fn drop(&mut self) {
                self.0.responses_stopped();
                self.1.notify_one();
            }
        }
        let receipt = Receipt(response_control, self.management_changed.clone());
        tokio::spawn(async move {
            let _receipt = receipt;
            loop {
                if cancelled.is_closed() {
                    break;
                }
                tokio::select! { biased;
                    _ = cancelled.cancelled() => break,
                    response = rx.recv() => {
                        let Some(response) = response else { break; };
                        // Only this bounded pump waits for its consumer. Device delivery
                        // remains nonblocking and records any queue discard upstream.
                        tokio::select! { biased;
                            _ = cancelled.cancelled() => break,
                            sent = destination.send_buffer(response) => if sent.is_err() { break; },
                        }
                    }
                }
            }
        });
        if !self.record_peers(packet, plane.clone(), peer, None) {
            return None;
        }
        let pin = self.entries.get_mut(&route_key)?;
        pin.accepted = false;
        pin.managed = Some(lease);
        pin.control = Some(control);
        pin.replies = Some(tx.clone().into());
        Some(tx.into())
    }
    #[cfg(test)]
    pub(crate) fn reject_unaccepted(&mut self, packet: &[u8], peer: Option<std::sync::Arc<str>>) {
        self.reject_unaccepted_key(packet::packet_conversation_key(packet), peer);
    }
    pub(crate) fn reject_unaccepted_key(
        &mut self,
        packet_key: Option<packet::PacketConversationKey>,
        peer: Option<std::sync::Arc<str>>,
    ) {
        if let Some(key) = packet_key.map(|key| (key, peer)) {
            if self.entries.get(&key).is_some_and(|p| !p.accepted) {
                self.entries.remove(&key);
            }
        }
    }
    pub(super) fn retire_closed(&mut self) {
        for pin in self.entries.values_mut() {
            if pin.control.as_ref().is_some_and(|c| c.is_closed()) {
                pin._traffic.clear();
                pin.managed.take();
                pin.observer.take();
            }
        }
    }
}
