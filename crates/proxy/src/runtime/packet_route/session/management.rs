use super::*;
impl PacketSessionPins {
    pub(crate) fn managed(mut self, engine: zero_engine::Engine, inbound: String) -> Self {
        self.management = Some((engine, inbound));
        self
    }
    pub(crate) async fn management_changed(&mut self) {
        let changed = self.management_changed.clone();
        tokio::select! {
            _ = changed.notified() => {},
            _ = self.standard_management_changed() => {},
        }
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
