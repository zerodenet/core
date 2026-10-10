//! Native reply lookup and delivery preserve the original buffer owner.
use super::*;

impl PacketReturns {
    pub(super) fn deliver_native<'a>(
        &self,
        packet: NativePacket<'a>,
    ) -> Result<(), NativePacket<'a>> {
        let Some(destination) = packet::ip_destination(packet.as_ref()) else {
            return Err(packet);
        };
        let replies = self
            .routes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&destination)
            .filter(|route| route.touched.elapsed() < IDLE_TIMEOUT)
            .map(|route| route.replies.clone());
        let Some(replies) = replies else {
            return Err(packet);
        };
        let key = packet::packet_return_key(packet.as_ref());
        let observation = key.and_then(|key| {
            self.observations
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&key)
                .filter(|observation| observation.touched.elapsed() < IDLE_TIMEOUT)
                .and_then(|observation| observation.observer.clone())
        });
        let observer = observation;
        let replies = key
            .and_then(|key| {
                self.conversations
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&key)
                    .filter(|c| c.touched.elapsed() < Duration::from_secs(600))
                    .map(|c| c.replies.clone())
            })
            .unwrap_or(replies);
        if replies.is_closed() {
            if let Some(observer) = &observer {
                observer.received(packet.as_ref().len());
            }
            self.discard(
                observer.as_deref(),
                zero_traits::PacketDropReason::QueueClosed,
            );
            return Ok(());
        }
        if let Some(observer) = &observer {
            observer.received(packet.as_ref().len());
        }
        let mut forwarded = packet.into_owned();
        if packet::advance_ip_hop(&mut forwarded) {
            self.send_reply(&replies, forwarded, observer.as_deref());
        } else {
            self.discard(observer.as_deref(), zero_traits::PacketDropReason::HopLimit);
        }
        Ok(())
    }
}
