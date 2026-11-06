//! Native reply lookup and delivery preserve the original buffer owner.
use super::*;

impl PacketReturns {
    /// This future stays in the same native wire loop's pending-frame slot.
    /// Full return credit or queue capacity pauses RX, while that loop keeps TX
    /// and control progressing. The whole original stays owned during waiting.
    pub(super) async fn deliver_native_wait(
        &self,
        mut original: zero_traits::PacketBuffer,
    ) -> Result<Option<zero_traits::PacketBuffer>, zero_platform_tokio::contract::ChannelError>
    {
        use zero_platform_tokio::contract::{ChannelError, Impact, Reason, Stage};
        let Some(destination) = packet::ip_destination(&original) else {
            return Ok(Some(original));
        };
        let replies = self
            .routes
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&destination)
            .filter(|route| route.touched.elapsed() < IDLE_TIMEOUT)
            .map(|route| route.replies.clone());
        let Some(replies) = replies else {
            return Ok(Some(original));
        };
        let key = packet::packet_return_key(&original);
        let observer = key.and_then(|key| {
            self.observations
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get(&key)
                .filter(|entry| entry.touched.elapsed() < IDLE_TIMEOUT)
                .and_then(|entry| entry.observer.clone())
        });
        let replies = key
            .and_then(|key| {
                self.conversations
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .get(&key)
                    .filter(|entry| entry.touched.elapsed() < Duration::from_secs(600))
                    .map(|entry| entry.replies.clone())
            })
            .unwrap_or(replies);
        if let Some(observer) = &observer {
            observer.received(original.len());
        }
        if !packet::advance_ip_hop(&mut original) {
            self.discard(observer.as_deref(), zero_traits::PacketDropReason::HopLimit);
            return Ok(None);
        }
        let storage = match replies.return_mtu() {
            Some(mtu) => self
                .retain_reply_buffer_wait(original, mtu)
                .await
                .map_err(|source| {
                    ChannelError::new(Stage::Receive, Reason::Capacity, Impact::Packet)
                        .with_source(source)
                })?,
            // The explicit old library sink has no actual Source link metadata.
            None => {
                self.send_reply(&replies, original, observer.as_deref());
                return Ok(None);
            }
        };
        if replies.send_buffer(storage).await.is_err() {
            self.discard(
                observer.as_deref(),
                zero_traits::PacketDropReason::QueueClosed,
            );
        }
        Ok(None)
    }
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
