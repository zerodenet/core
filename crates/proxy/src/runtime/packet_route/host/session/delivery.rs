//! This same native frame carries its complete return/fragment reservation.
use super::*;
impl Session {
    /// The same producing driver's pending future retains this native frame.
    /// It admits all return/derived allocations before entering the RX queue;
    /// waiting never occupies a second task or an uncounted payload slot.
    pub(super) async fn deliver_wait(&self, packet: IpPacket) -> Result<(), ChannelError> {
        use zero_platform_tokio::contract::context::PacketIoFact;
        let Some(parent) = self.parent.upgrade() else {
            return Ok(());
        };
        let association = packet.return_association.clone();
        let bytes = packet.payload().len();
        let received = async {
            let storage = parent
                .budget
                .retain_reply_buffer_wait(packet.into_payload(), self.return_mtu)
                .await
                .map_err(|source| {
                    error(
                        Reason::Capacity,
                        "native return cannot fit the installed return policy",
                    )
                    .with_source(source)
                })?;
            let packet =
                IpPacket::new(storage, association.clone()).expect("same validated IP envelope");
            let replies = self
                .replies
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone();
            if let Some(replies) = replies {
                if replies.send(packet).await.is_ok() {
                    if let Some(observer) = &self.observer {
                        observer.packet_io(&association, PacketIoFact::Received { bytes });
                    }
                }
            }
            Ok(())
        };
        tokio::select! { biased; _ = self.cancel.cancelled() => Ok(()), result = received => result }
    }
    pub(super) fn deliver(&self, packet: IpPacket, native: bool) -> bool {
        use zero_platform_tokio::contract::context::PacketIoFact;
        if self.cancel.is_cancelled() {
            return false;
        }
        let Some(parent) = self.parent.upgrade() else {
            return false;
        };
        let association = packet.return_association.clone();
        let bytes = packet.payload().len();
        let storage = match parent
            .budget
            .retain_reply_buffer_for(packet.into_payload(), self.return_mtu)
        {
            Ok(storage) => storage,
            Err(_) => {
                if let Some(observer) = &self.observer {
                    observer.packet_io(
                        &association,
                        PacketIoFact::Dropped {
                            bytes,
                            reason: zero_traits::PacketDropReason::QueueFull,
                        },
                    );
                }
                return false;
            }
        };
        let packet =
            IpPacket::new(storage, association.clone()).expect("same validated IP envelope");
        if native {
            if let Some(observer) = &self.observer {
                observer.packet_io(&association, PacketIoFact::Received { bytes });
            }
        }
        let delivered = self
            .replies
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|replies| replies.try_send(packet).is_ok());
        if !delivered {
            if let Some(observer) = &self.observer {
                observer.packet_io(
                    &association,
                    PacketIoFact::Dropped {
                        bytes,
                        reason: zero_traits::PacketDropReason::QueueClosed,
                    },
                );
            }
        }
        delivered
    }
}
