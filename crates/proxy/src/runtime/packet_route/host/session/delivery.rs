//! This same native frame carries its complete return/fragment reservation.
use super::*;
impl Session {
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
