use super::*;
use crate::quic::owned::HostedMetadataSocket;

pub(super) enum Native {
    Owned(Box<MetadataSocketOwner>),
    Hosted(HostedMetadataSocket),
    Packet(super::super::packet::PacketSocket),
}
impl Native {
    pub(super) fn socket(&self) -> Arc<dyn quinn::AsyncUdpSocket> {
        match self {
            Self::Owned(socket) => socket.socket(),
            Self::Hosted(socket) => socket.socket(),
            Self::Packet(socket) => socket.socket.clone(),
        }
    }
    pub(super) fn callbacks(&self) -> &Arc<super::super::callbacks::Broker> {
        match self {
            Self::Owned(socket) => &socket.callbacks,
            Self::Hosted(socket) => &socket.callbacks,
            Self::Packet(socket) => &socket.callbacks,
        }
    }
    pub(super) fn suppressed(&self) -> usize {
        self.callbacks().suppressed_count()
    }
}
