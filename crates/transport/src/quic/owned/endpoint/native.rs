use super::*;
use crate::quic::owned::HostedMetadataSocket;

pub(super) enum Native {
    Owned(Box<MetadataSocketOwner>),
    Hosted(HostedMetadataSocket),
    Borrowed(super::borrowed::BorrowedSocket),
}
impl Native {
    pub(super) fn socket(&self) -> Arc<dyn quinn::AsyncUdpSocket> {
        match self {
            Self::Owned(socket) => socket.socket(),
            Self::Hosted(socket) => socket.socket(),
            Self::Borrowed(socket) => socket.socket.clone(),
        }
    }
    pub(super) fn callbacks(&self) -> &Arc<super::super::callbacks::Broker> {
        match self {
            Self::Owned(socket) => &socket.callbacks,
            Self::Hosted(socket) => &socket.callbacks,
            Self::Borrowed(socket) => &socket.callbacks,
        }
    }
    pub(super) fn suppressed(&self) -> usize {
        self.callbacks().suppressed_count()
    }
}
