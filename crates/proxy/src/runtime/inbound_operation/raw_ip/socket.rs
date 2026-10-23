//! Preserve the actual granted UDP views, including packet-send egress policy.
use super::*;
use zero_platform_tokio::{
    EgressInterfaceControl, PacketSocket, PacketSocketReceiver, PacketSocketSender,
};

pub(super) enum RawIpReceive {
    Legacy(PacketSocket),
    Controlled(PacketSocketReceiver),
}
impl RawIpReceive {
    pub(super) fn split(
        bound: BoundInbound,
        egress: EgressInterfaceControl,
    ) -> Result<(Self, PacketSocketSender), EngineError> {
        match bound {
            BoundInbound::Datagram(socket) => {
                let sender = PacketSocketSender::new(socket.clone(), egress);
                Ok((Self::Legacy(socket), sender))
            }
            BoundInbound::ControlledDatagram(view) => {
                let (receiver, sender) = PacketSocketReceiver::from_granted(view, egress);
                Ok((Self::Controlled(receiver), sender))
            }
            _ => Err(std::io::Error::other("expected datagram listener").into()),
        }
    }
    pub(super) async fn recv_from(
        &mut self,
        bytes: &mut [u8],
    ) -> std::io::Result<(usize, SocketAddr)> {
        match self {
            Self::Legacy(socket) => socket.recv_from(bytes).await,
            Self::Controlled(receiver) => receiver.recv_from(bytes).await,
        }
    }
}

#[cfg(test)]
mod tests;
