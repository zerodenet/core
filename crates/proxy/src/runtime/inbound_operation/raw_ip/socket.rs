//! The standard source retains the granted original UDP views.
use zero_platform_tokio::{
    contract::component::GrantedDatagramListener, EgressInterfaceControl, PacketSocketReceiver,
    PacketSocketSender,
};
pub(super) struct RawIpReceive(PacketSocketReceiver);
impl RawIpReceive {
    pub(super) fn split(
        grant: GrantedDatagramListener,
        egress: EgressInterfaceControl,
    ) -> (Self, PacketSocketSender) {
        let (receiver, sender) = PacketSocketReceiver::from_granted(grant, egress);
        (Self(receiver), sender)
    }
    pub(super) async fn recv_from(
        &mut self,
        bytes: &mut [u8],
    ) -> std::io::Result<(usize, std::net::SocketAddr)> {
        self.0.recv_from(bytes).await
    }
}
#[cfg(test)]
mod tests;
