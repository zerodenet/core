//! Unique receive ownership over an already controlled native listener.
use super::PacketSocketSender;
use crate::{
    contract::component::GrantedDatagramListener,
    controlled_datagram::listener::ControlledDatagramListenerRx, EgressInterfaceControl,
};
use std::{io, net::SocketAddr};

/// The receive half is deliberately not clonable. Its host retains the original
/// listener controller and native owner; dropping either view is not close.
/// This preserves PacketSocket's payload/source contract. ECN, GRO, GSO and
/// destination/interface receive metadata are not implemented by this interface.
pub struct PacketSocketReceiver {
    rx: ControlledDatagramListenerRx,
}
impl PacketSocketReceiver {
    /// Consume the exact granted views without acquiring another descriptor,
    /// task, packet queue or send budget. The original grant's explicit limit
    /// accounts for every waiting and in-flight send, including other TX clones.
    pub fn from_granted(
        grant: GrantedDatagramListener,
        control: EgressInterfaceControl,
    ) -> (Self, PacketSocketSender) {
        (
            Self { rx: grant.rx },
            PacketSocketSender::controlled(grant.tx, control),
        )
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.rx.local_addr()
    }
    pub async fn recv_from(&mut self, bytes: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.rx.recv_from_addr(bytes).await
    }
}
