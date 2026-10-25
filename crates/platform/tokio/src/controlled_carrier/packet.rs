//! Unique view over the original packet controller retained by the carrier owner.
use super::{slot::Slot, CarrierDriver};
use crate::controlled_datagram::ControlledDatagramSocket;
use std::{io, net::SocketAddr, sync::Arc};

pub struct CarrierDatagram {
    pub(super) io: ControlledDatagramSocket,
    pub(super) slot: Arc<Slot>,
    pub(super) peer: SocketAddr,
}
impl CarrierDatagram {
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.local_addr()
    }
    pub async fn send(&self, bytes: &[u8]) -> io::Result<()> {
        if self.slot.cancel.is_cancelled() {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let size = self.io.send_to_addr(bytes, self.peer).await?;
        if size != bytes.len() {
            return Err(io::ErrorKind::WriteZero.into());
        }
        Ok(())
    }
    pub async fn receive(&self, bytes: &mut [u8]) -> io::Result<usize> {
        loop {
            if self.slot.cancel.is_cancelled() {
                return Err(io::ErrorKind::Interrupted.into());
            }
            let (size, peer) = self.io.recv_from_addr(bytes).await?;
            if peer == self.peer {
                return Ok(size);
            }
        }
    }
    pub fn into_driver(self) -> CarrierDriver<Self> {
        CarrierDriver {
            slot: self.slot.clone(),
            io: self,
        }
    }
}
impl Drop for CarrierDatagram {
    fn drop(&mut self) {
        self.slot.state().view_live = false;
        self.slot.cancel.cancel();
        self.slot.notify();
    }
}
