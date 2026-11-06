//! Unique view over the original packet controller retained by the carrier owner.
use super::{CarrierDriver, slot::Slot};
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
        self.slot.check_process_endpoint()?;
        if self.slot.cancel.is_cancelled() {
            return Err(io::ErrorKind::Interrupted.into());
        }
        let parent = self.slot.related_cancellation();
        let related = async {
            match parent {
                Some(parent) => parent.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        };
        let size = tokio::select! { biased;
            _ = self.slot.cancel.cancelled() => return Err(io::ErrorKind::Interrupted.into()),
            _ = related => return Err(io::ErrorKind::Interrupted.into()),
            size = self.io.send_to_addr(bytes, self.peer) => size?,
        };
        self.slot.check_process_endpoint()?;
        if size != bytes.len() {
            return Err(io::ErrorKind::WriteZero.into());
        }
        Ok(())
    }
    pub async fn receive(&self, bytes: &mut [u8]) -> io::Result<usize> {
        loop {
            self.slot.check_process_endpoint()?;
            if self.slot.cancel.is_cancelled() {
                return Err(io::ErrorKind::Interrupted.into());
            }
            let parent = self.slot.related_cancellation();
            let related = async {
                match parent {
                    Some(parent) => parent.cancelled().await,
                    None => std::future::pending::<()>().await,
                }
            };
            let (size, peer) = tokio::select! { biased;
                _ = self.slot.cancel.cancelled() => return Err(io::ErrorKind::Interrupted.into()),
                _ = related => return Err(io::ErrorKind::Interrupted.into()),
                value = self.io.recv_from_addr(bytes) => value?,
            };
            self.slot.check_process_endpoint()?;
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
