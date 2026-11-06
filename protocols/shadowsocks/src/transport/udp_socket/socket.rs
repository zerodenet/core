//! A native view stays in the same original slot; aliases hold no raw fd.
use super::*;
use std::sync::Mutex;
use zero_platform_tokio::controlled_carrier::CarrierDatagram;
#[derive(Clone)]
pub(super) enum Socket {
    Library(Arc<zero_platform_tokio::TokioDatagramSocket>),
    Native(Arc<Native>),
}
pub(super) struct Native {
    io: Mutex<Option<Arc<CarrierDatagram>>>,
    stop: zero_platform_tokio::contract::CancellationToken,
}
impl Native {
    pub(super) fn new(
        io: Arc<CarrierDatagram>,
        stop: zero_platform_tokio::contract::CancellationToken,
    ) -> Self {
        Self {
            io: Mutex::new(Some(io)),
            stop,
        }
    }
    fn io(&self) -> std::io::Result<Arc<CarrierDatagram>> {
        self.io
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| std::io::ErrorKind::Interrupted.into())
    }
    pub(super) fn retire(&self) {
        // Close the actual same-slot view after active operations return. No
        // cache alias can retain an unrevocable original after this point.
        let original = self.io.lock().unwrap().take();
        drop(original);
    }
    pub(super) fn unavailable(&self) -> bool {
        self.stop.is_cancelled() || self.io.lock().unwrap().is_none()
    }
}
impl Socket {
    pub(super) async fn send(&self, bytes: &[u8], peer: SocketAddr) -> std::io::Result<()> {
        match self {
            Self::Library(io) => {
                let sent = io.send_to_addr(bytes, peer).await?;
                if sent != bytes.len() {
                    return Err(std::io::ErrorKind::WriteZero.into());
                }
                Ok(())
            }
            Self::Native(io) => {
                let io = io.io()?;
                if io.peer() != peer {
                    return Err(std::io::ErrorKind::PermissionDenied.into());
                }
                io.send(bytes).await
            }
        }
    }
    pub(super) async fn receive(&self, bytes: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        match self {
            Self::Library(io) => io.recv_from_addr(bytes).await,
            Self::Native(io) => {
                let io = io.io()?;
                io.receive(bytes).await.map(|size| (size, io.peer()))
            }
        }
    }
}
pub(super) struct ReceiverGuard(
    pub(super) tokio::task::AbortHandle,
    pub(super) Arc<Responses>,
);
impl Drop for ReceiverGuard {
    fn drop(&mut self) {
        self.1.close_pending();
        self.0.abort();
    }
}

pub(super) struct ReceiveLifetime(pub(super) Arc<Responses>);
impl Drop for ReceiveLifetime {
    fn drop(&mut self) {
        self.0.close_pending();
    }
}
