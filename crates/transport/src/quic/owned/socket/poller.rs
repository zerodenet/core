use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use zero_platform_tokio::controlled_datagram::metadata::DatagramWritePoller;

pub(super) struct Poller {
    native: Mutex<Result<DatagramWritePoller, Arc<io::Error>>>,
    callbacks: Arc<callbacks::Broker>,
}
impl Poller {
    pub(super) fn new(
        poller: io::Result<DatagramWritePoller>,
        callbacks: Arc<callbacks::Broker>,
    ) -> Self {
        Self {
            native: Mutex::new(poller.map_err(Arc::new)),
            callbacks,
        }
    }
}
impl fmt::Debug for Poller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataWritePoller")
            .finish_non_exhaustive()
    }
}
impl quinn::UdpPoller for Poller {
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Quinn provides exclusive access here. get_mut supplies Sync without
        // holding an outer mutex across native polling, caller wakers or observers.
        let this = self.get_mut();
        this.callbacks.check()?;
        match this.native.get_mut().unwrap() {
            Ok(poller) => poller.poll_writable(cx),
            Err(source) => {
                Poll::Ready(Err(io::Error::new(source.kind(), SharedIo(source.clone()))))
            }
        }
    }
}
#[derive(Debug)]
pub(super) struct SharedIo(pub(super) Arc<io::Error>);
impl fmt::Display for SharedIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedIo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
