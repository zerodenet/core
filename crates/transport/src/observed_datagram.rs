//! Observation beneath UDP codecs; preserves socket batching capabilities.
use quinn::{
    udp::{RecvMeta, Transmit},
    AsyncUdpSocket, UdpPoller,
};
use std::{
    io::{self, IoSliceMut},
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use zero_traits::IoObserver;
#[derive(Debug)]
struct Socket {
    inner: Arc<dyn AsyncUdpSocket>,
    observer: Arc<dyn IoObserver>,
}
pub(crate) fn wrap(
    inner: Arc<dyn AsyncUdpSocket>,
    observer: Option<Arc<dyn IoObserver>>,
) -> Arc<dyn AsyncUdpSocket> {
    match observer {
        Some(observer) => {
            observer.datagram_boundary();
            Arc::new(Socket { inner, observer })
        }
        None => inner,
    }
}
impl AsyncUdpSocket for Socket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        self.inner.clone().create_io_poller()
    }
    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        let result = self.inner.try_send(transmit);
        match &result {
            Ok(()) => {
                let stride = transmit
                    .segment_size
                    .unwrap_or(transmit.contents.len().max(1))
                    .max(1);
                for segment in transmit.contents.chunks(stride) {
                    self.observer.sent_datagram(segment.len());
                }
                if transmit.contents.is_empty() {
                    self.observer.sent_datagram(0);
                }
            }
            Err(error) if error.kind() != io::ErrorKind::WouldBlock => self.observer.error(),
            _ => {}
        }
        result
    }
    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let result = self.inner.poll_recv(cx, bufs, meta);
        match &result {
            Poll::Ready(Ok(count)) => {
                for item in meta.iter().take(*count) {
                    let stride = item.stride.max(1);
                    let mut remaining = item.len;
                    while remaining > 0 {
                        let bytes = remaining.min(stride);
                        self.observer.received_datagram(bytes);
                        remaining -= bytes;
                    }
                    if item.len == 0 {
                        self.observer.received_datagram(0);
                    }
                }
            }
            Poll::Ready(Err(error)) if error.kind() != io::ErrorKind::WouldBlock => {
                self.observer.error()
            }
            _ => {}
        }
        result
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
    fn max_transmit_segments(&self) -> usize {
        self.inner.max_transmit_segments()
    }
    fn max_receive_segments(&self) -> usize {
        self.inner.max_receive_segments()
    }
    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

#[cfg(test)]
#[path = "../tests/observation/datagram.rs"]
mod tests;
