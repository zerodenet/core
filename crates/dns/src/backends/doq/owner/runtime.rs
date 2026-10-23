//! Quinn's opaque driver futures are registered rather than detached. Runtime
//! holds a Weak owner so EndpointRef/driver/runtime cannot create an owner cycle.
use super::{EndpointOwner, Status};
use quinn::{AsyncTimer, AsyncUdpSocket, Runtime, UdpPoller};
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll},
};

pub(super) struct OwnedRuntime {
    pub(super) owner: Weak<EndpointOwner>,
    pub(super) status: Arc<Status>,
}
impl std::fmt::Debug for OwnedRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsQuinnRuntime").finish_non_exhaustive()
    }
}
impl Runtime for OwnedRuntime {
    fn new_timer(&self, at: std::time::Instant) -> Pin<Box<dyn AsyncTimer>> {
        quinn::TokioRuntime.new_timer(at)
    }
    fn now(&self) -> std::time::Instant {
        quinn::TokioRuntime.now()
    }
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        match self.owner.upgrade() {
            Some(owner) => {
                if let Err(source) = owner.spawn(future) {
                    self.status.record(source);
                }
            }
            None => self.status.record(io::Error::new(
                io::ErrorKind::NotConnected,
                "DoQ runtime lost its registered endpoint owner",
            )),
        }
    }
    fn wrap_udp_socket(&self, socket: std::net::UdpSocket) -> io::Result<Arc<dyn AsyncUdpSocket>> {
        Ok(Arc::new(RecordedSocket {
            inner: quinn::TokioRuntime.wrap_udp_socket(socket)?,
            status: self.status.clone(),
        }))
    }
}
struct RecordedSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    status: Arc<Status>,
}
impl std::fmt::Debug for RecordedSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsQuinnSocket").finish_non_exhaustive()
    }
}
impl AsyncUdpSocket for RecordedSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(RecordedPoller {
            inner: self.inner.clone().create_io_poller(),
            status: self.status.clone(),
        })
    }
    fn try_send(&self, transmit: &quinn::udp::Transmit) -> io::Result<()> {
        self.inner
            .try_send(transmit)
            .map_err(|source| self.status.io_error(source))
    }
    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [std::io::IoSliceMut<'_>],
        meta: &mut [quinn::udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.inner
            .poll_recv(cx, bufs, meta)
            .map(|result| result.map_err(|source| self.status.io_error(source)))
    }
    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
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
struct RecordedPoller {
    inner: Pin<Box<dyn UdpPoller>>,
    status: Arc<Status>,
}
impl std::fmt::Debug for RecordedPoller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsQuinnPoller").finish_non_exhaustive()
    }
}
impl UdpPoller for RecordedPoller {
    fn poll_writable(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        let status = self.status.clone();
        self.inner
            .as_mut()
            .poll_writable(cx)
            .map(|result| result.map_err(|source| status.io_error(source)))
    }
}
