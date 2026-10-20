//! Byte I/O and explicit native close share one original TCP descriptor.
#[cfg(any(unix, windows))]
pub(crate) mod connect;
mod control;
mod lease;
mod native;
mod observation;
mod relay;
mod socket;
use crate::TokioSocket;
#[cfg(any(unix, windows))]
pub use connect::PendingTcpConnect;
pub use control::ControlledTcpControl;
use relay::{IoRelays, StateGuard, WaitTarget};
use socket::Socket;
use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::sync::CancellationToken;

struct Metadata {
    local: Option<SocketAddr>,
    peer: Option<SocketAddr>,
    interface: Option<crate::EgressInterface>,
}
struct State {
    socket: Option<Socket>,
    closed: bool,
    error: Option<Arc<io::Error>>,
    retry: Option<Arc<io::Error>>,
    unknown: Option<Arc<io::Error>>,
    closing: bool,
    polling: bool,
    metadata: Option<Metadata>,
}
struct Shared {
    state: Mutex<State>,
    cancel: CancellationToken,
    label: String,
    observer: Mutex<Option<Arc<dyn zero_traits::IoObserver>>>,
    observations: Arc<observation::Capture>,
    wakes: IoRelays,
}

impl Shared {
    fn state(&self) -> StateGuard<'_> {
        StateGuard::lock(self)
    }
    fn poll_io<T>(
        &self,
        operation: impl FnOnce(&mut Socket) -> Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        let Some(mut socket) = self.lease() else {
            return Poll::Ready(Err(closed()));
        };
        // The existing registration may still own a pre-transfer RawWaker.
        // Poll outside the state mutex as well as using a fixed relay Context.
        operation(&mut socket)
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        let state = self.state();
        match state.socket.as_ref() {
            Some(socket) => socket.local_addr(),
            None => state
                .metadata
                .as_ref()
                .and_then(|facts| facts.local)
                .ok_or_else(closed),
        }
    }
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        let state = self.state();
        match state.socket.as_ref() {
            Some(socket) => socket.peer_addr(),
            None => state
                .metadata
                .as_ref()
                .and_then(|facts| facts.peer)
                .ok_or_else(closed),
        }
    }
    fn interface(&self) -> Option<crate::EgressInterface> {
        let state = self.state();
        state
            .socket
            .as_ref()
            .and_then(|socket| socket.egress_interface().cloned())
            .or_else(|| {
                state
                    .metadata
                    .as_ref()
                    .and_then(|facts| facts.interface.clone())
            })
    }
}

/// A unique byte-stream view. Its separate control must remain in a persistent
/// owner before this view is transferred to a cancelable protocol operation.
/// Dropping either view requests cancellation, never a successful close receipt.
pub struct ControlledTcpStream {
    shared: Arc<Shared>,
    read_cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    write_cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    read_waiter: Arc<WaitTarget>,
    write_waiter: Arc<WaitTarget>,
}
impl ControlledTcpStream {
    pub fn new(mut socket: TokioSocket, parent: CancellationToken) -> (Self, ControlledTcpControl) {
        let observer = socket.observer.take();
        let observations = Arc::new(observation::Capture::default());
        socket.observer = Some(observations.clone());
        let cancel = parent.child_token();
        let shared = Arc::new(Shared {
            label: format!(
                "native TCP {:?} -> {:?}",
                socket.inner.local_addr().ok(),
                socket.inner.peer_addr().ok()
            ),
            state: Mutex::new(State {
                socket: Some(Socket::Tokio(socket)),
                closed: false,
                error: None,
                retry: None,
                unknown: None,
                closing: false,
                polling: false,
                metadata: None,
            }),
            cancel,
            observer: Mutex::new(observer),
            observations,
            wakes: IoRelays::new(),
        });
        (
            Self::from_shared(shared.clone()),
            ControlledTcpControl {
                shared,
                reported: false,
            },
        )
    }
    fn from_shared(shared: Arc<Shared>) -> Self {
        Self {
            read_cancelled: Box::pin(shared.cancel.clone().cancelled_owned()),
            write_cancelled: Box::pin(shared.cancel.clone().cancelled_owned()),
            read_waiter: shared.wakes.read.waiter(),
            write_waiter: shared.wakes.write.waiter(),
            shared,
        }
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.shared.local_addr()
    }
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.shared.peer_addr()
    }
    pub fn egress_interface(&self) -> Option<crate::EgressInterface> {
        self.shared.interface()
    }
}
fn closed() -> io::Error {
    // Permanent retirement must not use Interrupted, which I/O helpers retry.
    io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "TCP channel is cancelled or closed",
    )
}
impl AsyncRead for ControlledTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.read_waiter.publish(cx.waker());
        let waker = std::task::Waker::from(this.shared.wakes.read.clone());
        let mut native = Context::from_waker(&waker);
        if this.read_cancelled.as_mut().poll(&mut native).is_ready() {
            return Poll::Ready(Err(closed()));
        }
        this.shared
            .poll_io(|socket| socket.poll_read(&mut native, buffer))
    }
}
impl AsyncWrite for ControlledTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.write_waiter.publish(cx.waker());
        let waker = std::task::Waker::from(this.shared.wakes.write.clone());
        let mut native = Context::from_waker(&waker);
        if this.write_cancelled.as_mut().poll(&mut native).is_ready() {
            return Poll::Ready(Err(closed()));
        }
        this.shared
            .poll_io(|socket| socket.poll_write(&mut native, bytes))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.write_waiter.publish(cx.waker());
        let waker = std::task::Waker::from(this.shared.wakes.write.clone());
        let mut native = Context::from_waker(&waker);
        if this.write_cancelled.as_mut().poll(&mut native).is_ready() {
            return Poll::Ready(Err(closed()));
        }
        this.shared.poll_io(|socket| socket.poll_flush(&mut native))
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.write_waiter.publish(cx.waker());
        let waker = std::task::Waker::from(this.shared.wakes.write.clone());
        let mut native = Context::from_waker(&waker);
        if this.write_cancelled.as_mut().poll(&mut native).is_ready() {
            return Poll::Ready(Err(closed()));
        }
        this.shared
            .poll_io(|socket| socket.poll_shutdown(&mut native))
    }
}
impl Drop for ControlledTcpStream {
    fn drop(&mut self) {
        self.shared.cancel.cancel();
    }
}
impl TokioSocket {
    pub fn into_controlled(
        self,
        cancel: CancellationToken,
    ) -> (ControlledTcpStream, ControlledTcpControl) {
        ControlledTcpStream::new(self, cancel)
    }
}

#[cfg(test)]
#[path = "../tests/controlled_tcp/reentry/mod.rs"]
mod reentry_tests;
