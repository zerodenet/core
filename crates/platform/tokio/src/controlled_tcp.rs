//! Byte I/O and explicit native close share one original TCP descriptor.
#[cfg(any(unix, windows))]
pub(crate) mod connect;
mod control;
mod native;
mod socket;
use crate::TokioSocket;
#[cfg(any(unix, windows))]
pub use connect::PendingTcpConnect;
pub use control::ControlledTcpControl;
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

struct State {
    socket: Option<Socket>,
    closed: bool,
    error: Option<Arc<io::Error>>,
}
struct Shared {
    state: Mutex<State>,
    cancel: CancellationToken,
    label: String,
}

impl Shared {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        // A synchronous registration panic can poison this lock after its
        // consumed-unknown marker was installed. Keep that original state
        // available to explicit close instead of panicking during cleanup.
        self.state
            .lock()
            .unwrap_or_else(|source| source.into_inner())
    }
}

/// A unique byte-stream view. Its separate control must remain in a persistent
/// owner before this view is transferred to a cancelable protocol operation.
/// Dropping either view requests cancellation, never a successful close receipt.
pub struct ControlledTcpStream {
    shared: Arc<Shared>,
    read_cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
    write_cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
}
impl ControlledTcpStream {
    pub fn new(socket: TokioSocket, parent: CancellationToken) -> (Self, ControlledTcpControl) {
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
            }),
            cancel,
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
            shared,
        }
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.shared
            .state()
            .socket
            .as_ref()
            .ok_or_else(closed)?
            .local_addr()
    }
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.shared
            .state()
            .socket
            .as_ref()
            .ok_or_else(closed)?
            .peer_addr()
    }
    pub fn egress_interface(&self) -> Option<crate::EgressInterface> {
        self.shared
            .state()
            .socket
            .as_ref()
            .and_then(|socket| socket.egress_interface().cloned())
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
        if this.read_cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(closed()));
        }
        let mut state = this.shared.state();
        let Some(socket) = &mut state.socket else {
            return Poll::Ready(Err(closed()));
        };
        // Delegate to the original socket: its observer and actual readiness,
        // effective addresses, and selected interface remain intact.
        socket.poll_read(cx, buffer)
    }
}
impl AsyncWrite for ControlledTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.write_cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(closed()));
        }
        let mut state = this.shared.state();
        let Some(socket) = &mut state.socket else {
            return Poll::Ready(Err(closed()));
        };
        socket.poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.write_cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(closed()));
        }
        let mut state = this.shared.state();
        let Some(socket) = &mut state.socket else {
            return Poll::Ready(Err(closed()));
        };
        socket.poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.write_cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(closed()));
        }
        let mut state = this.shared.state();
        let Some(socket) = &mut state.socket else {
            return Poll::Ready(Err(closed()));
        };
        // Write half-close is not whole-channel cancellation or native close.
        socket.poll_shutdown(cx)
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
