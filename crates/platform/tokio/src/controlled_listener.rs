//! One actual listener, a cancel-safe accept view, and explicit close ownership.
mod control;
mod native;
use crate::TokioSocket;
pub use control::ControlledTcpListenerControl;
use std::{
    future::{poll_fn, Future},
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::Poll,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

struct State {
    listener: Option<TcpListener>,
    retired: bool,
    closed: bool,
    error: Option<Arc<io::Error>>,
}
struct Shared {
    state: Mutex<State>,
    cancel: CancellationToken,
    label: String,
}
impl Shared {
    fn cancel_admission(&self) {
        self.state
            .lock()
            .expect("TCP listener owner poisoned")
            .retired = true;
        // Wake callers after releasing the ownership lock.
        self.cancel.cancel();
    }
}

/// The unique accept view. Keep its separate control in a persistent owner
/// before transferring this view. Drop/cancel retires admission, not native close.
/// Accepted sockets belong to the caller and require their own lifecycle owner.
pub struct ControlledTcpListener {
    shared: Arc<Shared>,
    cancelled: Pin<Box<dyn Future<Output = ()> + Send>>,
}
impl ControlledTcpListener {
    pub fn new(
        listener: TcpListener,
        parent: CancellationToken,
    ) -> (Self, ControlledTcpListenerControl) {
        let cancel = parent.child_token();
        let shared = Arc::new(Shared {
            label: format!("native TCP listener {:?}", listener.local_addr().ok()),
            state: Mutex::new(State {
                listener: Some(listener),
                retired: false,
                closed: false,
                error: None,
            }),
            cancel,
        });
        (
            Self {
                cancelled: Box::pin(shared.cancel.clone().cancelled_owned()),
                shared: shared.clone(),
            },
            ControlledTcpListenerControl {
                shared,
                reported: false,
            },
        )
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.shared
            .state
            .lock()
            .expect("TCP listener owner poisoned")
            .listener
            .as_ref()
            .ok_or_else(retired)?
            .local_addr()
    }

    /// No task is started. Dropping a pending accept leaves the same listener
    /// available to the next accept; `&mut self` prevents competing Tokio wakers.
    pub async fn accept(&mut self) -> io::Result<(TokioSocket, SocketAddr)> {
        poll_fn(|cx| {
            if self.cancelled.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(retired()));
            }
            let state = self
                .shared
                .state
                .lock()
                .expect("TCP listener owner poisoned");
            if state.retired || self.shared.cancel.is_cancelled() {
                return Poll::Ready(Err(retired()));
            }
            let Some(listener) = &state.listener else {
                return Poll::Ready(Err(retired()));
            };
            match listener.poll_accept(cx) {
                Poll::Ready(Ok((socket, remote))) => {
                    // Match the ordinary platform listener's accepted-stream policy.
                    if let Err(source) = socket.set_nodelay(true) {
                        return Poll::Ready(Err(source));
                    }
                    Poll::Ready(Ok((TokioSocket::new(socket), remote)))
                }
                Poll::Ready(Err(source)) => Poll::Ready(Err(source)),
                Poll::Pending => Poll::Pending,
            }
        })
        .await
    }
}
fn retired() -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionAborted,
        "TCP listener is cancelled or closed",
    )
}
impl Drop for ControlledTcpListener {
    fn drop(&mut self) {
        self.shared.cancel_admission();
    }
}

#[cfg(test)]
#[path = "../tests/controlled_listener/mod.rs"]
mod tests;
