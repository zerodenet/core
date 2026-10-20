//! One actual listener, a cancel-safe accept view, and explicit close ownership.
mod accepted;
mod control;
pub use accepted::{
    AcceptedTcp, AcceptedTcpClaim, AcceptedTcpEvidence, AcceptedTcpNativeClosed, AcceptedTcpStream,
    AcceptedTcpUsers,
};
mod native;
mod owner;
use crate::TokioSocket;
pub use control::ControlledTcpListenerControl;
pub use owner::ControlledTcpListenerOwner;
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
    raw: Option<std::net::TcpListener>,
    confirmed: usize,
    retired: bool,
    closed: bool,
    error: Option<Arc<io::Error>>,
}
struct Shared {
    accepted: Option<Arc<accepted::Pool>>,
    state: Mutex<State>,
    cancel: CancellationToken,
    label: String,
}
fn uncertain(message: &'static str) -> Arc<io::Error> {
    Arc::new(io::Error::other(message))
}
impl State {
    /// No await: the original is either explicitly closed, retained by a
    /// known-open Winsock failure, or permanently Unknown with its source.
    fn close_original(&mut self) {
        let result = if let Some(original) = self.raw.take() {
            native::close(original)
        } else if let Some(listener) = self.listener.take() {
            self.error = Some(uncertain(
                "Tokio listener deregistration did not return a native outcome",
            ));
            listener.into_std().and_then(native::close)
        } else {
            return;
        };
        match result {
            Ok(()) => {
                self.confirmed += 1;
                self.error = None;
            }
            Err(source) => self.error = Some(Arc::new(source)),
        }
    }
}
impl Shared {
    fn cancel_admission(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
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
    /// The installation seam distinguishes an actual admission revocation from
    /// a native address-query failure while holding the same ownership lock.
    pub(crate) fn activation_address(&self) -> Result<SocketAddr, crate::contract::ChannelError> {
        use crate::contract::{ChannelError, Impact, Reason, Stage};
        let state = self
            .shared
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.retired || self.shared.cancel.is_cancelled() || state.listener.is_none() {
            return Err(
                ChannelError::new(Stage::Prepare, Reason::Revoked, Impact::Component)
                    .with_source(retired()),
            );
        }
        state
            .listener
            .as_ref()
            .unwrap()
            .local_addr()
            .map_err(|source| {
                ChannelError::new(Stage::Prepare, Reason::Io, Impact::Component).with_source(source)
            })
    }

    pub fn new(
        listener: TcpListener,
        parent: CancellationToken,
    ) -> (Self, ControlledTcpListenerControl) {
        let cancel = parent.child_token();
        let shared = Arc::new(Shared {
            accepted: None,
            label: format!("native TCP listener {:?}", listener.local_addr().ok()),
            state: Mutex::new(State {
                listener: Some(listener),
                raw: None,
                confirmed: 0,
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
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .listener
            .as_ref()
            .ok_or_else(retired)?
            .local_addr()
    }

    /// No task is started. Dropping a pending accept leaves the same listener
    /// available to the next accept; `&mut self` prevents competing Tokio wakers.
    pub async fn accept(&mut self) -> io::Result<(TokioSocket, SocketAddr)> {
        if self.shared.accepted.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "tracked accepted TCP requires accept_owned",
            ));
        }
        poll_fn(|cx| {
            if self.cancelled.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(retired()));
            }
            let state = self
                .shared
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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
