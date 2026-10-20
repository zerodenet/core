//! Native pre-connect ownership. No task or connect future owns the descriptor.
use super::{
    socket::{NativeSocket, Socket},
    ControlledTcpControl, ControlledTcpStream, Shared, State,
};
use crate::{EgressInterface, TcpConnectError};
use std::{
    future::poll_fn,
    io,
    net::{SocketAddr, TcpStream},
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use zero_traits::IoObserver;

/// Native TCP owner prepared before the first connect await.
///
/// Keep the returned control in a persistent resource owner. Cancellation,
/// timeout, or dropping `connect` preserves its original socket until explicit
/// `wait_closed`. This primitive is not wired into production dialing yet.
/// Windows registration consumes its input even on failure. Such a failure
/// retains the original source and descriptor identity as permanent unknown;
/// it never reconstructs a handle or returns a successful close receipt.
pub struct PendingTcpConnect {
    shared: Arc<Shared>,
    peer: SocketAddr,
    interface_bound: bool,
    last_local: Option<SocketAddr>,
    started: bool,
    transferred: bool,
    failure: Option<Arc<TcpConnectError>>,
}
impl PendingTcpConnect {
    pub(crate) fn from_socket(
        socket: TcpStream,
        peer: SocketAddr,
        interface: Option<EgressInterface>,
        parent: CancellationToken,
        observer: Option<Arc<dyn IoObserver>>,
    ) -> (Self, ControlledTcpControl) {
        let interface_bound = interface.is_some();
        let last_local = socket.local_addr().ok();
        let shared = Arc::new(Shared {
            label: format!("native TCP {:?} -> {peer}", socket.local_addr().ok()),
            cancel: parent.child_token(),
            state: Mutex::new(State {
                socket: Some(Socket::Native(NativeSocket::new(
                    socket, interface, observer,
                ))),
                closed: false,
                error: None,
            }),
        });
        (
            Self {
                shared: shared.clone(),
                peer,
                interface_bound,
                last_local,
                started: false,
                transferred: false,
                failure: None,
            },
            ControlledTcpControl {
                shared,
                reported: false,
            },
        )
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.shared
            .state()
            .socket
            .as_ref()
            .ok_or_else(super::closed)?
            .local_addr()
    }
    fn error(&self, stage: &'static str, error: io::Error) -> Arc<TcpConnectError> {
        let state = self.shared.state();
        Arc::new(TcpConnectError {
            stage,
            interface_bound: self.interface_bound,
            local_addr: state
                .socket
                .as_ref()
                .and_then(|socket| socket.local_addr().ok())
                .or(self.last_local),
            error,
        })
    }
    /// One connect attempt. A dropped attempt is terminal; retained native I/O
    /// must be explicitly closed rather than retried or reconstructed.
    pub async fn connect(
        &mut self,
        deadline: Instant,
    ) -> Result<ControlledTcpStream, Arc<TcpConnectError>> {
        if let Some(source) = &self.failure {
            return Err(source.clone());
        }
        if self.transferred {
            return Err(self.error(
                "connect_socket",
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "TCP stream already transferred",
                ),
            ));
        }
        let cancel = self.shared.cancel.clone();
        let mut attempt = Attempt {
            owner: self,
            armed: true,
        };
        let result = if Instant::now() >= deadline {
            Err(attempt.owner.error(
                "connect_socket",
                io::Error::new(io::ErrorKind::TimedOut, "TCP connect deadline expired"),
            ))
        } else {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(attempt.owner.error("connect_socket", super::closed())),
                result = tokio::time::timeout_at(deadline, poll_fn(|cx| attempt.owner.poll_connect(cx))) => match result {
                    Ok(result) => result,
                    Err(_) => Err(attempt.owner.error("connect_socket", io::Error::new(io::ErrorKind::TimedOut, "TCP connect deadline expired"))),
                }
            }
        };
        attempt.armed = false;
        match result {
            Ok(()) => {
                attempt.owner.transferred = true;
                Ok(ControlledTcpStream::from_shared(
                    attempt.owner.shared.clone(),
                ))
            }
            Err(source) => {
                attempt.owner.failure = Some(source.clone());
                attempt.owner.shared.cancel.cancel();
                Err(source)
            }
        }
    }
    fn poll_connect(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Arc<TcpConnectError>>> {
        let result = {
            let mut state = self.shared.state();
            let Some(Socket::Native(socket)) = state.socket.as_mut() else {
                return Poll::Ready(Err(
                    self.error_without_lock("connect_socket", super::closed())
                ));
            };
            if !self.started {
                self.started = true;
                if let Err((stage, source)) = socket.begin_connect(self.peer) {
                    self.last_local = socket.observed_local_addr().or(self.last_local);
                    return Poll::Ready(Err(Arc::new(TcpConnectError {
                        stage,
                        interface_bound: socket.interface.is_some(),
                        local_addr: self.last_local,
                        error: source,
                    })));
                }
            }
            self.last_local = socket.local_addr().ok().or(self.last_local);
            match socket.poll_connect(cx, self.peer) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(source)) => Err(("connect_socket", source)),
                Poll::Ready(Ok(())) => socket
                    .set_nodelay()
                    .map_err(|source| ("configure_socket", source)),
            }
        };
        Poll::Ready(result.map_err(|(stage, source)| self.error(stage, source)))
    }
    fn error_without_lock(&self, stage: &'static str, error: io::Error) -> Arc<TcpConnectError> {
        Arc::new(TcpConnectError {
            stage,
            interface_bound: self.interface_bound,
            local_addr: self.last_local,
            error,
        })
    }
}
struct Attempt<'a> {
    owner: &'a mut PendingTcpConnect,
    armed: bool,
}
impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.owner.failure = Some(self.owner.error("connect_socket", super::closed()));
            self.owner.shared.cancel.cancel();
        }
    }
}

#[cfg(all(test, unix))]
#[path = "../../tests/controlled_tcp/connect_pending.rs"]
mod tests;

#[cfg(all(test, windows))]
#[path = "../../tests/controlled_tcp/connect_windows.rs"]
mod windows_tests;
