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
    waiter: Arc<super::WaitTarget>,
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
        let observations = Arc::new(super::observation::Capture::default());
        let shared = Arc::new(Shared {
            label: format!("native TCP {:?} -> {peer}", socket.local_addr().ok()),
            cancel: parent.child_token(),
            state: Mutex::new(State {
                socket: Some(Socket::Native(NativeSocket::new(
                    socket,
                    interface,
                    Some(observations.clone()),
                ))),
                closed: false,
                error: None,
                retry: None,
                unknown: None,
                closing: false,
                polling: false,
                metadata: None,
            }),
            observer: Mutex::new(observer),
            observations,
            wakes: super::IoRelays::new(),
        });
        // The same original native owner is installed before external code.
        // No native/state lock is held during the real boundary callback.
        let observer = shared
            .observer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(observer) = observer {
            observer.stream_boundary();
        }
        (
            Self {
                shared: shared.clone(),
                peer,
                interface_bound,
                last_local,
                started: false,
                transferred: false,
                failure: None,
                waiter: shared.wakes.write.waiter(),
            },
            ControlledTcpControl {
                shared,
                reported: false,
            },
        )
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.shared.local_addr()
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
        self.waiter.publish(cx.waker());
        let waker = std::task::Waker::from(self.shared.wakes.write.clone());
        let mut native = Context::from_waker(&waker);
        let result = {
            let Some(mut lease) = self.shared.lease() else {
                return Poll::Ready(Err(
                    self.error_without_lock("connect_socket", super::closed())
                ));
            };
            let Socket::Native(socket) = &mut *lease else {
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
            match socket.poll_connect(&mut native, self.peer) {
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
