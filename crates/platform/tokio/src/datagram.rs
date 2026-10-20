//! Native datagram polling for owners that multiplex one receive task.
use std::{
    io,
    net::SocketAddr,
    task::{Context, Poll},
};
use tokio::io::ReadBuf;
mod fixed;

impl crate::TokioDatagramSocket {
    /// One uniquely owned raw UDP carrier to an already resolved peer. The
    /// caller still owns resolution, hard dial policy and prefix validation.
    pub fn into_fixed_datagram_session(
        self,
        association: zero_core::component::AssociationId,
        peer: SocketAddr,
        cancel: tokio_util::sync::CancellationToken,
    ) -> crate::contract::datagram::BoxDatagramSession {
        fixed::session(self, association, peer, cancel)
    }
    /// Poll the actual UDP submission without adding a per-packet future/task.
    pub fn poll_send_to_addr(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        peer: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        let result = self.inner.poll_send_to(cx, bytes, peer);
        if let Poll::Ready(result) = &result {
            if let Some(observer) = &self.observer {
                match result {
                    Ok(size) => observer.sent_datagram(*size),
                    Err(_) => observer.error(),
                }
            }
        }
        result
    }

    /// Consume the uniquely owned native socket and perform the OS close once.
    /// An error is an unconfirmed outcome: callers must not retry a possibly
    /// closed/reused descriptor or turn it into a successful close report.
    pub fn close(self) -> io::Result<()> {
        native_close(self.inner.into_std()?)
    }

    /// Retry only a close error that explicitly retains a known-open SOCKET.
    /// Unix/ambiguous outcomes never reconstruct or retry a descriptor.
    pub fn retry_close(error: &io::Error) -> io::Result<bool> {
        let retained = error
            .get_ref()
            .and_then(|source| source.downcast_ref::<NativeCloseFailure>())
            .and_then(|failure| {
                failure
                    .retained
                    .lock()
                    .expect("native close owner poisoned")
                    .take()
            });
        let Some(socket) = retained else {
            return Ok(false);
        };
        native_close(socket).map(|()| true)
    }

    pub fn close_is_retryable(error: &io::Error) -> bool {
        error
            .get_ref()
            .and_then(|source| source.downcast_ref::<NativeCloseFailure>())
            .is_some_and(|failure| {
                failure
                    .retained
                    .lock()
                    .expect("native close owner poisoned")
                    .is_some()
            })
    }
    /// Receive into caller-owned storage without constructing a waiting task.
    ///
    /// As with Tokio's native poll API, only the most recent receive waker is
    /// retained. The caller must serialize receive polling for this socket.
    /// Pending readiness is not an I/O error or a received datagram.
    pub fn poll_recv_from_addr(
        &self,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddr)>> {
        let mut buffer = ReadBuf::new(bytes);
        match self.inner.poll_recv_from(cx, &mut buffer) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                if let Some(observer) = &self.observer {
                    match &result {
                        Ok(_) => observer.received_datagram(buffer.filled().len()),
                        Err(_) => observer.error(),
                    }
                }
                Poll::Ready(result.map(|peer| (buffer.filled().len(), peer)))
            }
        }
    }
}

#[derive(Debug)]
struct NativeCloseFailure {
    source: io::Error,
    retained: std::sync::Mutex<Option<std::net::UdpSocket>>,
}
impl std::fmt::Display for NativeCloseFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}
impl std::error::Error for NativeCloseFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[cfg(unix)]
fn native_close(socket: std::net::UdpSocket) -> io::Result<()> {
    use std::os::fd::IntoRawFd;
    let descriptor = socket.into_raw_fd();
    // Ownership was consumed; even EINTR must not recreate/drop/retry this fd.
    // SAFETY: this is the original uniquely owned socket descriptor.
    if unsafe { libc::close(descriptor) } == 0 {
        Ok(())
    } else {
        let source = io::Error::last_os_error();
        Err(io::Error::new(
            source.kind(),
            NativeCloseFailure {
                source,
                retained: std::sync::Mutex::new(None),
            },
        ))
    }
}

#[cfg(windows)]
fn native_close(socket: std::net::UdpSocket) -> io::Result<()> {
    use std::os::windows::io::{FromRawSocket, IntoRawSocket};
    use windows_sys::Win32::Networking::WinSock::{closesocket, WSAGetLastError, WSAEWOULDBLOCK};
    let socket = socket.into_raw_socket();
    // SAFETY: this is the original uniquely owned Windows SOCKET, not a CRT fd.
    if unsafe { closesocket(socket as _) } == 0 {
        Ok(())
    } else {
        let code = unsafe { WSAGetLastError() };
        let source = io::Error::from_raw_os_error(code);
        // Winsock documents WSAEWOULDBLOCK as not closed. Retain that one
        // known-open handle; ambiguous errors must not recreate a reused one.
        let retained = (code == WSAEWOULDBLOCK)
            .then(|| unsafe { std::net::UdpSocket::from_raw_socket(socket) });
        Err(io::Error::new(
            source.kind(),
            NativeCloseFailure {
                source,
                retained: std::sync::Mutex::new(retained),
            },
        ))
    }
}
