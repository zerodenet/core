//! One explicit close of the original TCP fd/SOCKET, never a cloned descriptor.
use super::TokioSocket;
use std::{io, net::TcpStream, sync::Mutex};

impl TokioSocket {
    pub fn close(self) -> io::Result<()> {
        native_close(self.inner.into_std()?)
    }
    pub fn close_is_retryable(error: &io::Error) -> bool {
        error
            .get_ref()
            .and_then(|error| error.downcast_ref::<NativeCloseFailure>())
            .is_some_and(|failure| {
                failure
                    .retained
                    .lock()
                    .expect("TCP close owner poisoned")
                    .is_some()
            })
    }
    pub fn retry_close(error: &io::Error) -> io::Result<bool> {
        let retained = error
            .get_ref()
            .and_then(|error| error.downcast_ref::<NativeCloseFailure>())
            .and_then(|failure| {
                failure
                    .retained
                    .lock()
                    .expect("TCP close owner poisoned")
                    .take()
            });
        let Some(socket) = retained else {
            return Ok(false);
        };
        native_close(socket).map(|()| true)
    }
}
#[derive(Debug)]
struct NativeCloseFailure {
    source: io::Error,
    retained: Mutex<Option<TcpStream>>,
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
pub(super) fn native_close(socket: TcpStream) -> io::Result<()> {
    use std::os::fd::IntoRawFd;
    let descriptor = socket.into_raw_fd();
    // SAFETY: consume the original uniquely owned native descriptor once.
    if unsafe { libc::close(descriptor) } == 0 {
        return Ok(());
    }
    let source = io::Error::last_os_error();
    // Even EINTR must not recreate/drop/retry a potentially reused descriptor.
    Err(io::Error::new(
        source.kind(),
        NativeCloseFailure {
            source,
            retained: Mutex::new(None),
        },
    ))
}
#[cfg(windows)]
pub(super) fn native_close(socket: TcpStream) -> io::Result<()> {
    use std::os::windows::io::{FromRawSocket, IntoRawSocket};
    use windows_sys::Win32::Networking::WinSock::{closesocket, WSAGetLastError, WSAEWOULDBLOCK};
    let socket = socket.into_raw_socket();
    // SAFETY: the original uniquely owned SOCKET, not a CRT fd or clone.
    if unsafe { closesocket(socket as _) } == 0 {
        return Ok(());
    }
    let code = unsafe { WSAGetLastError() };
    let source = io::Error::from_raw_os_error(code);
    // Winsock guarantees WSAEWOULDBLOCK left this SOCKET open. Other error
    // outcomes are unconfirmed; never reconstruct a potentially reused handle.
    let retained = (code == WSAEWOULDBLOCK).then(|| unsafe { TcpStream::from_raw_socket(socket) });
    Err(io::Error::new(
        source.kind(),
        NativeCloseFailure {
            source,
            retained: Mutex::new(retained),
        },
    ))
}

#[cfg(all(test, unix))]
#[path = "../../tests/controlled_tcp/policy.rs"]
mod tests;
