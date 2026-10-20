//! Explicit close after successful Tokio deregistration, never a cloned handle.
use std::{io, net::TcpListener, sync::Mutex};
#[derive(Debug)]
struct CloseFailure {
    source: io::Error,
    retained: Mutex<Option<TcpListener>>,
}
impl std::fmt::Display for CloseFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(f)
    }
}
impl std::error::Error for CloseFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
pub(super) fn retryable(error: &io::Error) -> bool {
    error
        .get_ref()
        .and_then(|source| source.downcast_ref::<CloseFailure>())
        .is_some_and(|failure| {
            failure
                .retained
                .lock()
                .expect("listener close owner poisoned")
                .is_some()
        })
}
pub(super) fn retry(error: &io::Error) -> io::Result<bool> {
    let retained = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<CloseFailure>())
        .and_then(|failure| {
            failure
                .retained
                .lock()
                .expect("listener close owner poisoned")
                .take()
        });
    match retained {
        Some(listener) => close(listener).map(|()| true),
        None => Ok(false),
    }
}
#[cfg(unix)]
pub(super) fn close(listener: TcpListener) -> io::Result<()> {
    use std::os::fd::IntoRawFd;
    let descriptor = listener.into_raw_fd();
    // SAFETY: consume the original uniquely owned fd exactly once. Even EINTR
    // is ambiguous; never recreate/drop/retry a potentially reused descriptor.
    if unsafe { libc::close(descriptor) } == 0 {
        return Ok(());
    }
    let source = io::Error::last_os_error();
    Err(io::Error::new(
        source.kind(),
        CloseFailure {
            source,
            retained: Mutex::new(None),
        },
    ))
}
#[cfg(windows)]
pub(super) fn close(listener: TcpListener) -> io::Result<()> {
    use std::os::windows::io::{FromRawSocket, IntoRawSocket};
    use windows_sys::Win32::Networking::WinSock::{closesocket, WSAGetLastError, WSAEWOULDBLOCK};
    let socket = listener.into_raw_socket();
    // SAFETY: consume the original uniquely owned SOCKET, not a clone or CRT fd.
    if unsafe { closesocket(socket as _) } == 0 {
        return Ok(());
    }
    let code = unsafe { WSAGetLastError() };
    let source = io::Error::from_raw_os_error(code);
    // Winsock guarantees only WSAEWOULDBLOCK left the original SOCKET open.
    let retained =
        (code == WSAEWOULDBLOCK).then(|| unsafe { TcpListener::from_raw_socket(socket) });
    Err(io::Error::new(
        source.kind(),
        CloseFailure {
            source,
            retained: Mutex::new(retained),
        },
    ))
}

#[cfg(all(test, unix))]
#[path = "../../tests/controlled_listener/native_policy.rs"]
mod tests;
