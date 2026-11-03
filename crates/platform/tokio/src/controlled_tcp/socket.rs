use super::TokioSocket;
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub(super) use unix::NativeSocket;
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(super) use windows::NativeSocket;

pub(super) enum Socket {
    Tokio(TokioSocket),
    #[cfg(unix)]
    Unix(super::unix_connect::NativeUnix),
    #[cfg(any(unix, windows))]
    Native(NativeSocket),
}
impl Socket {
    pub(super) fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            #[cfg(unix)]
            Self::Unix(_) => Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "Unix endpoint is not an IP endpoint",
            )),
            Self::Tokio(socket) => socket.local_addr(),
            #[cfg(any(unix, windows))]
            Self::Native(socket) => socket.local_addr(),
        }
    }
    pub(super) fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self {
            #[cfg(unix)]
            Self::Unix(_) => Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "Unix endpoint is not an IP endpoint",
            )),
            Self::Tokio(socket) => socket.peer_addr(),
            #[cfg(any(unix, windows))]
            Self::Native(socket) => socket.peer_addr(),
        }
    }
    pub(super) fn egress_interface(&self) -> Option<&crate::EgressInterface> {
        match self {
            #[cfg(unix)]
            Self::Unix(_) => None,
            Self::Tokio(socket) => socket.egress_interface(),
            #[cfg(any(unix, windows))]
            Self::Native(socket) => socket.interface.as_ref(),
        }
    }
    pub(super) fn close(self) -> io::Result<()> {
        match self {
            #[cfg(unix)]
            Self::Unix(socket) => socket.close(),
            Self::Tokio(socket) => socket.close(),
            #[cfg(any(unix, windows))]
            Self::Native(socket) => socket.close(),
        }
    }
    pub(super) fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self {
            #[cfg(unix)]
            Self::Unix(socket) => socket.poll_read(cx, buffer),
            Self::Tokio(socket) => Pin::new(socket).poll_read(cx, buffer),
            #[cfg(any(unix, windows))]
            Self::Native(socket) => socket.poll_read(cx, buffer),
        }
    }
    pub(super) fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self {
            #[cfg(unix)]
            Self::Unix(socket) => socket.poll_write(cx, bytes),
            Self::Tokio(socket) => Pin::new(socket).poll_write(cx, bytes),
            #[cfg(any(unix, windows))]
            Self::Native(socket) => socket.poll_write(cx, bytes),
        }
    }
    pub(super) fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self {
            #[cfg(unix)]
            Self::Unix(_) => Poll::Ready(Ok(())),
            Self::Tokio(socket) => Pin::new(socket).poll_flush(cx),
            #[cfg(any(unix, windows))]
            Self::Native(_) => Poll::Ready(Ok(())),
        }
    }
    pub(super) fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self {
            #[cfg(unix)]
            Self::Unix(socket) => Poll::Ready(socket.shutdown()),
            Self::Tokio(socket) => Pin::new(socket).poll_shutdown(cx),
            #[cfg(any(unix, windows))]
            Self::Native(socket) => {
                #[cfg(unix)]
                {
                    Poll::Ready(socket.shutdown_write())
                }
                #[cfg(windows)]
                {
                    socket.poll_shutdown(cx)
                }
            }
        }
    }
}
