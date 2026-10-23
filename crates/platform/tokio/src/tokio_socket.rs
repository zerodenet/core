//! Same socket facade over a native original or its unique accepted byte view.
use crate::AcceptedTcpStream;
use std::{
    fmt, io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};

pub(crate) enum Storage {
    Native(TcpStream),
    Accepted(Box<AcceptedTcpStream>),
}
impl fmt::Debug for Storage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Native(socket) => f.debug_tuple("Native").field(socket).finish(),
            Self::Accepted(_) => {
                f.write_str("Accepted(original control retained by listener owner)")
            }
        }
    }
}
impl Storage {
    /// Crate-private native-only operations cannot expose an accepted original.
    pub(crate) fn native(&self) -> io::Result<&TcpStream> {
        match self {
            Self::Native(socket) => Ok(socket),
            Self::Accepted(_) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "accepted original belongs to its persistent listener owner",
            )),
        }
    }
    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Self::Native(socket) => socket.local_addr(),
            Self::Accepted(io) => io.local_addr(),
        }
    }
    pub(crate) fn peer_addr(&self) -> io::Result<SocketAddr> {
        match self {
            Self::Native(socket) => socket.peer_addr(),
            Self::Accepted(io) => io.peer_addr(),
        }
    }
    pub(crate) async fn peek(&self, bytes: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Native(socket) => socket.peek(bytes).await,
            Self::Accepted(io) => io.peek(bytes).await,
        }
    }
}
impl AsyncRead for Storage {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Native(socket) => Pin::new(socket).poll_read(cx, buf),
            Self::Accepted(io) => Pin::new(io.as_mut()).poll_read(cx, buf),
        }
    }
}
impl AsyncWrite for Storage {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Native(socket) => Pin::new(socket).poll_write(cx, bytes),
            Self::Accepted(io) => Pin::new(io.as_mut()).poll_write(cx, bytes),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Native(socket) => Pin::new(socket).poll_flush(cx),
            Self::Accepted(io) => Pin::new(io.as_mut()).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Native(socket) => Pin::new(socket).poll_shutdown(cx),
            Self::Accepted(io) => Pin::new(io.as_mut()).poll_shutdown(cx),
        }
    }
}
