use super::*;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
/// Unique same-original view. Drop delegates logical cancellation only. Neither
/// Drop nor a split half disappearing can declare protocol tasks/users joined.
pub struct AcceptedTcpStream {
    pub(super) io: ControlledTcpStream,
    pub(super) _usage: Option<graph::ViewUse>,
}
impl AcceptedTcpStream {
    pub(crate) async fn peek(&self, bytes: &mut [u8]) -> io::Result<usize> {
        self.io.peek_accepted(bytes).await
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.local_addr()
    }
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.io.peer_addr()
    }
}
impl AsyncRead for AcceptedTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, buf)
    }
}
impl AsyncWrite for AcceptedTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }
    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}
impl zero_traits::AsyncSocket for AcceptedTcpStream {
    type Error = io::Error;
    async fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        tokio::io::AsyncReadExt::read(self, bytes).await
    }
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(self, bytes).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        tokio::io::AsyncWriteExt::shutdown(self).await
    }
}
impl crate::ClientStream for AcceptedTcpStream {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.local_addr()
    }
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.peer_addr()
    }
}
