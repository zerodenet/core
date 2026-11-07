//! The exact returned view signals only after its original I/O has dropped.
use super::*;
impl InboundTargetNativeOwner {
    pub fn wrap_stream(&self, stream: TcpRelayStream) -> Result<TcpRelayStream, ChannelError> {
        self.0.check()?;
        if self.0.view_issued.swap(true, Ordering::AcqRel) {
            return Err(failure(Stage::Prepare, Reason::InvalidConfig));
        }
        Ok(TcpRelayStream::from_client(View {
            stream: Some(stream),
            core: self.0.clone(),
        }))
    }
}
struct View {
    stream: Option<TcpRelayStream>,
    core: Arc<Core>,
}
impl Drop for View {
    fn drop(&mut self) {
        let stream = self.stream.take();
        let returned = Returned(self.core.returned.clone());
        drop(stream);
        drop(returned);
    }
}
impl AsyncRead for View {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().unwrap()).poll_read(cx, buffer)
    }
}
impl AsyncWrite for View {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.stream.as_mut().unwrap()).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.stream.as_mut().unwrap()).poll_shutdown(cx)
    }
}
impl crate::ClientStream for View {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream.as_ref().unwrap().local_addr()
    }
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream.as_ref().unwrap().peer_addr()
    }
}
impl zero_traits::AsyncSocket for View {
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

struct Returned(CancellationToken);
impl Drop for Returned {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
