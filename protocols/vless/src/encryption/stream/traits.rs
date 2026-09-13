use super::*;

impl<S: zero_platform_tokio::ClientStream> zero_platform_tokio::ClientStream
    for EncryptionStream<S>
{
    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.inner.local_addr()
    }
    fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.inner.peer_addr()
    }
}
impl<S: zero_core::InboundRecording> zero_core::InboundRecording for EncryptionStream<S> {
    type Stream = EncryptionStream<S::Stream>;
    fn into_unrecorded(self) -> (Self::Stream, u64, u64) {
        let mut read = 0;
        let mut written = 0;
        let stream = self.map_inner(|inner| {
            let (stream, r, w) = inner.into_unrecorded();
            read = r;
            written = w;
            stream
        });
        (stream, read, written)
    }
}

impl<S: AsyncRead + AsyncWrite + Send + Sync + Unpin> zero_traits::AsyncSocket
    for EncryptionStream<S>
{
    type Error = io::Error;
    fn transport_bypass_control(&self) -> Option<zero_traits::TransportBypassControl> {
        Some(self.control.clone())
    }
    async fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        tokio::io::AsyncReadExt::read(self, bytes).await
    }
    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(self, bytes).await?;
        tokio::io::AsyncWriteExt::flush(self).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        tokio::io::AsyncWriteExt::shutdown(self).await
    }
}

impl<S: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static> EncryptionStream<S> {
    pub fn into_relay(self) -> zero_platform_tokio::TcpRelayStream {
        let control = self.control.clone();
        zero_platform_tokio::TcpRelayStream::with_transport_bypass_control(self, control)
    }
}
