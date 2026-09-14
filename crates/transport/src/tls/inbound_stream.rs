use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use zero_platform_tokio::ClientStream;
use zero_traits::AsyncSocket;

enum Inner<IO> {
    Rustls(Box<super::switchable::SwitchableTlsStream<IO>>),
    OpenSsl(super::openssl::OpenSslTlsStream<IO>),
}

pub struct InboundTlsStream<IO = TcpStream> {
    inner: Inner<IO>,
}

impl InboundTlsStream {
    pub fn new(inner: TlsStream<super::TlsRecordBoundary<TcpStream>>) -> Self {
        Self::new_generic(inner)
    }
}

impl<IO> InboundTlsStream<IO> {
    pub fn negotiated_alpn(&self) -> Option<&[u8]> {
        match &self.inner {
            Inner::Rustls(inner) => inner.alpn_protocol(),
            Inner::OpenSsl(inner) => inner.alpn_protocol(),
        }
    }

    pub fn negotiated_server_name(&self) -> Option<&str> {
        match &self.inner {
            Inner::Rustls(inner) => inner.server_name(),
            Inner::OpenSsl(inner) => inner.server_name(),
        }
    }

    pub fn new_generic(inner: TlsStream<super::TlsRecordBoundary<IO>>) -> Self {
        Self {
            inner: Inner::Rustls(Box::new(super::switchable::SwitchableTlsStream::server(
                inner,
            ))),
        }
    }

    pub(super) fn new_openssl(inner: super::openssl::OpenSslTlsStream<IO>) -> Self {
        Self {
            inner: Inner::OpenSsl(inner),
        }
    }
}

impl<IO> ClientStream for InboundTlsStream<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "not available"))
    }
}

impl<IO> AsyncSocket for InboundTlsStream<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    type Error = io::Error;
    fn transport_bypass_control(&self) -> Option<zero_traits::TransportBypassControl> {
        match &self.inner {
            Inner::Rustls(inner) => inner.control(),
            Inner::OpenSsl(inner) => inner.control(),
        }
    }

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        AsyncReadExt::read(self, buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        AsyncWriteExt::write_all(self, buf).await?;
        AsyncWriteExt::flush(self).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        AsyncWriteExt::shutdown(self).await
    }
}

impl<IO> AsyncRead for InboundTlsStream<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            Inner::Rustls(inner) => Pin::new(inner).poll_read(cx, buf),
            Inner::OpenSsl(inner) => Pin::new(inner).poll_read(cx, buf),
        }
    }
}

impl<IO> AsyncWrite for InboundTlsStream<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().inner {
            Inner::Rustls(inner) => Pin::new(inner).poll_write(cx, buf),
            Inner::OpenSsl(inner) => Pin::new(inner).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            Inner::Rustls(inner) => Pin::new(inner).poll_flush(cx),
            Inner::OpenSsl(inner) => Pin::new(inner).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            Inner::Rustls(inner) => Pin::new(inner).poll_shutdown(cx),
            Inner::OpenSsl(inner) => Pin::new(inner).poll_shutdown(cx),
        }
    }
}
