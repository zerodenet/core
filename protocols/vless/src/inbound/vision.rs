use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_traits::{AsyncSocket, TransportBypassControl};

use crate::vision::VisionStream;

pub enum VlessInboundTcpStream<S> {
    Plain(S),
    Vision {
        stream: VisionStream<S>,
        response_pending: bool,
    },
}

impl<S> VlessInboundTcpStream<S>
where
    S: AsyncSocket,
{
    pub(crate) fn plain(stream: S) -> Self {
        Self::Plain(stream)
    }

    pub(crate) fn vision(stream: S, uuid: [u8; 16]) -> Self {
        let control = stream.transport_bypass_control();
        Self::Vision {
            stream: VisionStream::new(stream, uuid, control),
            response_pending: true,
        }
    }
}

impl<S> AsyncRead for VlessInboundTcpStream<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Vision { stream, .. } => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl<S> AsyncWrite for VlessInboundTcpStream<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Vision {
                response_pending: true,
                ..
            } => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "VLESS Vision response header was not sent before relay data",
            ))),
            Self::Vision { stream, .. } => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Vision { stream, .. } => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Vision { stream, .. } => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

impl<S> AsyncSocket for VlessInboundTcpStream<S>
where
    S: AsyncSocket<Error = io::Error> + AsyncRead + AsyncWrite + Send + Sync + Unpin,
{
    type Error = io::Error;

    fn transport_bypass_control(&self) -> Option<TransportBypassControl> {
        match self {
            Self::Plain(stream) => stream.transport_bypass_control(),
            Self::Vision { stream, .. } => stream.inner().transport_bypass_control(),
        }
    }

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        AsyncReadExt::read(self, buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        if let Self::Vision {
            stream,
            response_pending,
        } = self
        {
            if *response_pending {
                if buf != [crate::shared::VLESS_VERSION, 0x00] {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "VLESS Vision requires the response header before relay data",
                    ));
                }
                stream.inner_mut().write_all(buf).await?;
                *response_pending = false;
                return Ok(());
            }
        }
        AsyncWriteExt::write_all(self, buf).await?;
        AsyncWriteExt::flush(self).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        AsyncWriteExt::shutdown(self).await
    }
}
