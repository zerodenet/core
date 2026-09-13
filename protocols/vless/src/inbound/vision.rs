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

    pub(crate) fn vision_after_response(stream: S, uuid: [u8; 16], testseed: [u32; 4]) -> Self {
        let mut stream = Self::vision(stream, uuid, testseed);
        if let Self::Vision {
            response_pending, ..
        } = &mut stream
        {
            *response_pending = false;
        }
        stream
    }

    pub(crate) fn vision(stream: S, uuid: [u8; 16], testseed: [u32; 4]) -> Self {
        let control = stream.transport_bypass_control();
        Self::Vision {
            stream: VisionStream::with_testseed(stream, uuid, control, testseed),
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

impl<S: zero_core::InboundRecording> zero_core::InboundRecording for VlessInboundTcpStream<S> {
    type Stream = VlessInboundTcpStream<S::Stream>;
    fn into_unrecorded(self) -> (Self::Stream, u64, u64) {
        match self {
            Self::Plain(inner) => {
                let (inner, read, written) = inner.into_unrecorded();
                (VlessInboundTcpStream::Plain(inner), read, written)
            }
            Self::Vision {
                stream,
                response_pending,
            } => {
                let mut traffic = (0, 0);
                let stream = stream.map_inner(|inner| {
                    let (inner, read, written) = inner.into_unrecorded();
                    traffic = (read, written);
                    inner
                });
                (
                    VlessInboundTcpStream::Vision {
                        stream,
                        response_pending,
                    },
                    traffic.0,
                    traffic.1,
                )
            }
        }
    }
}
impl<S> zero_platform_tokio::ClientStream for VlessInboundTcpStream<S>
where
    S: zero_platform_tokio::ClientStream + AsyncRead + AsyncWrite + Unpin + Sync,
{
    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        match self {
            Self::Plain(inner) => inner.local_addr(),
            Self::Vision { stream, .. } => stream.inner().local_addr(),
        }
    }
    fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        match self {
            Self::Plain(inner) => inner.peer_addr(),
            Self::Vision { stream, .. } => stream.inner().peer_addr(),
        }
    }
}
