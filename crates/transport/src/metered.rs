use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_traits::AsyncSocket;

use crate::stream::{ClientStream, RecordingStream};
use crate::StreamTraffic;

#[derive(Debug)]
pub struct MeteredStream<S> {
    inner: S,
    traffic: StreamTraffic,
}

impl<S> MeteredStream<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            traffic: StreamTraffic::default(),
        }
    }

    pub fn drain_traffic(&mut self) -> StreamTraffic {
        let traffic = self.traffic;
        self.traffic = StreamTraffic::default();
        traffic
    }

    pub fn into_inner(self) -> S {
        self.inner
    }
}

impl<S> MeteredStream<RecordingStream<S>> {
    pub fn into_unrecorded_inner(self) -> S {
        let (inner, _) = self.inner.into_parts();
        inner
    }
}

impl<S> zero_core::InboundFallbackCapture for MeteredStream<RecordingStream<S>> {
    type Stream = S;

    fn into_fallback_replay_parts(self) -> (Self::Stream, Vec<u8>) {
        self.into_inner().into_parts()
    }
}

impl<S> AsyncSocket for MeteredStream<S>
where
    S: AsyncSocket,
{
    type Error = S::Error;

    fn transport_bypass_control(&self) -> Option<zero_traits::TransportBypassControl> {
        self.inner.transport_bypass_control()
    }

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        let read = self.inner.read(buf).await?;
        self.traffic.read_bytes = self.traffic.read_bytes.saturating_add(read as u64);
        Ok(read)
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.inner.write_all(buf).await?;
        self.traffic.written_bytes = self.traffic.written_bytes.saturating_add(buf.len() as u64);
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        self.inner.shutdown().await
    }
}

impl<S> ClientStream for MeteredStream<S>
where
    S: ClientStream,
{
    fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.inner.local_addr()
    }

    fn peer_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.inner.peer_addr()
    }
}

impl<S> AsyncRead for MeteredStream<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let read = buf.filled().len().saturating_sub(before);
                self.traffic.read_bytes = self.traffic.read_bytes.saturating_add(read as u64);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<S> AsyncWrite for MeteredStream<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                self.traffic.written_bytes =
                    self.traffic.written_bytes.saturating_add(written as u64);
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
