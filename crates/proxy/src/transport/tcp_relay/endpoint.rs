use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::{attributed_error, TransportFailureOrigin};
use crate::transport::failure::is_local_network_error;

pub(super) struct RelayEndpoint<S> {
    stream: S,
    origin: TransportFailureOrigin,
}

impl<S> RelayEndpoint<S> {
    pub(super) fn new(stream: S, origin: TransportFailureOrigin) -> Self {
        Self { stream, origin }
    }

    fn error(&self, operation: &'static str, error: io::Error) -> io::Error {
        let origin =
            if self.origin == TransportFailureOrigin::Upstream && is_local_network_error(&error) {
                TransportFailureOrigin::LocalNetwork
            } else {
                self.origin
            };
        attributed_error(origin, operation, error)
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for RelayEndpoint<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream)
            .poll_read(cx, buf)
            .map_err(|error| self.error("TCP read failed", error))
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for RelayEndpoint<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream)
            .poll_write(cx, buf)
            .map_err(|error| self.error("TCP write failed", error))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream)
            .poll_flush(cx)
            .map_err(|error| self.error("TCP flush failed", error))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream)
            .poll_shutdown(cx)
            .map_err(|error| self.error("TCP shutdown failed", error))
    }
}
