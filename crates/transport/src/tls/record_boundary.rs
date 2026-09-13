//! Keep the handshake's rustls deframer on a record boundary before handoff.
use std::{
    io,
    pin::Pin,
    task::{ready, Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_traits::TransportBypassControl;

pub struct TlsRecordBoundary<S> {
    socket: S,
    header: [u8; 5],
    header_read: usize,
    remaining: usize,
    control: Option<TransportBypassControl>,
}
impl<S> TlsRecordBoundary<S> {
    pub(super) fn new(socket: S) -> Self {
        Self::with_control_option(socket, None)
    }
    pub(super) fn with_control(socket: S, control: TransportBypassControl) -> Self {
        Self::with_control_option(socket, Some(control))
    }
    fn with_control_option(socket: S, control: Option<TransportBypassControl>) -> Self {
        Self {
            socket,
            header: [0; 5],
            header_read: 0,
            remaining: 0,
            control,
        }
    }
    pub(super) fn at_record_boundary(&self) -> bool {
        self.header_read == 0 && self.remaining == 0
    }
    pub(super) fn into_inner(self) -> S {
        // rustls processes handshake messages only after authenticating a whole
        // record. This wrapper prevents a read from including any next record.
        debug_assert_eq!(self.header_read, 0);
        debug_assert_eq!(self.remaining, 0);
        self.socket
    }
}
impl<S: AsyncRead + Unpin> TlsRecordBoundary<S> {
    pub(super) fn raw_poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if !this.at_record_boundary() {
            return Poll::Ready(Err(io::Error::other("TLS handoff within a record")));
        }
        Pin::new(&mut this.socket).poll_read(cx, output)
    }
}
impl<S: AsyncWrite + Unpin> TlsRecordBoundary<S> {
    pub(super) fn raw_poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().socket).poll_write(cx, bytes)
    }
    pub(super) fn raw_poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().socket).poll_flush(cx)
    }
    pub(super) fn raw_poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().socket).poll_shutdown(cx)
    }
}
impl<S: AsyncRead + Unpin> AsyncRead for TlsRecordBoundary<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this
            .control
            .as_ref()
            .is_some_and(|control| control.read_bypass_requested())
        {
            return Poll::Ready(Err(io::Error::other(
                "OpenSSL attempted encrypted input after read bypass",
            )));
        }
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let limit = if this.header_read < 5 {
            5 - this.header_read
        } else {
            this.remaining
        };
        let mut buffer = [0u8; 4096];
        let size = limit.min(output.remaining()).min(buffer.len());
        let mut bounded = ReadBuf::new(&mut buffer[..size]);
        ready!(Pin::new(&mut this.socket).poll_read(cx, &mut bounded))?;
        let size = bounded.filled().len();
        output.put_slice(bounded.filled());
        if size == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.header_read < 5 {
            this.header[this.header_read..this.header_read + size].copy_from_slice(&buffer[..size]);
            this.header_read += size;
            if this.header_read == 5 {
                this.remaining = u16::from_be_bytes([this.header[3], this.header[4]]) as usize;
                if this.remaining > 18432 {
                    return Poll::Ready(Err(io::Error::other("TLS record exceeds limit")));
                }
            }
        } else {
            this.remaining -= size;
        }
        if this.header_read == 5 && this.remaining == 0 {
            this.header_read = 0;
        }
        Poll::Ready(Ok(()))
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for TlsRecordBoundary<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this
            .control
            .as_ref()
            .is_some_and(|control| control.write_bypass_requested())
        {
            return Poll::Ready(Err(io::Error::other(
                "OpenSSL attempted encrypted output after write bypass",
            )));
        }
        Pin::new(&mut this.socket).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().socket).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().socket).poll_shutdown(cx)
    }
}
pub(super) async fn accept_rustls_handshake<S: AsyncRead + AsyncWrite + Unpin>(
    acceptor: &tokio_rustls::TlsAcceptor,
    socket: S,
) -> io::Result<tokio_rustls::server::TlsStream<TlsRecordBoundary<S>>> {
    acceptor.accept(TlsRecordBoundary::new(socket)).await
}
