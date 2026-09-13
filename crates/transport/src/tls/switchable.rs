//! TLS record-boundary handoff for protocols with an authenticated raw transition.
//! Reads never prefetch the next encrypted record into rustls before delivering
//! current plaintext, so a caller can switch without losing coalesced wire bytes.
use std::{
    io::{self, Read, Write},
    pin::Pin,
    task::{ready, Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use zero_traits::TransportBypassControl;

pub(super) struct SwitchableTlsStream<S> {
    socket: S,
    connection: rustls::Connection,
    control: Option<TransportBypassControl>,
    input: Vec<u8>,
    needed: usize,
    output: Vec<u8>,
    written: usize,
    failed: bool,
    closing: bool,
}
impl<S> SwitchableTlsStream<S> {
    fn new(socket: S, mut connection: rustls::Connection) -> Self {
        connection.set_buffer_limit(Some(64 * 1024));
        let control = (connection.protocol_version() == Some(rustls::ProtocolVersion::TLSv1_3))
            .then(TransportBypassControl::default);
        Self {
            socket,
            connection,
            control,
            input: Vec::new(),
            needed: 5,
            output: Vec::new(),
            written: 0,
            failed: false,
            closing: false,
        }
    }
    pub(super) fn server(
        stream: tokio_rustls::server::TlsStream<super::TlsRecordBoundary<S>>,
    ) -> Self {
        let (socket, connection) = stream.into_inner();
        Self::new(socket.into_inner(), rustls::Connection::Server(connection))
    }
    pub(super) fn client(
        stream: tokio_rustls::client::TlsStream<super::TlsRecordBoundary<S>>,
    ) -> Self {
        let (socket, connection) = stream.into_inner();
        Self::new(socket.into_inner(), rustls::Connection::Client(connection))
    }
    pub(super) fn alpn_protocol(&self) -> Option<&[u8]> {
        self.connection.alpn_protocol()
    }
    pub(super) fn server_name(&self) -> Option<&str> {
        match &self.connection {
            rustls::Connection::Server(connection) => connection.server_name(),
            rustls::Connection::Client(_) => None,
        }
    }
    pub(super) fn control(&self) -> Option<TransportBypassControl> {
        self.control.clone()
    }
    fn raw_read(&self) -> bool {
        self.control
            .as_ref()
            .is_some_and(|c| c.read_bypass_requested())
    }
    fn raw_write(&self) -> bool {
        self.control
            .as_ref()
            .is_some_and(|c| c.write_bypass_requested())
    }
}
impl<S: AsyncWrite + Unpin> SwitchableTlsStream<S> {
    fn drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            while self.written < self.output.len() {
                let size = ready!(
                    Pin::new(&mut self.socket).poll_write(cx, &self.output[self.written..])
                )?;
                if size == 0 {
                    return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
                }
                self.written += size;
            }
            self.output.clear();
            self.written = 0;
            if !self.connection.wants_write() {
                return Poll::Ready(Ok(()));
            }
            self.connection.write_tls(&mut self.output)?;
        }
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin> SwitchableTlsStream<S> {
    fn receive(
        &mut self,
        cx: &mut Context<'_>,
        destination: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            match self
                .connection
                .reader()
                .read(destination.initialize_unfilled())
            {
                Ok(n) if n > 0 => {
                    destination.advance(n);
                    return Poll::Ready(Ok(()));
                }
                Ok(_) => return Poll::Ready(Ok(())),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                Err(error) => return Poll::Ready(Err(error)),
            }
            if self.raw_read() {
                if !self.input.is_empty() {
                    return Poll::Ready(Err(io::Error::other("TLS handoff within a record")));
                }
                return Pin::new(&mut self.socket).poll_read(cx, destination);
            }
            ready!(self.drain(cx))?;
            ready!(Pin::new(&mut self.socket).poll_flush(cx))?;
            while self.input.len() < self.needed {
                let mut bytes = [0; 4096];
                let length = bytes.len().min(self.needed - self.input.len());
                let mut buf = ReadBuf::new(&mut bytes[..length]);
                ready!(Pin::new(&mut self.socket).poll_read(cx, &mut buf))?;
                if buf.filled().is_empty() {
                    return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
                }
                self.input.extend_from_slice(buf.filled());
            }
            if self.needed == 5 {
                let length = usize::from(u16::from_be_bytes([self.input[3], self.input[4]]));
                if length > 18432 {
                    return Poll::Ready(Err(io::Error::other("TLS record exceeds limit")));
                }
                self.needed = 5 + length;
                if length > 0 {
                    continue;
                }
            }
            let mut record = io::Cursor::new(&self.input);
            while (record.position() as usize) < self.input.len() {
                // rustls may accept only part of a complete record into its
                // deframer. Retain and feed every byte before reading the next.
                if self.connection.read_tls(&mut record)? == 0 {
                    return Poll::Ready(Err(io::Error::other("TLS record input made no progress")));
                }
                self.connection
                    .process_new_packets()
                    .map_err(io::Error::other)?;
            }
            self.input.clear();
            self.needed = 5;
        }
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for SwitchableTlsStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        destination: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if destination.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if this.failed {
            return Poll::Ready(Err(io::Error::other("TLS stream failed")));
        }
        let result = ready!(this.receive(cx, destination));
        if result.is_err() {
            this.failed = true;
        }
        Poll::Ready(result)
    }
}
impl<S: AsyncWrite + Unpin> AsyncWrite for SwitchableTlsStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.failed || this.closing {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        ready!(this.drain(cx))?;
        if this.raw_write() {
            return Pin::new(&mut this.socket).poll_write(cx, bytes);
        }
        Poll::Ready(this.connection.writer().write(bytes))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.drain(cx))?;
        Pin::new(&mut this.socket).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if !this.closing {
            this.closing = true;
            if !this.raw_write() {
                this.connection.send_close_notify();
            }
        }
        ready!(this.drain(cx))?;
        Pin::new(&mut this.socket).poll_shutdown(cx)
    }
}
