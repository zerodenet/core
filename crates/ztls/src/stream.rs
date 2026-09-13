//! Async wrapper around Tls13Connection for tokio integration.
//!
//! Concrete sockets and relay streams share bounded, cancellation-safe async I/O.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

use super::handshake::{Tls13Config, Tls13Connection};

/// An async TLS 1.3 stream using our custom ClientHello handshake.
///
/// Generic over the inner stream type `S`. Implements `AsyncRead + AsyncWrite`.
///
/// # Construction
///
/// - `Tls13Stream::connect(tcp_stream, config)` — fresh TCP sockets.
/// - `Tls13Stream::connect_async(stream, config)` — generic async handshake for
///   any `AsyncRead + AsyncWrite + Unpin + Send + 'static` stream (relay streams,
///   trait objects, etc.).
pub struct Tls13Stream<S> {
    inner: S,
    conn: Tls13Connection,
    closing: bool,
    bypass: Option<zero_traits::TransportBypassControl>,
}

/// Fresh sockets share the cancellation-safe asynchronous handshake path.
impl Tls13Stream<TcpStream> {
    pub async fn connect(stream: TcpStream, config: Tls13Config) -> io::Result<Self> {
        Self::connect_async(stream, config).await
    }
}

/// Generic constructor for any AsyncRead + AsyncWrite stream.
impl<S> Tls13Stream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    /// Perform the full TLS 1.3 handshake asynchronously over a generic stream.
    ///
    /// Uses async I/O primitives directly — no `spawn_blocking` or `into_std()`.
    /// Suitable for relay streams, trait objects, and any `AsyncRead + AsyncWrite`
    /// carrier that is not a concrete `TcpStream`.
    pub async fn connect_async(stream: S, config: Tls13Config) -> io::Result<Self> {
        let timeout = std::time::Duration::from_millis(config.handshake_timeout_ms);
        tokio::time::timeout(timeout, Self::connect_async_inner(stream, config))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timed out"))?
    }

    async fn connect_async_inner(mut stream: S, config: Tls13Config) -> io::Result<Self> {
        let mut conn = Tls13Connection::new(config)?;
        loop {
            if conn.wants_write() {
                let mut buf = Vec::new();
                conn.write_tls(&mut buf)?;
                if !buf.is_empty() {
                    stream.write_all(&buf).await?;
                    stream.flush().await?;
                }
            }

            let _ = conn.process_new_packets()?;

            // The handshake only completes once our Finished flight has
            // actually been written; `is_handshaking()` flips to false while
            // the Finished is still queued, so also require no pending output
            // before breaking.
            if !conn.is_handshaking() && !conn.wants_write() {
                break;
            }

            // If processing just queued output (our Finished), flush it before
            // trying to read — the peer may be blocked waiting for it, so a
            // read here would deadlock.
            if conn.wants_write() {
                continue;
            }

            if conn.wants_read() {
                let mut buf = [0u8; 8192];
                let limit = conn.next_record_read_size()?.min(buf.len());
                let n = stream.read(&mut buf[..limit]).await?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "TLS handshake: connection closed",
                    ));
                }
                conn.read_tls(&mut io::Cursor::new(&buf[..n]))?;
            }
        }

        Ok(Self {
            inner: stream,
            conn,
            closing: false,
            bypass: None,
        })
    }
}

impl<S> Tls13Stream<S> {
    pub fn with_bypass_control(mut self, control: zero_traits::TransportBypassControl) -> Self {
        self.bypass = Some(control);
        self
    }

    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        self.conn.alpn_protocol()
    }

    pub fn get_ref(&self) -> &S {
        &self.inner
    }
}

impl<S> AsyncRead for Tls13Stream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        match self.as_mut().poll_flush(cx) {
            Poll::Ready(Ok(())) => {}
            other => return other,
        }
        // Serve already-decrypted plaintext first.
        let n = self.conn.read_plaintext(buf.initialize_unfilled());
        if n > 0 {
            buf.advance(n);
            return Poll::Ready(Ok(()));
        }

        if self
            .bypass
            .as_ref()
            .is_some_and(|c| c.read_bypass_requested())
        {
            if self.conn.buffered_ciphertext_len() != 0 {
                return Poll::Ready(Err(io::Error::other("TLS handoff within a record")));
            }
            return Pin::new(&mut self.inner).poll_read(cx, buf);
        }
        if !self.conn.wants_read() {
            return Poll::Ready(Ok(()));
        }

        // Loop until we either produce plaintext, hit a genuine EOF, or the
        // underlying stream has no more data right now. Returning Ok with zero
        // bytes filled would be interpreted by tokio as EOF, so a read that
        // brings in a partial (or non-application) record must keep reading.
        loop {
            let mut raw = [0u8; 8192];
            let limit = self.conn.next_record_read_size()?.min(raw.len());
            let mut raw_buf = ReadBuf::new(&mut raw[..limit]);
            match Pin::new(&mut self.inner).poll_read(cx, &mut raw_buf) {
                Poll::Ready(Ok(())) => {
                    let filled = raw_buf.filled();
                    if filled.is_empty() {
                        // Underlying stream closed. Surface any final plaintext
                        // already buffered above; otherwise this is a clean EOF.
                        return Poll::Ready(Ok(()));
                    }
                    if let Err(e) = self.conn.read_tls(&mut io::Cursor::new(filled)) {
                        return Poll::Ready(Err(e));
                    }
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }

            if let Err(e) = self.conn.process_new_packets() {
                return Poll::Ready(Err(e));
            }
            match self.as_mut().poll_flush(cx) {
                Poll::Ready(Ok(())) => {}
                other => return other,
            }

            let n = self.conn.read_plaintext(buf.initialize_unfilled());
            if n > 0 {
                buf.advance(n);
                return Poll::Ready(Ok(()));
            }
            if !self.conn.wants_read() {
                return Poll::Ready(Ok(()));
            }
            // Partial record or a non-application record (e.g. an alert) was
            // consumed; loop to read more.
        }
    }
}

#[path = "stream/write.rs"]
mod write;
