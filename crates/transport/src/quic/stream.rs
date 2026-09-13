use super::{handshake_metadata, QuicConnection};
use std::{
    future::Future,
    io,
    net::{IpAddr, SocketAddr},
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_platform_tokio::ClientStream;
use zero_traits::AsyncSocket;
type FinishFuture = Pin<Box<dyn Future<Output = io::Result<()>> + Send + Sync>>;
/// Bidirectional QUIC stream wrapping quinn SendStream and RecvStream.
pub struct QuicStream {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    peer: SocketAddr,
    tls: (Option<String>, Option<String>),
    local: Option<SocketAddr>,
    local_ip: Option<IpAddr>,
    finish: Option<FinishFuture>,
    finished: bool,
}

impl QuicStream {
    pub fn with_local_addr(mut self, mut local: SocketAddr) -> Self {
        if local.ip().is_unspecified() {
            if let Some(ip) = self.local_ip {
                local.set_ip(ip);
            }
        }
        self.local = Some(local);
        self
    }
    pub fn tls_metadata(&self) -> (Option<String>, Option<String>) {
        self.tls.clone()
    }
    pub(crate) fn new(
        send: quinn::SendStream,
        recv: quinn::RecvStream,
        connection: &QuicConnection,
    ) -> Self {
        Self {
            send,
            recv,
            peer: connection.remote_address(),
            tls: handshake_metadata(connection),
            local: None,
            local_ip: connection.local_ip(),
            finish: None,
            finished: false,
        }
    }
}

// ── AsyncRead / AsyncWrite / AsyncSocket / ClientStream ──

impl AsyncRead for QuicStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.recv).poll_read(cx, buf)
    }
}

impl AsyncWrite for QuicStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.send)
            .poll_write(cx, buf)
            .map_err(io::Error::other)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.send)
            .poll_flush(cx)
            .map_err(io::Error::other)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        if self.finished {
            return Poll::Ready(Ok(()));
        }
        if self.finish.is_none() {
            std::task::ready!(Pin::new(&mut self.send).poll_shutdown(cx))
                .map_err(io::Error::other)?;
            let stopped = self.send.stopped();
            self.finish = Some(Box::pin(async move {
                match stopped.await.map_err(io::Error::other)? {
                    None => Ok(()),
                    Some(code) => Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        format!("QUIC peer stopped stream: {code}"),
                    )),
                }
            }));
        }
        let result = std::task::ready!(self.finish.as_mut().unwrap().as_mut().poll(cx));
        self.finished = result.is_ok();
        self.finish = None;
        Poll::Ready(result)
    }
}

impl AsyncSocket for QuicStream {
    type Error = io::Error;

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

impl ClientStream for QuicStream {
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.peer)
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.local.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "QUIC local endpoint was not supplied",
            )
        })
    }
}
