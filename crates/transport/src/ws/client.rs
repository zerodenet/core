use super::*;
use base64::Engine;
use std::{
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio_tungstenite::tungstenite::http::Request;

type Handshake<S> = Pin<Box<dyn Future<Output = io::Result<WebSocketSocket<S>>> + Send + Sync>>;
pub struct WebSocketClient<S> {
    initial: Option<(S, Request<()>, u32)>,
    pending: Option<Handshake<S>>,
    socket: Option<WebSocketSocket<S>>,
    failed: bool,
    heartbeat_period_secs: u32,
}
impl<S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static> WebSocketClient<S> {
    pub(super) fn new(
        stream: S,
        request: Request<()>,
        limit: u32,
        heartbeat_period_secs: u32,
    ) -> Self {
        Self {
            initial: Some((stream, request, limit)),
            pending: None,
            socket: None,
            failed: false,
            heartbeat_period_secs,
        }
    }
    fn begin(&mut self, early: Option<&[u8]>) {
        if let Some((stream, mut request, _)) = self.initial.take() {
            if let Some(bytes) = early {
                let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes);
                request.headers_mut().insert(
                    "sec-websocket-protocol",
                    encoded.parse().expect("base64url is a header value"),
                );
            }
            self.pending = Some(Box::pin(async move {
                let (stream, _) = tokio_tungstenite::client_async(request, stream)
                    .await
                    .map_err(io::Error::other)?;
                Ok(WebSocketSocket::new(stream))
            }));
        }
    }
    pub(super) fn ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.failed {
            return Poll::Ready(Err(io::Error::other("WebSocket handshake failed")));
        }
        self.begin(None);
        if let Some(pending) = &mut self.pending {
            match ready!(pending.as_mut().poll(cx)) {
                Ok(mut socket) => {
                    socket.set_heartbeat(self.heartbeat_period_secs);
                    self.socket = Some(socket);
                }
                Err(error) => {
                    self.failed = true;
                    self.pending = None;
                    return Poll::Ready(Err(error));
                }
            }
            self.pending = None;
        }
        Poll::Ready(Ok(()))
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static> AsyncRead for WebSocketClient<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.ready(cx))?;
        Pin::new(this.socket.as_mut().unwrap()).poll_read(cx, buf)
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static> AsyncWrite for WebSocketClient<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if this
            .initial
            .as_ref()
            .is_some_and(|(_, _, limit)| *limit > 0 && buf.len() <= *limit as usize)
        {
            this.begin(Some(buf));
            return Poll::Ready(Ok(buf.len()));
        }
        ready!(this.ready(cx))?;
        Pin::new(this.socket.as_mut().unwrap()).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.ready(cx))?;
        Pin::new(this.socket.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.ready(cx))?;
        Pin::new(this.socket.as_mut().unwrap()).poll_shutdown(cx)
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static> AsyncSocket for WebSocketClient<S> {
    type Error = io::Error;
    async fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        AsyncReadExt::read(self, buf).await
    }
    async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        AsyncWriteExt::write_all(self, buf).await?;
        AsyncWriteExt::flush(self).await
    }
    async fn shutdown(&mut self) -> io::Result<()> {
        AsyncWriteExt::shutdown(self).await
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static> ClientStream
    for WebSocketClient<S>
{
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Err(io::ErrorKind::Unsupported.into())
    }
}
