//! HTTPUpgrade performs one HTTP handshake, then carries raw bidirectional bytes.
use crate::RuntimeError;
use http::{Method, Request};
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_platform_tokio::ClientStream;
use zero_traits::{AsyncSocket, HttpUpgradeTransportProfile};
mod wire;
use wire::*;

pub struct HttpUpgradeStream<S> {
    inner: S,
    prefetched: Vec<u8>,
    read_offset: usize,
    response_header: Option<Vec<u8>>,
    failed: bool,
}
impl<S> HttpUpgradeStream<S> {
    fn new(inner: S, prefetched: Vec<u8>, pending: bool) -> Self {
        Self {
            inner,
            prefetched,
            read_offset: 0,
            response_header: pending.then(Vec::new),
            failed: false,
        }
    }
}
pub async fn connect_http_upgrade<S, T: HttpUpgradeTransportProfile + ?Sized>(
    mut stream: S,
    config: &T,
) -> Result<HttpUpgradeStream<S>, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (path, early) = crate::http_early_data::path_options(config.path());
    let mut request = Request::builder()
        .method(Method::GET)
        .uri(path)
        .header("Host", config.host().unwrap_or("localhost"));
    for (key, value) in config.header_pairs() {
        if key.eq_ignore_ascii_case("host") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "HTTPUpgrade headers cannot override host",
            )
            .into());
        }
        request = request.header(key, value);
    }
    let mut request = request
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .body(())
        .map_err(io::Error::other)?;
    crate::browser::apply_websocket_headers(request.headers_mut());
    let mut bytes = Vec::new();
    write_http_request(&mut bytes, &request);
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    if early > 0 {
        return Ok(HttpUpgradeStream::new(stream, Vec::new(), true));
    }
    let (head, extra) = read_head(&mut stream).await?;
    validate_response(&head)?;
    Ok(HttpUpgradeStream::new(stream, extra, false))
}
pub async fn accept_http_upgrade<S, T: HttpUpgradeTransportProfile + ?Sized>(
    mut stream: S,
    config: &T,
) -> Result<HttpUpgradeStream<S>, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (head, extra) = read_head(&mut stream).await?;
    let text = std::str::from_utf8(&head).map_err(|_| invalid("invalid HTTPUpgrade request"))?;
    let mut line = text.lines().next().unwrap_or("").split_whitespace();
    if line.next() != Some("GET") {
        return Err(invalid("HTTPUpgrade requires GET").into());
    }
    let path = line
        .next()
        .ok_or_else(|| invalid("HTTPUpgrade missing path"))?;
    let (expected, _) = crate::http_early_data::path_options(config.path());
    if path.split('?').next() != expected.split('?').next() || line.next() != Some("HTTP/1.1") {
        return Err(invalid("HTTPUpgrade request path/version mismatch").into());
    }
    validate_upgrade(text)?;
    if let Some(expected) = config.host().filter(|host| !host.is_empty()) {
        if !header(text, "host")
            .is_some_and(|host| crate::http_early_data::host_matches(host, expected))
        {
            return Err(invalid("HTTPUpgrade host mismatch").into());
        }
    }
    stream.write_all(b"HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n").await?;
    stream.flush().await?;
    Ok(HttpUpgradeStream::new(stream, extra, false))
}
impl<S: AsyncRead + Unpin> HttpUpgradeStream<S> {
    fn poll_response(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            let header = self.response_header.as_mut().expect("pending response");
            if let Some(end) = find_header_end(header) {
                validate_response(&header[..end])?;
                self.prefetched = header[end..].to_vec();
                self.response_header = None;
                return Poll::Ready(Ok(()));
            }
            if header.len() >= MAX_HEADER {
                return Poll::Ready(Err(invalid("HTTPUpgrade response headers too large")));
            }
            let mut bytes = [0; 4096];
            let mut buf = ReadBuf::new(&mut bytes);
            std::task::ready!(Pin::new(&mut self.inner).poll_read(cx, &mut buf))?;
            if buf.filled().is_empty() {
                return Poll::Ready(Err(io::ErrorKind::UnexpectedEof.into()));
            }
            header.extend_from_slice(buf.filled());
        }
    }
}

impl<S> AsyncRead for HttpUpgradeStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let this = self.as_mut().get_mut();
        if this.failed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTPUpgrade handshake failed",
            )));
        }
        if this.response_header.is_some() {
            match std::task::ready!(this.poll_response(cx)) {
                Ok(()) => {}
                Err(error) => {
                    this.failed = true;
                    return Poll::Ready(Err(error));
                }
            }
        }
        if self.read_offset < self.prefetched.len() {
            let count = buf
                .remaining()
                .min(self.prefetched.len() - self.read_offset);
            buf.put_slice(&self.prefetched[self.read_offset..self.read_offset + count]);
            self.read_offset += count;
            if self.read_offset == self.prefetched.len() {
                self.prefetched.clear();
                self.read_offset = 0;
            }
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for HttpUpgradeStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl<S> AsyncSocket for HttpUpgradeStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
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

impl<S> ClientStream for HttpUpgradeStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "HttpUpgrade stream does not expose local_addr",
        ))
    }
}
