use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::RuntimeError;
use http::{Method, Request, Response};
use rand::Rng;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use zero_platform_tokio::ClientStream;
use zero_traits::{AsyncSocket, SplitHttpTransportProfile};

use super::chunked::{ChunkedDecoder, DecodeStep};
use super::wire::{find_header_end, parse_status, validate_path};

const DEFAULT_PADDING_MIN_BYTES: usize = 100;
const DEFAULT_PADDING_MAX_BYTES: usize = 1000;
const H2_PREFACE_PREFIX: &[u8; 4] = b"PRI ";

/// Parsed XHTTP framing mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XhttpMode {
    Auto,
    PacketUp,
    StreamUp,
    StreamOne,
}

impl XhttpMode {
    pub fn parse(s: &str) -> Self {
        match s {
            "" | "auto" => XhttpMode::Auto,
            "packet-up" => XhttpMode::PacketUp,
            "stream-up" => XhttpMode::StreamUp,
            "stream-one" => XhttpMode::StreamOne,
            _ => XhttpMode::Auto,
        }
    }

    pub fn is_single_connection(self) -> bool {
        matches!(self, XhttpMode::Auto | XhttpMode::StreamOne)
    }
}

/// Single-connection bidirectional XHTTP stream (`stream-one` mode).
pub struct XhttpStreamOne<S> {
    inner: S,
    decoder: ChunkedDecoder,
    response_headers: Option<Vec<u8>>,
    write_finished: bool,
}

/// Server-side XHTTP stream-one transport selected from the client's wire
/// protocol. Xray uses HTTP/1.1 for cleartext clients while H2/H2C clients use
/// one HTTP/2 request stream; both expose the same bidirectional byte stream.
pub struct AcceptedXhttpStreamOne<S> {
    inner: AcceptedXhttpStreamOneInner<S>,
}

enum AcceptedXhttpStreamOneInner<S> {
    H2(crate::h2::H2Stream),
    Http1(XhttpStreamOne<PrefixedIo<S>>),
}

struct PrefixedIo<S> {
    prefix: [u8; 4],
    offset: usize,
    inner: S,
}

impl<S> PrefixedIo<S> {
    fn new(inner: S, prefix: [u8; 4]) -> Self {
        Self {
            prefix,
            offset: 0,
            inner,
        }
    }
}

pub async fn connect_xhttp_stream_one<S, TProfile>(
    stream: S,
    config: &TProfile,
) -> Result<crate::h2::H2Stream, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    TProfile: SplitHttpTransportProfile + ?Sized,
{
    let host = config.host().unwrap_or("localhost");
    let path = normalize_stream_one_path(config.path());
    let referer = build_padding_referer(host, &path);
    let request = Request::builder()
        .method(Method::POST)
        .uri(&path)
        .header("host", host)
        .header("referer", referer)
        .header("content-type", "application/grpc")
        .body(())
        .map_err(|error| {
            RuntimeError::Io(io::Error::other(format!(
                "xhttp stream-one request build: {error}"
            )))
        })?;
    crate::h2::connect_h2_request(stream, request).await
}

pub async fn connect_xhttp_stream_one_http1<S, TProfile>(
    stream: S,
    config: &TProfile,
) -> Result<XhttpStreamOne<S>, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    TProfile: SplitHttpTransportProfile + ?Sized,
{
    let host = config.host().unwrap_or("localhost");
    let path = config.path();
    let referer = build_padding_referer(host, path);
    let mut stream = stream;
    let request = format!(
        "POST {path} HTTP/1.1\r\n\
         Host: {host}\r\n\
         Referer: {referer}\r\n\
         Transfer-Encoding: chunked\r\n\
         Content-Type: application/grpc\r\n\
         \r\n"
    );
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(RuntimeError::Io)?;
    stream.flush().await.map_err(RuntimeError::Io)?;

    Ok(XhttpStreamOne {
        inner: stream,
        decoder: ChunkedDecoder::new(),
        response_headers: Some(Vec::with_capacity(1024)),
        write_finished: false,
    })
}

fn normalize_stream_one_path(path: &str) -> String {
    let (base, query) = path.split_once('?').unwrap_or((path, ""));
    let mut normalized = if base.starts_with('/') {
        base.to_owned()
    } else {
        format!("/{base}")
    };
    if !normalized.ends_with('/') {
        normalized.push('/');
    }
    if !query.is_empty() {
        normalized.push('?');
        normalized.push_str(query);
    }
    normalized
}

fn build_padding_referer(host: &str, path: &str) -> String {
    let padding_len =
        rand::rng().random_range(DEFAULT_PADDING_MIN_BYTES..=DEFAULT_PADDING_MAX_BYTES);
    let separator = if path.contains('?') { '&' } else { '?' };
    format!(
        "https://{host}{path}{separator}x_padding={}",
        "X".repeat(padding_len)
    )
}

pub async fn accept_xhttp_stream_one<S, TProfile>(
    mut stream: S,
    config: &TProfile,
) -> Result<AcceptedXhttpStreamOne<S>, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    TProfile: SplitHttpTransportProfile + ?Sized,
{
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).await.map_err(|error| {
        RuntimeError::Io(io::Error::new(
            error.kind(),
            format!("xhttp stream-one accept: failed to read wire prefix: {error}"),
        ))
    })?;
    let stream = PrefixedIo::new(stream, prefix);

    if &prefix != H2_PREFACE_PREFIX {
        if &prefix != b"POST" {
            return Err(RuntimeError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "xhttp stream-one accept: unsupported wire prefix {:?}",
                    String::from_utf8_lossy(&prefix)
                ),
            )));
        }
        return accept_xhttp_stream_one_http1(stream, config)
            .await
            .map(|stream| AcceptedXhttpStreamOne {
                inner: AcceptedXhttpStreamOneInner::Http1(stream),
            });
    }

    let path = normalize_stream_one_path(config.path());
    let expected_path = path.split_once('?').map_or(path.as_str(), |(path, _)| path);
    let mut response = Response::new(());
    response
        .headers_mut()
        .insert("content-type", "text/event-stream".parse().unwrap());
    response
        .headers_mut()
        .insert("cache-control", "no-store".parse().unwrap());
    response.headers_mut().insert(
        "x-padding",
        "X".repeat(DEFAULT_PADDING_MIN_BYTES).parse().unwrap(),
    );
    crate::h2::accept_h2_with_response(stream, expected_path, response)
        .await
        .map(|stream| AcceptedXhttpStreamOne {
            inner: AcceptedXhttpStreamOneInner::H2(stream),
        })
}

pub async fn accept_xhttp_stream_one_http1<S, TProfile>(
    stream: S,
    config: &TProfile,
) -> Result<XhttpStreamOne<S>, RuntimeError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    TProfile: SplitHttpTransportProfile + ?Sized,
{
    let mut stream = stream;
    let mut buf = vec![0u8; 8192];
    let mut total = 0;
    let head_end = loop {
        let n = stream
            .read(&mut buf[total..])
            .await
            .map_err(RuntimeError::Io)?;
        if n == 0 {
            return Err(RuntimeError::Io(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "xhttp stream-one accept: unexpected EOF before request headers",
            )));
        }
        total += n;
        if let Some(end) = find_header_end(&buf[..total]) {
            break end;
        }
        if total >= buf.len() {
            return Err(RuntimeError::Io(io::Error::new(
                io::ErrorKind::InvalidData,
                "xhttp stream-one accept: request headers too large",
            )));
        }
    };

    validate_stream_one_http1_request(&buf[..head_end], config.path())?;
    stream
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\n\
                 Transfer-Encoding: chunked\r\n\
                 Content-Type: text/event-stream\r\n\
                 Cache-Control: no-store\r\n\
                 X-Padding: {}\r\n\
                 \r\n",
                "X".repeat(DEFAULT_PADDING_MIN_BYTES)
            )
            .as_bytes(),
        )
        .await
        .map_err(RuntimeError::Io)?;
    stream.flush().await.map_err(RuntimeError::Io)?;

    let prefetched = buf[head_end..total].to_vec();
    Ok(XhttpStreamOne {
        inner: stream,
        decoder: ChunkedDecoder::with_prefetched(prefetched),
        response_headers: None,
        write_finished: false,
    })
}

fn validate_stream_one_http1_request(
    headers: &[u8],
    expected_path: &str,
) -> Result<(), RuntimeError> {
    validate_path(headers, expected_path)?;
    let headers = std::str::from_utf8(headers).map_err(|_| {
        RuntimeError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "xhttp stream-one: non-UTF-8 request headers",
        ))
    })?;
    let mut lines = headers.lines();
    let request_line = lines.next().unwrap_or_default();
    if !request_line.starts_with("POST ") {
        return Err(RuntimeError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "xhttp stream-one: request method must be POST",
        )));
    }

    let mut chunked = false;
    let mut content_type = false;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("transfer-encoding")
            && value
                .split(',')
                .any(|encoding| encoding.trim().eq_ignore_ascii_case("chunked"))
        {
            chunked = true;
        }
        if name.eq_ignore_ascii_case("content-type")
            && value.trim().eq_ignore_ascii_case("application/grpc")
        {
            content_type = true;
        }
    }
    if !chunked {
        return Err(RuntimeError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "xhttp stream-one: request must use chunked transfer encoding",
        )));
    }
    if !content_type {
        return Err(RuntimeError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            "xhttp stream-one: request content-type must be application/grpc",
        )));
    }
    Ok(())
}

impl<S> AsyncRead for PrefixedIo<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.offset < this.prefix.len() {
            let available = this.prefix.len() - this.offset;
            let to_copy = available.min(buf.remaining());
            buf.put_slice(&this.prefix[this.offset..this.offset + to_copy]);
            this.offset += to_copy;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for PrefixedIo<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

impl<S> AsyncRead for AcceptedXhttpStreamOne<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            AcceptedXhttpStreamOneInner::H2(stream) => Pin::new(stream).poll_read(cx, buf),
            AcceptedXhttpStreamOneInner::Http1(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl<S> AsyncWrite for AcceptedXhttpStreamOne<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().inner {
            AcceptedXhttpStreamOneInner::H2(stream) => Pin::new(stream).poll_write(cx, buf),
            AcceptedXhttpStreamOneInner::Http1(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            AcceptedXhttpStreamOneInner::H2(stream) => Pin::new(stream).poll_flush(cx),
            AcceptedXhttpStreamOneInner::Http1(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            AcceptedXhttpStreamOneInner::H2(stream) => Pin::new(stream).poll_shutdown(cx),
            AcceptedXhttpStreamOneInner::Http1(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

impl<S> AsyncSocket for AcceptedXhttpStreamOne<S>
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

impl<S> ClientStream for AcceptedXhttpStreamOne<S> where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync
{
}

impl<S> AsyncRead for XhttpStreamOne<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        while let Some(headers) = this.response_headers.as_mut() {
            let mut tmp = [0u8; 8192];
            let mut rb = ReadBuf::new(&mut tmp);
            match Pin::new(&mut this.inner).poll_read(cx, &mut rb) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {
                    if rb.filled().is_empty() {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::ConnectionAborted,
                            "xhttp stream-one: unexpected EOF reading response",
                        )));
                    }
                    headers.extend_from_slice(rb.filled());
                }
            }

            let Some(head_end) = find_header_end(headers) else {
                if headers.len() >= 8192 {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "xhttp stream-one: response headers too large",
                    )));
                }
                continue;
            };
            let status = parse_status(&headers[..head_end]);
            if status != Some(200) {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::ConnectionRefused,
                    format!("xhttp stream-one: expected 200, got {status:?}"),
                )));
            }
            let prefetched = headers[head_end..].to_vec();
            this.response_headers = None;
            this.decoder.feed(&prefetched);
        }

        loop {
            match this.decoder.try_decode(buf)? {
                DecodeStep::Done => return Poll::Ready(Ok(())),
                DecodeStep::NeedsMore => {
                    let mut tmp = [0u8; 8192];
                    let mut rb = ReadBuf::new(&mut tmp);
                    match Pin::new(&mut this.inner).poll_read(cx, &mut rb) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Ready(Ok(())) => {
                            if rb.filled().is_empty() {
                                return Poll::Ready(Ok(()));
                            }
                            this.decoder.feed(rb.filled());
                        }
                    }
                }
            }
        }
    }
}

impl<S> AsyncWrite for XhttpStreamOne<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() || self.write_finished {
            return Poll::Ready(Ok(0));
        }
        let this = self.get_mut();
        let header = format!("{:x}\r\n", buf.len());
        let frame: Vec<u8> = header
            .as_bytes()
            .iter()
            .chain(buf.iter())
            .chain(b"\r\n".iter())
            .copied()
            .collect();

        match Pin::new(&mut this.inner).poll_write(cx, &frame) {
            Poll::Ready(Ok(written)) => {
                let data_written = if written >= header.len() + 2 {
                    buf.len().min(written - header.len() - 2)
                } else {
                    0
                };
                Poll::Ready(Ok(data_written))
            }
            other => other,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.write_finished {
            return Poll::Ready(Ok(()));
        }
        this.write_finished = true;
        match Pin::new(&mut this.inner).poll_write(cx, b"0\r\n\r\n") {
            Poll::Ready(Ok(_)) => {
                let _ = Pin::new(&mut this.inner).poll_flush(cx);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S> AsyncSocket for XhttpStreamOne<S>
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

impl<S> ClientStream for XhttpStreamOne<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "XHTTP stream-one stream does not expose local_addr",
        ))
    }
}
