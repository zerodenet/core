use super::*;
impl<S> AsyncRead for AcceptedXhttpStreamOne<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
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
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
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
