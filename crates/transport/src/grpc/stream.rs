use super::*;
pub struct GrpcStream {
    progress: std::sync::Arc<super::progress::Progress>,
    pub(super) driver: Option<std::sync::Arc<super::server::Driver>>,
    pub(super) incoming: Option<super::server::GrpcIncoming>,
    pub(super) active: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
    read_rx: mpsc::Receiver<io::Result<Vec<u8>>>,
    write_tx: tokio_util::sync::PollSender<Vec<u8>>,
    read_buffer: Vec<u8>,
    read_offset: usize,
    write_closed: bool,
}

impl GrpcStream {
    pub(super) fn new(
        read_rx: mpsc::Receiver<io::Result<Vec<u8>>>,
        write_tx: tokio_util::sync::PollSender<Vec<u8>>,
        progress: std::sync::Arc<super::progress::Progress>,
    ) -> Self {
        Self {
            progress,
            driver: None,
            incoming: None,
            active: None,
            read_rx,
            write_tx,
            read_buffer: Vec::new(),
            read_offset: 0,
            write_closed: false,
        }
    }
}

// Async stream interfaces.

impl AsyncRead for GrpcStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.read_offset < self.read_buffer.len() {
            let available = self.read_buffer.len() - self.read_offset;
            let to_copy = available.min(buf.remaining());
            buf.put_slice(&self.read_buffer[self.read_offset..self.read_offset + to_copy]);
            self.read_offset += to_copy;
            if self.read_offset >= self.read_buffer.len() {
                self.read_buffer.clear();
                self.read_offset = 0;
            }
            return Poll::Ready(Ok(()));
        }

        match self.read_rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(data))) => {
                let to_copy = data.len().min(buf.remaining());
                buf.put_slice(&data[..to_copy]);
                if to_copy < data.len() {
                    self.read_buffer = data;
                    self.read_offset = to_copy;
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Err(error)),
            Poll::Ready(None) => Poll::Ready(Ok(())),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for GrpcStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        if self.write_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "grpc write side closed",
            )));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        match self.write_tx.poll_reserve(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(_)) => return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())),
            Poll::Ready(Ok(())) => {}
        }
        let count = buf.len().min(GRPC_MAX_PAYLOAD);
        match self.write_tx.send_item(buf[..count].to_vec()) {
            Ok(()) => {
                self.progress.accept(count);
                Poll::Ready(Ok(count))
            }
            Err(_) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "grpc write side closed",
            ))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.progress.poll(cx, false)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        if !self.write_closed {
            self.write_closed = true;
            self.write_tx.close();
        }
        self.progress.poll(cx, true)
    }
}

impl AsyncSocket for GrpcStream {
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

impl ClientStream for GrpcStream {
    fn local_addr(&self) -> io::Result<SocketAddr> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "GrpcStream does not expose local_addr",
        ))
    }
}

impl Drop for GrpcStream {
    fn drop(&mut self) {
        self.write_tx.close();
        if let Some(active) = &self.active {
            active.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}
