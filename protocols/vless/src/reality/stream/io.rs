use super::*;

impl<IO> AsyncRead for RealityTlsStream<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.transport_bypass_control.read_bypass_requested() {
            return Pin::new(&mut this.io).poll_read(cx, buf);
        }
        if !this.state.readable() {
            return Poll::Ready(Ok(()));
        }

        // KeyUpdate responses must progress even when the application only reads.
        match Pin::new(&mut *this).poll_flush(cx) {
            Poll::Ready(Ok(())) => {}
            other => return other,
        }

        let mut io_pending = false;
        let mut eof = false;

        while this.state.readable() && this.session.wants_read() {
            let adapter = SyncReadAdapter {
                io: &mut this.io,
                cx,
            };
            let limit = this.session.next_tls_read_limit();
            let mut limited = adapter.take(limit as u64);
            match this.session.read_tls(&mut limited) {
                Ok(0) => {
                    eof = true;
                    break;
                }
                Ok(_) => {
                    if let Err(error) = this.session.process_new_packets() {
                        let _ = this.drain_all_writes(cx);
                        return Poll::Ready(Err(error));
                    }
                    match Pin::new(&mut *this).poll_flush(cx) {
                        Poll::Ready(Ok(())) => {}
                        other => return other,
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    io_pending = true;
                    break;
                }
                Err(error) => return Poll::Ready(Err(error)),
            }
        }

        let mut reader = this.session.reader();
        match reader.fill_buf() {
            Ok(available) if !available.is_empty() => {
                let len = buf.remaining().min(available.len());
                buf.put_slice(&available[..len]);
                reader.consume(len);
                Poll::Ready(Ok(()))
            }
            Ok(_) => {
                this.state.shutdown_read();
                Poll::Ready(Ok(()))
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if eof {
                    this.state.shutdown_read();
                    Poll::Ready(Ok(()))
                } else if io_pending {
                    Poll::Pending
                } else {
                    let adapter = SyncReadAdapter {
                        io: &mut this.io,
                        cx,
                    };
                    let limit = this.session.next_tls_read_limit();
                    let mut limited = adapter.take(limit as u64);
                    match this.session.read_tls(&mut limited) {
                        Ok(0) => {
                            this.state.shutdown_read();
                            Poll::Ready(Ok(()))
                        }
                        Ok(_) => {
                            if let Err(error) = this.session.process_new_packets() {
                                let _ = this.drain_all_writes(cx);
                                return Poll::Ready(Err(error));
                            }
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Poll::Pending,
                        Err(error) => Poll::Ready(Err(error)),
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::ConnectionAborted => {
                this.state.shutdown_read();
                Poll::Ready(Err(error))
            }
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

impl<IO> AsyncWrite for RealityTlsStream<IO>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.transport_bypass_control.write_bypass_requested() {
            return Pin::new(&mut self.io).poll_write(cx, buf);
        }
        if !self.state.writeable() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "write side is shut down",
            )));
        }

        let mut pos = 0;
        while pos < buf.len() {
            let mut would_block = false;
            match self.session.writer().write(&buf[pos..]) {
                Ok(read) => pos += read,
                Err(error) => return Poll::Ready(Err(error)),
            }

            while self.session.wants_write() {
                match self.write_tls_direct(cx) {
                    Poll::Ready(Ok(0)) | Poll::Pending => {
                        would_block = true;
                        self.need_flush = true;
                        break;
                    }
                    Poll::Ready(Ok(_)) => self.need_flush = true,
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                }
            }

            return match (pos, would_block) {
                (0, true) => Poll::Pending,
                (written, true) => Poll::Ready(Ok(written)),
                (_, false) => continue,
            };
        }

        Poll::Ready(Ok(pos))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.transport_bypass_control.write_bypass_requested() {
            return Pin::new(&mut self.io).poll_flush(cx);
        }
        self.session.writer().flush()?;

        while self.session.wants_write() {
            match self.write_tls_direct(cx) {
                Poll::Ready(Ok(0)) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                Poll::Ready(Ok(_)) => self.need_flush = true,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }

        if self.need_flush {
            match Pin::new(&mut self.io).poll_flush(cx) {
                Poll::Ready(Ok(())) => {
                    self.need_flush = false;
                    Poll::Ready(Ok(()))
                }
                result => result,
            }
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.transport_bypass_control.write_bypass_requested() {
            return Pin::new(&mut self.io).poll_shutdown(cx);
        }
        while self.session.wants_write() {
            match self.write_tls_direct(cx) {
                Poll::Ready(Ok(0)) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }

        if self.state.writeable() {
            self.session.send_close_notify();
            self.state.shutdown_write();
        }

        while self.session.wants_write() {
            match self.write_tls_direct(cx) {
                Poll::Ready(Ok(0)) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }

        match Pin::new(&mut self.io).poll_shutdown(cx) {
            Poll::Ready(Err(error)) if error.kind() == io::ErrorKind::NotConnected => {
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}

impl<IO> zero_traits::AsyncSocket for RealityTlsStream<IO>
where
    IO: AsyncRead + AsyncWrite + Send + Sync + Unpin,
{
    type Error = io::Error;

    fn transport_bypass_control(&self) -> Option<TransportBypassControl> {
        Some(self.transport_bypass_control())
    }

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        tokio::io::AsyncReadExt::read(self, buf).await
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        tokio::io::AsyncWriteExt::write_all(self, buf).await?;
        tokio::io::AsyncWriteExt::flush(self).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        tokio::io::AsyncWriteExt::shutdown(self).await
    }
}

impl<IO> zero_platform_tokio::ClientStream for RealityTlsStream<IO>
where
    IO: zero_platform_tokio::ClientStream,
{
    fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.io.peer_addr()
    }
    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.io.local_addr()
    }
}
