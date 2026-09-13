//! Retain ciphertext across partial writes, Pending and cancelled futures.
use super::*;
const WRITE_LIMIT: usize = 16 * 1024;
impl<S: AsyncRead + AsyncWrite + Unpin> Tls13Stream<S> {
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.conn.wants_write() {
            let mut writer = Writer {
                inner: &mut self.inner,
                cx,
            };
            match self.conn.write_tls(&mut writer) {
                Ok(0) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return Poll::Pending,
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
        Poll::Ready(Ok(()))
    }
}
impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for Tls13Stream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.closing {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        match self.poll_drain(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }
        if self
            .bypass
            .as_ref()
            .is_some_and(|c| c.write_bypass_requested())
        {
            return Pin::new(&mut self.inner).poll_write(cx, bytes);
        }
        let n = bytes.len().min(WRITE_LIMIT);
        self.conn.write_plaintext(&bytes[..n]);
        // These bytes are now owned by the connection. Return their accepted
        // count even if the carrier cannot immediately write the entire record.
        match self.poll_drain(cx) {
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            _ => Poll::Ready(Ok(n)),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_drain(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut self.inner).poll_flush(cx),
            other => other,
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.poll_drain(cx) {
            Poll::Ready(Ok(())) => {}
            other => return other,
        }
        if !self.closing {
            if !self
                .bypass
                .as_ref()
                .is_some_and(|c| c.write_bypass_requested())
            {
                self.conn.send_close_notify()?;
            }
            self.closing = true;
        }
        match self.as_mut().poll_flush(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut self.inner).poll_shutdown(cx),
            other => other,
        }
    }
}
struct Writer<'a, 'b, S> {
    inner: &'a mut S,
    cx: &'a mut Context<'b>,
}
impl<S: AsyncWrite + Unpin> io::Write for Writer<'_, '_, S> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match Pin::new(&mut *self.inner).poll_write(self.cx, bytes) {
            Poll::Ready(result) => result,
            Poll::Pending => Err(io::ErrorKind::WouldBlock.into()),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
