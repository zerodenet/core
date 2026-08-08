use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::VisionStream;

impl<S> AsyncRead for VisionStream<S>
where
    S: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.copy_read_output(buf) {
            return Poll::Ready(Ok(()));
        }

        loop {
            self.process_read_wire()?;
            if self.copy_read_output(buf) {
                return Poll::Ready(Ok(()));
            }

            let mut chunk = [0_u8; 8192];
            let mut read_buf = ReadBuf::new(&mut chunk);
            match Pin::new(&mut self.inner).poll_read(cx, &mut read_buf) {
                Poll::Ready(Ok(())) if read_buf.filled().is_empty() => {
                    if self.read_wire.is_empty() {
                        return Poll::Ready(Ok(()));
                    }
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "EOF inside VLESS Vision frame",
                    )));
                }
                Poll::Ready(Ok(())) => self.read_wire.extend_from_slice(read_buf.filled()),
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S> AsyncWrite for VisionStream<S>
where
    S: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.poll_drain_pending(cx)?.is_pending() {
            return Poll::Pending;
        }
        if self.poll_finish_transition(cx)?.is_pending() {
            return Poll::Pending;
        }
        if !self.write_framing {
            return Pin::new(&mut self.inner).poll_write(cx, buf);
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let consumed = self.encode_write_frame(buf);
        let _ = self.poll_drain_pending(cx)?;
        Poll::Ready(Ok(consumed))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.poll_drain_pending(cx)?.is_pending() {
            return Poll::Pending;
        }
        if self.poll_finish_transition(cx)?.is_pending() {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.as_mut().poll_flush(cx)?.is_pending() {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
