//! A VMess empty chunk ends one logical direction, not the TLS/H2 carrier.
use super::*;
#[derive(Clone, Copy, Default)]
pub(super) enum NativeShutdown {
    #[default]
    Idle,
    Reader,
    Writer,
    Complete,
}
impl<S: AsyncRead + AsyncWrite + Unpin> VmessAeadStream<S> {
    pub(super) fn poll_framed_read_end(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.write_shutdown_flushed {
            return Poll::Ready(Ok(()));
        }
        match self.native_shutdown {
            NativeShutdown::Idle => self.native_shutdown = NativeShutdown::Reader,
            NativeShutdown::Reader => {}
            // A still-pending writer owns its original native shutdown waker.
            // Do not overwrite it from read or require that finished read half
            // to keep polling the opposite application operation.
            NativeShutdown::Writer | NativeShutdown::Complete => return Poll::Ready(Ok(())),
        }
        self.poll_native_shutdown(cx)
    }
    pub(super) fn poll_shutdown_encrypted(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.chunk_stream {
            return Pin::new(&mut self.inner).poll_shutdown(cx);
        }
        match self.poll_flush_pending(cx) {
            Poll::Ready(Ok(())) => {}
            other => return other,
        }
        if !self.write_shutdown_sent {
            self.write_buf = self.writer.seal_chunk(&[]).map_err(protocol_error)?;
            self.write_pos = 0;
            self.write_shutdown_sent = true;
            match self.poll_flush_pending(cx) {
                Poll::Ready(Ok(())) => {}
                other => return other,
            }
        }
        match Pin::new(&mut self.inner).poll_flush(cx) {
            Poll::Ready(Ok(())) => {}
            other => return other,
        }
        self.write_shutdown_flushed = true;
        if !matches!(self.read_state, ReadState::Eof) {
            return Poll::Ready(Ok(()));
        }
        match self.native_shutdown {
            NativeShutdown::Idle => self.native_shutdown = NativeShutdown::Writer,
            NativeShutdown::Writer => {}
            NativeShutdown::Reader | NativeShutdown::Complete => return Poll::Ready(Ok(())),
        }
        self.poll_native_shutdown(cx)
    }
    fn poll_native_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match Pin::new(&mut self.inner).poll_shutdown(cx) {
            Poll::Ready(Ok(())) => {
                self.native_shutdown = NativeShutdown::Complete;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}
