//! Existing native TCP/codec streams use the physical packet FIFO's real permits.
use super::*;
impl VmessMuxConn {
    pub(in crate::mux) fn open_native_stream(
        self: &Arc<Self>,
        sid: u16,
        target: Address,
        port: u16,
        network: Network,
    ) -> Result<VmessMuxStream, RuntimeError> {
        let writer = match self.native_udp_tx.writer(encode_end_stream(sid)?) {
            Ok(writer) => writer,
            Err(source) => {
                self.release_stream(sid);
                return Err(source.into());
            }
        };
        let mut stream = self.open_reserved_stream(sid, target, port, network);
        stream.native_writer = Some(writer);
        Ok(stream)
    }
}
impl VmessMuxStream {
    pub(in crate::mux) fn poll_native_write(
        &mut self,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.ended {
            return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
        }
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        std::task::ready!(self.poll_native_flush(cx))?;
        std::task::ready!(self.native_writer.as_mut().unwrap().poll_ready(cx))?;
        // Encode only after real admission. Pending readiness retains no frame
        // copy and does not advance the protocol's NEW/KEEP state.
        Poll::Ready(self.queue_frame(bytes))
    }
    pub(in crate::mux) fn poll_native_flush(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        if self.write_pos < self.write_buf.len() {
            let writer = self.native_writer.as_mut().unwrap();
            std::task::ready!(writer.poll_send(cx, &self.write_buf[self.write_pos..]))?;
        }
        self.write_buf.clear();
        self.write_pos = 0;
        Poll::Ready(Ok(()))
    }
    pub(in crate::mux) fn poll_native_shutdown(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        std::task::ready!(self.poll_native_flush(cx))?;
        if !self.opened {
            std::task::ready!(self.native_writer.as_mut().unwrap().poll_ready(cx))?;
            self.queue_frame(&[])?;
        }
        // Drop sends reserved END behind every accepted frame in this FIFO.
        self.native_writer.take();
        self.ended = true;
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
#[path = "stream/tests.rs"]
mod tests;
