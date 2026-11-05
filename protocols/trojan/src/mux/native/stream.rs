//! Existing native TCP/codec streams use the physical packet FIFO's real permits.
use super::*;
/// The SID is already admitted before waiting for an old FIFO END to drain.
/// Cancelled queue admission returns that exact reservation without a NEW.
pub(super) struct StreamReservation {
    stream: Option<TrojanMuxStream>,
}
impl StreamReservation {
    pub(super) fn new(
        connection: Arc<TrojanMuxConn>,
        sid: u16,
        target: Address,
        port: u16,
        network: Network,
    ) -> Self {
        let mut stream = connection.open_reserved_stream(sid, target, port, network);
        // The real stream/downlink reserves this SID in the existing map.
        // No NEW was accepted: cancellation must not send a legacy ending.
        stream.ended = true;
        Self {
            stream: Some(stream),
        }
    }
    pub(super) async fn open(mut self) -> Result<TrojanMuxStream, RuntimeError> {
        let stream = self.stream.as_ref().unwrap();
        let queue = stream.conn.as_ref().unwrap().native_udp_tx.clone();
        let writer = queue
            .writer_when_ready(encode_end_stream(stream.session_id)?)
            .await?;
        let mut stream = self.stream.take().unwrap();
        stream.native_writer = Some(writer);
        stream.ended = false;
        Ok(stream)
    }
}
impl TrojanMuxStream {
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
