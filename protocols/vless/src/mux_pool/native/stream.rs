//! Same native physical FIFO for TCP NEW/DATA and reserved END.
use super::*;
pub(super) struct StreamReservation {
    stream: Option<MuxStreamRelay>,
}
impl StreamReservation {
    pub(super) fn new(
        connection: Arc<MuxPoolConn>,
        sid: u16,
        port: u16,
        address: &Address,
    ) -> Self {
        let (tx, rx) = mpsc::channel(connection.response_backlog_frames + 1);
        connection.streams.lock().unwrap().insert(
            sid,
            MuxClientStreamState {
                downlink: MuxClientDownlink::Tcp(tx),
                target: Some((address.clone(), port)),
            },
        );
        Self {
            stream: Some(MuxStreamRelay {
                native_writer: None,
                native_initial: Some((port, address.clone())),
                // No NEW is accepted while this actual stream waits for its FIFO
                // control permit. Drop only returns this real SID/downlink.
                write_open: false,
                sid,
                down_rx: Some(rx),
                read_pending: Vec::new(),
                read_offset: 0,
                conn: connection,
            }),
        }
    }
    pub(super) async fn open(mut self) -> Result<MuxStreamRelay, RuntimeError> {
        let stream = self.stream.as_ref().unwrap();
        let queue = stream.conn.native_udp_tx.clone();
        let writer = queue
            .writer_when_ready(encode_mux_end_frame(stream.sid)?)
            .await?;
        let mut stream = self.stream.take().unwrap();
        stream.native_writer = Some(writer);
        stream.write_open = true;
        Ok(stream)
    }
}
impl MuxStreamRelay {
    pub(in crate::mux_pool) fn poll_native_initial(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        if let (Some(writer), Some((port, address))) =
            (&mut self.native_writer, &self.native_initial)
        {
            std::task::ready!(writer.poll_ready(cx))?;
            let initial = encode_mux_new_stream(self.sid, crate::mux::NETWORK_TCP, *port, address)
                .map_err(io::Error::other)?;
            writer.send(initial)?;
            self.native_initial = None;
        }
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
#[path = "stream/tests.rs"]
mod tests;
