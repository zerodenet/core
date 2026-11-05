//! Same native physical FIFO for TCP NEW/DATA and reserved END.
use super::*;
impl MuxPoolConn {
    pub(in crate::mux_pool) fn open_bounded_tcp_stream(
        self: &Arc<Self>,
        sid: u16,
        port: u16,
        address: &Address,
    ) -> Result<MuxStreamRelay, RuntimeError> {
        let writer = match self.native_udp_tx.writer(encode_mux_end_frame(sid)?) {
            Ok(writer) => writer,
            Err(source) => {
                self.release_stream(sid);
                return Err(source.into());
            }
        };
        let (tx, rx) = mpsc::channel(self.response_backlog_frames + 1);
        self.streams.lock().unwrap().insert(
            sid,
            MuxClientStreamState {
                downlink: MuxClientDownlink::Tcp(tx),
                target: Some((address.clone(), port)),
            },
        );
        Ok(MuxStreamRelay {
            native_writer: Some(writer),
            native_initial: Some((port, address.clone())),
            write_open: true,
            sid,
            down_rx: Some(rx),
            read_pending: Vec::new(),
            read_offset: 0,
            conn: self.clone(),
        })
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
