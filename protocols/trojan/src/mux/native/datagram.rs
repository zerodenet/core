//! Packet framing on the same protocol MUX entry and original FIFO.
use super::*;
/// Direct packet access to the existing protocol map; no stream flattening or upload task.
pub(crate) struct DatagramIo {
    pub(super) connection: Arc<TrojanMuxConn>,
    pub(super) sid: u16,
    pub(super) rx: mpsc::Receiver<TrojanMuxDownlink>,
    pub(super) writer: Option<zero_transport::pooled_driver::datagram_queue::PacketWriter>,
    pub(super) first: bool,
    pub(super) target: Option<zero_core::request::Destination>,
}
impl zero_transport::pooled_driver::datagram::PacketIo for DatagramIo {
    fn send(&mut self, frame: &zero_core::packet::Datagram) -> std::io::Result<()> {
        if self.connection.closed.load(Ordering::Acquire) {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        if self
            .target
            .as_ref()
            .is_some_and(|target| target != &frame.address)
        {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        if frame.payload.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Trojan MUX does not support empty raw UDP packets",
            ));
        }
        let payload = frame.payload.as_ref();
        let wire = if self.first {
            encode_open_stream_with_network(
                self.sid,
                &frame.address.address,
                frame.address.port,
                Network::Udp,
                payload,
            )
        } else {
            encode_keep_stream(self.sid, payload)
        }
        .map_err(std::io::Error::other)?;
        let streams = self.connection.streams.lock().unwrap();
        if !streams.contains_key(&self.sid) {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        self.writer
            .as_mut()
            .expect("live packet writer")
            .send(wire)?;
        self.first = false;
        self.target = Some(frame.address.clone());
        Ok(())
    }
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> zero_transport::pooled_driver::datagram::PacketPoll {
        match std::task::ready!(self.rx.poll_recv(cx)) {
            Some(TrojanMuxDownlink::Data(packet)) => Poll::Ready(
                self.target
                    .clone()
                    .map(|target| Some((target, packet.into_inner())))
                    .ok_or_else(|| std::io::Error::other("UDP response before target submission")),
            ),
            Some(TrojanMuxDownlink::Overflow) => Poll::Ready(Err(std::io::Error::other(
                "MUX UDP response capacity exceeded",
            ))),
            None => Poll::Ready(Ok(None)),
        }
    }
}
impl Drop for DatagramIo {
    fn drop(&mut self) {
        drop(self.writer.take());
        self.connection.release_stream(self.sid);
    }
}
