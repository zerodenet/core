//! Packet framing on the same protocol MUX entry and original FIFO.
use super::*;
/// Direct packet access to the existing protocol map; no stream flattening or upload task.
pub(crate) struct DatagramIo {
    pub(super) connection: Arc<VmessMuxConn>,
    pub(super) sid: u16,
    pub(super) rx: mpsc::Receiver<VmessMuxDownlink>,
    pub(super) writer: Option<zero_transport::pooled_driver::datagram_queue::PacketWriter>,
    pub(super) first: bool,
}
impl zero_transport::pooled_driver::datagram::PacketIo for DatagramIo {
    fn send(&mut self, frame: &zero_core::packet::Datagram) -> std::io::Result<()> {
        if self.connection.closed.load(Ordering::Acquire) {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        let payload = crate::udp::encode_udp_flow_packet(
            &frame.address.address,
            frame.address.port,
            &frame.payload,
        )
        .map_err(std::io::Error::other)?;
        let wire = if self.first {
            encode_open_stream_with_network(
                self.sid,
                &frame.address.address,
                frame.address.port,
                Network::Udp,
                &payload,
            )
        } else {
            encode_keep_stream(self.sid, &payload)
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
        Ok(())
    }
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> zero_transport::pooled_driver::datagram::PacketPoll {
        match std::task::ready!(self.rx.poll_recv(cx)) {
            Some(VmessMuxDownlink::Data(packet)) => Poll::Ready(
                crate::udp::decode_udp_flow_packet(&packet.into_inner())
                    .map(|packet| {
                        let (address, port, payload) = packet.into_parts();
                        Some((zero_core::request::Destination::new(address, port), payload))
                    })
                    .map_err(std::io::Error::other),
            ),
            Some(VmessMuxDownlink::Overflow) => Poll::Ready(Err(std::io::Error::other(
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
