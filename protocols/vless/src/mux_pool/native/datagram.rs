//! Packet framing on the same protocol MUX entry and original FIFO.
use super::*;
/// Direct packet access to the existing protocol map; no stream flattening or upload task.
pub(crate) struct DatagramIo {
    pub(super) connection: Arc<MuxPoolConn>,
    pub(super) sid: u16,
    pub(super) rx: mpsc::Receiver<MuxDownlink<zero_core::UdpFlowPacket>>,
    pub(super) writer: Option<zero_transport::pooled_driver::datagram_queue::PacketWriter>,
    pub(super) first: bool,
    pub(super) global_id: Option<[u8; 8]>,
}
impl zero_transport::pooled_driver::datagram::PacketIo for DatagramIo {
    fn send(&mut self, frame: &zero_core::packet::Datagram) -> std::io::Result<()> {
        if self.connection.closed.load(Ordering::Acquire) {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        let wire = if self.first {
            crate::mux::codec::encode_new_udp_data_frame_optional(
                self.sid,
                &frame.address.address,
                frame.address.port,
                self.global_id,
                &frame.payload,
            )
        } else {
            crate::mux::encode_udp_data_frame(
                self.sid,
                &frame.address.address,
                frame.address.port,
                &frame.payload,
            )
        }
        .map_err(std::io::Error::other)?;
        let mut streams = self.connection.streams.lock().unwrap();
        if !streams.contains_key(&self.sid) {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        self.writer
            .as_mut()
            .expect("live packet writer")
            .send(wire)?;
        self.first = false;
        if let Some(state) = streams.get_mut(&self.sid) {
            state.target = Some((frame.address.address.clone(), frame.address.port));
        }
        Ok(())
    }
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> zero_transport::pooled_driver::datagram::PacketPoll {
        match std::task::ready!(self.rx.poll_recv(cx)) {
            Some(MuxDownlink::Data(packet)) => {
                let (address, port, payload) = packet.into_inner().into_parts();
                Poll::Ready(Ok(Some((
                    zero_core::request::Destination::new(address, port),
                    payload,
                ))))
            }
            Some(MuxDownlink::Overflow) => Poll::Ready(Err(std::io::Error::other(
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
