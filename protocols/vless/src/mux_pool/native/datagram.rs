//! Packet framing on the same protocol MUX entry and original FIFO.
use super::*;
/// Direct packet access to the existing protocol map; no stream flattening or upload task.
pub(crate) struct DatagramIo {
    pub(crate) block_udp443: bool,
    pub(crate) observer: Option<Arc<dyn zero_platform_tokio::contract::context::ConnectObserver>>,
    pub(crate) session: Option<zero_core::component::SessionId>,
    pub(super) connection: Arc<MuxPoolConn>,
    pub(super) sid: u16,
    pub(super) rx: mpsc::Receiver<MuxDownlink<zero_core::UdpFlowPacket>>,
    pub(super) writer: Option<zero_transport::pooled_driver::datagram_queue::PacketWriter>,
    pub(super) first: bool,
    pub(super) origin: Option<crate::mux::origin::Origin>,
    pub(super) global_id: Option<[u8; 8]>,
}
impl zero_transport::pooled_driver::datagram::PacketIo for DatagramIo {
    fn send(&mut self, frame: &zero_core::packet::Datagram) -> std::io::Result<()> {
        if self.block_udp443 && frame.address.port == 443 {
            let mut failure = zero_platform_tokio::contract::ChannelError::new(
                zero_platform_tokio::contract::Stage::Send,
                zero_platform_tokio::contract::Reason::Unsupported,
                zero_platform_tokio::contract::Impact::Datagram,
            );
            failure.delivery = zero_platform_tokio::contract::DeliveryState::NotSubmitted;
            failure.session = self.session;
            return Err(std::io::Error::other(failure.with_source(
                zero_core::Error::Unsupported(
                    "VLESS Vision rejects UDP/443; use xtls-rprx-vision-udp443",
                ),
            )));
        }
        if self.connection.closed.load(Ordering::Acquire) {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        let mut wire = if self.first {
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
        if self.first {
            if let Some(origin) = &self.origin {
                origin.append(&mut wire).map_err(std::io::Error::other)?;
            }
        }
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
        drop(streams);
        if let Some(observer) = &self.observer {
            // Preserve this codec's existing Flow accounting boundary. MUX
            // and carrier framing remain metered by the real physical driver.
            observer.datagram_sent(self.session, frame.payload.len() as u64);
        }
        Ok(())
    }
    fn poll_send(
        &mut self,
        cx: &mut Context<'_>,
        frame: &zero_core::packet::Datagram,
    ) -> Poll<std::io::Result<()>> {
        if !(self.block_udp443 && frame.address.port == 443) {
            std::task::ready!(self
                .writer
                .as_mut()
                .expect("live packet writer")
                .poll_ready(cx))?;
        }
        Poll::Ready(self.send(frame))
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

#[cfg(test)]
#[path = "../../../tests/mux_pool/reverse_datagram.rs"]
mod tests;

impl DatagramIo {
    /// Target-free runtime control owns real VLESS wire fields, not a fabricated
    /// kernel AssociationId/Session. It uses this same bounded physical FIFO.
    pub(crate) fn poll_control(
        &mut self,
        cx: &mut Context<'_>,
        payload: &[u8],
    ) -> Poll<std::io::Result<()>> {
        let writer = self.writer.as_mut().expect("live original control writer");
        std::task::ready!(writer.poll_ready(cx))?;
        let address = zero_core::Address::Domain(crate::reverse::CONTROL_DOMAIN.into());
        let wire = if self.first {
            crate::mux::codec::encode_new_udp_data_frame_optional(
                self.sid, &address, 0, None, payload,
            )
        } else {
            crate::mux::encode_udp_data_frame(self.sid, &address, 0, payload)
        }
        .map_err(std::io::Error::other)?;
        writer.send(wire)?;
        self.first = false;
        Poll::Ready(Ok(()))
    }
}
