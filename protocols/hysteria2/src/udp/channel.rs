//! A logical UDP session on a shared authenticated QUIC connection.
use super::{build_udp_fragments, dispatch::Registration, Hysteria2UdpReassembler};
use std::sync::Arc;
use tokio::sync::Mutex;
use zero_core::{Address, Error};

struct Receive {
    registration: Registration,
    reassembler: Hysteria2UdpReassembler,
}
pub struct Hysteria2UdpChannel {
    connection: Arc<crate::transport::Hysteria2AuthenticatedConnection>,
    id: u32,
    receive: Mutex<Receive>,
}
impl Hysteria2UdpChannel {
    pub fn new(
        connection: Arc<crate::transport::Hysteria2AuthenticatedConnection>,
    ) -> Result<Self, Error> {
        connection
            .require_udp()
            .map_err(|_| Error::Protocol("hysteria2 server does not support UDP relay"))?;
        if connection.connection().close_reason().is_some() {
            return Err(Error::Io("hysteria2 UDP connection closed"));
        }
        let registration = connection.udp_dispatcher().register()?;
        Ok(Self {
            id: registration.id,
            connection,
            receive: Mutex::new(Receive {
                registration,
                reassembler: Default::default(),
            }),
        })
    }
    pub async fn send_to(&self, target: &Address, port: u16, payload: &[u8]) -> Result<(), Error> {
        let conn = self.connection.connection();
        let max = conn
            .max_datagram_size()
            .ok_or(Error::Io("hysteria2 datagrams unavailable"))?;
        for fragment in build_udp_fragments(self.id, target, port, payload, max)? {
            conn.send_datagram(fragment.into())
                .map_err(|_| Error::Io("hysteria2 datagram send failed"))?;
        }
        Ok(())
    }
    pub async fn receive(&self) -> Result<(Address, u16, Vec<u8>), Error> {
        let mut receive = self.receive.lock().await;
        loop {
            if self.connection.connection().close_reason().is_some() {
                return Err(Error::Io("hysteria2 UDP connection closed"));
            }
            let packet = receive
                .registration
                .receiver
                .recv()
                .await
                .ok_or(Error::Io("hysteria2 UDP connection closed"))?;
            // Invalid fragments affect only this UDP session.
            if let Ok(Some(packet)) = receive.reassembler.push(packet) {
                return Ok(packet.into_datagram_parts());
            }
        }
    }
    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<usize, Error> {
        let (_, _, payload) = self.receive().await?;
        if payload.len() > buf.len() {
            return Err(Error::Io("hysteria2 UDP receive buffer too small"));
        }
        buf[..payload.len()].copy_from_slice(&payload);
        Ok(payload.len())
    }
}
