//! Generic UDP packet-path transport helpers.

use std::net::SocketAddr;
use std::sync::Arc;

use crate::RuntimeError;
use zero_core::Address;
use zero_traits::DatagramCodec;

pub struct UdpSocketPacketPath {
    socket: zero_platform_tokio::TokioDatagramSocket,
    endpoint: SocketAddr,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
}

impl UdpSocketPacketPath {
    pub async fn establish(
        endpoint: SocketAddr,
        codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
        sockets: &crate::OutboundDatagramSocketFactory,
    ) -> Result<Self, RuntimeError> {
        let socket = sockets.bind_tokio(endpoint).await?;
        Ok(Self {
            socket,
            endpoint,
            codec,
        })
    }

    pub async fn send_to(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), RuntimeError> {
        let packet = self.codec.encode(target, port, payload)?;
        self.socket.send_to_addr(&packet, self.endpoint).await?;
        Ok(())
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<usize, RuntimeError> {
        let (read, _) = self.socket.recv_from_addr(buf).await?;
        let (_, _, payload) = self.codec.decode(&buf[..read]).ok_or_else(|| {
            RuntimeError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "failed to decode UDP packet-path datagram",
            ))
        })?;
        copy_payload(buf, payload, "UDP socket carrier")
    }
}

#[cfg(feature = "quic")]
pub struct QuicDatagramPacketPath {
    conn: Arc<quinn::Connection>,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
}

#[cfg(feature = "quic")]
impl QuicDatagramPacketPath {
    pub fn new(
        conn: Arc<quinn::Connection>,
        codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
    ) -> Self {
        Self { conn, codec }
    }

    pub async fn send_to(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), RuntimeError> {
        let datagram = self.codec.encode(target, port, payload)?;
        self.conn.send_datagram(datagram.into()).map_err(|e| {
            RuntimeError::Io(std::io::Error::other(format!(
                "QUIC datagram packet-path carrier send: {e}"
            )))
        })?;
        Ok(())
    }

    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<usize, RuntimeError> {
        let data = self.conn.read_datagram().await.map_err(|e| {
            RuntimeError::Io(std::io::Error::other(format!(
                "QUIC datagram packet-path carrier recv: {e}"
            )))
        })?;
        let (_, _, payload) = self.codec.decode(&data).ok_or_else(|| {
            RuntimeError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "failed to decode QUIC packet-path datagram",
            ))
        })?;
        copy_payload(buf, payload, "QUIC datagram packet-path carrier")
    }
}

fn copy_payload(
    buf: &mut [u8],
    payload: Vec<u8>,
    carrier_name: &'static str,
) -> Result<usize, RuntimeError> {
    let len = payload.len();
    if len > buf.len() {
        return Err(RuntimeError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "{carrier_name} datagram ({len}B) exceeds recv buffer ({}B)",
                buf.len()
            ),
        )));
    }
    buf[..len].copy_from_slice(&payload);
    Ok(len)
}
