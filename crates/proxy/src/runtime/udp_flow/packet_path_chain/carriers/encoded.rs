//! Compose an adapter-provided datagram codec over an already opened packet path.
use crate::runtime::udp_flow::packet_path::{PacketPathCarrier, UdpDatagramSource};
use std::sync::Arc;
use zero_core::Address;
use zero_engine::EngineError;

#[cfg(test)]
#[path = "tests/encoded.rs"]
mod tests;

pub(crate) fn wrap(
    path: Arc<dyn PacketPathCarrier>,
    source: UdpDatagramSource,
) -> Arc<dyn PacketPathCarrier> {
    Arc::new(Encoded { path, source })
}
struct Encoded {
    path: Arc<dyn PacketPathCarrier>,
    source: UdpDatagramSource,
}
#[async_trait::async_trait]
impl PacketPathCarrier for Encoded {
    async fn send_to(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        let wire = self
            .source
            .codec
            .encode(target, port, payload)
            .map_err(|error| EngineError::Io(std::io::Error::other(error)))?;
        let endpoint = self.source.descriptor().endpoint();
        self.path
            .send_to(&endpoint.target(), endpoint.port(), &wire)
            .await
    }
    async fn recv_from(&self, output: &mut [u8]) -> Result<usize, EngineError> {
        // The caller's plaintext buffer does not include intermediate framing.
        let mut wire = vec![0; 65536];
        loop {
            let length = self.path.recv_from(&mut wire).await?;
            let Some((_, _, payload)) = self.source.codec.decode(&wire[..length]) else {
                tokio::task::consume_budget().await;
                continue;
            };
            if payload.len() > output.len() {
                return Err(EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "decoded relay datagram exceeds receive buffer",
                )));
            }
            output[..payload.len()].copy_from_slice(&payload);
            return Ok(payload.len());
        }
    }
}
