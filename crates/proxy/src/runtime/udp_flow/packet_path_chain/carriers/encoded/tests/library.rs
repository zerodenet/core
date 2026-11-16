//! Standalone wire fixtures only; no original provider or Host authority.
use super::*;
pub(super) fn wrap(
    path: Arc<dyn PacketPathCarrier>,
    source: UdpDatagramSource,
) -> Arc<dyn PacketPathCarrier> {
    super::wrap(path, source, CodecMemory::Library)
}
impl Encoded {
    pub(super) async fn send_library(
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
    pub(super) async fn receive_library(
        &self,
        output: &mut [u8],
    ) -> Result<(usize, Option<SocketAddr>), EngineError> {
        let mut wire = vec![0; 65536];
        loop {
            let length = self.path.recv_from(&mut wire).await?;
            let Some((source, port, payload)) = self.source.codec.decode(&wire[..length]) else {
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
            return Ok((payload.len(), packet_path_source(&source, port)));
        }
    }
}
