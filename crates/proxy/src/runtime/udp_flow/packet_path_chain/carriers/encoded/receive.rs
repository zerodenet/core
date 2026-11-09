//! Same actual packet RX, separated from pure TX dimension preparation.
use super::*;
impl Encoded {
    pub(super) async fn recv_decoded(
        &self,
        output: &mut [u8],
    ) -> Result<(usize, Option<SocketAddr>), EngineError> {
        if self.has_driver_handoff() {
            let memory = self.memory.as_ref().ok_or_else(layout_error)?;
            return memory
                .receive(self.path.as_ref(), self.source.codec.as_ref(), output)
                .await;
        }
        if let Some(memory) = &self.business {
            let mut wire = memory.buffer(65536)?;
            loop {
                let length = self.path.recv_from(&mut wire).await?;
                let decoded = match memory.decode(self.source.codec.as_ref(), &wire[..length]) {
                    Err(source)
                        if crate::protocol_registry::packet_memory::business::is_pressure(
                            &source,
                        ) =>
                    {
                        tracing::warn!(%source,"original Host cannot admit this new codec response");
                        tokio::task::consume_budget().await;
                        continue;
                    }
                    result => result?,
                };
                let Some((target, port, payload)) = decoded else {
                    tokio::task::consume_budget().await;
                    continue;
                };
                if payload.len() > output.len() {
                    drop(target);
                    drop(payload);
                    return Err(layout_error());
                }
                output[..payload.len()].copy_from_slice(&payload);
                let result = (payload.len(), packet_path_source(&target, port));
                drop(target);
                drop(payload);
                return Ok(result);
            }
        }
        // Remaining compatibility consumers retain their previous ownership;
        // absence of a Source grant does not identify a library caller.
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
