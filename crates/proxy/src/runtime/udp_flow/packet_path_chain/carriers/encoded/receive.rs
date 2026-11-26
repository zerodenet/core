//! Same actual packet RX, separated from pure TX dimension preparation.
use super::*;
impl Encoded {
    pub(super) async fn recv_decoded(
        &self,
        output: &mut [u8],
    ) -> Result<(usize, Option<SocketAddr>), EngineError> {
        match &self.memory {
            #[cfg(feature = "peer-raw-ip-runtime")]
            CodecMemory::Source(memory) => {
                memory
                    .receive(self.path.as_ref(), self.source.codec.as_ref(), output)
                    .await
            }
            CodecMemory::Business(memory) => {
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
            #[cfg(test)]
            CodecMemory::Library => self.receive_library(output).await,
        }
    }
}
