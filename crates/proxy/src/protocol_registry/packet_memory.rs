//! Actual Source-declared codec buffers, not another allocator or IO provider.
use std::sync::Arc;
use zero_engine::EngineError;
use zero_tun::packet_memory::NativeReadGrant;
pub(crate) mod business;
#[derive(Clone)]
pub(crate) struct PacketCodecReceiveMemory {
    wire: NativeReadGrant,
    buffers: Arc<[NativeReadGrant]>,
}
impl PacketCodecReceiveMemory {
    pub(crate) fn from_declared(
        wire: NativeReadGrant,
        buffers: Vec<NativeReadGrant>,
    ) -> Result<Self, EngineError> {
        if wire.bytes() != 65536 || buffers.is_empty() || buffers.len() > 2 {
            return Err(invalid());
        }
        let original = wire.memory_view().map_err(std::io::Error::other)?;
        for buffer in &buffers {
            if !buffer
                .memory_view()
                .map_err(std::io::Error::other)?
                .same_kernel_root(&original)
            {
                return Err(invalid());
            }
        }
        Ok(Self {
            wire,
            buffers: buffers.into(),
        })
    }
    pub(crate) fn matches_caller(
        &self,
        caller: &zero_platform_tokio::contract::component::GrantedPacketServiceCaller,
    ) -> bool {
        self.wire
            .memory_view()
            .is_ok_and(|view| caller.matches_memory_root(&view))
    }
    pub(crate) async fn receive(
        &self,
        path: &dyn crate::runtime::udp_flow::packet_path::PacketPathCarrier,
        codec: &dyn zero_traits::DatagramCodec<zero_core::Address, Error = zero_core::Error>,
        output: &mut [u8],
    ) -> Result<(usize, Option<std::net::SocketAddr>), EngineError> {
        if output.len() < self.wire.bytes() {
            return Err(invalid());
        }
        let mut wire = self.wire.buffer().await.map_err(std::io::Error::other)?;
        loop {
            let length = path.recv_from(wire.as_mut_bytes()).await?;
            if length > wire.as_bytes().len() {
                return Err(invalid());
            }
            let lengths = codec
                .decode_buffer_lengths(length)
                .map_err(std::io::Error::other)?;
            if lengths.len() != self.buffers.len()
                || lengths
                    .iter()
                    .zip(self.buffers.iter())
                    .any(|(needed, grant)| *needed > grant.bytes())
            {
                return Err(invalid());
            }
            let mut buffers = Vec::with_capacity(self.buffers.len());
            for grant in self.buffers.iter() {
                let actual = grant.buffer().await.map_err(std::io::Error::other)?;
                buffers.push(actual.into_packet(grant.bytes()).map_err(|_| invalid())?);
            }
            let decoded = codec
                .decode_buffered(&wire.as_bytes()[..length], &mut buffers)
                .map_err(std::io::Error::other)?;
            let Some((target, port, range)) = decoded else {
                // Every allocated workspace, including extracted Domain backing,
                // drops only after this same rejected frame has finished decode.
                drop(buffers);
                tokio::task::consume_budget().await;
                continue;
            };
            let payload = buffers
                .get(range.buffer)
                .and_then(|buffer| buffer.get(range.range))
                .ok_or_else(invalid)?;
            if payload.len() > output.len() {
                return Err(invalid());
            }
            output[..payload.len()].copy_from_slice(payload);
            let result = (
                payload.len(),
                crate::runtime::udp_flow::packet_path::packet_path_source(&target, port),
            );
            // Target owns its moved String allocation. Its original credit stays
            // in these exact buffers until Target is explicitly gone first.
            drop(target);
            drop(buffers);
            return Ok(result);
        }
    }
}
fn invalid() -> EngineError {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "actual prepaid codec receive layout changed",
    )
    .into()
}
