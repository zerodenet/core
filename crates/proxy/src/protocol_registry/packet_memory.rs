//! Actual Source-declared codec buffers, not another allocator or IO provider.
use zero_engine::EngineError;
use zero_tun::packet_memory::NativeReadGrant;
pub(crate) mod business;
#[derive(Clone)]
pub(crate) struct PacketCodecReceiveMemory {
    wire: NativeReadGrant,
    buffers: ReceiveWorkspaces,
}
// Both real protocol layouts are finite and inline. Cloning moves only their
// original grant handles; it never allocates a second array or read provider.
#[derive(Clone)]
enum ReceiveWorkspaces {
    One(NativeReadGrant),
    Two([NativeReadGrant; 2]),
}
impl ReceiveWorkspaces {
    fn as_slice(&self) -> &[NativeReadGrant] {
        match self {
            Self::One(original) => std::slice::from_ref(original),
            Self::Two(originals) => originals,
        }
    }
}
impl PacketCodecReceiveMemory {
    pub(crate) fn from_declared(
        wire: NativeReadGrant,
        first: NativeReadGrant,
        second: Option<NativeReadGrant>,
    ) -> Result<Self, EngineError> {
        let buffers = match second {
            Some(second) => ReceiveWorkspaces::Two([first, second]),
            None => ReceiveWorkspaces::One(first),
        };
        if wire.bytes() != 65536 {
            return Err(invalid());
        }
        let original = wire.memory_view().map_err(std::io::Error::other)?;
        for buffer in buffers.as_slice() {
            if !buffer
                .memory_view()
                .map_err(std::io::Error::other)?
                .same_kernel_root(&original)
            {
                return Err(invalid());
            }
        }
        Ok(Self { wire, buffers })
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
                .decode_buffer_layout(length)
                .map_err(std::io::Error::other)?;
            let grants = self.buffers.as_slice();
            if lengths.len() != grants.len()
                || lengths
                    .iter()
                    .zip(grants.iter())
                    .any(|(needed, grant)| *needed > grant.bytes())
            {
                return Err(invalid());
            }
            // Empty packet values allocate nothing. Each filled element owns
            // the SAME actual prepaid native read and erased-storage header.
            let mut buffers = [
                zero_traits::PacketBuffer::from(Vec::new()),
                zero_traits::PacketBuffer::from(Vec::new()),
            ];
            for (grant, target) in grants.iter().zip(&mut buffers) {
                let actual = grant.buffer().await.map_err(std::io::Error::other)?;
                *target = actual.into_packet(grant.bytes()).map_err(|_| invalid())?;
            }
            let decoded = codec
                .decode_buffered(&wire.as_bytes()[..length], &mut buffers[..grants.len()])
                .map_err(std::io::Error::other)?;
            let Some((target, port, range)) = decoded else {
                // Every allocated workspace, including extracted Domain backing,
                // drops only after this same rejected frame has finished decode.
                drop(buffers);
                tokio::task::consume_budget().await;
                continue;
            };
            let payload = buffers[..grants.len()]
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
