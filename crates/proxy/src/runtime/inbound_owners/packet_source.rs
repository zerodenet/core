//! Reuse only a live original Source whose full installed input is unchanged.
use super::*;
pub(crate) fn input_identity(input: &zero_config::InboundConfig) -> Result<[u8; 32], EngineError> {
    use sha2::{Digest, Sha256};
    // Opaque configuration hashing only: the host does not parse protocol
    // settings or retain another configuration/profile or resource owner.
    let encoded = serde_json::to_vec(input).map_err(std::io::Error::other)?;
    Ok(Sha256::digest(encoded).into())
}
impl InboundOwnerRegistry {
    #[cfg(all(feature = "raw-ip-runtime", feature = "datagram-route-runtime"))]
    pub(in crate::runtime) fn packet_source_memory(
        &self,
        input: &zero_config::InboundConfig,
    ) -> Result<Option<zero_platform_tokio::contract::packet::PacketSourceMemory>, EngineError>
    {
        let expected = input_identity(input)?;
        let owner = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&input.tag)
            .cloned();
        Ok(owner
            .filter(|owner| owner.is_running() && owner.source_input.get() == Some(&expected))
            .and_then(|owner| owner.packet_native.get().and_then(std::sync::Weak::upgrade))
            .and_then(|grant| grant.packet_memory()))
    }

    pub(in crate::runtime) fn packet_source_origin(
        &self,
        input: &zero_config::InboundConfig,
    ) -> Result<
        Option<zero_platform_tokio::controlled_datagram::provider::NativeDatagramOrigin>,
        EngineError,
    > {
        #[cfg(feature = "datagram-route-runtime")]
        {
            let expected = input_identity(input)?;
            let owner = self
                .entries
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&input.tag)
                .cloned();
            if let Some(owner) = owner
                .filter(|owner| owner.is_running() && owner.source_input.get() == Some(&expected))
            {
                return Ok(owner
                    .packet_native
                    .get()
                    .and_then(std::sync::Weak::upgrade)
                    .and_then(|original| original.original_origin()));
            }
        }
        let _ = input;
        Ok(None)
    }
}
