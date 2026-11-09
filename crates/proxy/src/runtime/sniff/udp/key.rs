//! This inspector's exact flow key, never a hash-only flow identity.
use super::*;
use zero_core::Address;
use zero_traits::packet_metadata::{PacketMemoryError, PacketMetadataCredit, PacketMetadataLayout};
pub(super) struct FlowKey {
    target: Address,
    port: u16,
    client_session_id: Option<u64>,
    // Original target storage is dropped before its exact allocation credit.
    _credit: Option<PacketMetadataCredit>,
}
impl FlowKey {
    pub(super) fn try_from_dispatch(
        dispatch: &InboundUdpDispatch,
        memory: Option<&zero_platform_tokio::contract::component::PacketDriverMetadataView>,
    ) -> Result<Self, PacketMemoryError> {
        let mut credit = None;
        let target = match (dispatch.target(), memory) {
            (Address::Domain(name), Some(memory)) if !name.is_empty() => {
                credit = Some(memory.reserve(PacketMetadataLayout::new(1, name.len()))?);
                // Box's requested byte layout is exact; conversion retains the
                // same sole allocation instead of a hidden Vec reserve peak.
                let bytes = Box::<[u8]>::from(name.as_bytes()).into_vec();
                Address::Domain(String::from_utf8(bytes).expect("same original UTF-8 target"))
            }
            (target, _) => target.clone(),
        };
        Ok(Self {
            target,
            port: dispatch.port(),
            client_session_id: dispatch.client_session_id(),
            _credit: credit,
        })
    }
    pub(super) fn matches(&self, dispatch: &InboundUdpDispatch) -> bool {
        &self.target == dispatch.target()
            && self.port == dispatch.port()
            && self.client_session_id == dispatch.client_session_id()
    }
    #[cfg(test)]
    pub(super) fn from_dispatch(dispatch: &InboundUdpDispatch) -> Self {
        Self::try_from_dispatch(dispatch, None).unwrap()
    }
}

#[cfg(test)]
#[path = "key/tests.rs"]
mod tests;
