use super::*;
impl UdpSniffingState {
    #[cfg(test)]
    pub(crate) fn new(
        policy: SniffingPolicy,
        runtime: crate::runtime::udp_ingress::UdpIngressRuntime,
    ) -> Self {
        Self::from_memory(policy, runtime, None)
    }
    pub(crate) fn try_new(
        policy: SniffingPolicy,
        runtime: crate::runtime::udp_ingress::UdpIngressRuntime,
    ) -> Result<Self, zero_engine::EngineError> {
        let memory = runtime.inspection_metadata()?;
        Ok(Self::from_memory(policy, runtime, Some(memory)))
    }
    fn from_memory(
        policy: SniffingPolicy,
        runtime: crate::runtime::udp_ingress::UdpIngressRuntime,
        memory: Option<zero_platform_tokio::contract::component::PacketDriverMetadataView>,
    ) -> Self {
        Self {
            policy,
            runtime,
            pending: Rows::new(memory.as_ref()),
            decisions: Rows::new(memory.as_ref()),
            packets: Packets::new(memory.as_ref(), MAX_QUEUED_DATAGRAMS),
            memory,
            output_pressure: None,
            next_sequence: 0,
            next_emit: 0,
            queued_datagrams: 0,
            queued_bytes: 0,
        }
    }
}

#[cfg(test)]
#[path = "source/tests.rs"]
mod tests;
