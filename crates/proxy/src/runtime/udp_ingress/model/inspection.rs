use super::*;
impl UdpIngressRuntime {
    pub(crate) fn inspection_metadata(
        &self,
    ) -> Result<
        zero_platform_tokio::contract::component::PacketDriverMetadataView,
        zero_engine::EngineError,
    > {
        let host = self
            .tcp_services
            .execution()
            .current_source
            .upgrade()
            .ok_or_else(|| {
                std::io::Error::other("passive UDP inspection has no actual Host memory owner")
            })?;
        host.packet_path_memory()
            .map(|(_, metadata)| metadata)
            .map_err(Into::into)
    }
}
