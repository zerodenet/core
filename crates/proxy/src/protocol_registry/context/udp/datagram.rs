use super::UdpRuntimeServices;
impl UdpRuntimeServices {
    pub(crate) fn datagram_execution(
        &self,
        inbound_tag: &str,
        session: &zero_core::Session,
    ) -> Result<crate::protocol_registry::TcpExecutionServices, zero_engine::EngineError> {
        Ok(self
            .tcp
            .clone()
            .for_inbound(inbound_tag.to_owned())
            .with_business_identity(session)?
            .execution())
    }
}
