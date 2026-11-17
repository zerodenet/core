use crate::runtime::{
    udp_dispatch::{FlowFailure, FlowStartResult, UdpDispatch},
    udp_flow::{outbound::UdpFlowOutbound, registered::UpstreamAssociationSend},
};

impl UdpDispatch {
    pub(crate) async fn start_datagram(
        &mut self,
        services: crate::protocol_registry::UdpRuntimeServices,
        target: crate::inventory::DatagramAssociationTarget,
        session: &zero_core::Session,
        payload: &[u8],
    ) -> Result<FlowStartResult, FlowFailure> {
        use crate::runtime::udp_flow::registered::UpstreamAssociationTarget;
        let (tag, endpoint) = target.log_parts();
        let (server, port) = endpoint.ok_or_else(|| FlowFailure {
            stage: "udp_upstream_proxy",
            error: std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "multiple-target network association requires its declared endpoint",
            )
            .into(),
            upstream: None,
        })?;
        let sent = self
            .flow_state
            .start_upstream_flow(
                &self.inbound_tag,
                UpstreamAssociationSend {
                    services: Some(services),
                    session,
                    server,
                    port,
                    target: target.clone(),
                    payload,
                },
            )
            .await?;
        let managed = self.flow_state.register_upstream_flow(target.clone());
        Ok(FlowStartResult::Flow {
            outbound: Box::new(UdpFlowOutbound::Relay {
                tag: tag.to_owned(),
                server: server.to_owned(),
                port,
                managed,
            }),
            tx_bytes: sent as u64,
        })
    }
}
