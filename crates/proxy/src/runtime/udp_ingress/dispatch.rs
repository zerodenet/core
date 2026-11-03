use zero_core::{InboundUdpDispatch, SessionAuth};
use zero_engine::EngineError;

use super::model::UdpIngressRuntime;
use crate::runtime::pipe::{KernelPipe, UdpPipe, UdpPipeInput};
use crate::runtime::udp_dispatch::UdpDispatch;

impl UdpIngressRuntime {
    pub(crate) async fn new_dispatch(&self, inbound_tag: &str) -> Result<UdpDispatch, EngineError> {
        self.tcp_services.ensure_configuration_committed()?;
        UdpDispatch::new(self.clone(), inbound_tag).await
    }

    pub(crate) async fn dispatch_inbound_packet(
        &self,
        dispatch: &mut UdpDispatch,
        inbound_dispatch: &InboundUdpDispatch,
        auth: Option<&SessionAuth>,
        source_addr: Option<std::net::SocketAddr>,
    ) -> Result<u64, EngineError> {
        self.tcp_services.ensure_configuration_committed()?;
        if !self.udp_enabled_for_inbound(dispatch.inbound_tag()) {
            return Err(EngineError::Io(std::io::Error::other(
                "udp disabled for inbound",
            )));
        }

        UdpPipe::new(dispatch)
            .dispatch(UdpPipeInput::from_inbound_dispatch(
                inbound_dispatch,
                auth,
                source_addr,
            ))
            .await
    }

    fn udp_enabled_for_inbound(&self, inbound_tag: &str) -> bool {
        let config = self.tcp_services.config();
        config.runtime.udp.enabled
            && config
                .inbounds
                .iter()
                .find(|inbound| inbound.tag == inbound_tag)
                .map(|inbound| inbound.udp.enabled)
                .unwrap_or(true)
    }
}
