use std::net::SocketAddr;
use zero_core::{Address, InboundUdpDispatch, ProtocolType, SessionAuth};
use zero_engine::EngineError;

use crate::runtime::udp_dispatch::UdpDispatch;

use super::contract::KernelPipe;

/// Input for one UDP packet dispatch within an inbound UDP association.
pub(crate) struct UdpPipeInput<'a> {
    pub(crate) target: Address,
    pub(crate) port: u16,
    pub(crate) payload: &'a [u8],
    pub(crate) protocol: ProtocolType,
    pub(crate) auth: Option<&'a SessionAuth>,
    pub(crate) source_addr: Option<SocketAddr>,
    /// Optional protocol-supplied client-session isolation key.
    ///
    /// When present, flows that would collide on `(target, port)` alone are
    /// treated as independent relay sessions. Generic runtime preserves this
    /// opaque identity without interpreting its protocol-specific meaning.
    pub(crate) client_session_id: Option<u64>,
    /// Whether the IP target came from transparent interception.
    pub(crate) transparent_target: bool,
}

/// UDP datagram pipe.
pub(crate) struct UdpPipe<'a> {
    dispatch: &'a mut UdpDispatch,
}

impl<'a> UdpPipe<'a> {
    pub(crate) fn new(dispatch: &'a mut UdpDispatch) -> Self {
        Self { dispatch }
    }
}

impl KernelPipe for UdpPipe<'_> {
    type Input<'a> = UdpPipeInput<'a>;
    type Output = u64;
    type Error = EngineError;

    async fn dispatch(&mut self, input: Self::Input<'_>) -> Result<Self::Output, Self::Error> {
        UdpDispatch::dispatch(self.dispatch, input).await
    }
}

impl<'a> UdpPipeInput<'a> {
    pub(crate) fn from_inbound_dispatch(
        dispatch: &'a InboundUdpDispatch,
        auth: Option<&'a SessionAuth>,
        source_addr: Option<SocketAddr>,
    ) -> Self {
        Self {
            target: dispatch.target().clone(),
            port: dispatch.port(),
            payload: dispatch.payload(),
            protocol: dispatch.protocol(),
            auth,
            source_addr,
            client_session_id: dispatch.client_session_id(),
            transparent_target: dispatch.transparent_target(),
        }
    }
}
