use zero_core::Session;

use super::super::resume::ManagedUdpFlowResume;
use crate::protocol_registry::UdpRuntimeServices;
use crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier;
use crate::runtime::udp_flow::packet_path::ChainTask;
use crate::transport::RelayCarrier;

pub(crate) enum ManagedRelayStreamCarrier<'a> {
    Ready(RelayCarrier),
    Lazy(LazyTcpRelayCarrier<'a>),
}

impl From<RelayCarrier> for ManagedRelayStreamCarrier<'_> {
    fn from(carrier: RelayCarrier) -> Self {
        Self::Ready(carrier)
    }
}

impl<'a> From<LazyTcpRelayCarrier<'a>> for ManagedRelayStreamCarrier<'a> {
    fn from(carrier: LazyTcpRelayCarrier<'a>) -> Self {
        Self::Lazy(carrier)
    }
}

pub(crate) struct ManagedStreamPacketFlow<'a> {
    pub(crate) chain_tasks: &'a mut tokio::task::JoinSet<ChainTask>,
    pub(crate) services: UdpRuntimeServices,
    pub(crate) session: &'a Session,
    pub(crate) server: &'a str,
    pub(crate) port: u16,
    pub(crate) resume: ManagedUdpFlowResume,
    pub(crate) payload: &'a [u8],
}

pub(crate) struct ManagedRelayStreamFlow<'a> {
    pub(crate) chain_tasks: &'a mut tokio::task::JoinSet<ChainTask>,
    pub(crate) services: Option<UdpRuntimeServices>,
    pub(crate) session: &'a Session,
    pub(crate) carrier: ManagedRelayStreamCarrier<'a>,
    pub(crate) tls_server_name: Option<&'a str>,
    pub(crate) server: &'a str,
    pub(crate) port: u16,
    pub(crate) resume: ManagedUdpFlowResume,
    pub(crate) payload: &'a [u8],
}
